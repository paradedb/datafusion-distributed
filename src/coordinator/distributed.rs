use crate::DistributedConfig;
use crate::common::{
    DistributedCancellationToken, on_drop_stream, require_one_child, serialize_uuid,
    task_ctx_with_extension,
};
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
use crate::distributed_planner::NetworkBoundaryExt;
use crate::worker::generated::worker::TaskKey;
use datafusion::common::internal_datafusion_err;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::TaskEstimator::route_tasks], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    plan: Arc<dyn ExecutionPlan>,
    prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    metrics: ExecutionPlanMetricsSet,
    pub(crate) metrics_store: Option<Arc<MetricsStore>>,
}

pub(super) struct PreparedPlan {
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    pub(super) join_set: JoinSet<Result<()>>,
}

impl DistributedExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            metrics_store: None,
        }
    }

    /// Enables task metrics collection from remote workers.
    pub fn with_metrics_collection(mut self, enabled: bool) -> Self {
        self.metrics_store = match enabled {
            true => Some(Arc::new(MetricsStore::new())),
            false => None,
        };
        self
    }

    /// Waits until all worker tasks have reported their metrics back via the coordinator channel.
    ///
    /// Metrics are delivered asynchronously after query execution completes, so callers that need
    /// complete metrics (e.g. for observability or display) should await this before inspecting
    /// [`Self::task_metrics`] or calling [`rewrite_distributed_plan_with_metrics`].
    ///
    /// [`rewrite_distributed_plan_with_metrics`]: crate::rewrite_distributed_plan_with_metrics
    pub async fn wait_for_metrics(&self) {
        let mut expected_keys: Vec<TaskKey> = Vec::new();
        let Some(task_metrics) = &self.metrics_store else {
            return;
        };
        let _ = self.plan.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                let stage = boundary.input_stage();
                for i in 0..stage.task_count() {
                    expected_keys.push(TaskKey {
                        query_id: serialize_uuid(&stage.query_id()),
                        stage_id: stage.num() as u64,
                        task_number: i as u64,
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if expected_keys.is_empty() {
            return;
        }
        let mut rx = task_metrics.rx.clone();
        let _ = rx
            .wait_for(|map| expected_keys.iter().all(|key| map.contains_key(key)))
            .await;
    }

    /// Prepares the plan for in-process execution through the registered [`crate::WorkerTransport`]
    /// and returns the head stage, ready to `execute()`.
    ///
    /// For embedders that drive worker fragments themselves (PG parallel workers, threads) instead
    /// of dispatching plans over the wire. Each network boundary is converted to a remote stage so
    /// it routes through the transport's `open()`; the transport's dispatcher does whatever the
    /// embedder needs (a no-op when its workers already hold the plan).
    ///
    /// What the embedder signs up for:
    /// - Call this once per query. Every call re-dispatches every boundary, and default routing
    ///   re-samples its starting URL.
    /// - Register a [`crate::WorkerResolver`]. Default routing needs at least one URL from it,
    ///   even a placeholder one when the transport routes by `target_task`; a custom
    ///   `route_tasks` has no such requirement.
    /// - Deliver plans synchronously inside dispatch. The query `join_set` is dropped on return,
    ///   so a dispatcher that spawned onto it gets an error rather than a silent abort. The
    ///   check runs after all boundaries dispatched, so whatever was already delivered stays
    ///   delivered.
    ///
    /// What this path does not do:
    /// - No cancellation token: teardown is the embedder's job.
    /// - No work-unit feeds: Flight pumps those from dispatch-spawned tasks, which don't exist
    ///   here, so feed-declaring plans are rejected.
    /// - No plan recording: metrics rewriting and `prepared_plan()` do not apply.
    pub fn prepare_in_process_plan(
        &self,
        ctx: &Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Feeds are pumped by dispatch-spawned background tasks, which this path does not run; a
        // plan that declares feeds would stall its worker fragments on channels nothing fills.
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        let registry = &d_cfg.__private_work_unit_feed_registry;
        let mut has_feeds = false;
        self.plan.apply(|plan| {
            if registry.get_work_unit_feed(plan).is_some() {
                has_feeds = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if has_feeds {
            return exec_err!(
                "the plan declares work-unit feeds, which are not delivered on the in-process path"
            );
        }

        let PreparedPlan {
            head_stage,
            join_set,
        } = prepare_static_plan(&self.plan, &self.metrics, &self.metrics_store, ctx)?;
        // Dropping the join_set aborts anything still on it, which would kill an async delivery
        // mid-flight and surface as a hang far from the cause. Reject instead.
        if !join_set.is_empty() {
            return exec_err!(
                "the registered transport spawned background delivery work; \
                 prepare_in_process_plan requires a transport whose dispatch completes \
                 synchronously"
            );
        }
        Ok(head_stage)
    }

    /// Returns the plan which is lazily prepared on `execute()` and actually gets executed.
    /// It is updated on every call to `execute()`. Returns an error if `.execute()` has not been
    /// called.
    pub(crate) fn prepared_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        self.prepared_plan
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
            })
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        // Mint one cancellation token for this execution and attach it to the context before the
        // plan is prepared, so the dispatch contexts handed to the transport carry it too, not
        // just the head-stage consume path. Producers and consumers watch it; dropping the head
        // stream fires it.
        let cancel = CancellationToken::new();
        let context = Arc::new(task_ctx_with_extension(
            &context,
            DistributedCancellationToken(cancel.clone()),
        ));

        let PreparedPlan {
            head_stage,
            join_set,
        } = prepare_static_plan(&self.plan, &self.metrics, &self.metrics_store, &context)?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {e}"))?;
            *guard = Some(head_stage.clone());
        }

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        // Spawn the task that pulls data from child...
        builder.spawn(async move {
            let mut stream = head_stage.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            Ok(())
        });
        // ...in parallel to the one that feeds the plan to workers.
        builder.spawn(async move {
            for res in join_set.join_all().await {
                res?;
            }
            Ok(())
        });
        let schema = self.schema();
        let stream = on_drop_stream(builder.build(), move || cancel.cancel());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::{LocalStage, Stage};
    use crate::worker::{WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerTransport};
    use crate::{DistributedExt, NetworkShuffleExec, WorkerResolver};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::SessionConfig;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use url::Url;
    use uuid::Uuid;

    struct OneUrlResolver;

    impl WorkerResolver for OneUrlResolver {
        fn get_urls(&self) -> Result<Vec<Url>> {
            Ok(vec![Url::parse("mem://0").unwrap()])
        }
    }

    #[derive(Default)]
    struct RecordingTransport {
        dispatches: Arc<AtomicUsize>,
        spawn_on_dispatch: bool,
    }

    struct RecordingDispatch {
        dispatches: Arc<AtomicUsize>,
        spawn_on_dispatch: bool,
    }

    impl WorkerTransport for RecordingTransport {
        fn open(
            &self,
            _input_stage: &crate::stage::RemoteStage,
            _target_partitions: Range<usize>,
            _target_task: usize,
            _ctx: &Arc<TaskContext>,
            _metrics: &ExecutionPlanMetricsSet,
        ) -> Result<Box<dyn WorkerConnection>> {
            datafusion::common::internal_err!("not used by this test")
        }

        fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
            Box::new(RecordingDispatch {
                dispatches: Arc::clone(&self.dispatches),
                spawn_on_dispatch: self.spawn_on_dispatch,
            })
        }
    }

    impl WorkerDispatch for RecordingDispatch {
        fn dispatch(&self, request: WorkerDispatchRequest<'_>) -> Result<()> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            if self.spawn_on_dispatch {
                request.join_set.spawn(async { Ok(()) });
            }
            Ok(())
        }
    }

    fn single_boundary_exec() -> DistributedExec {
        let child = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let plan =
            Arc::new(RepartitionExec::try_new(child, Partitioning::RoundRobinBatch(2)).unwrap());
        let stage = LocalStage {
            query_id: Uuid::new_v4(),
            num: 0,
            plan,
            tasks: 2,
        };
        DistributedExec::new(Arc::new(NetworkShuffleExec::from_stage(stage)))
    }

    fn ctx_with(transport: RecordingTransport) -> Arc<TaskContext> {
        let mut cfg = SessionConfig::new();
        cfg.set_distributed_worker_transport(transport);
        cfg.set_distributed_worker_resolver(OneUrlResolver);
        Arc::new(TaskContext::default().with_session_config(cfg))
    }

    #[test]
    fn prepare_in_process_converts_boundaries_and_dispatches_once() -> Result<()> {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let ctx = ctx_with(RecordingTransport {
            dispatches: Arc::clone(&dispatches),
            spawn_on_dispatch: false,
        });

        let head = single_boundary_exec().prepare_in_process_plan(&ctx)?;
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);

        let mut boundaries = 0;
        head.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                boundaries += 1;
                assert!(matches!(boundary.input_stage(), Stage::Remote(_)));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(boundaries, 1);
        Ok(())
    }

    #[tokio::test]
    async fn prepare_in_process_rejects_async_dispatch() {
        let ctx = ctx_with(RecordingTransport {
            dispatches: Arc::new(AtomicUsize::new(0)),
            spawn_on_dispatch: true,
        });

        let err = single_boundary_exec()
            .prepare_in_process_plan(&ctx)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("completes"), "unexpected error: {err}");
    }
}
