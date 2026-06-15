use crate::common::serialize_uuid;
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::MetricsStore;
use crate::coordinator::dispatch_metrics::CoordinatorToWorkerMetrics;
use crate::coordinator::plan_encoding::encode_task_plan;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::tonic_status_to_datafusion_error;
use crate::stage::LocalStage;
use crate::work_unit_feed::{
    build_work_unit_msg, collect_task_work_unit_feeds, set_work_unit_send_time,
};
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::{TaskKey, get_distributed_channel_resolver};
use datafusion::common::Result;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use http::Extensions;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;
use uuid::Uuid;

/// Builder for the different kind of tasks that handle the communications between the
/// [DistributedExec] node to the workers. This struct is responsible for instantiating the tasks
/// as boxed futures so that [DistributedExec] can tokio-spawn them at will.
///
/// This struct is responsible for:
/// - Building tasks that communicate a serialized plan to multiple workers for further execution.
/// - Building tasks that stream partition feeds from local [WorkUnitFeedExec] nodes to their
///   remote counterparts.
pub(crate) struct CoordinatorToWorkerTaskSpawner<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    metrics: &'a CoordinatorToWorkerMetrics,
    task_metrics: Option<&'a Arc<MetricsStore>>,
    join_set: &'a mut JoinSet<Result<()>>,
}

impl<'a> CoordinatorToWorkerTaskSpawner<'a> {
    /// Builds a new [CoordinatorToWorkerTaskSpawner] based on the [Stage] that needs to be
    /// fanned out to multiple workers.
    pub(crate) fn new(
        stage: &'a LocalStage,
        metrics: &'a CoordinatorToWorkerMetrics,
        task_metrics: Option<&'a Arc<MetricsStore>>,
        join_set: &'a mut JoinSet<Result<()>>,
    ) -> Result<Self> {
        Ok(Self {
            plan: &stage.plan,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks,
            metrics,
            task_metrics,
            join_set,
        })
    }

    /// Sends a serialized plan to a specific worker and sets up the bidirectional gRPC stream.
    /// Returns the sender for outbound coordinator-to-worker messages and the receiver for
    /// inbound worker-to-coordinator messages.
    pub(crate) fn send_plan_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        url: Url,
    ) -> Result<(
        UnboundedSender<pb::CoordinatorToWorkerMsg>,
        UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    )> {
        let encoded = encode_task_plan(self.plan, task_i, self.task_count, ctx.session_config())?;
        let plan_size = encoded.plan_proto.len();

        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let msg = pb::CoordinatorToWorkerMsg {
            inner: Some(Inner::SetPlanRequest(pb::SetPlanRequest {
                plan_proto: encoded.plan_proto,
                task_count: self.task_count as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations: encoded.feed_declarations,
                target_worker_url: url.to_string(),
                query_start_time_ns: self.metrics.instantiation_time,
            })),
        };

        let (coordinator_to_worker_tx, coordinator_to_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (worker_to_coordinator_tx, worker_to_coordinator_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());

        let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
        headers.extend(get_passthrough_headers(ctx.session_config()));

        let request = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            futures::stream::once(async { msg }).chain(
                UnboundedReceiverStream::new(coordinator_to_worker_rx).map(set_work_unit_send_time),
            ),
        );

        let metrics = self.metrics.clone();

        self.join_set.spawn(async move {
            let start = Instant::now();
            let mut client = channel_resolver.get_worker_client_for_url(&url).await?;
            let response = client.coordinator_channel(request).await.map_err(|e| {
                tonic_status_to_datafusion_error(&e).unwrap_or_else(|| {
                    exec_datafusion_err!("Error sending plan to worker {url}: {e}")
                })
            })?;
            metrics.plan_send_latency.record(&start);
            metrics.plan_bytes_sent.add(plan_size);
            let mut worker_to_coordinator_stream = response.into_inner();
            while let Some(msg_or_err) = worker_to_coordinator_stream.next().await {
                let msg = match msg_or_err {
                    Ok(msg) => msg,
                    Err(err) => {
                        return Err(tonic_status_to_datafusion_error(err).unwrap_or_else(|| {
                            exec_datafusion_err!("Unknown error on worker to coordinator stream")
                        }));
                    }
                };
                if worker_to_coordinator_tx.send(msg).is_err() {
                    break; // receiver dropped
                }
            }
            Ok::<_, DataFusionError>(())
        });

        Ok((coordinator_to_worker_tx, worker_to_coordinator_rx))
    }

    pub(crate) fn metrics_collection_task(
        &mut self,
        task_i: usize,
        mut worker_to_coordinator_rx: UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    ) {
        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let task_metrics = self.task_metrics.cloned();
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            while let Some(msg) = worker_to_coordinator_rx.recv().await {
                let Some(inner) = msg.inner else { continue };

                match inner {
                    pb::worker_to_coordinator_msg::Inner::TaskMetrics(pre_order_metrics) => {
                        if let Some(task_metrics) = &task_metrics {
                            task_metrics.insert(task_key.clone(), pre_order_metrics);
                        }
                    }
                }
            }
        });
    }

    /// Launches the task that based on the different local [WorkUnitFeedExec] nodes, sends their
    /// inner [WorkUnitFeeds] over the network to their remote counterparts.
    ///
    /// Once this function is called, all the [WorkUnitFeedExec]s feeds will be consumed.
    pub(crate) fn work_unit_feed_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        tx: UnboundedSender<pb::CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        let mut futures = vec![];
        for mut stream in collect_task_work_unit_feeds(self.plan, &ctx, task_i, self.task_count)? {
            let tx = tx.clone();
            futures.push(Box::pin(async move {
                // Wrap each encoded work unit in the Flight envelope and push it over the
                // coordinator-to-worker stream.
                while let Some(work_unit) = stream.next().await {
                    if tx.send(build_work_unit_msg(work_unit?)).is_err() {
                        break; // channel closed.
                    }
                }
                Ok::<_, DataFusionError>(())
            }));
        }
        self.join_set.spawn(async move {
            futures::future::try_join_all(futures).await?;
            Ok(())
        });
        Ok(())
    }
}
