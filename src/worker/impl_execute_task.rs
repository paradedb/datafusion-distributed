use crate::common::{TreeNodeExt, on_drop_stream};
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::TaskMetrics;
use crate::worker::worker_service::TaskDataEntries;
use crate::{DistributedConfig, DistributedTaskContext};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err, internal_err};

use crate::worker::generated::worker::ExecuteTaskRequest;
use crate::worker::task_data::TaskDataMetrics;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::oneshot::Sender;

const WAIT_PLAN_TIMEOUT_SECS: u64 = 10;

/// Builds several per-partition streams by retrieving the appropriate entry from [TaskDataEntries]
/// based on the task key extracted from [ExecuteTaskRequest].
///
/// This method is async mainly for the key retrieval operation from [TaskDataEntries], but it does
/// not start polling any stream, it just instantiates them.
pub(crate) async fn execute_local_task(
    task_data_entries: &Arc<TaskDataEntries>,
    body: ExecuteTaskRequest,
) -> Result<(Vec<SendableRecordBatchStream>, Arc<TaskContext>)> {
    let Some(key) = body.task_key.as_ref().cloned() else {
        return internal_err!("Missing task_key in LocalWorkerConnection");
    };
    let Some(producer_head) = body.producer_head.as_ref().cloned() else {
        return internal_err!("Missing producer_head");
    };
    let entry = task_data_entries
        .get_with(key.clone(), async { Default::default() })
        .await;

    // Other request is responsible for writing the plan that belongs to this TaskKey, so
    // we'll resolve immediately if it was already there, or wait until it's ready.
    let task_data = entry
        .read(Duration::from_secs(WAIT_PLAN_TIMEOUT_SECS))
        .await
        .map_err(|e| exec_datafusion_err!("Worker::execute_task timed-out while waiting for the plan to be set by the coordinator. ({e})"))?
        .map_err(DataFusionError::Shared)?;
    task_data.task_data_metrics.mark_execution_started_once();

    let plan = task_data.plan(producer_head)?;
    let task_ctx = task_data.task_ctx;
    let d_cfg = DistributedConfig::from_config_options(task_ctx.session_config().options())?;
    let d_ctx = *DistributedTaskContext::from_ctx(&task_ctx).as_ref();

    let send_metrics = d_cfg.collect_metrics;
    let partition_count = plan.properties().partitioning.partition_count();
    let plan_name = plan.name();

    // Execute all the requested partitions at once, and collect all the streams so that they
    // can be merged into a single one at the end of this function.
    let n_streams = body.target_partition_end - body.target_partition_start;
    let mut streams = Vec::with_capacity(n_streams as usize);
    for partition in body.target_partition_start..body.target_partition_end {
        if partition >= partition_count as u64 {
            return exec_err!(
                "partition {partition} not available. The head plan {plan_name} of the stage just has {partition_count} partitions"
            );
        }

        let stream = plan.execute(partition as usize, Arc::clone(&task_ctx))?;
        let stream_schema = plan.schema();

        let plan = Arc::clone(&plan);

        let task_data_entries = Arc::clone(task_data_entries);
        let num_partitions_remaining = Arc::clone(&task_data.num_partitions_remaining);
        let metrics_tx = Arc::clone(&task_data.metrics_tx);
        let task_data_metrics = Arc::clone(&task_data.task_data_metrics);
        let key = key.clone();
        let stream = on_drop_stream(stream, move || {
            // Stream was dropped before fully consumed -- see https://github.com/datafusion-contrib/datafusion-distributed/issues/412
            // Send metrics via the coordinator channel so they are not lost.
            if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                // Fire-and-forget background tokio task to handle async
                // invalidate() within synchronous on_drop_stream.
                #[allow(clippy::disallowed_methods)]
                tokio::spawn(async move {
                    task_data_entries.invalidate(&key).await;
                });
                task_data_metrics.mark_execution_finished();
                if send_metrics {
                    send_metrics_via_channel(&metrics_tx, &plan, d_ctx, &task_data_metrics);
                }
            }
        });
        streams.push(Box::pin(RecordBatchStreamAdapter::new(stream_schema, stream)) as _);
    }
    Ok((streams, task_ctx))
}

/// Per-node metrics of an executed plan in pre-order, the order the metrics rewriter consumes.
/// Nodes without metrics contribute an empty set so the positions stay aligned.
pub(crate) fn collect_plan_metrics_protos(
    plan: &Arc<dyn ExecutionPlan>,
    dt_ctx: DistributedTaskContext,
) -> Vec<pb::MetricsSet> {
    let mut pre_order_plan_metrics = vec![];
    let _ = plan.apply_with_dt_ctx(dt_ctx, |node, _| {
        pre_order_plan_metrics.push(
            node.metrics()
                .and_then(|m| df_metrics_set_to_proto(&m).ok())
                .unwrap_or_default(),
        );
        Ok(TreeNodeRecursion::Continue)
    });
    pre_order_plan_metrics
}

/// Collects metrics from the plan in pre-order traversal order and sends them via the
/// coordinator channel oneshot.
fn send_metrics_via_channel(
    metrics_tx: &Arc<Mutex<Option<Sender<TaskMetrics>>>>,
    plan: &Arc<dyn ExecutionPlan>,
    dt_ctx: DistributedTaskContext,
    task_data_metrics: &Arc<TaskDataMetrics>,
) {
    let pre_order_plan_metrics = collect_plan_metrics_protos(plan, dt_ctx);

    let tx = {
        let mut guard = match metrics_tx.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        guard.take()
    };
    let Some(tx) = tx else { return };
    // Ignore send errors — the coordinator channel may have been dropped (e.g. query cancelled).
    let _ = tx.send(TaskMetrics {
        pre_order_plan_metrics,
        task_metrics: Some(task_data_metrics.to_proto_metrics_set()),
    });
}
