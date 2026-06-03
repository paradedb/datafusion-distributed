//! The Arrow-Flight implementation of the pluggable transport extension points. New
//! Flight-side code lands here so the `flight` feature stays one gate at the `mod`
//! declaration instead of per-item attributes spread through transport-neutral files.

use crate::coordinator::{CoordinatorToWorkerMetrics, CoordinatorToWorkerTaskSpawner};
use crate::stage::RemoteStage;
use crate::worker::transport::{
    WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerTransport,
};
use crate::worker::worker_connection_pool::{
    LocalWorkerConnection, LocalWorkerContext, RemoteWorkerConnection,
};
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use std::ops::Range;
use std::sync::Arc;

/// The default [WorkerTransport]: opens an Arrow-Flight gRPC stream per remote task, or bypasses
/// gRPC with local comms when the target worker happens to be the current process.
#[derive(Clone, Default)]
pub struct FlightWorkerTransport;

impl WorkerTransport for FlightWorkerTransport {
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection + Send + Sync>> {
        let Some(target_url) = input_stage.workers.get(target_task) else {
            return internal_err!("input_stage.workers[{target_task}] out of range.");
        };
        if let Some(lw_ctx) = ctx.session_config().get_extension::<LocalWorkerContext>()
            && &lw_ctx.self_url == target_url
        {
            // Reach ourselves through local comms instead of a gRPC call.
            Ok(Box::new(LocalWorkerConnection::init(
                input_stage,
                target_partitions,
                target_task,
                lw_ctx,
                metrics,
            )))
        } else {
            RemoteWorkerConnection::init(input_stage, target_partitions, target_task, ctx, metrics)
                .map(|v| Box::new(v) as Box<dyn WorkerConnection + Send + Sync>)
        }
    }

    // The gRPC dispatch lives in `coordinator::task_spawner` next to the per-task send/metrics/feed
    // machinery it reuses; `FlightWorkerTransport` is both the read and the write side of Flight.
    fn dispatch(&self) -> &dyn WorkerDispatch {
        self
    }
}

/// The Flight transport delivers each task's plan over a bidirectional gRPC stream that also
/// carries the work-unit feed (coordinator -> worker) and the metrics back-channel
/// (worker -> coordinator). The per-task setup lives in [CoordinatorToWorkerTaskSpawner]; this
/// drives it for every task of the stage.
impl WorkerDispatch for FlightWorkerTransport {
    fn dispatch(&self, request: WorkerDispatchRequest<'_>) -> Result<()> {
        let WorkerDispatchRequest {
            stage,
            routed_urls,
            task_ctx,
            metrics,
            metrics_store,
            join_set,
        } = request;
        let metrics = CoordinatorToWorkerMetrics::new(metrics);
        let mut spawner = CoordinatorToWorkerTaskSpawner::new(
            stage,
            &metrics,
            metrics_store,
            task_ctx,
            join_set,
        )?;
        for (task, routed_url) in routed_urls.iter().enumerate() {
            let (tx, worker_rx) =
                spawner.send_plan_task(Arc::clone(task_ctx), task, routed_url.clone())?;
            spawner.metrics_collection_task(task, worker_rx);
            spawner.work_unit_feed_task(Arc::clone(task_ctx), task, tx)?;
        }
        Ok(())
    }
}
