use crate::coordinator::MetricsStore;
use crate::stage::{LocalStage, RemoteStage};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::common::runtime::JoinSet;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::stream::BoxStream;
use std::ops::Range;
use std::sync::Arc;
use url::Url;

/// A live connection to a single worker that demultiplexes the underlying transport into one
/// stream per partition.
///
/// One connection handles every partition in the `target_partition_range` requested at open time,
/// so the implementation can reuse a single underlying network/IPC stream and fan messages out to
/// per-partition queues. Each partition can be streamed exactly once.
pub trait WorkerConnection {
    /// Streams the specified partition. Consumers do not care if the implementation pulls data
    /// over the wire or from local comms. Streaming the same partition twice is an error.
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// Everything a [WorkerDispatch] needs to deliver one stage's plans to its workers.
///
/// The coordinator computes the per-task worker assignment (`routed_urls[i]` is the worker for
/// task `i`) and hands the transport this request; the transport delivers each task's plan and
/// wires up whatever per-task back-channels it needs. `join_set` is the query's, so background
/// delivery work spawned onto it propagates failures to the query head.
pub struct WorkerDispatchRequest<'a> {
    pub stage: &'a LocalStage,
    pub routed_urls: &'a [Url],
    pub task_ctx: &'a Arc<TaskContext>,
    pub metrics: &'a ExecutionPlanMetricsSet,
    pub metrics_store: &'a Option<Arc<MetricsStore>>,
    pub join_set: &'a mut JoinSet<Result<()>>,
}

/// The plan-delivery (write) side of a transport, symmetric to [WorkerConnection] (the read side).
///
/// Flight resolves each worker's URL, encodes the plan, and ships a `SetPlanRequest` over a
/// bidirectional gRPC stream that also carries the work-unit feed and the metrics back-channel. A
/// co-located transport registers the plan in a local table, or no-ops because the worker already
/// holds the plan tree. The coordinator just calls `dispatch`; delivery is no longer a fixed gRPC
/// step it special-cases.
pub trait WorkerDispatch: Send + Sync {
    fn dispatch(&self, request: WorkerDispatchRequest<'_>) -> Result<()>;
}

/// Factory that opens a [WorkerConnection] to a single worker task and delivers plans to workers.
///
/// The default implementation is the Arrow-Flight gRPC transport baked into this crate
/// ([crate::FlightWorkerTransport]). Custom transports (e.g. shared-memory queues for an embedded
/// execution context) plug in via [crate::DistributedExt::with_distributed_worker_transport].
pub trait WorkerTransport {
    /// Opens a connection to the worker hosting `target_task` of `input_stage` covering the
    /// partitions in `target_partitions`. The returned [WorkerConnection] takes ownership of any
    /// background resources (gRPC streams, demux tasks, cancellation tokens, ...) and is expected
    /// to clean them up on drop.
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection + Send + Sync>>;

    /// The plan-delivery side of this transport.
    fn dispatch(&self) -> &dyn WorkerDispatch;
}

impl WorkerTransport for Arc<dyn WorkerTransport + Send + Sync> {
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection + Send + Sync>> {
        self.as_ref()
            .open(input_stage, target_partitions, target_task, ctx, metrics)
    }

    fn dispatch(&self) -> &dyn WorkerDispatch {
        self.as_ref().dispatch()
    }
}
