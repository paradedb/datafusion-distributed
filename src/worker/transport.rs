use crate::stage::RemoteStage;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::stream::BoxStream;
use std::ops::Range;
use std::sync::Arc;

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

/// Factory that opens a [WorkerConnection] to a single worker task.
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
}
