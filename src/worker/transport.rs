use crate::Stage;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use futures::Stream;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

/// A schema-less stream of record batches produced by a single partition of a [WorkerConnection].
///
/// Operators wrap this with [datafusion::physical_plan::stream::RecordBatchStreamAdapter] when they
/// need a schema-bearing [datafusion::execution::SendableRecordBatchStream]. Keeping the transport
/// free of schema responsibilities makes alternative implementations (e.g. shared-memory queues
/// backing embedded executors) easier to write.
pub type WorkerPartitionStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send + 'static>>;

/// A live connection to a single worker that demultiplexes the underlying transport into one
/// [WorkerPartitionStream] per partition.
///
/// One connection handles every partition in the `target_partition_range` requested at open time,
/// so the implementation can reuse a single underlying network/IPC stream and fan messages out to
/// per-partition queues. Each partition can be streamed exactly once.
pub trait WorkerConnection: Send + Sync {
    /// Returns the stream of record batches for `partition`. Calling this twice for the same
    /// partition MUST return `Err(DataFusionError::Internal(...))`. Operators above (e.g.
    /// `NetworkShuffleExec`) do not retry, but pinning this contract lets future work assume
    /// `stream_partition` is a single-shot consumer per partition.
    fn stream_partition(&self, partition: usize) -> Result<WorkerPartitionStream>;

    /// Optional snapshot of metrics emitted by this connection. Operators surface these through
    /// their own `metrics()` method, so a transport that has nothing to report can stay at the
    /// default. The snapshot is taken at the moment of the call; metrics that update after the
    /// call won't be visible until the next snapshot.
    fn metrics(&self) -> MetricsSet {
        MetricsSet::new()
    }
}

/// Factory that opens a [WorkerConnection] to a single worker task.
///
/// The default implementation is the Arrow-Flight gRPC transport baked into this crate. Custom
/// transports (e.g. shared-memory queues for an embedded execution context) plug in via
/// [crate::DistributedExt::with_distributed_worker_transport].
///
/// `open` MUST NOT block on async I/O — it runs from a sync hot path inside `OnceLock::get_or_init`.
/// Implementations that need an async handshake should spawn a background task and surface errors
/// from the connection's first `stream_partition` call (the default `FlightWorkerTransport` does
/// this via `SpawnedTask`).
pub trait WorkerTransport: Send + Sync + 'static {
    /// Opens a connection to the worker hosting `target_task` of `input_stage` covering the
    /// partitions in `target_partitions`. The returned [WorkerConnection] takes ownership of any
    /// background resources (gRPC streams, demux tasks, cancellation tokens, ...) and is expected
    /// to clean them up on drop.
    fn open(
        &self,
        input_stage: &Stage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
    ) -> Result<Box<dyn WorkerConnection>>;
}
