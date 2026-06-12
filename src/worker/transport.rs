use crate::coordinator::MetricsStore;
use crate::stage::{LocalStage, RemoteStage};
use async_trait::async_trait;
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
/// One connection handles every partition in the `target_partitions` range requested at open
/// time, so the implementation can reuse a single underlying network/IPC stream and fan messages
/// out to per-partition queues. Each partition can be streamed exactly once.
pub trait WorkerConnection: Send + Sync {
    /// Streams the given output `partition`. The connection is opened per stage, so it closes over
    /// the stage rather than taking it per call. Consumers do not care if the implementation pulls
    /// data over the wire or from local comms. Streaming the same partition twice is an error.
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// Everything a [WorkerDispatch] needs to deliver one stage's plans to its workers.
///
/// The coordinator computes the per-task worker assignment (`routed_urls[i]` is the worker for
/// task `i`) and hands the transport this request; the transport delivers each task's plan and
/// wires up whatever per-task back-channels it needs. `join_set` is the query's, so background
/// delivery work spawned onto it propagates failures to the query head.
#[non_exhaustive]
pub struct WorkerDispatchRequest<'a> {
    pub stage: &'a LocalStage,
    pub routed_urls: &'a [Url],
    pub task_ctx: &'a Arc<TaskContext>,
    pub metrics: &'a ExecutionPlanMetricsSet,
    /// Back-channel for task metrics. Only the Flight transport reads it (workers push
    /// their metrics back over its gRPC stream); other transports ignore it.
    pub metrics_store: Option<&'a Arc<MetricsStore>>,
    pub join_set: &'a mut JoinSet<Result<()>>,
}

/// The producer's send end for one partition channel, symmetric to a [WorkerConnection] read.
///
/// Contract with the produce loop:
/// - Batches arrive in `send` order and can be assumed non-empty.
/// - After a failed `send` the channel state is unspecified, but the caller still calls
///   `finish` so the consumer sees EOF; `finish` must tolerate a prior `send` error.
/// - Dropping a sink without calling `finish` does not end the channel, by design: `finish` is
///   async so Drop can't run it, and an implicit EOF would make an aborted producer look like a
///   clean, short stream. Abnormal teardown belongs to the transport, not the sink.
/// - `send` borrows the batch because transports serialize it into their own buffers; none
///   needs ownership.
#[async_trait]
pub trait PartitionSink: Send {
    /// Sends one batch. Async so a blocked send can yield and let the transport make progress
    /// elsewhere; a full channel must not park the calling thread.
    async fn send(&mut self, batch: &RecordBatch) -> Result<()>;
    /// Per-channel EOF, independent of the underlying link. Async for the same reason as `send`.
    async fn finish(self: Box<Self>) -> Result<()>;
}

/// The producer (write) side: opens a [PartitionSink] per output partition, symmetric to
/// [WorkerConnection] (the read side). The worker's produce loop builds one and pushes each output
/// batch in. A non-Flight transport (e.g. a shared-memory mesh) provides the implementation; it is
/// constructed by the producer (which knows the per-partition routing), not handed out by the
/// consume-side transport.
///
/// Flight has no [WorkerSink]: it produces inside its gRPC worker service (a streaming response
/// bound to the request handler), so there is no free-standing sink.
pub trait WorkerSink: Send + Sync {
    /// Takes `stage` and `partition` separately because one sink serves every stage, unlike the
    /// per-stage read connection that closes over its stage.
    ///
    /// `stage` is the producing stage's number and `partition` the producer task's own output
    /// partition index, before routing. Several producer tasks of one stage may hold sinks for
    /// the same pair, and the consumer merges them, so one `finish` is one producer task's EOF,
    /// not channel completion (which stays transport-defined).
    fn open_partition(&self, stage: usize, partition: usize) -> Result<Box<dyn PartitionSink>>;
}

/// The plan-delivery (write) side of a transport, symmetric to [WorkerConnection] (the read side).
/// A dispatcher is a per-query object: [WorkerTransport::dispatcher] creates it before the first
/// stage is dispatched, and every stage of that query goes through the same instance.
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
/// (`FlightWorkerTransport`). Custom transports (e.g. shared-memory queues for an embedded
/// execution context) plug in via [crate::DistributedExt::with_distributed_worker_transport].
pub trait WorkerTransport: Send + Sync {
    /// Opens a connection to the worker hosting `target_task` of `input_stage` covering the
    /// partitions in `target_partitions`. The returned [WorkerConnection] takes ownership of any
    /// background resources (gRPC streams, demux tasks, cancellation tokens, ...) and is expected
    /// to clean them up on drop. Bypassing the network for a worker co-located with the
    /// coordinator is the implementation's concern (Flight compares the target URL against its
    /// own).
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection>>;

    /// Creates the plan-delivery side of this transport for one query.
    ///
    /// Called once per query, before any stage is dispatched, so the returned [WorkerDispatch]
    /// can hold per-query state. Flight uses this to share one set of plan-send metrics and one
    /// query start timestamp across every stage's dispatch; a fresh dispatcher per stage would
    /// register duplicate metrics and skew the per-task time offsets derived from that timestamp.
    fn dispatcher(&self) -> Box<dyn WorkerDispatch>;
}

impl WorkerTransport for Arc<dyn WorkerTransport> {
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection>> {
        self.as_ref()
            .open(input_stage, target_partitions, target_task, ctx, metrics)
    }

    fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
        self.as_ref().dispatcher()
    }
}
