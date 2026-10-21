use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;

/// The producer's send end for one partition channel, symmetric to a [crate::WorkerChannel] read.
///
/// This lives in the shm module rather than the core transport surface: the shared-memory mesh
/// streams produced batches into per-partition sinks opened when an `ExecuteTaskFrame` arrives.
/// Flight produces inside its gRPC worker service and the in-memory transport pulls straight from
/// the local task registry, so neither needs it.
///
/// Contract with the produce loop:
/// - Batches arrive in `send` order and can be assumed non-empty.
/// - After a failed `send` the channel state is unspecified, but the caller still calls `finish` so
///   the consumer sees EOF; `finish` must tolerate a prior `send` error.
/// - Dropping a sink without calling `finish` does not end the channel, by design: `finish` is async
///   so Drop can't run it, and an implicit EOF would make an aborted producer look like a clean,
///   short stream. Abnormal teardown belongs to the transport, not the sink.
/// - `send` borrows the batch because transports serialize it into their own buffers; none needs
///   ownership.
#[async_trait]
pub trait PartitionSink: Send {
    /// Sends one batch. Async so a blocked send can yield and let the transport make progress
    /// elsewhere; a full channel must not park the calling thread.
    async fn send(&mut self, batch: &RecordBatch) -> Result<()>;
    /// Per-channel EOF, independent of the underlying link. Async for the same reason as `send`.
    async fn finish(self: Box<Self>) -> Result<()>;
    /// Whether the consumer cancelled this stream. The produce loop stops pulling its input when
    /// this turns true, so a cancel doesn't just skip the send, it ends the upstream scan and drops
    /// the input stream, cascading the cancel further up. Default `false` for links that don't carry
    /// a cancel signal.
    fn cancelled(&self) -> bool {
        false
    }
}
