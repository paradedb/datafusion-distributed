use crate::common::now_ns;
use crate::worker::generated::worker as pb;
use crate::{BytesMetricExt, LatencyMetricExt, WorkUnit};
use datafusion::common::{HashMap, Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::MetricBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::common::serialize_uuid;
use tokio::sync::mpsc::UnboundedSender;

pub(crate) type WorkUnitTx = UnboundedSender<Result<pb::WorkUnit>>;
pub(crate) type WorkUnitRx = UnboundedReceiver<Result<pb::WorkUnit>>;
pub(crate) type RemoteWorkUnitFeedRxs = HashMap<(Uuid, usize), Mutex<Option<WorkUnitRx>>>;
pub(crate) type RemoteWorkUnitFeedTxs = HashMap<(Uuid, usize), WorkUnitTx>;

/// Bridge between the worker's transport receive path and the remote-variant
/// [`crate::WorkUnitFeed`]s installed in the deserialized plan.
///
/// One (sender, receiver) pair is created per `(feed id, partition)` when a new plan is
/// set on the worker:
/// - The **senders** are used by the transport receive path to push the serialized
///   [`crate::WorkUnit`]s that arrive from the coordinator into the right queue.
/// - The **receivers** are consumed by the worker-side [`RemoteFeedProvider`] (the remote
///   variant of [`crate::WorkUnitFeed`]), which decodes the bytes back into the leaf plan's
///   concrete `T::WorkUnit` type so the leaf sees the same typed stream as it would in a
///   single-node execution.
///
/// This is transport-neutral in shape: the Flight worker service fills the senders from its gRPC
/// stream, and an in-crate transport can build these channels and feed them its own way. The
/// builder helpers are crate-private, so an out-of-crate transport cannot reach them yet.
#[derive(Default)]
pub(crate) struct WorkUnitFeedChannels {
    pub(crate) receivers: RemoteWorkUnitFeedRxs,
    pub(crate) senders: RemoteWorkUnitFeedTxs,
}

impl WorkUnitFeedChannels {
    /// Creates all the receivers and senders for a specific [WorkUnit] Feed id. One feed per
    /// partition is created.
    pub(crate) fn add(&mut self, id: Uuid, partitions: usize) {
        for partition in 0..partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.receivers.insert((id, partition), Mutex::new(Some(rx)));
            self.senders.insert((id, partition), tx);
        }
    }
}

/// Encodes one [WorkUnit] into its wire message. Transport-neutral: the coordinator side of any
/// transport produces these, and the worker decodes them back via [`RemoteFeedProvider`].
pub(crate) fn build_work_unit(
    id: &Uuid,
    partition: usize,
    work_unit: Box<dyn WorkUnit>,
) -> pb::WorkUnit {
    pb::WorkUnit {
        id: serialize_uuid(id),
        partition: partition as u64,
        body: work_unit.encode_to_bytes(),
        created_timestamp_unix_nanos: now_ns(),
        sent_timestamp_unix_nanos: 0,
        received_timestamp_unix_nanos: 0,
        processed_timestamp_unix_nanos: 0,
    }
}

/// Stamps the send time on a bare unit. Lives in core (not flight-gated) so any transport can
/// stamp before delivery; the worker-side latency math treats a missing stamp as zero latency.
pub(crate) fn set_sent_time(work_unit: &mut pb::WorkUnit) {
    work_unit.sent_timestamp_unix_nanos = now_ns();
}

/// Stamps the receive time on a bare unit. See [set_sent_time].
pub(crate) fn set_received_time(work_unit: &mut pb::WorkUnit) {
    work_unit.received_timestamp_unix_nanos = now_ns();
}

/// Wraps an encoded [`pb::WorkUnit`] in the Flight coordinator-to-worker envelope.
#[cfg(feature = "flight")]
pub(crate) fn build_work_unit_msg(work_unit: pb::WorkUnit) -> pb::CoordinatorToWorkerMsg {
    pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnit(work_unit)),
    }
}

#[cfg(feature = "flight")]
pub(crate) fn set_work_unit_send_time(
    mut msg: pb::CoordinatorToWorkerMsg,
) -> pb::CoordinatorToWorkerMsg {
    if let pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnit(work_unit)),
    } = &mut msg
    {
        set_sent_time(work_unit);
    }
    msg
}

#[cfg(feature = "flight")]
pub(crate) fn set_work_unit_received_time(
    mut msg: pb::CoordinatorToWorkerMsg,
) -> pb::CoordinatorToWorkerMsg {
    if let pb::CoordinatorToWorkerMsg {
        inner: Some(pb::coordinator_to_worker_msg::Inner::WorkUnit(work_unit)),
    } = &mut msg
    {
        set_received_time(work_unit);
    }
    msg
}

/// Remote implementation of a [WorkUnitFeedProvider] that pulls [crate::WorkUnit]s coming over
/// the wire from the worker's [WorkUnitFeedChannels].
///
/// Deserializing a [crate::WorkUnitFeed] with [crate::WorkUnitFeed::from_proto] always returns a
/// [crate::WorkUnitFeed<RemoteFeedProvider>] that will receive messages over the network, rather
/// than executing the original [WorkUnitFeedProvider] locally.
///
/// There's a diagram about how this works in [crate::WorkUnitFeed].
#[derive(Debug, Clone)]
pub(crate) struct RemoteFeedProvider {
    pub(crate) id: Uuid,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl RemoteFeedProvider {
    pub(crate) fn feed<T: WorkUnit + Default>(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<T>>> {
        let bdr = || MetricBuilder::new(&self.metrics);

        let bytes_transferred = bdr().bytes_counter("work_unit_bytes");
        let msg_count = bdr().global_counter("work_unit_count");
        // Track end-to-end network latency distribution for all work units.
        let send_latency_max = bdr().max_latency("work_unit_send_latency_max");
        let send_latency_p50 = bdr().p50_latency("work_unit_send_latency_p50");

        let received_latency_max = bdr().max_latency("work_unit_received_latency_max");
        let received_latency_p50 = bdr().p50_latency("work_unit_received_latency_p50");

        let processed_latency_max = bdr().max_latency("work_unit_processed_latency_max");
        let processed_latency_p50 = bdr().p50_latency("work_unit_processed_latency_p50");

        let elapsed_compute = bdr().elapsed_compute(partition);

        let Some(rxs) = ctx
            .session_config()
            .get_extension::<RemoteWorkUnitFeedRxs>()
        else {
            return exec_err!("Missing work-unit feed channels in the session config");
        };

        let id = self.id;
        let Some(remote_feed) = rxs.get(&(id, partition)) else {
            return exec_err!(
                "Missing WorkUnit feed for id {id} and partition {partition}. Was the WorkUnitFeed registered with DistributedExt::with_distributed_work_unit_feed?"
            );
        };

        let Some(receiver) = std::mem::take(&mut *remote_feed.lock().unwrap()) else {
            return exec_err!(
                "WorkUnit feed for id {id} and partition {partition} was already consumed"
            );
        };

        Ok(UnboundedReceiverStream::new(receiver)
            .map(move |work_unit_or_err| {
                let mut work_unit = work_unit_or_err?;
                let timer = elapsed_compute.timer();
                let result = T::decode(work_unit.body.as_slice())
                    .map_err(|err| proto_error(format!("{err}")));
                timer.done();
                work_unit.processed_timestamp_unix_nanos = now_ns();

                let pb::WorkUnit {
                    created_timestamp_unix_nanos: base,
                    sent_timestamp_unix_nanos,
                    received_timestamp_unix_nanos,
                    processed_timestamp_unix_nanos,
                    body,
                    ..
                } = work_unit;

                bytes_transferred.add_bytes(body.len());
                msg_count.add(1);

                // A transport that does not stamp a hop leaves the timestamp at zero; report
                // zero latency for it instead of underflowing the subtraction.
                let latency = |ts: u64| ts.saturating_sub(base) as usize;

                send_latency_max.add_nanos(latency(sent_timestamp_unix_nanos));
                send_latency_p50.add_nanos(latency(sent_timestamp_unix_nanos));

                received_latency_max.add_nanos(latency(received_timestamp_unix_nanos));
                received_latency_p50.add_nanos(latency(received_timestamp_unix_nanos));

                processed_latency_max.add_nanos(latency(processed_timestamp_unix_nanos));
                processed_latency_p50.add_nanos(latency(processed_timestamp_unix_nanos));

                result
            })
            .boxed())
    }

    pub(crate) fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::task_ctx_with_extension;
    use datafusion::execution::TaskContext;

    /// Round-trips a unit through the neutral path with no Flight envelope and no stamps:
    /// channels -> `build_work_unit` -> `RemoteFeedProvider::feed`. Pins that an unstamping
    /// transport decodes correctly and reports zero latency instead of underflowing.
    #[tokio::test]
    async fn neutral_round_trip_without_stamps() -> Result<()> {
        let id = Uuid::new_v4();
        let mut channels = WorkUnitFeedChannels::default();
        channels.add(id, 1);

        let payload = pb::TaskKey {
            query_id: vec![1, 2, 3],
            stage_id: 7,
            task_number: 3,
        };
        let unit = build_work_unit(&id, 0, Box::new(payload.clone()));
        assert_eq!(unit.sent_timestamp_unix_nanos, 0);
        let tx = channels.senders.remove(&(id, 0)).unwrap();
        tx.send(Ok(unit)).unwrap();
        drop(tx);

        let ctx = Arc::new(task_ctx_with_extension(
            &TaskContext::default(),
            channels.receivers,
        ));
        let provider = RemoteFeedProvider {
            id,
            metrics: ExecutionPlanMetricsSet::new(),
        };
        let mut stream = provider.feed::<pb::TaskKey>(0, Arc::clone(&ctx))?;
        let decoded = stream.next().await.unwrap()?;
        assert_eq!(decoded, payload);
        assert!(stream.next().await.is_none());

        // The feed is consume-once per partition.
        assert!(provider.feed::<pb::TaskKey>(0, ctx).is_err());
        Ok(())
    }
}
