//! An in-process [`WorkerTransport`]: every "worker" is the current process, so plans are
//! delivered with a function call and partitions are read straight from the local task registry.
//! It is the default transport when the `flight` feature is off, and the reference
//! implementation for the transport extension points: it goes through the same plan
//! encode/decode, session building, work-unit feed, and metrics delivery as a remote transport,
//! just without a wire underneath.

use crate::common::serialize_uuid;
use crate::distributed_planner::ProducerHead;
use crate::stage::RemoteStage;
use crate::worker::generated::worker as pb;
use crate::worker::impl_execute_task::execute_local_task;
use crate::worker::transport::WorkerConnection;
use crate::worker::worker_service::TaskDataEntries;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// [WorkerConnection] over the local task registry: builds the per-partition record batch
/// streams straight from [execute_local_task], with no encoding in between.
///
/// Serves both the in-memory transport (every read) and the Flight transport (its local-bypass
/// read, when the target worker happens to be the current process).
pub(crate) struct LocalWorkerConnection {
    partition_start: usize,
    local_streams: Vec<Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>>,
    /// Drives the single `execute_local_task` call that produces every partition stream. Held so
    /// it lives as long as the connection (the pool keeps it for the query), not aborted early.
    _driver: SpawnedTask<()>,
}

impl LocalWorkerConnection {
    pub(crate) fn init(
        input_stage: &RemoteStage,
        target_partition_range: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        task_data_entries: Arc<TaskDataEntries>,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        MetricBuilder::new(metrics)
            .global_counter("local_connections_used")
            .add(1);

        let task_key = pb::TaskKey {
            query_id: serialize_uuid(&input_stage.query_id),
            stage_id: input_stage.num as u64,
            task_number: target_task as u64,
        };
        let producer_head = producer_head.to_proto(ctx)?;

        let partition_start = target_partition_range.start;
        let n_partitions = target_partition_range.len();

        // Execute the whole partition range in ONE `execute_local_task` call, the same way the
        // Flight and shm worker sides do, and then drive each partition stream from a dedicated
        // pump into a buffer the consumer reads from. This decouples the worker plan's execution
        // from when (or whether) the consumer pulls, exactly as Flight gets from its gRPC stream
        // and shm from its mesh ring. Letting the consumer drive the worker plan directly (its
        // `SortPreservingMergeExec` interleaving the polls of a partitioned `HashJoinExec`) can
        // otherwise leave some partitions empty.
        let request = pb::ExecuteTaskRequest {
            task_key: Some(task_key),
            target_partition_start: target_partition_range.start as u64,
            target_partition_end: target_partition_range.end as u64,
            producer_head: Some(producer_head),
        };

        let mut senders = Vec::with_capacity(n_partitions);
        let mut local_streams = Vec::with_capacity(n_partitions);
        for _ in 0..n_partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<RecordBatch>>();
            senders.push(tx);
            local_streams.push(Mutex::new(Some(UnboundedReceiverStream::new(rx).boxed())));
        }

        // Eagerly retrieve the entry (before Moka can evict it). The handle is kept on the
        // connection so the pumps are not aborted before they finish draining the streams.
        let driver = SpawnedTask::spawn(async move {
            match execute_local_task(&task_data_entries, request).await {
                Ok((streams, _)) => {
                    let pumps = senders
                        .into_iter()
                        .zip(streams)
                        .map(|(tx, mut stream)| async move {
                            while let Some(item) = stream.next().await {
                                if tx.send(item).is_err() {
                                    break; // consumer dropped this partition
                                }
                            }
                        })
                        .collect::<Vec<_>>();
                    futures::future::join_all(pumps).await;
                }
                Err(err) => {
                    let err = Arc::new(err);
                    for tx in senders {
                        let _ = tx.send(Err(DataFusionError::Shared(Arc::clone(&err))));
                    }
                }
            }
        });

        Ok(Self {
            partition_start,
            local_streams,
            _driver: driver,
        })
    }
}

impl WorkerConnection for LocalWorkerConnection {
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some(relative_i) = partition.checked_sub(self.partition_start) else {
            return internal_err!(
                "LocalWorkerConnection received an invalid partition {partition}, the starting partition is {}",
                self.partition_start
            );
        };
        let Some(slot) = self.local_streams.get(relative_i) else {
            return internal_err!(
                "LocalWorkerConnection has no stream for partition {partition}. Was it already consumed?"
            );
        };
        slot.lock().unwrap().take().ok_or_else(|| {
            internal_datafusion_err!(
                "LocalWorkerConnection stream for partition {partition} was already consumed"
            )
        })
    }
}
