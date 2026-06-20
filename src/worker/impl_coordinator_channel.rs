use crate::Worker;
use crate::common::deserialize_uuid;
use crate::protobuf::datafusion_error_to_tonic_status;
use crate::work_unit_feed::set_work_unit_received_time;
use crate::worker::LocalWorkerContext;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, WorkerToCoordinatorMsg, worker_to_coordinator_msg,
};
use datafusion::common::DataFusionError;
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use url::Url;

impl Worker {
    pub(super) async fn impl_coordinator_channel(
        &self,
        request: Request<Streaming<CoordinatorToWorkerMsg>>,
    ) -> Result<Response<<Worker as WorkerService>::CoordinatorChannelStream>, Status> {
        let (grpc_headers, _ext, mut body) = request.into_parts();

        // The first message must be a SetPlanRequest.
        let Some(msg) = body.next().await else {
            return Err(Status::internal("Empty Coordinator stream"));
        };
        let Some(Inner::SetPlanRequest(request)) = msg?.inner else {
            return Err(Status::internal(
                "First Coordinator message must be SetPlanRequest",
            ));
        };

        let task_data_entries = Arc::clone(&self.task_data_entries);
        let self_url = request.target_worker_url.clone();
        let outcome = self
            .set_task_plan(request, grpc_headers.into_headers(), move |cfg| {
                Ok(cfg.with_extension(Arc::new(LocalWorkerContext {
                    task_data_entries,
                    self_url: Url::parse(&self_url)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                })))
            })
            .await
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        // Continue reading remaining messages (work unit feed data) in the background.
        let work_unit_senders = outcome.work_unit_senders;
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let mut body = body.map_ok(set_work_unit_received_time);
            'outer: while let Some(Ok(msg)) = body.next().await {
                let Some(Inner::WorkUnitBatch(batch)) = msg.inner else {
                    continue;
                };
                for work_unit in batch.batch {
                    let Ok(id) = deserialize_uuid(&work_unit.id) else {
                        continue;
                    };
                    let Some(tx) = work_unit_senders.get(&(id, work_unit.partition as usize))
                    else {
                        continue;
                    };
                    if tx.send(Ok(work_unit)).is_err() {
                        break 'outer; // channel closed
                    }
                }
            }
        });

        // Stream back the metrics once the task finishes executing.
        // The oneshot receiver resolves when impl_execute_task sends the collected
        // metrics after all partitions have finished or been dropped.
        let metrics_stream = outcome.metrics_rx.into_stream();
        let metrics_stream = metrics_stream.filter_map(|task_metrics| async move {
            match task_metrics {
                Ok(task_metrics) => Some(WorkerToCoordinatorMsg {
                    inner: Some(worker_to_coordinator_msg::Inner::TaskMetrics(task_metrics)),
                }),
                Err(_) => None, // channel dropped without sending any message
            }
        });
        Ok(Response::new(metrics_stream.map(Ok).boxed()))
    }
}
