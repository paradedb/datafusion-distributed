// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Runtime glue between the leader's DataFusion execution and the DSM MPSC mesh.
//!
//! [`MppMesh`] is the runtime handle the leader builds at DSM-init time. It holds the
//! single [`super::transport::DrainHandle`] (the
//! `inbound_receiver`) that consolidates this proc's DSM inbox and self-loop, and gets
//! installed on the leader's `SessionConfig` extensions before plan execution.
//!
//! [`ShmMqWorkerTransport`] implements the fork's [`WorkerTransport`] trait, consulted
//! by `NetworkShuffleExec` / `NetworkCoalesceExec` / `NetworkBroadcastExec` at execute
//! time. `open(target_task=worker)` returns a [`ShmMqWorkerConnection`] that yields one
//! stream per consumer partition from the shared `inbound_receiver`.
//!
//! [`InProcessWorkerResolver`] hands the planner `n_workers` placeholder URLs. The transport
//! routes by task index, not URL, so the URLs are never dialed; the resolver exists only because
//! the planner sizes stages from the URL count. It replaces the placeholder URL the fork used to
//! substitute internally under the old `in_process_mode` flag.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use datafusion::common::HashSet;

use crate::{
    RemoteStage, WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerResolver,
    WorkerTransport,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use futures::stream::BoxStream;
use url::Url;

use super::transport::{CooperativeDrainSet, DrainHandle, DrainItem, Interrupt, MppSender};

/// The leader's outbound senders to each peer inbox, shared between the mesh (for `Cancel` frames)
/// and the embedder that owns their lifetime. Indexed by destination `proc_idx`; the leader's own
/// slot is `None`.
pub type PeerSenders = Arc<Mutex<Vec<Option<MppSender>>>>;

/// `task_idx → proc_idx` round-robin over the worker procs. The leader is `proc_idx = 0`
/// (consumer-only), workers are `1..n_procs` (each hosts producer fragments).
///
/// A stage's task count is set by the DF-D task estimator chain, not by the worker proc count.
/// The transport does NOT support more tasks than producer procs: channels are keyed
/// `(sender_proc, stage, partition)`, so two tasks wrapped onto one proc would interleave on one
/// channel and the first EOF would truncate the other task's output. `ShmMqWorkerTransport::open`
/// rejects that shape; the modulo here only keeps the function total.
#[inline]
pub fn proc_for_task(n_workers: u32, task_idx: u32) -> u32 {
    1 + (task_idx % n_workers.max(1))
}

/// Runtime handle the customscan populates at DSM-init time.
///
/// Each process owns one MPSC inbox in DSM that receives frames from every peer.
/// `inbound_receiver` consolidates that inbox plus the in-proc self-loop channel (for
/// producer-and-consumer-on-same-worker fragments) into a single `DrainHandle`. Frames
/// carry `(sender_proc, stage_id, partition)` in their header so the routing registry
/// inside the handle delivers each frame to the matching consumer.
///
/// [`MppFrameHeader`]: super::transport::MppFrameHeader
pub struct MppMesh {
    /// This process's `proc_idx` (= 0 for the leader, `ParallelWorkerNumber + 1` for workers).
    /// Frames addressed to this proc arrive on this proc's own inbox.
    pub this_proc: u32,
    /// Total proc count. Bounds the producer/consumer proc lookups in
    /// [`ShmMqWorkerTransport::open`].
    pub n_procs: u32,
    /// Single cooperative inbound handle pulling every frame addressed to this proc. The
    /// DSM MPSC inbox and an in-proc self-loop receiver both feed into this handle. Demux
    /// to per-`(sender_proc, stage_id, partition)` channel buffers happens inside via
    /// `DrainHandle::register_channel`.
    pub(super) inbound_receiver: Arc<DrainHandle>,
    /// Cancellation hook, injected by the embedder, checked at the transport's block points (the
    /// cooperative send spin and the consumer pull loop).
    interrupt: Arc<dyn Interrupt>,
    /// The leader's outbound senders to each peer inbox, shared with the embedder that owns their
    /// lifetime (it clears the `Vec` before the DSM unmaps). Used by [`Self::cancel_stage`] to ship
    /// `Cancel` frames when the leader's gather terminates early. `None` on workers and until the
    /// embedder installs it.
    cancel_senders: Mutex<Option<PeerSenders>>,
    /// Stages already cancelled, so [`Self::cancel_stage`] ships one round of frames per stage even
    /// though each gather partition's stream drop calls it.
    cancelled_stages: Mutex<HashSet<u32>>,
}

impl MppMesh {
    /// Build a fresh mesh.
    pub fn new(
        this_proc: u32,
        n_procs: u32,
        inbound_receiver: Arc<DrainHandle>,
        interrupt: Arc<dyn Interrupt>,
    ) -> Self {
        Self {
            this_proc,
            n_procs,
            inbound_receiver,
            interrupt,
            cancel_senders: Mutex::new(None),
            cancelled_stages: Mutex::new(HashSet::default()),
        }
    }

    /// Share the leader's outbound senders so [`Self::cancel_stage`] can reach the producers. The
    /// embedder keeps owning their lifetime: it passes a clone of the same `Arc` it releases before
    /// the DSM unmaps, so the mesh never extends the senders past teardown.
    pub fn set_cancel_senders(&self, senders: PeerSenders) {
        *self.cancel_senders.lock().unwrap() = Some(senders);
    }

    /// Tell every producer of `stage_id` to stop: the leader's consumer abandoned it (a satisfied
    /// `LIMIT`). Ships a `Cancel` frame to each peer inbox, leaving the rings healthy for metrics
    /// and other stages. Idempotent across the gather partitions that all drop at once. A no-op
    /// when no senders are installed (a worker, or after teardown cleared them).
    pub fn cancel_stage(&self, stage_id: u32) {
        if !self.cancelled_stages.lock().unwrap().insert(stage_id) {
            return;
        }
        let guard = self.cancel_senders.lock().unwrap();
        let Some(senders) = guard.as_ref() else {
            return;
        };
        for sender in senders.lock().unwrap().iter().flatten() {
            sender.try_send_cancel(stage_id);
        }
    }

    /// The single cooperative inbound handle that pulls frames from every peer (and the
    /// self-loop) into per-`(sender_proc, stage_id, partition)` channel buffers.
    pub(super) fn inbound_receiver(&self) -> &Arc<DrainHandle> {
        &self.inbound_receiver
    }

    /// Install the senders of one task's work-unit feed channels on this proc's drain, so
    /// inbound `WorkUnit` frames for `(stage_id, task_number)` flow into them. Units that
    /// arrived first are flushed; a `FeedEof` that already came through closes the channels
    /// immediately. The drain only fills channels: something on this proc must keep draining
    /// (a consumer pull loop, a producer send spin, or an explicit
    /// [`CooperativeDrainSet::try_drain_pass`] pump) or a fragment blocked on its feed starves.
    pub fn register_work_unit_senders(
        &self,
        stage_id: u32,
        task_number: u32,
        senders: crate::work_unit_feed::RemoteWorkUnitFeedTxs,
    ) {
        self.inbound_receiver
            .register_work_unit_senders(stage_id, task_number, senders);
    }

    /// Take the plan delivered for `(stage_id, task_number)` as a `SetPlan` frame on this proc's
    /// inbox, waiting for it if it has not arrived yet. Something on this proc must keep
    /// draining (a pump, a consumer pull loop, or a cooperative send spin) or the wait starves.
    pub async fn take_set_plan(
        &self,
        stage_id: u32,
        task_number: u32,
    ) -> Result<super::transport::SetPlanFrame> {
        self.inbound_receiver
            .take_set_plan(stage_id, task_number)
            .await
    }

    /// Take the stream of `TaskMetrics` frames arriving on this proc's inbox:
    /// `(stage_id, task_number, metrics)` per producer task that reported in. Meant for the
    /// leader; the first caller gets it, later calls get `None`.
    pub fn take_task_metrics_receiver(
        &self,
    ) -> Option<
        tokio::sync::mpsc::UnboundedReceiver<(
            u32,
            u32,
            crate::worker::generated::worker::TaskMetrics,
        )>,
    > {
        self.inbound_receiver.take_task_metrics_receiver()
    }

    /// Number of worker procs (= `n_procs - 1`, since the leader is proc 0). Used as the
    /// modulus in [`proc_for_task`].
    ///
    /// The embedder must guarantee `n_procs >= 2` (one consumer-only leader plus at least one
    /// producer worker) before constructing an `MppMesh`; the subtraction is otherwise unsound.
    /// `compute_dsm_layout` enforces the same bound. Asserted in debug builds so a future misuse
    /// fails loudly.
    pub fn n_workers(&self) -> u32 {
        debug_assert!(
            self.n_procs >= 2,
            "MppMesh::n_workers() called with n_procs={} (< 2); the embedder must gate on \
             n_procs >= 2",
            self.n_procs
        );
        self.n_procs - 1
    }

    /// Pull from the single inbound handle. Called from
    /// [`super::transport::MppSender`]'s cooperative-send spin so a
    /// producer stalled on a full outbound can pull inbound peer data inline. That's what
    /// prevents the symmetric-send deadlock when every peer is simultaneously stalled waiting
    /// for space.
    pub(super) fn drain_all_inbound(&self) -> Result<(), DataFusionError> {
        self.inbound_receiver.try_drain_pass()
    }
}

impl CooperativeDrainSet for MppMesh {
    fn try_drain_pass(&self) -> Result<(), DataFusionError> {
        self.drain_all_inbound()
    }

    fn check_interrupt(&self) -> Result<(), DataFusionError> {
        self.interrupt.check()
    }

    fn stage_cancelled(&self, stage_id: u32) -> bool {
        self.inbound_receiver.stage_cancelled(stage_id)
    }
}

/// Implements the DF-D fork's [`WorkerTransport`] over the leader's [`MppMesh`].
///
/// `open(input_stage, target_task)` translates the DF-D `(stage, task)`
/// addressing into the proc-pair grid: `proc_for_task(n_workers, target_task)`
/// selects which `sender_proc` hosts the producer-side task, and the returned
/// [`WorkerConnection`] pulls from that proc's inbound drain.
pub struct ShmMqWorkerTransport {
    mesh: Arc<MppMesh>,
}

impl ShmMqWorkerTransport {
    pub fn new(mesh: Arc<MppMesh>) -> Self {
        Self { mesh }
    }
}

impl WorkerTransport for ShmMqWorkerTransport {
    fn open(
        &self,
        input_stage: &RemoteStage,
        _target_partitions: Range<usize>,
        target_task: usize,
        _ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection>> {
        MetricBuilder::new(metrics)
            .global_counter("mesh_connections_used")
            .add(1);
        let target_task_u32 = u32::try_from(target_task).map_err(|_| {
            DataFusionError::Internal(format!(
                "ShmMqWorkerTransport: target_task={target_task} > u32::MAX"
            ))
        })?;
        let stage_id = u32::try_from(input_stage.num).map_err(|_| {
            DataFusionError::Internal(format!(
                "ShmMqWorkerTransport: input_stage.num={} > u32::MAX",
                input_stage.num
            ))
        })?;
        // More tasks than producer procs would fold two tasks onto one
        // `(sender_proc, stage, partition)` channel: interleaved batches, and the first task's
        // EOF truncates the second. Refuse the shape instead of wrapping.
        if input_stage.workers.len() > self.mesh.n_workers() as usize {
            return Err(DataFusionError::Internal(format!(
                "ShmMqWorkerTransport: stage {} has {} tasks but only {} producer procs; \
                 the shm_mq transport requires task_count <= n_workers",
                input_stage.num,
                input_stage.workers.len(),
                self.mesh.n_workers()
            )));
        }
        let sender_proc = proc_for_task(self.mesh.n_workers(), target_task_u32);
        if sender_proc >= self.mesh.n_procs {
            return Err(DataFusionError::Internal(format!(
                "ShmMqWorkerTransport: sender_proc={sender_proc} >= n_procs={} \
                 (stage_id={stage_id}, target_task={target_task})",
                self.mesh.n_procs
            )));
        }
        // First line to grep when a query hangs on a channel nothing registered.
        log::debug!(
            "shm transport open: this_proc={} stage_id={stage_id} \
             target_task={target_task} -> sender_proc={sender_proc}",
            self.mesh.this_proc
        );
        Ok(Box::new(ShmMqWorkerConnection {
            mesh: Arc::clone(&self.mesh),
            sender_proc,
            stage_id,
        }))
    }

    /// The leader never pushes plans to workers: the plan reaches them through the embedder's own
    /// channel (DSM for pg_search) before the leader's DataFusion plan gets dispatched, so the
    /// dispatcher is a no-op. This is what replaces the old `in_process_mode` flag.
    fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
        Box::new(NoOpDispatch)
    }
}

struct NoOpDispatch;

impl WorkerDispatch for NoOpDispatch {
    /// No-op, see [`ShmMqWorkerTransport::dispatcher`]. The workers already hold the plan and run
    /// their fragments, so there is nothing to deliver.
    fn dispatch(&self, _request: WorkerDispatchRequest<'_>) -> Result<()> {
        Ok(())
    }
}

/// Placeholder worker resolver for the in-process MPP transport.
///
/// The shm_mq transport routes by `target_task` (proc index), never by URL, so these URLs are
/// never dialed. The distributed planner still needs a resolver: it sizes stages and assigns
/// tasks from the URL count. `n_workers` placeholder URLs is exactly what the planner needs. This
/// replaces the placeholder URL the fork used to substitute internally under `in_process_mode`.
pub struct InProcessWorkerResolver {
    n_workers: usize,
}

impl InProcessWorkerResolver {
    pub fn new(n_workers: usize) -> Self {
        Self { n_workers }
    }
}

impl WorkerResolver for InProcessWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        (0..self.n_workers.max(1))
            .map(|i| {
                Url::parse(&format!("inprocess://worker/{i}")).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "InProcessWorkerResolver: invalid placeholder url: {e}"
                    ))
                })
            })
            .collect()
    }
}

struct ShmMqWorkerConnection {
    mesh: Arc<MppMesh>,
    sender_proc: u32,
    /// `stage_id` of the boundary's `input_stage`. Passed to `DrainHandle::register_channel` so
    /// the channel buffer this connection streams from sees only frames tagged with the same
    /// `(stage_id, p)`.
    stage_id: u32,
}

/// Cancels the leader's gather stage if a consumer stream drops before EOF. The leader is the only
/// consumer-only proc, so it's the only one that can stop pulling early (a top-N `LIMIT` above the
/// gather). When it does, its producers would otherwise spin on the full inbox until the statement
/// timeout. Disarmed on a clean EOF: there the producers already finished, so there's nothing to
/// cancel.
struct LeaderStageCancelGuard {
    mesh: Arc<MppMesh>,
    stage_id: u32,
    armed: bool,
}

impl Drop for LeaderStageCancelGuard {
    fn drop(&mut self) {
        if self.armed {
            self.mesh.cancel_stage(self.stage_id);
        }
    }
}

impl WorkerConnection for ShmMqWorkerConnection {
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let partition_u32 = u32::try_from(partition).map_err(|_| {
            DataFusionError::Internal(format!(
                "ShmMqWorkerConnection: partition={partition} > u32::MAX"
            ))
        })?;
        // One drain per process, shared across all sender_procs. The channel-buffer
        // registry keys by (sender_proc, stage_id, partition) so this consumer still
        // sees only its named sender's slice even though the underlying inbox is
        // shared with all peers.
        let drain = Arc::clone(self.mesh.inbound_receiver());
        log::debug!(
            "shm transport execute: register channel sender_proc={} stage_id={} \
             partition={partition_u32}",
            self.sender_proc,
            self.stage_id
        );
        let buffer = drain.register_channel(self.sender_proc, self.stage_id, partition_u32);
        let mesh = Arc::clone(&self.mesh);
        // Cooperative pull loop: the inbound drain runs inline on the consumer's thread. Each
        // iteration checks for cancellation (via the injected interrupt seam), drains the receiver
        // into the registry, pops one batch to yield, then yields back to Tokio so sibling tasks
        // (e.g. the leader's own producer subplan) advance. The send side runs the same interrupt
        // check inside `MppSender`'s retry spin.
        let stage_id = self.stage_id;
        let stream = async_stream::stream! {
            let mut cancel_guard = (mesh.this_proc == 0).then(|| LeaderStageCancelGuard {
                mesh: Arc::clone(&mesh),
                stage_id,
                armed: true,
            });
            loop {
                if let Err(e) = mesh.check_interrupt() {
                    yield Err(e);
                    return;
                }
                if let Err(e) = drain.try_drain_pass() {
                    yield Err(e);
                    return;
                }
                match buffer.try_pop() {
                    Some(DrainItem::Batch(batch)) => yield Ok(batch),
                    Some(DrainItem::Eof) => {
                        // Clean end: producers already EOF'd, so there's nothing to cancel.
                        if let Some(g) = cancel_guard.as_mut() {
                            g.armed = false;
                        }
                        break;
                    }
                    Some(DrainItem::Failed(msg)) => {
                        yield Err(DataFusionError::Execution(msg));
                        return;
                    }
                    None => tokio::task::yield_now().await,
                }
            }
        };
        Ok(Box::pin(stream))
    }
}
