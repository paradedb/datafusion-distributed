// Copyright (c) 2023-2026 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Mesh construction over a caller-supplied shared buffer, the seam between an embedder's buffer
//! allocation and the transport.
//!
//! The embedder allocates the shared region (a PG `dsm_segment`, or a heap buffer in-process),
//! sizes it with [`dsm_region_bytes`], then calls [`leader_setup`] on the proc that initializes the
//! rings and [`worker_setup`] on each producer proc. Both hand back an [`MppMesh`] the embedder
//! installs on its DataFusion session; `worker_setup` also returns the outbound senders and the
//! plan bytes copied out of the region. [`run_worker_fragment`] is the producer push loop.
//!
//! The two platform primitives the embedder supplies are the [`Wakeup`] (how to wake a blocked
//! consumer) and the [`Interrupt`] (how to check for cancellation); everything here is otherwise
//! pure Rust over the shared buffer.

use std::ffi::c_void;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::stream::StreamExt;

use super::dsm::{compute_dsm_layout, leader_init, peer_proc_for_index, read_region_total, worker_attach};
use super::mesh::{DsmInboxReceiver, DsmInboxSender};
use super::mpsc_ring::{DsmMpscSender, Wakeup};
use super::runtime::MppMesh;
use super::transport::{
    BatchChannelSender, DrainHandle, Interrupt, MppFrameHeader, MppReceiver, MppSender,
    SELF_LOOP_CAPACITY, SendBatchStats, in_proc_channel,
};

/// Total bytes the shared region needs for `n_procs` inboxes plus `plan_len` plan bytes, with
/// `queue_bytes` per inbox. The embedder reserves exactly this much before [`leader_setup`].
pub fn dsm_region_bytes(
    n_procs: u32,
    queue_bytes: usize,
    plan_len: usize,
) -> Result<usize, String> {
    compute_dsm_layout(n_procs, queue_bytes, plan_len)
        .map(|l| l.region_total)
        .map_err(|e| format!("mpp: dsm_region_bytes: {e}"))
}

/// Read `region_total` out of the header a leader wrote, so a worker that just mapped the region
/// can size its [`worker_setup`] call without knowing the header layout.
///
/// # Safety
/// `base` must point at the start of a region a leader initialized via [`leader_setup`].
pub unsafe fn region_total(base: *const c_void) -> u64 {
    unsafe { read_region_total(base) }
}

/// Wrap each peer-indexed `DsmMpscSender` into an outbound `MppSender` keyed by destination proc.
/// The dispatcher `clone_with_header`s these per output partition before sending, so the
/// placeholder header is never observed on the wire. Slot `this_proc` stays `None` until the
/// self-loop install.
fn build_outbound_senders(
    this_proc: u32,
    total_procs: u32,
    peer_senders: Vec<DsmMpscSender>,
) -> Vec<Option<MppSender>> {
    let mut senders: Vec<Option<MppSender>> = (0..total_procs).map(|_| None).collect();
    for (peer_idx, dsm_send) in peer_senders.into_iter().enumerate() {
        let target_proc = peer_proc_for_index(this_proc, peer_idx as u32);
        let shared: Arc<dyn BatchChannelSender> = Arc::new(DsmInboxSender::new(dsm_send));
        senders[target_proc as usize] = Some(MppSender::with_header(
            shared,
            // Stamp `sender_proc = this_proc` so a stray frame that escapes the dispatcher's
            // `clone_with_header` overwrite still identifies its origin on the drain side.
            MppFrameHeader::batch(0, 0, this_proc),
        ));
    }
    senders
}

/// Initialize the shared region as the leader (`proc 0`) and return its consumer-only mesh.
///
/// Writes the region header, copies `plan_bytes` in, initializes the `n_procs` inboxes, and
/// attaches the leader as receiver to its own inbox. `receiver_token` is registered so producers
/// resolve this proc's [`Wakeup`]; `interrupt` is consulted at the transport's block points.
///
/// # Safety
/// - `base` must point at an uninitialized region of at least `dsm_region_bytes(n_procs,
///   queue_bytes, plan_bytes.len())` bytes.
/// - The region must not be concurrently accessed until this returns.
pub unsafe fn leader_setup(
    base: *mut c_void,
    n_procs: u32,
    queue_bytes: usize,
    plan_bytes: &[u8],
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
) -> Result<Arc<MppMesh>, String> {
    let layout = compute_dsm_layout(n_procs, queue_bytes, plan_bytes.len())
        .map_err(|e| format!("mpp: leader_setup compute layout: {e}"))?;
    let attach = unsafe { leader_init(base, &layout, plan_bytes, Arc::clone(&wakeup)) }?;

    let inbox = DsmInboxReceiver::new(attach.inbound_receiver);
    inbox.set_receiver(receiver_token);
    let inbound = Arc::new(DrainHandle::cooperative(vec![MppReceiver::new(Box::new(inbox))]));
    // The leader is consumer-only; it never hosts a producer fragment.
    drop(attach.outbound_senders);
    Ok(Arc::new(MppMesh::new(0, n_procs, inbound, interrupt)))
}

/// What [`worker_setup`] hands back to the embedder.
pub struct WorkerAttach {
    /// The worker's mesh, installed on its DataFusion session.
    pub mesh: Arc<MppMesh>,
    /// Outbound senders keyed by destination proc index. The slot at `this_proc` is the in-proc
    /// self-loop; every other slot writes to that peer's inbox.
    pub outbound_senders: Vec<Option<MppSender>>,
    /// The plan bytes the leader wrote into the region, copied out for this worker.
    pub plan_bytes: Vec<u8>,
}

/// Attach to the leader-initialized region as worker `proc_idx` (`>= 1`).
///
/// # Safety
/// - `base`/`region_total` must match the region the leader initialized via [`leader_setup`].
pub unsafe fn worker_setup(
    base: *mut c_void,
    region_total: u64,
    proc_idx: u32,
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
) -> Result<WorkerAttach, String> {
    let (header, plan_bytes, attach) =
        unsafe { worker_attach(base, region_total, proc_idx, Arc::clone(&wakeup)) }?;
    let total_procs = header.n_procs;

    let mut outbound = build_outbound_senders(proc_idx, total_procs, attach.outbound_senders);

    // Self-loop in-proc channel: peer-mesh routing can land a producer and its consumer on the same
    // proc, and an MPSC inbox has no slot for a proc sending to itself. The unified drain pulls from
    // both the inbox and this channel.
    let (self_tx, self_rx) = in_proc_channel(SELF_LOOP_CAPACITY);
    let self_tx_arc: Arc<dyn BatchChannelSender> = Arc::new(self_tx);
    outbound[proc_idx as usize] =
        Some(MppSender::with_header(self_tx_arc, MppFrameHeader::batch(0, 0, proc_idx)));

    let inbox = DsmInboxReceiver::new(attach.inbound_receiver);
    inbox.set_receiver(receiver_token);
    let inbound = Arc::new(DrainHandle::cooperative(vec![
        MppReceiver::new(Box::new(inbox)),
        MppReceiver::new(Box::new(self_rx)),
    ]));
    let mesh = Arc::new(MppMesh::new(proc_idx, total_procs, inbound, interrupt));
    Ok(WorkerAttach {
        mesh,
        outbound_senders: outbound,
        plan_bytes,
    })
}

/// Run a producer fragment plan to exhaustion, pushing every output batch through the matching
/// per-partition sender. The output partition count of `plan` must equal `senders.len()`.
///
/// Each partition sends a per-channel EOF when its stream ends, regardless of how it ended: the
/// shared queue is multiplexed across fragments, so dropping a sender doesn't end the channel, only
/// the EOF frame does.
pub async fn run_worker_fragment(
    plan: Arc<dyn ExecutionPlan>,
    senders: Vec<MppSender>,
    ctx: Arc<TaskContext>,
) -> Result<()> {
    let n_partitions = plan.output_partitioning().partition_count();
    if n_partitions != senders.len() {
        return Err(DataFusionError::Internal(format!(
            "run_worker_fragment: plan has {n_partitions} output partitions but {} senders",
            senders.len()
        )));
    }
    let senders: Vec<Arc<MppSender>> = senders.into_iter().map(Arc::new).collect();
    let mut futures = Vec::with_capacity(n_partitions);
    for (partition, sender) in senders.iter().enumerate() {
        let plan = Arc::clone(&plan);
        let ctx = Arc::clone(&ctx);
        let sender = Arc::clone(sender);
        futures.push(async move {
            let mut stats = SendBatchStats::default();
            let stream_result: Result<()> = async {
                let mut stream = plan.execute(partition, ctx)?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    sender.as_ref().send_batch_traced(&batch, &mut stats).await?;
                }
                Ok(())
            }
            .await;
            let eof_result = sender.as_ref().send_eof_traced(&mut stats).await;
            // Surface the stream error first, then any EOF-send error, so neither disappears.
            stream_result.and(eof_result)
        });
    }
    // `join_all`, not `try_join_all`: fail-fast would cancel sibling partitions mid-await before
    // they reach `send_eof_traced`, leaving the consumer's channel buffer stuck.
    let results = futures::future::join_all(futures).await;
    drop(senders);
    for r in results {
        r?;
    }
    Ok(())
}
