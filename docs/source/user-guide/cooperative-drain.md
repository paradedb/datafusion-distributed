# Cooperative Drain — Symmetric-Send Deadlock Mitigation

A design pattern for in-process embedders running multi-stage MPP plans on a
single-threaded executor with a bounded byte transport.

## When this applies

This pattern is relevant if **all three** of these hold:

1. **Bounded transport.** Your `WorkerTransport` impl is backed by a byte queue
   with a fixed capacity per edge (e.g. Postgres `shm_mq` slots, a fixed-size
   shared-memory ring, a `bounded` `std::sync::mpsc`). A full outbound queue
   causes the sender to block or report "would block".
2. **Single-threaded executor.** Producer and consumer tasks for a given proc
   run on the same thread (e.g. `tokio::runtime::Builder::new_current_thread`).
   The embedder cannot rely on a separate OS thread to drain inbound traffic
   while a producer task is blocked on outbound.
3. **N-to-N peer-mesh traffic.** Each proc both produces frames for peers AND
   consumes frames from peers in the same query (the canonical example: a
   distributed shuffle where every proc reads from every other proc). Each
   producer's outbound queue is a peer's inbound queue.

The fork's default `FlightWorkerTransport` is **not** affected: gRPC streams are
backed by unbounded tokio channels per partition, with a global memory budget
([`worker_connection_buffer_budget_bytes`](task-estimator.md)) that gates the
upstream pull rather than the downstream send. The deadlock below cannot occur
on that transport — but it can on any in-process transport that maps logical
channels onto a fixed-capacity byte queue.

## The deadlock

Under the conditions above, a peer-mesh stage can lock up the runtime:

```text
proc 0 → proc 1 outbound queue: FULL.  proc 0 is blocked in send_bytes(...)
proc 1 → proc 0 outbound queue: FULL.  proc 1 is blocked in send_bytes(...)
proc 0 inbound from proc 1 is not being read (proc 0 is blocked sending).
proc 1 inbound from proc 0 is not being read (proc 1 is blocked sending).
Nobody makes progress.
```

The blocker is symmetric: each proc waits for the peer to drain its outbound,
but the peer is itself waiting for *its* outbound to drain. On a multi-threaded
runtime the consumer task on a different OS thread can step in and pull from
inbound. On a single-threaded current-thread runtime there's no such thread —
the producer task holds the executor while it spins on `would_block`.

## The fix

The producer side, while waiting for outbound space, must **pump inbound
traffic** on the same thread. That frees up the peer's outbound-to-us, which
unblocks the peer's send to us, which frees up our outbound-to-peer.

Concretely, every retry iteration of the send loop:

1. Calls a "drain everything inbound" hook that pulls available frames from
   every inbound peer queue into local channel buffers (does **not** block).
2. Yields back to the runtime so sibling tasks (other producer fragments on
   this proc) can advance.
3. Re-tries the send.

The pattern is a per-fragment producer-side spin, not a separate drain thread.
Drain work happens on the same OS thread as the send. This matches the
single-thread invariant most in-process embedders are subject to (e.g. Postgres
parallel workers, where FFI into the engine is backend-thread-only).

The "drain inbound" hook is supplied by the embedder, not the fork — the
embedder owns the proc-to-peer routing topology and knows how to enumerate
"every inbound queue". A trait like

```rust
pub trait CooperativeDrainSet: Send + Sync {
    fn try_drain_pass(&self) -> Result<(), DataFusionError>;
}
```

mounted on the embedder's mesh runtime handle gives the producer-side `send`
loop something to call at each retry.

## Reference implementation

paradedb/paradedb pg_search's MPP layer uses this pattern with Postgres `shm_mq`
as the bounded transport, the current-thread tokio runtime pinned to the
backend thread, and a peer-mesh shuffle across N parallel workers. See:

- [`MppSender::send_batch_traced`](https://github.com/paradedb/paradedb/blob/main/pg_search/src/postgres/customscan/mpp/transport.rs)
  — the producer-side spin that calls `try_drain_pass` inside the retry loop.
- The `CooperativeDrainSet` trait + `MppMesh::drain_all_inbound` impl that
  enumerate every inbound `shm_mq` for the current proc.

`pgrx::check_for_interrupts!()` is also called inside the spin so a user
CANCEL or query timeout `longjmp`s out before the next drain pass — a
Postgres-specific safety net that doesn't have a fork-level equivalent.

## Why the fork doesn't ship this pattern

Two reasons:

1. **The fork's default transport doesn't need it.** Unbounded per-partition
   channels + global memory budget sidesteps the bounded-queue precondition
   entirely.
2. **The drain mechanism is embedder-specific.** Enumerating "every inbound
   peer queue" requires the topology of the embedder's mesh, which the fork
   doesn't model. A fork-level abstraction would either be empty (a trait the
   embedder must implement entirely) or wrong (a default that assumes
   gRPC-style point-to-point).

If a second in-process embedder with a bounded transport appears, this pattern
can be promoted to a `CooperativeDrainSet` trait + a producer-side helper
inside the fork. Until then it's documented here so the next implementer
doesn't reinvent the deadlock-resolution wheel.
