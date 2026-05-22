# Symmetric-Send Deadlock in Bounded In-Process Transports

A failure mode in-process embedders hit on multi-stage MPP plans over a
bounded byte transport, and the **cooperative-drain** pattern that fixes it.

## When this applies

All three need to hold:

1. **Bounded transport.** Your `WorkerTransport` is backed by a byte queue
   with fixed per-edge capacity (Postgres `shm_mq`, a fixed shared-memory
   ring, `bounded` `std::sync::mpsc`). A full outbound queue blocks the sender
   or returns "would block".
2. **No spare thread to consume.** Producer and consumer tasks for a proc
   share execution resources. The canonical case is a current-thread tokio
   runtime: producer and consumer run on one OS thread, so a blocked producer
   can't get preempted in favor of a consumer that would unblock it.
   Multi-thread runtimes hit a degenerate version when every worker thread is
   parked in `send_bytes`.
3. **Bidirectional peer traffic.** At least two procs both send to and
   receive from each other in the same stage. Two peers is enough; the
   canonical case is an N-way distributed shuffle.

`FlightWorkerTransport` is **not** affected: gRPC uses unbounded per-partition
tokio channels with a global memory budget
([`worker_connection_buffer_budget_bytes`](../user-guide/task-estimator.md))
gating the upstream pull, not the downstream send.

## The deadlock

Under those conditions a peer-mesh stage locks up:

```text
proc 0 → proc 1 outbound queue: FULL.  proc 0 blocked in send_bytes(...)
proc 1 → proc 0 outbound queue: FULL.  proc 1 blocked in send_bytes(...)
proc 0 inbound from proc 1 is not being read (proc 0 is blocked sending).
proc 1 inbound from proc 0 is not being read (proc 1 is blocked sending).
Nobody makes progress.
```

Each proc waits for its peer's outbound to drain; the peer is waiting for
the same thing on its side. On a multi-thread runtime with spare capacity a
consumer task on another thread can step in. On a current-thread runtime
there's no such thread; the producer holds the executor and spins on
`would_block`.

## The fix: cooperative drain

While waiting for outbound space, the producer side must **pump inbound
traffic** on the same thread. That frees the peer's outbound-to-us, which
unblocks the peer's send to us, which frees our outbound-to-peer.

Every retry iteration of the send loop:

1. Calls a "drain everything inbound" hook that pulls available frames from
   every inbound peer queue into local channel buffers. Non-blocking.
2. Yields back to the runtime so sibling tasks on this proc can advance.
3. Re-tries the send.

It's a per-fragment producer-side spin, not a separate drain thread. Drain
runs on the same OS thread as the send, matching the single-thread invariant
most in-process embedders are subject to (e.g. Postgres parallel workers
where FFI into the engine is backend-thread-only).

The drain hook is supplied by the embedder. The embedder owns the
proc-to-peer topology and knows how to enumerate "every inbound queue". A
trait like

```rust
pub trait CooperativeDrainSet: Send + Sync {
    fn try_drain_pass(&self) -> Result<(), DataFusionError>;
}
```

on the embedder's mesh handle gives the producer-side `send` loop something
to call at each retry.

## Reference implementation

paradedb pg_search's MPP layer uses this with Postgres `shm_mq` as the
bounded transport, a current-thread tokio runtime pinned to the backend
thread, and a peer-mesh shuffle across N parallel workers. The producer-side
spin lives in `MppSender::send_batch_traced`
(`pg_search/src/postgres/customscan/mpp/transport.rs`) and calls
`try_drain_pass` on a `CooperativeDrainSet` injected at sender construction
time. The mesh-level impl (`impl CooperativeDrainSet for MppMesh`) enumerates
every inbound `shm_mq` for the proc; see `MppMesh::drain_all_inbound` in
`pg_search/src/postgres/customscan/mpp/runtime.rs`.

`pgrx::check_for_interrupts!()` runs inside the spin so a user CANCEL or
query timeout `longjmp`s out before the next drain pass. Postgres-specific
safety net; no fork-level equivalent.

## Why this isn't shipped as code

Two reasons:

1. The default transport doesn't need it. Unbounded per-partition channels
   plus a global memory budget sidesteps the precondition.
2. The drain mechanism is embedder-specific. Enumerating "every inbound peer
   queue" needs the topology of the embedder's mesh, which the fork doesn't
   model. A fork-level abstraction would either be empty (a trait the
   embedder fully implements) or wrong (a default that assumes
   point-to-point).

If a second bounded in-process embedder shows up, this can move into the
fork as a `CooperativeDrainSet` trait plus a producer-side helper. Until
then it lives here so the next implementer doesn't reinvent the fix.
