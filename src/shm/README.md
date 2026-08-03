# Shared-Memory Transport (`src/shm`)

This module provides a non-gRPC [`ChannelResolver`](../protocol/worker_channel.rs) and transport layer for co-located execution, where workers run as parallel tasks or processes sharing one machine and communicating over a shared-memory mesh rather than gRPC over TCP.

---

## Architecture Overview

The shared-memory transport mirrors the canonical gRPC protocol's pull-based RPC design while replacing network serialization and TCP sockets with Direct Shared Memory (DSM) ring buffers and compact 16-byte frame headers.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Coordinator                                    │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ SetPlan (Control Mesh)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Worker Process / Instance                          │
│                                                                             │
│  1. Receives SetPlan & registers plan fragment (TaskKey)                    │
│  2. Idles until ExecuteTaskFrame arrives from downstream consumer           │
│  3. ExecuteTaskFrame arrives specifying partition_range (start..end)        │
│  4. Opens per-partition sinks lazily for start..end                        │
│  5. Evaluates run_worker_fragment(plan, sinks, ctx, start..end)             │
│  6. Streams batches out via DSM ring buffers                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Control & Data Plane Design

### 1. Control Plane (`MppMesh` & `DrainHandle`)
- **Messages**: Control frames (`ExecuteTask`, `SetPlan`, `TaskMetrics`, `Cancel`, `EOF`) are tagged with a 16-byte [`MppFrameHeader`](./transport.rs) (`kind`, `stage_id`, `partition`, `sender_proc`) and routed through `MppMesh`.
- **Demand-Driven Pull Execution**:
  - Downstream consumers issue `ExecuteTaskFrame` to upstream producer tasks via `MppMesh::send_execute_task`.
  - Upstream worker tasks wait on `MppMesh::take_execute_task_rx(stage_id, task_number)` for incoming execution requests before spawning task fragments.
- **Demuxing**: Incoming frames are demuxed cooperatively by [`DrainHandle`](./transport.rs) into per-`(stage_id, task_number)` request registries and per-channel record batch buffers.

### 2. Data Plane (DSM Ring Buffers)
- Output partitions write record batches into lock-free/cache-aligned DSM ring buffers (`mpsc_ring.rs`).
- Batches are serialized as Arrow IPC streams directly into ring slots.
- Dedicated per-partition rings eliminate head-of-line blocking across tasks.

---

## Lifecycle & Memory Safety

- **Keep-Alive Senders**: `MppMesh` holds internal `_keep_alive_senders` while active to ensure worker inboxes maintain `sender_count > 0` and do not mark shared-memory slots detached prematurely while control requests (`ExecuteTask`, `Cancel`) are pending.
- **Teardown & Detachment**: Embedders invoke `MppMesh::mark_detached()` when unmapping memory segments or shutting down, clearing keep-alive senders so peer inboxes observe `Detached` cleanly without accessing freed shared-memory buffers.

---

## Extension Points for Embedders

Embedders (such as ParadeDB / Postgres backend engines) supply platform primitives via extension traits:
- **Buffer Allocation**: Standard POSIX `mmap` or Postgres Shared Memory allocations (`dsm.rs`).
- **Wakeup Primitives**: Custom signal/wakeup hooks (`NO_RECEIVER_TOKEN`, [`Wakeup`](./mpsc_ring.rs)).
