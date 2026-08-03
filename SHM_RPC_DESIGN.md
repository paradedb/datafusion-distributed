# Refactoring `src/shm` to a Pull-Based (RPC) Execution Model

## Overview & Motivation

The `src/shm` shared memory engine in `datafusion-distributed` (used by ParadeDB) previously used a **push-based** model where worker processes proactively evaluated all output partitions of every plan fragment upon setup.

In contrast, the canonical gRPC implementation (`src/protocol/grpc/worker_client.rs`) operates on a **pull-based (RPC)** model:
- A downstream consumer issues a `WorkerClient::execute_task(request)` RPC to an upstream worker for a specific partition range (`target_partition_start..end`).
- The upstream worker lazily receives this request and executes *only* the requested partition range.

The `src/shm` implementation has been refactored into a true pull-based RPC architecture over shared memory, aligning it with the gRPC transport semantics while preserving shared-memory performance and cooperative draining.

---

## End-to-End Execution Flow

```
+-------------------+           ExecuteTaskFrame (MppFrameHeader::execute_task)          +-------------------+
| Downstream Proc B | -----------------------------------------------------------------> | Upstream Proc A   |
| (Consumer)        |                                                                    | (Producer)        |
|                   | <----------------------------------------------------------------- |                   |
+-------------------+           Arrow Batch Stream (MppFrameHeader::batch)               +-------------------+
```

### 1. Plan Dispatch & Task Registration
- The coordinator sends physical plan fragments (`SetPlanRequest`) to the designated worker processes via `send_set_plan`.
- Upon receiving a plan, the worker process registers `(task_key, plan, task_ctx)` into its local task execution registry and idles without evaluating any output partitions.

### 2. Issuing the RPC (`ShmWorkerChannel::execute_task`)
- When a downstream consumer task requires input from an upstream stage, it invokes `ShmWorkerChannel::execute_task`.
- `execute_task` builds a `pb::ExecuteTaskRequest` specifying:
  - `task_key`: `(query_id, stage_id, task_number)`
  - `target_partition_start..target_partition_end`: Range of partitions to evaluate.
  - `producer_head_spec`: Specifying head node handling (e.g. `RepartitionExec`, `BroadcastExec`, or `None`).
  - Pass-through HTTP `HeaderMap` (for object store / S3 credentials).
- The request is serialized into an `ExecuteTaskFrame` and tagged with `MppFrameHeader::execute_task(stage_id, task_number, sender_proc)`.
- The consumer sends the frame to the upstream producer (`dest_proc = proc_for_task(...)`) over its control sender (`send_execute_task`).

### 3. Inbound Interception & Routing (`DrainHandle` & `ExecuteTaskRegistry`)
- Upstream worker `dest_proc` runs its cooperative inbound drain (`try_drain_pass`).
- When `try_drain_pass` reads a frame with `MppFrameKind::ExecuteTask`:
  - It decodes the payload into an `ExecuteTaskFrame`.
  - It routes the frame to `ExecuteTaskRegistry` via `route_execute_task(stage_id, task_number, frame)`.
  - If a worker task is already waiting in `take_execute_task_rx(stage_id, task_number)`, the frame is pushed directly onto its unbounded channel (`ExecuteTaskRx`). Otherwise, it is queued in `ExecuteTaskSlot::Pending`.

### 4. Demand-Driven Execution (`run_worker_fragment`)
- The worker task waiting for `(stage_id, task_number)` receives the `ExecuteTaskFrame` from `rx.recv()`.
- It extracts `target_partition_start..target_partition_end`.
- Sinks mapped to the requesting consumer are opened for only that partition range (`sinks_opt.iter_mut().take(end).skip(start)`).
- `run_worker_fragment` is spawned specifically for the requested partition slice, evaluating only the needed partitions instead of the global stage output.

### 5. Data Streaming & EOF
- The producer streams record batches back to the consumer over the $A \rightarrow B$ DSM ring buffer tagged with `MppFrameHeader::batch`.
- Upon completing the requested partition slice, `send_eof` is emitted, closing the partition stream.

---

## Key Transport & Lifecycle Mechanisms

### 1. Frame Tagging & Protocol Header
`MppFrameHeader` explicitly differentiates frame types:
- `MppFrameKind::Batch`: Record batch payload.
- `MppFrameKind::SetPlan`: Plan dispatch.
- `MppFrameKind::ExecuteTask`: Pull-based RPC task request.
- `MppFrameKind::Cancel`, `MppFrameKind::Eof`, `MppFrameKind::TaskMetrics`, `MppFrameKind::WorkUnit`.

### 2. Inbox Keep-Alive Senders
- In shared memory rings, a ring buffer signals `Detached` when its data sender count (`sender_count`) drops to 0.
- Because pull-based workers idle before receiving task requests (with 0 active data streams initially), `MppMesh` retains a set of base `_keep_alive_senders` for the lifetime of the mesh.
- This prevents worker inboxes from prematurely detaching when data sinks drop, ensuring workers remain open to receive `ExecuteTask` control requests at any point during query execution.

### 3. Continuous Cooperative Drain Loop
- Worker process tasks run a background drain loop (`mesh.drain_all_inbound()`) alongside Tokio async execution.
- This ensures inbound `ExecuteTask` control messages in the DSM ring buffer are continuously processed and routed even while worker tasks are awaiting I/O or channel events.

### 4. Self-Loop Support
- Tasks within the same process can issue `ExecuteTask` requests to themselves via the in-process self-loop (`in_proc_channel`). Control-plane senders are installed on self-loop channels so in-process task requests bypass DSM shared memory overhead safely.
