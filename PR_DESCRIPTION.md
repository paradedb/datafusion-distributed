# Ticket(s) Closed

- Closes #

## What

Adds support for `Partitioning::Range` across distributed stage network boundaries (`NetworkShuffleExec`), and fixes partition count tracking in `NetworkCoalesceExec` when downstream operator properties scale during stage preparation.

Specifically:
1. Enables `inject_network_boundaries` and `NetworkShuffleExec` to support `Partitioning::Range` alongside `Partitioning::Hash`.
2. Updates `NetworkCoalesceExec::with_input_stage` and `NetworkCoalesceExec::with_new_children` to dynamically recompute advertised plan properties from the underlying plan instead of retaining stale partition counts when child stages are scaled down during stage preparation.
3. Updates leaf file scan scaling (`file_scan_config.rs`) to allocate range-partitioned file groups contiguously across tasks in key order rather than interleaving via round-robin.
4. Patches DataFusion dependencies onto `paradedb/datafusion:stuhood.branch-55-range-scaling` (cherry-picking `RangePartitioning::scale` onto `branch-55`).
5. Adds comprehensive range partitioning test coverage in `tests/range_partitioning.rs`.

## Why

### 1. Range Shuffles Across Stages
Previously, `inject_network_boundaries` only checked for `Partitioning::Hash`. When DataFusion produced a `RepartitionExec: Range` (such as when aligning an intermediate join output with a downstream range-partitioned table), no `NetworkShuffleExec` was injected. The repartitioner ran entirely within each producer task, meaning rows from task `j` belonging in range partition `k` (`j != k`) were never routed across the network to task `k`, yielding incomplete results.

### 2. Partition Count Desynchronization in `NetworkCoalesceExec`
When a range shuffle occurs on the probe (right) side of a `HashJoinExec: mode=Partitioned`:
- During `inject_network_boundaries`, `NetworkShuffleExec` is initially created with stage-level output partition count `K` (matching the cluster task count, e.g. 4).
- Operators above it (`HashJoinExec`, `SortExec`) inherit `K = 4` partitions, and `NetworkCoalesceExec` initializes its output properties to `K * K = 16` partitions.
- Later, `prepare_network_boundaries` scales `NetworkShuffleExec`'s properties down to `UnknownPartitioning(1)` so each consumer task processes 1 partition.
- While `HashJoinExec` and `SortExec` update their properties to 1 partition, `NetworkCoalesceExec`'s `with_input_stage` previously only scaled properties by `new_tasks / old_tasks` (`4 / 4 = 1`), and `with_new_children` did not update properties at all.
- As a result, `NetworkCoalesceExec` retained the stale output partition count of 16 instead of 4, causing runtime task panics when tasks tried to fetch non-existent partitions from the head plan.

## How

1. **Range Boundary Injection & Plan Construction**
   - In `src/distributed_planner/inject_network_boundaries.rs`, matched `Partitioning::Hash(_, _) | Partitioning::Range(_)`.
   - In `src/execution_plans/network_shuffle.rs`, updated `try_new` and `producer_head` to accept range partitioning and dynamically scale range split points to consumer task counts using DataFusion's `RangePartitioning::scale`.
   - In `src/execution_plans/common.rs` and `src/stage.rs`, separated shuffle fan-out scaling (`scale_shuffle_partitioning`) from coalesce partition scaling (`coalesce_partitioning`), handling `Partitioning::Range` accurately in each context.
   - In `src/distributed_planner/prepare_network_boundaries.rs`, scaled `NetworkShuffleExec`'s properties down to `UnknownPartitioning(1)` for consumer-stage per-task execution.
   - In `src/codec/distributed_codec.rs`, serialized `NetworkShuffleExec`'s configured partitioning and ensured worker-side decoded properties use single-partition unknown partitioning for range shuffles.
   - In `src/events/defaults/file_scan_config.rs`, added range awareness to `desired_task_count` and introduced `rebalance_contiguous` to allocate file groups contiguously across tasks in range order.
   - In `src/metrics/bytes_metric.rs`, `src/execution_plans/sampler.rs`, and `src/protocol/grpc/worker_client.rs`, disambiguated `bytes_counter_metric` from DataFusion's inherent `MetricBuilder::bytes_counter` method.

2. **`NetworkCoalesceExec` Dynamic Property Recomputation**
   - In `src/execution_plans/network_coalesce.rs`, updated `with_input_stage` and `with_new_children` to recompute advertised `PlanProperties` directly from `local.plan.properties()` scaled by `local.tasks` whenever the input stage is local.
   - This ensures downstream property adjustments made during stage preparation (such as range shuffle scaling) properly propagate through the coalesce boundary.

3. **In-tree Integration Tests**
   - Added and updated tests in `tests/range_partitioning.rs` exercising:
     - `test_join_range_prepartitioned_both_sides`: Both sides pre-partitioned by range; no network shuffle.
     - `test_join_range_unsatisfied_stream_adapts_to_range`: Left-side range shuffle into right-side pre-partitioned fact table.
     - `test_join_range_unsatisfied_stream_adapts_to_range_under_parallelism`: Range shuffle scaling down to fewer consumer tasks.
     - `test_join_range_under_parallelism` & `test_join_range_over_parallelism`: Task count scaling with range partitioning.
     - `test_three_way_join_range_to_hash_shuffle`: 3-way join with range co-partitioned `dim` and `fact` chaining into a downstream hash shuffle with unpartitioned `services`.
     - `test_three_way_aggregation_broadcast_dimension`: 3-way aggregation with broadcast `services` dimension via `NetworkBroadcastExec` joined over a range-partitioned join, followed by partial agg, hash shuffle, and final partitioned agg.

## Tests

- `cargo test --test range_partitioning --features integration`
- `cargo check --all-targets --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo fmt --all -- --check`
