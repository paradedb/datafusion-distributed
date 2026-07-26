# Proposal: Shift Metric Aggregation from Rewrite-Time to Display-Time

## Problem Context
When running `EXPLAIN ANALYZE` on distributed plans in `pg_search`, the regression tests (`pg_regress`) flake due to non-deterministic metrics. 

Because `datafusion-distributed` displays MPP metrics at a per-task granularity (e.g., `metrics=[output_rows={0:13, 1:5}, row_replacements={0:16, 1:12}]`), any timing variation in how rows distribute across worker threads causes the `.out` file to mismatch.

In single-node `pg_search` plans, this doesn't flake because `pg_search`'s `render_plan_with_metrics` explicitly calls `aggregate_by_name()`. This sums the fluctuating per-thread counts into a single, deterministic global total (`output_rows=18`). 

However, we **do not** want to permanently aggregate distributed metrics, because seeing the per-task breakdown (like skew) is incredibly valuable for manual debugging.

## Current Architecture
Currently, `datafusion-distributed` solves metric formatting via `DistributedMetricsFormat::Aggregated` vs `DistributedMetricsFormat::PerTask`. This choice is applied during the **rewrite phase** (`rewrite_distributed_plan_with_metrics`).

Because `pg_search`'s `merge_worker_metrics` routine runs *before* the planner checks if the user requested `VERBOSE` output, `pg_search` is forced to hardcode the `PerTask` format for all executions. By the time `display_plan_ascii` is called, the metrics are permanently baked into the plan as `PerTask`, meaning regression tests (which run without `VERBOSE`) still flake.

## Proposed Solution
We propose shifting the aggregation decision from **rewrite-time** to **display-time**. 

By decoupling the display logic from the rewrite logic, `pg_search` can instruct `display_plan_ascii` to dynamically collapse metrics into deterministic totals for regression tests (`VERBOSE` off), while leaving them split out for human debugging (`VERBOSE` on).

### Implementation Steps

1. **Convert `DisplayMetrics` into a Configuration Struct**
   Change `DisplayMetrics` from an enum into a struct with explicit toggles, adding a flag for task aggregation:
   ```rust
   pub struct DisplayMetrics {
       pub show_metrics: bool,
       pub show_timing: bool,
       pub aggregate_tasks: bool, // When true, sum metrics across tasks
   }
   ```

2. **Strip Task IDs at Render Time**
   Update `format_metrics_by_task` in `datafusion-distributed/src/stage.rs`. When `aggregate_tasks` is `true`, strip the `DISTRIBUTED_DATAFUSION_TASK_ID_LABEL` from all metrics before grouping. 
   Without the task label, DataFusion's `aggregate_by_name()` will naturally sum the per-task counts (`output_rows` for partition 0 + partition 1) into a single deterministic total.

3. **Always Rewrite as `PerTask`**
   Deprecate or remove `DistributedMetricsFormat::Aggregated` from the metrics rewriter. `pg_search` and `datafusion-distributed` will always pull and embed metrics in the `PerTask` format.

4. **Map `VERBOSE` in `pg_search`**
   In `pg_search`'s `explain_physical_plan`, map Postgres' EXPLAIN flags directly into the new `DisplayMetrics` struct:
   ```rust
   let metrics = DisplayMetrics {
       show_metrics: explainer.is_analyze(),
       show_timing: explainer.is_verbose(), // Or is_timing() if added
       aggregate_tasks: !explainer.is_verbose(),
   };
   let rendered = display_plan_ascii(plan.as_ref(), metrics);
   ```

## Outcomes
- **Regression Tests (`VERBOSE` off)**: `aggregate_tasks` is true. `display_plan_ascii` sums the metrics and strips task IDs. The `.out` files become 100% deterministic (`row_replacements=28`).
- **Human Debugging (`VERBOSE` on)**: `aggregate_tasks` is false. `display_plan_ascii` retains task IDs. Developers get the full, per-task skew breakdown (`row_replacements={0:16, 1:12}`).
- **Cleaner API**: Rendering logic stays strictly within the display methods, and the plan tree itself always retains maximum fidelity.
