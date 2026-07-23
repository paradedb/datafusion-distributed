//! Validates the fundamental invariant of stage replication: for every stage,
//! executing its plan once per task over the per-task input assignment and unioning the
//! outputs must be equivalent to executing it once over all the data.
//!
//! DataFusion's optimizer discharges each operator's `required_input_distribution()`
//! obligations *process-locally* (a `SinglePartition` requirement is satisfied by a
//! `CoalescePartitionsExec`, a `HashPartitioned` one by a `RepartitionExec`). Replicating the
//! plan across tasks silently reinterprets "all the data" as "this task's slice", invalidating
//! those discharged obligations. Only the exchange operators — the `Network*Exec` boundaries —
//! restore properties globally:
//!
//! - `NetworkShuffleExec` → globally hash-partitioned (equal keys co-locate cluster-wide)
//! - `NetworkBroadcastExec` → a complete, identical copy in every task
//! - `NetworkCoalesceExec` → a global single partition, via a single consumer task
//!
//! The validator classifies every stage-local subtree bottom-up as either [Replicated]
//! (every task materializes the identical complete dataset) or [Partitioned] (every task
//! materializes a task-specific slice whose union is the whole), and enforces two obligations
//! along the way:
//!
//! (A) every declared input-distribution requirement must hold *cluster-globally*:
//!     `SinglePartition` may only be satisfied by a replicated subtree, and
//!     `HashPartitioned` only by a global shuffle (or a replicated copy);
//! (B) replicated data may only feed operators that never *emit* rows driven by it —
//!     N task instances would emit such rows N times, and the machinery that deduplicates
//!     them in a single process (e.g. the hash join's shared visited bitmap) does not exist
//!     across tasks. This is the one fact not derivable from any DataFusion API; see
//!     [emits_rows_driven_by].
//!
//! Ordering is a known gap: output-ordering claims weaken from global to task-local exactly
//! like distribution claims, but this validator does not model them.

use std::sync::Arc;

use datafusion::common::{Result, plan_err};
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{Distribution, ExecutionPlan};

use crate::execution_plans::DistributedLeafExec;
use crate::stage::{Stage, find_all_stages};
use crate::{NetworkBoundaryExt, NetworkBroadcastExec, NetworkShuffleExec};

use super::insert_broadcast::is_left_broadcast_safe;

/// How a subtree's data is laid out across the tasks of its stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataFlow {
    /// Every task materializes the identical, complete dataset.
    Replicated,
    /// Every task materializes a task-specific subset; the union across tasks is the whole
    /// dataset. `by_key` is true when rows were routed by a *global* hash exchange, so all
    /// rows with equal keys live in the same task-local partition.
    Partitioned { by_key: bool },
}

/// Validates every stage embedded in a fully planned distributed plan.
pub(crate) fn validate_distributed_stages(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    for stage in find_all_stages(plan) {
        if let Stage::Local(stage) = stage {
            validate_stage_plan(&stage.plan, stage.tasks)?;
        }
    }
    Ok(())
}

/// Validates a single stage's plan against its task count. Single-task stages are trivially
/// correct: one instance sees all the data, exactly as DataFusion's optimizer assumed.
pub(crate) fn validate_stage_plan(plan: &Arc<dyn ExecutionPlan>, tasks: usize) -> Result<()> {
    if tasks <= 1 {
        return Ok(());
    }
    match classify(plan, tasks)? {
        DataFlow::Partitioned { .. } => Ok(()),
        DataFlow::Replicated => plan_err!(
            "stage runs {tasks} tasks but its plan is fully replicated: every task would \
             produce the identical output and the stage would emit {tasks} copies of it"
        ),
    }
}

/// Classifies `node`'s output as [DataFlow::Replicated] or [DataFlow::Partitioned] and
/// enforces obligations (A) and (B) for its inputs. Recursion stops at network boundaries:
/// their subtrees belong to child stages, which are validated against their own task counts.
fn classify(node: &Arc<dyn ExecutionPlan>, tasks: usize) -> Result<DataFlow> {
    // Exchange operators re-establish global properties.
    if node.is::<NetworkBroadcastExec>() {
        return Ok(DataFlow::Replicated);
    }
    if node.is::<NetworkShuffleExec>() {
        return Ok(DataFlow::Partitioned { by_key: true });
    }
    if node.is_network_boundary() {
        // NetworkCoalesceExec (or a future boundary type): gathers all partitions into a
        // single consumer task, so it must never appear in a multi-task stage.
        return plan_err!(
            "stage runs {tasks} tasks but contains {}, which requires a single-task \
             consumer stage",
            node.name()
        );
    }
    // A DistributedLeafExec resolves to a different slice of the underlying source in every
    // task. (Its per-task variants are children in the plan tree, so check before the leaf
    // case below.)
    if node.is::<DistributedLeafExec>() {
        return Ok(DataFlow::Partitioned { by_key: false });
    }
    let children = node.children();
    if children.is_empty() {
        // Any other leaf (in-memory table, literal values, an unsliced scan) is embedded
        // verbatim in every task's serialized plan: identical, complete data everywhere.
        // NOTE: a volatile leaf (e.g. one backed by a random or time-dependent source)
        // would break this assumption; nothing in the ExecutionPlan API exposes that.
        return Ok(DataFlow::Replicated);
    }

    let child_flows = children
        .iter()
        .map(|child| classify(child, tasks))
        .collect::<Result<Vec<_>>>()?;

    // Obligation (A): declared input-distribution requirements must hold cluster-globally.
    let requirements = node.required_input_distribution();
    for (idx, ((child, flow), requirement)) in
        children.iter().zip(&child_flows).zip(&requirements).enumerate()
    {
        match requirement {
            Distribution::UnspecifiedDistribution => {}
            Distribution::SinglePartition => {
                if let DataFlow::Partitioned { .. } = flow {
                    return plan_err!(
                        "{} requires a single partition from its input {idx} ({}), but in a \
                         {tasks}-task stage that input only delivers the task's own slice of \
                         the data; each task would silently compute on partial data. The \
                         input must be replicated (broadcast) or the stage must run in a \
                         single task",
                        node.name(),
                        child.name()
                    );
                }
            }
            Distribution::HashPartitioned(_) => {
                if matches!(flow, DataFlow::Partitioned { by_key: false }) {
                    return plan_err!(
                        "{} requires its input {idx} ({}) to be hash-partitioned, but in a \
                         {tasks}-task stage that partitioning was established task-locally: \
                         equal keys living in different tasks would never meet. The input \
                         must come through a NetworkShuffleExec",
                        node.name(),
                        child.name()
                    );
                    // `Partitioned { by_key: true }` is a global shuffle. `Replicated` also
                    // passes: every task computes over the complete data, and whether the
                    // resulting duplication is legal is decided where it mixes into
                    // partitioned flow, or at the stage root.
                }
            }
        }
    }

    let any_partitioned = child_flows
        .iter()
        .any(|flow| matches!(flow, DataFlow::Partitioned { .. }));
    if !any_partitioned {
        // A deterministic operator over exclusively replicated inputs produces replicated
        // output; legality is deferred to the consumer.
        return Ok(DataFlow::Replicated);
    }

    // Obligation (B): this node mixes replicated inputs into partitioned flow, so its output
    // rows must be driven exclusively by the partitioned inputs.
    for (idx, flow) in child_flows.iter().enumerate() {
        if matches!(flow, DataFlow::Replicated) && emits_rows_driven_by(node, idx) {
            return plan_err!(
                "{} emits rows driven by its replicated input {idx}; each of the stage's \
                 {tasks} task instances would emit those rows, duplicating them in the \
                 stage output",
                node.name()
            );
        }
    }

    // Global key-partitioning survives only through operators that don't reshuffle rows
    // between partitions. A stage-local RepartitionExec re-routes within the task, so its
    // output partitions are no longer globally keyed.
    // NOTE: this is an approximation; a production version should consult
    // `node.properties().output_partitioning()` and equivalence classes instead.
    let by_key = !node.is::<RepartitionExec>()
        && child_flows.iter().all(|flow| match flow {
            DataFlow::Partitioned { by_key } => *by_key,
            DataFlow::Replicated => true,
        });
    Ok(DataFlow::Partitioned { by_key })
}

/// The one fact about an operator that no DataFusion API exposes: does it emit output rows
/// *driven by* the data of its `child_idx` input (as opposed to merely probing it)? An input
/// may be replicated across task instances only when the answer is no.
///
/// Unknown operators default to `true`: a `UnionExec`, a limit, a window — anything that
/// forwards or produces rows from a replicated input — would duplicate them, so the
/// conservative answer is the correct default. New operators must opt in here explicitly.
fn emits_rows_driven_by(node: &Arc<dyn ExecutionPlan>, child_idx: usize) -> bool {
    if let Some(join) = node.downcast_ref::<HashJoinExec>() {
        return child_idx == 0 && !is_left_broadcast_safe(join.join_type());
    }
    if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() {
        return child_idx == 0 && !is_left_broadcast_safe(join.join_type());
    }
    if node.is::<CrossJoinExec>() {
        // Every output row pairs a build row with a probe row, so output is probe-driven:
        // with a partitioned probe side, each pair is produced exactly once.
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::plans::TestPlanBuilder;

    async fn plan_distributed(query: &str, broadcast_joins: bool) -> Arc<dyn ExecutionPlan> {
        TestPlanBuilder::new()
            .target_partitions(3)
            .num_workers(4)
            .distributed_planner(true)
            .broadcast_joins(broadcast_joins)
            .build()
            .await
            .physical_plan(query)
            .await
    }

    #[tokio::test]
    async fn rejects_unbroadcast_collect_left_join_in_multi_task_stage() {
        // LEFT is not broadcast-safe, so `insert_broadcast_execs` never broadcasts its build
        // side; with broadcast joins enabled nothing caps the stage to one task either, and
        // each task collects only its slice of the build side.
        let plan = plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday""#,
            true,
        )
        .await;
        let err = validate_distributed_stages(&plan).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("requires a single partition"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn accepts_broadcast_inner_join() {
        // Inner is broadcast-safe: the build side arrives through a NetworkBroadcastExec
        // (replicated), the probe side is a sliced leaf (partitioned), and an inner join
        // only emits probe-driven rows.
        let plan = plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a INNER JOIN weather b ON a."RainToday" = b."RainToday""#,
            true,
        )
        .await;
        validate_distributed_stages(&plan).expect("expected validation to pass");
    }

    #[tokio::test]
    async fn rejects_unbroadcast_nested_loop_left_join() {
        // A Left NLJ is not broadcast-safe either; its collected left side arrives through
        // a plain CoalescePartitionsExec over a sliced leaf.
        let plan = plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a LEFT JOIN weather b ON a."MinTemp" < b."MaxTemp""#,
            true,
        )
        .await;
        let err = validate_distributed_stages(&plan).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("requires a single partition"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn rejects_cross_join_with_broadcast_disabled() {
        // Cross joins are always broadcast-safe, but with broadcast joins disabled there is
        // no broadcast at all, and no gating arm covers CrossJoinExec.
        let plan = plan_distributed(
            r#"SELECT sum(a."MinTemp" + b."MaxTemp")
               FROM weather a CROSS JOIN weather b"#,
            false,
        )
        .await;
        let err = validate_distributed_stages(&plan).expect_err("expected validation to fail");
        assert!(
            err.to_string().contains("requires a single partition"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn accepts_plan_with_broadcast_disabled() {
        // With broadcast joins disabled the planner caps CollectLeft joins to a single task,
        // so whatever stages remain must validate cleanly.
        let plan = plan_distributed(
            r#"SELECT a."MinTemp", b."MaxTemp"
               FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday""#,
            false,
        )
        .await;
        validate_distributed_stages(&plan).expect("expected validation to pass");
    }
}
