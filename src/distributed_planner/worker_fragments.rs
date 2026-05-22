use crate::distributed_planner::network_boundary::{NetworkBoundaryExt, NetworkBoundaryKind};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Description of a single producer-side fragment within a distributed plan,
/// yielded by [`for_each_worker_fragment`].
///
/// One [`WorkerFragment`] = one `(stage_id, task_idx)` tuple that some worker has to execute.
/// The visitor enumerates every such tuple in the plan; the caller decides:
///
/// 1. Which worker owns this fragment (e.g. URL-hash for gRPC, or proc-index round-robin
///    for an in-process embedder).
/// 2. How to route each of the producer's output partitions to a destination consumer,
///    using `kind` and `partitions_per_consumer_task` to do the receive-side math.
///
/// The visitor itself is pure plan-walking: it doesn't make routing decisions and doesn't
/// touch any transport. Use it whenever you need to enumerate the producer-side work
/// implied by a distributed plan — the existing fork uses URL-keyed gRPC plumbing inside
/// `DistributedExec::prepare_plan`, but in-process embedders (`paradedb/paradedb` pg_search
/// is the reference) consume this visitor directly to build their own dispatch table.
#[derive(Debug)]
pub struct WorkerFragment<'a> {
    /// `input_stage.num()` cast to `u32`. Frames carrying this fragment's output should be
    /// tagged with this value so the consumer side demuxes correctly.
    pub stage_id: u32,
    /// Task index within the stage (`0 ≤ task_idx < task_count`).
    pub task_idx: usize,
    /// Total task count for this stage. Each fragment in a stage shares the same
    /// `task_count` but differs in `task_idx`.
    pub task_count: usize,
    /// The producer-side plan to execute (`input_stage.local_plan()`). Borrowed from the
    /// walked tree; clone the `Arc` if you need to retain it past the visitor callback.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// What kind of network boundary owns this fragment. Drives the caller's routing
    /// decision — a `Shuffle` hash-partitions output partition `q` to consumer task
    /// `q / partitions_per_consumer_task`, a `Broadcast` (post-cap) is task-0-only, and
    /// a `Coalesce` gathers to a single consumer.
    pub kind: NetworkBoundaryKind,
    /// `B.properties().output_partitioning().partition_count()` = `P_c` in the receive-side
    /// formula `off = P_c * task_index` (see `NetworkShuffleExec::execute`). The
    /// per-consumer-task partition count: each consumer task reads partitions
    /// `[P_c * t, P_c * (t+1))` from this fragment's output.
    pub partitions_per_consumer_task: usize,
    /// `true` iff this fragment is nested inside another stage's local plan (the visitor
    /// recurses into `stage.local_plan()` with `nested = true`). The top-level boundary
    /// emitted from the plan root has `nested = false`. Routing differs by
    /// `(kind, nested)`: e.g. a top-level Coalesce gathers to the embedder's "root
    /// consumer" (a single destination), a nested Coalesce gathers to consumer task 0 of
    /// the parent stage.
    pub nested: bool,
}

/// Walk `root` and call `f` for every [`WorkerFragment`] produced by the distributed plan,
/// including those nested inside other stages.
///
/// Order: depth-first pre-order on the plan tree. For each network boundary, every
/// `(task_idx, task_count)` tuple in that stage is yielded before recursing into the
/// stage's `local_plan()`. Boundaries inside that local plan come later.
///
/// The visitor never recurses into the boundary's own `children()` — `NetworkBoundary`
/// returns `[stage.plan]`, so the visitor would otherwise double-process every nested
/// fragment.
///
/// `f` is called with a `WorkerFragment<'_>` that borrows from the plan tree. If you need
/// to retain the fragment past the callback (e.g. push into a `Vec`), clone the
/// `Arc<dyn ExecutionPlan>` for `plan`; the rest of the fields are `Copy`.
pub fn for_each_worker_fragment<F>(root: &Arc<dyn ExecutionPlan>, mut f: F)
where
    F: FnMut(WorkerFragment<'_>),
{
    walk(root, false, &mut f);
}

fn walk<F>(plan: &Arc<dyn ExecutionPlan>, nested: bool, f: &mut F)
where
    F: FnMut(WorkerFragment<'_>),
{
    if let Some(nb) = plan.as_ref().as_network_boundary() {
        let stage = nb.input_stage();
        let stage_id = stage.num() as u32;
        let kind = nb.kind();
        let p_c = plan.properties().partitioning.partition_count();
        let task_count = stage.task_count();
        if let Some(stage_plan) = stage.local_plan() {
            for task_idx in 0..task_count {
                f(WorkerFragment {
                    stage_id,
                    task_idx,
                    task_count,
                    plan: stage_plan,
                    kind,
                    partitions_per_consumer_task: p_c,
                    nested,
                });
            }
            // Recurse into the stage's plan with `nested = true`. Skipping `children()` is
            // intentional: `NetworkBoundary::children()` returns `[stage.plan]`, so descending
            // through it would re-visit every nested fragment.
            walk(stage_plan, true, f);
        }
        return;
    }
    // Non-boundary nodes recurse through plan children.
    for child in plan.children() {
        walk(child, nested, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::plans::{base_session_builder, context_with_query};
    use crate::{DistributedExt, SessionStateBuilderExt};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    fn empty_leaf() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn boundary_free_plan_yields_no_fragments() {
        let plan = empty_leaf();
        let mut count = 0;
        for_each_worker_fragment(&plan, |_frag| count += 1);
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn visits_every_task_in_every_stage_for_a_real_distributed_plan() {
        // SELECT … GROUP BY … against the test parquet fixture. With 3 workers and
        // broadcast disabled, the planner emits a Shuffle stage above the leaf scan and a
        // Coalesce at the root — every fragment of each stage should be enumerated.
        let query = "
            SELECT \"RainToday\", COUNT(*) AS c
            FROM weather
            GROUP BY \"RainToday\"
            ORDER BY c DESC
            LIMIT 5
        ";
        let builder = base_session_builder(4, 3, false).with_distributed_planner();
        let (ctx, query) = context_with_query(builder, query).await;
        let df = ctx.sql(&query).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        // Collect each fragment so we can assert on the shape AFTER the walk completes.
        #[derive(Debug, Clone, Copy)]
        struct CapturedFrag {
            stage_id: u32,
            task_idx: usize,
            task_count: usize,
            kind: NetworkBoundaryKind,
            nested: bool,
            partitions_per_consumer_task: usize,
        }
        let mut frags: Vec<CapturedFrag> = Vec::new();
        for_each_worker_fragment(&plan, |frag| {
            frags.push(CapturedFrag {
                stage_id: frag.stage_id,
                task_idx: frag.task_idx,
                task_count: frag.task_count,
                kind: frag.kind,
                nested: frag.nested,
                partitions_per_consumer_task: frag.partitions_per_consumer_task,
            });
        });

        assert!(
            !frags.is_empty(),
            "visitor must yield at least one fragment for a real distributed plan; got {frags:?}"
        );

        // Every (stage_id, task_idx) pair is unique — no double-yielding.
        let mut keys: Vec<_> = frags.iter().map(|f| (f.stage_id, f.task_idx)).collect();
        keys.sort();
        let n = keys.len();
        keys.dedup();
        assert_eq!(
            keys.len(),
            n,
            "fragments must have unique (stage_id, task_idx); got {frags:?}"
        );

        // For each stage_id, task_count is consistent across its fragments.
        let mut by_stage: std::collections::HashMap<u32, (usize, usize)> = Default::default();
        for f in &frags {
            let entry = by_stage.entry(f.stage_id).or_insert((0, f.task_count));
            entry.0 = entry.0.max(f.task_idx + 1);
            assert_eq!(
                entry.1, f.task_count,
                "task_count for stage {} must be consistent across fragments",
                f.stage_id,
            );
        }
        // Task indices fill `0..task_count` exactly.
        for (stage_id, (max_seen, task_count)) in &by_stage {
            assert_eq!(
                *max_seen, *task_count,
                "stage {stage_id} should yield fragments task_idx=0..{task_count}, max seen {max_seen}"
            );
        }

        // The plan has multiple stages: at least one top-level (`nested = false`) and one
        // nested (`nested = true`). The 3-worker GROUP BY produces a Coalesce stage at the
        // top + a Shuffle stage feeding it.
        assert!(
            frags.iter().any(|f| !f.nested),
            "expected at least one top-level (nested=false) fragment in a multi-stage plan; got {frags:?}"
        );
        assert!(
            frags.iter().any(|f| f.nested),
            "expected at least one nested (nested=true) fragment in a multi-stage plan; got {frags:?}"
        );

        // Iteration order: depth-first pre-order. The visitor must yield the top-level
        // stage's fragments BEFORE descending into nested stages. So the first emitted
        // fragment must have nested=false and the last fragment must be nested (otherwise
        // the visitor either failed to recurse or recursed before yielding).
        assert!(
            !frags.first().expect("non-empty").nested,
            "first yielded fragment must be top-level (depth-first pre-order); got {frags:?}"
        );
        assert!(
            frags.last().expect("non-empty").nested,
            "last yielded fragment must be from a nested stage (depth-first pre-order); got {frags:?}"
        );

        // `partitions_per_consumer_task` is propagated as a u32-castable count > 0 for every
        // fragment (`output_partitioning().partition_count()` is at least 1).
        for f in &frags {
            assert!(
                f.partitions_per_consumer_task > 0,
                "partitions_per_consumer_task must be > 0; got {f:?}"
            );
        }
    }
}
