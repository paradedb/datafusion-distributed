use crate::distributed_planner::network_boundary::{NetworkBoundaryExt, NetworkBoundaryKind};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// One producer-side fragment within a distributed plan, yielded by
/// [`for_each_worker_fragment`].
///
/// A [`WorkerFragment`] is one `(stage_id, task_idx)` tuple some worker has to execute.
/// The visitor enumerates every such tuple; the caller decides:
///
/// 1. Which worker owns this fragment (e.g. URL hash for gRPC, proc-index round-robin
///    for in-process embedders).
/// 2. How to route each output partition to a destination consumer, using `kind` and
///    `partitions_per_consumer_task` for the receive-side math.
///
/// The visitor is pure plan-walking. It makes no routing decisions and touches no
/// transport. Reach for it whenever you need to enumerate the producer-side work in a
/// distributed plan. The default gRPC path uses URL-keyed plumbing in
/// `DistributedExec::prepare_plan`; in-process embedders consume this visitor directly
/// to build their own dispatch.
#[derive(Debug)]
pub struct WorkerFragment<'a> {
    /// `input_stage.num()` cast to `u32`. Frames carrying this fragment's output should
    /// be tagged with this value so the consumer side demuxes correctly.
    pub stage_id: u32,
    /// Task index within the stage (`0 ≤ task_idx < task_count`).
    pub task_idx: usize,
    /// Total task count for this stage. Every fragment in a stage shares the same
    /// `task_count` but differs in `task_idx`.
    pub task_count: usize,
    /// The producer-side plan to execute (`input_stage.local_plan()`). Borrowed from
    /// the walked tree; clone the `Arc` if you need to keep it past the callback.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// Which kind of network boundary owns this fragment. Drives the routing decision.
    /// `Shuffle` hash-partitions output partition `q` to consumer task
    /// `q / partitions_per_consumer_task`; post-cap `Broadcast` is task-0-only;
    /// `Coalesce` gathers to a single consumer.
    pub kind: NetworkBoundaryKind,
    /// `B.properties().output_partitioning().partition_count()`, used as `P_c` in the
    /// receive-side formula `off = P_c * task_index` (see `NetworkShuffleExec::execute`).
    /// Each consumer task reads partitions `[P_c * t, P_c * (t+1))` from this output.
    pub partitions_per_consumer_task: usize,
    /// `true` iff this fragment is nested inside another stage's local plan (the visitor
    /// recurses into `stage.local_plan()` with `nested = true`). Top-level boundaries
    /// from the plan root have `nested = false`. Routing differs by `(kind, nested)`:
    /// e.g. a top-level Coalesce gathers to the embedder's root consumer, a nested
    /// Coalesce gathers to consumer task 0 of the parent stage.
    pub nested: bool,
}

/// Walk `root` and call `f` for every [`WorkerFragment`] in the distributed plan,
/// including those nested inside other stages.
///
/// Order is depth-first pre-order. For each network boundary, every
/// `(task_idx, task_count)` tuple in that stage is yielded before recursing into
/// `stage.local_plan()`. Boundaries inside that local plan come later.
///
/// The visitor never recurses into the boundary's own `children()`. `NetworkBoundary`
/// returns `[stage.plan]`, so descending through it would double-process every
/// nested fragment.
///
/// `f` receives a `WorkerFragment<'_>` that borrows from the plan tree. To keep a
/// fragment past the callback (e.g. push into a `Vec`), clone the `Arc<dyn ExecutionPlan>`
/// for `plan`. The rest of the fields are `Copy`.
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
            // Recurse into the stage's plan with `nested = true`. We skip `children()` on
            // purpose: `NetworkBoundary::children()` returns `[stage.plan]`, so descending
            // through it would re-visit every nested fragment.
            walk(stage_plan, true, f);
        }
        return;
    }
    // Non-boundary node: recurse through plan children.
    for child in plan.children() {
        walk(child, nested, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionStateBuilderExt;
    use crate::test_utils::plans::{base_session_builder, context_with_query};
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
        // SELECT … GROUP BY … against the parquet fixture. With 3 workers and broadcast
        // off, the planner emits a Shuffle stage above the leaf scan and a Coalesce at
        // the root. Every fragment of each stage should show up.
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

        // Collect every fragment so we can assert on shape after the walk. All fields get
        // read by `Debug` in assertion-failure messages even if some aren't checked by
        // an `assert!` line, so `#[allow(dead_code)]` keeps clippy quiet.
        #[derive(Debug, Clone, Copy)]
        #[allow(dead_code)]
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

        // Every (stage_id, task_idx) pair is unique. No double-yielding.
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
        let mut by_stage: datafusion::common::HashMap<u32, (usize, usize)> = Default::default();
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

        // The plan is multi-stage: at least one top-level fragment (`nested = false`) and
        // one nested (`nested = true`). The 3-worker GROUP BY gives a Coalesce at the top
        // and a Shuffle feeding it.
        assert!(
            frags.iter().any(|f| !f.nested),
            "expected at least one top-level (nested=false) fragment in a multi-stage plan; got {frags:?}"
        );
        assert!(
            frags.iter().any(|f| f.nested),
            "expected at least one nested (nested=true) fragment in a multi-stage plan; got {frags:?}"
        );

        // Iteration order is depth-first pre-order: top-level fragments come before
        // nested ones. So the first emitted fragment has nested=false and the last is
        // nested. Otherwise the visitor either failed to recurse or recursed before
        // yielding.
        assert!(
            !frags.first().expect("non-empty").nested,
            "first yielded fragment must be top-level (depth-first pre-order); got {frags:?}"
        );
        assert!(
            frags.last().expect("non-empty").nested,
            "last yielded fragment must be from a nested stage (depth-first pre-order); got {frags:?}"
        );

        // `partitions_per_consumer_task` is propagated as a positive count for every
        // fragment (`output_partitioning().partition_count()` is at least 1).
        for f in &frags {
            assert!(
                f.partitions_per_consumer_task > 0,
                "partitions_per_consumer_task must be > 0; got {f:?}"
            );
        }
    }
}
