//! Reproducers for joins that collect their build (left) side producing wrong results when
//! placed in a multi-task stage without the build side being broadcast.
//!
//! A CollectLeft HashJoin, a NestedLoopJoin, and a CrossJoin all require the *complete* build
//! side in every task. `insert_broadcast_execs` only guarantees that for join types that don't
//! emit build-side rows (and only when broadcast joins are enabled), but the task-count logic
//! in `inject_network_boundaries` does not cap the remaining shapes to a single task. The
//! build-side scan then gets sliced across tasks like any other leaf, and each task joins its
//! slice of the build side against its slice of the probe side.
//!
//! The tables are laid out so the slicing is visible: `build_side` holds ids 0..100 split
//! sequentially across 4 files, while `probe_side` holds the same ids (each repeated 50 times)
//! rotated one file forward. A task therefore sees *different* ids from each table, and any
//! cross-task match is silently lost.
//!
//! NOTE: these tests assert that the INCORRECT behavior happens (distributed results diverge
//! from single-node results). A passing test means the bug reproduced. When a fix lands, these
//! tests should start failing and be converted into correctness tests.

#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::property_based::compare_result_set;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
    use parquet::arrow::ArrowWriter;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const PARTITIONS: usize = 3;
    const FILES_PER_TABLE: i64 = 4;
    const IDS_PER_FILE: i64 = 25;
    const PROBE_DUPLICATES: usize = 50;

    fn data_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("target/multi_task_collect_join_repros")
    }

    static INIT: OnceCell<()> = OnceCell::const_new();

    async fn ensure_data() {
        INIT.get_or_init(|| async {
            let dir = data_dir();
            let _ = fs::remove_dir_all(&dir);
            // (table, rows per id, file rotation)
            for (table, duplicates, rotation) in [
                ("build_side", 1usize, 0i64),
                ("probe_side", PROBE_DUPLICATES, 1),
            ] {
                let table_dir = dir.join(table);
                fs::create_dir_all(&table_dir).unwrap();
                for file_idx in 0..FILES_PER_TABLE {
                    let chunk = (file_idx + rotation) % FILES_PER_TABLE;
                    let ids = (chunk * IDS_PER_FILE..(chunk + 1) * IDS_PER_FILE)
                        .flat_map(|id| std::iter::repeat(id).take(duplicates))
                        .collect::<Vec<_>>();
                    write_ids(&table_dir.join(format!("part-{file_idx}.parquet")), &ids);
                }
            }
        })
        .await;
    }

    fn write_ids(path: &Path, ids: &[i64]) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn register_tables(ctx: &SessionContext) -> Result<()> {
        for table in ["build_side", "probe_side"] {
            ctx.register_parquet(
                table,
                data_dir().join(table).to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
        Ok(())
    }

    async fn make_distributed_ctx(broadcast_joins: bool) -> Result<SessionContext> {
        let ctx = start_in_memory_context(NUM_WORKERS, DefaultSessionBuilder).await;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(1)?
            .with_distributed_broadcast_joins(broadcast_joins)?;
        register_tables(&ctx).await?;
        Ok(ctx)
    }

    async fn run(
        ctx: &SessionContext,
        query: &str,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<RecordBatch>)> {
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        Ok((plan, batches))
    }

    /// Runs `query` on a single-node and a distributed context, asserts the distributed plan
    /// contains `expected_plan_markers` (pinning the unsafe join shape), and asserts the two
    /// result sets DIVERGE — i.e. the bug reproduced.
    async fn assert_bug_reproduces(
        query: &str,
        broadcast_joins: bool,
        expected_plan_markers: &[&str],
    ) -> Result<()> {
        ensure_data().await;

        let s_ctx = SessionContext::new();
        register_tables(&s_ctx).await?;
        let d_ctx = make_distributed_ctx(broadcast_joins).await?;

        let (_, s_batches) = run(&s_ctx, query).await?;
        let (d_plan, d_batches) = run(&d_ctx, query).await?;

        let display = display_plan_ascii(d_plan.as_ref(), false);
        println!("distributed plan:\n{display}");
        assert!(
            d_plan.is::<DistributedExec>(),
            "query did not get distributed; plan was:\n{display}"
        );
        for marker in expected_plan_markers {
            assert!(
                display.contains(marker),
                "expected the distributed plan to contain `{marker}`; plan was:\n{display}"
            );
        }

        let s_rows: usize = s_batches.iter().map(|b| b.num_rows()).sum();
        let d_rows: usize = d_batches.iter().map(|b| b.num_rows()).sum();
        println!("single-node rows: {s_rows}, distributed rows: {d_rows}");

        let comparison = compare_result_set(&Ok(d_batches), &Ok(s_batches));
        assert!(
            comparison.is_err(),
            "distributed results unexpectedly matched single-node results — bug did not reproduce"
        );
        println!(
            "result sets diverge (this is the bug): {}",
            comparison.unwrap_err()
        );
        Ok(())
    }

    /// Case 1: CollectLeft HashJoin with a build-side-emitting join type (LeftSemi).
    /// Broadcast joins are ON, but `insert_broadcast_execs` skips LeftSemi, and nothing caps
    /// the stage to one task. Every id matches on a single node; distributed, a build id only
    /// survives if its probe rows landed in the same task.
    #[tokio::test]
    async fn repro_collect_left_semi_hash_join() -> Result<()> {
        assert_bug_reproduces(
            "SELECT id FROM build_side WHERE id IN (SELECT id FROM probe_side)",
            true,
            &["HashJoinExec: mode=CollectLeft, join_type=LeftSemi"],
        )
        .await
    }

    /// Case 2: anti join (`NOT IN`). Single-node: every build id exists in probe_side,
    /// so zero rows. Distributed, each task only sees a slice of probe_side, so most build ids
    /// look unmatched and phantom rows are emitted.
    #[tokio::test]
    async fn repro_not_in_anti_hash_join() -> Result<()> {
        assert_bug_reproduces(
            "SELECT id FROM build_side WHERE id NOT IN (SELECT id FROM probe_side)",
            true,
            &["join_type=LeftAnti"],
        )
        .await
    }

    /// Case 3: NestedLoopJoin with a build-side-emitting join type (LeftSemi), produced
    /// by a correlated EXISTS with a non-equi predicate (`p.id > b.id - 1 AND p.id < b.id + 1`
    /// is `p.id = b.id` for integers, but expressed as inequalities so no hash join is possible).
    #[tokio::test]
    async fn repro_nested_loop_left_semi_join() -> Result<()> {
        assert_bug_reproduces(
            "SELECT b.id FROM build_side b WHERE EXISTS ( \
                SELECT 1 FROM probe_side p WHERE p.id > b.id - 1 AND p.id < b.id + 1)",
            true,
            &["NestedLoopJoinExec", "join_type=LeftSemi"],
        )
        .await
    }

    /// Case 4: Full NestedLoopJoin. Emits unmatched rows from BOTH sides, so no
    /// broadcast orientation can ever be correct. Single-node: every row matches, no NULL
    /// padding. Distributed: cross-task matches are lost and spurious NULL-padded rows appear.
    #[tokio::test]
    async fn repro_nested_loop_full_join() -> Result<()> {
        assert_bug_reproduces(
            "SELECT b.id, p.id FROM build_side b FULL JOIN probe_side p \
                ON p.id > b.id - 1 AND p.id < b.id + 1",
            true,
            &["NestedLoopJoinExec", "join_type=Full"],
        )
        .await
    }

    /// Case 5: CrossJoin with broadcast joins DISABLED, so no BroadcastExec exists at
    /// all. Single-node: all 100 x 5000 = 500_000 pairs contribute to the sum. Distributed:
    /// each task only pairs its slice of each side, so most pairs are never produced.
    /// (A bare `count(*)` is folded to a constant from parquet statistics, so sum an
    /// expression the optimizer cannot answer from metadata.)
    #[tokio::test]
    async fn repro_cross_join_broadcast_disabled() -> Result<()> {
        assert_bug_reproduces(
            "SELECT sum(b.id + p.id) AS pair_sum FROM build_side b CROSS JOIN probe_side p",
            false,
            &["CrossJoinExec"],
        )
        .await
    }
}
