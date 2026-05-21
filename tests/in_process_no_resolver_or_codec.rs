#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::{
        DistributedExec, DistributedExt, SessionStateBuilderExt, WorkerConnection,
        WorkerPartitionStream, WorkerTransport,
    };

    /// Pins the R4 contract: under `in_process_mode = true`, neither `_annotate_plan`'s
    /// `max_tasks` computation nor `prepare_plan`'s `available_urls` lookup may require a
    /// registered `WorkerResolver`. We register only an in-process `WorkerTransport` stub —
    /// no resolver, no `PhysicalExtensionCodec` — and run both the physical-plan build and
    /// `prepare_in_process_plan` end to end. A regression at either site would error here
    /// with "WorkerResolver not present in the session config" or "not implemented".
    #[tokio::test]
    async fn in_process_mode_skips_resolver_and_codec() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1, 3]))],
        )?;
        let mem = MemTable::try_new(schema, vec![vec![batch]])?;

        // Build a session with the four knobs an embedded in-process runtime sets, and
        // crucially WITHOUT registering a `WorkerResolver` or a user `PhysicalExtensionCodec`.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .with_distributed_worker_transport(NoopWorkerTransport)
            .with_distributed_in_process_mode(true)?
            // Pick a task estimator that produces a 2-task per-stage shape so the planner
            // actually inserts a `NetworkShuffleExec` boundary above the `GROUP BY`.
            .with_distributed_task_estimator(2usize)
            .with_distributed_planner()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", Arc::new(mem))?;

        // `GROUP BY` with `target_partitions = 2` forces a `RepartitionExec` which the
        // distributed planner converts into a `NetworkShuffleExec` — i.e. the network
        // boundary that exercises both `_annotate_plan` and `prepare_plan`'s gates.
        let plan = ctx
            .sql("SELECT y, COUNT(*) FROM t GROUP BY y")
            .await?
            .create_physical_plan()
            .await?;

        let distributed = plan
            .as_any()
            .downcast_ref::<DistributedExec>()
            .expect("planner wraps the result in DistributedExec");

        // `prepare_in_process_plan` runs the same `prepare_plan` body the executor would,
        // minus the gRPC send tasks. With `in_process_mode = true`, the gates added by
        // paradedb/datafusion-distributed#10 mean neither call site touches the resolver.
        distributed.prepare_in_process_plan(&ctx.task_ctx())?;

        Ok(())
    }

    /// Bare-minimum `WorkerTransport` impl. The test never reaches an `open()` call (it
    /// stops at `prepare_in_process_plan`, which only walks and rewrites the plan), so
    /// the method body would only fire if the gate at `distributed.rs:292`'s
    /// `if in_process { continue; }` regressed.
    struct NoopWorkerTransport;
    impl WorkerTransport for NoopWorkerTransport {
        fn open(
            &self,
            _input_stage: &datafusion_distributed::RemoteStage,
            _target_partitions: std::ops::Range<usize>,
            _target_task: usize,
            _ctx: &Arc<datafusion::execution::TaskContext>,
            _metrics: &datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet,
        ) -> Result<Box<dyn WorkerConnection + Send + Sync>> {
            unreachable!(
                "NoopWorkerTransport::open called — the in-process prepare path should not \
                 dispatch through the WorkerTransport"
            )
        }
    }

    impl WorkerConnection for NoopWorkerTransport {
        fn stream_partition(&self, _partition: usize) -> Result<WorkerPartitionStream> {
            unreachable!("NoopWorkerTransport::stream_partition called")
        }
    }
}
