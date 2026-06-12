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
        DistributedExec, DistributedExt, SessionStateBuilderExt, WorkerConnection, WorkerTransport,
    };
    use futures::stream::BoxStream;

    /// Under `in_process_mode = true`, neither the boundary-injection planner's `max_tasks`
    /// computation nor `prepare_static_plan`'s `available_urls` lookup may require a registered
    /// `WorkerResolver`. We register only an in-process `WorkerTransport` stub, no resolver, no
    /// `PhysicalExtensionCodec`, and run both the physical-plan build and
    /// `prepare_in_process_plan` end to end. A regression at either site would error here with
    /// "WorkerResolver not present in the session config" or "not implemented".
    #[tokio::test]
    async fn in_process_mode_skips_resolver_and_codec() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1, 3]))],
        )?;
        let mem = MemTable::try_new(schema, vec![vec![batch]])?;

        // Set the knobs an in-process embedder needs, and crucially DO NOT register a
        // `WorkerResolver` or a user `PhysicalExtensionCodec`.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .with_distributed_worker_transport(NoopWorkerTransport)
            .with_distributed_in_process_mode(true)?
            // 2 tasks per stage so the planner actually inserts a `NetworkShuffleExec`
            // boundary above the `GROUP BY`.
            .with_distributed_task_estimator(2usize)
            .with_distributed_planner()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", Arc::new(mem))?;

        // `GROUP BY` with `target_partitions = 2` forces a `RepartitionExec`, which the
        // distributed planner converts into a `NetworkShuffleExec`. That's the boundary that
        // exercises both the max-tasks lookup and `prepare_static_plan`'s gates.
        let plan = ctx
            .sql("SELECT y, COUNT(*) FROM t GROUP BY y")
            .await?
            .create_physical_plan()
            .await?;

        let distributed = plan
            .as_any()
            .downcast_ref::<DistributedExec>()
            .expect("planner wraps the result in DistributedExec");

        // `prepare_in_process_plan` runs the same prep body the executor would, minus the gRPC
        // send tasks. With `in_process_mode = true` the gates short-circuit both resolver
        // lookups, so this call succeeds without one registered.
        distributed.prepare_in_process_plan(&ctx.task_ctx())?;

        Ok(())
    }

    /// Bare-minimum `WorkerTransport` impl. The test stops at `prepare_in_process_plan`, which
    /// only walks and rewrites the plan, so neither `open()` nor `dispatcher()` should be
    /// reached. They would only fire if the in-process skips in the prepare path regressed.
    struct NoopWorkerTransport;
    impl WorkerTransport for NoopWorkerTransport {
        fn open(
            &self,
            _input_stage: &datafusion_distributed::RemoteStage,
            _target_partitions: std::ops::Range<usize>,
            _target_task: usize,
            _ctx: &Arc<datafusion::execution::TaskContext>,
            _metrics: &datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet,
        ) -> Result<Box<dyn WorkerConnection>> {
            unreachable!(
                "NoopWorkerTransport::open called: the in-process prepare path should not \
                 dispatch through the WorkerTransport"
            )
        }

        fn dispatcher(&self) -> Box<dyn datafusion_distributed::WorkerDispatch> {
            unreachable!(
                "NoopWorkerTransport::dispatcher called: the in-process prepare path should \
                 not create a dispatcher"
            )
        }
    }

    impl WorkerConnection for NoopWorkerTransport {
        fn execute(&self, _partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
            unreachable!("NoopWorkerConnection::execute called")
        }
    }
}
