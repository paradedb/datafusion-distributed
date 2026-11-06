#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::JoinType;
    use datafusion::common::NullEquality;
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execute_stream,
    };
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::mock_exec::MockExec;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        BroadcastExec, DefaultSessionBuilder, DistributedExec, DistributedExt,
        DistributedMetricsFormat, WorkerQueryContext, assert_snapshot, display_plan_ascii,
        rewrite_distributed_plan_with_metrics,
    };
    use datafusion_proto::physical_plan::{
        PhysicalExtensionCodec, PhysicalProtoConverterExtension,
    };
    use datafusion_proto::protobuf::proto_error;
    use futures::TryStreamExt;
    use prost::Message;
    use std::fmt::Formatter;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    // This test proves that execution nodes do not get early dropped in the Worker when all the
    // partitions start being consumed.
    //
    // It uses a StatefulPassThroughExec custom node whose execution depends on it not being dropped.
    // The node spawns a background task that collects data from its child DataSourceExec.
    // If the Worker drops the node before the stream ends, this test will fail.
    #[tokio::test]
    async fn stateful_execution_plan() -> Result<(), Box<dyn std::error::Error>> {
        async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
            Ok(ctx
                .builder
                .with_distributed_user_codec(PassThroughExecCodec)
                .build())
        }

        let (mut ctx_distributed, _guard, _) = start_localhost_context(3, build_state).await;
        ctx_distributed.set_distributed_user_codec(PassThroughExecCodec);
        register_parquet_tables(&ctx_distributed).await?;

        let query = r#"SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 20.0 ORDER BY "MinTemp" DESC"#;

        let df_distributed = ctx_distributed.sql(query).await?;
        let plan = df_distributed.create_physical_plan().await?;

        let transformed = plan.transform_up(|plan| {
            if plan.children().is_empty() {
                return Ok(Transformed::yes(Arc::new(StatefulPassThroughExec::new(
                    plan,
                ))));
            }
            Ok(Transformed::no(plan))
        })?;
        let plan = transformed.data;

        let plan_str = display_plan_ascii(plan.as_ref(), false);

        assert_snapshot!(plan_str,
            @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=9
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   FilterExec: MinTemp@0 > 20
          │     StatefulPassThroughExec
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 20, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 20, required_guarantees=[]
          │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 20, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 20, required_guarantees=[]
          │         t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 20, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 20, required_guarantees=[]
          └──────────────────────────────────────────────────
        ",
        );

        let batches_distributed = pretty_format_batches(
            &execute_stream(plan, ctx_distributed.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        // Verify that the stateful execution completes successfully
        assert!(!batches_distributed.to_string().is_empty());

        Ok(())
    }

    // Verifies that `DistributedExec` drops Stage 0 (`head_stage`) as soon as the output stream
    // finishes, releasing Stage 0 `HashJoinExec` `CollectLeft` build-side `MemoryReservation`s
    // while keeping snapshotted Stage 0 metrics available for `rewrite_distributed_plan_with_metrics`.
    #[tokio::test]
    async fn head_stage_releases_hash_join_memory_on_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = start_in_memory_context(1, DefaultSessionBuilder).await;
        let (schema, batch) = id_batch(1024)?;

        let build_input = Arc::new(MockExec::new(vec![Ok(batch.clone())], Arc::clone(&schema)));
        let probe_input = Arc::new(MockExec::new(vec![Ok(batch)], Arc::clone(&schema)));
        let populated_join = collect_left_id_join(build_input, probe_input)?;
        let dist_plan: Arc<dyn ExecutionPlan> =
            Arc::new(DistributedExec::new(populated_join).with_metrics_collection(true));

        let task_ctx = ctx.task_ctx();
        let results = execute_stream(Arc::clone(&dist_plan), Arc::clone(&task_ctx))?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(results.iter().map(|b| b.num_rows()).sum::<usize>(), 1024);
        assert_eq!(
            task_ctx.memory_pool().reserved(),
            0,
            "Stage 0 HashJoinExec MemoryReservation must be released when stream completes"
        );

        let rewritten = rewrite_distributed_plan_with_metrics(
            Arc::clone(&dist_plan),
            DistributedMetricsFormat::Aggregated,
        )
        .await?;
        assert!(rewritten.children()[0].metrics().is_some());

        Ok(())
    }

    // Verifies that when an outer Stage 0 `CollectLeft` `HashJoinExec` short-circuits on an empty
    // build side (0 rows) without polling its right input (an inner `CollectLeft` `HashJoinExec`
    // over `BroadcastExec` and `StatefulPassThroughExec`), completing the `DistributedExec` stream
    // leaves `base_plan` unmutated and immediately drops `head_stage`'s `OnceAsync` and
    // `BroadcastExec` queues so the unpolled inner build task exits.
    #[tokio::test]
    async fn head_stage_drops_unpolled_short_circuited_streams_on_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let ctx = start_in_memory_context(1, DefaultSessionBuilder).await;
        let (schema, batch) = id_batch(1024)?;

        let empty_outer_left: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::new(BroadcastExec::new(
                Arc::new(EmptyExec::new(Arc::clone(&schema))),
                1,
            ))));
        // Emit 2 batches through a capacity-1 channel in `StatefulPassThroughExec` so the
        // spawned producer task blocks on the second batch until `rx` is dropped.
        let inner_build_source = Arc::new(StatefulPassThroughExec::new(Arc::new(
            MockExec::new(
                vec![Ok(batch.clone()), Ok(batch.clone())],
                Arc::clone(&schema),
            )
            .with_use_task(true),
        )));
        let inner_left: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(Arc::new(
            BroadcastExec::new(Arc::clone(&inner_build_source) as Arc<dyn ExecutionPlan>, 1),
        )));
        let inner_right: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new(vec![Ok(batch)], Arc::clone(&schema)));
        let inner_join = collect_left_id_join(inner_left, inner_right)?;
        let outer_join = collect_left_id_join(empty_outer_left, inner_join)?;

        let short_circuit_plan: Arc<dyn ExecutionPlan> =
            Arc::new(DistributedExec::new(outer_join).with_metrics_collection(true));
        let short_circuit_results =
            execute_stream(Arc::clone(&short_circuit_plan), ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?;
        assert!(short_circuit_results.is_empty());

        // `base_plan`'s node must never have been executed directly, while the isolated
        // `head_stage` copy must have spawned its background task and then dropped its receiver
        // on completion so the task exits immediately while `short_circuit_plan` is still alive.
        assert!(!inner_build_source.was_executed());
        let handles = inner_build_source.take_spawned_tasks();
        assert_eq!(handles.len(), 1);
        for handle in handles {
            tokio::time::timeout(Duration::from_secs(5), handle).await??;
        }

        let rewritten = rewrite_distributed_plan_with_metrics(
            Arc::clone(&short_circuit_plan),
            DistributedMetricsFormat::PerTask,
        )
        .await?;
        assert!(rewritten.children()[0].metrics().is_some());

        Ok(())
    }

    fn id_batch(num_rows: i32) -> Result<(SchemaRef, RecordBatch), DataFusionError> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(
                (0..num_rows).collect::<Vec<_>>(),
            ))],
        )?;
        Ok((schema, batch))
    }

    fn collect_left_id_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let join_on = vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
        )];
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            join_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }

    /// A stateful execution plan that wraps a child and spawns a background task
    /// to manage the stream collection. This tests that the node doesn't get
    /// dropped prematurely during distributed execution, and that isolated stage
    /// copies release their streams when the stage finishes.
    #[derive(Debug)]
    pub struct StatefulPassThroughExec {
        plan_properties: Arc<PlanProperties>,
        child: Arc<dyn ExecutionPlan>,
        executed: AtomicBool,
        spawned_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    }

    impl StatefulPassThroughExec {
        fn new(child: Arc<dyn ExecutionPlan>) -> Self {
            Self::with_shared_tasks(child, Arc::new(Mutex::new(Vec::new())))
        }

        fn with_shared_tasks(
            child: Arc<dyn ExecutionPlan>,
            spawned_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
        ) -> Self {
            let plan_properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                child.output_partitioning().clone(),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                plan_properties,
                child,
                executed: AtomicBool::new(false),
                spawned_tasks,
            }
        }

        fn was_executed(&self) -> bool {
            self.executed.load(Ordering::SeqCst)
        }

        fn take_spawned_tasks(&self) -> Vec<JoinHandle<()>> {
            std::mem::take(&mut *self.spawned_tasks.lock().unwrap())
        }
    }

    impl DisplayAs for StatefulPassThroughExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "StatefulPassThroughExec")
        }
    }

    impl ExecutionPlan for StatefulPassThroughExec {
        fn name(&self) -> &str {
            "StatefulPassThroughExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.plan_properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.child]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn PhysicalExpr>,
            ) -> datafusion::common::Result<TreeNodeRecursion>,
        ) -> datafusion::common::Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(StatefulPassThroughExec::new(children[0].clone())))
        }

        fn reset_state(self: Arc<Self>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(StatefulPassThroughExec::with_shared_tasks(
                Arc::clone(&self.child),
                Arc::clone(&self.spawned_tasks),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            self.executed.store(true, Ordering::SeqCst);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            // Spawn a background task to demonstrate stateful behavior
            let mut stream = self.child.execute(partition, context)?;

            #[allow(clippy::disallowed_methods)]
            let handle = tokio::spawn(async move {
                // Simulate some background work
                while let Some(batch) = stream.next().await {
                    if tx.send(batch).await.is_err() {
                        return;
                    }
                }
            });
            self.spawned_tasks.lock().unwrap().push(handle);

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                ReceiverStream::new(rx),
            )))
        }
    }

    #[derive(Debug)]
    struct PassThroughExecCodec;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct PassThroughExecProto {
        // Empty - we'll handle the child through normal codec mechanisms
    }

    impl PhysicalExtensionCodec for PassThroughExecCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let _node =
                PassThroughExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;

            if inputs.len() != 1 {
                return Err(proto_error(format!(
                    "StatefulPassThroughExec expects exactly one child, got {}",
                    inputs.len()
                )));
            }

            Ok(Arc::new(StatefulPassThroughExec::new(inputs[0].clone())))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> datafusion::common::Result<()> {
            let Some(_plan) = node.downcast_ref::<StatefulPassThroughExec>() else {
                return Err(proto_error(format!(
                    "Expected plan to be of type StatefulPassThroughExec, but was {}",
                    node.name()
                )));
            };
            PassThroughExecProto {}
                .encode(buf)
                .map_err(|err| proto_error(format!("{err}")))
        }
    }
}
