use crate::dynamic_filtering::{discover_dynamic_filter_consumers, dynamic_filter_producer_schema};
use crate::{
    ApplyDynamicFilter, BytesCounterMetric, BytesMetricExt, CoordinatorToWorkerMsg, MaybeEncoded,
    ProducedDynamicFilter, TaskKey,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::instant::Instant;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, exec_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{Count, MetricValue, Time};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{
    PhysicalBinaryExprNode, PhysicalDynamicFilterNode, PhysicalExprNode,
};
use prost::Message;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DynamicFilterMergePolicy {
    /// Wait for every planned producer task to report a complete dynamic filter, then
    /// union and forward the filter. Used for partitioned joins.
    UnionWhenAllComplete,
    /// Wait for any producer to report a complete dynamic filter and forward it.
    /// Used for collect left joins.
    ForwardFirstComplete,
    /// Intersect all updates from producers and immediately forward them. Used
    /// for TopK and Min/Max aggregate dynamic filters where intersecting updates
    /// is safe.
    IntersectOnUpdate,
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    // Producer and consumer tasks for a dynamic filter.
    //
    // Note that it is not guaranteed that every task within a stage produces / consumes dynamic filters. For
    // example, a distributed union may prevent a dynamic filter from appearing in all tasks. So, we
    // store task keys rather than stage ids.
    //
    /// Registered producers and their latest accepted snapshots, if any.
    pub(super) producers: HashMap<TaskKey, Option<PhysicalDynamicFilterNode>>,
    pub(super) consumer_tasks: HashSet<TaskKey>,

    /// Populated upon task registration when a producer is found.
    ///
    /// The behavior to take when an update arrives from a producer.
    merge_mode: Option<DynamicFilterMergePolicy>,
    /// Schema of the original producer arguments, shared by all producer tasks.
    pub(super) producer_schema: Option<SchemaRef>,

    /// Populated when an update is ready to be sent to consumers.
    ///
    /// Full dynamic filter containing the merged predicate and its completion state.
    pub(super) merged: Option<PhysicalDynamicFilterNode>,
    /// Immutable snapshot shared by local and remote delivery, encoded once per change.
    merged_bytes: Option<Vec<u8>>,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
    /// Track which stages have registered all of their tasks.
    pub(super) sealed_stages: HashSet<usize>,
    task_senders: HashMap<TaskKey, UnboundedSender<CoordinatorToWorkerMsg>>,
    delivered: HashSet<(u64, TaskKey)>,
}

struct LeaderRxMetrics {
    updates_received: Count,
    rx_bytes: BytesCounterMetric,
    decode_time: Time,
    updates_ignored: Count,
}

struct LeaderMergeMetrics {
    count: Count,
    time: Time,
    bytes: BytesCounterMetric,
}

struct LeaderTxMetrics {
    count: Count,
    bytes: BytesCounterMetric,
}

fn build_global_time_metric(metrics: &ExecutionPlanMetricsSet, name: &'static str) -> Time {
    let time = Time::new();
    MetricBuilder::new(metrics).build(MetricValue::Time {
        name: Cow::Borrowed(name),
        time: time.clone(),
    });
    time
}

/// Query-scoped hub for distributed dynamic filtering.
///
/// It stores the locations of dynamic filters and their runtime state. Informs the coordinator
/// - where dynamic filter updates are coming from
/// - how/if dynamic filter updates should be merged
/// - where dynamic filter updates should be forwarded
pub(crate) struct DynamicFilterRegistry {
    pub(super) state: Mutex<DynamicFilterRegistryState>,
    rx_metrics: LeaderRxMetrics,
    merge_metrics: LeaderMergeMetrics,
    tx_metrics: LeaderTxMetrics,
    query_finished: CancellationToken,
}

impl DynamicFilterRegistry {
    pub(crate) fn new(
        metrics: &ExecutionPlanMetricsSet,
        query_finished: CancellationToken,
    ) -> Self {
        Self {
            state: Mutex::new(DynamicFilterRegistryState::default()),
            rx_metrics: LeaderRxMetrics {
                updates_received: MetricBuilder::new(metrics)
                    .global_counter("dynamic_filter_updates_received"),
                rx_bytes: MetricBuilder::new(metrics).bytes_counter("df_leader_rx_bytes"),
                decode_time: build_global_time_metric(metrics, "df_leader_decode_time"),
                updates_ignored: MetricBuilder::new(metrics)
                    .global_counter("df_leader_updates_ignored"),
            },
            merge_metrics: LeaderMergeMetrics {
                count: MetricBuilder::new(metrics).global_counter("df_leader_merge_count"),
                time: build_global_time_metric(metrics, "df_leader_merge_time"),
                bytes: MetricBuilder::new(metrics).bytes_counter("df_leader_merged_bytes"),
            },
            tx_metrics: LeaderTxMetrics {
                count: MetricBuilder::new(metrics).global_counter("df_leader_tx_count"),
                bytes: MetricBuilder::new(metrics).bytes_counter("df_leader_tx_bytes"),
            },
            query_finished,
        }
    }

    pub(crate) fn record_update_received(&self) {
        self.rx_metrics.updates_received.add(1);
    }

    /// Adds any dynamic filter producers and consumers found in `plan` to the registry.
    pub(crate) fn register_task(
        &self,
        task_specialized_plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
    ) -> Result<()> {
        let mut producers = vec![];

        task_specialized_plan.apply(|node| {
            // `CollectLeft` joins broadcast an equivalent build side to every producer task,
            // so we can forward the first completed dynamic filter.
            //
            // TopK and MIN/MAX bounds are independently useful and incremental, so we can use
            // all updates. Single-child custom TopK operators (with `fetch()` set) have the
            // same incremental intersection semantics as `SortExec`.
            //
            // Any remaining dynamic filter should wait for completion and should be merged
            // at the coordinator before being applied.
            let merge_mode = if node
                .downcast_ref::<HashJoinExec>()
                .is_some_and(|join| matches!(join.partition_mode(), PartitionMode::CollectLeft))
            {
                DynamicFilterMergePolicy::ForwardFirstComplete
            } else if node.is::<SortExec>()
                || node.is::<AggregateExec>()
                || (node.children().len() == 1 && node.fetch().is_some())
            {
                DynamicFilterMergePolicy::IntersectOnUpdate
            } else {
                DynamicFilterMergePolicy::UnionWhenAllComplete
            };
            let produced_ids: HashSet<_> = node
                .dynamic_expressions_produced()
                .into_iter()
                .filter_map(|expression| {
                    expression
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                        .map(|_| expression.expression_id())
                })
                .map(|id| match id {
                    Some(id) => Ok(id),
                    None => {
                        internal_err!("DynamicFilterPhysicalExpr did not have an expression ID")
                    }
                })
                .collect::<Result<_>>()?;
            if !produced_ids.is_empty() {
                let schema = dynamic_filter_producer_schema(node.as_ref())?;
                producers.extend(
                    produced_ids
                        .iter()
                        .map(|id| (*id, merge_mode, Arc::clone(&schema))),
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        // We can safely ignore anchors because they are not evaluated by network boundaries. This
        // means they do not need updates forwarded to them.
        let consumers = discover_dynamic_filter_consumers(task_specialized_plan)?.consumers;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for (id, merge_mode, producer_schema) in producers {
            let filter = state.filters.entry(id).or_default();
            if let Some(existing) = &filter.producer_schema
                && existing != &producer_schema
            {
                return internal_err!("Dynamic filter {id} has conflicting producer schemas");
            }
            filter.producer_schema.get_or_insert(producer_schema);
            filter.merge_mode = Some(match filter.merge_mode {
                Some(existing) if existing != merge_mode => {
                    return internal_err!(
                        "Dynamic filter {id} has conflicting merge modes: \
                         {existing:?} and {merge_mode:?}"
                    );
                }
                Some(existing) => existing,
                None => merge_mode,
            });
            filter.producers.entry(task_key).or_insert(None);
        }
        for consumer in consumers {
            state
                .filters
                .entry(consumer.id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        Ok(())
    }

    pub(crate) fn register_sender(
        &self,
        task_key: TaskKey,
        sender: UnboundedSender<CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.task_senders.insert(task_key, sender);
        state.delivered.retain(|(_, task)| *task != task_key);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            self.dispatch(&mut state, id)?;
        }
        Ok(())
    }

    /// Mark that a stage has registered all of its tasks.
    pub(crate) fn seal_stage(&self, stage_id: usize) -> Result<()> {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.sealed_stages.insert(stage_id);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            if self.merge(&mut state, id) {
                self.dispatch(&mut state, id)?;
            }
        }
        Ok(())
    }

    /// Records a producer's latest dynamic-filter update and recomputes the merged filter.
    pub(crate) fn record_dynamic_filter_update(
        &self,
        task_key: TaskKey,
        report: ProducedDynamicFilter,
        task_ctx: &TaskContext,
    ) -> Result<()> {
        self.record_update_received();
        if let MaybeEncoded::Encoded(bytes) = &report.expression {
            self.rx_metrics.rx_bytes.add_bytes(bytes.len());
        }

        // Fast-reject updates for already-completed producers or already-completed merged filters
        // (e.g. subsequent `CollectLeft` workers under `ForwardFirstComplete`) before decoding a
        // potentially large protobuf payload on the coordinator thread.
        {
            let state = self.state.lock().expect("dynamic filter registry poisoned");
            let Some(filter) = state.filters.get(&report.expression_id) else {
                return exec_err!(
                    "Received update for unregistered dynamic filter {} from task {task_key:?}",
                    report.expression_id
                );
            };
            let Some(previous) = filter.producers.get(&task_key) else {
                return exec_err!(
                    "Task {task_key:?} is not a registered producer of dynamic filter {}",
                    report.expression_id
                );
            };
            if filter
                .merged
                .as_ref()
                .is_some_and(|merged| merged.is_complete)
                || previous
                    .as_ref()
                    .is_some_and(|previous| previous.is_complete)
            {
                self.rx_metrics.updates_ignored.add(1);
                return Ok(());
            }
        }

        let decode_start = Instant::now();
        let expression = report.expression.to_proto(task_ctx)?;
        self.rx_metrics
            .decode_time
            .add_duration(decode_start.elapsed());
        if expression.expr_id != Some(report.expression_id) {
            return exec_err!(
                "Dynamic filter {} from task {task_key:?} has mismatched expression ID {:?}",
                report.expression_id,
                expression.expr_id
            );
        }
        let Some(ExprType::DynamicFilter(dynamic_filter)) = expression.expr_type else {
            return exec_err!(
                "Expected dynamic filter expression for filter {} from task {task_key:?}",
                report.expression_id
            );
        };
        if dynamic_filter.inner_expr.is_none() {
            return exec_err!(
                "Missing inner expression for dynamic filter {} from task {task_key:?}",
                report.expression_id
            );
        }

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        let Some(filter) = state.filters.get_mut(&report.expression_id) else {
            return exec_err!(
                "Received update for unregistered dynamic filter {} from task {task_key:?}",
                report.expression_id
            );
        };
        if filter
            .merged
            .as_ref()
            .is_some_and(|merged| merged.is_complete)
        {
            self.rx_metrics.updates_ignored.add(1);
            return Ok(());
        }
        let Some(previous) = filter.producers.get_mut(&task_key) else {
            return exec_err!(
                "Task {task_key:?} is not a registered producer of dynamic filter {}",
                report.expression_id
            );
        };
        if (filter.merge_mode != Some(DynamicFilterMergePolicy::IntersectOnUpdate)
            && !dynamic_filter.is_complete)
            || previous
                .as_ref()
                .is_some_and(|previous| previous.is_complete || previous == dynamic_filter.as_ref())
        {
            self.rx_metrics.updates_ignored.add(1);
            return Ok(());
        }
        *previous = Some(*dynamic_filter);
        if self.merge(&mut state, report.expression_id) {
            self.dispatch(&mut state, report.expression_id)?;
        }
        Ok(())
    }

    /// Merges partial dynamic filters together for the provided dynamic filter
    /// id only if there are enough updates present.
    fn merge(&self, state: &mut DynamicFilterRegistryState, id: u64) -> bool {
        let Some(filter) = state.filters.get_mut(&id) else {
            return false;
        };
        if filter
            .merged
            .as_ref()
            .is_some_and(|filter| filter.is_complete)
        {
            return false;
        }
        let Some(mode) = filter.merge_mode else {
            return false;
        };
        let all_complete = !filter.producers.is_empty()
            && filter.producers.iter().all(|(task, report)| {
                state.sealed_stages.contains(&task.stage_id)
                    && report.as_ref().is_some_and(|report| report.is_complete)
            });
        if mode == DynamicFilterMergePolicy::UnionWhenAllComplete && !all_complete {
            return false;
        }

        let merge_start = Instant::now();
        let is_complete = mode == DynamicFilterMergePolicy::ForwardFirstComplete || all_complete;
        let mut reports: Vec<_> = filter
            .producers
            .iter_mut()
            .filter_map(|(task, report)| report.as_mut().map(|report| (*task, report)))
            .collect();
        reports.sort_unstable_by_key(|(key, _)| (key.stage_id, key.task_number));
        if mode == DynamicFilterMergePolicy::ForwardFirstComplete {
            reports.truncate(1);
        }
        let Some((_, first_report)) = reports.first() else {
            return false;
        };
        let template_children = first_report.children.clone();
        let template_remapped_children = first_report.remapped_children.clone();
        // Once the merged filter is complete (`ForwardFirstComplete` or `all_complete`), no
        // subsequent merge will ever run for this filter ID, so move `inner_expr` out of each
        // producer report instead of deep-cloning every producer's AST.
        let predicates: Vec<PhysicalExprNode> = if is_complete {
            reports
                .into_iter()
                .filter_map(|(_, report)| report.inner_expr.take().map(|expr| *expr))
                .collect()
        } else {
            reports
                .into_iter()
                .filter_map(|(_, report)| report.inner_expr.as_deref().cloned())
                .collect()
        };
        let inner_expr = merge_predicates(predicates, mode).map(Box::new);
        let previous = filter.merged.as_ref();
        if previous.is_some_and(|previous| {
            previous.inner_expr == inner_expr && previous.is_complete == is_complete
        }) {
            return false;
        }
        // Use a synthetic generation number for the merged filter. Each partial update has its own
        // generation which is not useful here.
        let generation = previous.map_or(0, |previous| previous.generation + 1);
        let merged = PhysicalDynamicFilterNode {
            children: template_children,
            remapped_children: template_remapped_children,
            generation,
            inner_expr,
            is_complete,
        };
        let node = PhysicalExprNode {
            expr_id: Some(id),
            expr_type: Some(ExprType::DynamicFilter(Box::new(merged))),
        };
        let encoded = node.encode_to_vec();
        let Some(ExprType::DynamicFilter(merged)) = node.expr_type else {
            unreachable!("just constructed ExprType::DynamicFilter");
        };
        self.merge_metrics.count.add(1);
        self.merge_metrics.time.add_duration(merge_start.elapsed());
        self.merge_metrics.bytes.add_bytes(encoded.len());
        filter.merged_bytes = Some(encoded);
        filter.merged = Some(*merged);
        state
            .delivered
            .retain(|(expression_id, _)| *expression_id != id);
        true
    }

    // Merge and enqueue under the same lock so successive snapshots cannot overtake each other.
    // Unbounded channel sends do not wait for the network or the receiving worker.
    fn dispatch(&self, state: &mut DynamicFilterRegistryState, id: u64) -> Result<()> {
        if self.query_finished.is_cancelled() {
            return Ok(());
        }
        let Some(filter) = state.filters.get(&id) else {
            return Ok(());
        };
        let Some(expression) = &filter.merged_bytes else {
            return Ok(());
        };
        let Some(producer_schema) = &filter.producer_schema else {
            return internal_err!(
                "Dynamic filter {id} has a merged predicate but no producer schema"
            );
        };
        for &task_key in &filter.consumer_tasks {
            // A task-local consumer is already updated directly by its producer.
            if filter.producers.contains_key(&task_key) || state.delivered.contains(&(id, task_key))
            {
                continue;
            }
            let Some(sender) = state.task_senders.get(&task_key) else {
                continue;
            };
            let encoded_len = expression.len();
            let update = CoordinatorToWorkerMsg::ApplyDynamicFilter(Box::new(ApplyDynamicFilter {
                expression_id: id,
                expression: MaybeEncoded::Encoded(expression.clone()),
                producer_schema: Arc::clone(producer_schema),
            }));
            if sender.send(update).is_err() {
                // Closing the channel is expected only once query shutdown has started.
                if !self.query_finished.is_cancelled() {
                    return exec_err!(
                        "Failed to send dynamic filter {id} to task {task_key:?}: channel closed"
                    );
                }
                return Ok(());
            }
            self.tx_metrics.count.add(1);
            self.tx_metrics.bytes.add_bytes(encoded_len);
            state.delivered.insert((id, task_key));
        }
        Ok(())
    }
}

/// Merges [`PhysicalExprNode`] together. Merge behavior (ex. AND vs OR) is determined
/// by the provided [`DynamicFilterMergePolicy`].
fn merge_predicates(
    mut predicates: Vec<PhysicalExprNode>,
    mode: DynamicFilterMergePolicy,
) -> Option<PhysicalExprNode> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::BinaryExpr(Box::new(PhysicalBinaryExprNode {
                l: None,
                r: None,
                op: if mode == DynamicFilterMergePolicy::IntersectOnUpdate {
                    "And"
                } else {
                    "Or"
                }
                .to_owned(),
                operands: predicates,
            }))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic_filtering::apply_dynamic_filter_update;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, apply_expression_roots,
    };
    use tokio::sync::mpsc::unbounded_channel;
    use uuid::Uuid;

    #[derive(Debug)]
    struct CustomSingleChildProducerExec {
        input: Arc<dyn ExecutionPlan>,
        dynamic_filter: Arc<dyn PhysicalExpr>,
        fetch: Option<usize>,
    }

    impl DisplayAs for CustomSingleChildProducerExec {
        fn fmt_as(
            &self,
            _: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "CustomSingleChildProducerExec")
        }
    }

    impl ExecutionPlan for CustomSingleChildProducerExec {
        fn name(&self) -> &str {
            "CustomSingleChildProducerExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            apply_expression_roots([&self.dynamic_filter], f)
        }

        fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
            vec![Arc::clone(&self.dynamic_filter)]
        }

        fn fetch(&self) -> Option<usize> {
            self.fetch
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: children.swap_remove(0),
                dynamic_filter: Arc::clone(&self.dynamic_filter),
                fetch: self.fetch,
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.input.execute(partition, context)
        }
    }

    #[derive(Debug)]
    struct CustomConsumerExec {
        input: Arc<dyn ExecutionPlan>,
        dynamic_filter: Arc<dyn PhysicalExpr>,
    }

    impl DisplayAs for CustomConsumerExec {
        fn fmt_as(
            &self,
            _: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "CustomConsumerExec")
        }
    }

    impl ExecutionPlan for CustomConsumerExec {
        fn name(&self) -> &str {
            "CustomConsumerExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            apply_expression_roots([&self.dynamic_filter], f)
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: children.swap_remove(0),
                dynamic_filter: Arc::clone(&self.dynamic_filter),
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.input.execute(partition, context)
        }
    }

    #[test]
    fn custom_single_child_producer_merge_policy_depends_on_fetch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let col_k: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let task_ctx = Arc::new(TaskContext::default());
        let query_id = Uuid::new_v4();

        for (fetch, expect_early_intersect, expected_op) in [
            (Some(10), true, "k@0 < 20 AND k@0 < 10"),
            (None, false, "k@0 < 20 OR k@0 < 10"),
        ] {
            let metrics = ExecutionPlanMetricsSet::new();
            let registry = DynamicFilterRegistry::new(&metrics, CancellationToken::new());

            let base_df = Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::clone(&col_k)],
                lit(true),
            ));
            let expr_id = base_df.expression_id().unwrap();

            // Consumer in stage 1.
            let consumer_df = Arc::clone(&base_df);
            let consumer_plan: Arc<dyn ExecutionPlan> = Arc::new(CustomConsumerExec {
                input: Arc::new(EmptyExec::new(Arc::clone(&schema))),
                dynamic_filter: Arc::clone(&consumer_df) as Arc<dyn PhysicalExpr>,
            });
            let consumer_key = TaskKey {
                query_id,
                stage_id: 1,
                task_number: 0,
            };
            let (consumer_tx, mut consumer_rx) = unbounded_channel();
            registry.register_task(&consumer_plan, consumer_key)?;
            registry.register_sender(consumer_key, consumer_tx)?;
            registry.seal_stage(1)?;

            // Two producer tasks in stage 2 with distinct DynamicFilterPhysicalExpr instances
            // sharing `expr_id` (via proto roundtrip).
            let mut producer_dfs = Vec::new();
            for task_number in 0..2 {
                let producer_df_expr =
                    MaybeEncoded::Decoded(Arc::clone(&base_df) as Arc<dyn PhysicalExpr>)
                        .encode(&task_ctx)
                        .and_then(|bytes| {
                            crate::codec::decode_physical_expr(
                                &PhysicalExprNode::decode(bytes.as_slice()).unwrap(),
                                schema.as_ref(),
                                &task_ctx,
                            )
                        })?;
                let producer_df = Arc::downcast::<DynamicFilterPhysicalExpr>(producer_df_expr)
                    .expect("DynamicFilterPhysicalExpr");
                let producer_plan: Arc<dyn ExecutionPlan> =
                    Arc::new(CustomSingleChildProducerExec {
                        input: Arc::new(EmptyExec::new(Arc::clone(&schema))),
                        dynamic_filter: Arc::clone(&producer_df) as Arc<dyn PhysicalExpr>,
                        fetch,
                    });
                let key = TaskKey {
                    query_id,
                    stage_id: 2,
                    task_number,
                };
                let (tx, _rx) = unbounded_channel();
                registry.register_task(&producer_plan, key)?;
                registry.register_sender(key, tx)?;
                producer_dfs.push((key, producer_df));
            }
            registry.seal_stage(2)?;

            // Incomplete update from producer 0 (`k < 20`).
            let p0_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::clone(&col_k),
                Operator::Lt,
                lit(20i32),
            ));
            producer_dfs[0].1.update(p0_bound)?;
            registry.record_dynamic_filter_update(
                producer_dfs[0].0,
                ProducedDynamicFilter {
                    expression_id: expr_id,
                    expression: MaybeEncoded::Decoded(
                        Arc::clone(&producer_dfs[0].1) as Arc<dyn PhysicalExpr>
                    ),
                },
                &task_ctx,
            )?;

            if expect_early_intersect {
                let Some(CoordinatorToWorkerMsg::ApplyDynamicFilter(apply)) =
                    consumer_rx.try_recv().ok()
                else {
                    panic!("expected immediate incremental ApplyDynamicFilter for fetch={fetch:?}");
                };
                apply_dynamic_filter_update(
                    &consumer_df,
                    &apply.expression,
                    apply.producer_schema.as_ref(),
                    &task_ctx,
                )?;
                assert_eq!(consumer_df.current()?.to_string(), "k@0 < 20");
            } else {
                assert!(
                    consumer_rx.try_recv().is_err(),
                    "non-fetch producer must not forward incomplete updates"
                );
            }

            // Complete both producers (`k < 20` and `k < 10`) and verify merged predicate.
            producer_dfs[0].1.mark_complete();
            registry.record_dynamic_filter_update(
                producer_dfs[0].0,
                ProducedDynamicFilter {
                    expression_id: expr_id,
                    expression: MaybeEncoded::Decoded(
                        Arc::clone(&producer_dfs[0].1) as Arc<dyn PhysicalExpr>
                    ),
                },
                &task_ctx,
            )?;

            let p1_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::clone(&col_k),
                Operator::Lt,
                lit(10i32),
            ));
            producer_dfs[1].1.update(p1_bound)?;
            producer_dfs[1].1.mark_complete();
            registry.record_dynamic_filter_update(
                producer_dfs[1].0,
                ProducedDynamicFilter {
                    expression_id: expr_id,
                    expression: MaybeEncoded::Decoded(
                        Arc::clone(&producer_dfs[1].1) as Arc<dyn PhysicalExpr>
                    ),
                },
                &task_ctx,
            )?;

            let mut last_apply = None;
            while let Ok(CoordinatorToWorkerMsg::ApplyDynamicFilter(apply)) = consumer_rx.try_recv()
            {
                last_apply = Some(apply);
            }
            let apply = last_apply.expect("expected final ApplyDynamicFilter");
            apply_dynamic_filter_update(
                &consumer_df,
                &apply.expression,
                apply.producer_schema.as_ref(),
                &task_ctx,
            )?;
            assert_eq!(consumer_df.current()?.to_string(), expected_op);
        }
        Ok(())
    }
}
