use crate::common::OnceLockResult;
use crate::common::now_ns;
use crate::dynamic_filtering::ConsumerApplyPhase;
use crate::{
    BytesCounterMetric, MaxLatencyMetric, ProducerHead, TaskCompletedDynamicFilters, TaskMetrics,
    TaskOutcome,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{Count, Metric, MetricValue, MetricsSet, Time};
use futures::{Stream, StreamExt, TryStreamExt, future, stream};
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(crate) task_ctx: Arc<TaskContext>,
    pub(crate) base_plan: Arc<dyn ExecutionPlan>,
    pub(crate) final_plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
    /// Sender half of the metrics channel. `impl_coordinator_channel` takes this (via
    /// `Option::take`) when the coordinator channel reaches EOS, sending the collected metrics
    /// back to the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<TaskMetrics>>>>,
    /// Sender half of the completed dynamic-filter channel. It is absent when the user does not
    /// want to display dynamic filters.
    pub(super) completed_dynamic_filters_tx:
        Arc<std::sync::Mutex<Option<oneshot::Sender<TaskCompletedDynamicFilters>>>>,
    /// Metrics related to the execution of a task within a stage. This metrics, instead of being
    /// associated to a specific node, they are global to the task, like the time at which the plan
    /// was fed by the coordinator to the worker.
    pub(super) task_data_metrics: Arc<TaskDataMetrics>,
    /// Records how the task's partition streams end, to report the task's [TaskOutcome].
    pub(super) outcome_tracker: Arc<TaskOutcomeTracker>,
}

/// Records the first error and the number of finished partition streams of a task.
#[derive(Debug, Default)]
pub(super) struct TaskOutcomeTracker {
    first_error: OnceLock<Arc<DataFusionError>>,
    finished_partitions: AtomicUsize,
}

impl TaskOutcomeTracker {
    /// Wraps a partition stream to record its errors and its end. Errors are passed on as
    /// [DataFusionError::Shared], so that the tracker can keep a reference to them.
    pub(super) fn track(
        self: &Arc<Self>,
        stream: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
    ) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
        let on_error = Arc::clone(self);
        let on_end = Arc::clone(self);
        let end_marker = stream::once(async move {
            on_end.finished_partitions.fetch_add(1, Ordering::Release);
            None
        });
        stream
            .map_err(move |err| {
                let err = match err {
                    DataFusionError::Shared(err) => err,
                    err => Arc::new(err),
                };
                let _ = on_error.first_error.set(Arc::clone(&err));
                DataFusionError::Shared(err)
            })
            .chain(end_marker.filter_map(future::ready))
    }

    /// Returns the outcome of a task whose final plan has `partition_count` partitions.
    pub(super) fn outcome(&self, partition_count: usize) -> TaskOutcome<'_> {
        if let Some(err) = self.first_error.get() {
            TaskOutcome::Failed(err)
        } else if self.finished_partitions.load(Ordering::Acquire) == partition_count {
            TaskOutcome::Succeeded
        } else {
            TaskOutcome::Incomplete
        }
    }
}

pub(crate) const PLAN_ADDED_AT_METRIC: &str = "plan_added_at";
pub(crate) const PLAN_EXECUTED_AT_METRIC: &str = "plan_executed_at";
pub(crate) const PLAN_FINISHED_AT_METRIC: &str = "plan_finished_at";

/// Whether an incoming `ApplyDynamicFilter` arrived while task partitions were still running or
/// after all partitions of the task had already finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DynamicFilterArrival {
    ActiveExecution,
    AfterExecutionComplete,
}

#[derive(Debug, Default)]
pub(super) struct DynamicFilterProducerTaskMetrics {
    count: Count,
    bytes: BytesCounterMetric,
    encode_time: Time,
    last_at: MaxLatencyMetric,
}

#[derive(Debug, Default)]
pub(super) struct DynamicFilterConsumerTaskMetrics {
    count: Count,
    bytes: BytesCounterMetric,
    late_count: Count,
    phase_times: [Time; 3],
    last_at: MaxLatencyMetric,
}

#[derive(Debug)]
pub(super) struct TaskDataMetrics {
    pub(super) query_start_time_ns: usize,
    /// When the plan was set by the coordinator.
    pub(super) plan_added_at: MaxLatencyMetric,
    /// When the plan execution was triggered by the parent worker.
    pub(super) plan_executed_at: MaxLatencyMetric,
    /// When the execution stream finished.
    pub(super) plan_finished_at: MaxLatencyMetric,
    pub(super) df_producer: DynamicFilterProducerTaskMetrics,
    pub(super) df_consumer: DynamicFilterConsumerTaskMetrics,
}

impl TaskDataMetrics {
    pub(super) fn new(query_start_time_ns: usize) -> Self {
        let plan_added_at = MaxLatencyMetric::default();
        plan_added_at.add_duration(Duration::from_nanos(
            now_ns::<u64>().saturating_sub(query_start_time_ns as u64),
        ));
        Self {
            query_start_time_ns,
            plan_added_at,
            plan_finished_at: MaxLatencyMetric::default(),
            plan_executed_at: MaxLatencyMetric::default(),
            df_producer: DynamicFilterProducerTaskMetrics::default(),
            df_consumer: DynamicFilterConsumerTaskMetrics::default(),
        }
    }

    fn elapsed_since_query_start(&self) -> Duration {
        Duration::from_nanos(now_ns::<u64>().saturating_sub(self.query_start_time_ns as u64))
    }

    pub(super) fn mark_execution_started_once(&self) {
        if self.plan_executed_at.value() == 0 {
            self.plan_executed_at
                .add_duration(self.elapsed_since_query_start())
        }
    }

    pub(super) fn mark_execution_finished(&self) {
        self.plan_finished_at
            .add_duration(self.elapsed_since_query_start())
    }

    pub(super) fn record_df_produced(&self, encoded_bytes: usize, encode_elapsed: Duration) {
        self.df_producer.count.add(1);
        self.df_producer.bytes.add_bytes(encoded_bytes);
        self.df_producer.encode_time.add_duration(encode_elapsed);
        self.df_producer
            .last_at
            .add_duration(self.elapsed_since_query_start());
    }

    pub(super) fn record_df_consumed_arrival(
        &self,
        encoded_bytes: usize,
        arrival: DynamicFilterArrival,
    ) {
        self.df_consumer.count.add(1);
        self.df_consumer.bytes.add_bytes(encoded_bytes);
        if arrival == DynamicFilterArrival::AfterExecutionComplete {
            self.df_consumer.late_count.add(1);
        }
    }

    pub(super) fn record_df_consumed_phase(&self, phase: ConsumerApplyPhase, elapsed: Duration) {
        self.df_consumer.phase_times[phase as usize].add_duration(elapsed);
    }

    pub(super) fn mark_df_consumed_finished(&self) {
        self.df_consumer
            .last_at
            .add_duration(self.elapsed_since_query_start());
    }

    pub(super) fn to_metrics_set(&self) -> MetricsSet {
        let mut metrics_set = MetricsSet::new();
        metrics_set.push(max_latency_metric(
            PLAN_ADDED_AT_METRIC,
            &self.plan_added_at,
        ));
        metrics_set.push(max_latency_metric(
            PLAN_EXECUTED_AT_METRIC,
            &self.plan_executed_at,
        ));
        metrics_set.push(max_latency_metric(
            PLAN_FINISHED_AT_METRIC,
            &self.plan_finished_at,
        ));
        if self.df_producer.count.value() > 0 {
            metrics_set.push(count_metric("df_prod_count", &self.df_producer.count));
            metrics_set.push(BytesCounterMetric::new_metric(
                "df_prod_bytes",
                self.df_producer.bytes.value(),
            ));
            metrics_set.push(time_metric(
                "df_prod_encode_time",
                &self.df_producer.encode_time,
            ));
            metrics_set.push(max_latency_metric(
                "df_prod_last_at",
                &self.df_producer.last_at,
            ));
        }
        if self.df_consumer.count.value() > 0 {
            metrics_set.push(count_metric("df_cons_count", &self.df_consumer.count));
            metrics_set.push(count_metric(
                "df_cons_late_count",
                &self.df_consumer.late_count,
            ));
            metrics_set.push(BytesCounterMetric::new_metric(
                "df_cons_bytes",
                self.df_consumer.bytes.value(),
            ));
            metrics_set.push(time_metric(
                "df_cons_proto_decode_time",
                &self.df_consumer.phase_times[ConsumerApplyPhase::ProtoDecode as usize],
            ));
            metrics_set.push(time_metric(
                "df_cons_expr_build_time",
                &self.df_consumer.phase_times[ConsumerApplyPhase::ExprBuild as usize],
            ));
            metrics_set.push(time_metric(
                "df_cons_remap_update_time",
                &self.df_consumer.phase_times[ConsumerApplyPhase::RemapUpdate as usize],
            ));
            metrics_set.push(max_latency_metric(
                "df_cons_last_at",
                &self.df_consumer.last_at,
            ));
        }

        metrics_set
    }
}

fn count_metric(name: &'static str, count: &Count) -> Arc<Metric> {
    let cloned = Count::new();
    cloned.add(count.value());
    Arc::new(Metric::new(
        MetricValue::Count {
            name: Cow::Borrowed(name),
            count: cloned,
        },
        None,
    ))
}

fn time_metric(name: &'static str, time: &Time) -> Arc<Metric> {
    let cloned = Time::new();
    cloned.add_duration(Duration::from_nanos(time.value() as u64));
    Arc::new(Metric::new(
        MetricValue::Time {
            name: Cow::Borrowed(name),
            time: cloned,
        },
        None,
    ))
}

fn max_latency_metric(name: &'static str, value: &MaxLatencyMetric) -> Arc<Metric> {
    Arc::new(Metric::new(
        MetricValue::Custom {
            name: Cow::Borrowed(name),
            value: Arc::new(MaxLatencyMetric::from_nanos(value.value())),
        },
        None,
    ))
}

impl TaskData {
    pub(crate) fn plan(&self, producer_head: ProducerHead) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self.final_plan.get_or_init(|| {
            let producer_head =
                producer_head.ensure_decoded(self.base_plan.schema(), &self.task_ctx)?;

            Ok(producer_head.insert(Arc::clone(&self.base_plan))?)
        });
        match result {
            Ok(plan) => Ok(Arc::clone(plan)),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }

    /// Returns whether all output partitions of this task have already terminated.
    pub(super) fn is_execution_complete(&self) -> bool {
        let Some(Ok(plan)) = self.final_plan.get() else {
            return false;
        };
        let partition_count = plan.properties().partitioning.partition_count();
        partition_count > 0
            && !matches!(
                self.outcome_tracker.outcome(partition_count),
                TaskOutcome::Incomplete
            )
    }
}
