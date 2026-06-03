use crate::coordinator::MetricsStore;
use crate::coordinator::distributed::PreparedPlan;
use crate::stage::RemoteStage;
use crate::{
    DistributedConfig, NetworkBoundaryExt, Stage, TaskEstimator, TaskRoutingContext,
    WorkerDispatchRequest, get_distributed_worker_resolver, get_distributed_worker_transport,
};
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use rand::Rng;
use std::sync::Arc;

/// Prepares the distributed plan for execution, which implies:
/// 1. Perform some worker URL assignation, choosing either:
///    - The URLs set by the user with [crate::TaskEstimator::route_tasks].
///    - Randomly otherwise
/// 2. Handing each stage's sliced subplans to the registered [crate::WorkerTransport]'s
///    dispatcher, which owns the delivery to the assigned workers.
/// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
///    become nodes without children and traversing them will not go further down in.
/// 4. The dispatcher may spawn background per-worker work (plan delivery, work-unit feeds)
///    onto the query's `JoinSet`, which propagates failures to the query head.
pub(super) fn prepare_static_plan(
    base_plan: &Arc<dyn ExecutionPlan>,
    metrics: &ExecutionPlanMetricsSet,
    task_metrics: &Option<Arc<MetricsStore>>,
    ctx: &Arc<TaskContext>,
) -> Result<PreparedPlan> {
    let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;

    let available_urls = worker_resolver.get_urls()?;
    // One dispatcher per query: it carries per-query state (plan-send metrics, the query start
    // timestamp) across every stage's dispatch.
    let dispatcher = get_distributed_worker_transport(ctx.session_config()).dispatcher();

    let mut join_set = JoinSet::new();
    let prepared = Arc::clone(base_plan).transform_up(|plan| {
        // The following logic is just applied on network boundaries.
        let Some(plan) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };

        let Stage::Local(stage) = plan.input_stage() else {
            return exec_err!("Input stage from network boundary was not in Local state");
        };

        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        let task_estimator = &d_cfg.__private_task_estimator;

        let routed_urls = match task_estimator.route_tasks(&TaskRoutingContext {
            task_ctx: Arc::clone(ctx),
            plan: &stage.plan,
            task_count: stage.tasks,
            available_urls: &available_urls,
        }) {
            Ok(Some(routed_urls)) => routed_urls,
            // If the user has not defined custom routing with a `route_tasks` implementation, we
            // default to round-robin task assignation from a randomized starting point.
            Ok(None) => {
                if available_urls.is_empty() {
                    return exec_err!(
                        "the worker resolver returned no URLs; default routing needs at least \
                         one (a custom `route_tasks` implementation lifts this requirement)"
                    );
                }
                let start_idx = rand::rng().random_range(0..available_urls.len());
                (0..stage.tasks)
                    .map(|i| available_urls[(start_idx + i) % available_urls.len()].clone())
                    .collect()
            }
            Err(e) => return exec_err!("error routing tasks to workers: {e}"),
        };

        if routed_urls.len() != stage.tasks {
            return exec_err!(
                "number of tasks ({}) was not equal to number of urls ({}) at execution time",
                stage.tasks,
                routed_urls.len()
            );
        }

        // Hand each task's plan to its assigned worker. The transport owns the delivery (Flight
        // ships a `SetPlanRequest` over gRPC; an embedded transport routes by `target_task`), so
        // the coordinator no longer special-cases the gRPC send.
        dispatcher.dispatch(WorkerDispatchRequest {
            stage,
            routed_urls: &routed_urls,
            task_ctx: ctx,
            metrics,
            metrics_store: task_metrics.as_ref(),
            join_set: &mut join_set,
        })?;

        Ok(Transformed::yes(plan.with_input_stage(Stage::Remote(
            RemoteStage {
                query_id: stage.query_id,
                num: stage.num,
                workers: routed_urls,
            },
        ))?))
    })?;
    Ok(PreparedPlan {
        head_stage: prepared.data,
        join_set,
    })
}
