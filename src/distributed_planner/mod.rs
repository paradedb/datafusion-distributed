mod distributed_config;
mod distributed_query_planner;
mod inject_network_boundaries;
mod insert_broadcast;
mod network_boundary;
mod partial_reduce_below_network_shuffles;
mod prepare_network_boundaries;
mod push_fetch_into_network_coalesce;
mod session_state_builder_ext;
mod statistics;
mod task_estimator;
// TODO: not yet wired in — call `validate_distributed_stages` at the end of
// `create_physical_plan` (static path) and from `prepare_dynamic_plan` (adaptive path).
// Doing so turns the wrong-results shapes in `tests/multi_task_collect_join_repros.rs`
// into planning errors, so those repro tests must be flipped at the same time.
#[allow(dead_code)]
mod validate_stages;

pub use distributed_config::DistributedConfig;
pub(crate) use inject_network_boundaries::{
    InjectNetworkBoundaryContext, NetworkBoundaryBuilderResult, inject_network_boundaries,
};
pub(crate) use network_boundary::ProducerHead;
pub use network_boundary::{NetworkBoundary, NetworkBoundaryExt, PartitionRoute};
pub use session_state_builder_ext::SessionStateBuilderExt;
pub(crate) use statistics::calculate_cost;
pub(crate) use task_estimator::{CombinedTaskEstimator, set_distributed_task_estimator};
pub use task_estimator::{TaskCountAnnotation, TaskEstimation, TaskEstimator, TaskRoutingContext};
