use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uuid::Uuid;

/// Constructs the per-stage network-boundary operators that
/// [`distribute_plan`](super::distribute_plan) inserts whenever the
/// walker crosses a shuffle / coalesce / broadcast boundary.
///
/// The default implementation ([`DefaultBoundaryFactory`]) emits the
/// upstream Arrow Flight-backed operators ([`NetworkShuffleExec`],
/// [`NetworkCoalesceExec`], [`NetworkBroadcastExec`]). Consumers that
/// run distributed plans over a non-Flight transport (e.g. paradedb's
/// in-process `shm_mq` mesh) implement this trait to plug their own
/// boundary types into the same walker.
///
/// # Concurrency
///
/// Methods take `&self` so the walker can hold a single shared
/// reference across recursion. Implementations that need per-call
/// mutable state (e.g. popping from a queue of pre-allocated transport
/// resources) should use interior mutability.
pub trait BoundaryFactory: Send + Sync {
    /// Build a hash-shuffle boundary node.
    ///
    /// `child` is the input subtree the walker placed beneath the
    /// shuffle annotation; for upstream consumers it is typically a
    /// `RepartitionExec(Hash)` synthesized earlier in the pipeline.
    fn shuffle(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Build a coalesce boundary node (multi-task → fewer-task gather).
    fn coalesce(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Build a broadcast boundary node (build-side replication).
    fn broadcast(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Default factory producing Arrow Flight-backed boundary operators.
/// Preserves the upstream behavior of `distribute_plan` when no
/// alternate factory is supplied.
#[derive(Debug, Default)]
pub struct DefaultBoundaryFactory;

impl BoundaryFactory for DefaultBoundaryFactory {
    fn shuffle(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetworkShuffleExec::try_new(
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )?))
    }

    fn coalesce(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetworkCoalesceExec::try_new(
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )?))
    }

    fn broadcast(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetworkBroadcastExec::try_new(
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )?))
    }
}
