use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Supplies the pre-serialized stage subplan the coordinator dispatches, instead of the coordinator
/// encoding the plan it holds.
///
/// An embedder whose dispatch plan differs from its execution plan registers one via
/// [`crate::DistributedExt::with_distributed_dispatch_plan_source`]. The shm embedder, for example,
/// dispatches a structure-only build (segment-free scans the workers specialize per task) that its
/// exec-time plan is not; sourcing the bytes here lets the coordinator route that plan rather than
/// re-encode its own, which carries exec-time state that is wrong to dispatch.
///
/// Returning `None` for a `(stage_id, task_number)` lets the coordinator fall back to encoding the
/// plan it holds, so a resolver that only overrides some stages stays correct.
pub trait DispatchPlanSource: Send + Sync {
    fn dispatch_plan_proto(&self, stage_id: usize, task_number: usize) -> Option<Vec<u8>>;
}

#[derive(Clone)]
pub(crate) struct DispatchPlanSourceExtension(pub(crate) Arc<dyn DispatchPlanSource>);

pub(crate) fn set_distributed_dispatch_plan_source(
    cfg: &mut SessionConfig,
    source: impl DispatchPlanSource + 'static,
) {
    set_distributed_dispatch_plan_source_arc(cfg, Arc::new(source))
}

pub(crate) fn set_distributed_dispatch_plan_source_arc(
    cfg: &mut SessionConfig,
    source: Arc<dyn DispatchPlanSource>,
) {
    cfg.set_extension(Arc::new(DispatchPlanSourceExtension(source)));
}

/// Returns the [`DispatchPlanSource`] registered on this config, if any.
pub fn get_distributed_dispatch_plan_source(
    cfg: &SessionConfig,
) -> Option<Arc<dyn DispatchPlanSource>> {
    cfg.get_extension::<DispatchPlanSourceExtension>()
        .map(|ext| Arc::clone(&ext.0))
}
