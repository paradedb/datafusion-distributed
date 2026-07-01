use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Serializes the stage subplan the coordinator dispatches, instead of the coordinator encoding
/// it with its own codec.
///
/// An embedder registers one via
/// [`crate::DistributedExt::with_distributed_dispatch_plan_source`] when the coordinator's codec
/// cannot represent its plan nodes, or when its serialization needs embedder-side handling the
/// codec extension point cannot express (the shm embedder's UDF definitions, for example). The
/// coordinator hands over `specialized`, the same ready-to-run per-task plan it would encode:
/// task-specialized, with nested stages already converted to `Remote`, so a worker executes the
/// decoded bytes as-is.
///
/// Returning `None` for a `(stage_id, task_number)` lets the coordinator fall back to encoding
/// the plan itself, so a source that only overrides some stages stays correct.
pub trait DispatchPlanSource: Send + Sync {
    fn dispatch_plan_proto(
        &self,
        stage_id: usize,
        task_number: usize,
        specialized: &Arc<dyn ExecutionPlan>,
    ) -> Option<Result<Vec<u8>>>;
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
