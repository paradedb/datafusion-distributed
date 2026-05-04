use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use crate::worker::{FlightWorkerTransport, WorkerTransport};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use std::sync::{Arc, LazyLock};

pub(crate) fn set_distributed_worker_transport(
    cfg: &mut SessionConfig,
    transport: impl WorkerTransport + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let extension = WorkerTransportExtension(Some(Arc::new(transport)));
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_worker_transport = extension;
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_worker_transport: extension,
                ..Default::default()
            },
        )
    }
}

// The default Flight transport carries no per-runtime state (it consults the channel resolver each
// time), so a single process-wide instance is sufficient for callers that have not registered
// their own.
static DEFAULT_WORKER_TRANSPORT: LazyLock<Arc<dyn WorkerTransport + Send + Sync>> =
    LazyLock::new(|| Arc::new(FlightWorkerTransport));

/// Returns the [WorkerTransport] registered on the session config attached to `task_ctx`, or a
/// process-wide [FlightWorkerTransport] if none has been set. This is what
/// [crate::worker::WorkerConnectionPool] consults at execute time when opening connections to
/// remote workers.
pub fn get_distributed_worker_transport(
    task_ctx: &TaskContext,
) -> Arc<dyn WorkerTransport + Send + Sync> {
    let opts = task_ctx.session_config().options();
    if let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>()
        && let Some(t) = &distributed_cfg.__private_worker_transport.0
    {
        return Arc::clone(t);
    }
    Arc::clone(&DEFAULT_WORKER_TRANSPORT)
}

#[derive(Clone, Default)]
pub(crate) struct WorkerTransportExtension(
    pub(crate) Option<Arc<dyn WorkerTransport + Send + Sync>>,
);
