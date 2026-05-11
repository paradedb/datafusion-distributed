use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use crate::worker::{FlightWorkerTransport, WorkerTransport};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use std::sync::{Arc, LazyLock};

/// Stores `transport` on `cfg` so that [crate::worker::WorkerConnectionPool] picks it up at
/// execute time instead of falling back to the default Flight gRPC dialer. Used by embedders
/// (e.g. shared-memory transports) that want to keep DataFusion's distributed plan tree but
/// swap out the network path.
pub(crate) fn set_distributed_worker_transport(
    cfg: &mut SessionConfig,
    transport: impl WorkerTransport,
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
static DEFAULT_WORKER_TRANSPORT: LazyLock<Arc<dyn WorkerTransport>> =
    LazyLock::new(|| Arc::new(FlightWorkerTransport));

/// Returns the [WorkerTransport] registered on the session config attached to `task_ctx`, or a
/// process-wide [FlightWorkerTransport] if none has been set. This is what
/// [crate::worker::WorkerConnectionPool] consults at execute time when opening connections to
/// remote workers. Returns a reference so the caller can decide whether to clone the `Arc`.
pub fn get_distributed_worker_transport(task_ctx: &TaskContext) -> &Arc<dyn WorkerTransport> {
    let opts = task_ctx.session_config().options();
    if let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>()
        && let Some(t) = &distributed_cfg.__private_worker_transport.0
    {
        return t;
    }
    &DEFAULT_WORKER_TRANSPORT
}

#[derive(Clone, Default)]
pub(crate) struct WorkerTransportExtension(pub(crate) Option<Arc<dyn WorkerTransport>>);
