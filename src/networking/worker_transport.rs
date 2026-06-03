use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
#[cfg(feature = "flight")]
use crate::worker::FlightWorkerTransport;
use crate::worker::WorkerTransport;
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
#[cfg(feature = "flight")]
static DEFAULT_WORKER_TRANSPORT: LazyLock<Arc<dyn WorkerTransport + Send + Sync>> =
    LazyLock::new(|| Arc::new(FlightWorkerTransport));

// With Flight compiled out there is no built-in transport. Embedders must register one; if they
// do not, opening a connection fails loudly instead of silently doing nothing.
#[cfg(not(feature = "flight"))]
static DEFAULT_WORKER_TRANSPORT: LazyLock<Arc<dyn WorkerTransport + Send + Sync>> =
    LazyLock::new(|| Arc::new(UnsetWorkerTransport));

/// Returns the [WorkerTransport] registered on the session config attached to `task_ctx`, or a
/// process-wide default if none has been set. This is what [crate::worker::WorkerConnectionPool]
/// consults at execute time when opening connections to remote workers.
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

#[cfg(not(feature = "flight"))]
struct UnsetWorkerTransport;

#[cfg(not(feature = "flight"))]
const UNSET_TRANSPORT_MSG: &str = "no WorkerTransport registered: the crate was built without the `flight` feature, so register \
     one via DistributedExt::with_distributed_worker_transport";

#[cfg(not(feature = "flight"))]
impl WorkerTransport for UnsetWorkerTransport {
    fn open(
        &self,
        _input_stage: &crate::RemoteStage,
        _target_partitions: std::ops::Range<usize>,
        _target_task: usize,
        _ctx: &Arc<TaskContext>,
        _metrics: &datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn crate::WorkerConnection + Send + Sync>> {
        datafusion::common::internal_err!("{UNSET_TRANSPORT_MSG}")
    }

    fn dispatch(&self) -> &dyn crate::WorkerDispatch {
        self
    }
}

#[cfg(not(feature = "flight"))]
impl crate::WorkerDispatch for UnsetWorkerTransport {
    fn dispatch(
        &self,
        _request: crate::WorkerDispatchRequest<'_>,
    ) -> datafusion::common::Result<()> {
        datafusion::common::internal_err!("{UNSET_TRANSPORT_MSG}")
    }
}
