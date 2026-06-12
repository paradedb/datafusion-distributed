use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
#[cfg(feature = "flight")]
use crate::worker::FlightWorkerTransport;
use crate::worker::WorkerTransport;
use datafusion::prelude::SessionConfig;
use std::sync::{Arc, LazyLock};

pub(crate) fn set_distributed_worker_transport(
    cfg: &mut SessionConfig,
    transport: impl WorkerTransport + 'static,
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
static DEFAULT_WORKER_TRANSPORT: LazyLock<Arc<dyn WorkerTransport>> =
    LazyLock::new(|| Arc::new(FlightWorkerTransport));

// With Flight compiled out there is no built-in transport. Embedders must register one; if they
// do not, opening a connection fails loudly instead of silently doing nothing.
#[cfg(not(feature = "flight"))]
static DEFAULT_WORKER_TRANSPORT: LazyLock<Arc<dyn WorkerTransport>> =
    LazyLock::new(|| Arc::new(UnsetWorkerTransport));

/// Returns the [WorkerTransport] registered on the provided session config, or a process-wide
/// default if none has been set. This is what `WorkerConnectionPool` consults at execute time
/// when opening connections to remote workers.
pub fn get_distributed_worker_transport(cfg: &SessionConfig) -> Arc<dyn WorkerTransport> {
    let opts = cfg.options();
    if let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>()
        && let Some(t) = &distributed_cfg.__private_worker_transport.0
    {
        return Arc::clone(t);
    }
    // A session built without a registered transport silently falls back to the process-wide
    // default. With `flight` on that is the gRPC transport, which keeps the pre-transport
    // behavior for existing users; a misregistered embedder surfaces as a connection error
    // against whatever URLs the resolver produced rather than an error here.
    Arc::clone(&DEFAULT_WORKER_TRANSPORT)
}

#[derive(Clone, Default)]
pub(crate) struct WorkerTransportExtension(pub(crate) Option<Arc<dyn WorkerTransport>>);

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
        _ctx: &Arc<datafusion::execution::TaskContext>,
        _metrics: &datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn crate::WorkerConnection>> {
        datafusion::common::not_impl_err!("{UNSET_TRANSPORT_MSG}")
    }

    fn dispatcher(&self) -> Box<dyn crate::WorkerDispatch> {
        Box::new(UnsetWorkerTransport)
    }
}

#[cfg(not(feature = "flight"))]
impl crate::WorkerDispatch for UnsetWorkerTransport {
    fn dispatch(
        &self,
        _request: crate::WorkerDispatchRequest<'_>,
    ) -> datafusion::common::Result<()> {
        datafusion::common::not_impl_err!("{UNSET_TRANSPORT_MSG}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::RemoteStage;
    use crate::worker::transport::{WorkerConnection, WorkerDispatch, WorkerDispatchRequest};
    use datafusion::common::{Result, internal_err};
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
    use uuid::Uuid;

    struct MockTransport;

    impl WorkerTransport for MockTransport {
        fn open(
            &self,
            _input_stage: &RemoteStage,
            _target_partitions: std::ops::Range<usize>,
            _target_task: usize,
            _ctx: &Arc<TaskContext>,
            _metrics: &ExecutionPlanMetricsSet,
        ) -> Result<Box<dyn WorkerConnection>> {
            internal_err!("mock transport open")
        }

        fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
            Box::new(MockDispatch)
        }
    }

    struct MockDispatch;

    impl WorkerDispatch for MockDispatch {
        fn dispatch(&self, _request: WorkerDispatchRequest<'_>) -> Result<()> {
            internal_err!("mock transport dispatch")
        }
    }

    #[test]
    fn registered_transport_wins_over_default() {
        let mut cfg = SessionConfig::new();
        set_distributed_worker_transport(&mut cfg, MockTransport);

        let transport = get_distributed_worker_transport(&cfg);
        let result = transport.open(
            &RemoteStage {
                query_id: Uuid::new_v4(),
                num: 0,
                workers: vec![],
            },
            0..1,
            0,
            &Arc::new(TaskContext::default()),
            &ExecutionPlanMetricsSet::new(),
        );
        let Err(err) = result else {
            panic!("expected the registered mock transport to be consulted");
        };
        assert!(err.to_string().contains("mock transport open"));
    }
}
