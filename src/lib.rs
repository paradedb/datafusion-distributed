#![deny(clippy::all)]

mod common;
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod config_extension_ext;
mod distributed_ext;
mod execution_plans;
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod metrics;
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod passthrough_headers;
mod stage;
// With `flight` off there is no remote transport, so the worker-side serve/execute path and the
// coordinator metrics back-channel are compiled but dormant. A non-Flight transport builds on
// that machinery, so it stays in the crate; the `allow` is scoped per module to keep dead-code
// detection live for the rest of the crate.
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod worker;

mod distributed_planner;
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod networking;
#[cfg(feature = "flight")]
mod observability;
mod protobuf;
pub use protobuf::DistributedCodec;
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod coordinator;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod work_unit_feed;

pub use arrow_ipc::CompressionType;
pub use coordinator::{DistributedExec, MetricsStore};
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, NetworkBoundary, NetworkBoundaryExt, SessionStateBuilderExt,
    TaskCountAnnotation, TaskEstimation, TaskEstimator, TaskRoutingContext,
};
pub use execution_plans::{
    BroadcastExec, DistributedLeafExec, NetworkBroadcastExec, NetworkCoalesceExec,
    NetworkShuffleExec,
};
pub use metrics::{
    AvgLatencyMetric, BytesCounterMetric, BytesMetricExt, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL,
    DistributedMetricsFormat, FirstLatencyMetric, LatencyMetricExt, MaxLatencyMetric,
    MinLatencyMetric, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
    rewrite_distributed_plan_with_metrics,
};
#[cfg(feature = "flight")]
pub use networking::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, create_worker_client,
    get_distributed_channel_resolver,
};
pub use networking::{
    WorkerResolver, get_distributed_worker_resolver, get_distributed_worker_transport,
};
pub use stage::{
    DistributedTaskContext, LocalStage, RemoteStage, Stage, display_plan_ascii,
    display_plan_graphviz, explain_analyze,
};
pub use work_unit_feed::{
    DistributedWorkUnitFeedContext, WorkUnit, WorkUnitFeed, WorkUnitFeedProto, WorkUnitFeedProvider,
};
#[cfg(feature = "flight")]
pub use worker::FlightWorkerTransport;
#[cfg(feature = "flight")]
pub use worker::generated::worker::worker_service_client::WorkerServiceClient;
#[cfg(feature = "flight")]
pub use worker::generated::worker::worker_service_server::WorkerServiceServer;
pub use worker::generated::worker::{GetWorkerInfoRequest, GetWorkerInfoResponse, TaskKey};
pub use worker::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt, TaskData,
    Worker, WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerQueryContext,
    WorkerSessionBuilder, WorkerTransport,
};

#[cfg(feature = "flight")]
pub use observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, ObservabilityService, ObservabilityServiceClient,
    ObservabilityServiceImpl, ObservabilityServiceServer, PingRequest, PingResponse, TaskProgress,
    TaskStatus, WorkerMetrics,
};

#[cfg(all(feature = "flight", any(feature = "integration", test)))]
pub use execution_plans::benchmarks::{
    LocalRepartitionBench, LocalRepartitionFixture, LocalRepartitionMode, ShuffleBench,
    ShuffleFixture, TransportBench, TransportBenchMode, TransportFixture,
};
