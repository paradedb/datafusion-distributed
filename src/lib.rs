#![deny(clippy::all)]

mod common;
mod config_extension_ext;
mod distributed_ext;
mod execution_plans;
mod metrics;
mod passthrough_headers;
mod stage;
mod worker;

mod distributed_planner;
mod networking;
mod observability;
mod protobuf;
pub use protobuf::DistributedCodec;
mod coordinator;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;
mod work_unit_feed;

pub use arrow_ipc::CompressionType;
pub use coordinator::DistributedExec;
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
    DistributedMetricsFormat, FirstLatencyMetric, GaugeMetricExt, LatencyMetricExt, MaxGaugeMetric,
    MaxLatencyMetric, MinLatencyMetric, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric,
    P99LatencyMetric, rewrite_distributed_plan_with_metrics,
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
    DistributedTaskContext, Stage, display_plan_ascii, display_plan_graphviz, explain_analyze,
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
    DefaultSessionBuilder, InMemoryWorkerTransport, MappedWorkerSessionBuilder,
    MappedWorkerSessionBuilderExt, TaskData, Worker, WorkerConnection, WorkerDispatch,
    WorkerDispatchRequest, WorkerQueryContext, WorkerSessionBuilder, WorkerTransport,
};

pub use observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, PingRequest, PingResponse, TaskProgress, TaskStatus, WorkerMetrics,
};
#[cfg(feature = "flight")]
pub use observability::{
    ObservabilityService, ObservabilityServiceClient, ObservabilityServiceImpl,
    ObservabilityServiceServer,
};

#[cfg(any(feature = "integration", test))]
pub use execution_plans::benchmarks::{
    LocalRepartitionBench, LocalRepartitionFixture, LocalRepartitionMode,
};
#[cfg(all(feature = "flight", any(feature = "integration", test)))]
pub use execution_plans::benchmarks::{
    ShuffleBench, ShuffleFixture, TransportBench, TransportBenchMode, TransportFixture,
};
