#![deny(clippy::all)]

mod common;
mod config_extension_ext;
mod distributed_ext;
// Public so an embedder (e.g. pg_search's shared-memory MPP) consumes the transport directly,
// and so its in-process test runs a real distributed query through it in this crate's CI.
pub mod embedded;
mod execution_plans;
mod metrics;
mod passthrough_headers;
mod stage;
// With `flight` off the in-memory transport keeps this machinery live; what remains dormant is
// the gRPC envelope side: the generated stream messages and the Flight-only `Worker` accessors.
#[cfg_attr(not(feature = "flight"), allow(dead_code))]
mod worker;

mod distributed_planner;
mod networking;
#[cfg(feature = "flight")]
mod observability;
mod protobuf;
pub use protobuf::DistributedCodec;
mod coordinator;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;
mod work_unit_feed;

pub use arrow_ipc::CompressionType;
pub use common::get_distributed_cancellation_token;
pub use coordinator::{DistributedExec, MetricsStore};
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, NetworkBoundary, NetworkBoundaryExt, PartitionRoute, SessionStateBuilderExt,
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
pub use worker::generated::worker::{
    GetWorkerInfoRequest, GetWorkerInfoResponse, SetPlanRequest, TaskKey, TaskMetrics,
};
pub use worker::{
    DefaultSessionBuilder, InMemoryWorkerTransport, MappedWorkerSessionBuilder,
    MappedWorkerSessionBuilderExt, PartitionSink, TaskData, Worker, WorkerConnection,
    WorkerDispatch, WorkerDispatchRequest, WorkerQueryContext, WorkerSessionBuilder, WorkerSink,
    WorkerTransport,
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
