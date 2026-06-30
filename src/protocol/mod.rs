#[cfg(feature = "grpc")]
pub mod grpc;

mod channel_resolver;
// The prost message types and the metrics codec carry no tonic dependency, so a non-gRPC transport
// (the in-process worker, a shared-memory mesh) can speak the same wire shape without pulling in the
// whole gRPC stack.
pub(crate) mod generated;
mod in_process;
pub(crate) mod metrics_proto;
mod worker_channel;

pub use channel_resolver::{ChannelResolver, get_distributed_channel_resolver};
pub(crate) use channel_resolver::{ChannelResolverExtension, set_distributed_channel_resolver};
pub use in_process::InProcessChannelResolver;

pub use worker_channel::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
    LoadInfo, ProducerHeadSpec, SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch,
    WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
};
