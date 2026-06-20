pub(crate) mod generated;
// The whole Arrow-Flight implementation hangs off this one gate; the sibling modules stay
// neutral so a no-flight build compiles them without per-item attributes.
mod flight;
mod impl_coordinator_channel;
mod impl_execute_task;
mod impl_set_plan;
mod in_memory;
mod session_builder;
mod single_write_multi_read;
mod spawn_select_all;
mod task_data;
#[cfg(any(test, feature = "integration"))]
pub(crate) mod test_utils;
pub(crate) mod transport;
mod worker_connection_pool;
mod worker_service;

pub use flight::FlightWorkerTransport;
pub(crate) use flight::LocalWorkerContext;
pub(crate) use single_write_multi_read::SingleWriteMultiRead;
pub use transport::{
    PartitionSink, WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerSink,
    WorkerTransport,
};
pub(crate) use worker_connection_pool::WorkerConnectionPool;

pub use session_builder::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt,
    WorkerQueryContext, WorkerSessionBuilder,
};
pub use task_data::TaskData;
pub use worker_service::Worker;
