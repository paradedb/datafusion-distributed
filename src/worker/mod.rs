pub(crate) mod generated;
// New Flight-side transport code lands here, behind this one gate, so the sibling modules
// stay free of per-item `flight` attributes.
#[cfg(feature = "flight")]
mod flight;
#[cfg(feature = "flight")]
mod impl_coordinator_channel;
mod impl_execute_task;
mod session_builder;
mod single_write_multi_read;
#[cfg(feature = "flight")]
mod spawn_select_all;
mod task_data;
#[cfg(any(test, feature = "integration"))]
pub(crate) mod test_utils;
pub(crate) mod transport;
mod worker_connection_pool;
mod worker_service;

#[cfg(feature = "flight")]
pub use flight::FlightWorkerTransport;
#[cfg(feature = "flight")]
pub(crate) use single_write_multi_read::SingleWriteMultiRead;
pub use transport::{WorkerConnection, WorkerTransport};
#[cfg(feature = "flight")]
pub(crate) use worker_connection_pool::LocalWorkerContext;
pub(crate) use worker_connection_pool::WorkerConnectionPool;

pub use session_builder::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt,
    WorkerQueryContext, WorkerSessionBuilder,
};
pub use worker_service::Worker;

pub use task_data::TaskData;
