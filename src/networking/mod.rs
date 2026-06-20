mod channel_resolver;
mod worker_resolver;
mod worker_transport;

pub(crate) use channel_resolver::set_distributed_channel_resolver;
pub use channel_resolver::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, create_worker_client,
    get_distributed_channel_resolver,
};

pub use worker_resolver::{WorkerResolver, get_distributed_worker_resolver};
pub(crate) use worker_resolver::{WorkerResolverExtension, set_distributed_worker_resolver};

pub use worker_transport::get_distributed_worker_transport;
pub(crate) use worker_transport::{WorkerTransportExtension, set_distributed_worker_transport};

#[derive(Clone, Default)]
pub(crate) struct ChannelResolverExtension(
    pub(crate) Option<std::sync::Arc<dyn ChannelResolver + Send + Sync>>,
);
