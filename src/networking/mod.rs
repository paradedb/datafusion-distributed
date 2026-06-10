#[cfg(feature = "flight")]
mod channel_resolver;
mod worker_resolver;
mod worker_transport;

#[cfg(feature = "flight")]
pub(crate) use channel_resolver::set_distributed_channel_resolver;
#[cfg(feature = "flight")]
pub use channel_resolver::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, create_worker_client,
    get_distributed_channel_resolver,
};

pub use worker_resolver::{WorkerResolver, get_distributed_worker_resolver};
pub(crate) use worker_resolver::{WorkerResolverExtension, set_distributed_worker_resolver};

pub use worker_transport::get_distributed_worker_transport;
pub(crate) use worker_transport::{WorkerTransportExtension, set_distributed_worker_transport};

// `ChannelResolverExtension` is a field of `DistributedConfig`, so it must exist in every build.
// Only the inner handle (which names the Flight-only `ChannelResolver` trait) is gated.
#[cfg(feature = "flight")]
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct ChannelResolverExtension(
    #[cfg(feature = "flight")] pub(crate) Option<Arc<dyn ChannelResolver + Send + Sync>>,
);
