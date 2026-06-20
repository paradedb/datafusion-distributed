// Flight-only: it spins a real gRPC worker over an in-memory duplex. The transport-neutral
// worker resolver lives in its own module so no-flight tests keep it.
pub mod in_memory_channel_resolver;
pub mod in_memory_worker_resolver;
pub mod insta;
pub mod localhost;
pub mod metrics;
pub mod mock_exec;
pub mod parquet;
pub mod plans;
pub mod property_based;
pub mod routing;
pub mod session_context;
pub mod test_work_unit_feed;
pub mod work_unit_file_scan;
