use crate::protocol::LocalWorkerContext;
use crate::worker::{SingleWriteMultiRead, WorkerSessionBuilder};
use crate::{DefaultSessionBuilder, TaskData, TaskKey};
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionState, SessionStateBuilder};
use moka::future::Cache;
use std::borrow::Cow;
#[cfg(feature = "integration")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use url::Url;

const TASK_CACHE_TTI: Duration = Duration::from_mins(10);

// Workers only decode and execute physical plans, so analyzer, logical optimizer, and physical
// optimizer rules are never used on the worker and can be omitted from the default base
// SessionState. Caching this process-wide avoids re-allocating default function registries
// whenever a Worker is constructed via Worker::default() or Worker::from_session_builder().
static DEFAULT_BASE_SESSION_STATE: LazyLock<SessionState> = LazyLock::new(|| {
    SessionStateBuilder::new()
        .with_default_features()
        .with_analyzer_rules(vec![])
        .with_optimizer_rules(vec![])
        .with_physical_optimizer_rules(vec![])
        .build()
});

pub(crate) type ResultTaskData = Result<TaskData, Arc<DataFusionError>>;
pub(crate) type TaskDataEntries = Cache<TaskKey, Arc<SingleWriteMultiRead<ResultTaskData>>>;

#[derive(Clone)]
pub struct Worker {
    pub(super) runtime: Arc<RuntimeEnv>,
    /// Pre-built base [SessionState] cloned per task via [SessionStateBuilder::new_from_existing]
    /// so default functions and registries are not re-allocated on every task setup.
    /// When `None`, lazily falls back to [DEFAULT_BASE_SESSION_STATE] so callers that supply
    /// a custom state via [Self::with_base_session_state] never pay to initialize the default one.
    pub(super) base_session_state: Option<SessionState>,
    /// Shares task state across partition requests. Coordinator-channel termination releases
    /// the stored task resources; time-to-idle eviction provides fallback cleanup for entries
    /// left by abandoned setup requests.
    pub(crate) task_data_entries: Arc<TaskDataEntries>,
    pub(super) session_builder: Arc<dyn WorkerSessionBuilder + Send + Sync>,
    pub(crate) max_message_size: Option<usize>,
    pub(super) version: Cow<'static, str>,
    #[cfg(feature = "integration")]
    pub(super) coordinator_channels_running: Arc<AtomicUsize>,
}

impl Default for Worker {
    fn default() -> Self {
        let cache = Cache::builder().time_to_idle(TASK_CACHE_TTI).build();
        Self {
            runtime: Arc::new(RuntimeEnv::default()),
            base_session_state: None,
            task_data_entries: Arc::new(cache),
            session_builder: Arc::new(DefaultSessionBuilder),
            max_message_size: Some(usize::MAX),
            version: Cow::Borrowed(""),
            #[cfg(feature = "integration")]
            coordinator_channels_running: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Worker {
    /// Builds a [Worker] with a custom [WorkerSessionBuilder]. Use this
    /// method whenever you need to add custom stuff to the `SessionContext` that executes the query.
    pub fn from_session_builder(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            session_builder: Arc::new(session_builder),
            ..Default::default()
        }
    }

    /// Sets the base [SessionState] cloned as the starting point for every task's
    /// [WorkerQueryContext::builder].
    ///
    /// Pre-registering static UDFs, UDAFs, UDWFs, and config extensions on `base_session_state`
    /// avoids re-building them on every task setup.
    pub fn with_base_session_state(mut self, base_session_state: SessionState) -> Self {
        self.base_session_state = Some(base_session_state);
        self
    }

    pub(super) fn base_session_state(&self) -> &SessionState {
        self.base_session_state
            .as_ref()
            .unwrap_or(&DEFAULT_BASE_SESSION_STATE)
    }

    /// Sets a [RuntimeEnv] to be used in all the queries this [Worker] will handle during
    /// its lifetime.
    pub fn with_runtime_env(mut self, runtime_env: Arc<RuntimeEnv>) -> Self {
        self.runtime = runtime_env;
        self
    }

    /// Set the maximum message size for FlightData chunks.
    ///
    /// Defaults to `usize::MAX` to minimize chunking overhead for internal communication.
    /// See [`FlightDataEncoderBuilder::with_max_flight_data_size`] for details.
    ///
    /// If you change this to a lower value, ensure you configure the server's
    /// max_encoding_message_size and max_decoding_message_size to at least 2x this value
    /// to allow for overhead. For most use cases, the default of `usize::MAX` is appropriate.
    ///
    /// [`FlightDataEncoderBuilder::with_max_flight_data_size`]: https://arrow.apache.org/rust/arrow_flight/encode/struct.FlightDataEncoderBuilder.html#structfield.max_flight_data_size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Sets a version string reported by the `GetWorkerInfo` gRPC endpoint.
    pub fn with_version(mut self, version: impl Into<Cow<'static, str>>) -> Self {
        self.version = version.into();
        self
    }

    /// Returns the version set by [Self::with_version].
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Builds a [LocalWorkerContext] suitable to be injecting into a coordinating [SessionContext].
    /// Having a [LocalWorkerContext] present in the coordinating [SessionContext] is not strictly
    /// necessary, but it allows the planner to better colocate small stages near it, avoiding
    /// unnecessary network hops.
    pub fn to_local_worker_context(&self, self_url: Url) -> LocalWorkerContext {
        LocalWorkerContext {
            local_worker: self.clone(),
            self_url,
        }
    }

    /// Returns the number of cached task entries currently held by this worker.
    #[cfg(any(test, feature = "integration"))]
    pub async fn tasks_running(&self) -> usize {
        // Use `run_pending_tasks()` to migigate inaccuracy from potential stale
        // `entry_count()` task data.
        self.task_data_entries.run_pending_tasks().await;
        self.task_data_entries.entry_count() as usize
    }

    /// Returns the number of live worker-to-coordinator streams.
    #[cfg(feature = "integration")]
    pub fn coordinator_channels_running(&self) -> usize {
        self.coordinator_channels_running.load(Ordering::SeqCst)
    }
}
