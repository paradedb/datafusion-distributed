use crate::common::OnceLockResult;
use crate::networking::get_distributed_worker_transport;
use crate::stage::RemoteStage;
use crate::worker::transport::{WorkerConnection, WorkerTransport};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::{Arc, OnceLock};

/// Holds a list of lazily initialized [WorkerConnection]s. Each position in the underlying
/// `connections` vector corresponds to the connection to one worker. It assumes a 1:1 mapping
/// between worker and tasks, and upon calling [WorkerConnectionPool::get_or_init_worker_connection]
/// it will initialize the corresponding position in the vector matching the provided `target_task`
/// index.
pub(crate) struct WorkerConnectionPool {
    connections: Vec<OnceLockResult<Box<dyn WorkerConnection>>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl WorkerConnectionPool {
    /// Builds a new [WorkerConnectionPool] with as many empty slots for [WorkerConnection]s as
    /// the provided `input_tasks`.
    pub(crate) fn new(input_tasks: usize) -> Self {
        let mut connections = Vec::with_capacity(input_tasks);
        for _ in 0..input_tasks {
            connections.push(OnceLock::new());
        }
        Self {
            connections,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    /// Lazily initializes the [WorkerConnection] corresponding to the provided `target_task`
    /// (therefore maintaining one independent [WorkerConnection] per `target_task`), and
    /// returns it.
    pub(crate) fn get_or_init_worker_connection(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
    ) -> Result<&dyn WorkerConnection> {
        let Some(worker_connection) = self.connections.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.connections.len()
            );
        };

        let conn = worker_connection.get_or_init(|| {
            get_distributed_worker_transport(ctx.session_config())
                .open(
                    input_stage,
                    target_partitions,
                    target_task,
                    ctx,
                    &self.metrics,
                )
                .map_err(Arc::new)
        });

        match conn {
            Ok(v) => Ok(v.as_ref()),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}

impl Debug for WorkerConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnections")
            .field("num_connections", &self.connections.len())
            .finish()
    }
}

impl Clone for WorkerConnectionPool {
    fn clone(&self) -> Self {
        Self::new(self.connections.len())
    }
}
