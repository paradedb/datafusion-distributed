use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Constructs the per-stage network-boundary operators that
/// [`distribute_plan`](super::distribute_plan) inserts whenever the
/// walker crosses a shuffle / coalesce / broadcast boundary.
///
/// The default implementation ([`DefaultBoundaryFactory`]) emits the
/// upstream Arrow Flight-backed operators ([`NetworkShuffleExec`],
/// [`NetworkCoalesceExec`], [`NetworkBroadcastExec`]). Consumers that
/// run distributed plans over a non-Flight transport (e.g. paradedb's
/// in-process `shm_mq` mesh) implement this trait to plug their own
/// boundary types into the same walker.
///
/// # Concurrency
///
/// Methods take `&self` so the walker can hold a single shared
/// reference across recursion. Implementations that need per-call
/// mutable state (e.g. popping from a queue of pre-allocated transport
/// resources) should use interior mutability.
pub trait BoundaryFactory: Send + Sync {
    /// Build a hash-shuffle boundary node.
    ///
    /// `child` is the input subtree the walker placed beneath the
    /// shuffle annotation; for upstream consumers it is typically a
    /// `RepartitionExec(Hash)` synthesized earlier in the pipeline.
    fn shuffle(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Build a coalesce boundary node (multi-task → fewer-task gather).
    fn coalesce(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Build a broadcast boundary node (build-side replication).
    fn broadcast(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Default factory producing Arrow Flight-backed boundary operators.
/// Preserves the upstream behavior of `distribute_plan` when no
/// alternate factory is supplied.
#[derive(Debug, Default)]
pub struct DefaultBoundaryFactory;

impl BoundaryFactory for DefaultBoundaryFactory {
    fn shuffle(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetworkShuffleExec::try_new(
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )?))
    }

    fn coalesce(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetworkCoalesceExec::try_new(
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )?))
    }

    fn broadcast(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetworkBroadcastExec::try_new(
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )?))
    }
}

/// Discriminator passed to [`PooledBoundaryFactory`]'s emit callback so a
/// single closure can dispatch on the boundary kind without the caller
/// duplicating the same pop-resource scaffolding three times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryKind {
    Shuffle,
    Coalesce,
    Broadcast,
}

impl fmt::Display for BoundaryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoundaryKind::Shuffle => write!(f, "Shuffle"),
            BoundaryKind::Coalesce => write!(f, "Coalesce"),
            BoundaryKind::Broadcast => write!(f, "Broadcast"),
        }
    }
}

/// [`BoundaryFactory`] adapter for non-Flight transports that pre-allocate
/// a fixed pool of transport resources (DSM regions, in-process channels,
/// etc.) at plan time and consume one resource per emitted boundary.
///
/// Pops one resource per `shuffle()` / `coalesce()` / `broadcast()` call —
/// in the bottom-up order
/// [`distribute_annotated_plan`](super::distribute_annotated_plan) visits
/// boundaries — and forwards it to the user-supplied `emit` closure along
/// with the standard [`BoundaryFactory`] arguments. After the walker
/// returns, [`assert_drained`](Self::assert_drained) verifies the pool
/// was sized correctly.
///
/// # Why
///
/// Implementing [`BoundaryFactory`] over a pre-allocated resource pool is
/// the typical shape for non-Flight consumers: the leader / workers
/// allocate transport resources up front (one per planned boundary), then
/// the walker consumes them in a fixed visit order. Without this adapter
/// every consumer reinvents the same `Mutex<VecDeque<R>>` + three-method
/// dispatch + drained-on-exit assertion.
///
/// # Concurrency
///
/// Methods take `&self` per the [`BoundaryFactory`] contract;
/// `Mutex<VecDeque<R>>` provides interior mutability for the pop. The
/// walker is single-threaded, so the lock is uncontended in normal use.
///
/// # Example
///
/// ```ignore
/// let factory = PooledBoundaryFactory::new(
///     pre_allocated_meshes,            // Vec<MyTransportMesh>
///     |kind, mesh, child, query_id, stage_id, task_count, input_task_count| {
///         match kind {
///             BoundaryKind::Shuffle  => emit_shuffle(mesh, child, ...),
///             BoundaryKind::Coalesce => emit_coalesce(mesh, child, ...),
///             BoundaryKind::Broadcast => Err(plan_err!("broadcast unsupported")),
///         }
///     },
/// );
/// let plan = distribute_annotated_plan(annotated, &cfg, query_id, &mut sid, &factory)?;
/// factory.assert_drained()?;
/// ```
pub struct PooledBoundaryFactory<R, F> {
    pool: Mutex<VecDeque<R>>,
    emit: F,
}

impl<R, F> PooledBoundaryFactory<R, F>
where
    R: Send,
    F: Fn(
            BoundaryKind,
            R,
            Arc<dyn ExecutionPlan>,
            Uuid,
            usize,
            usize,
            usize,
        ) -> Result<Arc<dyn ExecutionPlan>>
        + Send
        + Sync,
{
    /// Construct a factory from a fixed-size pool of resources and an
    /// `emit` callback.
    pub fn new(pool: impl IntoIterator<Item = R>, emit: F) -> Self {
        Self {
            pool: Mutex::new(pool.into_iter().collect()),
            emit,
        }
    }

    /// Number of resources still in the pool. Useful for tests; production
    /// code should rely on [`assert_drained`](Self::assert_drained).
    pub fn remaining(&self) -> usize {
        self.pool
            .lock()
            .expect("PooledBoundaryFactory pool poisoned")
            .len()
    }

    /// Returns `Err(DataFusionError::Plan)` if the pool still holds
    /// resources after the walker has run. Call once after
    /// [`distribute_annotated_plan`](super::distribute_annotated_plan)
    /// returns to assert the pre-allocated count matched the actual
    /// boundary count.
    pub fn assert_drained(&self) -> Result<()> {
        let leftover = self.remaining();
        if leftover > 0 {
            return Err(DataFusionError::Plan(format!(
                "PooledBoundaryFactory: {leftover} resource(s) left unconsumed; \
                 pre-allocated count exceeded the actual boundary count"
            )));
        }
        Ok(())
    }

    fn dispatch(
        &self,
        kind: BoundaryKind,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let resource = self
            .pool
            .lock()
            .expect("PooledBoundaryFactory pool poisoned")
            .pop_front()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "PooledBoundaryFactory: pool exhausted at stage_id={stage_id} ({kind}); \
                     pre-allocated count was below the actual boundary count"
                ))
            })?;
        (self.emit)(
            kind,
            resource,
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )
    }
}

impl<R, F> BoundaryFactory for PooledBoundaryFactory<R, F>
where
    R: Send,
    F: Fn(
            BoundaryKind,
            R,
            Arc<dyn ExecutionPlan>,
            Uuid,
            usize,
            usize,
            usize,
        ) -> Result<Arc<dyn ExecutionPlan>>
        + Send
        + Sync,
{
    fn shuffle(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.dispatch(
            BoundaryKind::Shuffle,
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )
    }

    fn coalesce(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.dispatch(
            BoundaryKind::Coalesce,
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )
    }

    fn broadcast(
        &self,
        child: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        stage_id: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.dispatch(
            BoundaryKind::Broadcast,
            child,
            query_id,
            stage_id,
            task_count,
            input_task_count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn pops_resource_per_boundary_in_visit_order() {
        let calls = Arc::new(Mutex::new(Vec::<(BoundaryKind, u32)>::new()));
        let calls_clone = Arc::clone(&calls);
        let factory = PooledBoundaryFactory::new(
            vec![10u32, 20, 30],
            move |kind, resource, _, _, _, _, _| {
                calls_clone.lock().unwrap().push((kind, resource));
                Ok(empty_plan())
            },
        );

        let qid = Uuid::nil();
        factory.shuffle(empty_plan(), qid, 0, 1, 1).unwrap();
        factory.coalesce(empty_plan(), qid, 1, 1, 1).unwrap();
        factory.broadcast(empty_plan(), qid, 2, 1, 1).unwrap();

        let log = calls.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                (BoundaryKind::Shuffle, 10),
                (BoundaryKind::Coalesce, 20),
                (BoundaryKind::Broadcast, 30),
            ]
        );
        factory.assert_drained().unwrap();
    }

    #[test]
    fn assert_drained_errors_on_unused_resources() {
        let factory =
            PooledBoundaryFactory::new(vec![1u32, 2], |_, _, _, _, _, _, _| Ok(empty_plan()));
        factory.shuffle(empty_plan(), Uuid::nil(), 0, 1, 1).unwrap();
        let err = factory.assert_drained().unwrap_err();
        assert!(
            err.to_string().contains("1 resource(s) left unconsumed"),
            "got: {err}"
        );
    }

    #[test]
    fn dispatch_errors_when_pool_exhausted() {
        let factory =
            PooledBoundaryFactory::new(vec![1u32], |_, _, _, _, _, _, _| Ok(empty_plan()));
        factory.shuffle(empty_plan(), Uuid::nil(), 0, 1, 1).unwrap();
        let err = factory
            .coalesce(empty_plan(), Uuid::nil(), 1, 1, 1)
            .unwrap_err();
        assert!(err.to_string().contains("pool exhausted"), "got: {err}");
        assert!(err.to_string().contains("Coalesce"), "got: {err}");
    }

    #[test]
    fn emit_errors_propagate() {
        let factory = PooledBoundaryFactory::new(vec![1u32], |kind, _, _, _, _, _, _| {
            Err(DataFusionError::Plan(format!("user rejected {kind}")))
        });
        let err = factory
            .broadcast(empty_plan(), Uuid::nil(), 0, 1, 1)
            .unwrap_err();
        assert!(
            err.to_string().contains("user rejected Broadcast"),
            "got: {err}"
        );
    }

    #[test]
    fn remaining_reports_pool_size() {
        let factory =
            PooledBoundaryFactory::new(vec![1u32, 2, 3], |_, _, _, _, _, _, _| Ok(empty_plan()));
        assert_eq!(factory.remaining(), 3);
        factory.shuffle(empty_plan(), Uuid::nil(), 0, 1, 1).unwrap();
        assert_eq!(factory.remaining(), 2);
    }

    #[test]
    fn factory_is_send_sync() {
        // Compile-time assertion that the adapter satisfies the
        // `BoundaryFactory: Send + Sync` bound.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<
            PooledBoundaryFactory<
                u32,
                fn(
                    BoundaryKind,
                    u32,
                    Arc<dyn ExecutionPlan>,
                    Uuid,
                    usize,
                    usize,
                    usize,
                ) -> Result<Arc<dyn ExecutionPlan>>,
            >,
        >();

        // Also check we can use the adapter through the trait object.
        let _: &dyn BoundaryFactory =
            &PooledBoundaryFactory::new(vec![0u32; 0], |_, _, _, _, _, _, _| Ok(empty_plan()));
    }
}
