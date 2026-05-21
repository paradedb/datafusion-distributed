use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Discriminator for the concrete [NetworkBoundary] kinds the planner produces.
///
/// Exposed so embedders (and any caller that needs to branch on the boundary's routing
/// semantics) can do a typed enum match instead of comparing against `ExecutionPlan::name()`
/// strings. The latter couples the embedder to internal type-name choices in this crate; a
/// future rename of `NetworkShuffleExec` would silently break consumers doing
/// `if plan.name() == "NetworkShuffleExec" { ... }`.
///
/// Marked `#[non_exhaustive]` so adding a new variant in a future release of this crate is
/// not a breaking change for downstream consumers — they're still required to keep a `_`
/// arm in their `match`. Each existing variant continues to map 1:1 to one of the three
/// concrete `Network*Exec` types and is stable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NetworkBoundaryKind {
    /// Hash-partitioned mesh — [`NetworkShuffleExec`].
    Shuffle,
    /// Broadcast (one producer task, every consumer task receives every partition) —
    /// [`NetworkBroadcastExec`].
    Broadcast,
    /// Gather to a single consumer task — [`NetworkCoalesceExec`].
    Coalesce,
}

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Returns the boundary's [`NetworkBoundaryKind`]. Embedders use this to switch on routing
    /// semantics (shuffle hash-partitions, broadcast replicates, coalesce gathers) without
    /// matching on `ExecutionPlan::name()` strings.
    fn kind(&self) -> NetworkBoundaryKind;

    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;
}

/// Extension trait for downcasting dynamic types to [NetworkBoundary].
pub trait NetworkBoundaryExt {
    /// Downcasts self to a [NetworkBoundary] if possible.
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary>;
    /// Returns whether self is a [NetworkBoundary] or not.
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        if let Some(node) = self.as_any().downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.as_any().downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else if let Some(node) = self.as_any().downcast_ref::<NetworkBroadcastExec>() {
            Some(node)
        } else {
            None
        }
    }
}
