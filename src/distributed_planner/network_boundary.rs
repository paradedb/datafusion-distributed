use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Discriminator for the concrete [NetworkBoundary] kinds the planner produces.
///
/// Callers that need to branch on routing semantics can match on this instead of comparing
/// `ExecutionPlan::name()` strings. A string match couples consumers to internal type-name
/// choices here; a rename would silently break them.
///
/// `#[non_exhaustive]` so adding a variant later isn't a breaking change. Consumers keep a
/// `_` arm. Existing variants map 1:1 to the three concrete `Network*Exec` types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NetworkBoundaryKind {
    /// Hash-partitioned mesh. See [`NetworkShuffleExec`].
    Shuffle,
    /// One producer task, every consumer task gets every partition. See [`NetworkBroadcastExec`].
    Broadcast,
    /// Gather to a single consumer task. See [`NetworkCoalesceExec`].
    Coalesce,
}

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Returns the boundary's [`NetworkBoundaryKind`]. Use this to branch on routing
    /// (shuffle hash-partitions, broadcast replicates, coalesce gathers) without matching
    /// on `ExecutionPlan::name()`.
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
