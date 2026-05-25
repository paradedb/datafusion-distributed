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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BroadcastExec, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::{Partitioning, expressions::Column};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use uuid::Uuid;

    fn empty_with_field(name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    /// Canonical dispatch pattern for [`NetworkBoundary`] consumers. The exhaustive
    /// match also acts as a compile-time anchor: adding a [`NetworkBoundaryKind`]
    /// variant forces every match site to update (or its `_` arm) instead of
    /// silently drifting.
    fn label(boundary: &dyn NetworkBoundary) -> &'static str {
        match boundary.kind() {
            NetworkBoundaryKind::Shuffle => "shuffle",
            NetworkBoundaryKind::Broadcast => "broadcast",
            NetworkBoundaryKind::Coalesce => "coalesce",
        }
    }

    #[test]
    fn shuffle_reports_shuffle_kind() -> datafusion::common::Result<()> {
        let leaf = empty_with_field("a");
        let part = Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1);
        let input: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(leaf, part)?);
        let exec = NetworkShuffleExec::try_new(input, Uuid::nil(), 1, 1, 1)?;
        assert_eq!(exec.kind(), NetworkBoundaryKind::Shuffle);
        assert_eq!(label(&exec), "shuffle");
        Ok(())
    }

    #[test]
    fn broadcast_reports_broadcast_kind() -> datafusion::common::Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(BroadcastExec::new(empty_with_field("a"), 1));
        let exec = NetworkBroadcastExec::try_new(input, Uuid::nil(), 1, 1, 1)?;
        assert_eq!(exec.kind(), NetworkBoundaryKind::Broadcast);
        assert_eq!(label(&exec), "broadcast");
        Ok(())
    }

    #[test]
    fn coalesce_reports_coalesce_kind() -> datafusion::common::Result<()> {
        let exec = NetworkCoalesceExec::try_new(empty_with_field("a"), Uuid::nil(), 1, 1, 1)?;
        assert_eq!(exec.kind(), NetworkBoundaryKind::Coalesce);
        assert_eq!(label(&exec), "coalesce");
        Ok(())
    }
}
