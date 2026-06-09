use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::common::{Result, internal_err};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Where a producer's output partition is read: which consumer task, and the local partition index
/// within that task's slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRoute {
    pub consumer_task: usize,
    pub consumer_partition: usize,
}

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;

    /// `P_c`: how many partitions each consumer task reads in the sliced layout
    /// (`global = P_c * consumer_task + local`) that shuffle and broadcast reads use. Surfaced so
    /// a transport that has to place a produced partition does not re-derive it from node
    /// properties. Meaningless for `NetworkCoalesceExec`, whose consumers read whole
    /// per-producer-task groups instead of slices.
    fn partitions_per_consumer_task(&self) -> usize {
        self.properties().partitioning.partition_count()
    }

    /// Maps a producer output partition to the consumer task and the local partition within that
    /// task that reads it, for the `global = P_c * consumer_task + local` layout.
    ///
    /// Boundaries whose consumers do not read that layout must override this with an error; the
    /// default would silently misroute them. A zero-partition boundary is a planner bug, so it
    /// errors instead of routing everything to task `0`.
    fn route_partition(&self, output_partition: usize) -> Result<PartitionRoute> {
        let p_c = self.partitions_per_consumer_task();
        if p_c == 0 {
            return internal_err!(
                "cannot route output partition {output_partition}: the boundary reports 0 \
                 partitions per consumer task"
            );
        }
        Ok(PartitionRoute {
            consumer_task: output_partition / p_c,
            consumer_partition: output_partition % p_c,
        })
    }
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

/// Scales up the head node of the input stage of a network boundary. Different network boundaries
/// have different needs for scaling up their input, like for example, scaling up a RepartitionExec
/// during shuffles.
pub(crate) fn network_boundary_scale_input(
    input: Arc<dyn ExecutionPlan>,
    consumer_partitions: usize,
    consumer_task_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let transformed = NetworkShuffleExec::scale_input(
        Arc::clone(&input),
        consumer_partitions,
        consumer_task_count,
    )?;
    if transformed.transformed {
        return Ok(transformed.data);
    }
    let transformed = NetworkBroadcastExec::scale_input(
        Arc::clone(&input),
        consumer_partitions,
        consumer_task_count,
    )?;
    if transformed.transformed {
        return Ok(transformed.data);
    }
    let transformed = NetworkCoalesceExec::scale_input(
        Arc::clone(&input),
        consumer_partitions,
        consumer_task_count,
    )?;
    if transformed.transformed {
        return Ok(transformed.data);
    }

    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::LocalStage;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use uuid::Uuid;

    fn shuffle_with_partitions(partitions: usize, tasks: usize) -> NetworkShuffleExec {
        let child = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let plan = Arc::new(
            RepartitionExec::try_new(child, Partitioning::RoundRobinBatch(partitions)).unwrap(),
        );
        NetworkShuffleExec::from_stage(LocalStage {
            query_id: Uuid::new_v4(),
            num: 0,
            plan,
            tasks,
        })
    }

    /// Pins that the produce-side route is the inverse of the consume-side read slices
    /// (`off = P_c * task_index`, reading `off..off + P_c`). The two live in different files
    /// and nothing else ties them together.
    #[test]
    fn route_partition_inverts_consumer_read_slices() -> Result<()> {
        for (p_c, consumer_tasks) in [(4, 3), (1, 2), (5, 1)] {
            let node = shuffle_with_partitions(p_c, consumer_tasks);
            assert_eq!(node.partitions_per_consumer_task(), p_c);
            for task_index in 0..consumer_tasks {
                let off = p_c * task_index;
                for local in 0..p_c {
                    let route = node.route_partition(off + local)?;
                    assert_eq!(route.consumer_task, task_index);
                    assert_eq!(route.consumer_partition, local);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn coalesce_refuses_partition_routing() {
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let node = NetworkCoalesceExec::try_new(child, 4, 2).unwrap();
        assert!(node.route_partition(0).is_err());
    }
}
