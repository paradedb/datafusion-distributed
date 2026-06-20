use crate::{BroadcastExec, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::common::{Result, internal_err};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Where a producer's output partition should be sent to: which consumer task, and the local
/// partition index within that task's slice.
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

    /// Defines what head node should the producer stage feeding this [NetworkBoundary]
    /// implementation have. This information is used during planning an executing for ensuring
    /// the head of a stage has the appropriate shape for consumption.
    fn producer_head(&self, consumer_tasks: usize) -> ProducerHead;

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

/// Defines what shape should the head node of a stage have upon getting executed. Depending
/// on the [NetworkBoundary] implementation, the stage below should have different head nodes.
pub enum ProducerHead {
    /// No specific head node is necessary.
    None,
    /// The head node should be a [BroadcastExec].
    BroadcastExec { output_partitions: usize },
    /// The head node should be a [RepartitionExec].
    RepartitionExec { partitioning: Partitioning },
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
        if let Some(node) = self.downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkBroadcastExec>() {
            Some(node)
        } else {
            None
        }
    }
}

/// Ensures the head of the provided plan complies with the passed [ProducerHead] definition. This
/// can be called both during planning and lazily at runtime.
pub(crate) fn insert_producer_head(
    input: Arc<dyn ExecutionPlan>,
    head: ProducerHead,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input = if let Some(r_exec) = input.downcast_ref::<RepartitionExec>() {
        Arc::clone(r_exec.input())
    } else if let Some(b_exec) = input.downcast_ref::<BroadcastExec>() {
        Arc::clone(b_exec.input())
    } else {
        input
    };
    let plan = match head {
        ProducerHead::None => input,
        ProducerHead::BroadcastExec { output_partitions } => {
            let partitions = input.output_partitioning().partition_count();
            Arc::new(BroadcastExec::new(input, output_partitions / partitions))
        }
        ProducerHead::RepartitionExec { partitioning } => {
            Arc::new(RepartitionExec::try_new(input, partitioning)?)
        }
    };
    Ok(plan)
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
        let properties = Arc::clone(plan.properties());
        NetworkShuffleExec::from_stage(
            Stage::Local(LocalStage {
                query_id: Uuid::new_v4(),
                num: 0,
                plan,
                tasks,
            }),
            properties,
        )
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
