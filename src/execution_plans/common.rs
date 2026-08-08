use datafusion::common::{Result, ScalarValue, not_impl_err};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::expressions::Literal;
use std::sync::Arc;

/// Scales plan properties for a coalesce boundary by multiplying the partition count.
pub(super) fn coalesce_partitioning_props(
    props: &Arc<PlanProperties>,
    task_multiplier: usize,
) -> Result<Arc<PlanProperties>> {
    Ok(Arc::new(PlanProperties::new(
        props.eq_properties.clone(),
        coalesce_partitioning(&props.partitioning, task_multiplier)?,
        props.emission_type,
        props.boundedness,
    )))
}

/// Returns a new Hash partitioning with `salt` appended to the expressions and the partition
/// count set to `consumer_task_count`. The salt breaks hash correlation that would skew
/// distribution when `consumer_task_count` and the producer's partition count share a common factor.
pub(super) fn salted_partitioning(
    partitioning: &Partitioning,
    salt: u64,
    consumer_task_count: usize,
) -> Result<Partitioning> {
    match partitioning {
        Partitioning::Hash(exprs, _) => {
            let salt_lit: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::UInt64(Some(salt))));
            let mut salted_exprs = exprs.clone();
            salted_exprs.push(salt_lit);
            Ok(Partitioning::Hash(salted_exprs, consumer_task_count))
        }
        _ => not_impl_err!("salted_partitioning only supports Hash partitioning"),
    }
}

/// Scales partitioning for a coalesce boundary across independent input tasks.
///
/// Preserves `Range` when `task_multiplier <= 1`, but falls back to `UnknownPartitioning`
/// when `task_multiplier > 1` because repeated ranges cannot be represented as a single `RangePartitioning`.
pub(super) fn coalesce_partitioning(
    partitioning: &Partitioning,
    task_multiplier: usize,
) -> Result<Partitioning> {
    match partitioning {
        Partitioning::RoundRobinBatch(p) => Ok(Partitioning::RoundRobinBatch(*p * task_multiplier)),
        Partitioning::Hash(hash, p) => Ok(Partitioning::Hash(hash.clone(), *p * task_multiplier)),
        Partitioning::UnknownPartitioning(p) => {
            Ok(Partitioning::UnknownPartitioning(*p * task_multiplier))
        }
        // TODO(#68): carry range properties across coalesce boundaries so downstream joins
        // can remain co-partitioned.
        Partitioning::Range(range) => {
            if task_multiplier <= 1 {
                Ok(Partitioning::Range(range.clone()))
            } else {
                Ok(Partitioning::UnknownPartitioning(
                    range.partition_count() * task_multiplier,
                ))
            }
        }
    }
}

/// Scales partitioning for a shuffle producer head across consumer tasks.
pub(super) fn scale_shuffle_partitioning(
    partitioning: &Partitioning,
    consumer_tasks: usize,
) -> Result<Partitioning> {
    match partitioning {
        Partitioning::RoundRobinBatch(p) => Ok(Partitioning::RoundRobinBatch(*p * consumer_tasks)),
        Partitioning::Hash(hash, p) => Ok(Partitioning::Hash(hash.clone(), *p * consumer_tasks)),
        Partitioning::UnknownPartitioning(p) => {
            Ok(Partitioning::UnknownPartitioning(*p * consumer_tasks))
        }
        Partitioning::Range(range) => {
            // Range shuffles currently allocate 1 partition per consumer task so that range
            // boundaries align 1:1 with per-task execution in the consumer stage and co-partitioned
            // leaf scans.
            // TODO(https://github.com/datafusion-contrib/datafusion-distributed/pull/730): Support
            // multi-partition range shuffles (p partitions per consumer task) for multi-core workers
            // once leaf scans and sample counts support sub-partitioning ranges within a task.
            //
            // TODO(https://github.com/apache/datafusion/pull/24766): `RangePartitioning::scale`
            // currently returns a generic `DataFusionError` when `target_partitions > max_partition_count()`.
            // Once the upstream PR introduces an interpretable error enum, handle that error result
            // here to decide on a fallback strategy.
            Ok(Partitioning::Range(range.scale(consumer_tasks)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{
        LexOrdering, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
    };

    fn sample_range() -> RangePartitioning {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            Default::default(),
        )])
        .unwrap();
        let splits = vec![
            SplitPoint::new(vec![ScalarValue::Int64(Some(10))]),
            SplitPoint::new(vec![ScalarValue::Int64(Some(20))]),
        ];
        RangePartitioning::try_new(ordering, splits).unwrap()
    }

    #[test]
    fn coalesce_partitioning_scales_variants() {
        let range = sample_range();
        let unscaled = coalesce_partitioning(&Partitioning::Range(range.clone()), 1).unwrap();
        assert!(matches!(unscaled, Partitioning::Range(_)));

        let scaled = coalesce_partitioning(&Partitioning::Range(range), 4).unwrap();
        assert!(matches!(scaled, Partitioning::UnknownPartitioning(12)));

        let hash = Partitioning::Hash(
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
            2,
        );
        let scaled_hash = coalesce_partitioning(&hash, 3).unwrap();
        assert_eq!(scaled_hash.partition_count(), 6);
    }

    #[test]
    fn scale_shuffle_partitioning_delegates_to_range_scale() {
        let range = sample_range();
        let scaled = scale_shuffle_partitioning(&Partitioning::Range(range), 2).unwrap();
        assert_eq!(scaled.partition_count(), 2);
        assert!(matches!(scaled, Partitioning::Range(_)));
    }

    #[test]
    fn scale_shuffle_partitioning_scales_other_variants() {
        let rrb = scale_shuffle_partitioning(&Partitioning::RoundRobinBatch(2), 3).unwrap();
        assert_eq!(rrb.partition_count(), 6);

        let unk = scale_shuffle_partitioning(&Partitioning::UnknownPartitioning(2), 3).unwrap();
        assert_eq!(unk.partition_count(), 6);
    }
}
