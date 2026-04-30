use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::{Arc, Mutex, OnceLock};

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
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

/// Registers a fallback extractor that [`NetworkBoundaryExt::as_network_boundary`]
/// consults after the built-in DF-D boundary types have been tried.
///
/// Use this from a consumer that introduces its own [`NetworkBoundary`]
/// implementations (e.g. an alternate-transport variant of
/// [`NetworkShuffleExec`]). Without registration, the walker would
/// not recognize the consumer's type as a boundary — its
/// idempotency check, metrics rewriter, and stage-extraction paths
/// all funnel through `as_network_boundary`.
///
/// The registry is global and append-only: repeated calls with the
/// same extractor will register it multiple times (each invocation
/// will be tried in order). Built-in types take priority over
/// registered extractors, matching the upstream-first convention
/// used elsewhere in the planner.
pub type NetworkBoundaryExtractor = fn(&dyn ExecutionPlan) -> Option<&dyn NetworkBoundary>;

static EXTRACTORS: OnceLock<Mutex<Vec<NetworkBoundaryExtractor>>> = OnceLock::new();

pub fn register_network_boundary_extractor(extractor: NetworkBoundaryExtractor) {
    EXTRACTORS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(extractor);
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        // Built-in DF-D types first — keeps upstream behavior byte-for-byte
        // when no extractors are registered.
        if let Some(node) = self.as_any().downcast_ref::<NetworkShuffleExec>() {
            return Some(node);
        }
        if let Some(node) = self.as_any().downcast_ref::<NetworkCoalesceExec>() {
            return Some(node);
        }
        if let Some(node) = self.as_any().downcast_ref::<NetworkBroadcastExec>() {
            return Some(node);
        }

        // Registered third-party extractors. Each is tried in registration
        // order; first match wins.
        if let Some(extractors) = EXTRACTORS.get() {
            for extractor in extractors.lock().unwrap().iter() {
                if let Some(node) = extractor(self) {
                    return Some(node);
                }
            }
        }
        None
    }
}
