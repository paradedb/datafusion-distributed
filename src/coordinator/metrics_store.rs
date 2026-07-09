use crate::{TaskKey, TaskMetrics};
use datafusion::common::HashMap;
use tokio::sync::watch;

type MetricsMap = HashMap<TaskKey, TaskMetrics>;

/// Stores the metrics collected from all worker tasks, and notifies waiters when new entries arrive.
#[derive(Debug, Clone)]
pub struct MetricsStore {
    tx: watch::Sender<MetricsMap>,
    pub(crate) rx: watch::Receiver<MetricsMap>,
}

impl MetricsStore {
    pub(crate) fn new() -> Self {
        let (tx, rx) = watch::channel(HashMap::new());
        Self { tx, rx }
    }

    // Public for the in-crate shm/embedder consumer, which files decoded worker metric frames into
    // the executed plan's store before the per-task EXPLAIN rewrite reads it.
    pub fn insert(&self, key: TaskKey, metrics: TaskMetrics) {
        self.tx.send_modify(|map| {
            map.insert(key, metrics);
        });
    }

    pub(crate) fn get(&self, key: &TaskKey) -> Option<TaskMetrics> {
        self.rx.borrow().get(key).cloned()
    }

    #[cfg(all(test, feature = "grpc"))]
    pub(crate) fn from_entries(entries: impl IntoIterator<Item = (TaskKey, TaskMetrics)>) -> Self {
        let map: HashMap<_, _> = entries.into_iter().collect();
        let (tx, rx) = watch::channel(map);
        Self { tx, rx }
    }
}
