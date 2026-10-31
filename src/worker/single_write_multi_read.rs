use std::fmt;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

/// Synchronization primitive that allows multiple readers to wait for a cloneable piece of data.
/// Writers may replace the value until a reader starts consuming it or the slot is sealed.
///
/// - If the writer writes before anyone is reading, all subsequent readers will immediately
///   resolve to the written piece of data.
/// - If one or more readers try to read before something is written, then they will asynchronously
///   wait until a writer writes something.
pub struct SingleWriteMultiRead<T: Clone> {
    tx: watch::Sender<Option<T>>,
    rx: watch::Receiver<Option<T>>,
    state: Mutex<SingleWriteMultiReadState>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WriteGeneration(u64);

#[derive(Default)]
struct SingleWriteMultiReadState {
    read_started: bool,
    latest_reserved_generation: WriteGeneration,
}

impl<T: Clone> Default for SingleWriteMultiRead<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) enum SingleWriteMultiReadError {
    AlreadyWritten,
    Timeout,
    NoValue,
}

impl fmt::Display for SingleWriteMultiReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyWritten => write!(f, "already written"),
            Self::Timeout => write!(f, "timed out waiting for value"),
            Self::NoValue => write!(f, "sender dropped without writing a value"),
        }
    }
}

impl std::error::Error for SingleWriteMultiReadError {}

impl<T: Clone> SingleWriteMultiRead<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self {
            tx,
            rx,
            state: Mutex::new(SingleWriteMultiReadState::default()),
        }
    }

    /// Reserves a monotonically increasing write generation before asynchronous setup begins.
    ///
    /// If a retry on the same worker starts before an earlier setup task finishes, the newer
    /// attempt reserves a higher generation so the stale setup task will not overwrite it.
    pub(crate) fn reserve_write(&self) -> WriteGeneration {
        let mut state = self.state.lock().unwrap();
        state.latest_reserved_generation = WriteGeneration(state.latest_reserved_generation.0 + 1);
        state.latest_reserved_generation
    }

    /// Writes `item` if `generation` is still the latest reserved generation.
    ///
    /// Returns `Ok(true)` if the value was written, `Ok(false)` if a newer writer has already
    /// reserved a later generation, or `Err(AlreadyWritten)` if a reader has already begun
    /// consuming a previously written value.
    pub(crate) fn write_generation(
        &self,
        generation: WriteGeneration,
        item: T,
    ) -> Result<bool, SingleWriteMultiReadError> {
        let state = self.state.lock().unwrap();
        if generation < state.latest_reserved_generation {
            return Ok(false);
        }
        let mut already_written = false;
        self.tx.send_modify(|v| {
            if v.is_none() || !state.read_started {
                *v = Some(item);
            } else {
                already_written = true;
            }
        });
        if already_written {
            return Err(SingleWriteMultiReadError::AlreadyWritten);
        }
        Ok(true)
    }

    /// Write the value. A written but unconsumed value may be replaced. Once a reader has begun
    /// consuming the value, further writes fail.
    #[cfg(any(feature = "integration", test))]
    pub(crate) fn write(&self, item: T) -> Result<(), SingleWriteMultiReadError> {
        let generation = self.reserve_write();
        self.write_generation(generation, item).map(|_| ())
    }

    /// Seals the slot with `item` if `predicate` accepts its current value, preventing further writes.
    /// Returns `true` on replacement, or `false` if empty or the predicate rejects the value.
    /// The displaced value is dropped after releasing the slot's locks.
    pub(crate) fn seal_with_value_if(&self, predicate: impl FnOnce(&T) -> bool, item: T) -> bool {
        let mut state = self.state.lock().unwrap();
        let mut previous = None;
        let sealed = self.tx.send_if_modified(|value| {
            if !value.as_ref().is_some_and(predicate) {
                return false;
            }
            state.read_started = true;
            previous = value.replace(item);
            true
        });
        drop(state);
        drop(previous);
        sealed
    }

    /// Reads the current value, if any, not waiting for it to be set by a writer.
    #[cfg(feature = "grpc")]
    pub(crate) fn read_now(&self) -> Option<T> {
        self.state.lock().unwrap().read_started = true;
        self.rx.borrow().clone()
    }

    /// Await until the writer has placed a value.
    pub(crate) async fn read(
        &self,
        timeout_duration: Duration,
    ) -> Result<T, SingleWriteMultiReadError> {
        self.state.lock().unwrap().read_started = true;
        let mut rx = self.rx.clone();
        let result = tokio::time::timeout(timeout_duration, rx.wait_for(|v| v.is_some()))
            .await
            .map(|r| r.map(|guard| guard.clone()));
        match result {
            Ok(Ok(val)) => val.ok_or(SingleWriteMultiReadError::NoValue),
            Ok(Err(_)) => {
                // Sender dropped; return the last value if one was written
                rx.borrow()
                    .clone()
                    .ok_or(SingleWriteMultiReadError::NoValue)
            }
            Err(_) => Err(SingleWriteMultiReadError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const TIMEOUT: Duration = Duration::from_secs(1);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);

    #[test]
    fn seal_only_releases_the_matching_value() {
        let slot = SingleWriteMultiRead::new();
        let old = Arc::new(1);
        let current = Arc::new(2);
        slot.write(current.clone()).unwrap();

        assert!(!slot.seal_with_value_if(|value| Arc::ptr_eq(value, &old), old.clone()));
        assert_eq!(Arc::strong_count(&current), 2);
        assert!(slot.seal_with_value_if(|value| Arc::ptr_eq(value, &current), old));
        assert_eq!(Arc::strong_count(&current), 1);
        assert!(slot.write(current).is_err());
    }

    #[tokio::test]
    async fn write_before_read() {
        let swmr = SingleWriteMultiRead::new();
        swmr.write(42).unwrap();
        assert_eq!(swmr.read(TIMEOUT).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn write_after_read() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        let handle = {
            let swmr = Arc::clone(&swmr);
            #[allow(clippy::disallowed_methods)]
            tokio::spawn(async move { swmr.read(TIMEOUT).await.unwrap() })
        };
        swmr.write(99).unwrap();
        assert_eq!(handle.await.unwrap(), 99);
    }

    #[tokio::test]
    async fn double_write_before_read_replaces_value() {
        let swmr = SingleWriteMultiRead::new();
        swmr.write(42).unwrap();
        swmr.write(99).unwrap();
        assert_eq!(swmr.read(TIMEOUT).await.unwrap(), 99);
    }

    #[tokio::test]
    async fn double_write_after_read_fails() {
        let swmr = SingleWriteMultiRead::new();
        swmr.write(42).unwrap();
        assert_eq!(swmr.read(TIMEOUT).await.unwrap(), 42);
        assert!(matches!(
            swmr.write(99),
            Err(SingleWriteMultiReadError::AlreadyWritten)
        ));
    }

    #[tokio::test]
    async fn read_times_out_when_no_write() {
        let swmr = SingleWriteMultiRead::<i32>::new();
        let err = swmr.read(SHORT_TIMEOUT).await.unwrap_err();
        assert!(matches!(err, SingleWriteMultiReadError::Timeout));
    }

    #[tokio::test]
    async fn read_after_write_with_many_concurrent_readers() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        swmr.write(55).unwrap();
        let mut handles = Vec::new();
        for _ in 0..5 {
            let swmr = Arc::clone(&swmr);
            #[allow(clippy::disallowed_methods)]
            handles.push(tokio::spawn(
                async move { swmr.read(TIMEOUT).await.unwrap() },
            ));
        }
        for handle in handles {
            assert_eq!(handle.await.unwrap(), 55);
        }
    }

    #[tokio::test]
    async fn write_after_read_multiple_readers() {
        let swmr = Arc::new(SingleWriteMultiRead::new());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let swmr = Arc::clone(&swmr);
            #[allow(clippy::disallowed_methods)]
            handles.push(tokio::spawn(
                async move { swmr.read(TIMEOUT).await.unwrap() },
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        swmr.write(7).unwrap();
        for handle in handles {
            assert_eq!(handle.await.unwrap(), 7);
        }
    }

    #[tokio::test]
    async fn older_reserved_generation_does_not_overwrite_newer_generation() {
        let swmr = SingleWriteMultiRead::new();
        let gen1 = swmr.reserve_write();
        let gen2 = swmr.reserve_write();

        // Even if the newer attempt finishes first (or second), the older attempt must be ignored.
        assert!(swmr.write_generation(gen2, 200).unwrap());
        assert!(!swmr.write_generation(gen1, 100).unwrap());
        assert_eq!(swmr.read(TIMEOUT).await.unwrap(), 200);
    }
}
