//! Live completion bookkeeping shared by trusted request contexts and services.
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

#[derive(Default, Debug)]
pub struct DurableTracker(AtomicUsize);
impl DurableTracker {
    pub fn live(&self) -> usize { self.0.load(Ordering::Acquire) }
    pub fn register(self: &Arc<Self>) -> LiveToken {
        self.0.fetch_add(1, Ordering::AcqRel);
        LiveToken(self.clone())
    }
}
#[must_use]
pub struct LiveToken(Arc<DurableTracker>);
impl Drop for LiveToken {
    fn drop(&mut self) { self.0.0.fetch_sub(1, Ordering::AcqRel); }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_scope_tracks_until_token_drop_and_restores_after_unwind() {
        let tracker = Arc::new(DurableTracker::default());
        let context = crate::Cancellation::default();
        context.set_durable_tracker(tracker.clone());
        assert!(crate::Cancellation::current_durable_token().is_none());
        let result = std::panic::catch_unwind(|| context.scope(|| {
            let _token = crate::Cancellation::current_durable_token().unwrap();
            assert_eq!(tracker.live(), 1);
            panic!("qualification unwind");
        }));
        assert!(result.is_err());
        assert_eq!(tracker.live(), 0);
        assert!(crate::Cancellation::current_durable_token().is_none());
    }
}
