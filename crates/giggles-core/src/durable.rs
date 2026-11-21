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

#[derive(Clone, Copy)]
pub enum DurabilityKind { Local }
pub(crate) fn wait(release_admission: &mut dyn FnMut()) -> Option<Box<dyn Send>> {
    if crate::relational::spill::durability_output_spilled() { return None; }
    crate::Cancellation::current()?.durability_wait(DurabilityKind::Local, release_admission)
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
    #[test]
    fn declined_wait_retains_core_admission() {
        let context = crate::Cancellation::default();
        let registration = context.register_durability_wait(|_, _| None);
        let mut released = false;
        context.scope(|| {
            assert!(wait(&mut || released = true).is_none());
        });
        assert!(!released);
        drop(registration);
        let _registration = context.register_durability_wait(|_, release_admission| {
            release_admission();
            Some(Box::new(()))
        });
        context.scope(|| {
            assert!(wait(&mut || released = true).is_some());
        });
        assert!(released);
    }
}
