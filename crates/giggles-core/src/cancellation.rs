//! Cooperative trusted synchronous request scope. Never hold a scope across await.
use crate::CoreError;
use std::{
    cell::RefCell,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

/// A single invocation, not a durable operation ID. Clones share its decision.
#[derive(Clone, Default)]
pub struct Cancellation(Arc<Context>);
#[derive(Default)]
struct Context {
    state: AtomicU8,
    deadline: Mutex<Option<Instant>>,
}
thread_local! { static CURRENT: RefCell<Option<Cancellation>> = const { RefCell::new(None) }; }
impl Cancellation {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self(Arc::new(Context {
            state: AtomicU8::new(0),
            deadline: Mutex::new(deadline),
        }))
    }
    pub fn current() -> Option<Self> {
        CURRENT.with(|c| c.borrow().clone())
    }
    pub fn deadline(&self, at: Instant) {
        let mut d = self.0.deadline.lock().expect("deadline lock");
        *d = Some(d.map_or(at, |old| old.min(at)));
    }
    /// True only if cancellation wins before decision. Too late is never rollback.
    pub fn cancel(&self) -> bool {
        self.0
            .state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub fn check(&self) -> Result<(), CoreError> {
        if self
            .0
            .deadline
            .lock()
            .expect("deadline lock")
            .is_some_and(|d| Instant::now() >= d)
        {
            self.cancel();
        }
        if self.0.state.load(Ordering::Acquire) == 1 {
            Err(CoreError::Cancelled)
        } else {
            Ok(())
        }
    }
    fn decide(&self) -> Result<(), CoreError> {
        self.check()?;
        match self
            .0
            .state
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(2) => Ok(()),
            _ => Err(CoreError::Cancelled),
        }
    }
    /// Trusted synchronous scope; restores the previous context even on panic.
    /// An invocation must not be reused for another request or execute async work.
    pub fn scope<T>(&self, work: impl FnOnce() -> T) -> T {
        struct Restore(Option<Cancellation>);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT.with(|c| {
                    c.replace(self.0.take());
                });
            }
        }
        let _restore = Restore(CURRENT.with(|c| c.replace(Some(self.clone()))));
        work()
    }
}
pub fn check() -> Result<(), CoreError> {
    CURRENT.with(|c| c.borrow().as_ref().map_or(Ok(()), Cancellation::check))
}
pub(crate) fn decide() -> Result<(), CoreError> {
    CURRENT.with(|c| c.borrow().as_ref().map_or(Ok(()), Cancellation::decide))
}
pub fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, CoreError> {
    lock_notified(mutex, || {})
}
pub(crate) fn lock_notified<T>(
    mutex: &Mutex<T>,
    mut waiting: impl FnMut(),
) -> Result<MutexGuard<'_, T>, CoreError> {
    let mut notified = false;
    loop {
        check()?;
        match mutex.try_lock() {
            Ok(g) => return Ok(g),
            Err(TryLockError::Poisoned(_)) => return Err(CoreError::Fenced),
            Err(TryLockError::WouldBlock) => {
                if !notified {
                    notified = true;
                    waiting();
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancellation_and_decision_have_exactly_one_winner() {
        for _ in 0..256 {
            let c = Cancellation::default();
            let racer = c.clone();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let other = barrier.clone();
            let job = std::thread::spawn(move || {
                other.wait();
                racer.cancel()
            });
            barrier.wait();
            let decision = c.decide();
            let cancelled = job.join().unwrap();
            assert_eq!(cancelled, decision == Err(CoreError::Cancelled));
            assert_eq!(c.check().is_err(), cancelled);
            assert!(!c.cancel());
        }
    }
    #[test]
    fn nested_and_parallel_scopes_restore_the_correct_context() {
        let outer = Cancellation::default();
        let inner = Cancellation::default();
        inner.cancel();
        outer.scope(|| {
            assert!(check().is_ok());
            inner.scope(|| assert_eq!(check(), Err(CoreError::Cancelled)));
            assert!(check().is_ok());
            assert!(std::thread::spawn(check).join().unwrap().is_ok());
        });
        assert!(Cancellation::current().is_none());
    }
}
