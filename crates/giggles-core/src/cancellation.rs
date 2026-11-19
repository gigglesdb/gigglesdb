//! Cooperative trusted synchronous request scope. Never hold a scope across await.
use crate::CoreError;
use std::{
    cell::RefCell,
    sync::{
        Arc, Condvar, LockResult, Mutex, MutexGuard, PoisonError, TryLockError,
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
    lock_timeout: Mutex<Option<Duration>>,
}
thread_local! { static CURRENT: RefCell<Option<Cancellation>> = const { RefCell::new(None) }; }
impl Cancellation {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self(Arc::new(Context {
            state: AtomicU8::new(0),
            deadline: Mutex::new(deadline),
            lock_timeout: Mutex::new(None),
        }))
    }
    pub fn current() -> Option<Self> {
        CURRENT.with(|c| c.borrow().clone())
    }
    /// Sets a per-acquisition lock timeout for this synchronous request.
    pub fn set_lock_timeout(&self, timeout: Option<Duration>) {
        *self.0.lock_timeout.lock().expect("lock timeout") = timeout;
    }
    pub(crate) fn lock_deadline() -> Option<Instant> {
        Self::current().and_then(|c| *c.0.lock_timeout.lock().expect("lock timeout"))
            .and_then(|duration| Instant::now().checked_add(duration))
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
    pub(crate) fn wait_slice(&self) -> Duration {
        const SLICE: Duration = Duration::from_millis(5);
        self.0
            .deadline
            .lock()
            .expect("deadline lock")
            .map_or(SLICE, |at| {
                SLICE.min(at.saturating_duration_since(Instant::now()))
            })
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
/// Installs `context` as the current invocation until the returned value
/// drops, which restores the previous one (LIFO, like `Cancellation::scope`).
pub(crate) fn install(context: Cancellation) -> Installed {
    Installed(Some(CURRENT.with(|c| c.replace(Some(context)))))
}
pub(crate) struct Installed(Option<Option<Cancellation>>);
impl Drop for Installed {
    fn drop(&mut self) {
        if let Some(previous) = self.0.take() {
            CURRENT.with(|c| {
                c.replace(previous);
            });
        }
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
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}

/// A blocking mutex with bounded cancellation checks while contended.
#[derive(Debug)]
pub(crate) struct WaitMutex<T> {
    inner: Mutex<T>,
    aux: Mutex<()>,
    cv: Condvar,
}

impl<T: Default> Default for WaitMutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[derive(Debug)]
pub(crate) struct WaitGuard<'a, T> {
    guard: Option<MutexGuard<'a, T>>,
    mutex: &'a WaitMutex<T>,
}

impl<T> std::ops::Deref for WaitGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref().expect("live guard")
    }
}
impl<T> std::ops::DerefMut for WaitGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.as_mut().expect("live guard")
    }
}
impl<'a, T> WaitGuard<'a, T> {
    /// Atomically release the data lock and wait for another unlock (or a slice).
    pub(crate) fn wait(mut self) -> Result<WaitGuard<'a, T>, CoreError> {
        let mutex = self.mutex;
        let aux = mutex.aux.lock().map_err(|_| CoreError::Fenced)?;
        drop(self.guard.take());
        mutex.cv.notify_all();
        let slice = Cancellation::current().map_or(Duration::from_millis(5), |c| c.wait_slice());
        let (aux, _) = mutex
            .cv
            .wait_timeout(aux, slice)
            .map_err(|_| CoreError::Fenced)?;
        drop(aux);
        mutex.lock_cancellable()
    }
}
impl<T> Drop for WaitGuard<'_, T> {
    fn drop(&mut self) {
        // Release data before taking aux: a waiter holds aux while trying inner.
        let Some(guard) = self.guard.take() else {
            return;
        };
        drop(guard);
        // Even a panicking callback must not prevent subsequent notifications.
        let _aux = self
            .mutex
            .aux
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.mutex.cv.notify_all();
    }
}

impl<T> WaitMutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            inner: Mutex::new(value),
            aux: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    fn guard<'a>(&'a self, guard: MutexGuard<'a, T>) -> WaitGuard<'a, T> {
        WaitGuard {
            guard: Some(guard),
            mutex: self,
        }
    }

    /// Plain blocking lock, preserving std's poison recovery semantics.
    #[allow(dead_code)] // Compatibility API; production callers use cancellable locking.
    pub(crate) fn lock(&self) -> LockResult<WaitGuard<'_, T>> {
        match self.inner.lock() {
            Ok(guard) => Ok(self.guard(guard)),
            Err(error) => Err(PoisonError::new(self.guard(error.into_inner()))),
        }
    }

    pub(crate) fn lock_cancellable(&self) -> Result<WaitGuard<'_, T>, CoreError> {
        self.lock_notified(|| {})
    }

    pub(crate) fn lock_notified(
        &self,
        mut waiting: impl FnMut(),
    ) -> Result<WaitGuard<'_, T>, CoreError> {
        check()?;
        match self.inner.try_lock() {
            Ok(guard) => return Ok(self.guard(guard)),
            Err(TryLockError::Poisoned(_)) => return Err(CoreError::Fenced),
            Err(TryLockError::WouldBlock) => {}
        }
        let mut notified = false;
        let mut aux = self.aux.lock().map_err(|_| CoreError::Fenced)?;
        loop {
            check()?;
            match self.inner.try_lock() {
                Ok(guard) => return Ok(self.guard(guard)),
                Err(TryLockError::Poisoned(_)) => return Err(CoreError::Fenced),
                Err(TryLockError::WouldBlock) => {
                    if !notified {
                        notified = true;
                        waiting();
                    }
                    let slice = Cancellation::current()
                        .map_or(Duration::from_millis(5), |c| c.wait_slice());
                    aux = self
                        .cv
                        .wait_timeout(aux, slice)
                        .map_err(|_| CoreError::Fenced)?
                        .0;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guard_wait_has_no_lost_unlock() {
        for _ in 0..100 {
            let mutex = Arc::new(WaitMutex::new(false));
            let mut guard = mutex.lock_cancellable().unwrap();
            let other = mutex.clone();
            let worker = std::thread::spawn(move || {
                *other.lock_cancellable().unwrap() = true;
            });
            while !*guard {
                guard = guard.wait().unwrap();
            }
            drop(guard);
            worker.join().unwrap();
        }
    }
    #[test]
    fn guard_wait_is_cancellable() {
        let mutex = WaitMutex::new(());
        let guard = mutex.lock().unwrap();
        let c = Cancellation::new(Some(Instant::now()));
        c.scope(|| assert_eq!(guard.wait().map(|_| ()), Err(CoreError::Cancelled)));
        assert!(mutex.lock_cancellable().is_ok());
    }
    #[test]
    fn wait_mutex_wakes_on_unlock_and_notifies_once() {
        let mutex = Arc::new(WaitMutex::new(()));
        let guard = mutex.lock().unwrap();
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = mutex.clone();
        let job = std::thread::spawn(move || {
            let mut calls = 0;
            let _guard = waiter
                .lock_notified(|| {
                    calls += 1;
                    waiting_tx.send(()).unwrap();
                })
                .unwrap();
            done_tx.send((Instant::now(), calls)).unwrap();
        });
        waiting_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Hold for 300 ms without a sleep or busy loop.
        assert!(done_rx.recv_timeout(Duration::from_millis(300)).is_err());
        let released = Instant::now();
        drop(guard);
        let (acquired, calls) = done_rx.recv_timeout(Duration::from_millis(50)).unwrap();
        assert!(acquired.duration_since(released) < Duration::from_millis(50));
        assert_eq!(calls, 1);
        job.join().unwrap();
    }

    #[test]
    fn wait_mutex_cancellation_leaves_holder_untouched() {
        let mutex = Arc::new(WaitMutex::new(42));
        let guard = mutex.lock().unwrap();
        let cancellation = Cancellation::default();
        let other = cancellation.clone();
        let waiter = mutex.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let job = std::thread::spawn(move || {
            other.scope(|| {
                let result = waiter.lock_notified(|| tx.send(()).unwrap()).map(|_| ());
                done_tx.send(result).unwrap();
            })
        });
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let started = Instant::now();
        assert!(cancellation.cancel());
        assert_eq!(
            done_rx.recv_timeout(Duration::from_millis(200)).unwrap(),
            Err(CoreError::Cancelled)
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        assert_eq!(*guard, 42);
        drop(guard);
        assert_eq!(*mutex.lock().unwrap(), 42);
        job.join().unwrap();
    }

    #[test]
    fn wait_mutex_expired_deadline() {
        let mutex = WaitMutex::new(());
        let c = Cancellation::new(Some(Instant::now()));
        c.scope(|| {
            assert_eq!(
                mutex.lock_cancellable().map(|_| ()),
                Err(CoreError::Cancelled)
            )
        });
    }

    #[test]
    fn wait_mutex_poison_is_fenced() {
        let mutex = Arc::new(WaitMutex::new(7));
        let other = mutex.clone();
        assert!(
            std::thread::spawn(move || {
                let _guard = other.lock().unwrap();
                panic!("poison inner mutex");
            })
            .join()
            .is_err()
        );
        assert_eq!(mutex.lock_cancellable().map(|_| ()), Err(CoreError::Fenced));
        assert_eq!(**mutex.lock().unwrap_err().get_ref(), 7);
    }

    #[test]
    fn wait_mutex_contended_counter_is_exact() {
        let mutex = Arc::new(WaitMutex::<u64>::default());
        let barrier = Arc::new(std::sync::Barrier::new(32));
        let jobs: Vec<_> = (0..32)
            .map(|_| {
                let mutex = mutex.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..1000 {
                        *mutex.lock_cancellable().unwrap() += 1u64;
                    }
                })
            })
            .collect();
        for job in jobs {
            job.join().unwrap();
        }
        assert_eq!(*mutex.lock().unwrap(), 32_000);
    }

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
