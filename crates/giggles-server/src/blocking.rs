//! Service-wide admission before submission to Tokio's blocking executor.
use std::{io, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const MAX_BLOCKING: usize = 32;

#[derive(Clone)]
pub(crate) struct Blocking(Arc<Semaphore>, #[cfg(test)] Arc<std::sync::atomic::AtomicUsize>);

impl Blocking {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Semaphore::new(MAX_BLOCKING)), #[cfg(test)] Arc::default())
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn test_waiters(&self) -> usize { self.1.load(std::sync::atomic::Ordering::SeqCst) }

    /// Permits held now (admitted and not yet returned).
    pub(crate) fn in_use(&self) -> usize {
        MAX_BLOCKING - self.0.available_permits()
    }

    /// Cancellation remains observable before any worker is submitted.
    pub(crate) async fn admit(
        &self,
        context: &giggles_core::Cancellation,
    ) -> Result<OwnedSemaphorePermit, giggles_core::CoreError> {
        context.check()?;
        // Tokio assigns released permits directly to FIFO queued waiters before
        // exposing any surplus to try_acquire, so this cannot overtake a waiter.
        // Check cancellation first, including when a permit is immediately free.
        if let Ok(permit) = self.0.clone().try_acquire_owned() {
            // As in the slow path: a cancel or deadline that lands while the
            // permit is taken drops it and never submits work.
            context.check()?;
            return Ok(permit);
        }
        #[cfg(test)]
        let _waiting = {
            struct Waiting(Arc<std::sync::atomic::AtomicUsize>);
            impl Drop for Waiting {
                fn drop(&mut self) { self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst); }
            }
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Waiting(self.1.clone())
        };
        let acquire = self.0.clone().acquire_owned();
        tokio::pin!(acquire);
        loop {
            context.check()?;
            tokio::select! {
                permit=&mut acquire => {
                    let permit=permit.map_err(|_| giggles_core::CoreError::Busy)?;
                    context.check()?;
                    return Ok(permit);
                }
                _=tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
            }
        }
    }
    pub(crate) async fn run_admitted<T: Send + 'static>(
        &self,
        permit: OwnedSemaphorePermit,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> io::Result<T> {
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(io::Error::other)
    }

    #[cfg(test)]
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> io::Result<T> {
        // Waiting connections retain one bounded frame each. Never submit a
        // blocking job before admission, or release its permit when the socket
        // task is aborted: started durable work may still finish afterwards.
        let permit = self
            .0
            .clone()
            .acquire_owned()
            .await
            .map_err(io::Error::other)?;
        self.run_admitted(permit, work).await
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "current_thread")]
    async fn admit_fast_path_cannot_overtake_queued_waiter() {
        let pool = super::Blocking(std::sync::Arc::new(tokio::sync::Semaphore::new(1)), Arc::default());
        let held = pool.0.clone().acquire_owned().await.unwrap();
        let context = giggles_core::Cancellation::new(None);
        let mut queued = Box::pin(pool.admit(&context));
        pending(queued.as_mut()).await;
        drop(held);
        // Poll the newcomer before polling the woken waiter. The released
        // permit must already belong to the waiter, not the free permit count.
        let mut newcomer = Box::pin(pool.admit(&context));
        pending(newcomer.as_mut()).await;
        let first = timeout(Duration::from_secs(2), queued)
            .await
            .unwrap()
            .unwrap();
        pending(newcomer.as_mut()).await;
        drop(first);
        let second = timeout(Duration::from_secs(2), newcomer)
            .await
            .unwrap()
            .unwrap();
        drop(second);
        assert_eq!(pool.0.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_admit_rejects_even_with_free_permit() {
        let pool = super::Blocking::new();
        let context = giggles_core::Cancellation::new(None);
        context.cancel();
        assert_eq!(
            pool.admit(&context).await.unwrap_err(),
            giggles_core::CoreError::Cancelled
        );
        assert_eq!(pool.0.available_permits(), super::MAX_BLOCKING);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn authenticated_cancellation_while_all_permits_held_never_submits_work() {
        let pool = super::Blocking::new();
        let held = pool
            .0
            .clone()
            .acquire_many_owned(super::MAX_BLOCKING as u32)
            .await
            .unwrap();
        let registry = crate::cancel::Registry::default();
        let backend = registry.register("bound-certificate", 1).unwrap();
        let active = backend.begin(true);
        let mut waiting = Box::pin(pool.admit(&active.context));
        pending(waiting.as_mut()).await;
        registry.cancel("bound-certificate", 1, backend.pid, backend.secret);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
                .await
                .unwrap()
                .unwrap_err(),
            giggles_core::CoreError::Cancelled
        );
        assert_eq!(pool.0.available_permits(), 0);
        drop(active);
        drop(held);
        let next = backend.begin(true);
        let permit = pool.admit(&next.context).await.unwrap();
        assert_eq!(pool.run_admitted(permit, || 42).await.unwrap(), 42);
        assert_eq!(pool.0.available_permits(), super::MAX_BLOCKING);
    }
    use super::*;
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::Poll,
        time::Duration,
    };
    use tokio::{sync::oneshot, time::timeout};

    // Poll admission exactly once, proving the job is waiting without a sleep.
    async fn pending<F: Future>(f: std::pin::Pin<&mut F>) {
        let mut f = f;
        std::future::poll_fn(|cx| {
            assert!(f.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_storage_does_not_starve_runtime_timer_or_socket_work() {
        let pool = Blocking::new();
        let (started, rx) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let job = tokio::spawn(async move {
            pool.run(move || {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                42
            })
            .await
        });
        timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
        let (mut a, mut b) = tokio::io::duplex(8);
        let io = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            a.write_all(b"live").await.unwrap();
        });
        use tokio::io::AsyncReadExt;
        let mut bytes = [0; 4];
        timeout(Duration::from_secs(2), b.read_exact(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(&bytes, b"live");
        assert!(!job.is_finished());
        release.send(()).unwrap();
        assert_eq!(job.await.unwrap().unwrap(), 42);
        io.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn all_service_permits_bound_submission_and_dropped_waiter_never_runs() {
        let pool = Blocking::new();
        let count = Arc::new(AtomicUsize::new(0));
        let mut releases = Vec::new();
        let mut jobs = Vec::new();
        for _ in 0..MAX_BLOCKING {
            let pool = pool.clone();
            let count = count.clone();
            let (started, rx) = oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            releases.push(release);
            jobs.push(tokio::spawn(async move {
                pool.run(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    started.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                })
                .await
            }));
            timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), MAX_BLOCKING);
        assert_eq!(pool.0.available_permits(), 0);
        let count2 = count.clone();
        let mut queued = Box::pin(pool.run(move || {
            count2.fetch_add(1000, Ordering::SeqCst);
        }));
        pending(queued.as_mut()).await;
        drop(queued);
        for release in releases {
            release.send(()).unwrap();
        }
        for job in jobs {
            job.await.unwrap().unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), MAX_BLOCKING);
        assert_eq!(pool.0.available_permits(), MAX_BLOCKING);
        assert_eq!(pool.run(|| 7).await.unwrap(), 7);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aborting_socket_waiter_retains_started_work_permit_until_actual_finish() {
        let pool = Blocking(Arc::new(Semaphore::new(1)), Arc::default());
        let worker = pool.clone();
        let (started, rx) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (finished, done) = oneshot::channel();
        let job = tokio::spawn(async move {
            worker
                .run(move || {
                    started.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                    finished.send(()).unwrap();
                })
                .await
        });
        rx.await.unwrap();
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert_eq!(pool.0.available_permits(), 0);
        let mut next = Box::pin(pool.run(|| 8));
        pending(next.as_mut()).await;
        release.send(()).unwrap();
        done.await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), next)
                .await
                .unwrap()
                .unwrap(),
            8
        );
        assert_eq!(pool.0.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panic_releases_admission_and_subsequent_work_succeeds() {
        let pool = Blocking(Arc::new(Semaphore::new(1)), Arc::default());
        assert!(
            pool.run(|| panic!("deliberate worker panic"))
                .await
                .is_err()
        );
        assert_eq!(pool.0.available_permits(), 1);
        assert_eq!(pool.run(|| "recovered").await.unwrap(), "recovered");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_deadline_uses_original_receipt_and_cannot_write() {
        use crate::{Binding, Request, Role, Service, clock::Clock};
        use giggles_core::{Admin, Limits};
        use std::{
            collections::BTreeSet,
            time::{Instant, SystemTime, UNIX_EPOCH},
        };
        let path = crate::test_support::fixture_root().join(format!(
            "queued-deadline-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let fingerprint = "11".repeat(32);
        let service = Service::new(
            admin.clone(),
            vec![Binding {
                fingerprint: fingerprint.clone(),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["app".into()]),
                },
            }],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let mut session = service.session(&fingerprint).unwrap();
        let pool = service.blocking.clone();
        let held = pool
            .0
            .clone()
            .acquire_many_owned(MAX_BLOCKING as u32)
            .await
            .unwrap();
        let received = Instant::now() - Duration::from_secs(1);
        let mut work = Box::pin(pool.run(move || {
            session.handle_received(
                Request::Deadline {
                    budget_ms: 500,
                    request: Box::new(Request::Sql {
                        sql: "INSERT INTO app (id) VALUES (1)".into(),
                        params: vec![],
                    }),
                },
                received,
            )
        }));
        pending(work.as_mut()).await;
        drop(held);
        assert_eq!(work.await.unwrap().code, "57014");
        assert!(
            admin
                .sql_session()
                .execute("SELECT id FROM app WHERE id = 1", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
        println!("retained_fixture={}", path.display());
    }
}
