//! Certificate-bound live backend registration; cancellation never waits for blocking admission.
use giggles_core::Cancellation;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
pub(crate) struct Registry(Mutex<Entries>);
#[derive(Default)]
struct Entries {
    used: BTreeSet<u32>,
    live: BTreeMap<u32, Weak<Backend>>,
}
pub(crate) struct Backend {
    pub(crate) pid: u32,
    pub(crate) secret: u32,
    fingerprint: String,
    epoch: u64,
    open: AtomicBool,
    active: Mutex<Option<Cancellation>>,
}
impl Registry {
    pub(crate) fn register(&self, fingerprint: &str, epoch: u64) -> io::Result<Arc<Backend>> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| io::Error::other("backend registry poisoned"))?;
        // Bound historical anti-reuse state. Exhaustion fails closed until service restart.
        if entries.used.len() >= 65_536 {
            return Err(io::Error::other("backend key lifetime exhausted"));
        }
        loop {
            let mut bytes = [0; 8];
            getrandom::fill(&mut bytes).map_err(io::Error::other)?;
            let pid = u32::from_be_bytes(bytes[..4].try_into().unwrap());
            if !entries.used.insert(pid) {
                continue;
            }
            let backend = Arc::new(Backend {
                pid,
                secret: u32::from_be_bytes(bytes[4..].try_into().unwrap()),
                fingerprint: fingerprint.into(),
                epoch,
                open: AtomicBool::new(true),
                active: Mutex::new(None),
            });
            entries.live.retain(|_, v| v.strong_count() > 0);
            entries.live.insert(pid, Arc::downgrade(&backend));
            return Ok(backend);
        }
    }
    pub(crate) fn cancel(&self, fingerprint: &str, epoch: u64, pid: u32, secret: u32) {
        let Ok(entries) = self.0.lock() else { return };
        let Some(backend) = entries.live.get(&pid).and_then(Weak::upgrade) else {
            return;
        };
        if backend.secret != secret || backend.fingerprint != fingerprint || backend.epoch != epoch
        {
            return;
        }
        if let Ok(active) = backend.active.lock()
            && backend.open.load(Ordering::Acquire)
            && let Some(context) = active.as_ref()
        {
            context.cancel();
        }
    }
}
impl Backend {
    pub(crate) fn close(&self) {
        let _active = self.active.lock().expect("backend lock");
        self.open.store(false, Ordering::Release);
    }
    pub(crate) fn begin(self: &Arc<Self>, cancellable: bool) -> Active {
        let context = Cancellation::default();
        *self.active.lock().expect("backend lock") = cancellable.then(|| context.clone());
        Active {
            backend: self.clone(),
            context,
        }
    }
}
pub(crate) struct Active {
    backend: Arc<Backend>,
    pub(crate) context: Cancellation,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.backend.active.lock().expect("backend lock").take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn key_lifecycle_idle_wrong_binding_closed_and_fresh_context() {
        let registry = Registry::default();
        let backend = registry.register("certificate-A", 1).unwrap();
        registry.cancel("certificate-A", 1, backend.pid, backend.secret); // idle
        let first = backend.begin(true);
        registry.cancel("certificate-B", 1, backend.pid, backend.secret);
        registry.cancel("certificate-A", 1, backend.pid, backend.secret ^ 1);
        assert!(first.context.check().is_ok());
        registry.cancel("certificate-A", 1, backend.pid, backend.secret);
        assert_eq!(
            first.context.check(),
            Err(giggles_core::CoreError::Cancelled)
        );
        drop(first);
        let next = backend.begin(true);
        assert!(next.context.check().is_ok());
        backend.close();
        registry.cancel("certificate-A", 1, backend.pid, backend.secret);
        assert!(next.context.check().is_ok());
        let another_authority = Registry::default();
        another_authority.cancel("certificate-A", 1, backend.pid, backend.secret);
        assert!(next.context.check().is_ok());
        drop(next);
        let old_pid = backend.pid;
        drop(backend);
        let replacement = registry.register("certificate-A", 1).unwrap();
        assert_ne!(replacement.pid, old_pid);
        assert_eq!(registry.0.lock().unwrap().live.len(), 1);
    }
    #[test]
    fn anti_reuse_history_is_bounded_and_fails_closed() {
        let registry = Registry::default();
        registry.0.lock().unwrap().used.extend(0..65_536);
        assert!(registry.register("certificate-A", 1).is_err());
    }
}
