//! Lease-checked transactions and exclusive shared-directory state ownership.
use super::{
    schema::Table,
    store::{Clock, ReadTxn, RedbStore, ScanRows, Store, StoreError, StoreMeta, WriteTxn},
};
use std::{
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
/// Current Lease term, present only while this pod is leader.
pub type LeadershipSource = Arc<dyn Fn() -> Option<u64> + Send + Sync>;
/// Initialization work after publishing a newly opened store.
pub type ReadyCallback = Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;
/// Cleanup work after releasing state ownership.
pub type ClosedCallback = Arc<dyn Fn() + Send + Sync>;
/// Exclusive database opener retried while leadership is held.
pub type StoreOpener = Arc<dyn Fn() -> Result<RedbStore, StoreError> + Send + Sync>;
enum Slot {
    Closed,
    Ready { store: Arc<RedbStore>, term: u64 },
}
static EMPTY_META: StoreMeta = StoreMeta {
    format_version: 0,
    deployment_id: [0; 16],
    created_unix_ms: 0,
};
/// Stable store proxy refusing transactions outside its opened Lease term.
pub struct LeaderStore {
    slot: RwLock<Slot>,
    fenced: AtomicBool,
    initialized: AtomicBool,
    initializer: Mutex<Option<std::thread::ThreadId>>,
    inflight: Mutex<usize>,
    drained: Condvar,
    leadership: LeadershipSource,
    clock: Clock,
    meta: OnceLock<StoreMeta>,
}
impl LeaderStore {
    /// Create a closed proxy with an injected Lease source and clock.
    pub fn new(leadership: LeadershipSource, clock: Clock) -> Self {
        Self {
            slot: RwLock::new(Slot::Closed),
            fenced: AtomicBool::new(true),
            initialized: AtomicBool::new(false),
            initializer: Mutex::new(None),
            inflight: Mutex::new(0),
            drained: Condvar::new(),
            leadership,
            clock,
            meta: OnceLock::new(),
        }
    }
    fn guard(&self) -> Result<(Arc<RedbStore>, Inflight<'_>, u64), StoreError> {
        if self.fenced.load(Ordering::SeqCst) { return Err(StoreError::Io); }
        let mut count = self.inflight.lock().map_err(|_| StoreError::Io)?;
        *count += 1;
        let permit = Inflight(self);
        drop(count);
        if self.fenced.load(Ordering::SeqCst) { return Err(StoreError::Io); }
        if !self.initialized.load(Ordering::SeqCst)
            && *self.initializer.lock().map_err(|_| StoreError::Io)? != Some(std::thread::current().id())
        { return Err(StoreError::Io); }
        let slot = self.slot.read().map_err(|_| StoreError::Io)?;
        match &*slot {
            Slot::Ready { store, term } if (self.leadership)() == Some(*term) =>
                Ok((store.clone(), permit, *term)),
            Slot::Ready { .. } | Slot::Closed => Err(StoreError::Io),
        }
    }
}
struct Inflight<'a>(&'a LeaderStore);
impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        let mut count = self.0.inflight.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *count -= 1;
        if *count == 0 { self.0.drained.notify_all(); }
    }
}
struct LeaderRead<'a> {
    inner: Box<dyn ReadTxn>,
    _permit: Inflight<'a>,
}
impl ReadTxn for LeaderRead<'_> {
    fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(t, key)
    }
    fn scan_prefix(&self, t: Table, prefix: &[u8]) -> Result<ScanRows, StoreError> {
        self.inner.scan_prefix(t, prefix)
    }
}
struct LeaderWrite<'a> {
    inner: Box<dyn WriteTxn>,
    _permit: Inflight<'a>,
    proxy: &'a LeaderStore,
    term: u64,
}
impl ReadTxn for LeaderWrite<'_> {
    fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(t, key)
    }
    fn scan_prefix(&self, t: Table, prefix: &[u8]) -> Result<ScanRows, StoreError> {
        self.inner.scan_prefix(t, prefix)
    }
}
impl WriteTxn for LeaderWrite<'_> {
    fn put(&mut self, t: Table, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.inner.put(t, key, value)
    }
    fn delete(&mut self, t: Table, key: &[u8]) -> Result<bool, StoreError> {
        self.inner.delete(t, key)
    }
    fn commit(self: Box<Self>) -> Result<(), StoreError> {
        if self.proxy.fenced.load(Ordering::SeqCst) || (self.proxy.leadership)() != Some(self.term)
        {
            return Err(StoreError::Io);
        }
        self.inner.commit()
    }
}
impl Store for LeaderStore {
    fn meta(&self) -> &StoreMeta {
        self.meta.get().unwrap_or(&EMPTY_META)
    }
    fn now_ms(&self) -> u64 {
        (self.clock)()
    }
    fn read(&self) -> Result<Box<dyn ReadTxn + '_>, StoreError> {
        let (store, permit, _) = self.guard()?;
        let inner = store.owned_read();
        drop(store);
        let inner = inner?;
        Ok(Box::new(LeaderRead { inner, _permit: permit }))
    }
    fn write(&self) -> Result<Box<dyn WriteTxn + '_>, StoreError> {
        let (store, permit, term) = self.guard()?;
        let inner = store.owned_write();
        drop(store);
        let inner = inner?;
        Ok(Box::new(LeaderWrite {
            inner,
            _permit: permit,
            proxy: self,
            term,
        }))
    }
}
/// Serialized opening, initialization and fencing of shared state.
pub struct StateHolder {
    proxy: Arc<LeaderStore>,
    leadership: LeadershipSource,
    opener: StoreOpener,
    lifecycle: Mutex<()>,
    on_ready: Mutex<Vec<ReadyCallback>>,
    on_closed: Mutex<Vec<ClosedCallback>>,
    attempts: AtomicU64,
    readiness: Mutex<(bool, bool)>,
}
impl StateHolder {
    /// Create the lifecycle owner for a closed proxy.
    pub fn new(proxy: Arc<LeaderStore>, leadership: LeadershipSource, opener: StoreOpener) -> Self {
        Self {
            proxy,
            leadership,
            opener,
            lifecycle: Mutex::new(()),
            on_ready: Mutex::default(),
            on_closed: Mutex::default(),
            attempts: AtomicU64::new(0),
            readiness: Mutex::new((false, false)),
        }
    }
    /// Append initialization work invoked in order without the slot lock.
    pub fn on_ready(&self, callback: ReadyCallback) {
        self.on_ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(callback);
    }
    /// Append cleanup work invoked after dropping the database.
    pub fn on_closed(&self, callback: ClosedCallback) {
        self.on_closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(callback);
    }
    /// Count all open attempts, including failures.
    pub fn open_attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }
    /// Return the initialized term only while the Lease still matches.
    pub fn is_ready(&self) -> Option<u64> {
        if !self.readiness.lock().unwrap_or_else(std::sync::PoisonError::into_inner).0 { return None; }
        self.proxy.guard().ok().map(|(_, _, term)| term)
    }
    /// Mark deferred configuration for initialization, including a concurrent ready callback.
    pub fn defer_config_if_closed(&self) -> bool {
        let mut readiness = self.readiness.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !readiness.0 || self.proxy.guard().is_err() {
            readiness.1 = true;
            true
        } else { false }
    }
    fn fence(&self) {
        self.readiness.lock().unwrap_or_else(std::sync::PoisonError::into_inner).0 = false;
        self.proxy.fenced.store(true, Ordering::SeqCst);
        self.proxy.initialized.store(false, Ordering::SeqCst);
        *self.proxy.initializer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let mut count = self.proxy.inflight.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while *count != 0 {
            count = self.proxy.drained.wait(count).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(count);
        let closed = {
            let mut slot = self
                .proxy
                .slot
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let was_ready = matches!(*slot, Slot::Ready { .. });
            *slot = Slot::Closed;
            was_ready
        };
        if closed {
            let callbacks = self
                .on_closed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            for callback in callbacks {
                callback();
            }
        }
    }
    /// Reconcile ownership with the Lease, retrying failed opens next tick.
    pub fn tick(&self) {
        let _lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let term = {
            let slot = self
                .proxy
                .slot
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*slot {
                Slot::Closed => None,
                Slot::Ready { term, .. } => Some(*term),
            }
        };
        if let Some(term) = term {
            if (self.leadership)() != Some(term) {
                self.fence();
            }
            return;
        }
        let Some(term) = (self.leadership)() else {
            return;
        };
        self.attempts.fetch_add(1, Ordering::Relaxed);
        let Ok(store) = (self.opener)() else {
            return;
        };
        if self
            .proxy
            .meta
            .get()
            .is_some_and(|m| m.deployment_id != store.meta().deployment_id)
        {
            eprintln!("state store deployment continuity mismatch");
            return;
        }
        if (self.leadership)() != Some(term) {
            return;
        }
        let meta = store.meta();
        let _ = self.proxy.meta.set(StoreMeta {
            format_version: meta.format_version,
            deployment_id: meta.deployment_id,
            created_unix_ms: meta.created_unix_ms,
        });
        {
            let mut slot = self
                .proxy
                .slot
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *slot = Slot::Ready {
                store: Arc::new(store),
                term,
            };
            self.proxy.fenced.store(false, Ordering::SeqCst);
        }
        *self.proxy.initializer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::thread::current().id());
        self.readiness.lock().unwrap_or_else(std::sync::PoisonError::into_inner).1 = false;
        let callbacks = self
            .on_ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        loop {
            for callback in &callbacks {
                if callback().is_err() {
                    self.fence();
                    return;
                }
            }
            if (self.leadership)() != Some(term) {
                self.fence();
                return;
            }
            let mut readiness = self.readiness.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if readiness.1 {
                readiness.1 = false;
                continue;
            }
            self.proxy.initialized.store(true, Ordering::SeqCst);
            *self.proxy.initializer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            readiness.0 = true;
            break;
        }
    }
    /// Fence transactions and release the database before releasing the Lease.
    pub fn close(&self) {
        let _lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.fence();
    }
    /// Reconcile on blocking workers periodically and close on shutdown.
    pub async fn run(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        period: Duration,
    ) {
        let mut interval = tokio::time::interval(period);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
                _ = interval.tick() => {
                    let holder = self.clone();
                    if tokio::task::spawn_blocking(move || holder.tick()).await.is_err() { break; }
                }
            }
        }
        let holder = self.clone();
        let _ = tokio::task::spawn_blocking(move || holder.close()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        state::{
            authority::{Authority, SessionLimits},
            model::PrincipalKind,
        },
        token::keys::RootKey,
    };
    use std::{
        path::{Path, PathBuf},
        sync::mpsc,
    };
    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let mut id = [0; 16];
            getrandom::fill(&mut id).unwrap();
            let path = PathBuf::from(std::env::var_os("TEMP").unwrap()).join(format!(
                "leader-store-{}",
                data_encoding::HEXLOWER.encode(&id)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn open(path: &Path) -> Result<RedbStore, StoreError> {
        RedbStore::open(path, &RootKey::from_hex(&"01".repeat(32)).unwrap())
    }
    fn holder(path: &Path, term: u64) -> (Arc<LeaderStore>, Arc<StateHolder>, Arc<AtomicU64>) {
        let value = Arc::new(AtomicU64::new(term));
        let source: LeadershipSource = {
            let v = value.clone();
            Arc::new(move || match v.load(Ordering::SeqCst) {
                0 => None,
                term => Some(term),
            })
        };
        let proxy = Arc::new(LeaderStore::new(source.clone(), Arc::new(|| 42)));
        let path = path.to_owned();
        let holder = Arc::new(StateHolder::new(
            proxy.clone(),
            source,
            Arc::new(move || open(&path)),
        ));
        (proxy, holder, value)
    }
    fn put(proxy: &LeaderStore, key: &[u8]) {
        let mut tx = proxy.write().unwrap();
        tx.put(Table::Sessions, key, b"opaque").unwrap();
        tx.commit().unwrap();
    }
    #[test]
    fn split_view_lock_and_handoff_preserve_rows() {
        let dir = TestDir::new();
        let (a, ha, term) = holder(&dir.0, 1);
        let (b, hb, _) = holder(&dir.0, 2);
        assert_eq!(a.meta().deployment_id, [0; 16]);
        assert_eq!(a.now_ms(), 42);
        ha.tick();
        put(&a, b"row");
        assert!(matches!(open(&dir.0), Err(StoreError::Owned)));
        hb.tick();
        hb.tick();
        assert_eq!(hb.open_attempts(), 2);
        assert_eq!(hb.is_ready(), None);
        assert!(b.read().is_err());
        assert!(b.write().is_err());
        term.store(0, Ordering::SeqCst);
        assert!(a.read().is_err());
        assert!(a.write().is_err());
        ha.tick();
        hb.tick();
        assert_eq!(hb.is_ready(), Some(2));
        assert_eq!(
            b.read().unwrap().get(Table::Sessions, b"row").unwrap(),
            Some(b"opaque".to_vec())
        );
        assert_eq!(a.meta().deployment_id, b.meta().deployment_id);
        ha.close();
        hb.close();
    }
    #[test]
    fn deposed_write_is_aborted_for_missing_or_changed_term() {
        for next in [0, 2] {
            let dir = TestDir::new();
            let (a, ha, term) = holder(&dir.0, 1);
            ha.tick();
            let mut tx = a.write().unwrap();
            tx.put(Table::Sessions, b"aborted", b"opaque").unwrap();
            term.store(next, Ordering::SeqCst);
            assert!(tx.commit().is_err());
            ha.tick();
            let (b, hb, _) = holder(&dir.0, 3);
            hb.tick();
            assert_eq!(
                b.read().unwrap().get(Table::Sessions, b"aborted").unwrap(),
                None
            );
            hb.close();
        }
    }
    #[test]
    fn term_change_closes_then_reopens_next_tick() {
        let dir = TestDir::new();
        let (a, h, term) = holder(&dir.0, 1);
        h.tick();
        let id = a.meta().deployment_id;
        term.store(2, Ordering::SeqCst);
        h.tick();
        assert_eq!(h.is_ready(), None);
        assert!(a.read().is_err());
        h.tick();
        assert_eq!(h.is_ready(), Some(2));
        assert_eq!(a.meta().deployment_id, id);
        h.close();
        h.close();
    }
    #[test]
    fn fence_waits_for_inflight_read_on_another_thread() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        h.tick();
        let (started, ready) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let tx = a.read().unwrap();
            started.send(()).unwrap();
            wait.recv().unwrap();
            drop(tx);
        });
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let (done, closed) = mpsc::channel();
        let closer = h.clone();
        let close = std::thread::spawn(move || {
            closer.close();
            done.send(()).unwrap();
        });
        assert!(closed.recv_timeout(Duration::from_millis(100)).is_err());
        release.send(()).unwrap();
        closed.recv_timeout(Duration::from_secs(5)).unwrap();
        reader.join().unwrap();
        close.join().unwrap();
        assert_eq!(h.is_ready(), None);
        drop(open(&dir.0).unwrap());
    }
    #[test]
    fn nested_begin_during_fence_is_refused_without_waiting() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        h.tick();
        let tx = a.read().unwrap();
        let closing = h.clone();
        let close = std::thread::spawn(move || closing.close());
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !a.fenced.load(Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(a.read().is_err());
        assert!(a.write().is_err());
        drop(tx);
        close.join().unwrap();
        assert_eq!(h.is_ready(), None);
    }
    #[test]
    fn fence_flag_aborts_write_even_with_unchanged_lease() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        h.tick();
        let mut tx = a.write().unwrap();
        tx.put(Table::Sessions, b"row", b"opaque").unwrap();
        let closing = h.clone();
        let close = std::thread::spawn(move || closing.close());
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !a.fenced.load(Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(tx.commit().is_err());
        close.join().unwrap();
        h.tick();
        assert_eq!(
            a.read().unwrap().get(Table::Sessions, b"row").unwrap(),
            None
        );
        h.close();
    }
    #[test]
    fn changed_deployment_is_refused_and_releases_lock() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        h.tick();
        let id = a.meta().deployment_id;
        h.close();
        std::fs::remove_dir_all(&dir.0).unwrap();
        h.tick();
        assert_eq!(h.is_ready(), None);
        assert!(a.write().is_err());
        assert_eq!(a.meta().deployment_id, id);
        drop(open(&dir.0).unwrap());
    }
    #[test]
    fn initialization_refuses_external_duplicate_before_reload() {
        use crate::state::authority::AuthorityError;
        let dir = TestDir::new();
        {
            let store = Arc::new(open(&dir.0).unwrap());
            let authority = Authority::load(store, SessionLimits::default()).unwrap();
            authority.create_principal("existing", PrincipalKind::Agent, false, "disk").unwrap();
        }
        let (proxy, h, _) = holder(&dir.0, 1);
        let authority = Authority::empty(proxy.clone(), SessionLimits::default());
        let (started, entered) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let wait = Mutex::new(wait);
        let a = authority.clone();
        h.on_ready(Arc::new(move || {
            started.send(()).unwrap();
            wait.lock().unwrap().recv().unwrap();
            a.reload().map_err(|_| StoreError::Io)
        }));
        let holder = h.clone();
        let task = std::thread::spawn(move || holder.tick());
        entered.recv().unwrap();
        assert_eq!(h.is_ready(), None);
        assert!(proxy.read().is_err());
        assert!(matches!(authority.create_principal("existing", PrincipalKind::Agent, false, "external"), Err(AuthorityError::Store)));
        release.send(()).unwrap();
        task.join().unwrap();
        assert_eq!(h.is_ready(), Some(1));
        assert!(matches!(authority.create_principal("existing", PrincipalKind::Agent, false, "external"), Err(AuthorityError::NameTaken)));
        authority.reload().unwrap();
        authority.reload().unwrap();
        h.close();
    }
    #[test]
    fn initialization_stays_unready_and_retries_concurrent_deferred_config() {
        let dir = TestDir::new();
        let (_, h, _) = holder(&dir.0, 1);
        let calls = Arc::new(AtomicU64::new(0));
        let observed = calls.clone();
        let weak = Arc::downgrade(&h);
        h.on_ready(Arc::new(move || {
            let holder = weak.upgrade().unwrap();
            assert_eq!(holder.is_ready(), None);
            if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                assert!(holder.defer_config_if_closed());
            }
            Ok(())
        }));
        h.tick();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(h.is_ready(), Some(1));
        assert!(!h.defer_config_if_closed());
        h.close();
    }
    #[test]
    fn callbacks_are_ordered_can_use_store_and_failure_closes() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        let order = Arc::new(Mutex::new(Vec::new()));
        for i in 0..3 {
            let order = order.clone();
            let a = a.clone();
            h.on_ready(Arc::new(move || {
                let _tx = a.read()?;
                order.lock().unwrap().push(i);
                if i == 1 { Err(StoreError::Io) } else { Ok(()) }
            }));
        }
        let events = order.clone();
        let a = a.clone();
        h.on_closed(Arc::new(move || {
            assert!(a.read().is_err());
            events.lock().unwrap().push(3);
        }));
        h.tick();
        assert_eq!(h.is_ready(), None);
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 3]);
        h.close();
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 3]);
        drop(open(&dir.0).unwrap());
    }
    #[test]
    fn failed_opener_is_retried_and_lost_lease_during_open_stays_closed() {
        let source: LeadershipSource = Arc::new(|| Some(1));
        let a = Arc::new(LeaderStore::new(source.clone(), Arc::new(|| 0)));
        let h = StateHolder::new(a.clone(), source, Arc::new(|| Err(StoreError::Io)));
        h.tick();
        h.tick();
        assert_eq!(h.open_attempts(), 2);
        assert!(a.read().is_err());
        let dir = TestDir::new();
        let (a, h, term) = holder(&dir.0, 1);
        let path = dir.0.clone();
        let source = h.leadership.clone();
        let h = StateHolder::new(
            a.clone(),
            source,
            Arc::new(move || {
                let store = open(&path)?;
                term.store(0, Ordering::SeqCst);
                Ok(store)
            }),
        );
        h.tick();
        assert_eq!(h.is_ready(), None);
        drop(open(&dir.0).unwrap());
    }
    #[test]
    fn authority_reload_and_clear_invalidate_cursors() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        h.tick();
        let authority = Authority::load(a.clone(), SessionLimits::default()).unwrap();
        let initial = authority.peer_changes(0, 1);
        h.close();
        {
            let other =
                Authority::load(Arc::new(open(&dir.0).unwrap()), SessionLimits::default()).unwrap();
            other
                .create_principal("agent", PrincipalKind::Agent, false, "key")
                .unwrap();
        }
        assert!(authority.principal_by_name("agent").is_none());
        h.tick();
        authority.reload().unwrap();
        assert!(authority.principal_by_name("agent").is_some());
        let reload = authority.peer_changes(initial.next, 1);
        assert!(reload.reset);
        assert_ne!(reload.next, initial.next);
        let mut tx = a.write().unwrap();
        tx.put(Table::Principals, b"malformed", b"{}").unwrap();
        tx.commit().unwrap();
        assert!(authority.reload().is_err());
        assert!(authority.principal_by_name("agent").is_some());
        h.close();
        authority.clear();
        assert!(authority.principal_by_name("agent").is_none());
        let clear = authority.peer_changes(reload.next, 1);
        assert!(clear.reset);
        assert_ne!(clear.next, reload.next);
    }
    #[tokio::test]
    async fn run_closes_on_shutdown() {
        let dir = TestDir::new();
        let (a, h, _) = holder(&dir.0, 1);
        h.tick();
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(h.clone().run(receiver, Duration::from_millis(10)));
        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(a.read().is_err());
        drop(open(&dir.0).unwrap());
    }
}
