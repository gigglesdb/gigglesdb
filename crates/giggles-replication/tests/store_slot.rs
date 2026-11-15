use giggles_core::{Limits, relational::Value, replicated as gr};
use giggles_replication::{
    error::Error,
    state::{AppliedPosition, StateMachine},
    store_slot::StoreSlot,
};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "store-slot-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn slot(&self, name: &str, domain: u8) -> Arc<StoreSlot> {
        Arc::new(StoreSlot::open_or_create(&self.0.join(name), [domain; 16]).unwrap())
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Local(Arc<StoreSlot>);
impl gr::BatchSink for Local {
    fn replicate(&self, batch: Vec<u8>) -> Result<(), gr::ReplicateError> {
        let index = self
            .0
            .applied()
            .map_err(|e| gr::ReplicateError::Unavailable(e.to_string()))?
            .map_or(1, |p| p.index + 1);
        self.0
            .apply(&batch, AppliedPosition { index, term: 1 })
            .map(|_| ())
            .map_err(|e| gr::ReplicateError::Unavailable(e.to_string()))
    }
}
fn admin(slot: &Arc<StoreSlot>) -> giggles_core::Admin {
    slot.current()
        .unwrap()
        .open_admin(Arc::new(Local(slot.clone())), Limits::default(), 64 << 20)
        .unwrap()
}
fn populate(slot: &Arc<StoreSlot>) {
    let admin = admin(slot);
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE t(id int PRIMARY KEY, v text)", &[], None)
        .unwrap();
    sql.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')", &[], None)
        .unwrap();
}
fn assert_rows(slot: &Arc<StoreSlot>) {
    let admin = admin(slot);
    let result = admin
        .sql_session()
        .execute("SELECT id, v FROM t ORDER BY id", &[], None)
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1), Value::Text("one".into())],
            vec![Value::Int(2), Value::Text("two".into())]
        ]
    );
}
#[test]
fn checkpoint_install_real_sql() {
    let root = Root::new();
    let a = root.slot("a", 7);
    populate(&a);
    let snapshot = root.0.join("snapshot");
    let at = a.checkpoint(&snapshot).unwrap();
    fs::create_dir(root.0.join("b")).unwrap();
    let b = root.slot("b", 7);
    assert_eq!(b.domain(), [7; 16]);
    assert_eq!(b.data_dir(), root.0.join("b"));
    assert_eq!(b.install(&snapshot).unwrap(), at);
    assert_eq!(b.applied().unwrap(), a.applied().unwrap());
    assert!(!snapshot.exists());
    b.flush().unwrap();
    assert_rows(&b);
}
#[test]
fn busy_install_preserves_store() {
    let root = Root::new();
    let slot = root.slot("a", 7);
    let held = slot.current().unwrap();
    assert!(!slot.can_install());
    assert!(
        slot.install(&root.0.join("missing"))
            .unwrap_err()
            .0
            .contains("store busy")
    );
    assert!(slot.applied().unwrap().is_none());
    drop(held);
    slot.apply_noop(AppliedPosition { index: 1, term: 1 })
        .unwrap();
}
#[test]
fn admin_busy_install_preserves_store() {
    let root = Root::new();
    let source = root.slot("source", 7);
    populate(&source);
    let snapshot = root.0.join("snapshot");
    let at = source.checkpoint(&snapshot).unwrap();
    let slot = root.slot("target", 7);
    populate(&slot);
    let before = slot.applied().unwrap();
    let held = admin(&slot);
    assert!(!slot.can_install());
    assert_eq!(slot.install(&snapshot).unwrap_err().0, "store busy");
    assert!(snapshot.exists());
    assert!(!root.0.join("target.old").exists());
    assert_eq!(slot.applied().unwrap(), before);
    assert_rows(&slot);
    drop(held);
    assert!(slot.can_install());
    assert_eq!(slot.install(&snapshot).unwrap(), at);
    assert_rows(&slot);
}

#[test]
fn rejected_installs_restore_old_data() {
    let root = Root::new();
    let slot = root.slot("a", 7);
    populate(&slot);
    let before = slot.applied().unwrap();
    for (name, domain, applied, message) in [
        ("foreign", 8, true, "domain mismatch"),
        ("empty", 7, false, "snapshot has no applied position"),
    ] {
        let source = root.slot(name, domain);
        if applied {
            source
                .apply_noop(AppliedPosition { index: 1, term: 1 })
                .unwrap();
        }
        let snapshot = root.0.join(format!("{name}-snapshot"));
        source.checkpoint(&snapshot).unwrap();
        assert!(slot.install(&snapshot).unwrap_err().0.contains(message));
        assert_eq!(slot.applied().unwrap(), before);
        assert_rows(&slot);
        assert!(!root.0.join("a.failed").exists());
    }
    assert!(slot.install(&root.0.join("missing")).is_err());
    let corrupt = root.0.join("corrupt");
    fs::create_dir(&corrupt).unwrap();
    fs::write(corrupt.join("garbage"), b"bad").unwrap();
    assert!(slot.install(&corrupt).is_err());
    assert_eq!(slot.applied().unwrap(), before);
    assert_rows(&slot);
}
#[test]
fn recovers_interrupted_swaps() {
    for mode in 0..3 {
        let root = Root::new();
        let old = root.slot("data.old", 7);
        old.apply_noop(AppliedPosition { index: 1, term: 1 })
            .unwrap();
        old.flush().unwrap();
        drop(old);
        if mode == 0 {
            let data = gr::ReplicaStore::create(root.0.join("data"), [7; 16]).unwrap();
            data.apply_noop(gr::AppliedPosition { index: 1, term: 2 })
                .unwrap();
            data.flush().unwrap();
        } else if mode == 1 {
            fs::create_dir(root.0.join("data")).unwrap();
            fs::write(root.0.join("data/garbage"), b"bad").unwrap();
        }
        fs::create_dir(root.0.join("data.failed")).unwrap();
        let data = root.slot("data", 7);
        assert_eq!(
            data.applied().unwrap(),
            Some(AppliedPosition {
                index: 1,
                term: if mode == 0 { 2 } else { 1 }
            })
        );
        assert!(!root.0.join("data.old").exists());
        assert!(!root.0.join("data.failed").exists());
        drop(data);
        assert!(matches!(
            StoreSlot::open_or_create(&root.0.join("data"), [8; 16]),
            Err(Error::Identity(_))
        ));
    }
}
