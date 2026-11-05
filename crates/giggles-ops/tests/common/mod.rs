#![allow(dead_code)]
use giggles_core::*;
use giggles_ops::*;
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn fresh(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "giggles-ops-{label}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
pub fn id() -> BackupId {
    BackupId::new("backup-1").unwrap()
}
pub fn fixture() -> (PathBuf, Catalog, Admin) {
    let root = fresh("catalog");
    let db = fresh("fixture").join("db");
    let a = Admin::create(db, Limits::default()).unwrap();
    let c = Catalog::open(&root, OpsLimits::default()).unwrap();
    (root, c, a)
}
pub fn key() -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(1),
    }
}
pub fn grant() -> Grant {
    Grant {
        task: TaskId(1),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: 25,
            transition_limit: 25,
            approval_above: 0,
            valid_from: 1,
            expires: 1000,
        },
    }
}
pub fn req(op: u64) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Keys(vec![key()]),
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn protected(a: &Admin) -> (Executor, ApprovalId, Decision) {
    a.put(
        key(),
        Row {
            status: Status::Failed,
            failed_at: 103,
            attempt_count: 0,
            version: 1,
        },
    )
    .unwrap();
    let e = a.grant(grant()).unwrap();
    let preview = e.preview(&req(1), Time::Certified(10)).unwrap();
    let approval = a
        .approve(Principal(3), preview, Time::Certified(10))
        .unwrap();
    let d = e
        .execute(req(1), Time::Certified(10), Some(approval))
        .unwrap();
    assert!(d.is_ok());
    (e, approval, d)
}
pub fn write_manifest(root: &std::path::Path, m: &Manifest) {
    std::fs::write(
        root.join(format!("{}.json", m.backup_id.as_str())),
        serde_json::to_vec(m).unwrap(),
    )
    .unwrap();
}
