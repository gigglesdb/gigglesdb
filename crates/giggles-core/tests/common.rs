#![allow(dead_code)]
use giggles_core::*;
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn fresh(name: &str) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{name}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
pub fn key(n: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(n),
    }
}
pub fn row() -> Row {
    Row {
        status: Status::Failed,
        failed_at: 103,
        attempt_count: 0,
        version: 999,
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
            approval_above: 5,
            valid_from: 1,
            expires: 1000,
        },
    }
}
pub fn request(op: u64, ids: &[u64]) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Keys(ids.iter().map(|i| key(*i)).collect()),
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn predicate(op: u64) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Predicate {
            tenant: TenantId(42),
            status: Status::Failed,
            from: 100,
            until: 107,
        },
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn fixture(n: u64) -> (PathBuf, Admin, Executor) {
    let p = fresh("db");
    let a = Admin::create(&p, Limits::default()).unwrap();
    for i in 0..n {
        a.put(key(i), row()).unwrap();
    }
    let e = a.grant(grant()).unwrap();
    (p, a, e)
}
