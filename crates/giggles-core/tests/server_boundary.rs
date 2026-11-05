use giggles_core::{
    relational::{DataType, SqlError, Value},
    *,
};
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};
fn admin() -> Admin {
    let path = std::path::PathBuf::from("artifacts").join(format!(
        "p2-core-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    Admin::create(path.join("db.redb"), Default::default()).unwrap()
}
#[test]
fn descriptor_has_no_write_or_allocator_side_effects() {
    let a = admin();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    let before = a.catalog().unwrap();
    let p = s.prepare("INSERT INTO app VALUES ($1,$2)").unwrap();
    for _ in 0..20 {
        let d = s.describe(&p).unwrap();
        assert_eq!(d.parameters, vec![DataType::BigInt, DataType::Text]);
        assert!(d.columns.is_empty());
    }
    assert_eq!(a.catalog().unwrap(), before);
    assert!(
        s.execute("SELECT * FROM app LIMIT 1", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    s.execute_prepared(&p, &[Value::Int(1), Value::Text("ok".into())], None)
        .unwrap();
    assert_eq!(
        s.execute("SELECT * FROM app LIMIT 1", &[], None)
            .unwrap()
            .rows[0]
            .version,
        1
    );
}
#[test]
fn descriptor_domain_schema_and_rollback_branch_fences() {
    let a = admin();
    let b = admin();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("CREATE TABLE abandoned (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let p = s.prepare("SELECT id FROM abandoned WHERE id=$1").unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    s.execute("START TRANSACTION", &[], None).unwrap();
    s.execute(
        "CREATE TABLE replacement (id BIGINT PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    assert!(matches!(
        s.describe(&p),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
    assert!(s.is_aborted());
    s.rollback();
    let p = s.prepare("BEGIN").unwrap();
    assert!(matches!(
        b.sql_session().describe(&p),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
}
#[test]
fn descriptor_rejects_parameter_type_conflicts_and_gateway_abort() {
    let a = admin();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    let p = s.prepare("UPDATE app SET body=$1 WHERE id=$1").unwrap();
    assert!(matches!(
        s.describe(&p),
        Err(CoreError::Sql(SqlError::Type))
    ));
    assert!(s.is_aborted());
    assert!(matches!(
        s.execute("COMMIT", &[], None),
        Err(CoreError::Sql(SqlError::Aborted))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    s.abort();
    assert!(s.is_aborted());
    s.rollback();
    assert!(!s.is_aborted());
}
#[test]
fn remote_decision_and_status_require_current_task_even_on_dedupe() {
    let a = admin();
    let key = Key {
        tenant: TenantId(42),
        row: RowId(1),
    };
    a.put(
        key,
        Row {
            status: Status::Failed,
            failed_at: 103,
            attempt_count: 0,
            version: 0,
        },
    )
    .unwrap();
    let e = a
        .grant(Grant {
            task: TaskId(1),
            issuer: Principal(1),
            executor: Principal(2),
            approver: Principal(3),
            policy: Policy {
                tenant: TenantId(42),
                columns: BTreeSet::from([Column::Status]),
                transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                incident: (100, 107),
                distinct_limit: 1,
                transition_limit: 1,
                approval_above: 1,
                valid_from: 10,
                expires: 30,
            },
        })
        .unwrap();
    a.provision_task_resources(
        TaskId(1),
        TaskResources {
            read_rows: 1000,
            disclosed_rows: 1000,
            encoded_bytes: 1000000,
        },
    )
    .unwrap();
    let r = Request {
        operation: OperationId(1),
        selection: Selection::Keys(vec![key]),
        column: Column::Status,
        after: Status::Queued,
    };
    assert!(e.execute_current(r.clone(), 9, 11, None).is_err());
    assert!(e.preview_current(&r, 29, 30).is_err());
    // The failed future check advanced the trusted watermark; no backward sample can authorize.
    assert!(e.execute_current(r.clone(), 20, 20, None).is_err());
    assert!(e.status_current(OperationId(1), 30, 30).is_err());
}

#[test]
fn remote_retry_revocation_gate_differs_from_trusted_forensic_status() {
    let a = admin();
    let key = Key {
        tenant: TenantId(42),
        row: RowId(1),
    };
    a.put(
        key,
        Row {
            status: Status::Failed,
            failed_at: 103,
            attempt_count: 0,
            version: 0,
        },
    )
    .unwrap();
    let e = a
        .grant(Grant {
            task: TaskId(1),
            issuer: Principal(1),
            executor: Principal(2),
            approver: Principal(3),
            policy: Policy {
                tenant: TenantId(42),
                columns: BTreeSet::from([Column::Status]),
                transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                incident: (100, 107),
                distinct_limit: 1,
                transition_limit: 1,
                approval_above: 1,
                valid_from: 1,
                expires: 30,
            },
        })
        .unwrap();
    a.provision_task_resources(
        TaskId(1),
        TaskResources {
            read_rows: 1000,
            disclosed_rows: 1000,
            encoded_bytes: 1000000,
        },
    )
    .unwrap();
    let r = Request {
        operation: OperationId(1),
        selection: Selection::Keys(vec![key]),
        column: Column::Status,
        after: Status::Queued,
    };
    let committed = e.execute_current(r.clone(), 10, 10, None).unwrap().unwrap();
    assert_eq!(
        e.execute_current(r.clone(), 11, 11, None).unwrap().unwrap(),
        committed
    );
    a.revoke(TaskId(1)).unwrap();
    assert_eq!(
        e.execute_current(r.clone(), 12, 12, None),
        Err(CoreError::Policy(Error::Revoked))
    );
    assert_eq!(
        e.preview_current(&r, 12, 12),
        Err(CoreError::Policy(Error::Revoked))
    );
    assert_eq!(
        e.status_current(OperationId(1), 12, 12),
        Err(CoreError::Policy(Error::Revoked))
    );
    assert_eq!(
        e.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(Ok(committed))
    );
}

#[test]
fn prepared_rollback_can_recover_abort_but_keeps_transaction_fence() {
    let a = admin();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    let old = s.prepare("ROLLBACK").unwrap();
    s.abort();
    let rollback = s.prepare("ROLLBACK").unwrap();
    assert!(s.describe(&rollback).unwrap().columns.is_empty());
    s.execute_prepared(&rollback, &[], None).unwrap();
    assert!(!s.is_aborted());
    s.execute("BEGIN", &[], None).unwrap();
    s.abort();
    assert!(matches!(
        s.describe(&old),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
    assert!(matches!(
        s.execute_prepared(&old, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
}
