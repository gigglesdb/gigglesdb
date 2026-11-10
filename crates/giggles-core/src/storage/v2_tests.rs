use super::*;
use crate::storage::*;

fn directory() -> std::path::PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!("giggles-v2-{:032x}", u128::from_be_bytes(nonce)))
}
#[test]
fn legacy_reopens_without_classifying_unrelated_files() {
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    let before = admin.snapshot().unwrap();
    drop(admin);
    fs::write(path.join("unrelated"), b"keep").unwrap();
    fs::create_dir(path.join("GIGGLES_FORMAT")).unwrap();
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.snapshot().unwrap(), before);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_reopens_with_temporary_format_marker() {
    let path = directory();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let before = admin.snapshot().unwrap();
    drop(admin);
    fs::write(path.join("GIGGLES_FORMAT.tmp"), b"").unwrap();
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.snapshot().unwrap(), before);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn temporary_format_marker_alone_is_corrupt() {
    let path = directory();
    fs::create_dir(&path).unwrap();
    fs::write(path.join("GIGGLES_FORMAT.tmp"), b"").unwrap();
    assert!(matches!(
        Admin::open(&path, Limits::default()),
        Err(CoreError::Corrupt(_))
    ));
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn marker_without_rocksdb_is_corrupt() {
    let path = directory();
    fs::create_dir(&path).unwrap();
    fs::write(path.join("GIGGLES_FORMAT"), b"giggles-v2\n").unwrap();
    assert!(matches!(
        Admin::open(&path, Limits::default()),
        Err(CoreError::Corrupt(_))
    ));
    fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_open_rejects_catalog_content_missing_feature_or_unknown_kind() {
    use crate::relational::{
        ColumnDef, DataType, TableDef,
        catalog_ext::{Constraint, ConstraintKind},
    };
    use engine::{
        catalog::CatalogParts,
        codec,
        hlc::Ts,
        kv::{Durability, Keyspace, WriteBatch},
        mvcc,
        txn::Isolation,
    };

    for unknown_kind in [false, true] {
        let path = directory();
        let db = V2Db::create(&path).unwrap();
        let mut batch = WriteBatch::default();
        if unknown_kind {
            batch.put(
                Keyspace::Catalog,
                codec::user_key_seek_at(&[99], Ts(1)),
                mvcc::encode_live(b"unknown"),
            );
        } else {
            let ids = db.catalog().reserve_ids(3).unwrap();
            let mut parts = CatalogParts::default();
            parts.tables.insert(
                "catalog_table".into(),
                TableDef {
                    id: ids.start,
                    name: "catalog_table".into(),
                    columns: vec![ColumnDef {
                        id: ids.start + 1,
                        name: "id".into(),
                        data_type: DataType::BigInt,
                        not_null: true,
                    }],
                    primary_key: vec![0],
                },
            );
            parts.ext.constraints.insert(
                ids.start,
                vec![Constraint {
                    id: ids.start + 2,
                    name: "catalog_table_pkey".into(),
                    kind: ConstraintKind::PrimaryKey,
                }],
            );
            let mut txn = db.begin(Isolation::ReadCommitted);
            db.catalog()
                .apply_diff(
                    &mut txn,
                    &CatalogParts::default(),
                    &parts,
                    &ids,
                    None,
                    &|| false,
                )
                .unwrap();
            db.catalog().commit_ddl(txn).unwrap();
            batch.put(
                Keyspace::Meta,
                b"features".to_vec(),
                0_u64.to_be_bytes().to_vec(),
            );
        }
        db.kv.write(batch, Durability::Sync).unwrap();
        drop(db);
        assert!(matches!(V2Db::open(&path), Err(CoreError::Corrupt(_))));
        fs::remove_dir_all(path).unwrap();
    }
}
#[cfg(not(feature = "engine-rocksdb"))]
#[test]
fn creation_requires_rocksdb_feature() {
    let path = directory();
    assert!(matches!(
        Admin::create_engine(&path, Limits::default(), Engine::V2),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: ROCKSDB_NOT_COMPILED
        })
    ));
    assert!(!path.exists());
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_sql_sessions_are_refused() {
    use crate::operations::{Bindings, Request, Value, ValueExpr};

    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    assert!(matches!(
        session.begin_native(),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: "SQL sessions"
        })
    ));
    let request = Request::Read {
        table: "app".into(),
        filter: None,
        order: vec![],
        limit: Some(ValueExpr::Literal(Value::Int(10))),
        projection: vec!["id".into()],
    };
    let error = session
        .execute_native(&request, &Bindings::new())
        .unwrap_err();
    eprintln!("v2 execute_native refusal: {error:?}");
    drop(session);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn lifecycle_and_public_refusals() {
    use crate::*;
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let domain = match &admin.inner.backend {
        Backend::V2(db) => {
            assert_eq!(
                engine::meta::validate_meta(db.kv.as_ref()).unwrap().engine,
                "rocksdb"
            );
            db.meta.domain
        }
        _ => panic!("legacy"),
    };
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    match &admin.inner.backend {
        Backend::V2(db) => assert_eq!(db.meta.domain, domain),
        _ => panic!("legacy"),
    }
    fn denied<T>(result: Result<T, CoreError>, feature: &'static str) {
        assert!(
            matches!(result, Err(CoreError::EngineUnsupported { engine: "v2", feature: actual }) if actual == feature)
        );
    }
    let policy = Policy {
        tenant: TenantId(0),
        columns: Default::default(),
        transitions: Default::default(),
        incident: (0, 0),
        distinct_limit: 0,
        transition_limit: 0,
        approval_above: 0,
        valid_from: 0,
        expires: 0,
    };
    denied(
        admin.provision_task_resources(TaskId(0), TaskResources::default()),
        TASK_EXECUTORS,
    );
    denied(admin.task_resources(TaskId(0)), TASK_EXECUTORS);
    denied(
        admin.grant(Grant {
            task: TaskId(0),
            issuer: Principal(0),
            executor: Principal(0),
            approver: Principal(0),
            policy: policy.clone(),
        }),
        TASK_EXECUTORS,
    );
    denied(admin.executor(TaskId(0), Principal(0)), TASK_EXECUTORS);
    denied(admin.revoke(TaskId(0)), TASK_EXECUTORS);
    let preview = Preview {
        domain: 0,
        task: TaskId(0),
        request: Request {
            operation: OperationId(0),
            selection: Selection::Keys(vec![]),
            column: Column::Status,
            after: Status::Failed,
        },
        schema: 0,
        policy,
        dependencies: Default::default(),
        effects: vec![],
        expires: 0,
    };
    denied(
        admin.approve(Principal(0), preview, Time::Uncertain),
        PROTECTED_OPERATIONS,
    );
    denied(admin.revoke_approval(ApprovalId(0)), PROTECTED_OPERATIONS);
    denied(admin.snapshot(), KERNEL_RECORDS);
    denied(
        admin.put(
            Key {
                tenant: TenantId(0),
                row: RowId(0),
            },
            Row {
                status: Status::Failed,
                failed_at: 0,
                attempt_count: 0,
                version: 0,
            },
        ),
        KERNEL_RECORDS,
    );
    denied(admin.bump_schema(), KERNEL_RECORDS);
    denied(admin.export(path.join("export")), LOGICAL_EXPORT);
    denied(
        admin.export_with_fault(path.join("export"), RecoveryPoint::ExportWrite),
        LOGICAL_EXPORT,
    );
    denied(admin.logical_export_catalog(), LOGICAL_EXPORT);
    let prepared = relational::Prepared::parse("SELECT 1", 0, 0).unwrap();
    let clock = relational::StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 0,
        zone: None,
        wall: None,
    };
    let mut session = admin.sql_session();
    assert!(matches!(
        session.describe(&prepared),
        Err(CoreError::Sql(relational::SqlError::StaleSchema))
    ));
    assert_eq!(
        session.validate_immutable_constants(&prepared),
        Err(CoreError::Sql(relational::SqlError::StaleSchema))
    );
    denied(
        session.scan(relational::Scan {
            table: String::new(),
            projection: vec![],
            prefix: vec![],
            lower: None,
            upper: None,
            reverse: false,
            limit: 0,
        }),
        SQL_SESSIONS,
    );
    let clock = relational::StatementClock {
        statement_us: clock.transaction_us,
        ..clock
    };
    let ddl = session
        .prepare("CREATE TABLE routed (id bigint PRIMARY KEY)")
        .unwrap();
    session.execute_prepared(&ddl, &[], None).unwrap();
    session
        .execute(
            "ALTER TABLE routed ALTER COLUMN id SET DEFAULT 1",
            &[],
            None,
        )
        .unwrap();
    session
        .execute_at(
            "ALTER TABLE routed ALTER COLUMN id DROP DEFAULT",
            &[],
            None,
            &clock,
        )
        .unwrap();
    let read = session.prepare("SELECT * FROM routed").unwrap();
    assert!(
        session
            .execute_prepared(&read, &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        session
            .execute_prepared_at(&read, &[], None, &clock)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        session
            .execute("INSERT INTO routed VALUES (1)", &[], None)
            .unwrap()
            .affected,
        1
    );
    denied(session.begin_engine(), SQL_SESSIONS);
    denied(session.commit_engine(), SQL_SESSIONS);
    denied(session.catalog_engine(), SQL_SESSIONS);
    denied(session.catalog_empty_engine(), SQL_SESSIONS);
    denied(session.snapshot_identity_engine(), SQL_SESSIONS);
    denied(
        session.create_table_engine(String::new(), vec![], vec![]),
        SQL_SESSIONS,
    );
    let legacy_path = directory();
    let legacy = Admin::create(&legacy_path, Limits::default()).unwrap();
    let mut legacy_session = legacy.sql_session();
    legacy_session
        .execute("CREATE TABLE test_items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let table = legacy.catalog().unwrap().1.remove(0);
    denied(session.row_version_engine(&table, &[]), SQL_SESSIONS);
    denied(session.insert_engine(&table, vec![]), SQL_SESSIONS);
    denied(
        session.update_engine(&table, vec![], vec![], 0),
        SQL_SESSIONS,
    );
    denied(session.delete_engine(&table, vec![], 0), SQL_SESSIONS);
    drop(legacy_session);
    drop(legacy);
    fs::remove_dir_all(legacy_path).unwrap();
    session.abort();
    session.rollback();
    assert!(!session.in_transaction());
    assert!(!session.is_aborted());
    drop(session);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}
