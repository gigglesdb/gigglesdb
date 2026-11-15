use super::*;
use crate::storage::*;

fn directory() -> std::path::PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!("giggles-v2-{:032x}", u128::from_be_bytes(nonce)))
}
#[test]
fn native_catalog_drop_decode() {
    use crate::operations::CatalogRequest;
    for (kind, field) in [("DropTable", "table"), ("DropIndex", "name")] {
        let value = serde_json::json!({kind: {field: "items", "cascade": false}});
        let request: CatalogRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), value);
        for extra in ["if_exists", "optional", "concurrently", "unknown"] {
            let mut invalid = value.clone();
            invalid[kind][extra] = true.into();
            assert!(serde_json::from_value::<CatalogRequest>(invalid).is_err());
        }
        let mut invalid = value;
        invalid[kind].as_object_mut().unwrap().remove("cascade");
        assert!(serde_json::from_value::<CatalogRequest>(invalid).is_err());
    }
}

#[test]
fn native_catalog_drops_match_sql() {
    use crate::operations::{CatalogRequest, Engine as Native, Error};
    use relational::with_sql_parser_forbidden;
    for backend in [Engine::Legacy, Engine::V2] {
        if backend == Engine::V2 && !cfg!(feature = "engine-rocksdb") { continue; }
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let native = Native::new(admin.clone());
        let mut sql = admin.sql_session();
        for ddl in ["CREATE TABLE parent(id BIGINT PRIMARY KEY, u BIGINT CONSTRAINT parent_uq UNIQUE)",
            "CREATE TABLE child(id BIGINT PRIMARY KEY, pid BIGINT REFERENCES parent(id))",
            "CREATE INDEX parent_idx ON parent(id)",
            "CREATE UNIQUE INDEX parent_unique_idx ON parent(id)"] {
            sql.execute(ddl, &[], None).unwrap();
        }
        if backend == Engine::V2 {
            sql.execute("INSERT INTO parent(id) VALUES(1)", &[], None).unwrap();
            sql.execute("INSERT INTO child VALUES(1,1)", &[], None).unwrap();
        }
        for (request, text, sqlstate) in [
            (CatalogRequest::DropTable { table: "parent".into(), cascade: false }, "DROP TABLE parent RESTRICT", "2BP01"),
            (CatalogRequest::DropTable { table: "missing".into(), cascade: false }, "DROP TABLE missing", "42P01"),
            (CatalogRequest::DropIndex { name: "missing".into(), cascade: false }, "DROP INDEX missing", "42704"),
            (CatalogRequest::DropIndex { name: "parent_pkey".into(), cascade: false }, "DROP INDEX parent_pkey RESTRICT", "2BP01"),
            (CatalogRequest::DropIndex { name: "parent_pkey".into(), cascade: true }, "DROP INDEX parent_pkey CASCADE", "2BP01"),
            (CatalogRequest::DropIndex { name: "parent_uq".into(), cascade: false }, "DROP INDEX parent_uq RESTRICT", "2BP01"),
            (CatalogRequest::DropIndex { name: "parent_uq".into(), cascade: true }, "DROP INDEX parent_uq CASCADE", "2BP01"),
            (CatalogRequest::DropIndex { name: "parent_unique_idx".into(), cascade: true }, "DROP INDEX parent_unique_idx CASCADE", "0A000"),
        ] {
            let before = admin.catalog().unwrap();
            let error = sql.execute(text, &[], None).unwrap_err();
            assert_eq!(crate::sqlstate(&error), sqlstate, "{text}");
            let native_error = with_sql_parser_forbidden(|| native.execute_catalog(&request)).unwrap_err();
            assert_eq!(native_error.sqlstate(), Some(crate::sqlstate(&error)), "{text}");
            assert_eq!(admin.catalog().unwrap(), before);
        }
        let preimage = admin.catalog().unwrap().1.into_iter().find(|t| t.name == "parent").unwrap();
        let index_id = sql.catalog_metadata().unwrap().indexes.iter().find(|i| i.name == "parent_idx").unwrap().id;
        with_sql_parser_forbidden(|| {
            sql.begin_engine().unwrap();
            sql.drop_catalog_engine("parent_idx".into(), false, true).unwrap();
            sql.drop_catalog_engine("parent".into(), true, false).unwrap();
            assert!(!sql.catalog_engine().unwrap().1.iter().any(|t| t.name == "parent"));
            sql.rollback();
            assert_eq!(sql.catalog_engine().unwrap().1.iter().find(|t| t.name == "parent"), Some(&preimage));
            assert_eq!(sql.catalog_metadata().unwrap().presentation.constraints.iter().filter(|c| c.kind == 'f').count(), 1);
            let drop_index = CatalogRequest::DropIndex { name: "parent_idx".into(), cascade: true };
            let bound = native.prepare_catalog_bound(&drop_index).unwrap();
            let outcome = native.execute_catalog_bound(&bound).unwrap();
            assert_eq!(outcome.index_id, Some(index_id));
            assert_eq!(outcome.table, preimage);
            assert!(outcome.catalog_epoch > bound.description().catalog_epoch);
            assert_eq!(native.execute_catalog_bound(&bound).unwrap_err().sqlstate(), Some("0A000"));
            native.execute_catalog(&CatalogRequest::DropIndex { name: "parent_unique_idx".into(), cascade: false }).unwrap();
            native.execute_catalog(&CatalogRequest::CreateIndex { table: "parent".into(), name: "automatic_idx".into(), columns: vec!["id".into()] }).unwrap();
            let request = CatalogRequest::DropTable { table: "parent".into(), cascade: true };
            let bound = native.prepare_catalog_bound(&request).unwrap();
            let outcome = native.execute_catalog_bound(&bound).unwrap();
            assert_eq!(outcome.table, preimage);
            assert_eq!(outcome.index_id, None);
            assert!(outcome.catalog_epoch > bound.description().catalog_epoch);
            assert_eq!(native.execute_catalog_bound(&bound).unwrap_err().sqlstate(), Some("0A000"));
        });
        assert!(sql.catalog_metadata().unwrap().indexes.is_empty());
        // CASCADE removes the FK, not its owning table (PG18.6).
        sql.execute("INSERT INTO child VALUES(2,999)", &[], None).unwrap();
        if backend == Engine::Legacy {
            assert_eq!(native.execute_catalog(&CatalogRequest::DropTable { table: "child".into(), cascade: false }), Err(Error::PlanRequired));
        }
        let expected = admin.catalog().unwrap();
        drop(sql);
        drop(native);
        drop(admin);
        let reopened = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(reopened.catalog().unwrap(), expected);
        assert!(reopened.sql_session().catalog_metadata().unwrap().indexes.is_empty());
        assert_eq!(reopened.sql_session().execute("SELECT * FROM child ORDER BY id", &[], None).unwrap().rows.len(), if backend == Engine::V2 { 2 } else { 1 });
        if backend == Engine::Legacy {
            reopened.sql_session().execute("DELETE FROM child WHERE id=2", &[], None).unwrap();
        }
        let dropped = Native::new(reopened.clone()).execute_catalog(&CatalogRequest::DropTable { table: "child".into(), cascade: false }).unwrap();
        assert_eq!(dropped.table.name, "child");
        assert!(reopened.catalog().unwrap().1.is_empty());
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn native_catalog_drop_view_dependencies() {
    use crate::operations::{CatalogRequest, Engine as Native};
    use relational::with_sql_parser_forbidden;
    if !cfg!(feature = "engine-rocksdb") { return; }
    for cascade in [false, true] {
        let mut catalogs = Vec::new();
        for native_path in [false, true] {
            let path = directory();
            let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
            let mut sql = admin.sql_session();
            for text in ["CREATE TABLE items(id BIGINT PRIMARY KEY)", "INSERT INTO items VALUES(1)",
                "CREATE VIEW visible AS SELECT id FROM items", "CREATE VIEW nested AS SELECT id FROM visible"] {
                sql.execute(text, &[], None).unwrap();
            }
            let native = Native::new(admin.clone());
            let result = if native_path {
                with_sql_parser_forbidden(|| native.execute_catalog(&CatalogRequest::DropTable { table: "items".into(), cascade }))
                    .map(|_| ()).map_err(|e| e.sqlstate().unwrap().to_owned())
            } else {
                sql.execute(if cascade { "DROP TABLE items CASCADE" } else { "DROP TABLE items RESTRICT" }, &[], None)
                    .map(|_| ()).map_err(|e| crate::sqlstate(&e).to_owned())
            };
            assert_eq!(result.is_ok(), cascade);
            if !cascade { assert_eq!(result.as_ref().unwrap_err(), "2BP01"); }
            let metadata = sql.catalog_metadata().unwrap();
            assert_eq!(metadata.presentation.views.len(), if cascade { 0 } else { 2 });
            catalogs.push((result, admin.catalog().unwrap().1, metadata.presentation.views.into_values().map(|v| v.table).collect::<Vec<_>>()));
            drop(native);
            drop(sql);
            drop(admin);
            fs::remove_dir_all(path).unwrap();
        }
        assert_eq!(catalogs[0], catalogs[1]);
    }
}

#[test]
fn native_catalog_drop_outcomes_resolve_schema_names() {
    use crate::operations::{CatalogRequest, Engine as Native};
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    let mut sql = admin.sql_session();
    for text in ["CREATE SCHEMA alpha", "CREATE TABLE alpha.items(id BIGINT PRIMARY KEY)",
        "CREATE TABLE items(id BIGINT PRIMARY KEY)", "CREATE INDEX same_name ON alpha.items(id)",
        "CREATE INDEX same_name ON items(id)"] { sql.execute(text, &[], None).unwrap(); }
    let metadata = sql.catalog_metadata().unwrap();
    let public_table = sql.inspect_metadata(|s| Ok(s.sql.tables["items"].clone())).unwrap();
    let public_id = metadata.indexes.iter().find(|i| i.table == public_table.id).unwrap().id;
    let native = Native::new(admin.clone());
    let dropped = relational::with_sql_parser_forbidden(|| native.execute_catalog(&CatalogRequest::DropIndex { name: "same_name".into(), cascade: false })).unwrap();
    assert_eq!(dropped.index_id, Some(public_id));
    assert_eq!(dropped.table, public_table);
    let table = dropped.table;
    assert_eq!(sql.catalog_metadata().unwrap().indexes.len(), 1);
    let dropped = native.execute_catalog(&CatalogRequest::DropTable { table: "items".into(), cascade: false }).unwrap();
    assert_eq!(dropped.table, table);
    assert_eq!(admin.catalog().unwrap().1.len(), 1);
    drop(native);
    drop(sql);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
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
    session.begin_native().unwrap();
    session.rollback_native().unwrap();
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
    admin.export(path.join("export")).unwrap();
    assert!(
        admin
            .export_with_fault(path.join("failed-export"), RecoveryPoint::ExportWrite)
            .is_err()
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
    assert!(matches!(
        session.scan(relational::Scan {
            table: String::new(),
            projection: vec![],
            prefix: vec![],
            lower: None,
            upper: None,
            reverse: false,
            limit: 0,
        }),
        Err(CoreError::Sql(relational::SqlError::Transaction))
    ));
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
    session.begin_engine().unwrap();
    session.commit_engine().unwrap();
    assert_eq!(session.catalog_engine().unwrap().1.len(), 1);
    assert_eq!(session.catalog_empty_engine(), Err(CoreError::Sql(relational::SqlError::Transaction)));
    assert_eq!(session.snapshot_identity_engine(), Err(CoreError::Sql(relational::SqlError::Transaction)));
    assert_eq!(
        session.create_table_engine(String::new(), vec![], vec![]),
        Err(CoreError::Sql(relational::SqlError::Transaction)),
    );
    let legacy_path = directory();
    let legacy = Admin::create(&legacy_path, Limits::default()).unwrap();
    let mut legacy_session = legacy.sql_session();
    legacy_session
        .execute("CREATE TABLE test_items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let table = legacy.catalog().unwrap().1.remove(0);
    assert_eq!(session.row_version_engine(&table, &[]),
        Err(CoreError::Sql(relational::SqlError::Transaction)));
    session.rollback();
    assert_eq!(session.insert_engine(&table, vec![]),
        Err(CoreError::Sql(relational::SqlError::StaleSchema)));
    session.rollback();
    assert_eq!(session.update_engine(&table, vec![], vec![], 0),
        Err(CoreError::Sql(relational::SqlError::StaleSchema)));
    session.rollback();
    assert_eq!(session.delete_engine(&table, vec![], 0),
        Err(CoreError::Sql(relational::SqlError::StaleSchema)));
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

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_controls_match_sql_without_parsing() {
    for controls in [
        vec!["BEGIN", "COMMIT"],
        vec!["BEGIN", "ROLLBACK"],
        vec!["BEGIN", "BEGIN", "COMMIT", "ROLLBACK"],
        vec!["COMMIT", "ROLLBACK"],
        vec!["BEGIN", "ERROR", "BEGIN", "COMMIT", "ROLLBACK"],
        vec!["BEGIN", "ERROR", "ROLLBACK"],
    ] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut native = admin.sql_session();
        let mut sql = admin.sql_session();
        for control in controls {
            if control == "ERROR" {
                assert!(
                    native
                        .execute("SELECT * FROM missing_control_table", &[], None)
                        .is_err()
                );
                assert!(
                    sql.execute("SELECT * FROM missing_control_table", &[], None)
                        .is_err()
                );
                continue;
            }
            let expected = sql.execute(control, &[], None).map(|_| ());
            let actual = crate::relational::with_sql_parser_forbidden(|| match control {
                "BEGIN" => native.begin_native(),
                "COMMIT" => native.commit_native(),
                _ => native.rollback_native(),
            });
            assert_eq!(actual, expected, "{control}");
            assert_eq!(native.in_transaction(), sql.in_transaction());
        }
        drop(native);
        drop(sql);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_binding_metadata_parity() {
    use crate::operations::{Bindings, Engine as Native, Error, Request};
    for backend in [Engine::Legacy, Engine::V2] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE keyed (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let request = Request::Read {
            table: "keyed".into(),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec!["id".into()],
        };
        let native = Native::new(admin.clone());
        let handle = native.prepare_bound(&request, &Bindings::new()).unwrap();
        // Bound reads now execute on either engine.
        assert!(native.execute_bound(&handle).is_ok());
        let before = admin
            .inspect_catalog_engine(|domain, fence, tables| (domain, fence, tables.clone()))
            .unwrap();
        admin.sql_session().execute("INSERT INTO keyed VALUES (1)", &[], None).unwrap();
        assert_eq!(admin.inspect_catalog_engine(|d, f, t| (d, f, t.clone())).unwrap(), before);
        let mut session = admin.sql_session();
        session.begin_native().unwrap();
        let identity = session.snapshot_identity_engine().unwrap();
        assert_eq!(identity, (before.0, before.1));
        session
            .execute("CREATE TABLE private (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert_ne!(identity, session.snapshot_identity_engine().unwrap());
        assert_eq!(session.catalog_engine().unwrap().1.len(), 2);
        assert_eq!(
            admin
                .inspect_catalog_engine(|_, _, tables| tables.len())
                .unwrap(),
            1
        );
        session.rollback_native().unwrap();
        assert_eq!(
            admin
                .inspect_catalog_engine(|d, f, t| (d, f, t.clone()))
                .unwrap(),
            before
        );
        admin
            .sql_session()
            .execute("CREATE TABLE changed (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let stale = Error::Sql { sqlstate: "0A000".into() };
        assert_eq!(native.execute_bound(&handle), Err(stale.clone()));
        let other_path = directory();
        let other = Admin::create_engine(&other_path, Limits::default(), backend).unwrap();
        assert_eq!(
            Native::new(other.clone()).execute_bound(&handle),
            Err(stale)
        );
        drop(other);
        fs::remove_dir_all(other_path).unwrap();
        let fence = admin.inspect_catalog_engine(|_, fence, _| fence).unwrap();
        drop(session);
        drop(native);
        drop(admin);
        let reopened = Admin::open(&path, Limits::default()).unwrap();
        let (new_fence, tables) = reopened
            .inspect_catalog_engine(|_, fence, tables| (fence, tables.len()))
            .unwrap();
        assert_eq!(tables, 2);
        if backend == Engine::V2 {
            assert_ne!(fence, new_fence);
        }
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn bounded_native_complete_reads() {
    use crate::relational::{MAX_ROWS, with_sql_parser_forbidden};
    for backend in [Engine::Legacy, Engine::V2] {
        for count in [0, MAX_ROWS, MAX_ROWS + 1] {
            let path = directory();
            let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
            let mut writer = admin.sql_session();
            writer.execute("CREATE TABLE bounded (id BIGINT PRIMARY KEY)", &[], None).unwrap();
            writer.execute("BEGIN", &[], None).unwrap();
            for id in 0..count {
                writer.execute(&format!("INSERT INTO bounded VALUES ({id})"), &[], None).unwrap();
            }
            writer.execute("COMMIT", &[], None).unwrap();
            let mut reader = admin.sql_session();
            reader.begin_native().unwrap();
            let table = reader.catalog_engine().unwrap().1.remove(0);
            // Pin the read snapshot before a concurrent boundary insert.
            let initial = reader.complete_table_engine(&table);
            if count <= MAX_ROWS { assert!(initial.is_ok()); }
            else { reader.rollback_native().unwrap(); reader.begin_native().unwrap(); }
            if count == MAX_ROWS {
                let concurrent = admin.clone();
                std::thread::spawn(move || concurrent.sql_session()
                    .execute("INSERT INTO bounded VALUES (1024)", &[], None).unwrap())
                    .join().unwrap();
            }
            with_sql_parser_forbidden(|| {
                let result = reader.complete_table_engine(&table);
                if count > MAX_ROWS {
                    assert!(matches!(result, Err(CoreError::Limit)));
                    assert!(reader.is_aborted());
                } else {
                    assert_eq!(reader.consume_complete_engine(result.unwrap(), &table).unwrap().len(), count);
                }
                reader.rollback_native().unwrap();
                reader.begin_native().unwrap();
                let cancelled = crate::Cancellation::default();
                cancelled.cancel();
                assert!(matches!(cancelled.scope(|| reader.complete_table_engine(&table)), Err(CoreError::Cancelled)));
                assert!(reader.is_aborted());
                reader.rollback_native().unwrap();
                if count == MAX_ROWS {
                    reader.begin_native().unwrap();
                    assert!(matches!(reader.complete_table_engine(&table), Err(CoreError::Limit)));
                    reader.rollback_native().unwrap();
                }
            });
            if count == 0 {
                reader.begin_native().unwrap();
                let relation = reader.complete_table_engine(&table).unwrap();
                reader.execute("INSERT INTO bounded VALUES (9)", &[], None).unwrap();
                assert_eq!(reader.consume_complete_engine(relation, &table),
                    Err(CoreError::Sql(relational::SqlError::StaleSchema)));
                reader.rollback_native().unwrap();
                if backend == Engine::V2 {
                    reader.begin_native().unwrap();
                    let relation = reader.complete_table_engine(&table).unwrap();
                    writer.execute("INSERT INTO bounded VALUES (10)", &[], None).unwrap();
                    reader.execute("SELECT * FROM bounded", &[], None).unwrap();
                    assert_eq!(reader.consume_complete_engine(relation, &table),
                        Err(CoreError::Sql(relational::SqlError::StaleSchema)));
                    reader.rollback_native().unwrap();
                }
            }
            drop(reader); drop(writer); drop(admin);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn typed_keyed_insert_parity() {
    use crate::relational::{Value as V, with_sql_parser_forbidden};
    for backend in [Engine::Legacy, Engine::V2] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let mut session = admin.sql_session();
        for ddl in [
            "CREATE TABLE parent (id BIGINT PRIMARY KEY)",
            "INSERT INTO parent VALUES (1)",
            "CREATE TABLE child (id BIGINT PRIMARY KEY, p BIGINT NOT NULL REFERENCES parent(id), n BIGINT CHECK(n > 0))",
        ] { session.execute(ddl, &[], None).unwrap(); }
        let table = session.catalog_engine().unwrap().1.into_iter().find(|t| t.name == "child").unwrap();
        let row = |id, p, n| vec![V::Int(id), p, V::Int(n)];
        session.begin_native().unwrap();
        assert_eq!(with_sql_parser_forbidden(|| session.insert_engine(&table, row(1, V::Int(1), 1))).unwrap().affected, 1);
        session.commit_native().unwrap();
        for (values, sql, error) in [
            (row(1, V::Int(1), 1), "INSERT INTO child VALUES (1,1,1)", relational::SqlError::Duplicate), // 23505
            (row(2, V::Null, 1), "INSERT INTO child VALUES (2,NULL,1)", relational::SqlError::Null), // 23502
            (row(2, V::Int(1), -1), "INSERT INTO child VALUES (2,1,-1)", relational::SqlError::CheckViolation), // 23514
            (row(2, V::Int(9), 1), "INSERT INTO child VALUES (2,9,1)", relational::SqlError::ForeignKeyViolation), // 23503
        ] {
            let expected = session.execute(sql, &[], None).unwrap_err();
            assert_eq!(expected, CoreError::Sql(error));
            session.begin_native().unwrap();
            session.execute("INSERT INTO child VALUES (99,1,1)", &[], None).unwrap();
            let actual = with_sql_parser_forbidden(|| session.insert_engine(&table, values)).unwrap_err();
            assert_eq!(std::mem::discriminant(&actual), std::mem::discriminant(&expected));
            if let (CoreError::Sql(a), CoreError::Sql(b)) = (actual, expected) {
                assert_eq!(a, b);
            } else { panic!("expected structured SQL errors"); }
            assert!(session.is_aborted());
            session.rollback_native().unwrap();
            assert_eq!(session.execute("SELECT * FROM child", &[], None).unwrap().rows.len(), 1);
        }
        for commit in [false, true] {
            session.begin_native().unwrap();
            let sql_count = session.execute("INSERT INTO child VALUES (2,1,1)", &[], None).unwrap().affected;
            assert_eq!(with_sql_parser_forbidden(|| session.insert_engine(&table, row(3, V::Int(1), 1))).unwrap().affected, sql_count);
            if commit { session.commit_native().unwrap(); } else { session.rollback_native().unwrap(); }
            assert_eq!(session.execute("SELECT * FROM child", &[], None).unwrap().rows.len(), if commit { 3 } else { 1 });
        }
        drop(session); drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(admin.sql_session().execute("SELECT * FROM child", &[], None).unwrap().rows.len(), 3);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn typed_keyed_update_parity() {
    use crate::operations::{Bindings, Compare, Expr, Intent, Request, Sort, ValueExpr};
    use crate::relational::{Value as V, with_sql_parser_forbidden};
    let lit = |v| ValueExpr::Literal(v);
    let filter = || Expr::Compare(Box::new(Expr::Field("n".into())), Compare::Eq,
        Box::new(Expr::Value(lit(V::Int(0)))));
    let request = |claim| Request::Update {
        table: "items".into(), filter: filter(), guard: None,
        values: vec![("n".into(), lit(V::Int(1)))],
        intent: if claim { Intent::Claim { order: vec![Sort { field: "id".into(), descending: false }] } }
            else { Intent::One },
    };
    for backend in [Engine::Legacy, Engine::V2] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT)", &[], None).unwrap();
        session.execute("INSERT INTO items VALUES(1,0)", &[], None).unwrap();
        let table = session.catalog_engine().unwrap().1.remove(0);
        let before = session.execute("SELECT * FROM items", &[], None).unwrap().rows[0].version;
        let native = crate::operations::Engine::new(admin.clone());
        let result = with_sql_parser_forbidden(|| native.execute(&request(false), &Bindings::new())).unwrap();
        assert_eq!((result.affected, result.changed), (1, 1));
        session.begin_native().unwrap();
        let version = with_sql_parser_forbidden(|| session.row_version_engine(&table, &[V::Int(1)])).unwrap();
        assert_ne!(version, before);
        session.rollback_native().unwrap();
        let noop = Request::Update { table: "items".into(), filter: Expr::Value(lit(V::Bool(true))),
            guard: None, values: vec![("n".into(), lit(V::Int(1)))], intent: Intent::One };
        let result = with_sql_parser_forbidden(|| native.execute(&noop, &Bindings::new())).unwrap();
        assert_eq!((result.affected, result.changed), (1, 0));
        assert_eq!(session.execute("SELECT * FROM items", &[], None).unwrap().rows[0].version, version);
        assert_eq!(session.execute("UPDATE items SET n=1 WHERE id=1", &[], None).unwrap().affected, 1);
        // SQL bumps even a no-op; native deliberately preserves its version.
        assert_ne!(session.execute("SELECT * FROM items", &[], None).unwrap().rows[0].version, version);
        session.begin_native().unwrap();
        assert_eq!(with_sql_parser_forbidden(|| session.update_engine(&table, vec![V::Int(1)],
            vec![(1, V::Int(2))], before)), Err(CoreError::Sql(relational::SqlError::Version)));
        assert!(session.is_aborted());
        session.rollback_native().unwrap();
        session.execute("UPDATE items SET n=0", &[], None).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = (0..2).map(|_| {
            let admin = admin.clone(); let barrier = barrier.clone(); let request = request(true);
            std::thread::spawn(move || {
                barrier.wait();
                crate::operations::Engine::new(admin).execute(&request, &Bindings::new())
            })
        }).collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter_map(|r| r.as_ref().ok()).map(|r| r.affected).sum::<usize>(), 1);
        for result in results.into_iter().flatten() {
            if result.affected == 1 { assert_eq!(result.rows[0].values, vec![V::Int(1), V::Int(1)]); }
        }
        session.execute("UPDATE items SET n=0", &[], None).unwrap();
        admin.set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
        assert_eq!(native.execute(&request(false), &Bindings::new()), Err(crate::operations::Error::Unknown));
        drop(native); drop(session); drop(admin);
        // Legacy fences the handle after an ambiguous commit; inspect durability
        // through reopen for both engines rather than retrying the mutation.
        let reopened = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(reopened.sql_session().execute("SELECT n FROM items", &[], None).unwrap().rows[0].values,
            vec![V::Int(1)]);
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn typed_keyed_delete_parity() {
    use crate::operations::{Bindings, Compare, Error, Expr, Request, ValueExpr};
    use crate::relational::{Value as V, with_sql_parser_forbidden};
    for backend in [Engine::Legacy, Engine::V2] {
        for action in ["RESTRICT", "NO ACTION", "CASCADE"] {
            let path = directory();
            let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
            let mut session = admin.sql_session();
            for ddl in [
                "CREATE TABLE parent(id BIGINT PRIMARY KEY)",
                "INSERT INTO parent VALUES(1),(2),(3)",
                &format!("CREATE TABLE child(id BIGINT PRIMARY KEY,p BIGINT REFERENCES parent(id) ON DELETE {action})"),
                "INSERT INTO child VALUES(1,3)",
            ] { session.execute(ddl, &[], None).unwrap(); }
            let native = crate::operations::Engine::new(admin.clone());
            let request = |id: Option<i64>, cap: Option<i64>| Request::Returning { request: Box::new(Request::Delete {
                table: "parent".into(),
                filter: id.map_or(Expr::Value(ValueExpr::Literal(V::Bool(true))), |id|
                    Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Eq,
                        Box::new(Expr::Value(ValueExpr::Literal(V::Int(id)))))),
                guard: None, cap: cap.map(|n| ValueExpr::Literal(V::Int(n))),
            }), projection: vec![] };
            // Cardinality rejection occurs before any mutation on both engines.
            assert_eq!(native.execute(&request(None, Some(2)), &Bindings::new()), Err(Error::TooManyMatches));
            let first = with_sql_parser_forbidden(|| native.execute(&request(Some(1), None), &Bindings::new())).unwrap();
            assert_eq!(first.affected, 1);
            assert_eq!(first.rows[0].values, vec![V::Int(1)]); // delete preimage
            session.execute("INSERT INTO parent VALUES(1)", &[], None).unwrap();
            let before = session.execute("SELECT * FROM parent ORDER BY id", &[], None).unwrap().rows;
            // Obtain SQL's result inside a transaction so native sees the same input.
            session.begin_native().unwrap();
            let sql_result = session.execute("DELETE FROM parent", &[], None);
            let sql_rows = if sql_result.is_ok() {
                Some((session.execute("SELECT * FROM parent", &[], None).unwrap().rows,
                    session.execute("SELECT * FROM child", &[], None).unwrap().rows))
            } else { None };
            session.rollback_native().unwrap();
            let result = with_sql_parser_forbidden(|| native.execute(&request(None, Some(3)), &Bindings::new()));
            if action != "CASCADE" {
                let expected = if action == "RESTRICT" {
                    relational::SqlError::RestrictViolation // 23001
                } else {
                    relational::SqlError::ForeignKeyViolation // 23503
                };
                assert_eq!(sql_result, Err(CoreError::Sql(expected.clone())));
                assert_eq!(result, Err(Error::Sql { sqlstate: crate::sqlstate(&CoreError::Sql(expected)).into() }));
                // Rows 1 and 2 were selected before the failing referenced row 3.
                assert_eq!(session.execute("SELECT * FROM parent ORDER BY id", &[], None).unwrap().rows, before);
                assert_eq!(session.execute("SELECT * FROM child", &[], None).unwrap().rows.len(), 1);
            } else {
                let result = result.unwrap();
                assert_eq!(result.affected as u64, sql_result.unwrap().affected);
                let mut preimages: Vec<_> = result.rows.into_iter().map(|r| r.values).collect();
                preimages.sort_by_key(|r| match r[0] { V::Int(i) => i, _ => unreachable!() });
                assert_eq!(preimages, vec![vec![V::Int(1)], vec![V::Int(2)], vec![V::Int(3)]]);
                let (parents, children) = sql_rows.unwrap();
                assert_eq!(session.execute("SELECT * FROM parent", &[], None).unwrap().rows, parents);
                assert_eq!(session.execute("SELECT * FROM child", &[], None).unwrap().rows, children);
            }
            drop(native); drop(session); drop(admin);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn typed_keyed_delete_admission_cap() {
    use crate::operations::{Bindings, Error, Expr, Request, ValueExpr};
    use crate::relational::{Value as V, MAX_ROWS, with_sql_parser_forbidden};
    for backend in [Engine::Legacy, Engine::V2] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None).unwrap();
        session.begin_native().unwrap();
        for id in 0..MAX_ROWS {
            session.execute(&format!("INSERT INTO items VALUES({id})"), &[], None).unwrap();
        }
        session.commit_native().unwrap();
        let native = crate::operations::Engine::new(admin.clone());
        let request = |cap| Request::Returning { request: Box::new(Request::Delete { table: "items".into(),
            filter: Expr::Value(ValueExpr::Literal(V::Bool(true))), guard: None,
            cap: Some(ValueExpr::Literal(V::Int(cap))) }), projection: vec![] };
        assert_eq!(native.execute(&request(MAX_ROWS as i64 + 1), &Bindings::new()), Err(Error::Type));
        assert_eq!(native.execute(&request(MAX_ROWS as i64 - 1), &Bindings::new()), Err(Error::TooManyMatches));
        let result = with_sql_parser_forbidden(|| native.execute(&request(MAX_ROWS as i64), &Bindings::new())).unwrap();
        assert_eq!((result.affected, result.rows.len()), (MAX_ROWS, MAX_ROWS));
        assert!(session.execute("SELECT * FROM items", &[], None).unwrap().rows.is_empty());
        // A complete-relation overflow must refuse even a selective delete.
        session.begin_native().unwrap();
        for id in 0..=MAX_ROWS {
            session.execute(&format!("INSERT INTO items VALUES({id})"), &[], None).unwrap();
        }
        session.commit_native().unwrap();
        assert_eq!(native.execute(&request(MAX_ROWS as i64), &Bindings::new()), Err(Error::Limit));
        drop(native); drop(session); drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_filtered_reads_fall_back_with_and_without_secondary_index() {
    use crate::operations::{Bindings, Compare, Expr, Request, Sort, ValueExpr};
    use crate::relational::{Value as V, with_sql_parser_forbidden};

    for indexed in [false, true] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE items(id BIGINT PRIMARY KEY, state TEXT)", &[], None).unwrap();
        session.execute("INSERT INTO items VALUES(1,'ready'),(2,'pending'),(3,'ready')", &[], None).unwrap();
        if indexed {
            session.execute("CREATE INDEX items_state ON items(state)", &[], None).unwrap();
        }
        let request = Request::Read {
            table: "items".into(),
            filter: Some(Expr::Compare(
                Box::new(Expr::Field("state".into())),
                Compare::Eq,
                Box::new(Expr::Value(ValueExpr::Literal(V::Text("ready".into())))),
            )),
            order: vec![Sort { field: "id".into(), descending: false }],
            limit: None,
            projection: vec!["id".into(), "state".into()],
        };
        let native = crate::operations::Engine::new(admin.clone());
        for transactional in [false, true] {
            if transactional {
                session.begin_native().unwrap();
                session.execute("INSERT INTO items VALUES(4,'ready')", &[], None).unwrap();
            }
            let expected = session.execute(
                "SELECT id,state FROM items WHERE state='ready' ORDER BY id", &[], None,
            ).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
            assert_eq!(expected.len(), if transactional { 3 } else { 2 });
            let result = with_sql_parser_forbidden(|| {
                if transactional {
                    session.execute_native(&request, &Bindings::new())
                } else {
                    native.execute(&request, &Bindings::new())
                }
            }).unwrap();
            assert_eq!(result.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), expected);
            if transactional {
                session.rollback_native().unwrap();
            }
        }
        drop(native);
        drop(session);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_catalog_publication_and_fences() {
    use crate::operations::{CatalogRequest, ColumnSpec, Engine as Native, Error};
    use relational::{DataType, with_sql_parser_forbidden};
    let create = |name: &str| CatalogRequest::CreateTable {
        table: name.into(),
        columns: vec![ColumnSpec { name: "id".into(), data_type: DataType::BigInt, not_null: true, default: None, generated: None }],
        primary_key: vec!["id".into()],
    };
    for backend in [Engine::Legacy, Engine::V2] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let native = Native::new(admin.clone());
        let stale = Error::Sql { sqlstate: "0A000".into() };
        with_sql_parser_forbidden(|| {
            let bound = native.prepare_catalog_bound(&create("items")).unwrap();
            native.execute_catalog_bound(&bound).unwrap();
            assert_eq!(native.execute_catalog_bound(&bound), Err(stale.clone()));
        });
        admin.sql_session().execute("INSERT INTO items VALUES (1)", &[], None).unwrap();
        let index = CatalogRequest::CreateIndex {
            table: "items".into(), name: "items_id".into(), columns: vec!["id".into()],
        };
        with_sql_parser_forbidden(|| {
            let bound = native.prepare_catalog_bound(&index).unwrap();
            if backend == Engine::Legacy {
                // Explicit eligibility difference: legacy retains its empty-state rule.
                assert_eq!(native.execute_catalog_bound(&bound), Err(Error::PlanRequired));
                assert_eq!(native.execute_catalog(&create("second")), Err(Error::PlanRequired));
            } else {
                let outcome = native.execute_catalog_bound(&bound).unwrap();
                assert_eq!(Some(admin.sql_session().catalog_metadata().unwrap().indexes[0].id), outcome.index_id);
                assert_eq!(native.execute_catalog_bound(&bound), Err(stale.clone()));
                native.execute_catalog(&create("second")).unwrap();
                let bound = native.prepare_catalog_bound(&create("never")).unwrap();
                native.execute_catalog(&create("drift")).unwrap();
                assert_eq!(native.execute_catalog_bound(&bound), Err(stale.clone()));
            }
        });
        if backend == Engine::Legacy {
            assert!(admin.sql_session().catalog_metadata().unwrap().indexes.is_empty());
            admin.sql_session().execute("CREATE INDEX items_id ON items(id)", &[], None).unwrap();
        }
        drop(native);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_v2_catalog_rollback_and_waiting_fence() {
    use relational::{ColumnDef, DataType, SqlError, with_sql_parser_forbidden};
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    let mut session = admin.sql_session();
    with_sql_parser_forbidden(|| {
        session.begin_native().unwrap();
        session.create_table_engine("private".into(), vec![ColumnDef {
            id: 0, name: "id".into(), data_type: DataType::BigInt, not_null: true,
        }], vec!["id".into()]).unwrap();
        let table = session.catalog_engine().unwrap().1.into_iter().find(|t| t.name == "items").unwrap();
        session.create_index_engine(&table, "private_index", &[table.columns[0].id]).unwrap();
        assert_eq!(session.catalog_metadata().unwrap().tables.len(), 2);
        assert_eq!(session.catalog_metadata().unwrap().indexes.len(), 1);
        assert_eq!(admin.sql_session().catalog_metadata().unwrap().tables.len(), 1);
        assert!(admin.sql_session().catalog_metadata().unwrap().indexes.is_empty());
        session.rollback_native().unwrap();
    });
    assert_eq!(session.catalog_metadata().unwrap().tables.len(), 1);
    assert!(session.catalog_metadata().unwrap().indexes.is_empty());
    session.begin_native().unwrap();
    let before = session.snapshot_identity_engine().unwrap();
    admin.sql_session().execute("CREATE TABLE drift(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    assert_eq!(session.snapshot_identity_engine().unwrap(), before);
    assert_eq!(session.catalog_empty_engine(), Err(CoreError::Sql(SqlError::StaleSchema)));
    session.rollback_native().unwrap();
    drop(session);
    drop(admin);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(reopened.sql_session().catalog_metadata().unwrap().tables.len(), 2);
    assert!(reopened.sql_session().catalog_metadata().unwrap().indexes.is_empty());
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}
