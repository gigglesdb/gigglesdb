#[cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::*;

#[cfg(not(feature = "engine-rocksdb"))]
#[test]
fn requires_rocksdb() {
    assert!(matches!(
        Admin::upgrade_to_v2("absent-upgrade-test", Limits::default()),
        Err(CoreError::EngineUnsupported { .. })
    ));
}

#[cfg(feature = "engine-rocksdb")]
mod rocks {
    use super::*;
    use giggles_core::{
        relational::{SqlResult, Value},
        roles::{NewRole, TablePrivilege},
    };
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
    };

    fn fresh(label: &str) -> PathBuf {
        let root = PathBuf::from(std::env::var_os("TEMP").unwrap()).join(format!(
            "upgrade-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        root.join("db")
    }
    fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
        let clock = relational::StatementClock {
            unavailable: None,
            transaction_us: 1,
            statement_us: 1,
            zone: None,
            wall: None,
        };
        s.execute_at(sql, &[], None, &clock)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }
    fn fixture(path: &Path, realistic: bool) -> (u128, Vec<SqlResult>) {
        let a = Admin::create(path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE heap (n BIGINT)");
        run(&mut s, "INSERT INTO heap VALUES (1), (2), (3)");
        run(&mut s, "DELETE FROM heap WHERE n = 3");
        if realistic {
            for sql in [
                "CREATE SCHEMA app",
                "CREATE TYPE app.mood AS ENUM ('happy', 'sad')",
                "CREATE SEQUENCE ids START 10",
                "SELECT setval('ids', 800)",
                "CREATE TABLE app.parent (id BIGINT PRIMARY KEY, mood app.mood)",
                "CREATE TABLE child (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES app.parent(id), code TEXT, n BIGINT CHECK (n >= 0))",
                "CREATE INDEX child_pid ON child(pid)",
                "CREATE UNIQUE INDEX child_code ON child(code)",
            ] {
                run(&mut s, sql);
            }
            let parents = (1..=120)
                .map(|i| format!("({i}, 'happy')"))
                .collect::<Vec<_>>()
                .join(",");
            let children = (1..=120)
                .map(|i| format!("({i}, {i}, 'c{i}', {i})"))
                .collect::<Vec<_>>()
                .join(",");
            let heaps = (10..=130)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",");
            run(&mut s, &format!("INSERT INTO app.parent VALUES {parents}"));
            run(&mut s, &format!("INSERT INTO child VALUES {children}"));
            run(&mut s, &format!("INSERT INTO heap VALUES {heaps}"));
            run(&mut s, "UPDATE child SET n = 999 WHERE id = 12");
            run(&mut s, "DELETE FROM child WHERE id = 120");
            run(&mut s, "DELETE FROM app.parent WHERE id = 120");
            fixture_roles(&a);
        }
        let queries = if realistic {
            vec![
                "SELECT * FROM app.parent ORDER BY id",
                "SELECT * FROM child ORDER BY id",
                "SELECT n FROM heap ORDER BY n",
            ]
        } else {
            vec!["SELECT n FROM heap ORDER BY n"]
        };
        (
            a.authority_metadata().unwrap().0,
            queries.into_iter().map(|q| run(&mut s, q)).collect(),
        )
    }

    fn fixture_roles(a: &Admin) {
        let roles = a.roles();
        for name in ["owner", "reader"] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    ..Default::default()
                })
                .unwrap();
        }
        roles.grant_membership("reader", "owner").unwrap();
        roles.set_table_owner("child", "owner").unwrap();
        roles
            .grant_table("child", "reader", &[TablePrivilege::Select])
            .unwrap();
    }

    #[test]
    fn realistic_upgrade_preserves_sql_catalog_roles_and_counters() {
        let path = fresh("realistic");
        let (domain, expected) = fixture(&path, true);
        let original = std::fs::read(path.join("data.redb")).unwrap();
        let report = Admin::upgrade_to_v2(&path, Limits::default()).unwrap();
        assert_eq!(
            std::fs::read(report.legacy.join("data.redb")).unwrap(),
            original,
            "upgrade changed the retained legacy database"
        );
        assert_eq!(&std::fs::read(&report.export).unwrap()[..8], b"GDEXP001");
        assert_eq!(Admin::verify_export(&report.export).unwrap().domain, domain);
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(a.authority_metadata().unwrap().0, domain);
        let mut s = a.sql_session();
        for (query, expected) in [
            "SELECT * FROM app.parent ORDER BY id",
            "SELECT * FROM child ORDER BY id",
            "SELECT n FROM heap ORDER BY n",
        ]
        .into_iter()
        .zip(expected)
        {
            assert_eq!(run(&mut s, query), expected);
        }
        assert_eq!(
            run(&mut s, "SELECT nextval('ids')").rows[0].values,
            vec![Value::Int(801)]
        );
        run(&mut s, "INSERT INTO heap VALUES (500)");
        run(&mut s, "INSERT INTO child VALUES (999, 1, 'new', 0)");
        assert!(matches!(
            s.execute("INSERT INTO child VALUES (998, 1, 'new', 0)", &[], None),
            Err(CoreError::Sql(relational::SqlError::Duplicate))
        ));
        assert!(matches!(
            s.execute("INSERT INTO child VALUES (998, 9999, 'fk', 0)", &[], None),
            Err(CoreError::Sql(relational::SqlError::ForeignKeyViolation))
        ));
        assert!(
            s.execute("INSERT INTO child VALUES (998, 1, 'check', -1)", &[], None)
                .is_err()
        );
        assert_eq!(s.table_owner("child").unwrap().as_deref(), Some("owner"));
        assert_eq!(
            s.table_privileges("child", "reader").unwrap(),
            BTreeSet::from([TablePrivilege::Select])
        );
        assert!(
            s.table_privileges("child", "reader")
                .unwrap()
                .is_disjoint(&BTreeSet::from([TablePrivilege::Insert]))
        );
        assert_eq!(
            a.roles().memberships("owner").unwrap(),
            BTreeSet::from(["reader".into()])
        );
        drop(s);
        drop(a);
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(
            run(&mut a.sql_session(), "SELECT count(*) FROM child").rows[0].values,
            vec![Value::Int(120)]
        );
        assert!(Admin::upgrade_to_v2(&path, Limits::default()).is_err());
    }

    #[test]
    fn every_returned_fault_rolls_back_and_retry_succeeds() {
        for point in [
            UpgradePoint::ExportWritten,
            UpgradePoint::ExportVerified,
            UpgradePoint::V2Built,
            UpgradePoint::Verified,
            UpgradePoint::FirstRename,
            UpgradePoint::SecondRename,
        ] {
            let path = fresh(&format!("{point:?}"));
            let (domain, expected) = fixture(&path, false);
            let original = std::fs::read(path.join("data.redb")).unwrap();
            assert!(Admin::upgrade_to_v2_with_fault(&path, Limits::default(), point).is_err());
            assert_eq!(
                std::fs::read(path.join("data.redb")).unwrap(),
                original,
                "failed upgrade at {point:?} changed the legacy database"
            );
            let a = Admin::open(&path, Limits::default()).unwrap();
            assert_eq!(a.authority_metadata().unwrap().0, domain);
            assert_eq!(
                run(&mut a.sql_session(), "SELECT n FROM heap ORDER BY n"),
                expected[0]
            );
            drop(a);
            Admin::upgrade_to_v2(&path, Limits::default()).unwrap();
        }
    }

    // Hash every file, including lock files if present: these legacy fixtures
    // use redb's in-file OS lock and require no lock-file exclusions.
    fn file_hashes(root: &Path) -> std::collections::BTreeMap<PathBuf, [u8; 32]> {
        use sha2::{Digest, Sha256};
        fn visit(root: &Path, dir: &Path, out: &mut std::collections::BTreeMap<PathBuf, [u8; 32]>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, out);
                } else {
                    out.insert(
                        path.strip_prefix(root).unwrap().into(),
                        Sha256::digest(std::fs::read(path).unwrap()).into(),
                    );
                }
            }
        }
        let mut out = std::collections::BTreeMap::new();
        visit(root, root, &mut out);
        out
    }
    fn refused_unchanged(path: &Path, kinds: &[&str]) {
        // Include siblings so an unexpected export or staging file is caught too.
        let root = path.parent().unwrap();
        let before = file_hashes(root);
        let error = Admin::upgrade_to_v2(path, Limits::default())
            .unwrap_err()
            .to_string();
        for kind in kinds {
            assert!(error.contains(&format!("{kind}: 1")), "{error}");
        }
        assert_eq!(file_hashes(root), before, "refusal changed files: {error}");
        std::fs::remove_dir_all(root).unwrap();
    }
    fn refusal_fixture(label: &str, seed: impl FnOnce(&Admin)) -> PathBuf {
        let path = fresh(label);
        let admin = Admin::create(&path, Limits::default()).unwrap();
        seed(&admin);
        drop(admin);
        path
    }

    #[test]
    fn refuses_tasks_without_changing_files() {
        let path = refusal_fixture("tasks", |a| {
            a.grant(common::grant()).unwrap();
        });
        refused_unchanged(&path, &["tasks"]);
    }

    #[test]
    fn refuses_receipts_without_changing_files() {
        let path = refusal_fixture("receipts", |a| {
            a.put(common::key(0), common::row()).unwrap();
            a.grant(common::grant())
                .unwrap()
                .execute(common::request(1, &[0]), Time::Certified(10), None)
                .unwrap()
                .unwrap();
            assert_eq!(a.snapshot().unwrap().operations.len(), 1);
        });
        refused_unchanged(&path, &["receipts", "protected operations"]);
    }

    #[test]
    fn refuses_protected_rejections_without_changing_files() {
        let path = refusal_fixture("protected-rejections", |a| {
            let mut request = common::request(1, &[]);
            request.selection = Selection::Keys(vec![Key {
                tenant: TenantId(43),
                row: RowId(0),
            }]);
            assert_eq!(
                a.grant(common::grant())
                    .unwrap()
                    .execute(request, Time::Certified(10), None)
                    .unwrap(),
                Err(Error::Scope)
            );
            assert_eq!(a.snapshot().unwrap().operations.len(), 1);
        });
        refused_unchanged(&path, &["protected operations"]);
    }

    #[test]
    fn refuses_approvals_without_changing_files() {
        let path = refusal_fixture("approvals", |a| {
            let executor = a.grant(common::grant()).unwrap();
            let preview = executor
                .preview(&common::request(1, &[]), Time::Certified(10))
                .unwrap();
            a.approve(Principal(3), preview, Time::Certified(10))
                .unwrap();
        });
        refused_unchanged(&path, &["approvals"]);
    }

    #[test]
    fn refuses_executor_ledger_without_changing_files() {
        let path = refusal_fixture("executor-ledger", |a| {
            a.grant(common::grant()).unwrap();
            a.provision_task_resources(TaskId(1), TaskResources::default())
                .unwrap();
        });
        refused_unchanged(&path, &["executor ledger"]);
    }

    #[test]
    fn refuses_kernel_records_without_changing_files() {
        let path = refusal_fixture("kernel-records", |a| {
            a.put(common::key(0), common::row()).unwrap();
            assert_eq!(a.snapshot().unwrap().rows.len(), 1);
        });
        refused_unchanged(&path, &["kernel records"]);
    }

    #[test]
    fn refuses_native_outcomes_without_changing_files() {
        use giggles_core::operations::{
            Bindings, Engine, Request as NativeRequest, ValueExpr,
            once::{ONCE_CONTRACT_VERSION, OnceBody, OnceIdentity, OnceRequest, OnceTime},
        };
        let path = refusal_fixture("native-outcomes", |a| {
            run(
                &mut a.sql_session(),
                "CREATE TABLE items (id BIGINT PRIMARY KEY)",
            );
            let request = OnceRequest {
                contract_version: ONCE_CONTRACT_VERSION,
                identity: OnceIdentity {
                    database_domain: a.snapshot().unwrap().domain,
                    tenant: 42,
                    namespace: "upgrade.fixture".into(),
                    key: b"one".to_vec(),
                },
                retention_seconds: 60,
                body: OnceBody::Operation(NativeRequest::Insert {
                    table: "items".into(),
                    values: vec![("id".into(), ValueExpr::Literal(Value::Int(1)))],
                }),
            };
            let receipt = Engine::new(a.clone())
                .execute_once(&request, &Bindings::new(), &|| {
                    Ok(OnceTime {
                        lower_unix_seconds: 1000,
                        upper_unix_seconds: 1001,
                        valid_until: std::time::Instant::now() + std::time::Duration::from_secs(10),
                    })
                })
                .unwrap();
            assert!(receipt.outcome.is_ok());
        });
        refused_unchanged(&path, &["once/outcome ledger"]);
    }

    #[test]
    fn restored_legacy_upgrade_preserves_domain_origin_rows_and_roles() {
        let path = fresh("restore-origin");
        let source = path.with_file_name("source");
        let export = path.with_file_name("source.gdexp");
        let (source_domain, expected) = fixture(&source, true);
        let admin = Admin::open(&source, Limits::default()).unwrap();
        admin.export(&export).unwrap();
        drop(admin);
        let restored = Admin::restore(&export, &path, Limits::default()).unwrap();
        // Legacy logical exports omit the separate role catalog. Populate the
        // restored v1 source so upgrade must preserve nonempty role records.
        fixture_roles(&restored);
        let authority = restored.authority_metadata().unwrap();
        assert_ne!(authority.0, source_domain);
        assert_eq!(authority.3, Some(source_domain));
        let roles = restored.roles().list().unwrap();
        drop(restored);
        Admin::upgrade_to_v2(&path, Limits::default()).unwrap();
        for _ in 0..2 {
            let upgraded = Admin::open(&path, Limits::default()).unwrap();
            assert_eq!(upgraded.authority_metadata().unwrap(), authority);
            assert_eq!(upgraded.roles().list().unwrap(), roles);
            assert_eq!(
                upgraded.roles().memberships("owner").unwrap(),
                BTreeSet::from(["reader".into()])
            );
            let mut session = upgraded.sql_session();
            assert_eq!(
                session.table_owner("child").unwrap().as_deref(),
                Some("owner")
            );
            assert_eq!(
                session.table_privileges("child", "reader").unwrap(),
                BTreeSet::from([TablePrivilege::Select])
            );
            for (query, rows) in [
                "SELECT * FROM app.parent ORDER BY id",
                "SELECT * FROM child ORDER BY id",
                "SELECT n FROM heap ORDER BY n",
            ]
            .into_iter()
            .zip(&expected)
            {
                assert_eq!(&run(&mut session, query), rows);
            }
        }
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn restored_legacy_with_tasks_still_refuses_only_tasks() {
        let path = fresh("restore-origin-tasks");
        let source = path.with_file_name("source");
        let export = path.with_file_name("source.gdexp");
        let admin = Admin::create(&source, Limits::default()).unwrap();
        admin.grant(common::grant()).unwrap();
        admin.export(&export).unwrap();
        drop(admin);
        let restored = Admin::restore(&export, &path, Limits::default()).unwrap();
        assert!(restored.authority_metadata().unwrap().3.is_some());
        drop(restored);
        let error = Admin::upgrade_to_v2(&path, Limits::default()).unwrap_err();
        assert!(
            matches!(&error, CoreError::Storage(message) if message == "upgrade cannot represent tasks: 1"),
            "{error}"
        );
        refused_unchanged(&path, &["tasks"]);
    }

    #[test]
    fn locked_corrupt_native_and_interrupted_directories_refuse() {
        let path = fresh("refusals");
        fixture(&path, false);
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert!(Admin::upgrade_to_v2(&path, Limits::default()).is_err());
        a.put(
            Key {
                tenant: TenantId(1),
                row: RowId(1),
            },
            Row {
                status: Status::Failed,
                failed_at: 0,
                attempt_count: 0,
                version: 0,
            },
        )
        .unwrap();
        drop(a);
        let error = Admin::upgrade_to_v2(&path, Limits::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains("kernel records: 1"), "{error}");
        let tmp = path.with_file_name("db.v2-upgrade-tmp");
        std::fs::create_dir(&tmp).unwrap();
        assert!(Admin::open(&path, Limits::default()).is_err());
        std::fs::remove_dir(&tmp).unwrap();
        let retained = path.with_file_name("db.v1-pre-upgrade-123");
        std::fs::rename(&path, &retained).unwrap();
        assert!(Admin::create(&path, Limits::default()).is_err());
        assert!(
            Admin::open(&path, Limits::default())
                .err()
                .unwrap()
                .to_string()
                .contains("upgrade in progress")
        );
        std::fs::rename(&retained, &path).unwrap();
        Admin::open(&path, Limits::default()).unwrap();
        let corrupt = fresh("corrupt");
        std::fs::create_dir(&corrupt).unwrap();
        std::fs::write(corrupt.join("data.redb"), b"corrupt").unwrap();
        assert!(Admin::upgrade_to_v2(&corrupt, Limits::default()).is_err());
        assert_eq!(
            std::fs::read(corrupt.join("data.redb")).unwrap(),
            b"corrupt"
        );
    }
}
