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

    #[test]
    fn realistic_upgrade_preserves_sql_catalog_roles_and_counters() {
        let path = fresh("realistic");
        let (domain, expected) = fixture(&path, true);
        let report = Admin::upgrade_to_v2(&path, Limits::default()).unwrap();
        assert!(report.legacy.join("data.redb").exists());
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
            assert!(Admin::upgrade_to_v2_with_fault(&path, Limits::default(), point).is_err());
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
