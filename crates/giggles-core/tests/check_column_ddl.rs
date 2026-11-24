mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::SqlError};

#[test]
#[cfg_attr(not(feature = "engine-rocksdb"), allow(clippy::single_element_loop))]
fn check_dependencies_follow_column_ddl() {
    for engine in [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ] {
        let path = common::fresh("check-column-ddl");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        session.begin_block().unwrap();
        for sql in [
            "CREATE TABLE t (id int PRIMARY KEY, n text, nn text CHECK (nn='n'), \"N\" int, lower text)",
            "ALTER TABLE t ADD CONSTRAINT qualified CHECK (lower(t.n)<>'n')",
            "ALTER TABLE t ADD CONSTRAINT quoted CHECK (\"N\">-1) NOT VALID",
            "ALTER TABLE t RENAME COLUMN n TO renamed",
            "ALTER TABLE t RENAME COLUMN \"N\" TO \"Renamed\"",
            "ALTER TABLE t DROP COLUMN lower",
            "INSERT INTO t VALUES (1, 'ok', 'n', 1)",
        ] {
            session.execute(sql, &[], None).unwrap_or_else(|e| panic!("{engine:?} {sql}: {e:?}"));
        }
        session.commit_block().unwrap();
        let quoted = session.catalog_metadata().unwrap().presentation.constraints
            .into_iter().find(|c| c.name == "quoted").unwrap();
        assert_eq!(quoted.expression.as_deref(), Some(r#"("Renamed" > '-1'::integer)"#));
        assert!(!quoted.validated);
        assert_eq!(session.execute("UPDATE t SET renamed='N' WHERE id=1", &[], None),
            Err(CoreError::Sql(SqlError::CheckViolation)));
        for sql in [
            "ALTER TABLE t DROP COLUMN renamed RESTRICT, DROP COLUMN \"Renamed\" CASCADE",
            "INSERT INTO t VALUES (2, 'n')",
        ] {
            session.execute(sql, &[], None).unwrap_or_else(|e| panic!("{engine:?} {sql}: {e:?}"));
        }
        let checks: Vec<_> = session.catalog_metadata().unwrap().presentation.constraints
            .into_iter().filter(|c| c.kind == 'c').map(|c| c.name).collect();
        assert_eq!(checks, ["t_nn_check"]);

        for sql in [
            "CREATE TYPE ddl_kind AS ENUM ('A', 'B', 'C')",
            "CREATE TABLE enum_columns (id int PRIMARY KEY, kind ddl_kind)",
            "ALTER TABLE enum_columns ADD CONSTRAINT enum_membership CHECK (enum_columns.kind::varchar(3) IN ('A', 'B')) NOT VALID",
            "ALTER TABLE enum_columns ADD CONSTRAINT enum_label CHECK (kind <> 'C')",
            "ALTER TABLE enum_columns RENAME COLUMN kind TO category",
            "ALTER TYPE ddl_kind RENAME VALUE 'C' TO 'D'",
            "INSERT INTO enum_columns VALUES (1, 'B')",
        ] {
            session.execute(sql, &[], None).unwrap_or_else(|e| panic!("{engine:?} {sql}: {e:?}"));
        }
        let catalog = session.catalog_metadata().unwrap().presentation.constraints;
        let membership = catalog.iter().find(|c| c.name == "enum_membership").unwrap();
        let expression = membership.expression.as_deref().unwrap();
        assert!(expression.contains("(category)::character varying(3)"));
        assert!(expression.contains("= ANY ("));
        assert!(!membership.validated);
        let label = catalog.iter().find(|c| c.name == "enum_label").unwrap();
        assert_eq!(label.expression.as_deref(), Some("(category <> 'D'::text)"));
        assert_eq!(session.execute("INSERT INTO enum_columns VALUES (2, 'D')", &[], None),
            Err(CoreError::Sql(SqlError::CheckViolation)));
        session.execute("ALTER TABLE enum_columns DROP COLUMN category", &[], None).unwrap();
        session.execute("INSERT INTO enum_columns VALUES (2)", &[], None).unwrap();
        assert!(session.catalog_metadata().unwrap().presentation.constraints.iter()
            .all(|c| !matches!(c.name.as_str(), "enum_membership" | "enum_label")));
        drop((session, admin));
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
#[cfg_attr(not(feature = "engine-rocksdb"), allow(clippy::single_element_loop))]
fn enum_text_checks_survive_column_type_changes() {
    for engine in [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ] {
        let path = common::fresh("check-column-type");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for sql in [
            "CREATE TYPE t_e AS ENUM ('A', 'B', 'C')",
            "CREATE TABLE t1 (id int PRIMARY KEY, k t_e, CONSTRAINT t1c CHECK (k::text IN ('A', 'B')))",
            "INSERT INTO t1 VALUES (1, 'A')",
            "ALTER TABLE t1 ALTER COLUMN k TYPE text",
            "INSERT INTO t1 VALUES (2, 'A')",
            "UPDATE t1 SET k = 'B' WHERE id = 1",
            "INSERT INTO t1 VALUES (4, NULL)",
            "CREATE TABLE t3 (id int PRIMARY KEY, k t_e, CONSTRAINT t3c CHECK (k::varchar(5) IN ('A', 'B')))",
            "INSERT INTO t3 VALUES (1, 'A')",
            "ALTER TABLE t3 ALTER COLUMN k TYPE varchar(10)",
            "INSERT INTO t3 VALUES (2, 'A')",
            "UPDATE t3 SET k = 'B' WHERE id = 1",
        ] {
            session
                .execute(sql, &[], None)
                .unwrap_or_else(|error| panic!("{engine:?} {sql}: {error:?}"));
        }
        for sql in [
            "INSERT INTO t1 VALUES (3, 'Z')",
            "UPDATE t1 SET k = 'Z' WHERE id = 1",
            "INSERT INTO t3 VALUES (3, 'Z')",
            "UPDATE t3 SET k = 'Z' WHERE id = 1",
        ] {
            assert_eq!(
                session.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::CheckViolation)),
                "{engine:?} {sql}"
            );
        }
        drop((session, admin));
        std::fs::remove_dir_all(path).unwrap();
    }
}
