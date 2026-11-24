mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::SqlError};

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
