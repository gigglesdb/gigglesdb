mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, Value, take_sql_notices},
};

#[cfg_attr(not(feature = "engine-rocksdb"), allow(clippy::single_element_loop))]
fn databases(mut test: impl FnMut(&Admin, &mut SqlSession)) {
    for engine in [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ] {
        let path = common::fresh("alter-add-if-not-exists");
        {
            let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut session = admin.sql_session();
            test(&admin, &mut session);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
fn run(session: &mut SqlSession, sql: &str) {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn notices(table: &str, columns: &[&str]) {
    assert_eq!(
        take_sql_notices(),
        columns
            .iter()
            .map(|column| (
                format!("column \"{column}\" of relation \"{table}\" already exists, skipping"),
                None,
                Some("42701"),
            ))
            .collect::<Vec<_>>()
    );
}
fn error(session: &mut SqlSession, sql: &str, expected: SqlError) {
    assert_eq!(
        session.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

#[test]
fn fresh_add_and_rerun_with_and_without_column() {
    databases(|admin, s| {
        run(s, "CREATE TABLE t (id int PRIMARY KEY)");
        run(s, "INSERT INTO t VALUES (1)");
        let sql = "ALTER TABLE t ADD COLUMN IF NOT EXISTS a smallint DEFAULT 7, ADD IF NOT EXISTS b bytea";
        run(s, sql);
        notices("t", &[]);
        let before = admin.catalog().unwrap().1;
        run(s, sql);
        notices("t", &["a", "b"]);
        assert_eq!(admin.catalog().unwrap().1, before);
        assert_eq!(
            s.execute("SELECT a,b FROM t", &[], None).unwrap().rows[0].values,
            vec![Value::Int(7), Value::Null]
        );
    });
}

#[test]
fn mixed_skip_and_fresh_add() {
    databases(|_, s| {
        run(s, "CREATE TABLE t (id int PRIMARY KEY, a int)");
        run(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS a text, ADD COLUMN b smallint DEFAULT 9",
        );
        notices("t", &["a"]);
        run(s, "INSERT INTO t (id,a) VALUES (1,2)");
        assert_eq!(
            s.execute("SELECT a,b FROM t", &[], None).unwrap().rows[0].values,
            vec![Value::Int(2), Value::Int(9)]
        );
    });
}

#[test]
fn second_action_sees_column_added_by_first() {
    databases(|_, s| {
        run(s, "CREATE TABLE t (id int PRIMARY KEY)");
        run(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS a int DEFAULT 3, ADD COLUMN IF NOT EXISTS a varchar(0) DEFAULT 'bad'",
        );
        notices("t", &["a"]);
        run(s, "INSERT INTO t (id) VALUES (1)");
        assert_eq!(
            s.execute("SELECT a FROM t", &[], None).unwrap().rows[0].values,
            vec![Value::Int(3)]
        );
    });
}

#[test]
fn duplicate_skip_precedes_type_typmod_and_default_validation() {
    databases(|admin, s| {
        run(s, "CREATE TABLE t (id int PRIMARY KEY, a int)");
        let before = admin.catalog().unwrap().1;
        for definition in [
            "missing_type",
            "missing_type(1,2)",
            "missing_schema.missing_type",
            "varchar(0)",
            "int(3)",
            "numeric(0,0)",
            "int DEFAULT 'not an integer'",
            "int DEFAULT missing_function()",
            "int DEFAULT nextval('missing_sequence')",
        ] {
            run(
                s,
                &format!("ALTER TABLE t ADD IF NOT EXISTS a {definition}"),
            );
            notices("t", &["a"]);
            assert_eq!(admin.catalog().unwrap().1, before);
        }
    });
}

#[test]
fn new_columns_and_plain_add_keep_validation_errors() {
    databases(|_, s| {
        run(s, "CREATE TABLE t (id int PRIMARY KEY, a int)");
        for optional in ["", "IF NOT EXISTS "] {
            error(
                s,
                &format!("ALTER TABLE t ADD {optional}fresh missing_type"),
                SqlError::Unsupported,
            );
            error(
                s,
                &format!("ALTER TABLE t ADD {optional}fresh varchar(0)"),
                SqlError::InvalidParameterValue,
            );
            assert!(
                s.execute(
                    &format!("ALTER TABLE t ADD {optional}fresh int DEFAULT 'bad'"),
                    &[],
                    None
                )
                .is_err()
            );
        }
        error(
            s,
            "ALTER TABLE t ADD a varchar(0)",
            SqlError::InvalidParameterValue,
        );
        error(s, "ALTER TABLE t ADD a int", SqlError::DuplicateColumn);
    });
}

#[test]
fn system_column_conflict_and_malformed_syntax_are_errors() {
    databases(|_, s| {
        run(s, "CREATE TABLE t (id int PRIMARY KEY)");
        error(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS ctid int",
            SqlError::DuplicateColumn,
        );
        error(s, "ALTER TABLE t ADD IF EXISTS id int", SqlError::Syntax);
        error(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS id varchar(",
            SqlError::Syntax,
        );
        error(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS id missing_type(",
            SqlError::Syntax,
        );
        error(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS id missing_type DEFAULT",
            SqlError::Syntax,
        );
        error(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS id int DEFAULT missing_function(",
            SqlError::Syntax,
        );
        error(
            s,
            "ALTER TABLE t ADD IF NOT EXISTS id int, ADD COLUMN",
            SqlError::Syntax,
        );
    });
}

#[test]
fn skedaddle_installer_alters_are_idempotent() {
    databases(|admin, s| {
        run(s, "CREATE SCHEMA _skedaddle");
        for table in ["apply_progress", "apply_receipts", "cutover_activation"] {
            run(
                s,
                &format!("CREATE TABLE _skedaddle.{table} (id text PRIMARY KEY)"),
            );
            let columns = if table == "cutover_activation" {
                "ADD COLUMN IF NOT EXISTS fence_owner text, ADD COLUMN IF NOT EXISTS destination_system_identifier text, ADD COLUMN IF NOT EXISTS destination_database text"
            } else {
                "ADD COLUMN IF NOT EXISTS record_kind smallint, ADD COLUMN IF NOT EXISTS manifest_digest bytea, ADD COLUMN IF NOT EXISTS previous_manifest_digest bytea"
            };
            let sql = format!("ALTER TABLE _skedaddle.{table} {columns}");
            run(s, &sql);
            notices(table, &[]);
            let before = admin.catalog().unwrap().1;
            run(s, &sql);
            notices(
                table,
                if table == "cutover_activation" {
                    &[
                        "fence_owner",
                        "destination_system_identifier",
                        "destination_database",
                    ]
                } else {
                    &["record_kind", "manifest_digest", "previous_manifest_digest"]
                },
            );
            assert_eq!(admin.catalog().unwrap().1, before);
            let table = before.iter().find(|t| t.name == table).unwrap();
            assert_eq!(table.logical_columns().count(), 4);
        }
    });
}
