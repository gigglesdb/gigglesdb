use super::*;
use crate::{Admin, CoreError, Engine, Limits, SqlSession};

fn run(state: &mut Relational, sql: &str) -> R<SqlResult> {
    state.execute(&Prepared::parse(sql, 1, state.epoch)?, &[], None)
}

fn fail(state: &mut Relational, sql: &str, error: SqlError, message: &str) {
    let before = state.clone();
    assert_eq!(run(state, sql), Err(error), "{sql}");
    assert_eq!(
        user_types::take_sql_error_message().as_deref(),
        Some(message),
        "{sql}"
    );
    assert_eq!(*state, before, "{sql}");
}

#[test]
fn alter_labels_order_notices_and_errors() {
    let mut state = Relational::default();
    for sql in [
        "CREATE TYPE status AS ENUM ('DRAFT','IN_REVIEW','DONE')",
        "CREATE TABLE eu (id int PRIMARY KEY, s status)",
        "INSERT INTO eu VALUES (1,'DONE'),(2,'DRAFT')",
        "ALTER TYPE public.status ADD VALUE 'READY' AFTER 'IN_REVIEW'",
        "ALTER TYPE status ADD VALUE 'FIRST' BEFORE 'DRAFT'",
        "ALTER TYPE status ADD VALUE 'LAST'",
        "ALTER TYPE status ADD VALUE IF NOT EXISTS 'VERIFYING'",
        "ALTER TYPE status ADD VALUE ''",
        "INSERT INTO eu VALUES (3,'READY'),(4,'FIRST')",
        "ALTER TYPE status ADD VALUE 'MID' BEFORE 'READY'",
        "INSERT INTO eu VALUES (5,'MID')",
        "CREATE SCHEMA enum_ns",
        "CREATE TYPE enum_ns.\"Mixed\" AS ENUM ('a')",
        "ALTER TYPE enum_ns.\"Mixed\" ADD VALUE 'b'",
    ] {
        run(&mut state, sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let (_, ty) = state.type_by_name("status").unwrap();
    assert!(matches!(ty, TypeDef::Enum { labels, .. } if labels == &[
        "FIRST", "DRAFT", "IN_REVIEW", "MID", "READY", "DONE", "LAST", "VERIFYING", ""
    ]));
    let before = state.clone();
    let sql = "ALTER TYPE status ADD VALUE IF NOT EXISTS 'LAST' BEFORE 'NOPE'";
    assert_eq!(Prepared::parse(sql, 1, 0).unwrap().command(), "ALTER TYPE");
    run(&mut state, sql).unwrap();
    assert_eq!(state, before);
    assert_eq!(
        user_types::take_sql_notices(),
        vec![(
            "enum label \"LAST\" already exists, skipping".into(),
            None,
            Some("42710")
        )]
    );
    for sql in [
        "ALTER TYPE status ADD VALUE 'X' BEFORE 'NOPE'",
        "ALTER TYPE status ADD VALUE 'X' AFTER 'NOPE'",
        "ALTER TYPE status RENAME VALUE 'NOPE' TO 'Y'",
    ] {
        fail(
            &mut state,
            sql,
            SqlError::InvalidParameterValue,
            "\"NOPE\" is not an existing enum label",
        );
    }
    for sql in [
        "ALTER TYPE status ADD VALUE 'LAST'",
        "ALTER TYPE status RENAME VALUE 'DONE' TO 'LAST'",
    ] {
        fail(
            &mut state,
            sql,
            SqlError::DuplicateObject,
            "enum label \"LAST\" already exists",
        );
    }
    fail(
        &mut state,
        "ALTER TYPE \"Nope\" ADD VALUE 'X'",
        SqlError::UndefinedObject,
        "type \"Nope\" does not exist",
    );
    fail(
        &mut state,
        "ALTER TYPE int4 ADD VALUE 'X'",
        SqlError::WrongObjectType,
        "integer is not an enum",
    );
    let too_long = "\u{00e9}".repeat(32);
    fail(
        &mut state,
        &format!("ALTER TYPE status ADD VALUE '{too_long}'"),
        SqlError::InvalidName,
        &format!("invalid enum label \"{too_long}\""),
    );
    assert_eq!(
        user_types::take_sql_error_detail().as_deref(),
        Some("Labels must be 63 bytes or less.")
    );
    assert_eq!(
        run(&mut state, "SELECT id FROM eu ORDER BY s")
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        [4, 2, 5, 3, 1].map(Value::Int)
    );
    assert_eq!(
        run(&mut state, "SELECT id FROM eu WHERE s > 'MID' ORDER BY s")
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        [3, 1].map(Value::Int)
    );
}

#[test]
fn rename_rewrites_keys_indexes_defaults_and_reopens_the_existing_format() {
    let mut state = Relational::default();
    for sql in [
        "CREATE TYPE phase AS ENUM ('todo','done')",
        "CREATE TABLE jobs (s phase PRIMARY KEY, related phase DEFAULT 'todo', unrelated text)",
        "CREATE INDEX jobs_related ON jobs(related)",
        "INSERT INTO jobs(s, unrelated) VALUES ('todo','todo'),('done','todo')",
    ] {
        run(&mut state, sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let (old_bytes, old_indexes) = state.secondary_persistence_engine(|| false).unwrap();
    let restore = |base: &[u8], indexes: &[u8]| {
        let mut restored: Relational = bincode::deserialize(base).unwrap();
        restored
            .secondary_restore_engine(
                secondary_index::codec::decode_pending(indexes, || false).unwrap(),
                || false,
            )
            .unwrap();
        restored
    };
    let mut reopened = restore(&old_bytes, &old_indexes);
    assert_eq!(reopened, state);
    assert_eq!(
        reopened.secondary_persistence_engine(|| false).unwrap(),
        (old_bytes.clone(), old_indexes)
    );
    let version = reopened.ext.wire_version();
    run(&mut reopened, "ALTER TYPE phase ADD VALUE '' BEFORE 'todo'").unwrap();
    run(
        &mut reopened,
        "ALTER TYPE phase RENAME VALUE 'todo' TO 'queued'",
    )
    .unwrap();
    assert_eq!(reopened.ext.wire_version(), version);
    assert_eq!(reopened.next_id, state.next_id);
    let (bytes, indexes) = reopened.secondary_persistence_engine(|| false).unwrap();
    assert_ne!(bytes, old_bytes);
    let mut restored = restore(&bytes, &indexes);
    assert_eq!(restored, reopened);
    assert_eq!(
        run(
            &mut restored,
            "SELECT s,related,unrelated FROM jobs WHERE related='queued' ORDER BY s"
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("queued".into()),
                Value::Text("queued".into()),
                Value::Text("todo".into())
            ],
            vec![
                Value::Text("done".into()),
                Value::Text("queued".into()),
                Value::Text("todo".into())
            ],
        ]
    );
    run(&mut restored, "INSERT INTO jobs(s) VALUES ('')").unwrap();
    let column = &restored.tables["jobs"].columns[1];
    assert_eq!(
        restored.catalog_presentation().unwrap().defaults[&column.id],
        "'queued'::phase"
    );
}

#[test]
fn rename_rejects_a_dependent_check_atomically() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TYPE phase AS ENUM ('todo','done')").unwrap();
    run(
        &mut state,
        "CREATE TABLE jobs (s phase PRIMARY KEY CHECK (s <> 'done'))",
    )
    .unwrap();
    run(&mut state, "INSERT INTO jobs VALUES ('todo')").unwrap();
    fail(
        &mut state,
        "ALTER TYPE phase RENAME VALUE 'done' TO 'closed'",
        SqlError::Unsupported,
        "cannot rename enum value with a dependent CHECK constraint",
    );
}

fn exec(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

#[test]
fn enum_values_are_transaction_local_in_both_sql_engines() {
    for engine in [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ] {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("enum-alter-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        exec(&mut session, "CREATE TYPE tx_status AS ENUM ('DRAFT')");
        exec(&mut session, "CREATE TYPE \"St\" AS ENUM ('DRAFT')");
        exec(
            &mut session,
            "CREATE TABLE tx_rows (id int PRIMARY KEY, s tx_status DEFAULT 'DRAFT')",
        );
        for (target, statement, spelling) in [
            ("\"St\"", "SELECT 'TXV'::\"St\"", "\"St\""),
            (
                "tx_status",
                "CREATE TABLE tx_default (s tx_status DEFAULT 'TXV')",
                "tx_status",
            ),
            (
                "tx_status",
                "INSERT INTO tx_rows VALUES (1,'TXV')",
                "tx_status",
            ),
            (
                "tx_status",
                "SELECT id FROM tx_rows WHERE s='TXV'",
                "tx_status",
            ),
        ] {
            exec(&mut session, "BEGIN");
            exec(
                &mut session,
                &format!("ALTER TYPE {target} ADD VALUE 'TXV'"),
            );
            assert_eq!(
                session.execute(statement, &[], None),
                Err(CoreError::Sql(SqlError::UnsafeNewEnumValue)),
                "{statement}"
            );
            assert_eq!(
                user_types::take_sql_error_message().as_deref(),
                Some(format!("unsafe use of new value \"TXV\" of enum type {spelling}").as_str())
            );
            assert_eq!(
                user_types::take_sql_error_hint(),
                Some("New enum values must be committed before they can be used.")
            );
            exec(&mut session, "ROLLBACK");
            assert_eq!(
                session.execute(&format!("SELECT 'TXV'::{target}"), &[], None),
                Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
            );
        }
        exec(&mut session, "BEGIN");
        exec(&mut session, "ALTER TYPE tx_status ADD VALUE 'TXV2'");
        exec(&mut session, "SAVEPOINT change_name");
        exec(
            &mut session,
            "ALTER TYPE tx_status RENAME VALUE 'TXV2' TO 'TXV3'",
        );
        assert_eq!(
            session.execute("SELECT 'TXV3'::tx_status", &[], None),
            Err(CoreError::Sql(SqlError::UnsafeNewEnumValue))
        );
        exec(&mut session, "ROLLBACK TO change_name");
        exec(
            &mut session,
            "ALTER TYPE tx_status RENAME VALUE 'TXV2' TO 'TXV4'",
        );
        exec(&mut session, "CREATE TYPE local_status AS ENUM ('a')");
        exec(&mut session, "ALTER TYPE local_status ADD VALUE 'b'");
        assert_eq!(
            exec(&mut session, "SELECT 'b'::local_status").rows[0].values,
            [Value::Text("b".into())]
        );
        exec(&mut session, "COMMIT");
        assert_eq!(
            exec(&mut session, "SELECT 'TXV4'::tx_status").rows[0].values,
            [Value::Text("TXV4".into())]
        );
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
