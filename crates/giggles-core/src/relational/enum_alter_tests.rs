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
    assert_eq!(reopened.ext.wire_version(), 6);
    run(&mut reopened, "ALTER TYPE phase ADD VALUE '' BEFORE 'todo'").unwrap();
    run(
        &mut reopened,
        "ALTER TYPE phase RENAME VALUE 'todo' TO 'queued'",
    )
    .unwrap();
    assert_eq!(reopened.ext.wire_version(), 12);
    assert_eq!(reopened.next_id, state.next_id + 1);
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
fn rename_updates_enum_literals_in_a_dependent_check() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TYPE phase AS ENUM ('todo','done')").unwrap();
    run(
        &mut state,
        "CREATE TABLE jobs (s phase PRIMARY KEY CHECK (s <> 'done'))",
    )
    .unwrap();
    run(&mut state, "INSERT INTO jobs VALUES ('todo')").unwrap();
    run(&mut state, "ALTER TYPE phase RENAME VALUE 'done' TO 'closed'").unwrap();
    fail(
        &mut state,
        "INSERT INTO jobs VALUES ('closed')",
        SqlError::CheckViolation,
        "new row for relation \"jobs\" violates check constraint \"jobs_s_check\"",
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
        for (create_before_savepoint, release_before_add) in [(true, false), (false, false), (false, true)] {
            exec(&mut session, "BEGIN");
            let create = "CREATE TYPE savepoint_status AS ENUM ('a')";
            if create_before_savepoint {
                exec(&mut session, create);
            }
            exec(&mut session, "SAVEPOINT sub");
            if !create_before_savepoint {
                exec(&mut session, create);
            }
            if release_before_add {
                exec(&mut session, "RELEASE SAVEPOINT sub");
            }
            exec(&mut session, "ALTER TYPE savepoint_status ADD VALUE 'b'");
            if create_before_savepoint {
                exec(&mut session, "RELEASE SAVEPOINT sub");
            }
            assert_eq!(
                session.execute("SELECT 'b'::savepoint_status", &[], None),
                Err(CoreError::Sql(SqlError::UnsafeNewEnumValue)),
            );
            assert_eq!(user_types::take_sql_error_message().as_deref(),
                Some("unsafe use of new value \"b\" of enum type savepoint_status"));
            assert_eq!(user_types::take_sql_error_hint(),
                Some("New enum values must be committed before they can be used."));
            exec(&mut session, "ROLLBACK");
        }
        exec(&mut session, "BEGIN");
        exec(&mut session, "CREATE TYPE rolled_back AS ENUM ('a')");
        exec(&mut session, "SAVEPOINT sub");
        exec(&mut session, "ALTER TYPE rolled_back ADD VALUE 'b'");
        exec(&mut session, "ROLLBACK TO SAVEPOINT sub");
        exec(&mut session, "RELEASE SAVEPOINT sub");
        exec(&mut session, "ALTER TYPE rolled_back ADD VALUE 'b'");
        assert_eq!(exec(&mut session, "SELECT 'b'::rolled_back").rows[0].values,
            [Value::Text("b".into())]);
        exec(&mut session, "COMMIT");

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
        exec(&mut session, "RELEASE SAVEPOINT change_name");
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

#[test]
fn initial_enum_uses_frozen_pre_am2_namespace_bytes() {
    // Legacy namespace 5, format 2 and catalog extension v6, as written by
    // the pre-AM-2 encoder for CREATE TYPE phase AS ENUM ('z','a').
    const OLD: &[u8] = &[
        2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 112, 104, 97, 115,
        101, 6, 0, 0, 0, 0, 0, 0, 0, 112, 117, 98, 108, 105, 99, 2,
        0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 122,
        1, 0, 0, 0, 0, 0, 0, 0, 97, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0,
    ];
    let mut fresh = Relational::default();
    run(&mut fresh, "CREATE TYPE phase AS ENUM ('z','a')").unwrap();
    assert_eq!(bincode::serialize(&fresh).unwrap(), OLD);

    let mut reopened: Relational = bincode::deserialize(OLD).unwrap();
    assert_eq!(bincode::serialize(&reopened).unwrap(), OLD);
    let (id, _) = reopened.type_by_name("phase").unwrap();
    let [z, a] = reopened.user_type_catalog().enum_label_oids[&id][..] else { panic!() };
    run(&mut reopened, "ALTER TYPE phase RENAME VALUE 'z' TO 'original'").unwrap();
    assert_eq!(reopened.ext.wire_version(), 6);
    assert_eq!(reopened.user_type_catalog().enum_label_oids[&id], [z, a]);

    run(&mut reopened, "CREATE TABLE later (id int PRIMARY KEY)").unwrap();
    run(&mut reopened, "ALTER TYPE phase ADD VALUE 'middle' BEFORE 'a'").unwrap();
    run(&mut reopened, "ALTER TYPE phase ADD VALUE 'after' AFTER 'a'").unwrap();
    run(&mut reopened, "ALTER TYPE phase ADD VALUE 'end'").unwrap();
    let ids = reopened.user_type_catalog().enum_label_oids[&id].clone();
    assert_eq!((ids[0], ids[2]), (z, a));
    assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), 5);
    assert!(!ids.contains(&type_oid(reopened.tables["later"].id).unwrap()));
    run(&mut reopened, "ALTER TYPE phase RENAME VALUE 'middle' TO 'changed'").unwrap();
    let new = bincode::serialize(&reopened).unwrap();
    let mut reopened: Relational = bincode::deserialize(&new).unwrap();
    assert_eq!(reopened.ext.wire_version(), 12);
    assert_eq!(reopened.user_type_catalog().enum_label_oids[&id], ids);
    assert_eq!(
        run(&mut reopened, "SELECT 'changed'::phase < 'a'::phase").unwrap().rows[0].values,
        [Value::Bool(true)],
    );
    run(&mut reopened, "DROP TYPE phase").unwrap();
    assert!(reopened.ext.added_enum_labels.is_empty());
    assert_eq!(reopened.ext.wire_version(), 1);
}

#[test]
fn enum_array_comparisons_assignments_and_aggregates_match_the_oracle() {
    fn rows(state: &mut Relational, sql: &str) -> Vec<Vec<Value>> {
        run(state, sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?} ({:?})", user_types::take_sql_error_message()))
            .rows.into_iter().map(|row| row.values).collect()
    }
    let text = |s: &str| Value::Text(s.into());
    let mut state = Relational::default();
    for sql in [
        r#"CREATE TYPE "SizeExtra" AS ENUM ('Z','A')"#,
        "CREATE TYPE rv_other AS ENUM ('Z','A')",
        r#"CREATE TABLE rv_arr(id int PRIMARY KEY, v "SizeExtra"[] DEFAULT ARRAY['Z']::"SizeExtra"[])"#,
        "INSERT INTO rv_arr(id) VALUES(1)",
        r#"INSERT INTO rv_arr VALUES(2,ARRAY['A']::"SizeExtra"[]),(3,ARRAY['Z']::"SizeExtra"[]),(4,NULL)"#,
        r#"UPDATE rv_arr SET v=ARRAY['Z',NULL]::"SizeExtra"[] WHERE id=3"#,
        r#"INSERT INTO rv_arr SELECT 5, ARRAY['A',NULL]::"SizeExtra"[]"#,
    ] {
        rows(&mut state, sql);
    }
    assert_eq!(
        rows(&mut state, "SELECT id FROM rv_arr ORDER BY v NULLS LAST"),
        [1, 3, 2, 5, 4].map(|id| vec![Value::Int(id)]),
    );
    assert_eq!(
        rows(&mut state, r#"SELECT id FROM rv_arr WHERE v < ARRAY['A']::"SizeExtra"[] ORDER BY id"#),
        [1, 3].map(|id| vec![Value::Int(id)]),
    );
    assert_eq!(
        rows(&mut state, r#"SELECT ARRAY['Z']::"SizeExtra"[] < ARRAY['A']::"SizeExtra"[], ARRAY['Z']::"SizeExtra"[] = '{Z}'"#),
        [vec![Value::Bool(true), Value::Bool(true)]],
    );
    fail(&mut state, r#"SELECT ARRAY['Z']::"SizeExtra"[] = ARRAY['Z']::rv_other[]"#,
        SqlError::UndefinedFunction, r#"operator does not exist: "SizeExtra"[] = rv_other[]"#);
    fail(&mut state, r#"SELECT 'BOGUS' = ANY(ARRAY['Z']::"SizeExtra"[])"#,
        SqlError::InvalidTextRepresentation, r#"invalid input value for enum "SizeExtra": "BOGUS""#);
    fail(&mut state, r#"INSERT INTO rv_arr VALUES(6, ARRAY['Z']::"SizeExtra"[]),(7, ARRAY['BOGUS']::"SizeExtra"[])"#,
        SqlError::InvalidTextRepresentation, r#"invalid input value for enum "SizeExtra": "BOGUS""#);
    assert_eq!(
        rows(&mut state, r#"SELECT id, v, 'Z'::"SizeExtra" = ANY(v), pg_typeof(v) FROM rv_arr ORDER BY id"#),
        [
            (1, text("{Z}"), Value::Bool(true)),
            (2, text("{A}"), Value::Bool(false)),
            (3, text("{Z,NULL}"), Value::Bool(true)),
            (4, Value::Null, Value::Null),
            (5, text("{A,NULL}"), Value::Null),
        ].map(|(id, v, any)| vec![Value::Int(id), v, any, text(r#""SizeExtra"[]"#)]),
    );
    let arrays = [text("{Z}"), text("{Z,NULL}"), text("{A}"), text("{A,NULL}"), Value::Null];
    assert_eq!(
        rows(&mut state, "SELECT DISTINCT v FROM rv_arr ORDER BY v NULLS LAST"),
        arrays.clone().map(|v| vec![v]),
    );
    assert_eq!(
        rows(&mut state, "SELECT v, count(*) FROM rv_arr GROUP BY v ORDER BY v NULLS LAST"),
        arrays.map(|v| vec![v, Value::Int(1)]),
    );
    assert_eq!(
        rows(&mut state, "SELECT min(v), max(v) FROM rv_arr"),
        [vec![text("{Z}"), text("{A,NULL}")]],
    );

    rows(&mut state, "INSERT INTO rv_arr VALUES(6,'{Z}')");
    assert_eq!(
        rows(&mut state, "SELECT v, count(*) FROM rv_arr GROUP BY v ORDER BY v NULLS LAST LIMIT 1"),
        [vec![text("{Z}"), Value::Int(2)]],
    );
    assert_eq!(
        rows(&mut state, "SELECT count(DISTINCT v) FROM rv_arr"),
        [vec![Value::Int(4)]],
    );

    for sql in [
        "CREATE TYPE feeling AS ENUM ('sad','happy')",
        "ALTER TYPE feeling ADD VALUE 'ok' BEFORE 'happy'",
        "ALTER TYPE feeling ADD VALUE 'great' AFTER 'happy'",
        "ALTER TYPE feeling RENAME VALUE 'great' TO 'joyful'",
        "CREATE TABLE moods(id int PRIMARY KEY, v feeling)",
        "INSERT INTO moods VALUES (1,'happy'),(2,'ok'),(3,'sad'),(4,'joyful')",
    ] {
        rows(&mut state, sql);
    }
    assert_eq!(
        rows(&mut state, "SELECT min(v), max(v), array_agg(v ORDER BY v) FROM moods"),
        [vec![text("sad"), text("joyful"), text("{sad,ok,happy,joyful}")]],
    );
    rows(&mut state, "INSERT INTO moods VALUES (5,NULL)");
    assert_eq!(
        rows(&mut state, "SELECT array_agg(v ORDER BY v DESC NULLS LAST) FROM moods"),
        [vec![text("{joyful,happy,ok,sad,NULL}")]],
    );
}

#[test]
fn qualified_type_introspection_and_drop_notices() {
    let mut state = Relational::default();
    for sql in [
        "CREATE SCHEMA rv_s",
        r#"CREATE TYPE rv_s."WithDefault" AS ENUM ('a')"#,
        r#"CREATE TABLE rv_s.hd(id int PRIMARY KEY, v rv_s."WithDefault"[] DEFAULT ARRAY['a']::rv_s."WithDefault"[])"#,
        "INSERT INTO rv_s.hd(id) VALUES(1)",
    ] {
        run(&mut state, sql).unwrap();
    }
    for sql in [
        "SELECT pg_typeof(v) FROM rv_s.hd",
        "SELECT pg_typeof(v) FROM (SELECT v FROM rv_s.hd) AS derived",
        "SELECT pg_typeof(v) FROM rv_s.hd GROUP BY v",
    ] {
        assert_eq!(
            run(&mut state, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")).rows[0].values,
            [Value::Text(r#"rv_s."WithDefault"[]"#.into())],
            "{sql}",
        );
    }
    let sql = "SELECT pg_typeof(v) FROM rv_s.hd";
    let mut prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    prepared.search_path = vec!["rv_s".into(), "public".into()];
    assert_eq!(state.execute(&prepared, &[], None).unwrap().rows[0].values,
        [Value::Text(r#""WithDefault"[]"#.into())]);

    for (sql, message) in [
        ("DROP TYPE IF EXISTS rv_s.absent", r#"type "rv_s.absent" does not exist, skipping"#),
        ("DROP DOMAIN IF EXISTS rv_s.absent_d", r#"type "rv_s.absent_d" does not exist, skipping"#),
        ("DROP TYPE IF EXISTS missing_schema.absent", r#"schema "missing_schema" does not exist, skipping"#),
    ] {
        run(&mut state, sql).unwrap();
        assert_eq!(user_types::take_sql_notices(), vec![(message.into(), None, None)]);
    }
}
