//! Column defaults, serial/identity generation, and session sequence semantics.
use giggles_core::{
    Admin, CoreError, Limits,
    relational::{SqlError, StatementClock, Value},
};
use giggles_pg_types::temporal::{SessionZone, TimestampTz};
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

#[test]
fn literal_defaults_cover_omitted_explicit_multi_default_values_and_update() {
    let path = std::env::temp_dir().join(format!("defaults-a-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE d (id integer PRIMARY KEY, label text DEFAULT 'x', score integer DEFAULT -7, active boolean DEFAULT false)", &[], None).unwrap();
    s.execute("INSERT INTO d (id) VALUES (1)", &[], None)
        .unwrap();
    let result = s.execute("INSERT INTO d VALUES (2, DEFAULT, DEFAULT, DEFAULT), (3, 'y', 8, true) RETURNING id, label, score, active", &[], None).unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![
            Value::Int(2),
            Value::Text("x".into()),
            Value::Int(-7),
            Value::Bool(false)
        ]
    );
    let result = s
        .execute(
            "UPDATE d SET label = DEFAULT, score = DEFAULT WHERE id = 3 RETURNING label, score",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![Value::Text("x".into()), Value::Int(-7)]
    );
    s.execute(
        "CREATE TABLE all_defaults (v integer DEFAULT 12, label text DEFAULT 'fallback')",
        &[],
        None,
    )
    .unwrap();
    let result = s
        .execute(
            "INSERT INTO all_defaults DEFAULT VALUES RETURNING v, label",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![Value::Int(12), Value::Text("fallback".into())]
    );
    let result = s
        .execute(
            "INSERT INTO all_defaults VALUES (NULL, DEFAULT) RETURNING v, label",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![Value::Null, Value::Text("fallback".into())]
    );
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let result = admin
        .sql_session()
        .execute(
            "INSERT INTO all_defaults DEFAULT VALUES RETURNING v, label",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![Value::Int(12), Value::Text("fallback".into())]
    );
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn defaults_validate_types_and_reject_unadmitted_expressions() {
    let path = std::env::temp_dir().join(format!("defaults-errors-a-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE bad (v integer DEFAULT true)",
        "CREATE TABLE bad (v boolean DEFAULT 1)",
    ] {
        assert!(
            matches!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::Type))
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        s.execute("CREATE TABLE bad (v integer DEFAULT 'abc')", &[], None),
        Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
    ));
    for sql in [
        "CREATE TABLE bad (v integer DEFAULT 1 + 2)",
        "CREATE TABLE bad (v integer DEFAULT abs(-1))",
        "CREATE TABLE bad (v integer DEFAULT $1)",
        "CREATE TABLE bad (v integer DEFAULT other)",
    ] {
        assert!(
            matches!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::Unsupported))
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        s.execute(
            "CREATE TABLE bad (v integer DEFAULT nextval('missing'))",
            &[],
            None
        ),
        Err(CoreError::Sql(SqlError::UnknownTable))
    ));
    s.execute(
        "CREATE TABLE required (v integer NOT NULL DEFAULT NULL)",
        &[],
        None,
    )
    .unwrap();
    assert!(matches!(
        s.execute("INSERT INTO required DEFAULT VALUES", &[], None),
        Err(CoreError::Sql(SqlError::Null))
    ));
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn clock_defaults_evaluate_per_insert_and_matched_update_row() {
    let path = std::env::temp_dir().join(format!("defaults-clocks-a-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE ticks (id integer PRIMARY KEY, stamp timestamptz DEFAULT clock_timestamp())",
        &[],
        None,
    )
    .unwrap();
    let calls = Arc::new(AtomicI64::new(100));
    let wall = calls.clone();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 10,
        statement_us: 20,
        zone: Some(SessionZone::Fixed(0)),
        wall: Some(Arc::new(move || Ok(wall.fetch_add(1, Ordering::SeqCst)))),
    };
    let tz = |n| Value::TimestampTz(TimestampTz::from_microseconds(n).unwrap());
    let result = s
        .execute_at(
            "INSERT INTO ticks (id, stamp) VALUES (1, DEFAULT), (2, DEFAULT) RETURNING stamp",
            &[],
            None,
            &clock,
        )
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![tz(100), tz(101)]
    );
    s.execute_at(
        "UPDATE ticks SET stamp = DEFAULT WHERE id = 99",
        &[],
        None,
        &clock,
    )
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 102);
    s.execute_at(
        "UPDATE ticks SET stamp = DEFAULT WHERE id >= 1",
        &[],
        None,
        &clock,
    )
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 104);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

struct SequenceDb {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl SequenceDb {
    fn new() -> Self {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("defaults-b-{:032x}", u128::from_le_bytes(nonce)));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}
impl Drop for SequenceDb {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn sequence_clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    }
}
fn run(s: &mut giggles_core::SqlSession, sql: &str) -> giggles_core::relational::SqlResult {
    s.execute_at(sql, &[], None, &sequence_clock())
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn scalar(s: &mut giggles_core::SqlSession, sql: &str) -> Value {
    run(s, sql).rows[0].values[0].clone()
}
fn fails(s: &mut giggles_core::SqlSession, sql: &str, error: SqlError) {
    assert_eq!(
        s.execute_at(sql, &[], None, &sequence_clock()),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
}
#[test]
fn serial_identity_harness_cases_and_overriding_match_pg18() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE serial_case (id serial PRIMARY KEY, name text)",
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO serial_case (name) VALUES ('Ada') RETURNING id"
        ),
        Value::Int(1)
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO serial_case (name) VALUES ('Bea') RETURNING id"
        ),
        Value::Int(2)
    );
    run(
        &mut s,
        "CREATE TABLE bigserial_case (id bigserial PRIMARY KEY, name text)",
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO bigserial_case (name) VALUES ('Ada') RETURNING id"
        ),
        Value::Int(1)
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO bigserial_case (name) VALUES ('Bea') RETURNING id"
        ),
        Value::Int(2)
    );
    run(
        &mut s,
        "CREATE TABLE keyword_case (row_id serial PRIMARY KEY, id integer DEFAULT 7, name text DEFAULT 'fallback')",
    );
    run(
        &mut s,
        "INSERT INTO keyword_case (id,name) VALUES (DEFAULT,'x')",
    );
    run(&mut s, "INSERT INTO keyword_case DEFAULT VALUES");
    assert_eq!(
        run(&mut s, "SELECT id,name FROM keyword_case ORDER BY id,name")
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(7), Value::Text("fallback".into())],
            vec![Value::Int(7), Value::Text("x".into())]
        ]
    );
    run(
        &mut s,
        "CREATE TABLE always_case (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,name text)",
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO always_case (name) VALUES ('generated') RETURNING id"
        ),
        Value::Int(1)
    );
    fails(
        &mut s,
        "INSERT INTO always_case VALUES (10,'rejected')",
        SqlError::GeneratedAlways,
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO always_case OVERRIDING SYSTEM VALUE VALUES (10,'overridden') RETURNING id"
        ),
        Value::Int(10)
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO always_case OVERRIDING USER VALUE VALUES (99,'replaced') RETURNING id"
        ),
        Value::Int(2)
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO always_case VALUES (DEFAULT,'default') RETURNING id"
        ),
        Value::Int(3)
    );
    fails(
        &mut s,
        "UPDATE always_case SET id = 40 WHERE id = 1",
        SqlError::GeneratedAlways,
    );
    assert_eq!(
        scalar(
            &mut s,
            "UPDATE always_case SET id = DEFAULT WHERE id = 1 RETURNING id"
        ),
        Value::Int(4)
    );
    run(
        &mut s,
        "CREATE TABLE by_default (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,name text)",
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO by_default (name) VALUES ('generated') RETURNING id"
        ),
        Value::Int(1)
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO by_default VALUES (10,'explicit') RETURNING id"
        ),
        Value::Int(10)
    );
    assert_eq!(
        scalar(&mut s, "INSERT INTO by_default DEFAULT VALUES RETURNING id"),
        Value::Int(2)
    );
}
#[test]
fn multirow_defaults_share_statement_time_but_generate_distinct_sequence_values() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE multi_defaults (id serial PRIMARY KEY, stamp timestamptz DEFAULT now())",
    );
    let result = run(
        &mut s,
        "INSERT INTO multi_defaults (stamp) VALUES (DEFAULT),(DEFAULT),(DEFAULT) RETURNING id,stamp",
    );
    let tz = Value::TimestampTz(TimestampTz::from_microseconds(100).unwrap());
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1), tz.clone()],
            vec![Value::Int(2), tz.clone()],
            vec![Value::Int(3), tz]
        ]
    );
    run(
        &mut s,
        "CREATE TABLE shared_default (v bigint DEFAULT nextval('public.multi_defaults_id_seq'::regclass))",
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO shared_default DEFAULT VALUES RETURNING v"
        ),
        Value::Int(4)
    );
    fails(
        &mut s,
        "CREATE TABLE bad_default (v integer DEFAULT -nextval('multi_defaults_id_seq'))",
        SqlError::Unsupported,
    );
}
#[test]
fn sequence_rollback_setval_currval_and_stale_sessions_match_pg18() {
    let db = SequenceDb::new();
    let mut a = db.admin().sql_session();
    let mut b = db.admin().sql_session();
    run(
        &mut a,
        "CREATE TABLE rollback_sequence (id serial PRIMARY KEY)",
    );
    fails(
        &mut a,
        "SELECT currval('rollback_sequence_id_seq')",
        SqlError::ObjectNotInPrerequisiteState,
    );
    run(&mut a, "BEGIN");
    assert_eq!(
        scalar(
            &mut a,
            "INSERT INTO rollback_sequence DEFAULT VALUES RETURNING id"
        ),
        Value::Int(1)
    );
    run(&mut a, "ROLLBACK");
    assert_eq!(
        scalar(
            &mut a,
            "INSERT INTO rollback_sequence DEFAULT VALUES RETURNING id"
        ),
        Value::Int(2)
    );
    fails(
        &mut b,
        "SELECT currval('rollback_sequence_id_seq')",
        SqlError::ObjectNotInPrerequisiteState,
    );
    run(&mut b, "BEGIN");
    assert_eq!(
        scalar(&mut b, "SELECT nextval('rollback_sequence_id_seq')"),
        Value::Int(3)
    );
    assert_eq!(
        scalar(&mut a, "SELECT setval('rollback_sequence_id_seq',1,false)"),
        Value::Int(1)
    );
    assert_eq!(
        scalar(&mut a, "SELECT currval('rollback_sequence_id_seq')"),
        Value::Int(2)
    );
    assert_eq!(
        scalar(&mut b, "SELECT nextval('rollback_sequence_id_seq')"),
        Value::Int(1)
    );
    run(&mut b, "ROLLBACK");
    assert_eq!(
        scalar(&mut a, "SELECT nextval('rollback_sequence_id_seq')"),
        Value::Int(2)
    );
    assert_eq!(
        scalar(&mut a, "SELECT setval('rollback_sequence_id_seq',9)"),
        Value::Int(9)
    );
    assert_eq!(
        scalar(&mut a, "SELECT currval('rollback_sequence_id_seq')"),
        Value::Int(9)
    );
    assert_eq!(
        scalar(&mut a, "SELECT nextval('rollback_sequence_id_seq')"),
        Value::Int(10)
    );
    fails(
        &mut a,
        "SELECT nextval('missing_sequence')",
        SqlError::UnknownTable,
    );
}
// CYCLE note (DD-0068 E1024 N1): the reopen guarantee is "never reissue a value from the current
// reservation". With CYCLE, a value from an earlier lap can legitimately come back after a reopen,
// exactly as PostgreSQL gives no uniqueness guarantee for cycling sequences.
#[test]
fn descending_and_cycle_identity_options_and_smallserial_limit_match_pg18() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE descending (id integer GENERATED BY DEFAULT AS IDENTITY (START WITH -1 INCREMENT BY -1 MINVALUE -3 MAXVALUE -1 CYCLE))",
    );
    let result = run(
        &mut s,
        "INSERT INTO descending VALUES (DEFAULT),(DEFAULT),(DEFAULT),(DEFAULT) RETURNING id",
    );
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Int(-1),
            Value::Int(-2),
            Value::Int(-3),
            Value::Int(-1)
        ]
    );
    run(
        &mut s,
        "CREATE TABLE ascending (id integer GENERATED ALWAYS AS IDENTITY (START WITH 5 INCREMENT BY 2 MINVALUE 1 MAXVALUE 7 CYCLE))",
    );
    let result = run(
        &mut s,
        "INSERT INTO ascending VALUES (DEFAULT),(DEFAULT),(DEFAULT) RETURNING id",
    );
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(5), Value::Int(7), Value::Int(1)]
    );
    run(&mut s, "CREATE TABLE tiny (id smallserial)");
    run(&mut s, "SELECT setval('tiny_id_seq',32767,false)");
    assert_eq!(
        scalar(&mut s, "INSERT INTO tiny DEFAULT VALUES RETURNING id"),
        Value::Int(32767)
    );
    fails(
        &mut s,
        "INSERT INTO tiny DEFAULT VALUES",
        SqlError::SequenceLimit,
    );
}
#[test]
fn failed_create_is_atomic_sequence_names_collide_and_drop_clears_session_identity() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(&mut s, "CREATE TABLE names_id_seq (v integer)");
    run(&mut s, "CREATE TABLE names (id serial)");
    assert_eq!(
        scalar(&mut s, "SELECT nextval('names_id_seq1')"),
        Value::Int(1)
    );
    fails(
        &mut s,
        "CREATE TABLE bad_create (a serial,b integer GENERATED ALWAYS AS IDENTITY (INCREMENT BY 0))",
        SqlError::InvalidParameterValue,
    );
    fails(
        &mut s,
        "SELECT nextval('bad_create_a_seq')",
        SqlError::UnknownTable,
    );
    run(&mut s, "CREATE TABLE bad_create (a serial)");
    assert_eq!(
        scalar(&mut s, "SELECT nextval('bad_create_a_seq')"),
        Value::Int(1)
    );
    assert_eq!(db.admin().drop_table("bad_create", false), Ok(true));
    fails(
        &mut s,
        "SELECT nextval('bad_create_a_seq')",
        SqlError::UnknownTable,
    );
    run(&mut s, "CREATE TABLE bad_create (a serial)");
    fails(
        &mut s,
        "SELECT currval('bad_create_a_seq')",
        SqlError::ObjectNotInPrerequisiteState,
    );
    assert_eq!(
        scalar(&mut s, "SELECT nextval('bad_create_a_seq')"),
        Value::Int(1)
    );
    run(
        &mut s,
        "CREATE TABLE forward_default (v bigint DEFAULT nextval('forward_default_id_seq'), id serial)",
    );
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO forward_default DEFAULT VALUES RETURNING v,id"
        )
        .rows[0]
            .values,
        vec![Value::Int(1), Value::Int(2)]
    );
}
#[test]
fn legacy_calls_pin_sequence_evaluation_unsupported_and_explicit_values_do_not_generate() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(&mut s, "CREATE TABLE legacy_serial (id serial PRIMARY KEY)");
    assert_eq!(
        s.execute("INSERT INTO legacy_serial DEFAULT VALUES", &[], None),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    s.execute("INSERT INTO legacy_serial VALUES (10)", &[], None)
        .unwrap();
    assert_eq!(
        s.execute("SELECT nextval('legacy_serial_id_seq')", &[], None),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO legacy_serial DEFAULT VALUES RETURNING id"
        ),
        Value::Int(1)
    );
    let input = serde_json::to_vec(&giggles_core::local::AdminCommand::Sql {
        sql: "INSERT INTO legacy_serial DEFAULT VALUES".into(),
        params: vec![],
        expected_version: None,
    })
    .unwrap();
    assert_eq!(
        giggles_core::local::admin(db.admin(), &input).data["error"],
        "Sql(Unsupported)"
    );
}

#[test]
fn quoted_names_identity_updates_and_conflict_assignments_preserve_rules() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(&mut s, "CREATE TABLE \"Odd.Table\" (id serial)");
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO \"Odd.Table\" DEFAULT VALUES RETURNING id"
        ),
        Value::Int(1)
    );
    assert_eq!(
        scalar(&mut s, "SELECT nextval('\"Odd.Table_id_seq\"')"),
        Value::Int(2)
    );
    run(
        &mut s,
        "CREATE TABLE update_identity (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)",
    );
    run(&mut s, "INSERT INTO update_identity DEFAULT VALUES");
    assert_eq!(
        scalar(
            &mut s,
            "UPDATE update_identity SET id = 20 WHERE id = 1 RETURNING id"
        ),
        Value::Int(20)
    );
    run(
        &mut s,
        "CREATE TABLE conflict_identity (key integer PRIMARY KEY,id integer GENERATED ALWAYS AS IDENTITY)",
    );
    run(&mut s, "INSERT INTO conflict_identity (key) VALUES (1)");
    fails(
        &mut s,
        "INSERT INTO conflict_identity (key) VALUES (1) ON CONFLICT (key) DO UPDATE SET id = 10",
        SqlError::GeneratedAlways,
    );
    assert_eq!(
        scalar(&mut s, "SELECT currval('conflict_identity_id_seq')"),
        Value::Int(1)
    );
}

#[test]
fn preflight_dependencies_cover_create_omitted_default_override_and_zero_row_update() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE dependency (id integer GENERATED BY DEFAULT AS IDENTITY,tag integer)",
    );
    for sql in [
        "INSERT INTO dependency (tag) VALUES (1)",
        "INSERT INTO dependency VALUES (DEFAULT,1)",
        "INSERT INTO dependency OVERRIDING USER VALUE VALUES (99,1)",
        "UPDATE dependency SET id = DEFAULT WHERE tag = 999",
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(
            s.implicit_sequence_dependencies(&p).unwrap(),
            vec![Some("dependency_id_seq".into())],
            "{sql}"
        );
    }
    let p = s.prepare("INSERT INTO dependency VALUES (99,1)").unwrap();
    assert!(s.implicit_sequence_dependencies(&p).unwrap().is_empty());
    assert_eq!(
        giggles_core::relational::Prepared::sequence_dependencies(
            "CREATE TABLE manual (id bigint DEFAULT nextval('missing'))"
        ),
        vec![Some("missing".into())]
    );
    assert!(
        giggles_core::relational::Prepared::sequence_dependencies(
            "CREATE TABLE generated (id serial)"
        )
        .is_empty()
    );
    assert_eq!(
        giggles_core::relational::Prepared::sequence_dependencies(
            "INSERT INTO dependency VALUES (9,1) RETURNING nextval('dependency_id_seq')"
        ),
        vec![Some("dependency_id_seq".into())]
    );
}

#[test]
fn searched_identity_rekeys_are_atomic_and_enforce_immediate_uniqueness() {
    let db = SequenceDb::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE rekeys (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)",
    );
    run(&mut s, "INSERT INTO rekeys VALUES (DEFAULT),(DEFAULT)");
    let before = run(&mut s, "SELECT id FROM rekeys ORDER BY id").rows;
    run(&mut s, "SELECT setval('rekeys_id_seq',2,false)");
    fails(
        &mut s,
        "UPDATE rekeys SET id = DEFAULT WHERE id >= 1",
        SqlError::Duplicate,
    );
    assert_eq!(
        scalar(&mut s, "SELECT currval('rekeys_id_seq')"),
        Value::Int(2)
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM rekeys ORDER BY id").rows,
        before
    );
    run(&mut s, "SELECT setval('rekeys_id_seq',3,false)");
    let result = run(
        &mut s,
        "UPDATE rekeys SET id = DEFAULT WHERE id >= 1 RETURNING id",
    );
    assert_eq!(result.affected, 2);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(3), Value::Int(4)]
    );
    let before = run(&mut s, "SELECT id FROM rekeys ORDER BY id").rows;
    fails(
        &mut s,
        "UPDATE rekeys SET id = 4 WHERE id = 3",
        SqlError::Duplicate,
    );
    fails(
        &mut s,
        "UPDATE rekeys SET id = 9 WHERE id >= 3",
        SqlError::Duplicate,
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM rekeys ORDER BY id").rows,
        before
    );
    run(
        &mut s,
        "CREATE TABLE wrap_rekeys (id integer GENERATED ALWAYS AS IDENTITY (MAXVALUE 3 CYCLE) PRIMARY KEY)",
    );
    run(&mut s, "INSERT INTO wrap_rekeys VALUES (DEFAULT),(DEFAULT)");
    run(&mut s, "SELECT setval('wrap_rekeys_id_seq',3,false)");
    assert_eq!(
        run(&mut s, "UPDATE wrap_rekeys SET id = DEFAULT WHERE id >= 1").affected,
        2
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM wrap_rekeys ORDER BY id")
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(3)]
    );
}

#[test]
fn stale_commits_preserve_latest_stamp_for_ascending_descending_and_wrapped_values() {
    let db = SequenceDb::new();
    let mut a = db.admin().sql_session();
    let mut b = db.admin().sql_session();
    run(&mut a, "CREATE TABLE stamped (id serial)");
    run(&mut a, "BEGIN");
    assert_eq!(
        scalar(&mut a, "SELECT nextval('stamped_id_seq')"),
        Value::Int(1)
    );
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_id_seq')"),
        Value::Int(2)
    );
    run(&mut a, "COMMIT");
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_id_seq')"),
        Value::Int(3)
    );
    run(&mut a, "BEGIN");
    run(&mut a, "SELECT setval('stamped_id_seq',1,false)");
    run(&mut a, "ROLLBACK");
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_id_seq')"),
        Value::Int(1)
    );
    run(
        &mut a,
        "CREATE TABLE stamped_down (id integer GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY -1))",
    );
    run(&mut a, "BEGIN");
    assert_eq!(
        scalar(&mut a, "SELECT nextval('stamped_down_id_seq')"),
        Value::Int(-1)
    );
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_down_id_seq')"),
        Value::Int(-2)
    );
    run(&mut a, "COMMIT");
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_down_id_seq')"),
        Value::Int(-3)
    );
    run(
        &mut a,
        "CREATE TABLE stamped_wrap (id integer GENERATED BY DEFAULT AS IDENTITY (MAXVALUE 3 CYCLE))",
    );
    run(&mut b, "SELECT nextval('stamped_wrap_id_seq')");
    run(&mut b, "SELECT nextval('stamped_wrap_id_seq')");
    run(&mut a, "BEGIN");
    assert_eq!(
        scalar(&mut a, "SELECT nextval('stamped_wrap_id_seq')"),
        Value::Int(3)
    );
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_wrap_id_seq')"),
        Value::Int(1)
    );
    run(&mut a, "COMMIT");
    assert_eq!(
        scalar(&mut b, "SELECT nextval('stamped_wrap_id_seq')"),
        Value::Int(2)
    );
}

#[test]
fn durable_reservations_keep_live_values_consecutive_and_skip_returned_values_after_reopen() {
    let mut db = SequenceDb::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE durable_up (id serial)");
        run(
            &mut s,
            "CREATE TABLE durable_down (id integer GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY -1))",
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_up_id_seq')"),
            Value::Int(1)
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_up_id_seq')"),
            Value::Int(2)
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_down_id_seq')"),
            Value::Int(-1)
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_down_id_seq')"),
            Value::Int(-2)
        );
        // An ordinary SQL publication must preserve the live cursor and its
        // independently durable reservation, including the descending stamp.
        run(
            &mut s,
            "CREATE TABLE reservation_publication (id integer PRIMARY KEY)",
        );
        run(&mut s, "INSERT INTO reservation_publication VALUES (1)");
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_up_id_seq')"),
            Value::Int(3)
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_down_id_seq')"),
            Value::Int(-3)
        );
    }
    db.reopen();
    {
        let mut s = db.admin().sql_session();
        let Value::Int(up) = scalar(&mut s, "SELECT nextval('durable_up_id_seq')") else {
            panic!("integer sequence result");
        };
        let Value::Int(down) = scalar(&mut s, "SELECT nextval('durable_down_id_seq')") else {
            panic!("integer sequence result");
        };
        assert!(
            up > 3,
            "reopened ascending value {up} repeated a returned value"
        );
        assert!(
            down < -3,
            "reopened descending value {down} repeated a returned value"
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_up_id_seq')"),
            Value::Int(up + 1)
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_down_id_seq')"),
            Value::Int(down - 1)
        );
    }
}

#[test]
fn durable_setval_false_survives_reopen_and_its_first_returned_value_is_not_reused() {
    let mut db = SequenceDb::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE durable_setval (id serial)");
        assert_eq!(
            scalar(&mut s, "SELECT setval('durable_setval_id_seq',100,false)"),
            Value::Int(100)
        );
    }
    db.reopen();
    {
        let mut s = db.admin().sql_session();
        fails(
            &mut s,
            "SELECT currval('durable_setval_id_seq')",
            SqlError::ObjectNotInPrerequisiteState,
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_setval_id_seq')"),
            Value::Int(100)
        );
        run(
            &mut s,
            "CREATE TABLE setval_publication (id integer PRIMARY KEY)",
        );
        run(&mut s, "INSERT INTO setval_publication VALUES (1)");
    }
    db.reopen();
    {
        let mut s = db.admin().sql_session();
        let Value::Int(value) = scalar(&mut s, "SELECT nextval('durable_setval_id_seq')") else {
            panic!("integer sequence result");
        };
        assert!(
            value > 100,
            "setval(false) first returned value was reused after restart: {value}"
        );
        assert_eq!(
            scalar(&mut s, "SELECT nextval('durable_setval_id_seq')"),
            Value::Int(value + 1)
        );
    }
}
