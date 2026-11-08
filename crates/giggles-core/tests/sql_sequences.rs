//! Literal PostgreSQL 18.6 sequence expectations, captured in
//! e1-sql/evidence/oracle-pg186-sequence-ddl{,-extra}.txt.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, SqlResult, StatementClock, Value},
};
use giggles_pg_types::temporal::SessionZone;

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::path::Path::new("J:/gigglesdb-work/e1-sql/tmp/seq")
            .join(format!("db-{:032x}", u128::from_le_bytes(nonce)));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
    fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}
impl Drop for Db {
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
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute_at(sql, &[], None, &sequence_clock())
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn run_tag(s: &mut SqlSession, sql: &str, expected: &str) -> SqlResult {
    let prepared = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(prepared.command(), expected, "{sql}");
    run(s, sql)
}
fn scalar(s: &mut SqlSession, sql: &str, expected: i64) {
    assert_eq!(
        run(s, sql).rows[0].values,
        vec![Value::Int(expected)],
        "{sql}"
    );
}
// Backlog: exact PG error and notice text via a designed side channel.
fn error(s: &mut SqlSession, sql: &str, expected: SqlError, state: &str) {
    let expected_state = match expected {
        SqlError::DuplicateTable => "42P07",
        SqlError::UnknownTable => "42P01",
        SqlError::DependentObjects => "2BP01",
        SqlError::InvalidParameterValue => "22023",
        SqlError::UnknownColumn => "42703",
        SqlError::ObjectNotInPrerequisiteState => "55000",
        SqlError::SequenceLimit => "2200H",
        SqlError::Syntax => "42601",
        SqlError::Unsupported => "0A000",
        SqlError::WrongObjectType => "42809",
        _ => panic!("unmapped sequence error: {expected:?}"),
    };
    assert_eq!(expected_state, state, "{sql}");
    assert_eq!(
        s.execute_at(sql, &[], None, &sequence_clock()),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

#[test]
fn restart_is_transactional_but_nextval_and_setval_are_not() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s START WITH 5 INCREMENT BY 2");
    scalar(&mut s, "SELECT nextval('s')", 5);
    scalar(&mut s, "SELECT setval('s',20,true)", 20);
    scalar(&mut s, "SELECT nextval('s')", 22);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE s RESTART WITH 100");
    scalar(&mut s, "SELECT nextval('s')", 100);
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('s')", 24);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE s RESTART");
    scalar(&mut s, "SELECT nextval('s')", 5);
    run(&mut s, "COMMIT");
    scalar(&mut s, "SELECT nextval('s')", 7);
    run(&mut s, "BEGIN");
    scalar(&mut s, "SELECT nextval('s')", 9);
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('s')", 11);
    run(&mut s, "BEGIN");
    scalar(&mut s, "SELECT setval('s',40,false)", 40);
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('s')", 40);
}

#[test]
fn ddl_and_restart_restore_at_savepoints_and_rollback() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE SEQUENCE abandoned");
    run(&mut s, "ROLLBACK");
    error(
        &mut s,
        "ALTER SEQUENCE abandoned RESTART",
        SqlError::UnknownTable,
        "42P01",
    );
    run(&mut s, "CREATE SEQUENCE s START 5 INCREMENT 2");
    scalar(&mut s, "SELECT nextval('s')", 5);
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT a");
    run(&mut s, "ALTER SEQUENCE s RESTART 100");
    scalar(&mut s, "SELECT nextval('s')", 100);
    run(&mut s, "ROLLBACK TO a");
    scalar(&mut s, "SELECT nextval('s')", 7);
    run(&mut s, "CREATE SEQUENCE transient");
    run(&mut s, "DROP SEQUENCE s");
    run(&mut s, "ROLLBACK TO a");
    scalar(&mut s, "SELECT nextval('s')", 9);
    run(&mut s, "COMMIT");
    error(
        &mut s,
        "DROP SEQUENCE transient",
        SqlError::UnknownTable,
        "42P01",
    );
    run(&mut s, "BEGIN");
    run(&mut s, "DROP SEQUENCE s");
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('s')", 11);
}

#[test]
fn start_bounds_cycle_negative_increment_and_type_changes() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s START 5");
    scalar(&mut s, "SELECT nextval('s')", 5);
    run(&mut s, "ALTER SEQUENCE s START 50");
    scalar(&mut s, "SELECT nextval('s')", 6);
    run(&mut s, "ALTER SEQUENCE s RESTART");
    scalar(&mut s, "SELECT nextval('s')", 50);
    run(&mut s, "CREATE SEQUENCE cycle MAXVALUE 3 CYCLE");
    for n in [1, 2, 3, 1] {
        scalar(&mut s, "SELECT nextval('cycle')", n);
    }
    run(
        &mut s,
        "CREATE SEQUENCE neg INCREMENT -2 MINVALUE -5 MAXVALUE -1 CYCLE",
    );
    for n in [-1, -3, -5, -1] {
        scalar(&mut s, "SELECT nextval('neg')", n);
    }
    run(&mut s, "CREATE SEQUENCE typ AS integer");
    run(&mut s, "ALTER SEQUENCE typ AS smallint");
    scalar(&mut s, "SELECT setval('typ',32767)", 32767);
    error(
        &mut s,
        "SELECT nextval('typ')",
        SqlError::SequenceLimit,
        "2200H",
    );
}

#[test]
fn literal_pg_validation_diagnostics() {
    let db = Db::new();
    let mut s = db.session();
    for sql in [
        "CREATE SEQUENCE a INCREMENT 0",
        "CREATE SEQUENCE a MINVALUE 10 MAXVALUE 5",
        "CREATE SEQUENCE a START 1 MINVALUE 5",
        "CREATE SEQUENCE a START 6 MAXVALUE 5",
        "CREATE SEQUENCE a CACHE 0",
        "CREATE SEQUENCE a AS text",
        "CREATE SEQUENCE a AS smallint MAXVALUE 32768",
        "CREATE SEQUENCE a AS smallint MINVALUE -32769",
    ] {
        error(&mut s, sql, SqlError::InvalidParameterValue, "22023");
    }
    error(
        &mut s,
        "CREATE SEQUENCE a INCREMENT 1 INCREMENT 2",
        SqlError::Syntax,
        "42601",
    );
    run(&mut s, "CREATE SEQUENCE a MINVALUE 5 START 5 MAXVALUE 10");
    error(
        &mut s,
        "ALTER SEQUENCE a RESTART 4",
        SqlError::InvalidParameterValue,
        "22023",
    );
    scalar(&mut s, "SELECT setval('a',9)", 9);
    error(
        &mut s,
        "ALTER SEQUENCE a MAXVALUE 8",
        SqlError::InvalidParameterValue,
        "22023",
    );
}

#[test]
fn namespace_collisions_missing_objects_and_notices() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s");
    for sql in ["CREATE SEQUENCE s", "CREATE TABLE s(id integer)"] {
        error(&mut s, sql, SqlError::DuplicateTable, "42P07");
    }
    run(&mut s, "CREATE TABLE t(id integer)");
    error(
        &mut s,
        "CREATE SEQUENCE t",
        SqlError::DuplicateTable,
        "42P07",
    );
    assert_eq!(
        run_tag(&mut s, "CREATE SEQUENCE IF NOT EXISTS s", "CREATE SEQUENCE").affected,
        0
    );

    error(
        &mut s,
        "ALTER SEQUENCE missing RESTART",
        SqlError::UnknownTable,
        "42P01",
    );
    error(
        &mut s,
        "DROP SEQUENCE missing",
        SqlError::UnknownTable,
        "42P01",
    );
    for (sql, tag) in [
        ("ALTER SEQUENCE IF EXISTS missing RESTART", "ALTER SEQUENCE"),
        ("DROP SEQUENCE IF EXISTS missing", "DROP SEQUENCE"),
    ] {
        assert_eq!(run_tag(&mut s, sql, tag).affected, 0);
    }
}

#[test]
fn dependencies_owned_by_detach_identity_and_default_cascade() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s");
    run(&mut s, "CREATE TABLE t(id integer DEFAULT nextval('s'))");
    error(
        &mut s,
        "DROP SEQUENCE s",
        SqlError::DependentObjects,
        "2BP01",
    );
    run_tag(&mut s, "DROP SEQUENCE s CASCADE", "DROP SEQUENCE");
    run(&mut s, "INSERT INTO t DEFAULT VALUES");
    assert_eq!(
        run(&mut s, "SELECT id FROM t").rows[0].values,
        vec![Value::Null]
    );
    run(&mut s, "CREATE SEQUENCE owned OWNED BY t.id");
    run(&mut s, "ALTER SEQUENCE owned OWNED BY NONE");
    run(&mut s, "DROP TABLE t");
    scalar(&mut s, "SELECT nextval('owned')", 1);
    run(&mut s, "CREATE TABLE owner(id integer)");
    error(
        &mut s,
        "ALTER SEQUENCE owned OWNED BY owner.nope",
        SqlError::UnknownColumn,
        "42703",
    );
    error(
        &mut s,
        "CREATE SEQUENCE missing_owner OWNED BY missing.id",
        SqlError::UnknownTable,
        "42P01",
    );
    run(&mut s, "ALTER SEQUENCE owned OWNED BY owner.id");
    run(&mut s, "DROP TABLE owner");
    error(
        &mut s,
        "DROP SEQUENCE owned",
        SqlError::UnknownTable,
        "42P01",
    );
    run(
        &mut s,
        "CREATE TABLE ident(id integer GENERATED ALWAYS AS IDENTITY)",
    );
    for sql in [
        "DROP SEQUENCE ident_id_seq",
        "DROP SEQUENCE ident_id_seq CASCADE",
    ] {
        error(&mut s, sql, SqlError::DependentObjects, "2BP01");
    }
}

#[test]
fn pg_dump_restore_id_four_and_sequence_metadata_counter_reopen() {
    let mut db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE SEQUENCE public.t_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1",
    );
    run(&mut s, "CREATE TABLE w(id integer NOT NULL)");
    run(
        &mut s,
        "ALTER SEQUENCE public.t_id_seq OWNED BY public.w.id",
    );
    run(
        &mut s,
        "ALTER TABLE ONLY public.w ALTER COLUMN id SET DEFAULT nextval('public.t_id_seq'::regclass)",
    );
    scalar(
        &mut s,
        "SELECT pg_catalog.setval('public.t_id_seq',3,true)",
        3,
    );
    run(&mut s, "INSERT INTO w DEFAULT VALUES");
    scalar(&mut s, "SELECT id FROM w", 4);
    run(
        &mut s,
        "CREATE SEQUENCE durable INCREMENT -2 MINVALUE -9 MAXVALUE -1 START -3 CYCLE",
    );
    scalar(&mut s, "SELECT nextval('durable')", -3);
    // Explicit setval avoids changing the existing 32-value reservation policy.
    scalar(&mut s, "SELECT setval('durable',-3,true)", -3);
    scalar(&mut s, "SELECT setval('t_id_seq',4,true)", 4);
    drop(s);
    db.reopen();
    let mut s = db.session();
    scalar(&mut s, "SELECT nextval('durable')", -5);
    scalar(&mut s, "SELECT nextval('t_id_seq')", 5);
    run(&mut s, "DROP TABLE w");
    error(
        &mut s,
        "DROP SEQUENCE t_id_seq",
        SqlError::UnknownTable,
        "42P01",
    );
}

#[test]
fn deliberately_unsupported_sequence_forms_remain_unsupported() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s");
    for sql in [
        "ALTER SEQUENCE s RENAME TO renamed",
        "ALTER SEQUENCE s OWNER TO postgres",
        "ALTER SEQUENCE s SET SCHEMA public",
        "CREATE TEMP SEQUENCE transient",
        "CREATE UNLOGGED SEQUENCE transient",
        "SELECT * FROM s",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{sql}: bounded 0A000"
        );
    }
}

#[test]
fn duplicate_options_all_families_and_table_skip_notice() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE a");
    for sql in [
        "CREATE SEQUENCE dup AS integer AS bigint",
        "CREATE SEQUENCE dup START 1 START 2",
        "CREATE SEQUENCE dup MINVALUE 1 NO MINVALUE",
        "CREATE SEQUENCE dup MAXVALUE 10 NO MAXVALUE",
        "CREATE SEQUENCE dup CACHE 1 CACHE 2",
        "CREATE SEQUENCE dup CYCLE NO CYCLE",
        "CREATE SEQUENCE dup OWNED BY NONE OWNED BY NONE",
        "ALTER SEQUENCE a RESTART 1 RESTART 2",
        "ALTER SEQUENCE a AS integer AS bigint",
    ] {
        error(&mut s, sql, SqlError::Syntax, "42601");
    }
    run(&mut s, "CREATE TABLE t(id integer)");
    assert_eq!(
        run_tag(&mut s, "CREATE SEQUENCE IF NOT EXISTS t", "CREATE SEQUENCE").affected,
        0
    );
}

#[test]
fn dependency_details_hints_notices_serial_and_comma_drop() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s");
    run(&mut s, "CREATE TABLE t(id integer DEFAULT nextval('s'))");
    error(
        &mut s,
        "DROP SEQUENCE s RESTRICT",
        SqlError::DependentObjects,
        "2BP01",
    );

    run_tag(&mut s, "DROP SEQUENCE s CASCADE", "DROP SEQUENCE");

    run(
        &mut s,
        "CREATE TABLE ident(id integer GENERATED ALWAYS AS IDENTITY)",
    );
    error(
        &mut s,
        "DROP SEQUENCE ident_id_seq CASCADE",
        SqlError::DependentObjects,
        "2BP01",
    );

    run(&mut s, "CREATE TABLE serial_owner(id serial)");
    error(
        &mut s,
        "DROP SEQUENCE serial_owner_id_seq RESTRICT",
        SqlError::DependentObjects,
        "2BP01",
    );

    run_tag(
        &mut s,
        "DROP SEQUENCE serial_owner_id_seq CASCADE",
        "DROP SEQUENCE",
    );

    assert_eq!(
        s.execute_at(
            "INSERT INTO serial_owner DEFAULT VALUES",
            &[],
            None,
            &sequence_clock()
        ),
        Err(CoreError::Sql(SqlError::Null)),
        "23502: serial remains NOT NULL after removing its default"
    );
    run(&mut s, "INSERT INTO serial_owner VALUES (7)");
    scalar(&mut s, "SELECT id FROM serial_owner", 7);
    run(&mut s, "CREATE SEQUENCE comma_a");
    run(&mut s, "CREATE SEQUENCE comma_b");
    run(&mut s, "DROP SEQUENCE comma_a, comma_b RESTRICT");
    for sql in ["DROP SEQUENCE comma_a", "DROP SEQUENCE comma_b"] {
        error(&mut s, sql, SqlError::UnknownTable, "42P01");
    }
}

#[test]
fn transactional_increment_and_restart_commit_without_consuming_seed() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s");
    scalar(&mut s, "SELECT nextval('s')", 1);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE s INCREMENT 3");
    scalar(&mut s, "SELECT nextval('s')", 4);
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('s')", 2);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE s RESTART 50");
    run(&mut s, "COMMIT");
    scalar(&mut s, "SELECT nextval('s')", 50);
}

#[test]
fn parameter_alter_rewrites_counter_but_owned_by_only_preserves_counter() {
    let db = Db::new();
    let mut s = db.session();
    for (name, option, after) in [
        ("start", "START 50", 2),
        ("cache", "CACHE 20", 2),
        ("cycle", "CYCLE", 2),
        ("minimum", "MINVALUE 0", 2),
        ("maximum", "MAXVALUE 100", 2),
        ("datatype", "AS bigint", 2),
        ("increment", "INCREMENT 1", 2),
        ("owned", "OWNED BY NONE", 3),
    ] {
        run(&mut s, &format!("CREATE SEQUENCE options_{name}"));
        let next = format!("SELECT nextval('options_{name}')");
        scalar(&mut s, &next, 1);
        run(&mut s, "BEGIN");
        run(&mut s, &format!("ALTER SEQUENCE options_{name} {option}"));
        scalar(&mut s, &next, 2);
        run(&mut s, "ROLLBACK");
        scalar(&mut s, &format!("SELECT currval('options_{name}')"), 2);
        scalar(&mut s, &next, after);
    }
    run(&mut s, "CREATE SEQUENCE boundstop MAXVALUE 3");
    scalar(&mut s, "SELECT setval('boundstop',3)", 3);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE boundstop MAXVALUE 10");
    scalar(&mut s, "SELECT nextval('boundstop')", 4);
    run(&mut s, "ROLLBACK");
    error(
        &mut s,
        "SELECT nextval('boundstop')",
        SqlError::SequenceLimit,
        "2200H",
    );
}

#[test]
fn currval_uses_logical_identity_across_rewrite_but_not_drop_recreate() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE s");
    scalar(&mut s, "SELECT nextval('s')", 1);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE s RESTART 50");
    scalar(&mut s, "SELECT nextval('s')", 50);
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT currval('s')", 50);
    run(&mut s, "DROP SEQUENCE s");
    run(&mut s, "CREATE SEQUENCE s");
    error(
        &mut s,
        "SELECT currval('s')",
        SqlError::ObjectNotInPrerequisiteState,
        "55000",
    );
}

#[test]
fn start_on_uncalled_sequence_only_changes_future_restart_seed() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE uncalled START 5");
    run(&mut s, "ALTER SEQUENCE uncalled START 50");
    scalar(&mut s, "SELECT nextval('uncalled')", 5);
    run(&mut s, "ALTER SEQUENCE uncalled RESTART");
    scalar(&mut s, "SELECT nextval('uncalled')", 50);
}

#[test]
fn quoted_qualified_sequence_default_dependency_and_cascade() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE public.\"QuotedSeq\"");
    run(
        &mut s,
        "CREATE TABLE quoted_owner(id int DEFAULT nextval('public.\"QuotedSeq\"'::regclass))",
    );
    error(
        &mut s,
        "DROP SEQUENCE public.\"QuotedSeq\"",
        SqlError::DependentObjects,
        "2BP01",
    );

    run_tag(
        &mut s,
        "DROP SEQUENCE public.\"QuotedSeq\" CASCADE",
        "DROP SEQUENCE",
    );

    run(&mut s, "INSERT INTO quoted_owner DEFAULT VALUES");
    assert_eq!(
        run(&mut s, "SELECT id FROM quoted_owner").rows[0].values,
        vec![Value::Null]
    );
}

// Captured in e1-sql/evidence/oracle-pg186-sequence-alter-txn.txt.
#[test]
fn pg186_alter_parameter_transaction_expectations() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE q");
    scalar(&mut s, "SELECT nextval('q')", 1);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE q INCREMENT 10");
    assert_eq!(
        run(&mut s, "SELECT nextval('q'), nextval('q')").rows[0].values,
        vec![Value::Int(11), Value::Int(21)]
    );
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('q')", 2);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE q MAXVALUE 1000");
    scalar(&mut s, "SELECT nextval('q')", 3);
    run(&mut s, "COMMIT");
    scalar(&mut s, "SELECT nextval('q')", 4);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE q START 7");
    scalar(&mut s, "SELECT nextval('q')", 5);
    run(&mut s, "ROLLBACK");
    scalar(&mut s, "SELECT nextval('q')", 5);
}
