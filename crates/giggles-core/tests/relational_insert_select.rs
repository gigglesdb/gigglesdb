//! PostgreSQL 18.6 oracle: e3-query/wo-insert-select/pg-probes.txt.
use giggles_core::{
    relational::{DataType as T, MAX_ROWS, SqlError as E, SqlResult, StatementClock, Value as V},
    *,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Db {
    session: Option<SqlSession>,
    admin: Option<Admin>,
    path: PathBuf,
}
impl Db {
    fn new() -> Self {
        Self::new_engine(Engine::Legacy)
    }
    fn new_engine(engine: Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "gd-insert-select-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let session = admin.sql_session();
        Self {
            session: Some(session),
            admin: Some(admin),
            path,
        }
    }
    fn session(&mut self) -> &mut SqlSession {
        self.session.as_mut().unwrap()
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.session.take());
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 200,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute_at(sql, &[], None, &clock())
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn values(result: SqlResult) -> Vec<Vec<V>> {
    result.rows.into_iter().map(|row| row.values).collect()
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    values(run(s, sql))
}
fn ints(ns: &[i64]) -> Vec<Vec<V>> {
    ns.iter().map(|&n| vec![V::Int(n)]).collect()
}
fn text(s: &str) -> V {
    V::Text(s.into())
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute_at(sql, &[], None, &clock()),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
fn fixture(s: &mut SqlSession) {
    run(
        s,
        "CREATE TABLE src(id int PRIMARY KEY,score int,name text,big bigint,amt numeric)",
    );
    run(
        s,
        "INSERT INTO src VALUES(2,20,'b',40000,2.5),(1,10,'a',1,1.4),(3,30,'c',3,3.6)",
    );
    run(
        s,
        "CREATE TABLE dst(id int PRIMARY KEY,score smallint,name text)",
    );
}
// SELECT without ORDER BY has no specified row order; compare its returned bag.
fn bag(mut rs: Vec<Vec<V>>) -> Vec<Vec<V>> {
    rs.sort_by_cached_key(|row| format!("{row:?}"));
    rs
}

#[test]
fn projected_rows_assignment_casts_and_unknown_inputs_match_postgres() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let inserted = run(
        s,
        "INSERT INTO dst SELECT id,score+1,name FROM src WHERE id<=2 ORDER BY id",
    );
    assert_eq!(inserted.affected, 2);
    assert!(inserted.rows.is_empty());
    assert_eq!(
        rows(s, "SELECT * FROM dst ORDER BY id"),
        vec![
            vec![V::Int(1), V::Int(11), text("a")],
            vec![V::Int(2), V::Int(21), text("b")]
        ]
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO dst(id,name) SELECT id+10,name FROM src WHERE id=1 RETURNING id,score,name"
        ),
        vec![vec![V::Int(11), V::Null, text("a")]]
    );
    error(
        s,
        "INSERT INTO dst(id,score) SELECT 20,name FROM src WHERE id=1",
        E::Type,
    );
    error(
        s,
        "INSERT INTO dst(id,score) SELECT 21,big FROM src WHERE id=2",
        E::Overflow,
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO dst(id,score) SELECT 22,amt FROM src WHERE id=3 RETURNING score"
        ),
        ints(&[4])
    );
    error(s, "INSERT INTO dst(id,score) SELECT 23,'1'||'2'", E::Type);
    assert_eq!(
        rows(s, "INSERT INTO dst(id,score) SELECT 24,'7' RETURNING score"),
        ints(&[7])
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO dst(id,score) SELECT 25,NULL RETURNING score"
        ),
        vec![vec![V::Null]]
    );
    error(
        s,
        "INSERT INTO dst(id,score) SELECT 26,'maybe'",
        E::InvalidTextRepresentation,
    );
    error(
        s,
        "INSERT INTO dst(id,score) SELECT 26,'40000'",
        E::Overflow,
    );
    assert_eq!(
        rows(s, "SELECT id FROM dst ORDER BY id"),
        ints(&[1, 2, 11, 22, 24, 25])
    );
}

#[test]
fn character_target_assignment_truncation_and_padding_match_postgres() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(
        s,
        "CREATE TABLE vc(id int PRIMARY KEY,v varchar(3),c char(3))",
    );
    // PG18.6 22001 applies to unknown literals and typed text source expressions.
    error(
        s,
        "INSERT INTO vc SELECT 1,'abcd','x'",
        E::StringDataRightTruncation,
    );
    error(
        s,
        "INSERT INTO vc SELECT 2,name||'xyz',name FROM src WHERE id=1",
        E::StringDataRightTruncation,
    );
    assert!(rows(s, "SELECT * FROM vc").is_empty());
    // Bpchar scalar functions are not admitted yet. Verify the stored padding
    // directly: PG length(c)=1 and octet_length(c)=3 for this ASCII value.
    assert_eq!(
        rows(s, "INSERT INTO vc SELECT 3,'ab   ','x' RETURNING v,c"),
        vec![vec![text("ab "), text("x  ")]]
    );
    assert_eq!(
        rows(s, "SELECT id,v,c FROM vc ORDER BY id"),
        vec![vec![V::Int(3), text("ab "), text("x  ")]]
    );
}

#[test]
fn varchar_explicit_cast_matches_select_and_values() {
    let mut db = Db::new();
    let s = db.session();
    run(
        s,
        "CREATE TABLE vc(id int PRIMARY KEY,v varchar(3),c char(3))",
    );
    assert_eq!(rows(s, "SELECT 'abc'::varchar(5)"), vec![vec![text("abc")]]);
    for sql in [
        "INSERT INTO vc SELECT 4,'abc'::varchar(5),'y' RETURNING v",
        "INSERT INTO vc VALUES(5,'abc'::varchar(5),'y') RETURNING v",
    ] {
        assert_eq!(rows(s, sql), vec![vec![text("abc")]]);
    }
    assert_eq!(rows(s, "SELECT id FROM vc ORDER BY id"), ints(&[4, 5]));
}

#[test]
fn positional_targets_implicit_trailing_defaults_and_parenthesized_select() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    error(s, "INSERT INTO dst(id) SELECT 1,2", E::Syntax);
    error(s, "INSERT INTO dst(id,score) SELECT 26", E::Syntax);
    assert_eq!(run(s, "INSERT INTO dst SELECT 27").affected, 1);
    assert_eq!(
        rows(s, "SELECT * FROM dst WHERE id=27"),
        vec![vec![V::Int(27), V::Null, V::Null]]
    );
    assert_eq!(run(s, "INSERT INTO dst(id) (SELECT 500)").affected, 1);
    assert_eq!(rows(s, "SELECT id FROM dst ORDER BY id"), ints(&[27, 500]));
    run(
        s,
        "CREATE TABLE d(id int PRIMARY KEY,v int DEFAULT 8,name text DEFAULT 'fallback')",
    );
    assert_eq!(
        rows(s, "INSERT INTO d SELECT 1 RETURNING id,v,name"),
        vec![vec![V::Int(1), V::Int(8), text("fallback")]]
    );
    // Output aliases never remap the positional target columns.
    assert_eq!(
        rows(
            s,
            "INSERT INTO d(name,id) SELECT 'named' AS id,2 AS name RETURNING id,v,name"
        ),
        vec![vec![V::Int(2), V::Int(8), text("named")]]
    );
}

#[test]
fn qualified_target_named_values_routes_to_select_source() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE public.values(id int PRIMARY KEY)");
    // Preserve the pre-existing routing boundary for non-SELECT statements.
    error(s, "INSERT INTO public.values VALUES(1),(2)", E::Unsupported);
    assert!(rows(s, "SELECT id FROM public.values").is_empty());
    assert_eq!(
        rows(s, "INSERT INTO public.values SELECT 9 RETURNING id"),
        ints(&[9])
    );
    assert_eq!(rows(s, "SELECT id FROM public.values"), ints(&[9]));
}

#[test]
fn join_predicates_named_conflict_do_not_end_the_select_source() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE a(id int PRIMARY KEY)");
    run(s, "INSERT INTO a VALUES(1),(2)");
    run(s, "CREATE TABLE flags(id int PRIMARY KEY,conflict bool)");
    run(s, "INSERT INTO flags VALUES(1,true),(2,false)");
    run(s, "CREATE TABLE joined_dst(id int PRIMARY KEY)");
    run(s, "INSERT INTO joined_dst VALUES(2)");
    assert_eq!(
        bag(rows(
            s,
            "SELECT a.id FROM a JOIN flags conflict ON conflict.id=a.id"
        )),
        bag(ints(&[1, 2]))
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO joined_dst SELECT a.id FROM a JOIN flags conflict ON conflict.id=a.id ON CONFLICT DO NOTHING RETURNING id"
        ),
        ints(&[1])
    );
    // A bare boolean column is also a complete standalone JOIN predicate.
    assert_eq!(
        bag(rows(s, "SELECT a.id+10 FROM a JOIN flags ON conflict")),
        bag(ints(&[11, 12]))
    );
    assert_eq!(
        bag(rows(
            s,
            "INSERT INTO joined_dst SELECT a.id+10 FROM a JOIN flags ON conflict ON CONFLICT DO NOTHING RETURNING id"
        )),
        bag(ints(&[11, 12]))
    );
    assert_eq!(
        rows(s, "SELECT id FROM joined_dst ORDER BY id"),
        ints(&[1, 2, 11, 12])
    );
}

#[test]
fn source_order_by_descending_output_ordinals_applies_to_stars_and_scalars() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE ordinal_src(id int PRIMARY KEY,v int)");
    run(s, "INSERT INTO ordinal_src VALUES(1,30),(2,10),(3,20)");
    run(s, "CREATE TABLE ordinal_dst(id int PRIMARY KEY,v int)");
    assert_eq!(
        rows(
            s,
            "INSERT INTO ordinal_dst SELECT * FROM ordinal_src ORDER BY 2 DESC RETURNING id,v"
        ),
        vec![
            vec![V::Int(1), V::Int(30)],
            vec![V::Int(3), V::Int(20)],
            vec![V::Int(2), V::Int(10)]
        ]
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO ordinal_dst SELECT id+10,100-v FROM ordinal_src ORDER BY 2 DESC RETURNING id,v"
        ),
        vec![
            vec![V::Int(12), V::Int(90)],
            vec![V::Int(13), V::Int(80)],
            vec![V::Int(11), V::Int(70)]
        ]
    );
}

#[test]
fn source_bind_errors_are_reported_even_when_no_rows_match() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    for (sql, e) in [
        ("INSERT INTO dst SELECT * FROM src", E::Syntax),
        ("INSERT INTO dst SELECT * FROM src WHERE false", E::Syntax),
        (
            "INSERT INTO dst(id,score) SELECT id FROM src WHERE false",
            E::Syntax,
        ),
        (
            "INSERT INTO dst(id) SELECT id,score FROM src WHERE false",
            E::Syntax,
        ),
        (
            "INSERT INTO dst(id,score) SELECT id,name FROM src WHERE false",
            E::Type,
        ),
        (
            "INSERT INTO dst(id,score) SELECT id,'1'||'2' FROM src WHERE false",
            E::Type,
        ),
        (
            "INSERT INTO dst(id,score) SELECT id,'maybe' FROM src WHERE false",
            E::InvalidTextRepresentation,
        ),
        (
            "INSERT INTO dst(id,score) SELECT id,'40000' FROM src WHERE false",
            E::Overflow,
        ),
        (
            "INSERT INTO dst(id) SELECT missing FROM src WHERE false",
            E::UnknownColumn,
        ),
        (
            "INSERT INTO dst(missing) SELECT id FROM src WHERE false",
            E::UnknownColumn,
        ),
        (
            "INSERT INTO dst(id) SELECT id FROM missing WHERE false",
            E::UnknownTable,
        ),
        (
            "INSERT INTO dst(id) SELECT id FROM src WHERE false RETURNING missing",
            E::UnknownColumn,
        ),
    ] {
        error(s, sql, e.clone());
        let p = s.prepare(sql);
        if let Ok(p) = p {
            assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(e), "{sql}");
        }
    }
    let empty = run(
        s,
        "INSERT INTO dst SELECT * FROM dst WHERE false RETURNING id",
    );
    assert_eq!(empty.affected, 0);
    assert!(empty.rows.is_empty());
    assert!(rows(s, "SELECT * FROM dst").is_empty());
}

#[test]
fn prepared_source_parameters_infer_target_and_predicate_types() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let p1 = s.prepare("INSERT INTO dst(id,score) SELECT $1,$2").unwrap();
    let d1 = s.describe(&p1).unwrap();
    assert_eq!(d1.parameters, [T::Integer, T::SmallInt]);
    assert!(d1.columns.is_empty());
    assert_eq!(
        s.execute_prepared(&p1, &[V::Int(30), V::Int(5)], None)
            .unwrap()
            .affected,
        1
    );
    let p2 = s
        .prepare("INSERT INTO dst(id,score) SELECT id+100,$1 FROM src WHERE id=$2")
        .unwrap();
    assert_eq!(
        s.describe(&p2).unwrap().parameters,
        [T::SmallInt, T::Integer]
    );
    assert_eq!(
        s.execute_prepared(&p2, &[V::Int(6), V::Int(2)], None)
            .unwrap()
            .affected,
        1
    );
    assert_eq!(
        rows(s, "SELECT id,score FROM dst ORDER BY id"),
        vec![vec![V::Int(30), V::Int(5)], vec![V::Int(102), V::Int(6)]]
    );
}

#[test]
fn contextual_source_parameters_and_conflicting_targets_are_bound_before_rows() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    // A concrete source predicate determines the parameter's type; the output
    // assignment then converts integer to smallint.
    for suffix in ["", " ON CONFLICT(id) DO NOTHING"] {
        let sql =
            format!("INSERT INTO dst(id,score) SELECT id+200,$1 FROM src WHERE id=$1{suffix}");
        let p = s.prepare(&sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, [T::Integer], "{sql}");
    }
    let p = s
        .prepare("INSERT INTO dst(id,score) SELECT id+200,$1 FROM src WHERE id=$1")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(2)], None).unwrap().affected,
        1
    );
    assert_eq!(
        rows(s, "SELECT id,score FROM dst"),
        vec![vec![V::Int(202), V::Int(2)]]
    );
    for (sql, expected) in [
        // The same bare parameter has conflicting inferred target types.
        ("INSERT INTO dst(id,score) SELECT $1,$1", E::Type),
        // ON CONFLICT infers smallint while the concrete source context
        // requires integer; the conflict must remain visible.
        (
            "INSERT INTO dst(id,score) SELECT $1,$2 FROM src WHERE id=$2 ON CONFLICT(id) DO UPDATE SET score=$2",
            E::Type,
        ),
        (
            "INSERT INTO dst(id) SELECT id FROM src WHERE $1 IS NULL",
            E::Parameters,
        ),
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(
            s.describe(&p).unwrap_err(),
            CoreError::Sql(expected),
            "{sql}"
        );
    }
}

#[test]
fn self_insert_reads_pre_statement_snapshot_and_duplicates_are_atomic() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "CREATE TABLE self_copy(id int PRIMARY KEY)");
    run(s, "INSERT INTO self_copy VALUES(1),(2)");
    let copied = run(
        s,
        "INSERT INTO self_copy SELECT id+2 FROM self_copy RETURNING id",
    );
    assert_eq!(copied.affected, 2);
    assert_eq!(bag(values(copied)), bag(ints(&[3, 4])));
    let before = run(s, "SELECT id FROM self_copy ORDER BY id").rows;
    error(s, "INSERT INTO self_copy SELECT 9 FROM src", E::Duplicate);
    assert_eq!(run(s, "SELECT id FROM self_copy ORDER BY id").rows, before);
    assert_eq!(rows(s, "SELECT count(*) FROM self_copy"), ints(&[4]));
    let skipped = run(
        s,
        "INSERT INTO self_copy SELECT id FROM src ON CONFLICT DO NOTHING RETURNING id",
    );
    assert_eq!(skipped.affected, 0);
    assert!(skipped.rows.is_empty());
    // PG18.6: 21000; same as the existing multi-row VALUES path
    error(
        s,
        "INSERT INTO self_copy SELECT 1 FROM src ON CONFLICT(id) DO UPDATE SET id=excluded.id",
        E::CardinalityViolation,
    );
    assert_eq!(run(s, "SELECT id FROM self_copy ORDER BY id").rows, before);
}

#[test]
fn conflict_updates_return_inserted_and_updated_rows() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "CREATE TABLE kv(k int PRIMARY KEY,v int)");
    run(s, "INSERT INTO kv VALUES(1,0)");
    let result = run(
        s,
        "INSERT INTO kv SELECT id,score FROM src ON CONFLICT(k) DO UPDATE SET v=excluded.v RETURNING k,v",
    );
    assert_eq!(result.affected, 3);
    let expected = vec![
        vec![V::Int(2), V::Int(20)],
        vec![V::Int(1), V::Int(10)],
        vec![V::Int(3), V::Int(30)],
    ];
    assert_eq!(bag(values(result)), bag(expected.clone()));
    assert_eq!(bag(rows(s, "SELECT k,v FROM kv")), bag(expected));
}

#[test]
fn aggregate_distinct_order_limit_join_and_in_subquery_sources() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    assert_eq!(
        rows(
            s,
            "INSERT INTO dst(id,score) SELECT max(id)+200,count(*) FROM src RETURNING id,score"
        ),
        vec![vec![V::Int(203), V::Int(3)]]
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO dst(id) SELECT DISTINCT score+300 FROM src ORDER BY 1 LIMIT 2 RETURNING id"
        ),
        ints(&[310, 320])
    );
    assert_eq!(
        bag(rows(
            s,
            "INSERT INTO dst(id,name) SELECT a.id+400,b.name FROM src a JOIN src b ON a.id=b.id RETURNING id,name"
        )),
        bag(vec![
            vec![V::Int(402), text("b")],
            vec![V::Int(401), text("a")],
            vec![V::Int(403), text("c")]
        ])
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO dst(id,score) SELECT id+800,max(score) FROM src WHERE id IN(SELECT id FROM src WHERE id<>2) GROUP BY id ORDER BY id DESC LIMIT 2 RETURNING id,score"
        ),
        vec![vec![V::Int(803), V::Int(30)], vec![V::Int(801), V::Int(10)]]
    );
    assert_eq!(
        rows(s, "SELECT id FROM dst ORDER BY id"),
        ints(&[203, 310, 320, 401, 402, 403, 801, 803])
    );
}

#[test]
fn serial_generation_follows_source_order_and_identity_overriding() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "CREATE TABLE ser(n serial PRIMARY KEY,v int)");
    assert_eq!(
        rows(
            s,
            "INSERT INTO ser(v) SELECT score FROM src ORDER BY score DESC RETURNING n,v"
        ),
        vec![
            vec![V::Int(1), V::Int(30)],
            vec![V::Int(2), V::Int(20)],
            vec![V::Int(3), V::Int(10)]
        ]
    );
    run(
        s,
        "CREATE TABLE ident(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,v int DEFAULT 7)",
    );
    error(s, "INSERT INTO ident SELECT 10,20", E::GeneratedAlways);
    assert_eq!(
        rows(
            s,
            "INSERT INTO ident OVERRIDING SYSTEM VALUE SELECT 10,20 RETURNING id,v"
        ),
        vec![vec![V::Int(10), V::Int(20)]]
    );
    assert_eq!(
        rows(
            s,
            "INSERT INTO ident OVERRIDING USER VALUE SELECT 99,30 RETURNING id,v"
        ),
        vec![vec![V::Int(1), V::Int(30)]]
    );
    assert_eq!(
        rows(s, "INSERT INTO ident(v) SELECT 40 RETURNING id,v"),
        vec![vec![V::Int(2), V::Int(40)]]
    );
}

#[test]
fn later_row_conversion_constraint_and_source_failures_preserve_versions() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "INSERT INTO dst VALUES(90,9,'keep')");
    let before = run(s, "SELECT * FROM dst ORDER BY id").rows;
    // id=1 converts successfully before id=2 overflows the smallint target.
    error(
        s,
        "INSERT INTO dst(id,score) SELECT id,big FROM src ORDER BY id",
        E::Overflow,
    );
    assert_eq!(run(s, "SELECT * FROM dst ORDER BY id").rows, before);
    // Seed a collision at the final source row, after two valid proposals.
    run(s, "INSERT INTO dst VALUES(103,3,'collision')");
    let before = run(s, "SELECT * FROM dst ORDER BY id").rows;
    error(
        s,
        "INSERT INTO dst(id,score) SELECT id+100,score FROM src ORDER BY id",
        E::Duplicate,
    );
    assert_eq!(run(s, "SELECT * FROM dst ORDER BY id").rows, before);
    error(
        s,
        "INSERT INTO dst(id,score) SELECT id,10/(3-id) FROM src ORDER BY id",
        E::DivisionByZero,
    );
    assert_eq!(run(s, "SELECT * FROM dst ORDER BY id").rows, before);
    run(
        s,
        "CREATE TABLE required(id int PRIMARY KEY,v int NOT NULL)",
    );
    run(s, "INSERT INTO required VALUES(90,9)");
    let before = run(s, "SELECT * FROM required ORDER BY id").rows;
    error(
        s,
        "INSERT INTO required SELECT id,CASE WHEN id=3 THEN NULL ELSE score END FROM src ORDER BY id",
        E::Null,
    );
    assert_eq!(run(s, "SELECT * FROM required ORDER BY id").rows, before);
}

#[test]
fn failed_insert_sequence_consumption_matches_existing_multirow_values() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    for table in ["seq_values", "seq_select"] {
        run(
            s,
            &format!("CREATE TABLE {table}(n serial PRIMARY KEY,v int UNIQUE)"),
        );
        run(s, &format!("INSERT INTO {table}(v) VALUES(2)"));
    }
    let before_values = run(s, "SELECT * FROM seq_values").rows;
    let before_select = run(s, "SELECT * FROM seq_select").rows;
    error(
        s,
        "INSERT INTO seq_values(v) VALUES(1),(2),(3)",
        E::Duplicate,
    );
    error(
        s,
        "INSERT INTO seq_select(v) SELECT id FROM src ORDER BY id",
        E::Duplicate,
    );
    assert_eq!(run(s, "SELECT * FROM seq_values").rows, before_values);
    assert_eq!(run(s, "SELECT * FROM seq_select").rows, before_select);
    let values_next = rows(s, "INSERT INTO seq_values(v) VALUES(4) RETURNING n");
    let select_next = rows(s, "INSERT INTO seq_select(v) SELECT 4 RETURNING n");
    assert_eq!(select_next, values_next);
    assert_eq!(select_next, ints(&[5]));
}

#[test]
fn later_assignment_overflow_consumes_serial_defaults_like_values() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE overflow_src(id int PRIMARY KEY,v int)");
    run(s, "INSERT INTO overflow_src VALUES(1,1),(2,40000)");
    for table in ["overflow_values", "overflow_select"] {
        run(
            s,
            &format!("CREATE TABLE {table}(n serial PRIMARY KEY,v smallint)"),
        );
        // An explicit serial value preserves a witness row and its version
        // without advancing the sequence from its initial value of one.
        run(s, &format!("INSERT INTO {table}(n,v) VALUES(100,0)"));
    }
    let before_values = run(s, "SELECT * FROM overflow_values ORDER BY n").rows;
    let before_select = run(s, "SELECT * FROM overflow_select ORDER BY n").rows;
    error(
        s,
        "INSERT INTO overflow_values(v) VALUES(1),(40000)",
        E::Overflow,
    );
    error(
        s,
        "INSERT INTO overflow_select(v) SELECT v FROM overflow_src ORDER BY id",
        E::Overflow,
    );
    assert_eq!(
        run(s, "SELECT * FROM overflow_values ORDER BY n").rows,
        before_values
    );
    assert_eq!(
        run(s, "SELECT * FROM overflow_select ORDER BY n").rows,
        before_select
    );
    // Each attempted row allocates its default before assignment conversion;
    // failure rolls back table rows but retains both sequence allocations.
    let values_next = rows(s, "INSERT INTO overflow_values(v) VALUES(2) RETURNING n");
    let select_next = rows(s, "INSERT INTO overflow_select(v) SELECT 2 RETURNING n");
    assert_eq!(values_next, ints(&[3]));
    assert_eq!(select_next, values_next);
}

#[test]
fn cancellation_during_source_evaluation_aborts_without_writes() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "INSERT INTO dst VALUES(90,9,'keep')");
    let before = run(s, "SELECT * FROM dst ORDER BY id").rows;
    let calls = Arc::new(AtomicU64::new(0));
    let observed = calls.clone();
    let cancel = Cancellation::default();
    let source_cancel = cancel.clone();
    let mut execution_clock = clock();
    execution_clock.wall = Some(Arc::new(move || {
        if observed.fetch_add(1, Ordering::Relaxed) == 1 {
            source_cancel.cancel();
        }
        Ok(200)
    }));
    run(s, "BEGIN");
    assert_eq!(cancel.scope(|| s.execute_at("INSERT INTO dst(id) SELECT id FROM src WHERE clock_timestamp() IS NOT NULL ORDER BY id",&[],None,&execution_clock)),Err(CoreError::Sql(E::Aborted)));
    assert!(
        calls.load(Ordering::Relaxed) >= 2,
        "cancellation must occur during source row evaluation"
    );
    assert!(s.is_aborted());
    error(s, "SELECT * FROM dst", E::Aborted);
    s.rollback();
    assert_eq!(run(s, "SELECT * FROM dst ORDER BY id").rows, before);
}

#[test]
fn more_than_max_rows_is_rejected_before_any_insert() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE many(id int PRIMARY KEY)");
    let input = (1..=33)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    run(s, &format!("INSERT INTO many VALUES{input}"));
    run(s, "CREATE TABLE cap(id int PRIMARY KEY)");
    run(s, "INSERT INTO cap VALUES(0)");
    let before = run(s, "SELECT id FROM cap").rows;
    let source_rows = rows(s, "SELECT id FROM many").len();
    assert!(source_rows * source_rows > MAX_ROWS);
    error(
        s,
        "INSERT INTO cap SELECT a.id*100+b.id FROM many a JOIN many b ON true",
        E::Limit,
    );
    assert_eq!(run(s, "SELECT id FROM cap").rows, before);
}

#[test]
fn union_sources_and_invalid_insert_forms() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    assert_eq!(
        run(s, "INSERT INTO dst(id) SELECT 600 UNION SELECT 601").affected,
        2
    );
    error(s, "INSERT INTO dst VALUES((SELECT 1))", E::Syntax);
    error(s, "EXPLAIN INSERT INTO dst(id) SELECT 1", E::Syntax);
    assert_eq!(rows(s, "SELECT id FROM dst ORDER BY id"), ints(&[600, 601]));
}

#[test]
fn prepared_read_tables_cover_source_joins_subqueries_and_self_reads() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "CREATE TABLE allowed(id int PRIMARY KEY)");
    for (sql, expected) in [
        ("INSERT INTO dst(id) SELECT id FROM src", vec!["src"]),
        (
            "INSERT INTO dst(id) SELECT a.id FROM src a JOIN allowed b ON a.id=b.id WHERE a.id IN(SELECT id FROM dst)",
            vec!["src", "allowed", "dst"],
        ),
        ("INSERT INTO dst SELECT * FROM dst WHERE false", vec!["dst"]),
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(p.table(), Some("dst"), "{sql}");
        assert_eq!(p.read_tables(), expected, "{sql}");
    }
}

#[test]
fn both_insert_select_corpus_cases_match_postgres() {
    let mut db = Db::new();
    let s = db.session();
    for (corpus, expected) in [
        (
            include_str!("../../../compat/corpus/insert/insert_select.sql"),
            vec![vec![V::Int(1), V::Int(11)], vec![V::Int(2), V::Int(21)]],
        ),
        (
            include_str!("../../../compat/corpus/insert/insert_select_returning.sql"),
            vec![vec![V::Int(1), text("Ada")]],
        ),
    ] {
        let sql = corpus
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut last = None;
        for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
            let result = run(s, statement);
            if statement.contains(" RETURNING ") {
                assert_eq!(values(result.clone()), expected);
            }
            last = Some(result);
        }
        assert_eq!(values(last.unwrap()), expected);
    }
}

#[test]
fn source_and_conflict_sequence_dependencies_preserve_target_alias() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE dst(id bigint PRIMARY KEY,score bigint)");
    run(s, "CREATE SEQUENCE input_seq");
    run(s, "CREATE SEQUENCE update_seq");
    run(s, "INSERT INTO dst VALUES(1,10)");
    let sql = "INSERT INTO dst AS d SELECT nextval('input_seq'),2 ON CONFLICT(id) DO UPDATE SET score=d.score+nextval('update_seq') RETURNING id,score";
    let prepared = s.prepare(sql).unwrap();
    let dependencies = prepared.sequences();
    assert!(
        dependencies.contains(&Some("input_seq".into())),
        "{dependencies:?}"
    );
    assert!(
        dependencies.contains(&Some("update_seq".into())),
        "{dependencies:?}"
    );
    assert_eq!(rows(s, sql), vec![vec![V::Int(1), V::Int(11)]]);
}

fn constraint_count(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    let prepared = s.prepare(sql).unwrap();
    s.open_cursor(&prepared, &[]).unwrap().fetch(1, 1024).unwrap()
        .into_iter().map(|row| row.values).collect()
}

fn constraint_cases() -> Vec<(Engine, usize)> {
    let mut cases = vec![(Engine::Legacy, 2), (Engine::Legacy, 201)];
    if cfg!(feature = "engine-rocksdb") {
        cases.extend([2, 201, 1100].map(|count| (Engine::V2, count)));
    }
    cases
}

#[test]
fn repeated_upsert_targets_are_cardinality_errors_and_statement_atomic() {
    for (engine, count) in constraint_cases() {
        for select in [false, true] {
            for existing in [false, true] {
                let mut db = Db::new_engine(engine);
                let s = db.session();
                run(s, "CREATE TABLE dst(k bigint PRIMARY KEY,v bigint)");
                run(s, "CREATE TABLE src(seq bigint PRIMARY KEY,k bigint,v bigint)");
                if existing { run(s, "INSERT INTO dst VALUES(0,7)"); }
                let proposals = (0..=count).map(|n| format!("({},9)", n % count))
                    .collect::<Vec<_>>().join(",");
                let input = if select {
                    let source = (0..=count).map(|n| format!("({n},{},9)", n % count))
                        .collect::<Vec<_>>().join(",");
                    run(s, &format!("INSERT INTO src VALUES {source}"));
                    "SELECT k,v FROM src".to_owned()
                } else { format!("VALUES {proposals}") };
                error(s, &format!("INSERT INTO dst {input} ON CONFLICT(k) DO UPDATE SET v=excluded.v"),
                    E::CardinalityViolation);
                assert_eq!(constraint_count(s, "SELECT count(*) FROM dst"), ints(&[i64::from(existing)]));
                if existing { assert_eq!(rows(s, "SELECT v FROM dst WHERE k=0"), ints(&[7])); }
                assert_eq!(run(s, &format!("INSERT INTO dst {input} ON CONFLICT(k) DO NOTHING")).affected,
                    (count - usize::from(existing)) as u64);
                assert_eq!(constraint_count(s, "SELECT count(*) FROM dst"), ints(&[count as i64]));
                assert_eq!(rows(s, "SELECT v FROM dst WHERE k=0"), ints(&[if existing { 7 } else { 9 }]));
            }
        }
    }
}

#[test]
fn mixed_fk_errors_follow_definition_order_not_child_table_order() {
    // PG18.6: first NO ACTION -> 23503; first RESTRICT -> 23001.
    // The existing monotonic constraint IDs retain ALTER's definition order.
    for (engine, count) in constraint_cases() {
        for restrict_first in [false, true] {
            let mut db = Db::new_engine(engine);
            let s = db.session();
            run(s, "CREATE TABLE p(id bigint PRIMARY KEY)");
            run(s, "INSERT INTO p VALUES(0)");
            // Deliberately create the later FK's table first.
            let children = if restrict_first { ["na", "r"] } else { ["r", "na"] };
            for child in children {
                run(s, &format!("CREATE TABLE {child}(id bigint PRIMARY KEY,pid bigint)"));
            }
            for child in children.into_iter().rev() {
                let action = if child == "r" { "RESTRICT" } else { "NO ACTION" };
                run(s, &format!("ALTER TABLE {child} ADD FOREIGN KEY(pid) REFERENCES p(id) ON DELETE {action} ON UPDATE {action}"));
                let batch = (0..count).map(|n| format!("({n},0)")).collect::<Vec<_>>().join(",");
                run(s, &format!("INSERT INTO {child} VALUES {batch}"));
            }
            let expected = if restrict_first { E::RestrictViolation } else { E::ForeignKeyViolation };
            error(s, "DELETE FROM p WHERE id=0", expected.clone());
            error(s, "UPDATE p SET id=1 WHERE id=0", expected);
            assert_eq!(rows(s, "SELECT id FROM p"), ints(&[0]));
            for child in children {
                assert_eq!(constraint_count(s, &format!("SELECT count(*) FROM {child} WHERE pid=0")), ints(&[count as i64]));
            }
        }
    }
}
