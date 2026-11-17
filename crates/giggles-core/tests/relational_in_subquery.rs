//! PostgreSQL 18.6 oracle: e3-query/wo-in-subquery/pg-probes.txt.
use giggles_core::{
    relational::{DataType as T, SqlError as E, SqlResult, Value as V},
    *,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Db {
    session: Option<SqlSession>,
    admin: Option<Admin>,
    path: PathBuf,
}
impl Db {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "gd-in-subquery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
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
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    run(s, sql).rows.into_iter().map(|r| r.values).collect()
}
fn ints(values: &[i64]) -> Vec<Vec<V>> {
    values.iter().map(|&n| vec![V::Int(n)]).collect()
}
fn fixture(s: &mut SqlSession) {
    run(s, "CREATE TABLE t(id int PRIMARY KEY,v text)");
    run(s, "CREATE TABLE u(id int PRIMARY KEY,x int,n int)");
    run(s, "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')");
    run(s, "INSERT INTO u VALUES(2,0,NULL),(10,1,3)");
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
#[test]
fn membership_null_and_empty_sets_match_postgres() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let cases: &[(&str, &[i64])] = &[
        ("id IN (SELECT id FROM u)", &[2]),
        ("id NOT IN (SELECT id FROM u)", &[1, 3]),
        ("id IN (SELECT n FROM u)", &[3]),
        ("id NOT IN (SELECT n FROM u)", &[]),
        ("(id IN (SELECT n FROM u)) IS NULL", &[1, 2]),
        ("(id IN (SELECT n FROM u)) IS NOT NULL", &[3]),
        ("NULL::int IN (SELECT id FROM u WHERE false)", &[]),
        (
            "NULL::int NOT IN (SELECT id FROM u WHERE false)",
            &[1, 2, 3],
        ),
        ("NULL::int IN (SELECT id FROM u)", &[]),
        ("(NULL::int IN (SELECT id FROM u)) IS NULL", &[1, 2, 3]),
        ("'2' IN (SELECT id FROM u)", &[1, 2, 3]),
    ];
    for &(predicate, expected) in cases {
        let sql = format!("SELECT id FROM t WHERE {predicate} ORDER BY id");
        assert_eq!(rows(s, &sql), ints(expected), "{sql}");
    }
    assert_eq!(
        rows(s, "SELECT 1 AS one WHERE 2 IN (SELECT id FROM u)"),
        ints(&[1])
    );
}
#[test]
fn standalone_subquery_features_and_outer_query_shapes() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    for (sql, expected) in [
        (
            "SELECT id FROM t WHERE id IN (SELECT id FROM u ORDER BY id LIMIT 1) ORDER BY id",
            vec![2],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT max(id) FROM u WHERE id<5) ORDER BY id",
            vec![2],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT DISTINCT n FROM u) ORDER BY id",
            vec![3],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE id IN (SELECT id FROM t)) ORDER BY id",
            vec![2],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT max(id) FILTER (WHERE id<5) FROM u) ORDER BY id",
            vec![2],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT n FROM u GROUP BY n) ORDER BY id",
            vec![3],
        ),
        (
            "SELECT t.id FROM t JOIN u ON t.id=u.id WHERE t.id IN (SELECT id FROM u) ORDER BY t.id",
            vec![2],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT id FROM u) GROUP BY id ORDER BY id",
            vec![2],
        ),
        (
            "SELECT id FROM t WHERE id IN (SELECT u.id FROM u JOIN t ON u.id=t.id) ORDER BY id",
            vec![2],
        ),
    ] {
        assert_eq!(rows(s, sql), ints(&expected), "{sql}");
    }
    run(s, "CREATE TABLE one(k int PRIMARY KEY)");
    run(s, "INSERT INTO one VALUES(3)");
    assert_eq!(
        rows(s, "SELECT id FROM t WHERE id IN (SELECT * FROM one)"),
        ints(&[3])
    );
}
#[test]
fn binding_arity_types_and_correlation_errors() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    error(
        s,
        "SELECT id FROM t WHERE id IN (SELECT id,x FROM u)",
        E::Syntax,
    );
    error(
        s,
        "SELECT id FROM t WHERE id IN (SELECT * FROM u)",
        E::Syntax,
    );
    for sql in [
        "SELECT id FROM t WHERE id IN (SELECT v FROM t)",
        "SELECT id FROM t WHERE v IN (SELECT id FROM u)",
        "SELECT id FROM t WHERE id IN (SELECT '2')",
    ] {
        error(s, sql, E::UndefinedFunction);
    }
    for sql in [
        "SELECT id FROM t WHERE id IN (SELECT 2.0::numeric)",
        "SELECT id FROM t WHERE id IN (SELECT 2::bigint)",
    ] {
        assert_eq!(rows(s, sql), ints(&[2]));
    }
    for (sql, expected) in [
        ("SELECT id FROM t WHERE id IN (SELECT u.id FROM u WHERE u.id=t.id)", ints(&[2])),
        ("SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE x=t.id)", vec![]),
        ("SELECT v FROM t WHERE id IN (SELECT id FROM u WHERE v='a')", vec![]),
    ] {
        assert_eq!(rows(s, sql), expected, "{sql}");
    }
    error(
        s,
        "SELECT id FROM t WHERE id IN (SELECT missing FROM u)",
        E::UnknownColumn,
    );
    error(
        s,
        "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE missing=1)",
        E::UnknownColumn,
    );
}
#[test]
fn bpchar_membership_is_rejected_during_binding() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE characters(id int PRIMARY KEY,c char(3))");
    run(s, "CREATE TABLE strings(id int PRIMARY KEY,v text)");
    run(s, "INSERT INTO characters VALUES(1,'abc'),(2,NULL)");
    run(s, "INSERT INTO strings VALUES(1,'abc'),(2,NULL)");
    for (outer, outer_filter, lhs, child, column) in [
        ("characters", "", "c", "strings", "v"),
        ("strings", "", "v", "characters", "c"),
        ("characters", "id=2 AND ", "c", "strings", "v"),
        ("strings", "", "NULL::text", "characters", "c"),
        ("strings", "", "NULL", "characters", "c"),
        ("strings", "", "'abc'", "characters", "c"),
    ] {
        for operator in ["IN", "NOT IN"] {
            for child_filter in ["", " WHERE false"] {
                let sql = format!(
                    "SELECT id FROM {outer} WHERE {outer_filter}{lhs} {operator} (SELECT {column} FROM {child}{child_filter})"
                );
                let prepared = s.prepare(&sql).unwrap();
                // Metadata binding rejects bpchar even with NULL operands or an empty child.
                assert_eq!(
                    s.describe(&prepared).unwrap_err(),
                    CoreError::Sql(E::Unsupported),
                    "{sql}"
                );
                assert_eq!(
                    s.execute_prepared(&prepared, &[], None),
                    Err(CoreError::Sql(E::Unsupported)),
                    "{sql}"
                );
                error(s, &sql, E::Unsupported);
            }
        }
    }
}
#[test]
fn varchar_and_text_membership_preserve_postgres_string_and_null_semantics() {
    let mut db = Db::new();
    let s = db.session();
    run(
        s,
        "CREATE TABLE strings(id int PRIMARY KEY,short varchar(3),wide text)",
    );
    run(
        s,
        "CREATE TABLE candidates(id int PRIMARY KEY,short varchar(3),wide text)",
    );
    run(
        s,
        "INSERT INTO strings VALUES(1,'abc','abc'),(2,'a ','a '),(3,'a','a'),(4,NULL,NULL)",
    );
    run(
        s,
        "INSERT INTO candidates VALUES(1,'a','a'),(2,NULL,NULL),(3,'xyz','xyz')",
    );
    // PG18.6: varchar/text comparisons preserve trailing spaces and SQL NULL semantics.
    for (lhs, child) in [("short", "wide"), ("wide", "short")] {
        for (predicate, expected) in [
            (
                format!("{lhs} IN (SELECT {child} FROM candidates)"),
                vec![3],
            ),
            (
                format!("{lhs} NOT IN (SELECT {child} FROM candidates)"),
                vec![],
            ),
            (
                format!("({lhs} IN (SELECT {child} FROM candidates)) IS NULL"),
                vec![1, 2, 4],
            ),
            (
                format!("{lhs} NOT IN (SELECT {child} FROM candidates WHERE id=1)"),
                vec![1, 2],
            ),
            (
                format!("{lhs} IN (SELECT {child} FROM candidates WHERE false)"),
                vec![],
            ),
            (
                format!("{lhs} NOT IN (SELECT {child} FROM candidates WHERE false)"),
                vec![1, 2, 3, 4],
            ),
        ] {
            let sql = format!("SELECT id FROM strings WHERE {predicate} ORDER BY id");
            let prepared = s.prepare(&sql).unwrap();
            s.describe(&prepared).unwrap();
            assert_eq!(rows(s, &sql), ints(&expected), "{sql}");
        }
    }
}
#[test]
fn unsupported_subquery_positions_remain_unsupported() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    error(s, "SELECT (SELECT id FROM u) FROM t", E::CardinalityViolation);
    for (sql, expected) in [
        ("SELECT id FROM t WHERE id = ANY (SELECT id FROM u)", ints(&[2])),
        ("SELECT id FROM t WHERE id = ALL (SELECT id FROM u)", vec![]),
        ("SELECT id,id IN (SELECT id FROM u) AS m FROM t ORDER BY id",
         vec![vec![V::Int(1), V::Bool(false)], vec![V::Int(2), V::Bool(true)], vec![V::Int(3), V::Bool(false)]]),
        ("SELECT (id IN (SELECT n FROM u)) IS NULL AS unk,id FROM t ORDER BY id",
         vec![vec![V::Bool(true), V::Int(1)], vec![V::Bool(true), V::Int(2)], vec![V::Bool(false), V::Int(3)]]),
        ("SELECT NULL::int IN (SELECT id FROM u WHERE false)", vec![vec![V::Bool(false)]]),
        ("SELECT '2' IN (SELECT id FROM u) AS lit", vec![vec![V::Bool(true)]]),
        ("SELECT CASE WHEN id IN (SELECT id FROM u) THEN 1 ELSE 0 END FROM t", ints(&[0, 1, 0])),
        ("SELECT id FROM t WHERE CASE WHEN id IN (SELECT id FROM u) THEN true ELSE false END", ints(&[2])),
        ("SELECT count(*) FILTER (WHERE id IN (SELECT id FROM u)) FROM t", ints(&[1])),
    ] {
        assert_eq!(rows(s, sql), expected, "{sql}");
    }
    let mut ordered = rows(s, "SELECT id FROM t ORDER BY id IN (SELECT id FROM u)");
    assert_eq!(ordered.pop(), Some(vec![V::Int(2)]));
    ordered.sort_by_key(|row| match row.as_slice() {
        [V::Int(id)] => *id,
        _ => panic!("{row:?}"),
    });
    assert_eq!(ordered, ints(&[1, 3]));
    for sql in [
        "SELECT id FROM t WHERE (id,v) IN (SELECT id,'b' FROM u)",
        "UPDATE t SET v=CASE WHEN id IN (SELECT id FROM u) THEN 'z' ELSE v END",
        "DELETE FROM t RETURNING id IN (SELECT id FROM u)",
        "UPDATE t SET v='z' RETURNING id IN (SELECT id FROM u)",
    ] {
        error(s, sql, E::Unsupported);
    }
    assert_eq!(rows(s, "SELECT t.id FROM t JOIN u ON t.id IN (SELECT id FROM u)"), ints(&[2, 2]));
    // PG18.6: 23505.
    error(
        s,
        "INSERT INTO t SELECT id FROM u WHERE id IN (SELECT id FROM t)",
        E::Duplicate,
    );
    // Unchanged from main 0ec7c954 (42601); not in this slice.
    error(
        s,
        "INSERT INTO t VALUES(4,(SELECT v FROM t WHERE id=1))",
        E::Syntax,
    );
}
#[test]
fn prepared_parameters_infer_and_materialization_is_per_execution() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    for sql in [
        "SELECT id FROM t WHERE $1 IN (SELECT id FROM u)",
        "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE x=$1)",
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, [T::Integer]);
    }
    let p = s
        .prepare("SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE x=$1) ORDER BY id")
        .unwrap();
    let execute = |s: &mut SqlSession, value| {
        s.execute_prepared(&p, &[V::Int(value)], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>()
    };
    assert_eq!(execute(s, 0), ints(&[2]));
    assert_eq!(execute(s, 1), ints(&[]));
    run(s, "UPDATE u SET id=3 WHERE id=2");
    assert_eq!(execute(s, 0), ints(&[3]));
}
#[test]
fn lazy_evaluation_and_failed_update_preserve_rows_and_versions() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "CREATE TABLE e(id int PRIMARY KEY)");
    assert!(rows(s, "SELECT id FROM e WHERE id IN (SELECT 1/x FROM u)").is_empty());
    error(
        s,
        "SELECT id FROM t WHERE id IN (SELECT 1/x FROM u)",
        E::DivisionByZero,
    );
    let before = run(s, "SELECT id,v FROM t ORDER BY id").rows;
    error(
        s,
        "UPDATE t SET v='z' WHERE id IN (SELECT 1/x FROM u)",
        E::DivisionByZero,
    );
    assert_eq!(run(s, "SELECT id,v FROM t ORDER BY id").rows, before);
}
#[test]
fn searched_dml_returning_and_self_reference_use_snapshot() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "BEGIN");
    let deleted = run(
        s,
        "DELETE FROM t WHERE id IN (SELECT id FROM u) RETURNING id",
    );
    assert_eq!(deleted.affected, 1);
    assert_eq!(
        deleted
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        ints(&[2])
    );
    run(s, "ROLLBACK");
    run(s, "BEGIN");
    let updated = run(
        s,
        "UPDATE t SET v='z' WHERE id NOT IN (SELECT id FROM u) RETURNING id,v",
    );
    assert_eq!(updated.affected, 2);
    assert_eq!(
        updated
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![
            vec![V::Int(1), V::Text("z".into())],
            vec![V::Int(3), V::Text("z".into())]
        ]
    );
    run(s, "ROLLBACK");
    assert_eq!(
        rows(
            s,
            "DELETE FROM t WHERE id IN (SELECT id FROM t WHERE id>1) RETURNING id"
        ),
        ints(&[2, 3])
    );
    assert_eq!(rows(s, "SELECT id FROM t ORDER BY id"), ints(&[1]));
}
#[test]
fn cancellation_aborts_transaction_and_explain_names_subquery() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let plan = format!(
        "{:?}",
        rows(s, "EXPLAIN SELECT id FROM t WHERE id IN (SELECT id FROM u)")
    );
    assert!(plan.contains("InSubquery"), "{plan}");
    run(s, "BEGIN");
    let cancel = Cancellation::default();
    cancel.cancel();
    assert_eq!(
        cancel.scope(|| s.execute("SELECT id FROM t WHERE id IN (SELECT id FROM u)", &[], None)),
        Err(CoreError::Cancelled)
    );
    assert!(s.is_aborted());
    error(s, "SELECT id FROM t", E::Aborted);
    s.rollback();
    assert_eq!(rows(s, "SELECT id FROM t ORDER BY id"), ints(&[1, 2, 3]));
}
#[test]
fn corpus_delete_with_in_subquery() {
    let mut db = Db::new();
    let s = db.session();
    let corpus = include_str!("../../../compat/corpus/dml_predicates/delete_with_in_subquery.sql");
    let sql = corpus
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut last = None;
    for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
        last = Some(run(s, statement));
    }
    assert_eq!(
        last.unwrap()
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        ints(&[1])
    );
}
#[test]
fn read_table_metadata_is_deduplicated_and_preserves_outer_target() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let cases: &[(&str, Option<&str>, &[&str])] = &[
        (
            "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE id IN (SELECT id FROM t))",
            Some("t"),
            &["t", "u"],
        ),
        (
            "SELECT t.id FROM t JOIN u ON t.id=u.id WHERE t.id IN (SELECT id FROM u)",
            Some("giggles_join"),
            &["t", "u"],
        ),
        (
            "SELECT 1 WHERE 2 IN (SELECT id FROM u WHERE id IN (SELECT id FROM t))",
            Some("giggles_join"),
            &["u", "t"],
        ),
        (
            "DELETE FROM t WHERE id IN (SELECT id FROM u)",
            Some("t"),
            &["u"],
        ),
        (
            "UPDATE t SET v='z' WHERE id IN (SELECT id FROM u WHERE id IN (SELECT id FROM t))",
            Some("t"),
            &["u", "t"],
        ),
        ("DELETE FROM t WHERE id=1", Some("t"), &[]),
    ];
    for &(sql, target, reads) in cases {
        let p = s.prepare(sql).unwrap();
        assert_eq!(p.table(), target, "{sql}");
        assert_eq!(p.read_tables(), reads, "{sql}");
    }
}
#[test]
fn subquery_and_outer_predicate_share_statement_clock() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let clock = giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 1_000_000,
        statement_us: 2_000_000,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    };
    for sql in [
        "SELECT id FROM t WHERE now() IN (SELECT now() FROM u) ORDER BY id",
        "SELECT id FROM t WHERE statement_timestamp() IN (SELECT statement_timestamp() FROM u) ORDER BY id",
        "SELECT id FROM t WHERE now() NOT IN (SELECT statement_timestamp() FROM u) ORDER BY id",
    ] {
        let result = s.execute_at(sql, &[], None, &clock).unwrap();
        assert_eq!(
            result
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            ints(&[1, 2, 3]),
            "{sql}"
        );
    }
}
#[test]
fn shared_parameter_numbers_and_compound_left_scalars() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let p = s
        .prepare("SELECT id FROM t WHERE id=$1 AND id IN (SELECT id FROM u WHERE x=$2) ORDER BY id")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, [T::Integer, T::Integer]);
    for (params, expected) in [
        ([V::Int(2), V::Int(0)], vec![2]),
        ([V::Int(2), V::Int(1)], vec![]),
        ([V::Int(1), V::Int(0)], vec![]),
    ] {
        let result = s.execute_prepared(&p, &params, None).unwrap();
        assert_eq!(
            result
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            ints(&expected)
        );
    }
    for sql in [
        "SELECT id FROM t WHERE (id+1) IN (SELECT id FROM u) ORDER BY id",
        "SELECT id FROM t WHERE ('1'::int+1) IN (SELECT id FROM u) AND id=1 ORDER BY id",
    ] {
        assert_eq!(rows(s, sql), ints(&[1]), "{sql}");
    }
    assert_eq!(
        rows(
            s,
            "SELECT id FROM t WHERE id::double precision IN (SELECT 2::bigint) ORDER BY id"
        ),
        ints(&[2])
    );
    assert_eq!(
        rows(
            s,
            "SELECT id FROM t WHERE id::double precision NOT IN (SELECT 2::bigint) ORDER BY id"
        ),
        ints(&[1, 3])
    );
}
#[test]
fn nested_subquery_depth_is_bounded_and_table_free_explain_works() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    let mut nested = "SELECT id FROM u".to_owned();
    for _ in 0..34 {
        nested = format!("SELECT id FROM u WHERE id IN ({nested})");
    }
    error(
        s,
        &format!("SELECT id FROM t WHERE id IN ({nested})"),
        E::Limit,
    );
    error(
        s,
        &format!("UPDATE t SET v='z' WHERE id IN ({nested})"),
        E::Limit,
    );
    error(
        s,
        &format!("DELETE FROM t WHERE id IN ({nested})"),
        E::Limit,
    );
    let plan = format!(
        "{:?}",
        rows(s, "EXPLAIN SELECT 1 WHERE 2 IN (SELECT id FROM u)")
    );
    assert!(plan.contains("InSubquery"), "{plan}");
}
#[test]
fn parameters_inherit_outer_and_sibling_concrete_contexts() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    for sql in [
        "SELECT id FROM t WHERE id=$1 AND id IN (SELECT $1) ORDER BY id",
        "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE id=$1) AND id IN (SELECT $1) ORDER BY id",
        "SELECT id FROM t WHERE id IN (SELECT $1) AND id IN (SELECT id FROM u WHERE id=$1) ORDER BY id",
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, [T::Integer], "{sql}");
        let result = s.execute_prepared(&p, &[V::Int(2)], None).unwrap();
        assert_eq!(
            result
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            ints(&[2]),
            "{sql}"
        );
    }
}
#[test]
fn child_distinct_deduplicates_before_order_and_limit() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    run(s, "INSERT INTO u VALUES(11,0,3)");
    assert_eq!(
        rows(
            s,
            "SELECT id FROM t WHERE id IN (SELECT DISTINCT x FROM u ORDER BY x LIMIT 2) ORDER BY id"
        ),
        ints(&[1])
    );
    // The groups for ids 2 and 11 both project 0; DISTINCT must run
    // before LIMIT, leaving 0 and 1 as the two candidate values.
    assert_eq!(
        rows(
            s,
            "SELECT id FROM t WHERE id IN (SELECT DISTINCT max(x) FROM u GROUP BY id ORDER BY 1 LIMIT 2) ORDER BY id"
        ),
        ints(&[1])
    );
}
#[test]
fn aggregate_preprocessing_preserves_nested_select_tokens() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    assert_eq!(
        rows(
            s,
            "SELECT count(*)+1 FROM t WHERE id IN (SELECT count(*) FROM u)"
        ),
        ints(&[2])
    );
    assert_eq!(
        rows(
            s,
            "SELECT count(*)+1 FROM t WHERE id IN (SELECT max(id) FILTER (WHERE id<5) FROM t WHERE id IN (SELECT count(*) FROM t))"
        ),
        ints(&[2])
    );
}
#[test]
fn qualified_correlation_and_local_shadowing_errors() {
    let mut db = Db::new();
    let s = db.session();
    fixture(s);
    for sql in [
        "SELECT a.id FROM t a WHERE a.id IN (SELECT id FROM u WHERE a.missing=1)",
        "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE t.missing=1)",
    ] {
        error(s, sql, E::UnknownColumn);
    }
    error(
        s,
        "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE absent.missing=1)",
        E::UnknownTable,
    );
    error(
        s,
        "SELECT a.id FROM t a WHERE a.id IN (SELECT id FROM u a WHERE a.missing=1)",
        E::UnknownColumn,
    );
}

#[test]
fn exists_admission_and_nesting_limit_on_each_engine() {
    for engine in [Engine::Legacy, #[cfg(feature = "engine-rocksdb")] Engine::V2] {
        let path = std::env::temp_dir().join(format!("c3-exists-{}-{:?}", std::process::id(), engine));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for sql in ["SELECT 1 WHERE EXISTS (SELECT NULL)",
            "SELECT 1 WHERE NOT EXISTS (SELECT NULL)",
            "SELECT 1 WHERE false OR NOT (EXISTS (SELECT NULL))",
            "SELECT 1 WHERE 1 IN (SELECT 1 WHERE EXISTS (SELECT NULL))"] {
            let prepared = session.prepare(sql).unwrap();
            session.describe(&prepared).unwrap();
            run(&mut session, sql);
        }
        let mut nested = "SELECT NULL".to_string();
        for _ in 0..33 { nested = format!("SELECT 1 WHERE EXISTS ({nested})"); }
        error(&mut session, &nested, E::Limit);
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
