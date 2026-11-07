use giggles_core::{
    relational::{DataType, SqlError as E, SqlResult, Value as V},
    *,
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn db() -> (Admin, SqlSession) {
    let path = std::env::temp_dir().join(format!(
        "gd-query-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let a = Admin::create(path, Limits::default()).unwrap();
    let s = a.sql_session();
    (a, s)
}
fn run(s: &mut SqlSession, q: &str) -> SqlResult {
    s.execute(q, &[], None).unwrap()
}
fn vals(r: SqlResult) -> Vec<Vec<V>> {
    r.rows.into_iter().map(|r| r.values).collect()
}
fn err(s: &mut SqlSession, q: &str, e: E) {
    assert_eq!(s.execute(q, &[], None), Err(CoreError::Sql(e)), "{q}");
}
fn schema(s: &mut SqlSession) {
    run(
        s,
        "CREATE TABLE people (id BIGINT PRIMARY KEY, manager BIGINT, label TEXT)",
    );
    run(
        s,
        "CREATE TABLE jobs (id BIGINT PRIMARY KEY, person BIGINT, wage BIGINT)",
    );
}
#[test]
fn qualified_alias_resolution_and_keywords() {
    let (_, mut s) = db();
    schema(&mut s);
    run(
        &mut s,
        "INSERT INTO people VALUES(1,NULL,'join from count left as')",
    );
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT p.label AS title FROM people AS p LIMIT 2"
        )),
        vec![vec![V::Text("join from count left as".into())]]
    );
    assert_eq!(
        vals(run(&mut s, "SELECT p.id FROM people p LIMIT 2")),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        vals(run(&mut s, "SELECT id FROM people LIMIT 2")),
        vec![vec![V::Int(1)]]
    );
}
#[test]
fn invalid_scope_duplicate_and_ambiguous_names() {
    let (_, mut s) = db();
    schema(&mut s);
    run(&mut s, "INSERT INTO people VALUES (1,NULL,'one')");
    err(
        &mut s,
        "SELECT people.id FROM people p LIMIT 1",
        E::UnknownTable,
    );
    for (q, e) in [
        (
            "SELECT id FROM people p JOIN jobs j ON j.id=p.id",
            E::Duplicate,
        ),
        (
            "SELECT p.id FROM people p JOIN jobs p ON p.id=p.id",
            E::Duplicate,
        ),
        ("SELECT p.missing FROM people p LIMIT 1", E::UnknownColumn),
        ("SELECT z.id FROM absent z LIMIT 1", E::UnknownTable),
        (
            "SELECT p.id FROM people p JOIN jobs j ON j.id=z.id",
            E::UnknownTable,
        ),
        (
            "SELECT p.id AS x,p.manager AS x FROM people p LIMIT 1",
            E::Duplicate,
        ),
        (
            "SELECT p.id FROM people p JOIN jobs j ON j.person=p.id",
            E::Unbounded,
        ),
        (
            "SELECT p.id FROM people p JOIN jobs j ON j.id>p.id",
            E::Unsupported,
        ),
        ("SELECT p.id FROM people p JOIN jobs j", E::Syntax),
        (
            "SELECT p.id FROM people p CROSS JOIN jobs j",
            E::Unsupported,
        ),
    ] {
        err(&mut s, q, e);
    }
    let p = s
        .prepare("SELECT p.id FROM people p JOIN jobs j ON j.id=p.id")
        .unwrap();
    assert_eq!(p.table(), Some("giggles_join"));
}
#[test]
fn typed_empty_and_stale_prepared() {
    let (_, mut s) = db();
    schema(&mut s);
    let p = s
        .prepare("SELECT p.label FROM people p WHERE p.id=$1 LIMIT $2")
        .unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(d.parameters, vec![DataType::BigInt, DataType::BigInt]);
    assert_eq!(
        s.execute_prepared(&p, &[V::Text("wrong".into()), V::Int(0)], None),
        Err(CoreError::Sql(E::Type))
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1)], None),
        Err(CoreError::Sql(E::Parameters))
    );
    run(&mut s, "CREATE TABLE newer (id BIGINT PRIMARY KEY)");
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1), V::Int(0)], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE TABLE branch (id BIGINT PRIMARY KEY)");
    let p = s.prepare("SELECT b.id FROM branch b LIMIT 1").unwrap();
    run(&mut s, "ROLLBACK");
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE TABLE branch (id BIGINT PRIMARY KEY)");
    assert_eq!(
        s.execute_prepared(&p, &[], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    s.rollback();
}
#[test]
fn indexed_join_multiplicity_left_null_self_and_composite() {
    let (_, mut s) = db();
    schema(&mut s);
    for q in [
        "INSERT INTO people VALUES(1,NULL,'boss')",
        "INSERT INTO people VALUES(2,1,'staff')",
        "INSERT INTO people VALUES(3,1,'other')",
        "INSERT INTO jobs VALUES(10,2,7)",
        "INSERT INTO jobs VALUES(11,2,9)",
        "INSERT INTO jobs VALUES(12,99,4)",
        "INSERT INTO jobs VALUES(13,NULL,5)",
    ] {
        run(&mut s, q);
    }
    let q = "SELECT j.id,p.label FROM jobs j LEFT JOIN people p ON p.id=j.person ORDER BY j.id LIMIT 10";
    let r = run(&mut s, q);
    assert!(r.rows.iter().all(|r| r.version == 0));
    assert_eq!(
        vals(r),
        vec![
            vec![V::Int(10), V::Text("staff".into())],
            vec![V::Int(11), V::Text("staff".into())],
            vec![V::Int(12), V::Null],
            vec![V::Int(13), V::Null]
        ]
    );
    let p = s.prepare(q).unwrap();
    assert!(!s.describe(&p).unwrap().columns[1].not_null);
    let p = s
        .prepare("SELECT p.id FROM jobs j LEFT JOIN people p ON p.id=j.person")
        .unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(d.columns[0].data_type, DataType::BigInt);
    assert!(
        !d.columns[0].not_null,
        "right PK metadata must become nullable"
    );
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id FROM jobs j INNER JOIN people p ON j.person=p.id ORDER BY j.id DESC LIMIT 10"
        )),
        vec![vec![V::Int(11)], vec![V::Int(10)]]
    );
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT p.id,m.id FROM people p LEFT JOIN people m ON m.id=p.manager ORDER BY p.id LIMIT 10"
        )),
        vec![
            vec![V::Int(1), V::Null],
            vec![V::Int(2), V::Int(1)],
            vec![V::Int(3), V::Int(1)]
        ]
    );
    run(
        &mut s,
        "CREATE TABLE pairs (a BIGINT,b BIGINT,label TEXT,PRIMARY KEY(a,b))",
    );
    run(&mut s, "INSERT INTO pairs VALUES(1,2,'pair')");
    assert!(
        run(
            &mut s,
            "SELECT j.id,x.label FROM jobs j JOIN pairs x ON x.b=j.person AND x.a=j.wage LIMIT 10"
        )
        .rows
        .is_empty()
    );
    err(
        &mut s,
        "SELECT j.id FROM jobs j JOIN pairs x ON x.a=j.id",
        E::Unbounded,
    );
    run(&mut s, "INSERT INTO jobs VALUES(14,2,1)");
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id,x.label FROM jobs j JOIN pairs x ON x.b=j.person AND x.a=j.wage LIMIT 10"
        )),
        vec![vec![V::Int(14), V::Text("pair".into())]]
    );
}
#[test]
fn independent_randomized_join_oracle() {
    for initial_seed in 1..=8u64 {
        let (_, mut s) = db();
        schema(&mut s);
        run(&mut s, "BEGIN");
        let mut seed = 0x12345678u64 ^ initial_seed;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed >> 32
        };
        let mut people = vec![];
        let mut jobs = vec![];
        for id in 0..35i64 {
            if next() % 3 != 0 {
                people.push((id, next() as i64 % 100));
            }
        }
        for &(id, manager) in &people {
            s.execute(
                "INSERT INTO people VALUES($1,$2,'x')",
                &[V::Int(id), V::Int(manager)],
                None,
            )
            .unwrap();
        }
        for id in 0..180i64 {
            let key = if next() % 7 == 0 {
                None
            } else {
                Some((next() % 48) as i64)
            };
            jobs.push((id, key));
            s.execute(
                "INSERT INTO jobs VALUES($1,$2,1)",
                &[V::Int(id), key.map_or(V::Null, V::Int)],
                None,
            )
            .unwrap();
        }
        for left in [false, true] {
            let mut expected = vec![];
            // Independent exhaustive nested-loop oracle, with no engine key encoding or binder.
            for &(id, key) in &jobs {
                let mut matched = false;
                for &(pid, _) in &people {
                    if key == Some(pid) {
                        expected.push(vec![V::Int(id), V::Int(pid)]);
                        matched = true;
                    }
                }
                if left && !matched {
                    expected.push(vec![V::Int(id), V::Null]);
                }
            }
            let q = format!(
                "SELECT j.id,p.id FROM jobs j {} JOIN people p ON j.person=p.id ORDER BY j.id LIMIT 1024",
                if left { "LEFT" } else { "INNER" }
            );
            assert_eq!(vals(run(&mut s, &q)), expected);
        }
        run(&mut s, "ROLLBACK");
    }
}
#[test]
fn aggregates_empty_null_negative_duplicate_and_join() {
    let (_, mut s) = db();
    schema(&mut s);
    let q = "SELECT COUNT(*),COUNT(j.wage),SUM(j.wage),MIN(j.wage),MAX(j.wage) FROM jobs j";
    assert_eq!(
        vals(run(&mut s, q)),
        vec![vec![V::Int(0), V::Int(0), V::Null, V::Null, V::Null]]
    );
    run(&mut s, "INSERT INTO jobs VALUES(1,NULL,NULL)");
    assert_eq!(
        vals(run(&mut s, q)),
        vec![vec![V::Int(1), V::Int(0), V::Null, V::Null, V::Null]]
    );
    for (id, wage) in [(2, -8), (3, -3), (4, -8)] {
        run(&mut s, &format!("INSERT INTO jobs VALUES({id},1,{wage})"));
    }
    assert_eq!(
        vals(run(&mut s, q)),
        vec![vec![
            V::Int(4),
            V::Int(3),
            V::Numeric(giggles_core::relational::Numeric::parse("-19").unwrap()),
            V::Int(-8),
            V::Int(-3)
        ]]
    );
    run(&mut s, "INSERT INTO people VALUES(1,NULL,'x')");
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT COUNT(*),SUM(j.wage) FROM jobs j JOIN people p ON p.id=j.person"
        )),
        vec![vec![
            V::Int(3),
            V::Numeric(giggles_core::relational::Numeric::parse("-19").unwrap())
        ]]
    );
    assert!(run(&mut s, q).rows.iter().all(|r| r.version == 0));
}
#[test]
fn checked_sum_overflow_and_group_rejection() {
    let (_, mut s) = db();
    schema(&mut s);
    for (id, w) in [(1, i64::MAX), (2, 1)] {
        run(&mut s, &format!("INSERT INTO jobs VALUES({id},NULL,{w})"));
    }
    assert!(
        run(&mut s, "SELECT SUM(j.wage) FROM jobs j LIMIT 0")
            .rows
            .is_empty()
    );
    assert_eq!(
        vals(run(&mut s, "SELECT SUM(j.wage) FROM jobs j")),
        vec![vec![V::Numeric(
            giggles_core::relational::Numeric::parse("9223372036854775808").unwrap()
        )]]
    );
    // PG18.6: an ungrouped column is 42803; grouped by it, one count per id.
    err(&mut s, "SELECT j.id,COUNT(*) FROM jobs j", E::GroupingError);
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id,COUNT(*) FROM jobs j GROUP BY j.id ORDER BY j.id"
        )),
        vec![vec![V::Int(1), V::Int(1)], vec![V::Int(2), V::Int(1)]]
    );
    err(&mut s, "SELECT SUM(p.label) FROM people p", E::Type);
}
#[test]
fn empty_and_read_your_writes_conflict() {
    let (a, mut s) = db();
    schema(&mut s);
    assert!(
        run(
            &mut s,
            "SELECT j.id,p.id FROM jobs j LEFT JOIN people p ON p.id=j.person"
        )
        .rows
        .is_empty()
    );
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO people VALUES(1,NULL,'x')");
    run(&mut s, "INSERT INTO jobs VALUES(1,1,7)");
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT COUNT(*) FROM jobs j JOIN people p ON p.id=j.person"
        )),
        vec![vec![V::Int(1)]]
    );
    let mut other = a.sql_session();
    run(&mut other, "INSERT INTO jobs VALUES(2,NULL,0)");
    err(&mut s, "COMMIT", E::Conflict);
    assert!(s.is_aborted());
    s.rollback();
    assert_eq!(
        vals(run(&mut s, "SELECT COUNT(*) FROM people p")),
        vec![vec![V::Int(0)]]
    );
}
#[test]
fn explain_is_physical_redacted_and_read_only() {
    let (_, mut s) = db();
    schema(&mut s);
    let r = run(
        &mut s,
        "EXPLAIN SELECT COUNT(*) FROM jobs j LEFT JOIN people p ON p.id=j.person WHERE j.wage=5678",
    );
    let V::Text(plan) = &r.rows[0].values[0] else {
        panic!()
    };
    for op in [
        "PrimaryKeyRangeScan",
        "LeftPrimaryKeyNestedLoop",
        "GlobalAggregate",
        "retained_bytes<=67108864",
        "result_rows<=65536",
        "result_bytes<=1048576",
    ] {
        assert!(plan.contains(op), "{plan}");
    }
    assert!(!plan.contains("5678"));
    err(&mut s, "EXPLAIN INSERT INTO jobs VALUES(1,1,1)", E::Syntax);
    err(
        &mut s,
        "EXPLAIN ANALYZE SELECT COUNT(*) FROM jobs",
        E::Syntax,
    );
    assert_eq!(
        vals(run(&mut s, "SELECT COUNT(*) FROM jobs")),
        vec![vec![V::Int(0)]]
    );
}
#[test]
fn cancellation_and_malformed_binding_abort_transaction() {
    let (_, mut s) = db();
    schema(&mut s);
    run(&mut s, "BEGIN");
    let cancel = Cancellation::default();
    cancel.cancel();
    assert_eq!(
        cancel.scope(|| s.execute("SELECT COUNT(*) FROM jobs", &[], None)),
        Err(CoreError::Cancelled)
    );
    assert!(s.is_aborted());
    s.rollback();
    run(&mut s, "BEGIN");
    let p = s
        .prepare("SELECT COUNT(*) FROM jobs j WHERE j.wage=$1")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[V::Bool(false)], None),
        Err(CoreError::Sql(E::Type))
    );
    assert!(s.is_aborted());
    s.rollback();
}
#[test]
fn limits_are_independent_of_limit() {
    let (_, mut s) = db();
    schema(&mut s);
    run(&mut s, "BEGIN");
    for id in 0..1024 {
        run(&mut s, &format!("INSERT INTO people VALUES({id},{id},'x')"));
    }
    let q = "SELECT p.id FROM people p LEFT JOIN people a ON a.id=p.manager LEFT JOIN people b ON b.id=a.manager LEFT JOIN people c ON c.id=b.manager";
    assert_eq!(run(&mut s, &format!("{q} LIMIT 0")).rows.len(), 0);
    let q = format!("{q} LEFT JOIN people d ON d.id=c.manager LIMIT 0");
    assert!(run(&mut s, &q).rows.is_empty());
    s.rollback();
}
#[test]
fn bytes_boundary_no_partial_result() {
    let (_, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE payloads (id BIGINT PRIMARY KEY, body TEXT)",
    );
    run(&mut s, "BEGIN");
    let body = V::Text("a".repeat(32759));
    for id in 0..32 {
        s.execute(
            "INSERT INTO payloads VALUES($1,$2)",
            &[V::Int(id), body.clone()],
            None,
        )
        .unwrap();
    }
    assert_eq!(
        run(&mut s, "SELECT p.body FROM payloads p LIMIT 32")
            .rows
            .len(),
        32
    );
    // LIMIT 0 emits no projected bytes, even when the full projection would overflow.
    assert!(run(&mut s, "SELECT p.body,p.body FROM payloads p LIMIT 0").rows.is_empty());
    s.rollback();
    run(&mut s, "BEGIN");
    for id in 0..32 {
        s.execute(
            "INSERT INTO payloads VALUES($1,$2)",
            &[V::Int(id), body.clone()],
            None,
        )
        .unwrap();
    }
    s.execute("INSERT INTO payloads VALUES(32,$1)", &[body], None)
        .unwrap();
    assert!(run(&mut s, "SELECT p.body FROM payloads p LIMIT 0").rows.is_empty());
    s.rollback();
}

#[test]
fn null_predicates_and_null_equalities_never_match() {
    let (_, mut s) = db();
    schema(&mut s);
    run(&mut s, "INSERT INTO people VALUES(1,NULL,'x')");
    run(&mut s, "INSERT INTO jobs VALUES(1,NULL,1)");
    assert_eq!(
        vals(run(&mut s, "SELECT COUNT(*) FROM people p WHERE p.id=NULL")),
        vec![vec![V::Int(0)]]
    );
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id,p.id FROM jobs j LEFT JOIN people p ON p.id=j.id AND p.manager=j.person"
        )),
        vec![vec![V::Int(1), V::Null]]
    );
    let p = s
        .prepare("SELECT COUNT(*) FROM people p WHERE p.id=$1")
        .unwrap();
    assert_eq!(
        vals(s.execute_prepared(&p, &[V::Null], None).unwrap()),
        vec![vec![V::Int(0)]]
    );
}

#[test]
fn scalar_extrema_binary_order_and_narrow_parameter_types() {
    let (_, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE scalars (id BIGINT PRIMARY KEY, n SMALLINT, flag BOOLEAN, label TEXT, raw BYTEA)",
    );
    for (id, n, flag, label, raw) in [
        (1, -4, false, "z", vec![255]),
        (2, -2, true, "é", vec![0, 255]),
        (3, 0, true, "a", vec![0]),
    ] {
        s.execute(
            "INSERT INTO scalars VALUES($1,$2,$3,$4,$5)",
            &[
                V::Int(id),
                V::Int(n),
                V::Bool(flag),
                V::Text(label.into()),
                V::Bytes(raw),
            ],
            None,
        )
        .unwrap();
    }
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT MIN(s.flag),MAX(s.flag),MIN(s.label),MAX(s.label),MIN(s.raw),MAX(s.raw) FROM scalars s"
        )),
        vec![vec![
            V::Bool(false),
            V::Bool(true),
            V::Text("a".into()),
            V::Text("é".into()),
            V::Bytes(vec![0]),
            V::Bytes(vec![255])
        ]]
    );
    let p = s
        .prepare("SELECT COUNT(*) FROM scalars s WHERE s.n=$1")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![DataType::SmallInt]);
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(32768)], None),
        Err(CoreError::Sql(E::Type))
    );
    err(
        &mut s,
        "SELECT s.id FROM scalars s JOIN scalars t ON t.id=s.n",
        E::Type,
    );
}

#[test]
fn stable_sort_null_order_and_global_limit() {
    let (_, mut s) = db();
    schema(&mut s);
    for q in [
        "INSERT INTO jobs VALUES(1,1,NULL)",
        "INSERT INTO jobs VALUES(2,1,-2)",
        "INSERT INTO jobs VALUES(3,1,-2)",
        "INSERT INTO jobs VALUES(4,1,9)",
    ] {
        run(&mut s, q);
    }
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id FROM jobs j ORDER BY j.wage ASC LIMIT 3"
        )),
        vec![vec![V::Int(2)], vec![V::Int(3)], vec![V::Int(4)]]
    );
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id FROM jobs j ORDER BY j.wage DESC,j.id DESC LIMIT 4"
        )),
        vec![
            vec![V::Int(1)],
            vec![V::Int(4)],
            vec![V::Int(3)],
            vec![V::Int(2)]
        ]
    );
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT j.id FROM jobs j ORDER BY j.wage ASC NULLS FIRST LIMIT 3"
        )),
        vec![vec![V::Int(1)], vec![V::Int(2)], vec![V::Int(3)]]
    );
    assert!(
        run(&mut s, "SELECT COUNT(*) FROM jobs LIMIT 0")
            .rows
            .is_empty()
    );
}

#[test]
fn future_join_scope_and_conflicting_parameter_types_reject_empty_tables() {
    let (_, mut s) = db();
    schema(&mut s);
    err(
        &mut s,
        "SELECT p.id FROM people p JOIN jobs j ON j.id=x.id JOIN people x ON x.id=p.id",
        E::UnknownTable,
    );
    let p = s
        .prepare("SELECT COUNT(*) FROM people p WHERE p.id=$1 AND p.label=$1")
        .unwrap();
    // PG 18.6 infers $1 = bigint from p.id; p.label = $1 then has no
    // text = bigint operator: 42883 "operator does not exist: text = bigint".
    assert!(matches!(
        s.describe(&p),
        Err(CoreError::Sql(E::UndefinedFunction))
    ));
    err(
        &mut s,
        "SELECT p.id FROM people p WHERE p.id=$2",
        E::Parameters,
    );
    err(&mut s, "SELECT COUNT(*) FROM people p LIMIT -1", E::Limit);
    assert_eq!(
        vals(run(&mut s, "SELECT COUNT(*) FROM people p LIMIT 1025")),
        vec![vec![V::Int(0)]]
    );
}

#[test]
fn indexed_point_explain_metadata_and_version_preservation() {
    let (_, mut s) = db();
    schema(&mut s);
    run(&mut s, "INSERT INTO people VALUES(1,NULL,'x')");
    let plain = s.prepare("SELECT id FROM people WHERE id=$1").unwrap();
    let aliased = s.prepare("SELECT p.id FROM people p WHERE p.id=$1").unwrap();
    assert_eq!(plain.command(), "SELECT");
    assert_eq!(plain.table(), Some("people"));
    let plain_description = s.describe(&plain).unwrap();
    let aliased_description = s.describe(&aliased).unwrap();
    assert_eq!(plain_description.parameters, aliased_description.parameters);
    assert_eq!(plain_description.columns, aliased_description.columns);
    let old = run(&mut s, "SELECT id FROM people WHERE id=1").rows[0].version;
    assert_eq!(
        run(&mut s, "SELECT p.id FROM people p WHERE p.id=1").rows[0].version,
        old
    );
    let p = s
        .prepare("EXPLAIN SELECT p.label FROM people p WHERE p.id=$1")
        .unwrap();
    assert_eq!(p.command(), "EXPLAIN");
    assert_eq!(p.table(), Some("people"));
    let d = s.describe(&p).unwrap();
    assert_eq!(d.columns[0].name, "plan");
    assert_eq!(d.parameters, vec![DataType::BigInt]);
    let r = s.execute_prepared(&p, &[V::Int(1)], None).unwrap();
    let V::Text(plan) = &r.rows[0].values[0] else {
        panic!()
    };
    assert!(plan.starts_with("PrimaryKeyLookup"));
    let p = s.prepare("SELECT p.id FROM people p WHERE p.id=1").unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[], Some(old)),
        Err(CoreError::Sql(E::Unsupported))
    );
}

#[test]
fn legacy_aggregate_keyword_identifiers_keep_working() {
    let (_, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE min (id BIGINT PRIMARY KEY, count BIGINT)",
    );
    run(&mut s, "INSERT INTO min VALUES(1,9)");
    assert_eq!(
        vals(run(&mut s, "SELECT count FROM min WHERE id=1")),
        vec![vec![V::Int(9)]]
    );
}

#[test]
fn aggregate_names_are_identifiers_unless_followed_by_parenthesis() {
    let (_, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE metrics (id BIGINT PRIMARY KEY, count BIGINT, sum BIGINT, min BIGINT, max BIGINT)",
    );
    run(&mut s, "INSERT INTO metrics VALUES(1,9,10,11,12)");
    for (name, expected) in [("count", 9), ("sum", 10), ("min", 11), ("max", 12)] {
        assert_eq!(
            vals(run(
                &mut s,
                &format!("SELECT {name} AS n FROM metrics WHERE id=1")
            )),
            vec![vec![V::Int(expected)]]
        );
        assert_eq!(
            vals(run(
                &mut s,
                &format!("SELECT {name}.count AS n FROM metrics AS {name} WHERE {name}.id=1")
            )),
            vec![vec![V::Int(9)]]
        );
        assert_eq!(
            run(
                &mut s,
                &format!("EXPLAIN SELECT {name} FROM metrics WHERE id=1")
            )
            .columns,
            vec!["plan"]
        );
    }
    assert_eq!(
        vals(run(
            &mut s,
            "SELECT COUNT(*),SUM(sum),MIN(min),MAX(max) FROM metrics"
        )),
        vec![vec![
            V::Int(1),
            V::Numeric(giggles_core::relational::Numeric::parse("10").unwrap()),
            V::Int(11),
            V::Int(12)
        ]]
    );
}
