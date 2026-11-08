mod common;
use common::*;
use giggles_core::{
    relational::{SqlError as E, Value as V, *},
    *,
};
fn db() -> (std::path::PathBuf, Admin, SqlSession) {
    let p = fresh("sql");
    let a = Admin::create(&p, Limits::default()).unwrap();
    let s = a.sql_session();
    (p, a, s)
}
#[test]
fn canonical_encoding_vectors_and_early_size_bounds() {
    assert_eq!(
        encode_key(&[V::Null, V::Bool(false), V::Bool(true)]).unwrap(),
        vec![0, 1, 0, 1, 1]
    );
    assert_eq!(
        encode_key(&[V::Int(i64::MIN)]).unwrap(),
        vec![2, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        encode_key(&[V::Int(-1)]).unwrap(),
        vec![2, 127, 255, 255, 255, 255, 255, 255, 255]
    );
    assert_eq!(
        encode_key(&[V::Int(0)]).unwrap(),
        vec![2, 128, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        encode_key(&[V::Text("a\0".into()), V::Bytes(vec![0, 255])]).unwrap(),
        vec![3, 97, 0, 255, 0, 0, 4, 0, 255, 255, 0, 0]
    );
    assert_eq!(
        encode_key(&[V::Text("\0".repeat(MAX_ROW_BYTES))])
            .unwrap()
            .len(),
        MAX_ROW_BYTES * 2 + 3
    );
    assert_eq!(
        encode_key(&[V::Bytes(vec![0; MAX_ROW_BYTES + 1])]),
        Err(E::Limit)
    );
    assert_eq!(encode_key(&vec![V::Null; MAX_COLUMNS + 1]), Err(E::Limit));
}
fn run(s: &mut SqlSession, q: &str) -> SqlResult {
    s.execute(q, &[], None).unwrap()
}
fn err(s: &mut SqlSession, q: &str, e: E) {
    assert_eq!(s.execute(q, &[], None), Err(CoreError::Sql(e)), "{q}");
}
fn values(r: SqlResult) -> Vec<Vec<V>> {
    r.rows.into_iter().map(|r| r.values).collect()
}
fn ints(ns: &[i64]) -> Vec<Vec<V>> {
    ns.iter().map(|n| vec![V::Int(*n)]).collect()
}
#[test]
fn crud_expected_rows_and_null_constraints() {
    let (_, _, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE things (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL, title TEXT, raw BYTEA)",
    );
    assert_eq!(
        s.execute(
            "INSERT INTO things VALUES ($1,$2,$3,$4)",
            &[
                V::Int(-7),
                V::Bool(true),
                V::Text("snow ☃\0'".into()),
                V::Bytes(vec![0, 255, 3])
            ],
            None
        )
        .unwrap()
        .affected,
        1
    );
    assert_eq!(
        values(run(
            &mut s,
            "SELECT title,raw,active FROM things WHERE id=-7"
        )),
        vec![vec![
            V::Text("snow ☃\0'".into()),
            V::Bytes(vec![0, 255, 3]),
            V::Bool(true)
        ]]
    );
    run(&mut s, "INSERT INTO things (id, active) VALUES (8,false)");
    assert_eq!(
        values(run(&mut s, "SELECT title,raw FROM things WHERE id=8")),
        vec![vec![V::Null, V::Null]]
    );
    err(
        &mut s,
        "INSERT INTO things VALUES (8,true,NULL,NULL)",
        E::Duplicate,
    );
    err(&mut s, "INSERT INTO things (id) VALUES(9)", E::Null);
    err(
        &mut s,
        "INSERT INTO things VALUES(NULL,true,NULL,NULL)",
        E::Null,
    );
    err(&mut s, "INSERT INTO things VALUES(9,1,NULL,NULL)", E::Type);
    err(&mut s, "UPDATE things SET active=NULL WHERE id=8", E::Null);
    assert_eq!(run(&mut s, "UPDATE things SET id=12 WHERE id=8").affected, 1);
    assert_eq!(run(&mut s, "UPDATE things SET id=8 WHERE id=12").affected, 1);
    assert_eq!(
        run(
            &mut s,
            "UPDATE things SET title='it''s new',active=true WHERE id=8"
        )
        .affected,
        1
    );
    assert_eq!(
        values(run(&mut s, "SELECT title,active FROM things WHERE id=8")),
        vec![vec![V::Text("it's new".into()), V::Bool(true)]]
    );
    assert_eq!(run(&mut s, "DELETE FROM things WHERE id=-7").affected, 1);
    assert_eq!(run(&mut s, "DELETE FROM things WHERE id=-7").affected, 0);
    assert_eq!(
        values(run(&mut s, "SELECT id FROM things LIMIT 10")),
        ints(&[8])
    );
}
#[test]
fn signed_boundaries_and_no_coercion() {
    let (_, _, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE widths (id BIGINT PRIMARY KEY, s SMALLINT, i INTEGER)",
    );
    for n in [i64::MIN, -256, -1, 0, 1, 256, i64::MAX] {
        s.execute(
            "INSERT INTO widths VALUES($1,-32768,2147483647)",
            &[V::Int(n)],
            None,
        )
        .unwrap();
    }
    assert_eq!(
        values(run(&mut s, "SELECT id FROM widths ORDER BY id LIMIT 20")),
        ints(&[i64::MIN, -256, -1, 0, 1, 256, i64::MAX])
    );
    assert_eq!(
        values(run(
            &mut s,
            "SELECT id FROM widths WHERE id > -1 AND id <= 256 ORDER BY id DESC LIMIT 2"
        )),
        ints(&[256, 1])
    );
    for q in [
        "INSERT INTO widths VALUES(2,32768,1)",
        "INSERT INTO widths VALUES(2,1,2147483648)",
        "INSERT INTO widths VALUES(9223372036854775808,1,1)",
    ] {
        err(&mut s, q, E::Overflow);
    }
    err(&mut s, "SELECT id FROM widths WHERE id='1'", E::Type);
    assert_eq!(
        s.execute(
            "INSERT INTO widths VALUES($1,$2,$3)",
            &[V::Bool(true), V::Int(1), V::Int(1)],
            None
        ),
        Err(CoreError::Sql(E::Type))
    );
}
#[test]
fn composite_prefix_ranges_and_binary_order() {
    let (_, _, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE messages (tenant BIGINT, bucket INTEGER, id BIGINT, body TEXT, PRIMARY KEY(tenant,bucket,id))",
    );
    for (a, b, c) in [
        (2, 1, 0),
        (1, 2, 9),
        (1, 1, 7),
        (1, 1, -1),
        (1, 1, 3),
        (1, 1, 9),
        (1, 0, 8),
    ] {
        s.execute(
            "INSERT INTO messages VALUES($1,$2,$3,'x')",
            &[V::Int(a), V::Int(b), V::Int(c)],
            None,
        )
        .unwrap();
    }
    assert_eq!(
        values(run(
            &mut s,
            "SELECT id FROM messages WHERE tenant=1 AND bucket=1 AND id<9 ORDER BY id DESC LIMIT 2"
        )),
        ints(&[7, 3])
    );
    assert_eq!(
        values(run(
            &mut s,
            "SELECT bucket,id FROM messages WHERE tenant=1 ORDER BY bucket,id LIMIT 20"
        )),
        vec![
            vec![V::Int(0), V::Int(8)],
            vec![V::Int(1), V::Int(-1)],
            vec![V::Int(1), V::Int(3)],
            vec![V::Int(1), V::Int(7)],
            vec![V::Int(1), V::Int(9)],
            vec![V::Int(2), V::Int(9)]
        ]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT * FROM messages WHERE tenant=1 AND bucket=1 AND id=3"
        )
        .rows
        .len(),
        1
    );
    run(&mut s, "BEGIN");
    for q in [
        "UPDATE messages SET body='z' WHERE tenant=1",
        "DELETE FROM messages WHERE tenant=1 AND bucket=1",
    ] {
        assert!(run(&mut s, q).affected > 0, "{q}");
    }
    run(&mut s, "ROLLBACK");
    for q in [
        "SELECT * FROM messages WHERE id=3 LIMIT 5",
        "SELECT * FROM messages WHERE tenant=1 AND id=3 LIMIT 5",
    ] {
        assert_eq!(
            values(run(&mut s, q)),
            vec![vec![V::Int(1), V::Int(1), V::Int(3), V::Text("x".into())]]
        );
    }
    assert_eq!(
        values(run(&mut s, "SELECT * FROM messages WHERE body='x' LIMIT 3")),
        vec![
            vec![V::Int(1), V::Int(0), V::Int(8), V::Text("x".into())],
            vec![V::Int(1), V::Int(1), V::Int(-1), V::Text("x".into())],
            vec![V::Int(1), V::Int(1), V::Int(3), V::Text("x".into())],
        ]
    );
    run(&mut s, "CREATE TABLE strings (id TEXT PRIMARY KEY)");
    for v in ["é", "a\0z", "", "a", "a\0", "aa", "雪"] {
        s.execute("INSERT INTO strings VALUES($1)", &[V::Text(v.into())], None)
            .unwrap();
    }
    assert_eq!(
        values(run(&mut s, "SELECT * FROM strings LIMIT 20")),
        ["", "a", "a\0", "a\0z", "aa", "é", "雪"]
            .iter()
            .map(|v| vec![V::Text((*v).into())])
            .collect::<Vec<_>>()
    );
    run(&mut s, "CREATE TABLE binary (id BYTES PRIMARY KEY)");
    for b in [vec![255], vec![0, 1], vec![], vec![0], vec![1], vec![0, 0]] {
        s.execute("INSERT INTO binary VALUES($1)", &[V::Bytes(b)], None)
            .unwrap();
    }
    assert_eq!(
        values(run(&mut s, "SELECT * FROM binary LIMIT 20")),
        [vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![255]]
            .into_iter()
            .map(|b| vec![V::Bytes(b)])
            .collect::<Vec<_>>()
    );
}
#[test]
fn explicit_abort_and_rollback_never_partially_commit() {
    let (_, a, mut s) = db();
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE TABLE tx (id BIGINT PRIMARY KEY)");
    run(&mut s, "INSERT INTO tx VALUES(1)");
    let mut other = a.sql_session();
    err(&mut other, "SELECT * FROM tx LIMIT 2", E::UnknownTable);
    err(&mut s, "INSERT INTO tx VALUES(1)", E::Duplicate);
    assert!(s.is_aborted());
    err(&mut s, "COMMIT", E::Aborted);
    err(&mut s, "SELECT * FROM tx LIMIT 2", E::Aborted);
    run(&mut s, "ROLLBACK");
    assert!(!s.in_transaction());
    assert!(a.catalog().unwrap().1.is_empty());
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE TABLE tx (id BIGINT PRIMARY KEY)");
    run(&mut s, "INSERT INTO tx VALUES(2)");
    assert_eq!(values(run(&mut s, "SELECT * FROM tx LIMIT 5")), ints(&[2]));
    run(&mut s, "COMMIT");
    assert_eq!(
        values(run(&mut other, "SELECT * FROM tx LIMIT 5")),
        ints(&[2])
    );
    run(&mut s, "BEGIN");
    run(&mut s, "DELETE FROM tx WHERE id=2");
    run(&mut s, "ROLLBACK");
    assert_eq!(
        values(run(&mut other, "SELECT * FROM tx LIMIT 5")),
        ints(&[2])
    );
    run(&mut s, "BEGIN");
    err(&mut s, "BEGIN", E::Transaction);
    assert!(s.is_aborted());
    run(&mut s, "ROLLBACK");
    err(&mut s, "COMMIT", E::Transaction);
    run(&mut s, "BEGIN");
    err(&mut s, "SELECT * FROM tx; DELETE FROM tx", E::Unsupported);
    assert!(s.is_aborted());
    run(&mut s, "ROLLBACK");
    {
        let mut dropped = a.sql_session();
        run(&mut dropped, "BEGIN");
        run(&mut dropped, "DELETE FROM tx WHERE id=2");
    }
    assert_eq!(values(run(&mut s, "SELECT * FROM tx LIMIT 5")), ints(&[2]));
}
#[test]
fn prepared_bindings_stale_schema_and_restore_domain() {
    let (_, a, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE bind (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );
    let p = s.prepare("INSERT INTO bind VALUES($1,$2)").unwrap();
    assert_eq!(p.parameter_count(), 2);
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1)], None),
        Err(CoreError::Sql(E::Parameters))
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1), V::Null], None),
        Err(CoreError::Sql(E::Null))
    );
    s.execute_prepared(&p, &[V::Int(1), V::Text("one".into())], None)
        .unwrap();
    err(&mut s, "SELECT * FROM bind WHERE id=$2", E::Parameters);
    err(&mut s, "SELECT * FROM bind WHERE id=$0", E::Parameters);
    run(&mut s, "CREATE TABLE extra (id BOOLEAN PRIMARY KEY)");
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(2), V::Text("two".into())], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    let read = s.prepare("SELECT * FROM bind WHERE id=$1").unwrap();
    let file = fresh("sql-export");
    a.export(&file).unwrap();
    let r = Admin::restore(file, fresh("sql-restore"), Limits::default()).unwrap();
    assert_eq!(
        r.sql_session().execute_prepared(&read, &[V::Int(1)], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
}
#[test]
fn row_version_cas_aba_and_reopen_export() {
    let (path, a, mut s) = db();
    run(&mut s, "CREATE TABLE cas (id BIGINT PRIMARY KEY, n BIGINT)");
    run(&mut s, "INSERT INTO cas VALUES(1,10)");
    let first = run(&mut s, "SELECT * FROM cas WHERE id=1").rows[0].version;
    assert_eq!(
        s.execute("UPDATE cas SET n=20 WHERE id=1", &[], Some(first + 1)),
        Err(CoreError::Sql(E::Version))
    );
    s.execute("UPDATE cas SET n=20 WHERE id=1", &[], Some(first))
        .unwrap();
    let second = run(&mut s, "SELECT * FROM cas WHERE id=1").rows[0].version;
    assert!(second > first);
    s.execute("DELETE FROM cas WHERE id=1", &[], Some(second))
        .unwrap();
    run(&mut s, "INSERT INTO cas VALUES(1,30)");
    let third = run(&mut s, "SELECT * FROM cas WHERE id=1").rows[0].version;
    assert!(third > second);
    assert_eq!(
        s.execute("DELETE FROM cas WHERE id=1", &[], Some(second)),
        Err(CoreError::Sql(E::Version))
    );
    let catalog = a.catalog().unwrap();
    let file = fresh("sql-cas-export");
    a.export(&file).unwrap();
    drop(s);
    drop(a);
    let a = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(a.catalog().unwrap(), catalog);
    assert_eq!(
        run(&mut a.sql_session(), "SELECT * FROM cas WHERE id=1").rows[0],
        SqlRow {
            values: vec![V::Int(1), V::Int(30)],
            version: third
        }
    );
    let restored = Admin::restore(file, fresh("sql-cas-restored"), Limits::default()).unwrap();
    assert_eq!(restored.catalog().unwrap(), catalog);
    assert_eq!(
        run(&mut restored.sql_session(), "SELECT * FROM cas WHERE id=1").rows[0].version,
        third
    );
    let mut s = restored.sql_session();
    run(&mut s, "UPDATE cas SET n=40 WHERE id=1");
    assert!(run(&mut s, "SELECT * FROM cas WHERE id=1").rows[0].version > third);
}
#[test]
fn strict_rejection_matrix_and_resource_bounds() {
    let (_, _, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE safe (id BIGINT PRIMARY KEY, n BIGINT)",
    );
    for q in [
        "UPDATE safe SET n=1",
        "DELETE FROM safe",
    ] {
        assert_eq!(run(&mut s, q).affected, 0, "{q}");
    }
    run(&mut s, "CREATE TABLE nopk (id BIGINT)");
    for q in [
        "CREATE TABLE deliveries (id BIGINT PRIMARY KEY)",
        "CREATE TABLE giggles_state (id BIGINT PRIMARY KEY)",
        "CREATE TABLE pg_catalog (id BIGINT PRIMARY KEY)",
        "CREATE TABLE system (id BIGINT PRIMARY KEY)",
    ] {
        err(&mut s, q, E::Reserved);
    }
    assert!(
        run(&mut s, "SELECT * FROM public.safe LIMIT 1")
            .rows
            .is_empty()
    );
    run(&mut s, "CREATE TABLE decimals (id DECIMAL PRIMARY KEY)");
    for q in [
        "CREATE TABLE timestamps (id TIMESTAMPTZ PRIMARY KEY)",
        "SELECT * FROM safe; DELETE FROM safe WHERE id=1",
        "SELECT * FROM safe WHERE id=(SELECT id FROM safe)",
        "WITH x AS (DELETE FROM safe) SELECT * FROM x",
        "SELECT * FROM safe LIMIT 1 OFFSET 1",
    ] {
        err(&mut s, q, E::Unsupported);
    }
    err(
        &mut s,
        "CREATE TABLE dup (id BIGINT, id TEXT, PRIMARY KEY(id))",
        E::DuplicateColumn,
    );
    for q in [
        "CREATE TABLE dup (id BIGINT, PRIMARY KEY(id,id))",
        "INSERT INTO safe (id,id) VALUES(1,2)",
    ] {
        err(&mut s, q, E::Duplicate);
    }
    assert!(run(&mut s, "SELECT * FROM safe ORDER BY n LIMIT 1").rows.is_empty());
    assert!(run(&mut s, "SELECT * FROM safe").rows.is_empty());
    assert_eq!(
        run(&mut s, "SELECT * FROM safe -- hi").rows,
        run(&mut s, "SELECT * FROM safe").rows
    );
    assert!(run(&mut s, "SELECT * FROM safe LIMIT 1025").rows.is_empty());
    err(&mut s, "SELECT * FROM safe LIMIT -1", E::Limit);
    assert!(run(&mut s, "SELECT * FROM safe LIMIT 0").rows.is_empty());
    // Table aliases are now part of the bounded query contract.
    assert!(run(&mut s, "SELECT * FROM safe s LIMIT 1").rows.is_empty());
    assert_eq!(
        s.execute(
            "INSERT INTO safe VALUES($1,$2)",
            &[V::Int(1), V::Text("x".repeat(MAX_ROW_BYTES + 1))],
            None
        ),
        Err(CoreError::Sql(E::Limit))
    );
    assert_eq!(
        s.prepare(&" ".repeat(MAX_SQL + 1)).unwrap_err(),
        CoreError::Sql(E::Limit)
    );
    assert!(run(&mut s, "SELECT * FROM safe WHERE id=1").rows.is_empty());
    assert!(
        run(&mut s, "SELECT * FROM safe WHERE id=1 OR id=2")
            .rows
            .is_empty()
    );
    run(&mut s, "INSERT INTO safe VALUES(1,10)");
    run(&mut s, "INSERT INTO safe VALUES(2,20)");
    run(&mut s, "INSERT INTO safe VALUES(3,30)");
    assert_eq!(
        run(&mut s, "SELECT id FROM safe WHERE id=1 OR id=2 ORDER BY id")
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![vec![V::Int(1)], vec![V::Int(2)]]
    );
}
#[test]
fn shared_repair_generation_conflicts_and_authority_stays_finite() {
    let (_, a, e) = fixture(1);
    let mut s = a.sql_session();
    run(&mut s, "CREATE TABLE app (id BIGINT PRIMARY KEY)");
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO app VALUES(1)");
    e.execute(request(1, &[0]), Time::Certified(10), None)
        .unwrap()
        .unwrap();
    err(&mut s, "COMMIT", E::Conflict);
    assert!(s.is_aborted());
    run(&mut s, "ROLLBACK");
    assert!(run(&mut s, "SELECT * FROM app LIMIT 10").rows.is_empty());
    assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    for sql in [
        "CREATE TABLE x (id BIGINT PRIMARY KEY)",
        "BEGIN",
        "UPDATE app SET id=2 WHERE id=1",
        "UPDATE deliveries d SET status='queued' WHERE tenant=42 AND id=0",
        "UPDATE deliveries SET status='queued' WHERE tenant=42 AND id=0; COMMIT",
    ] {
        assert_eq!(
            e.execute_sql(OperationId(9), sql, Time::Certified(11), None),
            Err(CoreError::Unsupported)
        );
    }
}
#[test]
fn real_concurrent_snapshots_one_commit_and_no_write_skew() {
    use std::sync::{Arc, Barrier};
    let (_, a, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE seats (id BIGINT PRIMARY KEY, n BIGINT)",
    );
    run(&mut s, "INSERT INTO seats VALUES(1,0)");
    run(&mut s, "INSERT INTO seats VALUES(2,0)");
    let barrier = Arc::new(Barrier::new(8));
    let mut joins = vec![];
    for n in 0..8 {
        let a = a.clone();
        let b = barrier.clone();
        joins.push(std::thread::spawn(move || {
            let mut s = a.sql_session();
            run(&mut s, "BEGIN");
            assert_eq!(
                values(run(&mut s, "SELECT n FROM seats LIMIT 2")),
                ints(&[0, 0])
            );
            s.execute(
                "UPDATE seats SET n=$1 WHERE id=$2",
                &[V::Int(n + 1), V::Int(n % 2 + 1)],
                None,
            )
            .unwrap();
            b.wait();
            s.execute("COMMIT", &[], None)
        }));
    }
    let results = joins
        .into_iter()
        .map(|j| j.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(CoreError::Sql(E::Conflict)))
            .count(),
        7
    );
    let r = values(run(&mut s, "SELECT n FROM seats LIMIT 2"));
    assert_eq!(r.iter().filter(|v| v[0] != V::Int(0)).count(), 1);
}
#[test]
fn generated_histories_against_independent_integer_map() {
    use sha2::{Digest, Sha256};
    // The oracle uses signed numeric keys, not engine key encoding or SQL evaluation.
    for seed in 1u64..=20 {
        let (path, _, mut s) = db();
        run(
            &mut s,
            "CREATE TABLE generated (id BIGINT PRIMARY KEY, n BIGINT)",
        );
        let mut model = std::collections::BTreeMap::<i64, i64>::new();
        let mut rng = seed;
        let mut trace = String::new();
        for step in 0..100 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = ((rng >> 32) % 31) as i64 - 15;
            let n = step as i64;
            let action = rng % 4;
            trace.push_str(&format!(
                "seed={seed} step={step} action={action} key={k} value={n}\n"
            ));
            match action {
                0 => {
                    let expected =
                        if let std::collections::btree_map::Entry::Vacant(e) = model.entry(k) {
                            e.insert(n);
                            Ok(1)
                        } else {
                            Err(CoreError::Sql(E::Duplicate))
                        };
                    assert_eq!(
                        s.execute(
                            "INSERT INTO generated VALUES($1,$2)",
                            &[V::Int(k), V::Int(n)],
                            None
                        )
                        .map(|r| r.affected),
                        expected,
                        "{trace}"
                    );
                }
                1 => {
                    let expected = if let Some(v) = model.get_mut(&k) {
                        *v = n;
                        1
                    } else {
                        0
                    };
                    assert_eq!(
                        s.execute(
                            "UPDATE generated SET n=$1 WHERE id=$2",
                            &[V::Int(n), V::Int(k)],
                            None
                        )
                        .unwrap()
                        .affected,
                        expected
                    );
                }
                2 => {
                    let expected = u64::from(model.remove(&k).is_some());
                    assert_eq!(
                        s.execute("DELETE FROM generated WHERE id=$1", &[V::Int(k)], None)
                            .unwrap()
                            .affected,
                        expected
                    );
                }
                _ => {
                    run(&mut s, "BEGIN");
                    s.execute("DELETE FROM generated WHERE id=$1", &[V::Int(k)], None)
                        .unwrap();
                    run(&mut s, "ROLLBACK");
                }
            }
            let expected = model
                .iter()
                .map(|(k, v)| vec![V::Int(*k), V::Int(*v)])
                .collect::<Vec<_>>();
            assert_eq!(
                values(run(&mut s, "SELECT * FROM generated LIMIT 100")),
                expected,
                "{trace}"
            );
        }
        std::fs::write(path.join("sql-seed.trace"), &trace).unwrap();
        std::fs::write(
            path.join("sql-seed.sha256"),
            format!("{:x}", Sha256::digest(trace.as_bytes())),
        )
        .unwrap();
    }
}
#[test]
fn bounded_native_scan_matches_expected_composite_results() {
    let (_, _, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE native_rows (tenant BIGINT, key TEXT, n BIGINT, PRIMARY KEY(tenant,key))",
    );
    for (tenant, key, n) in [(1, "a", 2), (1, "b", 3), (1, "c", 5), (2, "b", 7)] {
        s.execute(
            "INSERT INTO native_rows VALUES($1,$2,$3)",
            &[V::Int(tenant), V::Text(key.into()), V::Int(n)],
            None,
        )
        .unwrap();
    }
    let scan = Scan {
        table: "native_rows".into(),
        projection: vec!["key".into(), "n".into()],
        prefix: vec![V::Int(1)],
        lower: Some((V::Text("a".into()), false)),
        upper: Some((V::Text("c".into()), true)),
        reverse: true,
        limit: 2,
    };
    assert_eq!(
        values(s.scan(scan.clone()).unwrap()),
        vec![
            vec![V::Text("c".into()), V::Int(5)],
            vec![V::Text("b".into()), V::Int(3)]
        ]
    );
    let mut invalid = scan.clone();
    invalid.limit = 1025;
    assert_eq!(s.scan(invalid), Err(CoreError::Sql(E::Limit)));
    let mut oversized = scan.clone();
    oversized.prefix = vec![V::Int(1), V::Text("x".repeat(MAX_ROW_BYTES + 1))];
    oversized.lower = None;
    oversized.upper = None;
    assert_eq!(s.scan(oversized), Err(CoreError::Sql(E::Limit)));
    let mut wide = scan.clone();
    wide.projection = vec!["n".into(); MAX_COLUMNS + 1];
    assert_eq!(s.scan(wide), Err(CoreError::Sql(E::Limit)));
    let mut empty = scan;
    empty.lower = Some((V::Text("z".into()), true));
    assert!(s.scan(empty).unwrap().rows.is_empty());
}
#[test]
fn transaction_slots_released_on_rollback_drop_commit_and_abort() {
    let (_, a, mut main) = db();
    run(&mut main, "CREATE TABLE slots (id BIGINT PRIMARY KEY)");
    let mut sessions = (0..MAX_TRANSACTIONS)
        .map(|_| {
            let mut s = a.sql_session();
            run(&mut s, "BEGIN");
            s
        })
        .collect::<Vec<_>>();
    assert_eq!(main.execute("BEGIN", &[], None), Err(CoreError::Busy));
    assert!(!main.in_transaction());
    run(&mut sessions[0], "ROLLBACK");
    run(&mut main, "BEGIN");
    run(&mut main, "COMMIT");
    // Dropping one transaction frees admission; existing snapshots hold no mutex.
    drop(sessions.pop());
    run(&mut main, "BEGIN");
    err(&mut main, "SELECT * FROM missing_table", E::UnknownTable);
    assert!(main.is_aborted());
    run(&mut main, "ROLLBACK");
    run(&mut main, "BEGIN");
    run(&mut main, "ROLLBACK");
    drop(sessions);
    run(&mut main, "INSERT INTO slots VALUES(1)");
    assert_eq!(
        values(run(&mut main, "SELECT * FROM slots LIMIT 5")),
        ints(&[1])
    );
}
#[test]
fn boolean_pk_result_bytes_and_statement_rollback_bounds() {
    let (_, a, mut s) = db();
    run(&mut s, "CREATE TABLE bools (id BOOLEAN PRIMARY KEY)");
    run(&mut s, "INSERT INTO bools VALUES(true)");
    run(&mut s, "INSERT INTO bools VALUES(false)");
    assert_eq!(
        values(run(&mut s, "SELECT * FROM bools LIMIT 2")),
        vec![vec![V::Bool(false)], vec![V::Bool(true)]]
    );
    run(
        &mut s,
        "CREATE TABLE pages (id BIGINT PRIMARY KEY, body TEXT)",
    );
    let body = "x".repeat(60000);
    let wide = format!(
        "SELECT {} FROM pages LIMIT 1",
        vec!["body"; MAX_COLUMNS + 1].join(",")
    );
    err(&mut s, &wide, E::Limit);
    run(&mut s, "BEGIN");
    for n in 0..18 {
        s.execute(
            "INSERT INTO pages VALUES($1,$2)",
            &[V::Int(n), V::Text(body.clone())],
            None,
        )
        .unwrap();
    }
    run(&mut s, "COMMIT");
    err(&mut s, "SELECT * FROM pages LIMIT 18", E::Limit);
    assert_eq!(run(&mut s, "SELECT * FROM pages LIMIT 17").rows.len(), 17);
    run(&mut s, "BEGIN");
    s.execute(
        "UPDATE pages SET body=$1 WHERE id=0",
        &[V::Text("s".repeat(60000))],
        None,
    )
    .unwrap();
    err(&mut s, "SELECT * FROM pages LIMIT 18", E::Limit);
    assert!(s.is_aborted());
    run(&mut s, "ROLLBACK");
    assert_eq!(
        run(&mut a.sql_session(), "SELECT body FROM pages WHERE id=0").rows[0].values,
        vec![V::Text(body)]
    );
}
#[test]
fn prepared_control_handles_are_schema_and_domain_fenced() {
    let (_, a, mut s) = db();
    let begin = s.prepare("BEGIN").unwrap();
    run(&mut s, "CREATE TABLE new_schema (id BIGINT PRIMARY KEY)");
    assert_eq!(
        s.execute_prepared(&begin, &[], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    assert!(!s.in_transaction());
    let file = fresh("control-export");
    a.export(&file).unwrap();
    let restored = Admin::restore(file, fresh("control-restored"), Limits::default()).unwrap();
    let begin = s.prepare("BEGIN").unwrap();
    assert_eq!(
        restored.sql_session().execute_prepared(&begin, &[], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
}
#[test]
fn rolled_back_catalog_prepares_cannot_rebind_reused_epoch_or_ids() {
    let (_, a, mut s) = db();
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE TABLE branch (id BIGINT PRIMARY KEY)");
    let abandoned = s.prepare("INSERT INTO branch VALUES(7)").unwrap();
    run(&mut s, "ROLLBACK");
    let mut other = a.sql_session();
    run(&mut other, "CREATE TABLE branch (id BIGINT PRIMARY KEY)");
    assert_eq!(
        s.execute_prepared(&abandoned, &[], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    assert!(
        run(&mut other, "SELECT * FROM branch LIMIT 5")
            .rows
            .is_empty()
    );
    run(&mut s, "BEGIN");
    let scoped = s.prepare("INSERT INTO branch VALUES(8)").unwrap();
    assert_eq!(
        other.execute_prepared(&scoped, &[], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    run(&mut s, "COMMIT");
    assert_eq!(
        s.execute_prepared(&scoped, &[], None),
        Err(CoreError::Sql(E::StaleSchema))
    );
    let durable = s.prepare("INSERT INTO branch VALUES(9)").unwrap();
    run(&mut s, "BEGIN");
    s.execute_prepared(&durable, &[], None).unwrap();
    run(&mut s, "COMMIT");
    assert_eq!(
        values(run(&mut other, "SELECT * FROM branch LIMIT 5")),
        ints(&[9])
    );
}
