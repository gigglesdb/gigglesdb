mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{DataType as T, SqlError as E, SqlResult, SqlRow, Value as V},
};

fn database(test: impl FnOnce(&Admin, &mut SqlSession)) {
    let path = common::fresh("relational-searched-dml");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    test(&admin, &mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None).expect(sql)
}

fn rows(s: &mut SqlSession) -> Vec<SqlRow> {
    run(s, "SELECT * FROM t ORDER BY a").rows
}

fn seed(s: &mut SqlSession) {
    run(s, "CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)");
    run(s, "INSERT INTO t VALUES (4,40),(2,20),(1,10),(3,30)");
}

#[test]
fn searched_updates_change_exact_matches_and_only_their_versions() {
    for (predicate, matched) in [
        ("a > 1 AND a <= 3", vec![2, 3]),
        ("b >= 30", vec![3, 4]),
        ("a=1 OR b=40", vec![1, 4]),
        ("a IN (1,3)", vec![1, 3]),
        ("a BETWEEN 2 AND 3", vec![2, 3]),
        ("a <> 2", vec![1, 3, 4]),
    ] {
        database(|_, s| {
            seed(s);
            let before = rows(s);
            let result = run(s, &format!("UPDATE t SET b=b+100 WHERE {predicate}"));
            assert_eq!(result.affected, matched.len() as u64, "{predicate}");
            let after = rows(s);
            assert_eq!(after.len(), before.len());
            for (old, new) in before.iter().zip(&after) {
                let V::Int(key) = old.values[0] else { panic!("integer key") };
                if matched.contains(&key) {
                    assert_eq!(new.values, [V::Int(key), V::Int(key * 10 + 100)]);
                    assert!(new.version > old.version, "{predicate}: {key}");
                } else {
                    assert_eq!(new, old, "{predicate}: {key}");
                }
            }
        });
    }
}

#[test]
fn unqualified_mutations_visit_all_rows_and_point_mutations_still_work() {
    database(|_, s| {
        seed(s);
        assert_eq!(run(s, "UPDATE t SET b=b+1").affected, 4);
        assert_eq!(rows(s).iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
            (1..=4).map(|a| vec![V::Int(a), V::Int(a*10+1)]).collect::<Vec<_>>());
        assert_eq!(run(s, "UPDATE t SET b=99 WHERE a=2").affected, 1);
        assert_eq!(rows(s)[1].values, [V::Int(2), V::Int(99)]);
        assert_eq!(run(s, "DELETE FROM t WHERE a=3").affected, 1);
        assert_eq!(rows(s).iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
            [V::Int(1), V::Int(2), V::Int(4)]);
        assert_eq!(run(s, "DELETE FROM t").affected, 3);
        assert!(rows(s).is_empty());
    });
}

#[test]
fn later_row_errors_and_cross_row_unique_conflicts_are_atomic() {
    for (schema, inserts, sql, expected) in [
        ("v BIGINT", "(1,1),(2,10000000)", "UPDATE t SET v=v*1000000000000 WHERE a>0", E::Overflow),
        ("d BIGINT, v BIGINT", "(1,2,7),(2,0,8)", "UPDATE t SET v=10/d WHERE a>0", E::DivisionByZero),
        ("v BIGINT NOT NULL, d BIGINT", "(1,7,9),(2,8,NULL)", "UPDATE t SET v=d WHERE a>0", E::Null),
        ("v BIGINT", "(1,7),(2,8)", "UPDATE t SET v=99 WHERE a>0", E::Duplicate),
    ] {
        database(|_, s| {
            run(s, &format!("CREATE TABLE t (a BIGINT PRIMARY KEY, {schema})"));
            run(s, &format!("INSERT INTO t VALUES {inserts}"));
            if expected == E::Duplicate {
                run(s, "CREATE UNIQUE INDEX unique_v ON t (v)");
            }
            let before = rows(s);
            assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(expected)), "{sql}");
            assert_eq!(rows(s), before, "all values and versions: {sql}");
        });
    }
}

#[test]
fn returning_uses_key_order_update_postimages_and_delete_preimages() {
    database(|_, s| {
        seed(s);
        let before = rows(s);
        let updated = run(s, "UPDATE t SET b=b+1 WHERE a>=2 RETURNING a,b");
        assert_eq!(updated.affected, 3);
        assert_eq!(updated.rows, rows(s)[1..]);
        for (new, old) in updated.rows.iter().zip(&before[1..]) {
            assert!(new.version > old.version);
        }
        let preimages = rows(s);
        let deleted = run(s, "DELETE FROM t WHERE a>=2 RETURNING a,b");
        assert_eq!(deleted.affected, 3);
        assert_eq!(deleted.rows, preimages[1..]);
        assert_eq!(rows(s), preimages[..1]);
    });
}

#[test]
fn searched_parameter_descriptions_and_execution_agree() {
    database(|_, s| {
        run(s, "CREATE TABLE t (a BIGINT PRIMARY KEY, b TEXT)");
        run(s, "INSERT INTO t VALUES (1,'one'),(2,'two'),(3,'three')");
        let combined = s.prepare(
            "UPDATE t SET b=upper($1) WHERE a>$2 RETURNING a,b,upper($3) AS label",
        ).unwrap();
        assert_eq!(s.describe(&combined).unwrap().parameters, [T::Text, T::BigInt, T::Text]);
        run(s, "BEGIN");
        let result = s.execute_prepared(
            &combined,
            &[V::Text("value".into()), V::Int(1), V::Text("done".into())],
            None,
        ).unwrap();
        assert_eq!(result.affected, 2);
        assert_eq!(result.columns, ["a", "b", "label"]);
        assert_eq!(result.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(), [
            vec![V::Int(2), V::Text("VALUE".into()), V::Text("DONE".into())],
            vec![V::Int(3), V::Text("VALUE".into()), V::Text("DONE".into())],
        ]);
        run(s, "ROLLBACK");
        let conflict = s.prepare("UPDATE t SET b=upper($1) WHERE a>$1").unwrap();
        assert_eq!(s.describe(&conflict).unwrap_err(), CoreError::Sql(E::Type));
        assert_eq!(s.execute_prepared(&conflict, &[V::Text("value".into())], None), Err(CoreError::Sql(E::Type)));
        let unresolved = s.prepare("DELETE FROM t WHERE $1=$1").unwrap();
        assert_eq!(s.describe(&unresolved).unwrap_err(), CoreError::Sql(E::Parameters));
        assert_eq!(s.execute_prepared(&unresolved, &[V::Int(1)], None), Err(CoreError::Sql(E::Parameters)));
        let update = s.prepare("UPDATE t SET b=$1 WHERE a>$2").unwrap();
        assert_eq!(s.describe(&update).unwrap().parameters, [T::Text, T::BigInt]);
        assert_eq!(s.execute_prepared(&update, &[V::Text("new".into()), V::Int(1)], None).unwrap().affected, 2);
        assert_eq!(rows(s).iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
            [vec![V::Int(1), V::Text("one".into())], vec![V::Int(2), V::Text("new".into())], vec![V::Int(3), V::Text("new".into())]]);
        let delete = s.prepare("DELETE FROM t WHERE b=$1 OR a<$2").unwrap();
        assert_eq!(s.describe(&delete).unwrap().parameters, [T::Text, T::BigInt]);
        assert_eq!(s.execute_prepared(&delete, &[V::Text("new".into()), V::Int(2)], None).unwrap().affected, 3);
        assert!(rows(s).is_empty());
    });
}

#[test]
fn heap_updates_preserve_row_identity_and_deletes_use_logical_predicates() {
    database(|_, s| {
        run(s, "CREATE TABLE t (a BIGINT, b TEXT)");
        run(s, "INSERT INTO t VALUES (3,'same'),(1,'same'),(3,'same')");
        let before = run(s, "SELECT * FROM t").rows;
        let updated = run(s, "UPDATE t SET b='changed' WHERE a=3 RETURNING a,b");
        assert_eq!(updated.affected, 2);
        let after = run(s, "SELECT * FROM t").rows;
        assert_eq!(after.len(), 3);
        assert_eq!(after[1], before[1]);
        for i in [0, 2] {
            assert_eq!(after[i].values, [V::Int(3), V::Text("changed".into())]);
            assert!(after[i].version > before[i].version);
        }
        assert_eq!(updated.rows, [after[0].clone(), after[2].clone()]);
        assert_eq!(run(s, "SELECT * FROM t WHERE b='changed'").rows, updated.rows);
        let deleted = run(s, "DELETE FROM t WHERE b='changed' RETURNING a,b");
        assert_eq!(deleted.affected, 2);
        assert_eq!(deleted.rows, updated.rows);
        assert_eq!(run(s, "SELECT * FROM t").rows, [before[1].clone()]);
    });
}

#[test]
fn searched_update_uses_one_statement_clock_for_every_row() {
    use giggles_core::relational::StatementClock;
    use giggles_pg_types::temporal::{SessionZone, TimestampTz};
    database(|_, s| {
        run(s, "CREATE TABLE c (id BIGINT PRIMARY KEY, n BIGINT, at TIMESTAMPTZ)");
        run(s, "INSERT INTO c VALUES (1,1,NULL),(2,2,NULL),(3,3,NULL)");
        let clock = StatementClock {
            transaction_us: 1_000,
            statement_us: 2_000,
            zone: Some(SessionZone::Fixed(0)),
            wall: None,
        };
        let result = s
            .execute_at(
                "UPDATE c SET at = statement_timestamp() WHERE n >= 1 AND now() IS NOT NULL RETURNING at, now()",
                &[],
                None,
                &clock,
            )
            .unwrap();
        assert_eq!(result.affected, 3);
        let tz = |us| V::TimestampTz(TimestampTz::from_microseconds(us).unwrap());
        for row in &result.rows {
            assert_eq!(row.values, vec![tz(2_000), tz(1_000)]);
        }
        for row in run(s, "SELECT at FROM c").rows {
            assert_eq!(row.values, vec![tz(2_000)]);
        }
    });
}
