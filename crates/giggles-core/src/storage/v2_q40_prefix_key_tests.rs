//! GD-0084 Q40: SQL- and native-reachable layouts where two stored user keys
//! are prefix-related. `VersionedRange` bounds, forward/reverse order and the
//! `TxnScan` overlay merge are exact only over prefix-free key regions; each
//! test here pins one caller that must stay exact anyway.
use super::*;
use relational::Value;

fn count(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

/// A heap key `tid||rowid` is a proper prefix of the composite key
/// `tid||a||b` written by ADD PRIMARY KEY (a, b) when `a == rowid`. Inside the
/// DDL transaction the base still holds the old heap keys (deleted only in the
/// overlay), so a paged scan resuming after `tid||a||b` must not pull in the
/// old heap row whose rowid is `a`.
#[test]
fn add_primary_key_then_scan_in_the_same_transaction() {
    let f = Fixture::new();
    f.execute("CREATE TABLE h(a bigint NOT NULL, b bigint NOT NULL)");
    f.execute(
        "INSERT INTO h SELECT g, g FROM generate_series(1, 450) AS g",
    );
    let mut session = f.admin().sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("ALTER TABLE h ADD PRIMARY KEY (a, b)", &[], None)
        .unwrap();
    assert_eq!(
        count(&mut session, "SELECT count(*), count(DISTINCT a), sum(a)::bigint FROM h"),
        vec![vec![Value::Int(450), Value::Int(450), Value::Int(450 * 451 / 2)]]
    );
    session.execute("COMMIT", &[], None).unwrap();
    assert_eq!(
        count(&mut session, "SELECT count(*), count(DISTINCT a), sum(a)::bigint FROM h"),
        vec![vec![Value::Int(450), Value::Int(450), Value::Int(450 * 451 / 2)]]
    );
}

/// DROP PRIMARY KEY turns `tid||a||b` keys into heap keys `tid||rowid`, so the
/// new heap key is a proper prefix of an old composite key when `rowid == a`.
/// Inside the DDL transaction a point read of an unused rowid must not find the
/// old composite row that the overlay already deleted.
#[test]
fn drop_primary_key_then_insert_in_the_same_transaction() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(a bigint, b bigint, PRIMARY KEY (a, b))");
    f.execute("INSERT INTO p SELECT g, g FROM generate_series(11, 60) AS g");
    let mut session = f.admin().sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("ALTER TABLE p DROP CONSTRAINT p_pkey", &[], None)
        .unwrap();
    for g in 0..60 {
        session
            .execute(&format!("INSERT INTO p VALUES ({}, 0)", 1000 + g), &[], None)
            .unwrap_or_else(|e| panic!("insert {g}: {e:?}"));
    }
    for statement in [
        "DELETE FROM p WHERE a BETWEEN 11 AND 20",
        "UPDATE p SET b = b + 1 WHERE a BETWEEN 21 AND 30",
        "UPDATE p SET b = b + 1 WHERE a BETWEEN 21 AND 30",
        "DELETE FROM p WHERE a BETWEEN 1050 AND 1059",
    ] {
        session.execute(statement, &[], None).unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }
    let expected = vec![vec![
        Value::Int(90),
        Value::Int((21..=60).sum::<i64>() + (1000..1050).sum::<i64>()),
        Value::Int((21..=60).sum::<i64>() + 20),
    ]];
    let query = "SELECT count(*), sum(a)::bigint, sum(b)::bigint FROM p";
    assert_eq!(count(&mut session, query), expected);
    assert_eq!(
        count(&mut session, "SELECT a FROM p WHERE a < 1000 ORDER BY a DESC LIMIT 2"),
        vec![vec![Value::Int(60)], vec![Value::Int(59)]]
    );
    session.execute("COMMIT", &[], None).unwrap();
    assert_eq!(count(&mut session, query), expected);
}

/// DROP CONSTRAINT + ADD PRIMARY KEY (a) on a table keyed `(a, b)` writes the
/// short key `tid||a` while the base still holds the old `tid||a||b` (deleted
/// only in the overlay). A point range `[tid||5, tid||5]` physically ends at
/// `tid||5||[0xff; 8]||00`, which admits the old extension `tid||5||1`; the
/// overlay's exact range never reaches its delete. Point reads, searched DML
/// and a read after an in-transaction DELETE must see only the new key.
#[test]
fn narrow_primary_key_then_point_read_in_the_same_transaction() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(a bigint, b bigint, PRIMARY KEY (a, b))");
    f.execute("INSERT INTO t VALUES (5, 1), (6, 1), (7, 1)");
    let mut session = f.admin().sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    for statement in ["ALTER TABLE t DROP CONSTRAINT t_pkey", "ALTER TABLE t ADD PRIMARY KEY (a)"] {
        session.execute(statement, &[], None).unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }
    let point = |session: &mut SqlSession, a: i64| {
        count(session, &format!("SELECT count(*), coalesce(sum(b), 0)::bigint FROM t WHERE a = {a}"))
    };
    assert_eq!(point(&mut session, 5), vec![vec![Value::Int(1), Value::Int(1)]]);
    session.execute("UPDATE t SET b = 9 WHERE a = 5", &[], None).unwrap();
    assert_eq!(point(&mut session, 5), vec![vec![Value::Int(1), Value::Int(9)]]);
    session.execute("DELETE FROM t WHERE a = 6", &[], None).unwrap();
    assert_eq!(point(&mut session, 6), vec![vec![Value::Int(0), Value::Int(0)]]);
    let all = "SELECT a, b FROM t ORDER BY a";
    let expected = vec![
        vec![Value::Int(5), Value::Int(9)],
        vec![Value::Int(7), Value::Int(1)],
    ];
    assert_eq!(count(&mut session, all), expected);
    session.execute("COMMIT", &[], None).unwrap();
    assert_eq!(count(&mut session, all), expected);
    assert_eq!(point(&mut session, 6), vec![vec![Value::Int(0), Value::Int(0)]]);
}
