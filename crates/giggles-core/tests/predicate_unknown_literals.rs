// Quoted (unknown) literals compared with a typed column resolve to the
// column's type through its input function, as in PostgreSQL 18.6.
use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*};
use giggles_pg_types::temporal::SessionZone;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from("J:/gigglesdb-work/streamG/tmp").join(format!(
            "predicate-unknown-literals-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    }
}
fn rows(s: &mut SqlSession, sql: &str) -> Result<Vec<String>, CoreError> {
    let result = s.execute_at(sql, &[], None, &clock())?;
    Ok(result
        .rows
        .iter()
        .map(|r| match &r.values[0] {
            Value::Text(t) => t.clone(),
            v => panic!("{sql}: non-text {v:?}"),
        })
        .collect())
}
fn setup(s: &mut SqlSession) {
    for sql in [
        "CREATE TABLE t (i integer PRIMARY KEY, b bigint, n numeric, f boolean, d date, \
         ts timestamp, tz timestamptz, tx text, v varchar(10))",
        "INSERT INTO t VALUES (5,50,1.5,true,'2024-01-01','2024-01-01 10:00',\
         '2024-01-01 10:00+00','5','5')",
        "INSERT INTO t VALUES (7,70,2.5,false,'2024-02-01','2023-12-31 00:00',\
         '2023-06-01 00:00+00','10','10')",
    ] {
        s.execute_at(sql, &[], None, &clock())
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

// Expected results captured from PostgreSQL 18.6 (J:/gigglesdb-work/streamG/pg18.txt).
#[test]
fn predicate_unknown_literals_match_pg18() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    setup(&mut s);
    let mut mismatches = vec![];
    for (sql, expected) in [
        ("SELECT i::text FROM t WHERE i = '5' ORDER BY 1", &["5"][..]),
        ("SELECT i::text FROM t WHERE i <> '5' ORDER BY 1", &["7"]),
        ("SELECT i::text FROM t WHERE '6' < i ORDER BY 1", &["7"]),
        ("SELECT i::text FROM t WHERE i = ' 5 ' ORDER BY 1", &["5"]),
        (
            "SELECT i::text FROM t WHERE i BETWEEN '4' AND '6' ORDER BY 1",
            &["5"],
        ),
        (
            "SELECT i::text FROM t WHERE i IS DISTINCT FROM '5' ORDER BY 1",
            &["7"],
        ),
        (
            "SELECT i::text FROM t WHERE i NOT IN ('5','8') ORDER BY 1",
            &["7"],
        ),
        (
            "SELECT i::text FROM t WHERE i IN ('5','8') ORDER BY 1",
            &["5"],
        ),
        ("SELECT b::text FROM t WHERE b >= '60' ORDER BY 1", &["70"]),
        (
            "SELECT n::text FROM t WHERE n BETWEEN '1' AND '2.5' ORDER BY 1",
            &["1.5", "2.5"],
        ),
        (
            "SELECT n::text FROM t WHERE n = '1.50' ORDER BY 1",
            &["1.5"],
        ),
        ("SELECT f::text FROM t WHERE f = 't' ORDER BY 1", &["true"]),
        (
            "SELECT f::text FROM t WHERE f = 'off' ORDER BY 1",
            &["false"],
        ),
        (
            "SELECT d::text FROM t WHERE d IN ('2024-01-01','2024-02-01') ORDER BY 1",
            &["2024-01-01", "2024-02-01"],
        ),
        (
            "SELECT d::text FROM t WHERE d > '2024-01-15' ORDER BY 1",
            &["2024-02-01"],
        ),
        (
            "SELECT ts::text FROM t WHERE ts > '2024-01-01' ORDER BY 1",
            &["2024-01-01 10:00:00"],
        ),
        (
            "SELECT tz::text FROM t WHERE tz < '2024-01-01' ORDER BY 1",
            &["2023-06-01 00:00:00+00"],
        ),
        ("SELECT tx FROM t WHERE tx = '5' ORDER BY 1", &["5"]),
        ("SELECT tx FROM t WHERE tx > '4' ORDER BY 1", &["5"]),
        ("SELECT v::text FROM t WHERE v = '10' ORDER BY 1", &["10"]),
    ] {
        let expected: Vec<String> = expected.iter().map(|v| v.to_string()).collect();
        let actual = rows(&mut s, sql);
        if actual.as_ref() != Ok(&expected) {
            mismatches.push(format!("{sql}: {actual:?} != {expected:?}"));
        }
    }
    for (sql, expected) in [
        (
            "SELECT i FROM t WHERE i = 'x'",
            SqlError::InvalidTextRepresentation,
        ),
        (
            "SELECT i FROM t WHERE i = '1.5'",
            SqlError::InvalidTextRepresentation,
        ),
        (
            "SELECT f FROM t WHERE f = 'maybe'",
            SqlError::InvalidTextRepresentation,
        ),
        (
            "SELECT d FROM t WHERE d = 'notadate'",
            SqlError::InvalidDatetimeFormat,
        ),
        (
            "SELECT i FROM t WHERE i = '99999999999'",
            SqlError::Overflow,
        ),
    ] {
        let actual = s.execute_at(sql, &[], None, &clock()).map(|_| ());
        if actual != Err(CoreError::Sql(expected.clone())) {
            mismatches.push(format!("{sql}: {actual:?} != {expected:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{}",
        mismatches.join(
            "
"
        )
    );
}

#[test]
fn update_and_delete_predicates_resolve_unknown_literals() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    setup(&mut s);
    for sql in [
        "UPDATE t SET tx = 'u' WHERE i = '5'",
        "UPDATE t SET tx = 'w' WHERE n >= '2.5'",
        "DELETE FROM t WHERE d = '2024-02-01'",
    ] {
        s.execute_at(sql, &[], None, &clock())
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    assert_eq!(rows(&mut s, "SELECT tx FROM t ORDER BY i").unwrap(), ["u"]);
    assert_eq!(
        s.execute_at("DELETE FROM t WHERE i = 'x'", &[], None, &clock())
            .map(|_| ()),
        Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
    );
}
