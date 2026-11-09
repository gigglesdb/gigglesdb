// ORDER BY <integer> names a select-list output, as in PostgreSQL 18.6.
use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*};
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
        let path = std::env::temp_dir().join(format!(
            "sql-text-order-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
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
fn rows(s: &mut SqlSession, sql: &str) -> Vec<String> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|v| match v {
                    Value::Text(t) => t.clone(),
                    Value::Int(n) => n.to_string(),
                    v => panic!("{sql}: {v:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

// Expected results captured from PostgreSQL 18.6 (J:/gigglesdb-work/streamK/pg18.txt).
// '10' sorts before '5' in both en_US.utf8 and C collation.
#[test]
fn order_by_ordinal_names_select_list_output() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    for sql in [
        "CREATE TABLE t (i integer PRIMARY KEY, tx text, v varchar(10))",
        "INSERT INTO t VALUES (5,'5','5')",
        "INSERT INTO t VALUES (7,'10','10')",
        "CREATE TABLE n (i integer PRIMARY KEY, tx text)",
        "INSERT INTO n VALUES (1,'5')",
        "INSERT INTO n VALUES (2,'10')",
        "CREATE INDEX nix ON n (tx)",
    ] {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT tx FROM t WHERE tx < '6' ORDER BY 1", &["10", "5"]),
        ("SELECT tx FROM t WHERE tx < '6' ORDER BY tx", &["10", "5"]),
        ("SELECT tx FROM t ORDER BY 1", &["10", "5"]),
        ("SELECT tx FROM t ORDER BY 1 DESC", &["5", "10"]),
        ("SELECT tx FROM t ORDER BY tx DESC", &["5", "10"]),
        ("SELECT tx FROM t ORDER BY 1 LIMIT 1", &["10"]),
        ("SELECT v FROM t ORDER BY 1", &["10", "5"]),
        ("SELECT v FROM t WHERE v < '6' ORDER BY 1", &["10", "5"]),
        ("SELECT tx, i FROM t ORDER BY 1", &["10|7", "5|5"]),
        ("SELECT i, tx FROM t ORDER BY 2", &["7|10", "5|5"]),
        ("SELECT * FROM t ORDER BY 2", &["7|10|10", "5|5|5"]),
        ("SELECT * FROM t ORDER BY 3 DESC", &["5|5|5", "7|10|10"]),
        ("SELECT tx || 'a' FROM t ORDER BY 1", &["10a", "5a"]),
        ("SELECT tx FROM n ORDER BY 1", &["10", "5"]),
        ("SELECT tx FROM n WHERE tx < '6' ORDER BY 1", &["10", "5"]),
    ];
    let mut mismatches = vec![];
    for (sql, want) in cases {
        let got = rows(&mut s, sql);
        if got != *want {
            mismatches.push(format!("{sql}: got {got:?}, want {want:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    // PG18 reports 42P10 (ORDER BY position N is not in select list); GigglesDB
    // keeps the grouped path's existing UnknownColumn for an out-of-range ordinal.
    for sql in [
        "SELECT tx FROM t ORDER BY 2",
        "SELECT tx FROM t ORDER BY 0",
    ] {
        assert!(
            matches!(
                s.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::UnknownColumn))
            ),
            "{sql}"
        );
    }
}
