//! DD-0068 F5: quoted column identifiers resolve as in PostgreSQL 18.6
//! (quoted = exact case, unquoted = folded to lowercase) across CREATE INDEX,
//! INSERT column lists, SELECT list, WHERE, ORDER BY and UPDATE SET. Every
//! expectation was observed on postgres:18.6; UnknownColumn is PG 42703.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, SqlResult, Value as V},
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-sql-quoted-columns-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    test(&mut s);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
fn values(result: SqlResult) -> Vec<Vec<V>> {
    result.rows.into_iter().map(|r| r.values).collect()
}
fn setup(s: &mut SqlSession) {
    run(
        s,
        r#"CREATE TABLE t (id int primary key, "Mixed Col" text, "ID" int, "CamelCase" int, lower_c int)"#,
    );
}
// Quoted-column indexes exist before every read/write so the index-backed
// paths (E970: CREATE INDEX accepted quoted columns) are exercised too.
fn populated(s: &mut SqlSession) {
    setup(s);
    run(s, r#"CREATE INDEX ON t ("Mixed Col")"#);
    run(s, r#"CREATE INDEX ON t ("CamelCase")"#);
    run(
        s,
        r#"INSERT INTO t ("id","Mixed Col","ID","CamelCase","lower_c") VALUES (1,'x',10,100,1000)"#,
    );
}
fn final_rows(s: &mut SqlSession) {
    setup(s);
    run(
        s,
        r#"INSERT INTO t ("id","Mixed Col","ID","CamelCase","lower_c") VALUES (1,'y',10,101,1000)"#,
    );
    run(
        s,
        r#"INSERT INTO t ("id","Mixed Col","ID","CamelCase","lower_c") VALUES (2,'z',NULL,NULL,NULL)"#,
    );
}
fn rows(s: &mut SqlSession, sql: &str, expected: Vec<Vec<V>>) {
    assert_eq!(values(run(s, sql)), expected, "{sql}");
}

#[test]
fn case_01() {
    database(|s| {
        setup(s);
        run(s, r#"CREATE INDEX ON t ("Mixed Col")"#);
        run(s, r#"CREATE INDEX ON t ("CamelCase")"#);
    });
}

#[test]
fn case_02() {
    database(|s| {
        setup(s);
        run(
            s,
            r#"INSERT INTO t (id,"Mixed Col","ID","CamelCase",lower_c) VALUES (1,'x',10,100,1000)"#,
        );
    });
}

#[test]
fn case_03() {
    database(|s| {
        populated(s);
        let sql = r#"SELECT "Mixed Col" FROM t WHERE "Mixed Col"='x' ORDER BY "Mixed Col""#;
        let result = run(s, sql);
        assert_eq!(result.columns, ["Mixed Col"], "{sql}: {result:?}");
        assert_eq!(values(result), vec![vec![V::Text("x".into())]], "{sql}");
    });
}

#[test]
fn case_04() {
    database(|s| {
        populated(s);
        let sql = r#"UPDATE t SET "Mixed Col"='y' WHERE "Mixed Col"='x'"#;
        let result = run(s, sql);
        assert_eq!(result.affected, 1, "{sql}: {result:?}");
        rows(
            s,
            r#"SELECT "Mixed Col" FROM t"#,
            vec![vec![V::Text("y".into())]],
        );
    });
}

#[test]
fn case_05() {
    database(|s| {
        populated(s);
        let sql = r#"SELECT id, ID, "id", "ID" FROM t"#;
        let result = run(s, sql);
        assert_eq!(
            result.columns,
            ["id", "id", "id", "ID"],
            "{sql}: {result:?}"
        );
        assert_eq!(
            values(result),
            vec![vec![V::Int(1), V::Int(1), V::Int(1), V::Int(10)]],
            "{sql}"
        );
    });
}

#[test]
fn case_06() {
    database(|s| {
        populated(s);
        error(s, "SELECT CamelCase FROM t", E::UnknownColumn);
        error(s, "SELECT camelcase FROM t", E::UnknownColumn);
        error(s, r#"SELECT "camelcase" FROM t"#, E::UnknownColumn);
    });
}

#[test]
fn case_07() {
    database(|s| {
        populated(s);
        let sql = r#"SELECT "CamelCase" FROM t WHERE "CamelCase"=100 ORDER BY "CamelCase""#;
        let result = run(s, sql);
        assert_eq!(result.columns, ["CamelCase"], "{sql}: {result:?}");
        assert_eq!(values(result), vec![vec![V::Int(100)]], "{sql}");
    });
}

#[test]
fn case_08() {
    database(|s| {
        populated(s);
        rows(
            s,
            r#"SELECT LOWER_C, "lower_c" FROM t"#,
            vec![vec![V::Int(1000), V::Int(1000)]],
        );
        error(s, r#"SELECT "LOWER_C" FROM t"#, E::UnknownColumn);
        error(s, r#"SELECT "mixed col" FROM t"#, E::UnknownColumn);
    });
}

#[test]
fn case_09() {
    database(|s| {
        populated(s);
        error(s, "UPDATE t SET camelcase = 1", E::UnknownColumn);
        let sql = r#"UPDATE t SET "CamelCase"=101 WHERE "ID"=10"#;
        let result = run(s, sql);
        assert_eq!(result.affected, 1, "{sql}: {result:?}");
    });
}

#[test]
fn case_10() {
    database(|s| {
        populated(s);
        error(
            s,
            r#"INSERT INTO t ("id","CAMELCASE") VALUES (2,1)"#,
            E::UnknownColumn,
        );
        run(s, r#"INSERT INTO t (ID,"Mixed Col") VALUES (2,'z')"#);
        rows(
            s,
            r#"SELECT "Mixed Col", "ID" FROM t WHERE id=2"#,
            vec![vec![V::Text("z".into()), V::Null]],
        );
    });
}

#[test]
fn case_11() {
    database(|s| {
        setup(s);
        error(s, "CREATE INDEX ON t (camelcase)", E::UnknownColumn);
        error(s, r#"CREATE INDEX ON t ("mixed col")"#, E::UnknownColumn);
        run(s, "CREATE INDEX ON t (LOWER_C)");
    });
}

#[test]
fn case_12() {
    database(|s| {
        final_rows(s);
        rows(
            s,
            r#"SELECT t."Mixed Col", t.id FROM t ORDER BY t."CamelCase" DESC"#,
            vec![
                vec![V::Text("z".into()), V::Int(2)],
                vec![V::Text("y".into()), V::Int(1)],
            ],
        );
    });
}

#[test]
fn case_13() {
    database(|s| {
        final_rows(s);
        let sql = "SELECT * FROM t ORDER BY id";
        let result = run(s, sql);
        assert_eq!(
            result.columns,
            ["id", "Mixed Col", "ID", "CamelCase", "lower_c"],
            "{sql}: {result:?}"
        );
        assert_eq!(
            values(result),
            vec![
                vec![
                    V::Int(1),
                    V::Text("y".into()),
                    V::Int(10),
                    V::Int(101),
                    V::Int(1000)
                ],
                vec![V::Int(2), V::Text("z".into()), V::Null, V::Null, V::Null],
            ],
            "{sql}"
        );
    });
}
