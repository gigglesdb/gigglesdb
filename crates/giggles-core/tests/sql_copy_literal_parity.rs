// DD-0068 F18: COPY text fields and INSERT unknown literals share one
// assignment path (query::assignment::literal), so they store the same values
// and report the same PostgreSQL 18.6 input SQLSTATEs.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, StatementClock, Value as V},
};
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
        let path = PathBuf::from("J:/gigglesdb-work/dd68-f18/tmp").join(format!(
            "copy-literal-parity-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let admin = Admin::create(&path, Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE t (id integer PRIMARY KEY, b bytea, i integer, d date, n numeric)",
                &[],
                None,
            )
            .unwrap();
        Self {
            admin: Some(admin),
            path,
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
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
fn text(s: &str) -> V {
    V::Text(s.into())
}
fn copy(s: &mut SqlSession, fields: [&str; 5]) -> Result<(), CoreError> {
    let (_, p) = s.prepare_copy("COPY t FROM STDIN").unwrap();
    let (normalized, rows) =
        s.normalize_copy_rows_at(&p, vec![fields.map(text).to_vec()], &clock())?;
    s.ingest_copy_at(&normalized, rows, &clock()).map(|_| ())
}
fn insert(s: &mut SqlSession, fields: [&str; 5]) -> Result<(), CoreError> {
    let [id, b, i, d, n] = fields.map(|f| format!("'{}'", f.replace('\'', "''")));
    s.execute_at(
        &format!("INSERT INTO t VALUES ({id}, {b}, {i}, {d}, {n})"),
        &[],
        None,
        &clock(),
    )
    .map(|_| ())
}
fn row(s: &mut SqlSession, id: i64) -> Vec<V> {
    s.execute_at(
        &format!("SELECT b, i, d::text, n::text FROM t WHERE id = {id}"),
        &[],
        None,
        &clock(),
    )
    .unwrap()
    .rows
    .remove(0)
    .values
}

#[test]
fn copy_and_insert_store_the_same_values() {
    let f = Fixture::new();
    let mut s = f.session();
    copy(&mut s, ["1", r"\x0001ff", "-0012", "2024-02-29", "12.3400"]).unwrap();
    insert(&mut s, ["2", r"\x0001ff", "-0012", "2024-02-29", "12.3400"]).unwrap();
    // PG18.6: bytea \x0001ff, int -12, date 2024-02-29, numeric 12.3400.
    let expected = vec![
        V::Bytes(vec![0x00, 0x01, 0xff]),
        V::Int(-12),
        text("2024-02-29"),
        text("12.3400"),
    ];
    assert_eq!(row(&mut s, 1), expected);
    assert_eq!(row(&mut s, 2), expected);
}

#[test]
fn copy_and_insert_report_the_same_input_errors() {
    let f = Fixture::new();
    let mut s = f.session();
    // PG18.6: integer "4x" 22P02, date "2023-02-29" 22008, date "notadate"
    // 22007, numeric "abc" 22P02, numeric "1e" 22P02, bytea "\x123" 22023.
    for (fields, expected) in [
        (
            ["1", r"\x01", "4x", "2024-01-01", "1"],
            E::InvalidTextRepresentation,
        ),
        (
            ["1", r"\x01", "1", "2023-02-29", "1"],
            E::DatetimeFieldOverflow,
        ),
        (
            ["1", r"\x01", "1", "notadate", "1"],
            E::InvalidDatetimeFormat,
        ),
        (
            ["1", r"\x01", "1", "2024-01-01", "abc"],
            E::InvalidTextRepresentation,
        ),
        (
            ["1", r"\x01", "1", "2024-01-01", "1e"],
            E::InvalidTextRepresentation,
        ),
        (
            ["1", r"\x123", "1", "2024-01-01", "1"],
            E::InvalidParameterValue,
        ),
    ] {
        assert_eq!(
            copy(&mut s, fields),
            Err(CoreError::Sql(expected.clone())),
            "COPY {fields:?}"
        );
        assert_eq!(
            insert(&mut s, fields),
            Err(CoreError::Sql(expected)),
            "INSERT {fields:?}"
        );
    }
    assert!(
        s.execute("SELECT id FROM t LIMIT 1", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}
