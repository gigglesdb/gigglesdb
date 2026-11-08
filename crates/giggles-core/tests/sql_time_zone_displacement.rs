use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};
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
            "dd68-f18b-time-zone-displacement-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
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

#[test]
fn text_datetime_offsets_use_invalid_time_zone_displacement() {
    let fixture = Fixture::new();
    let mut session = fixture.session();
    session
        .execute(
            "CREATE TABLE offset_inputs (id integer PRIMARY KEY, v timetz)",
            &[],
            None,
        )
        .unwrap();

    for sql in [
        "SELECT '12:00+16'::timetz",
        "SELECT CAST('12:00+16' AS time)",
        "SELECT timestamptz '2024-01-01 12:00+16'",
        "INSERT INTO offset_inputs VALUES (1, '12:00+16')",
    ] {
        assert_eq!(
            session.execute(sql, &[], None).unwrap_err(),
            CoreError::Sql(SqlError::InvalidTimeZoneDisplacementValue),
            "{sql}"
        );
    }

    for (sql, expected) in [
        ("SELECT '12:00+15:59'::timetz::text", "12:00:00+15:59"),
        ("SELECT CAST('12:00+15:59' AS time)::text", "12:00:00"),
    ] {
        assert_eq!(
            session.execute(sql, &[], None).unwrap().rows[0].values,
            vec![Value::Text(expected.into())],
            "{sql}"
        );
    }
    session
        .execute(
            "INSERT INTO offset_inputs VALUES (1, '12:00+15:59')",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT v::text FROM offset_inputs", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("12:00:00+15:59".into())]
    );
}
