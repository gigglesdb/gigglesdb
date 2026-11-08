use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, StatementClock},
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
            "dd68-f18b-time-zone-word-{}-{}",
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

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}

fn expect_error(session: &mut SqlSession, sql: &str, error: SqlError) {
    assert_eq!(
        session.execute_at(sql, &[], None, &clock()).unwrap_err(),
        CoreError::Sql(error),
        "{sql}"
    );
}

#[test]
fn datetime_input_cast_zone_word_oracles() {
    let fixture = Fixture::new();
    let mut session = fixture.session();
    for text in [
        "noon-ish",
        "noon/ish",
        "noon.ish",
        "x-y",
        "12:00 foo-bar",
        "12:00 Mars/Base",
        "12:00 a.m.",
        "12:00 utc-ish",
        "12:00 america/new_yorkx",
    ] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS time)"),
            SqlError::InvalidParameterValue,
        );
    }
    for text in [
        "12:00 foo",
        "x",
        "Europe/Paris",
        "12:00 Europe/Paris",
        "noon-5",
        "foo-bar 12:00",
    ] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS time)"),
            SqlError::InvalidDatetimeFormat,
        );
    }
    for text in ["noon-ish", "12:00 Mars/Base", "12:00 foo-bar", "Mars/Base"] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS timetz)"),
            SqlError::InvalidParameterValue,
        );
    }
    for text in ["12:00 bogus", "x", "12:00 America/New_York"] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS timetz)"),
            SqlError::InvalidDatetimeFormat,
        );
    }
    for text in [
        "2024-01-01 Mars/Base",
        "2024-01-01 foo-bar",
        "2024-01-01 noon-ish",
    ] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS date)"),
            SqlError::InvalidParameterValue,
        );
    }
    for text in ["noon-ish", "Mars/Base", "notadate", "2024-01-01 bogus"] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS date)"),
            SqlError::InvalidDatetimeFormat,
        );
    }
    for text in [
        "2024-01-01 12:00 Mars/Base",
        "2024-01-01 12:00 noon-ish",
        "2024-01-01 Mars/Base",
    ] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS timestamp)"),
            SqlError::InvalidParameterValue,
        );
    }
    for text in ["noon-ish", "2024-01-02 bogus", "nope"] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS timestamp)"),
            SqlError::InvalidDatetimeFormat,
        );
    }
    for text in [
        "2024-01-01 12:00 Mars/Base",
        "2024-01-01 12:00 noon-ish",
        "2024-01-01 Mars/Base",
        "2024-01-01 12:00 foo-bar",
        "2024-01-01 12:00 a.b",
    ] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS timestamptz)"),
            SqlError::InvalidParameterValue,
        );
    }
    for text in ["noon-ish", "nope"] {
        expect_error(
            &mut session,
            &format!("SELECT CAST('{text}' AS timestamptz)"),
            SqlError::InvalidDatetimeFormat,
        );
    }
    expect_error(
        &mut session,
        "SELECT CAST('foo-bar 2024-01-01' AS date)",
        SqlError::Unsupported,
    );
}

#[test]
fn datetime_input_zone_word_errors_across_literal_and_assignment_paths() {
    let fixture = Fixture::new();
    let mut session = fixture.session();
    session
        .execute_at(
            "CREATE TABLE zone_time (id integer PRIMARY KEY, v time)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    session
        .execute_at(
            "INSERT INTO zone_time VALUES (1, NULL)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    for (text, error) in [
        ("noon-ish", SqlError::InvalidParameterValue),
        ("12:00 foo", SqlError::InvalidDatetimeFormat),
    ] {
        for sql in [
            format!("SELECT '{text}'::time"),
            format!("SELECT time '{text}'"),
            format!("INSERT INTO zone_time VALUES (2, '{text}')"),
            format!("UPDATE zone_time SET v = '{text}' WHERE id = 1"),
            format!(
                "INSERT INTO zone_time VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = '{text}'"
            ),
        ] {
            expect_error(&mut session, &sql, error.clone());
        }
    }
    session
        .execute_at(
            "CREATE TABLE zone_timetz (id integer PRIMARY KEY, v timetz)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    session
        .execute_at(
            "INSERT INTO zone_timetz VALUES (1, NULL)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    for (text, error) in [
        ("noon-ish", SqlError::InvalidParameterValue),
        ("12:00 bogus", SqlError::InvalidDatetimeFormat),
    ] {
        for sql in [
            format!("SELECT '{text}'::timetz"),
            format!("SELECT timetz '{text}'"),
            format!("INSERT INTO zone_timetz VALUES (2, '{text}')"),
            format!("UPDATE zone_timetz SET v = '{text}' WHERE id = 1"),
            format!(
                "INSERT INTO zone_timetz VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = '{text}'"
            ),
        ] {
            expect_error(&mut session, &sql, error.clone());
        }
    }
    session
        .execute_at(
            "CREATE TABLE zone_date (id integer PRIMARY KEY, v date)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    session
        .execute_at(
            "INSERT INTO zone_date VALUES (1, NULL)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    for (text, error) in [
        ("2024-01-01 Mars/Base", SqlError::InvalidParameterValue),
        ("2024-01-01 bogus", SqlError::InvalidDatetimeFormat),
    ] {
        for sql in [
            format!("SELECT '{text}'::date"),
            format!("SELECT date '{text}'"),
            format!("INSERT INTO zone_date VALUES (2, '{text}')"),
            format!("UPDATE zone_date SET v = '{text}' WHERE id = 1"),
            format!(
                "INSERT INTO zone_date VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = '{text}'"
            ),
        ] {
            expect_error(&mut session, &sql, error.clone());
        }
    }
    session
        .execute_at(
            "CREATE TABLE zone_timestamp (id integer PRIMARY KEY, v timestamp)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    session
        .execute_at(
            "INSERT INTO zone_timestamp VALUES (1, NULL)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    for (text, error) in [
        (
            "2024-01-01 12:00 Mars/Base",
            SqlError::InvalidParameterValue,
        ),
        ("noon-ish", SqlError::InvalidDatetimeFormat),
    ] {
        for sql in [
            format!("SELECT '{text}'::timestamp"),
            format!("SELECT timestamp '{text}'"),
            format!("INSERT INTO zone_timestamp VALUES (2, '{text}')"),
            format!("UPDATE zone_timestamp SET v = '{text}' WHERE id = 1"),
            format!(
                "INSERT INTO zone_timestamp VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = '{text}'"
            ),
        ] {
            expect_error(&mut session, &sql, error.clone());
        }
    }
    session
        .execute_at(
            "CREATE TABLE zone_timestamptz (id integer PRIMARY KEY, v timestamptz)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    session
        .execute_at(
            "INSERT INTO zone_timestamptz VALUES (1, NULL)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    let updated = session
        .execute_at(
            "UPDATE zone_timestamptz SET v = '2024-01-01 12:00+00' WHERE id = 1",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    assert_eq!(updated.affected, 1);
    let selected = session
        .execute_at(
            "SELECT v = timestamptz '2024-01-01 12:00+00' FROM zone_timestamptz WHERE id = 1",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    assert_eq!(
        selected.rows[0].values,
        vec![giggles_core::relational::Value::Bool(true)]
    );

    for (text, error) in [
        (
            "2024-01-01 12:00 Mars/Base",
            SqlError::InvalidParameterValue,
        ),
        ("noon-ish", SqlError::InvalidDatetimeFormat),
    ] {
        for sql in [
            format!("SELECT '{text}'::timestamptz"),
            format!("SELECT timestamptz '{text}'"),
            format!("INSERT INTO zone_timestamptz VALUES (2, '{text}')"),
            format!("UPDATE zone_timestamptz SET v = '{text}' WHERE id = 1"),
            format!(
                "INSERT INTO zone_timestamptz VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = '{text}'"
            ),
        ] {
            expect_error(&mut session, &sql, error.clone());
        }
    }
}

#[test]
fn datetime_input_valid_cast_controls() {
    let fixture = Fixture::new();
    let mut session = fixture.session();
    for sql in [
        "SELECT CAST('12:00' AS time)",
        "SELECT CAST('12:00+00' AS timetz)",
        "SELECT CAST('2024-01-01' AS date)",
        "SELECT CAST('2024-01-01 12:00' AS timestamp)",
        "SELECT CAST('2024-01-01 12:00+00' AS timestamptz)",
    ] {
        session.execute_at(sql, &[], None, &clock()).unwrap();
    }
}
