use giggles_core::{Admin, CoreError, Error, Limits};
use giggles_server::{Binding, Request, Role, Service, Session, clock::Clock};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const APP: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TASK: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove greatest/least authority test database");
    }
}

struct Fixture {
    admin: Admin,
    service: Arc<Service>,
    // Drop the database handles before removing their files on Windows.
    _directory: Directory,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "greatest-least-authority-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let directory = Directory(path);
        let admin = Admin::create(directory.0.join("db.redb"), Limits::default()).unwrap();
        {
            let mut owner = admin.sql_session();
            for sql in [
                "CREATE TABLE app (id serial PRIMARY KEY, date_col date, time_col time)",
                "CREATE TABLE secret (id serial PRIMARY KEY, d date, t time)",
                "INSERT INTO app (id, date_col, time_col) VALUES (1, '2024-07-01', '10:00')",
            ] {
                owner
                    .execute(sql, &[], None)
                    .unwrap_or_else(|error| panic!("fixture SQL {sql}: {error:?}"));
            }
        }
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let service = Service::new(
            admin.clone(),
            vec![
                Binding {
                    fingerprint: APP.into(),
                    role: Role::Application {
                        domain: domain.clone(),
                        tables: BTreeSet::from(["app".into()]),
                    },
                },
                Binding {
                    fingerprint: TASK.into(),
                    role: Role::NoSql { domain },
                },
            ],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        Self {
            admin,
            service,
            _directory: directory,
        }
    }
}

fn denied(session: &mut Session, sql: &str) {
    // Check the actual prepare error as well: task response disclosure must not
    // hide a different prepare error behind its fixed denial envelope.
    assert!(
        matches!(session.prepare(sql), Err(CoreError::Policy(Error::Scope))),
        "expected scope denial while preparing {sql}",
    );
    let response = session.handle(Request::Sql {
        sql: sql.into(),
        params: vec![],
    });
    assert_eq!(response.code, "42501", "{sql}");
    assert!(response.sql.is_none(), "{sql}");
    assert!(!session.in_transaction(), "{sql}");
}

#[test]
fn authority_denials_precede_greatest_type_classification() {
    let f = Fixture::new();
    let before = f.admin.snapshot().unwrap();
    let catalog = f.admin.catalog().unwrap();
    let mut task = f.service.session(TASK).unwrap();
    denied(
        &mut task,
        "SELECT greatest('2024-07-01'::date,'10:00'::time)",
    );
    let mut app = f.service.session(APP).unwrap();
    denied(&mut app, "SELECT greatest(d,t) FROM secret");
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.catalog().unwrap(), catalog);
}

#[test]
fn authorized_scalar_errors_keep_their_service_sqlstates() {
    let f = Fixture::new();
    let mut app = f.service.session(APP).unwrap();
    for (sql, code) in [
        ("SELECT greatest(date_col,time_col) FROM app", "42846"),
        ("SELECT format('%I',NULL)", "22004"),
    ] {
        let response = app.handle(Request::Sql {
            sql: sql.into(),
            params: vec![],
        });
        assert_eq!(response.code, code, "{sql}");
        assert!(!app.in_transaction(), "{sql}");
    }
}
