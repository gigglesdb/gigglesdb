use giggles_core::{Admin, CoreError, Error, Limits, Principal, TaskId, relational::Value};
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
        std::fs::remove_dir_all(&self.0).expect("remove aggregate authority test database");
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
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = PathBuf::from("J:/gigglesdb-work/e3-query/wo-agg-b").join(format!(
            "aggregate-authority-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        let directory = Directory(path);
        let admin = Admin::create(directory.0.join("db.redb"), Limits::default()).unwrap();
        {
            let mut owner = admin.sql_session();
            for sql in [
                "CREATE TABLE app (id serial PRIMARY KEY, n bigint)",
                "CREATE TABLE secret (id serial PRIMARY KEY)",
                "INSERT INTO app (id, n) VALUES (1, 10), (2, 20)",
            ] {
                owner.execute(sql, &[], None).unwrap();
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
                    role: Role::Task {
                        domain,
                        task: TaskId(91),
                        principal: Principal(92),
                    },
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
fn avg_descriptions_report_postgresql_result_oids_and_unconstrained_typmods() {
    let f = Fixture::new();
    let mut app = f.service.session(APP).unwrap();
    for modifier in ["", "DISTINCT "] {
        let expressions = [
            "n::smallint",
            "n::integer",
            "n",
            "n::numeric",
            "n::float4",
            "n::float8",
        ]
        .map(|expression| format!("avg({modifier}{expression})"))
        .join(", ");
        let prepared = app
            .prepare(&format!("SELECT {expressions} FROM app"))
            .unwrap();
        let description = app.describe(&prepared).unwrap();
        let oids: Vec<_> = description
            .columns
            .iter()
            .map(|column| giggles_server::pgwire::oid(&column.data_type))
            .collect();
        assert_eq!(oids, vec![1700, 1700, 1700, 1700, 701, 701], "{modifier}");
        assert_eq!(description.typmods, vec![-1; 6], "{modifier}");
    }
}

#[test]
fn task_sql_authority_denies_avg_and_distinct_like_existing_aggregates() {
    let f = Fixture::new();
    let mut task = f.service.session(TASK).unwrap();
    let before = f.admin.snapshot().unwrap();
    let catalog = f.admin.catalog().unwrap();
    for function in ["count", "sum", "avg", "min", "max"] {
        for modifier in ["", "DISTINCT "] {
            denied(
                &mut task,
                &format!("SELECT {function}({modifier}n) FROM app"),
            );
            assert_eq!(f.admin.snapshot().unwrap(), before);
            assert_eq!(f.admin.catalog().unwrap(), catalog);
        }
    }
}

#[test]
fn aggregate_sequence_precheck_denies_secret_dependencies_before_execution() {
    let f = Fixture::new();
    let mut app = f.service.session(APP).unwrap();
    assert_eq!(
        app.execute_sql("SELECT n FROM app ORDER BY id", &[])
            .unwrap()
            .rows
            .len(),
        2
    );
    // Establish that the application has sequence authority for its own table.
    assert_eq!(
        app.execute_sql("SELECT nextval('app_id_seq')", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
    let before = f.admin.snapshot().unwrap();
    let catalog = f.admin.catalog().unwrap();
    for function in ["count", "sum", "avg", "min", "max"] {
        for modifier in ["", "DISTINCT "] {
            let aggregate = format!("{function}({modifier}(nextval('secret_id_seq') + 1))");
            for sql in [
                format!("SELECT {aggregate} FROM app"),
                format!("SELECT count(*) FROM app HAVING {aggregate} > 0"),
                format!("SELECT count(*) FROM app ORDER BY {aggregate} + 1"),
            ] {
                denied(&mut app, &sql);
                assert_eq!(f.admin.snapshot().unwrap(), before);
                assert_eq!(f.admin.catalog().unwrap(), catalog);
            }
        }
    }
    // Sequences are not part of Snapshot: inspect both counters separately.
    let inspector_fingerprint = "cc".repeat(32);
    let inspector_service = Service::new(
        f.admin.clone(),
        vec![Binding {
            fingerprint: inspector_fingerprint.clone(),
            role: Role::Application {
                domain: f.admin.authority_metadata().unwrap().0.to_string(),
                tables: BTreeSet::from(["app".into(), "secret".into()]),
            },
        }],
        Clock::new(0, 3600).unwrap(),
    )
    .unwrap();
    let mut owner = inspector_service.session(&inspector_fingerprint).unwrap();
    assert_eq!(
        owner
            .execute_sql("SELECT nextval('secret_id_seq')", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
    assert_eq!(
        owner
            .execute_sql("SELECT nextval('app_id_seq')", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2)]
    );
}
