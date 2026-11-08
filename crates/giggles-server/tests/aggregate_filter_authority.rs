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
        let path = std::env::var_os("TEMP")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("giggles-core-artifacts")
            })
            .join(format!(
                "aggregate-authority-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
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
fn task_filtered_aggregates_remain_denied_before_execution() {
    let f = Fixture::new();
    let mut task = f.service.session(TASK).unwrap();
    let before = f.admin.snapshot().unwrap();
    let catalog = f.admin.catalog().unwrap();
    for aggregate in [
        "count(*)",
        "count(n)",
        "sum(n)",
        "avg(n)",
        "min(n)",
        "max(n)",
        "count(DISTINCT n)",
        "sum(DISTINCT n)",
        "avg(DISTINCT n)",
        "min(DISTINCT n)",
        "max(DISTINCT n)",
    ] {
        for sql in [
            format!("SELECT {aggregate} FILTER (WHERE false) FROM app"),
            format!(
                "SELECT count(*) FROM app WHERE false HAVING {aggregate} FILTER (WHERE true)>0"
            ),
            format!(
                "SELECT count(*) FROM app WHERE false ORDER BY {aggregate} FILTER (WHERE true)"
            ),
        ] {
            denied(&mut task, &sql);
            assert_eq!(f.admin.snapshot().unwrap(), before);
            assert_eq!(f.admin.catalog().unwrap(), catalog);
        }
    }
    for sql in [
        "SELECT count(*) FILTER (WHERE false)>0 FROM app",
        "SELECT CASE WHEN false THEN count(*) FILTER (WHERE true) ELSE 0 END FROM app",
        "SELECT count(*) FILTER (WHERE false)",
        "SELECT count(*) FILTER (WHERE false)>0",
        "SELECT CASE WHEN false THEN count(*) FILTER (WHERE true) ELSE 0 END",
    ] {
        denied(&mut task, sql);
        assert_eq!(f.admin.snapshot().unwrap(), before);
        assert_eq!(f.admin.catalog().unwrap(), catalog);
    }
}

#[test]
fn filter_sequence_dependencies_are_prechecked_even_without_input_rows() {
    let f = Fixture::new();
    let mut app = f.service.session(APP).unwrap();
    assert_eq!(
        app.execute_sql("SELECT nextval('app_id_seq')", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
    let before = f.admin.snapshot().unwrap();
    let catalog = f.admin.catalog().unwrap();
    for aggregate in [
        "count(*)",
        "count(n)",
        "sum(n)",
        "avg(n)",
        "min(n)",
        "max(n)",
        "count(DISTINCT n)",
        "sum(DISTINCT n)",
        "avg(DISTINCT n)",
    ] {
        for predicate in [
            "nextval('secret_id_seq')>0",
            "false AND nextval('secret_id_seq')>0",
        ] {
            for restriction in ["", " WHERE false", " WHERE id<0"] {
                for sql in [
                    format!("SELECT {aggregate} FILTER (WHERE {predicate}) FROM app{restriction}"),
                    format!(
                        "SELECT count(*) FROM app{restriction} HAVING {aggregate} FILTER (WHERE {predicate})>0"
                    ),
                    format!(
                        "SELECT count(*) FROM app{restriction} ORDER BY {aggregate} FILTER (WHERE {predicate})"
                    ),
                ] {
                    denied(&mut app, &sql);
                    assert_eq!(f.admin.snapshot().unwrap(), before);
                    assert_eq!(f.admin.catalog().unwrap(), catalog);
                }
            }
        }
    }
    for sql in [
        "SELECT count(*) FILTER (WHERE nextval('secret_id_seq')>0)>0 FROM app WHERE false",
        "SELECT CASE WHEN false THEN count(*) FILTER (WHERE nextval('secret_id_seq')>0) ELSE 0 END FROM app WHERE false",
        "SELECT count(*) FILTER (WHERE CASE WHEN false THEN nextval('secret_id_seq')>0 ELSE true END) FROM app WHERE false",
        "SELECT count(*) FILTER (WHERE nextval('secret_id_seq')>0)",
        "SELECT count(*) FILTER (WHERE nextval('secret_id_seq')>0)>0",
        "SELECT CASE WHEN false THEN count(*) FILTER (WHERE nextval('secret_id_seq')>0) ELSE 0 END",
        "SELECT count(*) FILTER (WHERE CASE WHEN false THEN nextval('secret_id_seq')>0 ELSE true END)",
    ] {
        denied(&mut app, sql);
        assert_eq!(f.admin.snapshot().unwrap(), before);
        assert_eq!(f.admin.catalog().unwrap(), catalog);
    }
    let fingerprint = "cc".repeat(32);
    let service = Service::new(
        f.admin.clone(),
        vec![Binding {
            fingerprint: fingerprint.clone(),
            role: Role::Application {
                domain: f.admin.authority_metadata().unwrap().0.to_string(),
                tables: BTreeSet::from(["app".into(), "secret".into()]),
            },
        }],
        Clock::new(0, 3600).unwrap(),
    )
    .unwrap();
    let mut inspector = service.session(&fingerprint).unwrap();
    assert_eq!(
        inspector
            .execute_sql("SELECT nextval('secret_id_seq')", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
    assert_eq!(
        inspector
            .execute_sql("SELECT nextval('app_id_seq')", &[])
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2)]
    );
}

#[test]
fn filtered_aggregate_describe_reports_parameters_and_result_types() {
    let f = Fixture::new();
    let mut app = f.service.session(APP).unwrap();
    let p = app
        .prepare(
            "SELECT count(*) FILTER (WHERE $1),avg(DISTINCT n) FILTER (WHERE n>$2 AND $3) FROM app",
        )
        .unwrap();
    let d = app.describe(&p).unwrap();
    assert_eq!(
        d.parameters
            .iter()
            .map(giggles_server::pgwire::oid)
            .collect::<Vec<_>>(),
        vec![16, 20, 16]
    );
    assert_eq!(
        d.columns
            .iter()
            .map(|c| giggles_server::pgwire::oid(&c.data_type))
            .collect::<Vec<_>>(),
        vec![20, 1700]
    );
    assert_eq!(d.typmods, vec![-1; 2]);
    for (sql, parameters, columns) in [
        (
            "SELECT count(*) FILTER (WHERE $1)>$2,CASE WHEN $3 THEN sum(n) FILTER (WHERE n>$4) ELSE 0 END FROM app",
            vec![16, 20, 16, 20],
            vec![16, 1700],
        ),
        (
            "SELECT CASE WHEN count(*) FILTER (WHERE $1)>$2 THEN sum($3::int) FILTER (WHERE $4) ELSE 0 END",
            vec![16, 20, 23, 16],
            vec![20],
        ),
    ] {
        let p = app.prepare(sql).unwrap();
        let d = app.describe(&p).unwrap();
        assert_eq!(
            d.parameters
                .iter()
                .map(giggles_server::pgwire::oid)
                .collect::<Vec<_>>(),
            parameters,
            "{sql}",
        );
        assert_eq!(
            d.columns
                .iter()
                .map(|c| giggles_server::pgwire::oid(&c.data_type))
                .collect::<Vec<_>>(),
            columns,
            "{sql}",
        );
        assert_eq!(d.typmods, vec![-1; columns.len()], "{sql}");
    }
}
