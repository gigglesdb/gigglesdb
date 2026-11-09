use giggles_core::{
    Admin, CoreError, Error, Limits, Principal, TaskId,
    relational::Value,
    roles::{NewRole, TablePrivilege},
};
use giggles_server::{Binding, Request, Role, Service, Session, clock::Clock};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const APP: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOTH: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const TASK: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    admin: Admin,
    service: Arc<Service>,
    both_service: Arc<Service>,
    _directory: Directory,
}
impl Fixture {
    fn new() -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path =
            PathBuf::from(std::env::var_os("TEMP").expect("task TEMP configured")).join(format!(
                "in-authority-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ));
        let directory = Directory(path);
        std::fs::create_dir_all(&directory.0).unwrap();
        let admin = Admin::create(directory.0.join("db.redb"), Limits::default()).unwrap();
        for (name, superuser) in [
            ("root", true),
            ("reader", false),
            ("uowner", false),
            ("no_delete", false),
            ("inserter", false),
        ] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..NewRole::default()
                })
                .unwrap();
        }
        {
            let mut setup = admin.sql_session();
            for sql in [
                "CREATE TABLE t(id int PRIMARY KEY)",
                "CREATE TABLE u(id int PRIMARY KEY)",
                "CREATE TABLE w(id int PRIMARY KEY)",
                "INSERT INTO t VALUES (1),(2),(3)",
                "INSERT INTO u VALUES (2),(10)",
                "INSERT INTO w VALUES (2)",
            ] {
                setup.execute(sql, &[], None).unwrap();
            }
        }
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let application_service = |fingerprint: &str, tables| {
            Service::new(
                admin.clone(),
                vec![
                    Binding {
                        fingerprint: fingerprint.into(),
                        role: Role::Application {
                            domain: domain.clone(),
                            tables,
                        },
                    },
                    Binding {
                        fingerprint: TASK.into(),
                        role: Role::Task {
                            domain: domain.clone(),
                            task: TaskId(91),
                            principal: Principal(92),
                        },
                    },
                ],
                Clock::new(0, 3600).unwrap(),
            )
            .unwrap()
        };
        // Distinct application scopes cannot overlap in one service's bindings.
        let service = application_service(APP, BTreeSet::from(["t".into()]));
        let both_service = application_service(BOTH, BTreeSet::from(["t".into(), "u".into()]));
        for name in ["reader", "uowner"] {
            admin
                .roles()
                .grant_table("t", name, &[TablePrivilege::Select, TablePrivilege::Delete])
                .unwrap();
        }
        admin
            .roles()
            .grant_table("t", "inserter", &[TablePrivilege::Insert])
            .unwrap();
        admin.roles().set_table_owner("u", "uowner").unwrap();
        admin
            .roles()
            .grant_table("u", "no_delete", &[TablePrivilege::Select])
            .unwrap();
        Self {
            admin,
            service,
            both_service,
            _directory: directory,
        }
    }
    fn role(&self, name: &str) -> Session {
        self.service.role_session(name).unwrap()
    }
    fn grant(&self, table: &str) {
        self.admin
            .roles()
            .grant_table(table, "reader", &[TablePrivilege::Select])
            .unwrap();
    }
    fn denied(&self, session: &mut Session, sql: &str) {
        let before = self.admin.snapshot().unwrap();
        let catalog = self.admin.catalog().unwrap();
        assert!(
            matches!(session.prepare(sql), Err(CoreError::Policy(Error::Scope))),
            "{sql}"
        );
        let response = session.handle(Request::Sql {
            sql: sql.into(),
            params: vec![],
        });
        assert_eq!(response.code, "42501", "{sql}");
        assert!(response.sql.is_none());
        assert_eq!(self.admin.snapshot().unwrap(), before, "{sql}");
        assert_eq!(self.admin.catalog().unwrap(), catalog, "{sql}");
    }
}
const READS: [&str; 3] = [
    "SELECT id FROM t WHERE id IN (SELECT id FROM u) ORDER BY id",
    "SELECT 1 WHERE 2 IN (SELECT id FROM u)",
    "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE id IN (SELECT id FROM u)) ORDER BY id",
];
const DELETE: &str = "DELETE FROM t WHERE id IN (SELECT id FROM u) RETURNING id";
fn rows(session: &mut Session, sql: &str, expected: &[&[i64]]) {
    let result = session.execute_sql(sql, &[]).unwrap();
    let expected = expected
        .iter()
        .map(|r| r.iter().map(|v| Value::Int(*v)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        expected,
        "{sql}"
    );
}
fn allowed(session: &mut Session) {
    for (sql, expected) in READS.into_iter().zip([2, 1, 2]) {
        rows(session, sql, &[&[expected]]);
    }
    session.execute_sql("BEGIN", &[]).unwrap();
    rows(session, DELETE, &[&[2]]);
    session.execute_sql("ROLLBACK", &[]).unwrap();
}
#[test]
fn subquery_reads_require_each_relation_and_preserve_target_authority() {
    let f = Fixture::new();
    let mut reader = f.role("reader");
    for sql in READS.into_iter().chain([DELETE]) {
        f.denied(&mut reader, sql);
    }
    reader.execute_sql("BEGIN", &[]).unwrap();
    rows(
        &mut reader,
        "DELETE FROM t WHERE id IN (SELECT id FROM t WHERE id>1) RETURNING id",
        &[&[2], &[3]],
    );
    reader.execute_sql("ROLLBACK", &[]).unwrap();
    f.grant("u");
    allowed(&mut reader);
    f.denied(&mut f.role("no_delete"), DELETE);
    allowed(&mut f.role("uowner"));
    allowed(&mut f.role("root"));
}
#[test]
fn application_subquery_reads_require_each_allowlisted_relation() {
    let f = Fixture::new();
    let mut app = f.service.session(APP).unwrap();
    for sql in READS.into_iter().chain([DELETE]) {
        f.denied(&mut app, sql);
    }
    allowed(&mut f.both_service.session(BOTH).unwrap());
}
const INNER: &str = "SELECT t.id,u.id FROM t JOIN u ON t.id=u.id ORDER BY t.id";
const LEFT: &str = "SELECT t.id FROM t LEFT JOIN u ON t.id=u.id ORDER BY t.id";
const THREE: &str = "SELECT t.id FROM t JOIN u ON t.id=u.id JOIN w ON u.id=w.id ORDER BY t.id";
const NESTED_JOIN: &str =
    "SELECT id FROM t WHERE id IN (SELECT u.id FROM u JOIN w ON u.id=w.id) ORDER BY id";
#[test]
fn joins_require_every_real_relation_including_nested_sources() {
    let f = Fixture::new();
    let mut reader = f.role("reader");
    for sql in [INNER, LEFT, THREE, NESTED_JOIN] {
        f.denied(&mut reader, sql);
    }
    f.denied(
        &mut reader,
        "EXPLAIN SELECT t.id FROM t JOIN u ON t.id=u.id",
    );
    rows(
        &mut reader,
        "SELECT a.id FROM t a JOIN t b ON a.id=b.id ORDER BY a.id",
        &[&[1], &[2], &[3]],
    );
    f.grant("u");
    rows(&mut reader, INNER, &[&[2, 2]]);
    rows(&mut reader, LEFT, &[&[1], &[2], &[3]]);
    for sql in [THREE, NESTED_JOIN] {
        f.denied(&mut reader, sql);
    }
    f.grant("w");
    for sql in [THREE, NESTED_JOIN] {
        rows(&mut reader, sql, &[&[2]]);
    }
    rows(&mut f.role("uowner"), INNER, &[&[2, 2]]);
    for sql in [INNER, LEFT] {
        f.denied(&mut f.service.session(APP).unwrap(), sql);
    }
    rows(
        &mut f.both_service.session(BOTH).unwrap(),
        INNER,
        &[&[2, 2]],
    );
    rows(
        &mut f.both_service.session(BOTH).unwrap(),
        LEFT,
        &[&[1], &[2], &[3]],
    );
    rows(&mut f.role("root"), THREE, &[&[2]]);
    let plan = reader
        .execute_sql("EXPLAIN SELECT t.id FROM t JOIN u ON t.id=u.id", &[])
        .unwrap();
    assert!(!plan.rows.is_empty());
}

#[test]
fn sequence_projection_does_not_bypass_subquery_authority() {
    let f = Fixture::new();
    f.role("root")
        .execute_sql("CREATE SEQUENCE t_seq OWNED BY t.id", &[])
        .unwrap();
    let mut app = f.service.session(APP).unwrap();
    f.denied(
        &mut app,
        "SELECT nextval('t_seq') WHERE 2 IN (SELECT id FROM u)",
    );
    rows(&mut app, "SELECT nextval('t_seq')", &[&[1]]);
}

#[test]
fn native_table_free_routine_errors_remain_denied_before_classification() {
    let f = Fixture::new();
    let mut reader = f.role("reader");
    let mut app = f.service.session(APP).unwrap();
    for sql in ["SELECT nosuchfn()", "SELECT lower(body)"] {
        f.denied(&mut reader, sql);
        f.denied(&mut app, sql);
    }
}

#[test]
fn insert_select_requires_source_select_even_with_target_insert() {
    let f = Fixture::new();
    let mut inserter = f.role("inserter");
    let sql = "INSERT INTO t(id) SELECT id FROM u WHERE id>3";
    f.denied(&mut inserter, sql);
    rows(
        &mut f.role("root"),
        "SELECT id FROM t ORDER BY id",
        &[&[1], &[2], &[3]],
    );
    f.admin
        .roles()
        .grant_table("u", "inserter", &[TablePrivilege::Select])
        .unwrap();
    assert_eq!(inserter.execute_sql(sql, &[]).unwrap().affected, 1);
    rows(
        &mut f.role("root"),
        "SELECT id FROM t ORDER BY id",
        &[&[1], &[2], &[3], &[10]],
    );
}

#[test]
fn native_task_join_and_in_subquery_witnesses_remain_denied() {
    let f = Fixture::new();
    let mut task = f.service.session(TASK).unwrap();
    // Inspect the prepare error as well as the task response's fixed envelope.
    for sql in [
        INNER,
        READS[0],
        "INSERT INTO t(id) SELECT id FROM u WHERE id>3",
    ] {
        f.denied(&mut task, sql);
        assert!(!task.in_transaction(), "{sql}");
    }
}
