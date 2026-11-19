#[cfg(feature = "legacy-task")]
use giggles_core::OperationId;
use giggles_core::{
    Admin, CoreError, Error, Limits,
    relational::{SqlResult, Value},
    roles::{NewRole, RoleChanges},
};
use giggles_server::{Binding, Request, Role, Service, Session, clock::Clock};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

static SERIAL: AtomicU64 = AtomicU64::new(0);
const APP_FP: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct Fixture {
    admin: Admin,
    service: Arc<Service>,
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "role-sessions-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
                &[],
                None,
            )
            .unwrap();
        for (name, superuser, login) in [
            ("owner", true, true),
            ("reader", false, true),
            ("nologin", true, false),
        ] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    superuser,
                    login,
                    ..NewRole::default()
                })
                .unwrap();
        }
        let bindings = vec![Binding {
            fingerprint: APP_FP.into(),
            role: Role::Application {
                domain: admin.authority_metadata().unwrap().0.to_string(),
                tables: BTreeSet::from(["app".into()]),
            },
        }];
        let service = Service::new(admin.clone(), bindings, Clock::new(0, 3600).unwrap()).unwrap();
        Self { admin, service }
    }
    fn owner(&self) -> Session {
        self.service.role_session("owner").unwrap()
    }
}
fn execute(session: &mut Session, sql: &str) -> SqlResult {
    session.execute_sql(sql, &[]).unwrap()
}
fn values(result: SqlResult) -> Vec<Vec<Value>> {
    result.rows.into_iter().map(|row| row.values).collect()
}
fn denied<T>(result: Result<T, CoreError>, expected: Error) {
    assert!(matches!(result, Err(CoreError::Policy(error)) if error == expected));
}

#[test]
fn owner_crud_and_subsequent_session_see_committed_table() {
    let f = Fixture::new();
    let mut owner = f.owner();
    execute(
        &mut owner,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    assert_eq!(
        execute(&mut owner, "INSERT INTO t VALUES (1, 'before')").affected,
        1
    );
    assert_eq!(
        values(execute(&mut owner, "SELECT id, body FROM t WHERE id = 1")),
        vec![vec![Value::Int(1), Value::Text("before".into())]]
    );
    assert_eq!(
        execute(&mut owner, "UPDATE t SET body = 'after' WHERE id = 1").affected,
        1
    );
    assert_eq!(
        f.admin.roles().table_owner("t").unwrap().as_deref(),
        Some("owner")
    );
    let mut next = f.owner();
    assert_eq!(
        values(execute(&mut next, "SELECT body FROM t WHERE id = 1")),
        vec![vec![Value::Text("after".into())]]
    );
    assert_eq!(execute(&mut next, "DELETE FROM t WHERE id = 1").affected, 1);
    assert!(
        execute(&mut owner, "SELECT id FROM t LIMIT 1")
            .rows
            .is_empty()
    );
}

#[test]
fn explicit_transaction_can_use_staged_table_and_commit_ownership() {
    let f = Fixture::new();
    let mut owner = f.owner();
    execute(&mut owner, "BEGIN");
    execute(
        &mut owner,
        "CREATE TABLE staged (id BIGINT PRIMARY KEY, body TEXT)",
    );
    execute(&mut owner, "INSERT INTO staged VALUES (7, 'staged')");
    let expected = vec![vec![Value::Int(7), Value::Text("staged".into())]];
    assert_eq!(
        values(execute(
            &mut owner,
            "SELECT id, body FROM staged WHERE id = 7"
        )),
        expected
    );
    execute(&mut owner, "COMMIT");
    assert_eq!(
        f.admin.roles().table_owner("staged").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(
        values(execute(
            &mut f.owner(),
            "SELECT id, body FROM staged WHERE id = 7"
        )),
        expected
    );
}

#[test]
fn ordinary_role_has_no_implicit_table_or_create_privileges() {
    let f = Fixture::new();
    execute(
        &mut f.owner(),
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    let mut reader = f.service.role_session("reader").unwrap();
    denied(
        reader.execute_sql("CREATE TABLE forbidden (id BIGINT PRIMARY KEY)", &[]),
        Error::Scope,
    );
    denied(
        reader.execute_sql("SELECT id FROM t LIMIT 1", &[]),
        Error::Scope,
    );
    execute(&mut reader, "BEGIN");
    execute(&mut reader, "ROLLBACK");
}

#[test]
fn unknown_and_nologin_roles_cannot_open_sessions() {
    let f = Fixture::new();
    denied(f.service.role_session("missing"), Error::Authentication);
    denied(f.service.role_session("nologin"), Error::Authentication);
}

#[test]
fn changing_password_invalidates_existing_role_session() {
    let f = Fixture::new();
    let mut owner = f.owner();
    execute(&mut owner, "BEGIN");
    execute(&mut owner, "ROLLBACK");
    f.admin
        .roles()
        .alter(
            "owner",
            RoleChanges {
                password: Some(Some("SCRAM-SHA-256$test-only-opaque-verifier".into())),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    denied(owner.execute_sql("BEGIN", &[]), Error::Authentication);
    let mut fresh_owner = f.owner();
    execute(&mut fresh_owner, "BEGIN");
    execute(&mut fresh_owner, "ROLLBACK");
}

#[test]
fn dropping_nonowning_role_invalidates_existing_session() {
    let f = Fixture::new();
    let mut reader = f.service.role_session("reader").unwrap();
    execute(&mut reader, "BEGIN");
    execute(&mut reader, "ROLLBACK");
    f.admin.roles().drop("reader").unwrap();
    denied(reader.execute_sql("BEGIN", &[]), Error::Authentication);
}

#[test]
fn recreating_role_invalidates_old_session_and_allows_new_session() {
    let f = Fixture::new();
    let mut reader = f.service.role_session("reader").unwrap();
    execute(&mut reader, "BEGIN");
    execute(&mut reader, "ROLLBACK");
    let original = f.admin.roles().get("reader").unwrap().unwrap();
    f.admin.roles().drop("reader").unwrap();
    f.admin
        .roles()
        .create(NewRole {
            name: "reader".into(),
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let recreated = f.admin.roles().get("reader").unwrap().unwrap();
    assert_ne!(original.oid, recreated.oid);
    assert_eq!(original.generation, recreated.generation);
    let result = reader.execute_sql("BEGIN", &[]);
    assert!(
        matches!(result, Err(CoreError::Policy(Error::Authentication))),
        "old session survived role recreation: {result:?}"
    );
    let mut fresh = f.service.role_session("reader").unwrap();
    execute(&mut fresh, "BEGIN");
    execute(&mut fresh, "ROLLBACK");
}

#[test]
fn application_binding_keeps_its_existing_sql_scope() {
    let f = Fixture::new();
    let mut app = f.service.session(APP_FP).unwrap();
    denied(
        app.execute_sql("CREATE TABLE forbidden (id BIGINT PRIMARY KEY)", &[]),
        Error::Scope,
    );
    execute(&mut app, "INSERT INTO app VALUES (1, 'application')");
    assert_eq!(
        values(execute(&mut app, "SELECT id, body FROM app WHERE id = 1")),
        vec![vec![Value::Int(1), Value::Text("application".into())]]
    );
}

#[test]
fn database_role_is_a_serializable_provisioned_certificate_binding() {
    let value = serde_json::json!({
        "fingerprint": APP_FP,
        "role": {"role": "Database", "name": "owner"}
    });
    let binding = serde_json::from_value::<Binding>(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(&binding).unwrap(), value);
    let f = Fixture::new();
    f.service.reload_bindings(vec![binding]).unwrap();
    assert_eq!(f.service.session(APP_FP).unwrap().handle(Request::Ping).code, "ok");
}

#[test]
fn database_session_rejects_task_status_but_handles_ping_and_sql() {
    let f = Fixture::new();
    let mut owner = f.owner();
    #[cfg(feature = "legacy-task")]
    assert_eq!(
        owner
            .handle(Request::Status {
                operation: OperationId(1)
            })
            .code,
        "42501"
    );
    assert_eq!(owner.handle(Request::Ping).code, "ok");
    execute(&mut owner, "CREATE TABLE sql_probe (id BIGINT PRIMARY KEY)");
    execute(&mut owner, "INSERT INTO sql_probe VALUES (1)");
    let response = owner.handle(Request::Sql {
        sql: "SELECT id FROM sql_probe LIMIT 1".into(),
        params: vec![],
    });
    assert_eq!(response.code, "ok");
    assert_eq!(values(response.sql.unwrap()), vec![vec![Value::Int(1)]]);
}

#[test]
fn superuser_can_join_multiple_tables() {
    let f = Fixture::new();
    let mut owner = f.owner();
    execute(
        &mut owner,
        "CREATE TABLE joined (id BIGINT PRIMARY KEY, body TEXT)",
    );
    execute(&mut owner, "INSERT INTO app VALUES (1, 'left')");
    execute(&mut owner, "INSERT INTO joined VALUES (1, 'right')");
    assert_eq!(
        values(execute(
            &mut owner,
            "SELECT a.body, j.body FROM app a JOIN joined j ON a.id = j.id LIMIT 1",
        )),
        vec![vec![
            Value::Text("left".into()),
            Value::Text("right".into())
        ]]
    );
}
