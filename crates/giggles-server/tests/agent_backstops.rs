//! GD-0061 R3-210 (design-2 section 4, call site 5): the backstops inside
//! `Session`. Whatever entry point an internal caller uses, a task session
//! reaches only the classes `admit` allows; a bare UPDATE needs the protected
//! seam; the framed surface admits only `Ping`; the classifiers have no
//! wildcard arm; and a non-task session is unaffected.
//!
//! Since GD-0079 (the query planner: set operations, VALUES, WITH and
//! data-modifying WITH) core classes every planner statement `Other` in a
//! task session, while the server answers pg_catalog relations on its own
//! catalog path (`Plan::Catalog`, R3-312). `planner_shapes_with_catalog_stay_in_scope`
//! shows that the mixed shapes return no row outside the task's scope and no
//! masked or hidden value, over the agent PostgreSQL listener.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::ordinary_wire;
use agent_support::{
    AGENT_STARTUP, AgentServer, Msg, admin, binding, certificate, connect_raw, error_of, now_ms,
    timestamptz, DAY_MS,
};
use giggles_core::{
    CoreError,
    relational::Value as CoreValue,
    roles::{NewRole, TablePrivilege},
    task::{
        StatementClass, TaskError,
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, WallClockMs,
        },
    },
};
use giggles_server::{Binding, Request, Role, Session};

/// Values the task must never see: the out-of-scope row's status, the raw
/// statuses of in-scope rows (status is masked), the hidden column, the other
/// table and its data.
const SECRETS: &[&str] = &[
    "other-merchant-status",
    "failed-raw",
    "queued-raw",
    "merchant_id",
    "customers",
    "ssn",
    "123-45-6789",
    "77777",
];

/// The worked example plus: a superuser login `boss`, a second table, rows
/// in and out of the task's scope, and task `inc-436` on `shipments` with
/// `id` readable, `status` masked as `***` and `merchant_id` hidden, rows
/// `merchant_id = 42`.
fn seeded() -> giggles_core::Admin {
    let admin = admin();
    admin
        .roles()
        .create(NewRole {
            name: "boss".into(),
            superuser: true,
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let mut sql = admin.sql_session();
    for statement in [
        "INSERT INTO shipments VALUES (1, 42, 'failed-raw'), (2, 42, 'queued-raw'), \
         (3, 77777, 'other-merchant-status')",
        "CREATE TABLE customers (id bigint primary key, ssn text)",
        "INSERT INTO customers VALUES (1, '123-45-6789')",
    ] {
        sql.execute(statement, &[], None).unwrap();
    }
    admin
        .roles()
        .grant_table("shipments", "agent_exec", &[TablePrivilege::Select])
        .unwrap();
    let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into()],
                masked: vec![("status".into(), Some(CoreValue::Text("***".into())))],
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 1 << 20,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: timestamptz(now_ms() + DAY_MS),
            },
            WallClockMs(now_ms()),
        )
        .unwrap();
    admin
}

/// A running agent listener on the seeded database, the agent certificate
/// (bound to `inc-436`) and an ordinary certificate bound to `boss`.
async fn started() -> (AgentServer, agent_support::TestCert, agent_support::TestCert) {
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let ordinary = certificate(Some(&ca), false);
    let server = AgentServer::start(
        seeded(),
        ca,
        vec![Binding {
            fingerprint: ordinary.fingerprint(),
            role: Role::Database {
                name: "boss".into(),
            },
        }],
        vec![binding(&agent, "agent_exec", &["inc-436"])],
    )
    .await;
    (server, agent, ordinary)
}

fn agent_session(server: &AgentServer, agent: &agent_support::TestCert) -> Session {
    let session = server
        .service
        .agent_session(&agent.fingerprint(), Some("inc-436"))
        .unwrap();
    assert!(session.task().is_some());
    session
}

fn boss_session(server: &AgentServer) -> Session {
    let session = server.service.role_session("boss").unwrap();
    assert!(session.task().is_none());
    session
}

/// What an admin sees in `shipments` and the database's tables: nothing a
/// refused statement could have changed.
fn database_state(server: &AgentServer) -> (Vec<String>, Vec<String>) {
    let mut sql = server.admin.sql_session();
    let shipments = sql
        .execute(
            "SELECT id, merchant_id, status FROM shipments ORDER BY id",
            &[],
            None,
        )
        .unwrap();
    let tables = server.admin.catalog().unwrap().1;
    let mut names: Vec<String> = tables.into_iter().map(|t| t.name).collect();
    names.sort();
    (
        shipments.rows.iter().map(|r| format!("{:?}", r.values)).collect(),
        names,
    )
}

fn not_allowed(class: StatementClass) -> CoreError {
    CoreError::Task(TaskError::NotAllowedInTaskSession(class))
}

/// Statements of every class outside the profile, with the class each is
/// refused as; `None` is a statement core does not parse (role DDL, GRANT),
/// which must still be refused.
const OUTSIDE: &[(&str, Option<StatementClass>)] = &[
    ("BEGIN", Some(StatementClass::TransactionControl)),
    ("START TRANSACTION", Some(StatementClass::TransactionControl)),
    ("COMMIT", Some(StatementClass::TransactionControl)),
    ("SAVEPOINT s1", Some(StatementClass::Savepoint)),
    ("COPY shipments TO STDOUT", Some(StatementClass::Copy)),
    ("COPY shipments FROM STDIN", Some(StatementClass::Copy)),
    ("CREATE TABLE intruder (id bigint primary key)", Some(StatementClass::Ddl)),
    ("ALTER TABLE shipments ADD COLUMN note text", Some(StatementClass::Ddl)),
    ("DROP TABLE customers", Some(StatementClass::Ddl)),
    ("CREATE INDEX intruder_idx ON shipments (status)", Some(StatementClass::Ddl)),
    ("CREATE SEQUENCE intruder_seq", Some(StatementClass::Ddl)),
    ("COMMENT ON TABLE shipments IS 'x'", Some(StatementClass::Ddl)),
    ("INSERT INTO shipments VALUES (9, 42, 'x')", Some(StatementClass::Insert)),
    ("DELETE FROM shipments", Some(StatementClass::Delete)),
    ("TRUNCATE shipments", Some(StatementClass::Delete)),
    ("EXPLAIN SELECT id FROM shipments", Some(StatementClass::Explain)),
    ("WITH t AS (SELECT id FROM shipments) SELECT id FROM t", Some(StatementClass::Other)),
    ("SELECT id FROM shipments UNION ALL SELECT id FROM customers", Some(StatementClass::Other)),
    ("CREATE ROLE intruder LOGIN", None),
    ("ALTER ROLE agent_exec SUPERUSER", None),
    ("GRANT SELECT ON customers TO agent_exec", None),
    ("DROP ROLE boss", None),
];

#[tokio::test]
async fn execute_sql_in_task_session_cannot_reach_other_classes() {
    let (server, agent, _) = started().await;
    let before = database_state(&server);
    let mut session = agent_session(&server, &agent);
    for (sql, class) in OUTSIDE {
        let plain = session.execute_sql(sql, &[]);
        assert!(plain.is_err(), "{sql}: {plain:?}");
        let unreleased = session.execute_sql_unreleased(sql, &[]).map(|_| ());
        let prepared = session.prepare(sql).map(|_| ());
        for (path, result) in [("unreleased", unreleased), ("prepare", prepared)] {
            match class {
                Some(class) => {
                    assert_eq!(result, Err(not_allowed(*class)), "{path} {sql}")
                }
                None => assert!(result.is_err(), "{path} {sql}: {result:?}"),
            }
        }
        assert!(!session.in_transaction(), "{sql}");
    }
    // The session still works for what the profile allows.
    assert_eq!(session.handle(Request::Ping).code, "ok");
    let read = session
        .execute_sql_unreleased("SELECT id, status FROM shipments ORDER BY id", &[])
        .unwrap();
    let rows = session.release_page(read).unwrap().into_inner().rows;
    assert_eq!(
        rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![
            vec![CoreValue::Int(1), CoreValue::Text("***".into())],
            vec![CoreValue::Int(2), CoreValue::Text("***".into())],
        ]
    );
    drop(session);
    assert_eq!(database_state(&server), before);
    let roles = server.admin.roles();
    assert!(roles.get("intruder").unwrap().is_none());
    assert!(!roles.get("agent_exec").unwrap().unwrap().superuser);
    assert!(roles.get("boss").unwrap().is_some());
    assert!(
        !roles
            .table_privileges("customers", "agent_exec")
            .unwrap()
            .contains(&TablePrivilege::Select)
    );
}

#[tokio::test]
async fn bare_update_through_execute_prepared_refused() {
    let (server, agent, _) = started().await;
    let before = database_state(&server);
    // Statements prepared on an ordinary session, handed to the task
    // session: the only gate left is the `authorize` backstop.
    let mut boss = boss_session(&server);
    let update = boss
        .prepare("UPDATE shipments SET status = 'pwned' WHERE merchant_id = 42")
        .unwrap();
    let insert = boss
        .prepare("INSERT INTO shipments VALUES (8, 42, 'pwned')")
        .unwrap();
    let delete = boss.prepare("DELETE FROM shipments").unwrap();
    let mut session = agent_session(&server, &agent);
    let operation_id = CoreError::Task(TaskError::OperationIdRequired);
    assert_eq!(session.execute_prepared(&update, &[]).map(drop), Err(operation_id.clone()));
    assert_eq!(
        session.execute_prepared_unreleased(&update, &[]).map(drop),
        Err(operation_id.clone())
    );
    assert_eq!(
        session
            .execute_sql_unreleased("UPDATE shipments SET status = 'pwned' WHERE id = 1", &[])
            .map(drop),
        Err(operation_id.clone())
    );
    assert_eq!(
        session
            .prepare("UPDATE shipments SET status = 'pwned' WHERE id = 1")
            .map(drop),
        Err(operation_id)
    );
    assert_eq!(
        session.execute_prepared_unreleased(&insert, &[]).map(drop),
        Err(not_allowed(StatementClass::Insert))
    );
    assert_eq!(
        session.execute_prepared_unreleased(&delete, &[]).map(drop),
        Err(not_allowed(StatementClass::Delete))
    );
    drop(session);
    assert_eq!(database_state(&server), before);
}

#[tokio::test]
async fn framed_request_on_agent_session_only_ping() {
    let (server, agent, _) = started().await;
    let before = database_state(&server);
    let mut session = agent_session(&server, &agent);
    assert_eq!(session.handle(Request::Ping).code, "ok");
    for request in [
        #[cfg(feature = "legacy-task")]
        Request::Status {
            operation: giggles_core::OperationId(1),
        },
        #[cfg(feature = "legacy-task")]
        Request::RepairSql {
            operation: giggles_core::OperationId(2),
            sql: "UPDATE shipments SET status = 'pwned'".into(),
            approval: None,
        },
        Request::Deadline {
            budget_ms: 1000,
            request: Box::new(Request::Ping),
        },
        Request::Deadline {
            budget_ms: 1000,
            request: Box::new(Request::Sql {
                sql: "SELECT id FROM shipments".into(),
                params: vec![],
            }),
        },
    ] {
        let shown = format!("{request:?}");
        let response = session.handle(request);
        assert_eq!(response.code, "0A000", "{shown}: {response:?}");
        assert!(response.sql.is_none() && response.operation.is_none(), "{response:?}");
    }
    // A framed `Sql` request goes on to the SQL backstop, which admits its
    // statement's own class.
    for (sql, code) in [
        ("BEGIN", "0A000"),
        ("CREATE TABLE intruder (id bigint primary key)", "0A000"),
        ("UPDATE shipments SET status = 'pwned'", "42T04"),
    ] {
        let response = session.handle(Request::Sql {
            sql: sql.into(),
            params: vec![],
        });
        assert_eq!(response.code, code, "{sql}: {response:?}");
    }
    assert_eq!(session.handle(Request::Ping).code, "ok");
    drop(session);
    assert_eq!(database_state(&server), before);
}

/// The body of `fn <name>` in `source`, from its signature to its closing
/// brace.
fn function_body<'a>(source: &'a str, name: &str, file: &str) -> &'a str {
    let start = source
        .find(&format!("fn {name}("))
        .unwrap_or_else(|| panic!("fn {name} not found in {file}"));
    let open = start + source[start..].find('{').unwrap();
    let mut depth = 0usize;
    for (offset, c) in source[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[start..open + offset + 1];
                }
            }
            _ => {}
        }
    }
    panic!("fn {name} in {file} is not closed")
}

/// Source lines without `//` comments.
fn code_lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines()
        .map(|line| line.split("//").next().unwrap_or("").trim())
}

#[test]
fn classification_matches_have_no_wildcard() {
    let server = env!("CARGO_MANIFEST_DIR");
    let read = |path: &str| {
        std::fs::read_to_string(format!("{server}/{path}"))
            .unwrap_or_else(|e| panic!("{path}: {e}"))
    };
    let core = "../giggles-core/src/relational/task_class.rs";
    for (file, functions) in [
        ("src/pgwire_task.rs", &["plan_class"][..]),
        ("src/api/native.rs", &["class", "request_class", "legacy_class"][..]),
        (core, &["task_class"][..]),
    ] {
        let source = read(file);
        for name in functions {
            let body = function_body(&source, name, file);
            for line in code_lines(body) {
                let wildcard = line.starts_with("_ =>")
                    || line.starts_with("_ if ")
                    || line.contains("| _ =>")
                    || line.contains("| _ if ");
                assert!(!wildcard, "{file} fn {name} has a wildcard arm: {line}");
            }
        }
    }
    // The session backstop's own classifiers are exhaustive too.
    let service = read("src/service.rs");
    for line in code_lines(function_body(&service, "execute_admin", "src/service.rs"))
        .take_while(|line| !line.starts_with("self.admit_backstop"))
    {
        assert!(!line.starts_with("_ =>"), "execute_admin: {line}");
    }
    // No implicit transaction is begun on the task path.
    let task = read("src/pgwire_task.rs");
    for line in code_lines(&task) {
        assert!(!line.contains("begin_implicit"), "pgwire_task.rs: {line}");
    }
    // The guard itself catches a wildcard.
    let planted = "fn plan_class(p: &Plan) -> X {\n    match p {\n        Plan::Empty => 1,\n        _ => 2,\n    }\n}";
    assert!(
        code_lines(function_body(planted, "plan_class", "planted")).any(|l| l.starts_with("_ =>"))
    );
}

/// Every message's text, lower-cased.
fn text(messages: &[Msg]) -> String {
    format!("{messages:?}").to_lowercase()
}

fn wire_rows(messages: &[Msg]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => Some(values.clone()),
            _ => None,
        })
        .collect()
}

/// The planner and mixed catalog shapes the coordinator named, plus the
/// pure-application WITH and set operation.
const PLANNER_SHAPES: &[&str] = &[
    "SELECT relname FROM pg_class UNION ALL SELECT status FROM shipments",
    "WITH c AS (SELECT relname FROM pg_class) SELECT * FROM c",
    "SELECT * FROM pg_class, shipments",
    "WITH t AS (SELECT * FROM shipments) SELECT * FROM t",
    "SELECT relname FROM pg_catalog.pg_class UNION SELECT status FROM shipments",
    "SELECT status FROM shipments UNION ALL SELECT relname FROM pg_class",
    "SELECT * FROM pg_class c JOIN shipments s ON true",
    "SELECT * FROM shipments, pg_class",
    "WITH c AS (SELECT relname FROM pg_class) SELECT c.relname, s.* FROM c, shipments s",
    "WITH t AS (SELECT * FROM shipments) SELECT relname FROM pg_class, t",
    "SELECT relname FROM pg_class WHERE relname IN (SELECT status FROM shipments)",
    "SELECT * FROM (SELECT * FROM shipments) s, pg_class",
    "VALUES ((SELECT status FROM shipments WHERE id = 3))",
    "SELECT ssn FROM customers UNION ALL SELECT relname FROM pg_class",
    "SELECT relname FROM pg_class UNION ALL SELECT ssn FROM customers",
    "WITH u AS (UPDATE shipments SET status = 'pwned' RETURNING *) SELECT * FROM u",
    "WITH d AS (DELETE FROM shipments RETURNING *) SELECT relname FROM pg_class",
];

#[tokio::test]
async fn planner_shapes_with_catalog_stay_in_scope() {
    let (server, agent, _) = started().await;
    let before = database_state(&server);
    let mut wire = connect_raw(server.agent.address, &server.ca, &agent, AGENT_STARTUP).await;
    for sql in PLANNER_SHAPES {
        for messages in [wire.simple(sql).await, wire.extended(sql, &[], 0).await] {
            assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}: {messages:?}");
            let all = text(&messages);
            for secret in SECRETS {
                assert!(!all.contains(secret), "{sql}: {secret} in {messages:?}");
            }
            if error_of(&messages).is_none() {
                // Answered: any application value is a masked status or an
                // in-scope id; the out-of-scope row (id 3) never shows up as
                // a shipments row.
                for row in wire_rows(&messages) {
                    assert!(
                        !row.iter().any(|v| v.as_deref() == Some("other-merchant-status")),
                        "{sql}: {row:?}"
                    );
                }
            }
        }
    }
    assert_eq!(database_state(&server), before);
    // The catalog engine joins only released rows: statuses equal to catalog
    // relation names, one in scope (masked) and one out of scope, never
    // match a predicate, and the hidden column cannot be filtered on.
    let mut sql = server.admin.sql_session();
    sql.execute(
        "INSERT INTO shipments VALUES (4, 42, 'pg_type'), (5, 77777, 'pg_namespace')",
        &[],
        None,
    )
    .unwrap();
    for oracle in [
        "SELECT relname FROM pg_class WHERE relname IN (SELECT status FROM shipments)",
        "SELECT c.relname FROM pg_class c JOIN shipments s ON c.relname = s.status",
        "SELECT c.relname FROM pg_class c, shipments s WHERE s.status = 'pg_type'",
        "SELECT c.relname FROM pg_class c, shipments s WHERE s.id = 5",
        "SELECT c.relname FROM pg_class c, shipments s WHERE s.merchant_id = 77777",
        "WITH c AS (SELECT relname FROM pg_class) SELECT relname FROM c, shipments s          WHERE c.relname = s.status",
    ] {
        for messages in [wire.simple(oracle).await, wire.extended(oracle, &[], 0).await] {
            assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{oracle}: {messages:?}");
            assert_eq!(wire_rows(&messages), Vec::<Vec<Option<String>>>::new(), "{oracle}");
        }
    }
    // Role DDL and GRANT on the agent listener are refused as well.
    for sql in ["CREATE ROLE intruder LOGIN", "GRANT SELECT ON customers TO agent_exec"] {
        let messages = wire.simple(sql).await;
        assert_eq!(
            error_of(&messages).map(|(code, _)| code),
            Some("0A000".to_owned()),
            "{sql}: {messages:?}"
        );
    }
    assert!(server.admin.roles().get("intruder").unwrap().is_none());
}

#[tokio::test]
async fn non_task_sessions_unaffected() {
    let (server, _, ordinary) = started().await;
    // In process: an ordinary role session runs every class the task
    // session was refused.
    let mut boss = boss_session(&server);
    assert_eq!(boss.handle(Request::Ping).code, "ok");
    for sql in [
        "BEGIN",
        "SAVEPOINT s1",
        "INSERT INTO shipments VALUES (9, 42, 'x')",
        "UPDATE shipments SET status = 'y' WHERE id = 9",
        "DELETE FROM shipments WHERE id = 9",
        "ROLLBACK",
        "CREATE TABLE extra (id bigint primary key)",
        "WITH t AS (SELECT id FROM shipments) SELECT id FROM t",
        "SELECT id FROM shipments UNION ALL SELECT id FROM customers",
        "EXPLAIN SELECT id FROM shipments",
        "DROP TABLE extra",
    ] {
        boss.execute_sql(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let update = boss.prepare("UPDATE shipments SET status = 'z' WHERE id = 99").unwrap();
    boss.execute_prepared(&update, &[]).unwrap();
    // COPY over execute_sql is not a task refusal outside a task session.
    let copy = boss.execute_sql("COPY shipments TO STDOUT", &[]);
    assert!(!matches!(copy, Err(CoreError::Task(_))), "{copy:?}");
    // Framed requests other than Ping keep their pre-R3 answer (no task
    // executor behind a database role), never the task refusal.
    #[cfg(feature = "legacy-task")]
    {
        let status = boss.handle(Request::Status {
            operation: giggles_core::OperationId(1),
        });
        assert_ne!(status.code, "0A000", "{status:?}");
    }
    drop(boss);

    // On the wire: the ordinary listener answers role DDL, GRANT, COPY,
    // transactions and the planner/catalog shapes for a non-task session.
    let (_listener, mut wire) = ordinary_wire(&server, &ordinary).await;
    for sql in [
        "BEGIN",
        "LOCK TABLE shipments IN ACCESS SHARE MODE",
        "COMMIT",
        "CREATE ROLE helper LOGIN",
        "GRANT SELECT ON customers TO helper",
        "COPY shipments TO STDOUT",
    ] {
        let messages = wire.simple(sql).await;
        assert_eq!(error_of(&messages), None, "{sql}: {messages:?}");
    }
    assert!(server.admin.roles().get("helper").unwrap().is_some());
    let all = wire
        .simple("SELECT relname FROM pg_class UNION ALL SELECT status FROM shipments")
        .await;
    let all_text = text(&all);
    for value in ["customers", "other-merchant-status", "failed-raw"] {
        assert!(all_text.contains(value), "{value} missing: {all:?}");
    }
    let catalog = wire
        .simple("SELECT relname FROM pg_class WHERE relnamespace = 2200 ORDER BY relname")
        .await;
    let catalog_text = text(&catalog);
    assert!(catalog_text.contains("customers"), "{catalog:?}");
    assert!(catalog_text.contains("shipments"), "{catalog:?}");
}

#[tokio::test]
async fn issue_credential_is_refused_without_writing() {
    let (server, agent, _) = started().await;
    let def = server.admin.tasks().get("inc-436").unwrap().unwrap();
    let before = server.admin.tasks().credentials(def.id, WallClockMs(now_ms())).unwrap();
    let mut wire = connect_raw(server.agent.address, &server.ca, &agent, AGENT_STARTUP).await;
    let reply = wire.simple("ISSUE CREDENTIAL FOR TASK \"inc-436\"").await;
    assert_eq!(error_of(&reply).unwrap().0, "0A000");
    assert_eq!(server.admin.tasks().credentials(def.id, WallClockMs(now_ms())).unwrap(), before);
}
