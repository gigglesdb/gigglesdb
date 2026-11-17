//! R3-207 seam -> R3-615: the direct protected write through
//! `Session::execute_protected`. `Update` and `OutcomeLookup` are in
//! `task::admit::ENABLED` (R3-626, design-2 section 4); `Preview` and `Apply`
//! are not, so the fail-closed test admits them through `test_route`.
//!
//! Task "requeue failed for merchant 42, 25 rows" on `shipments` (`deliveries`
//! is reserved): visible rows `merchant_id = 42`, write `status` from 'failed'
//! to 'queued'.
use super::{OperationId, ProtectedStatement, test_route};
use crate::{AgentBinding, Service, Session};
use giggles_core::{
    Admin, CoreError, Engine, Limits,
    relational::{Prepared, SqlResult, Value},
    roles::{NewRole, TablePrivilege},
    sqlstate,
    task::{
        DenyRule, StatementClass, TaskError,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
        },
    },
};
use std::sync::Arc;

const REQUEUE: &str =
    "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND status = 'failed'";
const FINGERPRINT: &str = "ab";
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn fixture() -> (Arc<Service>, Session) {
    let path = crate::test_support::fixture_root().join(format!(
        "agent-seam-{}-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    for statement in [
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         attempt_count int)",
        "INSERT INTO shipments VALUES (1, 42, 'failed', 1), (2, 42, 'failed', 2), \
         (3, 42, 'failed', 1), (4, 42, 'failed', 3), (5, 42, 'delivered', 0), (6, 7, 'failed', 1)",
    ] {
        admin.sql_session().execute(statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let clock = crate::clock::Clock::new(0, 3600).unwrap();
    let now = crate::agent::wall_now(&clock).unwrap();
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
                readable: ["id", "merchant_id", "status", "attempt_count"]
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: Some(AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText("status = 'failed'".into()),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: MutationCaps {
                    distinct_rows: 25,
                    transitions: 25,
                },
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 2_097_152,
                },
                limits: LoadLimits::default(),
                approval_threshold: 25,
                expires: "2999-01-01 00:00:00+00".into(),
            },
            now,
        )
        .unwrap();
    let fingerprint = FINGERPRINT.repeat(32);
    let service = Service::with_agent(
        admin,
        vec![],
        vec![AgentBinding {
            fingerprint: fingerprint.clone(),
            executor: "agent_exec".into(),
            tasks: vec!["inc-436".into()],
        }],
        clock,
        crate::admission::ServiceLimits::default(),
    )
    .unwrap();
    let session = service.agent_session(&fingerprint, None).unwrap();
    (service, session)
}

fn statuses(service: &Service) -> Vec<String> {
    service
        .admin
        .sql_session()
        .execute("SELECT status FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match &row.values[0] {
            Value::Text(text) => text.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

const BEFORE: [&str; 6] = ["failed", "failed", "failed", "failed", "delivered", "failed"];
const AFTER: [&str; 6] = ["queued", "queued", "queued", "queued", "delivered", "failed"];

fn prepare(session: &mut Session, class: StatementClass, sql: &str) -> Result<Prepared, CoreError> {
    session.prepare_protected(class, sql, &[])
}

/// `OPERATION '<op>' <sql>` as pgwire builds it: the wrapped UPDATE prepared in
/// the protected scope, then run through `execute_protected`.
fn direct(
    session: &mut Session,
    op: &str,
    sql: &str,
    params: &[Value],
) -> Result<SqlResult, CoreError> {
    let prepared = prepare(session, StatementClass::Update, sql)?;
    let statement = ProtectedStatement::Direct {
        op: OperationId::new(op).unwrap(),
        prepared: Box::new(prepared),
        text: Arc::from(sql),
    };
    let result = session.execute_protected(&statement, params);
    assert!(!session.in_protected(), "in_protected leaked: {result:?}");
    result
}

fn assert_bare_update_needs_operation_id(session: &mut Session) {
    assert_eq!(
        session.prepare(REQUEUE).map(|_| ()),
        Err(CoreError::Task(TaskError::OperationIdRequired))
    );
}

#[test]
fn direct_update_commits_and_returns_the_receipt_count() {
    let (service, mut session) = fixture();
    let result = direct(&mut session, "inc-436:requeue.1", REQUEUE, &[]).unwrap();
    assert_eq!(result.affected, 4);
    assert!(result.columns.is_empty() && result.rows.is_empty());
    assert_eq!(statuses(&service), AFTER);
    // Parameters travel to the commit; zero rows still commits.
    let result = direct(
        &mut session,
        "inc-436:requeue.2",
        "UPDATE shipments SET status = $1 WHERE merchant_id = 42 AND status = $2",
        &[Value::Text("queued".into()), Value::Text("failed".into())],
    )
    .unwrap();
    assert_eq!(result.affected, 0);
    assert_eq!(statuses(&service), AFTER);
    assert_bare_update_needs_operation_id(&mut session);
}

#[test]
fn update_outside_allowed_columns_is_denied_and_writes_nothing() {
    let (service, mut session) = fixture();
    let error = direct(
        &mut session,
        "inc-436:bad.1",
        "UPDATE shipments SET status = 'queued', attempt_count = 9 WHERE id = 1",
        &[],
    )
    .unwrap_err();
    assert_eq!(
        error,
        CoreError::Task(TaskError::Denied(DenyRule::ColumnNotWritable))
    );
    assert_eq!(
        sqlstate(&error),
        TaskError::Denied(DenyRule::ColumnNotWritable).sqlstate()
    );
    assert_eq!(statuses(&service), BEFORE);
    let attempts = service
        .admin
        .sql_session()
        .execute("SELECT attempt_count FROM shipments WHERE id = 1", &[], None)
        .unwrap();
    assert_eq!(attempts.rows[0].values, [Value::Int(1)]);
    // The session is usable afterwards and the flag did not leak.
    assert_bare_update_needs_operation_id(&mut session);
    assert_eq!(direct(&mut session, "inc-436:ok.1", REQUEUE, &[]).unwrap().affected, 4);
}

#[test]
fn reused_operation_id_replays_or_conflicts() {
    let (service, mut session) = fixture();
    assert_eq!(direct(&mut session, "op-1", REQUEUE, &[]).unwrap().affected, 4);
    // Make the rows match again: a replay must not run the statement.
    service
        .admin
        .sql_session()
        .execute("UPDATE shipments SET status = 'failed' WHERE id = 1", &[], None)
        .unwrap();
    let replay = direct(&mut session, "op-1", REQUEUE, &[]).unwrap();
    assert_eq!(replay.affected, 4);
    assert_eq!(
        statuses(&service),
        ["failed", "queued", "queued", "queued", "delivered", "failed"]
    );
    // Same ID, another request: 42T03, nothing written.
    for (sql, params) in [
        (
            "UPDATE shipments SET status = 'queued' WHERE id = 1 AND status = 'failed'",
            vec![],
        ),
        (
            "UPDATE shipments SET status = $1 WHERE merchant_id = 42 AND status = $2",
            vec![Value::Text("queued".into()), Value::Text("failed".into())],
        ),
    ] {
        let error = direct(&mut session, "op-1", sql, &params).unwrap_err();
        assert_eq!(error, CoreError::Task(TaskError::OperationConflict), "{sql}");
        assert_eq!(sqlstate(&error), "42T03");
    }
    // The same text with other parameters is another request too.
    let sql = "UPDATE shipments SET status = $1 WHERE id = $2 AND status = 'failed'";
    let params = |id| [Value::Text("queued".into()), Value::Int(id)];
    assert_eq!(direct(&mut session, "op-2", sql, &params(1)).unwrap().affected, 1);
    assert_eq!(
        direct(&mut session, "op-2", sql, &params(6)).unwrap_err(),
        CoreError::Task(TaskError::OperationConflict)
    );
    assert_eq!(statuses(&service), AFTER);
}

#[test]
fn update_class_is_admitted_without_the_route() {
    let (service, mut session) = fixture();
    test_route::enable(&[]);
    assert_eq!(session.admit(StatementClass::Update), Ok(()));
    // Prepared and run with no route: the protected commit runs.
    let result = direct(&mut session, "op-1", REQUEUE, &[]).unwrap();
    assert_eq!(result.affected, 4);
    assert_eq!(statuses(&service), AFTER);
    assert_bare_update_needs_operation_id(&mut session);
}

#[test]
fn returning_is_refused_before_anything_runs() {
    let (service, mut session) = fixture();
    let error = direct(&mut session, "op-r", &format!("{REQUEUE} RETURNING id"), &[]).unwrap_err();
    assert_eq!(error, CoreError::Unsupported);
    assert_eq!(sqlstate(&error), "0A000");
    assert_eq!(statuses(&service), BEFORE);
    // The ID was not consumed.
    assert_eq!(direct(&mut session, "op-r", REQUEUE, &[]).unwrap().affected, 4);
}

#[test]
fn preview_and_apply_still_fail_closed() {
    let (service, mut session) = fixture();
    test_route::enable(&[StatementClass::Preview, StatementClass::Apply]);
    let prepared = prepare(&mut session, StatementClass::Preview, REQUEUE).unwrap();
    let op = OperationId::new("op-1").unwrap();
    for statement in [
        ProtectedStatement::Preview {
            prepared: Box::new(prepared),
        },
        ProtectedStatement::Apply {
            op,
            preview: 1,
            digest: [0; 32],
        },
    ] {
        let error = session.execute_protected(&statement, &[]).unwrap_err();
        assert_eq!(
            error,
            CoreError::Task(TaskError::NotAllowedInTaskSession(statement.class()))
        );
        assert_eq!(sqlstate(&error), "0A000");
        assert!(!session.in_protected());
    }
    assert_eq!(statuses(&service), BEFORE);
}

#[test]
fn protected_flag_is_cleared_after_a_panic() {
    let (_service, mut session) = fixture();
    assert!(!session.panic_in_protected_scope());
    assert_bare_update_needs_operation_id(&mut session);
}

fn show(session: &mut Session, op: &str) -> Result<SqlResult, CoreError> {
    let statement = ProtectedStatement::Outcome {
        op: OperationId::new(op).unwrap(),
    };
    let result = session.execute_protected(&statement, &[]);
    assert!(!session.in_protected(), "in_protected leaked: {result:?}");
    result
}

#[test]
fn show_operation_returns_the_stored_outcome() {
    let (service, mut session) = fixture();
    // Unknown: no row.
    let unknown = show(&mut session, "op-1").unwrap();
    assert_eq!(
        unknown.columns,
        ["operation_id", "decision", "row_count", "receipt_seq", "effects_digest"]
    );
    assert!(unknown.rows.is_empty());
    // Committed: the receipt's count, seq and digest.
    assert_eq!(direct(&mut session, "op-1", REQUEUE, &[]).unwrap().affected, 4);
    let committed = show(&mut session, "op-1").unwrap();
    assert_eq!(committed.rows.len(), 1);
    let values = &committed.rows[0].values;
    assert_eq!(
        values[..4],
        [
            Value::Text("op-1".into()),
            Value::Text("committed".into()),
            Value::Int(4),
            Value::Int(1),
        ]
    );
    match &values[4] {
        Value::Text(hex) => assert!(hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())),
        other => panic!("{other:?}"),
    }
    // Denied: the decision only.
    direct(
        &mut session,
        "op-2",
        "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42",
        &[],
    )
    .unwrap_err();
    let denied = show(&mut session, "op-2").unwrap();
    assert_eq!(
        denied.rows[0].values,
        [
            Value::Text("op-2".into()),
            Value::Text("denied".into()),
            Value::Null,
            Value::Null,
            Value::Null,
        ]
    );
    // The lookup ran nothing.
    assert_eq!(statuses(&service), AFTER);
}

#[test]
fn outcome_class_is_admitted_without_the_route() {
    let (_service, mut session) = fixture();
    test_route::enable(&[]);
    assert_eq!(session.admit(StatementClass::OutcomeLookup), Ok(()));
    assert!(show(&mut session, "op-1").unwrap().rows.is_empty());
}
