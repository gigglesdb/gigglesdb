//! GD-0061 R3-313: the native API, typed reads and the SQL reply of a task
//! session go through the release protocol inside `native::run` (or the
//! `Request::Sql` arm) before anything encodes them, and the read log records
//! the transport. Driven through `execute_reply`, the function both HTTP and
//! gRPC call. `Select` is in `task::admit::ENABLED` since R3-315; the
//! cfg(test) route (`agent::statement::test_route`) admits only `Insert`
//! here, to reach core's own refusal of a task write.
use super::*;
use crate::{AgentBinding, Role, agent::statement::test_route};
use giggles_core::{
    Admin, Engine, Limits,
    relational::Value,
    roles::{NewRole, TablePrivilege},
    task::{
        StatementClass,
        definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText},
        readlog::{ENTRY_GRPC, ENTRY_HTTP, class_code},
    },
};
use serde_json::{Value as Json, json};

/// Rows of merchant 42 (the task's scope); merchant 7 has two more.
const ROWS: usize = 5;
const SQL: &str = "SELECT id, status FROM shipments ORDER BY id";
const FINGERPRINT: &str =
    "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `shipments` with `ROWS` rows of merchant 42 (status `failed`) and two of
/// merchant 7 (status `secret`); task `inc-436` sees merchant 42, reads `id`
/// and `merchant_id`, and gets `status` masked as `***`. Exact debits.
fn fixture() -> Arc<Service> {
    let path = crate::test_support::fixture_root().join(format!(
        "task-release-native-{}-{}-{}",
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
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
        "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'failed'), (3, 42, 'failed'), \
         (4, 42, 'failed'), (5, 42, 'failed'), (6, 7, 'secret'), (7, 7, 'secret')",
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
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Insert])
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
                readable: vec!["id".into(), "merchant_id".into()],
                masked: vec![("status".into(), Some(Value::Text("***".into())))],
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 10_000,
                    rows_returned: 10_000,
                    bytes_returned: 10_000_000,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2999-01-01 00:00:00+00".into(),
            },
            now,
        )
        .unwrap();
    Service::with_agent(
        admin,
        vec![],
        vec![AgentBinding {
            fingerprint: FINGERPRINT.into(),
            executor: "agent_exec".into(),
            tasks: vec!["inc-436".into()],
        }],
        clock,
        crate::admission::ServiceLimits::default(),
    )
    .unwrap()
}

fn agent(service: &Arc<Service>) -> Session {
    let session = service.agent_session(FINGERPRINT, None).unwrap();
    session.sql.set_disclosure_chunk(0, 0);
    session
}

fn task_id(service: &Service) -> giggles_core::task::definition::TaskObjectId {
    service.admin.tasks().get("inc-436").unwrap().unwrap().id
}

/// The invocation a JSON request decodes to, as on the HTTP path.
fn invocation(request: &Json) -> Invocation {
    let wire = json::decode_execute(&serde_json::to_vec(request).unwrap()).unwrap();
    codec::decode_execute(wire).unwrap().request
}

/// One request through `execute_reply`; the reply and its JSON view.
fn exec(session: &mut Session, request: &Json, encoding: Encoding) -> (proto::ExecuteReply, Json) {
    let (reply, _) =
        execute_reply(session, invocation(request), None, std::time::Instant::now(), encoding).unwrap();
    let view = serde_json::from_slice(&json::encode_reply(&reply).unwrap()).unwrap();
    (reply, view)
}

fn column(name: &str) -> Json {
    json!({"column": {"name": name}})
}

fn typed_read() -> Json {
    json!({"typedRead": {"table": "shipments",
        "projection": [{"expression": column("id"), "alias": "id"},
            {"expression": column("status"), "alias": "status"}],
        "orderBy": [{"expression": column("id"), "descending": false}], "limit": "100"}})
}

fn native_read_request() -> Json {
    json!({"read": {"table": "shipments", "order": [{"field": "id", "descending": false}],
        "limit": {"literal": {"intValue": "100"}}, "projection": ["id", "status"]}})
}

fn native_read() -> Json {
    json!({"operation": {"request": native_read_request(), "bindings": []}})
}

fn sql(text: &str) -> Json {
    json!({"sql": {"sql": text}})
}

/// The `(id, status)` pairs of a reply's rows (operation outcome or SQL).
fn rows(view: &Json) -> Vec<(String, String)> {
    let rows = view["operationOutcome"]["rows"]
        .as_array()
        .or_else(|| view["sql"]["rows"].as_array())
        .unwrap_or_else(|| panic!("no rows: {view}"));
    rows.iter()
        .map(|row| {
            let values = &row["values"];
            (
                values[0]["intValue"].as_str().unwrap().to_owned(),
                values[1]["textValue"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn masked_rows() -> Vec<(String, String)> {
    (1..=ROWS).map(|id| (id.to_string(), "***".to_owned())).collect()
}

/// A typed read is released exactly once, inside `run`: one page, one debit,
/// one read-log record of the HTTP entry, masked values on the wire, and no
/// row outside the task's scope.
#[test]
fn typed_read_released_once() {
    let service = fixture();
    let mut session = agent(&service);
    let (reply, view) = exec(&mut session, &typed_read(), Encoding::Json);
    assert_eq!(reply.code, "ok", "{view}");
    assert_eq!(rows(&view), masked_rows());
    let text = view.to_string();
    assert!(!text.contains("failed") && !text.contains("secret"), "{text}");
    let task = task_id(&service);
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.rows_returned, ROWS as u64);
    assert_eq!(disclosure.debits.len(), 1, "{disclosure:?}");
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.entry, ENTRY_HTTP);
    assert_eq!(record.class, class_code(StatementClass::Select));
    assert_eq!(record.pages, 1);
    assert_eq!(record.released_rows, ROWS as u64);
    assert_eq!(record.masked_values, ROWS as u64);
    assert!(record.text.contains("shipments"), "{}", record.text);
    // The plain exits stay refused in a task session.
    let Invocation::Read(read) = invocation(&typed_read()) else { panic!("typed read") };
    assert!(session.sql.execute_typed_read(*read, None).is_err());
}

/// A native operation's rows are masked and charged before encoding. A
/// native write with RETURNING is refused before it runs (R3-308): no row
/// is inserted, nothing is charged, and its refusal is logged once.
#[test]
fn native_returning_masked_and_charged() {
    test_route::enable(&[StatementClass::Insert]);
    let service = fixture();
    let mut session = agent(&service);
    let (reply, view) = exec(&mut session, &native_read(), Encoding::Json);
    assert_eq!(reply.code, "ok", "{view}");
    assert_eq!(rows(&view), masked_rows());
    let task = task_id(&service);
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!((disclosure.rows_returned, disclosure.debits.len()), (ROWS as u64, 1));
    let insert = json!({"operation": {"request": {"returning": {
        "request": {"insert": {"table": "shipments", "values": [
            {"column": "id", "value": {"literal": {"intValue": "50"}}},
            {"column": "merchant_id", "value": {"literal": {"intValue": "42"}}},
            {"column": "status", "value": {"literal": {"textValue": "new"}}}]}},
        "projection": [{"expression": {"field": "id"}, "alias": "id"},
            {"expression": {"field": "status"}, "alias": "status"}]}}, "bindings": []}});
    let (reply, view) = exec(&mut session, &insert, Encoding::Json);
    assert_ne!(reply.code, "ok", "{view}");
    assert!(view.get("operationOutcome").is_none(), "{view}");
    let count = service
        .admin
        .sql_session()
        .execute("SELECT count(*) FROM shipments", &[], None)
        .unwrap();
    assert_eq!(count.rows[0].values[0], Value::Int(7));
    let after = service.admin.task_disclosure(task).unwrap();
    assert_eq!((after.rows_returned, after.debits.len()), (ROWS as u64, 1));
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[0].class, class_code(StatementClass::Select));
    assert_eq!((records[0].released_rows, records[0].masked_values), (ROWS as u64, ROWS as u64));
    // Core logs a native RETURNING wrapper as `Other` (its `request_class`
    // does not look inside the wrapper; admission does).
    assert_eq!(records[1].class, class_code(StatementClass::Other));
    assert_eq!(records[1].sqlstate_text(), "0A000");
    assert_eq!((records[1].released_rows, records[1].pages), (0, 0));
    test_route::enable(&[]);
}

/// The same request costs the same over HTTP (JSON) and gRPC (protobuf):
/// equal debits and equal replies; only the read-log entry differs.
#[test]
fn http_and_grpc_same_charge() {
    let service = fixture();
    let task = task_id(&service);
    for request in [native_read(), typed_read(), sql(SQL)] {
        let mut replies = Vec::new();
        for encoding in [Encoding::Json, Encoding::Protobuf] {
            let mut session = agent(&service);
            let (reply, view) = exec(&mut session, &request, encoding);
            assert_eq!(reply.code, "ok", "{view}");
            assert_eq!(rows(&view), masked_rows());
            replies.push(reply);
        }
        assert_eq!(replies[0], replies[1], "{request}");
    }
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.debits.len(), 6, "{disclosure:?}");
    for pair in disclosure.debits.chunks(2) {
        assert_eq!(
            (pair[0].rows_returned, pair[0].bytes),
            (pair[1].rows_returned, pair[1].bytes),
            "{disclosure:?}"
        );
        assert_eq!(pair[0].rows_returned, ROWS as u64);
    }
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(
        records.iter().map(|r| r.entry).collect::<Vec<_>>(),
        [ENTRY_HTTP, ENTRY_GRPC, ENTRY_HTTP, ENTRY_GRPC, ENTRY_HTTP, ENTRY_GRPC]
    );
    for pair in records.chunks(2) {
        assert_eq!(
            (pair[0].released_rows, pair[0].released_bytes, &pair[0].text),
            (pair[1].released_rows, pair[1].released_bytes, &pair[1].text)
        );
    }
}

/// SQL over the API (`Request::Sql`, the framed reply both transports
/// encode) is released before the Response is built: masked, charged and
/// logged with its text. A refused statement is logged once and charges
/// nothing.
#[test]
fn framed_sql_reply_released() {
    let service = fixture();
    let task = task_id(&service);
    let mut session = agent(&service);
    let (reply, view) = exec(&mut session, &sql(SQL), Encoding::Json);
    assert_eq!(reply.code, "ok", "{view}");
    assert_eq!(rows(&view), masked_rows());
    // The framed handler itself.
    let response = session.handle(crate::Request::Sql {
        sql: SQL.into(),
        params: vec![],
    });
    assert_eq!(response.code, "ok");
    let result = response.sql.unwrap();
    assert_eq!(result.rows.len(), ROWS);
    assert!(result.rows.iter().all(|row| row.values[1] == Value::Text("***".into())));
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!((disclosure.rows_returned, disclosure.debits.len()), (2 * ROWS as u64, 2));
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 2, "{records:?}");
    for record in &records {
        assert_eq!(record.text, SQL);
        assert_eq!(record.class, class_code(StatementClass::Select));
        assert_eq!((record.pages, record.released_rows), (1, ROWS as u64));
    }
    assert_eq!(records[0].entry, ENTRY_HTTP);
    // Refused by admission (DELETE is not enabled): one record, nothing
    // charged.
    const DELETE: &str = "DELETE FROM shipments WHERE id = 1";
    let (reply, _) = exec(&mut session, &sql(DELETE), Encoding::Protobuf);
    assert_eq!(reply.code, "0A000");
    assert!(reply.sql.is_none_or(|sql| sql.rows.is_empty()));
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 3, "{records:?}");
    assert_eq!(
        (records[2].entry, records[2].pages, records[2].text.as_str()),
        (ENTRY_GRPC, 0, DELETE)
    );
    let after = service.admin.task_disclosure(task).unwrap();
    assert_eq!((after.rows_returned, after.debits.len()), (2 * ROWS as u64, 2));
}

/// Outside a task session the release is a move: the same requests return
/// every row unmasked and nothing is logged or charged.
#[test]
fn non_task_native_unchanged() {
    let service = fixture();
    let mut session = service.role_session("incident_controller").unwrap();
    for request in [native_read(), typed_read(), sql(SQL)] {
        for encoding in [Encoding::Json, Encoding::Protobuf] {
            let (reply, view) = exec(&mut session, &request, encoding);
            assert_eq!(reply.code, "ok", "{view}");
            let rows = rows(&view);
            assert_eq!(rows.len(), 7);
            assert_eq!(rows[0], ("1".to_owned(), "failed".to_owned()));
            assert_eq!(rows[6], ("7".to_owned(), "secret".to_owned()));
        }
    }
    let task = task_id(&service);
    assert!(service.admin.task_read_log(task).unwrap().is_empty());
    assert_eq!(service.admin.task_disclosure(task).unwrap().debits.len(), 0);
}

// GD-0091 Stack 6b-C: an agent request on the native wire, through
// execute_wire. A multi-thread runtime runs it inline on the connection
// task; a current-thread runtime sends it to the API pool (run_job).

fn open_agent_wire(runtime: &ApiRuntime) -> WireSession {
    let identity = runtime.authenticate(FINGERPRINT).unwrap();
    runtime
        .open_wire(&identity, &proto::OpenSessionRequest::default())
        .unwrap()
        .1
        .unwrap()
}

fn launch_wire(
    runtime: &Arc<ApiRuntime>,
    mut session: WireSession,
    request: &Json,
    budget_ms: u32,
) -> JoinHandle<(WireSession, Result<Completion, ApiError>)> {
    let runtime = runtime.clone();
    let request = invocation(request);
    let ownership = runtime
        .admit_call(runtime.service.admission.connect().unwrap())
        .unwrap();
    tokio::spawn(async move {
        let received = Instant::now();
        let call = WireCall {
            request,
            operation_id: None,
            budget_ms: Some(budget_ms),
            received,
            cancel: runtime.wire_cancellation(received, Some(budget_ms)).unwrap(),
        };
        let result = runtime.execute_wire(&mut session, call, ownership).await;
        (session, result)
    })
}

fn agent_role() -> Role {
    Role::Database {
        name: "agent_exec".into(),
    }
}

/// (available, concurrent, queued) of the agent role's statement pool.
fn role_pool(service: &Service) -> (usize, usize, usize) {
    let pools = service.admission.role_pool_counts();
    assert_eq!(pools.len(), 1, "{pools:?}");
    pools[0]
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The refusal reply (code, transaction state) of a request that waits for a
/// full agent admission through the refusal delay with a 10 ms budget, and
/// the code of the session's next request once a slot is free.
fn saturated_agent_refusal(runtime: tokio::runtime::Runtime) -> (String, i32, String) {
    runtime.block_on(async {
        let service = fixture();
        service
            .set_agent_limits(crate::agent::AgentLimits {
                max_concurrent_statements: 1,
                ceiling_refusal_delay_ms: 100,
                ..service.agent_limits()
            })
            .unwrap();
        let api = ApiRuntime::new_agent(service.clone(), ApiLimits::default()).unwrap();
        let session = open_agent_wire(&api);
        let held = service.agent_admission.try_admit().unwrap();
        let started = Instant::now();
        let (session, refused) = launch_wire(&api, session, &sql(SQL), 10).await.unwrap();
        // Both paths wait out the refusal delay, not the 10 ms budget.
        assert!(started.elapsed() >= Duration::from_millis(100));
        let refused = refused.unwrap().reply;
        drop(held);
        let (_session, next) = launch_wire(&api, session, &sql(SQL), 5_000).await.unwrap();
        let next = next.unwrap().reply;
        assert_eq!(service.agent_requests_admitted(), 0);
        api.shutdown().await;
        (refused.code, refused.transaction_state, next.code)
    })
}

#[test]
fn inline_agent_refusal_matches_pool_after_budget_expires() {
    let inline = saturated_agent_refusal(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );
    let pool = saturated_agent_refusal(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
    assert_eq!(inline, pool);
    assert_eq!(inline.0, "54T01", "{inline:?}");
    assert_eq!(inline.2, "ok", "{inline:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_agent_request_holds_every_permit_through_after_execution() {
    assert!(ApiRuntime::wire_runs_inline());
    let service = fixture();
    let api = ApiRuntime::new_agent(service.clone(), ApiLimits::default()).unwrap();
    let session = open_agent_wire(&api);
    let gate = super::tests::gate(&api, true);
    let task = launch_wire(&api, session, &sql(SQL), 5_000);
    super::tests::signal(gate.entered).await;
    // The barrier holds the job after run_admitted_job's work.
    assert_eq!(service.agent_requests_admitted(), 1);
    let (available, concurrent, queued) = role_pool(&service);
    assert_eq!((available, queued), (concurrent - 1, 0));
    assert_eq!(service.blocking_permits_held(), 1);
    gate.release.send(()).unwrap();
    let (_session, result) = task.await.unwrap();
    assert_eq!(result.unwrap().reply.code, "ok");
    super::tests::signal(gate.finished).await;
    assert_eq!(service.agent_requests_admitted(), 0);
    let (available, concurrent, _) = role_pool(&service);
    assert_eq!(available, concurrent);
    assert_eq!(service.blocking_permits_held(), 0);
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_agent_request_takes_agent_slot_before_role_and_blocking() {
    assert!(ApiRuntime::wire_runs_inline());
    let service = fixture();
    service
        .set_role_statement_limits(crate::admission::RoleStatementLimits {
            concurrent: 1,
            queue: 1,
            wait_ms: 5_000,
        })
        .unwrap();
    let api = ApiRuntime::new_agent(service.clone(), ApiLimits::default()).unwrap();
    let session = open_agent_wire(&api);
    let role = service
        .admission
        .statement(&agent_role(), None)
        .await
        .unwrap();
    let task = launch_wire(&api, session, &sql(SQL), 5_000);
    until("the request to queue for the role permit", || {
        role_pool(&service) == (0, 1, 1)
    })
    .await;
    // Waiting for the role permit, it already owns the agent slot and has
    // not taken a blocking permit.
    assert_eq!(service.agent_requests_admitted(), 1);
    assert_eq!(service.blocking_permits_held(), 0);
    drop(role);
    let (_session, result) = task.await.unwrap();
    assert_eq!(result.unwrap().reply.code, "ok");
    assert_eq!(service.agent_requests_admitted(), 0);
    assert_eq!(role_pool(&service), (1, 1, 0));
    assert_eq!(service.blocking_permits_held(), 0);
    api.shutdown().await;
}
