use super::*;
use crate::{Binding, Role, admission::RoleStatementLimits, clock::Clock};
use giggles_core::{Admin, Limits, relational::Value};
use proto::execute_request::Request;
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_FIXTURE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct Fixture {
    runtime: Arc<ApiRuntime>,
    owner: ConnectionIdentity,
    socket: Arc<OwnedSemaphorePermit>,
}

impl Fixture {
    fn new(sessions: usize) -> Self {
        let path = crate::test_support::fixture_root().join(format!(
            "api-wire-seam-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        sql.execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        sql.execute("INSERT INTO app (id) VALUES (1)", &[], None)
            .unwrap();

        let fingerprint = "11".repeat(32);
        let service = Service::new(
            admin.clone(),
            vec![Binding {
                fingerprint: fingerprint.clone(),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["app".into()]),
                },
            }],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let runtime = ApiRuntime::new(service.clone(), ApiLimits { sessions }).unwrap();
        let owner = runtime.authenticate(&fingerprint).unwrap();
        let socket = service.admission.connect().unwrap();
        Self {
            runtime,
            owner,
            socket,
        }
    }

    fn open_wire(&self) -> Result<WireSession, ApiError> {
        let (reply, session) = self
            .runtime
            .open_wire(&self.owner, &proto::OpenSessionRequest::default())?;
        assert!(!reply.session_id.is_empty());
        Ok(session.expect("binding open returns a session"))
    }

    fn call(&self) -> Arc<CallOwnership> {
        self.runtime.admit_call(self.socket.clone()).unwrap()
    }

    async fn execute(
        &self,
        token: &str,
        request: proto::ExecuteRequest,
    ) -> Result<proto::ExecuteReply, ApiError> {
        self.runtime
            .execute(
                &self.owner,
                token,
                request,
                Instant::now(),
                None,
                self.call(),
                Encoding::Protobuf,
                false,
            )
            .await
            .map(|completion| completion.reply)
    }

    async fn execute_wire(
        &self,
        session: &mut WireSession,
        request: proto::ExecuteRequest,
    ) -> Result<proto::ExecuteReply, ApiError> {
        let received = Instant::now();
        let budget_ms = request.budget_ms;
        let decoded = codec::decode_execute(request)?;
        let cancel = self.runtime.wire_cancellation(received, budget_ms)?;
        self.runtime
            .execute_wire(
                session,
                WireCall {
                    request: decoded.request,
                    operation_id: decoded.operation_id,
                    budget_ms,
                    received,
                    cancel,
                },
                self.call(),
            )
            .await
            .map(|completion| completion.reply)
    }

    fn ids(&self) -> Vec<Value> {
        self.runtime
            .service
            .admin
            .sql_session()
            .execute("SELECT id FROM app ORDER BY id LIMIT 10", &[], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row.values.into_iter().next().unwrap())
            .collect()
    }

    fn lease_cached(&self, session: &WireSession) -> Result<SessionLease, ApiError> {
        self.runtime.registry.lease_entry(
            &session.entry,
            &session.identity,
            Cancellation::default(),
        )
    }
}

fn request(request: Request) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(request),
        ..Default::default()
    }
}

fn ping() -> proto::ExecuteRequest {
    request(Request::Ping(proto::PingRequest {}))
}

fn native_text(text: &str) -> proto::NativeTextRequest {
    proto::NativeTextRequest {
        text: text.into(),
        ..Default::default()
    }
}

fn text(text: &str) -> proto::ExecuteRequest {
    request(Request::Text(native_text(text)))
}

fn with_operation_id(mut request: proto::ExecuteRequest) -> proto::ExecuteRequest {
    request.operation_id = Some("wire-test".into());
    request
}

fn task_preview() -> proto::ExecuteRequest {
    request(Request::TaskPreview(proto::TaskPreviewRequest {
        change: Some(proto::task_preview_request::Change::Text(native_text(
            "update app where id=1 set id=2",
        ))),
    }))
}

fn outcome(reply: &proto::ExecuteReply) -> &proto::NativeOutcome {
    let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) = &reply.native_result
    else {
        panic!("expected a native operation outcome, got {reply:?}");
    };
    outcome
}

#[tokio::test(flavor = "current_thread")]
async fn execute_wire_matches_execute() {
    let f = Fixture::new(2);
    let token = f.runtime.open(&f.owner).unwrap().session_id;
    let mut wire = f.open_wire().unwrap();
    let sql = request(Request::Sql(proto::SqlRequest {
        sql: "select 1".into(),
        params: vec![],
    }));
    let budget = proto::ExecuteRequest {
        budget_ms: Some(10_001),
        ..ping()
    };
    let with_id = with_operation_id(text("update app where id=1 set id=4"));
    assert!(
        codec::decode_execute(with_id.clone())
            .unwrap()
            .operation_id
            .is_some()
    );
    assert_eq!(
        codec::decode_execute(budget.clone()).err(),
        Some(ApiError::InvalidArgument)
    );

    for (name, existing, cached, expected_error) in [
        ("ping", ping(), ping(), None),
        ("sql", sql.clone(), sql, None),
        ("read", text("app select id"), text("app select id"), None),
        (
            "insert",
            text("insert app set id=2"),
            text("insert app set id=3"),
            None,
        ),
        (
            "budget",
            budget.clone(),
            budget,
            Some(ApiError::InvalidArgument),
        ),
        (
            "operation id",
            with_id.clone(),
            with_id,
            Some(ApiError::InvalidArgument),
        ),
    ] {
        let existing = f.execute(&token, existing).await;
        let cached = f.execute_wire(&mut wire, cached).await;
        assert_eq!(existing, cached, "{name}");
        assert_eq!(existing.as_ref().err().copied(), expected_error, "{name}");
        if let Ok(reply) = existing {
            assert_eq!(reply.code, "ok", "{name}");
            match name {
                "read" => assert_eq!(outcome(&reply).rows.len(), 1),
                "insert" => assert_eq!(outcome(&reply).affected, 1),
                _ => {}
            }
        }
    }
    assert_eq!(f.ids(), vec![Value::Int(1), Value::Int(2), Value::Int(3)]);

    let now = Instant::now();
    assert_eq!(
        f.runtime.wire_cancellation(now, Some(10_001)).err(),
        Some(ApiError::InvalidArgument)
    );
    assert!(f.runtime.wire_cancellation(now, Some(10_000)).is_ok());
    assert!(f.runtime.wire_cancellation(now, None).is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn execute_wire_task_forms_refused_off_agent() {
    let f = Fixture::new(2);
    let token = f.runtime.open(&f.owner).unwrap().session_id;
    let mut wire = f.open_wire().unwrap();
    for request in [
        task_preview(),
        with_operation_id(request(Request::TaskOutcome(proto::TaskOutcomeRequest {}))),
    ] {
        assert!(codec::decode_execute(request.clone()).is_ok());
        let existing = f.execute(&token, request.clone()).await;
        let cached = f.execute_wire(&mut wire, request).await;
        assert_eq!(existing, Err(ApiError::InvalidArgument));
        assert_eq!(cached, existing);
    }
    let existing = f.execute(&token, ping()).await;
    assert_eq!(existing.as_ref().unwrap().code, "ok");
    assert_eq!(f.execute_wire(&mut wire, ping()).await, existing);
}

#[tokio::test(flavor = "current_thread")]
async fn wire_identity_rechecked_after_binding_change() {
    for sweep_first in [false, true] {
        let f = Fixture::new(2);
        let token = f.runtime.open(&f.owner).unwrap().session_id;
        let mut wire = f.open_wire().unwrap();
        assert_eq!(f.execute_wire(&mut wire, ping()).await.unwrap().code, "ok");

        f.runtime.service.reload_bindings(vec![]).unwrap();
        assert!(wire.bindings.has_changed().unwrap());
        if sweep_first {
            // Invoke the same sweep the supervisor runs on notification.
            f.runtime.invalidate_bindings();
            assert_eq!(f.lease_cached(&wire).err(), Some(ApiError::SessionNotFound));
        }
        // Without the explicit sweep, the current-thread supervisor has not
        // polled since reload and this call's identity rejection is synchronous.
        let cached = f.execute_wire(&mut wire, ping()).await;
        let existing = f.execute(&token, ping()).await;
        assert_eq!(existing, Err(ApiError::Unauthenticated));
        assert_eq!(cached, existing);
        assert_eq!(f.lease_cached(&wire).err(), Some(ApiError::SessionNotFound));
        assert!(!wire.bindings.has_changed().unwrap());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn wire_identity_not_rechecked_without_binding_change() {
    let f = Fixture::new(1);
    let mut wire = f.open_wire().unwrap();
    assert!(!wire.bindings.has_changed().unwrap());
    assert_eq!(f.execute_wire(&mut wire, ping()).await.unwrap().code, "ok");

    f.runtime.service.reload_bindings(vec![]).unwrap();
    // Mark the notification seen to isolate the unchanged-watch branch. A
    // task form stops at request validation, so no authority checks in the
    // worker or during lease release can mask an identity check at this seam.
    wire.bindings.borrow_and_update();
    assert_eq!(
        wire.identity.check(&f.runtime.service),
        Err(ApiError::Unauthenticated)
    );
    assert_eq!(
        f.execute_wire(&mut wire, task_preview()).await,
        Err(ApiError::InvalidArgument)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn execute_wire_role_admission_matches() {
    let f = Fixture::new(2);
    let token = f.runtime.open(&f.owner).unwrap().session_id;
    let mut wire = f.open_wire().unwrap();
    let service = &f.runtime.service;
    service
        .set_role_statement_limits(RoleStatementLimits {
            concurrent: 1,
            queue: 0,
            ..Default::default()
        })
        .unwrap();
    let _held = service
        .admission
        .statement(&f.owner.role, None)
        .await
        .unwrap();

    let existing = f.execute(&token, ping()).await;
    assert_eq!(existing, Err(ApiError::ResourceExhausted));
    assert_eq!(f.execute_wire(&mut wire, ping()).await, existing);
}

#[tokio::test(flavor = "current_thread")]
async fn close_wire_rolls_back_open_transaction_and_frees_slot() {
    let f = Fixture::new(1);
    let mut wire = f.open_wire().unwrap();
    assert_eq!(f.open_wire().err(), Some(ApiError::ResourceExhausted));
    let begin = f
        .execute_wire(&mut wire, request(Request::Begin(proto::BeginRequest {})))
        .await
        .unwrap();
    assert_eq!(begin.code, "ok");
    assert_eq!(
        begin.transaction_state,
        proto::TransactionState::InTransaction as i32
    );
    let insert = f
        .execute_wire(&mut wire, text("insert app set id=2"))
        .await
        .unwrap();
    assert_eq!(insert.code, "ok");
    assert_eq!(outcome(&insert).affected, 1);
    let read = f
        .execute_wire(&mut wire, text("app select id"))
        .await
        .unwrap();
    assert_eq!(outcome(&read).rows.len(), 2);

    f.runtime.close_wire(wire);
    assert_eq!(f.ids(), vec![Value::Int(1)]);
    let mut replacement = f.open_wire().unwrap();
    assert_eq!(
        f.execute_wire(&mut replacement, ping()).await.unwrap().code,
        "ok"
    );
    assert_eq!(f.open_wire().err(), Some(ApiError::ResourceExhausted));
}
