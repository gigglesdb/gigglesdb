//! In-process management authentication and authorization checks.
use bytes::Bytes;
use http::{Request, Response};
use nunya::{
    mgmt::{
        audit::{Actor, AuditUnavailable, LifecycleKind, LifecycleRecord, LifecycleSink},
        auth::Origin,
        routes::{self, ConnectionSource, ConnectionView, MgmtContext, StatusSource},
    },
    pgwire::auth::ScramVerifier,
    state::{
        authority::{AcquireRequest, Authority, ConfigPrincipal, SessionLimits},
        model::*,
        store::RedbStore,
    },
    token::{PrincipalId, SessionId, keys::RootKey},
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[derive(Default)]
struct AuditSink {
    records: Mutex<Vec<LifecycleRecord>>,
    failing: AtomicBool,
}
impl LifecycleSink for AuditSink {
    fn record(&self, record: &LifecycleRecord) -> Result<(), AuditUnavailable> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(AuditUnavailable);
        }
        self.records.lock().unwrap().push(record.clone());
        Ok(())
    }
}
struct Sources;
impl ConnectionSource for Sources {
    fn list(&self) -> Vec<ConnectionView> {
        vec![ConnectionView {
            id: 7,
            session: "session-id".into(),
            principal: "principal-id".into(),
            route: "postgres".into(),
        }]
    }
}
impl StatusSource for Sources {
    fn policy_revision(&self) -> Option<String> {
        Some("policy-1".into())
    }
    fn config_revision(&self) -> Option<String> {
        Some("config-2".into())
    }
    fn route_ids(&self) -> Vec<String> {
        vec!["postgres".into()]
    }
}
struct Fixture {
    ctx: Option<MgmtContext>,
    dir: PathBuf,
    principals: Vec<PrincipalId>,
    sessions: Vec<SessionId>,
    forbidden: Vec<String>,
    clock: Arc<AtomicU64>,
    audit: Arc<AuditSink>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.ctx.take();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("ny-{}", &SessionId::random().to_hex()[..8]));
        std::fs::create_dir_all(&dir).unwrap();
        let clock = Arc::new(AtomicU64::new(1000));
        let c = clock.clone();
        let store = Arc::new(
            RedbStore::with_clock(
                &dir,
                &RootKey::from_hex(&"72".repeat(32)).unwrap(),
                Arc::new(move || c.load(Ordering::SeqCst)),
            )
            .unwrap(),
        );
        let authority = Authority::load(store, SessionLimits::default()).unwrap();
        let mut forbidden = vec![];
        let mut make_verifier = |secret: &str| {
            let salt: &[u8] = if secret == "session-private.secret" {
                b"session-fixture-salt"
            } else {
                b"management-fixture-salt"
            };
            let v = ScramVerifier::from_password(secret, salt, 4096);
            forbidden.extend([
                secret.to_owned(),
                v.to_pg_string(),
                data_encoding::HEXLOWER.encode(&v.stored_key),
                data_encoding::HEXLOWER.encode(&v.server_key),
            ]);
            // Principal salts remain private; session salts are public.
            if secret != "session-private.secret" {
                forbidden.extend([
                    data_encoding::HEXLOWER.encode(salt),
                    data_encoding::BASE64.encode(salt),
                ]);
            }
            Verifier::try_from(v.to_pg_string()).unwrap()
        };
        let mut principals = vec![];
        let mut sessions = vec![];
        for (name, secret, admin) in [
            ("alice", "alice-private.secret", false),
            ("bob", "bob-private.secret", false),
            ("admin", "admin-private.secret", true),
        ] {
            let p = authority
                .create_principal(name, PrincipalKind::Agent, admin, name)
                .unwrap();
            authority
                .add_principal_credential(p.id, make_verifier(secret), None)
                .unwrap();
            let route = RouteId::new("postgres").unwrap();
            authority
                .set_grants(p.id, vec![(route.clone(), Mode::ReadWrite)])
                .unwrap();
            let s = authority
                .acquire_session(
                    p.id,
                    AcquireRequest {
                        route,
                        mode: None,
                        lifetime_ms: None,
                        isolated: false,
                        label: Some("test-session".into()),
                        idempotency_key: "acquire".into(),
                        verifier: make_verifier("session-private.secret"),
                        secret_sha256: [0xbd; 32],
                    },
                )
                .unwrap();
            principals.push(p.id);
            sessions.push(s.id);
        }
        authority
            .sync_config_principals(&[ConfigPrincipal {
                name: "configured".into(),
                verifier: make_verifier("configured-private.secret"),
            }])
            .unwrap();
        let admin = make_verifier("config-admin-private.secret");
        let audit = Arc::new(AuditSink::default());
        let ctx = MgmtContext {
            authority,
            admins: vec![("config-admin".into(), admin.0)],
            connections: Arc::new(Sources),
            status: Arc::new(Sources),
            lifecycle: audit.clone(),
        };
        for s in ctx.authority.all_sessions() {
            let scope = ctx.authority.scope(s.scope).unwrap();
            forbidden.push(data_encoding::HEXLOWER.encode(&scope.salt));
        }
        forbidden.push("bd".repeat(32));
        Self {
            ctx: Some(ctx),
            dir,
            principals,
            sessions,
            forbidden,
            clock,
            audit,
        }
    }
    fn ctx(&self) -> &MgmtContext {
        self.ctx.as_ref().unwrap()
    }
    fn request(
        &self,
        origin: Origin,
        auth: Option<&str>,
        method: &str,
        path: &str,
        key: Option<&str>,
    ) -> Response<Bytes> {
        self.request_body(origin, auth, method, path, key, Bytes::new())
    }
    fn request_body(
        &self,
        origin: Origin,
        auth: Option<&str>,
        method: &str,
        path: &str,
        key: Option<&str>,
        body: Bytes,
    ) -> Response<Bytes> {
        let mut req = Request::builder().method(method).uri(path);
        if let Some(auth) = auth {
            req = req.header("Authorization", auth);
        }
        if let Some(key) = key {
            req = req.header("Idempotency-Key", key);
        }
        let r = routes::handle(self.ctx(), origin, req.body(body).unwrap());
        let text = std::str::from_utf8(r.body()).unwrap();
        for secret in &self.forbidden {
            assert!(!text.contains(secret), "response leaked fixture material");
        }
        assert_audit_material(self);
        r
    }
}
const ALICE: &str = "Bearer nunya.alice.alice-private.secret";
const BOB: &str = "Bearer nunya.bob.bob-private.secret";
const ADMIN: &str = "Bearer nunya.admin.admin-private.secret";
const CONFIG: &str = "Bearer nunya.config-admin.config-admin-private.secret";

#[test]
fn endpoint_authorization_matrix_and_uniform_auth_failures() {
    let f = Fixture::new();
    let sid = f.sessions[0].to_hex();
    let pid = f.principals[0].to_hex();
    let cid = CredentialId([6; 16]).to_hex();
    // Expected statuses for owner, other principal, store admin, config admin, socket.
    let rows = vec![
        ("POST", "/v1/sessions".into(), [400, 400, 400, 403, 403]),
        (
            "GET",
            format!("/v1/sessions/{sid}"),
            [200, 404, 200, 200, 200],
        ),
        (
            "POST",
            format!("/v1/sessions/{sid}/renew"),
            [200, 404, 404, 403, 403],
        ),
        (
            "POST",
            format!("/v1/sessions/{sid}/revoke"),
            [200, 404, 200, 200, 200],
        ),
        ("GET", "/v1/sessions".into(), [200, 200, 200, 200, 200]),
        ("POST", "/v1/principals".into(), [403, 403, 400, 400, 400]),
        ("GET", "/v1/principals".into(), [403, 403, 200, 200, 200]),
        (
            "GET",
            format!("/v1/principals/{pid}"),
            [403, 403, 200, 200, 200],
        ),
        (
            "POST",
            format!("/v1/principals/{pid}/disable"),
            [403, 403, 200, 200, 200],
        ),
        (
            "POST",
            format!("/v1/principals/{pid}/enable"),
            [403, 403, 200, 200, 200],
        ),
        (
            "POST",
            format!("/v1/principals/{pid}/credentials"),
            [403, 403, 400, 400, 400],
        ),
        (
            "DELETE",
            format!("/v1/principals/{pid}/credentials/{cid}"),
            [403, 403, 404, 404, 404],
        ),
        (
            "PUT",
            format!("/v1/principals/{pid}/grants"),
            [403, 403, 400, 400, 400],
        ),
        ("GET", "/v1/connections".into(), [403, 403, 200, 200, 200]),
        ("GET", "/v1/status".into(), [403, 403, 200, 200, 200]),
    ];
    let session_auth = format!("Bearer nunya.session:{sid}.session-private.secret");
    let mut unauthorized = None;
    for (method, path, statuses) in rows {
        for auth in [
            None,
            Some("Bearer nunya.alice.wrong"),
            Some("Bearer nunya.unknown.wrong"),
            Some(session_auth.as_str()),
        ] {
            let r = f.request(Origin::Network, auth, method, &path, Some("retry"));
            assert_eq!(r.status(), 401, "{method} {path}");
            assert_eq!(r.body().as_ref(), br#"{"error":"unauthorized"}"#);
            assert_eq!(r.headers()["WWW-Authenticate"], "Bearer");
            let signature = (r.headers().clone(), r.body().clone());
            if let Some(previous) = &unauthorized {
                assert_eq!(&signature, previous);
            } else {
                unauthorized = Some(signature);
            }
        }
        for (i, (origin, auth)) in [
            (Origin::Network, Some(ALICE)),
            (Origin::Network, Some(BOB)),
            (Origin::Network, Some(ADMIN)),
            (Origin::Network, Some(CONFIG)),
            (Origin::Socket, None),
        ]
        .into_iter()
        .enumerate()
        {
            let r = f.request(origin, auth, method, &path, Some("retry"));
            assert_eq!(
                r.status().as_u16(),
                statuses[i],
                "{method} {path} caller {i}"
            );
            if path.ends_with("/disable") && r.status() == 200 {
                f.ctx()
                    .authority
                    .set_principal_enabled(f.principals[0], true)
                    .unwrap();
            }
            if method != "GET"
                && (statuses[i] == 503
                    || statuses[i] == 200
                    || (path == "/v1/sessions" && statuses[i] == 400))
            {
                let r = f.request(origin, auth, method, &path, None);
                assert_eq!(r.status(), 400);
                assert_eq!(
                    r.body().as_ref(),
                    br#"{"error":"idempotency_key_required"}"#
                );
            }
        }
    }
}
#[test]
fn reads_are_scoped_sorted_and_secret_free() {
    let f = Fixture::new();
    let read = |auth: &str, path: &str| -> serde_json::Value {
        serde_json::from_slice(
            f.request(Origin::Network, Some(auth), "GET", path, None)
                .body(),
        )
        .unwrap()
    };
    let own = read(ALICE, "/v1/sessions");
    assert_eq!(own.as_array().unwrap().len(), 1);
    assert_eq!(own[0]["principal"], f.principals[0].to_hex());
    assert_eq!(own[0]["scope_continuity"], true);
    let all = read(ADMIN, "/v1/sessions");
    assert_eq!(all.as_array().unwrap().len(), 3);
    let ids: Vec<_> = all
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
    let principals = read(ADMIN, "/v1/principals");
    let names: Vec<_> = principals
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["admin", "alice", "bob", "configured"]);
    let p = read(
        ADMIN,
        &format!("/v1/principals/{}", f.principals[0].to_hex()),
    );
    assert_eq!(p["grants"][0]["route"], "postgres");
    assert_eq!(p["credentials"].as_array().unwrap().len(), 1);
    assert!(p["credentials"][0].get("verifier").is_none());
    assert_eq!(read(ADMIN, "/v1/connections")[0]["id"], 7);
    let status = read(ADMIN, "/v1/status");
    assert_eq!(
        status["deployment_id"],
        data_encoding::HEXLOWER.encode(&f.ctx().authority.deployment_id())
    );
    assert_eq!(
        status["store_format"],
        f.ctx().authority.store_format_version()
    );
    assert_eq!(status["route_ids"], serde_json::json!(["postgres"]));
    assert_eq!(status["policy_revision"], "policy-1");
    assert_eq!(status["config_revision"], "config-2");
}
#[test]
fn invalid_routes_keys_disabled_credentials_and_admin_shadowing() {
    let mut f = Fixture::new();
    for (method, path, status) in [
        ("GET", "/unknown", 404),
        ("PATCH", "/v1/status", 405),
        ("GET", "/v1/sessions/bad", 404),
        ("GET", "/v1/principals/bad", 404),
        ("DELETE", "/v1/principals/abcd/credentials/bad", 404),
        ("GET", "/v1/sessions/00000000000000000000000000000000", 404),
    ] {
        assert_eq!(
            f.request(Origin::Network, Some(ADMIN), method, path, None)
                .status()
                .as_u16(),
            status
        );
    }
    for key in ["", "contains space", &"a".repeat(129)] {
        assert_eq!(
            f.request(
                Origin::Network,
                Some(ALICE),
                "POST",
                "/v1/sessions",
                Some(key)
            )
            .status(),
            400
        );
    }
    f.ctx()
        .authority
        .set_principal_enabled(f.principals[0], false)
        .unwrap();
    assert_eq!(
        f.request(Origin::Network, Some(ALICE), "GET", "/v1/sessions", None)
            .status(),
        401
    );
    let configured = "Bearer nunya.configured.configured-private.secret";
    assert_eq!(
        f.request(
            Origin::Network,
            Some(configured),
            "GET",
            "/v1/sessions",
            None
        )
        .status(),
        200
    );
    let shadow = ScramVerifier::from_password("shadow-private", b"shadow-salt", 4096);
    f.forbidden.extend([
        "shadow-private".into(),
        data_encoding::HEXLOWER.encode(b"shadow-salt"),
        shadow.to_pg_string(),
        data_encoding::HEXLOWER.encode(&shadow.stored_key),
        data_encoding::HEXLOWER.encode(&shadow.server_key),
    ]);
    f.ctx.as_mut().unwrap().admins.push(("bob".into(), shadow));
    assert_eq!(
        f.request(Origin::Network, Some(BOB), "GET", "/v1/sessions", None)
            .status(),
        401
    );
}

impl Fixture {
    fn session_body(&mut self, secret: &str) -> serde_json::Value {
        use sha2::{Digest, Sha256};
        let verifier = ScramVerifier::from_password(secret, b"session-mutation-salt", 4096);
        let hash = data_encoding::HEXLOWER.encode(&Sha256::digest(secret.as_bytes()));
        self.forbidden.extend([
            secret.into(),
            verifier.to_pg_string(),
            hash.clone(),
            data_encoding::HEXLOWER.encode(&verifier.stored_key),
            data_encoding::HEXLOWER.encode(&verifier.server_key),
        ]);
        serde_json::json!({"route":"postgres", "verifier":verifier.to_pg_string(), "secret_sha256":hash})
    }
    fn post(&self, auth: &str, path: &str, key: &str, body: serde_json::Value) -> Response<Bytes> {
        self.request_body(
            Origin::Network,
            Some(auth),
            "POST",
            path,
            Some(key),
            Bytes::from(serde_json::to_vec(&body).unwrap()),
        )
    }
}
fn value(response: &Response<Bytes>) -> serde_json::Value {
    serde_json::from_slice(response.body()).unwrap()
}
fn error_is(response: Response<Bytes>, status: u16, code: &str) {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(value(&response), serde_json::json!({"error":code}));
}
#[test]
fn acquire_mutations_validation_idempotency_and_secret_checks() {
    let mut f = Fixture::new();
    let body = f.session_body("acquired-private.secret");
    let other = f.session_body("different-private.secret");
    let response = f.post(ALICE, "/v1/sessions", "new", body.clone());
    assert_eq!(response.status(), 201);
    let session = value(&response);
    let id = SessionId::from_hex(session["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        f.post(ALICE, "/v1/sessions", "new", body.clone()).body(),
        response.body()
    );
    assert!(
        f.ctx()
            .authority
            .check_session_secret(id, b"acquired-private.secret")
    );
    assert!(
        !f.ctx()
            .authority
            .check_session_secret(id, b"wrong-private.secret")
    );
    error_is(
        f.post(ALICE, "/v1/sessions", "new", other),
        409,
        "idempotency_conflict",
    );
    f.ctx().authority.revoke(id, EndReason::Requested).unwrap();
    error_is(
        f.post(ALICE, "/v1/sessions", "new", body.clone()),
        409,
        "session_ended",
    );
    for (field, replacement) in [
        ("verifier", serde_json::json!("malformed")),
        ("secret_sha256", serde_json::json!("AB".repeat(32))),
        ("secret_sha256", serde_json::json!("ab".repeat(31))),
        ("secret_sha256", serde_json::json!("gg".repeat(32))),
        ("unknown", serde_json::json!(true)),
        ("label", serde_json::json!("x".repeat(257))),
        ("label", serde_json::json!("bad\nlabel")),
        ("lifetime", serde_json::json!("not-a-duration")),
        ("route", serde_json::json!("")),
        ("mode", serde_json::json!("unknown")),
    ] {
        let mut bad = body.clone();
        bad[field] = replacement;
        error_is(
            f.post(ALICE, "/v1/sessions", "bad", bad),
            400,
            "bad_request",
        );
    }
    let mut too_long = body.clone();
    too_long["lifetime"] = serde_json::json!("9h");
    error_is(
        f.post(ALICE, "/v1/sessions", "long", too_long),
        400,
        "lifetime_too_long",
    );
    let mut no_grant = body.clone();
    no_grant["route"] = serde_json::json!("other");
    error_is(
        f.post(ALICE, "/v1/sessions", "no-grant", no_grant),
        403,
        "no_grant",
    );
    f.ctx()
        .authority
        .set_grants(
            f.principals[0],
            vec![(RouteId::new("postgres").unwrap(), Mode::ReadOnly)],
        )
        .unwrap();
    let mut wide = body.clone();
    wide["mode"] = serde_json::json!("read_write");
    error_is(
        f.post(ALICE, "/v1/sessions", "wide", wide),
        403,
        "mode_exceeds_grant",
    );
    let mut valid = body;
    valid["mode"] = serde_json::json!("read_only");
    valid["lifetime"] = serde_json::json!("30m");
    valid["isolated"] = serde_json::json!(true);
    valid["label"] = serde_json::json!("audit label");
    let r = f.post(ALICE, "/v1/sessions", "valid", valid);
    assert_eq!(r.status(), 201);
    assert_eq!(value(&r)["expires_ms"], 1_801_000);
    assert_eq!(value(&r)["isolated"], true);
}
#[test]
fn rotation_parameter_mismatch_is_atomic() -> TransportResult {
    use sha2::{Digest, Sha256};
    let mut f = Fixture::new();
    let body = f.session_body("rotation-original");
    let acquired = f.post(ALICE, "/v1/sessions", "rotation-acquire", body);
    assert_eq!(acquired.status(), 201);
    let view = value(&acquired);
    let sid = view["id"].as_str().ok_or("missing session id")?;
    let id = SessionId::from_hex(sid).ok_or("invalid session id")?;
    let before = f.ctx().authority.session(id).ok_or("missing session")?;
    let params = f
        .ctx()
        .authority
        .session_scram_params(id)
        .ok_or("missing SCRAM parameters")?;
    let detail = f.request(
        Origin::Network,
        Some(ALICE),
        "GET",
        &format!("/v1/sessions/{sid}"),
        None,
    );
    let detail_value = value(&detail);
    assert!(detail_value.get("scram_salt").is_none());
    assert!(detail_value.get("scram_iterations").is_none());
    assert!(detail_value.get("policy_revision").is_some());
    assert!(detail_value.get("config_revision").is_some());
    let list = value(&f.request(Origin::Network, Some(ALICE), "GET", "/v1/sessions", None));
    for session in list.as_array().ok_or("missing session list")? {
        assert!(session.get("scram_salt").is_none());
        assert!(session.get("scram_iterations").is_none());
    }
    let original = ScramVerifier::from_password("rotation-original", &params.0, params.1);
    for key in [original.stored_key, original.server_key] {
        assert!(
            !String::from_utf8_lossy(detail.body()).contains(&data_encoding::BASE64.encode(&key))
        );
    }
    for (salt, iterations, retry) in [
        (b"different-salt".as_slice(), params.1, "salt"),
        (params.0.as_slice(), params.1 + 1, "iterations"),
    ] {
        let verifier = ScramVerifier::from_password("refused-rotation-secret", salt, iterations);
        error_is(f.post(ALICE, &format!("/v1/sessions/{sid}/renew"), retry, serde_json::json!({
            "mode":"read_only", "add_credential": {"verifier":verifier.to_pg_string(), "secret_sha256":data_encoding::HEXLOWER.encode(&Sha256::digest(b"refused-rotation-secret"))}
        })), 400, "rotation_salt_mismatch");
        let after = f.ctx().authority.session(id).ok_or("missing session")?;
        assert_eq!(after.mode, before.mode);
        assert_eq!(after.last_used_ms, before.last_used_ms);
        assert!(
            !f.ctx()
                .authority
                .check_session_secret(id, b"refused-rotation-secret")
        );
        assert_eq!(
            f.ctx().authority.session_scram_params(id),
            Some(params.clone())
        );
    }
    f.clock.store(before.expires_ms, Ordering::SeqCst);
    let expired = value(&f.request(
        Origin::Network,
        Some(ALICE),
        "GET",
        &format!("/v1/sessions/{sid}"),
        None,
    ));
    assert!(expired.get("scram_salt").is_none());
    assert!(expired.get("scram_iterations").is_none());
    let list = value(&f.request(Origin::Network, Some(ALICE), "GET", "/v1/sessions", None));
    for session in list.as_array().ok_or("missing session list")? {
        assert!(session.get("scram_salt").is_none());
        assert!(session.get("scram_iterations").is_none());
    }
    Ok(())
}

#[test]
fn rotation_on_ended_sessions_returns_session_ended_and_records_refusal() {
    for expired in [true, false] {
        let mut f = Fixture::new();
        let id = f.sessions[0];
        let sid = id.to_hex();
        let rotation = f.session_body("ended-rotation-private.secret");
        let body = serde_json::json!({"add_credential": {
            "verifier": rotation["verifier"],
            "secret_sha256": rotation["secret_sha256"]
        }});
        if expired {
            let session = f.ctx().authority.session(id).unwrap();
            f.clock.store(session.expires_ms + 1, Ordering::SeqCst);
        } else {
            f.ctx().authority.revoke(id, EndReason::Requested).unwrap();
        }
        error_is(
            f.post(
                ALICE,
                &format!("/v1/sessions/{sid}/renew"),
                "ended-rotation",
                body,
            ),
            409,
            "session_ended",
        );
        let records = assert_audit_material(&f);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, LifecycleKind::ResumeRefused);
        assert_eq!(records[0].reason, "session_ended");
        assert_eq!(records[0].session.as_deref(), Some(sid.as_str()));
        assert_eq!(records[0].principal, f.principals[0].to_hex());
        assert_eq!(records[0].actor, Actor::Owner);
        assert_eq!(records[0].at_ms, f.clock.load(Ordering::SeqCst));
    }
}

#[test]
fn renew_mutations_credentials_narrowing_and_ownership() {
    let mut f = Fixture::new();
    let acquire = f.session_body("original-private.secret");
    let additional = f.session_body("additional-private.secret");
    let acquired = value(&f.post(ALICE, "/v1/sessions", "renew-acquire", acquire));
    let id = SessionId::from_hex(acquired["id"].as_str().unwrap()).unwrap();
    let path = format!("/v1/sessions/{}/renew", id.to_hex());
    let body = serde_json::json!({"mode":"read_only", "add_credential": {
        "verifier":additional["verifier"], "secret_sha256":additional["secret_sha256"]}});
    let r = f.post(ALICE, &path, "renew", body.clone());
    assert_eq!(r.status(), 200);
    assert_eq!(value(&r)["mode"], "read_only");
    assert_eq!(f.post(ALICE, &path, "renew", body).body(), r.body());
    assert!(
        f.ctx()
            .authority
            .check_session_secret(id, b"original-private.secret")
    );
    assert!(
        f.ctx()
            .authority
            .check_session_secret(id, b"additional-private.secret")
    );
    error_is(
        f.post(ALICE, &path, "renew", serde_json::json!({})),
        409,
        "idempotency_conflict",
    );
    error_is(
        f.post(ADMIN, &path, "admin-renew", serde_json::json!({})),
        404,
        "not_found",
    );
    for bad in [
        serde_json::json!({"unknown":true}),
        serde_json::json!({"add_credential":{"verifier":"bad","secret_sha256":"bad"}}),
    ] {
        error_is(f.post(ALICE, &path, "bad-renew", bad), 400, "bad_request");
    }
    assert_eq!(
        f.request(
            Origin::Network,
            Some(ALICE),
            "POST",
            &path,
            Some("empty-renew")
        )
        .status(),
        200
    );
    f.ctx().authority.revoke(id, EndReason::Requested).unwrap();
    error_is(
        f.post(ALICE, &path, "ended-renew", serde_json::json!({})),
        409,
        "session_ended",
    );
    error_is(
        f.post(
            ALICE,
            &path,
            "renew",
            serde_json::json!({"mode":"read_only", "add_credential":{
        "verifier":additional["verifier"], "secret_sha256":additional["secret_sha256"]}}),
        ),
        409,
        "session_ended",
    );
}
#[test]
fn revoke_mutations_owner_admin_retry_and_admission() {
    use nunya::state::authority::Operation;
    let f = Fixture::new();
    let path = format!("/v1/sessions/{}/revoke", f.sessions[0].to_hex());
    error_is(
        f.post(BOB, &path, "other", serde_json::json!({})),
        404,
        "not_found",
    );
    error_is(
        f.post(ALICE, &path, "bad", serde_json::json!({"reason":"secret"})),
        400,
        "bad_request",
    );
    let r = f.post(ALICE, &path, "owner", serde_json::json!({}));
    assert_eq!(r.status(), 200);
    assert_eq!(value(&r)["revoke_reason"], "requested");
    assert_eq!(
        f.request(Origin::Network, Some(ALICE), "POST", &path, Some("owner"))
            .body(),
        r.body()
    );
    assert!(
        f.ctx()
            .authority
            .admit(f.sessions[0], Operation::Read)
            .is_err()
    );
    let path = format!("/v1/sessions/{}/revoke", f.sessions[1].to_hex());
    let r = f.request(Origin::Network, Some(ADMIN), "POST", &path, Some("admin"));
    assert_eq!(r.status(), 200);
    assert_eq!(value(&r)["revoke_reason"], "admin_revoked");
    assert_eq!(
        f.post(ADMIN, &path, "admin", serde_json::json!({})).body(),
        r.body()
    );
    assert!(
        f.ctx()
            .authority
            .admit(f.sessions[1], Operation::Read)
            .is_err()
    );
    let other_path = format!("/v1/sessions/{}/revoke", f.sessions[2].to_hex());
    error_is(
        f.post(ADMIN, &other_path, "admin", serde_json::json!({})),
        409,
        "idempotency_conflict",
    );
    assert_eq!(
        f.request(Origin::Socket, None, "POST", &other_path, Some("socket"))
            .status(),
        200
    );
}

impl Fixture {
    fn mutation(
        &self,
        method: &str,
        path: &str,
        key: &str,
        body: serde_json::Value,
    ) -> Response<Bytes> {
        self.request_body(
            Origin::Network,
            Some(ADMIN),
            method,
            path,
            Some(key),
            Bytes::from(serde_json::to_vec(&body).unwrap()),
        )
    }
    fn principal_verifier(&mut self, secret: &str) -> String {
        self.session_body(secret)["verifier"]
            .as_str()
            .unwrap()
            .to_owned()
    }
    fn authenticates(&self, auth: &str, status: u16) {
        assert_eq!(
            self.request(Origin::Network, Some(auth), "GET", "/v1/sessions", None)
                .status()
                .as_u16(),
            status
        );
    }
}
#[test]
fn principal_creation_validation_authentication_and_retries() {
    use serde_json::json;
    let mut f = Fixture::new();
    let verifier = f.principal_verifier("created-private.secret");
    let body = json!({"name":"created", "kind":"service", "verifier":verifier});
    let first = f.post(ADMIN, "/v1/principals", "create", body.clone());
    assert_eq!(first.status(), 201);
    assert_eq!(
        f.post(ADMIN, "/v1/principals", "create", body).body(),
        first.body()
    );
    let pid = value(&first)["id"].as_str().unwrap().to_owned();
    assert_eq!(
        f.request(
            Origin::Network,
            Some(ADMIN),
            "GET",
            &format!("/v1/principals/{pid}"),
            None
        )
        .body(),
        first.body()
    );
    f.authenticates("Bearer nunya.created.created-private.secret", 200);
    let bare = f.post(
        ADMIN,
        "/v1/principals",
        "bare",
        json!({"name":"bare", "kind":"person"}),
    );
    assert_eq!(bare.status(), 201);
    assert_eq!(value(&bare)["credentials"], json!([]));
    assert_eq!(value(&bare)["administrator"], false);
    error_is(
        f.post(
            ADMIN,
            "/v1/principals",
            "create",
            json!({"name":"different", "kind":"service"}),
        ),
        409,
        "idempotency_conflict",
    );
    for name in ["created", "configured"] {
        error_is(
            f.post(
                ADMIN,
                "/v1/principals",
                name,
                json!({"name":name, "kind":"agent"}),
            ),
            409,
            "name_taken",
        );
    }
    for body in [
        json!({"name":"bad.name", "kind":"person"}),
        json!({"name":"invalid", "kind":"robot"}),
        json!({"name":"invalid", "kind":"person", "verifier":"bad"}),
        json!({"name":"invalid", "kind":"person", "extra":1}),
    ] {
        error_is(
            f.post(ADMIN, "/v1/principals", "invalid", body),
            400,
            "bad_request",
        );
    }
    assert!(f.ctx().authority.principal_by_name("invalid").is_none());
}
#[test]
fn principal_state_and_config_ownership() {
    use serde_json::json;
    let mut f = Fixture::new();
    let base = format!("/v1/principals/{}", f.principals[0].to_hex());
    let disabled = f.post(ADMIN, &format!("{base}/disable"), "disable", json!({}));
    assert_eq!(disabled.status(), 200);
    assert_eq!(
        f.post(ADMIN, &format!("{base}/disable"), "disable", json!({}))
            .body(),
        disabled.body()
    );
    let session = f.ctx().authority.session(f.sessions[0]).unwrap();
    assert_eq!(session.revoke_reason, Some(EndReason::PrincipalDisabled));
    f.authenticates(ALICE, 401);
    assert_eq!(
        f.post(ADMIN, &format!("{base}/enable"), "enable", json!({}))
            .status(),
        200
    );
    f.authenticates(ALICE, 200);
    assert_eq!(
        f.ctx().authority.session(f.sessions[0]).unwrap().state,
        SessionState::Revoked
    );
    let configured = f
        .ctx()
        .authority
        .principal_by_name("configured")
        .unwrap()
        .id;
    let base = format!("/v1/principals/{}", configured.to_hex());
    let verifier = f.principal_verifier("blocked-private.secret");
    for (method, suffix, body) in [
        ("POST", "disable", json!({})),
        ("POST", "enable", json!({})),
        ("POST", "credentials", json!({"verifier":verifier})),
        (
            "DELETE",
            "credentials/00000000000000000000000000000000",
            json!({}),
        ),
    ] {
        error_is(
            f.mutation(method, &format!("{base}/{suffix}"), suffix, body),
            409,
            "config_managed",
        );
    }
    error_is(
        f.post(
            ADMIN,
            &format!("/v1/principals/{}/enable", f.principals[0].to_hex()),
            "bad",
            json!({"extra":true}),
        ),
        400,
        "bad_request",
    );
}
#[test]
fn principal_credential_rotation_retries_and_validation() {
    use serde_json::json;
    let mut f = Fixture::new();
    let base = format!("/v1/principals/{}/credentials", f.principals[0].to_hex());
    let verifier = f.principal_verifier("rotated-private.secret");
    let body = json!({"verifier":verifier});
    let first = f.post(ADMIN, &base, "rotate", body.clone());
    assert_eq!(first.status(), 201);
    assert_eq!(
        f.post(ADMIN, &base, "rotate", body.clone()).body(),
        first.body()
    );
    assert_eq!(f.ctx().authority.credentials_of(f.principals[0]).len(), 2);
    let rotated = "Bearer nunya.alice.rotated-private.secret";
    f.authenticates(rotated, 200);
    f.authenticates(ALICE, 200);
    let cid = value(&first)["id"].as_str().unwrap().to_owned();
    error_is(
        f.mutation(
            "DELETE",
            &format!(
                "/v1/principals/{}/credentials/{cid}",
                f.principals[1].to_hex()
            ),
            "foreign",
            json!({}),
        ),
        404,
        "not_found",
    );
    let deleted = f.mutation("DELETE", &format!("{base}/{cid}"), "delete", json!({}));
    assert_eq!(deleted.status(), 200);
    assert_eq!(value(&deleted)["state"], "revoked");
    assert_eq!(
        f.mutation("DELETE", &format!("{base}/{cid}"), "delete", json!({}))
            .body(),
        deleted.body()
    );
    f.authenticates(rotated, 401);
    f.authenticates(ALICE, 200);
    let different = f.principal_verifier("conflicting-private.secret");
    error_is(
        f.post(ADMIN, &base, "rotate", json!({"verifier":different})),
        409,
        "idempotency_conflict",
    );
    for expires in ["0s", "garbage"] {
        error_is(
            f.post(
                ADMIN,
                &base,
                "invalid",
                json!({"verifier":verifier,"expires":expires}),
            ),
            400,
            "bad_request",
        );
    }
    error_is(
        f.post(ADMIN, &base, "invalid", json!({"verifier":"bad"})),
        400,
        "bad_request",
    );
    let expiring = f.post(
        ADMIN,
        &base,
        "expiring",
        json!({"verifier":verifier,"expires":"1h"}),
    );
    assert_eq!(expiring.status(), 201);
    assert_eq!(value(&expiring)["expires_ms"], 3_601_000);
}
#[test]
fn principal_credential_relative_expiry_retry_keeps_original_deadline() {
    use serde_json::json;
    let mut f = Fixture::new();
    let base = format!("/v1/principals/{}/credentials", f.principals[0].to_hex());
    let verifier = f.principal_verifier("expiring-private.secret");
    let body = json!({"verifier":verifier,"expires":"30m"});
    let first = f.post(ADMIN, &base, "K", body.clone());
    assert_eq!(first.status(), 201);
    assert_eq!(value(&first)["expires_ms"], 1_801_000);
    f.clock.store(61_000, Ordering::SeqCst);
    let again = f.post(ADMIN, &base, "K", body);
    assert_eq!(again.status(), 201);
    assert_eq!(value(&first)["id"], value(&again)["id"]);
    assert_eq!(first.body(), again.body());
    assert_eq!(f.ctx().authority.credentials_of(f.principals[0]).len(), 2);
    let expiring = "Bearer nunya.alice.expiring-private.secret";
    f.authenticates(expiring, 200);
    f.clock.store(1_801_001, Ordering::SeqCst);
    f.authenticates(expiring, 401);
    f.authenticates(ALICE, 200);
}
#[test]
fn grant_replacement_narrowing_removal_and_canonical_retry() {
    use nunya::state::authority::{AdmitError, Operation};
    use serde_json::json;
    let f = Fixture::new();
    let base = format!("/v1/principals/{}/grants", f.principals[0].to_hex());
    let body = json!({"grants":[{"route":"postgres","mode":"read_only"}, {"route":"other","mode":"read_write"}]});
    let first = f.mutation("PUT", &base, "grants", body.clone());
    assert_eq!(first.status(), 200);
    let reversed = json!({"grants":[{"route":"other","mode":"read_write"},{"route":"postgres","mode":"read_only"}]});
    assert_eq!(
        f.mutation("PUT", &base, "grants", reversed).body(),
        first.body()
    );
    assert!(matches!(
        f.ctx().authority.admit(f.sessions[0], Operation::Write),
        Err(AdmitError::ReadOnly)
    ));
    error_is(
        f.mutation("PUT", &base, "grants", json!({"grants":[]})),
        409,
        "idempotency_conflict",
    );
    for grants in [
        json!([{"route":"postgres","mode":"read_only"},{"route":"postgres","mode":"read_write"}]),
        json!([{"route":"","mode":"read_only"}]),
        json!([{"route":"postgres","mode":"unknown"}]),
        json!([{"route":"postgres","mode":"read_only","extra":true}]),
    ] {
        error_is(
            f.mutation("PUT", &base, "invalid", json!({"grants":grants})),
            400,
            "bad_request",
        );
    }
    assert_eq!(
        f.mutation("PUT", &base, "remove", json!({"grants":[]}))
            .status(),
        200
    );
    assert_eq!(
        f.ctx()
            .authority
            .session(f.sessions[0])
            .unwrap()
            .revoke_reason,
        Some(EndReason::GrantRemoved)
    );
    let configured = f
        .ctx()
        .authority
        .principal_by_name("configured")
        .unwrap()
        .id;
    let path = format!("/v1/principals/{}/grants", configured.to_hex());
    assert_eq!(
        f.mutation("PUT", &path, "config-grants", body).status(),
        200
    );
    assert_eq!(f.ctx().authority.grants_of(configured).len(), 2);
}

// Transport tests use the same forbidden-material list as the pure route tests.
type TransportResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn listener_section(tls: bool) -> nunya::config::ManagementSection {
    nunya::config::ManagementSection {
        listen: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        tls: tls.then(|| nunya::config::TlsSection {
            cert_file: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/mgmt/localhost.crt"),
            key_file: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/mgmt/localhost.key"),
        }),
        admins: vec![],
    }
}
fn status_request(auth: Option<&str>) -> String {
    let auth = auth
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
    format!("GET /v1/status HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n")
}
async fn wire<I>(io: &mut I, request: &[u8], forbidden: &[String]) -> TransportResult<String>
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    io.write_all(request).await?;
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(12),
        io.read_to_end(&mut response),
    )
    .await??;
    let response = String::from_utf8(response)?;
    for secret in forbidden {
        assert!(
            !response.contains(secret),
            "transport response leaked fixture material"
        );
    }
    Ok(response)
}
async fn tcp_request(
    addr: std::net::SocketAddr,
    request: &[u8],
    forbidden: &[String],
) -> TransportResult<String> {
    let mut io = tokio::net::TcpStream::connect(addr).await?;
    wire(&mut io, request, forbidden).await
}
async fn finish_server(
    tx: tokio::sync::watch::Sender<bool>,
    server: nunya::mgmt::server::ServerHandle,
) -> TransportResult {
    let addr = server.tcp_addr;
    tx.send(true)?;
    tokio::time::timeout(std::time::Duration::from_secs(10), server.wait()).await?;
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
    Ok(())
}
#[tokio::test]
async fn listener_loopback_auth_limits_and_shutdown() -> TransportResult {
    use nunya::mgmt::server::spawn;
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    #[cfg(windows)]
    assert!(server.socket_path.is_none());
    let addr = server.tcp_addr;
    let ok = tcp_request(addr, status_request(Some(ADMIN)).as_bytes(), &f.forbidden).await?;
    assert!(ok.starts_with("HTTP/1.1 200 "));
    let denied = tcp_request(addr, status_request(None).as_bytes(), &f.forbidden).await?;
    assert!(denied.starts_with("HTTP/1.1 401 "));
    assert!(
        denied
            .to_ascii_lowercase()
            .contains("www-authenticate: bearer\r\n")
    );
    assert!(denied.ends_with("{\"error\":\"unauthorized\"}"));
    let large_header =
        b"POST /v1/sessions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\n\r\n";
    let large = tcp_request(addr, large_header, &f.forbidden).await?;
    assert!(large.starts_with("HTTP/1.1 413 "));
    assert!(large.ends_with("{\"error\":\"payload_too_large\"}"));
    let mut chunked = b"POST /v1/sessions HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n10001\r\n".to_vec();
    chunked.extend(std::iter::repeat_n(b'x', 64 * 1024 + 1));
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");
    let large = tcp_request(addr, &chunked, &f.forbidden).await?;
    assert!(large.starts_with("HTTP/1.1 413 "));
    assert!(large.ends_with("{\"error\":\"payload_too_large\"}"));
    // Idle keep-alive connections must not hold shutdown open indefinitely.
    let _idle = tokio::net::TcpStream::connect(addr).await?;
    finish_server(tx, server).await
}
#[tokio::test]
async fn listener_tls_and_plaintext_refusal() -> TransportResult {
    use nunya::mgmt::server::spawn;
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(true), &f.dir, rx).await?;
    let mut roots = rustls::RootCertStore::empty();
    let cert = rustls_pemfile::certs(&mut &include_bytes!("fixtures/mgmt/localhost.crt")[..])
        .next()
        .ok_or("missing fixture cert")??;
    roots.add(cert)?;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let io = tokio::net::TcpStream::connect(server.tcp_addr).await?;
    let mut io = connector
        .connect(rustls::pki_types::ServerName::try_from("localhost")?, io)
        .await?;
    let ok = wire(
        &mut io,
        status_request(Some(ADMIN)).as_bytes(),
        &f.forbidden,
    )
    .await?;
    assert!(ok.starts_with("HTTP/1.1 200 "));
    // A TLS alert or a closed connection is acceptable; plaintext HTTP success is not.
    let mut plain = tokio::net::TcpStream::connect(server.tcp_addr).await?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    plain
        .write_all(status_request(Some(ADMIN)).as_bytes())
        .await?;
    let mut bytes = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(12),
        plain.read_to_end(&mut bytes),
    )
    .await?;
    let response = String::from_utf8_lossy(&bytes);
    for secret in &f.forbidden {
        assert!(
            !response.contains(secret),
            "TLS refusal leaked fixture material"
        );
    }
    assert!(!response.starts_with("HTTP/1.1 200 "));
    finish_server(tx, server).await
}
#[tokio::test]
async fn listener_non_loopback_requires_tls_and_sender_drop_stops() -> TransportResult {
    use nunya::mgmt::server::{SpawnError, spawn};
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let mut section = listener_section(false);
    section.listen = std::net::SocketAddr::from(([0, 0, 0, 0], 0));
    assert!(matches!(
        spawn(ctx.clone(), &section, &f.dir, rx.clone()).await,
        Err(SpawnError::TlsRequired)
    ));
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    let addr = server.tcp_addr;
    drop(tx);
    tokio::time::timeout(std::time::Duration::from_secs(10), server.wait()).await?;
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
    Ok(())
}
#[cfg(unix)]
#[tokio::test]
async fn listener_unix_socket_permissions_auth_cleanup_and_file_refusal() -> TransportResult {
    use nunya::mgmt::server::{SpawnError, spawn};
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let path = f.dir.join("nunya.sock");
    std::fs::write(&path, b"preserve")?;
    assert!(matches!(
        spawn(ctx.clone(), &listener_section(false), &f.dir, rx.clone()).await,
        Err(SpawnError::Socket)
    ));
    assert_eq!(std::fs::read(&path)?, b"preserve");
    std::fs::remove_file(&path)?;
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    assert_eq!(server.socket_path.as_ref(), Some(&path));
    assert_eq!(
        std::fs::metadata(&path)?.permissions().mode() & 0o777,
        0o600
    );
    let mut io = tokio::net::UnixStream::connect(&path).await?;
    let ok = wire(&mut io, status_request(None).as_bytes(), &f.forbidden).await?;
    assert!(ok.starts_with("HTTP/1.1 200 "));
    finish_server(tx, server).await?;
    assert!(!path.exists());
    Ok(())
}

fn assert_audit_material(f: &Fixture) -> Vec<LifecycleRecord> {
    let records = f.audit.records.lock().unwrap().clone();
    for record in &records {
        let text = serde_json::to_string(record).unwrap();
        for forbidden in f.forbidden.iter().map(String::as_str).chain([
            "test-session",
            "audit label",
            "private-audit-label",
            "config-admin",
        ]) {
            assert!(!text.contains(forbidden), "audit leaked fixture material");
        }
    }
    records
}

#[test]
fn lifecycle_records_deduplicate_mutations_and_refuse_resume() {
    let mut f = Fixture::new();
    let mut body = f.session_body("audit-session-private.secret");
    body["label"] = serde_json::json!("private-audit-label");
    let acquired = f.post(ALICE, "/v1/sessions", "audit-acquire", body.clone());
    assert_eq!(acquired.status(), 201);
    let sid = value(&acquired)["id"].as_str().unwrap().to_owned();
    assert_eq!(
        f.post(ALICE, "/v1/sessions", "audit-acquire", body)
            .status(),
        201
    );
    let renew = format!("/v1/sessions/{sid}/renew");
    for _ in 0..2 {
        assert_eq!(
            f.post(ALICE, &renew, "audit-renew", serde_json::json!({}))
                .status(),
            200
        );
    }
    let revoke = format!("/v1/sessions/{sid}/revoke");
    for _ in 0..2 {
        assert_eq!(
            f.post(ALICE, &revoke, "audit-revoke", serde_json::json!({}))
                .status(),
            200
        );
    }
    error_is(
        f.post(ALICE, &renew, "resume", serde_json::json!({})),
        409,
        "session_ended",
    );
    let admin_revoke = format!("/v1/sessions/{}/revoke", f.sessions[1].to_hex());
    assert_eq!(
        f.post(ADMIN, &admin_revoke, "admin-revoke", serde_json::json!({}))
            .status(),
        200
    );
    let disable = format!("/v1/principals/{}/disable", f.principals[1].to_hex());
    for _ in 0..2 {
        assert_eq!(
            f.post(ADMIN, &disable, "disable", serde_json::json!({}))
                .status(),
            200
        );
    }
    let enable = format!("/v1/principals/{}/enable", f.principals[1].to_hex());
    assert_eq!(
        f.post(ADMIN, &enable, "enable", serde_json::json!({}))
            .status(),
        200
    );
    let records = assert_audit_material(&f);
    assert_eq!(
        records.iter().map(|r| r.kind).collect::<Vec<_>>(),
        vec![
            LifecycleKind::Issued,
            LifecycleKind::Renewed,
            LifecycleKind::Revoked,
            LifecycleKind::ResumeRefused,
            LifecycleKind::Revoked,
            LifecycleKind::PrincipalDisabled,
        ]
    );
    for r in &records[..4] {
        assert_eq!(r.at_ms, f.clock.load(Ordering::SeqCst));
        assert_eq!(serde_json::to_value(r).unwrap()["actor"], "owner");
        assert_eq!(r.actor, Actor::Owner);
        assert_eq!(r.principal, f.principals[0].to_hex());
        assert_eq!(r.session.as_deref(), Some(sid.as_str()));
        assert_eq!(r.route.as_deref(), Some("postgres"));
    }
    assert_eq!(records[2].reason, "requested");
    assert_eq!(records[3].reason, "session_ended");
    assert_eq!(records[4].reason, "admin_revoked");
    assert_eq!(
        serde_json::to_value(&records[4]).unwrap()["actor"],
        serde_json::json!({"admin": f.principals[2].to_hex()})
    );
    assert_eq!(records[4].actor, Actor::Admin(f.principals[2].to_hex()));
    assert_eq!(records[5].actor, Actor::Admin(f.principals[2].to_hex()));
    assert_eq!(records[5].principal, f.principals[1].to_hex());
    assert_eq!(records[5].session, None);
    assert_eq!(records[5].route, None);
}

#[test]
fn lifecycle_config_and_socket_actors_hide_names() {
    let f = Fixture::new();
    for (index, origin, auth, actor) in [
        (0, Origin::Network, Some(CONFIG), Actor::ConfigAdmin),
        (1, Origin::Socket, None, Actor::Socket),
    ] {
        let path = format!("/v1/sessions/{}/revoke", f.sessions[index].to_hex());
        assert_eq!(
            f.request(
                origin,
                auth,
                "POST",
                &path,
                Some(if index == 0 {
                    "config-actor"
                } else {
                    "socket-actor"
                })
            )
            .status(),
            200
        );
        let records = assert_audit_material(&f);
        assert_eq!(records[index].actor, actor);
        assert_eq!(records[index].reason, "admin_revoked");
        let serialized = serde_json::to_value(&records[index]).unwrap();
        assert_eq!(
            serialized["actor"],
            if index == 0 { "config_admin" } else { "socket" }
        );
    }
}

#[test]
fn lifecycle_sink_failure_closes_access_and_preserves_refusal() {
    use nunya::state::authority::Operation;
    let mut f = Fixture::new();
    let body = f.session_body("audit-failed-private.secret");
    f.audit.failing.store(true, Ordering::SeqCst);
    error_is(
        f.post(ALICE, "/v1/sessions", "failed-acquire", body.clone()),
        503,
        "audit_unavailable",
    );
    let new_session = f
        .ctx()
        .authority
        .all_sessions()
        .into_iter()
        .find(|s| !f.sessions.contains(&s.id))
        .unwrap();
    assert_eq!(new_session.revoke_reason, Some(EndReason::AdminRevoked));
    assert!(
        f.ctx()
            .authority
            .admit(new_session.id, Operation::Read)
            .is_err()
    );
    error_is(
        f.post(ALICE, "/v1/sessions", "failed-acquire", body),
        409,
        "session_ended",
    );
    let renew_id = f.sessions[0];
    let renew = format!("/v1/sessions/{}/renew", renew_id.to_hex());
    let before = f.ctx().authority.session(renew_id).unwrap();
    let (salt, iterations) = f.ctx().authority.session_scram_params(renew_id).unwrap();
    let rotation = nunya::mgmt::client::SessionSecret::generate_with(&salt, iterations).unwrap();
    let renew_body = serde_json::json!({"mode":"read_only", "add_credential": {
        "verifier":rotation.verifier, "secret_sha256":rotation.sha256_hex
    }});
    f.clock.fetch_add(100, Ordering::SeqCst);
    error_is(
        f.post(ALICE, &renew, "failed-renew", renew_body.clone()),
        503,
        "audit_unavailable",
    );
    let after = f.ctx().authority.session(renew_id).unwrap();
    assert_eq!(after.state, SessionState::Active);
    assert_eq!(after.revoke_reason, None);
    assert_eq!(after.expires_ms, before.expires_ms);
    assert_eq!(after.last_used_ms, before.last_used_ms);
    assert_eq!(after.mode, before.mode);
    assert!(
        !f.ctx()
            .authority
            .check_session_secret(renew_id, rotation.secret.as_bytes())
    );
    let revoke = format!("/v1/sessions/{}/revoke", f.sessions[1].to_hex());
    error_is(
        f.post(BOB, &revoke, "failed-revoke", serde_json::json!({})),
        503,
        "audit_unavailable",
    );
    assert_eq!(
        f.ctx()
            .authority
            .session(f.sessions[1])
            .unwrap()
            .revoke_reason,
        Some(EndReason::Requested)
    );
    assert!(
        f.ctx()
            .authority
            .admit(f.sessions[1], Operation::Read)
            .is_err()
    );
    let bob_renew = format!("/v1/sessions/{}/renew", f.sessions[1].to_hex());
    error_is(
        f.post(BOB, &bob_renew, "refuse", serde_json::json!({})),
        409,
        "session_ended",
    );
    let disable = format!("/v1/principals/{}/disable", f.principals[0].to_hex());
    error_is(
        f.post(ADMIN, &disable, "failed-disable", serde_json::json!({})),
        503,
        "audit_unavailable",
    );
    assert_eq!(
        f.ctx().authority.principal(f.principals[0]).unwrap().state,
        PrincipalState::Disabled
    );
    assert!(assert_audit_material(&f).is_empty());
    // Failed audit attempts keep the mutation, but allow a same-key audit retry.
    f.audit.failing.store(false, Ordering::SeqCst);
    for _ in 0..2 {
        assert_eq!(
            f.post(BOB, &revoke, "failed-revoke", serde_json::json!({}))
                .status(),
            200
        );
        let records = assert_audit_material(&f);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, LifecycleKind::Revoked);
    }
    for _ in 0..2 {
        assert_eq!(
            f.post(ADMIN, &disable, "failed-disable", serde_json::json!({}))
                .status(),
            200
        );
        let records = assert_audit_material(&f);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].kind, LifecycleKind::Revoked);
        assert_eq!(records[1].kind, LifecycleKind::PrincipalDisabled);
    }
}

#[test]
fn renewal_audit_recovery_retries_same_key_once() -> TransportResult {
    let f = Fixture::new();
    let id = f.sessions[0];
    let path = format!("/v1/sessions/{}/renew", id.to_hex());
    let before = f.ctx().authority.session(id).ok_or("missing session")?;
    let (salt, iterations) = f
        .ctx()
        .authority
        .session_scram_params(id)
        .ok_or("missing parameters")?;
    let rotation = nunya::mgmt::client::SessionSecret::generate_with(&salt, iterations)?;
    let body = serde_json::json!({"mode":"read_only", "add_credential": {
        "verifier":rotation.verifier, "secret_sha256":rotation.sha256_hex
    }});
    f.clock.fetch_add(100, Ordering::SeqCst);
    f.audit.failing.store(true, Ordering::SeqCst);
    error_is(
        f.post(ALICE, &path, "retry-renew", body.clone()),
        503,
        "audit_unavailable",
    );
    let after = f.ctx().authority.session(id).ok_or("missing session")?;
    assert_eq!(after.state, SessionState::Active);
    assert_eq!(after.expires_ms, before.expires_ms);
    assert_eq!(after.last_used_ms, before.last_used_ms);
    assert_eq!(after.mode, before.mode);
    assert!(
        !f.ctx()
            .authority
            .check_session_secret(id, rotation.secret.as_bytes())
    );
    assert!(assert_audit_material(&f).is_empty());
    f.audit.failing.store(false, Ordering::SeqCst);
    for _ in 0..2 {
        let response = f.post(ALICE, &path, "retry-renew", body.clone());
        assert_eq!(response.status(), 200);
        assert_eq!(value(&response)["mode"], "read_only");
        assert!(
            f.ctx()
                .authority
                .check_session_secret(id, rotation.secret.as_bytes())
        );
        let records = assert_audit_material(&f);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, LifecycleKind::Renewed);
    }
    Ok(())
}

fn api_client(
    addr: std::net::SocketAddr,
    name: &str,
    secret: &str,
) -> nunya::mgmt::client::MgmtClient {
    use nunya::mgmt::client::{Credentials, MgmtClient};
    MgmtClient::new(
        format!("http://{addr}").parse().unwrap(),
        None,
        Credentials::Bearer {
            name: name.into(),
            secret: zeroize::Zeroizing::new(secret.into()),
        },
    )
    .unwrap()
}
#[tokio::test]
async fn client_all_methods_and_session_secret() -> TransportResult {
    use nunya::mgmt::{
        client::{AcquireParams, ClientError, SessionCredential, SessionSecret},
        server::spawn,
    };
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let authority = ctx.authority.clone();
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    let admin = api_client(server.tcp_addr, "admin", "admin-private.secret");
    let alice = api_client(server.tcp_addr, "alice", "alice-private.secret");
    let secret = SessionSecret::generate();
    let bearer = nunya::mgmt::client::Credentials::Bearer {
        name: "alice".into(),
        secret: zeroize::Zeroizing::new(secret.secret.to_string()),
    };
    for debug in [
        format!("{bearer:?}"),
        format!("{secret:?}"),
        format!("{alice:?}"),
    ] {
        for value in [
            secret.secret.as_str(),
            &secret.verifier,
            &secret.sha256_hex,
            "alice-private.secret",
        ] {
            assert!(
                !debug.contains(value),
                "client diagnostics leaked credential material"
            );
        }
    }
    let s = alice
        .acquire(AcquireParams {
            route: "postgres".into(),
            mode: Some(Mode::ReadOnly),
            lifetime: Some("30m".into()),
            isolated: false,
            label: Some("client-test".into()),
            verifier: secret.verifier.clone(),
            secret_sha256: secret.sha256_hex.clone(),
        })
        .await?;
    let sid = SessionId::from_hex(&s.id).ok_or("invalid session id")?;
    assert!(authority.check_session_secret(sid, secret.secret.as_bytes()));
    assert_eq!(alice.session(&s.id).await?.id, s.id);
    assert!(alice.sessions().await?.iter().any(|row| row.id == s.id));
    let params = authority
        .session_scram_params(sid)
        .ok_or("missing SCRAM parameters")?;
    let second = SessionSecret::generate_with(&params.0, params.1)?;
    let renewed = alice
        .renew(
            &s.id,
            Some(Mode::ReadOnly),
            Some(SessionCredential {
                verifier: second.verifier.clone(),
                secret_sha256: second.sha256_hex.clone(),
            }),
        )
        .await?;
    assert_eq!(renewed.id, s.id);
    assert!(authority.check_session_secret(sid, second.secret.as_bytes()));
    assert_eq!(alice.renew(&s.id, None, None).await?.id, s.id);
    assert_eq!(alice.revoke(&s.id).await?.state, SessionState::Revoked);
    let password = SessionSecret::generate();
    let p = admin
        .create_principal(
            "client-principal",
            PrincipalKind::Agent,
            false,
            Some(&password.verifier),
        )
        .await?;
    assert_eq!(p.credentials.len(), 1);
    assert!(admin.principals().await?.iter().any(|row| row.id == p.id));
    assert_eq!(admin.principal(&p.id).await?.name, "client-principal");
    let grants = admin
        .set_grants(&p.id, &[("postgres", Mode::ReadOnly)])
        .await?;
    assert_eq!(grants.grants.len(), 1);
    assert_eq!(grants.grants[0].mode, Mode::ReadOnly);
    assert_eq!(admin.disable(&p.id).await?.state, PrincipalState::Disabled);
    assert_eq!(admin.enable(&p.id).await?.state, PrincipalState::Enabled);
    let c = admin
        .add_credential(&p.id, &second.verifier, Some("1h"))
        .await?;
    assert!(c.expires_ms.is_some());
    assert_eq!(
        admin.delete_credential(&p.id, &c.id).await?.state,
        CredentialState::Revoked
    );
    assert_eq!(admin.connections().await?[0].id, 7);
    let status = admin.status().await?;
    assert_eq!(status.route_ids, ["postgres"]);
    assert_eq!(status.policy_revision.as_deref(), Some("policy-1"));
    let err = alice.status().await.unwrap_err();
    assert_eq!(
        err,
        ClientError::Api {
            status: 403,
            code: "forbidden".into()
        }
    );
    let wrong = api_client(server.tcp_addr, "alice", "wrong");
    assert_eq!(
        wrong.status().await.unwrap_err(),
        ClientError::Api {
            status: 401,
            code: "unauthorized".into()
        }
    );
    let no_grant = api_client(server.tcp_addr, "client-principal", &password.secret);
    admin.set_grants(&p.id, &[]).await?;
    assert_eq!(
        no_grant
            .acquire(AcquireParams {
                route: "postgres".into(),
                mode: None,
                lifetime: None,
                isolated: false,
                label: None,
                verifier: second.verifier,
                secret_sha256: second.sha256_hex,
            })
            .await
            .unwrap_err(),
        ClientError::Api {
            status: 403,
            code: "no_grant".into()
        }
    );
    let records = f.audit.records.lock().unwrap().clone();
    assert!(records.iter().any(|r| r.kind == LifecycleKind::Issued && r.session.as_deref() == Some(s.id.as_str())));
    assert!(records.iter().any(|r| r.kind == LifecycleKind::Renewed));
    assert!(records.iter().any(|r| r.kind == LifecycleKind::Revoked));
    assert!(
        records
            .iter()
            .any(|r| r.kind == LifecycleKind::PrincipalDisabled)
    );
    drop(authority);
    finish_server(tx, server).await
}
#[tokio::test]
async fn client_tls_custom_ca() -> TransportResult {
    use nunya::mgmt::{
        client::{Credentials, MgmtClient},
        server::spawn,
    };
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(true), &f.dir, rx).await?;
    let ca = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mgmt/localhost.crt");
    let client = MgmtClient::new(
        format!("https://localhost:{}", server.tcp_addr.port()).parse()?,
        Some(&ca),
        Credentials::Bearer {
            name: "admin".into(),
            secret: zeroize::Zeroizing::new("admin-private.secret".into()),
        },
    )?;
    assert_eq!(
        client.status().await?.config_revision.as_deref(),
        Some("config-2")
    );
    assert!(
        "http://10.0.0.1:1"
            .parse::<nunya::mgmt::client::Endpoint>()
            .is_err()
    );
    finish_server(tx, server).await
}
#[cfg(unix)]
#[tokio::test]
async fn client_unix_socket() -> TransportResult {
    use nunya::mgmt::{
        client::{Credentials, MgmtClient},
        server::spawn,
    };
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    let path = server.socket_path.as_ref().ok_or("missing socket")?;
    let client = MgmtClient::new(
        format!("unix:{}", path.display()).parse()?,
        None,
        Credentials::None,
    )?;
    assert_eq!(client.status().await?.route_ids, ["postgres"]);
    finish_server(tx, server).await
}
#[tokio::test]
async fn helper_acquire_resume_scope_replacement_and_isolation() -> TransportResult {
    use nunya::{
        cli::session::{
            AcquireOpts, ConnectionOpts, HelperError, HelperStore, acquire_or_resume, pg_env,
            revoke,
        },
        mgmt::server::spawn,
    };
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let authority = ctx.authority.clone();
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    let alice = api_client(server.tcp_addr, "alice", "alice-private.secret");
    let admin = api_client(server.tcp_addr, "admin", "admin-private.secret");
    let store = HelperStore::at(f.dir.join("helper"));
    let mut opts = AcquireOpts {
        connection: ConnectionOpts {
            server: format!("http://{}", server.tcp_addr),
            ca_file: None,
            principal: Some("alice".into()),
            principal_secret_file: None,
            context: None,
        },
        route: "postgres".into(),
        mode: Some(Mode::ReadOnly),
        ttl: Some("30m".into()),
        label: None,
        new_scope: false,
        rotate: false,
        pg_host: "127.0.0.1".into(),
        pg_port: 6432,
        isolated: false,
    };
    let first = acquire_or_resume(&alice, &store, &opts).await?;
    let stored = store.get("default")?;
    let sid = SessionId::from_hex(&first.session_id).ok_or("invalid session id")?;
    assert!(authority.check_session_secret(sid, stored.secret.as_bytes()));
    let resumed = acquire_or_resume(&alice, &store, &opts).await?;
    assert_eq!(first.session_id, resumed.session_id);
    assert!(
        first.secret.as_str() == resumed.secret.as_str(),
        "resumed credential changed"
    );
    let original_params = authority
        .session_scram_params(sid)
        .ok_or("missing parameters")?;
    opts.rotate = true;
    let rotated = acquire_or_resume(&alice, &store, &opts).await?;
    assert_eq!(rotated.session_id, first.session_id);
    assert_eq!(rotated.scope, first.scope);
    assert!(
        rotated.secret.as_str() != first.secret.as_str(),
        "rotation did not replace credential"
    );
    assert!(
        store.get("default")?.secret.as_str() == rotated.secret.as_str(),
        "rotated credential not stored"
    );
    assert!(authority.check_session_secret(sid, first.secret.as_bytes()));
    assert!(authority.check_session_secret(sid, rotated.secret.as_bytes()));
    assert_eq!(
        authority.session_scram_params(sid),
        Some(original_params.clone())
    );
    let mut renewed = rotated;
    nunya::cli::session::rotate(&alice, &store, &mut renewed, None).await?;
    assert_eq!(renewed.session_id, first.session_id);
    assert_eq!(renewed.scope, first.scope);
    assert!(authority.check_session_secret(sid, renewed.secret.as_bytes()));
    assert!(authority.check_session_secret(sid, first.secret.as_bytes()));
    assert_eq!(authority.session_scram_params(sid), Some(original_params));
    opts.rotate = false;
    admin.revoke(&first.session_id).await?;
    let err = acquire_or_resume(&alice, &store, &opts).await.unwrap_err();
    assert_eq!(err, HelperError::ScopeReplaced);
    assert_eq!(
        err.to_string(),
        "the stored session has ended and its token scope was replaced; tokens from it will not match a new session. Run again with --new-scope to start a new scope."
    );
    assert_eq!(store.get("default")?.session_id, first.session_id);
    opts.new_scope = true;
    let replacement = acquire_or_resume(&alice, &store, &opts).await?;
    assert_ne!(replacement.session_id, first.session_id);
    assert_ne!(replacement.scope, first.scope);
    assert_eq!(store.get("default")?.session_id, replacement.session_id);
    opts.isolated = true;
    let isolated = acquire_or_resume(&alice, &store, &opts).await?;
    assert!(isolated.isolated);
    assert!(isolated.name.starts_with("isolated-"));
    assert_eq!(store.get("default")?.session_id, replacement.session_id);
    assert_eq!(store.get(&isolated.name)?.session_id, isolated.session_id);
    opts.connection.context = Some("default".into());
    assert_eq!(
        acquire_or_resume(&alice, &store, &opts).await.unwrap_err(),
        HelperError::Conflict
    );
    opts.connection.context = Some("explicit-isolation".into());
    let named = acquire_or_resume(&alice, &store, &opts).await?;
    assert_eq!(named.name, "explicit-isolation");
    let env = pg_env(&replacement);
    assert_eq!(
        env.iter()
            .find(|(k, _)| k == "PGUSER")
            .ok_or("missing PGUSER")?
            .1
            .as_str(),
        format!("session:{}", replacement.session_id)
    );
    assert!(
        env.iter()
            .find(|(k, _)| k == "PGPASSWORD")
            .ok_or("missing PGPASSWORD")?
            .1
            .as_str()
            == replacement.secret.as_str(),
        "environment credential mismatch"
    );
    revoke(&alice, &store, &replacement).await?;
    assert_eq!(store.get("default").unwrap_err(), HelperError::Missing);
    assert!(store.get(&isolated.name).is_ok());
    drop(authority);
    finish_server(tx, server).await
}

#[tokio::test]
async fn principal_cli_lifecycle() -> TransportResult {
    use nunya::{
        cli::{
            principal::{self, CredentialOpts},
            session::{AcquireOpts, ConnectionOpts, HelperStore, acquire_or_resume},
        },
        mgmt::{client::ClientError, server::spawn},
    };
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing fixture context")?);
    let authority = ctx.authority.clone();
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = spawn(ctx, &listener_section(false), &f.dir, rx).await?;
    let admin = api_client(server.tcp_addr, "admin", "admin-private.secret");
    let opts = CredentialOpts {
        verifier: None,
        generate_secret: true,
    };
    let issued = principal::create(
        &admin,
        "cli-principal",
        PrincipalKind::Service,
        false,
        &opts,
    )
    .await?;
    let id = issued.value.id;
    let first_id = issued.value.credentials[0].id.clone();
    let secret = issued.secret.ok_or("missing generated secret")?;
    assert_eq!(secret.len(), 64);
    assert!(!format!("{opts:?}").contains(secret.as_str()));
    assert_eq!(principal::resolve(&admin, "cli-principal").await?, id);
    assert_eq!(principal::resolve(&admin, &id.to_uppercase()).await?, id);
    assert_eq!(
        principal::resolve(&admin, "unknown")
            .await
            .unwrap_err()
            .to_string(),
        "principal not found"
    );
    assert!(principal::list(&admin).await?.iter().any(|p| p.id == id));
    assert_eq!(
        principal::grant(
            &admin,
            &id,
            &["postgres=read-only".into(), "postgres=read-write".into()]
        )
        .await
        .unwrap_err()
        .to_string(),
        "duplicate route grant"
    );
    assert!(principal::show(&admin, &id).await?.grants.is_empty());
    let invalid = CredentialOpts {
        verifier: Some("private-invalid-verifier".into()),
        generate_secret: false,
    };
    assert_eq!(
        principal::credential_add(&admin, &id, &invalid, None)
            .await
            .unwrap_err()
            .to_string(),
        "invalid SCRAM verifier"
    );
    let granted =
        principal::grant(&admin, "cli-principal", &["postgres=read-write".into()]).await?;
    assert_eq!(granted.grants[0].mode, Mode::ReadWrite);
    let client = api_client(server.tcp_addr, "cli-principal", &secret);
    let store = HelperStore::at(f.dir.join("principal-cli-helper"));
    let acquire = AcquireOpts {
        connection: ConnectionOpts {
            server: format!("http://{}", server.tcp_addr),
            ca_file: None,
            principal: Some("cli-principal".into()),
            principal_secret_file: None,
            context: None,
        },
        route: "postgres".into(),
        mode: Some(Mode::ReadWrite),
        ttl: Some("30m".into()),
        label: None,
        new_scope: false,
        rotate: false,
        pg_host: "127.0.0.1".into(),
        pg_port: 6432,
        isolated: false,
    };
    let session = acquire_or_resume(&client, &store, &acquire).await?;
    let second = principal::credential_add(&admin, "cli-principal", &opts, Some("1h")).await?;
    assert!(second.value.expires_ms.is_some());
    assert_eq!(
        principal::credential_delete(&admin, &id, &first_id)
            .await?
            .state,
        CredentialState::Revoked
    );
    assert_eq!(
        client.sessions().await.unwrap_err(),
        ClientError::Api {
            status: 401,
            code: "unauthorized".into()
        }
    );
    let new_secret = second.secret.ok_or("missing second secret")?;
    let new_client = api_client(server.tcp_addr, "cli-principal", &new_secret);
    assert_eq!(
        acquire_or_resume(&new_client, &store, &acquire)
            .await?
            .session_id,
        session.session_id
    );
    assert_eq!(
        principal::disable(&admin, "cli-principal").await?.state,
        PrincipalState::Disabled
    );
    let sid = SessionId::from_hex(&session.session_id).ok_or("invalid session id")?;
    assert_eq!(
        authority
            .session(sid)
            .ok_or("missing session")?
            .revoke_reason,
        Some(EndReason::PrincipalDisabled)
    );
    assert_eq!(
        new_client.sessions().await.unwrap_err(),
        ClientError::Api {
            status: 401,
            code: "unauthorized".into()
        }
    );
    assert_eq!(
        principal::enable(&admin, &id).await?.state,
        PrincipalState::Enabled
    );
    assert!(new_client.sessions().await.is_ok());
    let connections = principal::connections_list(&admin).await?;
    assert_eq!(connections.len(), 1);
    let row = &connections[0];
    assert_eq!(
        (
            row.id,
            row.session.as_str(),
            row.principal.as_str(),
            row.route.as_str()
        ),
        (7, "session-id", "principal-id", "postgres")
    );
    assert!(principal::grant(&admin, &id, &[]).await?.grants.is_empty());
    drop(authority);
    finish_server(tx, server).await
}

#[cfg(unix)]
#[tokio::test]
async fn listener_refuses_overlong_socket_path() -> TransportResult {
    use nunya::mgmt::server::{SpawnError, spawn};
    use std::os::unix::ffi::OsStrExt;
    let mut f = Fixture::new();
    let ctx = Arc::new(f.ctx.take().ok_or("missing context")?);
    let long_dir = f.dir.join("x".repeat(108));
    std::fs::create_dir(&long_dir)?;
    assert!(long_dir.join("nunya.sock").as_os_str().as_bytes().len() > 107);
    let (_tx, rx) = tokio::sync::watch::channel(false);
    assert!(matches!(
        spawn(ctx, &listener_section(false), &long_dir, rx).await,
        Err(SpawnError::SocketPathTooLong)
    ));
    assert_eq!(std::fs::read_dir(&long_dir)?.count(), 0);
    Ok(())
}

#[test]
fn credential_limit_and_replay() {
    let f = Fixture::new();
    let path = format!("/v1/principals/{}/credentials", f.principals[0].to_hex());
    let verifier = ScramVerifier::from_password("extra", b"extra-salt", 4096).to_pg_string();
    let body = Bytes::from(serde_json::json!({"verifier": verifier}).to_string());
    for key in ["extra-1", "extra-2", "extra-3"] {
        assert_eq!(
            f.request_body(
                Origin::Network,
                Some(CONFIG),
                "POST",
                &path,
                Some(key),
                body.clone()
            )
            .status(),
            201
        );
    }
    let refused = f.request_body(
        Origin::Network,
        Some(CONFIG),
        "POST",
        &path,
        Some("extra-4"),
        body.clone(),
    );
    assert_eq!(refused.status(), 409);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(refused.body()).unwrap()["error"],
        "credential_limit"
    );
    assert_eq!(
        f.request_body(
            Origin::Network,
            Some(CONFIG),
            "POST",
            &path,
            Some("extra-3"),
            body
        )
        .status(),
        201
    );
}
#[test]
fn config_admin_names_reserved_and_retry_owners_separate() {
    let mut f = Fixture::new();
    f.ctx.as_mut().unwrap().admins.push((
        "second-admin".into(),
        ScramVerifier::from_password("second-secret", b"second-salt", 4096),
    ));
    let create = |auth, name: &str| {
        f.request_body(
            Origin::Network,
            Some(auth),
            "POST",
            "/v1/principals",
            Some("same-create-key"),
            Bytes::from(serde_json::json!({"name":name,"kind":"agent"}).to_string()),
        )
    };
    let first = create(CONFIG, "first-created");
    let second = create("Bearer nunya.second-admin.second-secret", "second-created");
    assert_eq!(first.status(), 201);
    assert_eq!(second.status(), 201);
    assert_ne!(
        serde_json::from_slice::<serde_json::Value>(first.body()).unwrap()["id"],
        serde_json::from_slice::<serde_json::Value>(second.body()).unwrap()["id"]
    );
    let collision = create(CONFIG, "second-admin");
    assert_eq!(collision.status(), 409);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(collision.body()).unwrap()["error"],
        "name_taken"
    );
    // Distinct targets with the same retry key would conflict under a shared owner.
    for (origin, auth, pid) in [
        (Origin::Socket, None, f.principals[0]),
        (Origin::Network, Some(CONFIG), f.principals[1]),
    ] {
        assert_eq!(
            f.request_body(
                origin,
                auth,
                "POST",
                &format!("/v1/principals/{}/disable", pid.to_hex()),
                Some("same-disable-key"),
                Bytes::from_static(b"{}")
            )
            .status(),
            200
        );
    }
}
