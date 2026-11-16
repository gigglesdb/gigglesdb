//! In-process management authentication and authorization checks.
use bytes::Bytes;
use http::{Request, Response};
use nunya::{
    mgmt::{
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
use std::{path::PathBuf, sync::Arc};

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
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.ctx.take();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
impl Fixture {
    fn new() -> Self {
        let dir = PathBuf::from("J:/gigglesdb-work/nunya/target-w2-tmp")
            .join(format!("mgmt-{}", SessionId::random().to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(
            RedbStore::with_clock(
                &dir,
                &RootKey::from_hex(&"72".repeat(32)).unwrap(),
                Arc::new(|| 1000),
            )
            .unwrap(),
        );
        let authority = Authority::load(store, SessionLimits::default()).unwrap();
        let mut forbidden = vec![];
        let mut make_verifier = |secret: &str| {
            let salt = b"management-fixture-salt";
            let v = ScramVerifier::from_password(secret, salt, 4096);
            forbidden.extend([
                secret.to_owned(),
                data_encoding::HEXLOWER.encode(salt),
                data_encoding::BASE64.encode(salt),
                v.to_pg_string(),
                data_encoding::HEXLOWER.encode(&v.stored_key),
                data_encoding::HEXLOWER.encode(&v.server_key),
            ]);
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
        let ctx = MgmtContext {
            authority,
            admins: vec![("config-admin".into(), admin.0)],
            connections: Arc::new(Sources),
            status: Arc::new(Sources),
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
        ("POST", "/v1/principals".into(), [403, 403, 503, 503, 503]),
        ("GET", "/v1/principals".into(), [403, 403, 200, 200, 200]),
        (
            "GET",
            format!("/v1/principals/{pid}"),
            [403, 403, 200, 200, 200],
        ),
        (
            "POST",
            format!("/v1/principals/{pid}/disable"),
            [403, 403, 503, 503, 503],
        ),
        (
            "POST",
            format!("/v1/principals/{pid}/enable"),
            [403, 403, 503, 503, 503],
        ),
        (
            "POST",
            format!("/v1/principals/{pid}/credentials"),
            [403, 403, 503, 503, 503],
        ),
        (
            "DELETE",
            format!("/v1/principals/{pid}/credentials/{cid}"),
            [403, 403, 503, 503, 503],
        ),
        (
            "PUT",
            format!("/v1/principals/{pid}/grants"),
            [403, 403, 503, 503, 503],
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
            data_encoding::BASE64.encode(&verifier.salt),
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
