//! Pure synchronous management request dispatch.
use super::{
    audit::{Actor, AuditUnavailable, LifecycleKind, LifecycleRecord, LifecycleSink},
    auth::{Caller, Origin, authenticate},
};
use crate::{
    pgwire::auth::ScramVerifier,
    state::{
        authority::{AcquireRequest, Authority, AuthorityError},
        model::*,
    },
    token::{PrincipalId, SessionId},
};
use bytes::Bytes;
use http::{
    HeaderMap, Request, Response, StatusCode,
    header::{CONTENT_TYPE, WWW_AUTHENTICATE},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fmt, sync::{Arc, RwLock}};

pub(super) fn idempotency_owner(caller: &Caller, origin: Origin) -> PrincipalId {
    if let Some(id) = caller.principal {
        return id;
    }
    let mut hash = Sha256::new();
    match origin {
        Origin::Socket => hash.update(b"nunya-mgmt-owner\0socket"),
        Origin::Network => {
            hash.update(b"nunya-mgmt-owner\0config-admin\0");
            hash.update(caller.name.as_bytes());
        }
    }
    let digest = hash.finalize();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    PrincipalId(id)
}
/// Read-only connection registry adapter.
pub trait ConnectionSource: Send + Sync {
    /// Snapshot current connections.
    fn list(&self) -> Vec<ConnectionView>;
}
/// Read-only runtime revision adapter.
pub trait StatusSource: Send + Sync {
    /// Current policy revision for a route, or None when unknown or unavailable.
    fn policy_revision(&self, route: &str) -> Option<String>;
    /// Current configuration revision.
    fn config_revision(&self) -> Option<String>;
    /// Pending restart-only configuration setting names.
    fn config_restart_pending(&self) -> Vec<String> { Vec::new() }
    /// Most recent configuration reload outcome, when available.
    fn config_last_reload(&self) -> Option<String> { None }
    /// Route kind ("postgres" or "spanner-native") for a configured route id.
    fn route_kind(&self, _route: &str) -> Option<&'static str> { None }
    /// Advertised connection details for a configured route.
    fn route_connect(&self, _route: &str) -> Option<&super::client::ConnectInfo> {
        None
    }
    /// Configured route identifiers.
    fn route_ids(&self) -> Vec<String>;
}
/// Non-secret connection metadata.
#[derive(Clone, Debug, Serialize)]
pub struct ConnectionView {
    /// Connection identifier.
    pub id: u64,
    /// Session identifier.
    pub session: String,
    /// Principal identifier.
    pub principal: String,
    /// Route identifier.
    pub route: String,
}
/// Reloadable configuration administrator credentials.
pub type Admins = Arc<RwLock<Vec<(String, ScramVerifier)>>>;

/// Leadership snapshot adapter for management routing.
pub trait Leadership: Send + Sync {
    /// Return local leadership and the last known leader URL atomically.
    fn leadership(&self) -> (bool, Option<String>);
}
/// Shared management services and configured administrators.
pub struct MgmtContext {
    /// Optional leader routing hook; absent for standalone deployments.
    pub leadership: Option<Arc<dyn Leadership>>,
    /// Optional policy management adapter.
    pub policy: Option<Arc<dyn super::policy::PolicySource>>,
    /// Durable authority.
    pub authority: Arc<Authority>,
    /// Configuration administrator verifiers.
    pub admins: Admins,
    /// Active connection snapshot provider.
    pub connections: Arc<dyn ConnectionSource>,
    /// Runtime revision provider.
    pub status: Arc<dyn StatusSource>,
    /// Lifecycle record sink.
    pub lifecycle: Arc<dyn LifecycleSink>,
}
impl fmt::Debug for MgmtContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MgmtContext { [REDACTED] }")
    }
}
/// Redirect before authentication, dispatch or body collection.
pub(super) fn leader_response(ctx: &MgmtContext, origin: Origin, uri: &http::Uri) -> Option<Response<Bytes>> {
    if origin == Origin::Socket { return None; }
    let (leader, url) = ctx.leadership.as_ref()?.leadership();
    if leader { return None; }
    if let Some(url) = url
        && let Ok(base) = url.parse::<http::Uri>()
        && matches!(base.scheme_str(), Some("http" | "https"))
        && base.authority().is_some_and(|a| !a.as_str().contains('@'))
        && base.path_and_query().is_none_or(|p| p.as_str() == "/")
        && !url.contains('#') {
        let location = format!("{}{}", url.trim_end_matches('/'), uri.path_and_query().map_or("/", |p| p.as_str()));
        if let Ok(value) = http::HeaderValue::from_str(&location) {
            let mut response = Response::new(Bytes::new());
            *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
            response.headers_mut().insert(http::header::LOCATION, value);
            return Some(response);
        }
    }
    let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "no_leader");
    response.headers_mut().insert(http::header::RETRY_AFTER, http::HeaderValue::from_static("2"));
    Some(response)
}
/// Build a JSON response without exposing serialization errors.
pub fn json_response(status: StatusCode, value: impl Serialize) -> Response<Bytes> {
    let bytes = match serde_json::to_vec(&value) {
        Ok(b) => b,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
    };
    let mut response = Response::new(Bytes::from(bytes));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}
/// Build a fixed-code error response.
pub fn error(status: StatusCode, code: &'static str) -> Response<Bytes> {
    json_response(status, json!({"error": code}))
}
/// Map authority errors to fixed public codes.
pub fn authority_error(err: AuthorityError) -> Response<Bytes> {
    use AuthorityError::*;
    let (status, code) = match err {
        NotFound => (404, "not_found"),
        NameInvalid => (400, "bad_request"),
        NameTaken => (409, "name_taken"),
        ConfigManaged => (409, "config_managed"),
        PrincipalDisabled => (403, "principal_disabled"),
        NoGrant => (403, "no_grant"),
        ModeExceedsGrant => (403, "mode_exceeds_grant"),
        LifetimeTooLong => (400, "lifetime_too_long"),
        IdempotencyConflict => (409, "idempotency_conflict"),
        SessionEnded => (409, "session_ended"),
        Store | StandingPending => (503, "unavailable"),
    };
    error(
        StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
        code,
    )
}
/// Extract a single bounded visible-ASCII mutation retry key.
pub fn idempotency_key(headers: &HeaderMap) -> Option<&str> {
    if headers.get_all("Idempotency-Key").iter().count() != 1 {
        return None;
    }
    let key = headers.get("Idempotency-Key")?.to_str().ok()?;
    (1..=128).contains(&key.len()).then_some(())?;
    key.bytes()
        .all(|b| (0x21..=0x7e).contains(&b))
        .then_some(key)
}
fn lifecycle_actor(caller: &Caller, origin: Origin, principal: PrincipalId) -> Actor {
    match caller.principal {
        Some(id) if id == principal => Actor::Owner,
        Some(id) => Actor::Admin(id.to_hex()),
        None if origin == Origin::Socket => Actor::Socket,
        None => Actor::ConfigAdmin,
    }
}
fn record_session(
    ctx: &MgmtContext,
    caller: &Caller,
    origin: Origin,
    session: &AccessSession,
    kind: LifecycleKind,
    reason: &'static str,
) -> Result<(), AuditUnavailable> {
    ctx.lifecycle.record(&LifecycleRecord {
        kind,
        at_ms: ctx.authority.now_ms(),
        principal: session.principal.to_hex(),
        session: Some(session.id.to_hex()),
        route: Some(session.route.as_str().to_owned()),
        actor: lifecycle_actor(caller, origin, session.principal),
        reason,
        policy: None,
    })
}
fn audit_unavailable() -> Response<Bytes> {
    error(StatusCode::SERVICE_UNAVAILABLE, "audit_unavailable")
}
fn session_view(ctx: &MgmtContext, s: &AccessSession) -> Value {
    json!({"policy_revision":ctx.status.policy_revision(s.route.as_str()),
        "route_kind":ctx.status.route_kind(s.route.as_str()),
        "connect":ctx.status.route_connect(s.route.as_str()),
        "config_revision":ctx.status.config_revision(), "id":s.id, "principal":s.principal, "route":s.route, "mode":s.mode,
        "state":s.state, "created_ms":s.created_ms, "expires_ms":s.expires_ms,
        "idle_timeout_ms":s.idle_timeout_ms, "last_used_ms":s.last_used_ms,
        "revoke_reason":s.revoke_reason.map(EndReason::as_str), "label":s.label,
        "isolated":s.isolated, "scope":s.scope, "scope_continuity":ctx.authority.scope_continuous(s)})
}
fn principal_view(ctx: &MgmtContext, id: PrincipalId) -> Result<Value, AuthorityError> {
    let p = ctx
        .authority
        .principal(id)
        .ok_or(AuthorityError::NotFound)?;
    let mut value = json!(p);
    value["grants"] = json!(
        ctx.authority
            .grants_of(id)
            .into_iter()
            .map(|(route, mode)| json!({"route":route,"mode":mode}))
            .collect::<Vec<_>>()
    );
    value["credentials"] = json!(ctx.authority.credentials_of(id));
    Ok(value)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    name: String,
    kind: PrincipalKind,
    #[serde(default)]
    admin: bool,
    verifier: Option<String>,
}
impl fmt::Debug for CreateBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CreateBody { [REDACTED] }")
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalCredentialBody {
    verifier: String,
    expires: Option<String>,
}
impl fmt::Debug for PrincipalCredentialBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PrincipalCredentialBody { [REDACTED] }")
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantsBody {
    grants: Vec<GrantBody>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantBody {
    route: String,
    mode: Mode,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcquireBody {
    route: String,
    mode: Option<Mode>,
    lifetime: Option<String>,
    #[serde(default)]
    isolated: bool,
    label: Option<String>,
    verifier: String,
    secret_sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialBody {
    verifier: String,
    secret_sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenewBody {
    mode: Option<Mode>,
    add_credential: Option<CredentialBody>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeBody {}
impl fmt::Debug for AcquireBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AcquireBody { [REDACTED] }")
    }
}
impl fmt::Debug for CredentialBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialBody { [REDACTED] }")
    }
}
impl fmt::Debug for RenewBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RenewBody { [REDACTED] }")
    }
}
impl fmt::Debug for RevokeBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RevokeBody {}")
    }
}
fn credential(verifier: String, digest: &str) -> Option<(Verifier, [u8; 32])> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut hash = [0; 32];
    data_encoding::HEXLOWER
        .decode_mut(digest.as_bytes(), &mut hash)
        .ok()?;
    Some((Verifier::try_from(verifier).ok()?, hash))
}
fn acquire_body(bytes: &[u8], key: &str) -> Option<AcquireRequest> {
    let body: AcquireBody = serde_json::from_slice(bytes).ok()?;
    let route = RouteId::new(&body.route)?;
    if body
        .label
        .as_ref()
        .is_some_and(|l| l.len() > 256 || l.chars().any(char::is_control))
    {
        return None;
    }
    let lifetime_ms = match body.lifetime.as_deref() {
        Some(s) => {
            Some(u64::try_from(crate::config::parse_duration(s)?.as_millis()).unwrap_or(u64::MAX))
        }
        None => None,
    };
    let (verifier, secret_sha256) = credential(body.verifier, &body.secret_sha256)?;
    Some(AcquireRequest {
        route,
        mode: body.mode,
        lifetime_ms,
        isolated: body.isolated,
        label: body.label,
        idempotency_key: key.to_owned(),
        verifier,
        secret_sha256,
    })
}
#[derive(Clone, Copy)]
enum Endpoint {
    Sessions,
    Session(SessionId),
    Renew(SessionId),
    Revoke(SessionId),
    Principals,
    Principal(PrincipalId),
    Disable(PrincipalId),
    Enable(PrincipalId),
    Credentials(PrincipalId),
    Credential(PrincipalId, CredentialId),
    Grants(PrincipalId),
    Connections,
    Status,
}
fn endpoint(path: &str) -> Option<Endpoint> {
    use Endpoint::*;
    let parts: Vec<_> = path.split('/').collect();
    match parts.as_slice() {
        ["", "v1", "sessions"] => Some(Sessions),
        ["", "v1", "sessions", id] => Some(Session(SessionId::from_hex(id)?)),
        ["", "v1", "sessions", id, "renew"] => Some(Renew(SessionId::from_hex(id)?)),
        ["", "v1", "sessions", id, "revoke"] => Some(Revoke(SessionId::from_hex(id)?)),
        ["", "v1", "principals"] => Some(Principals),
        ["", "v1", "principals", id] => Some(Principal(PrincipalId::from_hex(id)?)),
        ["", "v1", "principals", id, "disable"] => Some(Disable(PrincipalId::from_hex(id)?)),
        ["", "v1", "principals", id, "enable"] => Some(Enable(PrincipalId::from_hex(id)?)),
        ["", "v1", "principals", id, "credentials"] => {
            Some(Credentials(PrincipalId::from_hex(id)?))
        }
        ["", "v1", "principals", id, "credentials", cid] => Some(Credential(
            PrincipalId::from_hex(id)?,
            CredentialId::from_hex(cid)?,
        )),
        ["", "v1", "principals", id, "grants"] => Some(Grants(PrincipalId::from_hex(id)?)),
        ["", "v1", "connections"] => Some(Connections),
        ["", "v1", "status"] => Some(Status),
        _ => None,
    }
}
/// Authenticate, authorize and dispatch one bounded management request.
pub fn handle(ctx: &MgmtContext, origin: Origin, req: Request<Bytes>) -> Response<Bytes> {
    if let Some(response) = leader_response(ctx, origin, req.uri()) { return response; }
    use Endpoint::*;
    let caller = match authenticate(ctx, origin, req.headers()) {
        Ok(c) => c,
        Err(_) => {
            let mut r = error(StatusCode::UNAUTHORIZED, "unauthorized");
            r.headers_mut()
                .insert(WWW_AUTHENTICATE, http::HeaderValue::from_static("Bearer"));
            return r;
        }
    };
    if let Some((route, op, n)) = super::policy::endpoint(req.uri().path()) {
        return super::policy::handle(ctx, &caller, origin, &req, route, op, n);
    }
    let Some(ep) = endpoint(req.uri().path()) else {
        return error(StatusCode::NOT_FOUND, "not_found");
    };
    let method = req.method().as_str();
    let allowed = match ep {
        Sessions | Principals => method == "GET" || method == "POST",
        Session(_) | Principal(_) | Connections | Status => method == "GET",
        Renew(_) | Revoke(_) => method == "POST",
        Disable(_) | Enable(_) | Credentials(_) => method == "POST",
        Credential(_, _) => method == "DELETE",
        Grants(_) => method == "PUT",
    };
    if !allowed {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    }
    let mut authorized_session = None;
    match ep {
        Sessions if method == "POST" => {
            if caller.principal.is_none() {
                return error(StatusCode::FORBIDDEN, "forbidden");
            }
        }
        Session(id) | Revoke(id) | Renew(id) => {
            if matches!(ep, Renew(_)) && caller.principal.is_none() {
                return error(StatusCode::FORBIDDEN, "forbidden");
            }
            let Some(s) = ctx.authority.session(id) else {
                return error(StatusCode::NOT_FOUND, "not_found");
            };
            if caller.principal != Some(s.principal) && (!caller.admin || matches!(ep, Renew(_))) {
                return error(StatusCode::NOT_FOUND, "not_found");
            }
            authorized_session = Some(s);
        }
        Sessions => {}
        Principals
        | Principal(_)
        | Disable(_)
        | Enable(_)
        | Credentials(_)
        | Credential(_, _)
        | Grants(_)
        | Connections
        | Status => {
            if !caller.admin {
                return error(StatusCode::FORBIDDEN, "forbidden");
            }
        }
    }
    if matches!(method, "POST" | "PUT" | "DELETE") {
        let Some(key) = idempotency_key(req.headers()) else {
            return error(StatusCode::BAD_REQUEST, "idempotency_key_required");
        };
        let bad_request = || error(StatusCode::BAD_REQUEST, "bad_request");
        let body = if req.body().is_empty() {
            b"{}".as_slice()
        } else {
            req.body().as_ref()
        };
        let replay;
        let result = match ep {
            Sessions => {
                let Some(principal) = caller.principal else {
                    return error(StatusCode::FORBIDDEN, "forbidden");
                };
                let Some(request) = acquire_body(body, key) else {
                    return bad_request();
                };
                replay = ctx.authority.acquisition_recorded(principal, key);
                ctx.authority.acquire_session(principal, request)
            }
            Renew(id) => {
                let Some(principal) = caller.principal else {
                    return error(StatusCode::FORBIDDEN, "forbidden");
                };
                let Ok(request) = serde_json::from_slice::<RenewBody>(body) else {
                    return bad_request();
                };
                let add = match request.add_credential {
                    Some(c) => match credential(c.verifier, &c.secret_sha256) {
                        Some(c) => Some(c),
                        None => return bad_request(),
                    },
                    None => None,
                };
                replay = ctx.authority.renewal_recorded(principal, key);
                if !replay {
                    let Some(s) = authorized_session.as_ref() else {
                        return error(StatusCode::NOT_FOUND, "not_found");
                    };
                    // Ended sessions cannot renew; retain their refusal without requiring audit health.
                    if s.state != crate::state::model::SessionState::Active
                        || ctx.authority.now_ms() >= s.expires_ms
                    {
                        let _ = record_session(
                            ctx,
                            &caller,
                            origin,
                            s,
                            LifecycleKind::ResumeRefused,
                            "session_ended",
                        );
                        return authority_error(AuthorityError::SessionEnded);
                    }
                    if record_session(ctx, &caller, origin, s, LifecycleKind::Renewed, "renewed")
                        .is_err()
                    {
                        return audit_unavailable();
                    }
                }
                match ctx
                    .authority
                    .renew_idempotent(principal, id, request.mode, add, key)
                {
                    Ok(s) => Ok(s),
                    Err(e) => match e.into_authority() {
                        Some(e) => Err(e),
                        None => return error(StatusCode::BAD_REQUEST, "rotation_salt_mismatch"),
                    },
                }
            }
            Revoke(id) => {
                if serde_json::from_slice::<RevokeBody>(body).is_err() {
                    return bad_request();
                }
                let Some(s) = ctx.authority.session(id) else {
                    return error(StatusCode::NOT_FOUND, "not_found");
                };
                let reason = if caller.principal == Some(s.principal) {
                    EndReason::Requested
                } else {
                    EndReason::AdminRevoked
                };
                replay = ctx
                    .authority
                    .revocation_recorded(idempotency_owner(&caller, origin), key);
                ctx.authority
                    .revoke_idempotent(idempotency_owner(&caller, origin), id, reason, key)
            }
            Principals => {
                let Ok(request) = serde_json::from_slice::<CreateBody>(body) else {
                    return bad_request();
                };
                if ctx
                    .admins
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .any(|(name, _)| name == &request.name)
                {
                    return authority_error(AuthorityError::NameTaken);
                }
                let verifier = match request.verifier {
                    Some(v) => match Verifier::try_from(v) {
                        Ok(v) => Some(v),
                        Err(_) => return bad_request(),
                    },
                    None => None,
                };
                let p = match ctx.authority.create_principal_by_unless(
                    idempotency_owner(&caller, origin),
                    &request.name,
                    request.kind,
                    request.admin,
                    key,
                    |name| ctx.admins.read().unwrap_or_else(std::sync::PoisonError::into_inner)
                        .iter().any(|(admin, _)| admin == name),
                ) {
                    Ok(p) => p,
                    Err(e) => return authority_error(e),
                };
                if let Some(v) = verifier
                    && let Err(e) = ctx.authority.add_principal_credential_idempotent(
                        idempotency_owner(&caller, origin),
                        p.id,
                        v,
                        None,
                        &format!("create:{key}"),
                    )
                {
                    return match e.into_authority() {
                        Some(e) => authority_error(e),
                        None => error(StatusCode::CONFLICT, "credential_limit"),
                    };
                }
                return match principal_view(ctx, p.id) {
                    Ok(p) => json_response(StatusCode::CREATED, p),
                    Err(e) => authority_error(e),
                };
            }
            Disable(id) | Enable(id) => {
                if serde_json::from_slice::<RevokeBody>(body).is_err() {
                    return bad_request();
                }
                let disable_replay = ctx
                    .authority
                    .disablement_recorded(idempotency_owner(&caller, origin), key);
                if let Err(e) = ctx.authority.set_principal_enabled_idempotent(
                    idempotency_owner(&caller, origin),
                    id,
                    matches!(ep, Enable(_)),
                    key,
                ) {
                    return authority_error(e);
                }
                if matches!(ep, Disable(_))
                    && !disable_replay
                    && ctx
                        .lifecycle
                        .record(&LifecycleRecord {
                            kind: LifecycleKind::PrincipalDisabled,
                            policy: None,
                            at_ms: ctx.authority.now_ms(),
                            principal: id.to_hex(),
                            session: None,
                            route: None,
                            actor: lifecycle_actor(&caller, origin, id),
                            reason: "principal_disabled",
                        })
                        .is_err()
                {
                    let _ = ctx.authority.forget_retry(
                        idempotency_owner(&caller, origin),
                        &format!("\0disable:{key}"),
                    );
                    return audit_unavailable();
                }
                return match principal_view(ctx, id) {
                    Ok(p) => json_response(StatusCode::OK, p),
                    Err(e) => authority_error(e),
                };
            }
            Credentials(id) => {
                let Ok(request) = serde_json::from_slice::<PrincipalCredentialBody>(body) else {
                    return bad_request();
                };
                let Ok(verifier) = Verifier::try_from(request.verifier) else {
                    return bad_request();
                };
                let expires_in_ms = match request.expires {
                    Some(s) => {
                        let Some(duration) = crate::config::parse_duration(&s) else {
                            return bad_request();
                        };
                        let Ok(ms) = u64::try_from(duration.as_millis()) else {
                            return bad_request();
                        };
                        if ms == 0 {
                            return bad_request();
                        }
                        Some(ms)
                    }
                    None => None,
                };
                return match ctx.authority.add_principal_credential_idempotent(
                    idempotency_owner(&caller, origin),
                    id,
                    verifier,
                    expires_in_ms,
                    key,
                ) {
                    Ok(c) => json_response(StatusCode::CREATED, c),
                    Err(e) => match e.into_authority() {
                        Some(AuthorityError::LifetimeTooLong) => bad_request(),
                        Some(e) => authority_error(e),
                        None => error(StatusCode::CONFLICT, "credential_limit"),
                    },
                };
            }
            Credential(id, cid) => {
                if serde_json::from_slice::<RevokeBody>(body).is_err() {
                    return bad_request();
                }
                return match ctx.authority.revoke_principal_credential_idempotent(
                    idempotency_owner(&caller, origin),
                    id,
                    cid,
                    key,
                ) {
                    Ok(c) => json_response(StatusCode::OK, c),
                    Err(e) => authority_error(e),
                };
            }
            Grants(id) => {
                let Ok(request) = serde_json::from_slice::<GrantsBody>(body) else {
                    return bad_request();
                };
                let mut routes = std::collections::HashSet::new();
                let mut grants = Vec::new();
                for grant in request.grants {
                    let Some(route) = RouteId::new(&grant.route) else {
                        return bad_request();
                    };
                    if !routes.insert(route.clone()) {
                        return bad_request();
                    }
                    grants.push((route, grant.mode));
                }
                if let Err(e) = ctx.authority.set_grants_idempotent(
                    idempotency_owner(&caller, origin),
                    id,
                    grants,
                    key,
                ) {
                    return authority_error(e);
                }
                return match principal_view(ctx, id) {
                    Ok(p) => json_response(StatusCode::OK, p),
                    Err(e) => authority_error(e),
                };
            }
            Principal(_) | Session(_) | Connections | Status => {
                return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
            }
        };
        if let Ok(s) = &result {
            let (kind, reason, revoke_on_failure) = match ep {
                Sessions => (LifecycleKind::Issued, "issued", true),
                Renew(_) => (LifecycleKind::Renewed, "renewed", false),
                Revoke(_) => (
                    LifecycleKind::Revoked,
                    s.revoke_reason.unwrap_or(EndReason::AdminRevoked).as_str(),
                    false,
                ),
                _ => return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
            };
            if !replay
                && !matches!(ep, Renew(_))
                && record_session(ctx, &caller, origin, s, kind, reason).is_err()
            {
                if revoke_on_failure {
                    let _ = ctx.authority.revoke(s.id, EndReason::AdminRevoked);
                } else if matches!(ep, Revoke(_)) {
                    let _ = ctx.authority.forget_retry(
                        idempotency_owner(&caller, origin),
                        &format!("\0revoke:{key}"),
                    );
                }
                return audit_unavailable();
            }
        } else if matches!(&result, Err(AuthorityError::SessionEnded))
            && let Renew(id) = ep
            && let Some(s) = ctx.authority.session(id)
        {
            let _ = record_session(
                ctx,
                &caller,
                origin,
                &s,
                LifecycleKind::ResumeRefused,
                "session_ended",
            );
        }
        return match result {
            Ok(s) => json_response(
                if matches!(ep, Sessions) {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                session_view(ctx, &s),
            ),
            Err(e) => authority_error(e),
        };
    }
    let result = match ep {
        Sessions => {
            let rows = ctx.authority.all_sessions();
            json!(
                rows.iter()
                    .filter(|s| caller.admin || caller.principal == Some(s.principal))
                    .map(|s| session_view(ctx, s))
                    .collect::<Vec<_>>()
            )
        }
        Session(id) => match ctx.authority.session(id) {
            Some(s) => session_view(ctx, &s),
            None => return error(StatusCode::NOT_FOUND, "not_found"),
        },
        Principals => json!(ctx.authority.principals()),
        Principal(id) => match principal_view(ctx, id) {
            Ok(p) => p,
            Err(e) => return authority_error(e),
        },
        Connections => json!(ctx.connections.list()),
        Status => {
            let route_ids = ctx.status.route_ids();
            let policy_revisions: serde_json::Map<String, Value> = route_ids.iter()
                .map(|route| (route.clone(), json!(ctx.status.policy_revision(route))))
                .collect();
            let legacy = policy_revisions.values().next().filter(|first| {
                first.is_string() && (policy_revisions.len() == 1 || policy_revisions.values().all(|value| value == *first))
            }).cloned();
            let mut status = json!({"deployment_id":data_encoding::HEXLOWER.encode(&ctx.authority.deployment_id()),
            "store_format":ctx.authority.store_format_version(), "route_ids":route_ids,
            "policy_revisions":policy_revisions, "config_revision":ctx.status.config_revision(),
            "config_restart_pending":ctx.status.config_restart_pending(), "config_last_reload":ctx.status.config_last_reload()});
            if let Some(revision) = legacy {
                status["policy_revision"] = revision;
            }
            status
        }
        Renew(_)
        | Revoke(_)
        | Disable(_)
        | Enable(_)
        | Credentials(_)
        | Credential(_, _)
        | Grants(_) => return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
    };
    json_response(StatusCode::OK, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mgmt::audit::LifecycleRecord;
    use crate::state::{authority::SessionLimits, store::RedbStore};
    use crate::token::keys::RootKey;

    struct Sources;
    impl ConnectionSource for Sources {
        fn list(&self) -> Vec<ConnectionView> { vec![] }
    }
    impl StatusSource for Sources {
        fn policy_revision(&self, _: &str) -> Option<String> { None }
        fn config_revision(&self) -> Option<String> { None }
        fn route_ids(&self) -> Vec<String> { vec![] }
    }
    impl LifecycleSink for Sources {
        fn record(&self, _: &LifecycleRecord) -> Result<(), AuditUnavailable> { Ok(()) }
    }
    struct Follower(Option<String>);
    impl Leadership for Follower { fn leadership(&self) -> (bool, Option<String>) { (false, self.0.clone()) } }
    #[test]
    fn followers_redirect_all_paths_before_authentication() {
        let dir = std::env::temp_dir().join(format!("redirect-{}", SessionId::random().to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(RedbStore::open(&dir, &RootKey::from_hex(&"01".repeat(32)).unwrap()).unwrap());
        let mut ctx = MgmtContext { leadership: Some(Arc::new(Follower(Some("https://leader:443".into())))), policy: None,
            authority: Authority::load(store, SessionLimits::default()).unwrap(), admins: Arc::default(),
            connections: Arc::new(Sources), status: Arc::new(Sources), lifecycle: Arc::new(Sources) };
        assert!(leader_response(&ctx, Origin::Socket, &"/v1/status".parse().unwrap()).is_none());
        let local = handle(&ctx, Origin::Socket, Request::builder().uri("/v1/status").body(Bytes::new()).unwrap());
        assert_ne!(local.status(), StatusCode::TEMPORARY_REDIRECT);
        for path in ["/v1/status?x=1", "/v1/principals", "/unknown"] {
            for method in ["GET", "POST", "DELETE"] {
                let response = handle(&ctx, Origin::Network, Request::builder().method(method).uri(path).body(Bytes::from_static(b"invalid json")).unwrap());
                assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
                assert_eq!(response.headers()[http::header::LOCATION], format!("https://leader:443{path}"));
                assert!(response.body().is_empty());
            }
        }
        for url in [None, Some("invalid".into())] {
            ctx.leadership = Some(Arc::new(Follower(url)));
            let response = handle(&ctx, Origin::Network, Request::new(Bytes::new()));
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[http::header::RETRY_AFTER], "2");
            assert_eq!(serde_json::from_slice::<Value>(response.body()).unwrap()["error"], "no_leader");
        }
        drop(ctx); std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn configured_admin_name_refuses_principal_creation() {
        let dir = std::env::temp_dir().join(format!("routes-{}", SessionId::random().to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(RedbStore::open(&dir, &RootKey::from_hex(&"01".repeat(32)).unwrap()).unwrap());
        let authority = Authority::load(store, SessionLimits::default()).unwrap();
        let ctx = MgmtContext { leadership: None, policy: None,
            authority,
            admins: Arc::new(RwLock::new(vec![("reserved".into(),
                ScramVerifier::from_password("fixture", b"salt", 1))])),
            connections: Arc::new(Sources), status: Arc::new(Sources), lifecycle: Arc::new(Sources),
        };
        let request = Request::builder().method("POST").uri("/v1/principals")
            .header("idempotency-key", "create-reserved")
            .body(Bytes::from_static(br#"{"name":"reserved","kind":"agent"}"#)).unwrap();
        let response = handle(&ctx, Origin::Socket, request);
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(serde_json::from_slice::<Value>(response.body()).unwrap()["error"], "name_taken");
        assert!(ctx.authority.principal_by_name("reserved").is_none());
        drop(ctx);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
