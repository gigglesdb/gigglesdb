//! Pure synchronous management request dispatch.
use super::auth::{Origin, authenticate};
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
use std::{fmt, sync::Arc};

/// Read-only connection registry adapter.
pub trait ConnectionSource: Send + Sync {
    /// Snapshot current connections.
    fn list(&self) -> Vec<ConnectionView>;
}
/// Read-only runtime revision adapter.
pub trait StatusSource: Send + Sync {
    /// Current policy revision.
    fn policy_revision(&self) -> Option<String>;
    /// Current configuration revision.
    fn config_revision(&self) -> Option<String>;
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
/// Shared management services and configured administrators.
pub struct MgmtContext {
    /// Durable authority.
    pub authority: Arc<Authority>,
    /// Configuration administrator verifiers.
    pub admins: Vec<(String, ScramVerifier)>,
    /// Active connection snapshot provider.
    pub connections: Arc<dyn ConnectionSource>,
    /// Runtime revision provider.
    pub status: Arc<dyn StatusSource>,
}
impl fmt::Debug for MgmtContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MgmtContext { [REDACTED] }")
    }
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
        Store => (503, "unavailable"),
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
fn session_view(ctx: &MgmtContext, s: &AccessSession) -> Value {
    json!({"id":s.id, "principal":s.principal, "route":s.route, "mode":s.mode,
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
        let result = match ep {
            Sessions => {
                let Some(principal) = caller.principal else {
                    return error(StatusCode::FORBIDDEN, "forbidden");
                };
                let Some(request) = acquire_body(body, key) else {
                    return bad_request();
                };
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
                ctx.authority
                    .renew_idempotent(principal, id, request.mode, add, key)
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
                ctx.authority.revoke_idempotent(
                    caller.principal.unwrap_or(PrincipalId([0; 16])),
                    id,
                    reason,
                    key,
                )
            }
            Principals => {
                let Ok(request) = serde_json::from_slice::<CreateBody>(body) else {
                    return bad_request();
                };
                let verifier = match request.verifier {
                    Some(v) => match Verifier::try_from(v) {
                        Ok(v) => Some(v),
                        Err(_) => return bad_request(),
                    },
                    None => None,
                };
                let p = match ctx.authority.create_principal(
                    &request.name,
                    request.kind,
                    request.admin,
                    key,
                ) {
                    Ok(p) => p,
                    Err(e) => return authority_error(e),
                };
                if let Some(v) = verifier
                    && let Err(e) = ctx.authority.add_principal_credential_idempotent(
                        caller.principal.unwrap_or(PrincipalId([0; 16])),
                        p.id,
                        v,
                        None,
                        &format!("create:{key}"),
                    )
                {
                    return authority_error(e);
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
                if let Err(e) = ctx.authority.set_principal_enabled_idempotent(
                    caller.principal.unwrap_or(PrincipalId([0; 16])),
                    id,
                    matches!(ep, Enable(_)),
                    key,
                ) {
                    return authority_error(e);
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
                    caller.principal.unwrap_or(PrincipalId([0; 16])),
                    id,
                    verifier,
                    expires_in_ms,
                    key,
                ) {
                    Ok(c) => json_response(StatusCode::CREATED, c),
                    Err(AuthorityError::LifetimeTooLong) => bad_request(),
                    Err(e) => authority_error(e),
                };
            }
            Credential(id, cid) => {
                if serde_json::from_slice::<RevokeBody>(body).is_err() {
                    return bad_request();
                }
                return match ctx.authority.revoke_principal_credential_idempotent(
                    caller.principal.unwrap_or(PrincipalId([0; 16])),
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
                    caller.principal.unwrap_or(PrincipalId([0; 16])),
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
            json!({"deployment_id":data_encoding::HEXLOWER.encode(&ctx.authority.deployment_id()),
            "store_format":ctx.authority.store_format_version(), "route_ids":ctx.status.route_ids(),
            "policy_revision":ctx.status.policy_revision(), "config_revision":ctx.status.config_revision()})
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
