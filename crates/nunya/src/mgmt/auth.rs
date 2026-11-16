//! Management bearer authentication with uniform failures.
use super::routes::MgmtContext;
use crate::{
    pgwire::auth::ScramVerifier, spanner::auth::verify_password, state::model::PrincipalState,
    token::PrincipalId,
};
use http::{HeaderMap, header::AUTHORIZATION};
use std::{fmt, sync::OnceLock};

/// Trust boundary of a management request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// Locally authenticated Unix socket.
    Socket,
    /// Network connection requiring a bearer credential.
    Network,
}
/// Authenticated management identity.
pub struct Caller {
    /// Store principal, absent for configuration admins and socket callers.
    pub principal: Option<PrincipalId>,
    /// Authenticated name.
    pub name: String,
    /// Administrative privilege.
    pub admin: bool,
}
impl fmt::Debug for Caller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Caller")
            .field("admin", &self.admin)
            .field("has_principal", &self.principal.is_some())
            .finish()
    }
}
/// Data-free authentication refusal.
#[derive(Debug, PartialEq, Eq)]
pub struct Unauthorized;
fn bearer(headers: &HeaderMap) -> Result<(&str, &str), Unauthorized> {
    if headers.get_all(AUTHORIZATION).iter().count() != 1 {
        return Err(Unauthorized);
    }
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(Unauthorized)?;
    let (name, secret) = value
        .strip_prefix("Bearer nunya.")
        .and_then(|v| v.split_once('.'))
        .ok_or(Unauthorized)?;
    if name.is_empty()
        || name.len() > 64
        || secret.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Unauthorized);
    }
    Ok((name, secret))
}
fn dummy(secret: &str) {
    static VERIFIER: OnceLock<ScramVerifier> = OnceLock::new();
    let v = VERIFIER.get_or_init(|| {
        ScramVerifier::from_password("management-dummy", b"management-dummy-salt", 4096)
    });
    let _ = verify_password(v, secret);
}
/// Authenticate socket trust or a principal/configuration bearer credential.
pub fn authenticate(
    ctx: &MgmtContext,
    origin: Origin,
    headers: &HeaderMap,
) -> Result<Caller, Unauthorized> {
    if origin == Origin::Socket {
        return Ok(Caller {
            principal: None,
            name: "local-socket".into(),
            admin: true,
        });
    }
    let (name, secret) = bearer(headers)?;
    if let Some((_, verifier)) = ctx.admins.iter().find(|(n, _)| n == name) {
        return if verify_password(verifier, secret) {
            Ok(Caller {
                principal: None,
                name: name.into(),
                admin: true,
            })
        } else {
            Err(Unauthorized)
        };
    }
    let Some(p) = ctx.authority.principal_by_name(name) else {
        dummy(secret);
        return Err(Unauthorized);
    };
    if p.state != PrincipalState::Enabled {
        dummy(secret);
        return Err(Unauthorized);
    }
    let verifiers = ctx.authority.principal_credential_verifiers(p.id);
    let mut matches = false;
    if verifiers.is_empty() {
        dummy(secret);
    }
    for verifier in verifiers {
        if let Some(parsed) = ScramVerifier::parse(&verifier.0.to_pg_string()) {
            matches |= verify_password(&parsed, secret);
        }
    }
    if matches {
        Ok(Caller {
            principal: Some(p.id),
            name: p.name,
            admin: p.administrator,
        })
    } else {
        Err(Unauthorized)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn header_parsing_rejects_malformed_credentials() {
        assert!(bearer(&HeaderMap::new()).is_err());
        for value in [
            "Basic nunya.a.b",
            "Bearer a.b",
            "Bearer nunya..b",
            "Bearer nunya.a.",
            "Bearer nunya.session:abcdef.secret",
            "Bearer nunya.a:b.secret",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, value.parse().unwrap());
            assert!(bearer(&headers).is_err());
        }
    }
    #[test]
    fn secret_may_contain_dots() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            "Bearer nunya.agent.secret.with.dots".parse().unwrap(),
        );
        assert!(matches!(
            bearer(&headers),
            Ok(("agent", "secret.with.dots"))
        ));
    }
    #[test]
    fn caller_debug_hides_name() {
        let c = Caller {
            principal: None,
            name: "private-name".into(),
            admin: true,
        };
        assert!(!format!("{c:?}").contains("private-name"));
    }
}
