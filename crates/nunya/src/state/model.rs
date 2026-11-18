//! Strict durable domain records, redacted diagnostics, and binary keys.
use crate::{
    pgwire::auth::ScramVerifier,
    token::{PrincipalId, ScopeId, SessionId},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt;

crate::token::id_type!(CredentialId, "Non-secret credential identifier.");

/// Validated route identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RouteId(String);
impl RouteId {
    /// Accept a 1-32 byte lowercase alphanumeric route with internal hyphens.
    pub fn new(s: &str) -> Option<Self> {
        if s.is_empty()
            || s.len() > 32
            || s.starts_with('-')
            || !s
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return None;
        }
        Some(Self(s.to_owned()))
    }
    /// Borrow the validated route name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for RouteId {
    type Error = ModelError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(&s).ok_or(ModelError)
    }
}
impl From<RouteId> for String {
    fn from(route: RouteId) -> Self {
        route.0
    }
}

/// Authorization mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Read access.
    ReadOnly,
    /// Read and write access.
    ReadWrite,
}

/// Principal identity kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Human identity.
    Person,
    /// Agent identity.
    Agent,
    /// Service identity.
    Service,
}

/// Principal authorization state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalState {
    /// Enabled identity.
    Enabled,
    /// Disabled identity.
    Disabled,
}

/// Principal provisioning source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalSource {
    /// Provisioned through the API.
    Api,
    /// Provisioned through configuration.
    Config,
}

/// Credential lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialState {
    /// Usable credential.
    Active,
    /// Revoked credential.
    Revoked,
}

/// Session lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Usable session.
    Active,
    /// Revoked session.
    Revoked,
    /// Expired session.
    Expired,
}

/// Fixed data-free session ending reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// Session ended for this reason.
    Requested,
    /// Session ended for this reason.
    AdminRevoked,
    /// Session ended for this reason.
    PrincipalDisabled,
    /// Session ended for this reason.
    ConfigRemoved,
    /// Session ended for this reason.
    Expired,
    /// Session ended for this reason.
    IdleTimeout,
    /// The principal no longer grants the session route.
    GrantRemoved,
}
impl Mode {
    /// Return the more restrictive of two modes.
    pub fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::ReadWrite, Self::ReadWrite) => Self::ReadWrite,
            (Self::ReadOnly, _) | (_, Self::ReadOnly) => Self::ReadOnly,
        }
    }
    /// Whether this mode authorizes writes.
    pub fn permits_write(self) -> bool {
        self == Self::ReadWrite
    }
}
impl EndReason {
    /// Return the stable snake-case reason code.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AdminRevoked => "admin_revoked",
            Self::PrincipalDisabled => "principal_disabled",
            Self::ConfigRemoved => "config_removed",
            Self::Expired => "expired",
            Self::IdleTimeout => "idle_timeout",
            Self::GrantRemoved => "grant_removed",
        }
    }
}

/// SCRAM verifier serialized using PostgreSQL's secret verifier text.
#[derive(Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Verifier(
    /// Authentication verifier, excluded from diagnostics.
    pub ScramVerifier,
);
impl Clone for Verifier {
    fn clone(&self) -> Self {
        Self(ScramVerifier {
            salt: self.0.salt.clone(),
            iterations: self.0.iterations,
            stored_key: self.0.stored_key,
            server_key: self.0.server_key,
        })
    }
}
impl fmt::Debug for Verifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Verifier([REDACTED])")
    }
}
impl TryFrom<String> for Verifier {
    type Error = ModelError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        ScramVerifier::parse(&s).map(Self).ok_or(ModelError)
    }
}
impl From<Verifier> for String {
    fn from(v: Verifier) -> Self {
        v.0.to_pg_string()
    }
}

mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer, const N: usize>(
        bytes: &[u8; N],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.serialize_str(&data_encoding::HEXLOWER.encode(bytes))
    }
    pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        d: D,
    ) -> Result<[u8; N], D::Error> {
        let text = String::deserialize(d)?;
        if text.len() != N * 2
            || !text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom("invalid stored bytes"));
        }
        data_encoding::HEXLOWER
            .decode(text.as_bytes())
            .ok()
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| serde::de::Error::custom("invalid stored bytes"))
    }
}

/// Durable Principal record.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    /// Stored id.
    pub id: PrincipalId,
    /// Stored name.
    pub name: String,
    /// Stored kind.
    pub kind: PrincipalKind,
    /// Stored state.
    pub state: PrincipalState,
    /// Stored source.
    pub source: PrincipalSource,
    /// Stored administrator.
    pub administrator: bool,
    /// Stored revision.
    pub revision: u64,
}

/// Durable PrincipalCredential record.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalCredential {
    /// Stored id.
    pub id: CredentialId,
    /// Stored principal.
    pub principal: PrincipalId,
    /// Stored verifier.
    pub verifier: Verifier,
    /// Stored state.
    pub state: CredentialState,
    /// Stored expires ms.
    pub expires_ms: Option<u64>,
}
impl fmt::Debug for PrincipalCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrincipalCredential")
            .field("id", &self.id)
            .field("principal", &self.principal)
            .field("verifier", &"[REDACTED]")
            .field("state", &self.state)
            .field("expires_ms", &self.expires_ms)
            .finish()
    }
}

/// Durable Grant record.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    /// Stored principal.
    pub principal: PrincipalId,
    /// Stored route.
    pub route: RouteId,
    /// Stored mode.
    pub mode: Mode,
}

/// Durable AccessSession record.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessSession {
    /// Stored id.
    pub id: SessionId,
    /// Stored principal.
    pub principal: PrincipalId,
    /// Stored route.
    pub route: RouteId,
    /// Stored mode.
    pub mode: Mode,
    /// Stored scope.
    pub scope: ScopeId,
    /// Stored created ms.
    pub created_ms: u64,
    /// Stored expires ms.
    pub expires_ms: u64,
    /// Stored idle timeout ms.
    pub idle_timeout_ms: Option<u64>,
    /// Stored last used ms.
    pub last_used_ms: u64,
    /// Stored state.
    pub state: SessionState,
    /// Stored revoke reason.
    pub revoke_reason: Option<EndReason>,
    /// Stored ended ms.
    pub ended_ms: Option<u64>,
    /// Stored label.
    pub label: Option<String>,
    /// Stored isolated.
    pub isolated: bool,
}
impl fmt::Debug for AccessSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessSession")
            .field("id", &self.id)
            .field("principal", &self.principal)
            .field("route", &self.route)
            .field("mode", &self.mode)
            .field("scope", &self.scope)
            .field("created_ms", &self.created_ms)
            .field("expires_ms", &self.expires_ms)
            .field("idle_timeout_ms", &self.idle_timeout_ms)
            .field("last_used_ms", &self.last_used_ms)
            .field("state", &self.state)
            .field("revoke_reason", &self.revoke_reason)
            .field("ended_ms", &self.ended_ms)
            .field("label", &"[REDACTED]")
            .field("isolated", &self.isolated)
            .finish()
    }
}

/// Durable SessionCredential record.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCredential {
    /// Stored id.
    pub id: CredentialId,
    /// Stored session.
    pub session: SessionId,
    /// Stored verifier.
    pub verifier: Verifier,
    /// Stored secret hash.
    #[serde(with = "hex_bytes")]
    pub secret_hash: [u8; 32],
    /// Stored expires ms.
    pub expires_ms: u64,
    /// Stored state.
    pub state: CredentialState,
}
impl fmt::Debug for SessionCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionCredential")
            .field("id", &self.id)
            .field("session", &self.session)
            .field("verifier", &"[REDACTED]")
            .field("secret_hash", &"[REDACTED]")
            .field("expires_ms", &self.expires_ms)
            .field("state", &self.state)
            .finish()
    }
}

/// Durable TokenScope record.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenScope {
    /// Stored id.
    pub id: ScopeId,
    /// Stored salt.
    #[serde(with = "hex_bytes")]
    pub salt: [u8; 32],
    /// Stored derivation version.
    pub derivation_version: u32,
    /// Stored codec version.
    pub codec_version: u32,
    /// Stored deployment id.
    #[serde(with = "hex_bytes")]
    pub deployment_id: [u8; 16],
    /// Stored route.
    pub route: RouteId,
}
impl fmt::Debug for TokenScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenScope")
            .field("id", &self.id)
            .field("salt", &"[REDACTED]")
            .field("derivation_version", &self.derivation_version)
            .field("codec_version", &self.codec_version)
            .field("deployment_id", &self.deployment_id)
            .field("route", &self.route)
            .finish()
    }
}

/// Durable IdempotencyRecord record.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyRecord {
    /// Stored principal.
    pub principal: PrincipalId,
    /// Stored key.
    pub key: String,
    /// Stored request hash.
    #[serde(with = "hex_bytes")]
    pub request_hash: [u8; 32],
    /// Stored result.
    pub result: String,
}
impl fmt::Debug for IdempotencyRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyRecord")
            .field("principal", &self.principal)
            .field("key", &"[REDACTED]")
            .field("request_hash", &"[REDACTED]")
            .field("result", &self.result)
            .finish()
    }
}

/// Data-free malformed stored-record error.
#[derive(Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("stored record is malformed")]
pub struct ModelError;
impl fmt::Debug for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
/// Encode a serializable domain record as JSON.
pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).unwrap_or_else(|_| panic!("stored record encoding failed"))
}
/// Decode a strict domain record without exposing malformed input.
pub fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, ModelError> {
    serde_json::from_slice(b).map_err(|_| ModelError)
}
/// Encode an unsigned counter using fixed-width big-endian bytes.
pub fn encode_count(count: u64) -> [u8; 8] {
    count.to_be_bytes()
}
/// Decode exactly eight counter bytes.
pub fn decode_count(bytes: &[u8]) -> Result<u64, ModelError> {
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| ModelError)?,
    ))
}

/// Binary keys for opaque store tables.
pub mod keys {
    use super::{CredentialId, ModelError, PrincipalId, RouteId, ScopeId, SessionId};
    use crate::{policy::DomainName, schema::ColumnId};
    /// Route prefix for persisted column bindings.
    pub fn domain_binding_prefix(route: &RouteId) -> Vec<u8> {
        [route.as_str().as_bytes(), &[0]].concat()
    }
    /// Route and canonical column identity key.
    pub fn domain_binding(route: &RouteId, identity: &ColumnId) -> Vec<u8> {
        [domain_binding_prefix(route), super::encode(identity)].concat()
    }
    /// Globally reserved domain name key.
    pub fn domain_name(name: &DomainName) -> Vec<u8> {
        name.0.as_bytes().to_vec()
    }
    /// Principal record key.
    pub fn principal(id: PrincipalId) -> Vec<u8> {
        id.0.to_vec()
    }
    /// Principal credential key, also supporting principal prefix scans.
    pub fn principal_credential(principal: PrincipalId, cred: CredentialId) -> Vec<u8> {
        [principal.0.as_slice(), cred.0.as_slice()].concat()
    }
    /// Principal and route grant key.
    pub fn grant(principal: PrincipalId, route: &RouteId) -> Vec<u8> {
        [principal.0.as_slice(), route.as_str().as_bytes()].concat()
    }
    /// Access session record key.
    pub fn session(id: SessionId) -> Vec<u8> {
        id.0.to_vec()
    }
    /// Session credential key, also supporting session prefix scans.
    pub fn session_credential(session: SessionId, cred: CredentialId) -> Vec<u8> {
        [session.0.as_slice(), cred.0.as_slice()].concat()
    }
    /// Token scope record key.
    pub fn scope(id: ScopeId) -> Vec<u8> {
        id.0.to_vec()
    }
    fn domain_key(id: &[u8; 16], domain: &DomainName) -> Result<Vec<u8>, ModelError> {
        let len = u16::try_from(domain.0.len()).map_err(|_| ModelError)?;
        Ok([
            id.as_slice(),
            len.to_be_bytes().as_slice(),
            domain.0.as_bytes(),
        ]
        .concat())
    }
    /// Session and length-prefixed domain counter key; domains must fit u16 bytes.
    pub fn session_counter(session: SessionId, domain: &DomainName) -> Result<Vec<u8>, ModelError> {
        domain_key(&session.0, domain)
    }
    /// Principal and length-prefixed domain prefix; domains must fit u16 bytes.
    pub fn principal_counter_prefix(
        principal: PrincipalId,
        domain: &DomainName,
    ) -> Result<Vec<u8>, ModelError> {
        domain_key(&principal.0, domain)
    }
    /// Principal counter key with a big-endian window start.
    pub fn principal_counter(
        principal: PrincipalId,
        domain: &DomainName,
        window_start: u64,
    ) -> Result<Vec<u8>, ModelError> {
        let mut key = principal_counter_prefix(principal, domain)?;
        key.extend_from_slice(&window_start.to_be_bytes());
        Ok(key)
    }
    /// Parse an exact counter window suffix belonging to the supplied prefix.
    pub fn principal_counter_window(key: &[u8], prefix: &[u8]) -> Option<u64> {
        Some(u64::from_be_bytes(
            key.strip_prefix(prefix)?.try_into().ok()?,
        ))
    }
    /// Principal and UTF-8 idempotency key.
    pub fn idempotency(principal: PrincipalId, key: &str) -> Vec<u8> {
        [principal.0.as_slice(), key.as_bytes()].concat()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::DomainName;

    fn verifier() -> Verifier {
        Verifier(ScramVerifier::from_password(
            "test-only-password",
            b"test-only-salt",
            1,
        ))
    }
    fn route() -> RouteId {
        RouteId::new("postgres").unwrap()
    }
    fn principal_credential() -> PrincipalCredential {
        PrincipalCredential {
            id: CredentialId([1; 16]),
            principal: PrincipalId([2; 16]),
            verifier: verifier(),
            state: CredentialState::Active,
            expires_ms: Some(900),
        }
    }
    fn session_credential() -> SessionCredential {
        SessionCredential {
            id: CredentialId([3; 16]),
            session: SessionId([4; 16]),
            verifier: verifier(),
            secret_hash: [0xab; 32],
            expires_ms: 900,
            state: CredentialState::Revoked,
        }
    }
    fn scope() -> TokenScope {
        TokenScope {
            id: ScopeId([5; 16]),
            salt: [0xcd; 32],
            derivation_version: 2,
            codec_version: 1,
            deployment_id: [6; 16],
            route: route(),
        }
    }
    fn round_trip<T: Serialize + DeserializeOwned>(record: &T) {
        let encoded = encode(record);
        let decoded: T = decode(&encoded).unwrap();
        assert_eq!(encode(&decoded), encoded);
        let mut json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("unknown_field".into(), serde_json::Value::Bool(true));
        assert!(decode::<T>(&serde_json::to_vec(&json).unwrap()).is_err());
    }
    #[test]
    fn id_hex_round_trip_and_strict_rejection() {
        macro_rules! check {
            ($ty:ident) => {{
                let id = $ty([0xab; 16]);
                let hex = "ab".repeat(16);
                assert_eq!(id.to_hex(), hex);
                assert_eq!(format!("{id:?}"), hex);
                assert_eq!($ty::from_hex(&hex), Some(id));
                assert_eq!(decode::<$ty>(&encode(&id)).unwrap(), id);
                let random = $ty::random();
                assert_eq!($ty::from_hex(&random.to_hex()), Some(random));
                for invalid in [
                    hex.to_uppercase(),
                    "a".repeat(31),
                    "a".repeat(33),
                    "g".repeat(32),
                    "?".repeat(16),
                    format!(" {hex}"),
                ] {
                    assert!($ty::from_hex(&invalid).is_none());
                    assert!(decode::<$ty>(&encode(&invalid)).is_err());
                }
            }};
        }
        check!(PrincipalId);
        check!(SessionId);
        check!(ScopeId);
        check!(CredentialId);
    }
    #[test]
    fn route_validation_including_decode() {
        for name in ["postgres", "spanner-native", "a", "9", &"x".repeat(32)] {
            let id = RouteId::new(name).unwrap();
            assert_eq!(id.as_str(), name);
            assert_eq!(decode::<RouteId>(&encode(&id)).unwrap(), id);
        }
        for name in ["", "-x", "A", &"a".repeat(33), "a b", "?"] {
            assert!(RouteId::new(name).is_none());
            assert!(decode::<RouteId>(&encode(&name)).is_err());
        }
    }
    #[test]
    fn every_record_round_trips_and_rejects_unknown_fields() {
        round_trip(&Principal {
            id: PrincipalId([1; 16]),
            name: "test-agent".into(),
            kind: PrincipalKind::Agent,
            state: PrincipalState::Enabled,
            source: PrincipalSource::Api,
            administrator: false,
            revision: 1,
        });
        round_trip(&principal_credential());
        round_trip(&Grant {
            principal: PrincipalId([1; 16]),
            route: route(),
            mode: Mode::ReadOnly,
        });
        round_trip(&AccessSession {
            id: SessionId([2; 16]),
            principal: PrincipalId([1; 16]),
            route: route(),
            mode: Mode::ReadWrite,
            scope: ScopeId([3; 16]),
            created_ms: 100,
            expires_ms: 900,
            idle_timeout_ms: Some(200),
            last_used_ms: 150,
            state: SessionState::Revoked,
            revoke_reason: Some(EndReason::AdminRevoked),
            ended_ms: Some(160),
            label: Some("test".into()),
            isolated: true,
        });
        round_trip(&session_credential());
        round_trip(&scope());
        round_trip(&IdempotencyRecord {
            principal: PrincipalId([1; 16]),
            key: "test-key".into(),
            request_hash: [0xef; 32],
            result: SessionId([2; 16]).to_hex(),
        });
    }
    #[test]
    fn decode_errors_are_data_free_and_verifiers_are_strict() {
        let input = b"highly-sensitive-malformed-input";
        let err = decode::<Principal>(input).unwrap_err();
        assert_eq!(err.to_string(), "stored record is malformed");
        assert_eq!(format!("{err:?}"), "stored record is malformed");
        assert!(!format!("{err:?}").contains(std::str::from_utf8(input).unwrap()));
        assert!(decode::<Verifier>(br#""not-a-verifier""#).is_err());
        let v = verifier();
        let parsed = decode::<Verifier>(&encode(&v)).unwrap();
        assert_eq!(parsed.0.to_pg_string(), v.0.to_pg_string());
        let mut value = serde_json::to_value(scope()).unwrap();
        assert_eq!(value["salt"], "cd".repeat(32));
        value["salt"] = serde_json::Value::String("CD".repeat(32));
        assert!(decode::<TokenScope>(&serde_json::to_vec(&value).unwrap()).is_err());
        value["salt"] = serde_json::Value::String("cd".repeat(31));
        assert!(decode::<TokenScope>(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    #[test]
    fn secret_debug_fields_are_redacted() {
        let debug = [
            format!("{:?}", scope()),
            format!("{:?}", session_credential()),
            format!("{:?}", principal_credential()),
            format!("{:?}", verifier()),
        ];
        for text in debug {
            assert!(text.contains("REDACTED"));
            for secret in [
                "cd".repeat(32),
                "ab".repeat(32),
                verifier().0.to_pg_string(),
            ] {
                assert!(!text.contains(&secret));
            }
        }
    }
    #[test]
    fn session_labels_and_idempotency_inputs_are_redacted() {
        let session = AccessSession {
            id: SessionId([1; 16]),
            principal: PrincipalId([2; 16]),
            route: route(),
            mode: Mode::ReadOnly,
            scope: ScopeId([3; 16]),
            created_ms: 100,
            expires_ms: 900,
            idle_timeout_ms: None,
            last_used_ms: 100,
            state: SessionState::Active,
            revoke_reason: None,
            ended_ms: None,
            label: Some("PRIVATE-LABEL".into()),
            isolated: false,
        };
        let record = IdempotencyRecord {
            principal: session.principal,
            key: "PRIVATE-KEY".into(),
            request_hash: [0xab; 32],
            result: session.id.to_hex(),
        };
        for text in [format!("{session:?}"), format!("{record:?}")] {
            assert!(text.contains("[REDACTED]"));
            for marker in ["PRIVATE-LABEL", "PRIVATE-KEY", "abab", "171, 171"] {
                assert!(!text.contains(marker));
            }
        }
    }
    #[test]
    fn keys_are_fixed_width_and_domain_separated() {
        let p = PrincipalId([1; 16]);
        let s = SessionId([2; 16]);
        let c = CredentialId([3; 16]);
        let d = DomainName("ab".into());
        let prefix = keys::principal_counter_prefix(p, &d).unwrap();
        assert_eq!(prefix, [p.0.as_slice(), &[0, 2], b"ab"].concat());
        let key = keys::principal_counter(p, &d, 0x0102030405060708).unwrap();
        assert_eq!(&key[prefix.len()..], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            keys::principal_counter_window(&key, &prefix),
            Some(0x0102030405060708)
        );
        let other = keys::principal_counter_prefix(p, &DomainName("a".into())).unwrap();
        assert_ne!(
            [prefix.as_slice(), b"c"].concat(),
            [other.as_slice(), b"bc"].concat()
        );
        assert!(!prefix.starts_with(&other));
        assert_eq!(keys::principal_counter_window(&key, &other), None);
        assert_eq!(keys::principal_counter_window(&prefix, &prefix), None);
        assert_eq!(
            keys::principal_counter_window(&[key.as_slice(), &[0]].concat(), &prefix),
            None
        );
        let oversized = DomainName("x".repeat(usize::from(u16::MAX) + 1));
        assert!(keys::session_counter(s, &oversized).is_err());
        assert!(keys::principal_counter_prefix(p, &oversized).is_err());
        assert!(keys::principal_counter(p, &oversized, 0).is_err());
        assert_eq!(keys::principal(p), p.0);
        assert_eq!(keys::session(s), s.0);
        assert_eq!(keys::scope(ScopeId([4; 16])), [4; 16]);
        assert_eq!(keys::principal_credential(p, c), [p.0, c.0].concat());
        assert_eq!(keys::session_credential(s, c), [s.0, c.0].concat());
        assert_eq!(
            keys::grant(p, &route()),
            [p.0.as_slice(), b"postgres"].concat()
        );
        assert_eq!(
            keys::idempotency(p, "key"),
            [p.0.as_slice(), b"key"].concat()
        );
        assert_eq!(
            keys::session_counter(s, &d).unwrap(),
            [s.0.as_slice(), &[0, 2], b"ab"].concat()
        );
    }
    #[test]
    fn counter_values_require_exactly_eight_big_endian_bytes() {
        for value in [0, 1, 256, u64::MAX] {
            assert_eq!(decode_count(&encode_count(value)).unwrap(), value);
            assert_eq!(encode_count(value), value.to_be_bytes());
        }
        for bytes in [&[][..], &[0; 7], &[0; 9]] {
            assert!(decode_count(bytes).is_err());
        }
    }
    #[test]
    fn modes_and_enums_have_stable_encoding() {
        for a in [Mode::ReadOnly, Mode::ReadWrite] {
            for b in [Mode::ReadOnly, Mode::ReadWrite] {
                assert_eq!(
                    a.min(b).permits_write(),
                    a.permits_write() && b.permits_write()
                );
            }
        }
        assert_eq!(encode(&Mode::ReadOnly), br#""read_only""#);
        assert_eq!(encode(&Mode::ReadWrite), br#""read_write""#);
        for reason in [
            EndReason::Requested,
            EndReason::AdminRevoked,
            EndReason::PrincipalDisabled,
            EndReason::ConfigRemoved,
            EndReason::Expired,
            EndReason::IdleTimeout,
            EndReason::GrantRemoved,
        ] {
            assert_eq!(encode(&reason), encode(&reason.as_str()));
        }
        assert!(decode::<SessionState>(br#""unknown""#).is_err());
        assert!(decode::<PrincipalKind>(br#""unknown""#).is_err());
    }
}
