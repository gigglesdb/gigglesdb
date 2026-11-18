//! Durable state table layout and root-key continuity check.
use crate::token::keys::{self, RootKey};

/// Supported on-disk metadata format.
pub const FORMAT_VERSION: u32 = 1;
/// Store filename within the service state directory.
pub const STORE_FILE: &str = "nunya.redb";

/// Opaque record tables available to state transactions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Table {
    /// Principal records.
    Principals,
    /// Principal credential records.
    PrincipalCredentials,
    /// Authorization grants.
    Grants,
    /// Session records.
    Sessions,
    /// Session credential records.
    SessionCredentials,
    /// Token scope records.
    Scopes,
    /// Session usage counters.
    SessionCounters,
    /// Principal usage counters.
    PrincipalCounters,
    /// Idempotent operation records.
    Idempotency,
    /// Persisted column domain bindings.
    DomainBindings,
    /// Permanently reserved private domain names.
    DomainNames,
    /// Permanently retired generated domains.
    RetiredDomains,
}
impl Table {
    /// Every application table in schema order.
    pub const ALL: [Table; 12] = [
        Self::Principals,
        Self::PrincipalCredentials,
        Self::Grants,
        Self::Sessions,
        Self::SessionCredentials,
        Self::Scopes,
        Self::SessionCounters,
        Self::PrincipalCounters,
        Self::Idempotency,
        Self::DomainBindings,
        Self::DomainNames,
        Self::RetiredDomains,
    ];

    /// Stable on-disk table name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Principals => "principals",
            Self::PrincipalCredentials => "principal_credentials",
            Self::Grants => "grants",
            Self::Sessions => "sessions",
            Self::SessionCredentials => "session_credentials",
            Self::Scopes => "scopes",
            Self::SessionCounters => "session_counters",
            Self::PrincipalCounters => "principal_counters",
            Self::Idempotency => "idempotency",
            Self::DomainBindings => "domain_bindings",
            Self::DomainNames => "domain_names",
            Self::RetiredDomains => "retired_domains",
        }
    }

    pub(crate) fn definition(self) -> redb::TableDefinition<'static, &'static [u8], &'static [u8]> {
        redb::TableDefinition::new(self.name())
    }
}

/// Derive the domain-separated root-key continuity value for persistent state.
pub fn root_check_value(root: &RootKey) -> [u8; 32] {
    keys::root_check(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn root_check_fixed_vector() {
        let root = RootKey::from_hex(&"01".repeat(32)).unwrap();
        // python -I -c "import hmac,hashlib; ikm=bytes([1])*32; prk=hmac.new(bytes(32),ikm,hashlib.sha256).digest(); okm=hmac.new(prk,b'nunya/v1/root-check'+bytes([1]),hashlib.sha256).digest(); print(okm.hex()); print(list(okm))"
        assert_eq!(
            root_check_value(&root),
            [
                39, 208, 210, 217, 40, 85, 33, 153, 93, 54, 55, 169, 34, 126, 132, 163, 8, 27, 155,
                207, 81, 109, 26, 2, 221, 198, 219, 69, 194, 0, 24, 18,
            ]
        );
    }
}
