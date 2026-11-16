//! Durable state table layout and root-key continuity check.
use crate::token::keys::{RootKey, derive_scope};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// Supported on-disk metadata format.
pub const FORMAT_VERSION: u32 = 1;
/// Store filename within the service state directory.
pub const STORE_FILE: &str = "nunya.redb";

const ROOT_CHECK_SALT: [u8; 32] = [
    176, 141, 55, 252, 226, 223, 254, 235, 49, 213, 109, 38, 82, 125, 39, 249, 40, 125, 39, 67,
    237, 161, 36, 19, 21, 78, 158, 3, 45, 143, 31, 215,
];

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
}
impl Table {
    /// Every application table in schema order.
    pub const ALL: [Table; 9] = [
        Self::Principals,
        Self::PrincipalCredentials,
        Self::Grants,
        Self::Sessions,
        Self::SessionCredentials,
        Self::Scopes,
        Self::SessionCounters,
        Self::PrincipalCounters,
        Self::Idempotency,
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
        }
    }

    pub(crate) fn definition(self) -> redb::TableDefinition<'static, &'static [u8], &'static [u8]> {
        redb::TableDefinition::new(self.name())
    }
}

/// Derive the domain-separated root-key continuity value for persistent state.
pub fn root_check_value(root: &RootKey) -> [u8; 32] {
    let k = derive_scope(root, &ROOT_CHECK_SALT);
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&k.secret).expect("HMAC accepts a fixed 32-byte key");
    mac.update(b"nunya/v2/store-root-check");
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn root_check_fixed_vector() {
        assert_eq!(
            ROOT_CHECK_SALT,
            <[u8; 32]>::from(Sha256::digest(b"nunya/v2/store-root-check-salt"))
        );
        let root = RootKey::from_hex(&"01".repeat(32)).unwrap();
        assert_eq!(
            root_check_value(&root),
            [
                88, 167, 155, 192, 151, 194, 129, 168, 3, 14, 17, 38, 84, 128, 1, 219, 154, 133,
                108, 177, 23, 221, 238, 90, 244, 195, 212, 101, 62, 9, 3, 32,
            ]
        );
    }
}
