//! Token codec contracts and secret scope keys.
use crate::{
    error::Refusal,
    policy::{DomainName, TypeRange},
    value::{ColumnType, SqlValue},
};
/// Detection of token-shaped values.
pub mod detect;
/// Format-preserving token support.
pub mod fpe;
/// Scope key derivation.
pub mod keys;
/// Text token support.
pub mod text;
/// Non-secret task identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(
    /// Non-secret identifier bytes.
    pub [u8; 16],
);
impl std::fmt::Debug for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}
// Shared implementation also used by durable credential identifiers.
macro_rules! id_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(
            /// Non-secret identifier bytes.
            pub [u8; 16],
        );
        impl $name {
            /// Generate an identifier; operating-system entropy failure is fatal.
            pub fn random() -> Self {
                let mut bytes = [0; 16];
                getrandom::fill(&mut bytes).expect("identifier entropy unavailable");
                Self(bytes)
            }
            /// Return exactly 32 lowercase hexadecimal characters.
            pub fn to_hex(&self) -> String {
                data_encoding::HEXLOWER.encode(&self.0)
            }
            /// Parse exactly 32 lowercase hexadecimal characters.
            pub fn from_hex(s: &str) -> Option<Self> {
                if s.len() != 32
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return None;
                }
                Some(Self(
                    data_encoding::HEXLOWER
                        .decode(s.as_bytes())
                        .ok()?
                        .try_into()
                        .ok()?,
                ))
            }
        }
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.to_hex())
            }
        }
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_hex())
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::from_hex(&s).ok_or_else(|| serde::de::Error::custom("invalid identifier"))
            }
        }
    };
}
pub(crate) use id_type;
id_type!(PrincipalId, "Non-secret principal identifier.");
id_type!(SessionId, "Non-secret access session identifier.");
id_type!(ScopeId, "Non-secret token scope identifier.");
/// Scope secret, erased on drop and never cloned or exposed by Debug.
#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct ScopeKeys {
    /// Derived scope secret available only to gateway internals.
    pub(crate) secret: [u8; 32],
}
impl ScopeKeys {
    /// Own a derived scope secret, which is erased when this owner is dropped.
    pub fn new(secret: [u8; 32]) -> Self {
        Self { secret }
    }
}
impl std::fmt::Debug for ScopeKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScopeKeys(..)")
    }
}
/// Typed token conversion; implementations must return Null unchanged in both directions.
pub trait TokenCodec {
    /// Encode a value for a domain; Null passes through unchanged.
    fn encode(
        &self,
        keys: &ScopeKeys,
        domain: &DomainName,
        ty: &ColumnType,
        range: Option<&TypeRange>,
        v: &SqlValue,
    ) -> Result<SqlValue, Refusal>;
    /// Decode a value for a domain; Null passes through unchanged.
    fn decode(
        &self,
        keys: &ScopeKeys,
        domain: &DomainName,
        ty: &ColumnType,
        range: Option<&TypeRange>,
        v: &SqlValue,
    ) -> Result<SqlValue, Refusal>;
}
/// Standard deterministic text and format-preserving token codec.
pub struct StandardCodec;
impl StandardCodec {
    fn convert(
        &self,
        keys: &ScopeKeys,
        domain: &DomainName,
        ty: &ColumnType,
        range: Option<&TypeRange>,
        v: &SqlValue,
        decode: bool,
    ) -> Result<SqlValue, Refusal> {
        use crate::error::NyCode;
        if matches!(v, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        let matched = match ty {
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Bpchar(_) => {
                matches!(v, SqlValue::Text(_))
            }
            ColumnType::Int4 => matches!(v, SqlValue::Int4(_)),
            ColumnType::Int8 => matches!(v, SqlValue::Int8(_)),
            ColumnType::Date => matches!(v, SqlValue::Date(_)),
            ColumnType::Timestamp => matches!(v, SqlValue::Timestamp(_)),
            ColumnType::Timestamptz => matches!(v, SqlValue::Timestamptz(_)),
            ColumnType::Uuid => matches!(v, SqlValue::Uuid(_)),
            ColumnType::Int2 => matches!(v, SqlValue::Int2(_)),
            ColumnType::Bool => matches!(v, SqlValue::Bool(_)),
            ColumnType::Numeric
            | ColumnType::Bytea
            | ColumnType::Json
            | ColumnType::Jsonb
            | ColumnType::Array(_)
            | ColumnType::Enum(_)
            | ColumnType::Other(_) => {
                return Err(Refusal::new(
                    NyCode::BlindOperation,
                    "unsupported token type",
                ));
            }
        };
        if !matched {
            return Err(Refusal::new(NyCode::NotAnalysable, "token type mismatch"));
        }
        if ty.is_textual() {
            text::convert(keys, domain, v, decode)
        } else {
            fpe::convert(keys, domain, ty, range, v, decode)
        }
    }
}

impl TokenCodec for StandardCodec {
    fn encode(
        &self,
        keys: &ScopeKeys,
        domain: &DomainName,
        ty: &ColumnType,
        range: Option<&TypeRange>,
        v: &SqlValue,
    ) -> Result<SqlValue, Refusal> {
        self.convert(keys, domain, ty, range, v, false)
    }
    fn decode(
        &self,
        keys: &ScopeKeys,
        domain: &DomainName,
        ty: &ColumnType,
        range: Option<&TypeRange>,
        v: &SqlValue,
    ) -> Result<SqlValue, Refusal> {
        self.convert(keys, domain, ty, range, v, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::NyCode;
    #[test]
    fn dispatch_refusals_and_nulls() {
        let keys = ScopeKeys::new([0; 32]);
        let domain = DomainName("d".into());
        for decode in [false, true] {
            assert_eq!(
                StandardCodec
                    .convert(
                        &keys,
                        &domain,
                        &ColumnType::Int4,
                        None,
                        &SqlValue::Text("x".into()),
                        decode
                    )
                    .unwrap_err()
                    .code,
                NyCode::NotAnalysable
            );
            assert_eq!(
                StandardCodec
                    .convert(
                        &keys,
                        &domain,
                        &ColumnType::Int2,
                        None,
                        &SqlValue::Int2(0),
                        decode
                    )
                    .unwrap_err()
                    .code,
                NyCode::BlindOperation
            );
            assert_eq!(
                StandardCodec
                    .convert(
                        &keys,
                        &domain,
                        &ColumnType::Other("x".into()),
                        None,
                        &SqlValue::Null,
                        decode
                    )
                    .unwrap(),
                SqlValue::Null
            );
        }
    }
}
