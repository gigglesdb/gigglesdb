//! Root keys and domain-separated scope derivation.
use super::ScopeKeys;
use crate::{
    error::{NyCode, Refusal},
    policy::DomainName,
};
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

/// Root secret, erased on drop and redacted in diagnostics.
#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct RootKey([u8; 32]);
impl std::fmt::Debug for RootKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RootKey(..)")
    }
}
fn invalid() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "invalid root key")
}
impl RootKey {
    /// Parse exactly 32 hexadecimal secret bytes.
    pub fn from_hex(input: &str) -> Result<Self, Refusal> {
        let bytes = input.trim().as_bytes();
        if bytes.len() != 64 {
            return Err(invalid());
        }
        let mut key = Zeroizing::new([0; 32]);
        for (out, pair) in key.iter_mut().zip(bytes.as_chunks::<2>().0) {
            let digit = |b: u8| match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                b'A'..=b'F' => Some(b - b'A' + 10),
                _ => None,
            };
            *out = digit(pair[0]).ok_or_else(invalid)? * 16 + digit(pair[1]).ok_or_else(invalid)?;
        }
        Ok(Self(*key))
    }
    /// Load a raw or hexadecimal key file without exposing its contents.
    pub fn load_file(path: impl AsRef<std::path::Path>) -> Result<Self, Refusal> {
        let data = Zeroizing::new(std::fs::read(path).map_err(|_| invalid())?);
        if data.len() == 32 {
            let mut key = [0; 32];
            key.copy_from_slice(&data);
            return Ok(Self(key));
        }
        Self::from_hex(std::str::from_utf8(&data).map_err(|_| invalid())?)
    }
    /// Read a hexadecimal root key from an environment variable.
    pub fn from_env(var: &str) -> Result<Self, Refusal> {
        let value = Zeroizing::new(std::env::var(var).map_err(|_| invalid())?);
        Self::from_hex(&value)
    }
}
/// Derive a scope secret from its public random salt.
pub fn derive_scope(root: &RootKey, scope_salt: &[u8; 32]) -> ScopeKeys {
    let mut secret = [0; 32];
    Hkdf::<Sha256>::new(Some(scope_salt), &root.0)
        .expand(b"nunya/v1/scope", &mut secret)
        .expect("fixed HKDF output length");
    ScopeKeys::new(secret)
}
pub(crate) fn subkey(keys: &ScopeKeys, domain: &DomainName, purpose: &str) -> Zeroizing<[u8; 32]> {
    let mut output = Zeroizing::new([0; 32]);
    let info = format!("nunya/v1/{}/{purpose}", domain.0);
    Hkdf::<Sha256>::new(None, &keys.secret)
        .expand(info.as_bytes(), &mut *output)
        .expect("fixed HKDF output length");
    output
}
/// Generate an operating-system random scope salt; entropy failure is fatal.
pub fn random_salt() -> [u8; 32] {
    let mut salt = [0; 32];
    getrandom::fill(&mut salt).expect("scope entropy unavailable");
    salt
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn separation_and_redaction() {
        let root = RootKey::from_hex(&"00".repeat(32)).unwrap();
        let a = derive_scope(&root, &[0; 32]);
        let b = derive_scope(&root, &[1; 32]);
        assert!(
            a.secret
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
                == "50aac2c0c305bcfa9c0308d2b8148599073357c2b9b6a29c0f7bd2880cbf765f"
        );
        assert!(a.secret != b.secret);
        assert!(
            *subkey(&a, &DomainName("a".into()), "text")
                != *subkey(&a, &DomainName("b".into()), "text")
        );
        assert_eq!(format!("{root:?}"), "RootKey(..)");
        assert!(RootKey::from_hex("invalid").is_err());
    }
}
