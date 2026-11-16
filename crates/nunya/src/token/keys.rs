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
/// Derive the domain-separated root-key continuity value.
pub(crate) fn root_check(root: &RootKey) -> [u8; 32] {
    let mut output = [0; 32];
    Hkdf::<Sha256>::new(None, &root.0)
        .expand(b"nunya/v1/root-check", &mut output)
        .expect("fixed HKDF output length");
    output
}

/// Derivation version stored with each scope row.
pub const SCOPE_DERIVATION_V2: u16 = 2;

/// Derive a scope secret bound to its deployment and route (derivation v2).
pub fn derive_scope_v2(
    root: &RootKey,
    scope_salt: &[u8; 32],
    deployment_id: &[u8; 16],
    route_id: &str,
) -> Result<ScopeKeys, Refusal> {
    let route_len = u16::try_from(route_id.len())
        .ok()
        .filter(|len| *len != 0)
        .ok_or_else(|| Refusal::new(NyCode::NotAnalysable, "invalid scope route"))?;
    let mut info = Vec::with_capacity(b"nunya/v2/scope".len() + 2 + 16 + 2 + route_id.len());
    info.extend_from_slice(b"nunya/v2/scope");
    info.extend_from_slice(&16u16.to_be_bytes());
    info.extend_from_slice(deployment_id);
    info.extend_from_slice(&route_len.to_be_bytes());
    info.extend_from_slice(route_id.as_bytes());
    let mut secret = [0; 32];
    Hkdf::<Sha256>::new(Some(scope_salt), &root.0)
        .expand(&info, &mut secret)
        .expect("fixed HKDF output length");
    Ok(ScopeKeys::new(secret))
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
    fn root_check_fixed_vector() {
        // python -I -c "import hmac,hashlib; ikm=bytes([1])*32; prk=hmac.new(bytes(32),ikm,hashlib.sha256).digest(); okm=hmac.new(prk,b'nunya/v1/root-check'+bytes([1]),hashlib.sha256).digest(); print(okm.hex()); print(list(okm))"
        assert_eq!(
            root_check(&RootKey([1; 32])),
            [
                39, 208, 210, 217, 40, 85, 33, 153, 93, 54, 55, 169, 34, 126, 132, 163, 8, 27, 155,
                207, 81, 109, 26, 2, 221, 198, 219, 69, 194, 0, 24, 18,
            ]
        );
    }

    #[test]
    fn scope_v2_vectors() {
        let vectors = [
            (
                [0; 32],
                [0; 32],
                [0; 16],
                "postgres",
                "f13c7d291fa2d64e4b7362c0726ec37ebd835a050215076a4221775645ef064c",
            ),
            (
                [0; 32],
                [0; 32],
                [0; 16],
                "spanner-native",
                "b75e87876baea4e2d94246191b3ed32f3167fae49bea0914949fcdb10628540e",
            ),
            (
                [0; 32],
                [0; 32],
                [1; 16],
                "postgres",
                "644bc8f1fc4f262330101cde4b3974df5952aed1c539ad2da6396ac03f9a60a2",
            ),
            (
                std::array::from_fn(|i| i as u8),
                [0xab; 32],
                std::array::from_fn(|i| 0x10 + i as u8),
                "postgres",
                "bd07af90f6bded2b43923410a866e110f88ff5d6862472b5ea28079e6c5488fb",
            ),
        ];
        for (root, salt, deployment, route, expected) in vectors {
            let keys = derive_scope_v2(&RootKey(root), &salt, &deployment, route).unwrap();
            let actual = keys
                .secret
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn scope_v2_separation() {
        let root = RootKey([0; 32]);
        let a = derive_scope_v2(&root, &[0; 32], &[0; 16], "postgres").unwrap();
        let other_route = derive_scope_v2(&root, &[0; 32], &[0; 16], "spanner-native").unwrap();
        let other_deployment = derive_scope_v2(&root, &[0; 32], &[1; 16], "postgres").unwrap();
        assert!(a.secret != other_route.secret);
        assert!(a.secret != other_deployment.secret);
        for keys in [&a, &other_route, &other_deployment] {
            assert_eq!(format!("{keys:?}"), "ScopeKeys(..)");
        }
        assert_eq!(SCOPE_DERIVATION_V2, 2);
    }

    #[test]
    fn scope_v2_refuses_bad_route() {
        let root = RootKey([0; 32]);
        for route in [String::new(), "x".repeat(65536)] {
            let error = derive_scope_v2(&root, &[0; 32], &[0; 16], &route).unwrap_err();
            assert_eq!(error.code, NyCode::NotAnalysable);
            assert_eq!(error.reason, "invalid scope route");
            if !route.is_empty() {
                assert!(!format!("{error:?} {error}").contains(&route));
            }
        }
    }
}
