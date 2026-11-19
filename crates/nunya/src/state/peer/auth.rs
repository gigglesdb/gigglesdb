//! Request authentication, replay fencing and response sealing.
use super::PeerError;
use aes_siv::siv::Aes256Siv;
use hkdf::Hkdf;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::{collections::{HashMap, VecDeque}, path::Path, sync::Mutex};
use zeroize::Zeroizing;
const WINDOW: u64 = 30_000;
#[derive(Default)]
struct Replay { entries: HashMap<String, u64>, order: VecDeque<(String, u64)> }
/// Derived peer keys with redacted diagnostics.
pub struct PeerAuth {
    mac: Zeroizing<[u8; 32]>,
    seal: Zeroizing<[u8; 64]>,
    seen: Mutex<Replay>,
}
impl std::fmt::Debug for PeerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerAuth([REDACTED])")
    }
}
impl PeerAuth {
    /// Load a mounted credential containing at least 32 bytes.
    pub fn from_file(path: &Path) -> Result<Self, PeerError> {
        let secret = Zeroizing::new(std::fs::read(path).map_err(|_| PeerError::Unauthorized)?);
        Self::new(&secret)
    }
    /// Derive independent authentication and sealing keys.
    pub fn new(secret: &[u8]) -> Result<Self, PeerError> {
        if secret.len() < 32 {
            return Err(PeerError::Unauthorized);
        }
        let hk = Hkdf::<Sha256>::new(None, secret);
        let mut mac = Zeroizing::new([0; 32]);
        let mut seal = Zeroizing::new([0; 64]);
        hk.expand(b"nunya-peer-mac-v1", &mut *mac)
            .map_err(|_| PeerError::Protocol)?;
        hk.expand(b"nunya-peer-seal-v1", &mut *seal)
            .map_err(|_| PeerError::Protocol)?;
        Ok(Self {
            mac,
            seal,
            seen: Mutex::default(),
        })
    }
    fn signature(
        &self,
        method: &str,
        path: &str,
        ts: &str,
        nonce: &str,
        pod: &str,
        body: &[u8],
    ) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&*self.mac)
            .unwrap_or_else(|_| unreachable!());
        mac.update(
            format!(
                "v1\n{method}\n{path}\n{ts}\n{nonce}\n{pod}\n{}",
                data_encoding::HEXLOWER.encode(&Sha256::digest(body))
            )
            .as_bytes(),
        );
        mac
    }
    pub(super) fn sign(&self, path: &str, ts: &str, nonce: &str, pod: &str, body: &[u8]) -> String {
        data_encoding::HEXLOWER.encode(
            &self
                .signature("POST", path, ts, nonce, pod, body)
                .finalize()
                .into_bytes(),
        )
    }
    pub(super) fn verify(
        &self,
        method: &str,
        path: &str,
        headers: &http::HeaderMap,
        body: &[u8],
        now: u64,
    ) -> Result<String, PeerError> {
        let get = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .ok_or(PeerError::Unauthorized)
        };
        let ts = get("x-nunya-peer-ts")?;
        let nonce = get("x-nunya-peer-nonce")?;
        let pod = get("x-nunya-peer-pod")?;
        if now.abs_diff(ts.parse::<u64>().map_err(|_| PeerError::Unauthorized)?) > WINDOW
            || nonce.len() != 32
            || !nonce
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || pod.is_empty()
            || pod.len() > 253
        {
            return Err(PeerError::Unauthorized);
        }
        let tag = data_encoding::HEXLOWER
            .decode(get("x-nunya-peer-mac")?.as_bytes())
            .map_err(|_| PeerError::Unauthorized)?;
        self.signature(method, path, ts, nonce, pod, body)
            .verify_slice(&tag)
            .map_err(|_| PeerError::Unauthorized)?;
        let mut seen = self.seen.lock().map_err(|_| PeerError::Unauthorized)?;
        while seen.order.front().is_some_and(|(_, at)| now.saturating_sub(*at) > 2 * WINDOW) {
            if let Some((nonce, _)) = seen.order.pop_front() { seen.entries.remove(&nonce); }
        }
        // Refuse overload instead of evicting a still-valid replay fence.
        if seen.entries.len() >= 65_536 || seen.entries.contains_key(nonce) {
            return Err(PeerError::Unauthorized);
        }
        seen.entries.insert(nonce.into(), now);
        seen.order.push_back((nonce.into(), now));
        Ok(nonce.into())
    }
    pub(super) fn seal(
        &self,
        body: &[u8],
        nonce: &str,
        status: u16,
        generation: u64,
    ) -> Result<Vec<u8>, PeerError> {
        let ad = format!("{nonce}{status}{generation}");
        Aes256Siv::new((&*self.seal).into())
            .encrypt([ad.as_bytes()], body)
            .map_err(|_| PeerError::Protocol)
    }
    pub(super) fn open(
        &self,
        body: &[u8],
        nonce: &str,
        status: u16,
        generation: u64,
    ) -> Result<Vec<u8>, PeerError> {
        let ad = format!("{nonce}{status}{generation}");
        Aes256Siv::new((&*self.seal).into())
            .decrypt([ad.as_bytes()], body)
            .map_err(|_| PeerError::Protocol)
    }
}
pub(super) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}
pub(super) fn nonce() -> Result<String, PeerError> {
    let mut b = [0; 16];
    getrandom::fill(&mut b).map_err(|_| PeerError::Protocol)?;
    Ok(data_encoding::HEXLOWER.encode(&b))
}
