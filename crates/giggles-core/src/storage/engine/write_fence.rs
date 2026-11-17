//! GD-0077 v1 persisted write fence. Holder tokens are never persisted.
use super::meta::FormatError;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FenceState {
    Fenced,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WriteFenceRecord {
    pub(crate) state: FenceState,
    pub(crate) epoch: u64,
    pub(crate) holder_digest: [u8; 32],
    pub(crate) changed_at: u64,
    pub(crate) label: String,
}

pub(crate) fn holder_digest(token: &[u8]) -> [u8; 32] {
    Sha256::digest(token).into()
}

impl WriteFenceRecord {
    /// The API must validate the byte length before constructing a record.
    pub(crate) fn encode(&self) -> Vec<u8> {
        assert!(
            self.label.len() <= 256,
            "write fence label exceeds 256 bytes"
        );
        let mut bytes = Vec::with_capacity(52 + self.label.len());
        bytes.push(1);
        bytes.push(match self.state {
            FenceState::Fenced => 1,
            FenceState::Released => 2,
        });
        bytes.extend_from_slice(&self.epoch.to_be_bytes());
        bytes.extend_from_slice(&self.holder_digest);
        bytes.extend_from_slice(&self.changed_at.to_be_bytes());
        bytes.extend_from_slice(&(self.label.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.label.as_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let corrupt = || FormatError::Corrupt("invalid write_fence record".into());
        if bytes.len() < 52 || bytes[0] != 1 {
            return Err(corrupt());
        }
        let state = match bytes[1] {
            1 => FenceState::Fenced,
            2 => FenceState::Released,
            _ => return Err(corrupt()),
        };
        let len = u16::from_be_bytes(bytes[50..52].try_into().unwrap()) as usize;
        if len > 256 || bytes.len() != 52 + len {
            return Err(corrupt());
        }
        let label = std::str::from_utf8(&bytes[52..])
            .map_err(|_| corrupt())?
            .to_owned();
        Ok(Self {
            state,
            epoch: u64::from_be_bytes(bytes[2..10].try_into().unwrap()),
            holder_digest: bytes[10..42].try_into().unwrap(),
            changed_at: u64::from_be_bytes(bytes[42..50].try_into().unwrap()),
            label,
        })
    }
}

#[cfg(test)]
#[path = "write_fence_tests.rs"]
mod tests;
