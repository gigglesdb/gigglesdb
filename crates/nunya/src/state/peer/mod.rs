//! Leader-only authenticated state API; listener startup is explicitly owned by the caller.
mod auth;
mod client;
mod cache;
pub use cache::PeerStateCache;
mod proto;
mod server;
use crate::{state::model::EndReason, token::SessionId};
pub use auth::PeerAuth;
pub use client::PeerClient;
pub use proto::*;
pub use server::PeerServer;
use std::collections::VecDeque;
/// Data-free peer failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PeerError {
    /// Leadership moved.
    #[error("not leader")]
    NotLeader,
    /// Authentication refused.
    #[error("unauthorized")]
    Unauthorized,
    /// Older generation refused.
    #[error("stale leader")]
    Stale,
    /// Call deadline elapsed.
    #[error("peer timeout")]
    Timeout,
    /// Transport unavailable.
    #[error("peer transport unavailable")]
    Transport,
    /// Invalid wire data or unavailable state.
    #[error("peer protocol failure")]
    Protocol,
}
/// Bounded authority-owned invalidation history.
pub(crate) struct ChangeLog {
    epoch: u64,
    seq: u32,
    generation: Option<u64>,
    rows: VecDeque<(u32, SessionId, ChangeKind)>,
}
impl Default for ChangeLog {
    fn default() -> Self {
        let mut b = [0; 4];
        let epoch = if getrandom::fill(&mut b).is_ok() {
            u64::from(u32::from_be_bytes(b).max(1)) << 32
        } else {
            1 << 32
        };
        Self {
            epoch,
            seq: 0,
            generation: None,
            rows: VecDeque::new(),
        }
    }
}
impl ChangeLog {
    pub(crate) fn invalidate(&mut self) {
        // A distinct cursor epoch forces a refetch even with the same Lease term.
        self.epoch = self.epoch.wrapping_add(1u64 << 32);
        self.seq = 0;
        self.rows.clear();
    }

    pub(crate) fn push(&mut self, id: SessionId, reason: EndReason) {
        self.push_kind(id, ChangeKind::End { reason });
    }
    pub(crate) fn refresh(&mut self, id: SessionId) {
        self.push_kind(id, ChangeKind::Refresh {});
    }
    fn push_kind(&mut self, id: SessionId, kind: ChangeKind) {
        if self.seq == u32::MAX {
            *self = Self::default();
        }
        self.seq += 1;
        self.rows.push_back((self.seq, id, kind));
        if self.rows.len() > 4096 {
            self.rows.pop_front();
        }
    }
    pub(crate) fn read(&mut self, since: u64, generation: u64) -> Changes {
        if self.generation.is_some_and(|g| g != generation) {
            *self = Self::default();
        }
        self.generation = Some(generation);
        let seq = since as u32;
        let reset = since == 0
            || since & 0xffff_ffff_0000_0000 != self.epoch
            || seq > self.seq
            || self
                .rows
                .front()
                .is_some_and(|r| seq < r.0.saturating_sub(1));
        Changes {
            reset,
            changes: if reset {
                vec![]
            } else {
                self.rows
                    .iter()
                    .filter(|r| r.0 > seq)
                    .map(|r| Change {
                        session_id: r.1,
                        kind: r.2.clone(),
                    })
                    .collect()
            },
            next: self.epoch | u64::from(self.seq),
            generation,
        }
    }
}
#[cfg(test)]
mod tests;
