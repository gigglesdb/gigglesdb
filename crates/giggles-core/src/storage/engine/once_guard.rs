//! Caller-captured decision state transported to the commit worker.
use super::hlc::Ts;
use std::{sync::Arc, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GuardRejection { SnapshotMoved, LeaseExpired, Cancelled }
pub(crate) type GuardHook = Arc<dyn Fn(crate::DecisionPoint) + Send + Sync>;
pub(crate) struct CommitGuard {
    fence: Option<Ts>,
    lease_deadline: Instant,
    cancellation: Option<crate::Cancellation>,
    hook: Option<GuardHook>,
}
impl CommitGuard {
    /// Must run on the caller, never the worker: captures the caller's TLS scope.
    pub(crate) fn capture(fence: Ts, lease_deadline: Instant) -> Self {
        Self { fence: Some(fence), lease_deadline, cancellation: crate::Cancellation::current(), hook: None }
    }
    pub(crate) fn capture_unfenced(lease_deadline: Instant) -> Self {
        Self { fence: None, lease_deadline, cancellation: crate::Cancellation::current(), hook: None }
    }
    pub(crate) fn is_fenced(&self) -> bool { self.fence.is_some() }
    pub(crate) fn with_hook(mut self, hook: GuardHook) -> Self { self.hook = Some(hook); self }
    pub(crate) fn fence(&self) -> Ts { self.fence.expect("fenced guard") }
    pub(crate) fn lease_deadline(&self) -> Instant { self.lease_deadline }
    pub(crate) fn check_fence(&self, visible: Ts) -> Result<(), GuardRejection> {
        if self.fence.is_none_or(|fence| fence == visible) { Ok(()) } else { Err(GuardRejection::SnapshotMoved) }
    }
    pub(crate) fn decide(&self) -> Result<(), GuardRejection> { self.decide_with(&mut Instant::now) }
    pub(crate) fn decide_with(&self, now: &mut dyn FnMut() -> Instant) -> Result<(), GuardRejection> {
        if let Some(hook) = &self.hook { hook(crate::DecisionPoint::BeforeDecision); }
        if now() >= self.lease_deadline { return Err(GuardRejection::LeaseExpired); }
        if let Some(c) = &self.cancellation {
            c.scope(crate::cancellation::decide).map_err(|_| GuardRejection::Cancelled)?;
        }
        if now() >= self.lease_deadline { return Err(GuardRejection::LeaseExpired); }
        if let Some(hook) = &self.hook { hook(crate::DecisionPoint::Decided); }
        Ok(())
    }
}
#[cfg(test)]
#[path = "once_guard_tests.rs"]
mod tests;
