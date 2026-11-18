//! Agent statement ceiling and agent admission (GD-0061 R3-403, design-3
//! section 7, "All-agents ceiling").
//!
//! `agent.max_concurrent_statements` bounds the statements all agent sessions
//! run at once. It is set in two places with the same value: core's
//! all-agent slot count (`Admin::set_agent_statement_ceiling`) and the
//! [`AgentAdmission`] slots every agent request takes before it waits for a
//! blocking worker (`Blocking::admit`). A full admission refuses the request
//! after waiting up to the refusal delay for a freed slot (54T01). Agent
//! requests therefore never hold more than the ceiling of the blocking pool,
//! and applications always keep at least half of it and half of the engine's
//! admitted operations.
//!
//! `agent.disclosure_chunk_rows` and `agent.disclosure_chunk_bytes` are the
//! reservation chunk each new task session gets (`SqlSession::set_disclosure_chunk`);
//! zero means exact debits.

use crate::blocking::MAX_BLOCKING;
use giggles_core::{
    CoreError,
    task::{Budget, TaskError},
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// Default `agent.max_concurrent_statements`.
pub const DEFAULT_MAX_CONCURRENT_STATEMENTS: usize = 8;
/// Default `agent.disclosure_chunk_rows`.
pub const DEFAULT_DISCLOSURE_CHUNK_ROWS: u64 = 256;
/// Default `agent.disclosure_chunk_bytes`.
pub const DEFAULT_DISCLOSURE_CHUNK_BYTES: u64 = 262_144;
/// Default ceiling refusal wait in milliseconds.
pub const DEFAULT_CEILING_REFUSAL_DELAY_MS: u64 = 25;
/// Maximum ceiling refusal wait in milliseconds.
pub const MAX_CEILING_REFUSAL_DELAY_MS: u64 = 1000;
/// The largest ceiling the blocking pool allows: half of it.
pub const MAX_AGENT_CEILING: usize = MAX_BLOCKING / 2;

/// The agent load settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentLimits {
    pub max_concurrent_statements: usize,
    pub disclosure_chunk_rows: u64,
    pub disclosure_chunk_bytes: u64,
    pub ceiling_refusal_delay_ms: u64,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_concurrent_statements: DEFAULT_MAX_CONCURRENT_STATEMENTS,
            disclosure_chunk_rows: DEFAULT_DISCLOSURE_CHUNK_ROWS,
            disclosure_chunk_bytes: DEFAULT_DISCLOSURE_CHUNK_BYTES,
            ceiling_refusal_delay_ms: DEFAULT_CEILING_REFUSAL_DELAY_MS,
        }
    }
}

impl AgentLimits {
    /// The checks that need no engine: `1 <= ceiling <= MAX_BLOCKING / 2`.
    /// The engine bound (`Limits::admitted / 2`) is checked by
    /// `Service::set_agent_limits`.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_AGENT_CEILING).contains(&self.max_concurrent_statements) {
            return Err(format!(
                "agent max_concurrent_statements must be 1 to {MAX_AGENT_CEILING}"
            ));
        }
        if self.ceiling_refusal_delay_ms > MAX_CEILING_REFUSAL_DELAY_MS {
            return Err("agent ceiling_refusal_delay_ms must be 0 to 1000".into());
        }
        Ok(())
    }
}

/// The all-agents error a full admission returns.
pub(crate) fn all_agents_error() -> CoreError {
    CoreError::Task(TaskError::BudgetExceeded(Budget::AllAgents))
}

#[derive(Debug)]
struct Slots {
    ceiling: AtomicUsize,
    held: AtomicUsize,
    released: tokio::sync::Notify,
}

/// Agent admission: at most `ceiling` agent requests past this point at once,
/// waiting up to the refusal delay for a freed slot when full. The ceiling
/// changes on reload; a lower ceiling admits nothing new until the held slots
/// drop below it.
#[derive(Debug)]
pub(crate) struct AgentAdmission {
    slots: Arc<Slots>,
    chunk_rows: AtomicU64,
    chunk_bytes: AtomicU64,
    refusal_delay_ms: AtomicU64,
}

/// One admitted agent request; the slot returns on drop.
#[derive(Debug)]
pub(crate) struct AgentSlot(Arc<Slots>);

impl Drop for AgentSlot {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::AcqRel);
        self.0.released.notify_one();
    }
}

impl Default for AgentAdmission {
    fn default() -> Self {
        let limits = AgentLimits::default();
        Self {
            slots: Arc::new(Slots {
                ceiling: AtomicUsize::new(limits.max_concurrent_statements),
                held: AtomicUsize::new(0),
                released: tokio::sync::Notify::new(),
            }),
            chunk_rows: AtomicU64::new(limits.disclosure_chunk_rows),
            chunk_bytes: AtomicU64::new(limits.disclosure_chunk_bytes),
            refusal_delay_ms: AtomicU64::new(limits.ceiling_refusal_delay_ms),
        }
    }
}

impl AgentAdmission {
    /// Takes a slot, or the all-agents error at once.
    pub(crate) fn try_admit(&self) -> Result<AgentSlot, CoreError> {
        let ceiling = self.slots.ceiling.load(Ordering::Acquire);
        self.slots
            .held
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < ceiling).then_some(n + 1)
            })
            .map(|_| AgentSlot(self.slots.clone()))
            .map_err(|_| all_agents_error())
    }

    /// Waits asynchronously for a freed slot, pacing ceiling refusals.
    pub(crate) async fn admit(&self) -> Result<AgentSlot, CoreError> {
        if let Ok(slot) = self.try_admit() {
            return Ok(slot);
        }
        let delay = self.refusal_delay_ms.load(Ordering::Acquire);
        if delay == 0 {
            return Err(all_agents_error());
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(delay);
        loop {
            let notified = self.slots.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if tokio::time::Instant::now() >= deadline {
                return Err(all_agents_error());
            }
            if let Ok(slot) = self.try_admit() {
                return Ok(slot);
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => return Err(all_agents_error()),
                _ = notified => {}
            }
        }
    }

    /// Installs already-validated limits.
    pub(crate) fn apply(&self, limits: AgentLimits) {
        self.slots
            .ceiling
            .store(limits.max_concurrent_statements, Ordering::Release);
        self.refusal_delay_ms
            .store(limits.ceiling_refusal_delay_ms, Ordering::Release);
        self.chunk_rows
            .store(limits.disclosure_chunk_rows, Ordering::Release);
        self.chunk_bytes
            .store(limits.disclosure_chunk_bytes, Ordering::Release);
    }

    pub(crate) fn limits(&self) -> AgentLimits {
        AgentLimits {
            max_concurrent_statements: self.slots.ceiling.load(Ordering::Acquire),
            disclosure_chunk_rows: self.chunk_rows.load(Ordering::Acquire),
            disclosure_chunk_bytes: self.chunk_bytes.load(Ordering::Acquire),
            ceiling_refusal_delay_ms: self.refusal_delay_ms.load(Ordering::Acquire),
        }
    }

    /// Agent requests admitted now.
    pub(crate) fn held(&self) -> usize {
        self.slots.held.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refusal_delay_and_slot_release() {
        for delay in [0, 50] {
            let admission = AgentAdmission::default();
            admission.apply(AgentLimits {
                max_concurrent_statements: 1,
                ceiling_refusal_delay_ms: delay,
                ..AgentLimits::default()
            });
            let held = admission.try_admit().unwrap();
            let start = tokio::time::Instant::now();
            let error = admission.admit().await.unwrap_err();
            assert_eq!(giggles_core::sqlstate(&error), "54T01");
            if delay == 0 {
                assert!(start.elapsed() < std::time::Duration::from_millis(50));
            } else {
                assert!(start.elapsed() >= std::time::Duration::from_millis(50));
                let release = async {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    drop(held);
                };
                let (result, ()) = tokio::join!(admission.admit(), release);
                assert!(result.is_ok());
                continue;
            }
            drop(held);
        }
    }

    #[test]
    fn validate_refusal_delay() {
        for delay in [0, 1000, 1001] {
            let limits = AgentLimits {
                ceiling_refusal_delay_ms: delay,
                ..AgentLimits::default()
            };
            assert_eq!(limits.validate().is_ok(), delay <= 1000);
        }
    }

    #[test]
    fn admission_refuses_at_once_when_full_and_slots_return() {
        let admission = AgentAdmission::default();
        let slots: Vec<_> = (0..DEFAULT_MAX_CONCURRENT_STATEMENTS)
            .map(|_| admission.try_admit().unwrap())
            .collect();
        assert_eq!(admission.held(), 8);
        let error = admission.try_admit().unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "54T01");
        assert_eq!(admission.held(), 8);
        drop(slots);
        assert_eq!(admission.held(), 0);
        assert!(admission.try_admit().is_ok());
        assert_eq!(admission.held(), 0);
    }

    #[test]
    fn lower_ceiling_admits_nothing_until_held_drops_below_it() {
        let admission = AgentAdmission::default();
        let mut slots: Vec<_> = (0..4).map(|_| admission.try_admit().unwrap()).collect();
        admission.apply(AgentLimits {
            max_concurrent_statements: 2,
            ..AgentLimits::default()
        });
        assert!(admission.try_admit().is_err());
        slots.truncate(2);
        assert!(admission.try_admit().is_err());
        slots.truncate(1);
        let _again = admission.try_admit().unwrap();
        assert!(admission.try_admit().is_err());
    }

    #[test]
    fn validate_bounds_ceiling_by_half_the_blocking_pool() {
        let at = |n| AgentLimits {
            max_concurrent_statements: n,
            ..AgentLimits::default()
        };
        assert!(at(0).validate().is_err());
        assert!(at(1).validate().is_ok());
        assert!(at(MAX_BLOCKING / 2).validate().is_ok());
        assert!(at(MAX_BLOCKING / 2 + 1).validate().is_err());
        assert_eq!(AgentLimits::default().max_concurrent_statements, 8);
        assert_eq!(AgentLimits::default().disclosure_chunk_rows, 256);
        assert_eq!(AgentLimits::default().disclosure_chunk_bytes, 262_144);
    }
}
