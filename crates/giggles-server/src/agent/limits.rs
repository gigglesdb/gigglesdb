//! Agent statement ceiling and agent admission (GD-0061 R3-403, design-3
//! section 7, "All-agents ceiling").
//!
//! `agent.max_concurrent_statements` bounds the statements all agent sessions
//! run at once. It is set in two places with the same value: core's
//! all-agent slot count (`Admin::set_agent_statement_ceiling`) and the
//! [`AgentAdmission`] slots every agent request takes before it waits for a
//! blocking worker (`Blocking::admit`). A full admission refuses the request
//! at once with the all-agents budget error (54T01); nothing queues. Agent
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
/// The largest ceiling the blocking pool allows: half of it.
pub const MAX_AGENT_CEILING: usize = MAX_BLOCKING / 2;

/// The three agent load settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentLimits {
    pub max_concurrent_statements: usize,
    pub disclosure_chunk_rows: u64,
    pub disclosure_chunk_bytes: u64,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_concurrent_statements: DEFAULT_MAX_CONCURRENT_STATEMENTS,
            disclosure_chunk_rows: DEFAULT_DISCLOSURE_CHUNK_ROWS,
            disclosure_chunk_bytes: DEFAULT_DISCLOSURE_CHUNK_BYTES,
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
}

/// Agent admission: at most `ceiling` agent requests past this point at once,
/// refused at once when full (no queue). The ceiling changes on reload; a
/// lower ceiling admits nothing new until the held slots drop below it.
#[derive(Debug)]
pub(crate) struct AgentAdmission {
    slots: Arc<Slots>,
    chunk_rows: AtomicU64,
    chunk_bytes: AtomicU64,
}

/// One admitted agent request; the slot returns on drop.
#[derive(Debug)]
pub(crate) struct AgentSlot(Arc<Slots>);

impl Drop for AgentSlot {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for AgentAdmission {
    fn default() -> Self {
        let limits = AgentLimits::default();
        Self {
            slots: Arc::new(Slots {
                ceiling: AtomicUsize::new(limits.max_concurrent_statements),
                held: AtomicUsize::new(0),
            }),
            chunk_rows: AtomicU64::new(limits.disclosure_chunk_rows),
            chunk_bytes: AtomicU64::new(limits.disclosure_chunk_bytes),
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

    /// Installs already-validated limits.
    pub(crate) fn apply(&self, limits: AgentLimits) {
        self.slots
            .ceiling
            .store(limits.max_concurrent_statements, Ordering::Release);
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
