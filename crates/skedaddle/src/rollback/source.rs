//! Final rollback steps under the exclusive migration authority guard.
//! Reconstruct adapters from authenticated forward-cutover evidence on retry.
//! Destination is read-only here; no destination unfence capability is accepted.
use super::apply::{Error, Result};
use crate::{
    coordinator::{
        Materialized, State,
        rollback::{Record, Step},
    },
    cutover::Key,
    hooks::{self, HookAudit, HookPhase, OperatorHook},
    pg::{activate, fence},
    security::Redactor,
};
use std::time::Duration;

pub trait Journal {
    fn state(&self) -> Materialized;
    /// Return only after authenticated durable publication and materialization.
    fn publish(&mut self, record: Record) -> Result<()>;
    /// Retain native intents scoped to this rollback request, not forward state.
    fn native(&mut self, intent: &fence::Intent) -> std::result::Result<(), String>;
}

pub trait Effects {
    fn validate(&self, key: &Key) -> Result<()>;
    fn sequences(&mut self, boundary: &str) -> Result<()>;
    fn restore(&mut self, journal: &mut impl Journal) -> Result<()>;
    /// Idempotent compensation; never replaces the original saved snapshot.
    fn refence(&mut self, journal: &mut impl Journal) -> Result<()>;
}
pub trait PostHook {
    /// Must be idempotent by stable migration + rollback request, across epochs.
    fn run(&mut self, key: &Key) -> Result<()>;
}
pub struct Hooks<'a, A> {
    pub hooks: &'a [OperatorHook],
    pub redactor: &'a Redactor,
    pub audit: A,
}
impl<A: FnMut(&HookAudit)> PostHook for Hooks<'_, A> {
    fn run(&mut self, _: &Key) -> Result<()> {
        hooks::run_phase_then(
            HookPhase::PostRollback,
            self.hooks,
            self.redactor,
            &mut self.audit,
            || (),
        )
        .map_err(|error| match error.outcome {
            hooks::HookOutcome::InvalidConfiguration | hooks::HookOutcome::Failed => {
                Error::OperatorAction("post-rollback hook failed; inspect redacted audit")
            }
            _ => Error::Retry,
        })
    }
}

/// Construct only from the original authenticated forward fence (including its
/// original epoch), or its matching (migration, epoch) fence_state row. Never
/// snapshot the currently fenced catalog as the desired restoration state.
pub struct Pg<S, D> {
    pub source: S,
    pub destination: D,
    pub original: fence::Fence,
    pub scope: activate::SequenceScope,
    pub timeout: Duration,
}
fn database(error: fence::FenceError) -> Error {
    match error {
        fence::FenceError::Database(_) | fence::FenceError::Journal(_) => Error::Retry,
        _ => Error::OperatorAction("source fence evidence differs"),
    }
}
impl<S: fence::Connection, D: fence::Connection> Effects for Pg<S, D> {
    fn validate(&self, key: &Key) -> Result<()> {
        if self.original.migration != key.migration
            || self.original.config.epoch <= 0
            || self.original.config.epoch > key.epoch
            || self.original.saved.is_none()
            || self.original.next != fence::Step::Complete
        {
            return Err(Error::OperatorAction(
                "original source fence identity unavailable",
            ));
        }
        Ok(())
    }
    fn sequences(&mut self, boundary: &str) -> Result<()> {
        activate::sync_sequences(
            &mut self.destination,
            &mut self.source,
            &self.scope,
            boundary,
            self.timeout,
        )
        .map_err(|e| match e {
            activate::Error::Database(_) | activate::Error::Journal(_) => Error::Retry,
            _ => Error::OperatorAction("reverse sequence evidence differs"),
        })?;
        Ok(())
    }
    fn restore(&mut self, journal: &mut impl Journal) -> Result<()> {
        let saved = self.original.saved.as_ref().ok_or(Error::OperatorAction(
            "original source fence snapshot missing",
        ))?;
        // Detect changes to owners, memberships, and non-login role attributes
        // before granting anything. The fence helper verifies grants and login.
        let current =
            fence::read_catalog(&mut self.source, &self.original.config.tables, self.timeout)
                .map_err(database)?;
        let mut expected = saved.catalog.clone();
        let mut observed = current.clone();
        expected.grants.clear();
        observed.grants.clear();
        for role in &mut observed.roles {
            if self.original.config.applications.contains(&role.name)
                && let Some(want) = expected.roles.iter().find(|r| r.name == role.name)
            {
                role.login = want.login;
            }
        }
        for catalog in [&mut observed, &mut expected] {
            catalog.roles.sort_by(|a, b| a.name.cmp(&b.name));
            catalog
                .memberships
                .sort_by(|a, b| (&a.member, &a.role).cmp(&(&b.member, &b.role)));
            catalog.tables.sort();
        }
        if observed != expected {
            return Err(Error::OperatorAction(
                "source owners, memberships or role attributes changed",
            ));
        }
        self.original
            .unfence(&mut self.source, &mut |i| journal.native(i))
            .map_err(database)
    }
    fn refence(&mut self, journal: &mut impl Journal) -> Result<()> {
        let mut fence = self.original.clone();
        if fence.saved.is_none() {
            return Err(Error::OperatorAction(
                "original source fence snapshot missing",
            ));
        }
        fence.next = fence::Step::NoLogin;
        // NOLOGIN, revoke, terminate and drain use the ORIGINAL grants and roles.
        while fence.next != fence::Step::Recheck {
            fence
                .execute_next(&mut self.source, &mut |i| journal.native(i), &mut || Ok(()))
                .map_err(database)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Progress,
    Complete,
}

/// Owner must retain the exclusive authority guard across this call. Neither
/// deadline expiry nor any error after PONR can restore the destination.
/// Retry infrastructure faults with bounded backoff and reconstructed adapters.
pub fn execute_next(
    key: &Key,
    effects: &mut impl Effects,
    hook: &mut impl PostHook,
    journal: &mut impl Journal,
) -> Result<Outcome> {
    let state = journal.state();
    let cp = state
        .rollback
        .as_ref()
        .ok_or(Error::OperatorAction("rollback checkpoint missing"))?;
    if key.epoch <= 0
        || state.epoch != key.epoch as u64
        || state.owner != key.coordinator
        || !cp.point_of_no_return
        || cp.final_marker.is_none_or(|v| v == 0)
    {
        return Err(Error::OperatorAction(
            "rollback authority or final marker unavailable",
        ));
    }
    if cp.next == Step::Complete && state.lifecycle.state == State::SourceActiveAfterRollback {
        return Ok(Outcome::Complete);
    }
    if state.lifecycle.state != State::RollingBack
        || state.rollback_eligibility_ended.is_some()
        || !matches!(
            cp.next,
            Step::Sequences | Step::RestoreSource | Step::PostHook
        )
    {
        return Err(Error::OperatorAction(
            "source activation prerequisites unavailable",
        ));
    }
    let step = cp.next;
    let hook_key = Key {
        cutover: cp.request.clone(),
        ..key.clone()
    };
    let boundary = format!("{}:{}", cp.request, cp.final_marker.unwrap());
    effects.validate(key)?;
    let result = (|| {
        journal.publish(Record::Intent { step })?;
        match step {
            Step::Sequences => effects.sequences(&boundary)?,
            Step::RestoreSource => effects.restore(journal)?,
            Step::PostHook => {
                // A previous failed post-hook/checkpoint may have re-fenced it.
                // Repeating exact restoration is safe, even after a lost reply.
                effects.restore(journal)?;
                hook.run(&hook_key)?;
            }
            _ => unreachable!(),
        }
        journal.publish(Record::Checkpoint {
            step,
            final_marker: None,
        })
    })();
    if let Err(error) = result {
        // Publication can be durable even when its acknowledgement was lost.
        // Never re-fence a terminal rollback that the journal can authenticate.
        if journal.state().lifecycle.state == State::SourceActiveAfterRollback {
            return Ok(Outcome::Complete);
        }
        if step != Step::Sequences && effects.refence(journal).is_err() {
            return Err(Error::OperatorAction(
                "source re-fence uncertain; operator must fence source; destination remains fenced",
            ));
        }
        return Err(error);
    }
    Ok(if step == Step::PostHook {
        Outcome::Complete
    } else {
        Outcome::Progress
    })
}

#[cfg(test)]
mod tests;
