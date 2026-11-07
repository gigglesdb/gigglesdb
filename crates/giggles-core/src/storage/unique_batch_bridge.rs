use super::*;
use relational::{BatchRowChange, SqlError};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "private foundation awaiting the unique-conflict kernel"
    )
)]
pub(crate) struct BatchFence {
    pub transaction_id: u128,
    pub staged_revision: u64,
    pub domain: u128,
    pub epoch: u64,
    pub generation: u64,
}

/// Handed to the plan closure; applies the batch to the session-owned candidate.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "private foundation awaiting the unique-conflict kernel"
    )
)]
pub(crate) struct BatchApply<'c> {
    target: &'c mut relational::Relational,
    epoch: u64,
    cancel: &'c mut dyn FnMut() -> bool,
    result: Option<relational::SqlResult>,
    attempted: bool,
}
impl BatchApply<'_> {
    /// Must be called exactly once per batch. A second call refuses without applying.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "private foundation awaiting the unique-conflict kernel"
        )
    )]
    pub(crate) fn apply(
        &mut self,
        changes: &[BatchRowChange<'_>],
        next_version: u64,
    ) -> Result<(), SqlError> {
        if self.attempted {
            return Err(SqlError::Transaction);
        }
        self.attempted = true;
        self.result = Some(self.target.secondary_apply_batch_engine(
            self.epoch,
            changes,
            next_version,
            &mut *self.cancel,
        )?);
        Ok(())
    }
}
impl SqlSession {
    /// Capture the fence of the active explicit transaction.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "private foundation awaiting the unique-conflict kernel"
        )
    )]
    pub(crate) fn batch_fence_engine(&mut self) -> Result<BatchFence, CoreError> {
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(SqlError::Aborted));
            }
            let staged = self
                .staged
                .as_ref()
                .ok_or(CoreError::Sql(SqlError::Transaction))?;
            crate::cancellation::check()?;
            Ok(BatchFence {
                transaction_id: self
                    .transaction_id
                    .ok_or(CoreError::Sql(SqlError::Transaction))?,
                staged_revision: self.staged_revision,
                domain: staged.view.domain,
                epoch: staged.sql.epoch,
                generation: staged.sql.generation,
            })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "private foundation awaiting the unique-conflict kernel"
        )
    )]
    pub(crate) fn apply_batch_engine<T>(
        &mut self,
        fence: BatchFence,
        cancel: &mut dyn FnMut() -> bool,
        plan: impl FnOnce(&relational::Relational, &mut BatchApply<'_>) -> Result<T, SqlError>,
    ) -> Result<(T, relational::SqlResult), CoreError> {
        fn run<T>(
            session: &mut SqlSession,
            fence: BatchFence,
            cancel: &mut dyn FnMut() -> bool,
            plan: impl FnOnce(&relational::Relational, &mut BatchApply<'_>) -> Result<T, SqlError>,
        ) -> Result<(T, relational::SqlResult), CoreError> {
            if session.aborted {
                return Err(CoreError::Sql(SqlError::Aborted));
            }
            let staged = session
                .staged
                .as_ref()
                .ok_or(CoreError::Sql(SqlError::Transaction))?;
            let inner = session.admin.inner.clone();
            let _ticket = inner.enter_sql()?;
            crate::cancellation::check()?;
            if cancel() {
                return Err(CoreError::Sql(SqlError::Aborted));
            }
            if session.transaction_id != Some(fence.transaction_id)
                || session.staged_revision != fence.staged_revision
                || staged.view.domain != fence.domain
                || staged.sql.epoch != fence.epoch
                || staged.sql.generation != fence.generation
            {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            if staged.applications.is_some() {
                return Err(CoreError::Unsupported);
            }
            let next_revision = session
                .staged_revision
                .checked_add(1)
                .ok_or(CoreError::Sql(SqlError::Overflow))?;
            let mut candidate = staged.clone();
            let mut apply = BatchApply {
                target: &mut candidate.sql,
                epoch: fence.epoch,
                cancel,
                result: None,
                attempted: false,
            };
            let value = plan(&staged.sql, &mut apply).map_err(CoreError::Sql)?;
            let result = apply.result.ok_or(CoreError::Sql(SqlError::Transaction))?;
            records(&candidate)?;
            crate::cancellation::check()?;
            if cancel() {
                return Err(CoreError::Sql(SqlError::Aborted));
            }
            session.staged = Some(candidate);
            session.staged_revision = next_revision;
            Ok((value, result))
        }
        match catch_unwind(AssertUnwindSafe(|| run(self, fence, cancel, plan))) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                self.abort();
                Err(error)
            }
            Err(payload) => {
                self.abort();
                resume_unwind(payload)
            }
        }
    }
}
#[cfg(test)]
#[path = "unique_batch_bridge_tests.rs"]
mod tests;
