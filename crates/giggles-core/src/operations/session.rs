//! Typed operations in a caller-retained SQL/native transaction.
use super::*;

impl SqlSession {
    /// Execute against this session's snapshot. An idle session owns its
    /// transaction; an existing transaction remains provisional until COMMIT.
    pub fn execute_native(&mut self, request: &Request, bindings: &Bindings) -> Result<Outcome> {
        self.native_scope(
            false,
            || {
                validate_request(request)?;
                validate_bindings(bindings)
            },
            |session| {
                let (table, operation) = bind_request(session, request, bindings)?;
                operation.check_input_cap()?;
                let input = session
                    .complete_table_engine(&table)
                    .map_err(|error| match error {
                        CoreError::Limit => Error::Limit,
                        other => core(other),
                    })?;
                run(session, &table, operation, input, None)
            },
        )
    }

    /// A block stages as a unit into an outer transaction. Failure aborts that
    /// transaction; this API does not introduce nested commits or savepoints.
    /// V2 binds all steps upfront, then runs each with a fresh READ COMMITTED
    /// snapshot plus its own writes. Legacy retains one snapshot for the block.
    pub fn execute_native_transaction(
        &mut self,
        block: &Transaction,
        bindings: &Bindings,
    ) -> Result<TransactionOutcome> {
        self.native_scope(
            block.read_only,
            || {
                transaction::validate_transaction(block)?;
                validate_bindings(bindings)
            },
            |session| {
                let plan = transaction::bind_transaction(session, block, bindings)?;
                transaction::run_transaction(session, &plan)
            },
        )
    }

    fn native_scope<T>(
        &mut self,
        read_only: bool,
        validate: impl FnOnce() -> Result<()>,
        work: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if self.is_aborted() {
            return Err(core(CoreError::Sql(SqlError::Aborted)));
        }
        let owned = !self.in_transaction();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate()?;
            if owned {
                self.begin_engine().map_err(core)?;
            }
            let result = work(self)?;
            // Active outer transactions have no publication fence in this call.
            crate::cancellation::check().map_err(core)?;
            if owned {
                if read_only {
                    self.rollback();
                } else {
                    self.commit_engine().map_err(|error| match error {
                        CoreError::Storage(_) => Error::Unknown,
                        other => core(other),
                    })?;
                }
            }
            Ok(result)
        }));
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                if owned {
                    self.rollback();
                } else {
                    self.abort();
                }
                Err(error)
            }
            Err(panic) => {
                if owned {
                    self.rollback();
                } else {
                    self.abort();
                }
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Typed BEGIN with the same snapshot admission as the SQL control.
    pub fn begin_native(&mut self) -> std::result::Result<(), CoreError> {
        self.native_control(Self::begin_engine)
    }

    /// Typed COMMIT. A failed commit retains the aborted outer transaction;
    /// storage failures remain ambiguous and must not be automatically retried.
    pub fn commit_native(&mut self) -> std::result::Result<(), CoreError> {
        self.native_control(Self::commit_engine)
    }

    /// Typed ROLLBACK remains reachable while the transaction is aborted.
    pub fn rollback_native(&mut self) -> std::result::Result<(), CoreError> {
        if self.is_v2_engine() {
            return self.native_control(Self::rollback_v2_engine);
        }
        if !self.in_transaction() {
            return Err(CoreError::Sql(SqlError::Transaction));
        }
        self.rollback();
        Ok(())
    }

    fn native_control(
        &mut self,
        work: impl FnOnce(&mut Self) -> std::result::Result<(), CoreError>,
    ) -> std::result::Result<(), CoreError> {
        let owned = !self.in_transaction();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(self))) {
            Ok(result) => {
                if result.is_err() {
                    self.abort();
                }
                result
            }
            Err(panic) => {
                if owned {
                    self.rollback();
                } else {
                    self.abort();
                }
                std::panic::resume_unwind(panic)
            }
        }
    }
}

#[cfg(test)]
mod tests;
