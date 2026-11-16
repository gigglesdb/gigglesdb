//! Typed operations in a caller-retained SQL/native transaction.
use super::*;

impl SqlSession {
    /// Execute against this session's snapshot. An idle session owns its
    /// transaction; an existing transaction remains provisional until COMMIT.
    pub fn execute_native(&mut self, request: &Request, bindings: &Bindings) -> Result<Outcome> {
        let v2 = self.is_v2_engine();
        self.native_scope(
            false,
            || {
                validate_request(request, Admission::engine(v2))?;
                validate_bindings(bindings)
            },
            |session| {
                let (table, operation) = bind_request(session, request, bindings)?;
                run_complete(session, &table, operation, None)
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
        let v2 = self.is_v2_engine();
        self.native_scope(
            block.read_only,
            || {
                transaction::validate_transaction(block, Admission::engine(v2))?;
                validate_bindings(bindings)
            },
            |session| {
                let plan = transaction::bind_transaction(session, block, bindings)?;
                transaction::run_transaction(session, &plan)
            },
        )
    }

    pub(super) fn native_scope<T>(
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

    /// Capture a named native/SQL savepoint in the active transaction.
    /// Names are case-sensitive identifier values, like quoted SQL names,
    /// and are truncated at PostgreSQL's 63-byte UTF-8 boundary.
    pub fn savepoint(&mut self, name: &str) -> std::result::Result<(), CoreError> {
        self.native_control(|session| {
            session.named_control_engine(name, crate::relational::Prepared::savepoint_engine)
        })
    }

    /// Undo to a savepoint, recovering an aborted transaction like SQL ROLLBACK TO.
    pub fn rollback_to(&mut self, name: &str) -> std::result::Result<(), CoreError> {
        self.native_control(|session| {
            session.named_control_engine(name, crate::relational::Prepared::rollback_to_engine)
        })
    }

    /// Release the named savepoint and all savepoints created after it.
    pub fn release(&mut self, name: &str) -> std::result::Result<(), CoreError> {
        self.native_control(|session| {
            session.named_control_engine(name, crate::relational::Prepared::release_engine)
        })
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

#[cfg(test)]
mod savepoint_tests {
    use crate::{Admin, CoreError, Engine, Limits, relational::{SqlError, Value}};

    #[test]
    fn rollback_to_past_ddl_invalidates_bound_native_table_handles() {
        #[allow(unused_mut)]
        let mut engines = vec![Engine::Legacy];
        #[cfg(feature = "engine-rocksdb")]
        engines.push(Engine::V2);
        std::fs::create_dir_all(crate::test_artifact_root()).unwrap();
        for engine in engines {
            let mut token = [0; 16];
            getrandom::fill(&mut token).unwrap();
            let path = crate::test_artifact_root().join(format!(
                "native-savepoint-ddl-{:032x}", u128::from_be_bytes(token)));
            let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = admin.sql_session();
            s.execute("CREATE TABLE t(id BIGINT PRIMARY KEY)", &[], None).unwrap();
            for native in [false, true] {
                s.begin_native().unwrap();
                if native { s.savepoint("before_ddl").unwrap(); }
                else { s.execute("SAVEPOINT before_ddl", &[], None).unwrap(); }
                s.execute("ALTER TABLE t ADD COLUMN extra BIGINT", &[], None).unwrap();
                let (bound_fence, tables) = s.catalog_engine().unwrap();
                let bound = tables.into_iter().find(|table| table.name == "t").unwrap();
                if native { s.rollback_to("before_ddl").unwrap(); }
                else { s.execute("ROLLBACK TO before_ddl", &[], None).unwrap(); }
                assert_ne!(s.catalog_engine().unwrap().0, bound_fence);
                assert_eq!(s.insert_engine(&bound, vec![Value::Int(1), Value::Int(2)]),
                    Err(CoreError::Sql(SqlError::StaleSchema)));
                s.rollback_to("before_ddl").unwrap();
                assert!(!s.is_aborted());
                s.rollback_native().unwrap();
            }
            drop(s);
            drop(admin);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
