//! Typed operations in a caller-retained SQL/native transaction.
use super::*;

/// The task statement class of a native request (GD-0061 R3-309, read log).
fn request_class(request: &Request) -> crate::task::StatementClass {
    use crate::task::StatementClass as C;
    match request {
        Request::Read { .. } | Request::ReadExpressions { .. } => C::Select,
        Request::Update { .. } | Request::UpdateExpressions { .. } => C::Update,
        Request::Delete { .. } => C::Delete,
        Request::Insert { .. }
        | Request::InsertExpressions { .. }
        | Request::InsertMany { .. }
        | Request::OnConflict { .. } => C::Insert,
        _ => C::Other,
    }
}

/// The SQLSTATE a native error is logged with.
fn native_sqlstate(error: &Error) -> String {
    match error.sqlstate() {
        Some(state) => state.to_owned(),
        None => match error {
            Error::Unsupported => "0A000",
            Error::Limit => "54000",
            _ => "XX000",
        }
        .to_owned(),
    }
}

impl SqlSession {
    /// Execute against this session's snapshot. An idle session owns its
    /// transaction; an existing transaction remains provisional until COMMIT.
    ///
    /// Plain exit for trusted local callers: a task session is refused before
    /// anything runs (GD-0061 R3-301). Task callers use `execute_native_unreleased`.
    pub fn execute_native(&mut self, request: &Request, bindings: &Bindings) -> Result<Outcome> {
        self.refuse_task_session().map_err(core)?;
        self.execute_native_unreleased(request, bindings)?.untasked().map_err(core)
    }

    pub fn execute_native_unreleased(
        &mut self,
        request: &Request,
        bindings: &Bindings,
    ) -> Result<crate::task::release::Unreleased<Outcome>> {
        let v2 = self.is_v2_engine();
        // GD-0061 R3-309: the request's read-log entry, logged with its
        // canonical JSON and, on failure, its error.
        let entry = self.start_native_statement(request_class(request), bindings.len(), || {
            serde_json::to_string(request).unwrap_or_default()
        });
        // The tag carries the plan the task's column rules gave at binding
        // (GD-0061 R3-304/R3-308); a request refused there makes no page.
        let mut plan = None;
        let outcome = self.native_scope(
            false,
            || {
                validate_request(request, Admission::engine(v2))?;
                validate_bindings(bindings)
            },
            |session| {
                let (table, operation, bound) = bind_request_planned(session, request, bindings)?;
                plan = bound;
                run_complete(session, &table, operation, None)
            },
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some(entry) = &entry {
                    entry.fail_with(&native_sqlstate(&error));
                }
                return Err(error);
            }
        };
        let tag = entry.and_then(|entry| {
            self.release_tag_for(entry, plan.ok_or(crate::task::TaskError::Revoked))
        });
        Ok(crate::task::release::Unreleased::new(outcome, tag))
    }

    /// Starts the read-log entry of a native task statement with `text` as
    /// its text (GD-0061 R3-309); `None` without a task, and `text` never runs.
    fn start_native_statement(
        &self,
        class: crate::task::StatementClass,
        param_count: usize,
        text: impl FnOnce() -> String,
    ) -> Option<std::sync::Arc<crate::task::readlog::StatementEntry>> {
        self.task.as_ref()?;
        self.set_task_statement_text(&text());
        self.start_task_statement(class, param_count)
    }

    /// A block stages as a unit into an outer transaction. Failure aborts that
    /// transaction; this API does not introduce nested commits or savepoints.
    /// V2 binds all steps upfront, then runs each with a fresh READ COMMITTED
    /// snapshot plus its own writes. Legacy retains one snapshot for the block.
    ///
    /// Plain exit; refused in a task session like `execute_native`.
    pub fn execute_native_transaction(
        &mut self,
        block: &Transaction,
        bindings: &Bindings,
    ) -> Result<TransactionOutcome> {
        self.refuse_task_session().map_err(core)?;
        self.execute_native_transaction_unreleased(block, bindings)?
            .untasked()
            .map_err(core)
    }

    pub fn execute_native_transaction_unreleased(
        &mut self,
        block: &Transaction,
        bindings: &Bindings,
    ) -> Result<crate::task::release::Unreleased<TransactionOutcome>> {
        // A block's emitted values have no per-column plan yet: in a task
        // session the block is refused (fail closed, R3-308). Its steps are
        // still bound first, so a step's own column-rule denial keeps its
        // SQLSTATE, but no step runs and no row is read.
        if self.task.is_some() {
            self.set_task_statement_text(&serde_json::to_string(block).unwrap_or_default());
        }
        let tag = self.release_tag_with(
            crate::task::StatementClass::TransactionControl,
            bindings.len(),
            || {
                Err(crate::task::TaskError::NotAllowedInTaskSession(
                    crate::task::StatementClass::TransactionControl,
                ))
            },
        );
        let v2 = self.is_v2_engine();
        let result = self.native_scope(
            block.read_only,
            || {
                transaction::validate_transaction(block, Admission::engine(v2))?;
                validate_bindings(bindings)
            },
            |session| {
                let plan = transaction::bind_transaction(session, block, bindings)?;
                if let Some(tag) = &tag {
                    tag.refuse_denied().map_err(core_or_task)?;
                }
                transaction::run_transaction(session, &plan)
            },
        );
        if let (Err(error), Some(tag)) = (&result, &tag) {
            tag.fail_with(&native_sqlstate(error));
        }
        result.map(|outcome| crate::task::release::Unreleased::new(outcome, tag))
    }

    pub(super) fn native_scope<T>(
        &mut self,
        read_only: bool,
        validate: impl FnOnce() -> Result<()>,
        work: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if self.is_aborted() {
            if self.is_v2_engine() {
                self.v2_lease(crate::storage::Holder::Sql).map_err(core)?;
            }
            return Err(core(CoreError::Sql(SqlError::Aborted)));
        }
        // GD-0061 R3-401: a task request's slots and time limit, released on
        // return, error or unwind.
        let guard = crate::task::limits::StatementGuard::enter(self).map_err(task_error)?;
        let _active = guard.as_ref().map(crate::task::limits::StatementGuard::activate);
        let owned = !self.in_transaction();
        let previous_generation = self.native_generation.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate()?;
            if owned {
                if self.is_v2_engine() {
                    self.begin_native_implicit().map_err(core)?;
                } else {
                    self.begin_engine().map_err(core)?;
                }
            }
            self.pin_native_generation(owned).map_err(core)?;
            let result = work(self)?;
            // Active outer transactions have no publication fence in this call.
            crate::cancellation::check().map_err(core)?;
            if owned {
                if read_only {
                    self.rollback();
                } else {
                    let committed = if self.is_v2_engine() {
                        self.commit_native_implicit()
                    } else {
                        self.commit_engine()
                    };
                    committed.map_err(|error| match error {
                        CoreError::Storage(_) => Error::Unknown,
                        other => core(other),
                    })?;
                }
            }
            Ok(result)
        }));
        // Retain this operation's pin through rollback and result handling.
        let _generation = std::mem::replace(&mut self.native_generation, previous_generation);
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
