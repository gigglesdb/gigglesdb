//! A COPY statement keeps one savepoint and snapshot across bounded input pages.
use super::*;

pub(super) struct CopyInput {
    mark: engine::txn::Savepoint,
    implicit: bool,
    pub started: bool,
    pub executing: bool,
    pub floor: Option<u64>,
    pub table: Option<String>,
    pub versions: Option<std::ops::Range<u64>>,
    pub finishing: bool,
    affected: u64,
    deferred: relational::DeferralState,
}

impl SqlSession {
    /// Start bounded COPY input on v2. Legacy callers retain buffered ingestion.
    /// Abort or dropping this session discards unfinished input.
    pub fn begin_copy_input(&mut self) -> Result<bool, CoreError> {
        if !matches!(self.admin.inner.backend, Backend::V2(_)) {
            return Ok(false);
        }
        if self.copy_input.is_some() || self.aborted {
            return Err(CoreError::Sql(relational::SqlError::Transaction));
        }
        let implicit = !self.in_transaction();
        if implicit {
            self.execute("BEGIN", &[], None)?;
        }
        self.copy_input = Some(CopyInput {
            mark: self.v2_transaction.as_mut().expect("v2 transaction").savepoint(),
            implicit, executing: false, started: false, floor: None, table: None, versions: None, finishing: false, affected: 0,
            deferred: self.deferral.clone(),
        });
        Ok(true)
    }

    /// Feed at most 200 normalized rows through the ordinary insert publisher.
    pub fn feed_copy_input_at(&mut self, prepared: &relational::Prepared,
        rows: Vec<Vec<relational::Value>>, clock: &relational::StatementClock,
    ) -> Result<(), CoreError> {
        if self.copy_input.is_none() || rows.len() > 200 {
            self.abort();
            return Err(CoreError::Sql(relational::SqlError::Limit));
        }
        self.copy_input.as_mut().expect("active COPY").executing = true;
        let result = self.ingest_copy_at(prepared, rows, clock);
        if result.is_err() { self.abort(); }
        let result = result?;
        let input = self.copy_input.as_mut().expect("active COPY");
        input.executing = false;
        input.started = true;
        input.affected += result.affected;
        Ok(())
    }

    /// Validate end-of-statement FKs, then publish once (or join the outer txn).
    pub fn finish_copy_input_at(&mut self, prepared: &relational::Prepared,
        clock: &relational::StatementClock,
    ) -> Result<relational::SqlResult, CoreError> {
        self.copy_input.as_mut().ok_or(CoreError::Sql(relational::SqlError::Transaction))?.finishing = true;
        self.copy_input.as_mut().expect("active COPY").executing = true;
        let result = self.ingest_copy_at(prepared, vec![], clock);
        if result.is_err() { self.abort(); }
        result?;
        let input = self.copy_input.take().expect("active COPY");
        self.v2_transaction.as_mut().expect("v2 transaction").release(input.mark)?;
        if input.implicit {
            self.execute("COMMIT", &[], None)?;
        }
        Ok(relational::SqlResult { affected: input.affected, ..Default::default() })
    }

    pub(super) fn abort_copy_input(&mut self) {
        if let Some(input) = self.copy_input.take() {
            let txn = self.v2_transaction.as_mut().expect("v2 transaction");
            let restored = txn.rollback_to(&input.mark).and_then(|()| txn.release(input.mark));
            self.deferral = input.deferred;
            if input.implicit || restored.is_err() { self.rollback(); }
        }
    }
}
