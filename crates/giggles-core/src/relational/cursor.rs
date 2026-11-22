//! Owned cursor syntax and resumable scan position; no persisted state.
use super::*;

#[derive(Default)]
pub(crate) struct Progress {
    pub(super) initialized: bool,
    pub(super) skip: u64,
    pub(super) remaining: Option<u64>,
    pub(super) after: Option<Vec<u8>>,
    pub(super) pending: Option<SqlRow>,
    pub(crate) done: bool,
}
impl Progress {
    pub(crate) fn exhausted(&self) -> bool {
        self.done && self.pending.is_none()
    }
}
pub(crate) struct Plan {
    prepared: Prepared,
    params: Vec<Value>,
    clock: Option<clock::ExecutionClock>,
    pub(crate) progress: Progress,
}
impl Plan {
    pub(crate) fn new(
        state: &Relational,
        p: &Prepared,
        params: &[Value],
        clock: Option<clock::ExecutionClock>,
    ) -> R<Option<Self>> {
        let p = p.bind_schemas(state)?;
        p.validate_schemas(state)?;
        if p.epoch != state.epoch {
            return Err(SqlError::StaleSchema);
        }
        if params.len() != p.params || params.len() > MAX_PARAMS {
            return Err(SqlError::Parameters);
        }
        params_bound(params.iter())?;
        let Statement::Query(query) = &p.statement else {
            return Ok(None);
        };
        let bound = query
            .bind(state, p.params, &p.parameter_types)?
            .with_clock(clock.as_ref());
        if bound.cursor_direction().is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            prepared: p,
            params: params.to_vec(),
            clock,
            progress: Progress::default(),
        }))
    }
    pub(crate) fn fetch(
        &mut self,
        state: &Relational,
        source: &dyn scan::RowSource,
        max_rows: usize,
        max_bytes: usize,
    ) -> R<Vec<SqlRow>> {
        let _date_order = clock::LiteralDateOrder::enter(self.prepared.literal_date_order);
        let Statement::Query(query) = &self.prepared.statement else {
            unreachable!()
        };
        query
            .bind(state, self.prepared.params, &self.prepared.parameter_types)?
            .with_clock(self.clock.as_ref())
            .fetch_cursor(
                state,
                source,
                &self.params,
                &mut self.progress,
                max_rows,
                max_bytes,
            )
    }
}
