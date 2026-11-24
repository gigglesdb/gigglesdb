//! Owned cursor syntax and resumable scan position; no persisted state.
use super::query::BoundQuery;
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
// Keeps the two owners of the borrows in a bound cursor query alive.
// Invariant for the unsafe lifetime erasure in `new`: never add mutable
// access to `prepared` or `image` (no `Arc::get_mut`/`make_mut`, no `&mut`
// accessor, no interior mutability they lend to the bound query), never move
// a value out of them, and never expose the erased lifetime. Sharing this
// holder between plans preserves the same invariant.
struct CursorQuery {
    // Must be dropped before the owners below, in declaration order.
    bound: BoundQuery<'static>,
    prepared: Arc<Prepared>,
    image: Arc<Relational>,
}
impl CursorQuery {
    fn new(
        prepared: Prepared,
        image: &Arc<Relational>,
        clock: Option<&clock::ExecutionClock>,
        allow_sharing: bool,
    ) -> R<Option<(Self, bool)>> {
        let prepared = Arc::new(prepared);
        let image = Arc::clone(image);
        let Some(bound) = bind_query(&prepared, &image)? else {
            return Ok(None);
        };
        if bound.cursor_direction().is_none() {
            return Ok(None);
        }
        let shareable = allow_sharing && bound.cursor_clock_independent();
        let bound = if shareable { bound } else { bound.with_clock(clock) };
        // SAFETY: The Arc allocations keep Prepared and Relational at stable
        // addresses. Neither pointee can be moved out or mutated while bound
        // borrows it. The private holder retains both Arcs and drops bound
        // first. for_image shortens the covariant lifetime to the borrow of
        // the holder, so no reference can escape with the erased lifetime.
        let bound = unsafe { std::mem::transmute::<BoundQuery<'_>, BoundQuery<'static>>(bound) };
        Ok(Some((Self { bound, prepared, image }, shareable)))
    }

    fn for_image<'a>(&'a self, state: &Relational) -> Option<&'a BoundQuery<'a>> {
        std::ptr::eq(state, self.image.as_ref()).then_some(&self.bound)
    }
}

#[cfg(test)]
thread_local! {
    static QUERY_BINDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, feature = "engine-rocksdb"))]
pub(crate) fn query_binds() -> usize {
    QUERY_BINDS.get()
}

fn bind_query<'a>(
    prepared: &'a Prepared,
    state: &'a Relational,
) -> R<Option<BoundQuery<'a>>> {
    let Statement::Query(query) = prepared.statement.get() else {
        return Ok(None);
    };
    #[cfg(test)]
    QUERY_BINDS.set(QUERY_BINDS.get() + 1);
    query.bind(state, prepared.params, &prepared.parameter_types).map(Some)
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct BindingContext {
    statement_tokens: usize,
    value_bytes: usize,
    wide_values: bool,
    date_order: giggles_pg_types::temporal::DateOrder,
}

impl BindingContext {
    fn current(p: &Prepared) -> Option<Self> {
        // With no relations bind_schemas can use the current AST instead of
        // the saved unbound AST. Materialized collation metadata is ambient
        // and can override the catalog column metadata during Query::bind.
        if p.relations.is_empty()
            || p.unbound_statement.is_none()
            || !matches!(p.statement.get(), Statement::Query(_))
            || query::has_materialized_collations()
        {
            return None;
        }
        Some(Self {
            statement_tokens: max_statement_tokens(),
            value_bytes: max_value_bytes(),
            wide_values: wide_values(),
            date_order: clock::LiteralDateOrder::current(),
        })
    }
}

/// One last successful, clock-independent streaming binding per SQL session.
#[derive(Default)]
pub(crate) struct PlanCache(Option<(BindingContext, Arc<CursorQuery>)>);

impl PlanCache {
    fn reuse(
        &mut self,
        image: &Arc<Relational>,
        source: &Prepared,
        context: Option<BindingContext>,
    ) -> Option<Arc<CursorQuery>> {
        if let Some((saved_context, query)) = &self.0 {
            let p = &query.prepared;
            // The bound Prepared retains the original Arc. Any copy-on-write
            // edit of that syntax must acquire a different allocation.
            if Arc::ptr_eq(&query.image, image)
                && Some(*saved_context) == context
                && p.unbound_statement.as_ref().zip(source.unbound_statement.as_ref())
                    .is_some_and(|(bound, unbound)| Arc::ptr_eq(bound, unbound))
                && p.domain == source.domain
                && p.epoch == source.epoch
                && p.transaction == source.transaction
                && p.literal_date_order == source.literal_date_order
                && p.params == source.params
                && p.parameter_types == source.parameter_types
                && p.relations == source.relations
                && p.search_path == source.search_path
                && p.schema_user == source.schema_user
            {
                return Some(query.clone());
            }
        }
        // Release the old image even if binding the new key fails.
        self.0 = None;
        None
    }
}

pub(crate) struct Plan {
    query: Arc<CursorQuery>,
    params: Vec<Value>,
    clock: Option<clock::ExecutionClock>,
    pub(crate) progress: Progress,
}
impl Plan {
    pub(crate) fn new(
        state: &Arc<Relational>,
        p: &Prepared,
        params: &[Value],
        clock: Option<clock::ExecutionClock>,
        mut cache: Option<&mut PlanCache>,
    ) -> R<Option<Self>> {
        let context = cache.as_ref().and_then(|_| BindingContext::current(p));
        let query = if let Some(query) = cache.as_mut().and_then(|c| c.reuse(state, p, context)) {
            // This exact image and input already passed schema binding,
            // validation and the epoch check. Parameters are execution inputs.
            Self::check_params(p.params, params)?;
            query
        } else {
            let p = p.bind_schemas(state)?;
            p.validate_schemas(state)?;
            if p.epoch != state.epoch {
                return Err(SqlError::StaleSchema);
            }
            Self::check_params(p.params, params)?;
            let allow_sharing = context.is_some()
                && matches!(p.unbound_statement.as_deref(), Some(Statement::Query(query))
                    if query.is_plain_cursor_candidate());
            let Some((query, shareable)) = CursorQuery::new(p, state, clock.as_ref(), allow_sharing)? else {
                return Ok(None);
            };
            let query = Arc::new(query);
            if shareable && let Some((cache, context)) = cache.zip(context) {
                cache.0 = Some((context, query.clone()));
            }
            query
        };
        Ok(Some(Self {
            query,
            params: params.to_vec(),
            clock,
            progress: Progress::default(),
        }))
    }

    fn check_params(count: usize, params: &[Value]) -> R<()> {
        if params.len() != count || params.len() > MAX_PARAMS {
            return Err(SqlError::Parameters);
        }
        params_bound(params.iter())
    }

    pub(crate) fn fetch(
        &mut self,
        state: &Relational,
        source: &dyn scan::RowSource,
        max_rows: usize,
        max_bytes: usize,
    ) -> R<Vec<SqlRow>> {
        let prepared = &self.query.prepared;
        let _date_order = clock::LiteralDateOrder::enter(prepared.literal_date_order);
        let rebound;
        let bound = match self.query.for_image(state) {
            Some(bound) => bound,
            // Storage always fetches with the image the cursor opened on, so
            // this rebind is only a fallback for other callers (and tests);
            // a caller that passes another image pays a bind per fetch.
            None => {
                rebound = bind_query(prepared, state)?
                    .expect("cursor query")
                    .with_clock(self.clock.as_ref());
                &rebound
            }
        };
        bound.fetch_cursor(
            state,
            source,
            &self.params,
            &mut self.progress,
            max_rows,
            max_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOMAIN: u128 = 1;

    fn fixture() -> Arc<Relational> {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE t(id int PRIMARY KEY)",
            "INSERT INTO t VALUES (1),(2),(3),(4)",
        ] {
            let p = Prepared::parse(sql, DOMAIN, state.epoch).unwrap();
            state.execute_with_cancel(&p, &[], None, || false).unwrap();
        }
        Arc::new(state)
    }

    #[test]
    fn fetch_reuses_the_open_bind_for_the_same_image() {
        let image = fixture();
        let different = Arc::new((*image).clone());
        let p = Prepared::parse(
            "SELECT id FROM t ORDER BY id DESC LIMIT 3 OFFSET 1",
            DOMAIN,
            image.epoch,
        )
        .unwrap();
        QUERY_BINDS.set(0);
        let mut reused = Plan::new(&image, &p, &[], None, None).unwrap().unwrap();
        let mut rebound = Plan::new(&image, &p, &[], None, None).unwrap().unwrap();
        assert_eq!(QUERY_BINDS.get(), 2);
        for (page, expected) in [Some(3), Some(2), Some(1), None].into_iter().enumerate() {
            let actual = reused.fetch(&image, &scan::LegacyRows(&image), 1, 1024).unwrap();
            assert_eq!(QUERY_BINDS.get(), 2 + page);
            let old = rebound.fetch(&different, &scan::LegacyRows(&different), 1, 1024).unwrap();
            assert_eq!(QUERY_BINDS.get(), 3 + page);
            assert_eq!(actual, old);
            assert_eq!(actual.len(), usize::from(expected.is_some()));
            if let Some(id) = expected {
                assert_eq!(actual[0].values, [Value::Int(id)]);
            }
        }
        assert!(reused.progress.exhausted());
    }

    #[test]
    fn reused_binding_rechecks_params_and_respects_binder_context() {
        let image = fixture();
        let mut p = Prepared::parse("SELECT id FROM t WHERE id = $1", DOMAIN, image.epoch)
            .unwrap().bind_schemas(&image).unwrap();
        let mut cache = PlanCache::default();
        let params = [Value::Int(1)];
        QUERY_BINDS.set(0);

        Plan::new(&image, &p, &params, None, Some(&mut cache)).unwrap().unwrap();
        assert!(matches!(Plan::new(&image, &p, &[], None, Some(&mut cache)), Err(SqlError::Parameters)));
        assert_eq!(QUERY_BINDS.get(), 1);
        {
            let _limit = ValueBudget::enter(128, true);
            Plan::new(&image, &p, &params, None, Some(&mut cache)).unwrap().unwrap();
            assert_eq!(QUERY_BINDS.get(), 2);
        }
        Plan::new(&image, &p, &params, None, Some(&mut cache)).unwrap().unwrap();
        let _tokens = StatementBudget::enter(128, true);
        Plan::new(&image, &p, &params, None, Some(&mut cache)).unwrap().unwrap();
        assert_eq!(QUERY_BINDS.get(), 4);

        p.search_path.push("other".into());
        Plan::new(&image, &p, &params, None, Some(&mut cache)).unwrap().unwrap();
        assert_eq!(QUERY_BINDS.get(), 5);
    }

    #[test]
    fn clock_dependent_queries_keep_each_execution_clock() {
        let image = fixture();
        let p = Prepared::parse("SELECT statement_timestamp() FROM t", DOMAIN, image.epoch)
            .unwrap().bind_schemas(&image).unwrap();
        let mut cache = PlanCache::default();
        let mut open = |micros| {
            let clock = clock::ExecutionClock::new(&clock::ExecutionClock::pinned(micros, None));
            Plan::new(&image, &p, &[], Some(clock), Some(&mut cache)).unwrap().unwrap()
        };
        QUERY_BINDS.set(0);
        let mut first = open(1);
        let mut second = open(2);
        assert_eq!(QUERY_BINDS.get(), 2);
        assert!(cache.0.is_none());
        for (plan, micros) in [(&mut second, 2), (&mut first, 1)] {
            let rows = plan.fetch(&image, &scan::LegacyRows(&image), 1, 1024).unwrap();
            assert_eq!(rows[0].values, [Value::TimestampTz(
                giggles_pg_types::temporal::TimestampTz::from_microseconds(micros).unwrap(),
            )]);
        }
    }

    #[test]
    fn bind_error_is_returned_at_open() {
        let image = fixture();
        let p = Prepared::parse("SELECT missing FROM t", DOMAIN, image.epoch).unwrap();
        QUERY_BINDS.set(0);
        assert!(matches!(Plan::new(&image, &p, &[], None, None), Err(SqlError::UnknownColumn)));
        assert_eq!(QUERY_BINDS.get(), 1);
    }
}
