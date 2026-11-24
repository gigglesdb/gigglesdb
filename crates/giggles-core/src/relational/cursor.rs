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
// accessor, no interior mutability they lend to `bound`), never move a value
// out of them, and never return `bound` with its erased lifetime.
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
    ) -> R<Option<Self>> {
        let prepared = Arc::new(prepared);
        let image = Arc::clone(image);
        let Some(bound) = bind_query(&prepared, &image, clock)? else {
            return Ok(None);
        };
        if bound.cursor_direction().is_none() {
            return Ok(None);
        }
        // SAFETY: The Arc allocations keep Prepared and Relational at stable
        // addresses. Neither pointee can be moved out or mutated while bound
        // borrows it. The private holder retains both Arcs and drops bound
        // first. for_image shortens the covariant lifetime to the borrow of
        // the holder, so no reference can escape with the erased lifetime.
        let bound = unsafe { std::mem::transmute::<BoundQuery<'_>, BoundQuery<'static>>(bound) };
        Ok(Some(Self { bound, prepared, image }))
    }

    fn for_image<'a>(&'a self, state: &Relational) -> Option<&'a BoundQuery<'a>> {
        std::ptr::eq(state, self.image.as_ref()).then_some(&self.bound)
    }
}

#[cfg(test)]
thread_local! {
    static QUERY_BINDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn bind_query<'a>(
    prepared: &'a Prepared,
    state: &'a Relational,
    clock: Option<&clock::ExecutionClock>,
) -> R<Option<BoundQuery<'a>>> {
    let Statement::Query(query) = prepared.statement.get() else {
        return Ok(None);
    };
    #[cfg(test)]
    QUERY_BINDS.set(QUERY_BINDS.get() + 1);
    Ok(Some(
        query
            .bind(state, prepared.params, &prepared.parameter_types)?
            .with_clock(clock),
    ))
}

pub(crate) struct Plan {
    query: CursorQuery,
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
        let Some(query) = CursorQuery::new(p, state, clock.as_ref())? else {
            return Ok(None);
        };
        Ok(Some(Self {
            query,
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
        let prepared = &self.query.prepared;
        let _date_order = clock::LiteralDateOrder::enter(prepared.literal_date_order);
        let rebound;
        let bound = match self.query.for_image(state) {
            Some(bound) => bound,
            // Storage always fetches with the image the cursor opened on, so
            // this rebind is only a fallback for other callers (and tests);
            // a caller that passes another image pays a bind per fetch.
            None => {
                rebound = bind_query(prepared, state, self.clock.as_ref())?.expect("cursor query");
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
        let mut reused = Plan::new(&image, &p, &[], None).unwrap().unwrap();
        let mut rebound = Plan::new(&image, &p, &[], None).unwrap().unwrap();
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
    fn bind_error_is_returned_at_open() {
        let image = fixture();
        let p = Prepared::parse("SELECT missing FROM t", DOMAIN, image.epoch).unwrap();
        QUERY_BINDS.set(0);
        assert!(matches!(Plan::new(&image, &p, &[], None), Err(SqlError::UnknownColumn)));
        assert_eq!(QUERY_BINDS.get(), 1);
    }
}
