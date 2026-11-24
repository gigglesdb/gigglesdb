//! Owned cursor syntax and resumable scan position; no persisted state.
use super::query::BoundQuery;
use super::*;
use std::{
    collections::HashMap,
    sync::{
        Mutex, Weak,
        atomic::{AtomicPtr, Ordering},
    },
};

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

#[cfg(any(test, feature = "test-cursor-bindings"))]
thread_local! {
    static QUERY_BINDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(all(test, feature = "engine-rocksdb"), feature = "test-cursor-bindings"))]
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
    #[cfg(any(test, feature = "test-cursor-bindings"))]
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

struct CachedBinding {
    context: BindingContext,
    query: Arc<CursorQuery>,
}

/// Owned by a published catalog snapshot. A binding owns its image and
/// prepared syntax, and has no reference back to the snapshot.
#[derive(Default)]
pub(crate) struct PlanOwners(Mutex<HashMap<usize, Arc<CachedBinding>>>);

struct PlanCacheEntry {
    owners: Weak<PlanOwners>,
    // Also keeps the map key's allocation address from being reused.
    binding: Weak<CachedBinding>,
}

impl Drop for PlanCacheEntry {
    fn drop(&mut self) {
        if let Some(owners) = self.owners.upgrade() {
            let removed = owners.0.lock().unwrap_or_else(|e| e.into_inner())
                .remove(&(self.binding.as_ptr() as usize));
            drop(removed);
        }
    }
}

#[cfg(test)]
thread_local! {
    static SLOT_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// One last successful, clock-independent streaming binding per SQL session.
/// Weak handles let pruning drop the plan and its owners, even while idle.
#[derive(Default)]
pub(crate) struct PlanCache {
    // Identity only, never dereferenced. The entry's Weak prevents address
    // reuse while it is installed. Only exclusive access installs an entry,
    // publishing its identity first. Shared callers can only remove an entry
    // and publish null after taking it under the guard.
    owners: AtomicPtr<PlanOwners>,
    entry: Mutex<Option<PlanCacheEntry>>,
}

impl PlanCache {
    #[cfg(feature = "test-cursor-bindings")]
    pub(crate) fn binding_liveness(&self) -> Option<impl Fn() -> bool + use<>> {
        let entry = self.entry.lock().unwrap_or_else(|e| e.into_inner());
        let binding = entry.as_ref()?.binding.clone();
        Some(move || binding.strong_count() != 0)
    }

    pub(crate) fn clear(&self) {
        if !self.owners.load(Ordering::Acquire).is_null() {
            self.evict_if(|_| true);
        }
    }

    pub(crate) fn observe(&self, owners: Option<&Arc<PlanOwners>>) {
        let current = self.owners.load(Ordering::Acquire);
        let observed = owners.map_or(std::ptr::null(), Arc::as_ptr);
        if !current.is_null() && !std::ptr::eq(current, observed) {
            self.evict_if(|entry| !std::ptr::eq(entry.owners.as_ptr(), observed));
        }
    }

    pub(crate) fn forget_statement(&self, source: &Prepared) {
        if self.owners.load(Ordering::Acquire).is_null() {
            return;
        }
        // Keep the upgraded reference until eviction releases the slot guard.
        // Catalog pruning can otherwise make this the last strong reference.
        let mut binding = None;
        self.evict_if(|entry| {
            binding = entry.binding.upgrade();
            binding.as_ref().is_none_or(|binding| {
                Self::same_statement(&binding.query.prepared, source)
            })
        });
    }

    fn evict_if(&self, should_evict: impl FnOnce(&PlanCacheEntry) -> bool) {
        let removed = {
            #[cfg(test)]
            SLOT_LOCKS.set(SLOT_LOCKS.get() + 1);
            let mut entry = self.entry.lock().unwrap_or_else(|e| e.into_inner());
            if entry.as_ref().is_some_and(should_evict) {
                let removed = entry.take();
                self.owners.store(std::ptr::null_mut(), Ordering::Release);
                removed
            } else {
                None
            }
        };
        drop(removed);
    }

    fn same_statement(bound: &Prepared, source: &Prepared) -> bool {
        // The bound Prepared retains the original Arc. Any copy-on-write
        // edit of that syntax must acquire a different allocation.
        bound.unbound_statement.as_ref().zip(source.unbound_statement.as_ref())
            .is_some_and(|(bound, unbound)| Arc::ptr_eq(bound, unbound))
    }

    fn store(&mut self, owners: &Arc<PlanOwners>, context: BindingContext, query: Arc<CursorQuery>) {
        let binding = Arc::new(CachedBinding { context, query });
        let entry = PlanCacheEntry {
            owners: Arc::downgrade(owners),
            binding: Arc::downgrade(&binding),
        };
        owners.0.lock().unwrap_or_else(|e| e.into_inner())
            .insert(Arc::as_ptr(&binding) as usize, binding);
        self.owners.store(Arc::as_ptr(owners).cast_mut(), Ordering::Release);
        *self.entry.get_mut().unwrap_or_else(|e| e.into_inner()) = Some(entry);
    }

    fn reuse(
        &mut self,
        image: &Arc<Relational>,
        source: &Prepared,
        context: Option<BindingContext>,
    ) -> Option<Arc<CursorQuery>> {
        let entry = self.entry.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(binding) = entry.as_ref().and_then(|entry| entry.binding.upgrade()) {
            let query = &binding.query;
            let p = &query.prepared;
            if Arc::ptr_eq(&query.image, image)
                && Some(binding.context) == context
                && Self::same_statement(p, source)
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
        let removed = entry.take();
        self.owners.store(std::ptr::null_mut(), Ordering::Release);
        drop(removed);
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
        mut cache: Option<(&mut PlanCache, &Arc<PlanOwners>)>,
    ) -> R<Option<Self>> {
        let context = cache.as_ref().and_then(|_| BindingContext::current(p));
        let query = if let Some(query) = cache.as_mut().and_then(|(cache, _)| cache.reuse(state, p, context)) {
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
            if shareable && let Some(((cache, owners), context)) = cache.zip(context) {
                cache.store(owners, context, query.clone());
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
        let owners = Arc::default();
        let params = [Value::Int(1)];
        QUERY_BINDS.set(0);

        Plan::new(&image, &p, &params, None, Some((&mut cache, &owners))).unwrap().unwrap();
        assert!(matches!(Plan::new(&image, &p, &[], None, Some((&mut cache, &owners))), Err(SqlError::Parameters)));
        assert_eq!(QUERY_BINDS.get(), 1);
        {
            let _limit = ValueBudget::enter(128, true);
            Plan::new(&image, &p, &params, None, Some((&mut cache, &owners))).unwrap().unwrap();
            assert_eq!(QUERY_BINDS.get(), 2);
        }
        Plan::new(&image, &p, &params, None, Some((&mut cache, &owners))).unwrap().unwrap();
        let _tokens = StatementBudget::enter(128, true);
        Plan::new(&image, &p, &params, None, Some((&mut cache, &owners))).unwrap().unwrap();
        assert_eq!(QUERY_BINDS.get(), 4);

        p.search_path.push("other".into());
        Plan::new(&image, &p, &params, None, Some((&mut cache, &owners))).unwrap().unwrap();
        assert_eq!(QUERY_BINDS.get(), 5);
    }

    #[test]
    fn clock_dependent_queries_keep_each_execution_clock() {
        let image = fixture();
        let p = Prepared::parse("SELECT statement_timestamp() FROM t", DOMAIN, image.epoch)
            .unwrap().bind_schemas(&image).unwrap();
        let mut cache = PlanCache::default();
        let owners = Arc::default();
        let mut open = |micros| {
            let clock = clock::ExecutionClock::new(&clock::ExecutionClock::pinned(micros, None));
            Plan::new(&image, &p, &[], Some(clock), Some((&mut cache, &owners))).unwrap().unwrap()
        };
        QUERY_BINDS.set(0);
        let mut first = open(1);
        let mut second = open(2);
        assert_eq!(QUERY_BINDS.get(), 2);
        assert!(cache.entry.into_inner().unwrap().is_none());
        for (plan, micros) in [(&mut second, 2), (&mut first, 1)] {
            let rows = plan.fetch(&image, &scan::LegacyRows(&image), 1, 1024).unwrap();
            assert_eq!(rows[0].values, [Value::TimestampTz(
                giggles_pg_types::temporal::TimestampTz::from_microseconds(micros).unwrap(),
            )]);
        }
    }

    #[test]
    fn implicit_timestamptz_cast_keeps_each_execution_clock() {
        let mut image = Relational::default();
        for sql in [
            "CREATE TABLE t(id int PRIMARY KEY, observed_at timestamptz)",
            "INSERT INTO t VALUES (1,'2000-01-01 00:00:00+00'),(2,'2000-01-02 00:00:00+00')",
        ] {
            let p = Prepared::parse(sql, DOMAIN, image.epoch).unwrap();
            image.execute_with_cancel(&p, &[], None, || false).unwrap();
        }
        let image = Arc::new(image);
        let p = Prepared::parse("SELECT id FROM t WHERE observed_at = 'now'", DOMAIN, image.epoch)
            .unwrap().bind_schemas(&image).unwrap();
        assert!(matches!(p.unbound_statement.as_deref(), Some(Statement::Query(query))
            if query.is_plain_cursor_candidate()));

        let mut cache = PlanCache::default();
        let owners = Arc::default();
        let mut open = |micros| {
            let utc = giggles_pg_types::temporal::SessionZone::fixed(0).unwrap();
            let clock = clock::ExecutionClock::new(&clock::ExecutionClock::pinned(micros, Some(utc)));
            Plan::new(&image, &p, &[], Some(clock), Some((&mut cache, &owners))).unwrap().unwrap()
        };
        QUERY_BINDS.set(0);
        let mut first = open(0);
        let mut second = open(86_400_000_000);
        assert_eq!(QUERY_BINDS.get(), 2);
        assert!(cache.entry.into_inner().unwrap().is_none());
        for (plan, id) in [(&mut second, 2), (&mut first, 1)] {
            let rows = plan.fetch(&image, &scan::LegacyRows(&image), 10, 1024).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values, [Value::Int(id)]);
        }
    }

    #[test]
    fn empty_slot_and_same_catalog_observation_skip_the_slot_lock() {
        let image = fixture();
        let prepared = Prepared::parse("SELECT id FROM t", DOMAIN, image.epoch)
            .unwrap().bind_schemas(&image).unwrap();
        let mut cache = PlanCache::default();
        let owners = Arc::default();
        let before = SLOT_LOCKS.get();

        cache.observe(Some(&owners));
        cache.observe(None);
        cache.clear();
        cache.forget_statement(&prepared);
        Plan::new(&image, &prepared, &[], None, Some((&mut cache, &owners))).unwrap().unwrap();
        cache.observe(Some(&owners));

        assert_eq!(SLOT_LOCKS.get(), before);
        assert!(cache.entry.lock().unwrap().is_some());
    }

    #[test]
    fn eviction_releases_the_slot_guard_before_the_binding() {
        let image = fixture();
        let prepared = Prepared::parse("SELECT id FROM t", DOMAIN, image.epoch)
            .unwrap().bind_schemas(&image).unwrap();
        let evictions: [fn(&PlanCache, &Prepared); 3] = [
            |cache, _| cache.observe(None),
            |cache, _| cache.clear(),
            PlanCache::forget_statement,
        ];
        for evict in evictions {
            let owners = Arc::default();
            let mut cache = PlanCache::default();
            Plan::new(&image, &prepared, &[], None, Some((&mut cache, &owners))).unwrap().unwrap();
            let binding = cache.entry.lock().unwrap().as_ref().unwrap().binding.clone();

            std::thread::scope(|scope| {
                let registry = owners.0.lock().unwrap();
                let eviction = scope.spawn(|| evict(&cache, &prepared));
                let start = std::time::Instant::now();
                let slot_released = loop {
                    if cache.entry.try_lock().is_ok_and(|entry| entry.is_none()) {
                        break true;
                    }
                    if start.elapsed() >= std::time::Duration::from_secs(5) {
                        break false;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                };
                assert!(binding.upgrade().is_some());
                drop(registry);
                eviction.join().unwrap();
                assert!(slot_released, "eviction held the slot guard while waiting for the registry");
            });
            assert!(binding.upgrade().is_none());
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
