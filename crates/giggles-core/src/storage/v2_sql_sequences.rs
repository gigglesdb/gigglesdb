//! SQL defaults use the existing v2 counter codec and durable reservations.
use super::*;

/// Values exposed even by rolled-back transactions must never be reissued after a crash.
pub(in crate::storage) struct Obligation {
    generation: u64,
    slot: Arc<v2::SeqSlot>,
    id: u64,
    def: relational::catalog_ext::SequenceDef,
    value: i64,
    coverage_epoch: u64,
}
pub(in crate::storage) type Unsettled = Arc<Mutex<Vec<Obligation>>>;

pub(in crate::storage) fn settle(db: &EngineLease, list: &Unsettled) -> Result<(), CoreError> {
    let mut list = list.lock().map_err(|_| CoreError::Fenced)?;
    // Check the entire list before using any slot or resending any counter.
    if list.iter().any(|item| item.generation != db.stamp()) {
        return Err(CoreError::Fenced);
    }
    for item in list.iter() {
        let mut guard = item.slot.counter.lock_cancellable()?;
        drain_ready(&mut guard);
        // A synchronous setval/wrap may have replaced durable_hw since allocation.
        // Its durable install drained all older reservations before moving backwards.
        // Installs only follow setval, a cycle wrap, is_called=false (setval) or an
        // incarnation change (RESTART/DROP); each already lets PostgreSQL reissue
        // earlier values without a crash, so a reservation whose ack failed before
        // such an install needs no further coverage. A failed ack with no install
        // keeps the epoch and falls through to await_coverage's fresh reservation.
        if guard.coverage_epoch != item.coverage_epoch { continue; }
        if guard.counter.as_ref().is_some_and(|c| c.incarnation == item.def.incarnation)
            && guard.durable_hw.is_some_and(|hw| {
                if item.def.increment > 0 { hw >= item.value } else { hw <= item.value }
            })
        {
            continue;
        }
        // await_coverage checks incarnation retirement before consulting durable_hw.
        guard.waiting_returns += 1;
        let key = engine::catalog_codec::counter_key(item.id, item.def.incarnation);
        let outcome = await_coverage(db, &item.slot, guard, item.id, &key, &item.def, item.value);
        let mut guard = item.slot.counter.lock().map_err(|_| CoreError::Fenced)?;
        guard.waiting_returns -= 1;
        outcome?;
    }
    list.clear();
    Ok(())
}

/// A sequence privilege as PostgreSQL names it. GigglesDB has no sequence ACLs
/// yet; `Guard::held` is where GRANT ... ON SEQUENCE would plug in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SequencePrivilege {
    Usage,
    Select,
    Update,
}
const OWNER_ANALOGUE: &[SequencePrivilege] =
    &[SequencePrivilege::Usage, SequencePrivilege::Select, SequencePrivilege::Update];
/// Any one of these suffices (PostgreSQL 18 sequence.c): nextval needs USAGE or
/// UPDATE, setval UPDATE, currval (and lastval, unsupported) USAGE or SELECT.
fn required(operation: &str) -> &'static [SequencePrivilege] {
    use SequencePrivilege::*;
    match operation {
        "nextval" => &[Usage, Update],
        "setval" => &[Update],
        _ => &[Usage, Select],
    }
}
fn role_error(error: roles::RoleError) -> CoreError {
    match error {
        roles::RoleError::Core(error) => error,
        _ => CoreError::Policy(Error::Authentication),
    }
}
type DecisionKey = (u64, u128, relational::clock::SequenceOrigin, &'static [SequencePrivilege]);
struct Decision {
    roles: roles::RoleCatalogImage,
    latest: Option<Arc<relational::Relational>>,
    key: DecisionKey,
}

/// Resolve the owner key by stable table id, following this transaction's own
/// table name when it moved the owner record. Otherwise prefer the live name.
fn table_authority_key(
    id: u64,
    image: &relational::Relational,
    latest: Option<&relational::Relational>,
    private: bool,
) -> Option<String> {
    let named = |source: &relational::Relational| {
        source.tables.values()
            .find(|table| table.id == id)
            .map(|table| source.table_key(table))
    };
    if let Some(latest) = latest.filter(|_| !private) {
        named(latest).or_else(|| named(image))
    } else {
        named(image)
    }
}

impl SqlSession {
    /// The relation key whose owner record authorizes DDL on a resolved sequence.
    /// Its snapshot names the object, while its link and the owning table's name
    /// come from the latest committed catalog overlaid with its own DDL.
    pub fn sequence_ddl_authority_key(&self, name: &str) -> Result<Option<String>, CoreError> {
        let lease = self.statement_lease()?;
        self.inspect_metadata_leased(lease.as_deref(), |state| {
            let image = state.sql.image;
            let Some((&id, definition)) = image.catalog_ext().sequences.iter()
                .find(|(_, def)| def.name == name)
            else {
                // Keep the ordinary authorization and classification path for
                // absent names and names belonging to another kind of object.
                return Ok(Some(name.to_owned()));
            };
            if let Some(staged) = &self.staged {
                return self.admin.inner.inspect_metadata(|committed| {
                    let latest = committed.sql.image;
                    let private_link = staged.sequence_owner_updates.contains_key(&id)
                        || !self.sequence_baseline.contains_key(&id);
                    let current = if private_link {
                        definition
                    } else if let Some(current) = latest.catalog_ext().sequences.get(&id) {
                        current
                    } else {
                        return Ok(None);
                    };
                    match current.owned_by {
                        None => Ok(Some(current.name.clone())),
                        // Legacy moves role records only when the staged DDL commits.
                        Some((table, _)) => Ok(table_authority_key(table, image, Some(latest), false)),
                    }
                });
            }
            let latest = match (lease.as_deref(), &self.v2_transaction) {
                (Some(db), Some(_)) => Some(db.native_metadata_image(None)?.1),
                _ => None,
            };
            let own_catalog = |key: &[u8]| {
                self.v2_transaction.as_ref().is_some_and(|txn| txn.catalog_own(key).is_some())
            };
            let current = if let Some(latest) = &latest
                && !own_catalog(&engine::catalog_codec::sequence_key(id))
            {
                let Some(current) = latest.catalog_ext().sequences.get(&id) else {
                    return Ok(None);
                };
                current
            } else {
                definition
            };
            match current.owned_by {
                None => Ok(Some(current.name.clone())),
                Some((table, _)) => Ok(table_authority_key(
                    table, image, latest.as_deref(), own_catalog(&engine::catalog::table_key(table)),
                )),
            }
        })
    }
}

/// Catalog observations from the S2 authorization that admits this execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceDefaultContext {
    pub stamp: u64,
    pub latest_catalog: u64,
    pub statement_catalog: u64,
}

type DefaultGrant = (u64, u128, &'static [SequencePrivilege]);

/// Opaque successes for resolved column defaults, scoped to one S2 entry.
pub struct SequenceDefaultGrants {
    context: SequenceDefaultContext,
    authority: SequenceAuthority,
    allowed: Vec<DefaultGrant>,
}

pub(in crate::storage) struct DefaultExecution {
    context: SequenceDefaultContext,
    previous: Option<Arc<SequenceDefaultGrants>>,
    guard: Option<Arc<Guard>>,
}

impl DefaultExecution {
    pub(in crate::storage) fn new(
        context: SequenceDefaultContext, previous: Option<Arc<SequenceDefaultGrants>>,
    ) -> Self {
        Self { context, previous, guard: None }
    }

    pub(in crate::storage) fn bind(
        &mut self, db: &EngineLease, txn: &Transaction, mut guard: Guard,
    ) -> Arc<Guard> {
        if guard.pin_default_images
            && db.stamp() == self.context.stamp
            && db.catalog().snapshot_version(txn).is_some_and(|version| version.0 == self.context.statement_catalog)
        {
            guard.memo = Some(DefaultMemo {
                context: self.context,
                previous: self.previous.take().filter(|grants| {
                    grants.context == self.context && grants.authority == guard.authority
                }),
                replay_valid: std::sync::OnceLock::new(),
            });
        }
        let guard = Arc::new(guard);
        if guard.memo.is_some() {
            self.guard = Some(guard.clone());
        }
        guard
    }

    pub(in crate::storage) fn grants(self) -> Option<Arc<SequenceDefaultGrants>> {
        self.guard?.grants()
    }
}

struct DefaultMemo {
    context: SequenceDefaultContext,
    previous: Option<Arc<SequenceDefaultGrants>>,
    replay_valid: std::sync::OnceLock<bool>,
}

type Versioned<T> = (T, Option<u64>);

#[derive(Default)]
struct GuardState {
    roles: Option<Versioned<roles::RoleCatalogImage>>,
    latest: Option<Versioned<Arc<relational::Relational>>>,
    last_pinned: Option<(DecisionKey, bool)>,
    defaults: Vec<DefaultGrant>,
    last_live: Option<Decision>,
}

#[cfg(any(test, feature = "test-metadata-counts"))]
thread_local! {
    // Count resolution attempts in ColumnDefault, Call, ReferentialAction order.
    static SEQUENCE_RESOLUTIONS: std::cell::Cell<[usize; 3]> = const { std::cell::Cell::new([0; 3]) };
    static DEFAULT_GRANT_HITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, feature = "engine-rocksdb"))]
type ColumnDefaultDrawHook = Box<dyn FnOnce(i64)>;

#[cfg(all(test, feature = "engine-rocksdb"))]
thread_local! {
    static AFTER_COLUMN_DEFAULT_DRAW: std::cell::RefCell<Option<ColumnDefaultDrawHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-metadata-counts")]
impl SequenceDefaultGrants {
    #[doc(hidden)]
    pub fn replay_hits() -> usize {
        DEFAULT_GRANT_HITS.get()
    }
}

/// Execution-time sequence authority for one statement (GD-0084 Q24).
///
/// The statement authorizer can only judge names it sees. A dynamic name
/// (`nextval($1)`, a text expression), a sequence replaced or re-owned after
/// authorization, or a referential action's default all reach this callback
/// unjudged, so every call is checked here on the sequence it resolved to, before
/// any session value, counter or cluster sequence RPC is touched.
pub(in crate::storage) struct Guard {
    authority: SequenceAuthority,
    roles: Option<roles::RoleCatalog>,
    /// Owner records this transaction wrote itself (role keyspace, prefix 10).
    private_owners: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Sequences whose definition this transaction created or changed itself.
    private_sequences: std::collections::BTreeSet<u64>,
    /// Tables whose definition this transaction changed itself (for example a
    /// rename, which also moves the owner record to the new name).
    private_tables: std::collections::BTreeSet<u64>,
    pin_default_images: bool,
    memo: Option<DefaultMemo>,
    state: Mutex<GuardState>,
}
impl Guard {
    pub(in crate::storage) fn new(
        authority: &SequenceAuthority,
        roles: roles::RoleCatalog,
        txn: Option<&engine::txn::Transaction>,
        image: &relational::Relational,
    ) -> Self {
        let mut private_owners = BTreeMap::new();
        let mut private_sequences = std::collections::BTreeSet::new();
        let mut private_tables = std::collections::BTreeSet::new();
        if !matches!(authority, SequenceAuthority::Unrestricted)
            && let Some(txn) = txn
            && txn.catalog_writes().next().is_some()
        {
            private_owners = txn
                .catalog_writes()
                .filter(|(key, _)| key.first() == Some(&10))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            private_sequences = image
                .catalog_ext()
                .sequences
                .keys()
                .copied()
                .filter(|id| txn.catalog_own(&engine::catalog_codec::sequence_key(*id)).is_some())
                .collect();
            private_tables = image
                .tables
                .values()
                .map(|table| table.id)
                .filter(|id| txn.catalog_own(&engine::catalog::table_key(*id)).is_some())
                .collect();
        }
        Self {
            authority: authority.clone(),
            roles: Some(roles),
            private_owners,
            private_sequences,
            private_tables,
            pin_default_images: false,
            memo: None,
            state: Mutex::default(),
        }
    }
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(in crate::storage) fn unrestricted() -> Self {
        Self {
            authority: SequenceAuthority::Unrestricted,
            roles: None,
            private_owners: BTreeMap::new(),
            private_sequences: std::collections::BTreeSet::new(),
            private_tables: std::collections::BTreeSet::new(),
            pin_default_images: false,
            memo: None,
            state: Mutex::default(),
        }
    }
    pub(in crate::storage) fn pin_default_images(&mut self, txn: &Transaction) {
        self.pin_default_images = self.can_pin_default_images(txn);
    }

    pub(super) fn can_pin_default_images(&self, txn: &Transaction) -> bool {
        txn.catalog_writes().next().is_none()
            && self.private_owners.is_empty()
            && self.private_sequences.is_empty()
            && self.private_tables.is_empty()
    }

    /// The authority key (owner record name) of table `id`, found by stable id. A
    /// table this transaction changed itself (renamed, say) is named as this
    /// transaction sees it, matching the owner record its DDL moved; any other
    /// table is named as the latest committed catalog has it, falling back to the
    /// statement image when a gateway's latest image trails its routed read.
    fn table_key(
        &self,
        id: u64,
        image: &relational::Relational,
        latest: Option<&relational::Relational>,
    ) -> Option<String> {
        table_authority_key(id, image, latest, self.private_tables.contains(&id))
    }
    /// The owner record that answers for `def`: its owning table's (resolved by id
    /// as in `table_key`), or its own for a standalone sequence.
    fn authority_key(
        &self,
        def: &relational::catalog_ext::SequenceDef,
        image: &relational::Relational,
        latest: Option<&relational::Relational>,
    ) -> Option<String> {
        match def.owned_by {
            None => Some(def.name.clone()),
            Some((table, _)) => self.table_key(table, image, latest),
        }
    }
    /// The owner of `key`, this transaction's own record first (the same codec
    /// as `roles_engine::own_owner`).
    fn owner(&self, roles: &roles::RoleCatalogImage, key: &str) -> Result<Option<String>, CoreError> {
        if let Some(value) = self.private_owners.get(&roles::engine::owner_key(key)?) {
            return roles::engine::decode_owner(value);
        }
        roles.table_owner(key).map_err(role_error)
    }
    /// The privileges `principal` holds on the sequence whose owner record is `key`.
    fn held(
        &self,
        principal: &SequenceAuthority,
        roles: &roles::RoleCatalogImage,
        key: &str,
        standalone: bool,
    ) -> Result<&'static [SequencePrivilege], CoreError> {
        let owner_analogue = match principal {
            SequenceAuthority::Unrestricted => true,
            SequenceAuthority::Denied => false,
            SequenceAuthority::Tables(tables) => !standalone && tables.contains(key),
            SequenceAuthority::Role(name) => {
                if roles.get(name).map_err(role_error)?.is_some_and(|role| role.superuser) {
                    true
                } else {
                    match self.owner(roles, key)? {
                        Some(owner) => roles.privileges_of(name).map_err(role_error)?.contains(&owner),
                        None => false,
                    }
                }
            }
        };
        Ok(if owner_analogue { OWNER_ANALOGUE } else { &[] })
    }
    /// Allow `operation` on sequence `id` (statement image definition `def`) or
    /// refuse it with 42501. `committed` says whether the latest committed catalog
    /// holds this same incarnation.
    #[allow(clippy::too_many_arguments)]
    fn check(
        &self,
        db: &EngineLease,
        image: &relational::Relational,
        operation: &str,
        origin: relational::clock::SequenceOrigin,
        id: u64,
        def: &relational::catalog_ext::SequenceDef,
        committed: bool,
    ) -> Result<(), CoreError> {
        use relational::clock::SequenceOrigin;
        // Column defaults are checked like calls: the statement authorizer judges
        // the stored name through the session search_path and the statement
        // snapshot, misses calls nested in extension functions, and cannot see a
        // later OWNED BY move, while execution draws the resolved sequence.
        if matches!(self.authority, SequenceAuthority::Unrestricted) {
            return Ok(());
        }
        let pin_images = self.pin_default_images && origin == SequenceOrigin::ColumnDefault;
        if pin_images {
            self.roles.as_ref().ok_or(CoreError::SequencePrivilege)?
                .check_fenced().map_err(role_error)?;
        }
        let required = required(operation);
        let key = (id, def.incarnation, origin, required);
        let grant = (pin_images && committed).then_some((id, def.incarnation, required));
        if let Some(grant) = &grant
            && let Some(memo) = &self.memo
            && let Some(previous) = &memo.previous
            && previous.allowed.contains(grant)
            && *memo.replay_valid.get_or_init(|| {
                db.stamp() == memo.context.stamp
                    && db.catalog().current_version() == Some(memo.context.latest_catalog)
            })
        {
            #[cfg(any(test, feature = "test-metadata-counts"))]
            DEFAULT_GRANT_HITS.set(DEFAULT_GRANT_HITS.get() + 1);
            return Ok(());
        }

        // Use the live definition only for a committed sequence the transaction
        // has not altered. The statement image still names the resolved object.
        let use_latest = committed && !self.private_sequences.contains(&id);
        if !pin_images {
            let roles = self.read_roles(db)?;
            let latest = use_latest.then(|| db.native_metadata_image(None).map(|(_, image)| image)).transpose()?;
            let state = self.state.lock().map_err(|_| CoreError::Fenced)?;
            if let Some(decision) = &state.last_live
                && decision.key == key
                && decision.roles.same_image(&roles)
                && match (&decision.latest, &latest) {
                    (None, None) => true,
                    (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                    _ => false,
                }
            {
                return Ok(());
            }
            drop(state);
            self.check_permission(image, key, def, &roles, latest.as_deref())?;
            self.state.lock().map_err(|_| CoreError::Fenced)?.last_live =
                Some(Decision { roles, latest, key });
            return Ok(());
        }

        let mut state = self.state.lock().map_err(|_| CoreError::Fenced)?;
        if state.last_pinned == Some((key, use_latest))
            || grant.as_ref().is_some_and(|grant| state.defaults.contains(grant))
        {
            return Ok(());
        }
        if state.roles.is_none() {
            state.roles = Some(self.read_versioned(db, || self.read_roles(db))?);
        }
        if use_latest && state.latest.is_none() {
            state.latest = Some(self.read_versioned(db, || db.native_metadata_image(None).map(|(_, image)| image))?);
        }
        let roles = &state.roles.as_ref().expect("statement role image").0;
        let latest = state.latest.as_ref().filter(|_| use_latest).map(|(image, _)| &**image);
        self.check_permission(image, key, def, roles, latest)?;
        state.last_pinned = Some((key, use_latest));
        if let Some(grant) = grant {
            state.defaults.push(grant);
        }
        Ok(())
    }

    fn read_roles(&self, db: &EngineLease) -> Result<roles::RoleCatalogImage, CoreError> {
        let catalog = self.roles.as_ref().ok_or(CoreError::SequencePrivilege)?;
        catalog.image_leased(Some(db)).map_err(role_error)
    }

    fn read_versioned<T>(
        &self, db: &EngineLease, read: impl FnOnce() -> Result<T, CoreError>,
    ) -> Result<Versioned<T>, CoreError> {
        // Both reads take fresh committed snapshots through this lease. Catalog
        // publication invalidates the observer before making changes visible.
        // Matching observations bracket the read at exactly the S2 version.
        let version = self.memo.as_ref().map(|memo| memo.context.latest_catalog)
            .filter(|version| db.catalog().current_version() == Some(*version));
        let value = read()?;
        Ok((value, version.filter(|version| db.catalog().current_version() == Some(*version))))
    }

    fn grants(&self) -> Option<Arc<SequenceDefaultGrants>> {
        let memo = self.memo.as_ref()?;
        let previous = memo.previous.as_ref().filter(|_| memo.replay_valid.get() == Some(&true));
        let state = self.state.lock().ok()?;
        let version = Some(memo.context.latest_catalog);
        if state.defaults.is_empty()
            || state.roles.as_ref().is_none_or(|(_, read)| *read != version)
            || state.latest.as_ref().is_none_or(|(_, read)| *read != version)
        {
            return previous.cloned();
        }
        let mut allowed = state.defaults.clone();
        if let Some(previous) = previous {
            for grant in &previous.allowed {
                if !allowed.contains(grant) {
                    allowed.push(*grant);
                }
            }
        }
        Some(Arc::new(SequenceDefaultGrants {
            context: memo.context,
            authority: self.authority.clone(),
            allowed,
        }))
    }

    fn check_permission(
        &self,
        image: &relational::Relational,
        key: DecisionKey,
        def: &relational::catalog_ext::SequenceDef,
        roles: &roles::RoleCatalogImage,
        latest: Option<&relational::Relational>,
    ) -> Result<(), CoreError> {
        use relational::clock::SequenceOrigin;
        let (id, _, origin, required) = key;
        let (current, owner_key) = match latest {
            Some(latest) => {
                // A replacement (another incarnation) is never judged under this name.
                let current = latest
                    .catalog_ext()
                    .sequences
                    .get(&id)
                    .filter(|current| current.incarnation == def.incarnation)
                    .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
                // The ownership link (owning table id) is the latest committed one, so
                // an OWNED BY transfer committed elsewhere applies even under
                // REPEATABLE READ; only the table's name follows this transaction's
                // own DDL (see `table_key`).
                (current, self.authority_key(current, image, Some(latest)))
            }
            None => (def, self.authority_key(def, image, None)),
        };
        let standalone = current.owned_by.is_none();
        // Referential actions run as the referencing table's owner (PostgreSQL
        // ri_triggers.c), found by stable id like the owning table above.
        // A referencing table with no owner record (created by an application
        // identity or an embedded session) is judged on the session's own principal,
        // exactly as a direct call by the caller would be: a caller with direct
        // authority on the sequence passes, and a caller without it is refused with
        // 42501. That refusal is a behaviour change from main, where such FK actions
        // always succeeded; it never grants more than a direct call.
        let principal = match origin {
            SequenceOrigin::ReferentialAction(child) => self
                .table_key(child, image, latest)
                .map(|key| self.owner(roles, &key))
                .transpose()?
                .flatten()
                .map(SequenceAuthority::Role),
            _ => None,
        };
        let principal = principal.as_ref().unwrap_or(&self.authority);
        let allowed = match &owner_key {
            Some(owner_key) => {
                let held = self.held(principal, roles, owner_key, standalone)?;
                required.iter().any(|privilege| held.contains(privilege))
            }
            // An owning table missing from every image: refuse.
            None => false,
        };
        if !allowed {
            return Err(CoreError::SequencePrivilege);
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn callback(
    db: impl Into<CallbackLease>,
    image: Arc<relational::Relational>,
    baseline: BTreeMap<u64, u128>,
    pending: sequences::Pending,
    values: sequences::SessionValues,
    unsettled: Unsettled,
    search_path: Vec<String>,
    guard: Arc<Guard>,
    cache_defaults: bool,
) -> relational::clock::SequenceCallback {
    use relational::clock::SequenceOrigin;
    let db = db.into();
    // Only the statement executor opts in, after acquiring Table(0). Local
    // sequence DDL holds that barrier exclusively through catalog publication.
    let defaults = cache_defaults.then(|| Mutex::new(BTreeMap::<String, (u64, bool)>::new()));
    Arc::new(move |operation, name, value, called, origin| {
        let db = db.lease(Holder::Sequence)?;
        crate::cancellation::check()?;
        let mut cache = defaults.as_ref()
            .filter(|_| origin == SequenceOrigin::ColumnDefault && !db.catalog().is_poisoned())
            .map(|cache| cache.lock().map_err(|_| CoreError::Fenced))
            .transpose()?;
        let (id, def, committed) = if let Some(&(id, committed)) =
            cache.as_ref().and_then(|cache| cache.get(name))
        {
            (id, &image.catalog_ext().sequences[&id], committed)
        } else {
            #[cfg(any(test, feature = "test-metadata-counts"))]
            {
                let index = match origin {
                    SequenceOrigin::ColumnDefault => 0,
                    SequenceOrigin::Call => 1,
                    SequenceOrigin::ReferentialAction(_) => 2,
                };
                let mut counts = SEQUENCE_RESOLUTIONS.get();
                counts[index] += 1;
                SEQUENCE_RESOLUTIONS.set(counts);
            }
            let parsed = sequences::sequence_name(name).map_err(CoreError::Sql)?;
            let key = image.resolve_relation_key(&parsed, &search_path);
            let (&id, def) = image
                .catalog_ext()
                .sequences
                .iter()
                .find(|(_, def)| def.name == key)
                .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
            let committed = if db.cluster_services().is_some() {
                // Gateways retain the routed fresh read; their cache can lag remote DDL.
                let mut check = db.begin(Isolation::ReadCommitted);
                check
                    .get(Keyspace::Catalog, &engine::catalog_codec::sequence_key(id))?
                    .map(|bytes| engine::catalog_codec::decode_sequence(&bytes))
                    .transpose()?
                    .is_some_and(|current| current.incarnation == def.incarnation)
            } else {
                // Reuse the authority image when it is already pinned. The
                // first draw still checks committedness before Guard::check.
                let latest = if cache.is_some() {
                    let state = guard.state.lock().map_err(|_| CoreError::Fenced)?;
                    state.latest.as_ref().map(|(image, _)| Arc::clone(image))
                } else {
                    None
                };
                if let Some(latest) = latest {
                    latest.catalog_ext().sequences.get(&id)
                        .is_some_and(|current| current.incarnation == def.incarnation)
                } else {
                    db.catalog()
                        .current_local_view()?
                        .sequences
                        .get(&id)
                        .is_some_and(|current| current.incarnation == def.incarnation)
                }
            };
            if !committed && baseline.get(&id) == Some(&def.incarnation) {
                return Err(CoreError::Sql(SqlError::UnknownTable));
            }
            if let Some(cache) = cache.as_mut() {
                cache.insert(name.to_owned(), (id, committed));
            }
            (id, def, committed)
        };
        drop(cache);
        // Authority on this resolved id and incarnation, before session values,
        // counters or the cluster sequence RPC; every use below names the same pair.
        guard.check(&db, &image, operation, origin, id, def, committed)?;
        if operation == "currval" {
            let values = values.lock().map_err(|_| CoreError::Fenced)?;
            return sequences::currval(&values, id, def.incarnation)
                .map(relational::Value::Int)
                .map_err(CoreError::Sql);
        }
        if committed && let Some(services) = db.cluster_services() {
            // A cluster gateway keeps no counter: the system range leader evaluates every
            // nextval/setval of a committed sequence (GD-0075 R5-43, design.md Changes).
            use crate::range_api::{SequenceOp, SequenceReply};
            let op = match operation {
                "nextval" => SequenceOp::Next,
                "setval" => SequenceOp::Set {
                    value: value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                    called,
                },
                _ => return Err(CoreError::Sql(SqlError::Unsupported)),
            };
            let reply = services
                .client()
                .sequence(id, def.incarnation, op)
                .map_err(|e| {
                    CoreError::from(engine::txn::TxnError::Kv(match e {
                        crate::range_api::RangeError::Corrupt(m) => engine::kv::KvError::Corrupt(m),
                        other => engine::kv::KvError::Io(other.to_string()),
                    }))
                })?;
            let result = match reply {
                SequenceReply::Value(result) => result,
                SequenceReply::Gone => return Err(CoreError::Sql(SqlError::UnknownTable)),
                SequenceReply::Limit => return Err(CoreError::Sql(SqlError::SequenceLimit)),
                SequenceReply::OutOfRange => return Err(CoreError::Sql(SqlError::Overflow)),
            };
            if operation == "nextval" || called {
                values
                    .lock()
                    .map_err(|_| CoreError::Fenced)?
                    .insert((id, def.incarnation), result);
            }
            return Ok(relational::Value::Int(result));
        }
        if committed {
            let slot = {
                let mut live = db.sql_counters.lock_cancellable()?;
                live.entry(id).or_default().clone()
            };
            let result = committed_value(&db, &slot, id, def, operation, value, called, &unsettled)?;
            if operation == "nextval" || called {
                values
                    .lock()
                    .map_err(|_| CoreError::Fenced)?
                    .insert((id, def.incarnation), result);
            }
            #[cfg(all(test, feature = "engine-rocksdb"))]
            if origin == SequenceOrigin::ColumnDefault
                && operation == "nextval"
                && let Some(hook) = AFTER_COLUMN_DEFAULT_DRAW.take()
            {
                hook(result);
            }
            return Ok(relational::Value::Int(result));
        }
        let mut pending = if committed {
            None
        } else {
            Some(pending.lock().map_err(|_| CoreError::Fenced)?)
        };
        let prior = pending
            .as_ref()
            .expect("private sequence")
            .get(&id)
            .cloned();
        let (result, counter) = match operation {
            "nextval" => sequences::next_on(def, prior),
            "setval" => sequences::set_on(
                def,
                prior,
                value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                called,
            ),
            _ => Err(SqlError::Unsupported),
        }
        .map_err(CoreError::Sql)?;
        pending
            .as_mut()
            .expect("private sequence")
            .insert(id, counter);
        if operation == "nextval" || called {
            values
                .lock()
                .map_err(|_| CoreError::Fenced)?
                .insert((id, def.incarnation), result);
        }
        Ok(relational::Value::Int(result))
    })
}

type SlotGuard<'a> = crate::cancellation::WaitGuard<'a, v2::SeqSlotState>;

#[derive(Debug)]
enum ReceiveError {
    Core(CoreError),
    Gone(u128),
}
impl From<CoreError> for ReceiveError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}
impl From<ReceiveError> for CoreError {
    fn from(error: ReceiveError) -> Self {
        match error {
            ReceiveError::Core(error) => error,
            ReceiveError::Gone(_) => engine::txn::TxnError::SequenceGone.into(),
        }
    }
}

/// One receiver owns the oldest ack; all other callers release the slot while waiting.
fn receive_one<'a>(
    slot: &'a v2::SeqSlot,
    mut guard: SlotGuard<'a>,
) -> Result<SlotGuard<'a>, ReceiveError> {
    if guard.receiving {
        // Observe under the slot lock so a subsequent state change either bumps
        // the generation before we sleep or notifies us. Always reacquire the
        // slot after at most one cancellation slice: this also rechecks state
        // after a missed signal and returns Fenced if the slot was poisoned.
        let observed = *slot.ack_gen.lock().map_err(|_| CoreError::Fenced)?;
        drop(guard);
        crate::cancellation::check()?;
        let slice = crate::cancellation::Cancellation::current()
            .map_or(std::time::Duration::from_millis(5), |c| c.wait_slice());
        let deadline = std::time::Instant::now() + slice;
        let mut generation = slot.ack_gen.lock().map_err(|_| CoreError::Fenced)?;
        while *generation == observed {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() { break; }
            let (next, timeout) = slot.ack_cv.wait_timeout(generation, remaining)
                .map_err(|_| CoreError::Fenced)?;
            generation = next;
            if timeout.timed_out() { break; }
        }
        drop(generation);
        return slot.counter.lock_cancellable().map_err(ReceiveError::Core);
    }
    let Some(write) = guard.inflight.pop_front() else {
        return Ok(guard);
    };
    guard.receiving = true;
    drop(guard);
    let _durable = crate::Cancellation::current_durable_token();
    let outcome = loop {
        if let Err(error) = crate::cancellation::check() {
            // Cancellation must not lose the oldest ack or leave receiving set.
            let mut guard = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
            guard.inflight.push_front(write);
            guard.receiving = false;
            wake_ack_waiters(slot)?;
            return Err(error.into());
        }
        match write
            .ack
            .0
            .recv_timeout(std::time::Duration::from_millis(5))
        {
            Ok(result) => break result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                break Err(engine::txn::TxnError::Fenced);
            }
        }
    };
    let mut guard = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
    guard.receiving = false;
    if outcome.is_ok() {
        guard.acknowledge(write.incarnation, write.hw);
    }
    wake_ack_waiters(slot)?;
    match outcome {
        Ok(()) => Ok(guard),
        Err(engine::txn::TxnError::SequenceGone) => Err(ReceiveError::Gone(write.incarnation)),
        Err(error) => Err(ReceiveError::Core(error.into())),
    }
}

// Only callers that observed receiving=true use this wakeup. drain_ready cannot
// advance coverage while receiving; synchronous installs drain receivers first.
// The durable DDL publication path also signals after installing a new incarnation.
// Inflight pushes do not complete the oldest ack. Retirement without an install
// is observed when the retired receiver finishes (including SequenceGone).
pub(super) fn wake_ack_waiters(slot: &v2::SeqSlot) -> Result<(), CoreError> {
    let mut generation = slot.ack_gen.lock().map_err(|_| CoreError::Fenced)?;
    *generation = generation.wrapping_add(1);
    slot.ack_cv.notify_all();
    Ok(())
}

/// Publish a durable incarnation and wake callers waiting on the old receiver.
pub(in crate::storage) fn install_and_wake(
    slot: &v2::SeqSlot,
    current: &mut v2::SeqSlotState,
    counter: sequences::SeqCounter,
) -> Result<(), CoreError> {
    current.install(counter);
    wake_ack_waiters(slot)
}

fn drain_ready(guard: &mut v2::SeqSlotState) {
    if guard.receiving {
        return;
    }
    while let Some(write) = guard.inflight.front() {
        match write.ack.0.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => break,
            result => {
                let write = guard.inflight.pop_front().unwrap();
                if matches!(result, Ok(Ok(()))) {
                    guard.acknowledge(write.incarnation, write.hw);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn committed_value(
    db: &EngineLease,
    slot: &Arc<v2::SeqSlot>,
    id: u64,
    def: &relational::catalog_ext::SequenceDef,
    operation: &str,
    value: Option<i64>,
    called: bool,
    unsettled: &Unsettled,
) -> Result<i64, CoreError> {
    let key = engine::catalog_codec::counter_key(id, def.incarnation);
    let mut guard = slot.counter.lock_cancellable()?;
    // Re-evaluate after waiting: another allocator may have advanced or wrapped.
    let (mut counter, result, synchronous, prior_hw) = loop {
        drain_ready(&mut guard);
        let in_memory = guard
            .counter
            .as_ref()
            .is_some_and(|c| c.incarnation == def.incarnation);
        if !in_memory {
            if !db
                .catalog()
                .current_local_view()?
                .sequences
                .get(&id)
                .is_some_and(|current| current.incarnation == def.incarnation)
            {
                return Err(CoreError::Sql(SqlError::UnknownTable));
            }
            if guard.receiving || !guard.inflight.is_empty() {
                guard = receive_one(slot, guard)?;
                continue;
            }
            if let Some(bytes) = db.kv.get(Keyspace::Seq, &key)? {
                guard.install(engine::catalog_codec::decode_counter(&key, &bytes, def)?);
            }
        }
        let prior = guard
            .counter
            .as_ref()
            .filter(|c| c.incarnation == def.incarnation)
            .cloned();
        let (result, counter) = match operation {
            "nextval" => sequences::next_on(def, prior.clone()),
            "setval" => sequences::set_on(
                def,
                prior.clone(),
                value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                called,
            ),
            _ => Err(SqlError::Unsupported),
        }
        .map_err(CoreError::Sql)?;
        let wrap = prior.as_ref().is_some_and(|old| {
            def.cycle
                && if def.increment > 0 {
                    counter.last_value < old.last_value
                } else {
                    counter.last_value > old.last_value
                }
        });
        let synchronous = operation == "setval"
            || !in_memory
            || wrap
            || prior.as_ref().is_none_or(|c| !c.is_called);
        if synchronous
            && (guard.receiving || !guard.inflight.is_empty() || guard.waiting_returns != 0)
        {
            guard = if guard.receiving || !guard.inflight.is_empty() {
                receive_one(slot, guard)?
            } else {
                guard.wait()?
            };
            continue;
        }
        break (counter, result, synchronous, prior.map(|c| c.log_hw));
    };
    if synchronous {
        db.sequencer.write_counter(
            key,
            engine::catalog_codec::encode_counter(&counter, def)?,
            256,
        )?;
        guard.install(counter);
        return Ok(result);
    }
    let remaining = (i128::from(counter.log_hw) - i128::from(result)) / i128::from(def.increment);
    if remaining <= 24 {
        let mut high = result;
        for _ in 0..sequences::SEQ_LOG_VALS {
            match high
                .checked_add(def.increment)
                .filter(|v| *v >= def.min && *v <= def.max)
            {
                Some(value) => high = value,
                None => break,
            }
        }
        counter.log_hw = high;
    }
    if prior_hw != Some(counter.log_hw) {
        let ack = db.sequencer.send_counter(
            key.clone(),
            engine::catalog_codec::encode_counter(&counter, def)?,
            256,
        )?;
        guard.inflight.push_back(v2::InFlight {
            incarnation: def.incarnation,
            hw: counter.log_hw,
            ack,
        });
    }
    guard.counter = Some(counter);
    // The reservation precedes a later write commit in the sequencer FIFO.
    // Other outcomes must settle this obligation before returning to the caller.
    if guard.inflight.iter().any(|write| {
        write.incarnation == def.incarnation
            && if def.increment > 0 { write.hw >= result } else { write.hw <= result }
    }) {
        unsettled.lock().map_err(|_| CoreError::Fenced)?.push(Obligation {
            generation: db.stamp(),
            slot: slot.clone(), id, def: def.clone(), value: result,
            coverage_epoch: guard.coverage_epoch,
        });
        return Ok(result);
    }
    if guard.durable_hw.is_some_and(|hw| {
        if def.increment > 0 {
            hw >= result
        } else {
            hw <= result
        }
    }) {
        return Ok(result);
    }
    guard.waiting_returns += 1;
    let outcome = await_coverage(db, slot, guard, id, &key, def, result);
    // Even an error or cancellation releases the backwards-move barrier.
    let mut guard = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
    guard.waiting_returns -= 1;
    outcome
}

#[allow(clippy::too_many_arguments)]
fn await_coverage<'a>(
    db: &v2::V2Db,
    slot: &'a v2::SeqSlot,
    mut guard: SlotGuard<'a>,
    id: u64,
    key: &[u8],
    def: &relational::catalog_ext::SequenceDef,
    result: i64,
) -> Result<i64, CoreError> {
    loop {
        drain_ready(&mut guard);
        if !guard
            .counter
            .as_ref()
            .is_some_and(|c| c.incarnation == def.incarnation)
        {
            if !db
                .catalog()
                .current_local_view()?
                .sequences
                .get(&id)
                .is_some_and(|current| current.incarnation == def.incarnation)
            {
                // A durable RESTART/DROP retired this incarnation before install.
                // Its allocated value can never be reissued by this incarnation.
                return Ok(result);
            }
            // With installs restricted to the committed-current incarnation, this cannot happen.
            debug_assert!(false, "current sequence incarnation missing from slot");
            return Err(CoreError::Fenced);
        }
        if guard.durable_hw.is_some_and(|hw| {
            if def.increment > 0 {
                hw >= result
            } else {
                hw <= result
            }
        }) {
            return Ok(result);
        }
        if !guard.receiving && guard.inflight.is_empty() {
            let counter = guard.counter.as_ref().ok_or(CoreError::Fenced)?;
            let hw = counter.log_hw;
            let ack = match db.sequencer.send_counter(
                key.to_vec(),
                engine::catalog_codec::encode_counter(counter, def)?,
                256,
            ) {
                Ok(ack) => ack,
                Err(engine::txn::TxnError::SequenceGone) => {
                    drop(guard);
                    return await_retirement(db, id, def.incarnation, result);
                }
                Err(error) => return Err(error.into()),
            };
            guard.inflight.push_back(v2::InFlight {
                incarnation: def.incarnation,
                hw,
                ack,
            });
        }
        guard = match receive_one(slot, guard) {
            Ok(guard) => guard,
            Err(ReceiveError::Gone(incarnation)) if incarnation == def.incarnation => {
                return await_retirement(db, id, def.incarnation, result);
            }
            Err(error) => return Err(error.into()),
        };
    }
}

// SequenceGone can precede the DDL's Sync and catalog publication. Release
// the slot so publication can retire it, and remain cancellable until then.
fn await_retirement(
    db: &v2::V2Db,
    id: u64,
    incarnation: u128,
    result: i64,
) -> Result<i64, CoreError> {
    loop {
        crate::cancellation::check()?;
        if !db.catalog().current_local_view()?.sequences.get(&id)
            .is_some_and(|current| current.incarnation == incarnation)
        {
            return Ok(result);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

pub(in crate::storage) fn stage_pending(
    db: &v2::V2Db,
    txn: &mut Transaction,
    pending: &sequences::Pending,
) -> Result<(), CoreError> {
    // No counter writes or catalog read tracking are needed for an empty map.
    // Release this guard before taking catalog locks, as on the nonempty path.
    if pending.lock().map_err(|_| CoreError::Fenced)?.is_empty() {
        return Ok(());
    }
    let parts = db.catalog().parts(txn)?;
    let mut pending = pending.lock().map_err(|_| CoreError::Fenced)?;
    // A savepoint rollback can discard a RESTART's private definition. Exclude
    // its counter from both durable staging and the caller's live publication.
    pending.retain(|id, counter| {
        parts
            .ext
            .sequences
            .get(id)
            .is_some_and(|def| def.incarnation == counter.incarnation)
    });
    for (id, counter) in pending.iter() {
        if let Some(def) = parts.ext.sequences.get(id)
            && def.incarnation == counter.incarnation
        {
            txn.put_counter(
                engine::catalog_codec::counter_key(*id, def.incarnation),
                engine::catalog_codec::encode_counter(counter, def)?,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "v2_sequence_reservation_tests.rs"]
mod reservation_tests;

#[cfg(test)]
#[path = "v2_sequence_restart_tests.rs"]
mod restart_tests;
