//! Transactional table metadata and snapshot-versioned catalog views.
//!
//! Cluster gateways (GD-0075): another gateway's DDL reaches this cache through the
//! catalog-generation record (`refresh_locked`), checked when a statement takes its snapshot.
//! Table locks are local to each gateway; Phase 11 (R5-100, `cluster::txn::ddl`) orders DDL
//! against every other gateway: before a DDL statement chooses its snapshot, each other member
//! takes, as the DDL's proxy, `Table(0)` and the statement's tables AccessExclusive in its own
//! lock manager, and holds them until the DDL has committed or rolled back. DML holds `Table(0)`
//! RowExclusive to transaction end and takes its statement snapshot after that grant, so no
//! transaction writes across a DDL, and every statement after the DDL's commit returned reads
//! the new generation here and reloads. Readers of the DDL's tables are drained the same way.
//! A member that cannot be reached is evicted (fenced) and serves again only after the DDL.
#[cfg(test)]
use super::kv::Kv;
use super::{
    catalog_codec::{self, CatalogKind},
    commit::Sequencer,
    hlc::Ts,
    kv::{Direction, Keyspace, KvError, KvRead},
    locks::{CATALOG_NAME_INDEX, LockKey, LockManager, LockMode},
    mvcc,
    once_guard::CommitGuard,
    seq::SequenceAllocator,
    txn::{Isolation, SerializationReason, Transaction, TxnError, TxnServices},
};
use crate::{
    CoreError,
    relational::{
        self, ColumnDef, SqlError, TableDef,
        catalog_ext::{CatalogExt, ColumnExt, Constraint, LogicalKind, SequenceDef, TypeDef},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Bound, Range},
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::Instant,
};


// Tests exercise reclamation without requiring 1024 follow-up statements.
const CURRENT_PRUNE_INTERVAL: u32 = if cfg!(test) { 1 } else { 1024 };
thread_local! {
    static CURRENT_HITS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct TableEntry {
    pub def: TableDef,
    pub heap: bool,
    #[serde(skip)] // Runtime metadata uses the explicit versioned table DTO.
    pub primary_index_birth: Option<u64>,
    pub columns_ext: BTreeMap<u64, ColumnExt>,
}

#[derive(Debug)]
pub(crate) enum CatalogError {
    Core(CoreError),
    Txn(TxnError),
}
impl From<CoreError> for CatalogError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}
impl From<TxnError> for CatalogError {
    fn from(error: TxnError) -> Self {
        Self::Txn(error)
    }
}
impl From<SqlError> for CatalogError {
    fn from(error: SqlError) -> Self {
        Self::Core(CoreError::Sql(error))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CatalogView {
    pub(crate) sql_views: BTreeMap<u64, catalog_codec::sql_views::SqlViewDef>,
    pub(crate) comments: BTreeMap<(u64, u64), String>,
    /// Kind 21 column labels: (table id, column id) -> flags (GD-0061 R3-307).
    pub(crate) column_labels: BTreeMap<(u64, u64), u8>,
    pub(crate) by_name: Arc<BTreeMap<String, Arc<TableEntry>>>,
    pub(crate) by_id: BTreeMap<u64, Arc<TableEntry>>,
    // The V2 codec's array marker stays available on transaction overlays.
    pub(crate) enum_array_columns: BTreeSet<u64>,
    pub(crate) names: Arc<BTreeMap<String, u64>>,
    pub(crate) constraints: BTreeMap<u64, BTreeMap<u64, Constraint>>,
    pub(crate) sequences: BTreeMap<u64, SequenceDef>,
    pub(crate) types: BTreeMap<u64, TypeDef>,
    pub(crate) added_enum_labels: BTreeMap<u64, relational::catalog_ext::AddedEnumLabels>,
    pub(crate) collations: BTreeMap<u64, relational::collations::Collation>,
    pub(crate) extensions: BTreeMap<String, relational::InstalledExtension>,
    pub(crate) column_collations: BTreeMap<u64, relational::column_collations::Binding>,
    pub(crate) schemas: BTreeMap<String, relational::schemas::Schema>,
    pub(crate) indexes: BTreeMap<u64, relational::secondary_index::Descriptor>,
    index_predicates: Arc<IndexPredicateImage>,
    metadata: BTreeMap<u64, catalog_codec::TableMetadata>,
    /// Task definitions (kind 19), live and revoked. Not part of `parts()`:
    /// SQL DDL rebuilds parts from the relational image, and tasks have
    /// their own writer, like roles.
    pub(crate) tasks: BTreeMap<u64, Arc<crate::task::definition::TaskDef>>,
    /// Credential id -> (task id, persisted verifier record).
    pub(crate) task_credentials: BTreeMap<u64, (u64, Arc<crate::task::credential::CredentialRecord>)>,
    /// Task name -> id, rebuilt from `tasks`; there is no name record.
    pub(crate) task_names: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CatalogParts {
    pub(crate) sql_views: BTreeMap<u64, catalog_codec::sql_views::SqlViewDef>,
    pub(crate) comments: BTreeMap<(u64, u64), String>,
    /// Kind 21 column labels: (table id, column id) -> flags (GD-0061 R3-307).
    /// Outside `ext`: no namespace-5 or CatalogExt change.
    pub(crate) column_labels: BTreeMap<(u64, u64), u8>,
    pub(crate) tables: BTreeMap<String, TableDef>,
    pub(crate) heap: BTreeSet<u64>,
    pub(crate) indexes: Vec<relational::secondary_index::Descriptor>,
    pub(crate) ext: CatalogExt,
}
impl CatalogView {
    fn has_uuid_arrays(&self) -> bool {
        self.by_id.values().any(|entry| {
            entry.def.columns.iter().any(|c| c.data_type == relational::DataType::UuidArray)
        }) || self.sql_views.values().any(|view| {
            view.columns.iter().any(|c| c.storage_type == relational::DataType::UuidArray)
        }) || self.types.values().any(TypeDef::has_uuid_arrays)
    }

    fn has_text_search_types(&self) -> bool {
        self.by_id.values().any(|entry| {
            entry.def.columns.iter().any(|c| c.data_type.is_text_search())
        })
    }

    fn needs_wide_catalog(&self) -> bool {
        relational::catalog_scale::Counts {
            tables: self.by_id.len(),
            schemas: self.schemas.len(),
            sequences: self.sequences.len(),
            types: self.types.len(),
            collations: self.collations.len(),
            // Main's V2 image checked column bindings after removing index
            // bindings. Legacy's serialized map cap includes both.
            column_collations: self.column_collations.values()
                .filter(|binding| !matches!(binding, relational::column_collations::Binding::Index(_)))
                .count(),
            constraint_tables: self.constraints.len(),
            constraints: self.constraints.values().map(BTreeMap::len).sum(),
        }
        .needs_feature(self.indexes.values())
    }

    pub(crate) fn parts(&self) -> CatalogParts {
        let mut parts = CatalogParts {
            sql_views: self.sql_views.clone(),
            comments: self.comments.clone(),
            column_labels: self.column_labels.clone(),
            ..CatalogParts::default()
        };
        for entry in self.by_id.values() {
            let mut def = entry.def.clone();
            let (schema, bare) = relational::split_key(&entry.def.name);
            def.name = bare.into();
            if schema != "public" {
                parts.ext.table_schemas.insert(def.id, schema.into());
            }
            parts.tables.insert(entry.def.name.clone(), def);
            if entry.heap {
                parts.heap.insert(entry.def.id);
            }
            if let Some(birth) = entry.primary_index_birth { parts.ext.primary_index_birth.insert(entry.def.id, birth); }
            parts.ext.columns.extend(entry.columns_ext.clone());
            for column in &entry.def.columns {
                let kind = match column.data_type {
                    relational::DataType::Uuid => LogicalKind::Uuid,
                    relational::DataType::Jsonb => LogicalKind::Jsonb,
                    relational::DataType::IntArray => LogicalKind::IntArray,
                    relational::DataType::BigIntArray => LogicalKind::BigIntArray,
                    relational::DataType::TextArray => LogicalKind::TextArray,
                    relational::DataType::UuidArray => LogicalKind::UuidArray,
                    relational::DataType::TsVector => LogicalKind::TsVector,
                    relational::DataType::TsQuery => LogicalKind::TsQuery,
                    _ => continue,
                };
                parts.ext.logical_kinds.insert(column.id, kind);
            }
        }
        parts.ext.constraints = self
            .constraints
            .iter()
            .map(|(id, cs)| (*id, cs.values().cloned().collect()))
            .collect();
        parts.indexes = self.indexes.values().cloned().collect();
        for table in self.metadata.values() {
            parts.ext.gin_indexes.extend(table.gin.clone());
            parts.ext.generated_columns.extend(table.generated.clone());
        }
        parts.ext.sequences = self.sequences.clone();
        parts.ext.types = self.types.clone();
        parts.ext.added_enum_labels = self.added_enum_labels.clone();
        parts.ext.collations = self.collations.clone();
        parts.ext.extensions = self.extensions.clone();
        parts.ext.column_collations = self.column_collations.clone();
        parts.ext.schemas = self.schemas.clone();
        if parts.ext.constraints.values().flatten().any(|c| matches!(&c.kind, crate::relational::catalog_ext::ConstraintKind::ForeignKey { deferral, .. } if *deferral != crate::relational::catalog_ext::Deferral::NotDeferrable)) {
            parts.ext.ext_version = 2;
        }
        if parts
            .ext
            .columns
            .values()
            .any(|column| column.text_kind.is_some())
        {
            parts.ext.ext_version = 3;
        }
        if !parts.ext.logical_kinds.is_empty() {
            parts.ext.ext_version = 4;
        }
        if !parts.ext.schemas.is_empty() {
            parts.ext.ext_version = 5;
        }
        if !parts.ext.types.is_empty() {
            parts.ext.ext_version = 6;
        }
        parts.ext.ext_version = parts.ext.wire_version();
        parts
    }
}

/// Derived, rows-free metadata for one committed catalog version, or one private
/// overlay. Only committed instances enter CatalogCache; their images are never
/// mutated by a statement. Failed/cancelled construction is not cached.
pub(crate) struct CatalogSnapshot {
    pub(crate) parts: CatalogParts,
    checked_base: Option<Arc<relational::Relational>>,
    image: OnceLock<Arc<relational::Relational>>,
    cursor_image: OnceLock<Arc<relational::Relational>>,
    // Present only for snapshots published in the shared catalog cache.
    pub(crate) cursor_plan_owners: Option<Arc<relational::cursor::PlanOwners>>,
    image_build: Mutex<()>,
    pub(crate) sequence_owners: OnceLock<Arc<crate::storage::SequenceOwners>>,
    allocation_floor: OnceLock<Result<u64, SqlError>>,
    roles: OnceLock<Arc<crate::storage::roles::RoleImage>>,
    roles_build: Mutex<()>,
}
impl CatalogSnapshot {
    fn new(view: &CatalogView, checked: Option<&CheckedCatalogParts>) -> Self {
        let parts = view.parts();
        let checked_base = checked
            .filter(|checked| *checked.parts == parts)
            .map(|checked| checked.base.clone());
        Self {
            parts,
            checked_base,
            image: OnceLock::new(),
            cursor_image: OnceLock::new(),
            cursor_plan_owners: None,
            image_build: Mutex::new(()),
            sequence_owners: OnceLock::new(),
            allocation_floor: OnceLock::new(),
            roles: OnceLock::new(),
            roles_build: Mutex::new(()),
        }
    }

    pub(crate) fn cached_roles(&self) -> Option<Arc<crate::storage::roles::RoleImage>> {
        self.roles.get().cloned()
    }

    pub(crate) fn roles(
        &self,
        load: impl FnOnce()
            -> Result<crate::storage::roles::RoleTable, crate::storage::roles::RoleError>,
    ) -> Result<Arc<crate::storage::roles::RoleImage>, crate::storage::roles::RoleError> {
        if let Some(image) = self.roles.get() {
            return Ok(image.clone());
        }
        let _build = self
            .roles_build
            .lock()
            .map_err(|_| corrupt("role image build mutex poisoned"))?;
        if let Some(image) = self.roles.get() {
            return Ok(image.clone());
        }
        let image = Arc::new(crate::storage::roles::RoleImage::new(load()?));
        let _ = self.roles.set(image.clone());
        Ok(image)
    }

    pub(crate) fn image(&self) -> Result<Arc<relational::Relational>, SqlError> {
        if let Some(image) = self.image.get() {
            return Ok(image.clone());
        }
        let _build = self.image_build.lock().map_err(|_| SqlError::Unsupported)?;
        if let Some(image) = self.image.get() {
            return Ok(image.clone());
        }
        let mut image = if let Some(base) = &self.checked_base {
            base.catalog_image_for_statement(u64::MAX, u64::MAX)
        } else {
            relational::Relational::catalog_image(
                self.parts.tables.clone(),
                self.parts.ext.clone(),
                u64::MAX,
                u64::MAX,
            )?
        };
        image.catalog_indexes(&self.parts.indexes)?;
        image.catalog_views(&self.parts.sql_views)?;
        let image = Arc::new(image);
        let _ = self.image.set(image.clone());
        Ok(image)
    }

    /// Preserve the cursor's existing rows-free image: no in-memory secondary
    /// sidecar and a next-version ceiling of one. Its RowSource owns the index
    /// descriptors independently. This variant is validated once per version.
    pub(crate) fn cursor_image(&self) -> Result<Arc<relational::Relational>, SqlError> {
        if let Some(image) = self.cursor_image.get() {
            return Ok(image.clone());
        }
        let _build = self.image_build.lock().map_err(|_| SqlError::Unsupported)?;
        if let Some(image) = self.cursor_image.get() {
            return Ok(image.clone());
        }
        let mut image = relational::Relational::catalog_image(
            self.parts.tables.clone(),
            self.parts.ext.clone(),
            u64::MAX,
            1,
        )?;
        image.catalog_views(&self.parts.sql_views)?;
        let image = Arc::new(image);
        let _ = self.cursor_image.set(image.clone());
        Ok(image)
    }

    /// The same non-DDL validation ceiling formerly derived at each execution.
    pub(crate) fn allocation_floor(&self) -> Result<u64, SqlError> {
        self.allocation_floor
            .get_or_init(|| {
                self.parts
                    .tables
                    .values()
                    .flat_map(|table| {
                        std::iter::once(table.id)
                            .chain(table.columns.iter().map(|column| column.id))
                    })
                    .chain(self.parts.sql_views.iter().flat_map(|(id, v)| std::iter::once(*id).chain(v.columns.iter().map(|c| c.id))))
                    .chain(self.parts.indexes.iter().map(|index| index.id))
                    .chain(self.parts.ext.gin_indexes.keys().copied())
                    .chain(self.parts.ext.primary_index_birth.values().copied())
                    .chain(self.parts.ext.constraints.values().flatten().map(|c| c.id))
                    .chain(self.parts.ext.sequences.keys().copied())
                    .chain(self.parts.ext.collations.keys().copied())
                    .chain(self.parts.ext.types.iter().map(|(&id, ty)| match ty {
                        TypeDef::Enum { labels, .. } => id + labels.len() as u64
                            - self.parts.ext.added_enum_labels.get(&id).map_or(0, |added| added.0.len()) as u64,
                        _ => id,
                    }))
                    .chain(self.parts.ext.added_enum_labels.values().flat_map(|labels| labels.0.values().copied()))
                    .chain(self.parts.ext.schemas.values().map(|schema| schema.id))
                    .max()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(SqlError::Overflow)
            })
            .clone()
    }
}
pub(crate) struct CatalogCache {
    versions: Vec<(Ts, Arc<CatalogView>)>,
    snapshots: BTreeMap<Ts, Arc<CatalogSnapshot>>,
    /// Cluster gateways only (GD-0075 P7-1): for a cached version, the catalog-generation
    /// token it was loaded with and the newest timestamp at which that token was still current.
    tokens: BTreeMap<Ts, (u128, Ts)>,
    poisoned: bool,
    // Diagnostics only: never persisted or returned in client-visible errors.
    poison_cause: Option<String>,
    poison_reported: std::sync::atomic::AtomicBool,
}
impl CatalogCache {
    pub(crate) fn load(
        kv: &dyn KvRead,
        visible: Ts,
        end: Bound<&[u8]>,
    ) -> Result<Self, CoreError> {
        let mut cache = Self {
            versions: Vec::new(),
            snapshots: BTreeMap::new(),
            tokens: BTreeMap::new(),
            poisoned: false,
            poison_cause: None,
            poison_reported: std::sync::atomic::AtomicBool::new(false),
        };
        cache.publish(visible, load_view_until(kv, visible, end)?, &mut Vec::new());
        Ok(cache)
    }
    pub(crate) fn view_at(&self, snapshot: Ts) -> Option<Arc<CatalogView>> {
        if self.poisoned {
            return None;
        }
        let end = self.versions.partition_point(|(ts, _)| *ts <= snapshot);
        end.checked_sub(1)
            .map(|index| self.versions[index].1.clone())
    }
    pub(crate) fn publish(&mut self, ts: Ts, view: CatalogView, retired: &mut Vec<Arc<CatalogSnapshot>>) {
        if self.poisoned {
            return;
        }
        if let Some(snapshot) = self.snapshots.remove(&ts) {
            retired.push(snapshot);
        }
        self.tokens.remove(&ts);
        match self.versions.binary_search_by_key(&ts, |(ts, _)| *ts) {
            Ok(index) => self.versions[index].1 = Arc::new(view),
            Err(index) => self.versions.insert(index, (ts, Arc::new(view))),
        }
    }
    /// Detach retired snapshots so the caller can drop their plans outside the cache lock.
    fn prune(&mut self, oldest_active: Option<Ts>, visible: Ts, retired: &mut Vec<Arc<CatalogSnapshot>>) {
        if self.poisoned {
            return;
        }
        let horizon = oldest_active.unwrap_or(visible).min(visible);
        let end = self.versions.partition_point(|(ts, _)| *ts <= horizon);
        let Some(anchor) = end.checked_sub(1) else {
            return;
        };
        self.versions.drain(..anchor);
        let first = self.versions[0].0;
        self.tokens.retain(|ts, _| *ts >= first);
        retired.extend(self.snapshots.extract_if(..first, |_, _| true).map(|(_, snapshot)| snapshot));
    }
    fn ensure_live(&self) -> Result<(), TxnError> {
        if self.poisoned {
            if !self.poison_reported.swap(true, std::sync::atomic::Ordering::Relaxed) {
                super::diagnostics::error(format!("catalog fenced: {}", self.poison_cause.as_deref().unwrap_or("unknown poison cause")));
            }
            Err(TxnError::Fenced)
        } else {
            Ok(())
        }
    }
}

/// The two publication atomics needed by SQL cache checks. This observer
/// holds only metadata and can outlive the engine that published it.
pub(crate) struct CatalogVersion {
    visible: super::commit::VisibleClock,
    version: Arc<std::sync::atomic::AtomicU64>,
}
impl CatalogVersion {
    pub(crate) fn read(&self) -> Option<u64> {
        read_current_version(self.visible.read(), &self.version)
    }
}

fn read_current_version(visible: Ts, version: &std::sync::atomic::AtomicU64) -> Option<u64> {
    let version = version.load(std::sync::atomic::Ordering::Acquire);
    (version != 0 && version <= visible.0).then_some(version)
}

struct CheckedCatalogParts {
    parts: Arc<CatalogParts>,
    // The row-free base passed the full post-DDL Relational::validate. The
    // snapshot runs the ordinary index and view checks when its image is used.
    base: Arc<relational::Relational>,
}

pub(super) struct PrivateCatalogSnapshot {
    version: Ts,
    generation: u64,
    snapshot: Arc<CatalogSnapshot>,
}

/// One catalog and allocator are shared by all transactions of an engine.
/// Callers must use transactions from the same sequencer and lock manager.
/// The cache mutex also orders DDL commit, rebuild and publication. It is never
/// held while waiting for a transaction lock.
pub(crate) struct Catalog {
    cache: Mutex<CatalogCache>,
    /// The exact inputs of a previously checked SQL diff. This is an encoding
    /// proof only; it is never used to resolve names or to select a snapshot.
    checked_parts: Mutex<Option<Arc<CheckedCatalogParts>>>,
    // Recovery observes this while holding the cause mutex. Catalog reload
    // records its cause with the cache mutex already held.
    poisoned: std::sync::atomic::AtomicBool,
    current: RwLock<Option<(Ts, Arc<CatalogSnapshot>)>>,
    current_version: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    token_read_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    load_at_error: Mutex<Option<CoreError>>,
    services: TxnServices,
    seq: Arc<SequenceAllocator>,
}
impl Catalog {
    pub(crate) fn open(
        sequencer: Arc<Sequencer>,
        locks: Arc<LockManager>,
        seq: Arc<SequenceAllocator>,
    ) -> Result<Self, CoreError> {
        Self::open_with(
            TxnServices::Local {
                seq: sequencer,
                locks,
            },
            seq,
        )
    }
    /// A catalog over any transaction services; in a cluster it loads through the
    /// routed reader (the catalog lives in the system range).
    pub(crate) fn open_with(
        services: TxnServices,
        seq: Arc<SequenceAllocator>,
    ) -> Result<Self, CoreError> {
        let visible = services.visible_ts()?;
        let mut cache =
            CatalogCache::load(&*services.read_view(visible), visible, catalog_end(&services))?;
        if matches!(services, TxnServices::Cluster(_)) {
            let token = generation_token(&*services.read_view(visible), visible)?;
            cache.tokens.insert(visible, (token, visible));
        }
        Ok(Self {
            cache: Mutex::new(cache),
            checked_parts: Mutex::new(None),
            poisoned: std::sync::atomic::AtomicBool::new(false),
            current: RwLock::new(None),
            current_version: Arc::new(std::sync::atomic::AtomicU64::new(visible.0)),
            #[cfg(test)]
            token_read_hook: Mutex::new(None),
            #[cfg(test)]
            load_at_error: Mutex::new(None),
            services,
            seq,
        })
    }
    // Only the local snapshot paths call this helper. Gateway oracle reads
    // happen before acquiring the cache mutex in with_snapshot_cache/prune.
    fn prune_locked(
        &self,
        cache: &mut CatalogCache,
        retired: &mut Vec<Arc<CatalogSnapshot>>,
    ) -> Result<(), TxnError> {
        // Read visible FIRST: a newly registered snapshot cannot be older than it.
        let visible = self.services.visible_ts()?;
        cache.prune(self.services.oldest_active(), visible, retired);
        Ok(())
    }

    fn check(&self) -> Result<(), TxnError> {
        self.cache
            .lock()
            .map_err(|_| TxnError::Misuse)?
            .ensure_live()?;
        Ok(())
    }

    pub(in crate::storage) fn is_poisoned(&self) -> bool {
        self.poisoned.load(std::sync::atomic::Ordering::Acquire)
    }

    /// A local committed generation, or None during publication or before warming.
    /// Invalidation precedes sequencer submission, just like `current`.
    pub(crate) fn current_version(&self) -> Option<u64> {
        let TxnServices::Local { seq, .. } = &self.services else { return None; };
        // Acquire visibility before reading the generation, as `current_at` does.
        // Observing a committed DDL then also observes its earlier invalidation.
        read_current_version(seq.visible_ts(), &self.current_version)
    }

    pub(crate) fn version_observer(&self) -> Option<CatalogVersion> {
        let TxnServices::Local { seq, .. } = &self.services else { return None; };
        Some(CatalogVersion { visible: seq.visible_clock(), version: Arc::clone(&self.current_version) })
    }

    /// Latest committed catalog observation, independent of transaction snapshots.
    /// Role/admin writes use commit_ddl too. The publication mutex closes the
    /// invalidation window in current_version; gateways read the durable token.
    pub(crate) fn committed_observation(&self) -> Result<u128, CoreError> {
        if matches!(self.services, TxnServices::Local { .. }) {
            let cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
            cache.ensure_live()?;
            return Ok(cache.versions.last().expect("open catalog").0.0 as u128);
        }
        let ts = self.services.visible_ts()?;
        generation_token(&*self.services.read_view(ts), ts)
    }

    fn current_for(&self, txn: &Transaction, ts: Ts) -> Option<(Ts, Arc<CatalogSnapshot>)> {
        if !matches!(self.services, TxnServices::Local { .. })
            || txn.catalog_writes().next().is_some()
            || txn.snapshot_error().is_some()
        {
            return None;
        }
        self.current_at(ts)
    }

    /// A pinned, committed local image without constructing a Transaction. A
    /// caller must retain the pin through all MVCC reads derived from this view.
    /// None asks the caller to use the ordinary snapshot path during publication.
    pub(crate) fn current_local_metadata(
        &self,
    ) -> Result<Option<(Arc<CatalogSnapshot>, super::txn::Pin)>, CoreError> {
        if !matches!(self.services, TxnServices::Local { .. }) {
            return Ok(None);
        }
        let pin = self.services.pin()?;
        Ok(self.current_at(pin.ts()).map(|(_, snapshot)| (snapshot, pin)))
    }

    fn current_at(&self, ts: Ts) -> Option<(Ts, Arc<CatalogSnapshot>)> {
        // DDL clears current before sequencer submission. Its visible Release
        // store and the reader's snapshot Acquire load precede this RwLock read:
        // a snapshot at/after that commit sees None or the newly published view.
        let current = self.current.read().ok()?.as_ref()
            .filter(|(version, _)| ts >= *version).cloned()?;
        // Release the RwLock before trying the cache mutex (DDL takes them in
        // the opposite order). Reclaim after old readers leave, even without DDL;
        // most acquisitions skip pruning, and periodic ones never wait for DDL.
        CURRENT_HITS.with(|hits| {
            let next = hits.get() + 1;
            let reclaim = next == CURRENT_PRUNE_INTERVAL;
            hits.set(if reclaim { 0 } else { next });
            let mut retired = Vec::new();
            if reclaim
                && let Ok(mut cache) = self.cache.try_lock()
                && !cache.poisoned
            {
                let _ = self.prune_locked(&mut cache, &mut retired);
            }
        });
        Some(current)
    }

    pub(crate) fn reserve_ids(&self, n: u64) -> Result<Range<u64>, CoreError> {
        self.seq.reserve_range(0, 0, n)
    }

    pub(crate) fn parts(&self, txn: &mut Transaction) -> Result<CatalogParts, CoreError> {
        Ok(self.snapshot(txn)?.parts.clone())
    }

    /// Fresh local view without allocating/registering a transaction. DDL holds
    /// this mutex across commit and publication, so a visible DDL timestamp
    /// cannot select the previous view. Read the timestamp under the same lock
    /// to avoid racing cache pruning. Gateways must use their routed read path.
    pub(crate) fn current_local_view(&self) -> Result<Arc<CatalogView>, CoreError> {
        self.check().map_err(CoreError::from)?;
        let cache = self.cache.lock().map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        cache.ensure_live().map_err(CoreError::from)?;
        let TxnServices::Local { seq, .. } = &self.services else {
            return Err(corrupt("local catalog view requested on gateway"));
        };
        cache.view_at(seq.visible_ts()).ok_or_else(|| corrupt("missing current catalog view"))
    }

    /// The committed catalog view at `txn`'s snapshot (task catalog reads,
    /// GD-0061). Ignores the transaction's own uncommitted catalog writes.
    pub(crate) fn committed_view(&self, txn: &mut Transaction) -> Result<Arc<CatalogView>, CoreError> {
        self.check().map_err(CoreError::from)?;
        let cache = self
            .cache
            .lock()
            .map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        cache.ensure_live().map_err(CoreError::from)?;
        let snapshot = txn.snapshot_ts();
        if let Some(error) = txn.snapshot_error() {
            return Err(error.clone().into());
        }
        match cache.view_at(snapshot) {
            Some(view) => Ok(view),
            None => Ok(Arc::new(load_view_until(
                &*self.services.read_view(snapshot),
                snapshot,
                catalog_end(&self.services),
            )?)),
        }
    }

    pub(crate) fn snapshot(
        &self,
        txn: &mut Transaction,
    ) -> Result<Arc<CatalogSnapshot>, CoreError> {
        self.snapshot_with_catalog_writes(txn, None)
    }

    /// Read one committed snapshot with an optional session's private DDL overlay.
    pub(crate) fn snapshot_with_catalog_writes(
        &self,
        txn: &mut Transaction,
        private: Option<&Transaction>,
    ) -> Result<Arc<CatalogSnapshot>, CoreError> {
        let ts = txn.snapshot_ts();
        if let Some(error) = txn.snapshot_error() {
            return Err(error.clone().into());
        }
        let writes = private.unwrap_or(txn);
        if let Some((_, snapshot)) = self.current_for(writes, ts) {
            return Ok(snapshot);
        }
        self.check().map_err(CoreError::from)?;
        self.with_snapshot_cache(ts, |cache| self.snapshot_locked(cache, writes, ts))
    }

    /// Does not refresh or assign a transaction snapshot; metadata preserves the
    /// existing fallback to visible_ts when no snapshot has been chosen yet.
    pub(crate) fn metadata_snapshot(
        &self,
        txn: &Transaction,
    ) -> Result<Arc<CatalogSnapshot>, CoreError> {
        // Pin the existing visible-timestamp fallback only for this acquisition.
        // Do not assign a transaction snapshot: metadata must still leave an
        // unselected transaction unselected. The pin prevents pruning/GC from
        // passing the chosen timestamp while this reader waits for the mutex.
        let pin;
        let ts = match txn.current_snapshot_ts() {
            Some(ts) => ts,
            None => {
                pin = self.services.pin()?;
                pin.ts()
            }
        };
        if let Some((_, snapshot)) = self.current_for(txn, ts) {
            return Ok(snapshot);
        }
        self.check().map_err(CoreError::from)?;
        self.with_snapshot_cache(ts, |cache| self.snapshot_locked(cache, txn, ts))
    }

    /// The committed catalog version `txn`'s statement snapshot reads, as a cache
    /// key for derived per-version data (GD-0061 R3-305 task predicates). `None`
    /// for a private catalog overlay, before a snapshot is chosen, or for the
    /// old-snapshot fallback.
    pub(crate) fn snapshot_version(&self, txn: &Transaction) -> Option<Ts> {
        if txn.catalog_writes().next().is_some() {
            return None;
        }
        let ts = txn.current_snapshot_ts()?;
        if let Some((version, _)) = self.current_for(txn, ts) {
            return Some(version);
        }
        let cache = self.cache.lock().ok()?;
        let end = cache.versions.partition_point(|(version, _)| *version <= ts);
        end.checked_sub(1).map(|index| cache.versions[index].0)
    }

    /// Acquire the served version and image under the same cache lock.
    /// None marks the old-snapshot fallback, whose version is unknown.
    pub(crate) fn binding_snapshot(
        &self,
        txn: &Transaction,
    ) -> Result<(Arc<CatalogSnapshot>, Option<Ts>), CoreError> {
        let pin;
        let ts = match txn.current_snapshot_ts() {
            Some(ts) => ts,
            None => {
                pin = self.services.pin()?;
                pin.ts()
            }
        };
        if let Some((version, snapshot)) = self.current_for(txn, ts) {
            return Ok((snapshot, Some(version)));
        }
        self.check().map_err(CoreError::from)?;
        self.with_snapshot_cache(ts, |cache| {
            let snapshot = self.snapshot_locked(cache, txn, ts)?;
            let end = cache
                .versions
                .partition_point(|(version, _)| *version <= ts);
            let version = end.checked_sub(1).map(|index| cache.versions[index].0);
            Ok((snapshot, version))
        })
    }

    fn with_snapshot_cache<T>(
        &self,
        ts: Ts,
        read: impl FnOnce(&mut CatalogCache) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        let cluster = if matches!(self.services, TxnServices::Local { .. }) {
            None
        } else {
            // Read visible FIRST: a newly registered snapshot cannot be older than it.
            // Neither the oracle nor the token range read may hold the cache mutex.
            let visible = self.services.visible_ts()?;
            let oldest = self.services.oldest_active();
            let token = self.refresh_token(ts)?;
            Some((oldest, visible, token))
        };
        // Declare retired snapshots before the guard so the guard drops first,
        // including on errors and unwinding.
        let mut retired = Vec::new();
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        cache.ensure_live()?;
        if let Some((oldest, visible, token)) = cluster {
            cache.prune(oldest, visible, &mut retired);
            self.refresh_locked(&mut cache, ts, token, &mut retired)?;
        } else {
            self.prune_locked(&mut cache, &mut retired)?;
        }
        read(&mut cache)
    }

    fn snapshot_locked(
        &self,
        cache: &mut CatalogCache,
        txn: &Transaction,
        ts: Ts,
    ) -> Result<Arc<CatalogSnapshot>, CoreError> {
        cache.ensure_live().map_err(CoreError::from)?;
        let end = cache
            .versions
            .partition_point(|(version, _)| *version <= ts);
        let Some(index) = end.checked_sub(1) else {
            // Preserve the existing old-snapshot fallback. Do not invent a
            // committed catalog-version key for a view loaded at a data timestamp.
            let view = self.load_at(ts)?;
            return Ok(Arc::new(CatalogSnapshot::new(&overlay_view(
                &view,
                txn.catalog_writes(),
            )?, None)));
        };
        let (version, view) = &cache.versions[index];
        if txn.catalog_writes().next().is_some() {
            if matches!(self.services, TxnServices::Local { .. }) {
                let generation = txn.catalog_generation();
                let mut private = txn.catalog_overlay.lock().map_err(|_| TxnError::Misuse)?;
                if let Some(cached) = private.as_ref()
                    && cached.version == *version && cached.generation == generation
                {
                    return Ok(cached.snapshot.clone());
                }
                let view = overlay_view(view, txn.catalog_writes())?;
                let snapshot = self.make_snapshot(&view);
                *private = Some(PrivateCatalogSnapshot {
                    version: *version,
                    generation,
                    snapshot: snapshot.clone(),
                });
                return Ok(snapshot);
            }
            return Ok(Arc::new(CatalogSnapshot::new(&overlay_view(
                view,
                txn.catalog_writes(),
            )?, None)));
        }
        let version = *version;
        let snapshot = cache.snapshots.entry(version)
            .or_insert_with(|| {
                // Shared snapshots own the cursor binding registry; private overlays do not.
                let checked = self.checked_parts.lock().ok().and_then(|proof| proof.clone());
                Arc::new(CatalogSnapshot {
                    cursor_plan_owners: Some(Arc::default()),
                    ..CatalogSnapshot::new(view, checked.as_deref())
                })
            }).clone();
        if matches!(self.services, TxnServices::Local { .. })
            && cache.versions.last().is_some_and(|(latest, _)| *latest == version)
        {
            *self.current.write().map_err(|_| corrupt("catalog current lock poisoned"))? =
                Some((version, snapshot.clone()));
            self.current_version.store(version.0, std::sync::atomic::Ordering::Release);
        }
        Ok(snapshot)
    }
    fn make_snapshot(&self, view: &CatalogView) -> Arc<CatalogSnapshot> {
        let checked = self.checked_parts.lock().ok().and_then(|proof| proof.clone());
        Arc::new(CatalogSnapshot::new(view, checked.as_deref()))
    }

    /// Cluster gateways (GD-0075 P7-1): DDL committed through another gateway reaches this
    /// cache only here. When the catalog-generation token at `ts` differs from the one the
    /// version serving `ts` was loaded with, the catalog is reloaded at `ts` and published there.
    /// The cross-gateway DDL/DML limit this leaves is in the module doc.
    fn refresh_token(&self, ts: Ts) -> Result<Option<u128>, CoreError> {
        if !matches!(self.services, TxnServices::Cluster(_)) {
            return Ok(None);
        }
        {
            let cache = self
                .cache
                .lock()
                .map_err(|_| corrupt("catalog cache mutex poisoned"))?;
            cache.ensure_live()?;
            let end = cache.versions.partition_point(|(version, _)| *version <= ts);
            let Some(index) = end.checked_sub(1) else {
                return Ok(None);
            };
            if cache
                .tokens
                .get(&cache.versions[index].0)
                .is_some_and(|(_, checked)| *checked >= ts)
            {
                return Ok(None);
            }
        }
        #[cfg(test)]
        if let Some(hook) = self.token_read_hook.lock().unwrap().as_ref() {
            hook();
        }
        generation_token(&*self.services.read_view(ts), ts).map(Some)
    }

    fn refresh_locked(
        &self,
        cache: &mut CatalogCache,
        ts: Ts,
        token: Option<u128>,
        retired: &mut Vec<Arc<CatalogSnapshot>>,
    ) -> Result<(), CoreError> {
        let Some(token) = token else {
            return Ok(());
        };
        // Re-find the served version: another statement may have published or
        // pruned while this statement read the token. Tokens never repeat.
        let end = cache.versions.partition_point(|(version, _)| *version <= ts);
        let Some(index) = end.checked_sub(1) else {
            return Ok(());
        };
        let version = cache.versions[index].0;
        if cache.tokens.get(&version).is_some_and(|(_, checked)| *checked >= ts) {
            return Ok(());
        }
        match cache.tokens.get_mut(&version) {
            Some((known, checked)) if *known == token => *checked = (*checked).max(ts),
            _ => self.publish_loaded(cache, ts, retired)?,
        }
        Ok(())
    }

    /// Cluster gateways (Phase 11): checks the generation token at a fresh oracle timestamp and
    /// reloads the catalog there when another gateway's DDL changed it.
    pub(crate) fn refresh_latest(&self) -> Result<(), CoreError> {
        let now = self.services.visible_ts()?;
        let token = self.refresh_token(now)?;
        let mut retired = Vec::new();
        let mut cache = self.cache.lock().map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        cache.ensure_live()?;
        self.refresh_locked(&mut cache, now, token, &mut retired)
    }

    /// The committed catalog at `ts` (bounded below the cluster records on a gateway).
    fn load_at(&self, ts: Ts) -> Result<CatalogView, CoreError> {
        #[cfg(test)]
        if let Some(error) = self.load_at_error.lock().unwrap().take() {
            return Err(error);
        }
        load_view_until(&*self.services.read_view(ts), ts, catalog_end(&self.services))
    }

    /// Loads the catalog at `ts` and publishes it, with its generation token on a gateway.
    fn publish_loaded(&self, cache: &mut CatalogCache, ts: Ts, retired: &mut Vec<Arc<CatalogSnapshot>>) -> Result<(), CoreError> {
        let kv = self.services.read_view(ts);
        let view = load_view_until(&*kv, ts, catalog_end(&self.services))?;
        if matches!(self.services, TxnServices::Local { .. }) {
            self.current_version.store(0, std::sync::atomic::Ordering::Release);
            *self.current.write().map_err(|_| corrupt("catalog current lock poisoned"))? = None;
        }
        cache.publish(ts, view, retired);
        if matches!(self.services, TxnServices::Local { .. }) && !cache.poisoned {
            self.current_version.store(ts.0, std::sync::atomic::Ordering::Release);
        }
        if matches!(self.services, TxnServices::Cluster(_)) && !cache.poisoned {
            cache.tokens.insert(ts, (generation_token(&*kv, ts)?, ts));
        }
        Ok(())
    }

    /// Stages an SQL catalog diff in `txn`.
    ///
    /// The caller must hold `Table(0)` AccessExclusive through commit for any
    /// sequence record change. This keeps committed sequence incarnations
    /// stable while an INSERT reuses its resolution of a column default.
    /// Debug builds assert this here; sequence-record writes that bypass
    /// `apply_diff` are not checked and must follow the same rule.
    pub(crate) fn apply_diff(
        &self,
        txn: &mut Transaction,
        old: &CatalogParts,
        new: &CatalogParts,
        ids: &Range<u64>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), CoreError> {
        self.apply_diff_inner(txn, old, new, ids, deadline, cancel, false)
    }

    pub(crate) fn apply_sql_diff(
        &self,
        txn: &mut Transaction,
        old: &CatalogParts,
        new: Arc<CatalogParts>,
        validated_base: relational::Relational,
        ids: &Range<u64>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), CoreError> {
        let checked = self.checked_parts.lock().map_err(|_| TxnError::Misuse)?.clone();
        let reuse = checked.as_ref().is_some_and(|checked| checked.parts.as_ref() == old);
        self.apply_diff_inner(txn, old, &new, ids, None, cancel, reuse)?;
        *self.checked_parts.lock().map_err(|_| TxnError::Misuse)? = Some(Arc::new(CheckedCatalogParts {
            parts: new,
            base: Arc::new(validated_base),
        }));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_diff_inner(
        &self,
        txn: &mut Transaction,
        old: &CatalogParts,
        new: &CatalogParts,
        ids: &Range<u64>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
        reuse: bool,
    ) -> Result<(), CoreError> {
        self.check().map_err(CoreError::from)?;
        let skip = if reuse {
            let own = txn.catalog_writes().map(|(key, _)| key.as_slice()).collect();
            UnchangedRecords::between(old, new, &own)
        } else {
            UnchangedRecords::default()
        };
        let (old_records, old_ids) = encode_parts(old, &skip, reuse)?;
        let (new_records, new_ids) = encode_parts(new, &skip, false)?;
        for (id, kind) in &new_ids {
            match old_ids.get(id) {
                Some(old_kind) if old_kind != kind => {
                    return Err(corrupt("catalog object id changed kind or owner"));
                }
                None if !ids.contains(id) => {
                    return Err(corrupt("catalog object id outside reservation"));
                }
                _ => {}
            }
        }
        // Column names are mutable and may be reused by DROP + ADD in one
        // ALTER. The object-ID checks above preserve ownership and require a
        // reserved ID for every newly added column.
        for (name, table) in &new.tables {
            if let Some(previous) = old.tables.get(name)
                && previous.id != table.id
            {
                return Err(corrupt("existing table id changed"));
            }
        }
        if old_records == new_records {
            return Ok(());
        }
        super::super::roles::engine::reconcile_tables(
            txn,
            &*self.services.latest_view()?,
            old,
            new,
            deadline,
            cancel,
        )?;
        crate::task::catalog::check_table_ddl(
            txn,
            &*self.services.latest_view()?,
            old,
            new,
            deadline,
            cancel,
        )?;
        for (key, value) in &new_records {
            if old_records.get(key) != Some(value) || txn.catalog_own(key).is_some() {
                let bits = super::content_check::required_bits_for_catalog_value(key, value)
                    .map_err(|e| corrupt(&e.to_string()))?;
                // Admit these features from the final catalog overlay.
                txn.require_features(bits & !(super::meta::UUID_ARRAY_COLUMNS
                    | super::meta::GENERATED_COLUMNS | super::meta::TEXT_SEARCH_TYPES
                    | super::meta::GIN_INDEXES | super::meta::PARTIAL_INDEXES));
            }
        }
        self.lock_and_write(txn, &old_records, &new_records, deadline, cancel)?;
        let partial = new.indexes.iter().any(|index| index.predicate.is_some());
        let temporal = relational::secondary_index::has_temporal_indexes(
            &new.indexes, new.tables.values(),
        );
        txn.set_secondary_index_features(partial, temporal);
        if !new.indexes.is_empty() {
            txn.require_features(super::meta::SECONDARY_INDEXES);
        }
        if new.column_labels != old.column_labels {
            // Labels (and their tombstones) need TASK_CATALOG, which needs role_catalog.
            txn.require_features(super::meta::TASK_CATALOG | super::super::roles::engine::FEATURE);
        }
        if !new.ext.namespace5_ext_is_empty() {
            txn.require_features(1 << 6);
        }
        if new.ext.columns.values().any(|c| c.typmod.is_some()) {
            txn.require_features(1 << 7);
        }
        if new
            .tables
            .values()
            .any(catalog_codec::requires_text_backed_types)
        {
            txn.require_features(super::meta::TEXT_BACKED_COLUMN_TYPES);
        }
        if !new.ext.schemas.is_empty() || !new.ext.types.is_empty() {
            txn.require_features(super::meta::SCHEMAS_AND_USER_TYPES);
        }
        for (id, def) in &old.ext.sequences {
            if !new.ext.sequences.contains_key(id) {
                txn.delete_counter(catalog_codec::counter_key(*id, def.incarnation))
                    .map_err(CoreError::from)?;
            }
        }
        // TODO: reclaim dropped table rows with a snapshot-safe GC sweep.
        Ok(())
    }

    fn lock_and_write(
        &self,
        txn: &mut Transaction,
        old_records: &BTreeMap<Vec<u8>, Vec<u8>>,
        new_records: &BTreeMap<Vec<u8>, Vec<u8>>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        let changes: BTreeMap<_, _> = old_records
            .keys()
            .chain(new_records.keys())
            .filter_map(|key| {
                if old_records.get(key) == new_records.get(key) {
                    None
                } else {
                    Some((key.clone(), new_records.get(key).cloned()))
                }
            })
            .collect();
        if changes.is_empty() {
            return Ok(());
        }
        #[cfg(debug_assertions)]
        if changes.keys().any(|key| {
            matches!(catalog_codec::classify_catalog_key(key), Ok(CatalogKind::Sequence))
        }) {
            let locks = match txn.services() {
                TxnServices::Local { locks, .. } => locks.as_ref(),
                TxnServices::Cluster(cluster) => cluster.gateway_locks(),
            };
            debug_assert!(
                locks.holds_mode(txn.id(), LockKey::Table(0), LockMode::AccessExclusive),
                "sequence catalog changes require Table(0) AccessExclusive"
            );
        }
        let mut tables = BTreeSet::new();
        for key in changes.keys() {
            match key[0] {
                1 | 4 | 17 | 18 => {
                    tables.insert(decode_id(&key[1..9]).map_err(commit_error)?);
                }
                16 => {
                    if let Some(value) = new_records.get(key).or_else(|| old_records.get(key)) {
                        let descriptor =
                            super::indexes::decode_descriptor(value).map_err(commit_error)?;
                        tables.insert(descriptor.table);
                    }
                }
                3 => {
                    let name = std::str::from_utf8(&key[1..key.len() - 1])
                        .map_err(|_| commit_error(corrupt("invalid catalog name")))?;
                    self.lock_name(txn, name, deadline, cancel)?;
                }
                _ => {}
            }
        }
        // Phase 11: in a cluster the other gateways drain these tables' readers too (their
        // writers are already excluded by the DDL's proxies' `Table(0)`).
        if !tables.is_empty() {
            txn.ddl_close(&tables.iter().copied().collect::<Vec<_>>(), cancel)?;
        }
        for id in tables {
            txn.acquire_lock(
                LockKey::Table(id),
                LockMode::AccessExclusive,
                deadline,
                cancel,
            )?;
        }
        self.check()?;
        // The caller's snapshot may predate a competing DDL commit. Validate
        // after all locks, before buffering any part of this change.
        for key in changes.keys() {
            let current = match txn.catalog_own(key) {
                Some(value) => value.clone(),
                None => txn.catalog_latest_committed(key)?,
            };
            if current.as_ref() != old_records.get(key) {
                return Err(TxnError::Serialization(SerializationReason::WriteConflict));
            }
        }
        for (key, value) in changes {
            self.write_record(txn, key, value)?;
        }
        Ok(())
    }

    fn write_record(
        &self,
        txn: &mut Transaction,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(), TxnError> {
        match value {
            Some(value) => txn.put(Keyspace::Catalog, key, value),
            None => txn.delete(Keyspace::Catalog, key),
        }
    }

    pub(crate) fn create_table(
        &self,
        txn: &mut Transaction,
        mut def: TableDef,
        heap: bool,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Arc<TableEntry>, CatalogError> {
        self.check()?;
        validate_definition(&def)?;
        self.lock_name(txn, &def.name, deadline, cancel)?;
        self.check()?;
        let name_key_bytes = name_key(&def.name);
        let existing = match txn.catalog_own(&name_key_bytes) {
            Some(value) => value.clone(),
            None => txn.catalog_latest_committed(&name_key_bytes)?,
        };
        if existing.is_some() {
            return Err(SqlError::Duplicate.into());
        }
        def.id = self.seq.next(0, 0)?;
        for column in &mut def.columns {
            column.id = self.seq.next(0, 0)?;
        }
        let entry = Arc::new(TableEntry {
            primary_index_birth: None,
            def,
            heap,
            columns_ext: BTreeMap::new(),
        });
        let records = BTreeMap::from([
            (table_key(entry.def.id), encode_entry(&entry)?),
            (
                name_key(&entry.def.name),
                entry.def.id.to_be_bytes().to_vec(),
            ),
        ]);
        let old = self.parts(txn)?;
        let mut new = old.clone();
        new.tables.insert(entry.def.name.clone(), entry.def.clone());
        super::super::roles::engine::reconcile_tables(
            txn,
            &*self.services.latest_view()?,
            &old,
            &new,
            deadline,
            cancel,
        )?;
        self.lock_and_write(txn, &BTreeMap::new(), &records, deadline, cancel)?;
        Ok(entry)
    }

    pub(crate) fn resolve(
        &self,
        txn: &mut Transaction,
        name: &str,
        mode: LockMode,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Arc<TableEntry>, CatalogError> {
        self.resolve_inner::<false>(txn, name, mode, deadline, cancel)
            .map(|entry| entry.expect("ordinary resolution retries drift"))
    }

    /// Bind must re-authorize before resolving a changed catalog image.
    /// None returns control to its bounded authorization loop without retrying.
    pub(crate) fn resolve_for_bind(
        &self,
        txn: &mut Transaction,
        name: &str,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<Arc<TableEntry>>, CatalogError> {
        self.resolve_inner::<true>(txn, name, LockMode::AccessShare, None, cancel)
    }

    fn resolve_inner<const RETURN_ON_DRIFT: bool>(
        &self,
        txn: &mut Transaction,
        name: &str,
        mode: LockMode,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<Arc<TableEntry>>, CatalogError> {
        self.check()?;
        validate_identifier(relational::split_key(name).1)?;
        if !mode.is_table() {
            return Err(TxnError::Misuse.into());
        }
        for attempt in 0..3 {
            let snapshot = txn.snapshot_ts();
            let token = self.refresh_token(snapshot)?;
            let view = {
                let mut retired = Vec::new();
                let mut cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
                cache.ensure_live()?;
                self.refresh_locked(&mut cache, snapshot, token, &mut retired)?;
                match cache.view_at(snapshot) {
                    Some(view) => view,
                    None => {
                        self.publish_loaded(&mut cache, snapshot, &mut retired)?;
                        cache.view_at(snapshot).unwrap()
                    }
                }
            };
            let (base, entry) = {
                let base = view.by_name.get(name).cloned();
                let name_key = name_key(name);
                if txn.isolation() == Isolation::Serializable {
                    txn.get(Keyspace::Catalog, &name_key)?;
                }
                let id = match txn.catalog_own(&name_key) {
                    Some(Some(value)) => decode_id(value)?,
                    Some(None) => return Err(SqlError::UnknownTable.into()),
                    None => base.as_ref().ok_or(SqlError::UnknownTable)?.def.id,
                };
                let table_key = table_key(id);
                if txn.isolation() == Isolation::Serializable {
                    txn.get(Keyspace::Catalog, &table_key)?;
                }
                let entry = match txn.catalog_own(&table_key) {
                    Some(Some(value)) => Arc::new(decode_entry(value)?),
                    Some(None) => return Err(SqlError::UnknownTable.into()),
                    None => view
                        .by_id
                        .get(&id)
                        .cloned()
                        .ok_or_else(|| corrupt("catalog name references absent table"))?,
                };
                if entry.def.id != id || entry.def.name != name {
                    return Err(corrupt("catalog name/table mismatch").into());
                }
                (base, entry)
            };
            txn.acquire_lock(LockKey::Table(entry.def.id), mode, deadline, cancel)?;
            let now = if txn.isolation() != Isolation::ReadCommitted
                && matches!(self.services, TxnServices::Cluster(_))
            {
                // A gateway's cache is only as new as its last refresh: compare a fixed-snapshot
                // transaction against the catalog at a fresh oracle timestamp, so a table changed
                // through another gateway fails it with 40001 as on one node (Phase 11).
                Some(txn.catalog_check_ts()?)
            } else {
                None
            };
            let token = now.map(|now| self.refresh_token(now)).transpose()?.flatten();
            let mut retired = Vec::new();
            let mut cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
            cache.ensure_live()?;
            if let Some(now) = now {
                self.refresh_locked(&mut cache, now, token, &mut retired)?;
            }
            let latest = cache.versions.last().unwrap().1.by_name.get(name);
            if latest == base.as_ref() {
                return Ok(Some(entry));
            }
            if RETURN_ON_DRIFT {
                drop(cache);
                if txn.isolation() == Isolation::ReadCommitted { txn.begin_statement(); }
                return Ok(None);
            }
            if txn.isolation() != Isolation::ReadCommitted || attempt == 2 {
                return Err(TxnError::Serialization(SerializationReason::WriteConflict).into());
            }
            drop(cache);
            txn.begin_statement();
        }
        unreachable!()
    }

    pub(crate) fn drop_table(
        &self,
        txn: &mut Transaction,
        name: &str,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), CatalogError> {
        self.check()?;
        validate_identifier(relational::split_key(name).1)?;
        self.lock_name(txn, name, deadline, cancel)?;
        let entry = self.resolve(txn, name, LockMode::AccessExclusive, deadline, cancel)?;
        let old = self.parts(txn)?;
        let mut new = old.clone();
        new.tables.remove(name);
        new.comments.retain(|(table, _), _| *table != entry.def.id);
        new.column_labels.retain(|(table, _), _| *table != entry.def.id);
        new.heap.remove(&entry.def.id);
        new.indexes.retain(|d| d.table != entry.def.id);
        new.ext.gin_indexes.retain(|_, index| index.table != entry.def.id);
        for column in &entry.def.columns {
            new.ext.columns.remove(&column.id);
            new.ext.generated_columns.remove(&column.id);
        }
        new.ext.constraints.remove(&entry.def.id);
        new.ext.primary_index_birth.remove(&entry.def.id);
        new.ext.sequences.retain(|_, sequence| {
            sequence
                .owned_by
                .is_none_or(|(table, _)| table != entry.def.id)
        });
        self.apply_diff(txn, &old, &new, &(0..0), deadline, cancel)?;
        Ok(())
    }

    pub(crate) fn add_column(
        &self,
        txn: &mut Transaction,
        name: &str,
        mut column: ColumnDef,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Arc<TableEntry>, CatalogError> {
        let current = self.resolve(txn, name, LockMode::AccessExclusive, deadline, cancel)?;
        relational::column_ident(&column.name)?;
        if current
            .def
            .columns
            .iter()
            .any(|existing| existing.name == column.name)
        {
            return Err(SqlError::Duplicate.into());
        }
        if current.def.columns.len() >= relational::MAX_COLUMNS {
            return Err(SqlError::Limit.into());
        }
        column.id = self.seq.next(0, 0)?;
        let mut entry = (*current).clone();
        entry.def.columns.push(column);
        let bytes = if catalog_codec::decoded_enum_array_columns(&entry.def, &entry.columns_ext)
            .next().is_some()
        {
            let parts = self.parts(txn)?;
            catalog_codec::encode_table_with_types(&entry, &entry.columns_ext, &parts.ext.types)?
        } else {
            encode_entry(&entry)?
        };
        txn.put(Keyspace::Catalog, table_key(entry.def.id), bytes)?;
        Ok(Arc::new(entry))
    }

    fn lock_name(
        &self,
        txn: &Transaction,
        name: &str,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        txn.acquire_lock(
            LockKey::Unique {
                index: CATALOG_NAME_INDEX,
                tuple: name.as_bytes().to_vec(),
            },
            LockMode::Exclusive,
            deadline,
            cancel,
        )
    }

    /// Retain the transaction (and therefore its locks) through cache publication.
    pub(crate) fn commit_ddl(&self, txn: Transaction) -> Result<Ts, TxnError> {
        self.commit_ddl_inner(txn, None, None, false)
    }
    pub(crate) fn commit_sql_ddl(&self, txn: Transaction, release_admission: Option<&mut dyn FnMut()>) -> Result<Ts, TxnError> {
        self.commit_ddl_inner(txn, None, release_admission, true)
    }
    pub(crate) fn commit_ddl_guarded(&self, txn: Transaction, guard: CommitGuard) -> Result<Ts, TxnError> {
        self.commit_ddl_inner(txn, Some(guard), None, false)
    }
    fn commit_ddl_inner(
        &self,
        mut txn: Transaction,
        guard: Option<CommitGuard>,
        release_admission: Option<&mut dyn FnMut()>,
        sql: bool,
    ) -> Result<Ts, TxnError> {
        fn finish(txn: Transaction, guard: Option<CommitGuard>, publish: impl FnOnce(Ts) -> Result<(), TxnError>) -> Result<Ts, TxnError> {
            match guard { Some(guard) => txn.commit_guarded(guard, publish), None => txn.commit_then(publish) }
        }
        self.check()?;
        // Data-only and read-only commits cannot change any cached catalog
        // version. Do not rebuild or republish an identical view, and do not
        // hold the publication mutex while waiting for their durable commit.
        // DDL still holds it across sequencer success and publication below:
        // a reader at the newly visible timestamp must never use the old view.
        if txn.catalog_writes().next().is_none() {
            if let Some(release_admission) = release_admission {
                return txn.commit_then_with_tail(|_| Ok(()), Some(release_admission));
            }
            return finish(txn, guard, |_| Ok(()));
        }
        let mut retired = Vec::new();
        let mut cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
        cache.ensure_live()?;
        let latest = &cache.versions.last().unwrap().1;
        // A DROP may have waited behind COMMENT without a table-record change.
        // A newly committed comment absent from its tombstones makes it stale.
        for (key, value) in txn.catalog_writes() {
            if key.first() == Some(&18) && value.is_some() {
                let id = decode_id(&key[1..9]).map_err(commit_error)?;
                let column = u64::from_be_bytes(key[9..17].try_into().unwrap());
                if txn.catalog_own(&table_key(id)).is_none() && txn.catalog_own(&catalog_codec::sql_views::view_key(id)).is_none()
                    && catalog_codec::validate_comments(&BTreeMap::from([((id, column), String::new())]), latest.by_id.values().map(|e| &e.def), &latest.sql_views).is_err() {
                    return Err(TxnError::Serialization(SerializationReason::WriteConflict));
                }
            }
            // A label racing a DROP of its table or column is stale, like a comment.
            if key.first() == Some(&super::catalog_label_codec::LABEL_KIND) && value.is_some() {
                let (id, column) = super::catalog_label_codec::decode_label_key(key).map_err(commit_error)?;
                if txn.catalog_own(&table_key(id)).is_none()
                    && !latest.by_id.get(&id).is_some_and(|e| e.def.columns.iter().any(|c| c.id == column)) {
                    return Err(TxnError::Serialization(SerializationReason::WriteConflict));
                }
            }
            if key.first() == Some(&1) {
                let id = decode_id(&key[1..9]).map_err(commit_error)?;
                let retained: BTreeSet<u64> = match value {
                    None => BTreeSet::new(),
                    Some(v) => decode_entry(v).map_err(commit_error)?.def.columns.iter().map(|c| c.id).collect(),
                };
                for &(table, column) in latest.column_labels.keys().filter(|(table, _)| *table == id) {
                    if !retained.contains(&column)
                        && !matches!(txn.catalog_own(&super::catalog_label_codec::label_key(table, column)), Some(None)) {
                        return Err(TxnError::Serialization(SerializationReason::WriteConflict));
                    }
                }
            }
            if matches!(key.first(), Some(1 | 17)) {
                let id = decode_id(&key[1..9]).map_err(commit_error)?;
                let retained: BTreeSet<_> = match (key[0], value) {
                    (_, None) => BTreeSet::new(),
                    (17, Some(v)) => catalog_codec::sql_views::decode_view(v).map_err(commit_error)?.columns.iter().map(|c| c.id).chain([0]).collect(),
                    (_, Some(v)) => decode_entry(v).map_err(commit_error)?.def.columns.iter().map(|c| c.id).chain([0]).collect(),
                };
                for &(table, column) in latest.comments.keys().filter(|(table, _)| *table == id) {
                    if !retained.contains(&column) && !matches!(txn.catalog_own(&catalog_codec::comment_key(table, column)), Some(None)) {
                        return Err(TxnError::Serialization(SerializationReason::WriteConflict));
                    }
                }
            }
        }
        // Reject limit/corruption before committing anything durably.
        let final_view = overlay_view(latest, txn.catalog_writes()).map_err(commit_error)?;
        if final_view.needs_wide_catalog() {
            txn.require_features(super::meta::WIDE_CATALOG);
        }
        if final_view.has_uuid_arrays() {
            txn.require_features(super::meta::UUID_ARRAY_COLUMNS);
        }
        txn.set_secondary_index_features(
            !final_view.index_predicates.predicates.is_empty(),
            final_view.indexes.values().any(|index| {
                index.has_temporal_key(&final_view.by_id[&index.table].def)
            }),
        );
        if final_view.has_text_search_types() {
            txn.require_features(super::meta::TEXT_SEARCH_TYPES);
        }
        if final_view.metadata.values().any(|table| !table.generated.is_empty()) {
            txn.require_features(super::meta::GENERATED_COLUMNS);
        }
        if final_view.metadata.values().any(|table| !table.gin.is_empty()) {
            txn.require_features(super::meta::GIN_INDEXES);
        }
        // A gateway's catalog commit also replaces the generation record, so other gateways
        // see that their cached catalog is stale (GD-0075 P7-1).
        let token = match self.services {
            TxnServices::Cluster(_) => {
                let token = new_generation_token().map_err(commit_error)?;
                let mut value = vec![GENERATION_VERSION];
                value.extend_from_slice(&token.to_be_bytes());
                txn.put(Keyspace::Catalog, GENERATION_KEY.to_vec(), value)?;
                Some(token)
            }
            TxnServices::Local { .. } => None,
        };
        // Clear under the cache mutex BEFORE finish submits to the sequencer:
        // clear -> submit -> visible Release -> reader snapshot Acquire ->
        // current RwLock read. New snapshots cannot return the old catalog;
        // they wait on this mutex until commit and publication are complete.
        if matches!(self.services, TxnServices::Local { .. }) {
            self.current_version.store(0, std::sync::atomic::Ordering::Release);
            *self.current.write().map_err(|_| TxnError::Misuse)? = None;
        }
        // The local cache mutex orders all SQL catalog writers. For an overlay
        // whose entries have no user-type or view normalization, the same
        // decoded records already form the view that a durable scan would see.
        // Gateways still reload against the commit timestamp to include changes
        // that were made outside this cache's publication mutex.
        if sql && token.is_none()
            && latest.types.is_empty() && final_view.types.is_empty()
            && latest.sql_views.is_empty() && final_view.sql_views.is_empty()
        {
            return finish(txn, guard, |ts| {
                cache.publish(ts, final_view, &mut retired);
                self.current_version.store(ts.0, std::sync::atomic::Ordering::Release);
                cache.prune(self.services.oldest_active(), ts, &mut retired);
                Ok(())
            });
        }
        finish(txn, guard, |ts| {
            let state = match &self.services {
                TxnServices::Local { seq, .. } => Some(&*seq.kv().causes),
                TxnServices::Cluster(_) => None,
            };
            let (loaded, read_error) = match state {
                Some(state) => state.capture_read_error(|| self.load_at(ts)),
                None => (self.load_at(ts), None),
            };
            let view = match loaded {
                Ok(view) => view,
                // On a cluster gateway the reload is a network read and may fail
                // transiently. The commit and its new generation token are durable, so the
                // next snapshot's generation check reloads; fencing the gateway would turn
                // one lost read into an outage (delta review D3).
                Err(_) if token.is_some() => return Ok(()),
                Err(error) => {
                    let cause = format!(
                        "durable catalog reload failed commit_ts={} replaced_version={:?}: {}",
                        ts.0, cache.versions.last().map(|(ts, _)| ts.0),
                        super::diagnostics::error_text(&error)
                    );
                    super::diagnostics::error(cause.clone());
                    if let Some(state) = state {
                        use super::kv::{FenceCause, FenceSite};
                        let follow_kv_fence = read_error.is_none() && error == CoreError::Fenced;
                        let mut recorded = match read_error {
                            Some(error) => FenceCause::for_kv(FenceSite::CatalogReload, &error),
                            None => FenceCause::for_core(FenceSite::CatalogReload, &error),
                        };
                        recorded.error_text = cause.clone();
                        state.record_fence(recorded, &self.poisoned, follow_kv_fence);
                    }
                    cache.poison_cause = Some(cause);
                    // Durability already succeeded. Never expose the previous
                    // view as current; this catalog must be reopened.
                    cache.poisoned = true;
                    return Err(TxnError::CommitAmbiguous);
                }
            };
            cache.publish(ts, view, &mut retired);
            if matches!(self.services, TxnServices::Local { .. }) {
                self.current_version.store(ts.0, std::sync::atomic::Ordering::Release);
            }
            if let Some(token) = token {
                cache.tokens.insert(ts, (token, ts));
            }
            // The just-committed timestamp is a safe pruning horizon; no
            // oracle round trip is needed while holding the publication lock.
            cache.prune(self.services.oldest_active(), ts, &mut retired);
            Ok(())
        })
    }

    pub(crate) fn prune(&self) -> Result<(), CoreError> {
        let visible = self.services.visible_ts()?;
        let oldest = self.services.oldest_active();
        let mut retired = Vec::new();
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        cache.ensure_live().map_err(CoreError::from)?;
        cache.prune(oldest, visible, &mut retired);
        Ok(())
    }
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn assert_current_matches_reload(&self) {
        let cache = self.cache.lock().unwrap();
        let (ts, current) = cache.versions.last().unwrap();
        assert_eq!(current.as_ref(), &self.load_at(*ts).unwrap());
    }

    #[cfg(test)]
    pub(crate) fn cache_versions(&self) -> Vec<Ts> {
        let cache = self.cache.lock().unwrap();
        if cache.poisoned {
            Vec::new()
        } else {
            cache.versions.iter().map(|(ts, _)| *ts).collect()
        }
    }
    #[cfg(test)]
    pub(crate) fn view_at(&self, snapshot: Ts) -> Option<Arc<CatalogView>> {
        self.cache.lock().unwrap().view_at(snapshot)
    }
}

pub(crate) fn table_key(id: u64) -> Vec<u8> {
    let mut key = vec![1];
    key.extend_from_slice(&id.to_be_bytes());
    key
}
pub(crate) fn name_key(name: &str) -> Vec<u8> {
    let mut key = vec![3];
    key.extend_from_slice(name.as_bytes());
    key.push(0);
    key
}
pub(crate) fn encode_entry(entry: &TableEntry) -> Result<Vec<u8>, CoreError> {
    super::catalog_codec::encode_table(entry, &entry.columns_ext)
}
pub(crate) fn decode_entry(value: &[u8]) -> Result<TableEntry, CoreError> {
    let (mut entry, columns_ext) = super::catalog_codec::decode_table(value)?;
    entry.columns_ext = columns_ext;
    Ok(entry)
}

/// The catalog-generation record (GD-0075 P7-1): system-range Catalog key `[0xF4]`, value
/// `[1][token u128 BE]`, replaced with a fresh random token by every catalog commit through a
/// cluster gateway. It is not a catalog object: catalog loads skip it.
pub(crate) const GENERATION_KEY: &[u8] = &[0xF4];
const GENERATION_VERSION: u8 = 1;

/// The generation token at `ts`; 0 when no gateway has written the record yet.
fn generation_token(kv: &dyn KvRead, ts: Ts) -> Result<u128, CoreError> {
    match mvcc::read_at(kv, Keyspace::Catalog, GENERATION_KEY, ts)? {
        None => Ok(0),
        Some(value) => match value.split_first() {
            Some((&GENERATION_VERSION, token)) if token.len() == 16 => {
                Ok(u128::from_be_bytes(token.try_into().expect("checked length")))
            }
            _ => Err(corrupt("invalid catalog generation record")),
        },
    }
}

fn new_generation_token() -> Result<u128, CoreError> {
    let mut token = [0; 16];
    getrandom::fill(&mut token).map_err(|_| CoreError::Storage("no randomness".into()))?;
    // 0 stands for an absent record.
    Ok(u128::from_be_bytes(token).max(1))
}

fn decode_id(value: &[u8]) -> Result<u64, CoreError> {
    let bytes = value
        .try_into()
        .map_err(|_| corrupt("invalid catalog table id"))?;
    let id = u64::from_be_bytes(bytes);
    if id == 0 {
        return Err(corrupt("zero catalog table id"));
    }
    Ok(id)
}
/// First Catalog user key of the system range's cluster records (`cluster_record_key`: 0xF0
/// directory, 0xF1 `gw/`, 0xF2 `ddl/`, 0xF3 `XACT/`, 0xF4 generation).
const CLUSTER_RECORDS: &[u8] = &[0xF0];

/// Where a catalog load stops. On a cluster gateway the scan ends before the cluster records
/// (review S4): there is one `XACT/` record per assigned xid, and a prepared one (a cross-range
/// xid commit between prepare and finalize) would make the load wait on it. The generation
/// record is read by point (`generation_token`). Single-node loads keep scanning everything,
/// so a stray key there still fails validation.
fn catalog_end(services: &TxnServices) -> Bound<&'static [u8]> {
    match services {
        TxnServices::Cluster(_) => Bound::Excluded(CLUSTER_RECORDS),
        TxnServices::Local { .. } => Bound::Unbounded,
    }
}

#[cfg(test)]
fn load_view(kv: &dyn KvRead, snapshot: Ts) -> Result<CatalogView, CoreError> {
    load_view_until(kv, snapshot, Bound::Unbounded)
}
fn load_view_until(
    kv: &dyn KvRead,
    snapshot: Ts,
    end: Bound<&[u8]>,
) -> Result<CatalogView, CoreError> {
    let mut view = CatalogView::default();
    let mut names = BTreeMap::new();
    let records = mvcc::VersionedRange::new(
        kv,
        Keyspace::Catalog,
        Bound::Unbounded,
        end,
        Direction::Forward,
        snapshot,
    )
    .map_err(CoreError::from)?;
    for record in records {
        let (key, value) = record.map_err(CoreError::from)?;
        if key == GENERATION_KEY {
            continue;
        }
        apply_record(&mut view, &mut names, &key, Some(&value))?;
    }
    finish_view(view, names, None).map_err(loaded_catalog_error)
}
/// A range leader has the final physical batch for both one-range commit
/// and prepared finalize. All SQL catalog records live in the system range.
#[cfg(feature = "engine-rocksdb")]
pub(crate) fn range_batch_needs_wide_catalog(
    kv: &dyn KvRead,
    batch: &super::kv::WriteBatch,
) -> Result<bool, CoreError> {
    use super::kv::Op;
    let mut writes = BTreeMap::new();
    let mut grows_catalog = false;
    for op in batch.ops() {
        let Op::Put { ks: Keyspace::Catalog, key, value } = op else { continue };
        let (user, _) = super::codec::split_versioned(key)?;
        if user >= CLUSTER_RECORDS {
            continue;
        }
        let kind = catalog_codec::classify_catalog_key(user)?;
        let value = mvcc::decode(value)?;
        grows_catalog |= value.is_some() && matches!(kind,
            CatalogKind::Table | CatalogKind::Schema | CatalogKind::Sequence
                | CatalogKind::TypeDefinition | CatalogKind::Collation | CatalogKind::ColumnCollation
                | CatalogKind::Constraint | CatalogKind::Index | CatalogKind::SqlView);
        writes.insert(user.to_vec(), value.map(<[u8]>::to_vec));
    }
    if !grows_catalog {
        return Ok(false);
    }
    let previous = load_view_until(kv, Ts(u64::MAX), Bound::Excluded(CLUSTER_RECORDS))?;
    Ok(overlay_view(&previous, writes.iter())?.needs_wide_catalog())
}

/// Derived, bounded predicate bindings for one catalog image.
#[derive(Default)]
pub(super) struct IndexPredicateImage {
    tables: BTreeMap<u64, Arc<TableEntry>>,
    predicates: BTreeMap<u64, (relational::secondary_index::Descriptor, relational::partial_index::BoundPredicate)>,
}

impl std::fmt::Debug for IndexPredicateImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexPredicateImage")
            .field("tables", &self.tables.keys())
            .field("indexes", &self.predicates.keys())
            .finish()
    }
}

impl PartialEq for IndexPredicateImage {
    fn eq(&self, other: &Self) -> bool {
        // The enclosing CatalogView compares the binding environment separately.
        self.tables == other.tables
            && self.predicates.values().map(|(d, _)| d)
                .eq(other.predicates.values().map(|(d, _)| d))
    }
}

impl IndexPredicateImage {
    fn matches_plain_overlay(&self, view: &CatalogView, base: &CatalogView) -> bool {
        // Complex catalog metadata can influence type and collation binding.
        // Rebind those catalogs; plain catalogs can reuse exact descriptor and
        // owning table definitions even when unrelated tables change.
        [view, base].iter().all(|v| {
            v.types.is_empty() && v.schemas.is_empty() && v.extensions.is_empty()
                && v.collations.is_empty() && v.column_collations.is_empty()
        }) && self.tables.iter().all(|(id, table)| view.by_id.get(id) == Some(table))
            && self.predicates.values().map(|(d, _)| d)
                .eq(view.indexes.values().filter(|d| d.predicate.is_some()))
    }

    pub(super) fn get(
        &self,
        table: &TableDef,
        index: &relational::secondary_index::Descriptor,
    ) -> Result<&relational::partial_index::BoundPredicate, CoreError> {
        // TableEntry can include a schema prefix. Bound slots depend on the
        // table identity and physical layout, regardless of the name spelling.
        let same_layout = self.tables.get(&index.table).is_some_and(|expected| {
            expected.def.id == table.id
                && expected.def.columns == table.columns
                && expected.def.primary_key == table.primary_key
        });
        self.predicates.get(&index.id)
            .filter(|(descriptor, _)| descriptor == index && same_layout)
            .map(|(_, predicate)| predicate)
            .ok_or_else(|| corrupt("index predicate does not match catalog snapshot"))
    }
}

// RowStore callers have no CatalogStore. Build this only for native operations
// on partial indexes. Transaction reuses it at the same snapshot and DDL generation.
pub(super) fn index_predicate_image(txn: &mut Transaction) -> Result<Arc<IndexPredicateImage>, CoreError> {
    let mut view = CatalogView::default();
    let mut names = BTreeMap::new();
    let end = catalog_end(txn.services());
    for record in txn.scan(Keyspace::Catalog, Bound::Unbounded, end, Direction::Forward, None)? {
        let (key, value) = record?;
        if key != GENERATION_KEY {
            apply_record(&mut view, &mut names, &key, Some(&value))?;
        }
    }
    finish_view(view, names, None).map(|view| view.index_predicates)
}

fn loaded_catalog_error(e: CoreError) -> CoreError {
    match e {
        CoreError::Limit => e,
        e => corrupt(&format!("invalid catalog: {e:?}")),
    }
}
fn overlay_view<'a>(
    base: &CatalogView,
    records: impl Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)>,
) -> Result<CatalogView, CoreError> {
    let mut view = base.clone();
    let mut names = (*base.names).clone();
    // Apply deletions before inserts so a transaction may reuse a dropped name.
    let records: Vec<_> = records.collect();
    for (key, value) in records.iter().filter(|(_, value)| value.is_none()) {
        apply_record(&mut view, &mut names, key, value.as_deref())?;
    }
    for (key, value) in records.iter().filter(|(_, value)| value.is_some()) {
        apply_record(&mut view, &mut names, key, value.as_deref())?;
    }
    // Reload decodes every constraint against its owning table. The overlay
    // otherwise retains already-decoded constraints when only a table changes.
    // Round-trip unchanged constraints through that same decoder before commit.
    for (key, value) in &records {
        if key.first() != Some(&1) || value.is_none() {
            continue;
        }
        let table = decode_id(&key[1..])?;
        let def = &view.by_id[&table].def;
        if let Some(constraints) = view.constraints.get(&table) {
            for constraint in constraints.values() {
                let constraint_key = catalog_codec::constraint_key(table, constraint.id);
                if records.iter().any(|(key, _)| **key == constraint_key) {
                    continue;
                }
                let encoded = catalog_codec::encode_constraint(constraint, &def.name)?;
                catalog_codec::decode_constraint(&encoded, def)?;
            }
        }
    }
    finish_view(view, names, Some(base))
}
fn apply_record(
    view: &mut CatalogView,
    names: &mut BTreeMap<String, u64>,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<(), CoreError> {
    match catalog_codec::classify_catalog_key(key)? {
        CatalogKind::SqlView => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.sql_views.insert(id, catalog_codec::sql_views::decode_view(value)?);
            } else {
                view.sql_views.remove(&id);
            }
        }
        CatalogKind::ColumnLabel => {
            let owner = super::catalog_label_codec::decode_label_key(key)?;
            if let Some(value) = value {
                view.column_labels.insert(owner, super::catalog_label_codec::decode_label(value)?);
            } else {
                view.column_labels.remove(&owner);
            }
        }
        CatalogKind::RelationComment => {
            let owner = (
                decode_id(&key[1..9])?,
                u64::from_be_bytes(key[9..17].try_into().unwrap()),
            );
            if let Some(value) = value {
                view.comments.insert(owner, catalog_codec::decode_comment(value)?);
            } else {
                view.comments.remove(&owner);
            }
        }
        CatalogKind::Table => {
            let id = decode_id(&key[1..])?;
            if let Some(previous) = view.by_id.get(&id) {
                for column in &previous.def.columns {
                    view.enum_array_columns.remove(&column.id);
                }
            }
            if let Some(value) = value {
                let (mut entry, columns_ext, metadata) = catalog_codec::decode_table_with_metadata(value)?;
                entry.columns_ext = columns_ext;
                if entry.def.id != id {
                    return Err(corrupt("catalog table key/id mismatch"));
                }
                view.enum_array_columns.extend(catalog_codec::decoded_enum_array_columns(
                    &entry.def, &entry.columns_ext,
                ));
                if metadata.is_empty() {
                    view.metadata.remove(&id);
                } else {
                    view.metadata.insert(id, metadata);
                }
                view.by_id.insert(id, Arc::new(entry));
            } else {
                view.by_id.remove(&id);
                view.metadata.remove(&id);
            }
        }
        CatalogKind::Name => {
            let name = std::str::from_utf8(&key[1..key.len() - 1])
                .map_err(|_| corrupt("invalid catalog name UTF-8"))?;
            if let Some(value) = value {
                names.insert(name.to_owned(), decode_id(value)?);
            } else {
                names.remove(name);
            }
        }
        CatalogKind::Constraint => {
            let table = decode_id(&key[1..9])?;
            let id = decode_id(&key[9..])?;
            if let Some(value) = value {
                let def = &view
                    .by_id
                    .get(&table)
                    .ok_or_else(|| corrupt("constraint table absent"))?
                    .def;
                let constraint = catalog_codec::decode_constraint(value, def)?;
                if constraint.id != id {
                    return Err(corrupt("constraint key/id mismatch"));
                }
                view.constraints
                    .entry(table)
                    .or_default()
                    .insert(id, constraint);
            } else if let std::collections::btree_map::Entry::Occupied(mut entry) =
                view.constraints.entry(table)
            {
                entry.get_mut().remove(&id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
        CatalogKind::Sequence => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.sequences
                    .insert(id, catalog_codec::decode_sequence(value)?);
            } else {
                view.sequences.remove(&id);
            }
        }
        CatalogKind::Type => return Err(corrupt("unsupported historical type record")),
        CatalogKind::Extensions => {
            view.extensions = match value { Some(v) => catalog_codec::decode_extensions(v)?, None => BTreeMap::new() };
        }
        CatalogKind::ColumnCollation => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value { view.column_collations.insert(id, catalog_codec::decode_column_collation(value)?); }
            else { view.column_collations.remove(&id); }
        }
        CatalogKind::Collation => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.collations.insert(id, catalog_codec::decode_collation(value)?);
            } else { view.collations.remove(&id); }
        }
        CatalogKind::EnumLabels => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.added_enum_labels.insert(id, catalog_codec::decode_enum_labels(value)?);
            } else {
                view.added_enum_labels.remove(&id);
            }
        }
        CatalogKind::TypeDefinition => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.types.insert(id, catalog_codec::decode_type(value)?);
            } else {
                view.types.remove(&id);
            }
        }
        CatalogKind::Schema => {
            let name = std::str::from_utf8(&key[1..key.len() - 1])
                .map_err(|_| corrupt("invalid schema name UTF-8"))?;
            if let Some(value) = value {
                view.schemas
                    .insert(name.into(), catalog_codec::decode_schema(value)?);
            } else {
                view.schemas.remove(name);
            }
        }
        CatalogKind::Index => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                let descriptor = super::indexes::decode_descriptor(value)?;
                if descriptor.id != id {
                    return Err(corrupt("index catalog key/id mismatch"));
                }
                view.indexes.insert(id, descriptor);
            } else {
                view.indexes.remove(&id);
            }
        }
        CatalogKind::TaskCredential => {
            let (task, id) = crate::task::credential::decode_credential_key(key)?;
            if let Some((owner, _)) = view.task_credentials.get(&id)
                && *owner != task
            {
                return Err(corrupt("duplicate credential id"));
            }
            if let Some(value) = value {
                let record = crate::task::credential::decode_credential(value)?;
                view.task_credentials.insert(id, (task, Arc::new(record)));
            } else {
                view.task_credentials.remove(&id);
            }
        }
        CatalogKind::Task => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.tasks
                    .insert(id, Arc::new(crate::task::catalog::decode_task(key, value)?));
                if view.tasks.len() > crate::task::definition::MAX_TASKS {
                    return Err(CoreError::Limit);
                }
            } else {
                view.tasks.remove(&id);
            }
        }
        CatalogKind::RoleBootstrap => {}
        CatalogKind::Role
        | CatalogKind::RoleMembership
        | CatalogKind::TableOwner
        | CatalogKind::TableGrant
        | CatalogKind::SchemaGrant
        | CatalogKind::RoleOid => {
            if let Some(value) = value {
                super::super::roles::engine::validate_record(key, value)?;
            }
        }
    }
    Ok(())
}
fn finish_view(
    mut view: CatalogView,
    names: BTreeMap<String, u64>,
    base: Option<&CatalogView>,
) -> Result<CatalogView, CoreError> {
    let mut by_name = BTreeMap::new();
    catalog_codec::validate_comments(&view.comments, view.by_id.values().map(|entry| &entry.def), &view.sql_views)?;
    super::catalog_label_codec::validate_labels(&view.column_labels, view.by_id.values().map(|entry| &entry.def))?;
    if !view.sql_views.is_empty() {
        let parts = view.parts();
        catalog_codec::sql_views::validate_catalog(&parts)?;
        object_ids(&parts)?;
    }
    let mut expected = BTreeMap::new();
    for entry in view.by_id.values_mut() {
        if entry.columns_ext.values().any(|ext| ext.type_id.is_some()) {
            let entry = Arc::make_mut(entry);
            for column in &mut entry.def.columns {
                if let Some(id) = entry.columns_ext.get(&column.id).and_then(|ext| ext.type_id) {
                    let ty = view.types.get(&id)
                        .ok_or_else(|| corrupt("column user type absent"))?;
                    column.data_type = if view.enum_array_columns.contains(&column.id) {
                        if !matches!(ty, TypeDef::Enum { .. }) {
                            return Err(corrupt("enum array column references a non-enum"));
                        }
                        relational::DataType::TextArray
                    } else {
                        ty.storage_type().map_err(|_| corrupt("unsupported user type column"))?
                    };
                    let ext = entry.columns_ext.get_mut(&column.id).unwrap();
                    ext.text_kind = match column.data_type {
                        relational::DataType::Varchar => Some(relational::catalog_ext::TextKind::Varchar),
                        relational::DataType::Bpchar => Some(relational::catalog_ext::TextKind::Bpchar),
                        _ => None,
                    };
                }
            }
        }
        if expected
            .insert(entry.def.name.clone(), entry.def.id)
            .is_some()
        {
            return Err(corrupt("duplicate catalog relation name"));
        }
        by_name.insert(entry.def.name.clone(), entry.clone());
    }
    for (id, def) in &view.sql_views {
        let name = catalog_codec::sql_views::relation_name(def, &view.schemas)?;
        if expected.insert(name, *id).is_some() {
            return Err(corrupt("duplicate catalog relation name"));
        }
    }
    for descriptor in view.indexes.values() {
        let table = view
            .by_id
            .get(&descriptor.table)
            .ok_or_else(|| corrupt("index table absent"))?;
        super::indexes::validate_descriptor_columns(descriptor, &table.def)?;
        if expected
            .insert(descriptor.name.clone(), descriptor.id)
            .is_some()
        {
            return Err(corrupt("duplicate catalog relation name"));
        }
    }
    for (table, metadata) in &view.metadata {
        let entry = view.by_id.get(table).ok_or_else(|| corrupt("GIN table absent"))?;
        if metadata.generated.keys().any(|id| !entry.def.columns.iter().any(|c| c.id == *id)) {
            return Err(corrupt("generated column absent"));
        }
        for (id, index) in &metadata.gin {
            index.validate(&entry.def).map_err(|_| corrupt("invalid GIN index"))?;
            if expected.insert(index.name.clone(), *id).is_some() {
                return Err(corrupt("duplicate catalog relation name"));
            }
        }
    }
    for (id, seq) in &view.sequences {
        if expected.insert(seq.name.clone(), *id).is_some() {
            return Err(corrupt("duplicate catalog relation name"));
        }
    }
    if expected != names {
        return Err(corrupt("catalog relation/name mismatch"));
    }
    if view
        .constraints
        .keys()
        .any(|id| !view.by_id.contains_key(id))
    {
        return Err(corrupt("constraint table absent"));
    }
    relational::catalog_ext::validate_unique_indexes(view.constraints.iter().flat_map(|(t, cs)| cs.values().map(move |c| (*t, c))), view.indexes.values()).map_err(|_| corrupt("invalid UNIQUE backing index"))?;
    if view.by_id.values().any(|e| e.primary_index_birth.is_some()) || view.constraints.values().flat_map(|cs| cs.values()).any(|c|
        matches!(c.kind, relational::catalog_ext::ConstraintKind::ForeignKey { target, .. } if target != relational::catalog_ext::ReferencedKey::Unknown)) {
        let parts = view.parts();
        object_ids(&parts)?;
        relational::catalog_ext::validate_key_targets(&parts.ext, parts.tables.values(), parts.indexes.iter(), u64::MAX).map_err(|_| corrupt("invalid referenced key or primary birth"))?;
    }
    if !base.is_some_and(|base| base.index_predicates.matches_plain_overlay(&view, base)) {
        view.index_predicates = Arc::new(bind_index_predicates(&view)?);
    }
    view.by_name = Arc::new(by_name);
    view.names = Arc::new(names);
    for (task, _) in view.task_credentials.values() {
        if !view.tasks.contains_key(task) {
            return Err(corrupt("credential task absent"));
        }
    }
    view.task_names.clear();
    for (id, task) in &view.tasks {
        if view.task_names.insert(task.name.as_str().to_owned(), *id).is_some() {
            return Err(corrupt("duplicate task name"));
        }
        // A DDL that would strand a live task fails before anything is
        // written (commit_ddl runs overlay_view first); 2BP01 there.
        if !task.revoked && !view.by_id.contains_key(&task.table) {
            return Err(CoreError::Sql(SqlError::DependentObjects));
        }
    }
    if !view.schemas.is_empty()
        || !view.types.is_empty() || !view.added_enum_labels.is_empty()
        || !view.extensions.is_empty() || !view.collations.is_empty() || !view.column_collations.is_empty()
        || !view.metadata.is_empty()
        || view.by_name.keys().any(|name| name.contains('\0'))
    {
        let parts = view.parts();
        relational::column_collations::validate_index_bindings(&parts.ext,&parts.indexes).map_err(|_|corrupt("invalid index collation binding"))?;
        if !view.collations.is_empty() || !view.types.is_empty() || !view.added_enum_labels.is_empty() || !view.metadata.is_empty() { object_ids(&parts)?; }
        relational::Relational::catalog_image(parts.tables, parts.ext, u64::MAX, 1)
            .map_err(|e| corrupt(&format!("invalid catalog image: {e:?}")))?;
    }
    Ok(view)
}

fn bind_index_predicates(view: &CatalogView) -> Result<IndexPredicateImage, CoreError> {
    let mut predicates = IndexPredicateImage::default();
    let mut partial = view.indexes.values().filter_map(|index| {
        index.predicate.as_ref().map(|predicate| (index, predicate))
    }).peekable();
    if partial.peek().is_none() {
        return Ok(predicates);
    }
    let parts = view.parts();
    let state = relational::Relational::catalog_image(parts.tables, parts.ext, u64::MAX, u64::MAX)
        .map_err(|e| corrupt(&format!("invalid index predicate catalog: {e:?}")))?;
    for (index, predicate) in partial {
        // The binding limits input bytes, tokens and expression complexity.
        let table = &view.by_id[&index.table];
        let bound = predicate.bind(&state, &table.def)
            .map_err(|e| corrupt(&format!("invalid stored index predicate: {e:?}")))?;
        predicates.tables.entry(index.table).or_insert_with(|| table.clone());
        predicates.predicates.insert(index.id, (index.clone(), bound));
    }
    Ok(predicates)
}
pub(super) fn object_ids(parts: &CatalogParts) -> Result<BTreeMap<u64, (u8, u64)>, CoreError> {
    let mut result = BTreeMap::new();
    let mut insert = |id, kind| {
        if id == 0 || result.insert(id, kind).is_some() {
            Err(corrupt("duplicate or zero catalog object id"))
        } else {
            Ok(())
        }
    };
    for table in parts.tables.values() {
        insert(table.id, (1, 0))?;
        for column in &table.columns {
            insert(column.id, (2, table.id))?;
        }
    }
    for (id, view) in &parts.sql_views {
        insert(*id, (17, 0))?;
        for column in &view.columns { insert(column.id, (2, *id))?; }
    }
    for (table, cs) in &parts.ext.constraints {
        for c in cs {
            insert(c.id, (4, *table))?;
        }
    }
    for descriptor in &parts.indexes {
        insert(descriptor.id, (16, descriptor.table))?;
    }
    for (id, index) in &parts.ext.gin_indexes {
        insert(*id, (16, index.table))?;
    }
    for id in parts.ext.sequences.keys() {
        insert(*id, (5, 0))?;
    }
    for id in parts.ext.collations.keys() { insert(*id, (22, 0))?; }
    for id in parts.ext.types.keys() {
        insert(*id, (15, 0))?;
    }
    let extension_oids: BTreeSet<_> = parts.ext.extensions.values().map(|extension| extension.oid).collect();
    for (owner, id) in parts.ext.enum_label_ids().map_err(|_| corrupt("invalid enum label identity"))? {
        if extension_oids.contains(&relational::type_oid(id).map_err(CoreError::Sql)?) {
            return Err(corrupt("enum label identity collides with an extension"));
        }
        insert(id, (catalog_codec::ENUM_LABELS_KIND, owner))?;
    }
    for (table, birth) in &parts.ext.primary_index_birth { insert(*birth, (18, *table))?; }
    for schema in parts.ext.schemas.values() {
        insert(schema.id, (14, 0))?;
    }
    Ok(result)
}
/// Shared inputs are safe to elide only after the complete old parts passed
/// these same checks. Ownership, names and catalog-wide checks still run.
#[derive(Default)]
struct UnchangedRecords<'a> {
    names: BTreeSet<&'a str>,
    tables: BTreeSet<u64>,
    constraints: BTreeSet<u64>,
    indexes: BTreeSet<u64>,
}

impl<'a> UnchangedRecords<'a> {
    fn between(old: &'a CatalogParts, new: &'a CatalogParts, own: &BTreeSet<&[u8]>) -> Self {
        let mut unchanged = Self::default();
        let owned_names: BTreeSet<_> = own
            .iter()
            .filter(|key| key.first() == Some(&3) && key.len() >= 2)
            .map(|key| &key[1..key.len() - 1])
            .collect();
        let mut unchanged_defs = BTreeSet::new();
        let has_own = |kind, first: u64, second: Option<u64>| {
            let mut key = [0; 17];
            key[0] = kind;
            key[1..9].copy_from_slice(&first.to_be_bytes());
            let len = if let Some(second) = second {
                key[9..].copy_from_slice(&second.to_be_bytes());
                17
            } else {
                9
            };
            own.contains(&key[..len])
        };
        for (name, table) in &old.tables {
            let Some(next) = new.tables.get(name) else { continue; };
            if table.id == next.id && !owned_names.contains(name.as_bytes()) {
                unchanged.names.insert(name);
            }
            if next != table {
                continue;
            }
            unchanged_defs.insert(table.id);
            if !has_own(1, table.id, None)
                && old.heap.contains(&table.id) == new.heap.contains(&table.id)
                && old.ext.primary_index_birth.get(&table.id) == new.ext.primary_index_birth.get(&table.id)
                && table.columns.iter().all(|column| {
                    old.ext.columns.get(&column.id) == new.ext.columns.get(&column.id)
                })
            {
                unchanged.tables.insert(table.id);
            }
            if let Some(constraints) = old.ext.constraints.get(&table.id)
                && new.ext.constraints.get(&table.id) == Some(constraints)
                && !constraints.iter().any(|c| has_own(4, table.id, Some(c.id)))
            {
                unchanged.constraints.insert(table.id);
            }
        }
        let indexes: BTreeMap<_, _> = old.indexes.iter().map(|d| (d.id, d)).collect();
        for index in &new.indexes {
            let Some(previous) = indexes.get(&index.id) else { continue; };
            if previous.name == index.name && !owned_names.contains(index.name.as_bytes()) {
                unchanged.names.insert(&index.name);
            }
            if unchanged_defs.contains(&index.table)
                && *previous == index
                && !has_own(16, index.id, None)
            {
                unchanged.indexes.insert(index.id);
            }
        }
        unchanged
    }
}

type CatalogRecords = BTreeMap<Vec<u8>, Vec<u8>>;
type ObjectIds = BTreeMap<u64, (u8, u64)>;

pub(crate) fn parts_records(parts: &CatalogParts) -> Result<CatalogRecords, CoreError> {
    encode_parts(parts, &UnchangedRecords::default(), false).map(|(records, _)| records)
}

fn encode_parts(
    parts: &CatalogParts,
    unchanged: &UnchangedRecords<'_>,
    checked_old: bool,
) -> Result<(CatalogRecords, ObjectIds), CoreError> {
    // A checked old image is exactly equal to previously validated parts.
    // Direct callers and every new image still run the catalog-wide checks.
    if !checked_old {
        catalog_codec::validate_comments(&parts.comments, parts.tables.values(), &parts.sql_views)?;
        catalog_codec::sql_views::validate_catalog(parts)?;
        relational::catalog_ext::validate_unique_indexes(parts.ext.constraints.iter().flat_map(|(t, cs)| cs.iter().map(move |c| (*t, c))), parts.indexes.iter()).map_err(|_| corrupt("invalid UNIQUE backing index"))?;
        relational::column_collations::validate_index_bindings(&parts.ext,&parts.indexes).map_err(|_|corrupt("invalid index collation binding"))?;
    }
    let ids = object_ids(parts)?;
    if !checked_old {
        relational::catalog_ext::validate_key_targets(&parts.ext, parts.tables.values(), parts.indexes.iter(), u64::MAX).map_err(|_| corrupt("invalid referenced key or primary birth"))?;
        super::catalog_label_codec::validate_labels(&parts.column_labels, parts.tables.values())?;
    }
    let mut records = BTreeMap::new();
    for (&(table, column), &flags) in &parts.column_labels {
        records.insert(
            super::catalog_label_codec::label_key(table, column),
            super::catalog_label_codec::encode_label(flags)?,
        );
    }
    for (&(relation, column), text) in &parts.comments {
        records.insert(
            catalog_codec::comment_key(relation, column),
            catalog_codec::encode_comment(text)?,
        );
    }
    use std::borrow::Cow;
    let mut names = BTreeSet::new();
    for (id, view) in &parts.sql_views {
        let name = catalog_codec::sql_views::relation_name(view, &parts.ext.schemas)?;
        let key = name_key(&name);
        if !names.insert(Cow::Owned(name)) { return Err(CoreError::Sql(SqlError::Duplicate)); }
        records.insert(key, id.to_be_bytes().to_vec());
        records.insert(catalog_codec::sql_views::view_key(*id), catalog_codec::sql_views::encode_view(view)?);
    }
    let mut columns = BTreeSet::new();
    let mut table_ids = BTreeMap::new();
    for (name, def) in &parts.tables {
        let schema = parts
            .ext
            .table_schemas
            .get(&def.id)
            .map_or("public", String::as_str);
        if *name != relational::relation_key(schema, &def.name) {
            return Err(corrupt("catalog table name mismatch"));
        }
        if !names.insert(Cow::Borrowed(name.as_str())) {
            return Err(CoreError::Sql(SqlError::Duplicate));
        }
        table_ids.insert(def.id, def);
        if !parts.ext.columns.is_empty() {
            columns.extend(def.columns.iter().map(|c| c.id));
        }
        if !unchanged.names.contains(name.as_str()) {
            records.insert(name_key(name), def.id.to_be_bytes().to_vec());
        }
        if unchanged.tables.contains(&def.id) {
            continue;
        }
        let ext = def.columns.iter()
            .filter_map(|c| parts.ext.columns.get(&c.id).map(|v| (c.id, v.clone())))
            .collect();
        let mut stored_def = def.clone();
        stored_def.name = name.clone();
        let entry = TableEntry {
            primary_index_birth: parts.ext.primary_index_birth.get(&def.id).copied(),
            def: stored_def,
            heap: parts.heap.contains(&def.id),
            columns_ext: ext,
        };
        let metadata = catalog_codec::TableMetadata {
            gin: parts.ext.gin_indexes.iter().filter(|(_, index)| index.table == def.id)
                .map(|(id, index)| (*id, index.clone())).collect(),
            generated: def.columns.iter().filter_map(|column|
                parts.ext.generated_columns.get(&column.id).map(|generation| (column.id, generation.clone()))
            ).collect(),
        };
        let value = catalog_codec::encode_table_with_metadata(&entry, &entry.columns_ext, &parts.ext.types, &metadata)?;
        decode_entry(&value)?;
        records.insert(table_key(def.id), value);
    }
    if parts.heap.iter().any(|id| !table_ids.contains_key(id))
        || parts.ext.columns.keys().chain(parts.ext.generated_columns.keys()).any(|id| !columns.contains(id))
    {
        return Err(corrupt("catalog extension owner absent"));
    }
    for (table, cs) in &parts.ext.constraints {
        let def = table_ids.get(table).ok_or_else(|| corrupt("constraint table absent"))?;
        if unchanged.constraints.contains(table) {
            continue;
        }
        for c in cs {
            let value = catalog_codec::encode_constraint(c, &def.name)?;
            catalog_codec::decode_constraint(&value, def)?;
            records.insert(catalog_codec::constraint_key(*table, c.id), value);
        }
    }
    for descriptor in &parts.indexes {
        let table = table_ids.get(&descriptor.table).ok_or_else(|| corrupt("index table absent"))?;
        let changed = !unchanged.indexes.contains(&descriptor.id);
        if changed {
            super::indexes::validate_descriptor(descriptor, table)?;
        }
        if !names.insert(Cow::Borrowed(descriptor.name.as_str())) {
            return Err(CoreError::Sql(SqlError::Duplicate));
        }
        if changed {
            records.insert(
                super::indexes::descriptor_key(descriptor.id),
                super::indexes::encode_descriptor(descriptor)?,
            );
        }
        if !unchanged.names.contains(descriptor.name.as_str()) {
            records.insert(
                name_key(&descriptor.name),
                descriptor.id.to_be_bytes().to_vec(),
            );
        }
    }
    for (id, index) in &parts.ext.gin_indexes {
        let table = table_ids.get(&index.table).ok_or_else(|| corrupt("GIN table absent"))?;
        index.validate(table).map_err(|_| corrupt("invalid GIN index"))?;
        if !names.insert(Cow::Owned(index.name.clone())) {
            return Err(CoreError::Sql(SqlError::Duplicate));
        }
        records.insert(name_key(&index.name), id.to_be_bytes().to_vec());
    }
    for (id, def) in &parts.ext.sequences {
        if !names.insert(Cow::Borrowed(def.name.as_str())) {
            return Err(CoreError::Sql(SqlError::Duplicate));
        }
        let value = catalog_codec::encode_sequence(def)?;
        catalog_codec::decode_sequence(&value)?;
        records.insert(catalog_codec::sequence_key(*id), value);
        records.insert(name_key(&def.name), id.to_be_bytes().to_vec());
    }
    if !parts.ext.extensions.is_empty() {
        records.insert(vec![catalog_codec::EXTENSIONS_KIND], catalog_codec::encode_extensions(&parts.ext.extensions)?);
    }
    for (id, binding) in &parts.ext.column_collations {
        records.insert(catalog_codec::column_collation_key(*id), catalog_codec::encode_column_collation(binding)?);
    }
    for (id, c) in &parts.ext.collations {
        records.insert(catalog_codec::collation_key(*id), catalog_codec::encode_collation(c)?);
    }
    for (id, labels) in &parts.ext.added_enum_labels {
        records.insert(catalog_codec::enum_labels_key(*id), catalog_codec::encode_enum_labels(labels)?);
    }
    for (id, ty) in &parts.ext.types {
        records.insert(
            catalog_codec::type_key(*id),
            catalog_codec::encode_type(ty.clone())?,
        );
    }
    for (name, schema) in &parts.ext.schemas {
        records.insert(
            catalog_codec::schema_key(name),
            catalog_codec::encode_schema(schema)?,
        );
    }
    Ok((records, ids))
}

fn validate_definition(def: &TableDef) -> Result<(), SqlError> {
    validate_identifier(&def.name)?;
    if def.columns.is_empty() || def.columns.len() > relational::MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let mut names = BTreeSet::new();
    for column in &def.columns {
        relational::column_ident(&column.name)?;
        if !names.insert(&column.name) {
            return Err(SqlError::Duplicate);
        }
    }
    let mut indexes = BTreeSet::new();
    let mut keys = Vec::new();
    for &index in &def.primary_key {
        let column = def.columns.get(index).ok_or(SqlError::UnknownColumn)?;
        if !indexes.insert(index) {
            return Err(SqlError::Duplicate);
        }
        if !column.not_null {
            return Err(SqlError::Null);
        }
        keys.push(column.name.clone());
    }
    if !keys.is_empty() {
        relational::validate_create_definition(&def.name, &def.columns, &keys)?;
    }
    Ok(())
}
fn validate_identifier(name: &str) -> Result<(), SqlError> {
    // Names are already parsed SQL identifiers, including quoted identifiers.
    // Apply the same content rules as SQL rather than unquoted token syntax.
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        return Err(SqlError::Syntax);
    }
    if [
        "deliveries",
        "giggles",
        "system",
        "information_schema",
        "pg_catalog",
        "repair",
        "internal",
        "tasks",
        "operations",
        "approvals",
        "receipts",
    ]
    .contains(&name)
        || ["giggles_", "system_", "pg_", "repair_", "internal_"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
    {
        return Err(SqlError::Reserved);
    }
    Ok(())
}
fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(message.into())
}
fn commit_error(error: CoreError) -> TxnError {
    match error {
        CoreError::Sql(SqlError::InvalidSchema) => TxnError::Serialization(SerializationReason::WriteConflict),
        CoreError::Sql(SqlError::DependentObjects) => TxnError::DependentObjects,
        CoreError::Limit => TxnError::WriteSetLimit,
        CoreError::Fenced => TxnError::Fenced,
        CoreError::WriteFenced { epoch } => TxnError::WriteFenced { epoch },
        CoreError::Storage(message) => TxnError::Kv(KvError::Io(message)),
        error => TxnError::Kv(KvError::Corrupt(format!("{error:?}"))),
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "catalog_diff_tests.rs"]
mod diff_tests;

#[cfg(test)]
#[path = "catalog_commit_fast_tests.rs"]
mod commit_fast_tests;

#[cfg(test)]
#[path = "catalog_snapshot_tests.rs"]
mod snapshot_tests;

#[cfg(test)]
#[path = "catalog_current_tests.rs"]
mod current_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[test]
fn unknown_binding_fence_fails_closed() {
    use crate::storage::{Admin, Engine, Limits};
    let root = crate::test_artifact_root().join("unknown-binding-fence");
    let admin = Admin::create_engine(&root, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    session.begin_native().unwrap();
    let db = admin.test_v2();
    {
        let mut cache = db.catalog().cache.lock().unwrap();
        *db.catalog().current.write().unwrap() = None;
        cache.versions.clear();
    }
    assert_eq!(
        session.snapshot_identity_engine(),
        Err(CoreError::Sql(relational::SqlError::StaleSchema))
    );
    session.rollback_native().unwrap();
    assert_eq!(
        admin.inspect_catalog_engine(|_, _, _| ()),
        Err(CoreError::Sql(relational::SqlError::StaleSchema))
    );
    drop(session);
    drop(db);
    drop(admin);
    std::fs::remove_dir_all(root).unwrap();
}
