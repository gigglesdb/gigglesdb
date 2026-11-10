//! Transactional table metadata and snapshot-versioned catalog views.
use super::{
    catalog_codec::{self, CatalogKind},
    commit::Sequencer,
    hlc::Ts,
    kv::{Direction, Keyspace, Kv, KvError},
    locks::{CATALOG_NAME_INDEX, LockKey, LockManager, LockMode},
    mvcc,
    seq::SequenceAllocator,
    txn::{Isolation, SerializationReason, Transaction, TxnError},
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
    sync::{Arc, Mutex},
    time::Instant,
};

pub(crate) const MAX_TABLES: usize = 100_000;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct TableEntry {
    pub def: TableDef,
    pub heap: bool,
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
    pub(crate) by_name: BTreeMap<String, Arc<TableEntry>>,
    pub(crate) by_id: BTreeMap<u64, Arc<TableEntry>>,
    pub(crate) names: BTreeMap<String, u64>,
    pub(crate) constraints: BTreeMap<u64, BTreeMap<u64, Constraint>>,
    pub(crate) sequences: BTreeMap<u64, SequenceDef>,
    pub(crate) types: BTreeMap<u64, TypeDef>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CatalogParts {
    pub(crate) tables: BTreeMap<String, TableDef>,
    pub(crate) heap: BTreeSet<u64>,
    pub(crate) ext: CatalogExt,
}
impl CatalogView {
    pub(crate) fn parts(&self) -> CatalogParts {
        let mut parts = CatalogParts::default();
        for entry in self.by_id.values() {
            parts
                .tables
                .insert(entry.def.name.clone(), entry.def.clone());
            if entry.heap {
                parts.heap.insert(entry.def.id);
            }
            parts.ext.columns.extend(entry.columns_ext.clone());
            for column in &entry.def.columns {
                let kind = match column.data_type {
                    relational::DataType::Uuid => LogicalKind::Uuid,
                    relational::DataType::Jsonb => LogicalKind::Jsonb,
                    relational::DataType::IntArray => LogicalKind::IntArray,
                    relational::DataType::BigIntArray => LogicalKind::BigIntArray,
                    relational::DataType::TextArray => LogicalKind::TextArray,
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
        parts.ext.sequences = self.sequences.clone();
        parts.ext.types = self.types.clone();
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
        parts
    }
}

pub(crate) struct CatalogCache {
    versions: Vec<(Ts, Arc<CatalogView>)>,
    poisoned: bool,
}
impl CatalogCache {
    pub(crate) fn load(kv: &Kv, visible: Ts) -> Result<Self, CoreError> {
        let mut cache = Self {
            versions: Vec::new(),
            poisoned: false,
        };
        cache.publish(visible, load_view(kv, visible)?);
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
    pub(crate) fn publish(&mut self, ts: Ts, view: CatalogView) {
        if self.poisoned {
            return;
        }
        match self.versions.binary_search_by_key(&ts, |(ts, _)| *ts) {
            Ok(index) => self.versions[index].1 = Arc::new(view),
            Err(index) => self.versions.insert(index, (ts, Arc::new(view))),
        }
    }
    pub(crate) fn prune(&mut self, oldest_active: Option<Ts>, visible: Ts) {
        if self.poisoned {
            return;
        }
        let horizon = oldest_active.unwrap_or(visible).min(visible);
        let end = self.versions.partition_point(|(ts, _)| *ts <= horizon);
        if let Some(anchor) = end.checked_sub(1) {
            self.versions.drain(..anchor);
        }
    }
    fn ensure_live(&self) -> Result<(), TxnError> {
        if self.poisoned {
            Err(TxnError::Fenced)
        } else {
            Ok(())
        }
    }
}

/// One catalog and allocator are shared by all transactions of an engine.
/// Callers must use transactions from the same sequencer and lock manager.
/// The cache mutex also orders DDL commit, rebuild and publication. It is never
/// held while waiting for a transaction lock.
pub(crate) struct Catalog {
    cache: Mutex<CatalogCache>,
    sequencer: Arc<Sequencer>,
    seq: Arc<SequenceAllocator>,
}
impl Catalog {
    pub(crate) fn open(
        sequencer: Arc<Sequencer>,
        _locks: Arc<LockManager>,
        seq: Arc<SequenceAllocator>,
    ) -> Result<Self, CoreError> {
        let cache = CatalogCache::load(sequencer.kv(), sequencer.visible_ts())?;
        Ok(Self {
            cache: Mutex::new(cache),
            sequencer,
            seq,
        })
    }

    fn check(&self) -> Result<(), TxnError> {
        self.cache
            .lock()
            .map_err(|_| TxnError::Misuse)?
            .ensure_live()?;
        Ok(())
    }

    pub(crate) fn reserve_ids(&self, n: u64) -> Result<Range<u64>, CoreError> {
        self.seq.reserve_range(0, 0, n)
    }

    pub(crate) fn parts(&self, txn: &mut Transaction) -> Result<CatalogParts, CoreError> {
        self.check().map_err(CoreError::from)?;
        let cache = self
            .cache
            .lock()
            .map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        let view = match cache.view_at(txn.snapshot_ts()) {
            Some(view) => view,
            None => Arc::new(load_view(self.sequencer.kv(), txn.snapshot_ts())?),
        };
        Ok(overlay_view(&view, txn.catalog_writes())?.parts())
    }

    pub(crate) fn apply_diff(
        &self,
        txn: &mut Transaction,
        old: &CatalogParts,
        new: &CatalogParts,
        ids: &Range<u64>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), CoreError> {
        self.check().map_err(CoreError::from)?;
        let old_records = parts_records(old)?;
        let new_records = parts_records(new)?;
        let old_ids = object_ids(old)?;
        let new_ids = object_ids(new)?;
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
        for (name, table) in &new.tables {
            if let Some(previous) = old.tables.get(name) {
                if previous.id != table.id {
                    return Err(corrupt("existing table id changed"));
                }
                for column in &table.columns {
                    if previous
                        .columns
                        .iter()
                        .any(|c| c.name == column.name && c.id != column.id)
                    {
                        return Err(corrupt("existing column id changed"));
                    }
                }
            }
        }
        if old_records == new_records {
            return Ok(());
        }
        self.lock_and_write(txn, &old_records, &new_records, deadline, cancel)?;
        if !new.ext.is_empty() {
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
        let mut tables = BTreeSet::new();
        for key in changes.keys() {
            match key[0] {
                1 | 4 => {
                    tables.insert(decode_id(&key[1..9]).map_err(commit_error)?);
                }
                3 => {
                    let name = std::str::from_utf8(&key[1..key.len() - 1])
                        .map_err(|_| commit_error(corrupt("invalid catalog name")))?;
                    self.lock_name(txn, name, deadline, cancel)?;
                }
                _ => {}
            }
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
        self.check()?;
        validate_identifier(name)?;
        if !matches!(
            mode,
            LockMode::AccessShare
                | LockMode::RowExclusive
                | LockMode::Share
                | LockMode::AccessExclusive
        ) {
            return Err(TxnError::Misuse.into());
        }
        for attempt in 0..3 {
            let (base, entry) = {
                let mut cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
                cache.ensure_live()?;
                let snapshot = txn.snapshot_ts();
                let view = match cache.view_at(snapshot) {
                    Some(view) => view,
                    None => {
                        cache.publish(snapshot, load_view(self.sequencer.kv(), snapshot)?);
                        cache.view_at(snapshot).unwrap()
                    }
                };
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
            let cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
            cache.ensure_live()?;
            let latest = cache.versions.last().unwrap().1.by_name.get(name);
            if latest == base.as_ref() {
                return Ok(entry);
            }
            if txn.isolation() != Isolation::ReadCommitted || attempt == 2 {
                return Err(TxnError::Serialization(SerializationReason::WriteConflict).into());
            }
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
        validate_identifier(name)?;
        self.lock_name(txn, name, deadline, cancel)?;
        let entry = self.resolve(txn, name, LockMode::AccessExclusive, deadline, cancel)?;
        let old = self.parts(txn)?;
        let mut new = old.clone();
        new.tables.remove(name);
        new.heap.remove(&entry.def.id);
        for column in &entry.def.columns {
            new.ext.columns.remove(&column.id);
        }
        new.ext.constraints.remove(&entry.def.id);
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
        validate_identifier(&column.name)?;
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
        txn.put(
            Keyspace::Catalog,
            table_key(entry.def.id),
            encode_entry(&entry)?,
        )?;
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
        self.check()?;
        let mut cache = self.cache.lock().map_err(|_| TxnError::Misuse)?;
        cache.ensure_live()?;
        let latest = &cache.versions.last().unwrap().1;
        // Reject limit/corruption before committing anything durably.
        overlay_view(latest, txn.catalog_writes()).map_err(commit_error)?;
        txn.commit_then(|ts| {
            let view = match load_view(self.sequencer.kv(), ts) {
                Ok(view) => view,
                Err(_) => {
                    // Durability already succeeded. Never expose the previous
                    // view as current; this catalog must be reopened.
                    cache.poisoned = true;
                    return Err(TxnError::CommitAmbiguous);
                }
            };
            cache.publish(ts, view);
            cache.prune(self.sequencer.oldest_active(), self.sequencer.visible_ts());
            Ok(())
        })
    }

    pub(crate) fn prune(&self) -> Result<(), CoreError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| corrupt("catalog cache mutex poisoned"))?;
        cache.ensure_live().map_err(CoreError::from)?;
        cache.prune(self.sequencer.oldest_active(), self.sequencer.visible_ts());
        Ok(())
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
fn load_view(kv: &Kv, snapshot: Ts) -> Result<CatalogView, CoreError> {
    let mut view = CatalogView::default();
    let mut names = BTreeMap::new();
    let records = mvcc::VersionedRange::new(
        kv,
        Keyspace::Catalog,
        Bound::Unbounded,
        Bound::Unbounded,
        Direction::Forward,
        snapshot,
    )
    .map_err(CoreError::from)?;
    for record in records {
        let (key, value) = record.map_err(CoreError::from)?;
        apply_record(&mut view, &mut names, &key, Some(&value))?;
    }
    finish_view(view, names)
}
fn overlay_view<'a>(
    base: &CatalogView,
    records: impl Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)>,
) -> Result<CatalogView, CoreError> {
    let mut view = base.clone();
    let mut names = base.names.clone();
    // Remove first, so a full catalog can drop and create within one DDL txn
    // without exceeding the bound transiently due to key ordering.
    let records: Vec<_> = records.collect();
    for (key, value) in records.iter().filter(|(_, value)| value.is_none()) {
        apply_record(&mut view, &mut names, key, value.as_deref())?;
    }
    for (key, value) in records.iter().filter(|(_, value)| value.is_some()) {
        apply_record(&mut view, &mut names, key, value.as_deref())?;
    }
    finish_view(view, names)
}
fn apply_record(
    view: &mut CatalogView,
    names: &mut BTreeMap<String, u64>,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<(), CoreError> {
    match catalog_codec::classify_catalog_key(key)? {
        CatalogKind::Table => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                let entry = decode_entry(value)?;
                if entry.def.id != id {
                    return Err(corrupt("catalog table key/id mismatch"));
                }
                view.by_id.insert(id, Arc::new(entry));
                if view.by_id.len() > MAX_TABLES {
                    return Err(CoreError::Limit);
                }
            } else {
                view.by_id.remove(&id);
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
            } else if let Some(cs) = view.constraints.get_mut(&table) {
                cs.remove(&id);
            }
            view.constraints.retain(|_, cs| !cs.is_empty());
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
        CatalogKind::Type => {
            let id = decode_id(&key[1..])?;
            if let Some(value) = value {
                view.types.insert(id, catalog_codec::decode_type(value)?);
            } else {
                view.types.remove(&id);
            }
        }
        CatalogKind::RoleBootstrap => {}
    }
    Ok(())
}
fn finish_view(
    mut view: CatalogView,
    names: BTreeMap<String, u64>,
) -> Result<CatalogView, CoreError> {
    view.by_name.clear();
    let mut expected = BTreeMap::new();
    for entry in view.by_id.values() {
        if expected
            .insert(entry.def.name.clone(), entry.def.id)
            .is_some()
        {
            return Err(corrupt("duplicate catalog relation name"));
        }
        view.by_name.insert(entry.def.name.clone(), entry.clone());
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
    view.names = names;
    Ok(view)
}
fn object_ids(parts: &CatalogParts) -> Result<BTreeMap<u64, (u8, u64)>, CoreError> {
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
    for (table, cs) in &parts.ext.constraints {
        for c in cs {
            insert(c.id, (4, *table))?;
        }
    }
    for id in parts.ext.sequences.keys() {
        insert(*id, (5, 0))?;
    }
    for id in parts.ext.types.keys() {
        insert(*id, (6, 0))?;
    }
    Ok(result)
}
fn parts_records(parts: &CatalogParts) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    object_ids(parts)?;
    let mut records = BTreeMap::new();
    let mut names = BTreeSet::new();
    let mut columns = BTreeSet::new();
    let mut table_ids = BTreeSet::new();
    for (name, def) in &parts.tables {
        if name != &def.name || !names.insert(name.clone()) {
            return Err(corrupt("catalog table name mismatch"));
        }
        table_ids.insert(def.id);
        let ext = def
            .columns
            .iter()
            .filter_map(|c| {
                columns.insert(c.id);
                parts.ext.columns.get(&c.id).map(|v| (c.id, v.clone()))
            })
            .collect();
        let entry = TableEntry {
            def: def.clone(),
            heap: parts.heap.contains(&def.id),
            columns_ext: ext,
        };
        let value = encode_entry(&entry)?;
        decode_entry(&value)?;
        records.insert(table_key(def.id), value);
        records.insert(name_key(name), def.id.to_be_bytes().to_vec());
    }
    if parts.heap.iter().any(|id| !table_ids.contains(id))
        || parts.ext.columns.keys().any(|id| !columns.contains(id))
    {
        return Err(corrupt("catalog extension owner absent"));
    }
    for (table, cs) in &parts.ext.constraints {
        let def = parts
            .tables
            .values()
            .find(|d| d.id == *table)
            .ok_or_else(|| corrupt("constraint table absent"))?;
        for c in cs {
            let value = catalog_codec::encode_constraint(c, &def.name)?;
            catalog_codec::decode_constraint(&value, def)?;
            records.insert(catalog_codec::constraint_key(*table, c.id), value);
        }
    }
    for (id, def) in &parts.ext.sequences {
        if !names.insert(def.name.clone()) {
            return Err(corrupt("duplicate catalog relation name"));
        }
        let value = catalog_codec::encode_sequence(def)?;
        catalog_codec::decode_sequence(&value)?;
        records.insert(catalog_codec::sequence_key(*id), value);
        records.insert(name_key(&def.name), id.to_be_bytes().to_vec());
    }
    // User types currently belong to the namespace-5 ext-version-six layout.
    // Refuse them rather than dropping metadata in the independent v2 codec.
    if !parts.ext.types.is_empty() {
        return Err(CoreError::Unsupported);
    }
    Ok(records)
}

fn validate_definition(def: &TableDef) -> Result<(), SqlError> {
    validate_identifier(&def.name)?;
    if def.columns.is_empty() || def.columns.len() > relational::MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let mut names = BTreeSet::new();
    for column in &def.columns {
        validate_identifier(&column.name)?;
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
    if name.is_empty()
        || name.len() > 63
        || !name.as_bytes()[0].is_ascii_alphabetic()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
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
        CoreError::Limit => TxnError::WriteSetLimit,
        CoreError::Fenced => TxnError::Fenced,
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
