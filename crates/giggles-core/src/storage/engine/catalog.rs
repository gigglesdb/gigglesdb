//! Transactional table metadata and snapshot-versioned catalog views.
use super::{
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
    relational::{self, ColumnDef, SqlError, TableDef},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{Arc, Mutex},
    time::Instant,
};

pub(crate) const MAX_TABLES: usize = 100_000;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct TableEntry {
    pub def: TableDef,
    pub heap: bool,
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
        let entry = Arc::new(TableEntry { def, heap });
        txn.put(
            Keyspace::Catalog,
            table_key(entry.def.id),
            encode_entry(&entry)?,
        )?;
        txn.put(
            Keyspace::Catalog,
            name_key(&entry.def.name),
            entry.def.id.to_be_bytes().to_vec(),
        )?;
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
        txn.delete(Keyspace::Catalog, table_key(entry.def.id))?;
        txn.delete(Keyspace::Catalog, name_key(name))?;
        // Physical rows remain until snapshot-safe GC can reclaim them.
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
    super::catalog_codec::encode_table(entry, &BTreeMap::new())
}
pub(crate) fn decode_entry(value: &[u8]) -> Result<TableEntry, CoreError> {
    let (entry, columns_ext) = super::catalog_codec::decode_table(value)?;
    if !columns_ext.is_empty() {
        return Err(corrupt("catalog wrapper cannot represent column extensions"));
    }
    validate_definition(&entry.def).map_err(|_| corrupt("invalid catalog table definition"))?;
    if entry.def.id == 0 || entry.def.columns.iter().any(|column| column.id == 0) {
        return Err(corrupt("zero catalog object id"));
    }
    let ids: BTreeSet<_> = entry.def.columns.iter().map(|column| column.id).collect();
    if ids.len() != entry.def.columns.len() {
        return Err(corrupt("duplicate catalog column id"));
    }
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
    let mut names: BTreeMap<String, u64> = base
        .by_name
        .iter()
        .map(|(name, entry)| (name.clone(), entry.def.id))
        .collect();
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
    match key.first() {
        Some(1) if key.len() == 9 => {
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
        Some(3) if key.len() >= 3 && key.last() == Some(&0) => {
            let name = std::str::from_utf8(&key[1..key.len() - 1])
                .map_err(|_| corrupt("invalid catalog name UTF-8"))?;
            validate_identifier(name).map_err(|_| corrupt("invalid catalog name key"))?;
            if let Some(value) = value {
                names.insert(name.to_owned(), decode_id(value)?);
            } else {
                names.remove(name);
            }
            if names.len() > MAX_TABLES {
                return Err(CoreError::Limit);
            }
        }
        _ => return Err(corrupt("invalid catalog user key")),
    }
    Ok(())
}
fn finish_view(
    mut view: CatalogView,
    names: BTreeMap<String, u64>,
) -> Result<CatalogView, CoreError> {
    view.by_name.clear();
    for (name, id) in names {
        let entry = view
            .by_id
            .get(&id)
            .ok_or_else(|| corrupt("catalog name references absent table"))?;
        if entry.def.name != name || view.by_name.insert(name, entry.clone()).is_some() {
            return Err(corrupt("catalog name/table mismatch"));
        }
    }
    if view.by_name.len() != view.by_id.len() {
        return Err(corrupt("catalog table missing name"));
    }
    Ok(view)
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
