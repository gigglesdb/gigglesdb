//! Catalog kind 19: task definition records (design section 2).
//!
//! Key: `13h || task_id u64 BE`, id != 0. Value version 1, big-endian:
//!
//! ```text
//! 01 | u64 def_version (>= 1) | str name
//! str issuer | str executor | u8 has_approver [str approver]
//! u64 table_id (!= 0) | text visible_rows
//! u16 n_columns, ascending: str column | u8 mode (0 Readable, 1 Masked{None},
//!     2 Masked{Some}, 3 Hidden); mode 2: u8 tag (1 Int i64 | 2 Text text | 3 Bool u8)
//! u16 n_untrusted, ascending: str column
//! u8 has_write; if 1: u16 n, ascending str column | text old_row | text new_row
//! u64 distinct_rows | u64 transitions
//! u64 rows_read | u64 rows_returned | u64 bytes_returned
//! u64 statement_ms | u64 rows_scanned | u32 concurrent_statements | u64 lock_wait_ms
//! u64 pending_write_bytes | u32 stored_previews | u64 preview_ttl_s
//! u64 approval_threshold | i64 expires_at_ms | u8 revoked
//! u16 n_grants, ascending: str role | u8 bits
//! ```
//!
//! `str` is a u16 length and 1..=63 bytes of UTF-8; `text` is a u32 length and
//! at most 8192 bytes of UTF-8. The encoder never writes mode 3; the decoder
//! maps it to an absent entry. Every decoder rejection is `CoreError::Corrupt`.
use super::definition::{
    AllowedWrite, ColumnMode, DisclosureCaps, LoadLimits, MutationCaps, PredicateText, TaskDef,
    TaskName, TaskObjectId, TaskPrivileges, WallClockMs,
};
use crate::{CoreError, relational::Value};
use std::collections::{BTreeMap, BTreeSet};

/// Catalog key kind of a task definition.
pub(crate) const TASK_KIND: u8 = 19;
/// Longest `str` field, in bytes.
pub(crate) const MAX_STR: usize = 63;
/// Longest `text` field, in bytes.
pub(crate) const MAX_TEXT: usize = 8192;
/// Largest encoded record.
pub(crate) const MAX_RECORD: usize = 64 * 1024;
const VERSION: u8 = 1;

fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(format!("task record: {message}"))
}

#[cfg_attr(not(test), allow(dead_code))] // The task writer (R3-105) is the first caller.
pub(crate) fn task_key(id: u64) -> Vec<u8> {
    let mut key = vec![TASK_KIND];
    key.extend(id.to_be_bytes());
    key
}

/// The task id of a well-formed kind-19 key.
pub(crate) fn task_id(key: &[u8]) -> Result<u64, CoreError> {
    if key.len() != 9 || key[0] != TASK_KIND {
        return Err(corrupt("invalid key"));
    }
    let id = u64::from_be_bytes(key[1..9].try_into().unwrap());
    if id == 0 {
        return Err(corrupt("zero task id"));
    }
    Ok(id)
}

struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, n: usize) -> Result<(), CoreError> {
        let n = u16::try_from(n).map_err(|_| corrupt("too many entries"))?;
        self.0.extend(n.to_be_bytes());
        Ok(())
    }
    fn u32(&mut self, v: u32) {
        self.0.extend(v.to_be_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend(v.to_be_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend(v.to_be_bytes());
    }
    fn str(&mut self, s: &str) -> Result<(), CoreError> {
        if s.is_empty() || s.len() > MAX_STR {
            return Err(corrupt("name length"));
        }
        self.u16(s.len())?;
        self.0.extend(s.as_bytes());
        Ok(())
    }
    fn text(&mut self, s: &str) -> Result<(), CoreError> {
        if s.len() > MAX_TEXT {
            return Err(corrupt("text length"));
        }
        self.u32(s.len() as u32);
        self.0.extend(s.as_bytes());
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))] // The task writer (R3-105) is the first caller.
pub(crate) fn encode_task(def: &TaskDef) -> Result<Vec<u8>, CoreError> {
    let mut w = Writer(vec![VERSION]);
    w.u64(def.def_version);
    w.str(def.name.as_str())?;
    w.str(&def.issuer)?;
    w.str(&def.executor)?;
    match &def.approver {
        Some(approver) => {
            w.u8(1);
            w.str(approver)?;
        }
        None => w.u8(0),
    }
    w.u64(def.table);
    w.text(&def.visible_rows.0)?;
    let listed: Vec<_> = def
        .columns
        .iter()
        .filter(|(_, mode)| !matches!(mode, ColumnMode::Hidden))
        .collect();
    w.u16(listed.len())?;
    for (column, mode) in listed {
        w.str(column)?;
        match mode {
            ColumnMode::Readable => w.u8(0),
            ColumnMode::Masked { constant: None } => w.u8(1),
            ColumnMode::Masked { constant: Some(value) } => {
                w.u8(2);
                match value {
                    Value::Int(v) => {
                        w.u8(1);
                        w.i64(*v);
                    }
                    Value::Text(v) => {
                        w.u8(2);
                        w.text(v)?;
                    }
                    Value::Bool(v) => {
                        w.u8(3);
                        w.u8(u8::from(*v));
                    }
                    _ => return Err(corrupt("masked constant type")),
                }
            }
            ColumnMode::Hidden => unreachable!(),
        }
    }
    w.u16(def.untrusted_allowed.len())?;
    for column in &def.untrusted_allowed {
        w.str(column)?;
    }
    match &def.allowed_write {
        Some(write) => {
            w.u8(1);
            w.u16(write.columns.len())?;
            for column in &write.columns {
                w.str(column)?;
            }
            w.text(&write.old_row.0)?;
            w.text(&write.new_row.0)?;
        }
        None => w.u8(0),
    }
    w.u64(def.mutation_caps.distinct_rows);
    w.u64(def.mutation_caps.transitions);
    w.u64(def.disclosure_caps.rows_read);
    w.u64(def.disclosure_caps.rows_returned);
    w.u64(def.disclosure_caps.bytes_returned);
    let limits = &def.limits;
    w.u64(limits.statement_ms);
    w.u64(limits.rows_scanned);
    w.u32(limits.concurrent_statements);
    w.u64(limits.lock_wait_ms);
    w.u64(limits.pending_write_bytes);
    w.u32(limits.stored_previews);
    w.u64(limits.preview_ttl_s);
    w.u64(def.approval_threshold);
    w.i64(def.expires_at.0);
    w.u8(u8::from(def.revoked));
    w.u16(def.grants.len())?;
    for (role, privileges) in &def.grants {
        w.str(role)?;
        w.u8(privileges.bits());
    }
    // One set of rules: whatever is written must be accepted by the decoder.
    decode_task(&task_key(def.id.0), &w.0)?;
    Ok(w.0)
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CoreError> {
        if self.0.len() < n {
            return Err(corrupt("truncated"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, CoreError> {
        Ok(self.take(1)?[0])
    }
    fn flag(&mut self) -> Result<bool, CoreError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt("invalid flag")),
        }
    }
    fn u16(&mut self) -> Result<usize, CoreError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize)
    }
    fn u32(&mut self) -> Result<u32, CoreError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, CoreError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, CoreError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn str(&mut self) -> Result<String, CoreError> {
        let n = self.u16()?;
        if n == 0 || n > MAX_STR {
            return Err(corrupt("name length"));
        }
        let bytes = self.take(n)?;
        let s = std::str::from_utf8(bytes).map_err(|_| corrupt("invalid UTF-8"))?;
        if s.contains('\0') {
            return Err(corrupt("NUL in name"));
        }
        Ok(s.to_owned())
    }
    fn text(&mut self) -> Result<String, CoreError> {
        let n = self.u32()? as usize;
        if n > MAX_TEXT {
            return Err(corrupt("text length"));
        }
        let bytes = self.take(n)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("invalid UTF-8"))
    }
    /// Strictly ascending names: no duplicates, canonical order.
    fn names(&mut self) -> Result<BTreeSet<String>, CoreError> {
        let n = self.u16()?;
        let mut out = BTreeSet::new();
        let mut last: Option<String> = None;
        for _ in 0..n {
            let name = self.str()?;
            ascending(&mut last, &name)?;
            out.insert(name);
        }
        Ok(out)
    }
    fn nonzero(&mut self) -> Result<u64, CoreError> {
        match self.u64()? {
            0 => Err(corrupt("zero cap")),
            v => Ok(v),
        }
    }
    fn nonzero32(&mut self) -> Result<u32, CoreError> {
        match self.u32()? {
            0 => Err(corrupt("zero cap")),
            v => Ok(v),
        }
    }
}

fn ascending(last: &mut Option<String>, name: &str) -> Result<(), CoreError> {
    if last.as_deref().is_some_and(|prev| prev >= name) {
        return Err(corrupt("unsorted or duplicate entries"));
    }
    *last = Some(name.to_owned());
    Ok(())
}

pub(crate) fn decode_task(key: &[u8], value: &[u8]) -> Result<TaskDef, CoreError> {
    let id = task_id(key)?;
    if value.len() > MAX_RECORD {
        return Err(corrupt("record too large"));
    }
    let mut r = Reader(value);
    if r.u8()? != VERSION {
        return Err(corrupt("unsupported version"));
    }
    let def_version = r.u64()?;
    if def_version == 0 {
        return Err(corrupt("zero def_version"));
    }
    let name = TaskName::new(&r.str()?).map_err(|_| corrupt("invalid task name"))?;
    let issuer = r.str()?;
    let executor = r.str()?;
    let approver = if r.flag()? { Some(r.str()?) } else { None };
    let table = r.u64()?;
    if table == 0 {
        return Err(corrupt("zero table id"));
    }
    let visible_rows = PredicateText(r.text()?);
    let mut columns = BTreeMap::new();
    let mut last = None;
    for _ in 0..r.u16()? {
        let column = r.str()?;
        ascending(&mut last, &column)?;
        let mode = match r.u8()? {
            0 => ColumnMode::Readable,
            1 => ColumnMode::Masked { constant: None },
            2 => {
                let constant = match r.u8()? {
                    1 => Value::Int(r.i64()?),
                    2 => Value::Text(r.text()?),
                    3 => Value::Bool(r.flag()?),
                    _ => return Err(corrupt("unknown masked constant tag")),
                };
                ColumnMode::Masked { constant: Some(constant) }
            }
            3 => continue,
            _ => return Err(corrupt("unknown column mode")),
        };
        columns.insert(column, mode);
    }
    let untrusted_allowed = r.names()?;
    let allowed_write = if r.flag()? {
        let columns = r.names()?;
        let old_row = PredicateText(r.text()?);
        let new_row = PredicateText(r.text()?);
        Some(AllowedWrite { columns, old_row, new_row })
    } else {
        None
    };
    let mutation_caps = MutationCaps { distinct_rows: r.u64()?, transitions: r.u64()? };
    let has_write = allowed_write.is_some();
    let caps_zero = [mutation_caps.distinct_rows, mutation_caps.transitions].map(|c| c == 0);
    if (has_write && caps_zero.contains(&true)) || (!has_write && caps_zero.contains(&false)) {
        return Err(corrupt("mutation caps do not match the write clause"));
    }
    let disclosure_caps = DisclosureCaps {
        rows_read: r.nonzero()?,
        rows_returned: r.nonzero()?,
        bytes_returned: r.nonzero()?,
    };
    let limits = LoadLimits {
        statement_ms: r.nonzero()?,
        rows_scanned: r.nonzero()?,
        concurrent_statements: r.nonzero32()?,
        lock_wait_ms: r.nonzero()?,
        pending_write_bytes: r.nonzero()?,
        stored_previews: r.nonzero32()?,
        preview_ttl_s: r.nonzero()?,
    };
    let approval_threshold = r.u64()?;
    let expires_at = WallClockMs(r.i64()?);
    let revoked = r.flag()?;
    let mut grants = BTreeMap::new();
    let mut last = None;
    for _ in 0..r.u16()? {
        let role = r.str()?;
        ascending(&mut last, &role)?;
        let bits = r.u8()?;
        let privileges = TaskPrivileges::from_bits(bits)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| corrupt("invalid privilege bits"))?;
        grants.insert(role, privileges);
    }
    if !r.0.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(TaskDef {
        id: TaskObjectId(id),
        name,
        def_version,
        issuer,
        executor,
        approver,
        table,
        visible_rows,
        columns,
        untrusted_allowed,
        allowed_write,
        mutation_caps,
        disclosure_caps,
        limits,
        approval_threshold,
        expires_at,
        revoked,
        grants,
    })
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;


// ---------------------------------------------------------------------------
// Store half (R3-105): create, revoke, grant, purge and cached reads.
// ---------------------------------------------------------------------------

use super::definition::{NewTask, MAX_TASKS};
use crate::relational::{
    catalog_ext::{Action, ConstraintKind},
    DataType, SqlError,
};
use crate::storage::{
    engine::{
        catalog::{CatalogView, TableEntry},
        kv::Keyspace,
        locks::{LockKey, LockMode, TASK_CATALOG_LOCK_TABLE},
        meta::TASK_CATALOG,
        txn::{Isolation, Transaction, TxnError},
    },
    roles::{CatalogRole, RoleCatalog, RoleError, TablePrivilege},
    v2::V2Db,
};
use giggles_pg_types::temporal::{ParserSettings, TimestampTz};
use std::sync::Arc;

/// A task DDL failure: SQLSTATE and message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDdlError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl std::fmt::Display for TaskDdlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.sqlstate)
    }
}

impl std::error::Error for TaskDdlError {}

pub(crate) fn ddl(sqlstate: &'static str, message: impl Into<String>) -> TaskDdlError {
    TaskDdlError {
        sqlstate,
        message: message.into(),
    }
}

impl From<CoreError> for TaskDdlError {
    fn from(error: CoreError) -> Self {
        ddl(crate::sqlstate(&error), error.to_string())
    }
}

impl From<TxnError> for TaskDdlError {
    fn from(error: TxnError) -> Self {
        CoreError::from(error).into()
    }
}

impl From<SqlError> for TaskDdlError {
    fn from(error: SqlError) -> Self {
        CoreError::Sql(error).into()
    }
}

impl From<RoleError> for TaskDdlError {
    fn from(error: RoleError) -> Self {
        let sqlstate = match &error {
            RoleError::Duplicate => "42710",
            RoleError::Missing => "42704",
            RoleError::Dependent => "2BP01",
            RoleError::Invalid => "22023",
            RoleError::Limit => "54000",
            RoleError::Core(core) => crate::sqlstate(core),
        };
        ddl(sqlstate, error.to_string())
    }
}

/// Writes, reads and purges task definitions (catalog kind 19). Every writer
/// is a standalone DDL transaction, like role DDL.
#[derive(Clone)]
pub struct TaskCatalog {
    /// `None` on the legacy engine, where every method is 0A000 (D9).
    db: Option<Arc<V2Db>>,
    roles: RoleCatalog,
}

impl TaskCatalog {
    pub(crate) fn new(db: Option<Arc<V2Db>>, roles: RoleCatalog) -> Self {
        Self { db, roles }
    }

    pub(crate) fn db(&self) -> Result<&Arc<V2Db>, TaskDdlError> {
        self.db
            .as_ref()
            .ok_or_else(|| ddl("0A000", "tasks require the v2 storage engine"))
    }

    /// The committed catalog view now.
    pub(crate) fn view(&self) -> Result<Arc<CatalogView>, TaskDdlError> {
        let db = self.db()?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        Ok(db.catalog().committed_view(&mut txn)?)
    }

    /// Writer prologue, in the design lock order: the table in AccessShare
    /// (when given), the role lock shared, then the task catalog lock
    /// exclusive. The snapshot is refreshed after the last wait, as
    /// `roles_engine::write` does. Returns whether the table resolved.
    pub(crate) fn begin_write(&self, table: Option<&str>) -> Result<(Transaction, bool), TaskDdlError> {
        let db = self.db()?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        let resolved = table.is_some_and(|key| {
            db.catalog()
                .resolve(&mut txn, key, LockMode::AccessShare, None, &|| false)
                .is_ok()
        });
        crate::storage::roles::lock_shared(&mut txn, None, &|| false)?;
        txn.acquire_lock(
            LockKey::Table(TASK_CATALOG_LOCK_TABLE),
            LockMode::AccessExclusive,
            None,
            &|| false,
        )?;
        txn.begin_statement();
        Ok((txn, resolved))
    }

    fn write_record(&self, mut txn: Transaction, def: &TaskDef) -> Result<(), TaskDdlError> {
        let value = encode_task(def)?;
        txn.require_features(TASK_CATALOG);
        txn.put(Keyspace::Catalog, task_key(def.id.0), value)?;
        self.db()?.catalog().commit_ddl(txn)?;
        Ok(())
    }

    pub(crate) fn current(view: &CatalogView, name: &str) -> Result<TaskDef, TaskDdlError> {
        view.task_names
            .get(name)
            .and_then(|id| view.tasks.get(id))
            .map(|def| def.as_ref().clone())
            .ok_or_else(|| ddl("42704", format!("task \"{name}\" does not exist")))
    }

    fn role(&self, name: &str) -> Result<CatalogRole, TaskDdlError> {
        match self.roles.get(name) {
            Ok(Some(role)) => Ok(role),
            Ok(None) | Err(RoleError::Invalid) => {
                Err(ddl("42704", format!("role \"{name}\" does not exist")))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn may_administer(actor: &CatalogRole, def: &TaskDef) -> Result<(), TaskDdlError> {
        if actor.superuser || actor.name == def.issuer {
            Ok(())
        } else {
            Err(ddl("42501", format!("must be issuer of task \"{}\"", def.name)))
        }
    }

    /// CREATE TASK: validation rows 3 to 14 of design-1 section 5, in order;
    /// the first failure wins. The id comes from the shared catalog allocator.
    pub fn create(
        &self,
        issuer: &CatalogRole,
        new: NewTask,
        now: WallClockMs,
    ) -> Result<TaskDef, TaskDdlError> {
        let db = Arc::clone(self.db()?);
        let schema = new.schema.as_deref().unwrap_or("public");
        let key = crate::relational::relation_key(schema, &new.table);
        let (mut txn, resolved) = self.begin_write(Some(&key))?;
        let view = db.catalog().committed_view(&mut txn)?;
        let checked = Checked { view: &view, key: &key, resolved };
        let mut def = self.validate(issuer, &new, now, &checked, &db, &mut txn)?;
        def.id = TaskObjectId(db.sequences.next(0, 0)?);
        self.write_record(txn, &def)?;
        Ok(def)
    }

    /// A CREATE TASK draft (R3-502): the same locks and validation as
    /// [`Self::create`], so it fails with the same error, but nothing is
    /// allocated or written. The result has `id` 0 and `def_version` 0.
    pub fn draft(
        &self,
        issuer: &CatalogRole,
        new: &NewTask,
        now: WallClockMs,
    ) -> Result<TaskDef, TaskDdlError> {
        let db = Arc::clone(self.db()?);
        let schema = new.schema.as_deref().unwrap_or("public");
        let key = crate::relational::relation_key(schema, &new.table);
        let (mut txn, resolved) = self.begin_write(Some(&key))?;
        let view = db.catalog().committed_view(&mut txn)?;
        let checked = Checked { view: &view, key: &key, resolved };
        let mut def = self.validate(issuer, new, now, &checked, &db, &mut txn)?;
        // Dropping the transaction uncommitted releases its locks.
        drop(txn);
        def.def_version = 0;
        Ok(def)
    }

    fn validate(
        &self,
        issuer: &CatalogRole,
        new: &NewTask,
        now: WallClockMs,
        at: &Checked<'_>,
        db: &V2Db,
        txn: &mut Transaction,
    ) -> Result<TaskDef, TaskDdlError> {
        let view = at.view;
        // Row 3: name, duplicate, capacity.
        let name = TaskName::new(&new.name)
            .map_err(|_| ddl("42602", format!("invalid task name \"{}\"", new.name)))?;
        if view.task_names.contains_key(name.as_str()) {
            return Err(ddl("42710", format!("task \"{name}\" already exists")));
        }
        if view.tasks.len() >= MAX_TASKS {
            return Err(ddl("54000", "too many tasks"));
        }
        // Row 4: a base table, not a view.
        let entry = match view.by_name.get(at.key).filter(|_| at.resolved) {
            Some(entry) => Arc::clone(entry),
            None if view.sql_views.values().any(|v| v.name == new.table) => {
                return Err(ddl("42809", format!("\"{}\" is not a table", new.table)));
            }
            None => {
                return Err(ddl(
                    "42P01",
                    format!("relation \"{}\" does not exist", new.table),
                ));
            }
        };
        let table = &entry.def;
        // Row 5: roles exist.
        let executor = self.role(&new.executor)?;
        let approver = new.approver.as_deref().map(|a| self.role(a)).transpose()?;
        // Row 6: who may play which part.
        if issuer.name == executor.name {
            return Err(ddl("22023", "task issuer cannot be its executor"));
        }
        if !executor.task_only {
            return Err(ddl("22023", "task executor must be a task-only role"));
        }
        if issuer.task_only {
            return Err(ddl("22023", "a task-only role cannot issue tasks"));
        }
        if approver
            .as_ref()
            .is_some_and(|a| a.task_only || a.name == executor.name)
        {
            return Err(ddl(
                "22023",
                "task approver must be an ordinary role other than the executor",
            ));
        }
        // Row 7: below the cap, someone must approve.
        if approver.is_none() && new.approval_threshold < new.mutation_caps.distinct_rows {
            return Err(ddl(
                "22023",
                "an approver is required when APPROVE ABOVE is below the distinct-row cap",
            ));
        }
        // Row 8: the issuer holds what it delegates.
        let owner = self.roles.table_owner(at.key)?;
        if !issuer.superuser && owner.as_deref() != Some(issuer.name.as_str()) {
            let held = self.roles.table_privileges(at.key, &issuer.name)?;
            let write = new.allowed_write.is_some();
            if !held.contains(&TablePrivilege::Select)
                || (write && !held.contains(&TablePrivilege::Update))
            {
                return Err(ddl(
                    "42501",
                    format!("permission denied for table {}", new.table),
                ));
            }
        }
        // Row 9: columns exist, none listed twice; masked constants fit.
        let column = |name: &str| table.logical_columns().find(|c| c.name == name);
        let write_columns = new.allowed_write.iter().flat_map(|w| w.columns.iter());
        let named = new
            .readable
            .iter()
            .chain(new.masked.iter().map(|(c, _)| c))
            .chain(new.untrusted_text.iter())
            .chain(write_columns);
        for name in named {
            if column(name).is_none() {
                return Err(ddl("42703", format!("column \"{name}\" does not exist")));
            }
        }
        let twice = |name: &str| ddl("42701", format!("column \"{name}\" specified more than once"));
        let mut columns = BTreeMap::new();
        for name in &new.readable {
            if columns.insert(name.clone(), ColumnMode::Readable).is_some() {
                return Err(twice(name));
            }
        }
        for (name, constant) in &new.masked {
            let data_type = column(name).map(|c| c.data_type.clone());
            if constant
                .as_ref()
                .is_some_and(|value| !constant_fits(value, data_type.as_ref()))
            {
                return Err(ddl(
                    "42804",
                    format!("masked constant does not fit column \"{name}\""),
                ));
            }
            let mode = ColumnMode::Masked {
                constant: constant.clone(),
            };
            if columns.insert(name.clone(), mode).is_some() {
                return Err(twice(name));
            }
        }
        // Row 10: untrusted text and writable columns are readable.
        let readable = |name: &str| matches!(columns.get(name), Some(ColumnMode::Readable));
        let mut untrusted_allowed = BTreeSet::new();
        for name in &new.untrusted_text {
            let text = column(name).is_some_and(|c| is_text(&c.data_type));
            if !readable(name) || !text {
                return Err(ddl(
                    "22023",
                    format!("untrusted text column \"{name}\" must be a readable text column"),
                ));
            }
            untrusted_allowed.insert(name.clone());
        }
        if let Some(write) = &new.allowed_write {
            if let Some(name) = write.columns.iter().find(|c| !readable(c)) {
                return Err(ddl(
                    "22023",
                    format!("writable column \"{name}\" must be readable"),
                ));
            }
            // Row 11: D7.
            for name in &write.columns {
                if let Err(reason) = writable(view, &entry, name) {
                    return Err(ddl("0A000", format!("column \"{name}\" {reason}")));
                }
            }
        }
        // Row 12: predicates in the section 4 subset, deterministic.
        let image = db.catalog().snapshot(txn)?.image()?;
        let canon = |sql: &PredicateText| {
            crate::relational::task_predicate::canonical(&image, table, &sql.0)
                .map(PredicateText)
                .map_err(|error| match error {
                    SqlError::Unsupported => {
                        ddl("0A000", "task predicate is not a deterministic row condition")
                    }
                    other => other.into(),
                })
        };
        let visible_rows = canon(&new.visible_rows)?;
        let allowed_write = match &new.allowed_write {
            Some(write) => Some(AllowedWrite {
                columns: write.columns.clone(),
                old_row: canon(&write.old_row)?,
                new_row: canon(&write.new_row)?,
            }),
            None => None,
        };
        // Row 13: caps.
        if !caps_valid(new) {
            return Err(ddl("22023", "invalid task caps"));
        }
        // Row 14: expiry with an explicit offset, in the future.
        let expires_at = parse_expiry(&new.expires)?;
        if expires_at <= now {
            return Err(ddl("22023", "task expiry is not in the future"));
        }
        Ok(TaskDef {
            id: TaskObjectId(0),
            name,
            def_version: 1,
            issuer: issuer.name.clone(),
            executor: executor.name,
            approver: approver.map(|a| a.name),
            table: table.id,
            visible_rows,
            columns,
            untrusted_allowed,
            allowed_write,
            mutation_caps: new.mutation_caps,
            disclosure_caps: new.disclosure_caps,
            limits: new.limits,
            approval_threshold: new.approval_threshold,
            expires_at,
            revoked: false,
            grants: BTreeMap::new(),
        })
    }

    /// REVOKE TASK: issuer or superuser. Bumps def_version once; revoking a
    /// revoked task succeeds without a write.
    pub fn revoke(&self, actor: &CatalogRole, name: &str) -> Result<TaskDef, TaskDdlError> {
        let (mut txn, _) = self.begin_write(None)?;
        let view = self.db()?.catalog().committed_view(&mut txn)?;
        let mut def = Self::current(&view, name)?;
        Self::may_administer(actor, &def)?;
        if def.revoked {
            return Ok(def);
        }
        def.revoked = true;
        def.def_version = def
            .def_version
            .checked_add(1)
            .ok_or_else(|| ddl("54000", "task def_version exhausted"))?;
        self.write_record(txn, &def)?;
        Ok(def)
    }

    /// GRANT / REVOKE { UNDO | INSPECT } ON TASK: issuer or superuser. Grants
    /// are not definition changes and never bump def_version. APPROVE is not
    /// grantable in R3.
    pub fn grant(
        &self,
        actor: &CatalogRole,
        name: &str,
        role: &str,
        bits: TaskPrivileges,
        grant: bool,
    ) -> Result<(), TaskDdlError> {
        if bits.contains(TaskPrivileges::APPROVE) {
            return Err(ddl("0A000", "APPROVE on a task is not grantable"));
        }
        let (mut txn, _) = self.begin_write(None)?;
        let view = self.db()?.catalog().committed_view(&mut txn)?;
        let mut def = Self::current(&view, name)?;
        Self::may_administer(actor, &def)?;
        let target = self.role(role)?;
        if target.name == def.executor || target.task_only {
            return Err(ddl(
                "22023",
                "task privileges cannot be granted to a task-only role",
            ));
        }
        let held = def.grants.get(role).copied().unwrap_or_default();
        let next = if grant {
            held | bits
        } else {
            TaskPrivileges::from_bits(held.bits() & !bits.bits()).unwrap_or_default()
        };
        if next == held {
            return Ok(());
        }
        if next.is_empty() {
            def.grants.remove(role);
        } else {
            def.grants.insert(role.to_owned(), next);
        }
        self.write_record(txn, &def)
    }

    pub fn get(&self, name: &str) -> Result<Option<Arc<TaskDef>>, TaskDdlError> {
        let view = self.view()?;
        Ok(view
            .task_names
            .get(name)
            .and_then(|id| view.tasks.get(id))
            .cloned())
    }

    pub fn get_by_id(&self, id: TaskObjectId) -> Result<Option<Arc<TaskDef>>, TaskDdlError> {
        Ok(self.view()?.tasks.get(&id.0).cloned())
    }

    /// Every task, live and revoked, by id.
    pub fn list(&self) -> Result<Vec<Arc<TaskDef>>, TaskDdlError> {
        Ok(self.view()?.tasks.values().cloned().collect())
    }

    /// Deletes a task record. Phase 6 retention only; its name becomes reusable.
    #[cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))] // The retention sweeper (Phase 6) is the first caller.
    pub(crate) fn purge(&self, id: TaskObjectId) -> Result<(), TaskDdlError> {
        let (mut txn, _) = self.begin_write(None)?;
        let view = self.db()?.catalog().committed_view(&mut txn)?;
        if !view.tasks.contains_key(&id.0) {
            return Err(ddl("42704", "task does not exist"));
        }
        txn.require_features(TASK_CATALOG);
        txn.delete(Keyspace::Catalog, task_key(id.0))?;
        self.db()?.catalog().commit_ddl(txn)?;
        Ok(())
    }
}

/// The catalog state CREATE TASK validates against.
struct Checked<'a> {
    view: &'a CatalogView,
    /// The target's relation key (`name` or `schema\0name`).
    key: &'a str,
    /// The target resolved and is held in AccessShare.
    resolved: bool,
}

fn is_text(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Text | DataType::Varchar | DataType::Bpchar)
}

/// A masked constant is Int, Text or Bool and must fit its column's type.
fn constant_fits(value: &Value, data_type: Option<&DataType>) -> bool {
    let Some(data_type) = data_type else {
        return false;
    };
    match value {
        Value::Int(_) => matches!(
            data_type,
            DataType::SmallInt | DataType::Integer | DataType::BigInt
        ),
        Value::Text(_) => is_text(data_type),
        Value::Bool(_) => matches!(data_type, DataType::Boolean),
        _ => false,
    }
}

/// Row 13: caps and limits at least 1 (the threshold may be 0); a read-only
/// task has no mutation caps; transitions cover distinct rows; rows returned
/// do not exceed rows read.
fn caps_valid(new: &NewTask) -> bool {
    let caps = &new.disclosure_caps;
    let limits = &new.limits;
    let mutation = &new.mutation_caps;
    let positive = [
        caps.rows_read,
        caps.rows_returned,
        caps.bytes_returned,
        limits.statement_ms,
        limits.rows_scanned,
        u64::from(limits.concurrent_statements),
        limits.lock_wait_ms,
        limits.pending_write_bytes,
        u64::from(limits.stored_previews),
        limits.preview_ttl_s,
    ]
    .into_iter()
    .all(|v| v >= 1);
    let mutation_ok = match &new.allowed_write {
        Some(_) => mutation.distinct_rows >= 1 && mutation.transitions >= mutation.distinct_rows,
        None => mutation.distinct_rows == 0 && mutation.transitions == 0,
    };
    positive && mutation_ok && caps.rows_returned <= caps.rows_read
}

/// D7: whether a task may write `column` of `entry`'s table. Not a primary
/// key column, no sequence-backed default or identity, and not referenced by
/// an incoming foreign key whose ON UPDATE rewrites the referencing rows.
fn writable(view: &CatalogView, entry: &TableEntry, column: &str) -> Result<(), &'static str> {
    let table = &entry.def;
    let Some(index) = table.columns.iter().position(|c| c.name == column) else {
        return Err("does not exist");
    };
    if table.primary_key.contains(&index) {
        return Err("is a primary key column");
    }
    let id = table.columns[index].id;
    let sequence_backed = entry.columns_ext.get(&id).is_some_and(|ext| {
        ext.identity.is_some()
            || ext
                .default
                .as_deref()
                .is_some_and(|sql| sql.to_ascii_lowercase().contains("nextval("))
    });
    if sequence_backed {
        return Err("has a sequence-backed default");
    }
    let cascades = view
        .constraints
        .values()
        .flat_map(|c| c.values())
        .any(|constraint| {
            matches!(
                &constraint.kind,
                ConstraintKind::ForeignKey { ref_table, ref_columns, on_update, .. }
                    if *ref_table == table.id
                        && ref_columns.contains(&id)
                        && matches!(on_update, Action::Cascade | Action::SetNull | Action::SetDefault)
            )
        });
    if cascades {
        return Err("is referenced by a foreign key that rewrites rows on update");
    }
    Ok(())
}

/// Milliseconds from the Unix epoch to the PostgreSQL epoch (2000-01-01).
pub(crate) const PG_EPOCH_UNIX_MS: i64 = 946_684_800_000;

/// The EXPIRES literal: a finite timestamp with an explicit UTC offset (22007).
fn parse_expiry(text: &str) -> Result<WallClockMs, TaskDdlError> {
    let invalid = || ddl("22007", "EXPIRES needs a timestamp with an explicit offset");
    let at = |offset_east| {
        let settings = ParserSettings {
            offset_east,
            ..ParserSettings::default()
        };
        TimestampTz::parse(text, &settings)
            .map(TimestampTz::microseconds)
            .map_err(|_| invalid())
    };
    let utc = at(0)?;
    // Without an explicit offset the value follows the session offset.
    if utc != at(3600)? || utc == i64::MIN || utc == i64::MAX {
        return Err(invalid());
    }
    Ok(WallClockMs(utc.div_euclid(1000) + PG_EPOCH_UNIX_MS))
}

impl TaskDef {
    /// Bind-time recheck (design rows 9 to 11): the table and every column the
    /// task names still exist, and every writable column still satisfies D7.
    #[cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))] // Phases 2 and 3 call it at bind.
    pub(crate) fn check_against(&self, view: &CatalogView) -> Result<(), SqlError> {
        let entry = view.by_id.get(&self.table).ok_or(SqlError::UnknownTable)?;
        let known = |c: &str| entry.def.logical_columns().any(|col| col.name == c);
        let write_columns = self.allowed_write.iter().flat_map(|w| w.columns.iter());
        let mut named = self
            .columns
            .keys()
            .chain(self.untrusted_allowed.iter())
            .chain(write_columns);
        if !named.all(|c| known(c)) {
            return Err(SqlError::UnknownColumn);
        }
        if let Some(write) = &self.allowed_write {
            for column in &write.columns {
                writable(view, entry, column).map_err(|_| SqlError::Unsupported)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Dependency guards (R3-106, design-1 section 8): table DDL and role changes
// that would strand a task definition are refused with 2BP01.
// ---------------------------------------------------------------------------

use crate::relational::{ColumnDef, Relational, TableDef};
use crate::storage::{
    engine::{
        catalog::CatalogParts,
        codec::split_versioned,
        kv::{Direction, KvRead},
        txn::SerializationReason,
    },
    roles::RoleTable,
};
use std::{ops::Bound, time::Instant};

/// Bounds of every kind-19 key.
fn task_range() -> (Bound<&'static [u8]>, Bound<&'static [u8]>) {
    (Bound::Included(&[TASK_KIND]), Bound::Excluded(&[TASK_KIND + 1]))
}

fn dependent() -> CoreError {
    CoreError::Sql(SqlError::DependentObjects)
}

/// Whether `new` keeps `column`: the same column id with the same name and type.
fn keeps(new: &TableDef, column: &ColumnDef) -> bool {
    new.columns.iter().any(|kept| {
        kept.id == column.id && kept.name == column.name && kept.data_type == column.data_type
    })
}

/// The columns of `table` a task depends on: listed, untrusted, writable, and
/// read by any of its three predicates. `None` when a predicate no longer
/// binds against the old image; the caller then treats every column as
/// referenced (fail closed).
fn referenced_columns(def: &TaskDef, table: &TableDef, image: &Relational) -> Option<BTreeSet<String>> {
    let mut named: BTreeSet<String> = def
        .columns
        .keys()
        .chain(def.untrusted_allowed.iter())
        .chain(def.allowed_write.iter().flat_map(|w| w.columns.iter()))
        .cloned()
        .collect();
    let predicates = std::iter::once(&def.visible_rows)
        .chain(def.allowed_write.iter().flat_map(|w| [&w.old_row, &w.new_row]));
    for predicate in predicates {
        named.extend(crate::relational::task_predicate::columns(image, table, &predicate.0).ok()?);
    }
    Some(named)
}

/// Table DDL guard, called from `Catalog::apply_diff` after
/// `reconcile_tables`. A live (non-revoked) task blocks dropping its table and
/// dropping, renaming or retyping any column it references (2BP01). Columns
/// are matched by id between `old` and `new`. Adding columns, renaming the
/// table and changes to unreferenced columns pass.
///
/// Fast path: when no table loses or changes an existing column, or no kind-19
/// key exists at all, nothing is locked. Otherwise the task catalog lock is
/// taken (after the table and role locks the DDL already holds) and any task
/// record newer than the DDL's snapshot is a serialization failure.
pub(crate) fn check_table_ddl(
    txn: &mut Transaction,
    kv: &dyn KvRead,
    old: &CatalogParts,
    new: &CatalogParts,
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    let new_by_id: BTreeMap<u64, &TableDef> = new.tables.values().map(|t| (t.id, t)).collect();
    let affected: BTreeMap<u64, &TableDef> = old
        .tables
        .values()
        .filter(|table| match new_by_id.get(&table.id) {
            None => true,
            Some(next) => table.columns.iter().any(|c| !keeps(next, c)),
        })
        .map(|table| (table.id, table))
        .collect();
    if affected.is_empty() {
        return Ok(());
    }
    let (start, end) = task_range();
    if kv
        .range(Keyspace::Catalog, start, end, Direction::Forward)?
        .next()
        .is_none()
    {
        return Ok(());
    }
    txn.acquire_lock(
        LockKey::Table(TASK_CATALOG_LOCK_TABLE),
        LockMode::AccessExclusive,
        deadline,
        cancel,
    )?;
    // Never refresh the DDL's snapshot: a task committed after it would be
    // invisible to the check below, so it is a conflict instead.
    let snapshot = txn.snapshot_ts();
    for entry in kv.range(Keyspace::Catalog, start, end, Direction::Forward)? {
        let (key, _) = entry?;
        let (_, ts) = split_versioned(&key).map_err(CoreError::from)?;
        if ts > snapshot {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict).into());
        }
    }
    let entries: Vec<_> = txn
        .scan(Keyspace::Catalog, start, end, Direction::Forward, None)?
        .collect();
    let mut image: Option<Relational> = None;
    for entry in entries {
        let (key, value) = entry?;
        let def = decode_task(&key, &value)?;
        if def.revoked {
            continue;
        }
        let Some(table) = affected.get(&def.table) else {
            continue;
        };
        let Some(next) = new_by_id.get(&def.table) else {
            return Err(dependent());
        };
        if image.is_none() {
            let mut built =
                Relational::catalog_image(old.tables.clone(), old.ext.clone(), u64::MAX, u64::MAX)
                    .map_err(CoreError::Sql)?;
            built.catalog_views(&old.sql_views).map_err(CoreError::Sql)?;
            image = Some(built);
        }
        let referenced = image.as_ref().and_then(|image| referenced_columns(&def, table, image));
        let stranded = table.logical_columns().any(|column| {
            referenced.as_ref().is_none_or(|r| r.contains(&column.name)) && !keeps(next, column)
        });
        if stranded {
            return Err(dependent());
        }
    }
    Ok(())
}

/// Role change guard, called from `roles_engine::write` between the change and
/// staging. The writer holds the role lock exclusively and task writers hold
/// it shared, so the task records read here cannot change before commit.
///
/// - A role that any task, revoked or not, names as issuer, executor, approver
///   or grantee cannot be dropped (2BP01): a re-created role of the same name
///   would inherit the task's authority.
/// - A task executor cannot lose the task-only attribute (2BP01).
pub(crate) fn check_role_change(
    txn: &mut Transaction,
    before: &RoleTable,
    after: &RoleTable,
) -> Result<(), RoleError> {
    let before = before.role_records()?;
    let after = after.role_records()?;
    let dropped: BTreeSet<&str> = before
        .keys()
        .filter(|name| !after.contains_key(*name))
        .map(String::as_str)
        .collect();
    let cleared: BTreeSet<&str> = before
        .iter()
        .filter(|(name, role)| role.task_only && after.get(*name).is_some_and(|r| !r.task_only))
        .map(|(name, _)| name.as_str())
        .collect();
    if dropped.is_empty() && cleared.is_empty() {
        return Ok(());
    }
    let (start, end) = task_range();
    let entries: Vec<_> = txn
        .scan(Keyspace::Catalog, start, end, Direction::Forward, None)
        .map_err(CoreError::from)?
        .collect();
    for entry in entries {
        let (key, value) = entry.map_err(CoreError::from)?;
        let def = decode_task(&key, &value)?;
        let mut named = [Some(&def.issuer), Some(&def.executor), def.approver.as_ref()]
            .into_iter()
            .flatten()
            .chain(def.grants.keys());
        if named.any(|role| dropped.contains(role.as_str())) || cleared.contains(def.executor.as_str()) {
            return Err(RoleError::Dependent);
        }
    }
    Ok(())
}

impl TaskCatalog {
    /// The current definition named `name`, revoked or not; `None` when it
    /// does not exist or on the legacy engine (tasks are v2 only).
    pub(crate) fn lookup(&self, name: &str) -> Result<Option<Arc<TaskDef>>, CoreError> {
        let Some(db) = &self.db else {
            return Ok(None);
        };
        let mut txn = db.begin(Isolation::ReadCommitted);
        let view = db.catalog().committed_view(&mut txn)?;
        Ok(view.task_names.get(name).and_then(|id| view.tasks.get(id)).cloned())
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "store_tests.rs"]
mod store_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "dependency_tests.rs"]
mod dependency_tests;
