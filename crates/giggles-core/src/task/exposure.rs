//! The exposure report of an existing task (GD-0061 R3-501, design-3
//! section 8): `SHOW TASK EXPOSURE <task>`, and of a draft (R3-502):
//! `SHOW TASK EXPOSURE FOR CREATE TASK ...`.
//!
//! One result of `(section, item, detail, value)` rows in a fixed order: the
//! definition (who, which table, each column's mode, the visible rows and
//! how many rows match them now, the caps, the allowed write, the load limits
//! and the listeners), then what the task was charged for reading (the
//! disclosure head `20|task|01` and the read log `2A`, with `readlog::verify`
//! supplying `reconciled`) and what it changed (the mutation head
//! `20|task|00`; zeros while that record is absent).
//!
//! The live count reads the task table at a fresh snapshot through
//! [`Scoped`] with the task's own predicate, as the caller: it needs SELECT
//! on the table (without it the value is NULL), it runs under the caller's
//! cancellation deadline, and it is never charged to the task (no read-log
//! record, no debit).
//!
//! The report needs the inspect privilege on the task (its issuer, its
//! approver, a role granted INSPECT, or a superuser; 42501 otherwise) and is
//! refused in a task session (0A000). The caller passes the role it
//! authenticated, as for `SHOW TASK READS`.
//!
//! A draft runs CREATE TASK's locks and validation with the caller as issuer
//! ([`TaskCatalog::draft`](super::catalog::TaskCatalog::draft)), stores
//! nothing, and reports the unsaved definition (`id` 0, `def_version` 0)
//! without the `charged` and `changed` sections. Any role that could run the
//! CREATE TASK may see it; a failed validation is CREATE TASK's own error.
use std::borrow::Cow;
use std::ops::Bound;

use super::admit::StatementClass;
use super::catalog::TaskDdlError;
use super::columns::ColumnMarks;
use super::definition::{ColumnMode, NewTask, TaskDef, TaskPrivileges, WallClockMs};
use super::error::TaskError;
use super::readlog::{records, verify};
use super::records::{disclosure_head_key, mutation_head_key, DisclosureHead, MutationHead};
use super::rows::{scope_of_def, Scoped, StatementMeter};
use crate::relational::scan::{Direction, KeyRange, RowSource, SourceRow, SourceRows};
use crate::relational::{output_text, SqlError, SqlResult, SqlRow, TableDef, TimestampTz, Value};
use crate::roles::{CatalogRole, TablePrivilege};
use crate::storage::engine::codec;
use crate::storage::engine::kv::Direction as KvDirection;
use crate::storage::roles::{table_owner_txn, table_privileges_txn};
use crate::storage::{CoreError, Isolation, Keyspace, SqlSession, Transaction};

/// The result columns of `SHOW TASK EXPOSURE`.
pub const COLUMNS: [&str; 4] = ["section", "item", "detail", "value"];

/// The fixed `listener` row: an executor is task-only (D6).
pub const LISTENER_DETAIL: &str = "task-only executor: agent listener only";

/// Rows the live count reads per engine page.
const COUNT_PAGE: usize = 512;

/// `SHOW TASK EXPOSURE <task>` for `role`: looks the task up by name, then
/// runs [`report`]. An unknown task is 42704.
pub fn show_exposure(
    session: &SqlSession,
    role: &CatalogRole,
    task: &str,
    extra: &[(String, String)],
) -> Result<SqlResult, CoreError> {
    refuse_in_task(session)?;
    let db = session.statement_lease()?;
    let def = super::catalog::lookup_leased(db.as_deref(), task)?
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
    if !def.privileges_of(role).contains(TaskPrivileges::INSPECT) {
        return Err(CoreError::Policy(crate::Error::Scope));
    }
    build(db.as_deref().ok_or(CoreError::Unsupported)?, role, &def, extra, Kind::Saved)
}

/// The exposure report of `def` for `role`. `extra` holds the server's
/// `(item, detail)` rows for the `listener` section, one per credential
/// bound to the task. The caller wraps the result as `Released::synthetic`.
pub fn report(
    session: &SqlSession,
    role: &CatalogRole,
    def: &TaskDef,
    extra: &[(String, String)],
) -> Result<SqlResult, CoreError> {
    refuse_in_task(session)?;
    if !def.privileges_of(role).contains(TaskPrivileges::INSPECT) {
        return Err(CoreError::Policy(crate::Error::Scope));
    }
    let db = session.v2_lease(crate::storage::Holder::TaskRun)?;
    build(&db, role, def, extra, Kind::Saved)
}

/// `SHOW TASK EXPOSURE FOR CREATE TASK ...` for `role`: validates `new` as
/// CREATE TASK would with `role` as issuer at `now`, stores nothing, and
/// reports the draft. `extra` is as for [`report`]. Every failure, including
/// validation, is reported as CREATE TASK reports it.
pub fn draft_exposure(
    session: &SqlSession,
    role: &CatalogRole,
    new: &NewTask,
    now: WallClockMs,
    extra: &[(String, String)],
) -> Result<SqlResult, TaskDdlError> {
    refuse_in_task(session)?;
    let catalog = session.tasks();
    let db = catalog.db()?;
    let def = catalog.draft_leased(&db, role, new, now)?;
    Ok(build(&db, role, &def, extra, Kind::Draft)?)
}

/// Whether the definition is stored; a draft has nothing charged or changed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Saved,
    Draft,
}

fn build(
    db: &crate::storage::EngineLease,
    role: &CatalogRole,
    def: &TaskDef,
    extra: &[(String, String)],
    kind: Kind,
) -> Result<SqlResult, CoreError> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    let image = db.catalog().snapshot(&mut txn)?.image().map_err(CoreError::Sql)?;
    let view = db.catalog().committed_view(&mut txn)?;
    let entry = view
        .by_id
        .get(&def.table)
        .cloned()
        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
    let table = image
        .tables
        .values()
        .find(|table| table.id == def.table)
        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
    let marks = ColumnMarks::from_labels(table, &view.column_labels);

    let mut out = Rows::default();
    // task
    out.text("task", "name", def.name.as_str());
    out.text("task", "issuer", &def.issuer);
    out.text("task", "executor", &def.executor);
    out.push("task", "approver", def.approver.clone(), None);
    out.push(
        "task",
        "expires_at",
        timestamptz_text(def.expires_at.0),
        Some(def.expires_at.0),
    );
    out.number("task", "revoked", u64::from(def.revoked));
    out.number("task", "def_version", def.def_version);
    // table
    let (schema, bare) = crate::relational::split_key(&entry.def.name);
    let name = if schema == "public" {
        bare.to_owned()
    } else {
        format!("{schema}.{bare}")
    };
    out.push("table", &name, None, None);
    // column, in the table's logical order
    for column in table.logical_columns() {
        out.text("column", &column.name, &column_detail(def, &marks, &column.name));
    }
    // visible_rows
    out.text("visible_rows", "predicate", &def.visible_rows.0);
    let live = if may_select(&mut txn, role, &entry.def.name)? {
        Some(live_count(&mut txn, &image, table, def)?)
    } else {
        None
    };
    out.push(
        "visible_rows",
        "matching_now",
        live.is_none().then(|| "requires SELECT on the table".to_owned()),
        live.map(clamp),
    );
    // disclosure_cap, mutation_cap
    let d = def.disclosure_caps;
    out.number("disclosure_cap", "rows_read", d.rows_read);
    out.number("disclosure_cap", "rows_returned", d.rows_returned);
    out.number("disclosure_cap", "bytes_returned", d.bytes_returned);
    let m = def.mutation_caps;
    out.number("mutation_cap", "distinct_rows", m.distinct_rows);
    out.number("mutation_cap", "transitions", m.transitions);
    // write: NULL predicates for a read-only task
    let write = def.allowed_write.as_ref();
    out.push("write", "old_row", write.map(|w| w.old_row.0.clone()), None);
    out.push("write", "new_row", write.map(|w| w.new_row.0.clone()), None);
    // load_limit
    let l = def.limits;
    out.number("load_limit", "statement_ms", l.statement_ms);
    out.number("load_limit", "rows_scanned", l.rows_scanned);
    out.number("load_limit", "concurrent_statements", u64::from(l.concurrent_statements));
    out.number("load_limit", "lock_wait_ms", l.lock_wait_ms);
    out.number("load_limit", "pending_write_bytes", l.pending_write_bytes);
    out.number("load_limit", "stored_previews", u64::from(l.stored_previews));
    out.number("load_limit", "preview_ttl_s", l.preview_ttl_s);
    // listener
    out.text("listener", "agent", LISTENER_DETAIL);
    for (item, detail) in extra {
        out.text("listener", item, detail);
    }
    if kind == Kind::Draft {
        return Ok(out.result());
    }
    // charged: the disclosure head and the read log
    let head = match txn.get(Keyspace::Kernel, &disclosure_head_key(def.id))? {
        Some(value) => DisclosureHead::decode(&value)?,
        None => DisclosureHead::default(),
    };
    let log = records(&mut txn, def.id, None, None)?;
    let untrusted_released = log
        .iter()
        .fold(0u64, |sum, r| sum.saturating_add(r.untrusted_released));
    let reconciled = verify(&mut txn, def.id)?.ok();
    out.number("charged", "rows_read", head.rows_read);
    out.number("charged", "rows_returned", head.rows_returned);
    out.number("charged", "bytes_returned", head.bytes_returned);
    out.number("charged", "statements", log.len() as u64);
    out.number("charged", "untrusted_released", untrusted_released);
    out.number("charged", "withheld_untrusted", head.withheld_values);
    out.number("charged", "reconciled", u64::from(reconciled));
    // changed: the mutation head (zeros while absent)
    let mutation = match txn.get(Keyspace::Kernel, &mutation_head_key(def.id))? {
        Some(value) => MutationHead::decode(&value)?,
        None => MutationHead::default(),
    };
    out.number("changed", "distinct_rows", mutation.distinct_rows);
    out.number("changed", "transitions", mutation.transitions);
    // Seqs start at 1: every committed operation (change or undo) took one.
    out.number("changed", "operations", mutation.next_seq.saturating_sub(1));
    Ok(out.result())
}

fn refuse_in_task(session: &SqlSession) -> Result<(), CoreError> {
    if session.task().is_some() {
        return Err(TaskError::NotAllowedInTaskSession(StatementClass::Show).into());
    }
    Ok(())
}

/// `readable`, `masked` or `hidden`, then `writable`, then
/// `untrusted:allowed` or `untrusted:withheld` for a marked column the task
/// can see, separated by `, `.
fn column_detail(def: &TaskDef, marks: &ColumnMarks, column: &str) -> String {
    let mode = def.columns.get(column).cloned().unwrap_or(ColumnMode::Hidden);
    let mut parts = vec![match mode {
        ColumnMode::Readable => "readable",
        ColumnMode::Masked { .. } => "masked",
        ColumnMode::Hidden => "hidden",
    }];
    if def
        .allowed_write
        .as_ref()
        .is_some_and(|w| w.columns.contains(column))
    {
        parts.push("writable");
    }
    if mode != ColumnMode::Hidden && marks.is_untrusted(column) {
        parts.push(if def.untrusted_allowed.contains(column) {
            "untrusted:allowed"
        } else {
            "untrusted:withheld"
        });
    }
    parts.join(", ")
}

/// SELECT on `table` as SQL allows it: a superuser, the owner or a grant.
fn may_select(txn: &mut Transaction, role: &CatalogRole, table: &str) -> Result<bool, CoreError> {
    if role.superuser {
        return Ok(true);
    }
    let role_error = |e| CoreError::from(super::commit::role_error(e));
    if table_owner_txn(txn, table).map_err(role_error)?.as_deref() == Some(role.name.as_str()) {
        return Ok(true);
    }
    Ok(table_privileges_txn(txn, table, &role.name)
        .map_err(role_error)?
        .contains(&TablePrivilege::Select))
}

/// The task table's rows that match the task's visible-rows predicate at
/// `txn`'s snapshot, counted through [`Scoped`] page by page. Nothing is
/// charged or logged.
fn live_count(
    txn: &mut Transaction,
    image: &crate::relational::Relational,
    table: &TableDef,
    def: &TaskDef,
) -> Result<u64, CoreError> {
    let scope = scope_of_def(image, def)?;
    let meter = StatementMeter::default();
    let prefix = table.id.to_be_bytes();
    let end = table
        .id
        .checked_add(1)
        .map(|next| next.to_be_bytes().to_vec());
    let mut after: Option<Vec<u8>> = None;
    loop {
        crate::cancellation::check()?;
        let start = match &after {
            Some(key) => Bound::Excluded(key.as_slice()),
            None => Bound::Included(prefix.as_slice()),
        };
        let stop = match &end {
            Some(end) => Bound::Excluded(end.as_slice()),
            None => Bound::Unbounded,
        };
        let mut page = Vec::new();
        for entry in txn.scan(Keyspace::Rows, start, stop, KvDirection::Forward, Some(COUNT_PAGE))? {
            let (key, value) = entry?;
            let record = codec::decode_row(&key, &value)?;
            page.push((key, record));
        }
        let Some((last, _)) = page.last() else {
            break;
        };
        after = Some(last.clone());
        let full = page.len() == COUNT_PAGE;
        let rows = Page(
            page.into_iter()
                .filter(|(_, record)| !record.tombstone)
                .map(|(key, record)| {
                    let mut values = record.values;
                    values.resize(table.columns.len(), Value::Null);
                    (
                        key[prefix.len()..].to_vec(),
                        SqlRow {
                            values,
                            version: record.row_version,
                        },
                    )
                })
                .collect(),
        );
        let scoped = Scoped::new(&rows, &scope, &meter);
        let all = KeyRange {
            lo: Bound::Unbounded,
            hi: Bound::Unbounded,
        };
        for row in scoped.scan(table, &all, Direction::Forward).map_err(CoreError::Sql)? {
            row.map_err(CoreError::Sql)?;
        }
        if let Some(error) = scoped.take_error() {
            return Err(error.into());
        }
        if !full {
            break;
        }
    }
    Ok(meter.counts().read)
}

/// One engine page of decoded rows of the task table, served as a row
/// source so the count goes through [`Scoped`].
struct Page(Vec<(Vec<u8>, SqlRow)>);

impl RowSource for Page {
    fn scan<'s>(&'s self, _table: &TableDef, _range: &KeyRange, _dir: Direction) -> Result<SourceRows<'s>, SqlError> {
        Ok(Box::new(self.0.iter().map(|(key, row)| {
            Ok(SourceRow {
                key: Cow::Borrowed(key.as_slice()),
                row: Cow::Borrowed(row),
            })
        })))
    }

    fn get<'s>(&'s self, _table: &TableDef, key: &[u8]) -> Result<Option<SourceRow<'s>>, SqlError> {
        Ok(self.0.iter().find(|(k, _)| k == key).map(|(key, row)| SourceRow {
            key: Cow::Borrowed(key.as_slice()),
            row: Cow::Borrowed(row),
        }))
    }
}

/// The report's rows.
#[derive(Default)]
struct Rows(Vec<SqlRow>);

impl Rows {
    fn push(&mut self, section: &str, item: &str, detail: Option<String>, value: Option<i64>) {
        self.0.push(SqlRow {
            values: vec![
                Value::Text(section.to_owned()),
                Value::Text(item.to_owned()),
                detail.map_or(Value::Null, Value::Text),
                value.map_or(Value::Null, Value::Int),
            ],
            version: 0,
        });
    }

    fn text(&mut self, section: &str, item: &str, detail: &str) {
        self.push(section, item, Some(detail.to_owned()), None);
    }

    fn number(&mut self, section: &str, item: &str, value: u64) {
        self.push(section, item, None, Some(clamp(value)));
    }

    fn result(self) -> SqlResult {
        SqlResult {
            columns: COLUMNS.iter().map(|c| c.to_string()).collect(),
            rows: self.0,
            affected: 0,
        }
    }
}

fn clamp(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Unix milliseconds in PostgreSQL's timestamptz text form, or `None` when
/// out of range.
fn timestamptz_text(ms: i64) -> Option<String> {
    let us = ms
        .checked_sub(super::catalog::PG_EPOCH_UNIX_MS)?
        .checked_mul(1000)?;
    let at = TimestampTz::from_microseconds(us).ok()?;
    output_text(&Value::TimestampTz(at)).ok().flatten()
}
