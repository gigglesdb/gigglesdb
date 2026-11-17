//! APPROVE and REVOKE APPROVAL (GD-0061 design-7 D7.5, R3-704): the `29`
//! approval record of a stored change preview.
//!
//! Both run in one ReadCommitted transaction under the task's authority
//! hold (`20 | task | 00`), taken with a direct kernel lock and followed by
//! a fresh statement snapshot, as the preview store does.
//!
//! APPROVE checks, in the D7.5 order (the first failure wins):
//! 1. the approve privilege on the task (42501);
//! 2. the role is not the executor (42501);
//! 3. when the task names an approver, the role is that approver (42501);
//! 4. the header exists, is open, unexpired and has kind 01 (`ApprovalStale`);
//! 5. the supplied digest equals the header's (`ApprovalStale`);
//! 6. the approval is not already consumed (`ApprovalStale`).
//!
//! It then writes the `29` record with state 01. Approving again with the
//! same role and digest while active writes nothing. APPROVE never tests
//! data freshness; apply does (D7.6).
//!
//! REVOKE APPROVAL is allowed to the approving role or the issuer and sets
//! state 02; a later APPROVE may make it active again. A consumed approval
//! can be neither revoked nor approved again (`ApprovalStale`).
//!
//! SHOW CHANGES and SHOW CHANGE (R3-705) need the approve or the inspect
//! privilege on the task; SHOW CHANGE also needs SELECT on the table, checked
//! as SQL checks it (superuser, owner or grant). The task's column modes
//! narrow the executor only, so the approver sees every column. Both read
//! one ReadCommitted snapshot and return an [`Unreleased`] page without a
//! release tag: they never run in a task session, so releasing it is a move.
use super::admit::StatementClass;
use super::commit::{catalog_def, engine, hold_error, role_error, unsupported, DefAt};
use super::definition::{RoleName, TaskDef, TaskName, TaskPrivileges};
use super::error::TaskError;
use super::catalog::PG_EPOCH_UNIX_MS;
use super::preview::{
    digest_hex, headers, stored_rows, PreviewHeader, PreviewId, PreviewKind, PreviewRow,
    STATE_APPLIED, STATE_OPEN,
};
use super::records::{
    approval_key, mutation_head_key, preview_key, preview_row_key, ApprovalRecord, TaskObjectId,
};
use super::release::Unreleased;
use crate::CoreError;
use crate::relational::{output_text, SqlError, SqlResult, SqlRow, TableDef, TimestampTz, Value};
use crate::storage::engine::codec;
use crate::storage::roles::{role_txn, table_owner_txn, table_privileges_txn, TablePrivilege};
use crate::storage::{Isolation, Keyspace, SqlSession, Transaction, V2Db};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `29` state: active.
pub(crate) const APPROVAL_ACTIVE: u8 = 1;
/// `29` state: revoked.
pub(crate) const APPROVAL_REVOKED: u8 = 2;
/// `29` state: consumed by an apply.
pub(crate) const APPROVAL_CONSUMED: u8 = 3;

/// 42501 insufficient_privilege, as the other task SHOW and DDL paths report it.
fn no_privilege() -> TaskError {
    CoreError::Policy(crate::Error::Scope).into()
}

/// APPROVE CHANGE `id` of `task` as `role`, for the preview whose digest is
/// `digest` (D7.5).
pub fn approve(
    session: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
    id: PreviewId,
    digest: [u8; 32],
) -> Result<(), TaskError> {
    let (db, def) = open(session, task)?;
    // 1-3 on the latest definition, before waiting on the hold.
    may_approve(&def, &mut db.begin(Isolation::ReadCommitted), role)?;
    let mut txn = hold(&db, &def)?;
    // 1-3 again at the statement snapshot taken after the hold.
    let def = snapshot_def(&db, def.id, &mut txn)?;
    may_approve(&def, &mut txn, role)?;
    // 4. The header: present, open, unexpired, a change preview.
    let now = db.wall_now_ms();
    let header = load_header(&mut txn, def.id, id)?.ok_or(TaskError::ApprovalStale)?;
    if header.kind != PreviewKind::Change as u8
        || header.state != STATE_OPEN
        || header.expires_ms <= now
    {
        return Err(TaskError::ApprovalStale);
    }
    // 5. The digest.
    if header.digest != digest {
        return Err(TaskError::ApprovalStale);
    }
    // 6. Not consumed; the same active approval again writes nothing.
    let current = load_approval(&mut txn, def.id, id)?;
    if let Some(current) = &current {
        if current.state == APPROVAL_CONSUMED {
            return Err(TaskError::ApprovalStale);
        }
        if current.state == APPROVAL_ACTIVE
            && current.approver == *role
            && current.digest == digest
        {
            return Ok(());
        }
    }
    let approver_oid = role_txn(&mut txn, role)
        .map_err(role_error)?
        .map_or(0, |role| role.oid);
    let record = ApprovalRecord {
        state: APPROVAL_ACTIVE,
        approver: role.clone(),
        approver_oid,
        digest,
        approved_ms: now,
        consumed_seq: 0,
    };
    put_and_commit(&db, txn, def.id, id, &record)
}

/// REVOKE APPROVAL of change `id` of `task` as `role` (D7.5): the approving
/// role or the issuer sets the approval to state 02.
pub fn revoke_approval(
    session: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
    id: PreviewId,
) -> Result<(), TaskError> {
    let (db, def) = open(session, task)?;
    may_revoke(&def, None, role)?;
    let mut txn = hold(&db, &def)?;
    let def = snapshot_def(&db, def.id, &mut txn)?;
    may_revoke(&def, None, role)?;
    let mut record = load_approval(&mut txn, def.id, id)?.ok_or(TaskError::ApprovalStale)?;
    may_revoke(&def, Some(&record), role)?;
    match record.state {
        APPROVAL_CONSUMED => Err(TaskError::ApprovalStale),
        APPROVAL_REVOKED => Ok(()),
        _ => {
            record.state = APPROVAL_REVOKED;
            put_and_commit(&db, txn, def.id, id, &record)
        }
    }
}

/// `SHOW CHANGES` columns (D7.5).
pub const CHANGES_COLUMNS: [&str; 8] = [
    "change_id",
    "kind",
    "state",
    "digest",
    "row_count",
    "created_at",
    "expires_at",
    "approved_by",
];

/// `SHOW CHANGE` columns (D7.5): one line per changed column.
pub const CHANGE_COLUMNS: [&str; 5] = ["row_no", "key", "column_name", "old_value", "new_value"];

/// Suffix of `key` when the row's version differs from the one previewed.
pub const CHANGED_SINCE_PREVIEW: &str = " (changed since preview)";

/// SHOW CHANGES OF TASK `task` as `role` (D7.5): one row per stored preview,
/// in id order: `(change_id, kind, state, digest, row_count, created_at,
/// expires_at, approved_by)`. `kind` is `change` or `undo`; `state` is
/// `applied`, `expired`, `approved` (an active approval) or `open`;
/// `approved_by` names the role of an active or consumed approval.
pub fn list_changes(
    session: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
) -> Result<Unreleased<SqlResult>, TaskError> {
    let (db, def) = open(session, task)?;
    let mut txn = db.begin(Isolation::ReadCommitted);
    may_show(&def, &mut txn, role)?;
    let now = db.wall_now_ms();
    let mut rows = Vec::new();
    for (id, header) in headers(&mut txn, def.id)? {
        let approval = load_approval(&mut txn, def.id, id)?;
        let state = if header.state == STATE_APPLIED {
            "applied"
        } else if header.expires_ms <= now {
            "expired"
        } else if approval.as_ref().is_some_and(|a| a.state == APPROVAL_ACTIVE) {
            "approved"
        } else {
            "open"
        };
        let kind = if header.kind == PreviewKind::Undo as u8 { "undo" } else { "change" };
        let approved_by = approval
            .filter(|a| matches!(a.state, APPROVAL_ACTIVE | APPROVAL_CONSUMED))
            .map_or(Value::Null, |a| Value::Text(a.approver));
        rows.push(SqlRow {
            values: vec![
                int(id.0)?,
                Value::Text(kind.into()),
                Value::Text(state.into()),
                Value::Text(digest_hex(&header.digest)),
                int(u64::from(header.counts[0]))?,
                timestamptz(header.created_ms)?,
                timestamptz(header.expires_ms)?,
                approved_by,
            ],
            version: 0,
        });
    }
    Ok(page(&CHANGES_COLUMNS, rows))
}

/// SHOW CHANGE `id` OF TASK `task` as `role` (D7.5), in long form: one line
/// `(row_no, key, column_name, old_value, new_value)` per changed column of
/// each stored row, in row order then column order; a row whose values do
/// not change gets one line with a NULL `column_name`. Values are rendered
/// with PostgreSQL's text output; `row_no` counts from 1.
///
/// Old values come from the current row only while its version is still
/// the one previewed. Otherwise (or when the row is gone) the key is
/// suffixed [`CHANGED_SINCE_PREVIEW`], `old_value` is NULL and every column
/// of the previewed row is listed, since what it changed is no longer known.
pub fn show_change(
    session: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
    id: PreviewId,
) -> Result<Unreleased<SqlResult>, TaskError> {
    let (db, def) = open(session, task)?;
    let mut txn = db.begin(Isolation::ReadCommitted);
    may_show(&def, &mut txn, role)?;
    let header = load_header(&mut txn, def.id, id)?
        .filter(|header| header.kind == PreviewKind::Change as u8)
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
    let view = db.catalog().committed_view(&mut txn)?;
    let entry = view
        .by_id
        .get(&header.table)
        .cloned()
        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
    may_select(&mut txn, &entry.def.name, role)?;
    let table = &entry.def;
    let width = table.columns.len();
    let offsets = table.logical_offsets();
    let mut lines = Vec::new();
    for row_no in 0..stored_rows(&header) {
        let key = preview_row_key(def.id, id.0, row_no);
        let Some(value) = txn.get(Keyspace::Kernel, &key).map_err(CoreError::from)? else {
            continue;
        };
        let row = PreviewRow::decode(&value)?;
        if row.class != 0 {
            continue;
        }
        let mut after = crate::storage::task_row_values(&row.after)?;
        after.resize(width, Value::Null);
        let current = current_row(&mut txn, &row)?
            .filter(|(version, _)| *version == row.version_read)
            .map(|(_, mut values)| {
                values.resize(width, Value::Null);
                values
            });
        let mut key = key_text(table, &after)?;
        if current.is_none() {
            key.push_str(CHANGED_SINCE_PREVIEW);
        }
        let row_no = int(u64::from(row_no) + 1)?;
        let mut changed = 0usize;
        for &column in &offsets {
            let new = &after[column];
            let old = match &current {
                Some(values) if values[column] == *new => continue,
                Some(values) => text(&values[column])?,
                None => Value::Null,
            };
            let name = Value::Text(table.columns[column].name.clone());
            lines.push(line(&row_no, &key, name, old, text(new)?));
            changed += 1;
        }
        if changed == 0 {
            lines.push(line(&row_no, &key, Value::Null, Value::Null, Value::Null));
        }
    }
    Ok(page(&CHANGE_COLUMNS, lines))
}

fn line(row_no: &Value, key: &str, column: Value, old: Value, new: Value) -> SqlRow {
    SqlRow {
        values: vec![row_no.clone(), Value::Text(key.to_owned()), column, old, new],
        version: 0,
    }
}

fn page(columns: &[&str], rows: Vec<SqlRow>) -> Unreleased<SqlResult> {
    Unreleased::new(
        SqlResult {
            columns: columns.iter().map(|c| (*c).to_owned()).collect(),
            rows,
            affected: 0,
        },
        None,
    )
}

fn int(n: u64) -> Result<Value, TaskError> {
    Ok(Value::Int(i64::try_from(n).map_err(|_| CoreError::Sql(SqlError::Overflow))?))
}

/// Unix milliseconds as a timestamptz.
fn timestamptz(ms: u64) -> Result<Value, TaskError> {
    let us = i64::try_from(ms)
        .ok()
        .and_then(|ms| ms.checked_sub(PG_EPOCH_UNIX_MS))
        .and_then(|ms| ms.checked_mul(1000))
        .ok_or(CoreError::Sql(SqlError::Overflow))?;
    let at = TimestampTz::from_microseconds(us).map_err(|_| CoreError::Sql(SqlError::Overflow))?;
    Ok(Value::TimestampTz(at))
}

/// A value in PostgreSQL's text output, NULL as NULL.
fn text(value: &Value) -> Result<Value, TaskError> {
    Ok(output_text(value)
        .map_err(CoreError::Sql)?
        .map_or(Value::Null, Value::Text))
}

/// `(a, b)=(1, 2)`, PostgreSQL's key detail spelling.
fn key_text(table: &TableDef, row: &[Value]) -> Result<String, TaskError> {
    let mut names = Vec::new();
    let mut values = Vec::new();
    for &column in &table.primary_key {
        names.push(table.columns[column].name.as_str());
        let value = output_text(&row[column]).map_err(CoreError::Sql)?;
        values.push(value.unwrap_or_else(|| "null".into()));
    }
    Ok(format!("({})=({})", names.join(", "), values.join(", ")))
}

/// The current version and values of the previewed row, or `None` when it
/// is gone.
fn current_row(
    txn: &mut Transaction,
    row: &PreviewRow,
) -> Result<Option<(u64, Vec<Value>)>, CoreError> {
    let mut key = row.table.to_be_bytes().to_vec();
    key.extend_from_slice(&row.pk);
    let Some(value) = txn.get(Keyspace::Rows, &key)? else {
        return Ok(None);
    };
    let record = codec::decode_row(&key, &value)?;
    Ok((!record.tombstone).then_some((record.row_version, record.values)))
}

/// SHOW CHANGES and SHOW CHANGE: the approve or the inspect privilege on
/// the task (42501).
fn may_show(def: &TaskDef, txn: &mut Transaction, role: &RoleName) -> Result<(), TaskError> {
    let held = match role_txn(txn, role).map_err(role_error)? {
        Some(record) => def.privileges_of(&record),
        None => TaskPrivileges::NONE,
    };
    if held.contains(TaskPrivileges::APPROVE) || held.contains(TaskPrivileges::INSPECT) {
        Ok(())
    } else {
        Err(no_privilege())
    }
}

/// SELECT on `table` as SQL allows it: a superuser, the owner or a SELECT
/// grant (the server's `table_privilege_allowed`), else 42501.
fn may_select(txn: &mut Transaction, table: &str, role: &RoleName) -> Result<(), TaskError> {
    let superuser = role_txn(txn, role)
        .map_err(role_error)?
        .is_some_and(|record| record.superuser);
    let owner = table_owner_txn(txn, table).map_err(role_error)?;
    if superuser
        || owner.as_deref() == Some(role.as_str())
        || table_privileges_txn(txn, table, role)
            .map_err(role_error)?
            .contains(&TablePrivilege::Select)
    {
        Ok(())
    } else {
        Err(no_privilege())
    }
}

/// The v2 database and the task's latest definition. Neither statement runs
/// inside a task session, on a cluster gateway or on a replicated store.
fn open(session: &SqlSession, task: &TaskName) -> Result<(Arc<V2Db>, Arc<TaskDef>), TaskError> {
    if session.task().is_some() {
        return Err(TaskError::NotAllowedInTaskSession(StatementClass::Other));
    }
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.task_kernel_unsupported() {
        return Err(unsupported());
    }
    let def = session
        .tasks()
        .lookup(task.as_str())?
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
    Ok((db, def))
}

/// Checks 1-3 of APPROVE; `txn` reads the role record at its snapshot.
fn may_approve(def: &TaskDef, txn: &mut Transaction, role: &RoleName) -> Result<(), TaskError> {
    let held = match role_txn(txn, role).map_err(role_error)? {
        Some(record) => def.privileges_of(&record),
        None => TaskPrivileges::NONE,
    };
    // 1. The approve privilege.
    if !held.contains(TaskPrivileges::APPROVE) {
        return Err(no_privilege());
    }
    // 2. Never the executor.
    if *role == def.executor {
        return Err(no_privilege());
    }
    // 3. The named approver. Phase 1 defines no membership-derived task
    // privilege, so only the approver itself qualifies.
    if def.approver.as_ref().is_some_and(|approver| approver != role) {
        return Err(no_privilege());
    }
    Ok(())
}

/// REVOKE APPROVAL: the issuer, the task's approver, or (once the record is
/// read) the role that approved. The executor never.
fn may_revoke(
    def: &TaskDef,
    record: Option<&ApprovalRecord>,
    role: &RoleName,
) -> Result<(), TaskError> {
    if *role == def.executor {
        return Err(no_privilege());
    }
    let allowed = *role == def.issuer
        || match record {
            Some(record) => record.approver == *role,
            None => def.approver.as_ref() == Some(role),
        };
    if allowed { Ok(()) } else { Err(no_privilege()) }
}

/// ReadCommitted, the hold on the mutation head, then a statement snapshot
/// taken after the wait (`authority::hold` refuses an RC transaction that
/// already has a snapshot).
fn hold(db: &Arc<V2Db>, def: &TaskDef) -> Result<Transaction, TaskError> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    let deadline = Instant::now().checked_add(Duration::from_millis(def.limits.lock_wait_ms));
    txn.lock_kernel_key(&mutation_head_key(def.id), deadline, &|| {
        crate::cancellation::check().is_err()
    })
    .map_err(hold_error)?;
    txn.begin_statement();
    Ok(txn)
}

/// The definition at the snapshot taken after the hold.
fn snapshot_def(
    db: &V2Db,
    task: TaskObjectId,
    txn: &mut Transaction,
) -> Result<Arc<TaskDef>, TaskError> {
    Ok(catalog_def(db, task, DefAt::Snapshot(txn))?
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?)
}

fn load_header(
    txn: &mut Transaction,
    task: TaskObjectId,
    id: PreviewId,
) -> Result<Option<PreviewHeader>, CoreError> {
    txn.get(Keyspace::Kernel, &preview_key(task, id.0))?
        .map(|value| PreviewHeader::decode(&value))
        .transpose()
}

fn load_approval(
    txn: &mut Transaction,
    task: TaskObjectId,
    id: PreviewId,
) -> Result<Option<ApprovalRecord>, CoreError> {
    txn.get(Keyspace::Kernel, &approval_key(task, id.0))?
        .map(|value| ApprovalRecord::decode(&value))
        .transpose()
}

fn put_and_commit(
    db: &Arc<V2Db>,
    mut txn: Transaction,
    task: TaskObjectId,
    id: PreviewId,
    record: &ApprovalRecord,
) -> Result<(), TaskError> {
    txn.put(Keyspace::Kernel, approval_key(task, id.0), record.encode())
        .map_err(CoreError::from)?;
    db.catalog().commit_ddl(txn).map_err(|e| engine(e.into()))?;
    Ok(())
}

/// One `29` record, as [`crate::storage::Admin::task_approval`] shows it.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredApproval {
    pub state: u8,
    pub approver: String,
    pub approver_oid: u32,
    pub digest: [u8; 32],
    pub approved_ms: u64,
    pub consumed_seq: u64,
}

impl crate::storage::Admin {
    /// The approval of preview `id` of `task` at the latest committed state
    /// (v2 only). For tests.
    #[doc(hidden)]
    pub fn task_approval(
        &self,
        task: TaskObjectId,
        id: PreviewId,
    ) -> Result<Option<StoredApproval>, CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        Ok(load_approval(&mut txn, task, id)?.map(|record| StoredApproval {
            state: record.state,
            approver: record.approver,
            approver_oid: record.approver_oid,
            digest: record.digest,
            approved_ms: record.approved_ms,
            consumed_seq: record.consumed_seq,
        }))
    }

    /// Test seam until apply (R3-706) lands: marks the approval of preview
    /// `id` consumed at `seq`, as apply's stage step does (D7.6 step 6).
    #[doc(hidden)]
    pub fn consume_task_approval(
        &self,
        task: TaskObjectId,
        id: PreviewId,
        seq: u64,
    ) -> Result<(), CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        let mut record = load_approval(&mut txn, task, id)?
            .ok_or_else(|| CoreError::Corrupt("no approval to consume".into()))?;
        record.state = APPROVAL_CONSUMED;
        record.consumed_seq = seq;
        txn.put(Keyspace::Kernel, approval_key(task, id.0), record.encode())?;
        db.catalog().commit_ddl(txn)?;
        Ok(())
    }

    /// Test seam until the undo preview (R3-802) lands: rewrites the stored
    /// header of preview `id` as kind 02 (task scope), keeping its digest.
    #[doc(hidden)]
    pub fn mark_task_preview_undo(&self, task: TaskObjectId, id: PreviewId) -> Result<(), CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        let mut header = load_header(&mut txn, task, id)?
            .ok_or_else(|| CoreError::Corrupt("no preview to mark".into()))?;
        header.kind = PreviewKind::Undo as u8;
        header.scope = Some(super::records::PreviewScope::default());
        txn.put(Keyspace::Kernel, preview_key(task, id.0), header.encode())?;
        db.catalog().commit_ddl(txn)?;
        Ok(())
    }
}
