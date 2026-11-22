//! The read log of task sessions (GD-0061 R3-309, design-3 section 6).
//!
//! Every statement a task session runs gets one read-log record
//! `2A|task|read_seq`, including statements refused before they run (a
//! column-rule denial, a write outside the protected commit, a parse error,
//! or an `admit()` refusal the server reports with
//! [`SqlSession::log_refused_task_statement`]). The record stores a SHA-256
//! fingerprint of the full statement text, the first 1,024 bytes of the text
//! (cut at a character boundary) and its length; parameter values are never
//! stored, only their count.
//!
//! Life of a record:
//! - a statement that releases a page creates it, open, in its first durable
//!   debit commit (`release.rs`), and its end finalizes it in one small
//!   commit;
//! - a statement without a durable debit writes it once, at its end;
//! - a statement ends when its last handle goes (the released page of a
//!   buffered statement, the cursor), or at
//!   [`SqlSession::finish_task_statement`], whichever comes first;
//! - a record left open by a crash stays open; the totals still reconcile
//!   from the debit records `2B` ([`verify`]);
//! - finalizing never recreates a record the retention sweeper deleted.
//!
//! The finalize commit also adds the statement's withheld untrusted values to
//! the disclosure head (`withheld_values`), so the head's count equals the sum
//! over the finalized records.
use std::ops::Bound;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use super::admit::StatementClass;
use super::commit::{catalog_def, DefAt};
use super::definition::{DisclosureCaps, TaskPrivileges};
use super::error::TaskError;
use super::records::{
    debit_key, disclosure_head_key, read_log_key, task_prefix, DebitRecord, DisclosureHead,
    ReadColumn, ReadLogRecord, TaskObjectId, MAX_READ_LOG_TEXT, NS_DEBIT, NS_READ_LOG,
};
use super::release::{lock, lock_error, TaskSessionState};
use crate::relational::{SqlError, SqlResult, SqlRow, Value};
use crate::roles::CatalogRole;
use crate::storage::engine::kv::Direction;
use crate::storage::{
    CoreError, Isolation, Keyspace, SqlSession, Transaction, V2Db, PROTECTED_RECORDS,
};

/// Record outcome: still running, or left open by a crash.
pub const OUTCOME_OPEN: u8 = 0;
pub const OUTCOME_COMPLETE: u8 = 1;
pub const OUTCOME_ERROR: u8 = 2;
/// Refused: a column rule, `admit()`, a write outside the protected commit,
/// revocation or expiry.
pub const OUTCOME_DENIED: u8 = 3;
pub const OUTCOME_CANCELLED: u8 = 4;
pub const OUTCOME_BUDGET: u8 = 5;

/// Entry points (`SqlSession::set_task_entry`).
pub const ENTRY_PGWIRE_SIMPLE: u8 = 0;
pub const ENTRY_PGWIRE_EXTENDED: u8 = 1;
pub const ENTRY_NATIVE: u8 = 2;
pub const ENTRY_HTTP: u8 = 3;
pub const ENTRY_GRPC: u8 = 4;

/// `SHOW TASK READS` returns this many records unless told otherwise.
pub const SHOW_LIMIT_DEFAULT: u64 = 100;
/// The largest `LIMIT` `SHOW TASK READS` accepts.
pub const SHOW_LIMIT_MAX: u64 = 1024;

/// The stored form of a statement's text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    /// SHA-256 over the full text.
    pub sha256: [u8; 32],
    /// Length of the full text in bytes (saturating).
    pub len: u32,
    /// The first at most 1,024 bytes, cut at a character boundary.
    pub text: String,
}

/// The fingerprint and capped text of `text` (design-3 section 6).
pub fn fingerprint_and_cap(text: &str) -> Fingerprint {
    let mut end = text.len().min(MAX_READ_LOG_TEXT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Fingerprint {
        sha256: Sha256::digest(text.as_bytes()).into(),
        len: u32::try_from(text.len()).unwrap_or(u32::MAX),
        text: text[..end].to_owned(),
    }
}

/// The stored code of a statement class: its index in [`StatementClass::ALL`].
pub fn class_code(class: StatementClass) -> u8 {
    StatementClass::ALL
        .iter()
        .position(|c| *c == class)
        .and_then(|i| u8::try_from(i).ok())
        .unwrap_or(u8::MAX)
}

/// The class a stored code names.
pub fn class_of_code(code: u8) -> Option<StatementClass> {
    StatementClass::ALL.get(usize::from(code)).copied()
}

/// The outcome of a statement that failed with `sqlstate`.
pub fn outcome_of(sqlstate: &str) -> u8 {
    match sqlstate {
        // Column rules, admit(), writes outside the protected commit,
        // revocation, expiry.
        "42501" | "0A000" | "28T01" | "28T02" => OUTCOME_DENIED,
        "54T01" => OUTCOME_BUDGET,
        "57014" => OUTCOME_CANCELLED,
        _ => OUTCOME_ERROR,
    }
}

/// The name of an outcome code, as `SHOW TASK READS` prints it.
pub fn outcome_name(code: u8) -> &'static str {
    match code {
        OUTCOME_OPEN => "open",
        OUTCOME_COMPLETE => "complete",
        OUTCOME_ERROR => "error",
        OUTCOME_DENIED => "denied",
        OUTCOME_CANCELLED => "cancelled",
        OUTCOME_BUDGET => "budget",
        _ => "unknown",
    }
}

fn sqlstate_bytes(sqlstate: &str) -> [u8; 5] {
    let mut out = *b"XX000";
    if sqlstate.len() == 5 && sqlstate.is_ascii() {
        out.copy_from_slice(sqlstate.as_bytes());
    }
    out
}

/// One read-log record as stored, with its `read_seq` (for tests, the
/// exposure report and `SHOW TASK READS`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadRecord {
    pub read_seq: u64,
    pub wall_ms: u64,
    pub snapshot_ts: u64,
    pub credential_fingerprint: [u8; 32],
    pub def_version: u64,
    pub catalog_version: u64,
    pub entry: u8,
    pub class: u8,
    pub statement_sha256: [u8; 32],
    pub text_len: u32,
    pub text: String,
    pub param_count: u16,
    pub table_id: u64,
    /// (column id, position bits) of each column the statement referenced.
    pub columns: Vec<(u64, u16)>,
    pub rows_scanned: u64,
    pub charged_rows_read: u64,
    pub charged_rows_returned: u64,
    pub charged_bytes: u64,
    pub released_rows: u64,
    pub released_bytes: u64,
    pub masked_values: u64,
    pub withheld_untrusted: u64,
    pub untrusted_released: u64,
    pub pages: u32,
    pub outcome: u8,
    pub sqlstate: [u8; 5],
    pub left_rows_read: u64,
    pub left_rows_returned: u64,
    pub left_bytes: u64,
}

impl ReadRecord {
    fn from_stored(read_seq: u64, r: ReadLogRecord) -> Self {
        Self {
            read_seq,
            wall_ms: r.wall_ms,
            snapshot_ts: r.snapshot_ts,
            credential_fingerprint: r.credential_fingerprint,
            def_version: r.def_version,
            catalog_version: r.catalog_version,
            entry: r.entry,
            class: r.class,
            statement_sha256: r.statement_sha256,
            text_len: r.text_len,
            text: r.text,
            param_count: r.param_count,
            table_id: r.table_id,
            columns: r
                .columns
                .iter()
                .map(|c| (c.column_id, c.position_bits))
                .collect(),
            rows_scanned: r.rows_scanned,
            charged_rows_read: r.charged_rows_read,
            charged_rows_returned: r.charged_rows_returned,
            charged_bytes: r.charged_bytes,
            released_rows: r.released_rows,
            released_bytes: r.released_bytes,
            masked_values: r.masked_values,
            withheld_untrusted: r.withheld_untrusted,
            untrusted_released: r.untrusted_released,
            pages: r.pages,
            outcome: r.outcome,
            sqlstate: r.sqlstate,
            left_rows_read: r.left_rows_read,
            left_rows_returned: r.left_rows_returned,
            left_bytes: r.left_bytes,
        }
    }

    /// The SQLSTATE as text (`00000` for a statement that did not fail).
    pub fn sqlstate_text(&self) -> String {
        String::from_utf8_lossy(&self.sqlstate).into_owned()
    }
}

/// What a statement's start knows about it.
#[derive(Clone, Debug, Default)]
pub(crate) struct StatementInfo {
    pub(crate) entry: u8,
    pub(crate) class: u8,
    pub(crate) sha256: [u8; 32],
    pub(crate) text_len: u32,
    pub(crate) text: String,
    pub(crate) param_count: u16,
    pub(crate) columns: Vec<ReadColumn>,
}

/// A statement's running totals.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Totals {
    pub(crate) rows_scanned: u64,
    pub(crate) charged_rows_read: u64,
    pub(crate) charged_rows_returned: u64,
    pub(crate) charged_bytes: u64,
    pub(crate) released_rows: u64,
    pub(crate) released_bytes: u64,
    pub(crate) masked: u64,
    pub(crate) withheld: u64,
    pub(crate) untrusted_released: u64,
    /// Pages handed over.
    pub(crate) pages: u32,
}

/// One statement's place in the read log.
#[derive(Debug, Default)]
pub(crate) struct StatementLog {
    /// Taken by the statement's first durable write (debit or finalize).
    pub(crate) read_seq: Option<u64>,
    /// Debit records written (the next `2B` page number).
    pub(crate) debit_pages: u32,
    pub(crate) info: StatementInfo,
    pub(crate) totals: Totals,
    /// The first failure's outcome and SQLSTATE; `None` completes.
    pub(crate) outcome: Option<(u8, [u8; 5])>,
    /// Finalized, abandoned (an injected crash) or discarded: nothing more
    /// is written for it.
    pub(crate) done: bool,
}

impl StatementLog {
    /// The record as the statement stands: its fields and totals, outcome
    /// open. The writer fills the clock, snapshot, binding and budget fields.
    pub(crate) fn record(&self) -> ReadLogRecord {
        let t = &self.totals;
        ReadLogRecord {
            entry: self.info.entry,
            class: self.info.class,
            statement_sha256: self.info.sha256,
            text_len: self.info.text_len,
            text: self.info.text.clone(),
            param_count: self.info.param_count,
            columns: self.info.columns.clone(),
            rows_scanned: t.rows_scanned,
            charged_rows_read: t.charged_rows_read,
            charged_rows_returned: t.charged_rows_returned,
            charged_bytes: t.charged_bytes,
            released_rows: t.released_rows,
            released_bytes: t.released_bytes,
            masked_values: t.masked,
            withheld_untrusted: t.withheld,
            untrusted_released: t.untrusted_released,
            pages: t.pages,
            outcome: OUTCOME_OPEN,
            sqlstate: *b"00000",
            ..ReadLogRecord::default()
        }
    }
}

/// A running task statement. Its last handle going finalizes its record.
pub(crate) struct StatementEntry {
    state: Arc<TaskSessionState>,
    generation: Result<Option<crate::storage::GenerationTag>, CoreError>,
    /// The full text, put back on discard for the statement that replaces it.
    text: Option<Arc<str>>,
    pub(crate) log: Mutex<StatementLog>,
}

impl std::fmt::Debug for StatementEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementEntry")
            .field("log", &self.log)
            .finish_non_exhaustive()
    }
}

impl StatementEntry {
    /// Starts a statement of `state`'s session: takes the session's pending
    /// text and becomes its current statement.
    pub(crate) fn start(
        state: &Arc<TaskSessionState>,
        generation: Result<Option<crate::storage::GenerationTag>, CoreError>,
        class: StatementClass,
        param_count: usize,
    ) -> Arc<Self> {
        let text = lock(&state.pending_text).take();
        let print = fingerprint_and_cap(text.as_deref().unwrap_or(""));
        let info = StatementInfo {
            entry: state.entry(),
            class: class_code(class),
            sha256: print.sha256,
            text_len: print.len,
            text: print.text,
            param_count: u16::try_from(param_count).unwrap_or(u16::MAX),
            columns: Vec::new(),
        };
        let entry = Arc::new(Self {
            state: state.clone(),
            generation,
            text,
            log: Mutex::new(StatementLog {
                info,
                ..StatementLog::default()
            }),
        });
        *lock(&state.current) = Some(Arc::downgrade(&entry));
        entry
    }

    /// Records the columns the statement references (the output plan's refs).
    pub(crate) fn set_columns(&self, refs: &[(u64, u16)]) {
        lock(&self.log).info.columns = refs
            .iter()
            .map(|&(column_id, position_bits)| ReadColumn {
                column_id,
                position_bits,
            })
            .collect();
    }

    /// The statement failed with `error`; the first failure wins.
    pub(crate) fn fail(&self, error: &CoreError) {
        self.fail_with(crate::sqlstate(error));
    }

    /// The statement failed with `sqlstate`; the first failure wins.
    pub(crate) fn fail_with(&self, sqlstate: &str) {
        let mut log = lock(&self.log);
        if log.outcome.is_none() {
            log.outcome = Some((outcome_of(sqlstate), sqlstate_bytes(sqlstate)));
        }
    }

    /// The statement is replaced by another run of itself (a cursor that
    /// falls back to buffering): it writes nothing, and its text goes back
    /// to the session for the replacement.
    pub(crate) fn discard(&self) {
        let mut log = lock(&self.log);
        if !log.done {
            log.done = true;
            *lock(&self.state.pending_text) = self.text.clone();
        }
    }

    pub(crate) fn engine(&self) -> Result<Option<crate::storage::EngineLease>, CoreError> {
        // A dormant entry must not keep RocksDB open. Debit and finalization
        // take counted, no-wait continuations on the issuing generation.
        self.generation.as_ref().map_err(Clone::clone)?.as_ref()
            .map(|tag| tag.lease(crate::storage::Holder::Readlog)).transpose()
    }

    /// Ends the statement: writes its final record (no-op once done).
    pub(crate) fn finish(&self) -> Result<(), CoreError> {
        if let Some(db) = self.engine()? {
            finalize(&db, &self.state, &self.log);
        } else {
            lock(&self.log).done = true;
        }
        Ok(())
    }

}

impl Drop for StatementEntry {
    fn drop(&mut self) {
        // While unwinding, the record stays open, as after a crash.
        if !std::thread::panicking() {
            let _ = self.finish();
        }
    }
}

/// The task's disclosure caps (unbounded when the definition is gone).
fn caps(def: Option<&super::definition::TaskDef>) -> DisclosureCaps {
    def.map_or(
        DisclosureCaps {
            rows_read: u64::MAX,
            rows_returned: u64::MAX,
            bytes_returned: u64::MAX,
        },
        |def| def.disclosure_caps,
    )
}

/// The budget left after `head`, into `record`.
pub(crate) fn fill_left(record: &mut ReadLogRecord, caps: &DisclosureCaps, head: &DisclosureHead) {
    record.left_rows_read = caps.rows_read.saturating_sub(head.rows_read);
    record.left_rows_returned = caps.rows_returned.saturating_sub(head.rows_returned);
    record.left_bytes = caps.bytes_returned.saturating_sub(head.bytes_returned);
}

/// Writes the statement's final record, charging the session's pending
/// `rows_read` (saturating at the cap). A failure leaves the record as it
/// was (open, or absent) and the delta pending.
fn finalize(db: &V2Db, state: &TaskSessionState, log: &Mutex<StatementLog>) {
    let mut log = lock(log);
    if log.done {
        return;
    }
    log.done = true;
    log.totals.rows_scanned = log
        .totals
        .rows_scanned
        .saturating_add(state.take_rows_scanned());
    let rows_read = state.take_rows_read();
    if db.task_kernel_unsupported() || write_final(db, state, &mut log, rows_read).is_err() {
        state.add_rows_read(rows_read);
    }
}

fn write_final(
    db: &V2Db,
    state: &TaskSessionState,
    log: &mut StatementLog,
    pending_read: u64,
) -> Result<(), CoreError> {
    let ctx = state.ctx();
    let task = ctx.task;
    let now = db.wall_now_ms();
    let latest = catalog_def(db, task, DefAt::Latest)?;
    let wait = latest.as_ref().map_or(0, |def| def.limits.lock_wait_ms);
    let deadline = Instant::now().checked_add(Duration::from_millis(wait));
    let mut txn = db.begin(Isolation::ReadCommitted);
    let head_key = disclosure_head_key(task);
    // Not cancellable: a cancelled statement still records its end.
    txn.lock_kernel_key(&head_key, deadline, &|| false)
        .map_err(lock_error)?;
    txn.begin_statement();
    let def = catalog_def(db, task, DefAt::Snapshot(&mut txn))?;
    let caps = caps(def.as_deref());
    let mut head = match txn.get(Keyspace::Kernel, &head_key)? {
        Some(value) => DisclosureHead::decode(&value)?,
        None => DisclosureHead {
            next_read_seq: 1,
            ..DisclosureHead::default()
        },
    };
    let overflow = || CoreError::Corrupt("task disclosure head overflow".into());
    let rows_read = pending_read.min(caps.rows_read.saturating_sub(head.rows_read));
    // The open record, when the statement already has one; a swept one is
    // never recreated.
    let (read_seq, existing, first) = match log.read_seq {
        Some(read_seq) => {
            let stored = txn
                .get(Keyspace::Kernel, &read_log_key(task, read_seq))?
                .map(|value| ReadLogRecord::decode(&value))
                .transpose()?;
            (read_seq, stored, false)
        }
        None => {
            let read_seq = head.next_read_seq;
            head.next_read_seq = read_seq.checked_add(1).ok_or_else(overflow)?;
            (read_seq, None, true)
        }
    };
    let write_record = first || existing.is_some();
    if !write_record && rows_read == 0 {
        return Ok(());
    }
    if rows_read > 0 {
        head.rows_read = head.rows_read.checked_add(rows_read).ok_or_else(overflow)?;
        let debit = DebitRecord {
            wall_ms: now,
            rows_read,
            ..DebitRecord::default()
        };
        txn.put(
            Keyspace::Kernel,
            debit_key(task, read_seq, log.debit_pages),
            debit.encode(),
        )?;
    }
    if write_record {
        head.withheld_values = head
            .withheld_values
            .checked_add(log.totals.withheld)
            .ok_or_else(overflow)?;
        let mut record = log.record();
        record.charged_rows_read = record.charged_rows_read.saturating_add(rows_read);
        let (outcome, sqlstate) = log.outcome.unwrap_or((OUTCOME_COMPLETE, *b"00000"));
        record.outcome = outcome;
        record.sqlstate = sqlstate;
        record.credential_fingerprint = ctx.credential_fingerprint;
        record.def_version = ctx.bound_def_version;
        record.table_id = def.as_ref().map_or(0, |def| def.table);
        match &existing {
            Some(open) => {
                record.wall_ms = open.wall_ms;
                record.snapshot_ts = open.snapshot_ts;
                record.catalog_version = open.catalog_version;
            }
            None => {
                record.wall_ms = now;
                record.snapshot_ts = txn.current_snapshot_ts().map_or(0, |ts| ts.0);
                record.catalog_version = db.catalog().snapshot_version(&txn).map_or(0, |ts| ts.0);
            }
        }
        fill_left(&mut record, &caps, &head);
        txn.put(
            Keyspace::Kernel,
            read_log_key(task, read_seq),
            record.encode(),
        )?;
    }
    txn.put(Keyspace::Kernel, head_key, head.encode())?;
    txn.require_features(PROTECTED_RECORDS);
    db.catalog().commit_ddl(txn)?;
    log.read_seq = Some(read_seq);
    if rows_read > 0 {
        log.debit_pages = log.debit_pages.saturating_add(1);
        log.totals.charged_rows_read = log.totals.charged_rows_read.saturating_add(rows_read);
    }
    Ok(())
}

/// The reconciliation of one task's disclosure records (design-3 section 6).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reconciliation {
    pub head_rows_read: u64,
    pub head_rows_returned: u64,
    pub head_bytes_returned: u64,
    pub head_withheld: u64,
    /// Sums over the debit records `2B` (returned rows and bytes include the
    /// reserved amounts).
    pub debit_rows_read: u64,
    pub debit_rows_returned: u64,
    pub debit_bytes: u64,
    pub records: u64,
    /// Records still open (running, or left by a crash).
    pub open_records: u64,
    /// Withheld untrusted values over the finalized records.
    pub closed_withheld: u64,
    /// `read_seq` of finalized records whose charges differ from their debits.
    pub mismatched: Vec<u64>,
    /// Debit records whose read-log record is gone (the retention sweep).
    pub orphan_debits: u64,
}

impl Reconciliation {
    /// The head equals the sum of the debits, every finalized record equals
    /// the sum of its debits, and the head's withheld count equals the sum
    /// over the finalized records.
    pub fn ok(&self) -> bool {
        self.head_rows_read == self.debit_rows_read
            && self.head_rows_returned == self.debit_rows_returned
            && self.head_bytes_returned == self.debit_bytes
            && self.head_withheld == self.closed_withheld
            && self.mismatched.is_empty()
    }
}

/// A Kernel key and its value.
type Entry = (Vec<u8>, Vec<u8>);

fn scan_task(
    txn: &mut Transaction,
    ns: u8,
    task: TaskObjectId,
    after: Option<&[u8]>,
    limit: Option<usize>,
) -> Result<Vec<Entry>, CoreError> {
    let (start, end) = task_prefix(ns, task);
    let start = match after {
        Some(key) => Bound::Excluded(key),
        None => Bound::Included(start.as_slice()),
    };
    let rows = txn.scan(
        Keyspace::Kernel,
        start,
        Bound::Excluded(&end),
        Direction::Forward,
        limit,
    )?;
    let mut out = Vec::new();
    for entry in rows {
        out.push(entry?);
    }
    Ok(out)
}

fn seq_of(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[9..17].try_into().expect("task key carries read_seq"))
}

/// Checks the reconciliation invariant of `task` at `txn`'s snapshot.
pub(crate) fn verify(
    txn: &mut Transaction,
    task: TaskObjectId,
) -> Result<Reconciliation, CoreError> {
    let mut out = Reconciliation::default();
    if let Some(value) = txn.get(Keyspace::Kernel, &disclosure_head_key(task))? {
        let head = DisclosureHead::decode(&value)?;
        out.head_rows_read = head.rows_read;
        out.head_rows_returned = head.rows_returned;
        out.head_bytes_returned = head.bytes_returned;
        out.head_withheld = head.withheld_values;
    }
    // Per statement: (rows_read, rows_returned, bytes) over its debits.
    let mut per: std::collections::BTreeMap<u64, (u64, u64, u64)> = Default::default();
    for (key, value) in scan_task(txn, NS_DEBIT, task, None, None)? {
        let d = DebitRecord::decode(&value)?;
        let returned = d.rows_returned.saturating_add(d.reserved_rows);
        let bytes = d.bytes.saturating_add(d.reserved_bytes);
        out.debit_rows_read = out.debit_rows_read.saturating_add(d.rows_read);
        out.debit_rows_returned = out.debit_rows_returned.saturating_add(returned);
        out.debit_bytes = out.debit_bytes.saturating_add(bytes);
        let sum = per.entry(seq_of(&key)).or_default();
        sum.0 = sum.0.saturating_add(d.rows_read);
        sum.1 = sum.1.saturating_add(returned);
        sum.2 = sum.2.saturating_add(bytes);
    }
    for (key, value) in scan_task(txn, NS_READ_LOG, task, None, None)? {
        let record = ReadLogRecord::decode(&value)?;
        let read_seq = seq_of(&key);
        out.records += 1;
        let debits = per.remove(&read_seq).unwrap_or_default();
        if record.outcome == OUTCOME_OPEN {
            out.open_records += 1;
            continue;
        }
        out.closed_withheld = out
            .closed_withheld
            .saturating_add(record.withheld_untrusted);
        let charged = (
            record.charged_rows_read,
            record.charged_rows_returned,
            record.charged_bytes,
        );
        if charged != debits {
            out.mismatched.push(read_seq);
        }
    }
    out.orphan_debits = per.len() as u64;
    Ok(out)
}

/// The read-log records of `task` after `after` (exclusive), at most `limit`.
pub(crate) fn records(
    txn: &mut Transaction,
    task: TaskObjectId,
    after: Option<u64>,
    limit: Option<usize>,
) -> Result<Vec<ReadRecord>, CoreError> {
    let after = after.map(|seq| read_log_key(task, seq));
    scan_task(txn, NS_READ_LOG, task, after.as_deref(), limit)?
        .into_iter()
        .map(|(key, value)| {
            Ok(ReadRecord::from_stored(
                seq_of(&key),
                ReadLogRecord::decode(&value)?,
            ))
        })
        .collect()
}

impl crate::storage::Admin {
    /// The read-log records of `task` (v2 only). For tests and the exposure
    /// report.
    #[doc(hidden)]
    pub fn task_read_log(&self, task: TaskObjectId) -> Result<Vec<ReadRecord>, CoreError> {
        let db = self.sql_session().v2_lease(crate::storage::Holder::Readlog)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        records(&mut txn, task, None, None)
    }

    /// The reconciliation of `task`'s disclosure records (v2 only).
    pub fn task_read_verify(&self, task: TaskObjectId) -> Result<Reconciliation, CoreError> {
        let db = self.sql_session().v2_lease(crate::storage::Holder::Readlog)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        verify(&mut txn, task)
    }
}

/// The result columns of `SHOW TASK READS`.
pub const SHOW_COLUMNS: [&str; 30] = [
    "read_seq",
    "wall_ms",
    "snapshot_ts",
    "credential_fingerprint",
    "def_version",
    "catalog_version",
    "entry",
    "class",
    "statement_sha256",
    "text_len",
    "text",
    "param_count",
    "table_id",
    "columns",
    "rows_scanned",
    "charged_rows_read",
    "charged_rows_returned",
    "charged_bytes",
    "released_rows",
    "released_bytes",
    "masked_values",
    "withheld_untrusted",
    "untrusted_released",
    "pages",
    "outcome",
    "sqlstate",
    "left_rows_read",
    "left_rows_returned",
    "left_bytes",
    "class_name",
];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn int(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn show_row(r: &ReadRecord) -> SqlRow {
    let columns = r
        .columns
        .iter()
        .map(|(id, bits)| format!("{id}:{bits}"))
        .collect::<Vec<_>>()
        .join(",");
    let class = class_of_code(r.class).map_or_else(|| "unknown".to_string(), |c| format!("{c:?}"));
    SqlRow {
        values: vec![
            int(r.read_seq),
            int(r.wall_ms),
            int(r.snapshot_ts),
            Value::Text(hex(&r.credential_fingerprint)),
            int(r.def_version),
            int(r.catalog_version),
            int(u64::from(r.entry)),
            int(u64::from(r.class)),
            Value::Text(hex(&r.statement_sha256)),
            int(u64::from(r.text_len)),
            Value::Text(r.text.clone()),
            int(u64::from(r.param_count)),
            int(r.table_id),
            Value::Text(columns),
            int(r.rows_scanned),
            int(r.charged_rows_read),
            int(r.charged_rows_returned),
            int(r.charged_bytes),
            int(r.released_rows),
            int(r.released_bytes),
            int(r.masked_values),
            int(r.withheld_untrusted),
            int(r.untrusted_released),
            int(u64::from(r.pages)),
            Value::Text(outcome_name(r.outcome).into()),
            Value::Text(r.sqlstate_text()),
            int(r.left_rows_read),
            int(r.left_rows_returned),
            int(r.left_bytes),
            Value::Text(class),
        ],
        version: 0,
    }
}

/// `SHOW TASK READS <task> [AFTER <read_seq>] [LIMIT <n>]` for `role`.
///
/// Never in a task session (0A000). `role` must hold the task's inspect
/// privilege: its issuer, its approver, a role granted INSPECT, or a
/// superuser (42501 otherwise). `limit` defaults to 100 and may not exceed
/// 1,024 (22023). The caller wraps the result as `Released::synthetic`.
///
/// Deviation from the item text: core sessions carry no authenticated role,
/// so the caller passes the role it authenticated.
pub fn show_reads(
    session: &SqlSession,
    role: &CatalogRole,
    task: &str,
    after: Option<u64>,
    limit: Option<u64>,
) -> Result<SqlResult, CoreError> {
    if session.task().is_some() {
        return Err(TaskError::NotAllowedInTaskSession(StatementClass::Show).into());
    }
    let limit = limit.unwrap_or(SHOW_LIMIT_DEFAULT);
    if limit > SHOW_LIMIT_MAX {
        return Err(CoreError::Sql(SqlError::InvalidParameterValue));
    }
    let db = session.v2_lease(crate::storage::Holder::Readlog)?;
    let def = super::catalog::lookup_leased(Some(&db), task)?
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
    if !def.privileges_of(role).contains(TaskPrivileges::INSPECT) {
        return Err(CoreError::Policy(crate::Error::Scope));
    }
    let mut txn = db.begin(Isolation::ReadCommitted);
    let rows = records(&mut txn, def.id, after, Some(limit as usize))?;
    Ok(SqlResult {
        columns: SHOW_COLUMNS.iter().map(|c| c.to_string()).collect(),
        rows: rows.iter().map(show_row).collect(),
        affected: 0,
    })
}

impl SqlSession {
    /// Sets the entry point the session's next statements are logged with
    /// ([`ENTRY_PGWIRE_SIMPLE`] .. [`ENTRY_GRPC`]). No-op without a task.
    pub fn set_task_entry(&self, entry: u8) {
        if let Some(state) = &self.task_state {
            state.set_entry(entry);
        }
    }

    /// The text the next task statement is logged with. The SQL-text exits
    /// set it themselves; a caller that runs a prepared statement or opens
    /// a cursor sets it first. No-op without a task.
    pub fn set_task_statement_text(&self, text: &str) {
        if let Some(state) = &self.task_state {
            *lock(&state.pending_text) = Some(Arc::from(text));
        }
    }

    /// Logs a statement the caller refused before it reached core (an
    /// `admit()` refusal, a server-side parse error) as one finished record.
    /// No-op without a task.
    pub fn log_refused_task_statement(
        &self,
        text: &str,
        class: StatementClass,
        param_count: usize,
        error: &CoreError,
    ) {
        if self.task_state.is_some() {
            self.set_task_statement_text(text);
            self.log_unstarted_task_failure(class, param_count, error);
        }
    }

    /// Starts the read-log entry of a task statement; `None` without a task.
    pub(crate) fn start_task_statement(
        &self,
        class: StatementClass,
        param_count: usize,
    ) -> Option<Arc<StatementEntry>> {
        let state = self.task_state.as_ref()?;
        let generation = self.statement_lease().map(|db| db.as_deref().map(crate::storage::EngineLease::tag));
        Some(StatementEntry::start(state, generation, class, param_count))
    }

    pub(crate) fn start_task_statement_leased(
        &self, class: StatementClass, param_count: usize, db: Option<&crate::storage::EngineLease>,
    ) -> Option<Arc<StatementEntry>> {
        let state = self.task_state.as_ref()?;
        let generation = match db {
            Some(db) => Ok(Some(db.tag())),
            None if self.is_v2_engine() => Err(CoreError::Fenced),
            None => Ok(None),
        };
        Some(StatementEntry::start(state, generation, class, param_count))
    }

    /// Logs a failure of a statement whose entry never started (its pending
    /// text is still set): a parse or an abort error before the kernel.
    pub(crate) fn log_unstarted_task_failure(
        &self,
        class: StatementClass,
        param_count: usize,
        error: &CoreError,
    ) {
        let Some(state) = &self.task_state else {
            return;
        };
        if lock(&state.pending_text).is_none() {
            return;
        }
        if let Some(entry) = self.start_task_statement(class, param_count) {
            entry.fail(error);
            let _ = entry.finish();
        }
    }

    /// The session's current statement, if its entry is still running.
    pub(crate) fn current_task_statement(&self) -> Option<Arc<StatementEntry>> {
        let state = self.task_state.as_ref()?;
        let current = lock(&state.current).take();
        current.and_then(|weak: Weak<StatementEntry>| weak.upgrade())
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "readlog_tests.rs"]
mod tests;
