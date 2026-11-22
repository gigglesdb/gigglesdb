//! Spillable v2 statement output and sorts (PostgreSQL's tuplestore and
//! external sort past work_mem).
//!
//! Rows stay in memory up to `SPILL_BYTES`, then move to a private temp file
//! that is deleted on drop, so error, rollback and unwind paths clean up.
//! A completed spill reaches the caller through a thread-local slot: the first
//! outermost statement inside [`streaming`] that spills leaves it for that
//! caller; nested or later statements, and every other SqlSession caller, get
//! the rows materialised into `SqlResult::rows` (never truncated), except that
//! inside [`bounded`] output past the result budget fails with 54000.
//! [`sort_external`] keeps `SORT_BYTES` of sort input in memory, writes sorted
//! runs past that and merges them, stable in input order.
use super::{SqlError, SqlResult, SqlRow, scan, searched};
use crate::CoreError;
use serde::{Serialize, de::DeserializeOwned};
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

/// In-memory threshold before rows move to disk.
pub const SPILL_BYTES: usize = super::MAX_RESULT_BYTES;
/// Sort input kept in memory before sorted runs spill (PostgreSQL's default work_mem).
pub const SORT_BYTES: usize = 4 * 1024 * 1024;
/// Sort memory before a run spills: work_mem, but at most an eighth of the
/// statement retention ledger so pages, group state and other sorters fit.
pub(crate) fn work_mem() -> usize {
    SORT_BYTES.min(scan::retention_limit() / 8)
}
/// Runs merged at once; more runs are first merged into longer runs.
const MERGE_FAN_IN: usize = 64;

thread_local! {
    static STREAMING: Cell<bool> = const { Cell::new(false) };
    static TAGGED: Cell<bool> = const { Cell::new(false) };
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static BOUNDED: Cell<bool> = const { Cell::new(false) };
    static RETURNING_DEPTH: Cell<usize> = const { Cell::new(0) };
    static SLOT: RefCell<Option<SpilledRows>> = const { RefCell::new(None) };
    static ERROR: RefCell<Option<CoreError>> = const { RefCell::new(None) };
    static DIR: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    static QUERY_ROW_LIMIT: Cell<Option<usize>> = const { Cell::new(None) };
    static IN_HASH_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

/// A temp-file failure: 53100 disk_full when the device is full, else 58030.
fn io(error: std::io::Error) -> CoreError {
    CoreError::TempFile { disk_full: error.kind() == std::io::ErrorKind::StorageFull, detail: error.to_string() }
}

/// Carry a non-SQL spill error through SQL execution; [`statement`] restores it.
pub(crate) fn sql_error(error: CoreError) -> SqlError {
    match error {
        CoreError::Sql(error) => error,
        error => {
            ERROR.set(Some(error));
            SqlError::Unsupported
        }
    }
}

pub(crate) fn mark_committed() {
    SLOT.with_borrow_mut(|slot| {
        if let Some(spill) = slot {
            spill.after_commit = true;
        }
    });
}

pub(crate) fn durability_output_spilled() -> bool { SLOT.with_borrow(Option::is_some) }

/// Check the current statement's reply and reset its spill for the caller.
pub(crate) fn check_pending(
    check: impl FnOnce(Option<&mut SpilledRows>) -> Result<(), CoreError>,
) -> Result<(), CoreError> {
    let Some(mut spill) = SLOT.take() else { return check(None); };
    let remaining = spill.remaining;
    check(Some(&mut spill))?;
    if let Some(error) = spill.failed.take() {
        return Err(error);
    }
    spill.reader.seek(SeekFrom::Start(0)).map_err(io)?;
    spill.remaining = remaining;
    SLOT.set(Some(spill));
    Ok(())
}

/// True inside a SqlSession statement, whose boundary settles a published spill.
pub(crate) fn armed() -> bool {
    ARMED.get()
}

/// V2 RETURNING bounds each projected row, while RowBuffer owns the total.
/// Keep nested insert pages in memory for their outer executor to collect.
pub(crate) struct ReturningScope(usize);

impl ReturningScope {
    pub(crate) fn outermost(&self) -> bool {
        self.0 == 0
    }
}

impl Drop for ReturningScope {
    fn drop(&mut self) {
        RETURNING_DEPTH.set(self.0);
    }
}

pub(crate) fn returning_unbounded() -> ReturningScope {
    let depth = RETURNING_DEPTH.get();
    RETURNING_DEPTH.set(depth + 1);
    ReturningScope(depth)
}

pub(crate) fn returning_is_unbounded() -> bool {
    RETURNING_DEPTH.get() > 0
}

/// Removes the file when dropped; declared after the handle that uses it.
struct TempPath(PathBuf);
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Run `f` with spilled rows left on disk for the caller.
/// The returned handle yields every row in statement order.
pub fn streaming<T>(f: impl FnOnce() -> T) -> (T, Option<SpilledRows>) {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            STREAMING.set(self.0);
            SLOT.take();
        }
    }
    let _reset = Reset(STREAMING.replace(true));
    SLOT.take();
    let value = f();
    (value, SLOT.take())
}

/// [`streaming`] for a task-session cursor (GD-0061): the spill it receives
/// is read back only through that cursor, whose pages carry the statement's
/// release tag and go through `release_page` like buffered rows.
pub(crate) fn streaming_tagged<T>(f: impl FnOnce() -> T) -> (T, Option<SpilledRows>) {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            TAGGED.set(self.0);
        }
    }
    let _reset = Reset(TAGGED.replace(true));
    streaming(f)
}

/// Run a task-session statement (GD-0061): its spill reaches only a
/// [`streaming_tagged`] caller. Under a plain [`streaming`] caller it is
/// materialised into the result, which stays behind the release tag, so an
/// untagged temp-file handle never carries a task's rows.
pub(crate) fn task_statement<T>(f: impl FnOnce() -> T) -> T {
    struct Reset(bool, Option<bool>);
    impl Drop for Reset {
        fn drop(&mut self) {
            TAGGED.set(self.0);
            if let Some(streaming) = self.1 {
                STREAMING.set(streaming);
            }
        }
    }
    // A statement nested in this one is never the tagged stream's statement.
    let tagged = TAGGED.replace(false);
    let _reset = Reset(tagged, (!tagged).then(|| STREAMING.replace(false)));
    f()
}

/// Run `f` for a caller that materialises whole results (HTTP/gRPC API, native
/// reads): statement output stays in memory and fails with 54000 past the
/// per-statement result budget instead of spilling (their pre-spill contract).
pub fn bounded<T>(f: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            BOUNDED.set(self.0);
        }
    }
    let _reset = Reset(BOUNDED.replace(true));
    f()
}

/// Override this thread's spill directory (default: the process temp dir).
pub fn set_thread_dir(dir: Option<PathBuf>) {
    DIR.set(dir);
}

/// Override this thread's IN-subquery hash threshold for spill qualification.
#[doc(hidden)]
pub fn set_in_hash_bytes(bytes: Option<usize>) {
    IN_HASH_BYTES.set(bytes);
}

/// Test-only: lowers the per-thread complete-image row cap.
#[doc(hidden)]
pub fn set_query_row_limit(rows: Option<usize>) {
    QUERY_ROW_LIMIT.set(rows);
}

pub(crate) fn query_row_limit(current: usize) -> usize {
    QUERY_ROW_LIMIT.get().map_or(current, |rows| rows.min(current))
}

pub(crate) fn in_hash_bytes() -> usize {
    IN_HASH_BYTES.get().unwrap_or(4 * SORT_BYTES)
}

/// One SqlSession statement: arms SELECT output spills, settles the slot, and
/// drops any spill when `f` errors or unwinds (the slot never outlives it).
/// A nested statement (or one after the streamed one) never takes, hides or
/// drops another statement's spill: the slot is saved and restored around it.
pub(crate) fn statement(f: impl FnOnce() -> Result<SqlResult, CoreError>) -> Result<SqlResult, CoreError> {
    struct Armed(bool);
    impl Drop for Armed {
        fn drop(&mut self) {
            ARMED.set(self.0);
            if std::thread::panicking() {
                SLOT.take();
                ERROR.take();
            }
        }
    }
    let saved = SLOT.take();
    let outer_error = ERROR.take();
    let armed = Armed(ARMED.replace(true));
    let nested = armed.0;
    let result = f();
    drop(armed);
    let stashed = ERROR.replace(outer_error);
    let result = settle(result.map_err(|error| match (error, stashed) {
        (CoreError::Sql(SqlError::Unsupported), Some(stashed)) => stashed,
        (error, _) => error,
    }), !nested && STREAMING.get());
    if SLOT.with_borrow(Option::is_none) {
        SLOT.set(saved);
    }
    result
}

/// Statement boundary: drop a spill on error; leave it for a streaming caller
/// (consuming that caller's one stream) or materialise it.
fn settle(result: Result<SqlResult, CoreError>, stream: bool) -> Result<SqlResult, CoreError> {
    let mut result = match result {
        Ok(result) => result,
        Err(error) => {
            SLOT.take();
            return Err(error);
        }
    };
    if stream && SLOT.with_borrow(Option::is_some) {
        STREAMING.set(false);
    } else if let Some(spilled) = SLOT.take() {
        let after_commit = spilled.after_commit();
        result.rows = spilled.materialise().map_err(|error| {
            if after_commit {
                CoreError::Storage(error.to_string())
            } else {
                error
            }
        })?;
    }
    Ok(result)
}

/// Statement output (RETURNING or SELECT rows) with no fixed ceiling.
pub(crate) struct RowBuffer {
    rows: Vec<SqlRow>,
    bytes: usize,
    count: u64,
    file: Option<(BufWriter<File>, TempPath)>,
}

impl RowBuffer {
    pub(crate) fn new(rows: Vec<SqlRow>) -> Result<Self, CoreError> {
        let mut buffer = Self { rows: Vec::new(), bytes: 0, count: 0, file: None };
        buffer.extend(rows)?;
        Ok(buffer)
    }

    pub(crate) fn extend(&mut self, rows: Vec<SqlRow>) -> Result<(), CoreError> {
        rows.into_iter().try_for_each(|row| self.push(row))
    }

    pub(crate) fn push(&mut self, row: SqlRow) -> Result<(), CoreError> {
        self.count += 1;
        if let Some((writer, _)) = &mut self.file {
            return write_item(writer, &row);
        }
        let bytes = searched::retained_row_bytes(&[], &row).map_err(CoreError::Sql)?;
        self.bytes = self.bytes.saturating_add(bytes);
        self.rows.push(row);
        if self.bytes > SPILL_BYTES {
            if BOUNDED.get() && !STREAMING.get() {
                return Err(CoreError::Sql(SqlError::Limit));
            }
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<(), CoreError> {
        let (file, temp) = create("returning")?;
        let mut writer = BufWriter::new(file);
        for row in std::mem::take(&mut self.rows) {
            write_item(&mut writer, &row)?;
        }
        self.bytes = 0;
        self.file = Some((writer, temp));
        Ok(())
    }

    /// Store in-memory rows in `result`, or publish the spill for this
    /// statement; outside a statement nothing settles a slot, so read it back.
    pub(crate) fn finish(self, result: &mut SqlResult) -> Result<(), CoreError> {
        let Some((writer, temp)) = self.file else {
            result.rows = self.rows;
            return Ok(());
        };
        let mut file = writer.into_inner().map_err(|e| io(e.into_error()))?;
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let spilled = SpilledRows { reader: BufReader::new(file), remaining: self.count, temp, failed: None, after_commit: false };
        result.rows = if armed() { SLOT.set(Some(spilled)); Vec::new() } else { spilled.materialise()? };
        Ok(())
    }
}

/// A new private temp file in this thread's spill directory, removed on drop.
fn create(kind: &str) -> Result<(File, TempPath), CoreError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = DIR.with_borrow(Clone::clone).unwrap_or_else(std::env::temp_dir);
    let mut nonce = [0u8; 8];
    getrandom::fill(&mut nonce).map_err(|e| CoreError::Storage(e.to_string()))?;
    let path = dir.join(format!(
        "giggles-{kind}-{}-{}-{:016x}.spill",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        u64::from_le_bytes(nonce)
    ));
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options.open(&path).map_err(io)?;
    Ok((file, TempPath(path)))
}

fn write_item<T: Serialize>(writer: &mut BufWriter<File>, item: &T) -> Result<(), CoreError> {
    let bytes = bincode::serialize(item).map_err(|e| io(std::io::Error::other(e)))?;
    let len = u32::try_from(bytes.len()).map_err(|_| CoreError::Sql(SqlError::Limit))?;
    writer.write_all(&len.to_le_bytes()).map_err(io)?;
    writer.write_all(&bytes).map_err(io)
}

fn read_item<T: DeserializeOwned>(reader: &mut BufReader<File>) -> Result<T, CoreError> {
    let mut len = [0u8; 4];
    reader.read_exact(&mut len).map_err(io)?;
    let mut bytes = vec![0; u32::from_le_bytes(len) as usize];
    reader.read_exact(&mut bytes).map_err(io)?;
    bincode::deserialize(&bytes).map_err(|e| io(std::io::Error::other(e)))
}

/// Rows of one completed statement, read once in order from a temp file.
/// A read error is sticky: every later read repeats it, never a short end.
pub struct SpilledRows {
    reader: BufReader<File>,
    remaining: u64,
    temp: TempPath,
    failed: Option<CoreError>,
    after_commit: bool,
}

impl std::fmt::Debug for SpilledRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpilledRows").field("remaining", &self.remaining).field("path", &self.temp.0).finish()
    }
}

impl SpilledRows {
    /// Whether a successful autocommit write produced these rows.
    pub fn after_commit(&self) -> bool {
        self.after_commit
    }

    /// Rows not yet read.
    pub fn len(&self) -> u64 {
        self.remaining
    }
    /// True only once every row was read successfully.
    pub fn is_empty(&self) -> bool {
        self.remaining == 0 && self.failed.is_none()
    }
    fn materialise(self) -> Result<Vec<SqlRow>, CoreError> {
        let mut rows = Vec::with_capacity(self.remaining.try_into().unwrap_or(0));
        for row in self {
            rows.push(row?);
            if rows.len().is_multiple_of(4096) {
                crate::cancellation::check()?;
            }
        }
        Ok(rows)
    }
    #[cfg(test)]
    pub(crate) fn fail_next_read(&mut self) {
        // A write-only handle: the next read fails like a lost or damaged file.
        self.reader = BufReader::new(OpenOptions::new().write(true).open(&self.temp.0).expect("reopen"));
    }
    /// The backing temp file, removed when this handle drops.
    pub fn path(&self) -> &Path {
        &self.temp.0
    }
}

impl Iterator for SpilledRows {
    type Item = Result<SqlRow, CoreError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = &self.failed {
            return Some(Err(error.clone()));
        }
        if self.remaining == 0 {
            return None;
        }
        let row = read_item(&mut self.reader);
        match &row {
            Ok(_) => self.remaining -= 1,
            Err(error) => self.failed = Some(error.clone()),
        }
        Some(row)
    }
}

/// One sorted run on disk.
pub(crate) struct Run {
    reader: BufReader<File>,
    remaining: u64,
    /// Merge passes behind this run (0 = written from memory).
    level: u32,
    _temp: TempPath,
}

fn write_run<T: Serialize>(items: impl Iterator<Item = Result<T, SqlError>>) -> Result<Run, SqlError> {
    let (file, temp) = create("sort").map_err(sql_error)?;
    let mut writer = BufWriter::new(file);
    let mut remaining = 0;
    for item in items {
        write_item(&mut writer, &item?).map_err(sql_error)?;
        remaining += 1;
    }
    let mut file = writer.into_inner().map_err(|e| sql_error(io(e.into_error())))?;
    file.seek(SeekFrom::Start(0)).map_err(|e| sql_error(io(e)))?;
    Ok(Run { reader: BufReader::new(file), remaining, level: 0, _temp: temp })
}

pub(crate) enum Input<T> {
    Run(Run),
    Memory(std::vec::IntoIter<T>),
}
impl<T: DeserializeOwned> Input<T> {
    fn next(&mut self) -> Result<Option<T>, SqlError> {
        match self {
            Self::Memory(rows) => Ok(rows.next()),
            Self::Run(run) if run.remaining == 0 => Ok(None),
            Self::Run(run) => {
                run.remaining -= 1;
                read_item(&mut run.reader).map(Some).map_err(sql_error)
            }
        }
    }
}

/// Merges sorted inputs; equal keys come from the earliest input.
pub(crate) struct Merge<T, C> {
    inputs: Vec<Input<T>>,
    heads: Vec<Option<T>>,
    cmp: C,
    emitted: u64,
    done: bool,
}

impl<T: DeserializeOwned, C: Fn(&T, &T) -> Ordering> Merge<T, C> {
    fn new(mut inputs: Vec<Input<T>>, cmp: C) -> Result<Self, SqlError> {
        let heads = inputs.iter_mut().map(Input::next).collect::<Result<_, _>>()?;
        Ok(Self { inputs, heads, cmp, emitted: 0, done: false })
    }
}

impl<T: DeserializeOwned, C: Fn(&T, &T) -> Ordering> Iterator for Merge<T, C> {
    type Item = Result<T, SqlError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.emitted += 1;
        if self.emitted.is_multiple_of(4096) && crate::cancellation::check().is_err() {
            self.done = true;
            return Some(Err(SqlError::Aborted));
        }
        let mut best: Option<usize> = None;
        for (index, head) in self.heads.iter().enumerate() {
            let Some(head) = head else { continue };
            if best.is_none_or(|b| {
                (self.cmp)(head, self.heads[b].as_ref().expect("head")) == Ordering::Less
            }) {
                best = Some(index);
            }
        }
        let Some(best) = best else {
            self.done = true;
            return None;
        };
        let row = self.heads[best].take();
        match self.inputs[best].next() {
            Ok(next) => self.heads[best] = next,
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        }
        row.map(Ok)
    }
}

/// Sorted rows, in memory or merged from spilled runs. Dropping it removes the runs.
pub(crate) enum Sorted<T, C> {
    Memory(std::vec::IntoIter<T>),
    Merge(Merge<T, C>),
}

impl<T: DeserializeOwned, C: Fn(&T, &T) -> Ordering> Iterator for Sorted<T, C> {
    type Item = Result<T, SqlError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(rows) => rows.next().map(Ok),
            Self::Merge(merge) => merge.next(),
        }
    }
}

/// Push-based external sort: up to `SORT_BYTES` of input (by caller size) is
/// charged to retention while held; past that, sorted runs spill to temp files.
pub(crate) struct Sorter<T> {
    memory: Vec<T>,
    runs: Vec<Run>,
    bytes: usize,
}

impl<T: Serialize + DeserializeOwned> Sorter<T> {
    pub(crate) fn new() -> Self {
        Self { memory: Vec::new(), runs: Vec::new(), bytes: 0 }
    }

    pub(crate) fn push<C: Fn(&T, &T) -> Ordering + Copy>(
        &mut self,
        item: T,
        n: usize,
        cmp: C,
        retention: &mut impl scan::RetentionBudget,
    ) -> Result<(), SqlError> {
        if !self.memory.is_empty() && self.bytes.saturating_add(n) > work_mem() {
            self.spill_run(cmp, retention)?;
        }
        // Less than work_mem left in the statement ledger: spill, don't fail.
        if retention.charge(n).is_err() {
            if self.memory.is_empty() {
                return Err(SqlError::Limit);
            }
            self.spill_run(cmp, retention)?;
            retention.charge(n)?;
        }
        self.bytes += n;
        self.memory.push(item);
        Ok(())
    }

    /// Writes memory as a run. Every `MERGE_FAN_IN` newest runs of one level
    /// merge into one run of the next level, so open run files stay at most
    /// `MERGE_FAN_IN` per level (logarithmic in input size); adjacent runs
    /// merge in input order, so ties stay stable.
    fn spill_run<C: Fn(&T, &T) -> Ordering + Copy>(
        &mut self,
        cmp: C,
        retention: &mut impl scan::RetentionBudget,
    ) -> Result<(), SqlError> {
        self.memory.sort_by(cmp);
        self.runs.push(write_run(self.memory.drain(..).map(Ok))?);
        retention.release(std::mem::take(&mut self.bytes));
        while let Some(tail) = self.runs.len().checked_sub(MERGE_FAN_IN)
            && self.runs[tail..].iter().all(|run| run.level == self.runs[tail].level)
        {
            let level = self.runs[tail].level + 1;
            let inputs = self.runs.drain(tail..).map(Input::Run).collect();
            let mut merged = write_run(Merge::new(inputs, cmp)?)?;
            merged.level = level;
            self.runs.push(merged);
        }
        Ok(())
    }

    /// Releases the held retention; the result is stable in push order.
    pub(crate) fn finish<C: Fn(&T, &T) -> Ordering + Copy>(
        mut self,
        cmp: C,
        retention: &mut impl scan::RetentionBudget,
    ) -> Result<Sorted<T, C>, SqlError> {
        retention.release(std::mem::take(&mut self.bytes));
        self.memory.sort_by(cmp);
        let mut runs = self.runs;
        if runs.is_empty() {
            return Ok(Sorted::Memory(self.memory.into_iter()));
        }
        while runs.len() >= MERGE_FAN_IN {
            let first = runs.drain(..MERGE_FAN_IN).map(Input::Run).collect();
            let merged = write_run(Merge::new(first, cmp)?)?;
            runs.insert(0, merged);
        }
        let mut inputs: Vec<_> = runs.into_iter().map(Input::Run).collect();
        inputs.push(Input::Memory(self.memory.into_iter()));
        Ok(Sorted::Merge(Merge::new(inputs, cmp)?))
    }
}

/// Stable full sort with no size ceiling (see [`Sorter`]).
pub(crate) fn sort_external<T, C>(
    rows: impl Iterator<Item = Result<T, SqlError>>,
    size: impl Fn(&T) -> usize,
    cmp: C,
    retention: &mut impl scan::RetentionBudget,
) -> Result<Sorted<T, C>, SqlError>
where
    T: Serialize + DeserializeOwned,
    C: Fn(&T, &T) -> Ordering + Copy,
{
    let mut sorter = Sorter::new();
    let fill = || -> Result<(), SqlError> {
        for row in rows {
            let row = row?;
            let n = size(&row);
            sorter.push(row, n, cmp, retention)?;
        }
        Ok(())
    };
    if let Err(error) = fill() {
        retention.release(sorter.bytes);
        return Err(error);
    }
    sorter.finish(cmp, retention)
}

type BySeq<T> = fn(&(u64, T), &(u64, T)) -> Ordering;
/// Output of [`unique_external`]; names no borrow of the retention budget.
pub(crate) type Unique<T> = std::iter::Map<Sorted<(u64, T), BySeq<T>>, fn(Result<(u64, T), SqlError>) -> Result<T, SqlError>>;

/// The first item of each `key`-equal set, in input order (a sort-based
/// PostgreSQL Unique): sort by key, keep the first of each run, restore order.
pub(crate) fn unique_external<T, K>(
    rows: impl Iterator<Item = Result<T, SqlError>>,
    size: impl Fn(&T) -> usize + Copy,
    key: K,
    retention: &mut impl scan::RetentionBudget,
) -> Result<Unique<T>, SqlError>
where
    T: Serialize + DeserializeOwned + Clone,
    K: Fn(&T, &T) -> Ordering + Copy,
{
    let numbered = rows.zip(0u64..).map(|(row, seq)| row.map(|row| (seq, row)));
    let item_size = move |(_, row): &(u64, T)| size(row) + 8;
    let by_key = sort_external(numbered, item_size, move |a: &(u64, T), b: &(u64, T)| key(&a.1, &b.1), retention)?;
    let mut last: Option<T> = None;
    let firsts = by_key.filter(move |item| match item {
        Ok((_, row)) if last.as_ref().is_some_and(|last| key(last, row) == Ordering::Equal) => false,
        Ok((_, row)) => {
            last = Some(row.clone());
            true
        }
        Err(_) => true,
    });
    let by_seq: BySeq<T> = |a, b| a.0.cmp(&b.0);
    let ordered = sort_external(firsts, item_size, by_seq, retention)?;
    Ok(ordered.map(|item| item.map(|(_, row)| row)))
}

pub(crate) type StoreIter<'a, T> = Box<dyn Iterator<Item = Result<Cow<'a, T>, SqlError>> + 'a>;

type CachedKeys = (usize, Vec<u8>, Vec<std::ops::Range<usize>>);
/// Sorted, deduplicated membership keys: one sparse entry per ~64 KiB block.
pub(crate) struct KeyFile {
    file: RefCell<File>,
    blocks: Vec<(Vec<u8>, u64, usize)>,
    cache: RefCell<CachedKeys>,
    _temp: TempPath,
}

pub(crate) struct KeyFileBuilder(Sorter<Vec<u8>>);
impl KeyFileBuilder {
    pub(crate) fn new() -> Self { Self(Sorter::new()) }

    pub(crate) fn push(&mut self, key: Vec<u8>, retention: &mut impl scan::RetentionBudget) -> Result<(), SqlError> {
        let bytes = key.len() + std::mem::size_of::<Vec<u8>>();
        self.0.push(key, bytes, Vec::cmp, retention)
    }

    pub(crate) fn finish(self, retention: &mut impl scan::RetentionBudget) -> Result<KeyFile, SqlError> {
        let sorted = self.0.finish(Vec::cmp, retention)?;
        let (file, temp) = create("inkeys").map_err(sql_error)?;
        let mut writer = BufWriter::new(file);
        let mut blocks: Vec<(Vec<u8>, u64, usize)> = Vec::new();
        let mut offset = 0u64;
        let mut block = Vec::new();
        let mut last = None;
        for key in sorted {
            let key = key?;
            if last.as_ref() == Some(&key) { continue; }
            if !block.is_empty() && block.len() + 4 + key.len() > 64 * 1024 {
                writer.write_all(&block).map_err(|e| sql_error(io(e)))?;
                blocks.last_mut().expect("block").2 = block.len();
                offset += block.len() as u64;
                block.clear();
            }
            if block.is_empty() {
                retention.charge(key.len() + std::mem::size_of::<(Vec<u8>, u64, usize)>())?;
                blocks.push((key.clone(), offset, 0));
            }
            let len = u32::try_from(key.len()).map_err(|_| SqlError::Limit)?;
            block.extend_from_slice(&len.to_le_bytes());
            block.extend_from_slice(&key);
            last = Some(key);
        }
        if let Some(entry) = blocks.last_mut() {
            writer.write_all(&block).map_err(|e| sql_error(io(e)))?;
            entry.2 = block.len();
        }
        let file = writer.into_inner().map_err(|e| sql_error(io(e.into_error())))?;
        Ok(KeyFile { file: RefCell::new(file), blocks, cache: RefCell::new((usize::MAX, Vec::new(), Vec::new())), _temp: temp })
    }
}

impl KeyFile {
    pub(crate) fn contains(&self, key: &[u8]) -> Result<bool, SqlError> {
        let Some(index) = self.blocks.partition_point(|b| b.0.as_slice() <= key).checked_sub(1) else {
            return Ok(false);
        };
        let mut cache = self.cache.borrow_mut();
        if cache.0 != index {
            let (_, offset, len) = &self.blocks[index];
            let mut bytes = vec![0; *len];
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(*offset)).and_then(|_| file.read_exact(&mut bytes)).map_err(|e| sql_error(io(e)))?;
            let mut input = bytes.as_slice();
            let mut keys = Vec::new();
            while !input.is_empty() {
                let mut len = [0; 4];
                input.read_exact(&mut len).map_err(|e| sql_error(io(e)))?;
                let len = u32::from_le_bytes(len) as usize;
                let start = bytes.len() - input.len();
                input = input.get(len..).ok_or_else(|| {
                    sql_error(io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)))
                })?;
                keys.push(start..start + len);
            }
            *cache = (index, bytes, keys);
        }
        Ok(cache.2.binary_search_by(|range| cache.1[range.clone()].cmp(key)).is_ok())
    }
}

/// Rewindable materialised items (PostgreSQL's tuplestore): memory up to
/// `SPILL_BYTES`, then a temp file that each [`Store::iter`] re-reads.
pub(crate) struct Store<T> {
    memory: Vec<T>,
    bytes: usize,
    len: u64,
    file: Option<(BufWriter<File>, TempPath)>,
}

impl<T: Serialize + DeserializeOwned + Clone> Store<T> {
    pub(crate) fn new() -> Self {
        Self { memory: Vec::new(), bytes: 0, len: 0, file: None }
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn push(&mut self, item: T, size: usize) -> Result<(), SqlError> {
        self.len += 1;
        if let Some((writer, _)) = &mut self.file {
            return write_item(writer, &item).map_err(sql_error);
        }
        self.bytes = self.bytes.saturating_add(size);
        self.memory.push(item);
        if self.bytes > SPILL_BYTES {
            let (file, temp) = create("store").map_err(sql_error)?;
            let mut writer = BufWriter::new(file);
            for item in self.memory.drain(..) {
                write_item(&mut writer, &item).map_err(sql_error)?;
            }
            self.file = Some((writer, temp));
        }
        Ok(())
    }

    /// Flush pushed items so [`Store::iter`] can read them.
    pub(crate) fn finish(&mut self) -> Result<(), SqlError> {
        match &mut self.file {
            Some((writer, _)) => writer.flush().map_err(|e| sql_error(io(e))),
            None => Ok(()),
        }
    }

    /// Every item in push order; each call reads the file from the start.
    pub(crate) fn iter(&self) -> Result<StoreIter<'_, T>, SqlError> {
        let Some((_, temp)) = &self.file else {
            return Ok(Box::new(self.memory.iter().map(|item| Ok(Cow::Borrowed(item)))));
        };
        let mut reader = BufReader::new(File::open(&temp.0).map_err(|e| sql_error(io(e)))?);
        let mut remaining = self.len;
        Ok(Box::new(std::iter::from_fn(move || {
            remaining = remaining.checked_sub(1)?;
            let item = read_item(&mut reader).map(Cow::Owned).map_err(sql_error);
            if item.is_err() {
                remaining = 0;
            }
            Some(item)
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returning_scope_restores_depth_after_nested_unwind() {
        assert!(!returning_is_unbounded());
        let outer = returning_unbounded();
        assert!(outer.outermost());
        let panic = std::panic::catch_unwind(|| {
            let nested = returning_unbounded();
            assert!(!nested.outermost());
            panic!("unwind a nested RETURNING executor");
        });
        assert!(panic.is_err());
        assert!(returning_is_unbounded());
        drop(outer);
        assert!(!returning_is_unbounded());
    }

    struct Budget(usize);
    impl scan::RetentionBudget for Budget {
        fn charge(&mut self, bytes: usize) -> Result<(), SqlError> {
            self.0 += bytes;
            assert!(self.0 <= SORT_BYTES, "sort retention stays within work_mem");
            Ok(())
        }
        fn release(&mut self, bytes: usize) {
            self.0 -= bytes;
        }
    }

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        set_thread_dir(Some(dir.clone()));
        dir
    }

    fn files(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    #[test]
    fn key_file_indexes_blocks_deduplicates_and_cleans_up() {
        let dir = dir("giggles-inkeys-unit");
        let key = |n: u64| { let mut key = n.to_be_bytes().to_vec(); key.resize(64, 0); key };
        let mut budget = scan::Retention::new(32 * 1024 * 1024);
        let mut builder = KeyFileBuilder::new();
        for n in (1..=80_000).rev() {
            for _ in 0..2 { builder.push(key(2 * n), &mut budget).unwrap(); }
        }
        let keys = builder.finish(&mut budget).unwrap();
        assert!(keys.blocks.len() > 2);
        assert_eq!(keys.blocks.iter().map(|block| block.2).sum::<usize>(), 80_000 * 68);
        assert!(budget.used() > 0 && budget.used() < 128 * 1024);
        assert_eq!(files(&dir), 1);
        for n in 1..=80_000 { assert!(keys.contains(&key(2 * n)).unwrap()); }
        for block in &keys.blocks {
            assert!(keys.contains(&block.0).unwrap());
            let first = u64::from_be_bytes(block.0[..8].try_into().unwrap());
            assert!(!keys.contains(&key(first - 1)).unwrap());
            if first > 2 { assert!(keys.contains(&key(first - 2)).unwrap()); }
        }
        for n in [0, 1, 999, 160_001, u64::MAX] { assert!(!keys.contains(&key(n)).unwrap()); }
        drop(keys);
        assert_eq!(files(&dir), 0);
        let empty = KeyFileBuilder::new().finish(&mut budget).unwrap();
        assert!(!empty.contains(&[]).unwrap() && !empty.contains(&key(2)).unwrap());
        drop(empty);
        assert_eq!(files(&dir), 0);
        set_thread_dir(None);
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn external_sort_is_stable_across_many_runs_and_removes_them() {
        let dir = dir("giggles-sort-unit");
        let cmp = |a: &(u64, u64), b: &(u64, u64)| a.0.cmp(&b.0);
        let mut budget = Budget(0);
        // 300k rows charged 1 KiB each: ~73 runs, so one extra merge pass.
        let rows = (0..300_000u64).map(|i| Ok(((i * 7919) % 1000, i)));
        let sorted = sort_external(rows, |_| 1024, cmp, &mut budget).unwrap();
        assert!(files(&dir) > 1);
        let out = sorted.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!((out.len(), budget.0), (300_000, 0));
        assert!(out.windows(2).all(|w| w[0].0 < w[1].0 || (w[0].0 == w[1].0 && w[0].1 < w[1].1)));
        assert_eq!(files(&dir), 0);

        // An input error and an early drop both remove the spilled runs.
        let failing = (0..20_000u64).map(|i| if i == 19_999 { Err(SqlError::Aborted) } else { Ok((i, i)) });
        assert!(matches!(sort_external(failing, |_| 1024, cmp, &mut budget), Err(SqlError::Aborted)));
        let rows = (0..20_000u64).rev().map(|i| Ok((i, i)));
        let mut partial = sort_external(rows, |_| 1024, cmp, &mut budget).unwrap();
        assert_eq!(partial.next().unwrap().unwrap(), (0, 0));
        drop(partial);
        assert_eq!((files(&dir), budget.0), (0, 0));
        set_thread_dir(None);
    }

    #[test]
    fn statement_unwind_clears_a_published_spill() {
        let dir = dir("giggles-spill-unwind");
        let unwound = std::panic::catch_unwind(|| {
            statement(|| {
                let mut buffer = RowBuffer::new(Vec::new())?;
                for i in 0..20_000 {
                    let value = super::super::Value::Text(format!("{i:0>64}"));
                    buffer.push(SqlRow { values: vec![value], version: 0 })?;
                }
                let mut result = SqlResult::default();
                buffer.finish(&mut result)?;
                assert!(SLOT.with_borrow(Option::is_some) && files(&dir) == 1);
                panic!("injected after publishing a spill");
            })
        });
        assert!(unwound.is_err());
        assert!(SLOT.with_borrow(Option::is_none) && !armed());
        assert_eq!(files(&dir), 0);
        set_thread_dir(None);
    }

    fn spill_rows(n: usize) -> Result<SqlResult, CoreError> {
        let mut buffer = RowBuffer::new(Vec::new())?;
        for i in 0..n {
            let value = super::super::Value::Text(format!("{i:0>64}"));
            buffer.push(SqlRow { values: vec![value], version: 0 })?;
        }
        let mut result = SqlResult::default();
        buffer.finish(&mut result)?;
        Ok(result)
    }

    #[test]
    fn streaming_is_per_statement_and_reads_fail_stickily() {
        let dir = dir("giggles-spill-scope");
        // Outside any statement, finish materialises instead of hiding rows.
        assert_eq!(spill_rows(20_000).unwrap().rows.len(), 20_000);
        assert!(SLOT.with_borrow(Option::is_none) && files(&dir) == 0);
        // The streamed statement keeps its spill; a nested statement and a later
        // one in the same closure get their own rows and leave it in place.
        let ((outer, later), spilled) = streaming(|| {
            let outer = statement(|| {
                let nested = statement(|| spill_rows(15_000)).unwrap();
                assert_eq!(nested.rows.len(), 15_000);
                spill_rows(20_000)
            })
            .unwrap();
            (outer, statement(|| spill_rows(18_000)).unwrap())
        });
        assert!(outer.rows.is_empty());
        assert_eq!(later.rows.len(), 18_000);
        let mut spilled = spilled.expect("outer spill");
        assert_eq!(spilled.len(), 20_000);
        // A read error repeats on every later read; the handle never looks done.
        assert!(spilled.next().unwrap().is_ok());
        spilled.fail_next_read();
        assert!(matches!(spilled.next(), Some(Err(CoreError::TempFile { disk_full: false, .. }))));
        assert!(matches!(spilled.next(), Some(Err(CoreError::TempFile { .. }))));
        assert!(!spilled.is_empty());
        drop(spilled);
        // Bounded callers fail at the result budget instead of spilling.
        let bounded_result = bounded(|| statement(|| spill_rows(20_000)));
        assert_eq!(bounded_result, Err(CoreError::Sql(SqlError::Limit)));
        assert_eq!(files(&dir), 0);
        set_thread_dir(None);
    }

    #[test]
    fn sorter_spills_under_a_small_ledger_and_bounds_open_runs() {
        let dir = dir("giggles-sort-ledger");
        let mut ledger = scan::Retention::new(64 * 1024);
        ledger.charge(32 * 1024).unwrap();
        let cmp = |a: &u64, b: &u64| a.cmp(b);
        let mut sorter = Sorter::new();
        for i in (0..200_000u64).rev() {
            sorter.push(i, 256, cmp, &mut ledger).unwrap();
            assert!(sorter.runs.len() < 2 * MERGE_FAN_IN && files(&dir) < 2 * MERGE_FAN_IN);
        }
        let sorted = sorter.finish(cmp, &mut ledger).unwrap();
        assert!(sorted.map(Result::unwrap).eq(0..200_000));
        assert_eq!((ledger.used(), files(&dir)), (32 * 1024, 0));
        set_thread_dir(None);
    }
}
