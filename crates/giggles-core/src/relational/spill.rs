//! Spillable v2 statement output and sorts (PostgreSQL's tuplestore and
//! external sort past work_mem).
//!
//! Rows stay in memory up to `SPILL_BYTES`, then move to a private temp file
//! that is deleted on drop, so error, rollback and unwind paths clean up.
//! A completed spill reaches the caller through a thread-local slot: callers
//! inside [`streaming`] read it sequentially; every other SqlSession caller
//! gets the rows materialised into `SqlResult::rows` (never truncated).
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
/// Runs merged at once; more runs are first merged into longer runs.
const MERGE_FAN_IN: usize = 64;

thread_local! {
    static STREAMING: Cell<bool> = const { Cell::new(false) };
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static SLOT: RefCell<Option<SpilledRows>> = const { RefCell::new(None) };
    static ERROR: RefCell<Option<CoreError>> = const { RefCell::new(None) };
    static DIR: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

fn io(error: std::io::Error) -> CoreError {
    CoreError::Storage(format!("spill file: {error}"))
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

/// True inside a SqlSession statement, whose boundary settles a published spill.
pub(crate) fn armed() -> bool {
    ARMED.get()
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

/// Override this thread's spill directory (default: the process temp dir).
pub fn set_thread_dir(dir: Option<PathBuf>) {
    DIR.set(dir);
}

/// Drop any spill left by an earlier statement on this thread.
pub(crate) fn clear() {
    SLOT.take();
}

/// One SqlSession statement: arms SELECT output spills, settles the slot, and
/// drops any spill when `f` errors or unwinds (the slot never outlives it).
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
    clear();
    ERROR.take();
    let armed = Armed(ARMED.replace(true));
    let result = f();
    drop(armed);
    let stashed = ERROR.take();
    settle(result.map_err(|error| match (error, stashed) {
        (CoreError::Sql(SqlError::Unsupported), Some(stashed)) => stashed,
        (error, _) => error,
    }))
}

/// Statement boundary: drop a spill on error; materialise it unless streaming.
pub(crate) fn settle(result: Result<SqlResult, CoreError>) -> Result<SqlResult, CoreError> {
    let mut result = match result {
        Ok(result) => result,
        Err(error) => {
            SLOT.take();
            return Err(error);
        }
    };
    if !STREAMING.get()
        && let Some(spilled) = SLOT.take()
    {
        result.rows.reserve(spilled.len().try_into().unwrap_or(0));
        for row in spilled {
            result.rows.push(row?);
        }
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

    /// Store in-memory rows in `result`, or publish the spill for this statement.
    pub(crate) fn finish(self, result: &mut SqlResult) -> Result<(), CoreError> {
        let Some((writer, temp)) = self.file else {
            result.rows = self.rows;
            return Ok(());
        };
        let mut file = writer.into_inner().map_err(|e| io(e.into_error()))?;
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        result.rows = Vec::new();
        SLOT.set(Some(SpilledRows { reader: BufReader::new(file), remaining: self.count, temp }));
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
    let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path).map_err(io)?;
    Ok((file, TempPath(path)))
}

fn write_item<T: Serialize>(writer: &mut BufWriter<File>, item: &T) -> Result<(), CoreError> {
    let bytes = bincode::serialize(item).map_err(|e| CoreError::Storage(e.to_string()))?;
    let len = u32::try_from(bytes.len()).map_err(|_| CoreError::Sql(SqlError::Limit))?;
    writer.write_all(&len.to_le_bytes()).map_err(io)?;
    writer.write_all(&bytes).map_err(io)
}

fn read_item<T: DeserializeOwned>(reader: &mut BufReader<File>) -> Result<T, CoreError> {
    let mut len = [0u8; 4];
    reader.read_exact(&mut len).map_err(io)?;
    let mut bytes = vec![0; u32::from_le_bytes(len) as usize];
    reader.read_exact(&mut bytes).map_err(io)?;
    bincode::deserialize(&bytes).map_err(|e| CoreError::Storage(e.to_string()))
}

/// Rows of one completed statement, read once in order from a temp file.
pub struct SpilledRows {
    reader: BufReader<File>,
    remaining: u64,
    temp: TempPath,
}

impl std::fmt::Debug for SpilledRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpilledRows").field("remaining", &self.remaining).field("path", &self.temp.0).finish()
    }
}

impl SpilledRows {
    /// Rows not yet read.
    pub fn len(&self) -> u64 {
        self.remaining
    }
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }
    /// The backing temp file, removed when this handle drops.
    pub fn path(&self) -> &Path {
        &self.temp.0
    }
}

impl Iterator for SpilledRows {
    type Item = Result<SqlRow, CoreError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let row = read_item(&mut self.reader);
        if row.is_err() {
            self.remaining = 0;
        }
        Some(row)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        (n, Some(n))
    }
}

/// One sorted run on disk.
pub(crate) struct Run {
    reader: BufReader<File>,
    remaining: u64,
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
    Ok(Run { reader: BufReader::new(file), remaining, _temp: temp })
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
        if !self.memory.is_empty() && self.bytes.saturating_add(n) > SORT_BYTES {
            self.memory.sort_by(cmp);
            self.runs.push(write_run(self.memory.drain(..).map(Ok))?);
            retention.release(std::mem::take(&mut self.bytes));
        }
        retention.charge(n)?;
        self.bytes += n;
        self.memory.push(item);
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
}
