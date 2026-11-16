//! Spillable v2 RETURNING rows (PostgreSQL's tuplestore past work_mem).
//!
//! Rows stay in memory up to `SPILL_BYTES`, then move to a private temp file
//! that is deleted on drop, so error, rollback and unwind paths clean up.
//! A completed spill reaches the caller through a thread-local slot: callers
//! inside [`streaming`] read it sequentially; every other SqlSession caller
//! gets the rows materialised into `SqlResult::rows` (never truncated).
use super::{SqlResult, SqlRow, searched};
use crate::CoreError;
use std::cell::{Cell, RefCell};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory threshold before rows move to disk.
pub const SPILL_BYTES: usize = super::MAX_RESULT_BYTES;

thread_local! {
    static STREAMING: Cell<bool> = const { Cell::new(false) };
    static SLOT: RefCell<Option<SpilledRows>> = const { RefCell::new(None) };
    static DIR: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

fn io(error: std::io::Error) -> CoreError {
    CoreError::Storage(format!("RETURNING spill: {error}"))
}

/// Removes the file when dropped; declared after the handle that uses it.
struct TempPath(PathBuf);
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Run `f` with spilled RETURNING rows left on disk for the caller.
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

/// Statement RETURNING output with no fixed ceiling.
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
        for row in rows {
            self.count += 1;
            if let Some((writer, _)) = &mut self.file {
                write_row(writer, &row)?;
                continue;
            }
            let bytes = searched::retained_row_bytes(&[], &row).map_err(CoreError::Sql)?;
            self.bytes = self.bytes.saturating_add(bytes);
            self.rows.push(row);
            if self.bytes > SPILL_BYTES {
                self.spill()?;
            }
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<(), CoreError> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = DIR.with_borrow(Clone::clone).unwrap_or_else(std::env::temp_dir);
        let mut nonce = [0u8; 8];
        getrandom::fill(&mut nonce).map_err(|e| CoreError::Storage(e.to_string()))?;
        let path = dir.join(format!(
            "giggles-returning-{}-{}-{:016x}.spill",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            u64::from_le_bytes(nonce)
        ));
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path).map_err(io)?;
        let temp = TempPath(path);
        let mut writer = BufWriter::new(file);
        for row in std::mem::take(&mut self.rows) {
            write_row(&mut writer, &row)?;
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

fn write_row(writer: &mut BufWriter<File>, row: &SqlRow) -> Result<(), CoreError> {
    let bytes = bincode::serialize(row).map_err(|e| CoreError::Storage(e.to_string()))?;
    let len = u32::try_from(bytes.len()).map_err(|_| CoreError::Sql(super::SqlError::Limit))?;
    writer.write_all(&len.to_le_bytes()).map_err(io)?;
    writer.write_all(&bytes).map_err(io)
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
        let mut len = [0u8; 4];
        let row = self.reader.read_exact(&mut len).map_err(io).and_then(|()| {
            let mut bytes = vec![0; u32::from_le_bytes(len) as usize];
            self.reader.read_exact(&mut bytes).map_err(io)?;
            bincode::deserialize(&bytes).map_err(|e| CoreError::Storage(e.to_string()))
        });
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
