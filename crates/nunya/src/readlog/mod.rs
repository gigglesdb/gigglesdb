//! JSON-line read logging with bounded file rotation.
use crate::{
    error::{NyCode, Refusal},
    policy::ReadLogSink,
    token::TaskId,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};
/// Result of the logged statement.
pub enum Outcome {
    /// Statement was permitted.
    Allowed,
    /// Statement was refused with a fixed code.
    Refused(NyCode),
}
/// Data supplied by the session after redacting explicit tokenize arguments.
pub struct ReadLogRecord {
    /// Unix timestamp in milliseconds.
    pub ts_unix_ms: u64,
    /// Non-secret task identifier.
    pub task: Option<TaskId>,
    /// Credential identifier.
    pub credential: String,
    /// Hex SHA-256 of normalized statement text.
    pub statement_fingerprint: String,
    /// Statement with tokenize plaintext removed.
    pub statement: String,
    /// Statement outcome.
    pub outcome: Outcome,
    /// Number of rows released.
    pub rows: u64,
    /// Tokens released by this statement.
    pub tokens_released: Vec<String>,
}
fn failure() -> Refusal {
    Refusal::new(NyCode::ReadLogUnavailable, "read log unavailable")
}
fn line(rec: &ReadLogRecord) -> Result<Vec<u8>, Refusal> {
    let outcome = match rec.outcome {
        Outcome::Allowed => "allowed",
        Outcome::Refused(c) => c.sqlstate(),
    };
    let mut bytes=serde_json::to_vec(&serde_json::json!({"ts":rec.ts_unix_ms,"task":rec.task.map(|t|format!("{t:?}")),"credential":rec.credential,"fingerprint":rec.statement_fingerprint,"statement":rec.statement,"outcome":outcome,"rows":rec.rows,"tokens":rec.tokens_released})).map_err(|_|failure())?;
    bytes.push(b'\n');
    Ok(bytes)
}
/// Durable-enough per-record logging contract; failures always use NY009.
pub trait ReadLog: Send + Sync {
    /// Write and flush one JSON record.
    fn write(&self, rec: &ReadLogRecord) -> Result<(), Refusal>;
}
/// Standard-output JSON log.
pub struct StdoutLog;
impl ReadLog for StdoutLog {
    fn write(&self, rec: &ReadLogRecord) -> Result<(), Refusal> {
        let bytes = line(rec)?;
        let mut out = std::io::stdout().lock();
        out.write_all(&bytes)
            .and_then(|_| out.flush())
            .map_err(|_| failure())
    }
}
struct FileState {
    file: Option<File>,
    size: u64,
}
/// Append-only JSON log with one rotated predecessor.
pub struct FileLog {
    path: PathBuf,
    max_bytes: u64,
    state: Mutex<FileState>,
}
impl FileLog {
    /// Open an existing log for append, or create it without creating parents.
    pub fn open(path: &Path, max_bytes: u64) -> Result<Self, Refusal> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| failure())?;
        let size = file.metadata().map_err(|_| failure())?.len();
        Ok(Self {
            path: path.into(),
            max_bytes,
            state: Mutex::new(FileState {
                file: Some(file),
                size,
            }),
        })
    }
}
impl ReadLog for FileLog {
    fn write(&self, rec: &ReadLogRecord) -> Result<(), Refusal> {
        let bytes = line(rec)?;
        let len = u64::try_from(bytes.len()).map_err(|_| failure())?;
        let mut state = self.state.lock().map_err(|_| failure())?;
        if state.size != 0 && state.size.checked_add(len).ok_or_else(failure)? > self.max_bytes {
            let file = state.file.as_mut().ok_or_else(failure)?;
            file.flush()
                .and_then(|_| file.sync_data())
                .map_err(|_| failure())?;
            // Close before rename for Windows; a failed rotation poisons this sink closed.
            drop(state.file.take());
            let mut name = self.path.as_os_str().to_os_string();
            name.push(".1");
            let backup = PathBuf::from(name);
            match std::fs::remove_file(&backup) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(failure()),
            }
            std::fs::rename(&self.path, &backup).map_err(|_| failure())?;
            state.file = Some(
                OpenOptions::new()
                    .create_new(true)
                    .append(true)
                    .open(&self.path)
                    .map_err(|_| failure())?,
            );
            state.size = 0;
        }
        let file = state.file.as_mut().ok_or_else(failure)?;
        if file.write_all(&bytes).and_then(|_| file.flush()).is_err() {
            drop(state.file.take());
            return Err(failure());
        }
        state.size = state.size.checked_add(len).ok_or_else(failure)?;
        Ok(())
    }
}
/// Open the configured read-log destination.
pub fn open_sink(sink: &ReadLogSink) -> Result<Box<dyn ReadLog>, Refusal> {
    match sink {
        ReadLogSink::Stdout => Ok(Box::new(StdoutLog)),
        ReadLogSink::File { path, max_bytes } => Ok(Box::new(FileLog::open(path, *max_bytes)?)),
    }
}
/// Fingerprint preserving case and collapsing whitespace everywhere, including quotes.
/// This intentionally simple normalization is not a SQL semantic canonicalization.
/// Callers MUST pass text already processed by `redact_tokenize_args`: hashes of
/// low-entropy plaintext are brute-forceable.
pub fn statement_fingerprint(sql: &str) -> String {
    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    Sha256::digest(normalized.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 128
}
// Skip one SQL lexical unit, including nested block comments and dollar quotes.
fn unit(s: &str, i: usize) -> Result<usize, ()> {
    let b = s.as_bytes();
    if b[i..].starts_with(b"--") {
        return Ok(s[i..].find('\n').map_or(b.len(), |n| i + n + 1));
    }
    if b[i..].starts_with(b"/*") {
        let mut p = i + 2;
        let mut depth = 1;
        while p < b.len() {
            if b[p..].starts_with(b"/*") {
                depth += 1;
                p += 2;
            } else if b[p..].starts_with(b"*/") {
                depth -= 1;
                p += 2;
                if depth == 0 {
                    return Ok(p);
                }
            } else {
                p += 1;
            }
        }
        return Err(());
    }
    let escaped = (b[i] == b'e' || b[i] == b'E') && b.get(i + 1) == Some(&b'\'');
    let q = if escaped { i + 1 } else { i };
    if b[q] == b'\'' || b[q] == b'"' {
        let quote = b[q];
        let mut p = q + 1;
        while p < b.len() {
            if escaped && b[p] == b'\\' {
                p += 2;
            } else if b[p] == quote {
                if b.get(p + 1) == Some(&quote) {
                    p += 2;
                } else {
                    return Ok(p + 1);
                }
            } else {
                p += 1;
            }
        }
        return Err(());
    }
    if b[i] == b'$' {
        let mut p = i + 1;
        while p < b.len() && (b[p].is_ascii_alphanumeric() || b[p] == b'_') {
            p += 1;
        }
        if b.get(p) == Some(&b'$') && (p == i + 1 || !b[i + 1].is_ascii_digit()) {
            let delimiter = &s[i..=p];
            return s[p + 1..]
                .find(delimiter)
                .map(|n| p + 1 + n + delimiter.len())
                .ok_or(());
        }
    }
    Ok(i + 1)
}
fn gap(s: &str, mut i: usize) -> Result<usize, ()> {
    let b = s.as_bytes();
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
        } else if b[i..].starts_with(b"--") || b[i..].starts_with(b"/*") {
            i = unit(s, i)?;
        } else {
            break;
        }
    }
    Ok(i)
}
fn args(s: &str, open: usize) -> Result<(usize, usize, usize), ()> {
    let b = s.as_bytes();
    let mut p = open + 1;
    let mut stack = Vec::new();
    let mut comma = None;
    let mut second_end = None;
    while p < b.len() {
        let next = unit(s, p)?;
        if next != p + 1 {
            p = next;
            continue;
        }
        match b[p] {
            b'(' => stack.push(b')'),
            b'[' => stack.push(b']'),
            b')' if stack.is_empty() => {
                return comma
                    .map(|c| (c + 1, second_end.unwrap_or(p), p + 1))
                    .ok_or(());
            }
            b')' | b']' => {
                if stack.pop() != Some(b[p]) {
                    return Err(());
                }
            }
            b',' if stack.is_empty() => {
                if comma.is_none() {
                    comma = Some(p);
                } else if second_end.is_none() {
                    second_end = Some(p);
                }
            }
            _ => {}
        }
        p += 1;
    }
    Err(())
}
/// Remove every explicit tokenize plaintext argument; malformed calls redact the tail.
pub fn redact_tokenize_args(sql: &str) -> String {
    redact_inner(sql, 0)
}
fn redact_inner(sql: &str, depth: usize) -> String {
    if depth >= 64 {
        return "\'<redacted>\'".into();
    }
    let b = sql.as_bytes();
    let mut out = String::new();
    let mut p = 0;
    let mut copied = 0;
    while p < b.len() {
        let start = p;
        let (end, is_call) = if b[p] == b'"' {
            match unit(sql, p) {
                Ok(e) => (e, &sql[p..e] == "\"nunya_tokenize\""),
                Err(()) => break,
            }
        } else if b[p].is_ascii_alphabetic() || b[p] == b'_' || b[p] >= 128 {
            let mut e = p + 1;
            while e < b.len() && ident(b[e]) {
                e += 1;
            }
            (e, sql[p..e].eq_ignore_ascii_case("nunya_tokenize"))
        } else {
            match unit(sql, p) {
                Ok(e) => {
                    p = e;
                    continue;
                }
                Err(()) => break,
            }
        };
        if is_call {
            match gap(sql, end) {
                Ok(open) if b.get(open) == Some(&b'(') => match args(sql, open) {
                    Ok((a, z, close)) => {
                        out.push_str(&sql[copied..start]);
                        out.push_str(&sql[start..open + 1]);
                        out.push_str(&redact_inner(&sql[open + 1..a], depth + 1));
                        out.push_str("'<redacted>'");
                        out.push_str(&redact_inner(&sql[z..close], depth + 1));
                        p = close;
                        copied = p;
                        continue;
                    }
                    Err(()) => {
                        out.push_str(&sql[copied..start]);
                        out.push_str("'<redacted>'");
                        return out;
                    }
                },
                Err(()) => {
                    out.push_str(&sql[copied..start]);
                    out.push_str("'<redacted>'");
                    return out;
                }
                _ => {}
            }
        }
        // E-prefixed strings need to be consumed as a single unit.
        if (b[start] == b'E' || b[start] == b'e') && b.get(end) == Some(&b'\'') {
            match unit(sql, start) {
                Ok(e) => p = e,
                Err(()) => break,
            }
        } else {
            p = end;
        }
    }
    if p < b.len()
        && let Some(offset) = b[copied..]
            .windows(b"nunya_tokenize".len())
            .position(|candidate| candidate.eq_ignore_ascii_case(b"nunya_tokenize"))
    {
        out.push_str(&sql[copied..copied + offset]);
        out.push_str("'<redacted>'");
        return out;
    }
    out.push_str(&sql[copied..]);
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> ReadLogRecord {
        ReadLogRecord {
            ts_unix_ms: 42,
            task: Some(TaskId([1; 16])),
            credential: "reader".into(),
            statement_fingerprint: statement_fingerprint(&redact_tokenize_args("SELECT  1")),
            statement: "SELECT 1".into(),
            outcome: Outcome::Allowed,
            rows: 1,
            tokens_released: vec!["tok".into()],
        }
    }
    #[test]
    fn json_line() {
        let bytes = line(&record()).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["ts"], 42);
        assert_eq!(v["task"], "01".repeat(16));
        assert_eq!(v["outcome"], "allowed");
        assert_eq!(v["tokens"][0], "tok");
        assert_eq!(
            statement_fingerprint(&redact_tokenize_args("SELECT  1")),
            statement_fingerprint(&redact_tokenize_args("SELECT\n1"))
        );
    }
    fn temp() -> PathBuf {
        let root = std::env::temp_dir();
        let path = root.join(format!(
            "readlog-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
    #[test]
    fn rotation() {
        let dir = temp();
        let path = dir.join("audit");
        let rec = record();
        let len = line(&rec).unwrap().len() as u64;
        let log = FileLog::open(&path, len).unwrap();
        log.write(&rec).unwrap();
        assert!(!dir.join("audit.1").exists());
        log.write(&rec).unwrap();
        log.write(&rec).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
        assert_eq!(std::fs::metadata(dir.join("audit.1")).unwrap().len(), len);
        drop(log);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn oversized_records_rotate_only_nonempty_files() {
        let dir = temp();
        let path = dir.join("audit");
        let backup = dir.join("audit.1");
        std::fs::write(&backup, b"existing backup").unwrap();
        let first = record();
        let mut second = record();
        second.ts_unix_ms += 1;
        let first_bytes = line(&first).unwrap();
        let second_bytes = line(&second).unwrap();
        let log = FileLog::open(&path, 1).unwrap();
        log.write(&first).unwrap();
        assert_eq!(std::fs::read(&backup).unwrap(), b"existing backup");
        assert_eq!(std::fs::read(&path).unwrap(), first_bytes);
        log.write(&second).unwrap();
        assert_eq!(std::fs::read(&backup).unwrap(), first_bytes);
        assert_eq!(std::fs::read(&path).unwrap(), second_bytes);
        drop(log);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn fingerprints_use_redacted_plaintext() {
        assert_eq!(
            statement_fingerprint(&redact_tokenize_args("SELECT nunya_tokenize('x', 'bob')")),
            statement_fingerprint(&redact_tokenize_args("SELECT nunya_tokenize('x', 'alice')"))
        );
    }
    #[test]
    fn malformed_units_redact_tokenize_tail() {
        for sql in [
            "SELECT 'abc nunya_tokenize('x', 'bob')",
            "SELECT /* nunya_tokenize('x','bob')",
            "SELECT E'abc NuNyA_ToKeNiZe('x', 'bob')",
            "SELECT \"abc NUNYA_TOKENIZE('x', 'bob')",
            "SELECT nunya_tokenize('x','bob'), /* NuNyA_ToKeNiZe('x','bob')",
        ] {
            let redacted = redact_tokenize_args(sql);
            assert!(!redacted.contains("bob"));
            assert!(redacted.ends_with("'<redacted>'"));
        }
        assert_eq!(redact_tokenize_args("SELECT /* unfinished"), "SELECT /* unfinished");
    }
    #[test]
    fn missing_parent() {
        let dir = temp();
        assert_eq!(
            FileLog::open(&dir.join("missing/audit"), 100)
                .err()
                .unwrap()
                .code,
            NyCode::ReadLogUnavailable
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn redaction() {
        for sql in [
            "SELECT nunya_tokenize('email', 'bob@x.com')",
            "SELECT nunya_tokenize('x', ARRAY['bob', 'secret'])",
            "SELECT NUNYA_TOKENIZE /* gap */ ('email', E'bob\\\'x'), nunya_tokenize('x', $$bob$$)",
            "SELECT nunya_tokenize('x', (SELECT 'bob'))",
            "SELECT nunya_tokenize(nunya_tokenize('x','bob'),'bob')",
            "SELECT nunya_tokenize('x','bob'",
            "SELECT \"nunya_tokenize\"('x', 'bob')",
            "SELECT nunya_tokenize('x', /* nested /* bob */ hi */ 'bob')",
        ] {
            let redacted = redact_tokenize_args(sql);
            assert!(!redacted.contains("bob"));
            assert!(!redacted.contains("secret"));
        }
        assert_eq!(
            redact_tokenize_args("SELECT 'nunya_tokenize('"),
            "SELECT 'nunya_tokenize('"
        );
    }
}
