//! JSON-line read logging with bounded file rotation.
use crate::{
    error::{NyCode, Refusal},
    policy::ReadLogSink,
    token::{PrincipalId, SessionId, ScopeId},
    state::model::CredentialId,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};
/// Result of the logged statement.
#[derive(Clone)]
pub enum Outcome {
    /// Statement was permitted.
    Allowed,
    /// Statement was refused with a fixed code.
    Refused(NyCode),
}
/// Data supplied by the session after redacting explicit tokenize arguments.
#[derive(Clone)]
pub struct ReadLogRecord {
    /// Unix timestamp in milliseconds.
    pub ts_unix_ms: u64,
    /// Owning principal identifier.
    pub principal: PrincipalId,
    /// Access session identifier.
    pub session: SessionId,
    /// Stored token scope identifier.
    pub scope: ScopeId,
    /// Registered connection identifier.
    pub connection: Option<u64>,
    /// Matched credential identifier.
    pub credential_id: Option<CredentialId>,
    /// Serving route identifier.
    pub route: String,
    /// Applied policy file digest.
    pub policy_revision: Option<String>,
    /// Startup config file digest.
    pub config_revision: Option<String>,
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
/// Fixed lifecycle event kinds.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKind {
    /// Imported policy.
    PolicyImported,
    /// Applied policy.
    PolicyApplied,
    /// Restored policy.
    PolicyRolledBack,
    /// A session was issued.
    Issued,
    /// A session was renewed.
    Renewed,
    /// A session was revoked.
    Revoked,
    /// A principal was disabled.
    PrincipalDisabled,
    /// A resume was refused.
    ResumeRefused,
    /// A durable session ended.
    SessionEnded,
    /// A registered connection ended.
    ConnectionEnded,
    /// A forwarded write has an unknown outcome.
    WriteOutcomeUnknown,
    /// A route's shared domains changed and its sessions were ended.
    MembershipReset {
        /// Retired generated domain names, without column data.
        retired_domains: Vec<String>,
        /// Active sessions ended by the reset.
        sessions: usize,
    },
}
/// Non-secret actor responsible for a lifecycle event.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleActor {
    /// The session owner.
    Owner,
    /// An administrator's principal hex identifier.
    Admin(String),
    /// The configuration administrator.
    ConfigAdmin,
    /// The local management socket.
    Socket,
    /// Automatic state maintenance or connection termination.
    System,
}
/// Data-free lifecycle audit record containing identifiers and fixed codes only.
#[derive(Clone, Debug)]
pub struct LifecycleRecord {
    /// Optional policy mutation evidence; omitted from legacy encodings.
    pub policy: Option<crate::mgmt::audit::PolicyAudit>,
    /// Lifecycle event kind.
    pub kind: LifecycleKind,
    /// Unix timestamp in milliseconds.
    pub at_ms: u64,
    /// Principal hex identifier.
    pub principal: String,
    /// Optional session hex identifier.
    pub session: Option<String>,
    /// Optional route identifier.
    pub route: Option<String>,
    /// Optional registered connection identifier.
    pub connection: Option<u64>,
    /// Event actor.
    pub actor: LifecycleActor,
    /// Fixed, data-free reason code.
    pub reason: &'static str,
}
fn lifecycle_line(rec: &LifecycleRecord) -> Result<Vec<u8>, Refusal> {
    let event = match &rec.kind {
        LifecycleKind::MembershipReset { .. } => serde_json::json!("membership_reset"),
        kind => serde_json::to_value(kind).map_err(|_| failure())?,
    };
    let mut record = serde_json::json!({
        "kind": "lifecycle", "event": event, "ts": rec.at_ms,
        "principal": rec.principal, "session": rec.session, "route": rec.route,
        "connection": rec.connection, "actor": rec.actor, "reason": rec.reason,
    });
    if let LifecycleKind::MembershipReset { retired_domains, sessions } = &rec.kind {
        record["retired_domains"] = serde_json::json!(retired_domains);
        record["sessions"] = serde_json::json!(sessions);
    }
    if let Some(policy) = &rec.policy {
        record["policy"] = serde_json::to_value(policy).map_err(|_| failure())?;
    }
    let mut bytes = serde_json::to_vec(&record).map_err(|_| failure())?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn failure() -> Refusal {
    Refusal::new(NyCode::ReadLogUnavailable, "read log unavailable")
}
fn line(rec: &ReadLogRecord) -> Result<Vec<u8>, Refusal> {
    let outcome = match rec.outcome {
        Outcome::Allowed => "allowed",
        Outcome::Refused(c) => c.sqlstate(),
    };
    let mut bytes=serde_json::to_vec(&serde_json::json!({"ts":rec.ts_unix_ms,"kind":"statement","principal":rec.principal.to_hex(),"session":rec.session.to_hex(),"scope":rec.scope.to_hex(),"connection":rec.connection,"credential_id":rec.credential_id.map(|id| id.to_hex()),"route":rec.route,"policy_revision":rec.policy_revision,"config_revision":rec.config_revision,"fingerprint":rec.statement_fingerprint,"statement":rec.statement,"outcome":outcome,"rows":rec.rows,"tokens":rec.tokens_released})).map_err(|_|failure())?;
    bytes.push(b'\n');
    Ok(bytes)
}
/// Durable-enough per-record logging contract; failures always use NY009.
pub trait ReadLog: Send + Sync {
    /// Write and flush one JSON record.
    fn write(&self, rec: &ReadLogRecord) -> Result<(), Refusal>;
    /// Write and flush one lifecycle record.
    fn write_lifecycle(&self, rec: &LifecycleRecord) -> Result<(), Refusal>;
}
/// Standard-output JSON log.
pub struct StdoutLog;
impl ReadLog for StdoutLog {
    fn write(&self, rec: &ReadLogRecord) -> Result<(), Refusal> {
        self.write_bytes(&line(rec)?)
    }
    fn write_lifecycle(&self, rec: &LifecycleRecord) -> Result<(), Refusal> {
        self.write_bytes(&lifecycle_line(rec)?)
    }
}
impl StdoutLog {
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), Refusal> {
        let mut out = std::io::stdout().lock();
        out.write_all(bytes)
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
        self.write_bytes(&line(rec)?)
    }
    fn write_lifecycle(&self, rec: &LifecycleRecord) -> Result<(), Refusal> {
        self.write_bytes(&lifecycle_line(rec)?)
    }
}
impl FileLog {
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), Refusal> {
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
        if file.write_all(bytes).and_then(|_| file.flush()).is_err() {
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
    #[test]
    fn old_lifecycle_bytes_and_policy_payload_are_stable() {
        let mut record = lifecycle(LifecycleActor::Owner);
        let old = lifecycle_line(&record).unwrap();
        assert_eq!(old, format!("{{\"actor\":\"owner\",\"connection\":null,\"event\":\"session_ended\",\"kind\":\"lifecycle\",\"principal\":\"{}\",\"reason\":\"grant_removed\",\"route\":null,\"session\":\"{}\",\"ts\":42}}\n", "01".repeat(16), "02".repeat(16)).into_bytes());
        record.kind = LifecycleKind::PolicyApplied;
        record.policy = Some(crate::mgmt::audit::PolicyAudit {
            revision: Some(7), outcome: "active", summary: Default::default(),
            newly_visible: vec![serde_json::json!({"schema":"public","table":"users","column":"email"})], newly_masked: vec![],
        });
        let value: serde_json::Value = serde_json::from_slice(&lifecycle_line(&record).unwrap()).unwrap();
        assert_eq!(value["event"], "policy_applied");
        assert_eq!(value["policy"]["revision"], 7);
        assert_eq!(value["policy"]["newly_visible"][0]["column"], "email");
        record.kind = LifecycleKind::SessionEnded; record.policy = None;
        assert_eq!(lifecycle_line(&record).unwrap(), old);
    }

    #[test]
    fn membership_reset_audit_contains_only_route_domains_and_count() {
        let mut record = lifecycle(LifecycleActor::System);
        record.kind = LifecycleKind::MembershipReset { retired_domains: vec!["retired".into()], sessions: 4 };
        record.route = Some("postgres".into()); record.reason = "policy_reset";
        let value: serde_json::Value = serde_json::from_slice(&lifecycle_line(&record).unwrap()).unwrap();
        assert_eq!(value["event"], "membership_reset");
        assert_eq!(value["retired_domains"], serde_json::json!(["retired"]));
        assert_eq!(value["sessions"], 4);
        assert_eq!(value["route"], "postgres");
    }

    fn lifecycle(actor: LifecycleActor) -> LifecycleRecord {
        LifecycleRecord {
            policy: None, kind: LifecycleKind::SessionEnded, at_ms: 42,
            principal: "01".repeat(16), session: Some("02".repeat(16)),
            route: None, connection: None, actor, reason: "grant_removed",
        }
    }
    #[test]
    fn lifecycle_json_has_only_identifiers_and_codes() {
        for (actor, expected) in [
            (LifecycleActor::Admin("03".repeat(16)), serde_json::json!({"admin": "03".repeat(16)})),
            (LifecycleActor::System, serde_json::json!("system")),
        ] {
            let bytes = lifecycle_line(&lifecycle(actor)).unwrap();
            assert_eq!(bytes.last(), Some(&b'\n'));
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value, serde_json::json!({
                "kind": "lifecycle", "event": "session_ended", "ts": 42,
                "principal": "01".repeat(16), "session": "02".repeat(16),
                "route": null, "connection": null, "actor": expected, "reason": "grant_removed",
            }));
        }
        for (kind, code) in [
            (LifecycleKind::WriteOutcomeUnknown, "write_outcome_unknown"),
            (LifecycleKind::Issued, "issued"), (LifecycleKind::Renewed, "renewed"),
            (LifecycleKind::Revoked, "revoked"), (LifecycleKind::PrincipalDisabled, "principal_disabled"),
            (LifecycleKind::ResumeRefused, "resume_refused"), (LifecycleKind::ConnectionEnded, "connection_ended"),
        ] {
            assert_eq!(serde_json::to_value(kind).unwrap(), code);
        }
        for (actor, code) in [(LifecycleActor::Owner, "owner"), (LifecycleActor::ConfigAdmin, "config_admin"), (LifecycleActor::Socket, "socket")] {
            assert_eq!(serde_json::to_value(actor).unwrap(), code);
        }
    }
    #[test]
    fn write_outcome_unknown_json_is_data_free() {
        let mut record = lifecycle(LifecycleActor::System);
        record.kind = LifecycleKind::WriteOutcomeUnknown;
        record.route = Some("postgres".into());
        record.connection = Some(7);
        record.reason = "upstream_lost";
        let value: serde_json::Value = serde_json::from_slice(&lifecycle_line(&record).unwrap()).unwrap();
        assert_eq!(value, serde_json::json!({
            "kind": "lifecycle", "event": "write_outcome_unknown", "ts": 42,
            "principal": "01".repeat(16), "session": "02".repeat(16),
            "route": "postgres", "connection": 7, "actor": "system", "reason": "upstream_lost"
        }));
    }
    #[test]
    fn file_log_orders_both_record_kinds() {
        let dir = temp();
        let path = dir.join("audit");
        let log = FileLog::open(&path, u64::MAX).unwrap();
        let statement = record();
        let lifecycle = lifecycle(LifecycleActor::System);
        log.write(&statement).unwrap();
        log.write_lifecycle(&lifecycle).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), [line(&statement).unwrap(), lifecycle_line(&lifecycle).unwrap()].concat());
        drop(log);
        let log = FileLog::open(&path, 1).unwrap();
        log.write_lifecycle(&lifecycle).unwrap();
        assert_eq!(std::fs::read(dir.join("audit.1")).unwrap(), [line(&statement).unwrap(), lifecycle_line(&lifecycle).unwrap()].concat());
        assert_eq!(std::fs::read(&path).unwrap(), lifecycle_line(&lifecycle).unwrap());
        drop(log);
        std::fs::remove_dir_all(dir).unwrap();
    }
    fn record() -> ReadLogRecord {
        ReadLogRecord {
            ts_unix_ms: 42,
            principal: PrincipalId([1; 16]),
            session: SessionId([2; 16]),
            scope: ScopeId([3; 16]),
            connection: None,
            credential_id: None,
            route: "postgres".into(),
            policy_revision: None,
            config_revision: None,
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
        assert_eq!(v["principal"], "01".repeat(16));
        let keys: std::collections::BTreeSet<_> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, ["kind", "ts", "principal", "session", "scope", "connection", "credential_id", "route", "policy_revision", "config_revision", "fingerprint", "statement", "outcome", "rows", "tokens"].into_iter().collect());
        assert!(v["connection"].is_null());
        assert!(v["credential_id"].is_null());
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
