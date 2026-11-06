//! Bounded actual-driver evidence. An absent case is never evidence of support.
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    task::JoinHandle,
};

pub const CAP: usize = 256 * 1024;
pub const JDBC_CASES: &[(&str, &str)] = &[
    ("connect_init", "connected, initialized, autocommit true"),
    ("is_valid", "isValid(3) true"),
    ("simple_query", "zero rows for fixture id 8000"),
    ("prepared_empty", "zero rows for bound id 8000"),
    ("type_bigint", "exact bound BIGINT row identity"),
    ("type_text", "exact Unicode TEXT round trip"),
    (
        "type_flag",
        "false BOOLEAN, non-null, across prepare threshold",
    ),
    ("type_small", "negative SMALLINT across prepare threshold"),
    ("type_num", "negative INTEGER across prepare threshold"),
    ("type_data", "exact BYTEA 00 7f ff across prepare threshold"),
    (
        "typed_values",
        "exact BIGINT/TEXT/BOOLEAN/SMALLINT/INTEGER/BYTEA across prepare threshold",
    ),
    (
        "null_values",
        "SQL NULL and wasNull for every nullable supported type",
    ),
    ("rollback", "insert rolled back and absent"),
    ("commit", "committed insert visible to a second connection"),
    (
        "error_recovery",
        "23505 then 25P02 then rollback restores queries without leaked row",
    ),
    ("authorization", "42501 for other table"),
    (
        "metadata_tables",
        "exact visible table set app; other absent",
    ),
    (
        "metadata_columns",
        "six actual app columns with correct JDBC types",
    ),
    ("metadata_pk", "exact primary key id"),
    ("metadata_types", "all six supported JDBC types advertised"),
    ("metadata_hidden", "no columns for unauthorized other table"),
    (
        "repeat_prepare_close",
        "twelve prepare/query/close operations",
    ),
    ("close", "isClosed true and isValid false after close"),
    (
        "schema_fence",
        "prepared statement invalidated after schema change",
    ),
    (
        "cancel",
        "authenticated cancellation and subsequent recovery",
    ),
];
pub const TOKIO_CASES: &[(&str, &str)] = &[
    (
        "connect_init",
        "pinned tokio driver connects with verified mTLS",
    ),
    ("prepared_empty", "zero rows for bound BIGINT id 8000"),
    ("simple_query", "zero simple-query rows for id 8000"),
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Pass,
    Fail,
    Untested,
}
#[derive(Debug, Serialize)]
pub struct Case {
    pub id: String,
    pub expected: String,
    pub actual: String,
    pub status: Status,
}
#[derive(Debug, Serialize)]
pub struct Profile {
    pub client: String,
    pub mode: String,
    pub version: Option<String>,
    pub cases: Vec<Case>,
    pub integrity: Vec<String>,
    pub verdict: Status,
}

/// Strict private line protocol. PASS is accepted only from the checked-in asserting probe.
pub fn evaluate(
    client: &str,
    mode: &str,
    expected_version: &str,
    required: &[(&str, &str)],
    output: &Capture,
) -> Profile {
    let mut seen = BTreeMap::new();
    let mut integrity = Vec::new();
    let mut version = None;
    if output.code != Some(0) {
        integrity.push(format!("process exit {:?}", output.code));
    }
    if output.timed_out {
        integrity.push("process timeout".into());
    }
    if output.truncated {
        integrity.push("capture limit exceeded".into());
    }
    match std::str::from_utf8(&output.stdout) {
        Err(_) => integrity.push("stdout is not UTF-8".into()),
        Ok(s) => {
            for line in s.lines() {
                if line.len() > 8192 {
                    integrity.push("record length exceeded".into());
                    continue;
                }
                let fields: Vec<_> = line.split('\t').collect();
                if fields.len() == 2 && fields[0] == "VERSION" {
                    if version.replace(fields[1].to_owned()).is_some() {
                        integrity.push("duplicate version".into());
                    }
                } else if fields.len() == 4 && fields[0] == "CASE" {
                    let id = fields[1];
                    if !required.iter().any(|(key, _)| *key == id) {
                        integrity.push(format!("unknown case {id}"));
                        continue;
                    }
                    let status = match fields[2] {
                        "PASS" => Status::Pass,
                        "FAIL" => Status::Fail,
                        "UNTESTED" => Status::Untested,
                        _ => {
                            integrity.push("unknown status".into());
                            continue;
                        }
                    };
                    let actual = base64::engine::general_purpose::STANDARD
                        .decode(fields[3])
                        .ok()
                        .and_then(|b| String::from_utf8(b).ok());
                    match actual {
                        Some(actual) if actual.len() <= 4096 && !actual.is_empty() => {
                            if status == Status::Pass && actual != "assertions satisfied" {
                                integrity.push(format!("noncanonical PASS {id}"));
                            }
                            if seen.insert(id.to_owned(), (status, actual)).is_some() {
                                integrity.push(format!("duplicate case {id}"));
                            }
                        }
                        _ => integrity.push(format!("invalid actual {id}")),
                    }
                } else {
                    integrity.push("malformed/unsolicited stdout record".into());
                }
            }
        }
    }
    if version.as_deref() != Some(expected_version) {
        integrity.push(format!(
            "expected driver {expected_version}, observed {version:?}"
        ));
    }
    let cases = required
        .iter()
        .map(|(id, expected)| {
            let (status, actual) = seen.remove(*id).unwrap_or((
                Status::Fail,
                "required case missing from probe output".into(),
            ));
            Case {
                id: (*id).into(),
                expected: (*expected).into(),
                actual,
                status,
            }
        })
        .collect::<Vec<_>>();
    let verdict = if !integrity.is_empty() || cases.iter().any(|c| c.status == Status::Fail) {
        Status::Fail
    } else if cases.iter().any(|c| c.status == Status::Untested) {
        Status::Untested
    } else {
        Status::Pass
    };
    Profile {
        client: client.into(),
        mode: mode.into(),
        version,
        cases,
        integrity,
        verdict,
    }
}
pub fn untested(client: &str, mode: &str, required: &[(&str, &str)], reason: &str) -> Profile {
    Profile {
        client: client.into(),
        mode: mode.into(),
        version: None,
        cases: required
            .iter()
            .map(|(id, e)| Case {
                id: (*id).into(),
                expected: (*e).into(),
                actual: reason.into(),
                status: Status::Untested,
            })
            .collect(),
        integrity: vec![],
        verdict: Status::Untested,
    }
}

pub fn failed(client: &str, mode: &str, required: &[(&str, &str)], reason: &str) -> Profile {
    let mut p = untested(client, mode, required, reason);
    p.verdict = Status::Fail;
    p.integrity.push(reason.into());
    for case in &mut p.cases {
        case.status = Status::Fail;
    }
    p
}

#[derive(Debug, Default, Serialize)]
pub struct Capture {
    pub code: Option<i32>,
    pub timed_out: bool,
    pub truncated: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}
#[derive(Default)]
struct Buffer {
    bytes: Vec<u8>,
    truncated: bool,
}
type Shared = Arc<Mutex<Buffer>>;
async fn drain<R: AsyncRead + Unpin>(mut reader: R, buffer: Shared) -> std::io::Result<()> {
    let mut chunk = [0; 4096];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        let mut b = buffer.lock().expect("capture mutex");
        let keep = n.min(CAP - b.bytes.len());
        b.bytes.extend_from_slice(&chunk[..keep]);
        b.truncated |= keep < n;
    }
}
/// Owns exactly the process it spawned. Drop kills it if a caller returns early.
pub struct Managed {
    child: Child,
    out: Shared,
    err: Shared,
    readers: Vec<JoinHandle<std::io::Result<()>>>,
}
impl Managed {
    pub fn spawn(command: &mut Command) -> std::io::Result<Self> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let out = Shared::default();
        let err = Shared::default();
        let readers = vec![
            tokio::spawn(drain(child.stdout.take().expect("piped"), out.clone())),
            tokio::spawn(drain(child.stderr.take().expect("piped"), err.clone())),
        ];
        Ok(Self {
            child,
            out,
            err,
            readers,
        })
    }
    pub async fn port(&mut self, timeout: Duration) -> Result<u16, String> {
        let start = tokio::time::Instant::now();
        loop {
            {
                let b = self.out.lock().map_err(|e| e.to_string())?;
                if b.truncated {
                    return Err("server readiness output exceeded limit".into());
                }
                if let Some(end) = b.bytes.iter().position(|c| *c == b'\n') {
                    return parse_port(&b.bytes[..end]);
                }
            }
            if self.child.try_wait().map_err(|e| e.to_string())?.is_some() {
                return Err("server exited before readiness".into());
            }
            if start.elapsed() >= timeout {
                return Err("server readiness timeout".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    pub async fn finish(mut self, timeout: Duration, shutdown: bool) -> std::io::Result<Capture> {
        if shutdown && let Some(mut input) = self.child.stdin.take() {
            let _ = input.write_all(b"shutdown\n").await;
        }
        let (code, timed_out) = match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(result) => (result?.code(), false),
            Err(_) => {
                self.child.kill().await?;
                (self.child.wait().await?.code(), true)
            }
        };
        let mut reader_error = false;
        for mut reader in self.readers.drain(..) {
            match tokio::time::timeout(Duration::from_secs(2), &mut reader).await {
                Ok(Ok(Ok(()))) => {}
                _ => {
                    reader.abort();
                    reader_error = true;
                }
            }
        }
        let out = self.out.lock().expect("capture mutex");
        let err = self.err.lock().expect("capture mutex");
        Ok(Capture {
            code,
            timed_out,
            truncated: out.truncated || err.truncated || reader_error,
            stdout: out.bytes.clone(),
            stderr: err.bytes.clone(),
        })
    }
}
pub fn parse_port(line: &[u8]) -> Result<u16, String> {
    if line.len() > 1024 {
        return Err("readiness line too long".into());
    }
    let line = std::str::from_utf8(line).map_err(|e| e.to_string())?;
    let parts = line.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3
        || parts[2] != "preview=single-host"
        || !parts[0].starts_with("native=127.0.0.1:")
    {
        return Err("unexpected readiness line".into());
    }
    let port = parts[1]
        .strip_prefix("postgres=127.0.0.1:")
        .ok_or("non-loopback postgres listener")?
        .parse::<u16>()
        .map_err(|e| e.to_string())?;
    if port == 0 {
        return Err("zero postgres port".into());
    }
    Ok(port)
}
pub async fn run(command: &mut Command, timeout: Duration) -> std::io::Result<Capture> {
    Managed::spawn(command)?.finish(timeout, false).await
}
pub fn retain(dir: &Path, name: &str, capture: &Capture) -> std::io::Result<()> {
    std::fs::write(dir.join(format!("{name}.stdout.log")), &capture.stdout)?;
    std::fs::write(dir.join(format!("{name}.stderr.log")), &capture.stderr)?;
    std::fs::write(
        dir.join(format!("{name}.process.json")),
        serde_json::to_vec_pretty(capture)?,
    )
}
pub fn bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::other("input size limit"));
    }
    Ok(bytes)
}
#[derive(Debug, Serialize)]
pub struct Identity {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}
/// Emit and identify the exact source embedded in this executable, independently
/// of any checkout changes since the executable was built.
pub fn write_embedded_jdbc_probe(path: &Path) -> std::io::Result<Identity> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(include_bytes!("../probes/JdbcProbe.java"))?;
    drop(file);
    identity(path)
}

/// Identify the class files produced by the successful javac invocation.
pub fn jdbc_class_identities(dir: &Path) -> std::io::Result<Vec<Identity>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "JdbcProbe.class" || (name.starts_with("JdbcProbe$") && name.ends_with(".class"))
        {
            if !entry.file_type()?.is_file() || files.len() >= 32 {
                return Err(std::io::Error::other(
                    "invalid or excessive JDBC class files",
                ));
            }
            files.push(entry.path());
        }
    }
    if !files
        .iter()
        .any(|p| p.file_name().is_some_and(|n| n == "JdbcProbe.class"))
    {
        return Err(std::io::Error::other("missing compiled JDBC probe"));
    }
    files.sort();
    files.iter().map(|p| identity(p)).collect()
}
pub fn identity(path: &Path) -> std::io::Result<Identity> {
    let path = path.canonicalize()?;
    let mut file = std::fs::File::open(&path)?;
    let mut hash = Sha256::new();
    let mut chunk = [0; 65536];
    let mut bytes = 0;
    loop {
        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        hash.update(&chunk[..n]);
    }
    Ok(Identity {
        path,
        bytes,
        sha256: format!("{:x}", hash.finalize()),
    })
}

/// Snapshot either a legacy single-file DB or the current bounded database directory.
/// Reject links and special files; preserve no paths outside the supplied tree.
pub fn copy_fixture_tree(
    src: &Path,
    dest: &Path,
    limit: u64,
) -> Result<Vec<Identity>, Box<dyn std::error::Error>> {
    let root = src.canonicalize()?;
    let mut pending = vec![(src.to_path_buf(), dest.to_path_buf(), 0)];
    let mut count = 0;
    let mut total = 0u64;
    let mut identities = Vec::new();
    while let Some((from, to, depth)) = pending.pop() {
        count += 1;
        if count > 1024 || depth > 8 {
            return Err("fixture tree entry/depth limit".into());
        }
        let meta = std::fs::symlink_metadata(&from)?;
        if meta.file_type().is_symlink() || !from.canonicalize()?.starts_with(&root) {
            return Err("fixture tree link/escape rejected".into());
        }
        if meta.is_dir() {
            std::fs::create_dir(&to)?;
            let mut entries = Vec::new();
            for entry in std::fs::read_dir(&from)? {
                entries.push(entry?);
                if entries.len() > 1024 {
                    return Err("fixture directory entry limit".into());
                }
            }
            entries.sort_by_key(|e| e.file_name());
            for entry in entries {
                pending.push((entry.path(), to.join(entry.file_name()), depth + 1));
            }
        } else if meta.is_file() {
            total = total
                .checked_add(meta.len())
                .ok_or("fixture size overflow")?;
            if total > limit {
                return Err("fixture total size limit".into());
            }
            let before = identity(&from)?;
            std::fs::copy(&from, &to)?;
            if identity(&to)?.sha256 != before.sha256 || identity(&from)?.sha256 != before.sha256 {
                return Err("fixture changed while copying".into());
            }
            identities.push(before);
        } else {
            return Err("fixture special file rejected".into());
        }
    }
    Ok(identities)
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub server: PathBuf,
    pub fixture: PathBuf,
    pub server_config: PathBuf,
    pub config_base: PathBuf,
    pub output: PathBuf,
    pub java: Option<PathBuf>,
    pub javac: Option<PathBuf>,
    pub jdbc_jar: Option<PathBuf>,
    pub expected_server_sha256: Option<String>,
    pub expected_jar_sha256: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}
fn default_timeout() -> u64 {
    60
}
impl Manifest {
    pub fn read(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let m: Self = serde_json::from_slice(&bounded(path, 65536)?)?;
        for p in [
            &m.server,
            &m.fixture,
            &m.server_config,
            &m.config_base,
            &m.output,
        ]
        .into_iter()
        .chain(m.java.iter())
        .chain(m.javac.iter())
        .chain(m.jdbc_jar.iter())
        {
            if !p.is_absolute() {
                return Err(format!("absolute path required: {}", p.display()).into());
            }
        }
        if !(1..=300).contains(&m.timeout_seconds) {
            return Err("timeout_seconds must be 1..300".into());
        }
        Ok(m)
    }
}
/// Only copy known fixture inputs. Never open the supplied database through the server.
pub fn isolate(m: &Manifest, dir: &Path) -> Result<Vec<Identity>, Box<dyn std::error::Error>> {
    let root = m.fixture.canonicalize()?;
    let mut config: serde_json::Value =
        serde_json::from_slice(&bounded(&m.server_config, 1024 * 1024)?)?;
    if !config.is_object() {
        return Err("server config must be an object".into());
    }
    if config.get("bindings_file").is_some_and(|v| !v.is_null()) {
        return Err("external bindings_file not supported; use inline fixture bindings".into());
    }
    let mut identities = Vec::new();
    for (field, name, limit) in [
        ("database", "db.redb", 512 * 1024 * 1024),
        ("certificate", "server.pem", 65536),
        ("private_key", "server.key.TEST-ONLY", 65536),
        ("client_ca", "ca.pem", 65536),
    ] {
        let src = config
            .get(field)
            .and_then(|v| v.as_str())
            .ok_or("missing fixture config path")?;
        let src = m.config_base.join(src).canonicalize()?;
        if !src.starts_with(&root) {
            return Err(format!("{field} outside explicit fixture").into());
        }
        let dest = dir.join(name);
        if field != "database" && !src.is_file() {
            return Err(format!("{field} must be file").into());
        }
        identities.extend(copy_fixture_tree(&src, &dest, limit)?);
        config[field] = dest.to_string_lossy().into_owned().into();
    }
    for name in ["app.pem", "app.key.TEST-ONLY"] {
        let src = root.join(name).canonicalize()?;
        if !src.starts_with(&root) {
            return Err("client credential escaped fixture".into());
        }
        let data = bounded(&src, 65536)?;
        std::fs::write(dir.join(name), data)?;
        identities.push(identity(&src)?);
    }
    config["native"] = "127.0.0.1:0".into();
    config["postgres"] = "127.0.0.1:0".into();
    std::fs::write(dir.join("server.json"), serde_json::to_vec_pretty(&config)?)?;
    Ok(identities)
}
