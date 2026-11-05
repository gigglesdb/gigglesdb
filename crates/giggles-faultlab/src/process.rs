//! Retained fixtures and bounded control of explicitly owned child processes.
use crate::Result;
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const MAX_LINE: usize = 1024 * 1024;
pub const MAX_LOG: u64 = 16 * 1024 * 1024;
pub fn file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash.finalize().iter().map(|b| format!("{b:02x}")).collect())
}
pub fn fresh_directory(label: &str) -> Result<PathBuf> {
    if label.is_empty()
        || label.len() > 40
        || !label
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
    {
        return Err("invalid fixture label".into());
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    for _ in 0..32 {
        let path = std::env::temp_dir().join(format!(
            "giggles-faultlab-{label}-{}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err("unique fixture path exhausted".into())
}
pub fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
pub struct OwnedNode {
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    failed: Arc<AtomicBool>,
    invocations: File,
    invoked: u64,
    readers: Vec<std::thread::JoinHandle<()>>,
}
impl OwnedNode {
    pub fn start(binary: &Path, config: &Path, root: &Path, id: u64) -> Result<Self> {
        let mut command = Command::new(binary);
        command.arg(config);
        Self::start_command(command, root, id)
    }
    /// Spawn and own a caller-configured local command using the node JSON control protocol.
    pub fn start_command(mut command: Command, root: &Path, id: u64) -> Result<Self> {
        let mut err = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join(format!("node-{id}.stderr.log")))?;
        let mut log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join(format!("node-{id}.stdout.jsonl")))?;
        let invocations = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join(format!("node-{id}.invocations.jsonl")))?;
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().ok_or("child stdin missing")?;
        let stdout = child.stdout.take().ok_or("child stdout missing")?;
        let (tx, output) = mpsc::sync_channel(128);
        let failed = Arc::new(AtomicBool::new(false));
        let failure = failed.clone();
        let stderr = child.stderr.take().ok_or("child stderr missing")?;
        let stderr_failure = failed.clone();
        let err_reader = std::thread::spawn(move || {
            let result = std::io::copy(&mut stderr.take(MAX_LOG + 1), &mut err);
            if result.is_err() || result.is_ok_and(|n| n > MAX_LOG) {
                stderr_failure.store(true, Ordering::Release);
            }
        });
        let out_reader = std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut reader = BufReader::new(stdout);
                let mut total = 0;
                loop {
                    let mut line = Vec::new();
                    let n = Read::by_ref(&mut reader)
                        .take(MAX_LINE as u64 + 1)
                        .read_until(b'\n', &mut line)?;
                    if n == 0 {
                        break;
                    }
                    total += n as u64;
                    if n > MAX_LINE || total > MAX_LOG {
                        return Err("child output cap exceeded".into());
                    }
                    log.write_all(&line)?;
                    log.flush()?;
                    if let Ok(value) = serde_json::from_slice(&line) {
                        tx.try_send(value)
                            .map_err(|_| "child output channel cap exceeded")?;
                    }
                }
                Ok(())
            })();
            if result.is_err() {
                failure.store(true, Ordering::Release);
            }
        });
        let mut node = Self {
            child,
            input,
            output,
            failed,
            invocations,
            invoked: 0,
            readers: vec![err_reader, out_reader],
        };
        node.until(|v| v["ready"] == true, Duration::from_secs(8))?;
        Ok(node)
    }
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
    pub fn send(&mut self, value: Value) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err("child output failed or exceeded bounds".into());
        }
        let bytes = serde_json::to_vec(&value)?;
        self.invoked += bytes.len() as u64 + 1;
        if bytes.len() > MAX_LINE || self.invoked > MAX_LOG {
            return Err("child input log cap exceeded".into());
        }
        self.invocations.write_all(&bytes)?;
        self.invocations.write_all(b"\n")?;
        self.invocations.flush()?;
        self.input.write_all(&bytes)?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;
        Ok(())
    }
    pub fn until(
        &mut self,
        predicate: impl Fn(&Value) -> bool,
        timeout: Duration,
    ) -> Result<Value> {
        let start = Instant::now();
        loop {
            if self.failed.load(Ordering::Acquire) {
                return Err("child output failed or exceeded bounds".into());
            }
            let remain = timeout
                .checked_sub(start.elapsed())
                .ok_or("child response deadline")?;
            let value = self.output.recv_timeout(remain)?;
            if predicate(&value) {
                return Ok(value);
            }
        }
    }
    pub fn control(&mut self, id: u64, mut value: Value) -> Result<Value> {
        value["id"] = json!(id);
        self.send(value)?;
        self.until(|v| v["id"] == id, Duration::from_secs(8))
    }
    pub fn admin(&mut self, id: u64, value: Value) -> Result<Value> {
        let command = giggles_consensus::Command::Admin(serde_json::to_vec(&value)?);
        self.control(id, json!({"op":"propose","command":command}))
    }
    pub fn diagnostics(&mut self) -> Result<Value> {
        self.send(json!({"op":"diagnostics"}))?;
        self.until(|v| v.get("is_leader").is_some(), Duration::from_secs(3))
    }
    pub fn terminate(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        for reader in self.readers.drain(..) {
            reader.join().map_err(|_| "child log reader panicked")?;
        }
        if self.failed.load(Ordering::Acquire) {
            return Err("child log capture failed or exceeded cap".into());
        }
        Ok(())
    }
}
impl Drop for OwnedNode {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}
