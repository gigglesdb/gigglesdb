//! Explicit local process adapter. Configuration and binary are owner-trusted.
use crate::{Action, Code, Error, MAX_BYTES, Owner, Result};
use giggles_distributed::runtime::Observation;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    pub binary: PathBuf,
    pub config: PathBuf,
    pub artifacts: PathBuf,
    pub timeout_ms: u64,
}
pub struct Process {
    child: Child,
    input: mpsc::SyncSender<(Vec<u8>, mpsc::SyncSender<std::io::Result<()>>)>,
    output: mpsc::Receiver<Result<Value>>,
    timeout: Duration,
    poisoned: bool,
}
fn transport(e: impl ToString) -> Error {
    Error::new(Code::Transport, e)
}
impl Process {
    /// Spawn with pipes, never a shell. Artifacts must be a new retained directory.
    pub fn start(c: &ProcessConfig) -> Result<Self> {
        if !(100..=30_000).contains(&c.timeout_ms)
            || !c.binary.is_absolute()
            || !c.config.is_absolute()
        {
            return Err(transport(
                "absolute binary/config and timeout 100..30000ms required",
            ));
        }
        std::fs::create_dir(&c.artifacts).map_err(transport)?;
        let mut outlog = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(c.artifacts.join("stdout.jsonl"))
            .map_err(transport)?;
        let mut errlog = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(c.artifacts.join("stderr.log"))
            .map_err(transport)?;
        let mut inlog = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(c.artifacts.join("stdin.jsonl"))
            .map_err(transport)?;
        let mut child = Command::new(&c.binary)
            .arg(&c.config)
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(transport)?;
        let mut stdin = child.stdin.take().ok_or_else(|| transport("stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| transport("stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| transport("stderr"))?;
        let (send, output) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut total = 0usize;
            loop {
                let mut bytes = Vec::new();
                let result = reader
                    .by_ref()
                    .take((MAX_BYTES + 1) as u64)
                    .read_until(b'\n', &mut bytes);
                let value = match result {
                    Ok(0) => Err(transport("owner EOF")),
                    Ok(_) => {
                        total = total.saturating_add(bytes.len());
                        if bytes.len() > MAX_BYTES
                            || bytes.last() != Some(&b'\n')
                            || total > 64 * 1024 * 1024
                        {
                            Err(Error::new(Code::Limit, "owner output bound"))
                        } else if let Err(e) =
                            outlog.write_all(&bytes).and_then(|()| outlog.flush())
                        {
                            Err(transport(e))
                        } else {
                            serde_json::from_slice(&bytes).map_err(transport)
                        }
                    }
                    Err(e) => Err(transport(e)),
                };
                let failed = value.is_err();
                if send.send(value).is_err() || failed {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut stderr.take(64 * 1024 * 1024), &mut errlog);
        });
        let (input, writer) =
            mpsc::sync_channel::<(Vec<u8>, mpsc::SyncSender<std::io::Result<()>>)>(1);
        std::thread::spawn(move || {
            let mut total = 0usize;
            while let Ok((bytes, ack)) = writer.recv() {
                total = total.saturating_add(bytes.len());
                let r = if total > 64 * 1024 * 1024 {
                    Err(std::io::Error::other("owner input log bound"))
                } else {
                    inlog
                        .write_all(&bytes)
                        .and_then(|()| inlog.flush())
                        .and_then(|()| stdin.write_all(&bytes))
                        .and_then(|()| stdin.flush())
                };
                let failed = r.is_err();
                let _ = ack.send(r);
                if failed {
                    break;
                }
            }
        });
        let mut p = Self {
            child,
            input,
            output,
            timeout: Duration::from_millis(c.timeout_ms),
            poisoned: false,
        };
        let ready = p.receive()?;
        if ready != json!({"ready":true}) {
            p.poison();
            return Err(transport("owner readiness protocol"));
        }
        Ok(p)
    }
    fn poison(&mut self) {
        self.poisoned = true;
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    fn receive(&mut self) -> Result<Value> {
        match self.output.recv_timeout(self.timeout) {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                self.poison();
                Err(e)
            }
            Err(e) => {
                self.poison();
                Err(transport(format!("indeterminate response: {e}")))
            }
        }
    }
    /// Trusted local test/operator access; placement itself emits only three commands.
    pub fn request(&mut self, value: Value) -> Result<Value> {
        if self.poisoned {
            return Err(transport(
                "owner pipe poisoned; restart from retained config",
            ));
        }
        let mut bytes = serde_json::to_vec(&value).map_err(transport)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_BYTES {
            return Err(Error::new(Code::Limit, "owner command bytes"));
        }
        let (tx, rx) = mpsc::sync_channel(1);
        if let Err(e) = self.input.try_send((bytes, tx)) {
            self.poison();
            return Err(transport(e));
        }
        match rx.recv_timeout(self.timeout) {
            Ok(Ok(())) => {}
            r => {
                self.poison();
                return Err(transport(format!("indeterminate write: {r:?}")));
            }
        }
        let reply = self.receive()?;
        if reply["ok"] != true {
            return Err(Error::new(Code::Runtime, reply["error"].to_string()));
        }
        reply
            .get("data")
            .cloned()
            .ok_or_else(|| transport("missing owner data"))
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.poison();
    }
}
/// Bounded roster of already provisioned processes; no cloud administration.
pub struct StdioOwner {
    pub processes: BTreeMap<u64, Process>,
    leaders: BTreeMap<u64, u64>,
}
impl StdioOwner {
    pub fn start(configs: BTreeMap<u64, ProcessConfig>) -> Result<Self> {
        if configs.is_empty() || configs.len() > 31 || configs.contains_key(&0) {
            return Err(transport("owner roster bounds"));
        }
        let mut processes = BTreeMap::new();
        for (id, c) in configs {
            processes.insert(id, Process::start(&c)?);
        }
        Ok(Self {
            processes,
            leaders: BTreeMap::new(),
        })
    }
}
impl Owner for StdioOwner {
    fn current(&mut self, group: u64) -> Result<Observation> {
        let mut ids: Vec<_> = self.processes.keys().copied().collect();
        if let Some(id) = self.leaders.get(&group) {
            ids.sort_by_key(|n| n != id);
        }
        let mut errors = Vec::new();
        for id in ids {
            let result = self
                .processes
                .get_mut(&id)
                .expect("roster")
                .request(json!({"command":"current","group":group}));
            match result.and_then(|v| serde_json::from_value::<Observation>(v).map_err(transport)) {
                Ok(o) if o.is_leader && o.quorum_checked && o.leader == id => {
                    self.leaders.insert(group, id);
                    return Ok(o);
                }
                Ok(_) => errors.push(format!("{id}: non-authoritative current")),
                Err(e) => errors.push(format!("{id}: {e}")),
            }
        }
        Err(Error::new(Code::NoQuorum, errors.join("; ")))
    }
    fn act(&mut self, action: &Action) -> Result<()> {
        let (group, v) = match action {
            Action::Begin { group, intent } => (
                *group,
                json!({"command":"membership_begin","group":group,"intent":intent}),
            ),
            Action::Resume { group, id } => (
                *group,
                json!({"command":"membership_resume","group":group,"id":id}),
            ),
        };
        let id = self
            .leaders
            .get(&group)
            .ok_or_else(|| transport("fresh current required"))?;
        let v = self
            .processes
            .get_mut(id)
            .ok_or_else(|| transport("leader missing"))?
            .request(v)?;
        let o: Observation = serde_json::from_value(v).map_err(transport)?;
        if o.reply.is_some_and(|r| r.code != 0) {
            return Err(Error::new(Code::Runtime, "membership apply rejected"));
        }
        Ok(())
    }
}
