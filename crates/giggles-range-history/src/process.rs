use crate::{Result, require, save};
use giggles_distributed::{
    Descriptor,
    runtime::{Config, GroupConfig, Initial},
    transport::{Configuration, Peer, fingerprint},
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};
pub const TIMEOUT: Duration = Duration::from_secs(60);
pub struct Trace {
    pub tick: u64,
    file: File,
    start: Instant,
}
pub type Shared = Arc<Mutex<Trace>>;
pub fn event(trace: &Shared, node: u64, kind: &str, value: Value) -> Result<u64> {
    let mut t = trace.lock().map_err(|_| "trace poisoned")?;
    t.tick = t.tick.checked_add(1).ok_or("trace sequence exhausted")?;
    let record = json!({"tick":t.tick,"elapsed_ns":t.start.elapsed().as_nanos().to_string(),"node":node,"kind":kind,"value":value});
    writeln!(t.file, "{record}")?;
    t.file.flush()?;
    Ok(t.tick)
}
pub struct Process {
    pub id: u64,
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    reader: Option<JoinHandle<Result<()>>>,
    trace: Shared,
    pending: bool,
}
fn poll_response(
    pending: &mut bool,
    output: &mpsc::Receiver<Value>,
    wait: Duration,
) -> Result<Option<Value>> {
    require(*pending, "no pending request")?;
    match output.recv_timeout(wait) {
        Ok(value) => {
            *pending = false;
            Ok(Some(value))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
        Err(error) => Err(error.into()),
    }
}
impl Process {
    fn start(binary: &Path, root: &Path, id: u64, round: u64, trace: Shared) -> Result<Self> {
        let mut log = File::create_new(root.join(format!("stdout-{id}-{round}.jsonl")))?;
        let mut child = Command::new(binary)
            .arg(root.join(format!("config-{id}.json")))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(File::create_new(
                root.join(format!("stderr-{id}-{round}.log")),
            )?)
            .spawn()?;
        // Construct the owning guard before any fallible readiness work.
        let input = child.stdin.take().ok_or("missing stdin")?;
        let stdout = child.stdout.take().ok_or("missing stdout")?;
        let (sender, output) = mpsc::channel();
        let capture = trace.clone();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line?;
                writeln!(log, "{line}")?;
                log.flush()?;
                let v: Value = serde_json::from_str(&line)?;
                event(&capture, id, "stdout", v.clone())?;
                if sender.send(v).is_err() {
                    break;
                }
            }
            Ok(())
        });
        let p = Self {
            id,
            child,
            input,
            output,
            reader: Some(reader),
            trace,
            pending: false,
        };
        event(
            &p.trace,
            id,
            "spawn",
            json!({"pid":p.child.id(),"incarnation":round}),
        )?;
        require(
            p.output.recv_timeout(TIMEOUT)?["ready"] == true,
            "missing ready",
        )?;
        Ok(p)
    }
    pub fn send(&mut self, v: Value) -> Result<u64> {
        require(!self.pending, "uncorrelated request already pending")?;
        let t = event(&self.trace, self.id, "invoke", v.clone())?;
        self.pending = true;
        writeln!(self.input, "{v}")?;
        self.input.flush()?;
        Ok(t)
    }
    pub fn receive(&mut self) -> Result<(u64, Value)> {
        self.poll_receive(TIMEOUT)?
            .ok_or_else(|| "owner response timeout; request remains pending".into())
    }
    pub fn poll_receive(&mut self, wait: Duration) -> Result<Option<(u64, Value)>> {
        let v = match poll_response(&mut self.pending, &self.output, wait)? {
            Some(v) => v,
            None => {
                event(
                    &self.trace,
                    self.id,
                    "pending",
                    json!({"wait_ms":wait.as_millis()}),
                )?;
                return Ok(None);
            }
        };
        let t = event(&self.trace, self.id, "complete", v.clone())?;
        Ok(Some((t, v)))
    }
    pub fn request(&mut self, v: Value) -> Result<(u64, u64, Value)> {
        let i = self.send(v)?;
        let (c, v) = self.receive()?;
        Ok((i, c, v))
    }
    fn stop(&mut self) -> Result<()> {
        if self.reader.is_none() {
            return Ok(());
        }
        let before = event(
            &self.trace,
            self.id,
            "terminate",
            json!({"pid":self.child.id()}),
        );
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        let status = self.child.wait()?;
        let after = event(
            &self.trace,
            self.id,
            "terminated",
            json!({"status":status.to_string()}),
        );
        if let Some(r) = self.reader.take() {
            r.join().map_err(|_| "reader panic")??;
        }
        before?;
        after?;
        Ok(())
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
pub struct Cluster {
    pub nodes: BTreeMap<u64, Process>,
    pub trace: Shared,
    root: PathBuf,
    binary: PathBuf,
    round: u64,
}
impl Cluster {
    pub fn events(&self) -> Result<Vec<Value>> {
        Ok(std::fs::read_to_string(self.root.join("events.jsonl"))?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn new(binary: &Path, root: &Path, descriptor: Descriptor) -> Result<Self> {
        let trace = Arc::new(Mutex::new(Trace {
            tick: 0,
            file: File::create_new(root.join("events.jsonl"))?,
            start: Instant::now(),
        }));
        let mut cp = CertificateParams::new(Vec::<String>::new())?;
        cp.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        cp.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cak = KeyPair::generate()?;
        let ca = cp.self_signed(&cak)?;
        std::fs::write(root.join("ca.pem"), ca.pem())?;
        let mut leaves = BTreeMap::new();
        for id in 1..=3 {
            let mut p = CertificateParams::new(vec!["localhost".into()])?;
            p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            p.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let k = KeyPair::generate()?;
            let c = p.signed_by(&k, &ca, &cak)?;
            std::fs::write(root.join(format!("{id}.pem")), c.pem())?;
            std::fs::write(root.join(format!("{id}.key.pem")), k.serialize_pem())?;
            leaves.insert(id, fingerprint(c.der()));
        }
        let mut configs = BTreeMap::<u64, Vec<GroupConfig>>::new();
        let mut listeners = vec![];
        for group in 1..=4 {
            let mut peers = BTreeMap::new();
            for id in 1..=3 {
                let l = std::net::TcpListener::bind("127.0.0.1:0")?;
                peers.insert(
                    id,
                    Peer {
                        address: l.local_addr()?.to_string(),
                        fingerprint: leaves[&id].clone(),
                    },
                );
                listeners.push(l);
            }
            for id in 1..=3 {
                configs.entry(id).or_default().push(GroupConfig {
                    initial: match group {
                        1 => Initial::Directory(descriptor.clone()),
                        2 => Initial::Data(descriptor.clone()),
                        _ => Initial::Vacant,
                    },
                    peer: Configuration {
                        id,
                        domain: 42,
                        group,
                        database: root.join(format!("node-{id}-group-{group}.redb")),
                        listen: peers[&id].address.clone(),
                        certificate: root.join(format!("{id}.pem")),
                        key: root.join(format!("{id}.key.pem")),
                        ca: root.join("ca.pem"),
                        peers: peers.clone(),
                    },
                });
            }
        }
        for (node, groups) in configs {
            save(
                &root.join(format!("config-{node}.json")),
                &Config {
                    domain: 42,
                    node,
                    groups,
                },
            )?;
        }
        drop(listeners);
        let mut c = Self {
            nodes: BTreeMap::new(),
            trace,
            root: root.into(),
            binary: binary.into(),
            round: 0,
        };
        for id in 1..=3 {
            c.start(id)?;
        }
        c.colocate()?;
        Ok(c)
    }
    pub fn start(&mut self, id: u64) -> Result<()> {
        self.round += 1;
        self.nodes.insert(
            id,
            Process::start(&self.binary, &self.root, id, self.round, self.trace.clone())?,
        );
        Ok(())
    }
    pub fn kill(&mut self, id: u64) -> Result<()> {
        let mut p = self.nodes.remove(&id).ok_or("missing process")?;
        p.stop()?;
        Ok(())
    }
    pub fn shutdown(&mut self) -> Result<()> {
        let mut error = None;
        for process in self.nodes.values_mut() {
            if let Err(e) = process.stop() {
                error.get_or_insert(e);
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    pub fn request(&mut self, id: u64, v: Value) -> Result<(u64, u64, Value)> {
        self.nodes.get_mut(&id).ok_or("missing process")?.request(v)
    }
    pub fn request_until(&mut self, id: u64, v: Value, end: Instant) -> Result<(u64, u64, Value)> {
        require(Instant::now() < end, "request deadline exhausted")?;
        let p = self.nodes.get_mut(&id).ok_or("missing process")?;
        let i = p.send(v)?;
        let (t, v) = p
            .poll_receive(end.saturating_duration_since(Instant::now()))?
            .ok_or("request deadline exhausted; unresolved response")?;
        Ok((i, t, v))
    }
    pub fn colocate_until(&mut self, end: Instant) -> Result<()> {
        for group in 1..=4 {
            self.request_until(1, json!({"command":"campaign","group":group}), end)?;
            loop {
                require(Instant::now() < end, "colocation deadline exhausted")?;
                let mut leader = None;
                for id in self.nodes.keys().copied().collect::<Vec<_>>() {
                    let (_, _, v) =
                        self.request_until(id, json!({"command":"inspect","group":group}), end)?;
                    if v["ok"] == true && v["data"]["is_leader"] == true {
                        leader = Some(id);
                        break;
                    }
                }
                if leader == Some(1) {
                    break;
                }
                if let Some(id) = leader {
                    self.request_until(
                        id,
                        json!({"command":"transfer","group":group,"to":1}),
                        end,
                    )?;
                }
                std::thread::yield_now();
            }
        }
        Ok(())
    }
    pub fn ok(&mut self, id: u64, v: Value) -> Result<Value> {
        let (_, _, r) = self.request(id, v)?;
        require(r["ok"] == true, &format!("owner refused: {r}"))?;
        Ok(r["data"].clone())
    }
    pub fn leader(&mut self, group: u64) -> Result<u64> {
        let end = Instant::now() + TIMEOUT;
        loop {
            for id in self.nodes.keys().copied().collect::<Vec<_>>() {
                let (_, _, v) = self.request(id, json!({"command":"inspect","group":group}))?;
                if v["ok"] == true && v["data"]["is_leader"] == true {
                    return Ok(id);
                }
            }
            require(Instant::now() < end, "leader election timeout")?;
            std::thread::yield_now();
        }
    }
    pub fn transfer(&mut self, group: u64, to: u64) -> Result<()> {
        let end = Instant::now() + TIMEOUT;
        loop {
            let leader = self.leader(group)?;
            if leader == to {
                return Ok(());
            }
            self.request(leader, json!({"command":"transfer","group":group,"to":to}))?;
            require(Instant::now() < end, "leader transfer timeout")?;
        }
    }
    pub fn colocate(&mut self) -> Result<()> {
        for group in 1..=4 {
            self.request(1, json!({"command":"campaign","group":group}))?;
            self.transfer(group, 1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn polling_timeout_retains_pending_and_late_response() {
        let (sender, output) = mpsc::channel();
        let mut pending = true;
        assert!(
            poll_response(&mut pending, &output, Duration::ZERO)
                .unwrap()
                .is_none()
        );
        assert!(pending);
        sender.send(json!({"ok":true})).unwrap();
        assert_eq!(
            poll_response(&mut pending, &output, Duration::ZERO).unwrap(),
            Some(json!({"ok":true}))
        );
        assert!(!pending);
        assert!(poll_response(&mut pending, &output, Duration::ZERO).is_err());
    }
    #[test]
    fn polling_without_pending_rejected() {
        let (_sender, output) = mpsc::channel();
        assert!(poll_response(&mut false, &output, Duration::ZERO).is_err());
    }
    #[test]
    fn concurrent_capture_has_one_total_monotonic_order() {
        let root = crate::fresh(
            &std::env::temp_dir().join("giggles-range-history-retained"),
            "trace",
        )
        .unwrap();
        let trace = Arc::new(Mutex::new(Trace {
            tick: 0,
            file: File::create_new(root.join("events.jsonl")).unwrap(),
            start: Instant::now(),
        }));
        let workers = (0..4)
            .map(|node| {
                let t = trace.clone();
                std::thread::spawn(move || {
                    for n in 0..64 {
                        event(&t, node, "observation", json!({"n":n})).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        let input = std::fs::read_to_string(root.join("events.jsonl")).unwrap();
        for (n, line) in input.lines().enumerate() {
            let v: Value = serde_json::from_str(line).unwrap();
            assert_eq!(v["tick"].as_u64().unwrap(), n as u64 + 1);
        }
        assert_eq!(input.lines().count(), 256);
    }
}
