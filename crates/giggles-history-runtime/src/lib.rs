//! External observations of three actual consensus processes.
#![forbid(unsafe_code)]
use giggles_history::{
    Attempt, History, Invariants, Limits, Observation, Op, Outcome, State, Verdict,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
const TIMEOUT: Duration = Duration::from_secs(15);
const KEYS: i64 = 8;

fn ensure(test: bool, message: &str) -> Result<()> {
    if test {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}
fn hash(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(std::fs::read(path)?)))
}
fn save(path: &Path, value: &Value) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

struct Trace {
    tick: u64,
    file: File,
    start: Instant,
}
impl Trace {
    fn event(&mut self, node: u64, kind: &str, value: Value) -> Result<u64> {
        self.tick += 1;
        writeln!(
            self.file,
            "{}",
            json!({"tick":self.tick,"elapsed_ns":self.start.elapsed().as_nanos().to_string(),"node":node,"kind":kind,"value":value})
        )?;
        self.file.flush()?;
        Ok(self.tick)
    }
}
type Shared = Arc<Mutex<Trace>>;
fn event(trace: &Shared, node: u64, kind: &str, value: Value) -> Result<u64> {
    trace
        .lock()
        .map_err(|_| "trace poisoned")?
        .event(node, kind, value)
}

struct Process {
    id: u64,
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    pending: Mutex<VecDeque<Value>>,
    reader: Option<JoinHandle<Result<()>>>,
    trace: Shared,
}
impl Process {
    fn start(binary: &Path, root: &Path, id: u64, round: u64, trace: Shared) -> Result<Self> {
        let mut child = Command::new(binary)
            .arg(root.join(format!("node-{id}.json")))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(File::create_new(
                root.join(format!("node-{id}-{round}.stderr.log")),
            )?)
            .spawn()?;
        event(&trace, id, "spawn", json!({"pid":child.id(),"round":round}))?;
        let input = child.stdin.take().ok_or("missing stdin")?;
        let stdout = child.stdout.take().ok_or("missing stdout")?;
        let (tx, output) = mpsc::channel();
        let mut log = File::create_new(root.join(format!("node-{id}-{round}.stdout.jsonl")))?;
        let capture = trace.clone();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line?;
                writeln!(log, "{line}")?;
                log.flush()?;
                let value: Value = serde_json::from_str(&line)?;
                event(&capture, id, "wire_reply", value.clone())?;
                if tx.send(value).is_err() {
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
            pending: Mutex::new(VecDeque::new()),
            reader: Some(reader),
            trace,
        };
        p.until(|v| v["ready"] == true, TIMEOUT)?;
        Ok(p)
    }
    fn send(&mut self, value: Value) -> Result<u64> {
        let tick = event(&self.trace, self.id, "invoke", value.clone())?;
        writeln!(self.input, "{value}")?;
        self.input.flush()?;
        Ok(tick)
    }
    fn until(&self, predicate: impl Fn(&Value) -> bool, timeout: Duration) -> Result<Value> {
        {
            let mut pending = self.pending.lock().map_err(|_| "pending poisoned")?;
            if let Some(index) = pending.iter().position(&predicate) {
                return pending
                    .remove(index)
                    .ok_or_else(|| "pending reply missing".into());
            }
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remain = deadline
                .checked_duration_since(Instant::now())
                .ok_or("response deadline")?;
            let value = self.output.recv_timeout(remain)?;
            if predicate(&value) {
                return Ok(value);
            }
            if value["id"].is_u64() {
                self.pending
                    .lock()
                    .map_err(|_| "pending poisoned")?
                    .push_back(value);
            }
        }
    }
    fn reply(&self, id: u64, timeout: Duration) -> Result<(Value, u64)> {
        let value = self.until(|v| v["id"] == id, timeout)?;
        let tick = event(&self.trace, self.id, "deliver", value.clone())?;
        Ok((value, tick))
    }
    fn request(&mut self, value: Value) -> Result<Value> {
        let id = value["id"].as_u64().ok_or("request id missing")?;
        self.send(value)?;
        Ok(self.reply(id, TIMEOUT)?.0)
    }
    fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        let status = self.child.wait()?;
        event(
            &self.trace,
            self.id,
            "stopped",
            json!({"pid":self.child.id(),"status":status.to_string()}),
        )?;
        if let Some(reader) = self.reader.take() {
            reader.join().map_err(|_| "reader panicked")??;
        }
        Ok(())
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn setup(root: &Path) -> Result<()> {
    let mut cp = CertificateParams::new(Vec::<String>::new())?;
    cp.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    cp.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate()?;
    let ca = cp.self_signed(&ca_key)?;
    std::fs::write(root.join("ca.pem"), ca.pem())?;
    let mut peers = serde_json::Map::new();
    let mut sockets = Vec::new();
    for id in 1..=3 {
        let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
        let mut p = CertificateParams::new(vec!["localhost".into()])?;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate()?;
        let cert = p.signed_by(&key, &ca, &ca_key)?;
        std::fs::write(root.join(format!("node-{id}.pem")), cert.pem())?;
        std::fs::write(root.join(format!("node-{id}.key.pem")), key.serialize_pem())?;
        peers.insert(id.to_string(),json!({"address":socket.local_addr()?.to_string(),"fingerprint":format!("{:x}",Sha256::digest(cert.der()))}));
        sockets.push(socket);
    }
    for id in 1..=3 {
        save(
            &root.join(format!("node-{id}.json")),
            &json!({"id":id,"domain":42,"database":root.join(format!("node-{id}.redb")),"listen":peers[&id.to_string()]["address"],"certificate":root.join(format!("node-{id}.pem")),"key":root.join(format!("node-{id}.key.pem")),"ca":root.join("ca.pem"),"peers":peers,"task":null,"pgwire":null}),
        )?;
    }
    let mut hashes = serde_json::Map::new();
    for id in 1..=3 {
        for suffix in ["json", "pem", "key.pem"] {
            let name = format!("node-{id}.{suffix}");
            hashes.insert(name.clone(), json!(hash(&root.join(name))?));
        }
    }
    hashes.insert("ca.pem".into(), json!(hash(&root.join("ca.pem"))?));
    save(
        &root.join("configuration-hashes.json"),
        &Value::Object(hashes),
    )?;
    Ok(())
}

fn leader(nodes: &mut BTreeMap<u64, Process>) -> Result<u64> {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        for (id, p) in nodes.iter_mut() {
            p.send(json!({"op":"diagnostics"}))?;
            let v = p.until(
                |v| v.get("authority_ready").is_some(),
                Duration::from_secs(2),
            )?;
            if v["authority_ready"] == true {
                return Ok(*id);
            }
        }
        std::thread::yield_now();
    }
    Err("no functionally ready leader by deadline".into())
}
fn admin(value: Value) -> Value {
    json!({"Admin":serde_json::to_vec(&value).expect("JSON encoding")})
}
fn seed(key: i64, status: &str) -> Value {
    admin(
        json!({"command":"seed","key":{"tenant":42,"row":key},"row":{"status":status,"failed_at":103,"attempt_count":0,"version":0}}),
    )
}
fn task(operation: u64, keys: &[i64], status: &str) -> Value {
    json!({"Task":{"task":1,"principal":2,"lower":10,"upper":10,"action":{"Execute":[{"operation":operation,"selection":{"Keys":keys.iter().map(|k|json!({"tenant":42,"row":k})).collect::<Vec<_>>()},"column":"Status","after":status},null]}}})
}
fn propose(id: u64, command: Value) -> Value {
    json!({"op":"propose","id":id,"command":command})
}
fn success(v: &Value) -> Result<()> {
    ensure(
        v["reply"]["code"] == 0,
        "owner command did not acknowledge success",
    )
}

fn projection(reply: &Value) -> Result<State> {
    success(reply)?;
    let rows = reply["reply"]["data"]["rows"]
        .as_array()
        .ok_or("missing complete rows")?;
    let mut result = State::new();
    for row in rows {
        let key = row[0]["row"].as_i64().ok_or("bad row key")?;
        ensure(
            row[0]["tenant"] == 42 && (0..KEYS).contains(&key),
            "unmodeled key/tenant",
        )?;
        let value = match row[1]["status"].as_str() {
            Some("Failed") => 0,
            Some("Queued") => 1,
            Some("Succeeded") => 2,
            _ => return Err("unmodeled status".into()),
        };
        ensure(
            row[1]["failed_at"] == 103
                && row[1]["attempt_count"] == 0
                && row[1]["version"].is_u64(),
            "unsupported row shape",
        )?;
        ensure(result.insert(key, value).is_none(), "duplicate row key")?;
    }
    Ok(result)
}
fn attempt(
    id: u64,
    logical: String,
    invoke: u64,
    complete: u64,
    ops: Vec<Op>,
    outcome: Outcome,
    observations: Vec<Observation>,
) -> Attempt {
    Attempt {
        id: format!("attempt-{id}"),
        logical_id: logical,
        invoke,
        complete: Some(complete),
        outcome,
        durable: None,
        decision_at: None,
        ops,
        observations,
        receipt: None,
    }
}
fn assignments(keys: &[i64], value: i64) -> Vec<Op> {
    keys.iter().map(|&key| Op::Write { key, value }).collect()
}
#[derive(PartialEq, Eq)]
struct Witness {
    state: State,
    inspection: Value,
}
fn witness(p: &mut Process, id: u64, h: &mut History) -> Result<Witness> {
    let invoke = p.send(json!({"op":"read","id":id}))?;
    let (reply, complete) = p.reply(id, TIMEOUT)?;
    ensure(reply["barrier"].is_u64(), "read lacks barrier")?;
    let state = projection(&reply)?;
    h.attempts.push(attempt(
        id,
        format!("read-{id}"),
        invoke,
        complete,
        vec![Op::Scan {
            start: 0,
            end: KEYS,
        }],
        Outcome::Committed,
        vec![Observation::Scan {
            rows: state.iter().map(|(&k, &v)| (k, v)).collect(),
        }],
    ));
    Ok(Witness {
        state,
        inspection: reply["reply"]["data"].clone(),
    })
}
fn writes(p: &mut Process, ids: &[(u64, u64, Vec<i64>, &str, i64)], h: &mut History) -> Result<()> {
    let mut pending = Vec::new();
    // All invocations precede any client delivery: independent outstanding callers.
    for (id, logical, keys, status, value) in ids {
        let invoke = p.send(propose(*id, task(*logical, keys, status)))?;
        pending.push((*id, *logical, invoke, assignments(keys, *value)));
    }
    for (id, logical, invoke, ops) in pending {
        let (reply, complete) = p.reply(id, TIMEOUT)?;
        success(&reply)?;
        h.attempts.push(attempt(
            id,
            format!("task-1-op-{logical}"),
            invoke,
            complete,
            ops,
            Outcome::Committed,
            vec![],
        ));
    }
    Ok(())
}

/// Run one bounded campaign. Each invocation creates and retains a unique directory.
/// `binary` must be the real `giggles-node` executable; no evaluator is linked.
pub fn run(binary: &Path, parent: &Path, seed_value: u64) -> Result<Value> {
    let binary = std::fs::canonicalize(binary)?;
    std::fs::create_dir_all(parent)?;
    let root = parent.join(format!(
        "capture-{}-{}-{seed_value}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    std::fs::create_dir(&root)?;
    let root = std::fs::canonicalize(root)?;
    let trace = Arc::new(Mutex::new(Trace {
        tick: 0,
        file: File::create_new(root.join("events.jsonl"))?,
        start: Instant::now(),
    }));
    save(
        &root.join("manifest.json"),
        &json!({"schema":1,"seed":seed_value,"node_binary":binary,"node_sha256":hash(&binary)?,"harness_sha256":hash(&std::env::current_exe()?)?,"transport":"trusted owner stdin; mTLS consensus peers","scope":"complete tenant42 status projection, keys0..8; no cross-range claims","durable_evidence":"none","postgres_control":"deferred"}),
    )?;
    let result = campaign(&binary, &root, seed_value, trace.clone());
    match &result {
        Ok(value) => save(&root.join("result.json"), value)?,
        Err(error) => save(
            &root.join("error.json"),
            &json!({"error":error.to_string(),"verdict":"INCONCLUSIVE","artifacts":root}),
        )?,
    }
    result.map_err(|error| format!("{error}; artifacts: {}", root.display()).into())
}

fn campaign(binary: &Path, root: &Path, seed_value: u64, trace: Shared) -> Result<Value> {
    let mut runtime_checks = serde_json::Map::new();
    setup(root)?;
    let mut nodes = BTreeMap::new();
    for id in 1..=3 {
        nodes.insert(id, Process::start(binary, root, id, 0, trace.clone())?);
    }
    nodes
        .get_mut(&1)
        .ok_or("node1")?
        .send(json!({"op":"campaign"}))?;
    let mut l = leader(&mut nodes)?;
    for key in 0..KEYS {
        success(
            &nodes
                .get_mut(&l)
                .ok_or("leader")?
                .request(propose(100 + key as u64, seed(key, "Failed")))?,
        )?;
    }
    let grant = json!({"command":"grant","grant":{"task":1,"issuer":1,"executor":2,"approver":3,"policy":{"tenant":42,"columns":["Status"],"transitions":[["Failed","Queued"],["Queued","Succeeded"],["Succeeded","Queued"],["Failed","Succeeded"]],"incident":[100,107],"distinct_limit":100,"transition_limit":100,"approval_above":100,"valid_from":1,"expires":1000}}});
    success(
        &nodes
            .get_mut(&l)
            .ok_or("leader")?
            .request(propose(200, admin(grant)))?,
    )?;
    success(&nodes.get_mut(&l).ok_or("leader")?.request(propose(201,admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":10000,"encoded_bytes":10000000}}))))?)?;
    let mut h = History {
        version: 1,
        horizon: 1,
        initial: State::new(),
        invariants: Invariants::default(),
        attempts: vec![],
    };
    h.initial = witness(nodes.get_mut(&l).ok_or("leader")?, 300, &mut h)?.state;
    ensure(
        h.initial.len() == KEYS as usize,
        "initial witness incomplete",
    )?;
    // Independent keys, seeded ordering; then overlapping multirow assignments.
    let pair = if seed_value & 1 == 0 { [0, 1] } else { [1, 0] };
    writes(
        nodes.get_mut(&l).ok_or("leader")?,
        &[
            (301, 1, vec![pair[0]], "Queued", 1),
            (302, 2, vec![pair[1]], "Queued", 1),
        ],
        &mut h,
    )?;
    witness(nodes.get_mut(&l).ok_or("leader")?, 303, &mut h)?;
    writes(
        nodes.get_mut(&l).ok_or("leader")?,
        &[
            (304, 3, vec![2, 3], "Queued", 1),
            (305, 4, vec![3, 4], "Succeeded", 2),
        ],
        &mut h,
    )?;
    let before_loss = witness(nodes.get_mut(&l).ok_or("leader")?, 306, &mut h)?;
    nodes.get_mut(&l).ok_or("leader")?.stop()?;
    nodes.remove(&l);
    let old = l;
    l = leader(&mut nodes)?;
    runtime_checks.insert(
        "full_inspection_survives_leader_loss".into(),
        json!(witness(nodes.get_mut(&l).ok_or("leader")?, 307, &mut h)? == before_loss),
    );
    nodes.insert(old, Process::start(binary, root, old, 1, trace.clone())?);
    // Proxy discards a successful reply. It is retained raw, never a client commit.
    let p = nodes.get_mut(&l).ok_or("leader")?;
    let invoke = p.send(propose(308, task(5, &[5, 6], "Queued")))?;
    let discarded = p.until(|v| v["id"] == 308, TIMEOUT)?;
    success(&discarded)?;
    let complete = event(
        &trace,
        l,
        "proxy_discard",
        json!({"id":308,"reason":"injected lost client reply"}),
    )?;
    h.attempts.push(attempt(
        308,
        "task-1-op-5".into(),
        invoke,
        complete,
        assignments(&[5, 6], 1),
        Outcome::Indeterminate,
        vec![],
    ));
    p.stop()?;
    nodes.remove(&l);
    let lost = l;
    l = leader(&mut nodes)?;
    writes(
        nodes.get_mut(&l).ok_or("leader")?,
        &[(309, 5, vec![5, 6], "Queued", 1)],
        &mut h,
    )?;
    witness(nodes.get_mut(&l).ok_or("leader")?, 310, &mut h)?;
    // Reset after the logical operation, then retry again. Reapply is observable.
    let p = nodes.get_mut(&l).ok_or("leader")?;
    let invoke = p.send(propose(311, seed(5, "Failed")))?;
    let (reply, complete) = p.reply(311, TIMEOUT)?;
    success(&reply)?;
    h.attempts.push(attempt(
        311,
        "reset-5".into(),
        invoke,
        complete,
        assignments(&[5], 0),
        Outcome::Committed,
        vec![],
    ));
    writes(p, &[(312, 5, vec![5, 6], "Queued", 1)], &mut h)?;
    let retained = witness(p, 313, &mut h)?;
    runtime_checks.insert(
        "retry_does_not_reapply_after_reset".into(),
        json!(retained.state.get(&5) == Some(&0)),
    );
    nodes.insert(lost, Process::start(binary, root, lost, 1, trace.clone())?);
    for p in nodes.values_mut() {
        p.send(json!({"op":"checkpoint"}))?;
        p.until(|v| v["checkpoint"].is_u64(), TIMEOUT)?;
        p.stop()?;
    }
    nodes.clear();
    for id in 1..=3 {
        nodes.insert(id, Process::start(binary, root, id, 2, trace.clone())?);
    }
    l = leader(&mut nodes)?;
    runtime_checks.insert(
        "full_inspection_survives_checkpoint_restart".into(),
        json!(witness(nodes.get_mut(&l).ok_or("leader")?, 314, &mut h)? == retained),
    );
    // Isolate every peer; request cannot gain a quorum. Timeout stays ambiguous.
    for p in nodes.values_mut() {
        p.send(json!({"op":"pause_peers","paused":true}))?;
        p.until(|v| v["peers_paused"] == true, TIMEOUT)?;
    }
    let p = nodes.get_mut(&l).ok_or("leader")?;
    let invoke = p.send(propose(315, task(6, &[7], "Queued")))?;
    let response = p.reply(315, Duration::from_secs(2));
    let complete = event(
        &trace,
        l,
        "minority_window_closed",
        json!({"id":315,"response":response.as_ref().ok().map(|r|&r.0),"timeout_is_abort":false}),
    )?;
    let acknowledged = response.as_ref().is_ok_and(|r| r.0["reply"]["code"] == 0);
    runtime_checks.insert(
        "minority_does_not_acknowledge_in_window".into(),
        json!(!acknowledged),
    );
    h.attempts.push(attempt(
        315,
        "task-1-op-6".into(),
        invoke,
        complete,
        assignments(&[7], 1),
        if acknowledged {
            Outcome::Committed
        } else {
            Outcome::Indeterminate
        },
        vec![],
    ));
    for p in nodes.values_mut() {
        p.send(json!({"op":"pause_peers","paused":false}))?;
        p.until(|v| v["peers_paused"] == false, TIMEOUT)?;
    }
    l = leader(&mut nodes)?;
    writes(
        nodes.get_mut(&l).ok_or("leader")?,
        &[(316, 6, vec![7], "Queued", 1)],
        &mut h,
    )?;
    witness(nodes.get_mut(&l).ok_or("leader")?, 317, &mut h)?;
    for p in nodes.values_mut() {
        p.stop()?;
    }
    h.horizon = event(&trace, 0, "horizon", json!({"attempts":h.attempts.len()}))?;
    giggles_history::write_jsonl(&h, File::create_new(root.join("history.jsonl"))?)?;
    let check = giggles_history::check(&h, Limits::default())?;
    save(&root.join("checker.json"), &serde_json::to_value(&check)?)?;
    save(
        &root.join("runtime-checks.json"),
        &Value::Object(runtime_checks.clone()),
    )?;
    let controls = if check.verdict == Verdict::Pass {
        negative_controls(&h, root)?
    } else {
        json!({"skipped":"original history is not PASS; original verdict preserved"})
    };
    let verdict = if runtime_checks.values().any(|v| v == false) {
        "FAIL".to_owned()
    } else {
        format!("{:?}", check.verdict).to_uppercase()
    };
    Ok(
        json!({"verdict":verdict,"checker_verdict":check.verdict,"runtime_checks":runtime_checks,"artifacts":root,"attempts":h.attempts.len(),"negative_controls":controls,"scope":"bounded status assignment projection; trusted owner transport; no qualification claims"}),
    )
}

fn negative_controls(h: &History, root: &Path) -> Result<Value> {
    let mut results = serde_json::Map::new();
    for (name, read_id, key, value) in [
        ("stale_read", "attempt-303", 0, 0),
        ("partial_acknowledged_write", "attempt-306", 2, 0),
        ("duplicate_logical_effect", "attempt-317", 5, 1),
    ] {
        let mut copy = h.clone();
        let a = copy
            .attempts
            .iter_mut()
            .find(|a| a.id == read_id)
            .ok_or("control witness missing")?;
        let Some(Observation::Scan { rows }) = a.observations.first_mut() else {
            return Err("control scan missing".into());
        };
        let row = rows
            .iter_mut()
            .find(|r| r.0 == key)
            .ok_or("control key missing")?;
        ensure(row.1 != value, "negative control is unchanged")?;
        row.1 = value;
        let result = giggles_history::check(&copy, Limits::default())?;
        giggles_history::write_jsonl(
            &copy,
            File::create_new(root.join(format!("negative-{name}.jsonl")))?,
        )?;
        save(
            &root.join(format!("negative-{name}.verdict.json")),
            &serde_json::to_value(&result)?,
        )?;
        ensure(
            result.verdict == Verdict::Fail,
            "negative control did not exhaustively FAIL",
        )?;
        results.insert(name.into(), json!("FAIL"));
    }
    Ok(Value::Object(results))
}
