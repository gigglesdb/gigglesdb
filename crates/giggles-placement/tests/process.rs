#[path = "support/process.rs"]
mod actual;
#[path = "support/cli.rs"]
mod cli;
mod support;
use actual::*;
use giggles_membership::Phase;
use giggles_placement::*;
use serde_json::json;
use support::*;

#[test]
fn real_four_process_replacement_restart_lost_reply_leader_change_data_receipt_and_noquorum() {
    let mut f = Fixture::new();
    let tx = Fixture::transaction();
    let receipt = f.request(
        2,
        json!({"command":"transact","plan":tx,"outcome":"Commit"}),
    );
    assert_eq!(receipt["decision"]["outcome"], "Commit");
    let before = f.current(2);
    let mut p = Plan::create(request(), &mut f.owner).unwrap();
    let journal = f.root.join("controller.redb");
    std::fs::write(
        f.root.join("exact-plan.json"),
        serde_json::to_vec_pretty(&p).unwrap(),
    )
    .unwrap();
    let mut unsafe_request = request();
    unsafe_request.topology.nodes[3].zone = "z1".into();
    assert!(Plan::create(unsafe_request, &mut f.owner).is_err());
    let original = p.clone();
    p.request.intent.id += 1;
    assert!(Controller::open(f.root.join("changed.redb"), p).is_err());
    p = original;
    let mut c = Controller::open(&journal, p.clone()).unwrap();
    // Drop the begin reply after the actual runtime has persisted it.
    struct Lose<'a>(&'a mut giggles_placement::owner::StdioOwner);
    impl Owner for Lose<'_> {
        fn current(&mut self, g: u64) -> Result<giggles_distributed::runtime::Observation> {
            self.0.current(g)
        }
        fn act(&mut self, a: &Action) -> Result<()> {
            self.0.act(a)?;
            Err(Error {
                code: Code::Transport,
                message: "test drops actual committed begin reply".into(),
            })
        }
    }
    assert_code(c.step(&mut Lose(&mut f.owner)), Code::Transport);
    drop(c);
    let mut c = Controller::open(&journal, p.clone()).unwrap();
    f.step_until(&mut c, Phase::Joint);
    // Controller and all four runtime processes restart during actual joint state.
    drop(c);
    f.restart();
    assert_eq!(f.current(2).membership.receipts[&4001].phase, Phase::Joint);
    let mut c = Controller::open(&journal, p.clone()).unwrap();
    let leader = f.current(2).leader;
    let target = if leader == 1 { 2 } else { 1 };
    let _ = f
        .owner
        .processes
        .get_mut(&leader)
        .unwrap()
        .request(json!({"command":"transfer","group":2,"to":target}));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if f.current(2).leader != leader {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "leader did not change"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    f.step_until(&mut c, Phase::Complete);
    assert_eq!(c.step(&mut f.owner).unwrap(), StepResult::Complete);
    let after = f.current(2);
    let mut voters = after.membership.voters.clone();
    voters.sort_unstable();
    assert_eq!(voters, vec![1, 2, 4]);
    assert_eq!(after.membership.epoch, 2);
    assert_eq!(after.machine.domain().unwrap(), 42);
    let (
        giggles_distributed::Machine::Range { data: old, .. },
        giggles_distributed::Machine::Range { data: new, .. },
    ) = (before.machine, after.machine)
    else {
        panic!("range");
    };
    assert_eq!(old.descriptor, new.descriptor);
    assert_eq!(
        old.image.snapshot().unwrap().rows,
        new.image.snapshot().unwrap().rows
    );
    assert_eq!(new.transactions.record(tx.id()).unwrap().plan, tx);
    let status = f.request(
        2,
        json!({"command":"tx_status","home":tx.home,"id":tx.id()}),
    );
    assert_eq!(status["decision"], receipt["decision"]);
    let replay=f.request(2,json!({"command":"tx_step","request":giggles_distributed::transaction::Request{route:tx.home.clone(),step:giggles_distributed::transaction::Step::Open(tx.clone()),evidence:vec![]}}));
    assert!(!replay.is_null());
    drop(c);
    f.restart();
    let mut c = Controller::open(&journal, p).unwrap();
    assert_eq!(c.step(&mut f.owner).unwrap(), StepResult::Complete);
    let count = c.events().unwrap().len();
    for id in 1..=4 {
        let others: Vec<_> = (1..=4).filter(|n| *n != id).collect();
        f.owner
            .processes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":2,"peers":others}))
            .unwrap();
    }
    assert_code(c.step(&mut f.owner), Code::NoQuorum);
    assert_eq!(c.events().unwrap().len(), count);
    for id in 1..=4 {
        let v = f
            .owner
            .processes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"inspect","group":2}))
            .unwrap();
        let m: giggles_membership::Membership =
            serde_json::from_value(v["membership"].clone()).unwrap();
        assert_eq!(m.receipts.len(), 1);
    }
    std::fs::write(
        f.root.join("journal-events.json"),
        serde_json::to_vec_pretty(&c.events().unwrap()).unwrap(),
    )
    .unwrap();
}

#[test]
fn process_configuration_rejects_relative_paths_and_reused_artifacts() {
    use giggles_placement::owner::{Process, ProcessConfig};
    let c = ProcessConfig {
        binary: "relative.exe".into(),
        config: "relative.json".into(),
        artifacts: fresh("bad-config"),
        timeout_ms: 100,
    };
    assert!(Process::start(&c).is_err());
    let mut c = c;
    c.binary = std::env::current_exe().unwrap();
    c.config = std::env::current_exe().unwrap();
    assert!(Process::start(&c).is_err());
}

#[test]
fn cli_usage_rejects_missing_roster() {
    let o = std::process::Command::new(env!("CARGO_BIN_EXE_giggles-placement"))
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("usage"));
}

#[test]
fn actual_cli_plan_step_restart_midtransition_and_strict_commands() {
    use std::time::{Duration, Instant};
    let mut f = Fixture::new();
    f.owner.processes.clear();
    for (id, c) in &mut f.configs {
        c.artifacts = f.root.join(format!("cli-runtime-{id}-1"));
    }
    let roster = f.root.join("roster-1.json");
    std::fs::write(
        &roster,
        serde_json::to_vec(&json!({"processes":f.configs})).unwrap(),
    )
    .unwrap();
    let mut child = cli::Cli::start(&roster, &f.root.join("cli-1"));
    let bad = child.request(json!({"command":"inspect","group":2}));
    assert_eq!(bad["ok"], false);
    let deadline = Instant::now() + Duration::from_secs(45);
    let p: Plan = loop {
        let r = child.request(json!({"command":"plan","request":request()}));
        if r["ok"] == true {
            break serde_json::from_value(r["data"].clone()).unwrap();
        }
        assert!(Instant::now() < deadline, "plan:{r}");
    };
    let journal = f.root.join("cli-journal.redb");
    for _ in 0..2 {
        let r = child.request(json!({"command":"step","plan":p,"journal":journal}));
        assert_eq!(r["ok"], true, "{r}");
        assert_eq!(r["data"], "Pending");
    }
    drop(child); // EOF stops CLI and its runtime children; all stores retained.
    for (id, c) in &mut f.configs {
        c.artifacts = f.root.join(format!("cli-runtime-{id}-2"));
    }
    let roster = f.root.join("roster-2.json");
    std::fs::write(
        &roster,
        serde_json::to_vec(&json!({"processes":f.configs})).unwrap(),
    )
    .unwrap();
    let mut child = cli::Cli::start(&roster, &f.root.join("cli-2"));
    let mut changed = p.clone();
    changed.request.intent.id += 1;
    let r = child.request(json!({"command":"step","plan":changed,"journal":journal}));
    assert_eq!(r["ok"], false);
    assert_eq!(r["error"]["code"], "StalePlan");
    let deadline = Instant::now() + Duration::from_secs(50);
    loop {
        let r = child.request(json!({"command":"step","plan":p,"journal":journal}));
        if r["ok"] == true && r["data"] == "Complete" {
            break;
        }
        assert!(Instant::now() < deadline, "resume:{r}");
        std::thread::sleep(Duration::from_millis(25));
    }
    drop(child);
    let c = Controller::open(&journal, p).unwrap();
    assert!(
        c.events()
            .unwrap()
            .last()
            .unwrap()
            .result
            .starts_with("complete")
    );
}

#[test]
fn owner_pipe_malformed_oversized_eof_timeout_are_bounded_and_poisoned() {
    use giggles_placement::owner::{Process, ProcessConfig};
    let shell = std::path::PathBuf::from(std::env::var_os("SystemRoot").unwrap())
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
    for (name, tail, expected) in [
        (
            "malformed",
            "[Console]::Out.WriteLine('not-json')",
            Code::Transport,
        ),
        (
            "oversized",
            "[Console]::Out.WriteLine(('x' * 2097153))",
            Code::Limit,
        ),
        ("eof", "exit 0", Code::Transport),
        ("timeout", "Start-Sleep -Seconds 6", Code::Transport),
    ] {
        let root = fresh(name);
        let script = root.join("probe.ps1");
        std::fs::write(&script,format!("[Console]::Out.WriteLine('{{\"ready\":true}}'); [Console]::In.ReadLine() | Out-Null; {tail}")).unwrap();
        let mut p = Process::start(&ProcessConfig {
            binary: shell.clone(),
            config: script,
            artifacts: root.join("logs"),
            timeout_ms: 3000,
        })
        .unwrap();
        assert_code(p.request(json!({"command":"current","group":2})), expected);
        assert_code(
            p.request(json!({"command":"current","group":2})),
            Code::Transport,
        );
    }
}
