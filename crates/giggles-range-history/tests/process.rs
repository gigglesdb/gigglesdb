use giggles_history::{Limits, Verdict, check, parse_jsonl};
use giggles_range_history::{fresh, source_hashes};
use serde_json::{Value, json};
use std::{fs::File, io::BufReader, path::Path, process::Command};
fn save(p: &Path, v: &impl serde::Serialize) {
    std::fs::write(p, serde_json::to_vec_pretty(v).unwrap()).unwrap();
}
// Shutdown kills do not establish a crash/restart fault campaign.
fn fault_restarts(events: &std::collections::BTreeMap<u64, Value>) -> Vec<(u64, u64)> {
    let mut pid = None;
    let mut terminating = None;
    let mut terminated = None;
    let mut restarts = Vec::new();
    for (&tick, e) in events {
        if e["node"] != 1 {
            continue;
        }
        match e["kind"].as_str() {
            Some("spawn") => {
                let next_pid = e["value"]["pid"].as_u64().expect("spawn PID");
                if let Some(killed_at) = terminated.take() {
                    assert_ne!(pid, Some(next_pid), "restart must name a new process");
                    restarts.push((killed_at, tick));
                } else {
                    assert!(pid.is_none(), "new spawn without witnessed termination");
                }
                pid = Some(next_pid);
            }
            Some("terminate") => {
                assert_eq!(e["value"]["pid"].as_u64(), pid, "wrong killed PID");
                assert!(terminating.replace(tick).is_none());
            }
            Some("terminated") => {
                terminated = Some(terminating.take().expect("termination request"));
            }
            _ => (),
        }
    }
    restarts
}

#[test]
fn fault_restart_assertion_excludes_shutdown_and_requires_pid_identity() {
    let records = [
        json!({"node":1,"kind":"spawn","value":{"pid":10}}),
        json!({"node":1,"kind":"terminate","value":{"pid":10}}),
        json!({"node":1,"kind":"terminated","value":{}}),
        json!({"node":2,"kind":"terminated","value":{}}),
        json!({"node":3,"kind":"terminated","value":{}}),
    ];
    let mut events = records
        .into_iter()
        .enumerate()
        .map(|(i, e)| (i as u64 + 1, e))
        .collect();
    assert!(fault_restarts(&events).is_empty());
    events.insert(6, json!({"node":1,"kind":"spawn","value":{"pid":11}}));
    assert_eq!(fault_restarts(&events), vec![(2, 6)]);
    events.insert(2, json!({"node":1,"kind":"terminate","value":{"pid":99}}));
    assert!(std::panic::catch_unwind(|| fault_restarts(&events)).is_err());
}
#[test]
fn actual_process_seeded_campaign() {
    let root = fresh(
        &std::env::temp_dir().join("giggles-range-history-retained"),
        "verification",
    )
    .unwrap();
    let before = source_hashes().unwrap();
    save(&root.join("sources-before-build.json"), &before);
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target = manifest.join("target/owner");
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let mut build = Command::new(env!("CARGO"));
    build
        .args(["build", "--locked", "--manifest-path"])
        .arg(manifest.join("../giggles-distributed/Cargo.toml"))
        .args(["--bin", "giggles-ranges", "-j1", "--target-dir"])
        .arg(&target)
        .env("CARGO_BUILD_JOBS", "1");
    if profile == "release" {
        build.arg("--release");
    }
    let output = build.output().unwrap();
    std::fs::write(root.join("owner-build.stdout.log"), &output.stdout).unwrap();
    std::fs::write(root.join("owner-build.stderr.log"), &output.stderr).unwrap();
    assert!(
        output.status.success(),
        "owner build failed: {}",
        root.display()
    );
    let binary = target
        .join(profile)
        .join(format!("giggles-ranges{}", std::env::consts::EXE_SUFFIX));
    for seed in [42, 43] {
        let out = Command::new(env!("CARGO_BIN_EXE_giggles-range-history"))
            .arg(&binary)
            .arg(&root)
            .arg(seed.to_string())
            .output()
            .unwrap();
        std::fs::write(root.join(format!("seed-{seed}.stdout.json")), &out.stdout).unwrap();
        std::fs::write(root.join(format!("seed-{seed}.stderr.log")), &out.stderr).unwrap();
        assert!(
            out.status.success(),
            "campaign failed: {} {}",
            root.display(),
            String::from_utf8_lossy(&out.stdout)
        );
        let v: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(v["verdict"], "PASS");
        let p = Path::new(v["artifacts"].as_str().unwrap());
        let h = parse_jsonl(BufReader::new(File::open(p.join("history.jsonl")).unwrap())).unwrap();
        assert_eq!(check(&h, Limits::default()).unwrap().verdict, Verdict::Pass);
        assert!(
            h.attempts
                .iter()
                .any(|a| a.complete.is_none() && a.durable.is_some())
        );
        assert!(h.attempts.iter().any(|a| a.id == "same-id-retry"));
        for n in ["stale_read", "partial_write", "double_effect"] {
            let h = parse_jsonl(BufReader::new(
                File::open(p.join(format!("{n}.jsonl"))).unwrap(),
            ))
            .unwrap();
            assert_eq!(check(&h, Limits::default()).unwrap().verdict, Verdict::Fail);
        }
        let events = std::fs::read_to_string(p.join("events.jsonl")).unwrap();
        let by_tick = events
            .lines()
            .map(|line| {
                let e: Value = serde_json::from_str(line).unwrap();
                (e["tick"].as_u64().unwrap(), e)
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let raw = by_tick.values().cloned().collect::<Vec<_>>();
        giggles_range_history::validate_trace(&h, &raw).unwrap();
        let overlap = ["external-coordinator-2", "external-coordinator-3"]
            .map(|id| h.attempts.iter().find(|a| a.id == id).unwrap());
        assert_ne!(
            by_tick[&overlap[0].invoke]["node"],
            by_tick[&overlap[1].invoke]["node"]
        );
        assert!(overlap[1].invoke < overlap[0].complete.unwrap());
        assert!(overlap[0].invoke < overlap[1].complete.unwrap());
        let read = h
            .attempts
            .iter()
            .find(|a| a.id == "external-locked-read")
            .unwrap();
        assert_eq!(by_tick[&read.invoke]["value"]["command"], "observe");
        assert!(
            raw.iter()
                .any(|e| e["kind"] == "read_barrier" && e["value"]["invoke"] == read.invoke)
        );
        assert!(
            raw.iter().any(|e| e["kind"] == "read_capability"
                && e["value"]["complete"] == read.complete.unwrap())
        );
        // Attribution negative controls mutate copies only, never raw artifacts.
        let mut bad = h.clone();
        bad.attempts
            .iter_mut()
            .find(|a| a.decision_at.is_some())
            .unwrap()
            .decision_at = Some(1);
        assert!(
            giggles_range_history::validate_trace(&bad, &raw).is_err(),
            "fabricated durable bound"
        );
        for field in ["nonce", "home", "participants"] {
            let mut changed = raw.clone();
            let e = changed
                .iter_mut()
                .find(|e| e["tick"] == overlap[1].invoke)
                .unwrap();
            match field {
                "nonce" => e["value"]["plan"]["nonce"] = json!(999),
                "home" => e["value"]["plan"]["home"]["epoch"] = json!(3),
                _ => {
                    e["value"]["plan"]["participants"][0]["writes"][0]["row"]["attempt_count"] =
                        json!(999)
                }
            }
            assert!(
                giggles_range_history::validate_trace(&h, &changed).is_err(),
                "changed ID/body/home {field}"
            );
        }
        let dropped = raw
            .iter()
            .filter(|e| e["tick"] != read.complete.unwrap())
            .cloned()
            .collect::<Vec<_>>();
        assert!(
            giggles_range_history::validate_trace(&h, &dropped).is_err(),
            "dropped read response"
        );
        let mut bad = h.clone();
        let scan = bad
            .attempts
            .iter_mut()
            .find(|a| !a.observations.is_empty())
            .unwrap();
        if let giggles_history::Observation::Scan { rows } = &mut scan.observations[0] {
            rows.pop();
        }
        assert!(
            giggles_range_history::validate_trace(&bad, &raw).is_err(),
            "incomplete scan"
        );
        let without_spawn = by_tick
            .iter()
            .filter(|(_, e)| {
                !(e["kind"] == "spawn" && e["node"] == 1 && e["value"]["incarnation"] != 1)
            })
            .map(|(t, e)| (*t, e.clone()))
            .collect();
        assert!(
            std::panic::catch_unwind(|| fault_restarts(&without_spawn)).is_err(),
            "missing fault restarts"
        );
        save(
            &p.join("attribution-controls.json"),
            &json!({"original":"PASS","fabricated_durable_bound":"REJECT","changed_id":"REJECT","changed_body":"REJECT","changed_home":"REJECT","dropped_read_response":"REJECT","incomplete_scan":"REJECT","missing_fault_restarts":"REJECT","shutdown_only":"dedicated regression PASS","retry_duplicate_effect":"double_effect checker FAIL"}),
        );
        for a in &h.attempts {
            assert!(
                ["invoke", "logical_invoke"]
                    .contains(&by_tick[&a.invoke]["kind"].as_str().unwrap())
            );
            if let Some(t) = a.complete {
                let e = &by_tick[&t];
                assert_eq!(e["kind"], "complete");
                if a.outcome == giggles_history::Outcome::Committed {
                    assert_eq!(e["value"]["ok"], true);
                    if let Some(giggles_history::Observation::Scan { rows }) =
                        a.observations.first()
                    {
                        let raw: Vec<(giggles_core::Key, giggles_core::Row)> =
                            serde_json::from_value(e["value"]["data"].clone()).unwrap();
                        assert_eq!(&giggles_range_history::decode(&raw).unwrap(), rows);
                    } else {
                        assert_eq!(e["value"]["data"]["decision"]["outcome"], "Commit");
                    }
                }
            }
            if let Some(t) = a.decision_at {
                let e = &by_tick[&t];
                assert_eq!(e["kind"], "complete");
                assert_eq!(e["value"]["ok"], true);
                let expected = match a.durable.unwrap() {
                    giggles_history::Outcome::Committed => "Commit",
                    giggles_history::Outcome::Aborted => "Abort",
                    _ => panic!("indeterminate durable evidence"),
                };
                assert_eq!(e["value"]["data"]["decision"]["outcome"], expected);
                assert_eq!(
                    format!("tx-{}", e["value"]["data"]["id"]["nonce"]),
                    a.logical_id
                );
            }
        }
        let original = h
            .attempts
            .iter()
            .find(|a| a.id == "postdecision-loss")
            .unwrap();
        let competitor = h
            .attempts
            .iter()
            .find(|a| a.id == "overlapping-stale-cas")
            .unwrap();
        let delayed = h
            .attempts
            .iter()
            .find(|a| a.id == "delayed-participant-scan")
            .unwrap();
        assert!(
            original.invoke < competitor.invoke
                && competitor.complete.unwrap() < original.decision_at.unwrap()
        );
        assert!(original.decision_at.unwrap() < delayed.invoke);
        for label in [
            "predecision-loss",
            "overlapping-stale-cas",
            "stale-before-cas",
        ] {
            assert_eq!(
                h.attempts.iter().find(|a| a.id == label).unwrap().durable,
                Some(giggles_history::Outcome::Aborted)
            );
        }
        let mut last = 0;
        for line in events.lines() {
            let e: Value = serde_json::from_str(line).unwrap();
            let tick = e["tick"].as_u64().unwrap();
            assert!(tick > last);
            last = tick;
        }
        let restarts = fault_restarts(&by_tick);
        assert_eq!(restarts.len(), 2, "two fault restarts, excluding shutdown");
        let predecision = h
            .attempts
            .iter()
            .find(|a| a.id == "predecision-loss")
            .unwrap();
        assert!(predecision.invoke < restarts[0].0);
        assert!(restarts[0].1 < predecision.decision_at.unwrap());
        assert!(original.decision_at.unwrap() < restarts[1].0);
        assert!(restarts[1].1 < delayed.invoke);
        assert!(events.contains("participant_finalize_delayed"));
    }
    let after = source_hashes().unwrap();
    save(&root.join("sources-after-tests.json"), &after);
    assert_eq!(
        before,
        after,
        "dependency changed; repeat verification at {}",
        root.display()
    );
    save(
        &root.join("summary.json"),
        &json!({"profile":profile,"seeds":[42,43],"passed":2,"failed":0}),
    );
    println!("retained verification {}", root.display());
}
#[test]
fn invalid_cli_is_usage() {
    for args in [vec![], vec!["x", "y", "not-a-seed"]] {
        let out = Command::new(env!("CARGO_BIN_EXE_giggles-range-history"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(64));
    }
}
#[test]
fn missing_binary_is_inconclusive() {
    let root = fresh(
        &std::env::temp_dir().join("giggles-range-history-retained"),
        "missing",
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_giggles-range-history"))
        .arg(root.join("absent.exe"))
        .arg(&root)
        .arg("42")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["verdict"], "INCONCLUSIVE");
    assert!(v["error"].as_str().unwrap().contains("artifacts="));
}
