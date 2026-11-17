mod common;
use common::*;
use giggles_history::*;
use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);
fn run(args: &[&str], input: &[u8]) -> Output {
    let serial = NEXT.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir()
        .join("giggles-history-cli")
        .join(format!("cli-{}-{nanos}-{serial}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("input.jsonl"), input).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-history"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _ = child.stdin.take().unwrap().write_all(input);
    let out = child.wait_with_output().unwrap();
    std::fs::write(dir.join("stdout.bin"), &out.stdout).unwrap();
    std::fs::write(dir.join("stderr.bin"), &out.stderr).unwrap();
    std::fs::write(
        dir.join("invocation.json"),
        serde_json::to_vec(&serde_json::json!({"args":args,"exit_code":out.status.code()}))
            .unwrap(),
    )
    .unwrap();
    out
}
fn bytes(h: &History) -> Vec<u8> {
    let mut out = vec![];
    write_jsonl(h, &mut out).unwrap();
    out
}
fn result(output: &Output) -> Check {
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn cli_seed_generation_replay_and_corruption_32_seeds() {
    for seed in 0..32 {
        let generated = run(&["generate", &seed.to_string(), "6"], b"");
        assert_eq!(generated.status.code(), Some(0));
        let mut h = parse_jsonl(std::io::Cursor::new(&generated.stdout)).unwrap();
        assert_eq!(h, generate(seed, 6).unwrap());
        let pass = run(&["replay", "-"], &generated.stdout);
        assert_eq!(pass.status.code(), Some(0));
        assert_eq!(result(&pass).verdict, Verdict::Pass);
        if let Observation::Scan { rows } = &mut h.attempts[5].observations[0] {
            rows[0].1 += 1;
        }
        let fail = run(&["check", "-"], &bytes(&h));
        assert_eq!(fail.status.code(), Some(1));
        assert_eq!(result(&fail).verdict, Verdict::Fail);
    }
}
#[test]
fn cli_anomaly_positive_and_corrupted_fixtures() {
    let mut pairs = vec![];
    let good = history(vec![writer("w", 0, 1, 0, 7), reader("r", 2, 3, 0, Some(7))]);
    let mut bad = good.clone();
    bad.attempts[1].observations = vec![Observation::Read { value: None }];
    pairs.push((good, bad));
    let good = history(vec![
        tx(
            "w",
            0,
            1,
            vec![
                Op::Write { key: 0, value: 7 },
                Op::Write { key: 1, value: 8 },
            ],
            vec![],
        ),
        scanner("r", 2, 3, 0, 2, vec![(0, 7), (1, 8)]),
    ]);
    let mut bad = good.clone();
    bad.attempts[1].observations = vec![Observation::Scan { rows: vec![(0, 7)] }];
    pairs.push((good, bad));
    for scan in [false, true] {
        let op = if scan {
            Op::Scan { start: 1, end: 2 }
        } else {
            Op::Read { key: 1 }
        };
        let obs = if scan {
            Observation::Scan { rows: vec![] }
        } else {
            Observation::Read { value: None }
        };
        let bad = history(vec![
            tx(
                "a",
                0,
                4,
                vec![op, Op::Write { key: 0, value: 1 }],
                vec![obs],
            ),
            tx(
                "b",
                0,
                4,
                vec![Op::Read { key: 0 }, Op::Write { key: 1, value: 1 }],
                vec![Observation::Read { value: None }],
            ),
        ]);
        let mut good = bad.clone();
        good.attempts[1].observations = vec![Observation::Read { value: Some(1) }];
        pairs.push((good, bad));
    }
    let mut a = writer("a", 0, 1, 0, 7);
    a.outcome = Outcome::Aborted;
    let good = history(vec![a, reader("r", 2, 3, 0, None)]);
    let mut bad = good.clone();
    bad.attempts[1].observations = vec![Observation::Read { value: Some(7) }];
    pairs.push((good, bad));
    for duplicated in [true, false] {
        let a = tx(
            "a",
            0,
            1,
            vec![Op::Transfer {
                from: 0,
                to: 1,
                amount: 2,
            }],
            vec![],
        );
        let mut retry = a.clone();
        retry.id = "retry".into();
        retry.invoke = 2;
        retry.complete = Some(3);
        let mut good = history(vec![
            a,
            retry,
            scanner("r", 4, 5, 0, 2, vec![(0, 8), (1, 12)]),
        ]);
        good.initial = State::from([(0, 10), (1, 10)]);
        let mut bad = good.clone();
        bad.attempts[2].observations = vec![Observation::Scan {
            rows: if duplicated {
                vec![(0, 6), (1, 14)]
            } else {
                vec![(0, 8), (1, 10)]
            },
        }];
        pairs.push((good, bad));
    }
    for duplicate_receipt in [true, false] {
        let mut good = history(vec![writer("a", 0, 1, 0, 7), writer("b", 2, 3, 1, 8)]);
        good.invariants.require_receipts = true;
        good.invariants.budgets.insert("budget".into(), 2);
        for (i, a) in good.attempts.iter_mut().enumerate() {
            a.receipt = Some(Receipt {
                id: format!("receipt{i}"),
                budget: "budget".into(),
                cost: 1,
            });
        }
        let mut bad = good.clone();
        if duplicate_receipt {
            bad.attempts[1].receipt.as_mut().unwrap().id = "receipt0".into();
        } else {
            bad.invariants.budgets.insert("budget".into(), 1);
        };
        pairs.push((good, bad));
    }
    for (good, bad) in pairs {
        let pass = run(&["check", "-"], &bytes(&good));
        assert_eq!(
            pass.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&pass.stderr)
        );
        let fail = run(&["check", "-"], &bytes(&bad));
        assert_eq!(fail.status.code(), Some(1));
        assert!(!result(&fail).diagnostics.is_empty());
    }
}
#[test]
fn cli_limit_codes_and_invalid_bounds() {
    let h = bytes(&generate(0, 4).unwrap());
    for args in [
        vec!["check", "-", "--nodes", "0"],
        vec!["check", "-", "--memory", "0"],
    ] {
        let out = run(&args, &h);
        assert_eq!(out.status.code(), Some(2));
        assert_eq!(result(&out).verdict, Verdict::Inconclusive);
    }
    for args in [
        vec![],
        vec!["unknown"],
        vec!["check"],
        vec!["check", "-", "--nodes", "-1"],
        vec!["check", "-", "--memory", "18446744073709551616"],
        vec!["check", "-", "--nodes", "1", "--nodes", "2"],
        vec!["check", "-", "--unknown", "1"],
        vec!["check", "-", "extra"],
        vec!["generate", "0", "129"],
        vec!["generate", "wrong", "1"],
    ] {
        assert_eq!(run(&args, &h).status.code(), Some(64), "{args:?}");
    }
}
#[test]
fn cli_malformed_adversarial_input_codes() {
    let base = bytes(&generate(3, 1).unwrap());
    for input in [
        b"".to_vec(),
        b"{}\n".to_vec(),
        b"null\n".to_vec(),
        vec![255, 254],
        vec![b' '; 1024 * 1024 + 1],
        b"{\"record\":\"attempt\",\"attempt\":{}}\n".to_vec(),
        [base.clone(), base.clone()].concat(),
    ] {
        assert_eq!(run(&["check", "-"], &input).status.code(), Some(64));
    }
    let lines: Vec<_> = base.split(|b| *b == b'\n').collect();
    let mut huge = lines[0].to_vec();
    huge.push(b'\n');
    for _ in 0..5 {
        huge.extend_from_slice(lines[1]);
        huge.extend(std::iter::repeat_n(b' ', 900_000));
        huge.push(b'\n');
    }
    let out = run(&["check", "-"], &huge);
    assert_eq!(out.status.code(), Some(64));
    assert!(String::from_utf8_lossy(&out.stderr).contains("byte/line limit"));
}
#[test]
fn cli_minimize_then_replay_failure() {
    let h = history(vec![
        writer("w", 0, 1, 0, 7),
        reader("r", 2, 3, 0, None),
        writer("unrelated", 4, 5, 10, 20),
    ]);
    let out = run(&["minimize", "-"], &bytes(&h));
    assert_eq!(out.status.code(), Some(0));
    let small = parse_jsonl(std::io::Cursor::new(&out.stdout)).unwrap();
    assert_eq!(small.attempts.len(), 2);
    let replay = run(&["replay", "-"], &out.stdout);
    assert_eq!(replay.status.code(), Some(1));
    assert_eq!(
        run(&["minimize", "-", "--nodes", "0"], &bytes(&h))
            .status
            .code(),
        Some(64)
    );
    assert_eq!(
        run(&["minimize", "-"], &bytes(&history(vec![])))
            .status
            .code(),
        Some(64)
    );
}
#[test]
fn cli_real_file_input_and_missing_file() {
    let dir = std::env::temp_dir()
        .join("giggles-history-cli")
        .join(format!("file-input-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("history.jsonl");
    std::fs::write(&path, bytes(&generate(44, 3).unwrap())).unwrap();
    let out = run(&["check", path.to_str().unwrap()], b"");
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        run(
            &["check", dir.join("never-created.jsonl").to_str().unwrap()],
            b""
        )
        .status
        .code(),
        Some(64)
    );
}
#[test]
fn cli_help_documents_codes() {
    let out = run(&["--help"], b"");
    assert_eq!(out.status.code(), Some(0));
    let help = String::from_utf8(out.stdout).unwrap();
    for expected in [
        "PASS",
        "FAIL",
        "INCONCLUSIVE",
        "64",
        "No system qualification",
    ] {
        assert!(help.contains(expected));
    }
}

fn control_cases(name: &str) {
    let mut h = history(vec![
        scanner("initial", 0, 1, 0, 2, vec![(0, 10), (1, 10)]),
        tx(
            "transfer",
            2,
            3,
            vec![Op::Transfer {
                from: 0,
                to: 1,
                amount: 2,
            }],
            vec![],
        ),
        scanner("final", 4, 5, 0, 2, vec![(0, 8), (1, 12)]),
    ]);
    h.initial = State::from([(0, 10), (1, 10)]);
    let out = run(&["control", name, "-"], &bytes(&h));
    assert_eq!(out.status.code(), Some(0), "{name}: {:?}", out.stderr);
    let mutated = parse_jsonl(std::io::Cursor::new(&out.stdout)).unwrap();
    assert_ne!(mutated, h);
    assert_eq!(run(&["check", "-"], &out.stdout).status.code(), Some(1));
    let out = run(&["control", name, "-"], &bytes(&history(vec![])));
    assert_eq!(out.status.code(), Some(3));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not applicable"));
}

#[test]
fn cli_control_stale_read() {
    control_cases("stale-read");
}
#[test]
fn cli_control_stale_final_read() {
    control_cases("stale-final-read");
}
#[test]
fn cli_control_aborted_but_visible() {
    control_cases("aborted-but-visible");
}
#[test]
fn cli_control_duplicate_apply() {
    control_cases("duplicate-apply");
}
