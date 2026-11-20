#[path = "support/root.rs"]
mod test_root;

use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

use skedaddle::{
    hooks::{HookOutcome, HookPhase, OperatorHook, run_phase_then},
    security::{REDACTED, Redactor, SecretRef},
};

fn scratch() -> PathBuf {
    let path = crate::test_root::scratch_root().join(format!("hooks-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn reference(root: &std::path::Path, name: &str, value: &str) -> SecretRef {
    let path = root.join(name);
    fs::write(&path, value).unwrap();
    SecretRef::parse(&format!("file:{}", path.display())).unwrap()
}

fn hook(windows: &str, unix: &str, phase: HookPhase) -> OperatorHook {
    let root = scratch();
    let command = if cfg!(windows) {
        let script = root.join("hook.ps1");
        fs::write(&script, windows).unwrap();
        vec![
            format!(
                "{}/System32/WindowsPowerShell/v1.0/powershell.exe",
                std::env::var("SystemRoot").unwrap()
            ),
            "-NoLogo".into(),
            "-NoProfile".into(),
            "-NonInteractive".into(),
            "-ExecutionPolicy".into(),
            "Bypass".into(),
            "-File".into(),
            script.to_string_lossy().into_owned(),
        ]
    } else {
        let script = root.join("hook.sh");
        fs::write(&script, unix).unwrap();
        vec!["/bin/sh".into(), script.to_string_lossy().into_owned()]
    };
    let mut secret_env = BTreeMap::new();
    if cfg!(windows) {
        // Windows PowerShell's CLR startup requires SystemRoot. It is declared
        // explicitly, never inherited as an exception to env_clear.
        secret_env.insert(
            "SystemRoot".into(),
            SecretRef::parse("env:SystemRoot").unwrap(),
        );
    }
    OperatorHook {
        name: "operator-test".into(),
        phase,
        command,
        secret_env,
        timeout: Duration::from_secs(10),
    }
}

#[test]
fn hook_failure_aborts_before_side_effects() {
    let hook = hook(
        "[Console]::Out.WriteLine('before failure'); exit 7",
        "echo 'before failure'; exit 7",
        HookPhase::PreFence,
    );
    let mut events = Vec::new();
    let mut effect = false;
    let error = run_phase_then(
        HookPhase::PreFence,
        &[hook],
        &Redactor::default(),
        |event| events.push(event.clone()),
        || effect = true,
    )
    .unwrap_err();
    assert_eq!(error.outcome, HookOutcome::Failed);
    assert!(!effect);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].exit_code, Some(7));
    assert!(events[0].stdout.contains("before failure"));
}

#[test]
fn hook_timeout_aborts_and_audits() {
    let mut hook = hook(
        "[Console]::Out.WriteLine('started'); Start-Sleep -Seconds 30",
        "echo started; /bin/sleep 30",
        HookPhase::PreRollback,
    );
    hook.timeout = Duration::from_secs(2);
    let started = Instant::now();
    let mut events = Vec::new();
    let mut effect = false;
    let error = run_phase_then(
        HookPhase::PreRollback,
        &[hook],
        &Redactor::default(),
        |event| events.push(event.clone()),
        || effect = true,
    )
    .unwrap_err();
    assert_eq!(error.outcome, HookOutcome::TimedOut);
    assert!(started.elapsed() < Duration::from_secs(8));
    assert!(!effect);
    assert_eq!(events[0].outcome, HookOutcome::TimedOut);
}

#[test]
fn hook_environment_isolation() {
    let root = scratch();
    let mut hook = hook(
        "if ($env:DECLARED_VALUE -ne 'declared-private-value') { exit 21 }; if ($env:SKEDADDLE_P12A_PARENT_SENTINEL -or $env:SKEDADDLE_TEST_ROOT -or $env:CARGO_TARGET_DIR -or $env:PATH -or $env:TEMP) { exit 22 }; [Console]::Out.WriteLine($env:DECLARED_VALUE); exit 0",
        "[ \"$DECLARED_VALUE\" = 'declared-private-value' ] || exit 21; [ -z \"$SKEDADDLE_P12A_PARENT_SENTINEL$SKEDADDLE_TEST_ROOT$CARGO_TARGET_DIR$TEMP\" ] || exit 22; printf '%s' \"$DECLARED_VALUE\"",
        HookPhase::PreFence,
    );
    hook.secret_env.insert(
        "DECLARED_VALUE".into(),
        reference(&root, "declared", "declared-private-value"),
    );
    let mut events = Vec::new();
    let value = run_phase_then(
        HookPhase::PreFence,
        &[hook],
        &Redactor::default(),
        |event| events.push(event.clone()),
        || 123,
    )
    .unwrap();
    assert_eq!(value, 123);
    assert_eq!(events[0].stdout.trim(), REDACTED);
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("declared-private-value")
    );
}

#[test]
fn stdout_stderr_and_sql_echo_are_redacted_before_audit() {
    let root = scratch();
    let mut hook = hook(
        "[Console]::Out.WriteLine($env:DB); [Console]::Error.WriteLine('SQL error: bad value ' + 'very-Secret-Pass'); [Console]::Error.WriteLine('password=unregistered-value'); exit 0",
        "printf '%s\\n' \"$DB\"; echo 'SQL error: bad value very-Secret-Pass' >&2; echo 'password=unregistered-value' >&2",
        HookPhase::PostActivate,
    );
    hook.secret_env.insert(
        "DB".into(),
        reference(&root, "db", "postgres://u:very-Secret-Pass@host/db"),
    );
    let mut events = Vec::new();
    run_phase_then(
        HookPhase::PostActivate,
        &[hook],
        &Redactor::default(),
        |event| events.push(event.clone()),
        || (),
    )
    .unwrap();
    let audit = serde_json::to_string(&events).unwrap();
    assert!(!audit.contains("very-Secret-Pass"));
    assert!(!audit.contains("unregistered-value"));
    assert!(events[0].stdout.contains(REDACTED));
    assert!(events[0].stderr.contains("SQL error: bad value [REDACTED]"));
}

#[test]
fn all_four_phases_gate_effect_and_keep_definition_order() {
    for phase in [
        HookPhase::PreFence,
        HookPhase::PostActivate,
        HookPhase::PreRollback,
        HookPhase::PostRollback,
    ] {
        let mut one = hook("[Console]::Out.WriteLine('one')", "echo one", phase);
        one.name = "one".into();
        let mut two = hook("[Console]::Out.WriteLine('two')", "echo two", phase);
        two.name = "two".into();
        let irrelevant = hook(
            "exit 99",
            "exit 99",
            if phase == HookPhase::PreFence {
                HookPhase::PostRollback
            } else {
                HookPhase::PreFence
            },
        );
        let mut events = Vec::new();
        let value = run_phase_then(
            phase,
            &[one, irrelevant, two],
            &Redactor::default(),
            |event| events.push(event.clone()),
            || "effect",
        )
        .unwrap();
        assert_eq!(value, "effect");
        assert_eq!(
            events.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
            ["one", "two"]
        );
        assert!(events.iter().all(|e| e.outcome == HookOutcome::Succeeded));
    }
}

#[test]
fn failure_stops_later_hooks() {
    let marker = scratch().join("later-hook-ran");
    let one = hook("exit 3", "exit 3", HookPhase::PreFence);
    let two = hook(
        &format!(
            "[System.IO.File]::WriteAllText('{}', 'ran')",
            marker.display()
        ),
        &format!("echo ran > '{}'", marker.display()),
        HookPhase::PreFence,
    );
    assert!(
        run_phase_then(
            HookPhase::PreFence,
            &[one, two],
            &Redactor::default(),
            |_| {},
            || ()
        )
        .is_err()
    );
    assert!(!marker.exists());
}

#[test]
fn invalid_configuration_and_missing_secrets_abort_before_spawn() {
    let mut hook = hook("exit 0", "exit 0", HookPhase::PreFence);
    hook.timeout = Duration::ZERO;
    let mut audit = Vec::new();
    let error = run_phase_then(
        HookPhase::PreFence,
        &[hook.clone()],
        &Redactor::default(),
        |event| audit.push(event.clone()),
        || panic!("effect ran"),
    )
    .unwrap_err();
    assert_eq!(error.outcome, HookOutcome::InvalidConfiguration);
    hook.timeout = Duration::from_secs(10);
    hook.secret_env.insert(
        "BAD-NAME".into(),
        SecretRef::parse("env:DOES_NOT_EXIST_P12A").unwrap(),
    );
    assert_eq!(
        run_phase_then(
            HookPhase::PreFence,
            &[hook.clone()],
            &Redactor::default(),
            |_| {},
            || panic!("effect ran")
        )
        .unwrap_err()
        .outcome,
        HookOutcome::InvalidConfiguration
    );
    hook.secret_env.clear();
    hook.secret_env.insert(
        "VALID_NAME".into(),
        SecretRef::parse("env:DOES_NOT_EXIST_P12A").unwrap(),
    );
    assert_eq!(
        run_phase_then(
            HookPhase::PreFence,
            &[hook],
            &Redactor::default(),
            |_| {},
            || panic!("effect ran")
        )
        .unwrap_err()
        .outcome,
        HookOutcome::SecretUnavailable
    );
}

#[test]
fn phase_secrets_are_registered_before_first_hook_runs() {
    let root = scratch();
    let one = hook(
        "[Console]::Out.WriteLine('second-hook-secret')",
        "echo second-hook-secret",
        HookPhase::PreFence,
    );
    let mut two = hook("exit 0", "exit 0", HookPhase::PreFence);
    two.secret_env.insert(
        "SECRET".into(),
        reference(&root, "second", "second-hook-secret"),
    );
    let mut events = Vec::new();
    run_phase_then(
        HookPhase::PreFence,
        &[one, two],
        &Redactor::default(),
        |event| events.push(event.clone()),
        || (),
    )
    .unwrap();
    assert_eq!(events[0].stdout.trim(), REDACTED);
}

#[test]
fn large_output_does_not_deadlock_or_disclose_truncated_secrets() {
    let hook = hook(
        "$line = 'x' * 8192; for ($i = 0; $i -lt 140; $i++) { [Console]::Out.WriteLine($line); [Console]::Error.WriteLine($line) }; exit 0",
        "i=0; while [ $i -lt 140000 ]; do printf 'xxxxxxxxx\\n'; printf 'xxxxxxxxx\\n' >&2; i=$((i+1)); done",
        HookPhase::PostRollback,
    );
    let mut events = Vec::new();
    run_phase_then(
        HookPhase::PostRollback,
        &[hook],
        &Redactor::default(),
        |event| events.push(event.clone()),
        || (),
    )
    .unwrap();
    assert_eq!(events[0].stdout, "[hook output exceeded capture limit]");
    assert_eq!(events[0].stderr, "[hook output exceeded capture limit]");
}
