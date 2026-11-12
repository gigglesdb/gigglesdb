//! Operator hooks with isolated environments and an audit boundary.
//!
//! Use `run_phase_then` to gate the next coordinator effect. Hook commands are
//! trusted operator programs: their own effects cannot be rolled back here.
//! Post hooks gate subsequent work, not an activation/rollback already committed.

use std::{
    collections::BTreeMap,
    fmt,
    io::Read,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::security::{Redactor, SecretRef, resolve};

const CAPTURE_LIMIT: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookPhase {
    PreFence,
    PostActivate,
    PreRollback,
    PostRollback,
}

/// All variables are explicitly declared references. No parent variables are
/// inherited, including PATH. Prefer an absolute executable path. The definition
/// schema currently lacks these four phases and a timeout (see P12a report).
#[derive(Clone, Debug)]
pub struct OperatorHook {
    pub name: String,
    pub phase: HookPhase,
    pub command: Vec<String>,
    pub secret_env: BTreeMap<String, SecretRef>,
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookOutcome {
    Succeeded,
    Failed,
    TimedOut,
    InvalidConfiguration,
    SecretUnavailable,
    ProcessError,
}

/// Only redacted text crosses the audit callback. No executable/arguments,
/// environment values, or platform error strings are copied into errors.
#[derive(Clone, Debug, Serialize)]
pub struct HookAudit {
    pub name: String,
    pub phase: HookPhase,
    pub outcome: HookOutcome,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HookError {
    pub outcome: HookOutcome,
}

impl fmt::Display for HookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.outcome {
            HookOutcome::Succeeded => "hook completed",
            HookOutcome::Failed => "operator hook failed; inspect the redacted audit before retrying",
            HookOutcome::TimedOut => "operator hook timed out; inspect the redacted audit and timeout before retrying",
            HookOutcome::InvalidConfiguration => "invalid operator hook; specify a command, positive timeout and valid environment names",
            HookOutcome::SecretUnavailable => "operator hook secret unavailable; check the declared references",
            HookOutcome::ProcessError => "operator hook process or capture failed; inspect the redacted audit",
        })
    }
}

impl std::error::Error for HookError {}

/// Hooks run in definition order. Failure stops the phase and the supplied
/// effect is never called. The caller must not perform the effect beforehand.
pub fn run_phase_then<T>(
    phase: HookPhase,
    hooks: &[OperatorHook],
    redactor: &Redactor,
    mut audit: impl FnMut(&HookAudit),
    effect: impl FnOnce() -> T,
) -> Result<T, HookError> {
    // Resolve/register the whole phase before execution so a hook that echoes
    // another hook's declared secret is redacted too.
    let selected: Vec<_> = hooks.iter().filter(|hook| hook.phase == phase).collect();
    let mut redactor = redactor.clone();
    let mut environments = Vec::new();
    for hook in &selected {
        let result = prepare(hook, &mut redactor);
        match result {
            Ok(environment) => environments.push(environment),
            Err(outcome) => {
                emit(hook, outcome, None, "", "", &redactor, &mut audit);
                return Err(HookError { outcome });
            }
        }
    }
    for (hook, environment) in selected.into_iter().zip(environments) {
        let result = execute(hook, environment);
        emit(
            hook,
            result.outcome,
            result.exit_code,
            &result.stdout,
            &result.stderr,
            &redactor,
            &mut audit,
        );
        if result.outcome != HookOutcome::Succeeded {
            return Err(HookError {
                outcome: result.outcome,
            });
        }
    }
    Ok(effect())
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn prepare(
    hook: &OperatorHook,
    redactor: &mut Redactor,
) -> Result<BTreeMap<String, crate::security::Secret>, HookOutcome> {
    if hook.name.trim().is_empty()
        || hook.command.first().is_none_or(|s| s.trim().is_empty())
        || hook.timeout.is_zero()
        || hook.command.iter().any(|s| s.contains('\0'))
        || hook.secret_env.keys().any(|name| !valid_env_name(name))
    {
        return Err(HookOutcome::InvalidConfiguration);
    }
    let mut environment = BTreeMap::new();
    for (name, reference) in &hook.secret_env {
        let secret = resolve(reference).map_err(|_| HookOutcome::SecretUnavailable)?;
        redactor.register(&secret);
        environment.insert(name.clone(), secret);
    }
    Ok(environment)
}

fn emit(
    hook: &OperatorHook,
    outcome: HookOutcome,
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
    redactor: &Redactor,
    audit: &mut impl FnMut(&HookAudit),
) {
    audit(&HookAudit {
        name: redactor.redact(&hook.name),
        phase: hook.phase,
        outcome,
        exit_code,
        stdout: redactor.redact(stdout),
        stderr: redactor.redact(stderr),
    });
}

struct Execution {
    outcome: HookOutcome,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Execution {
    fn process_error() -> Self {
        Self {
            outcome: HookOutcome::ProcessError,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }
}

fn execute(
    hook: &OperatorHook,
    environment: BTreeMap<String, crate::security::Secret>,
) -> Execution {
    let started = Instant::now();
    let mut command = Command::new(&hook.command[0]);
    command
        .args(&hook.command[1..])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in &environment {
        command.env(name, value.expose_secret());
    }
    process_tree::configure(&mut command);
    let Ok(tree) = process_tree::Tree::new() else {
        return Execution::process_error();
    };
    let Ok(mut child) = command.spawn() else {
        return Execution::process_error();
    };
    if tree.attach(&child).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        return Execution::process_error();
    }
    let stdout = capture(child.stdout.take().expect("piped stdout"));
    let stderr = capture(child.stderr.take().expect("piped stderr"));
    let mut out = None;
    let mut err = None;
    let mut status = None;
    let outcome = loop {
        if started.elapsed() >= hook.timeout {
            break HookOutcome::TimedOut;
        }
        if status.is_none() {
            match child.try_wait() {
                Ok(value) => status = value,
                Err(_) => break HookOutcome::ProcessError,
            }
        }
        if receive(&stdout, &mut out).is_err() || receive(&stderr, &mut err).is_err() {
            break HookOutcome::ProcessError;
        }
        if let Some(status) = status
            && out.is_some()
            && err.is_some()
        {
            break if status.success() {
                HookOutcome::Succeeded
            } else {
                HookOutcome::Failed
            };
        }
        thread::sleep(Duration::from_millis(5).min(hook.timeout.saturating_sub(started.elapsed())));
    };
    // A job/process group also contains descendants holding inherited pipes.
    // Terminate it even when the direct child has already exited.
    tree.terminate(&child);
    let _ = child.kill();
    let waited = child.wait().ok();
    // Do not block indefinitely waiting for a misbehaving capture thread. Job
    // termination closes ordinary descendant pipes; a detached writer cannot
    // postpone the caller's timeout or cause partial unredacted audit text.
    let finish = |received: Option<String>, rx: Receiver<Result<String, ()>>| {
        received
            .or_else(|| {
                rx.recv_timeout(Duration::from_millis(100))
                    .ok()
                    .and_then(Result::ok)
            })
            .unwrap_or_else(|| "[hook output unavailable]".to_owned())
    };
    Execution {
        outcome,
        exit_code: status.or(waited).and_then(|s| s.code()),
        stdout: finish(out, stdout),
        stderr: finish(err, stderr),
    }
}

fn receive(rx: &Receiver<Result<String, ()>>, output: &mut Option<String>) -> Result<(), ()> {
    if output.is_none() {
        match rx.try_recv() {
            Ok(value) => *output = Some(value?),
            Err(TryRecvError::Disconnected) => return Err(()),
            Err(TryRecvError::Empty) => {}
        }
    }
    Ok(())
}

fn capture(mut pipe: impl Read + Send + 'static) -> Receiver<Result<String, ()>> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut chunk = [0; 8192];
        let mut exceeded = false;
        let result = loop {
            match pipe.read(&mut chunk) {
                Ok(0) => {
                    break Ok(if exceeded {
                        // Discard all output, rather than exposing a truncated
                        // prefix of a secret that cannot be matched in full.
                        "[hook output exceeded capture limit]".to_owned()
                    } else {
                        String::from_utf8_lossy(&bytes).into_owned()
                    });
                }
                Ok(n) => {
                    if bytes.len() + n <= CAPTURE_LIMIT && !exceeded {
                        bytes.extend_from_slice(&chunk[..n]);
                    } else {
                        exceeded = true;
                        bytes.clear();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => break Err(()),
            }
        };
        let _ = tx.send(result);
    });
    rx
}

#[cfg(windows)]
mod process_tree {
    use super::*;
    use std::{
        ffi::c_void,
        os::windows::{io::AsRawHandle, process::CommandExt},
        ptr,
    };

    type Handle = *mut c_void;

    #[repr(C)]
    #[derive(Default)]
    struct BasicLimits {
        process_time: i64,
        job_time: i64,
        flags: u32,
        min_working_set: usize,
        max_working_set: usize,
        active_processes: u32,
        affinity: usize,
        priority: u32,
        scheduling: u32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct ExtendedLimits {
        basic: BasicLimits,
        io_counters: [u64; 6],
        process_memory: usize,
        job_memory: usize,
        peak_process_memory: usize,
        peak_job_memory: usize,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateJobObjectW(attributes: *const c_void, name: *const u16) -> Handle;
        fn SetInformationJobObject(
            job: Handle,
            class: i32,
            info: *const c_void,
            length: u32,
        ) -> i32;
        fn AssignProcessToJobObject(job: Handle, process: Handle) -> i32;
        fn TerminateJobObject(job: Handle, code: u32) -> i32;
        fn CloseHandle(handle: Handle) -> i32;
    }

    pub fn configure(command: &mut Command) {
        // Do not create a visible console for non-interactive hooks.
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }

    pub struct Tree(Handle);

    impl Tree {
        pub fn new() -> Result<Self, ()> {
            // SAFETY: null security/name pointers request an unnamed job with
            // default security. The returned handle is owned by Tree.
            let job = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
            if job.is_null() {
                return Err(());
            }
            let tree = Self(job);
            let limits = ExtendedLimits {
                basic: BasicLimits {
                    flags: 0x2000,
                    ..Default::default()
                },
                ..Default::default()
            };
            // SAFETY: limits matches JOBOBJECT_EXTENDED_LIMIT_INFORMATION and
            // remains live throughout the call; class 9 selects that layout.
            if unsafe {
                SetInformationJobObject(
                    job,
                    9,
                    (&limits as *const ExtendedLimits).cast(),
                    std::mem::size_of::<ExtendedLimits>() as u32,
                )
            } == 0
            {
                return Err(());
            }
            Ok(tree)
        }

        pub fn attach(&self, child: &Child) -> Result<(), ()> {
            // SAFETY: both handles remain owned/live across this call. Failure
            // is fatal; hooks never run without descendant termination support.
            if unsafe { AssignProcessToJobObject(self.0, child.as_raw_handle()) } == 0 {
                Err(())
            } else {
                Ok(())
            }
        }

        pub fn terminate(&self, _child: &Child) {
            // SAFETY: this job is owned by Tree and contains only this hook.
            unsafe {
                TerminateJobObject(self.0, 1);
            }
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            // SAFETY: close exactly the handle created above. KILL_ON_JOB_CLOSE
            // also covers unwinding/failure paths after attachment.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(unix)]
mod process_tree {
    use super::*;
    use std::os::unix::process::CommandExt;

    pub fn configure(command: &mut Command) {
        command.process_group(0);
    }

    pub struct Tree;

    impl Tree {
        pub fn new() -> Result<Self, ()> {
            Ok(Self)
        }
        pub fn attach(&self, _child: &Child) -> Result<(), ()> {
            Ok(())
        }
        pub fn terminate(&self, child: &Child) {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &format!("-{}", child.id())])
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}
