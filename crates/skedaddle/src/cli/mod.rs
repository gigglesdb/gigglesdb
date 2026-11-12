//! Operator command boundary. Every stdout line is a schema-version 1 JSON envelope.
//!
//! | Exit | Outcome | Meaning |
//! |---|---|---|
//! | 0 | success | Completed; includes help/version |
//! | 2 | blocked | Capability prerequisite missing |
//! | 3 | invalid-input | Arguments or definition invalid |
//! | 4 | illegal-transition | Lifecycle forbids operation |
//! | 5 | recovery-required | Preserve and reconcile durable evidence |
//! | 6 | infrastructure | Storage/runtime unavailable |
//! | 7 | internal | Unexpected serialization failure |
//! | 8 | non-owner | Another process owns mutation |
//! | 9 | not-wired | Operational adapter unavailable |
//!
//! `status --watch` emits NDJSON, flushing one envelope per second until stopped.
use crate::{
    CapabilityReport, MigrationDefinition,
    coordinator::{
        self, Event, State, Transition, epoch::Coordinator, journal::Journal, local::Local,
    },
};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[repr(u8)]
pub enum Outcome {
    Success = 0,
    Blocked = 2,
    InvalidInput = 3,
    IllegalTransition = 4,
    RecoveryRequired = 5,
    Infrastructure = 6,
    Internal = 7,
    NonOwner = 8,
    NotWired = 9,
}
#[derive(Serialize)]
struct Envelope {
    schema_version: u32,
    command: String,
    outcome: Outcome,
    exit_code: u8,
    data: Value,
    problem: Option<Problem>,
}
#[derive(Serialize)]
struct Problem {
    what: String,
    why: String,
    next_step: String,
}
struct Failure(Outcome, Problem, Value);
type Result<T> = std::result::Result<T, Failure>;
fn fail(code: Outcome, what: impl Into<String>, why: &str, next: &str) -> Failure {
    Failure(
        code,
        Problem {
            what: what.into(),
            why: why.into(),
            next_step: next.into(),
        },
        Value::Null,
    )
}
impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        fail(
            Outcome::Infrastructure,
            e.to_string(),
            "The configured state cannot be read or persisted.",
            "Restore access to --state-root and retry the same command.",
        )
    }
}
impl From<coordinator::Error> for Failure {
    fn from(e: coordinator::Error) -> Self {
        let code = match &e {
            coordinator::Error::Lifecycle(rejection)
                if rejection.code
                    == coordinator::TransitionRejectionCode::OperatorRecoveryRequired =>
            {
                Outcome::RecoveryRequired
            }
            coordinator::Error::Lifecycle(_) | coordinator::Error::Rejected(_) => {
                Outcome::IllegalTransition
            }
            coordinator::Error::RecoveryRequired(_)
            | coordinator::Error::UnknownOutcome(_)
            | coordinator::Error::Core(_)
            | coordinator::Error::Json(_) => Outcome::RecoveryRequired,
            coordinator::Error::LostOwnership | coordinator::Error::Observer => Outcome::NonOwner,
            coordinator::Error::BudgetExceeded(_) => Outcome::Blocked,
            _ => Outcome::Infrastructure,
        };
        let next = match &e {
            coordinator::Error::Lifecycle(rejection) => format!("{} Current state {:?} permits {:?}.", rejection.code.remediation(), rejection.from, rejection.from.allowed_next()),
            _ if code == Outcome::RecoveryRequired => "Stop mutation; preserve --state-root, restore a verified journal and rebuild its local projection before retrying.".into(),
            _ => "Run status <id> --state-root <root>; restore storage access before retrying the failed operation.".into(),
        };
        fail(
            code,
            e.to_string(),
            "The journal prerequisite or durable-state operation did not succeed.",
            &next,
        )
    }
}
#[derive(Parser)]
#[command(version, about = "Operate a Skedaddle migration; stdout is JSON")]
struct Cli {
    #[arg(long, global = true)]
    state_root: Option<PathBuf>,
    #[arg(long = "definition", global = true)]
    definition_override: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Check {
        definition: PathBuf,
    },
    Init {
        definition: PathBuf,
    },
    Assess {
        id: String,
        #[arg(long)]
        sample: Option<u64>,
    },
    Start {
        id: String,
        #[arg(long, value_enum, default_value_t = Until::CatchingUp)]
        until: Until,
    },
    Resume {
        id: String,
        #[arg(long, value_enum, default_value_t = Until::CatchingUp)]
        until: Until,
    },
    Pause {
        id: String,
    },
    Status {
        id: String,
        #[arg(long)]
        watch: bool,
    },
    Verify {
        id: String,
        #[arg(long,num_args=1..)]
        tables: Vec<String>,
    },
    Cutover {
        id: String,
        #[arg(long)]
        deadline: Option<String>,
    },
    Rollback {
        id: String,
    },
    Finalize {
        id: String,
        #[arg(long)]
        drop_objects: bool,
    },
    Abort {
        id: String,
    },
    Report {
        id: String,
    },
    Buffer {
        #[command(subcommand)]
        command: BufferCommand,
    },
}
#[derive(Clone, Copy, ValueEnum)]
enum Until {
    CatchingUp,
    VerifiedReady,
    Paused,
}
impl From<Until> for crate::engine::StopAt {
    fn from(until: Until) -> Self {
        match until {
            Until::CatchingUp => Self::CatchingUp,
            Until::VerifiedReady => Self::VerifiedReady,
            Until::Paused => Self::UntilPaused,
        }
    }
}
#[derive(Subcommand)]
enum BufferCommand {
    Gc { id: String },
}
fn definition(path: &Path) -> Result<MigrationDefinition> {
    MigrationDefinition::from_path(path).map_err(|e| {
        fail(
            Outcome::InvalidInput,
            e.to_string(),
            "An invalid definition cannot safely describe a migration.",
            "Correct the definition JSON and run check <definition> again.",
        )
    })
}
fn emit(command: &str, result: Result<(Outcome, Value)>) -> u8 {
    let (outcome, mut data, problem) = match result {
        Ok((o, d)) => (o, d, None),
        Err(Failure(o, p, data)) => (o, data, Some(p)),
    };
    redact_ca_references(&mut data);
    let envelope = Envelope {
        schema_version: 1,
        command: command.into(),
        outcome,
        exit_code: outcome as u8,
        data,
        problem,
    };
    println!(
        "{}",
        serde_json::to_string(&envelope).expect("JSON values serialize")
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
    outcome as u8
}
// Init, mutation, status/watch and report share this final output boundary.
// Journal persistence retains references; operator envelopes expose summaries.
fn redact_ca_references(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.remove("tls_ca_ref");
            for child in fields.values_mut() {
                redact_ca_references(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                redact_ca_references(child);
            }
        }
        _ => {}
    }
}

fn unwired(name: &str) -> Failure {
    fail(
        Outcome::NotWired,
        if matches!(name, "rollback" | "finalize") {
            format!("{name} requires the rollback-window owner (not yet available)")
        } else {
            format!("{name} has no operational CLI adapter")
        },
        "The public primitives require live components and verified evidence; a lifecycle change alone cannot perform this operation.",
        &format!(
            "Do not retry {name} until its operational adapter is installed; run report <id> with the same --state-root to inspect durable evidence."
        ),
    )
}
fn valid_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        Err(fail(
            Outcome::InvalidInput,
            "Invalid migration ID",
            "IDs must identify one state namespace.",
            "Use the ID returned by init.",
        ))
    } else {
        Ok(())
    }
}
async fn journal(root: &Path, id: &str) -> Result<Journal> {
    Ok(crate::runner::open_journal(root, id).await?)
}
fn lock(root: &Path, id: &str) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(format!("{id}.owner")))?;
    file.try_lock().map_err(|e| match e {
        std::fs::TryLockError::WouldBlock => fail(
            Outcome::NonOwner,
            "Migration is owned by another process",
            "Concurrent mutation could compete with the active coordinator.",
            "Use status/report as an observer; retry mutation after the owning process exits.",
        ),
        std::fs::TryLockError::Error(e) => e.into(),
    })?;
    Ok(file)
}
async fn mutate(root: &Path, id: &str, transition: Transition) -> Result<Value> {
    let journal = journal(root, id).await?;
    let path = root.join(format!("{id}.local"));
    let local = if path.exists() {
        Local::open_journal(&path, &journal).await?
    } else {
        let mut local = Local::create(&path)?;
        local.replay_journal(&journal).await?;
        local
    };
    let mut owner = Coordinator::observer(journal, local, format!("cli-{}", std::process::id()))?;
    owner.acquire().await?;
    owner.commit(transition).await?;
    Ok(json!(owner.state()))
}
fn state_definition(state: &coordinator::Materialized) -> Result<&MigrationDefinition> {
    state.definition.as_ref().ok_or_else(|| {
        fail(
            Outcome::InvalidInput,
            "Migration does not exist",
            "There is no committed definition for this ID.",
            "Run init <definition> with this --state-root, then use its returned ID.",
        )
    })
}
async fn stop_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
async fn run(cli: &Cli) -> Result<(Outcome, Value)> {
    if let Some(override_path) = &cli.definition_override {
        let positional = match &cli.command {
            Command::Check { definition } | Command::Init { definition } => Some(definition),
            _ => None,
        };
        if let Some(path) = positional
            && definition(path)? != definition(override_path)?
        {
            return Err(fail(
                Outcome::InvalidInput,
                "--definition differs from positional definition",
                "Two different migration contracts were supplied.",
                "Use one matching definition for both arguments, or omit --definition for check/init.",
            ));
        }
    }
    if let Command::Check { definition: path } = &cli.command {
        let report = CapabilityReport::from_definition(&definition(path)?).map_err(|e| {
            fail(
                Outcome::InvalidInput,
                e.to_string(),
                "Capability validation failed.",
                "Correct the definition and retry check.",
            )
        })?;
        return capability(report);
    }
    let root = cli.state_root.as_ref().ok_or_else(|| {
        fail(
            Outcome::InvalidInput,
            "--state-root is required",
            "Durable state needs an explicit storage location.",
            "Pass --state-root <absolute directory> to this command.",
        )
    })?;
    if !root.is_absolute() {
        return Err(fail(
            Outcome::InvalidInput,
            "--state-root must be absolute",
            "A relative location can select a different migration after changing directories.",
            "Pass an absolute --state-root path.",
        ));
    }
    if let Command::Init { definition: path } = &cli.command {
        let def = definition(path)?;
        let digest = def.canonical_digest().map_err(|e| {
            fail(
                Outcome::Internal,
                e.to_string(),
                "Definition serialization failed.",
                "Preserve the definition and report this serialization failure.",
            )
        })?;
        let id = &digest[..24];
        std::fs::create_dir_all(root)?;
        let _lock = lock(root, id)?;
        let state = journal(root, id).await?.load(true).await?;
        if state.definition.is_some() {
            return Err(fail(
                Outcome::IllegalTransition,
                "Migration already initialized",
                "Reinitialization must not overwrite durable history.",
                &format!("Run status {id} with the same --state-root."),
            ));
        }
        let data = mutate(
            root,
            id,
            Transition::Define {
                definition: Box::new(def),
            },
        )
        .await?;
        return Ok((Outcome::Success, json!({"id":id,"state":data})));
    }
    let id = match &cli.command {
        Command::Assess { id, .. }
        | Command::Start { id, .. }
        | Command::Resume { id, .. }
        | Command::Pause { id }
        | Command::Status { id, .. }
        | Command::Verify { id, .. }
        | Command::Cutover { id, .. }
        | Command::Rollback { id }
        | Command::Finalize { id, .. }
        | Command::Abort { id }
        | Command::Report { id } => id,
        Command::Buffer {
            command: BufferCommand::Gc { id },
        } => id,
        _ => unreachable!(),
    };
    valid_id(id)?;
    // Control remains usable while authoritative journal/buffer reads fail.
    // Only the owner can publish Paused; this acknowledges the request alone.
    if matches!(cli.command, Command::Pause { .. }) && cli.definition_override.is_none() {
        let control = root.join(format!("{id}.control"));
        if control.is_dir() && crate::engine::PauseControl::new(control)?.request_pause()? {
            return Ok((
                Outcome::Success,
                json!({"id":id,"pause_requested":true,"final_state":null,"note":"Pause requested from the active owner; inspect status for its joined Paused boundary."}),
            ));
        }
    }
    let _lock = if matches!(
        cli.command,
        Command::Status { .. }
            | Command::Report { .. }
            | Command::Assess { .. }
            | Command::Pause { .. }
            | Command::Verify { .. }
            | Command::Cutover { .. }
            | Command::Finalize { .. }
            | Command::Buffer { .. }
    ) {
        None
    } else {
        Some(lock(root, id)?)
    };
    let state = journal(root, id).await?.load(true).await?;
    let def = state_definition(&state)?;
    if let Some(path) = &cli.definition_override {
        let supplied = definition(path)?;
        if &supplied != def {
            return Err(fail(
                Outcome::InvalidInput,
                "--definition differs from committed definition",
                "Executing against a different contract would invalidate journal evidence.",
                "Use the definition originally committed by init.",
            ));
        }
    }
    match &cli.command {
        Command::Status { .. } | Command::Report { .. } => {
            let trust = json!({
                "source": crate::capability::tls_trust(&def.source),
                "destination": crate::capability::tls_trust(&def.destination),
            });
            let mut visible_state = serde_json::to_value(&state).map_err(|_| {
                fail(
                    Outcome::Internal,
                    "State serialization failed",
                    "The observer response could not be prepared.",
                    "Preserve --state-root and report this serialization failure.",
                )
            })?;
            for side in ["source", "destination"] {
                if let Some(connection) = visible_state["definition"][side].as_object_mut() {
                    connection.remove("tls_ca_ref");
                }
            }
            Ok((
                Outcome::Success,
                json!({"id":id,"observer":true,"state":visible_state,"tls_trust":trust,"verification":crate::runner::verification_status(root,id,&state),"reverse":{"capture_healthy":state.rollback_eligibility_ended.is_none() && state.progress.get("reverse-capture-health").is_some_and(|(health,_)| health == "healthy"),"capture_status":state.progress.get("reverse-capture-health").map(|(health,_)| health),"lag_bytes":null,"eligible":state.rollback_eligibility_ended.is_none(),"eligibility_ended":state.rollback_eligibility_ended}}),
            ))
        }
        Command::Assess { sample, .. } => {
            let report = CapabilityReport::from_definition(def).map_err(|e| {
                fail(
                    Outcome::InvalidInput,
                    e.to_string(),
                    "Assessment cannot validate its contract.",
                    "Run check on the committed definition.",
                )
            })?;
            if sample.is_some() {
                let mut error = unwired("assess --sample");
                error.2 = json!({"offline_report":report,"requested_sample":sample,"sampling_performed":false});
                return Err(error);
            }
            capability(report)
        }
        Command::Pause { .. } => {
            let requested = crate::engine::PauseControl::new(root.join(format!("{id}.control")))?
                .request_pause()?;
            Ok((
                Outcome::Success,
                json!({"id":id,"pause_requested":requested,"final_state":state.lifecycle.state,"note":if requested {"Pause requested from the active owner; inspect status for its joined Paused boundary."} else {"No active engine owner; no lifecycle change was made."}}),
            ))
        }
        Command::Abort { .. } => {
            coordinator::next_lifecycle(
                &state.lifecycle,
                &Event::Advance {
                    to: State::AbortedToSource,
                },
                state.applied_seq + 1,
            )?;
            let journal = journal(root, id).await?;
            let mut after = 0;
            while after < state.applied_seq {
                let page = journal
                    .history_page_through(after, 128, state.applied_seq)
                    .await?;
                if page.is_empty() {
                    return Err(fail(
                        Outcome::RecoveryRequired,
                        "Abort history is incomplete",
                        "Cleanup safety cannot be established from an incomplete journal.",
                        "Preserve --state-root and restore the missing journal entries before abort.",
                    ));
                }
                for entry in page {
                    if matches!(
                        entry.transition,
                        Transition::FenceState { .. }
                            | Transition::Intent { .. }
                            | Transition::ResumeEffect { .. }
                            | Transition::EffectCompleted { .. }
                    ) {
                        return Err(unwired(
                            "abort (recorded external effects require source fence restoration)",
                        ));
                    }
                    after = entry.seq;
                }
            }
            let updated = mutate(
                root,
                id,
                Transition::Lifecycle {
                    event: Event::Advance {
                        to: State::AbortedToSource,
                    },
                },
            )
            .await?;
            Ok((
                Outcome::Success,
                json!({"state":updated,"operation_scope":"coordinator-only: verified journal contains no recorded external effects; no database restoration performed","external_effects_performed":false}),
            ))
        }
        Command::Start { until, .. } | Command::Resume { until, .. } => {
            // Preserve the existing unassessed boundary. Preparation cannot
            // silently assess a definition or manufacture runtime evidence.
            if state.lifecycle.state == State::Defined {
                return Err(unwired(if matches!(cli.command, Command::Start { .. }) {
                    "start"
                } else {
                    "resume"
                }));
            }
            let event = if matches!(cli.command, Command::Start { .. }) {
                if state.lifecycle.state != State::Assessed {
                    return Err(fail(
                        Outcome::IllegalTransition,
                        "Start preparation requires assessed state",
                        "Installation must begin from a committed assessed lifecycle.",
                        "Inspect status and complete a live assessment before start.",
                    ));
                }
                Some(Event::Advance { to: State::Copying })
            } else if matches!(
                state.lifecycle.state,
                State::Copying | State::CatchingUp | State::VerifiedReady
            ) {
                None
            } else {
                Some(Event::Resume)
            };
            if let Some(event) = event {
                coordinator::next_lifecycle(&state.lifecycle, &event, state.applied_seq + 1)?;
            }
            let (pause, paused) = tokio::sync::watch::channel(false);
            let mut operation = Box::pin(crate::runner::run(
                root,
                id,
                def,
                matches!(cli.command, Command::Resume { .. }),
                (*until).into(),
                paused,
            ));
            let result = tokio::select! {
                result = &mut operation => result,
                signal = stop_signal() => {
                    pause.send_replace(true);
                    let result = operation.await;
                    signal?;
                    result
                }
            };
            let reached = result.map_err(|error| {
                let crate::runner::RunError::Preparation(error) = error else {
                    let crate::runner::RunError::Engine(error) = error else { unreachable!() };
                    let mut failure = Failure::from(error);
                    failure.1.what = "Foreground migration stopped before the requested boundary".into();
                    failure.1.why = "The joined runner reported an endpoint, ownership or durable-state failure.".into();
                    failure.2 = json!({"id":id,"installation_may_have_started":true});
                    return failure;
                };
                let outcome = match error.kind {
                    crate::runner::FailureKind::InvalidInput => Outcome::InvalidInput,
                    crate::runner::FailureKind::Infrastructure => Outcome::Infrastructure,
                    crate::runner::FailureKind::Blocked => Outcome::Blocked,
                };
                let mut failure = fail(
                    outcome,
                    &error.what,
                    "Live runner preparation did not establish qualified adapters; no installation was attempted.",
                    &error.next_step,
                );
                failure.2 = json!({"id":id,"final_state":state.lifecycle,"blocker":error,"external_effects_performed":false});
                failure
            });
            match reached {
                Ok(reached) => {
                    let data = json!({"id":id,"reached_state":reached.state,"blockers":reached.blockers,"target_reached":reached.target_reached});
                    if reached.target_reached || reached.state == State::Paused {
                        Ok((Outcome::Success, data))
                    } else {
                        let mut failure = fail(
                            Outcome::Blocked,
                            "Migration stopped before the requested boundary",
                            "The joined engine reported unresolved blockers.",
                            "Inspect status and the returned blockers; restore their prerequisites before resuming.",
                        );
                        failure.2 = data;
                        Err(failure)
                    }
                }
                Err(mut error) => {
                    if error.2["installation_may_have_started"] == true {
                        if let Ok(journal) = journal(root, id).await
                            && let Ok(latest) = journal.load(true).await
                        {
                            error.2["final_state"] = json!(latest.lifecycle.state);
                        } else {
                            error.2["final_state"] = Value::Null;
                            error.2["state_reload_unavailable"] = json!(true);
                        }
                    }
                    Err(error)
                }
            }
        }
        Command::Verify { tables, .. } => {
            let response = Box::pin(crate::runner::request_verification(root, id, def, tables))
                .await
                .map_err(|error| {
                    let outcome = match error.kind {
                        crate::runner::FailureKind::Infrastructure => Outcome::Infrastructure,
                        crate::runner::FailureKind::InvalidInput => Outcome::InvalidInput,
                        crate::runner::FailureKind::Blocked => Outcome::Blocked,
                    };
                    fail(
                        outcome,
                        &error.what,
                        "Independent verification could not run.",
                        &error.next_step,
                    )
                })?;
            let outcome = match response.exit_code {
                0 => Outcome::Success,
                2 => Outcome::Blocked,
                _ => Outcome::Infrastructure,
            };
            if outcome == Outcome::Success {
                Ok((outcome, response.data))
            } else {
                let mut error = fail(
                    outcome,
                    "Verification did not establish complete matching scope",
                    "Inspect per-table coverage and range details.",
                    "Restore active capture/apply and repair mismatches before verifying again.",
                );
                error.2 = response.data;
                Err(error)
            }
        }
        Command::Cutover { deadline, .. } => {
            let deadline = cutover_deadline(deadline.as_deref())?;
            let response = crate::runner::request_cutover(root, id, deadline)
                .await
                .map_err(|error| {
                    let outcome = match error.kind {
                        crate::runner::FailureKind::Infrastructure => Outcome::Infrastructure,
                        crate::runner::FailureKind::InvalidInput => Outcome::InvalidInput,
                        crate::runner::FailureKind::Blocked => Outcome::Blocked,
                    };
                    fail(
                        outcome,
                        &error.what,
                        "The cutover request could not reach the owner.",
                        &error.next_step,
                    )
                })?;
            cutover_result(response)
        }
        Command::Rollback { .. } => Err(unwired("rollback")),
        Command::Finalize { drop_objects, .. } => {
            if *drop_objects
                || !matches!(
                    state.lifecycle.state,
                    State::DestinationActive | State::RollbackWindow
                )
            {
                return Err(unwired("finalize"));
            }
            let response = crate::runner::request_finalize(root, id).await;
            if response.exit_code == 0 {
                Ok((Outcome::Success, response.data))
            } else {
                let mut error = fail(
                    Outcome::Blocked,
                    "Finalization could not reach the active owner",
                    "No lifecycle change was authorized.",
                    "Resume the post-activation owner and retry.",
                );
                error.2 = response.data;
                Err(error)
            }
        }
        Command::Buffer { .. } => {
            let response = crate::runner::request_gc(root, id).await;
            let outcome = match response.exit_code {
                0 => Outcome::Success,
                3 => Outcome::InvalidInput,
                8 => Outcome::NonOwner,
                _ => Outcome::Infrastructure,
            };
            if response.exit_code == 0 {
                Ok((outcome, response.data))
            } else {
                let mut error = fail(
                    outcome,
                    "Buffer GC request failed",
                    "Inspect data.error.kind.",
                    "Restore the active owner and retry.",
                );
                error.2 = response.data;
                Err(error)
            }
        }
        _ => unreachable!(),
    }
}
/// Parse `--deadline`: a positive integer with an optional `ms`, `s`, `m` or
/// `h` unit (bare numbers are seconds).
fn cutover_deadline(raw: Option<&str>) -> Result<Duration> {
    let invalid = || {
        fail(
            Outcome::InvalidInput,
            "cutover requires a positive --deadline",
            "Cutover must be bounded by an explicit deadline before fencing writes.",
            "Pass --deadline <n>[ms|s|m|h], for example --deadline 30s.",
        )
    };
    let raw = raw.ok_or_else(invalid)?.trim();
    let split = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
    let (digits, unit) = raw.split_at(split);
    let value: u64 = digits.parse().map_err(|_| invalid())?;
    let duration = match unit {
        "ms" => Some(Duration::from_millis(value)),
        "" | "s" => Some(Duration::from_secs(value)),
        "m" => value.checked_mul(60).map(Duration::from_secs),
        "h" => value.checked_mul(3600).map(Duration::from_secs),
        _ => None,
    }
    .ok_or_else(invalid)?;
    if duration.is_zero() || u64::try_from(duration.as_millis()).is_err() {
        return Err(invalid());
    }
    Ok(duration)
}
/// Map the runner's cutover envelope onto the CLI exit-code scheme.
fn cutover_result(response: crate::runner::CutoverOutcome) -> Result<(Outcome, Value)> {
    use crate::runner::{CutoverErrorKind as Kind, CutoverOutcome as O};
    let data = serde_json::to_value(&response).map_err(|e| {
        fail(
            Outcome::Internal,
            "cutover outcome could not be serialized",
            &e.to_string(),
            "Run status <id> --state-root <root> and report this defect.",
        )
    })?;
    let (outcome, what, why, next) = match &response {
        O::Completed { .. } => return Ok((Outcome::Success, data)),
        O::NotReady { next_step, .. } => (
            Outcome::Blocked,
            "Cutover refused: migration is not ready",
            "One or more readiness blockers are listed in data.reasons.",
            next_step.as_str(),
        ),
        O::DeadlineAborted { next_step, .. } => (
            Outcome::Blocked,
            "Cutover deadline expired before activation; writes were restored to the source",
            "The cutover did not finish within --deadline and was aborted to the source.",
            next_step.as_str(),
        ),
        O::Failed {
            error, next_step, ..
        } => {
            let outcome = match error.kind {
                Kind::InvalidRequest => Outcome::InvalidInput,
                Kind::LostOwnership | Kind::StaleGeneration => Outcome::NonOwner,
                Kind::ActivationUncertain | Kind::SourceBreach => Outcome::RecoveryRequired,
                Kind::NoOwner | Kind::OwnerStopped | Kind::Infrastructure | Kind::Driver => {
                    Outcome::Infrastructure
                }
            };
            (
                outcome,
                "Cutover did not complete",
                error.message.as_str(),
                next_step.as_str(),
            )
        }
    };
    let mut error = fail(outcome, what, why, next);
    error.2 = data;
    Err(error)
}
fn capability(report: CapabilityReport) -> Result<(Outcome, Value)> {
    if report.blocking().is_empty() {
        Ok((Outcome::Success, json!(report)))
    } else {
        let mut error = fail(
            Outcome::Blocked,
            "Offline capability assessment has blocking items",
            "Offline capability blockers are not proof of database readiness.",
            "Resolve each capability item's remediation and obtain live inspection evidence before start.",
        );
        error.2 = json!(report);
        Err(error)
    }
}
fn contextualize(cli: &Cli, mut failure: Failure) -> Failure {
    fn quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }
    let invocation = match &cli.command {
        Command::Check { definition } | Command::Init { definition } => {
            format!("skedaddle check {}", quote(&definition.to_string_lossy()))
        }
        command => {
            let id = match command {
                Command::Assess { id, .. }
                | Command::Start { id, .. }
                | Command::Resume { id, .. }
                | Command::Pause { id }
                | Command::Status { id, .. }
                | Command::Verify { id, .. }
                | Command::Cutover { id, .. }
                | Command::Rollback { id }
                | Command::Finalize { id, .. }
                | Command::Abort { id }
                | Command::Report { id }
                | Command::Buffer {
                    command: BufferCommand::Gc { id },
                } => id,
                _ => unreachable!(),
            };
            if let Some(root) = &cli.state_root {
                format!(
                    "skedaddle --state-root {} status {}",
                    quote(&root.to_string_lossy()),
                    quote(id)
                )
            } else {
                "skedaddle --help".into()
            }
        }
    };
    failure.1.next_step = format!("Run {invocation}; then {}", failure.1.next_step);
    failure
}
/// Parse without clap's process exit so even usage failures have a JSON envelope.
pub fn main_entry() -> u8 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let help = matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            return if help {
                emit(
                    "help",
                    Ok((Outcome::Success, json!({"text":error.to_string()}))),
                )
            } else {
                emit(
                    "parse",
                    Err(fail(
                        Outcome::InvalidInput,
                        error.to_string(),
                        "The command cannot be interpreted.",
                        "Run skedaddle --help, then correct the command arguments.",
                    )),
                )
            };
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => return emit("runtime", Err(e.into())),
    };
    let name = match &cli.command {
        Command::Check { .. } => "check",
        Command::Init { .. } => "init",
        Command::Assess { .. } => "assess",
        Command::Start { .. } => "start",
        Command::Resume { .. } => "resume",
        Command::Pause { .. } => "pause",
        Command::Status { .. } => "status",
        Command::Verify { .. } => "verify",
        Command::Cutover { .. } => "cutover",
        Command::Rollback { .. } => "rollback",
        Command::Finalize { .. } => "finalize",
        Command::Abort { .. } => "abort",
        Command::Report { .. } => "report",
        Command::Buffer { .. } => "buffer gc",
    };
    loop {
        let code = emit(
            name,
            runtime
                .block_on(run(&cli))
                .map_err(|failure| contextualize(&cli, failure)),
        );
        if code != 0 || !matches!(cli.command, Command::Status { watch: true, .. }) {
            return code;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
