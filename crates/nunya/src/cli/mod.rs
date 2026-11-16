//! Operator commands.
/// Session helper and command implementation.
pub mod session;
use crate::{
    config::Config,
    task::{TaskStore, format_task_id, parse_task_id},
};
use clap::{Parser, Subcommand};
use std::{
    io::{self, BufRead},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;
#[derive(Parser)]
#[command(name = "nunya", about = "Tokenizing database gateway")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Session {
        #[command(subcommand)]
        command: session::SessionCommand,
    },
    Exec(session::ExecOpts),
    Version,
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Standing {
        #[command(subcommand)]
        command: StandingCommand,
    },
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
}
#[derive(Subcommand)]
enum PolicyCommand {
    Check {
        #[arg(long)]
        config: PathBuf,
    },
}
#[derive(Subcommand)]
enum TaskCommand {
    Open {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, value_parser=parse_ttl)]
        ttl: u64,
        #[arg(long)]
        label: Option<String>,
    },
    Close {
        #[arg(long)]
        config: PathBuf,
        id: String,
    },
    List {
        #[arg(long)]
        config: PathBuf,
    },
}
#[derive(Subcommand)]
enum StandingCommand {
    Hash,
}
#[derive(Subcommand)]
enum KeyCommand {
    Generate,
}
fn parse_ttl(text: &str) -> Result<u64, String> {
    let (number, scale) = match text.as_bytes().last() {
        Some(b's') => (&text[..text.len() - 1], 1000),
        Some(b'm') => (&text[..text.len() - 1], 60_000),
        Some(b'h') => (&text[..text.len() - 1], 3_600_000),
        _ => return Err("TTL must use s, m or h".into()),
    };
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid TTL".into());
    }
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(scale))
        .filter(|n| *n > 0)
        .ok_or_else(|| "invalid TTL".into())
}
fn store(cfg: &Config) -> Result<TaskStore, Vec<String>> {
    TaskStore::open_dir(&cfg.state_dir.join("tasks"))
        .map_err(|_| vec!["task store unavailable".into()])
}
fn task(command: TaskCommand) -> Result<i32, Vec<String>> {
    match command {
        TaskCommand::Open { config, ttl, label } => {
            let cfg = Config::load(&config)?;
            let opened = store(&cfg)?
                .open(ttl, label)
                .map_err(|_| vec!["unable to open task".into()])?;
            println!(
                "user: task:{}\nsecret: {}\nexpires_unix_ms: {}",
                format_task_id(&opened.id),
                opened.secret,
                opened.expires_unix_ms
            );
            eprintln!("Save this secret now; it cannot be shown again.");
        }
        TaskCommand::Close { config, id } => {
            let id = parse_task_id(id.strip_prefix("task:").unwrap_or(&id))
                .ok_or_else(|| vec!["invalid task identifier".into()])?;
            let cfg = Config::load(&config)?;
            if !store(&cfg)?
                .close(&id)
                .map_err(|_| vec!["unable to close task".into()])?
            {
                return Err(vec!["task not found".into()]);
            }
            println!("task closed");
        }
        TaskCommand::List { config } => {
            let cfg = Config::load(&config)?;
            for (id, expiry, label) in store(&cfg)?
                .list()
                .map_err(|_| vec!["unable to list tasks".into()])?
            {
                println!(
                    "task:{}\t{}\t{}",
                    format_task_id(&id),
                    expiry,
                    label.unwrap_or_default()
                );
            }
        }
    }
    Ok(0)
}
async fn validate(path: &Path) -> Result<(), Vec<String>> {
    let cfg = Config::load(path)?;
    if let Some(tls) = &cfg.tls {
        crate::config::load_tls(tls).map_err(|e| vec![e])?;
    }
    let mut policy = crate::policy::file::load_policy(&cfg.policy).map_err(|errors| {
        errors
            .into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    })?;
    if let crate::policy::ReadLogSink::File { path, .. } = &mut policy.readlog
        && path.is_relative()
    {
        *path = cfg.policy.parent().unwrap_or(Path::new(".")).join(&*path);
    }
    let _store = store(&cfg)?;
    let _sink = crate::readlog::open_sink(&policy.readlog)
        .map_err(|_| vec!["read log unavailable".into()])?;
    if let Some(upstream) = cfg.upstream_config() {
        let schema = crate::serve::connect_schema(&upstream)
            .await
            .map_err(|e| vec![e])?;
        let snapshot = crate::serve::snapshot(&schema.client, &cfg)
            .await
            .map_err(|e| vec![e])?;
        crate::policy::validate::validate(&policy, &snapshot).map_err(|errors| {
            errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
        })?;
    }
    if let Some(section) = &cfg.spanner_native {
        use crate::spanner::proxy::SpannerUpstream;
        let upstream = match &section.emulator_endpoint {
            Some(endpoint) => SpannerUpstream::Emulator {
                endpoint: endpoint.clone(),
            },
            None => SpannerUpstream::Google {
                endpoint: section
                    .endpoint
                    .clone()
                    .unwrap_or_else(|| "https://spanner.googleapis.com".into()),
            },
        };
        let runner =
            crate::spanner::catalog::CatalogRunner::new(&upstream, section.database.clone())
                .await
                .map_err(|_| vec!["spanner: catalog unavailable".into()])?;
        let snapshot = crate::schema::spanner_gsql::load_gsql_snapshot(&runner, 0)
            .await
            .map_err(|_| vec!["spanner: catalog unavailable".into()])?;
        crate::policy::validate::validate(&policy, &snapshot).map_err(|errors| {
            errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
        })?;
    }
    Ok(())
}
fn runtime_validate(path: &Path) -> Result<(), Vec<String>> {
    tokio::runtime::Runtime::new()
        .map_err(|_| vec!["runtime unavailable".into()])?
        .block_on(validate(path))
}
fn execute(command: Command) -> Result<i32, Vec<String>> {
    match command {
        Command::Session { command } => Ok(session::run_command(command)),
        Command::Exec(opts) => Ok(session::run_exec(opts)),
        Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            Ok(0)
        }
        Command::Task { command } => task(command),
        Command::Key {
            command: KeyCommand::Generate,
        } => {
            let mut key = Zeroizing::new([0u8; 32]);
            getrandom::fill(&mut *key).map_err(|_| vec!["randomness unavailable".into()])?;
            println!("{}", data_encoding::HEXLOWER.encode(&*key));
            Ok(0)
        }
        Command::Standing {
            command: StandingCommand::Hash,
        } => {
            let mut password = Zeroizing::new(String::new());
            if io::stdin()
                .lock()
                .read_line(&mut password)
                .map_err(|_| vec!["password input unavailable".into()])?
                == 0
            {
                return Err(vec!["password input unavailable".into()]);
            }
            if password.ends_with('\n') {
                password.pop();
                if password.ends_with('\r') {
                    password.pop();
                }
            }
            let mut salt = [0u8; 16];
            getrandom::fill(&mut salt).map_err(|_| vec!["randomness unavailable".into()])?;
            println!(
                "{}",
                crate::pgwire::auth::ScramVerifier::from_password(&password, &salt, 4096)
                    .to_pg_string()
            );
            Ok(0)
        }
        Command::Serve { config } => {
            let cfg = Config::load(&config)?;
            tokio::runtime::Runtime::new()
                .map_err(|_| vec!["runtime unavailable".into()])?
                .block_on(crate::serve::run(cfg))
                .map_err(|e| vec![e])?;
            Ok(0)
        }
        Command::Policy {
            command: PolicyCommand::Check { config },
        } => {
            runtime_validate(&config)?;
            println!("policy OK");
            Ok(0)
        }
    }
}
/// Run the operator CLI and return its exit code.
pub fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let code = i32::from(error.use_stderr());
            let _ = error.print();
            return code;
        }
    };
    match execute(cli.command) {
        Ok(code) => code,
        Err(errors) => {
            for error in errors {
                eprintln!("{error}");
            }
            1
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_commands() {
        for verb in ["show", "renew", "revoke", "env"] {
            assert!(
                Cli::try_parse_from([
                    "nunya",
                    "session",
                    verb,
                    "--server",
                    "http://127.0.0.1:8000",
                    "--context",
                    "work"
                ])
                .is_ok()
            );
        }
        let args = [
            "nunya",
            "session",
            "acquire",
            "--server",
            "http://127.0.0.1:8000",
            "--ca-file",
            "ca",
            "--principal",
            "alice",
            "--principal-secret-file",
            "secret",
            "--context",
            "work",
            "--route",
            "postgres",
            "--mode",
            "read-write",
            "--ttl",
            "1d",
            "--label",
            "label",
            "--isolated",
            "--new-scope",
            "--pg-host",
            "localhost",
            "--pg-port",
            "6543",
        ];
        assert!(Cli::try_parse_from(args).is_ok());
        assert!(
            Cli::try_parse_from([
                "nunya",
                "session",
                "renew",
                "--server",
                "unix:/tmp/nunya.sock",
                "--mode",
                "read-only"
            ])
            .is_ok()
        );
        for format in ["sh", "powershell"] {
            assert!(
                Cli::try_parse_from([
                    "nunya",
                    "session",
                    "env",
                    "--server",
                    "unix:/tmp/nunya.sock",
                    "--format",
                    format
                ])
                .is_ok()
            );
        }
        let cli = Cli::try_parse_from([
            "nunya",
            "exec",
            "--server",
            "unix:/tmp/nunya.sock",
            "--new-scope",
            "--",
            "psql",
            "-c",
            "select 1",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Exec(_)));
        assert!(
            Cli::try_parse_from([
                "nunya",
                "exec",
                "--server",
                "unix:/tmp/nunya.sock",
                "--isolated",
                "--",
                "psql"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "nunya",
                "session",
                "acquire",
                "--server",
                "unix:/tmp/nunya.sock",
                "--principal-secret",
                "secret"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "nunya",
                "session",
                "acquire",
                "--server",
                "unix:/tmp/nunya.sock",
                "--ttl",
                "0s"
            ])
            .is_err()
        );
    }
    #[test]
    fn rotation_flags() {
        for verb in ["acquire", "renew"] {
            let args = [
                "nunya",
                "session",
                verb,
                "--server",
                "unix:/tmp/nunya.sock",
                "--rotate",
            ];
            assert!(Cli::try_parse_from(args).is_ok());
            for conflict in ["--new-scope", "--isolated"] {
                assert!(Cli::try_parse_from(args.into_iter().chain([conflict])).is_err());
            }
        }
    }
    #[test]
    fn durations() {
        for (text, ms) in [("30m", 1_800_000), ("2h", 7_200_000), ("3600s", 3_600_000)] {
            assert_eq!(parse_ttl(text).unwrap(), ms);
        }
        for text in ["", "0s", "1d", "-2h", "1.5h", "18446744073709551615h", "é"] {
            assert!(parse_ttl(text).is_err());
        }
    }
    #[test]
    fn commands() {
        for args in [
            vec!["nunya", "version"],
            vec!["nunya", "serve", "--config", "x"],
            vec!["nunya", "policy", "check", "--config", "x"],
            vec!["nunya", "task", "open", "--config", "x", "--ttl", "30m"],
            vec!["nunya", "task", "close", "--config", "x", "abc"],
            vec!["nunya", "task", "list", "--config", "x"],
            vec!["nunya", "standing", "hash"],
            vec!["nunya", "key", "generate"],
        ] {
            assert!(Cli::try_parse_from(args).is_ok());
        }
        assert!(
            Cli::try_parse_from(["nunya", "task", "open", "--config", "x", "--ttl", "0s"]).is_err()
        );
    }
}
