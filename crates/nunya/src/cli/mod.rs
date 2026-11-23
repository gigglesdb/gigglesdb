//! Operator commands.
/// Principal administration and connection inspection.
pub mod principal;
/// Policy management commands.
pub mod policy;
/// Session helper and command implementation.
pub mod session;
use crate::config::Config;
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
        command: policy::PolicyCommand,
    },
    Principal {
        #[command(subcommand)]
        command: principal::PrincipalCommand,
    },
    Connections {
        #[command(subcommand)]
        command: principal::ConnectionsCommand,
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
enum StandingCommand {
    Hash,
}
#[derive(Subcommand)]
enum KeyCommand {
    Generate,
}
fn load_policy(path: &Path, service: &crate::policy::ServicePolicySettings) -> Result<crate::policy::Policy, Vec<String>> {
    let bytes = std::fs::read(path).map_err(|_| vec!["policy file unavailable".into()])?;
    crate::serve::parse_policy_bytes(path, &bytes, service).map(|doc| doc.policy).map_err(|errors| {
        errors
            .into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    })
}
async fn validate(path: &Path) -> Result<(), Vec<String>> {
    let cfg = Config::load(path)?;
    if let Some(tls) = &cfg.tls {
        crate::config::load_tls(tls).map_err(|e| vec![e])?;
    }
    let postgres_policy = cfg.postgres_policy.as_deref().map(|path| load_policy(path, &cfg.policy_settings)).transpose()?;
    let spanner_policy = cfg.spanner_policy.as_deref().map(|path| load_policy(path, &cfg.policy_settings)).transpose()?;
    if let (Some(pg), Some(spanner)) = (&postgres_policy, &spanner_policy)
        && crate::serve::readlog_key(&pg.readlog) != crate::serve::readlog_key(&spanner.readlog)
    {
        return Err(vec!["policy files name different read logs".into()]);
    }
    let policy = postgres_policy
        .as_ref()
        .or(spanner_policy.as_ref())
        .ok_or_else(|| vec!["route policy unavailable".into()])?;
    let _sink = crate::readlog::open_sink(&policy.readlog)
        .map_err(|_| vec!["read log unavailable".into()])?;
    if let Some(upstream) = cfg.upstream_config() {
        let policy = postgres_policy
            .as_ref()
            .ok_or_else(|| vec!["route policy unavailable".into()])?;
        let schema = crate::serve::connect_schema(&upstream)
            .await
            .map_err(|e| vec![e])?;
        let snapshot = crate::serve::snapshot(&schema.client, &cfg)
            .await
            .map_err(|e| vec![e])?;
        crate::policy::validate::validate(policy, &snapshot).map_err(|errors| {
            errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
        })?;
    }
    if let Some(section) = &cfg.spanner_native {
        let policy = spanner_policy
            .as_ref()
            .ok_or_else(|| vec!["route policy unavailable".into()])?;
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
        crate::policy::validate::validate(policy, &snapshot).map_err(|errors| {
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
        Command::Principal { command } => Ok(principal::run_command(command)),
        Command::Connections { command } => Ok(principal::run_connections(command)),
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
        Command::Policy { command: policy::PolicyCommand::Check { config } } => {
            runtime_validate(&config)?;
            println!("policy OK");
            Ok(0)
        }
        Command::Policy { command } => Ok(policy::run_command(command)),
    }
}
/// Run the operator CLI and return its exit code.
pub fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let code = i32::from(error.use_stderr());
            if error.use_stderr() {
                eprintln!("invalid command arguments");
            } else {
                let _ = error.print();
            }
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
    fn policy_check_uses_serving_path_resolution() {
        let dir = std::env::temp_dir().join(format!(
            "readlog-key-{}",
            crate::token::SessionId::random().to_hex()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.as_path().join("policy.toml");
        std::fs::write(&path, "[readlog]\nsink='file'\npath='./read.log'\n").unwrap();
        let checked = load_policy(&path, &crate::policy::ServicePolicySettings::default()).unwrap();
        let served =
            crate::serve::parse_policy_bytes(&path, &std::fs::read(&path).unwrap(), &crate::policy::ServicePolicySettings::default()).unwrap();
        assert_eq!(checked.readlog, served.policy.readlog);
        let absolute = crate::policy::ReadLogSink::File {
            path: dir.as_path().join("read.log"),
            max_bytes: 104857600,
            max_files: 10,
        };
        assert_eq!(
            crate::serve::readlog_key(&checked.readlog),
            crate::serve::readlog_key(&absolute)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
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
    fn principal_commands() {
        let verifier =
            crate::pgwire::auth::ScramVerifier::from_password("test", b"salt", 4096).to_pg_string();
        let valid = vec![
            vec!["create", "alice", "--kind", "person"],
            vec![
                "create",
                "alice",
                "--kind",
                "agent",
                "--admin",
                "--generate-secret",
            ],
            vec![
                "create",
                "alice",
                "--kind",
                "service",
                "--verifier",
                &verifier,
            ],
            vec!["list"],
            vec!["show", "alice"],
            vec!["disable", "alice"],
            vec!["enable", "alice"],
            vec![
                "credential",
                "add",
                "alice",
                "--generate-secret",
                "--expires",
                "1h",
            ],
            vec!["credential", "add", "alice", "--verifier", &verifier],
            vec![
                "credential",
                "delete",
                "alice",
                "0123456789abcdef0123456789abcdef",
            ],
            vec![
                "grant",
                "alice",
                "--route",
                "postgres=read-write",
                "--route",
                "other=read-only",
            ],
            vec!["grant", "alice", "--none"],
        ];
        for args in valid {
            assert!(
                Cli::try_parse_from(["nunya", "principal"].into_iter().chain(args).chain([
                    "--server",
                    "http://127.0.0.1:8000",
                    "--ca-file",
                    "ca",
                    "--principal",
                    "admin",
                    "--principal-secret-file",
                    "secret",
                    "--context",
                    "admin",
                ]))
                .is_ok()
            );
        }
        for args in [
            vec![
                "create",
                "alice",
                "--kind",
                "person",
                "--verifier",
                &verifier,
                "--generate-secret",
            ],
            vec![
                "credential",
                "add",
                "alice",
                "--verifier",
                &verifier,
                "--generate-secret",
            ],
            vec!["grant", "alice"],
            vec!["grant", "alice", "--none", "--route", "postgres=read-only"],
        ] {
            assert!(
                Cli::try_parse_from(
                    ["nunya", "principal"]
                        .into_iter()
                        .chain(args)
                        .chain(["--server", "http://127.0.0.1:8000"])
                )
                .is_err()
            );
        }
        assert!(
            Cli::try_parse_from([
                "nunya",
                "connections",
                "list",
                "--server",
                "http://127.0.0.1:8000"
            ])
            .is_ok()
        );
    }
    #[test]
    fn commands() {
        for args in [
            vec!["nunya", "version"],
            vec!["nunya", "serve", "--config", "x"],
            vec!["nunya", "policy", "check", "--config", "x"],
            vec!["nunya", "standing", "hash"],
            vec!["nunya", "key", "generate"],
        ] {
            assert!(Cli::try_parse_from(args).is_ok());
        }
        for removed in [
            vec!["nunya", "task", "open", "--config", "x", "--ttl", "30m"],
            vec!["nunya", "task", "close", "--config", "x", "abc"],
            vec!["nunya", "task", "list", "--config", "x"],
        ] {
            assert!(Cli::try_parse_from(removed).is_err());
        }
    }
}
