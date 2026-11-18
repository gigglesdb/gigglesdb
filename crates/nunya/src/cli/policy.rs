//! Policy administration with value-free human output.
use super::session::ConnectionOpts;
use clap::Subcommand;
use std::path::PathBuf;
/// File checking and management API commands.
#[derive(Subcommand)]
pub enum PolicyCommand {
    /// Check the serving configuration as before.
    Check { #[arg(long)] config: PathBuf },
    /// Status route policy.
    Status {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
    },
    /// Effective route policy.
    Effective {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        #[arg(long)] json: bool,
    },
    /// Validate route policy.
    Validate {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        file: PathBuf,
    },
    /// Preview route policy.
    Preview {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        file: PathBuf,
        #[arg(long)] json: bool,
    },
    /// Apply route policy.
    Apply {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        file: PathBuf,
        #[arg(long)] expected_revision: u64,
        #[arg(long)] catalog_fingerprint: String,
        #[arg(long)] idempotency_key: Option<String>,
    },
    /// Rollback route policy.
    Rollback {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        #[arg(long)] expected_revision: u64,
        #[arg(long)] to: u64,
        #[arg(long)] idempotency_key: Option<String>,
    },
    /// Import route policy.
    Import {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        #[arg(long)] idempotency_key: Option<String>,
    },
    /// Revisions route policy.
    Revisions {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
    },
    /// Revision route policy.
    Revision {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        n: u64,
    },
    /// Export route policy.
    Export {
        route: String,
        #[command(flatten)] connection: ConnectionOpts,
        #[arg(short, long)] output: Option<PathBuf>,
    },
}
fn encode(value: impl serde::Serialize) -> Result<serde_json::Value, String> {
    serde_json::to_value(value).map_err(|_| "management response invalid".into())
}
fn draft(path: &std::path::Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|_| "policy file unavailable".into())
}
async fn execute(command: PolicyCommand) -> Result<(), String> {
    use PolicyCommand::*;
    let (value, json) = match command {
        Check { config } => {
            super::validate(&config).await.map_err(|_| "policy validation failed")?;
            println!("policy OK"); return Ok(());
        }
        Status { route, connection } => (encode(connection.client().map_err(|e| e.to_string())?.policy_status(&route).await.map_err(|e| e.to_string())?)?, false),
        Effective { route, connection, json } => (encode(connection.client().map_err(|e| e.to_string())?.policy_effective(&route).await.map_err(|e| e.to_string())?)?, json),
        Validate { route, connection, file } => (encode(connection.client().map_err(|e| e.to_string())?.policy_validate(&route, &draft(&file)?).await.map_err(|e| e.to_string())?)?, false),
        Preview { route, connection, file, json } => (encode(connection.client().map_err(|e| e.to_string())?.policy_preview(&route, &draft(&file)?).await.map_err(|e| e.to_string())?)?, json),
        Revisions { route, connection } => (encode(connection.client().map_err(|e| e.to_string())?.policy_revisions(&route).await.map_err(|e| e.to_string())?)?, false),
        Revision { route, connection, n } => (encode(connection.client().map_err(|e| e.to_string())?.policy_revision(&route, n).await.map_err(|e| e.to_string())?)?, false),
        Apply { route, connection, file, expected_revision, catalog_fingerprint, idempotency_key } => {
            let client = connection.client().map_err(|e| e.to_string())?;
            let fingerprint = catalog_fingerprint;
            let (result, key) = client.policy_apply(&route, &draft(&file)?, expected_revision, &fingerprint, idempotency_key).await.map_err(|e| e.to_string())?;
            println!("revision {} replayed={} idempotency-key={key}", result.revision.number, result.replayed); return Ok(());
        }
        Rollback { route, connection, to, expected_revision, idempotency_key } => {
            let (result, key) = connection.client().map_err(|e| e.to_string())?.policy_rollback(&route, to, expected_revision, idempotency_key).await.map_err(|e| e.to_string())?;
            println!("revision {} replayed={} idempotency-key={key}", result.revision.number, result.replayed); return Ok(());
        }
        Import { route, connection, idempotency_key } => {
            let (result, key) = connection.client().map_err(|e| e.to_string())?.policy_import(&route, idempotency_key).await.map_err(|e| e.to_string())?;
            println!("revision {} replayed={} idempotency-key={key}", result.revision.number, result.replayed); return Ok(());
        }
        Export { route, connection, output } => {
            let result = connection.client().map_err(|e| e.to_string())?.policy_export(&route).await.map_err(|e| e.to_string())?;
            match output { Some(path) => std::fs::write(path, result.policy_toml).map_err(|_| "policy output unavailable")?, None => print!("{}", result.policy_toml) }
            return Ok(());
        }
    };
    if json { println!("{value}"); }
    else if let Some(owner) = value.get("owner") { println!("owner={owner} active-revision={} catalog-fingerprint={}", value["active_revision"]["number"], value["catalog_fingerprint"]); }
    else if let Some(summary) = value.get("summary") { println!("{summary} catalog-fingerprint={}", value["catalog_fingerprint"]); }
    else if let Some(columns) = value.get("columns").and_then(|c| c.as_array()) { for column in columns { println!("{}.{}.{} treatment={} origin={}", column["schema"], column["table"], column["column"], column["treatment"], column["origin"]); } }
    else if let Some(revisions) = value.as_array() { for r in revisions { println!("revision {}", r["number"]); } }
    else if let Some(number) = value.get("number") { println!("revision {number}"); }
    else { println!("policy valid"); }
    Ok(())
}
/// Execute an operator policy command and return its exit status.
pub fn run_command(command: PolicyCommand) -> i32 {
    let result = tokio::runtime::Runtime::new().map_err(|_| "runtime unavailable".to_string())
        .and_then(|rt| rt.block_on(execute(command)));
    match result { Ok(()) => 0, Err(error) => { eprintln!("{error}"); 1 } }
}
#[cfg(test)]
mod tests {
    use clap::Parser;
    #[test]
    fn policy_commands_parse() {
        assert!(super::super::Cli::try_parse_from(["nunya", "policy", "apply", "pg", "p.toml", "--expected-revision", "1", "--server", "http://127.0.0.1:8000"]).is_err());
        for args in [vec!["status","pg"],vec!["effective","pg","--json"],vec!["validate","pg","p.toml"],vec!["preview","pg","p.toml","--json"],vec!["apply","pg","p.toml","--expected-revision","1","--catalog-fingerprint","catalog"],vec!["rollback","pg","--to","1","--expected-revision","2"],vec!["import","pg"],vec!["revisions","pg"],vec!["revision","pg","1"],vec!["export","pg","-o","p.toml"]] {
            assert!(super::super::Cli::try_parse_from(["nunya","policy"].into_iter().chain(args).chain(["--server","http://127.0.0.1:8000"])).is_ok());
        }
        assert!(super::super::Cli::try_parse_from(["nunya","policy","check","--config","c.toml"]).is_ok());
    }
}
