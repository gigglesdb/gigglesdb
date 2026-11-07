use std::{path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand};
use skedaddle::{CapabilityReport, MigrationDefinition};

#[derive(Parser)]
#[command(
    version,
    about = "Inspect a Skedaddle migration definition (offline P1)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate strict JSON and print a capability report without connecting.
    Check { definition: PathBuf },
}

fn run(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    match cli.command {
        Command::Check { definition } => {
            let definition = MigrationDefinition::from_path(definition)?;
            let report = CapabilityReport::from_definition(&definition)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(report.blocking().is_empty())
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(error) => {
            eprintln!("skedaddle: {error}");
            ExitCode::FAILURE
        }
    }
}
