use clap::{Parser, Subcommand, ValueEnum};
use giggles_engine_bench::workload;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "GD-0054 F0 MVCC engine qualification harness")]
struct Cli {
    #[arg(long, value_enum)]
    engine: Engine,
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, global = true, default_value_t = 1024)]
    cache_mb: u64,
    #[arg(long, global = true, default_value_t = 300)]
    value_bytes: usize,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, global = true)]
    allow_any_dir: bool,
    #[command(subcommand)]
    phase: Phase,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Engine {
    Redb,
    Fjall,
    #[cfg(feature = "rocksdb")]
    Rocksdb,
}

#[derive(Subcommand, Debug)]
enum Phase {
    Load {
        #[arg(long, default_value_t = 1.0)]
        dataset_gb: f64,
    },
    Reopen,
    Run {
        #[arg(long, value_enum, default_value_t = Mix::Read)]
        mix: Mix,
        #[arg(long, default_value_t = 16)]
        threads: usize,
        #[arg(long, alias = "duration", default_value_t = 300)]
        duration_secs: u64,
        #[arg(long, alias = "warmup", default_value_t = 30)]
        warmup_secs: u64,
        #[arg(long, default_value_t = 64)]
        group_max: usize,
        #[arg(long, default_value_t = 500)]
        group_wait_us: u64,
        #[arg(long = "zipf", alias = "zipf-theta")]
        zipf_theta: Option<f64>,
        #[arg(long)]
        pin_snapshot_secs: Option<u64>,
    },
    Gc,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mix {
    Read,
    Write,
}

fn main() {
    #[cfg(not(feature = "rocksdb"))]
    {
        let args: Vec<_> = std::env::args_os().collect();
        if args.iter().any(|arg| arg == "--engine=rocksdb")
            || args
                .windows(2)
                .any(|pair| pair[0] == "--engine" && pair[1] == "rocksdb")
        {
            eprintln!("giggles-engine-bench: rocksdb requires building with --features rocksdb");
            std::process::exit(1);
        }
    }
    if let Err(error) = execute(Cli::parse()) {
        eprintln!("giggles-engine-bench: {error}");
        std::process::exit(1);
    }
}

fn execute(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = workload::Config {
        engine: match cli.engine {
            Engine::Redb => "redb",
            Engine::Fjall => "fjall",
            #[cfg(feature = "rocksdb")]
            Engine::Rocksdb => "rocksdb",
        }
        .into(),
        dir: cli.dir,
        cache_mb: cli.cache_mb,
        value_bytes: cli.value_bytes,
        out: cli.out,
        allow_any_dir: cli.allow_any_dir,
    };
    let phase = match cli.phase {
        Phase::Load { dataset_gb } => workload::Phase::Load { dataset_gb },
        Phase::Reopen => workload::Phase::Reopen,
        Phase::Gc => workload::Phase::Gc,
        Phase::Run {
            mix,
            threads,
            duration_secs,
            warmup_secs,
            group_max,
            group_wait_us,
            zipf_theta,
            pin_snapshot_secs,
        } => workload::Phase::Run {
            write_mix: matches!(mix, Mix::Write),
            threads,
            duration_secs,
            warmup_secs,
            group_max,
            group_wait_us,
            zipf_theta,
            pin_snapshot_secs,
        },
    };
    workload::execute(config, phase)
}
