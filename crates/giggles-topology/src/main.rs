use giggles_topology::{MAX_BYTES, Report, Validated};
use serde::Serialize;
use std::fs::File;
use std::io::{self, Read, Write};
use std::process::ExitCode;

fn emit(value: &impl Serialize) -> io::Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer_pretty(&mut out, value)?;
    writeln!(out)
}

fn run() -> Result<u8, String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 || args.len() > 3 {
        return Err("usage: giggles-topology <validate|inspect|export|analyze> <topology.json> [scenario-id]; analyze without an id evaluates baseline, all single-domain losses and declared sets".into());
    }
    let command = args[0].as_str();
    if !["validate", "inspect", "export", "analyze"].contains(&command)
        || (command != "analyze" && args.len() != 2)
    {
        return Err("invalid command or arguments".into());
    }
    let mut input = Vec::new();
    File::open(&args[1])
        .map_err(|e| e.to_string())?
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut input)
        .map_err(|e| e.to_string())?;
    let validated = Validated::from_json(&input)
        .map_err(|e| serde_json::to_string(&e).expect("error serializes"))?;
    match command {
        "validate" => {
            emit(&serde_json::json!({"version":1,"valid":true,"disclaimer":giggles_topology::DISCLAIMER})).map_err(|e| e.to_string())?;
            Ok(0)
        }
        "inspect" | "export" => {
            emit(validated.topology()).map_err(|e| e.to_string())?;
            Ok(0)
        }
        "analyze" => {
            let mut report: Report = validated.analyze();
            if let Some(id) = args.get(2) {
                report.scenarios.retain(|s| &s.id == id);
                if report.scenarios.is_empty() {
                    return Err(format!("unknown scenario {id}"));
                }
            }
            let code = report.exit_code();
            emit(&report).map_err(|e| e.to_string())?;
            Ok(code)
        }
        _ => unreachable!(),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code),
        Err(reason) => {
            let _ = writeln!(
                io::stderr().lock(),
                "{}",
                serde_json::json!({"error": reason})
            );
            ExitCode::from(2)
        }
    }
}
