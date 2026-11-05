//! Local owner JSON-line controller. No remote administrative listener.
use giggles_placement::{
    Controller, PROFILE, Plan, Request,
    owner::{ProcessConfig, StdioOwner},
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::{BufRead, Read, Write},
    path::PathBuf,
};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    processes: BTreeMap<u64, ProcessConfig>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    Plan { request: Box<Request> },
    Step { plan: Box<Plan>, journal: PathBuf },
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: giggles-placement ROSTER.json")?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((giggles_placement::MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    let c: Config = giggles_placement::decode(&bytes)?;
    let mut owner = StdioOwner::start(c.processes)?;
    println!("{}", json!({"ready":true,"profile":PROFILE}));
    let mut input = std::io::stdin().lock();
    loop {
        let mut line = Vec::new();
        if input
            .by_ref()
            .take((giggles_placement::MAX_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)?
            == 0
        {
            break;
        }
        let result = (|| -> giggles_placement::Result<serde_json::Value> {
            let mut envelope: serde_json::Map<String, serde_json::Value> =
                giggles_placement::decode(&line)?;
            let name = envelope
                .remove("command")
                .ok_or_else(|| giggles_placement::Error {
                    code: giggles_placement::Code::Transport,
                    message: "missing command".into(),
                })?;
            let name = name.as_str().ok_or_else(|| giggles_placement::Error {
                code: giggles_placement::Code::Transport,
                message: "invalid command".into(),
            })?;
            let c: Control = serde_json::from_value(json!({name:envelope})).map_err(|e| {
                giggles_placement::Error {
                    code: giggles_placement::Code::Transport,
                    message: e.to_string(),
                }
            })?;
            match c {
                Control::Plan { request } => {
                    serde_json::to_value(Plan::create(*request, &mut owner)?).map_err(|e| {
                        giggles_placement::Error {
                            code: giggles_placement::Code::Transport,
                            message: e.to_string(),
                        }
                    })
                }
                Control::Step { plan, journal } => {
                    serde_json::to_value(Controller::open(journal, *plan)?.step(&mut owner)?)
                        .map_err(|e| giggles_placement::Error {
                            code: giggles_placement::Code::Transport,
                            message: e.to_string(),
                        })
                }
            }
        })();
        let reply = match result {
            Ok(data) => json!({"ok":true,"data":data}),
            Err(e) => json!({"ok":false,"error":e}),
        };
        println!("{reply}");
        std::io::stdout().flush()?;
        if line.len() > giggles_placement::MAX_BYTES {
            break;
        }
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(2);
    }
}
