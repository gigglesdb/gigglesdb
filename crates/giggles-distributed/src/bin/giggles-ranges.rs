//! Trusted owner stdin control. Peer listeners accept only authenticated Raft.
use giggles_distributed::{
    runtime::{Config, Runtime},
    *,
};
use serde::Deserialize;
use std::io::{BufRead, Read, Write};
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    TxPin {
        route: Descriptor,
    },
    TxStep {
        request: Box<transaction::Request>,
    },
    Transact {
        plan: transaction::Plan,
        outcome: giggles_transactions::Outcome,
    },
    TxStatus {
        home: Descriptor,
        id: giggles_transactions::TransactionId,
    },
    TxRecover {
        home: Descriptor,
        id: giggles_transactions::TransactionId,
        outcome: giggles_transactions::Outcome,
    },
    Inspect {
        group: u64,
    },
    ArmCrash {
        group: u64,
        after: bool,
    },
    Concurrent {
        group: u64,
        actions: Vec<Action>,
    },
    ObserveResume {
        bounds: Bounds,
        id: u64,
    },
    Transfer {
        group: u64,
        to: u64,
    },
    Block {
        group: u64,
        peers: std::collections::BTreeSet<u64>,
    },
    Current {
        group: u64,
    },
    Campaign {
        group: u64,
    },
    Checkpoint {
        group: u64,
    },
    Apply {
        group: u64,
        action: Box<Action>,
    },
    Resume {
        id: u64,
    },
    Put {
        key: giggles_core::Key,
        operation: u64,
        expected: Option<u64>,
        row: giggles_core::Row,
    },
    Observe {
        bounds: Bounds,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: giggles-ranges CONFIG.json")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let runtime = Runtime::start(config).await?;
    println!("{}", serde_json::json!({"ready":true}));
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    std::thread::spawn(move || {
        let mut input = std::io::stdin().lock();
        loop {
            let mut line = Vec::new();
            match input
                .by_ref()
                .take((MAX_COMMAND + 2) as u64)
                .read_until(b'\n', &mut line)
            {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if tx.blocking_send(line).is_err() {
                        break;
                    }
                }
            }
        }
    });
    while let Some(line) = rx.recv().await {
        let answer = async {
            if line.len() > MAX_COMMAND || line.last() != Some(&b'\n') {
                return Err("owner input length/termination".into());
            }
            // Serde's internally tagged content buffer rejects u128. Keep the
            // existing {command:...} wire grammar while decoding the body via
            // serde_json's arbitrary-precision Value deserializer instead.
            let mut envelope: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(&line)?;
            let name = envelope.remove("command").ok_or("missing owner command")?;
            let name = name.as_str().ok_or("invalid owner command")?;
            let control: Control = serde_json::from_value(serde_json::json!({name:envelope}))?;
            let value = match control {
                Control::TxPin { route } => serde_json::to_value(runtime.tx_pin(&route).await?)?,
                Control::TxStep { request } => {
                    serde_json::to_value(runtime.tx_step(*request).await?)?
                }
                Control::Transact { plan, outcome } => runtime.transact(plan, outcome).await?,
                Control::TxStatus { home, id } => runtime.tx_status(&home, id).await?,
                Control::TxRecover { home, id, outcome } => {
                    runtime.tx_recover(&home, id, outcome).await?
                }
                Control::Inspect { group } => {
                    serde_json::to_value(runtime.group(group)?.inspect().await?)?
                }
                Control::ArmCrash { group, after } => {
                    serde_json::to_value(runtime.group(group)?.arm_crash(after).await?)?
                }
                Control::Concurrent { group, actions } => {
                    serde_json::to_value(runtime.concurrent(group, actions).await?)?
                }
                Control::ObserveResume { bounds, id } => {
                    serde_json::to_value(runtime.observe_interleaved(bounds, Some(id)).await?)?
                }
                Control::Transfer { group, to } => {
                    serde_json::to_value(runtime.group(group)?.transfer(to).await?)?
                }
                Control::Block { group, peers } => {
                    serde_json::to_value(runtime.group(group)?.block(peers).await?)?
                }
                Control::Current { group } => {
                    serde_json::to_value(runtime.group(group)?.current().await?)?
                }
                Control::Campaign { group } => {
                    serde_json::to_value(runtime.group(group)?.campaign().await?)?
                }
                Control::Checkpoint { group } => {
                    serde_json::to_value(runtime.group(group)?.checkpoint().await?)?
                }
                Control::Apply { group, action } => {
                    serde_json::to_value(runtime.apply(group, *action).await?)?
                }
                Control::Resume { id } => serde_json::to_value(runtime.resume(id).await?)?,
                Control::Put {
                    key,
                    operation,
                    expected,
                    row,
                } => serde_json::to_value(runtime.put(key, operation, expected, row).await?)?,
                Control::Observe { bounds } => {
                    serde_json::to_value(runtime.observe(bounds).await?)?
                }
            };
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(value)
        }
        .await;
        let response = match answer {
            Ok(data) => serde_json::json!({"ok":true,"data":data}),
            Err(error) => serde_json::json!({"ok":false,"error":error.to_string()}),
        };
        let mut out = std::io::stdout().lock();
        serde_json::to_writer(&mut out, &response)?;
        writeln!(out)?;
        out.flush()?;
    }
    // Dedicated actors intentionally live until process exit; no retained state
    // is removed and shutdown does not reinterpret pending work as aborted.
    std::process::exit(0)
}
