use giggles_ledger::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::{self, Read};

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Create {
        path: String,
        definition: Definition,
    },
    Account {
        path: String,
        definition: AccountDefinition,
    },
    GetAccount {
        path: String,
        id: String,
    },
    Execute {
        path: String,
        intent: Intent,
    },
    AtomicBatch {
        path: String,
        intents: Vec<Intent>,
    },
    IndividualBatch {
        path: String,
        intents: Vec<Intent>,
    },
    Status {
        path: String,
        id: String,
    },
    Position {
        path: String,
        id: String,
    },
    Reconcile {
        path: String,
    },
    Export {
        path: String,
        destination: String,
    },
    Restore {
        source: String,
        path: String,
    },
    /// Explicit trusted test-only command, not an implicit environment switch.
    Inject {
        path: String,
        intent: Intent,
        fault: Fault,
    },
}
fn value<T: serde::Serialize>(v: T) -> Result<Value> {
    serde_json::to_value(v).map_err(|e| Error::Invalid(e.to_string()))
}
fn run(c: Command) -> Result<Value> {
    match c {
        Command::Create { path, definition } => {
            value(Ledger::create(path, definition)?.definition()?)
        }
        Command::Account { path, definition } => {
            Ledger::open(path)?.create_account(definition)?;
            Ok(json!(true))
        }
        Command::GetAccount { path, id } => value(Ledger::open(path)?.account(&id)?),
        Command::Execute { path, intent } => value(Ledger::open(path)?.execute(intent)?),
        Command::Inject {
            path,
            intent,
            fault,
        } => value(Ledger::open(path)?.execute_with_fault(intent, fault)?),
        Command::AtomicBatch { path, intents } => value(Ledger::open(path)?.atomic_batch(intents)?),
        Command::IndividualBatch { path, intents } => {
            value(Ledger::open(path)?.individual_batch(intents)?)
        }
        Command::Status { path, id } => value(Ledger::open(path)?.status(&id)?),
        Command::Position { path, id } => value(Ledger::open(path)?.position(&id)?),
        Command::Reconcile { path } => value(Ledger::open(path)?.reconcile()?),
        Command::Export { path, destination } => value(Ledger::open(path)?.export(destination)?),
        Command::Restore { source, path } => value(Ledger::restore(source, path)?.reconcile()?),
    }
}
fn main() {
    // Bounded stdin prevents allocation from an unbounded command stream.
    let mut input = String::new();
    let result = io::stdin()
        .take(128 * 1024 + 1)
        .read_to_string(&mut input)
        .map_err(|e| Error::Invalid(e.to_string()))
        .and({
            if input.len() > 128 * 1024 {
                Err(Error::Limit)
            } else {
                Ok(())
            }
        })
        .and_then(|_| {
            serde_json::from_str::<Command>(&input).map_err(|e| Error::Invalid(e.to_string()))
        })
        .and_then(run);
    match result {
        Ok(v) => println!("{}", json!({"ok":v})),
        Err(e) => {
            println!("{}", json!({"error":e}));
            std::process::exit(1)
        }
    }
}
