use giggles_core::{Admin, Limits};
use giggles_ops::*;
use std::{collections::BTreeMap, path::Path};
fn run() -> giggles_ops::Result<serde_json::Value> {
    let args: Vec<_> = std::env::args_os().skip(1).take(16).collect();
    if args.len() > 9 || args.is_empty() {
        return Err(OpsError::Invalid);
    }
    let command = args[0].to_str().ok_or(OpsError::Invalid)?;
    let allowed: &[&str] = match command {
        "backup" => &["--root", "--source", "--id"],
        "list" => &["--root"],
        "inspect" | "verify" => &["--root", "--id"],
        "restore" => &["--root", "--id", "--target"],
        _ => return Err(OpsError::Invalid),
    };
    if args.len() != 1 + 2 * allowed.len() {
        return Err(OpsError::Invalid);
    }
    let mut flags = BTreeMap::new();
    for pair in args[1..].as_chunks::<2>().0 {
        let key = pair[0].to_str().ok_or(OpsError::Invalid)?;
        if !allowed.contains(&key) || flags.insert(key, &pair[1]).is_some() {
            return Err(OpsError::Invalid);
        }
    }
    let get = |s: &str| flags.get(s).copied().ok_or(OpsError::Invalid);
    let c = Catalog::open(Path::new(get("--root")?), OpsLimits::default())?;
    let id = if command == "list" {
        None
    } else {
        Some(BackupId::new(
            get("--id")?.to_str().ok_or(OpsError::Invalid)?,
        )?)
    };
    let result = match command {
        "list" => serde_json::to_value(c.list()?),
        "backup" => {
            let source = Path::new(get("--source")?);
            // Core owns live files, but reject reparse roots/data files first.
            checked_source(source)?;
            let a = Admin::open(source, Limits::default())?;
            serde_json::to_value(c.backup(&a, id.as_ref().expect("command ID"))?)
        }
        "inspect" => serde_json::to_value(c.inspect(id.as_ref().expect("command ID"))?),
        "verify" => serde_json::to_value(c.verify(id.as_ref().expect("command ID"))?),
        "restore" => serde_json::to_value(c.restore(
            id.as_ref().expect("command ID"),
            Path::new(get("--target")?),
        )?),
        _ => unreachable!(),
    };
    result.map_err(|_| OpsError::Invalid)
}
fn checked_source(source: &Path) -> giggles_ops::Result<()> {
    giggles_ops::validate_source(source)
}
fn main() {
    match run() {
        Ok(v) => println!("{v}"),
        Err(e) => {
            eprintln!("{}", serde_json::json!({"error":e.code()}));
            std::process::exit(1)
        }
    }
}
