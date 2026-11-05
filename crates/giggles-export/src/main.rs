use giggles_core::{Admin, Limits};
use giggles_export::{Bounds, Error, Result};
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let report = match args.first().and_then(|s| s.to_str()) {
        Some("export") if args.len() == 3 => {
            let admin = Admin::open(&args[1], Limits::default())?;
            giggles_export::export(&admin, &args[2], Bounds::default())?
        }
        Some("import") if args.len() == 3 => {
            giggles_export::import(&args[1], &args[2], Bounds::default())?
        }
        Some("verify") if args.len() == 2 => giggles_export::verify(&args[1], Bounds::default())?,
        _ => return Err(Error::Invalid(
            "usage: giggles-export export SOURCE OUTPUT | import INPUT NEW_TARGET | verify INPUT"
                .into(),
        )),
    };
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|e| Error::Invalid(e.to_string()))?
    );
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1)
    }
}
