use giggles_faultlab::{Result, lab};
use std::path::Path;
#[tokio::main]
async fn main() {
    if let Err(e) = command().await {
        eprintln!("faultlab: {e}");
        std::process::exit(1);
    }
}
async fn command() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [help] if help == "help" || help == "--help" => println!(
            "giggles-faultlab run NODE_EXE [SAMPLES=2]\ngiggles-faultlab check REPORT.json\nLOCAL simulation only. Retains all files; terminates only owned children. Samples 1..=8."
        ),
        [cmd, node] if cmd == "run" => {
            println!("{}", lab::run(Path::new(node), 2).await?.display())
        }
        [cmd, node, samples] if cmd == "run" => println!(
            "{}",
            lab::run(Path::new(node), samples.parse()?).await?.display()
        ),
        [cmd, path] if cmd == "check" => {
            let r = lab::check(Path::new(path))?;
            println!(
                "valid LOCAL observation report: {} attempts",
                r.invocations.len()
            );
        }
        _ => {
            return Err(
                "usage: giggles-faultlab run NODE_EXE [SAMPLES] | check REPORT | help".into(),
            );
        }
    }
    Ok(())
}
