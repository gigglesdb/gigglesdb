use std::path::Path;
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 || args[3].parse::<u64>().is_err() {
        eprintln!("usage: giggles-range-history NODE_BINARY ARTIFACT_PARENT SEED");
        std::process::exit(64);
    }
    match giggles_range_history::run(
        Path::new(&args[1]),
        Path::new(&args[2]),
        args[3].parse().unwrap(),
    ) {
        Ok(r) => {
            println!("{}", serde_json::to_string(&r).unwrap());
            std::process::exit(match r.verdict {
                giggles_history::Verdict::Pass
                    if r.controls
                        .values()
                        .all(|v| *v == giggles_history::Verdict::Fail)
                        && r.runtime_checks.values().all(|v| *v) =>
                {
                    0
                }
                giggles_history::Verdict::Fail => 1,
                _ => 2,
            });
        }
        Err(e) => {
            println!(
                "{}",
                serde_json::json!({"verdict":"INCONCLUSIVE","error":e.to_string()})
            );
            std::process::exit(2);
        }
    }
}
