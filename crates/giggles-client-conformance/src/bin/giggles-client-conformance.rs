use giggles_client_conformance::{HarnessError, Scenario, run};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = execute().await {
        eprintln!("conformance: {error}");
        std::process::exit(2);
    }
}
async fn execute() -> Result<(), HarnessError> {
    let args: Vec<_> = std::env::args().skip(1).take(4).collect();
    if args.len() == 1 && args[0] == "--list" {
        for scenario in Scenario::ALL {
            println!("{}", scenario.name());
        }
        return Ok(());
    }
    if args.len() != 2 {
        return Err(HarnessError::Arguments);
    }
    let scenario = args[0].parse::<Scenario>()?;
    let seed = args[1]
        .parse::<u64>()
        .map_err(|_| HarnessError::Arguments)?;
    let report = run(scenario, seed).await?;
    let path = report.retain()?;
    println!("{}", path.display());
    report.validate()?;
    Ok(())
}
