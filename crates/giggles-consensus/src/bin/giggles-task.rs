//! Small native mTLS client: giggles-task CLIENT-CONFIG.json REQUEST.json
#[tokio::main]
async fn main() -> giggles_consensus::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: giggles-task CLIENT-CONFIG.json REQUEST.json".into());
    }
    let config = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let request = giggles_consensus::native::parse(&std::fs::read(&args[1])?)?;
    let response = giggles_consensus::native::call(&config, &request).await?;
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
