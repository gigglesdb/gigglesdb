//! cargo run --example resolve -- CONFIG.json PERSISTED-NATIVE-REQUEST.json
//! Persist the execute request and its OperationId before calling this example.
use giggles_client::{Client, Config, Operation, Outcome};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let config: Config =
        serde_json::from_slice(&std::fs::read(args.next().ok_or("config path")?)?)?;
    let bytes = std::fs::read(args.next().ok_or("persisted request path")?)?;
    let operation = Operation::from_request(giggles_consensus::native::parse(&bytes)?)?;
    let client = Client::new(config)?;
    let outcome = client.send(&operation).await;
    if let Some(status) = outcome.resolution(2, 5000)? {
        // Status absence is not a rollback guarantee. Never invent another ID.
        match client.send(&status).await {
            Outcome::Recorded(r) => println!("resolved committed: {}", r.is_committed()),
            unresolved => println!("status remains unresolved: {unresolved:?}"),
        }
    } else {
        println!("{outcome:?}");
    }
    Ok(())
}
