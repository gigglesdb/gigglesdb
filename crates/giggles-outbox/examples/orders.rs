use giggles_outbox::*;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("supply a fresh local database path")?;
    let o = Outbox::create(&path, Limits::default())?;
    let mut tx = o.begin()?;
    tx.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)",
        &[],
        None,
    )?;
    tx.execute(
        "INSERT INTO orders VALUES ($1,$2)",
        &[Value::Int(1), Value::Text("accepted".into())],
        None,
    )?;
    tx.enqueue(EnqueueIntent {
        stream: "shop",
        effect_id: "order-1-accepted",
        destination_kind: "notification",
        payload: b"order 1 accepted",
    })?;
    assert_eq!(tx.commit()?, CommitOutcome::Committed);
    drop(o);
    let o = Outbox::open(path)?;
    let rows = o.scan_application(Scan {
        table: "orders".into(),
        projection: vec![],
        prefix: vec![Value::Int(1)],
        lower: None,
        upper: None,
        reverse: false,
        limit: 1,
    })?;
    println!(
        "{}",
        serde_json::json!({"orders":rows,"enqueue":o.status("shop","order-1-accepted")?})
    );
    Ok(())
}
