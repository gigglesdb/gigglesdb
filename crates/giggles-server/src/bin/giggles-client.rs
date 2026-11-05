use giggles_server::{Client, Request, tls};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<_> = std::env::args().collect();
    if a.len() != 7 {
        return Err("usage: giggles-client ADDRESS SERVER_NAME CERT KEY CA REQUEST.json".into());
    }
    let config = tls::client(tls::certs(&a[3])?, tls::key(&a[4])?, tls::certs(&a[5])?)?;
    let bytes = std::fs::read(&a[6])?;
    if bytes.len() > giggles_server::MAX_FRAME {
        return Err("frame limit".into());
    }
    let request: Request = serde_json::from_slice(&bytes)?;
    let mut client = Client::connect(a[1].parse()?, &a[2], config).await?;
    println!("{}", serde_json::to_string(&client.call(&request).await?)?);
    Ok(())
}
