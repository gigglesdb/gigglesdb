//! Demonstration client. Only consensus native Preview/Execute/Status are accepted.
use giggles_client::{Client, Config, Operation, Outcome};
use std::{io::Read, path::Path};
fn bounded(path: &Path, max: usize) -> Result<Vec<u8>, ()> {
    let mut b = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| ())?
        .take(max as u64 + 1)
        .read_to_end(&mut b)
        .map_err(|_| ())?;
    if b.len() > max { Err(()) } else { Ok(b) }
}
#[tokio::main]
async fn main() {
    let code = run().await;
    if code != 0 {
        std::process::exit(code);
    }
}
async fn run() -> i32 {
    let args = std::env::args_os().skip(1).take(3).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("usage: giggles-client CONFIG.json PERSISTED-NATIVE-REQUEST.json");
        return 2;
    }
    let parsed = (|| {
        let config: Config =
            serde_json::from_slice(&bounded(Path::new(&args[0]), 64 * 1024)?).map_err(|_| ())?;
        let request = giggles_consensus::native::parse(&bounded(
            Path::new(&args[1]),
            giggles_consensus::native::MAX_FRAME,
        )?)
        .map_err(|_| ())?;
        let op = Operation::from_request(request).map_err(|_| ())?;
        let client = Client::new(config).map_err(|_| ())?;
        Ok::<_, ()>((client, op))
    })();
    let Ok((client, op)) = parsed else {
        eprintln!("invalid client configuration, credentials, or native task request");
        return 2;
    };
    match client.send(&op).await {
        Outcome::Recorded(r) => {
            match serde_json::to_string(r.reply()) {
                Ok(s) => println!("{s}"),
                Err(_) => return 3,
            };
            0
        }
        Outcome::Unknown {
            operation_id,
            reason,
        } => {
            eprintln!(
                "unknown operation {}; resolve status with this ID ({reason:?})",
                operation_id.0
            );
            3
        }
        other => {
            eprintln!("{other:?}");
            4
        }
    }
}
