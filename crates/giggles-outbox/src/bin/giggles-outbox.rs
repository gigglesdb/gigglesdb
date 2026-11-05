//! Trusted local JSON commands. This binary never sends external effects.
use giggles_outbox::*;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::{
    io::{Read, Write},
    path::PathBuf,
};

const MAX_INPUT: u64 = 64 * 1024;
#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Create {},
    Enqueue {
        stream: String,
        effect_id: String,
        destination_kind: String,
        payload: String,
        #[serde(default)]
        application: Vec<Statement>,
    },
    Status {
        stream: String,
        effect_id: String,
    },
    Claim {
        stream: String,
        owner: String,
        now: TrustedTime,
        ttl: u64,
        limit: usize,
    },
    Ack {
        token: ClaimToken,
        now: TrustedTime,
    },
    Reconcile {},
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Statement {
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
    expected_version: Option<u64>,
}
fn run() -> Result<Json, Error> {
    let args: Vec<_> = std::env::args_os().take(5).collect();
    if !(args.len() == 2 || args.len() == 3) || args.iter().any(|a| a.len() > 4096) {
        return Err(Error::Invalid);
    }
    let path = PathBuf::from(&args[1]);
    let mut bytes = vec![];
    if args.len() == 3 && args[2] != "-" {
        let f = std::fs::File::open(&args[2]).map_err(|_| Error::Invalid)?;
        f.take(MAX_INPUT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Invalid)?;
    } else {
        std::io::stdin()
            .take(MAX_INPUT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Invalid)?;
    }
    if bytes.len() > MAX_INPUT as usize {
        return Err(Error::Limit);
    }
    let req: Request = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
    if matches!(req, Request::Create {}) {
        Outbox::create(path, Limits::default())?;
        return Ok(json!({"created":true}));
    }
    let o = Outbox::open(path)?;
    match req {
        Request::Create {} => unreachable!(),
        Request::Enqueue {
            stream,
            effect_id,
            destination_kind,
            payload,
            application,
        } => {
            if application.len() > 64 {
                return Err(Error::Limit);
            }
            let mut t = o.begin()?;
            for s in application {
                t.execute(&s.sql, &s.params, s.expected_version)?;
            }
            let receipt = t.enqueue(EnqueueIntent {
                stream: &stream,
                effect_id: &effect_id,
                destination_kind: &destination_kind,
                payload: payload.as_bytes(),
            })?;
            let outcome = t.commit()?;
            Ok(json!({"enqueue_receipt":receipt,"commit":outcome}))
        }
        Request::Status { stream, effect_id } => {
            Ok(json!({"status":o.status(&stream,&effect_id)?}))
        }
        Request::Claim {
            stream,
            owner,
            now,
            ttl,
            limit,
        } => Ok(json!({"claim":o.claim(&stream,&owner,now,ttl,limit)?})),
        Request::Ack { token, now } => Ok(json!({"ack":o.ack(&token,now)?})),
        Request::Reconcile {} => Ok(json!({"reconciliation":o.reconcile()?})),
    }
}
fn main() {
    let (output, code) = match run() {
        Ok(v) => (json!({"ok":v}), 0),
        Err(e) => (json!({"error":e.to_string()}), 2),
    };
    // JSON escaping bounds control characters; errors contain only typed codes.
    if writeln!(std::io::stdout().lock(), "{output}").is_err() {
        std::process::exit(3);
    }
    std::process::exit(code);
}
