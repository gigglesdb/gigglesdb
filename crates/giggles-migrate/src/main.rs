use giggles_migrate::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{fs::File, io::Read, path::Path};

fn read<T: DeserializeOwned>(path: &str, max: usize) -> Result<T> {
    let f = File::open(path)?;
    let mut bytes = vec![];
    f.take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(Error("input file byte bound".into()));
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn emit(v: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}
fn chunks(path: &str, m: &Manifest) -> Result<(Vec<Chunk>, State)> {
    let chunks: Vec<Chunk> = read(path, 48 * 1024)?;
    let mut state = State::default();
    let mut ids = std::collections::BTreeMap::new();
    if chunks.len() > 256 {
        return Err(Error("chunk count bound".into()));
    }
    for c in &chunks {
        if c.id.is_empty()
            || c.events.len() > m.bounds.max_events
            || serde_json::to_vec(c)?.len() > m.bounds.max_chunk_bytes
        {
            return Err(Error("chunk identity/bound".into()));
        }
        let hash = chunk_digest(c)?;
        if let Some(old) = ids.insert(c.id.clone(), hash.clone())
            && old != hash
        {
            return Err(Error("permanent chunk ID mismatch".into()));
        }
        state.apply(m, &c.events)?;
    }
    Ok((chunks, state))
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let usage = "usage: giggles-migrate manifest validate MANIFEST | reconcile MANIFEST CHUNKS | import MANIFEST CHUNKS DB | verify MANIFEST CHUNKS DB | status MANIFEST DB";
    if args.len() == 3 && args[0] == "manifest" && args[1] == "validate" {
        let m: Manifest = read(&args[2], 48 * 1024)?;
        m.validate()?;
        return emit(
            &serde_json::json!({"valid":true,"contract":"normalized-offline-v1","manifest_sha256":digest(&m)?}),
        );
    }
    match args.first().map(String::as_str) {
        Some("reconcile") if args.len() == 3 => {
            let m: Manifest = read(&args[1], 48 * 1024)?;
            m.validate()?;
            let (_, s) = chunks(&args[2], &m)?;
            emit(&s.rows(&m, m.horizon_micros)?)
        }
        Some("import") if args.len() == 4 => {
            let m: Manifest = read(&args[1], 48 * 1024)?;
            m.validate()?;
            let (cs, _) = chunks(&args[2], &m)?;
            let mut db = if Path::new(&args[3]).exists() {
                Importer::open(&args[3], m)?
            } else {
                Importer::create(&args[3], m)?
            };
            for c in &cs {
                db.import(c)?;
            }
            emit(&db.status()?)
        }
        Some("verify") if args.len() == 4 => {
            let m: Manifest = read(&args[1], 48 * 1024)?;
            m.validate()?;
            let (_, expected) = chunks(&args[2], &m)?;
            let mut db = Importer::open(&args[3], m)?;
            let report = db.verify(&expected, 64)?;
            emit(&report)?;
            if !report.equal {
                return Err(Error("destination comparison mismatch".into()));
            }
            Ok(())
        }
        Some("status") if args.len() == 3 => {
            let m: Manifest = read(&args[1], 48 * 1024)?;
            let db = Importer::open(&args[2], m)?;
            emit(&db.status()?)
        }
        _ => Err(Error(usage.into())),
    }
}
fn main() {
    if let Err(error) = run() {
        eprintln!(
            "{}",
            serde_json::json!({"error":error.to_string(),"scope":"normalized offline rehearsal"})
        );
        std::process::exit(1);
    }
}
