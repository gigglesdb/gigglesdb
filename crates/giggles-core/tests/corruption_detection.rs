//! Deterministic single-bit corruption qualification through public APIs.
#![cfg(any(feature = "engine-rocksdb", feature = "legacy-task"))]
mod common;
#[cfg(feature = "legacy-task")]
use giggles_core::Time;

use giggles_core::{Admin, CoreError, Engine, Limits, Snapshot, relational::SqlResult};
use serde::Serialize;
use std::{fs, path::Path};

const QUERIES: [&str; 3] = [
    "SELECT * FROM items ORDER BY id",
    "SELECT * FROM labels ORDER BY id",
    "SELECT * FROM items WHERE label = 'payload-17' ORDER BY id",
];

#[derive(Default, Serialize)]
struct Counts {
    refused_at_open: usize,
    detected_by_verify: usize,
    harmless: usize,
}

fn reads(admin: &Admin) -> Result<Vec<SqlResult>, CoreError> {
    let mut sql = admin.sql_session();
    QUERIES.iter().map(|q| sql.execute(q, &[], None)).collect()
}

fn workload(path: &Path, engine: Engine) -> (Vec<SqlResult>, Option<Snapshot>) {
    let admin = Admin::create_engine(path, Limits::default(), engine).unwrap();
    let mut sql = admin.sql_session();
    for ddl in [
        "CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT)",
        "CREATE TABLE labels (id BIGINT PRIMARY KEY, label TEXT)",
        "CREATE INDEX items_label ON items (label)",
    ] {
        if matches!(engine, Engine::V2) && ddl.starts_with("CREATE INDEX") {
            continue; // v2 explicitly rejects secondary indexes.
        }
        sql.execute(ddl, &[], None).unwrap();
    }
    for id in 0..64 {
        for table in ["items", "labels"] {
            sql.execute(
                &format!("INSERT INTO {table} VALUES ({id}, 'payload-{id}')"),
                &[],
                None,
            )
            .unwrap();
        }
    }
    drop(sql);
    #[cfg(feature = "legacy-task")]
    let snapshot = if matches!(engine, Engine::Legacy) {
        admin.put(common::key(1), common::row()).unwrap();
        let executor = admin.grant(common::grant()).unwrap();
        executor
            .execute(common::request(71, &[1]), Time::Certified(110), None)
            .unwrap()
            .unwrap();
        let snapshot = admin.snapshot().unwrap();
        assert_eq!(
            snapshot.operations.len(),
            1,
            "fixture must contain a receipt"
        );
        Some(snapshot)
    } else {
        // The v2 public API rejects the legacy task/receipt and export APIs.
        None
    };
    #[cfg(not(feature = "legacy-task"))]
    let snapshot = None;
    (reads(&admin).unwrap(), snapshot)
}

fn copy_dir(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else if entry.file_name() != "giggles.lock" {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn corruption_error(error: &CoreError) {
    assert!(
        matches!(
            error,
            CoreError::Corrupt(_) | CoreError::Storage(_) | CoreError::Unsupported
        ),
        "unexpected error instead of corruption rejection: {error:?}"
    );
    if let CoreError::Storage(message) = error {
        assert!(!message.is_empty(), "storage error must explain rejection");
    }
}

fn qualify(engine: Engine) -> Counts {
    let root = common::fresh("corruption-detection");
    fs::create_dir_all(&root).unwrap();
    let source = root.join("source");
    let (expected, snapshot) = workload(&source, engine);
    #[cfg(feature = "engine-rocksdb")]
    if matches!(engine, Engine::V2) {
        // Small public-API workloads remain in the WAL. Materialize SSTs only
        // during fixture setup; every corruption observation uses Admin.
        let options = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&options, &source).unwrap();
        let db = rocksdb::DB::open_cf(&options, &source, &families).unwrap();
        for family in &families {
            db.flush_cf(db.cf_handle(family).unwrap()).unwrap();
        }
    }
    let mut files: Vec<_> = fs::read_dir(&source)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| match engine {
            Engine::Legacy => p.file_name().unwrap() == "data.redb",
            Engine::V2 => {
                p.extension().is_some_and(|e| e == "sst")
                    || p.file_name().unwrap() == "GIGGLES_FORMAT"
            }
        })
        .collect();
    files.sort();
    assert!(!files.is_empty());
    if matches!(engine, Engine::V2) {
        assert!(
            files
                .iter()
                .any(|p| p.extension().is_some_and(|e| e == "sst")),
            "v2 qualification must exercise SST checksums"
        );
    }
    let mut counts = Counts::default();
    for file in files {
        let original = fs::read(&file).unwrap();
        let offsets: std::collections::BTreeSet<_> =
            (0..64).map(|i| i * (original.len() - 1) / 63).collect();
        for offset in offsets {
            let candidate = root.join("candidate");
            copy_dir(&source, &candidate);
            let mut bytes = original.clone();
            bytes[offset] ^= 1 << (offset % 8);
            fs::write(candidate.join(file.file_name().unwrap()), bytes).unwrap();
            let context = format!(
                "engine={engine:?} file={} offset={offset} bit={} workload=64 rows in items/labels; legacy additionally has items_label index and receipt op=71",
                file.display(),
                offset % 8
            );
            eprintln!("corruption probe: {context}");
            save_report(engine, &counts);
            match Admin::open(&candidate, Limits::default()) {
                Err(error) => {
                    corruption_error(&error);
                    if matches!(engine, Engine::Legacy) && offset == 79_643 {
                        assert!(
                            matches!(error, CoreError::Corrupt(_)),
                            "{context}: {error:?}"
                        );
                        // The rejected open must release the file lock as well.
                        assert!(matches!(
                            Admin::open(&candidate, Limits::default()),
                            Err(CoreError::Corrupt(_))
                        ));
                    }
                    counts.refused_at_open += 1;
                }
                Ok(admin) => {
                    let actual = reads(&admin);
                    let verified = if snapshot.is_some() {
                        let export = candidate.join("verification.export");
                        admin
                            .export(&export)
                            .and_then(|_| Admin::verify_export(&export))
                            .map(Some)
                    } else {
                        // Opening validates v2 metadata; catalog + all row scans
                        // exercise checksummed reads through GigglesDB itself.
                        admin.catalog().map(|_| None)
                    };
                    match (&actual, &verified) {
                        (Err(error), _) | (_, Err(error)) => {
                            corruption_error(error);
                            counts.detected_by_verify += 1;
                        }
                        (Ok(rows), Ok(state)) => {
                            assert_eq!(*rows, expected, "SILENT CORRUPTION: {context}");
                            assert_eq!(*state, snapshot, "SILENT CORRUPTION: {context}");
                            counts.harmless += 1;
                        }
                    }
                    drop(admin);
                }
            }
            fs::remove_dir_all(candidate).unwrap();
            if matches!(engine, Engine::Legacy) {
                // Recovery in this same process must not retain a poisoned gate
                // or leaked redb/file lock from a failed open or read.
                let clean = Admin::open(&source, Limits::default()).unwrap();
                assert_eq!(reads(&clean).unwrap(), expected);
                assert_eq!(Some(clean.snapshot().unwrap()), snapshot);
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
    counts
}

fn save_report(engine: Engine, counts: &Counts) {
    let path = std::env::temp_dir().join("corruption_report.json");
    let mut report: serde_json::Value = fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let name = match engine {
        Engine::Legacy => "legacy",
        Engine::V2 => "v2",
    };
    report[name] = serde_json::to_value(counts).unwrap();
    fs::write(path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
}

static REPORT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
#[cfg(feature = "legacy-task")]
fn legacy_single_bit_corruption() {
    let _lock = REPORT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    save_report(Engine::Legacy, &qualify(Engine::Legacy));
}

#[test]
#[cfg(feature = "engine-rocksdb")]
fn v2_single_bit_corruption() {
    let _lock = REPORT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    save_report(Engine::V2, &qualify(Engine::V2));
}
