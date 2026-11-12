//! Explicit sizing experiment against the real, pinned-core Local projection.
//! No source rows are stored here: only coordinator metadata is generated.
use giggles_core::{
    Admin, Limits,
    relational::{SqlRow, Value, encode_key},
};
use skedaddle::coordinator::{
    Chunk, EffectKind, Event, Materialized, State, Transition, journal::Entry, local::Local,
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const TABLES: &[&str] = &[
    "definition",
    "lifecycle",
    "journal_applied",
    "progress",
    "chunks",
    "audit",
    "fence_state",
    "verifier_ranges",
    "verifier_exceptions",
];
const INSTANCE: &str = "gd49-d2-sizing-coordinator";
const CAP: u64 = 16 * 1024 * 1024;

fn root(name: &str) -> PathBuf {
    let root = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT")
            .expect("set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
    );
    assert!(root.starts_with("J:/gigglesdb-work"));
    let root = root.join(format!(
        "{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

// Fixed-int bincode sizes of public SqlRow/Value, and its public canonical PK.
// Includes the inner row-map's key length, row version and vector length.
// Catalog/outer maps/metadata are measured separately in the empty export.
fn stored_row_bytes(row: &SqlRow) -> u64 {
    let values: usize = row
        .values
        .iter()
        .map(|v| match v {
            Value::Null => 4,
            Value::Bool(_) => 5,
            Value::Int(_) => 12,
            Value::Text(s) => 12 + s.len(),
            Value::Bytes(b) => 12 + b.len(),
        })
        .sum();
    (24 + encode_key(&row.values[..1]).unwrap().len() + values) as u64
}

fn table_sizes(local: &Local, page: usize) -> BTreeMap<String, serde_json::Value> {
    TABLES.iter().map(|name| {
        let rows = local.read_collection(name, page).unwrap();
        let stored: u64 = rows.iter().map(stored_row_bytes).sum();
        let payload: usize = rows.iter().flat_map(|r| &r.values).map(|v| match v {
            Value::Text(s) => s.len(), Value::Bytes(b) => b.len(), _ => 9,
        }).sum();
        ((*name).into(), serde_json::json!({"rows": rows.len(), "row_map_bytes": stored, "charged_payload_bytes": payload}))
    }).collect()
}

fn row_total(tables: &BTreeMap<String, serde_json::Value>) -> u64 {
    tables
        .values()
        .map(|v| v["row_map_bytes"].as_u64().unwrap())
        .sum()
}

fn export_bytes(db: &Path, export: &Path) -> u64 {
    let admin = Admin::open(db, Limits::default()).unwrap();
    admin.export(export).unwrap();
    let bytes = fs::read(export).unwrap();
    assert_eq!(&bytes[..8], b"GDEXP001");
    let payload = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    assert_eq!(bytes.len() as u64, payload + 52);
    payload
}

fn apply(local: &mut Local, transition: Transition) -> skedaddle::coordinator::Result<()> {
    let entry = Entry::new(local.state(), transition, INSTANCE)?;
    local.apply(&entry)
}

fn definition() -> skedaddle::MigrationDefinition {
    let readme = include_str!("../README.md");
    let json = readme
        .split("```json")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    serde_json::from_str(json).unwrap()
}

fn chunk_transitions(n: u64, manifest_items: usize) -> Vec<Transition> {
    let table = format!("public.orders_{:03}", n % 100);
    let lo = format!("[\"tenant-{:04}\",\"{:012}\"]", n % 100, n * 100);
    let hi = format!("[\"tenant-{:04}\",\"{:012}\"]", n % 100, (n + 1) * 100);
    let low = format!("0/{:08X}", n * 4096);
    let high = format!("0/{:08X}", (n + 1) * 4096);
    let chunk = |state: &str| Transition::Chunk {
        chunk: Chunk {
            table: table.clone(),
            range_lo: lo.clone(),
            range_hi: hi.clone(),
            state: state.into(),
            low_wm: (state == "done").then(|| low.clone()),
            high_wm: (state == "done").then(|| high.clone()),
        },
    };
    let payload = serde_json::json!({
        "object": format!("buffer/migration/forward/{n:020}.manifest"),
        "sha256": "a".repeat(64), "table": table, "lo": lo, "hi": hi, "position": high,
        // Optional wider publication manifest, still metadata, never source rows.
        "segments": (0..manifest_items).map(|i| serde_json::json!({
            "object": format!("buffer/migration/forward/{n:020}/{i:020}.segment"),
            "sha256": "b".repeat(64), "bytes": 65536, "rows": 128,
        })).collect::<Vec<_>>(),
    })
    .to_string();
    vec![
        chunk("started"),
        Transition::Intent {
            effect: EffectKind::BufferPublication,
            payload,
        },
        chunk("done"),
    ]
}

fn finish(
    root: &Path,
    local: Local,
    baseline: u64,
    base_rows: u64,
    failure: Option<serde_json::Value>,
    elapsed: f64,
    page: usize,
) {
    let tables = table_sizes(&local, page);
    let expected = baseline + row_total(&tables) - base_rows;
    let state_json_bytes = serde_json::to_vec(local.state()).unwrap().len();
    let seq = local.state().applied_seq;
    drop(local);
    let envelope = export_bytes(&root.join("local.db"), &root.join("final.export"));
    assert_eq!(
        envelope, expected,
        "row accounting must equal the real public export"
    );
    let result = serde_json::json!({
        "tables": tables, "baseline_envelope_bytes": baseline,
        "catalog_and_fixed_bytes": baseline - base_rows,
        "envelope_bytes": envelope, "envelope_cap_bytes": CAP,
        "headroom_bytes": CAP - envelope, "physical_db_bytes": fs::metadata(root.join("local.db/data.redb")).unwrap().len(),
        "materialized_json_bytes": state_json_bytes, "applied_transitions": seq,
        "elapsed_seconds": elapsed, "first_failure": failure,
    });
    let output = serde_json::to_string_pretty(&result).unwrap();
    fs::write(root.join("measurements.json"), &output).unwrap();
    println!("SIZING {}\n{}", root.display(), output);
}

fn experiment(name: &str, chunks: u64, soak_ticks: u64, require_cap: bool) {
    let root = root(name);
    let db = root.join("local.db");
    let local = Local::create(&db).unwrap();
    let base_rows = row_total(&table_sizes(&local, 128));
    drop(local);
    let baseline = export_bytes(&db, &root.join("empty.export"));
    let mut local = Local::open(&db, &Materialized::default()).unwrap();
    apply(&mut local, Transition::AcquireOwnership { epoch: 1 }).unwrap();
    apply(
        &mut local,
        Transition::Define {
            definition: Box::new(definition()),
        },
    )
    .unwrap();
    apply(&mut local, Transition::FenceState { db: "source".into(), role: "application".into(), saved_privileges_json: serde_json::json!({"connect": true, "tables": ["public.orders"], "privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"]}).to_string() }).unwrap();
    for to in [State::Assessed, State::Copying] {
        apply(
            &mut local,
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        )
        .unwrap();
    }
    let items: usize =
        std::env::var("SKEDADDLE_SIZING_MANIFEST_ITEMS").map_or(0, |s| s.parse().unwrap());
    let page = if items == 0 { 512 } else { 8 };
    let start = Instant::now();
    let mut failure = None;
    'chunks: for n in 0..chunks {
        for transition in chunk_transitions(n, items) {
            let intent = matches!(transition, Transition::Intent { .. });
            let seq = local.state().applied_seq + 1;
            if let Err(error) = apply(&mut local, transition) {
                failure = Some(
                    serde_json::json!({"attempted_seq": seq, "chunk_index": n, "error": error.to_string()}),
                );
                break 'chunks;
            }
            if intent {
                let intent_seq = local.state().applied_seq;
                if let Err(error) = apply(&mut local, Transition::EffectCompleted { intent_seq }) {
                    failure = Some(
                        serde_json::json!({"attempted_seq": intent_seq + 1, "chunk_index": n, "error": error.to_string()}),
                    );
                    break 'chunks;
                }
            }
        }
        if (n + 1) % 100 == 0 {
            println!(
                "SIZING_PROGRESS chunks={} seq={} elapsed={:.3}",
                n + 1,
                local.state().applied_seq,
                start.elapsed().as_secs_f64()
            );
        }
    }
    if failure.is_none() {
        'soak: for tick in 0..soak_ticks {
            for stream in ["buffered", "applied", "verified"] {
                if let Err(error) = apply(
                    &mut local,
                    Transition::Progress {
                        stream: stream.into(),
                        marker: format!("txn-{tick:020}"),
                        position: format!("0/{:08X}", tick * 4096),
                    },
                ) {
                    failure = Some(
                        serde_json::json!({"attempted_seq": local.state().applied_seq + 1, "soak_tick": tick, "error": error.to_string()}),
                    );
                    break 'soak;
                }
            }
        }
    }
    if require_cap {
        assert!(
            failure.is_some(),
            "increase SKEDADDLE_SIZING_CHUNKS to reach the cap"
        );
    } else {
        assert!(failure.is_none(), "sample must fit: {failure:?}");
    }
    if let Some(error) = &failure {
        assert!(
            error["error"]
                .as_str()
                .unwrap()
                .to_ascii_lowercase()
                .contains("local-budget"),
            "{error}"
        );
    }
    finish(
        &root,
        local,
        baseline,
        base_rows,
        failure,
        start.elapsed().as_secs_f64(),
        page,
    );
}

#[test]
#[ignore = "sizing: run explicitly"]
fn local_sizing_sample() {
    // 100 chunks at ~10 MB/chunk covers a decimal 1 GB source. One minute of
    // marker activity supplies an observed per-tick slope for a four-hour soak.
    experiment("sample", 100, 60, false);
}

#[test]
#[ignore = "sizing: run explicitly"]
fn local_sizing_until_cap() {
    let chunks = std::env::var("SKEDADDLE_SIZING_CHUNKS").map_or(20_000, |s| s.parse().unwrap());
    experiment("until-cap", chunks, 0, true);
}

#[test]
#[ignore = "sizing: run explicitly"]
fn local_sizing_row_limit() {
    let root = root("row-limit");
    let mut local = Local::create(root.join("local.db")).unwrap();
    apply(&mut local, Transition::AcquireOwnership { epoch: 1 }).unwrap();
    let seq = local.state().applied_seq;
    let error = apply(
        &mut local,
        Transition::Intent {
            effect: EffectKind::BufferPublication,
            payload: "x".repeat(65_536),
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        skedaddle::coordinator::Error::BudgetExceeded(_)
    ));
    assert_eq!(
        local.state().applied_seq,
        seq,
        "rejected row must not advance projection"
    );
    println!(
        "ROW_LIMIT payload=65536 seq_before={seq} seq_after={} error={error}",
        local.state().applied_seq
    );
    let mut wide = definition();
    let selector = wide.selected_objects[0].clone();
    wide.selected_objects = (0..2000)
        .map(|n| {
            let mut object = selector.clone();
            object.name = format!("orders_{n:05}");
            object
        })
        .collect();
    assert!(wide.validate().is_empty());
    let definition_bytes = serde_json::to_vec(&wide).unwrap().len();
    let transition = Transition::Define {
        definition: Box::new(wide),
    };
    let audit_json_bytes = serde_json::to_vec(&transition).unwrap().len();
    apply(&mut local, transition).unwrap();
    assert_eq!(local.state().applied_seq, seq + 1);
    println!(
        "WIDE_DEFINITION tables=2000 definition_json_bytes={definition_bytes} audit_json_bytes={audit_json_bytes} normalized=true"
    );
}

fn verifier(n: u64) -> skedaddle::coordinator::VerifierRange {
    skedaddle::coordinator::VerifierRange {
        table: format!("public.orders_{:03}", n % 100),
        range_lo: format!("[\"tenant-{:04}\",\"{:012}\"]", n % 100, n * 100),
        range_hi: format!("[\"tenant-{:04}\",\"{:012}\"]", n % 100, (n + 1) * 100),
        source_lsn: format!("0/{:08X}", (n + 1) * 4096),
        source_digest: "a".repeat(64),
        destination_digest: "b".repeat(64),
        state: "verified".into(),
        rows: 128,
        retries: 0,
    }
}

fn wide_definition(tables: usize) -> skedaddle::MigrationDefinition {
    let mut d = definition();
    let selector = d.selected_objects[0].clone();
    d.selected_objects = (0..tables)
        .map(|n| {
            let mut object = selector.clone();
            object.name = format!("orders_{n:05}");
            object
        })
        .collect();
    assert!(d.validate().is_empty());
    d
}

#[test]
fn rw4_and_two_thousand_tables_fit_with_headroom() {
    use skedaddle::coordinator::budget;
    // D2's measured row-map bytes: 328/chunk and a conservative 512/current
    // verifier range. History adds zero local rows, including 43,200 progress
    // events over a four-hour, one-tick/second soak.
    let d = wide_definition(2000);
    assert!(budget::preflight(&d).unwrap() > 65_536);
    let mut state = Materialized {
        definition: Some(d),
        ..Default::default()
    };
    for n in 0..10_000 {
        let Transition::Chunk { chunk } = chunk_transitions(n, 0).pop().unwrap() else {
            panic!()
        };
        state.chunks.insert(
            serde_json::to_string(&(&chunk.table, &chunk.range_lo)).unwrap(),
            chunk,
        );
        state
            .verifier_ranges
            .insert(format!("{n:020}"), verifier(n));
    }
    for stream in ["buffered", "applied", "verified"] {
        state.progress.insert(
            stream.into(),
            (format!("txn-{:020}", 14_399), "0/0383F000".into()),
        );
    }
    let estimate = Local::estimate(&state).unwrap();
    let d2_upper = 10_000 * (328 + 512)
        + budget::DEFINITION_BYTES
        + budget::PRIVILEGE_BYTES
        + budget::INTENT_BYTES
        + budget::EXCEPTION_BYTES
        + budget::PROGRESS_BYTES
        + 128 * 1024;
    assert!(estimate < d2_upper);
    assert!(d2_upper < budget::ENVELOPE_BYTES);
    assert!(estimate < 12 * 1024 * 1024);
    println!(
        "RW4 source_bytes=1000000000 chunks=10000 verifier_ranges=10000 soak_ticks=14400 estimate={estimate} conservative_upper={d2_upper} core_headroom={}",
        CAP - estimate as u64
    );
}

#[tokio::test]
async fn normalized_definition_and_privileges_round_trip_in_real_core() {
    let task = root("normalized");
    let path = task.join("local.db");
    let mut local = Local::create(&path).unwrap();
    let objects = task.join("objects");
    fs::create_dir_all(&objects).unwrap();
    let journal = skedaddle::coordinator::journal::Journal::new(
        std::sync::Arc::new(skedaddle::coordinator::journal::local_store(&objects).unwrap()),
        "sizing",
        "normalized",
        std::time::Duration::from_secs(3),
    )
    .unwrap();
    for transition in [
        Transition::AcquireOwnership { epoch: 1 },
        Transition::Define {
            definition: Box::new(wide_definition(2000)),
        },
        Transition::FenceState {
            db: "source".into(),
            role: "app".into(),
            saved_privileges_json: "\"\u{1f600}".repeat(20_000),
        },
    ] {
        let entry = Entry::new(local.state(), transition, INSTANCE).unwrap();
        journal.put(&entry).await.unwrap();
        local.apply(&entry).unwrap();
    }
    let state = local.state().clone();
    assert!(local.read_collection("definition", 2).unwrap().len() > 1);
    assert!(local.read_collection("fence_state", 2).unwrap().len() > 1);
    let estimate = Local::estimate(&state).unwrap();
    let sizes = table_sizes(&local, 2);
    // Exercise Skedaddle's real export/cleanup path. Direct Admin::export is
    // used only by the opt-in research captures, which retain core hardlinks.
    local.checkpoint(&journal).await.unwrap();
    let checkpoint = journal.recover(true).await.unwrap().checkpoint.unwrap();
    use base64::Engine;
    let archive = base64::engine::general_purpose::STANDARD
        .decode(checkpoint.local_archive.unwrap())
        .unwrap();
    assert_eq!(&archive[..8], b"GDEXP001");
    let payload = u64::from_be_bytes(archive[12..20].try_into().unwrap());
    assert_eq!(archive.len() as u64, payload + 52);
    fs::write(task.join("normalized.capture"), archive).unwrap();
    assert!(!path.with_extension("db.skedaddle-scratch").exists());
    drop(local);
    assert!(payload < estimate as u64);
    let fixed = payload - row_total(&sizes);
    assert!(fixed < 128 * 1024);
    assert_eq!(Local::open(&path, &state).unwrap().state(), &state);
}

#[test]
fn four_hour_marker_history_does_not_accumulate_in_materialized() {
    let mut state = Materialized::default();
    let e = Entry::new(&state, Transition::AcquireOwnership { epoch: 1 }, INSTANCE).unwrap();
    state.apply(&e).unwrap();
    for tick in 0..14_400 {
        for stream in ["buffered", "applied", "verified"] {
            let e = Entry::new(
                &state,
                Transition::Progress {
                    stream: stream.into(),
                    marker: format!("txn-{tick:020}"),
                    position: format!("0/{:08X}", tick * 4096),
                },
                INSTANCE,
            )
            .unwrap();
            state.apply(&e).unwrap();
        }
    }
    assert_eq!(state.applied_seq, 43_201);
    assert_eq!(state.audit.len(), 1);
    assert_eq!(state.progress.len(), 3);
    assert!(serde_json::to_vec(&state).unwrap().len() < 4096);
}

#[test]
fn every_local_budget_overflow_is_a_typed_blocker() {
    use skedaddle::coordinator::{
        Error, VerifierException,
        budget::{self, Budget},
    };
    fn blocked(state: Materialized, expected: Budget) {
        let Error::BudgetExceeded(blocker) = Local::estimate(&state).unwrap_err() else {
            panic!("untyped overflow")
        };
        assert_eq!(blocker.budget, expected);
        assert!(blocker.used > blocker.limit);
        assert!(blocker.id().starts_with("local-budget."));
        assert!(!blocker.remediation.is_empty());
    }
    let c = Chunk {
        table: "t".into(),
        range_lo: "0".into(),
        range_hi: "1".into(),
        state: "done".into(),
        low_wm: None,
        high_wm: None,
    };
    let mut state = Materialized::default();
    for n in 0..=budget::CHUNKS {
        state.chunks.insert(n.to_string(), c.clone());
    }
    blocked(state, Budget::Chunks);
    let mut state = Materialized::default();
    for n in 0..=budget::VERIFIER_RANGES {
        state.verifier_ranges.insert(n.to_string(), verifier(0));
    }
    blocked(state, Budget::VerifierRanges);
    let mut state = Materialized::default();
    for n in 0..=budget::EXCEPTIONS {
        state.verifier_exceptions.insert(
            n.to_string(),
            VerifierException {
                range: "r".into(),
                key: "k".into(),
                reason: "busy".into(),
            },
        );
    }
    blocked(state, Budget::VerifierExceptions);
    blocked(
        Materialized {
            definition: Some(wide_definition(10_000)),
            ..Default::default()
        },
        Budget::Definition,
    );
    let mut state = Materialized::default();
    state.fence_state.insert(
        "role".into(),
        (
            "source".into(),
            "app".into(),
            "x".repeat(budget::PRIVILEGE_BYTES),
        ),
    );
    blocked(state, Budget::Privileges);
    let mut state = Materialized::default();
    for n in 0..=budget::INTENTS {
        state.intent_roots.insert(n as u64, n as u64);
    }
    blocked(state, Budget::ActiveIntents);
    let mut state = Materialized::default();
    for n in 0..=budget::PROGRESS {
        state
            .progress
            .insert(n.to_string(), ("m".into(), "p".into()));
    }
    blocked(state, Budget::Progress);
    let mut state = Materialized::default();
    state
        .progress
        .insert("s".into(), ("m".into(), "x".repeat(budget::ROW_BYTES)));
    blocked(state, Budget::Row);
    let mut state = Materialized::default();
    state
        .progress
        .insert("s".repeat(65_536), ("m".into(), "p".into()));
    blocked(state, Budget::Row);
    blocked(
        Materialized {
            owner: "x".repeat(budget::MEMORY_BYTES),
            ..Default::default()
        },
        Budget::Materialized,
    );
}

#[test]
fn oversized_definition_is_a_critical_check_time_blocker() {
    let task = root("oversized-check");
    let path = task.join("definition.json");
    fs::write(&path, serde_json::to_vec(&wide_definition(10_000)).unwrap()).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("check")
        .arg(path)
        .env("TEMP", std::env::var_os("TEMP").unwrap())
        .env("TMP", std::env::var_os("TMP").unwrap())
        .env(
            "CARGO_TARGET_DIR",
            std::env::var_os("CARGO_TARGET_DIR").unwrap(),
        )
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["schema_version"], 1);
    assert_eq!(envelope["outcome"], "blocked");
    assert_eq!(envelope["exit_code"], 2);
    let report: skedaddle::CapabilityReport =
        serde_json::from_value(envelope["data"].clone()).unwrap();
    let item = report
        .blocking()
        .into_iter()
        .find(|item| item.id == "local-budget.definition")
        .expect("check must block");
    assert_eq!(item.criticality, skedaddle::Criticality::Critical);
    assert!(!item.remediation.is_empty());
}

#[test]
fn aggregate_envelope_overflow_is_typed_even_when_individual_budgets_fit() {
    use skedaddle::coordinator::{
        Error, VerifierException,
        budget::{self, Budget},
    };
    // Fill each independently admitted resource close to its bound, then
    // combine them. This proves that the global reserve is enforced as well.
    fn largest(mut make: impl FnMut(usize) -> Materialized, max: usize) -> Materialized {
        let (mut lo, mut hi) = (0, max);
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            if Local::estimate(&make(mid)).is_ok() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        make(lo)
    }
    let mut combined = largest(
        |padding| {
            let c = Chunk {
                table: "t".into(),
                range_lo: "0".into(),
                range_hi: "h".repeat(padding),
                state: "done".into(),
                low_wm: None,
                high_wm: None,
            };
            let mut state = Materialized::default();
            for n in 0..budget::CHUNKS {
                state.chunks.insert(format!("{n:06}"), c.clone());
            }
            state
        },
        400,
    );
    combined.verifier_ranges = largest(
        |padding| {
            let mut range = verifier(0);
            range.source_lsn = "l".repeat(padding);
            let mut state = Materialized::default();
            for n in 0..budget::VERIFIER_RANGES {
                state
                    .verifier_ranges
                    .insert(format!("{n:06}"), range.clone());
            }
            state
        },
        600,
    )
    .verifier_ranges;
    combined.verifier_exceptions = largest(
        |padding| {
            let mut state = Materialized::default();
            for n in 0..budget::EXCEPTIONS {
                state.verifier_exceptions.insert(
                    format!("{n:06}"),
                    VerifierException {
                        range: "r".into(),
                        key: "k".into(),
                        reason: "x".repeat(padding),
                    },
                );
            }
            state
        },
        600,
    )
    .verifier_exceptions;
    let mut d = definition();
    // Long identifiers are legal in the offline definition schema; use a
    // definition close to its own bound without thousands of duplicate items.
    d.selected_objects[0].name =
        "n".repeat(budget::DEFINITION_BYTES - serde_json::to_vec(&d).unwrap().len());
    combined.definition = Some(d);
    combined.fence_state.insert(
        "role".into(),
        (
            "source".into(),
            "app".into(),
            "p".repeat(budget::PRIVILEGE_BYTES - 100),
        ),
    );
    let intents = largest(
        |padding| {
            let mut state = Materialized {
                epoch: 1,
                owner: INSTANCE.into(),
                ..Default::default()
            };
            for _ in 0..budget::INTENTS {
                let entry = Entry::new(
                    &state,
                    Transition::Intent {
                        effect: EffectKind::BufferPublication,
                        payload: "i".repeat(padding),
                    },
                    INSTANCE,
                )
                .unwrap();
                state.applied_seq = entry.seq;
                state.digest = entry.digest.clone();
                state.intent_roots.insert(entry.seq, entry.seq);
                state.audit.insert(entry.seq, entry);
            }
            state
        },
        9000,
    );
    combined.audit = intents.audit;
    combined.intent_roots = intents.intent_roots;
    combined.applied_seq = intents.applied_seq;
    combined.digest = intents.digest;
    combined.epoch = intents.epoch;
    combined.owner = intents.owner;
    let Error::BudgetExceeded(blocker) = Local::estimate(&combined).unwrap_err() else {
        panic!("untyped envelope overflow")
    };
    assert_eq!(blocker.budget, Budget::Envelope);
    assert_eq!(blocker.limit, budget::ENVELOPE_BYTES);
}
