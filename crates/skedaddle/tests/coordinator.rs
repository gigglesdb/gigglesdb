mod support;

fn typed_plan() -> chunk::ChunkPlan {
    use skedaddle::{
        mapping::{PgType, Value},
        pg::chunk::*,
    };
    chunk::ChunkPlan::new(
        TableKey {
            table: "public.rows".into(),
            relation: QualifiedName {
                schema: "public".into(),
                name: "rows".into(),
            },
            columns: vec![KeyColumn {
                name: "k".into(),
                pg_type: PgType::Int4,
                sql_type: QualifiedName {
                    schema: "pg_catalog".into(),
                    name: "int4".into(),
                },
                collation: None,
                direction: Direction::Asc,
            }],
        },
        100,
        vec![vec![Value::Int4(10)], vec![Value::Int4(20)]],
    )
    .unwrap()
}

#[tokio::test]
async fn typed_done_without_publication_is_rejected_during_journal_recovery() {
    let objects = scratch("typed-invalid-done");
    let store = fs_store(&objects);
    let (buffer, config) = chunk_buffer();
    let journal = support::journal(store.clone(), "migration").with_chunk_buffer(
        buffer,
        config,
        Arc::new(ChunkOrder),
    );
    let mut c = coordinator(journal.clone(), "typed-invalid-done");
    c.acquire().await.unwrap();
    let plan = typed_plan();
    c.commit(Transition::ChunkPlanned {
        plan: journal.publish_chunk_plan(&plan).await.unwrap(),
    })
    .await
    .unwrap();
    c.commit(Transition::ChunkStarted {
        chunk: plan.ranges[0].clone(),
        attempt: 1,
    })
    .await
    .unwrap();
    let invalid = Entry::new(
        c.state(),
        Transition::ChunkDone {
            chunk: plan.ranges[0].clone(),
            attempt: 1,
            high: skedaddle::event::Position::new("pg-chunk-test", 100u64.to_be_bytes()),
            chunk_rows: chunk::ChunkRowsRef {
                mseq: 0,
                manifest_digest: [0; 32],
            },
        },
        c.instance_id(),
    )
    .unwrap();
    assert!(journal.put(&invalid).await.is_err());
    // Inject a correctly hashed but semantically invalid durable journal entry.
    store
        .put_opts(
            &journal.entry_key(invalid.seq),
            serde_json::to_vec(&invalid).unwrap().into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        journal.load(false).await,
        Err(Error::RecoveryRequired(_))
    ));
}

struct ChunkOrder;
impl skedaddle::event::PositionOrder for ChunkOrder {
    fn domain(&self) -> &str {
        "pg-chunk-test"
    }
    fn compare(
        &self,
        a: &[u8],
        b: &[u8],
    ) -> std::result::Result<std::cmp::Ordering, skedaddle::event::EventError> {
        Ok(a.cmp(b))
    }
}

fn chunk_buffer() -> (
    Arc<dyn skedaddle::buffer::backends::BufferStore>,
    skedaddle::buffer::reader::ReaderConfig,
) {
    let store = skedaddle::buffer::backends::ObjectBufferStore::local(
        scratch("chunk-buffer"),
        Duration::from_secs(3),
    )
    .unwrap();
    (
        Arc::new(store),
        skedaddle::buffer::reader::ReaderConfig {
            prefix: "chunks".into(),
            migration_id: "migration".into(),
            stream: "forward".into(),
            limits: skedaddle::buffer::format::Limits::default(),
            required_next_mseq: 0,
        },
    )
}

async fn chunk_manifest(
    store: &dyn skedaddle::buffer::backends::BufferStore,
    config: &skedaddle::buffer::reader::ReaderConfig,
    range: &skedaddle::pg::chunk::ChunkRange,
    attempt: u64,
    high: u64,
    durable: bool,
    transaction_kind: bool,
) -> chunk::ChunkRowsRef {
    use sha2::{Digest, Sha256};
    use skedaddle::{buffer::format::*, event::*};
    let mut hash = Sha256::new();
    hash.update(b"skedaddle-chunk-record-v1\0");
    hash.update(serde_json::to_vec(&range.id).unwrap());
    hash.update(attempt.to_be_bytes());
    hash.update(high.to_be_bytes());
    let position = Position::new("pg-chunk-test", high.to_be_bytes());
    let txn = AssembledTxn::new(
        SourceIdentity {
            engine: "postgres".into(),
            system_id: "source".into(),
            lineage: "lineage".into(),
            db_name: "test".into(),
            stream: "slot".into(),
        },
        TxnId(hash.finalize().to_vec()),
        position.clone(),
        None,
        vec![],
    )
    .unwrap();
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: config.migration_id.clone(),
            stream: config.stream.clone(),
            epoch: 1,
            first_pos: position.clone(),
            last_pos: position.clone(),
            record_count: 1,
        },
        config.limits,
    )
    .unwrap();
    encoder
        .write_record(&if transaction_kind {
            SegmentRecord::Transaction(txn)
        } else {
            SegmentRecord::ChunkRows(txn)
        })
        .unwrap();
    let (bytes, summary) = encoder.finish().unwrap();
    let manifest = ManifestEntry {
        mseq: attempt,
        epoch: 1,
        prev_digest: [0; 32],
        segment_key: segment_key(
            &config.prefix,
            &config.migration_id,
            &config.stream,
            &position,
            &position,
            1,
        ),
        segment_digest: summary.digest,
        first_pos: position.clone(),
        last_pos: position,
        txn_count: 1,
        bytes: summary.bytes,
    };
    if durable {
        store
            .put_create_only(&manifest.segment_key, &bytes)
            .await
            .unwrap();
        store
            .put_create_only(
                &config.manifest_key(manifest.mseq),
                &manifest.canonical_json().unwrap(),
            )
            .await
            .unwrap();
    }
    chunk::ChunkRowsRef {
        mseq: manifest.mseq,
        manifest_digest: manifest.digest().unwrap(),
    }
}

#[tokio::test]
async fn typed_chunk_codec_and_durable_done_roundtrip() {
    let (buffer, config) = chunk_buffer();
    let journal = fresh_journal("typed-codec").with_chunk_buffer(
        buffer.clone(),
        config.clone(),
        Arc::new(ChunkOrder),
    );
    let mut c = coordinator(journal.clone(), "typed-codec");
    c.acquire().await.unwrap();
    let plan = typed_plan();
    let reference = journal.publish_chunk_plan(&plan).await.unwrap();
    let chunk_rows = chunk_manifest(
        buffer.as_ref(),
        &config,
        &plan.ranges[1],
        1,
        100,
        true,
        false,
    )
    .await;
    for transition in [
        Transition::ChunkPlanned { plan: reference },
        Transition::ChunkStarted {
            chunk: plan.ranges[1].clone(),
            attempt: 1,
        },
        Transition::ChunkDone {
            chunk: plan.ranges[1].clone(),
            attempt: 1,
            high: skedaddle::event::Position::new("pg-chunk-test", 100u64.to_be_bytes()),
            chunk_rows,
        },
    ] {
        let entry = c.commit(transition.clone()).await.unwrap();
        let bytes = serde_json::to_vec(&entry).unwrap();
        let decoded: Entry = serde_json::from_slice(&bytes).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.transition, transition);
        assert_eq!(journal.entry(entry.seq).await.unwrap(), decoded);
    }
    assert_eq!(journal.load(false).await.unwrap(), *c.state());
    c.checkpoint().await.unwrap();
    let local = Local::recover(scratch("typed-restored").join("db"), &journal)
        .await
        .unwrap();
    assert_eq!(local.state(), c.state());
    assert_eq!(
        local.state().unfinished_chunks(&plan).unwrap(),
        vec![plan.ranges[0].clone(), plan.ranges[2].clone()]
    );
    c.commit(Transition::ChunkStarted {
        chunk: plan.ranges[1].clone(),
        attempt: 2,
    })
    .await
    .unwrap();
    assert_eq!(c.state().unfinished_chunks(&plan).unwrap(), plan.ranges);
    assert_eq!(c.state().chunk_attempts.len(), 1);
}

#[tokio::test]
async fn typed_chunk_stale_attempt_and_foreign_bounds_fail_closed() {
    let journal = fresh_journal("typed-attempt");
    let mut c = coordinator(journal.clone(), "typed-attempt");
    c.acquire().await.unwrap();
    let plan = typed_plan();
    let reference = journal.publish_chunk_plan(&plan).await.unwrap();
    c.commit(Transition::ChunkPlanned { plan: reference })
        .await
        .unwrap();
    for attempt in [1, 2] {
        c.commit(Transition::ChunkStarted {
            chunk: plan.ranges[0].clone(),
            attempt,
        })
        .await
        .unwrap();
    }
    let before = c.state().clone();
    for attempt in [0, 1, 2] {
        assert!(
            c.commit(Transition::ChunkStarted {
                chunk: plan.ranges[0].clone(),
                attempt
            })
            .await
            .is_err()
        );
    }
    assert!(
        c.commit(Transition::ChunkDone {
            chunk: plan.ranges[0].clone(),
            attempt: 1,
            high: skedaddle::event::Position::new("pg-chunk-test", 100u64.to_be_bytes()),
            chunk_rows: chunk::ChunkRowsRef {
                mseq: 1,
                manifest_digest: [0; 32]
            }
        })
        .await
        .is_err()
    );
    assert_eq!(c.state(), &before);
    let mut foreign = plan.ranges[1].clone();
    foreign.lo = None;
    assert!(
        c.commit(Transition::ChunkStarted {
            chunk: foreign.clone(),
            attempt: 1
        })
        .await
        .is_err()
    );
    // Bypass the coordinator API to prove journal replay also validates bounds.
    let entry = Entry::new(
        &before,
        Transition::ChunkStarted {
            chunk: foreign,
            attempt: 1,
        },
        &before.owner,
    )
    .unwrap();
    assert!(journal.put(&entry).await.is_err());
    let objects = scratch("typed-corrupt");
    let store = fs_store(&objects);
    let corrupt = support::journal(store.clone(), "migration");
    corrupt.publish_chunk_plan(&plan).await.unwrap();
    for seq in 1..=before.applied_seq {
        let saved = journal.entry(seq).await.unwrap();
        store
            .put_opts(
                &corrupt.entry_key(seq),
                serde_json::to_vec(&saved).unwrap().into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
    }
    store
        .put_opts(
            &corrupt.entry_key(entry.seq),
            serde_json::to_vec(&entry).unwrap().into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    assert!(corrupt.load(false).await.is_err());
}

#[tokio::test]
async fn typed_chunk_done_requires_durable_matching_chunk_rows() {
    let (buffer, config) = chunk_buffer();
    let journal = fresh_journal("typed-publication").with_chunk_buffer(
        buffer.clone(),
        config.clone(),
        Arc::new(ChunkOrder),
    );
    let mut c = coordinator(journal.clone(), "typed-publication");
    c.acquire().await.unwrap();
    let plan = typed_plan();
    c.commit(Transition::ChunkPlanned {
        plan: journal.publish_chunk_plan(&plan).await.unwrap(),
    })
    .await
    .unwrap();
    let chunk = plan.ranges[0].clone();
    c.commit(Transition::ChunkStarted {
        chunk: chunk.clone(),
        attempt: 1,
    })
    .await
    .unwrap();
    let missing = chunk_manifest(buffer.as_ref(), &config, &chunk, 1, 100, false, false).await;
    assert!(
        c.commit(Transition::ChunkDone {
            chunk: chunk.clone(),
            attempt: 1,
            high: skedaddle::event::Position::new("pg-chunk-test", 100u64.to_be_bytes()),
            chunk_rows: missing.clone()
        })
        .await
        .is_err()
    );
    assert!(c.state().chunk_attempts.values().all(|a| a.done.is_none()));
    // A durable Transaction with the expected ID is still not ChunkRows.
    let wrong = chunk_manifest(buffer.as_ref(), &config, &chunk, 1, 100, true, true).await;
    assert!(
        c.commit(Transition::ChunkDone {
            chunk: chunk.clone(),
            attempt: 1,
            high: skedaddle::event::Position::new("pg-chunk-test", 100u64.to_be_bytes()),
            chunk_rows: wrong
        })
        .await
        .is_err()
    );
    c.commit(Transition::ChunkStarted {
        chunk: chunk.clone(),
        attempt: 2,
    })
    .await
    .unwrap();
    let durable = chunk_manifest(buffer.as_ref(), &config, &chunk, 2, 200, true, false).await;
    assert!(
        c.commit(Transition::ChunkDone {
            chunk: chunk.clone(),
            attempt: 2,
            high: skedaddle::event::Position::new("pg-chunk-test", 201u64.to_be_bytes()),
            chunk_rows: durable.clone()
        })
        .await
        .is_err()
    );
    let mut wrong_digest = durable.clone();
    wrong_digest.manifest_digest[0] ^= 1;
    assert!(
        c.commit(Transition::ChunkDone {
            chunk: chunk.clone(),
            attempt: 2,
            high: skedaddle::event::Position::new("pg-chunk-test", 200u64.to_be_bytes()),
            chunk_rows: wrong_digest
        })
        .await
        .is_err()
    );
    c.commit(Transition::ChunkDone {
        chunk,
        attempt: 2,
        high: skedaddle::event::Position::new("pg-chunk-test", 200u64.to_be_bytes()),
        chunk_rows: durable,
    })
    .await
    .unwrap();
    assert_eq!(journal.load(false).await.unwrap(), *c.state());
}

#[tokio::test]
async fn typed_plan_reuses_source_samples_on_resume_and_is_bounded() {
    let journal = fresh_journal("typed-plan");
    let mut c = coordinator(journal.clone(), "typed-plan");
    c.acquire().await.unwrap();
    let plan = typed_plan();
    let reference = journal.publish_chunk_plan(&plan).await.unwrap();
    c.commit(Transition::ChunkPlanned {
        plan: reference.clone(),
    })
    .await
    .unwrap();
    c.commit(Transition::ChunkStarted {
        chunk: plan.ranges[0].clone(),
        attempt: 7,
    })
    .await
    .unwrap();
    c.checkpoint().await.unwrap();
    drop(c);
    let saved = journal.load(true).await.unwrap();
    let recovered = journal
        .chunk_plan(&saved.chunk_plans["public.rows"])
        .await
        .unwrap();
    assert_eq!(recovered, plan);
    assert_eq!(saved.unfinished_chunks(&recovered).unwrap(), plan.ranges);
    let mut c = coordinator(journal.clone(), "typed-resumed");
    c.acquire().await.unwrap();
    assert!(
        c.commit(Transition::ChunkPlanned { plan: reference })
            .await
            .is_err()
    );
    c.commit(Transition::ChunkStarted {
        chunk: plan.ranges[0].clone(),
        attempt: 8,
    })
    .await
    .unwrap();
    // >60 KiB of samples/ranges stay in an immutable object, not a local row.
    let mut large_table = plan.table.clone();
    large_table.table = "public.large".into();
    let many = chunk::ChunkPlan::new(
        large_table,
        100,
        (0..1500)
            .map(|i| vec![skedaddle::mapping::Value::Int4(i)])
            .collect(),
    )
    .unwrap();
    assert!(serde_json::to_vec(&many).unwrap().len() > budget::ROW_BYTES);
    let reference = journal.publish_chunk_plan(&many).await.unwrap();
    assert!(serde_json::to_vec(&reference).unwrap().len() < 512);
    assert_eq!(journal.chunk_plan(&reference).await.unwrap(), many);
    c.commit(Transition::ChunkPlanned { plan: reference })
        .await
        .unwrap();
    assert!(Local::estimate(c.state()).unwrap() < budget::ENVELOPE_BYTES);
    c.checkpoint().await.unwrap();
    assert_eq!(journal.load(true).await.unwrap(), *c.state());
    let mut oversized = plan.table.clone();
    oversized.table = "t".repeat(chunk::PLAN_BYTES / 2);
    assert!(matches!(
        chunk::ChunkPlan::new(
            oversized,
            100,
            vec![vec![skedaddle::mapping::Value::Int4(10)]]
        ),
        Err(Error::BudgetExceeded(_))
    ));
    assert!(
        chunk::ChunkPlan::new(
            plan.table,
            100,
            (0..budget::CHUNKS as i32)
                .map(|i| vec![skedaddle::mapping::Value::Int4(i)])
                .collect()
        )
        .is_err()
    );
}

#[tokio::test]
async fn upstream_local_store_and_core_checkpoints_leave_no_scratch() {
    let objects = support::scratch("cleanup-objects");
    let store = support::fs_store(&objects);
    let j = support::journal(store, "cleanup");
    let state = support::history(&j, 3).await;
    // Successful retry and a losing create must both clean upstream staging.
    j.put(&j.entry(1).await.unwrap()).await.unwrap();
    let mut conflict = j.entry(1).await.unwrap();
    conflict.coordinator_instance = "competitor".into();
    conflict.digest = independent_json_hash(serde_json::json!([
        conflict.version,
        conflict.seq,
        conflict.epoch,
        conflict.prev_digest,
        conflict.transition,
        conflict.at,
        conflict.coordinator_instance
    ]));
    assert!(matches!(j.put(&conflict).await, Err(Error::LostOwnership)));

    let original = support::scratch("cleanup-original");
    let local = Local::rebuild(original.join("db"), &state).unwrap();
    for _ in 0..2 {
        local.checkpoint(&j).await.unwrap();
        support::assert_database_only(&original);
    }
    let restored = support::scratch("cleanup-restored");
    let new = Local::recover(restored.join("db"), &j).await.unwrap();
    assert_eq!(new.state(), &state);
    support::assert_database_only(&restored);
    drop(new);
    // Core rejects an existing destination; the input archive is still removed.
    assert!(Local::recover(restored.join("db"), &j).await.is_err());
    support::assert_database_only(&restored);
    let mut expected = (1..=3)
        .map(|n| std::path::PathBuf::from(format!("p2/journal/cleanup/{n:020}.json")))
        .collect::<Vec<_>>();
    expected.push("p2/journal/cleanup/checkpoint-00000000000000000003.json".into());
    expected.push("p2/checkpoint-index/cleanup/00000000000000000001.json".into());
    expected.sort();
    assert_eq!(support::relative_files(&objects), expected);
    // Reopening through the product's local-store constructor accepts a clean root.
    skedaddle::coordinator::journal::local_store(&objects).unwrap();
}

#[test]
fn crash_staging_is_identifiable_and_reported_on_restart() {
    // The same test executable is an explicit subprocess fixture. No product
    // behavior reads these controls, and exit bypasses Rust cleanup/destructors.
    if let Some(kind) = std::env::var_os("SKEDADDLE_STAGING_CRASH_KIND") {
        let root =
            std::path::PathBuf::from(std::env::var_os("SKEDADDLE_STAGING_CRASH_ROOT").unwrap());
        assert!(root.starts_with("J:/gigglesdb-work"));
        if kind == "object" {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let store = skedaddle::coordinator::journal::local_store(&root).unwrap();
                let mut upload = store
                    .put_multipart(&Path::from("unfinished.json"))
                    .await
                    .unwrap();
                upload.put_part("partial upload".into()).await.unwrap();
                std::process::exit(86);
            });
        } else {
            let admin = giggles_core::Admin::open(root.join("db"), giggles_core::Limits::default())
                .unwrap();
            let scratch = root.join("db.skedaddle-scratch");
            std::fs::create_dir(&scratch).unwrap();
            admin
                .export_with_fault(
                    scratch.join("archive.export"),
                    giggles_core::RecoveryPoint::ExportWrite,
                )
                .unwrap_err();
            std::process::exit(86);
        }
    }
    for kind in ["object", "export"] {
        let root = support::scratch(&format!("staging-crash-{kind}"));
        let state = Materialized::default();
        if kind == "export" {
            drop(Local::create(root.join("db")).unwrap());
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "crash_staging_is_identifiable_and_reported_on_restart",
                "--nocapture",
            ])
            .env("SKEDADDLE_STAGING_CRASH_KIND", kind)
            .env("SKEDADDLE_STAGING_CRASH_ROOT", &root)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let start = Instant::now();
        while child.try_wait().unwrap().is_none() {
            if start.elapsed() > Duration::from_secs(15) {
                child.kill().unwrap();
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let result = child.wait_with_output().unwrap();
        assert_eq!(result.status.code(), Some(86), "{result:?}");
        let files = support::relative_files(&root);
        if kind == "object" {
            assert_eq!(files, [std::path::PathBuf::from("unfinished.json#1")]);
            let error = skedaddle::coordinator::journal::local_store(&root).unwrap_err();
            assert!(matches!(error, Error::RecoveryRequired(_)));
            assert!(error.to_string().contains("unfinished.json#1"));
        } else {
            assert_eq!(files.len(), 2, "{files:?}");
            assert!(
                files
                    .iter()
                    .any(|p| p.to_string_lossy().contains("archive.export.staging-"))
            );
            for _ in 0..2 {
                let error = Local::open(root.join("db"), &state).err().unwrap();
                assert!(matches!(error, Error::RecoveryRequired(_)));
                assert!(error.to_string().contains("db.skedaddle-scratch"));
            }
            assert!(Local::create(root.join("db")).is_err());
        }
        // Reporting must preserve the orphan and the database, even on retry.
        assert_eq!(support::relative_files(&root), files);
    }
}

use object_store::{ObjectStore, ObjectStoreExt, PutMode, path::Path};
use skedaddle::coordinator::{
    epoch::*,
    journal::{Entry, JOURNAL_FORMAT_VERSION, Journal},
    local::Local,
    *,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use support::*;

const STATES: &[State] = &[
    State::Defined,
    State::Assessed,
    State::Copying,
    State::CatchingUp,
    State::VerifiedReady,
    State::SourceFenced,
    State::FinalBoundaryApplied,
    State::DestinationActive,
    State::RollbackWindow,
    State::RollingBack,
    State::Finalized,
    State::SourceActiveAfterRollback,
    State::AbortedToSource,
    State::Paused,
    State::Blocked,
    State::Failed,
    State::RecoveryRequired,
];

// Advance events exclude edges requiring interruption reasons or fresh facts.
fn reference_advance_edges(state: State) -> &'static [State] {
    use State::*;
    match state {
        Defined => &[Assessed],
        Assessed => &[Copying],
        Copying => &[CatchingUp],
        CatchingUp => &[],
        VerifiedReady => &[SourceFenced],
        SourceFenced => &[FinalBoundaryApplied, AbortedToSource],
        FinalBoundaryApplied => &[DestinationActive, AbortedToSource],
        DestinationActive => &[RollbackWindow, Finalized],
        RollbackWindow => &[RollingBack, Finalized],
        RollingBack => &[SourceActiveAfterRollback],
        Finalized
        | SourceActiveAfterRollback
        | AbortedToSource
        | Paused
        | Blocked
        | Failed
        | RecoveryRequired => &[],
    }
}

#[test]
fn only_legal_transitions() {
    for state in STATES {
        let current = Lifecycle {
            state: *state,
            authority: Authority::Unknown,
            ..Default::default()
        };
        for to in STATES {
            let result = next_lifecycle(&current, &Event::Advance { to: *to }, 42);
            assert_eq!(
                result.is_ok(),
                reference_advance_edges(*state).contains(to),
                "{state:?} -> {to:?}"
            );
            if let Ok(next) = result {
                assert_eq!(next.state, *to);
                assert_eq!(next.since_seq, 42);
                let expected = match to {
                    State::SourceFenced | State::FinalBoundaryApplied | State::RollingBack => {
                        Authority::Neither
                    }
                    State::DestinationActive | State::RollbackWindow | State::Finalized => {
                        Authority::Destination
                    }
                    State::SourceActiveAfterRollback | State::AbortedToSource => Authority::Source,
                    _ => current.authority,
                };
                assert_eq!(next.authority, expected);
            }
        }
        for to in STATES {
            let expected = (*state == State::Defined && *to == State::Blocked)
                || ([State::Copying, State::CatchingUp].contains(state)
                    && [
                        State::Paused,
                        State::Blocked,
                        State::Failed,
                        State::RecoveryRequired,
                    ]
                    .contains(to));
            assert_eq!(
                next_lifecycle(
                    &current,
                    &Event::Interrupt {
                        to: *to,
                        reason: "injected".into()
                    },
                    42
                )
                .is_ok(),
                expected
            );
            assert!(
                next_lifecycle(
                    &current,
                    &Event::Interrupt {
                        to: *to,
                        reason: String::new()
                    },
                    42
                )
                .is_err()
            );
        }
        assert_eq!(
            next_lifecycle(&current, &Event::Readiness { facts: ready() }, 42).is_ok(),
            [State::CatchingUp, State::VerifiedReady].contains(state)
        );
        for prior in STATES {
            let current = Lifecycle {
                resume_state: Some(*prior),
                ..current.clone()
            };
            let expected =
                *state == State::Paused && [State::Copying, State::CatchingUp].contains(prior);
            assert_eq!(
                next_lifecycle(&current, &Event::Resume, 42).is_ok(),
                expected
            );
        }
    }
    // Seeded walks include rejected events; failed moves must preserve the prior value.
    for seed in 1..=64u64 {
        let mut rng = seed;
        let mut current = Lifecycle::default();
        for seq in 1..=1024 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let to = STATES[rng as usize % STATES.len()];
            match next_lifecycle(&current, &Event::Advance { to }, seq) {
                Ok(next) => {
                    assert!(reference_advance_edges(current.state).contains(&to));
                    current = next;
                }
                Err(_) => assert!(!reference_advance_edges(current.state).contains(&to)),
            }
            if reference_advance_edges(current.state).is_empty() {
                current = Lifecycle::default();
            }
        }
    }
}

#[test]
fn every_readiness_regression_is_recorded() {
    let current = Lifecycle {
        state: State::CatchingUp,
        ..Default::default()
    };
    let ready_state = next_lifecycle(&current, &Event::Readiness { facts: ready() }, 9).unwrap();
    assert_eq!(ready_state.state, State::VerifiedReady);
    use readiness::{BlockerCode::*, Readiness, Reason};
    assert_eq!(ready().evaluate(), Readiness::Ready);
    type FactChange = fn(&mut ReadinessFacts);
    let changes: [(FactChange, readiness::BlockerCode); 15] = [
        (|f| f.all_chunks_done = false, ChunksIncomplete),
        (|f| f.capture_healthy = false, CaptureUnhealthy),
        (|f| f.lineage_matches = false, LineageMismatch),
        (|f| f.apply_lag = 6, ApplyLagOverBudget),
        (|f| f.blocked_ddl = true, SchemaBoundaryPending),
        (|f| f.unknown_schema_job = true, SchemaBoundaryPending),
        (|f| f.verified_rows = 99, VerificationIncomplete),
        (|f| f.verified_rows = 101, VerificationIncomplete),
        (|f| f.busy_rows_past_budget = 1, BusyRowBudgetExhausted),
        (|f| f.dead_letters = 1, DeadLettersPresent),
        (
            |f| f.indexes_constraints_valid = false,
            DestinationSchemaInvalid,
        ),
        (|f| f.sequences_syncable = false, SequencesNotSyncable),
        (
            |f| f.reverse_prerequisites = false,
            ReversePrerequisitesMissing,
        ),
        (
            |f| f.fence_risks_acknowledged = false,
            FenceRiskUnacknowledged,
        ),
        (
            |f| f.buffer_contract_matches = false,
            BufferContractMismatch,
        ),
    ];
    for (change, code) in changes {
        let mut facts = ready();
        change(&mut facts);
        assert_eq!(
            facts.evaluate(),
            Readiness::NotReady {
                reasons: vec![Reason { code }]
            }
        );
        let next = next_lifecycle(&ready_state, &Event::Readiness { facts }, 10).unwrap();
        assert_eq!(next.state, State::CatchingUp);
        assert_eq!(next.since_seq, 10);
        assert_eq!(next.authority, Authority::Source);
        assert_eq!(next.blockers, [code.as_str()]);
        // A subsequent call uses new facts, rather than the previous result.
        let restored = next_lifecycle(&next, &Event::Readiness { facts: ready() }, 11).unwrap();
        assert_eq!(restored.state, State::VerifiedReady);
        assert!(restored.blockers.is_empty());
    }
}

// Independent literal §3.11 table; saved-state resume is checked separately.
fn reference_edges(state: State) -> &'static [State] {
    use State::*;
    match state {
        Defined => &[Assessed, Blocked],
        Assessed => &[Copying],
        Copying => &[CatchingUp, Paused, Blocked, Failed, RecoveryRequired],
        CatchingUp => &[VerifiedReady, Paused, Blocked, Failed, RecoveryRequired],
        VerifiedReady => &[SourceFenced, CatchingUp],
        SourceFenced => &[FinalBoundaryApplied, AbortedToSource],
        FinalBoundaryApplied => &[DestinationActive, AbortedToSource],
        DestinationActive => &[RollbackWindow, Finalized],
        RollbackWindow => &[RollingBack, Finalized],
        RollingBack => &[SourceActiveAfterRollback],
        Finalized
        | SourceActiveAfterRollback
        | AbortedToSource
        | Paused
        | Blocked
        | Failed
        | RecoveryRequired => &[],
    }
}

#[test]
fn every_states_allowed_set_equals_the_plan_table() {
    assert_eq!(State::ALL.as_slice(), STATES);
    for state in STATES {
        assert_eq!(state.allowed_next(), reference_edges(*state));
        assert_eq!(
            state.is_terminal(),
            matches!(
                state,
                State::Finalized | State::SourceActiveAfterRollback | State::AbortedToSource
            )
        );
        let current = Lifecycle {
            state: *state,
            ..Default::default()
        };
        assert_eq!(current.allowed_next(), reference_edges(*state));
        for to in STATES {
            let mut regressed = ready();
            regressed.capture_healthy = false;
            let events = [
                Event::Advance { to: *to },
                Event::Interrupt {
                    to: *to,
                    reason: "test interruption".into(),
                },
                Event::Readiness { facts: ready() },
                Event::Readiness { facts: regressed },
                Event::Resume,
                Event::ResumeAfterRecheck {
                    remaining_blockers: vec![],
                    facts: Some(ready()),
                },
            ];
            let mut accepted = false;
            for event in events {
                match next_lifecycle(&current, &event, 42) {
                    Ok(next) => accepted |= next.state != *state && next.state == *to,
                    Err(error) => assert!(matches!(error, Error::Lifecycle(_)), "{error:?}"),
                }
            }
            assert_eq!(
                accepted,
                reference_edges(*state).contains(to),
                "{state:?} -> {to:?}"
            );
        }
    }
}

#[test]
fn resume_is_saved_state_only_and_blocked_requires_recheck() {
    use TransitionRejectionCode as Code;
    let codes = [
        Code::NotAllowed,
        Code::ReadinessRequired,
        Code::ReasonRequired,
        Code::ResumeStateMissing,
        Code::ResumeBoundary,
        Code::BlockerRecheckRequired,
        Code::BlockersStillHold,
        Code::OperatorRecoveryRequired,
    ];
    for code in codes {
        assert!(!code.remediation().trim().is_empty());
    }
    let assert_code = |result: Result<Lifecycle>, code: TransitionRejectionCode| {
        let error = result.unwrap_err();
        assert!(error.to_string().contains(code.remediation()));
        assert!(matches!(error, Error::Lifecycle(rejection) if rejection.code == code));
    };
    for state in [State::Paused, State::Blocked] {
        for saved in STATES {
            let current = Lifecycle {
                state,
                resume_state: Some(*saved),
                authority: Authority::Unknown,
                blockers: vec!["operator pause or blocker".into()],
                ..Default::default()
            };
            let allowed = [State::Copying, State::CatchingUp].contains(saved)
                || (state == State::Blocked && *saved == State::Defined);
            assert_eq!(
                current.allowed_next(),
                if allowed {
                    std::slice::from_ref(saved)
                } else {
                    &[]
                }
            );
            let mut facts = ready();
            facts.lineage_matches = false;
            let checked = Event::ResumeAfterRecheck {
                remaining_blockers: vec![],
                facts: Some(facts),
            };
            let result = next_lifecycle(&current, &checked, 4);
            if allowed {
                let next = result.unwrap();
                assert_eq!(next.state, *saved);
                assert_eq!(next.authority, Authority::Unknown);
                assert_eq!(next.resume_state, None);
                assert_eq!(
                    next.blockers,
                    if *saved == State::CatchingUp {
                        vec!["lineage-mismatch"]
                    } else {
                        vec![]
                    }
                );
                let ready_checked = Event::ResumeAfterRecheck {
                    remaining_blockers: vec![],
                    facts: Some(ready()),
                };
                assert_eq!(
                    next_lifecycle(&current, &ready_checked, 4).unwrap().state,
                    *saved
                );
                if state == State::Blocked {
                    assert_code(
                        next_lifecycle(&current, &Event::Resume, 4),
                        Code::BlockerRecheckRequired,
                    );
                }
                let still_blocked = Event::ResumeAfterRecheck {
                    remaining_blockers: vec!["still holds".into()],
                    facts: Some(ready()),
                };
                assert_code(
                    next_lifecycle(&current, &still_blocked, 4),
                    Code::BlockersStillHold,
                );
                if *saved == State::CatchingUp {
                    let missing = Event::ResumeAfterRecheck {
                        remaining_blockers: vec![],
                        facts: None,
                    };
                    assert_code(
                        next_lifecycle(&current, &missing, 4),
                        Code::ReadinessRequired,
                    );
                }
            } else {
                assert_code(result, Code::ResumeBoundary);
            }
        }
        let missing = Lifecycle {
            state,
            ..Default::default()
        };
        assert_code(
            next_lifecycle(&missing, &Event::Resume, 4),
            Code::ResumeStateMissing,
        );
    }
    for state in [State::Failed, State::RecoveryRequired] {
        let current = Lifecycle {
            state,
            resume_state: Some(State::Copying),
            ..Default::default()
        };
        for event in [
            Event::Resume,
            Event::ResumeAfterRecheck {
                remaining_blockers: vec![],
                facts: Some(ready()),
            },
        ] {
            assert_code(
                next_lifecycle(&current, &event, 4),
                Code::OperatorRecoveryRequired,
            );
        }
    }
}

#[test]
fn readiness_reports_all_failed_conditions_and_every_code_has_remediation() {
    use readiness::{BlockerCode, Condition, Readiness};
    let facts = ReadinessFacts {
        apply_lag: 1,
        blocked_ddl: true,
        in_scope_rows: 1,
        busy_rows_past_budget: 1,
        dead_letters: 1,
        ..Default::default()
    };
    let Readiness::NotReady { reasons } = facts.evaluate() else {
        panic!("must fail")
    };
    assert_eq!(
        reasons.iter().map(|r| r.code).collect::<Vec<_>>(),
        BlockerCode::ALL
    );
    let conditions = [
        Condition::Chunks,
        Condition::CaptureAndLineage,
        Condition::ApplyLag,
        Condition::SchemaJobs,
        Condition::Verification,
        Condition::DestinationSchema,
        Condition::Sequences,
        Condition::ReverseReplication,
        Condition::FencePlan,
        Condition::BufferContract,
    ];
    for condition in conditions {
        assert!(
            reasons
                .iter()
                .any(|reason| reason.code.condition() == condition)
        );
    }
    let mut codes = std::collections::BTreeSet::new();
    for code in BlockerCode::ALL {
        assert!(codes.insert(code.as_str()));
        assert!(!code.message().trim().is_empty());
        assert!(!code.remediation().trim().is_empty());
        assert_eq!(serde_json::to_value(code).unwrap(), code.as_str());
    }
    let mut boundary = ready();
    boundary.apply_lag = boundary.lag_budget;
    boundary.verified_rows = 0;
    boundary.in_scope_rows = 0;
    assert_eq!(boundary.evaluate(), Readiness::Ready);
}

async fn advance_to_catching_up(c: &mut Coordinator) {
    for to in [State::Assessed, State::Copying, State::CatchingUp] {
        c.commit(Transition::Lifecycle {
            event: Event::Advance { to },
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn interruptions_preserve_authority_and_every_marker_through_replay() {
    for start in [State::Defined, State::Copying, State::CatchingUp] {
        for to in [
            State::Paused,
            State::Blocked,
            State::Failed,
            State::RecoveryRequired,
        ] {
            if !reference_edges(start).contains(&to) {
                continue;
            }
            // Last-known authority is preserved even if unknown; it is never inferred
            // from an interruption state. The durable path below starts at source.
            for authority in [
                Authority::Source,
                Authority::Destination,
                Authority::Neither,
                Authority::Unknown,
            ] {
                let current = Lifecycle {
                    state: start,
                    authority,
                    ..Default::default()
                };
                let next = next_lifecycle(
                    &current,
                    &Event::Interrupt {
                        to,
                        reason: "preserve evidence".into(),
                    },
                    7,
                )
                .unwrap();
                assert_eq!(next.authority, authority);
                assert_eq!(next.resume_state, Some(start));
            }
            let journal = fresh_journal("lifecycle-markers");
            let mut c = coordinator(journal.clone(), "owner");
            c.acquire().await.unwrap();
            if start != State::Defined {
                for state in [State::Assessed, State::Copying, State::CatchingUp] {
                    c.commit(Transition::Lifecycle {
                        event: Event::Advance { to: state },
                    })
                    .await
                    .unwrap();
                    if state == start {
                        break;
                    }
                }
            }
            for i in 0..budget::PROGRESS {
                c.commit(Transition::Progress {
                    stream: format!("stream-{i}"),
                    marker: format!("marker-{i}"),
                    position: format!("position-{i}"),
                })
                .await
                .unwrap();
            }
            let before = c.state().clone();
            let event = Event::Interrupt {
                to,
                reason: "preserve evidence".into(),
            };
            let entry = c
                .commit(Transition::Lifecycle {
                    event: event.clone(),
                })
                .await
                .unwrap();
            assert_eq!(c.state().progress, before.progress);
            assert_eq!(c.state().lifecycle.authority, before.lifecycle.authority);
            assert_eq!(c.state().lifecycle.resume_state, Some(start));
            assert_eq!(
                journal.entry(entry.seq).await.unwrap().transition,
                Transition::Lifecycle { event }
            );
            assert_eq!(&journal.load(false).await.unwrap(), c.state());
            c.checkpoint().await.unwrap();
            assert_eq!(&journal.load(true).await.unwrap(), c.state());
            if matches!(to, State::Paused | State::Blocked) {
                c.commit(Transition::Lifecycle {
                    event: Event::ResumeAfterRecheck {
                        remaining_blockers: vec![],
                        facts: Some(ready()),
                    },
                })
                .await
                .unwrap();
                assert_eq!(c.state().lifecycle.state, start);
                assert_eq!(c.state().progress, before.progress);
                assert_eq!(&journal.load(false).await.unwrap(), c.state());
            }
        }
    }
}

#[tokio::test]
async fn readiness_regression_is_journaled_and_lifecycle_budget_rejection_is_atomic() {
    let journal = fresh_journal("readiness-regression");
    let mut c = coordinator(journal.clone(), "owner");
    c.acquire().await.unwrap();
    advance_to_catching_up(&mut c).await;
    c.commit(Transition::Lifecycle {
        event: Event::Readiness { facts: ready() },
    })
    .await
    .unwrap();
    let mut facts = ready();
    facts.capture_healthy = false;
    facts.dead_letters = 1;
    let event = Event::Readiness { facts };
    let entry = c
        .commit(Transition::Lifecycle {
            event: event.clone(),
        })
        .await
        .unwrap();
    assert_eq!(c.state().lifecycle.state, State::CatchingUp);
    assert_eq!(
        c.state().lifecycle.blockers,
        ["capture-unhealthy", "dead-letters-present"]
    );
    assert_eq!(
        journal.entry(entry.seq).await.unwrap().transition,
        Transition::Lifecycle { event }
    );
    assert_eq!(&journal.load(false).await.unwrap(), c.state());
    c.checkpoint().await.unwrap();
    assert_eq!(&journal.load(true).await.unwrap(), c.state());
    let before = c.state().clone();
    let error = c
        .commit(Transition::Lifecycle {
            event: Event::Advance {
                to: State::VerifiedReady,
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Lifecycle(TransitionRejection {
            code: TransitionRejectionCode::ReadinessRequired,
            ..
        })
    ));
    let error = c
        .commit(Transition::Lifecycle {
            event: Event::Interrupt {
                to: State::Blocked,
                reason: "x".repeat(budget::ROW_BYTES + 1),
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::BudgetExceeded(_)), "{error:?}");
    assert_eq!(c.state(), &before);
    assert_eq!(journal.load(false).await.unwrap(), before);
}

async fn race(store: Arc<dyn ObjectStore>, name: &str) {
    let name = format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let observed = Arc::new(support::counting::CountingStore::new(store.clone()));
    observed.arm_put_barrier(2);
    let j = journal(observed, &name);
    let mut ca = coordinator(j.clone(), "same-label");
    let mut cb = coordinator(j.clone(), "same-label");
    assert_ne!(
        ca.instance_id(),
        cb.instance_id(),
        "labels must not be ownership identities"
    );
    let (a, b) = tokio::join!(ca.acquire(), cb.acquire());
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "{name}: {a:?}, {b:?}"
    );
    assert!(matches!(a, Err(Error::LostOwnership)) || matches!(b, Err(Error::LostOwnership)));
    assert_eq!(ca.is_owner(), a.is_ok());
    assert_eq!(cb.is_owner(), b.is_ok());
    assert_eq!(j.load(false).await.unwrap().epoch, 1);
    let key = j.entry_key(1);
    let bytes = store.get(&key).await.unwrap().bytes().await.unwrap();
    let meta = store.head(&key).await.unwrap();
    assert_eq!(meta.size, bytes.len() as u64);
    assert_eq!(meta.location, key);
}

#[tokio::test]
async fn local_two_coordinators_race_one_wins() {
    race(fs_store(scratch("race")), "race").await;
}

#[tokio::test]
#[ignore = "DOCKER REQUIRED: harness/run.ps1 p2 runs real coordinator acquisitions against local FS, pinned Thanos MinIO and fake-gcs-server JSON API"]
async fn two_coordinators_race_one_wins() {
    let cfg = config();
    for name in ["local", "minio", "gcs"] {
        race(backend(name, &cfg), &format!("race-{name}")).await;
    }
}

#[tokio::test]
async fn owned_coordinator_steps_down_when_its_intent_put_loses() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let observed = Arc::new(support::counting::CountingStore::new(fs_store(scratch(
        "lost-intent-put",
    ))));
    let j = journal(observed.clone(), "migration");
    let mut owner = coordinator(j.clone(), "owner");
    owner.acquire().await.unwrap();
    let gate = observed.block_next_put();
    let effected = Arc::new(AtomicBool::new(false));
    let flag = effected.clone();
    let task = tokio::spawn(async move {
        let result = owner
            .effect(EffectKind::Fence, "intent".into(), move |_| async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;
        (owner, result)
    });
    tokio::time::timeout(Duration::from_secs(10), gate.entered())
        .await
        .expect("owner did not reach the intent put before the test deadline");
    let mut successor = coordinator(j.clone(), "successor");
    assert_eq!(successor.acquire().await.unwrap(), 2);
    gate.release();
    let (owner, result) = task.await.unwrap();
    assert!(matches!(result, Err(Error::LostOwnership)));
    assert!(!owner.is_owner());
    assert!(!effected.load(Ordering::SeqCst));
    assert_eq!(j.load(false).await.unwrap().applied_seq, 2);
}

#[tokio::test]
async fn effect_errors_revoke_ownership_when_authority_or_outcome_is_lost() {
    for error in [
        Error::LostOwnership,
        Error::UnknownOutcome("commit uncertain".into()),
    ] {
        let j = fresh_journal("effect-authority-lost");
        let mut c = coordinator(j.clone(), "owner");
        c.acquire().await.unwrap();
        assert!(
            c.effect(
                EffectKind::Activation,
                "intent".into(),
                move |_| async move { Err(error) }
            )
            .await
            .is_err()
        );
        assert!(!c.is_owner());
        assert_eq!(j.load(false).await.unwrap().applied_seq, 2);
    }
}

#[tokio::test]
#[ignore = "DOCKER REQUIRED: real fake-gcs-server JSON create, multi-page listing and ranged reads; harness/run.ps1 p2"]
async fn gcs_json_create_paging_and_ranges() {
    use futures::TryStreamExt;
    use object_store::{Attribute, GetOptions, GetRange, PutOptions};
    let cfg = config();
    let store = backend("gcs", &cfg);
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = Path::from(format!("p2-json-pages/{nonce}"));
    for n in 0..7 {
        store
            .put_opts(
                &prefix.clone().join(n.to_string()),
                format!("payload-{n}").into_bytes().into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
    }
    let mut page_url = reqwest::Url::parse(&format!(
        "{}/storage/v1/b/p2-bucket/o",
        cfg["gcs"].as_str().unwrap()
    ))
    .unwrap();
    page_url
        .query_pairs_mut()
        .append_pair("prefix", &format!("{}/", prefix.as_ref()))
        .append_pair("maxResults", "2");
    let first_page: serde_json::Value = reqwest::Client::new()
        .get(page_url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first_page["items"].as_array().unwrap().len(), 2);
    assert!(
        first_page["nextPageToken"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );
    let entries = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|m| m.location.clone())
            .collect::<Vec<_>>(),
        (0..7)
            .map(|n| prefix.clone().join(n.to_string()))
            .collect::<Vec<_>>()
    );
    let key = prefix.clone().join("0");
    let meta = store.head(&key).await.unwrap();
    assert_eq!(meta.size, 9);
    assert!(meta.version.is_some());
    let result = store
        .get_opts(
            &key,
            GetOptions {
                range: Some(GetRange::Bounded(1..4)),
                version: meta.version.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.bytes().await.unwrap().as_ref(), b"ayl");
    assert!(matches!(
        store
            .put_opts(
                &key,
                b"must-not-replace".to_vec().into(),
                PutMode::Create.into()
            )
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"payload-0"
    );
    for kind in 0..3 {
        let mut options = PutOptions::from(PutMode::Create);
        match kind {
            0 => options.mode = PutMode::Overwrite,
            1 => options.tags.push("key", "value"),
            _ => {
                options
                    .attributes
                    .insert(Attribute::ContentType, "text/plain".into());
            }
        }
        assert!(matches!(
            store
                .put_opts(
                    &prefix.clone().join(format!("unsupported-{kind}")),
                    b"no".to_vec().into(),
                    options
                )
                .await,
            Err(object_store::Error::NotImplemented { .. })
        ));
    }
    let unsupported_destination = prefix.clone().join("unsupported-copy-or-rename");
    assert!(matches!(
        store
            .copy_opts(&key, &unsupported_destination, Default::default())
            .await,
        Err(object_store::Error::NotImplemented { .. })
    ));
    assert!(matches!(
        store
            .rename_opts(&key, &unsupported_destination, Default::default())
            .await,
        Err(object_store::Error::NotImplemented { .. })
    ));
    assert!(matches!(
        store.get(&unsupported_destination).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"payload-0"
    );
    assert_eq!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        7
    );
}

#[tokio::test]
async fn ownership_observers_and_intents() {
    let j = fresh_journal("ownership");
    let mut a = coordinator(j.clone(), "a");
    assert!(matches!(a.commit(progress(1)).await, Err(Error::Observer)));
    assert_eq!(a.acquire().await.unwrap(), 1);
    assert!(a.is_owner());
    a.commit(progress(1)).await.unwrap();
    let observed = j.clone();
    a.effect(EffectKind::Fence,"trusted SQL payload".into(),move |token|async move {
        let state=observed.load(false).await?;
        assert_eq!(token.epoch(),1);assert_eq!(token.seq(),3);
        assert!(matches!(&state.audit[&3].transition,Transition::Intent { effect:EffectKind::Fence,payload } if payload=="trusted SQL payload"));
        Ok(())
    }).await.unwrap();
    assert!(matches!(
        a.state().audit[&4].transition,
        Transition::EffectCompleted { intent_seq: 3 }
    ));
    let mut b = coordinator(j.clone(), "b");
    assert_eq!(b.acquire().await.unwrap(), 2);
    assert!(matches!(
        a.commit(progress(9)).await,
        Err(Error::LostOwnership)
    ));
    assert!(!a.is_owner());
    a.refresh().await.unwrap();
    assert_eq!(a.state(), b.state());
    assert!(matches!(
        a.effect(EffectKind::Activation, String::new(), |_| async {
            panic!("observer ran an effect")
        })
        .await,
        Err(Error::Observer)
    ));
    assert!(matches!(
        b.commit(Transition::Intent {
            effect: EffectKind::Fence,
            payload: String::new()
        })
        .await,
        Err(Error::Rejected(_))
    ));
    b.effect(EffectKind::Unfence, "unfence".into(), |token| async move {
        assert_eq!(token.epoch(), 2);
        Ok(())
    })
    .await
    .unwrap();
}

#[test]
fn coordinator_errors_name_an_action() {
    for error in [
        Error::RecoveryRequired("gap".into()),
        Error::UnsupportedFormatVersion {
            format: "journal-entry",
            found: None,
            expected: JOURNAL_FORMAT_VERSION,
        },
        Error::LostOwnership,
        Error::Observer,
        Error::UnknownOutcome("timeout".into()),
        Error::TruncatedRead("limit".into()),
        Error::Rejected("illegal move".into()),
    ] {
        let message = error.to_string();
        assert!(message.contains(';'), "error has no next step: {message}");
    }
}

fn independent_seal(entry: &mut Entry) {
    use sha2::{Digest, Sha256};
    let json = serde_json::json!([
        entry.version,
        entry.seq,
        entry.epoch,
        entry.prev_digest,
        entry.transition,
        entry.at,
        entry.coordinator_instance
    ]);
    entry.digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&json).unwrap()));
}

#[tokio::test]
async fn clock_regression_does_not_change_journal_or_epoch_order() {
    let j = fresh_journal("clock-regression");
    let mut state = Materialized::default();
    for (transition, instance, at) in [
        (Transition::AcquireOwnership { epoch: 1 }, "first", 10_000),
        (progress(2), "first", 5_000),
        (Transition::AcquireOwnership { epoch: 2 }, "second", 0),
    ] {
        let mut entry = Entry::new(&state, transition, instance).unwrap();
        entry.at = at;
        independent_seal(&mut entry);
        j.put(&entry).await.unwrap();
        state.apply(&entry).unwrap();
    }
    let recovered = j.scrub().await.unwrap();
    assert_eq!(recovered.applied_seq, 3);
    assert_eq!(recovered.epoch, 2);
    assert_eq!(recovered.owner, "second");
    assert_eq!(recovered.progress["s"].1, "2");
}

#[tokio::test]
async fn entry_hash_and_semantic_validation_are_independent() {
    let j = fresh_journal("entry-validation");
    assert_eq!(j.namespace(), "p2/journal/migration");
    let state = history(&j, 1).await;
    let entry = Entry::new(&state, progress(2), "writer").unwrap();
    let mut sealed = entry.clone();
    independent_seal(&mut sealed);
    assert_eq!(entry.digest, sealed.digest);
    for mutate in [
        (|e: &mut Entry| e.seq = 0) as fn(&mut Entry),
        |e| e.epoch = 0,
        |e| e.coordinator_instance.clear(),
    ] {
        let mut bad = entry.clone();
        mutate(&mut bad);
        independent_seal(&mut bad);
        assert!(bad.validate().is_err());
        assert!(j.put(&bad).await.is_err());
    }
    for mutate in [
        (|e: &mut Entry| e.seq += 1) as fn(&mut Entry),
        |e| e.prev_digest = "other".into(),
        |e| e.epoch = 2,
        |e| e.coordinator_instance = "impostor".into(),
    ] {
        let mut bad = entry.clone();
        mutate(&mut bad);
        independent_seal(&mut bad);
        assert!(state.clone().apply(&bad).is_err());
    }
    let mut bad = Entry::new(&state, Transition::AcquireOwnership { epoch: 3 }, "other").unwrap();
    assert!(state.clone().apply(&bad).is_err());
    bad = Entry::new(&state, Transition::AcquireOwnership { epoch: 2 }, "other").unwrap();
    bad.epoch = 1;
    independent_seal(&mut bad);
    assert!(state.clone().apply(&bad).is_err());
    let mut other = state.audit[&1].clone();
    other.at += 1;
    independent_seal(&mut other);
    assert!(state.clone().apply(&other).is_err());
    let bad = Entry::new(
        &state,
        Transition::EffectCompleted { intent_seq: 1 },
        "writer",
    )
    .unwrap();
    assert!(state.clone().apply(&bad).is_err());
    let mut initial = Materialized::default();
    let bad = Entry::new(&initial, progress(0), "writer").unwrap();
    assert!(initial.apply(&bad).is_err());
    let intent = Entry::new(
        &state,
        Transition::Intent {
            effect: EffectKind::Fence,
            payload: "p".into(),
        },
        "writer",
    )
    .unwrap();
    let mut with_intent = state.clone();
    with_intent.apply(&intent).unwrap();
    let acquired = Entry::new(
        &with_intent,
        Transition::AcquireOwnership { epoch: 2 },
        "writer",
    )
    .unwrap();
    with_intent.apply(&acquired).unwrap();
    let bad = Entry::new(
        &with_intent,
        Transition::EffectCompleted { intent_seq: 2 },
        "writer",
    )
    .unwrap();
    assert!(with_intent.apply(&bad).is_err());
    for intent_seq in [1, 3, 99] {
        let bad = Entry::new(
            &with_intent,
            Transition::ResumeEffect { intent_seq },
            "writer",
        )
        .unwrap();
        assert!(with_intent.clone().apply(&bad).is_err());
    }
    let adoption = Entry::new(
        &with_intent,
        Transition::ResumeEffect { intent_seq: 2 },
        "writer",
    )
    .unwrap();
    with_intent.apply(&adoption).unwrap();
    let same_epoch = Entry::new(
        &with_intent,
        Transition::ResumeEffect { intent_seq: 4 },
        "writer",
    )
    .unwrap();
    assert!(with_intent.clone().apply(&same_epoch).is_err());
    let completed = Entry::new(
        &with_intent,
        Transition::EffectCompleted { intent_seq: 4 },
        "writer",
    )
    .unwrap();
    with_intent.apply(&completed).unwrap();
}

#[tokio::test]
async fn retry_effect_adopts_across_multiple_owners_without_changing_identity() {
    let j = fresh_journal("adopt");
    let mut a = coordinator(j.clone(), "first");
    a.acquire().await.unwrap();
    assert!(
        a.effect(EffectKind::Rollback, "original".into(), |_| async {
            Err(Error::UnknownOutcome("lost response".into()))
        })
        .await
        .is_err()
    );
    assert!(matches!(
        a.resume_effect(2, |_| async { panic!("observer effect") })
            .await,
        Err(Error::Observer)
    ));
    let mut b = coordinator(j.clone(), "second");
    b.acquire().await.unwrap();
    assert!(matches!(
        b.resume_effect(1, |_| async { panic!("ownership is not an intent") })
            .await,
        Err(Error::Rejected(_))
    ));
    assert!(matches!(
        b.resume_effect(99, |_| async { panic!("missing intent") })
            .await,
        Err(Error::Rejected(_))
    ));
    assert!(
        b.resume_effect(2, |token| async move {
            assert_eq!(
                (token.epoch(), token.seq(), token.authority_epoch()),
                (1, 2, 2)
            );
            Err(Error::UnknownOutcome("lost again".into()))
        })
        .await
        .is_err()
    );
    let mut c = coordinator(j, "third");
    c.acquire().await.unwrap();
    c.resume_effect(4, |token| async move {
        assert_eq!(
            (token.epoch(), token.seq(), token.authority_epoch()),
            (1, 2, 3)
        );
        assert_eq!(token.payload(), "original");
        assert_eq!(token.effect(), &EffectKind::Rollback);
        Ok(())
    })
    .await
    .unwrap();
    for seq in [2, 4, 6] {
        c.resume_effect(seq, |_| async {
            panic!("completion did not deduplicate root")
        })
        .await
        .unwrap();
    }
    assert!(matches!(
        c.commit(Transition::ResumeEffect { intent_seq: 2 }).await,
        Err(Error::Rejected(_))
    ));
}

#[tokio::test]
async fn ownership_observers_reject_old_capabilities_with_reused_instance() {
    let j = fresh_journal("same-instance-new-epoch");
    let mut owner = coordinator(j.clone(), "owner");
    owner.acquire().await.unwrap();
    owner
        .effect(EffectKind::Fence, "completed".into(), |_| async { Ok(()) })
        .await
        .unwrap();
    let instance = owner.instance_id().to_owned();
    assert_eq!(owner.acquire().await.unwrap(), 2);
    assert_eq!(owner.instance_id(), instance);
    assert!(matches!(
        owner
            .retry_effect(2, |_| async { panic!("old capability executed") })
            .await,
        Err(Error::LostOwnership)
    ));
    // A valid journal epoch advance under the same identity must still fence the
    // old local capability. The store and chain remain genuine and gap-free.
    let next = Entry::new(
        owner.state(),
        Transition::AcquireOwnership { epoch: 3 },
        &instance,
    )
    .unwrap();
    j.put(&next).await.unwrap();
    owner.refresh().await.unwrap();
    assert_eq!(owner.state().epoch, 3);
    assert_eq!(owner.state().owner, instance);
    assert!(!owner.is_owner());
    assert!(matches!(
        owner.commit(progress(4)).await,
        Err(Error::Observer)
    ));
}

#[tokio::test]
async fn retry_effect_is_idempotent_and_rejects_old_epochs() {
    let j = fresh_journal("retry");
    let mut a = coordinator(j.clone(), "a");
    a.acquire().await.unwrap();
    assert!(
        a.effect(EffectKind::FinalMarker, "marker".into(), |_| async {
            Err(Error::Rejected("network failed".into()))
        })
        .await
        .is_err()
    );
    assert_eq!(a.state().applied_seq, 2);
    a.retry_effect(2, |token| async move {
        assert_eq!(token.epoch(), 1);
        assert_eq!(token.seq(), 2);
        assert_eq!(token.payload(), "marker");
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(a.local().state().applied_seq, 3);
    a.retry_effect(2, |_| async { panic!("completed effect ran twice") })
        .await
        .unwrap();
    assert!(matches!(
        a.retry_effect(99, |_| async { Ok(()) }).await,
        Err(Error::Rejected(_))
    ));
    assert!(matches!(
        a.retry_effect(1, |_| async { Ok(()) }).await,
        Err(Error::Rejected(_))
    ));
    // Reused instance name must still lose its old epoch.
    let mut b = coordinator(j, "a");
    assert_eq!(b.acquire().await.unwrap(), 2);
    assert!(matches!(
        a.retry_effect(2, |_| async { panic!("stale effect ran") })
            .await,
        Err(Error::LostOwnership)
    ));
    assert!(!a.is_owner());
    assert!(matches!(
        a.retry_effect(2, |_| async { Ok(()) }).await,
        Err(Error::Observer)
    ));
    assert!(matches!(
        b.retry_effect(2, |_| async { Ok(()) }).await,
        Err(Error::LostOwnership)
    ));
}

#[tokio::test]
async fn local_apply_is_atomic_idempotent_and_validates_on_open() {
    let j = fresh_journal("local-apply");
    let state = history(&j, 6).await;
    let path = scratch("local-apply-db").join("db");
    let mut local = Local::rebuild(&path, &state).unwrap();
    let before = local.read_collection("audit", 2).unwrap();
    local.replay(&state).unwrap();
    assert_eq!(local.read_collection("audit", 2).unwrap(), before);
    let mut malformed = Entry::new(&state, progress(99), "writer").unwrap();
    malformed.prev_digest = "bad".into();
    assert!(local.apply(&malformed).is_err());
    assert_eq!(local.state(), &state);
    drop(local);
    let local = Local::open(&path, &state).unwrap();
    assert_eq!(local.state(), &state);
    drop(local);
    // Damage an actual local table, then ensure open never silently repairs it.
    let admin = giggles_core::Admin::open(&path, giggles_core::Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "UPDATE progress SET position='corrupt' WHERE stream='s'",
            &[],
            None,
        )
        .unwrap();
    drop(admin);
    assert!(matches!(
        Local::open(&path, &state),
        Err(Error::RecoveryRequired(_))
    ));
}

#[tokio::test]
async fn complete_projection_round_trips_and_preflight_rejects_oversize() {
    let j = fresh_journal("complete-projection");
    let path = scratch("complete-projection-db").join("db");
    let mut c =
        Coordinator::observer(j.clone(), Local::create(&path).unwrap(), "complete").unwrap();
    c.acquire().await.unwrap();
    let readme =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
            .unwrap();
    let definition = skedaddle::MigrationDefinition::from_json(
        readme
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    let mut invalid = definition.clone();
    invalid.definition_version = 999;
    assert!(matches!(
        c.commit(Transition::Define {
            definition: Box::new(invalid)
        })
        .await,
        Err(Error::Rejected(_))
    ));
    assert_eq!(c.state().applied_seq, 1);
    c.commit(Transition::Define {
        definition: Box::new(definition.clone()),
    })
    .await
    .unwrap();
    assert!(
        c.commit(Transition::Define {
            definition: Box::new(definition)
        })
        .await
        .is_err()
    );
    for to in [State::Assessed, State::Copying] {
        c.commit(Transition::Lifecycle {
            event: Event::Advance { to },
        })
        .await
        .unwrap();
    }
    let mut chunk = Chunk {
        table: "public.widgets".into(),
        range_lo: "0".into(),
        range_hi: "10".into(),
        state: "copying".into(),
        low_wm: Some("low".into()),
        high_wm: None,
    };
    c.commit(Transition::Chunk {
        chunk: chunk.clone(),
    })
    .await
    .unwrap();
    chunk.state = "done".into();
    chunk.high_wm = Some("high".into());
    c.commit(Transition::Chunk { chunk }).await.unwrap();
    for saved in ["before", "updated"] {
        c.commit(Transition::FenceState {
            db: "source".into(),
            role: "app".into(),
            saved_privileges_json: saved.into(),
        })
        .await
        .unwrap();
    }
    c.commit(Transition::Lifecycle {
        event: Event::Interrupt {
            to: State::Paused,
            reason: "operator requested pause".into(),
        },
    })
    .await
    .unwrap();
    let row = c.local().read_collection("lifecycle", 1).unwrap().remove(0);
    let giggles_core::relational::Value::Text(blockers) = &row.values[4] else {
        panic!("blockers_json must be text")
    };
    assert_eq!(
        serde_json::from_str::<Vec<String>>(blockers).unwrap(),
        vec!["operator requested pause"]
    );
    c.commit(Transition::Lifecycle {
        event: Event::Resume,
    })
    .await
    .unwrap();
    let row = c.local().read_collection("lifecycle", 1).unwrap().remove(0);
    assert_eq!(
        row.values[1],
        giggles_core::relational::Value::Text("copying".into())
    );
    assert_eq!(
        row.values[3],
        giggles_core::relational::Value::Text("source".into())
    );
    let before = c.state().applied_seq;
    assert!(
        c.commit(Transition::FenceState {
            db: "source".into(),
            role: "app".into(),
            saved_privileges_json: "x".repeat(budget::PRIVILEGE_BYTES + 1)
        })
        .await
        .is_err()
    );
    assert_eq!(j.load(false).await.unwrap().applied_seq, before);
    let state = c.state().clone();
    drop(c);
    assert_eq!(Local::open(&path, &state).unwrap().state(), &state);
    j.checkpoint(&state).await.unwrap();
    assert_eq!(j.load(true).await.unwrap(), state);
}

#[tokio::test]
async fn malformed_checkpoints_and_noncanonical_paths_are_rejected() {
    for mode in ["digest", "state", "seq", "nested", "zero-checkpoint"] {
        let store = fs_store(scratch("bad-checkpoint"));
        let j = journal(store.clone(), "migration");
        let state = history(&j, 3).await;
        j.checkpoint(&state).await.unwrap();
        let mut cp: serde_json::Value = serde_json::from_slice(
            &store
                .get(&j.checkpoint_key(3))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        match mode {
            "digest" => cp["digest"] = "bad".into(),
            "state" => cp["state"]["owner"] = "bad".into(),
            "seq" => cp["seq"] = 4.into(),
            _ => {}
        }
        // Unpublished malformed checkpoint artifacts are found by the full scrub.
        let key = if mode == "nested" {
            Path::from("p2/journal/migration/nested/checkpoint-00000000000000000004.json")
        } else if mode == "zero-checkpoint" {
            j.checkpoint_key(0)
        } else {
            j.checkpoint_key(4)
        };
        store
            .put_opts(
                &key,
                serde_json::to_vec(&cp).unwrap().into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert!(
            matches!(j.scrub().await, Err(Error::RecoveryRequired(_))),
            "{mode}"
        );
    }
    for id in ["", ".", "..", "a/b", "a\\b"] {
        assert!(
            Journal::new(
                fs_store(scratch("bad-id")),
                "p2",
                id,
                Duration::from_secs(1)
            )
            .is_err()
        );
    }
    assert!(Journal::new(fs_store(scratch("bad-timeout")), "p2", "id", Duration::ZERO).is_err());
}

#[tokio::test]
async fn broken_chain_or_gap_is_recovery_required() {
    for mode in [
        "gap",
        "digest",
        "predecessor",
        "epoch",
        "malformed",
        "name",
        "unknown-field",
    ] {
        let store = fs_store(scratch(mode));
        let j = journal(store.clone(), "migration");
        let state = history(&j, 1).await;
        let mut entry = Entry::new(&state, progress(2), "writer").unwrap();
        let mut key = j.entry_key(2);
        match mode {
            "gap" => key = j.entry_key(3),
            "digest" => entry.digest = "0".repeat(64),
            "predecessor" => {
                let mut wrong = state.clone();
                wrong.digest = "0".repeat(64);
                entry = Entry::new(&wrong, progress(2), "writer").unwrap();
            }
            "epoch" => {
                let mut wrong = state.clone();
                wrong.epoch = 9;
                entry = Entry::new(&wrong, progress(2), "writer").unwrap();
            }
            "name" => key = Path::from("p2/journal/migration/wrong.json"),
            _ => {}
        }
        let mut value = serde_json::to_value(entry).unwrap();
        if mode == "unknown-field" {
            value["surprise"] = true.into();
        }
        let bytes = if mode == "malformed" {
            b"not JSON".to_vec()
        } else {
            serde_json::to_vec(&value).unwrap()
        };
        store
            .put_opts(&key, bytes.into(), PutMode::Create.into())
            .await
            .unwrap();
        for checkpoint in [false, true] {
            assert!(
                matches!(j.load(checkpoint).await, Err(Error::RecoveryRequired(_))),
                "{mode}"
            );
        }
    }
}

#[tokio::test]
async fn checkpoints_do_not_hide_corruption_or_replace_history() {
    let j = fresh_journal("checkpoint");
    let state = history(&j, 24).await;
    // Reject a forged projection before any prior checkpoint can mask missing
    // publication validation through its duplicate-state comparison.
    let mut forged = state.clone();
    forged.progress.clear();
    assert!(matches!(
        j.checkpoint(&forged).await,
        Err(Error::RecoveryRequired(_))
    ));
    j.checkpoint(&state).await.unwrap();
    j.checkpoint(&state).await.unwrap();
    assert_eq!(j.load(true).await.unwrap(), state);
    assert_eq!(j.load(false).await.unwrap(), state);
    let mut forged = state.clone();
    forged.progress.clear();
    assert!(matches!(
        j.checkpoint(&forged).await,
        Err(Error::RecoveryRequired(_))
    ));
    let path = scratch("checkpoint-db").join("db");
    let local = Local::rebuild(path, &j.load(true).await.unwrap()).unwrap();
    assert_eq!(local.state(), &state);
    let mut with_tail = state.clone();
    let tail = Entry::new(&state, progress(25), "writer").unwrap();
    j.put(&tail).await.unwrap();
    with_tail.apply(&tail).unwrap();
    assert_eq!(j.scrub().await.unwrap(), with_tail);
    assert_eq!(j.load(true).await.unwrap(), with_tail);
}

#[tokio::test]
async fn truncated_read_is_error() {
    let local = local("truncated");
    assert!(matches!(
        local.read_collection("audit", 1025),
        Err(Error::TruncatedRead(_))
    ));
    assert!(matches!(
        local.read_collection("audit", 0),
        Err(Error::TruncatedRead(_))
    ));
    // Real core result-byte truncation/error, no fake result objects.
    let root = scratch("core-read-limit");
    let path = root.join("db");
    drop(local);
    let local = Local::create(&path).unwrap();
    drop(local);
    let admin = giggles_core::Admin::open(&path, giggles_core::Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    for i in 0..20i64 {
        s.execute(
            "INSERT INTO audit VALUES ($1,'fixture',$2)",
            &[
                giggles_core::relational::Value::Int(i),
                giggles_core::relational::Value::Text("x".repeat(60_000)),
            ],
            None,
        )
        .unwrap();
    }
    s.execute("COMMIT", &[], None).unwrap();
    drop(s);
    drop(admin);
    // Open validates audit and must reject actual corrupted/truncated local data.
    assert!(matches!(
        Local::open(&path, &Materialized::default()),
        Err(Error::TruncatedRead(_))
    ));
}

#[tokio::test]
async fn budget_rejection_preserves_projection_and_sequence() {
    let j = fresh_journal("atomic-failure");
    let state = history(&j, 1).await;
    let path = scratch("atomic-failure-db").join("db");
    let mut local = Local::rebuild(&path, &state).unwrap();
    // Privilege admission fails before publication and preserves all current rows.
    let transition = Transition::FenceState {
        db: "source".into(),
        role: "app".into(),
        saved_privileges_json: "x".repeat(budget::PRIVILEGE_BYTES + 1),
    };
    let entry = Entry::new(&state, transition, "writer").unwrap();
    assert!(local.apply(&entry).is_err());
    assert!(
        local
            .read_collection("fence_state", 128)
            .unwrap()
            .is_empty()
    );
    assert_eq!(local.read_collection("audit", 128).unwrap().len(), 1);
    assert_eq!(local.state(), &state);
    drop(local);
    assert_eq!(Local::open(path, &state).unwrap().state(), &state);
}

#[tokio::test]
async fn paging_exceeds_1024_without_losing_rows() {
    let j = fresh_journal("paging");
    let mut state = history(&j, 1).await;
    for n in 0..1100 {
        let e = Entry::new(
            &state,
            Transition::Chunk {
                chunk: Chunk {
                    table: "public.t".into(),
                    range_lo: format!("{n:06}"),
                    range_hi: format!("{:06}", n + 1),
                    state: "done".into(),
                    low_wm: None,
                    high_wm: None,
                },
            },
            "writer",
        )
        .unwrap();
        j.put(&e).await.unwrap();
        state.apply(&e).unwrap();
    }
    let start = Instant::now();
    let scrubbed = j.load(false).await.unwrap();
    let local = Local::rebuild(scratch("paging-db").join("db"), &scrubbed).unwrap();
    let full_rebuild = start.elapsed();
    let small = local.read_collection("chunks", 37).unwrap();
    let large = local.read_collection("chunks", 1024).unwrap();
    assert_eq!(small, large);
    assert_eq!(small.len(), 1100);
    local.checkpoint(&j).await.unwrap();
    let start = Instant::now();
    let restored = Local::recover(scratch("paging-cp-db").join("db"), &j)
        .await
        .unwrap();
    let checkpoint_rebuild = start.elapsed();
    assert!(
        checkpoint_rebuild < full_rebuild / 4,
        "core restore {checkpoint_rebuild:?}, full replay {full_rebuild:?}"
    );
    assert_eq!(restored.state(), &state);
    assert_eq!(
        restored
            .read_collection("chunks", 1024)
            .unwrap()
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        large.iter().map(|r| r.values.clone()).collect::<Vec<_>>()
    );
    let mut cp = serde_json::to_value(j.recover(true).await.unwrap().checkpoint.unwrap()).unwrap();
    assert!(cp["local_archive"].is_string());
    let expected_digest = cp["digest"].clone();
    seal_checkpoint_json(&mut cp);
    assert_eq!(cp["digest"], expected_digest);
    println!("core checkpoint: full={full_rebuild:?} checkpoint={checkpoint_rebuild:?}");
    let history = entries(&j).await;
    assert_eq!(history.len(), 1101);
    assert_eq!(local.read_collection("audit", 128).unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "LONG RUN: harness/run.ps1 p2; 100,000 real local-FS journal objects are retained on J:"]
async fn journal_long_run() {
    use futures::{StreamExt, TryStreamExt, stream};
    let root = scratch("long-run");
    // Recovery-throughput fixture only. Crash/durability tests use fs_store's
    // fsync=true; avoid benchmarking 100k physical flushes in fixture setup.
    let store = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&root)
            .unwrap()
            .with_automatic_cleanup(false)
            .with_fsync(false),
    );
    let counted = Arc::new(support::counting::CountingStore::new(store));
    let j = journal(counted.clone(), "long");
    let start = Instant::now();
    // Build a complete, chained fixture, then publish using bounded concurrency.
    // This measures recovery, not sequential coordinator append throughput.
    let mut state = Materialized::default();
    let mut fixture_entries = Vec::new();
    for n in 0..100_000 {
        let transition = if n == 0 {
            Transition::AcquireOwnership { epoch: 1 }
        } else {
            progress(n)
        };
        let entry = Entry::new(&state, transition, "writer").unwrap();
        state.apply(&entry).unwrap();
        fixture_entries.push(entry);
    }
    stream::iter(fixture_entries.iter())
        .map(|entry| j.put(entry))
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let write = start.elapsed();
    counted.reset();
    let start = Instant::now();
    let original = j.load(false).await.unwrap();
    let without = start.elapsed();
    let without_requests = counted.counts();
    assert_eq!(original, state);
    j.checkpoint(&state).await.unwrap();
    counted.reset();
    let start = Instant::now();
    let rebuilt = j.load(true).await.unwrap();
    let with = start.elapsed();
    let with_requests = counted.counts();
    assert_eq!(rebuilt, state);
    assert_eq!(with_requests.1, 0, "checkpoint recovery listed history");
    assert!(
        with_requests.0 <= 16,
        "checkpoint recovery made {} GETs",
        with_requests.0
    );
    assert!(without_requests.0 >= 100_000);
    assert!(
        with < without / 2,
        "checkpoint {with:?} must be well below full rebuild {without:?}"
    );
    let bytes = directory_bytes(&root);
    assert!(bytes < 1_000_000_000, "{bytes}");
    let timings = serde_json::json!({"transitions":100_000,"fixture_fsync":false,"fixture_write_seconds":write.as_secs_f64(),"without_checkpoint_seconds":without.as_secs_f64(),"with_checkpoint_seconds":with.as_secs_f64(),"without_checkpoint_gets":without_requests.0,"with_checkpoint_gets":with_requests.0,"with_checkpoint_lists":with_requests.1,"journal_bytes":bytes,"checkpoint_integrity":"validated prefix trusted; checkpoint+anchor+tail checked; full scrub is separate"});
    std::fs::write(
        root.join("long-run-timings.json"),
        serde_json::to_vec_pretty(&timings).unwrap(),
    )
    .unwrap();
    println!("journal_long_run {timings}");
}
fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            if e.path().is_dir() {
                directory_bytes(&e.path())
            } else {
                e.metadata().unwrap().len()
            }
        })
        .sum()
}

#[tokio::test]
#[ignore = "DOCKER REQUIRED: real PG18 authority and idempotency rows; harness/run.ps1 p2"]
async fn stale_epoch_effect_rejected_by_db_authority_row() {
    let cfg = config();
    let (mut client, connection) = tls_connect(cfg["pg"].as_str().unwrap()).await.unwrap();
    let connection = tokio::spawn(async move {
        connection.await.unwrap();
    });
    client.batch_execute("DO $$ BEGIN IF to_regclass('_skedaddle.authority') IS NOT NULL THEN DELETE FROM _skedaddle.authority; END IF; END $$;").await.unwrap();
    initialize_pg_authority(&client).await.unwrap();
    // Reset only rows in this run's isolated PG fixture. Preserve all host files.
    client
        .batch_execute("UPDATE _skedaddle.authority SET epoch=0; DELETE FROM _skedaddle.effects;")
        .await
        .unwrap();
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS business_effects (id bigint PRIMARY KEY); DELETE FROM business_effects;")
        .await
        .unwrap();
    let j = fresh_journal("pg-authority");
    let mut a = coordinator(j.clone(), "a");
    a.acquire().await.unwrap();
    let old_token = Arc::new(tokio::sync::Mutex::new(None));
    let capture = old_token.clone();
    a.effect(
        EffectKind::Fence,
        "INSERT INTO business_effects VALUES (1)".into(),
        move |token| async move {
            *capture.lock().await = Some(token);
            Ok(())
        },
    )
    .await
    .unwrap();
    let mut b = coordinator(j.clone(), "b");
    b.acquire().await.unwrap();
    let token_holder = Arc::new(tokio::sync::Mutex::new(None));
    let capture = token_holder.clone();
    b.effect(
        EffectKind::Activation,
        "INSERT INTO business_effects VALUES (2)".into(),
        move |token| async move {
            *capture.lock().await = Some(token);
            Ok(())
        },
    )
    .await
    .unwrap();
    let token = token_holder.lock().await.take().unwrap();
    apply_pg_effect(&mut client, token.clone()).await.unwrap();
    apply_pg_effect(&mut client, token).await.unwrap();
    assert!(matches!(
        apply_pg_effect(&mut client, old_token.lock().await.take().unwrap()).await,
        Err(Error::LostOwnership)
    ));
    let rows = client
        .query("SELECT id FROM business_effects ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 2);
    assert_eq!(
        client
            .query_one("SELECT epoch FROM _skedaddle.authority", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
#[ignore = "DOCKER REQUIRED: Toxiproxy 2.13.0 slow puts, response loss and real MinIO re-read; harness/run.ps1 p2"]
async fn object_store_latency_and_errors() {
    let cfg = config();
    let api = cfg["toxiproxy"].as_str().unwrap();
    let client = reqwest::Client::new();
    let toxics = format!("{api}/proxies/minio/toxics");
    // A failed mutation may leave a toxic; reset only this run's named proxy.
    let _ = client
        .delete(format!("{toxics}/delay"))
        .send()
        .await
        .unwrap();
    let add = |latency| serde_json::json!({"name":"delay","type":"latency","stream":"downstream","toxicity":1,"attributes":{"latency":latency,"jitter":0}});
    client
        .post(&toxics)
        .json(&add(80))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let store = s3(cfg["proxy_minio"].as_str().unwrap());
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let j = Journal::new(
        store.clone(),
        "p2",
        &format!("latency-{nonce}"),
        Duration::from_secs(2),
    )
    .unwrap();
    let start = Instant::now();
    let state = history(&j, 2).await;
    assert!(start.elapsed() >= Duration::from_millis(160));
    assert_eq!(j.load(false).await.unwrap(), state);
    client
        .delete(format!("{toxics}/delay"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // Delay the successful PUT response past the caller deadline. Remove the
    // downstream toxic while Journal::create is re-reading that same key.
    client
        .post(&toxics)
        .json(&add(1000))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let j = Journal::new(
        store,
        "p2",
        &format!("unknown-outcome-{nonce}"),
        Duration::from_millis(300),
    )
    .unwrap();
    let key = j.entry_key(1);
    let entry = Entry::new(
        &Materialized::default(),
        Transition::AcquireOwnership { epoch: 1 },
        "writer",
    )
    .unwrap();
    let cleanup_client = client.clone();
    let cleanup_url = format!("{toxics}/delay");
    let restore = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        cleanup_client
            .delete(cleanup_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    });
    let result = j.put(&entry).await;
    restore.await.unwrap();
    // A real timed-out upload can complete after cancellation; either exact
    // re-read success or explicit unknown is safe, never assume success.
    assert!(
        result.is_ok() || matches!(result, Err(Error::UnknownOutcome(_))),
        "{result:?}"
    );
    let direct = s3(cfg["minio"].as_str().unwrap());
    let stored: Entry =
        serde_json::from_slice(&direct.get(&key).await.unwrap().bytes().await.unwrap()).unwrap();
    assert_eq!(stored, entry);
    // A retry after connectivity returns is resolved by re-reading the exact
    // create-only key, including when the first call returned UnknownOutcome.
    // Open a fresh transport after the fault: a cancelled HTTP connection can
    // remain unusable even after Toxiproxy confirms removal of the toxic.
    let restored_store = Arc::new(support::counting::CountingStore::new(s3(
        cfg["proxy_minio"].as_str().unwrap(),
    )));
    let recovered = Journal::new(
        restored_store.clone(),
        "p2",
        &format!("unknown-outcome-{nonce}"),
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(recovered.entry_key(1), key);
    recovered.put(&entry).await.unwrap();
    assert!(
        restored_store.counts().0 > 0,
        "the retry must resolve the existing key by re-read"
    );
    assert_eq!(recovered.load(false).await.unwrap().applied_seq, 1);
    // Wrong credentials are a real infrastructure error and cannot commit state.
    let bad = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("p2-bucket")
        .with_region("us-east-1")
        .with_access_key_id("wrong")
        .with_secret_access_key("wrong")
        .with_endpoint(cfg["minio"].as_str().unwrap())
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .unwrap();
    let j = journal(Arc::new(bad), "wrong-credentials");
    assert!(j.put(&entry).await.is_err());
    let missing = journal(
        s3_bucket(cfg["minio"].as_str().unwrap(), "missing-p2-bucket"),
        "missing-bucket",
    );
    assert!(matches!(missing.put(&entry).await, Err(Error::Store(_))));
    // A response-only fault sidecar injects a deterministic HTTP 503 through
    // Toxiproxy. Successful correctness behavior above uses real MinIO.
    let error_endpoint = cfg["proxy_errors"].as_str().unwrap();
    let error_journal = journal(s3(error_endpoint), "http503");
    let error = error_journal.put(&entry).await.unwrap_err();
    assert!(matches!(error, Error::UnknownOutcome(_)));
    assert!(error.to_string().contains("503"), "{error}");
    let mut error_cfg = cfg.clone();
    error_cfg["gcs"] = error_endpoint.into();
    let error_journal = journal(backend("gcs", &error_cfg), "http503-gcs");
    let error = error_journal.put(&entry).await.unwrap_err();
    assert!(matches!(error, Error::UnknownOutcome(_)));
    assert!(error.to_string().contains("503"), "{error}");
}

#[tokio::test]
async fn coordinator_checkpoints_are_owned_periodic_and_restore_core() {
    let j = fresh_journal("periodic-checkpoint");
    let mut c = coordinator(j.clone(), "periodic")
        .with_checkpoint_interval(2)
        .unwrap();
    assert!(matches!(c.checkpoint().await, Err(Error::Observer)));
    assert!(
        coordinator(j.clone(), "invalid-interval")
            .with_checkpoint_interval(0)
            .is_err()
    );
    c.acquire().await.unwrap();
    c.commit(progress(1)).await.unwrap();
    let recovery = j.recover(true).await.unwrap();
    let cp = recovery.checkpoint.unwrap();
    assert_eq!(cp.seq, 2);
    assert!(cp.local_archive.is_some());
    let mut encoded = serde_json::to_value(&cp).unwrap();
    let digest = encoded["digest"].clone();
    seal_checkpoint_json(&mut encoded);
    assert_eq!(
        encoded["digest"], digest,
        "archive bytes must be covered by the independent checksum"
    );
    let fresh = Local::recover(scratch("periodic-new-host").join("db"), &j)
        .await
        .unwrap();
    assert_eq!(fresh.state(), c.state());
    c.checkpoint().await.unwrap();
    let mut successor = coordinator(j.clone(), "successor");
    successor.acquire().await.unwrap();
    assert!(matches!(c.checkpoint().await, Err(Error::LostOwnership)));
    assert!(matches!(c.checkpoint().await, Err(Error::Observer)));
    successor.checkpoint().await.unwrap();
    assert_eq!(j.recover(true).await.unwrap().checkpoint.unwrap().seq, 3);
}

fn independent_json_hash(value: serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(serde_json::to_vec(&value).unwrap()))
}
fn seal_checkpoint_json(cp: &mut serde_json::Value) {
    let fields = if let Some(archive) = cp.get("local_archive") {
        serde_json::json!([
            cp["version"],
            cp["seq"],
            cp["journal_digest"],
            cp["state"],
            archive
        ])
    } else {
        serde_json::json!([cp["version"], cp["seq"], cp["journal_digest"], cp["state"]])
    };
    cp["digest"] = independent_json_hash(fields).into();
}
fn checkpoint_index_json(cp: &serde_json::Value, ordinal: u64) -> serde_json::Value {
    let mut head = serde_json::json!({"version":JOURNAL_FORMAT_VERSION,"ordinal":ordinal,"seq":cp["seq"],"checkpoint_digest":cp["digest"],"journal_digest":cp["journal_digest"]});
    seal_checkpoint_index_json(&mut head);
    head
}
fn seal_checkpoint_index_json(head: &mut serde_json::Value) {
    head["digest"] = independent_json_hash(serde_json::json!([
        head["version"],
        head["ordinal"],
        head["seq"],
        head["checkpoint_digest"],
        head["journal_digest"]
    ]))
    .into();
}

#[tokio::test]
async fn compact_verifier_state_and_active_intents_recover_equivalently() {
    let j = fresh_journal("compact-current-state");
    let mut c = coordinator(j.clone(), "bounded");
    c.acquire().await.unwrap();
    for n in 0..20 {
        c.commit(Transition::VerifierRange {
            id: n.to_string(),
            range: Some(VerifierRange {
                table: "public.t".into(),
                range_lo: n.to_string(),
                range_hi: (n + 1).to_string(),
                source_lsn: "0/100".into(),
                source_digest: "a".repeat(64),
                destination_digest: "b".repeat(64),
                state: "verified".into(),
                rows: 100,
                retries: 0,
            }),
        })
        .await
        .unwrap();
        c.commit(Transition::VerifierException {
            id: n.to_string(),
            exception: Some(VerifierException {
                range: n.to_string(),
                key: "busy-key".into(),
                reason: "busy".into(),
            }),
        })
        .await
        .unwrap();
    }
    c.commit(Transition::VerifierRange {
        id: "0".into(),
        range: None,
    })
    .await
    .unwrap();
    c.commit(Transition::VerifierException {
        id: "0".into(),
        exception: None,
    })
    .await
    .unwrap();
    assert!(
        c.effect(EffectKind::Fence, "pending".into(), |_| async {
            Err(Error::Rejected("injected pre-effect stop".into()))
        })
        .await
        .is_err()
    );
    let pending = c.state().applied_seq;
    let expected = c.state().clone();
    assert_eq!(expected.audit.len(), 1);
    c.checkpoint().await.unwrap();
    assert_eq!(j.load(false).await.unwrap(), expected);
    assert_eq!(j.load(true).await.unwrap(), expected);
    let rebuilt = Local::rebuild(
        scratch("compact-rebuilt").join("db"),
        &j.load(false).await.unwrap(),
    )
    .unwrap();
    let restored = Local::recover(scratch("compact-restored").join("db"), &j)
        .await
        .unwrap();
    assert_eq!(rebuilt.state(), restored.state());
    for table in ["audit", "verifier_ranges", "verifier_exceptions"] {
        assert_eq!(
            rebuilt
                .read_collection(table, 2)
                .unwrap()
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            restored
                .read_collection(table, 2)
                .unwrap()
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        );
    }
    c.retry_effect(pending, |_| async { Ok(()) }).await.unwrap();
    assert!(c.state().intent_roots.is_empty());
    assert_eq!(c.state().audit.len(), 1);
    assert_eq!(
        j.history_page_through(0, 128, c.state().applied_seq)
            .await
            .unwrap()
            .len(),
        c.state().applied_seq as usize
    );
}

#[tokio::test]
async fn oversized_effect_returns_typed_blocker_before_journal_commit() {
    let j = fresh_journal("typed-admission");
    let mut c = coordinator(j.clone(), "bounded");
    c.acquire().await.unwrap();
    let before = c.state().clone();
    let error = c
        .effect(
            EffectKind::Fence,
            "x".repeat(budget::ROW_BYTES),
            |_| async { panic!("oversized effect ran") },
        )
        .await
        .unwrap_err();
    let Error::BudgetExceeded(blocker) = error else {
        panic!("expected typed budget blocker")
    };
    assert_eq!(blocker.budget, budget::Budget::Row);
    assert!(!blocker.remediation.is_empty());
    assert_eq!(c.state(), &before);
    assert_eq!(j.load(false).await.unwrap(), before);
}

#[tokio::test]
async fn lagging_cache_reopens_without_a_history_clone() {
    let j = fresh_journal("lagging-bounded-cache");
    let mut state = history(&j, 1).await;
    let path = scratch("lagging-bounded-db").join("db");
    drop(Local::rebuild(&path, &state).unwrap());
    for n in 2..=6 {
        let entry = Entry::new(&state, progress(n), "writer").unwrap();
        j.put(&entry).await.unwrap();
        state.apply(&entry).unwrap();
    }
    let reopened = Local::open_journal(path, &j).await.unwrap();
    assert_eq!(reopened.state(), &state);
    assert_eq!(reopened.state().audit.len(), 1);
}

#[tokio::test]
async fn old_adoption_aliases_rebuild_without_retaining_the_chain() {
    let j = fresh_journal("old-aliases");
    let mut state = Materialized::default();
    for transition in [
        Transition::AcquireOwnership { epoch: 1 },
        Transition::Intent {
            effect: EffectKind::Fence,
            payload: "original".into(),
        },
        Transition::AcquireOwnership { epoch: 2 },
        Transition::ResumeEffect { intent_seq: 2 },
        Transition::AcquireOwnership { epoch: 3 },
        Transition::ResumeEffect { intent_seq: 4 },
        Transition::AcquireOwnership { epoch: 4 },
        Transition::ResumeEffect { intent_seq: 4 },
    ] {
        let entry = Entry::new(&state, transition, "writer").unwrap();
        j.put(&entry).await.unwrap();
        state = j.load(false).await.unwrap();
    }
    assert_eq!(state.audit.keys().copied().collect::<Vec<_>>(), [2, 8]);
    assert_eq!(state.intent_roots.get(&8), Some(&2));
    j.checkpoint(&state).await.unwrap();
    assert_eq!(j.load(true).await.unwrap(), state);
}

#[tokio::test]
async fn historical_pages_never_hide_a_gap_before_the_verified_head() {
    let j = fresh_journal("history-gap");
    let state = history(&j, 1).await;
    let mut entry = Entry::new(&state, progress(3), "writer").unwrap();
    entry.seq = 3;
    independent_seal(&mut entry);
    j.put(&entry).await.unwrap();
    assert!(matches!(
        j.history_page_through(0, 128, 3).await,
        Err(Error::RecoveryRequired(_))
    ));
    assert!(matches!(
        j.history_page(0, 0).await,
        Err(Error::TruncatedRead(_))
    ));
}
async fn put_json(store: &Arc<dyn ObjectStore>, key: &Path, value: &serde_json::Value) {
    store
        .put_opts(
            key,
            serde_json::to_vec(value).unwrap().into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
}

fn assert_unsupported_version(error: Error, format: &str, found: Option<u16>) {
    let expected = if format.starts_with("journal-") {
        JOURNAL_FORMAT_VERSION
    } else {
        skedaddle::buffer::format::FORMAT_VERSION
    };
    assert!(
        matches!(&error, Error::UnsupportedFormatVersion {
            format: actual, found: actual_found, expected: actual_expected,
        } if *actual == format && *actual_found == found && *actual_expected == expected),
        "{error:?}"
    );
}

#[tokio::test]
async fn journal_versions_are_rejected_before_payload_decode_and_raw_rebuild() {
    let original = fresh_journal("version-original");
    let state = history(&original, 1).await;
    original.checkpoint(&state).await.unwrap();
    let cp =
        serde_json::to_value(original.recover(true).await.unwrap().checkpoint.unwrap()).unwrap();
    let head = checkpoint_index_json(&cp, 1);
    let entry = serde_json::to_value(&state.audit[&1]).unwrap();
    for format in [
        "journal-entry",
        "journal-checkpoint",
        "journal-checkpoint-head",
    ] {
        for found in [None, Some(1), Some(2), Some(4)] {
            for minimal in [false, true] {
                let store = fs_store(scratch("unsupported-journal-version"));
                let j = journal(store.clone(), "migration");
                let mut objects = [entry.clone(), cp.clone(), head.clone()];
                let index = match format {
                    "journal-entry" => 0,
                    "journal-checkpoint" => 1,
                    _ => 2,
                };
                let object = &mut objects[index];
                if minimal {
                    *object = serde_json::json!({});
                }
                match found {
                    Some(version) => object["version"] = version.into(),
                    None => {
                        object.as_object_mut().unwrap().remove("version");
                    }
                }
                for (key, object) in [
                    j.entry_key(1),
                    j.checkpoint_key(1),
                    j.checkpoint_index_key(1),
                ]
                .iter()
                .zip(&objects)
                {
                    put_json(&store, key, object).await;
                }
                assert_unsupported_version(j.load(true).await.unwrap_err(), format, found);
                assert_unsupported_version(j.scrub().await.unwrap_err(), format, found);
                if index == 0 {
                    assert_unsupported_version(j.entry(1).await.unwrap_err(), format, found);
                    // Rebuild deliberately skips derived checkpoints, but must
                    // reject an old raw entry before copying any of its bytes.
                    let destination = fresh_journal("unsupported-version-rebuild");
                    assert_unsupported_version(
                        j.copy_entries_for_rebuild(&destination).await.unwrap_err(),
                        format,
                        found,
                    );
                    assert!(destination.entry(1).await.is_err());
                }
            }
        }
    }
}

#[tokio::test]
async fn journal_version_is_authenticated_by_each_digest() {
    let original = fresh_journal("version-digests");
    let state = history(&original, 1).await;
    original.checkpoint(&state).await.unwrap();
    let cp =
        serde_json::to_value(original.recover(true).await.unwrap().checkpoint.unwrap()).unwrap();
    let head = checkpoint_index_json(&cp, 1);
    let entry = serde_json::to_value(&state.audit[&1]).unwrap();
    for index in 0..3 {
        let store = fs_store(scratch("tampered-journal-version"));
        let j = journal(store.clone(), "migration");
        let mut objects = [entry.clone(), cp.clone(), head.clone()];
        let object = &mut objects[index];
        // Seal a different version, then change only the version to the
        // supported envelope. Version admission passes; integrity must fail.
        object["version"] = 1.into();
        match index {
            0 => {
                let mut entry: Entry = serde_json::from_value(object.clone()).unwrap();
                independent_seal(&mut entry);
                *object = serde_json::to_value(entry).unwrap();
            }
            1 => seal_checkpoint_json(object),
            _ => seal_checkpoint_index_json(object),
        }
        object["version"] = JOURNAL_FORMAT_VERSION.into();
        for (key, object) in [
            j.entry_key(1),
            j.checkpoint_key(1),
            j.checkpoint_index_key(1),
        ]
        .iter()
        .zip(&objects)
        {
            put_json(&store, key, object).await;
        }
        let error = j.load(true).await.unwrap_err();
        assert!(matches!(error, Error::RecoveryRequired(_)), "{error:?}");
        assert!(
            error.to_string().contains(match index {
                0 => "invalid entry/digest",
                1 => "checkpoint digest/state mismatch",
                _ => "checkpoint discovery record is corrupt",
            }),
            "{error}"
        );
    }
}

#[tokio::test]
async fn buffer_version_error_survives_chunk_publication_and_journal_replay() {
    for format in ["manifest", "segment", "chunk-rows"] {
        let store = fs_store(scratch("buffer-version-journal"));
        let (_, config) = chunk_buffer();
        let root = scratch("obsolete-buffer-object");
        let buffer: Arc<dyn skedaddle::buffer::backends::BufferStore> = Arc::new(
            skedaddle::buffer::backends::ObjectBufferStore::local(&root, Duration::from_secs(3))
                .unwrap(),
        );
        let j = journal(store.clone(), "migration").with_chunk_buffer(
            buffer.clone(),
            config.clone(),
            Arc::new(ChunkOrder),
        );
        let mut c = coordinator(j.clone(), "buffer-version");
        c.acquire().await.unwrap();
        let plan = typed_plan();
        c.commit(Transition::ChunkPlanned {
            plan: j.publish_chunk_plan(&plan).await.unwrap(),
        })
        .await
        .unwrap();
        let chunk = plan.ranges[0].clone();
        c.commit(Transition::ChunkStarted {
            chunk: chunk.clone(),
            attempt: 1,
        })
        .await
        .unwrap();
        j.checkpoint(c.state()).await.unwrap();
        let rows = chunk_manifest(
            buffer.as_ref(),
            &config,
            &chunk,
            1,
            100,
            format != "manifest",
            false,
        )
        .await;
        if format == "manifest" {
            buffer
                .put_create_only(&config.manifest_key(rows.mseq), br#"{"version":1}"#)
                .await
                .unwrap();
        } else {
            let manifest = skedaddle::buffer::format::ManifestEntry::from_canonical_json(
                &buffer
                    .get(&config.manifest_key(rows.mseq))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let mut bytes = buffer.get(&manifest.segment_key).await.unwrap().unwrap();
            let version_at = if format == "segment" {
                8
            } else {
                bytes.windows(4).position(|part| part == b"SKCR").unwrap() + 4
            };
            bytes[version_at..version_at + 2].copy_from_slice(&1_u16.to_be_bytes());
            std::fs::write(root.join(&manifest.segment_key), bytes).unwrap();
        }
        let done = Transition::ChunkDone {
            chunk,
            attempt: 1,
            high: skedaddle::event::Position::new("pg-chunk-test", 100u64.to_be_bytes()),
            chunk_rows: rows,
        };
        assert_unsupported_version(c.commit(done.clone()).await.unwrap_err(), format, Some(1));
        let entry = Entry::new(c.state(), done, c.instance_id()).unwrap();
        put_json(
            &store,
            &j.entry_key(entry.seq),
            &serde_json::to_value(entry).unwrap(),
        )
        .await;
        for checkpoint in [false, true] {
            assert_unsupported_version(j.load(checkpoint).await.unwrap_err(), format, Some(1));
        }
    }
}

#[tokio::test]
async fn full_scrub_finds_corrupt_old_object_but_checkpoint_recovery_succeeds() {
    let original_store = fs_store(scratch("scrub-original"));
    let original = journal(original_store.clone(), "migration");
    let mut expected = history(&original, 6).await;
    original.checkpoint(&expected).await.unwrap();
    let cp = original_store
        .get(&original.checkpoint_key(6))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let head = original_store
        .get(&original.checkpoint_index_key(1))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let store = fs_store(scratch("scrub-corrupt-prefix"));
    let j = journal(store.clone(), "migration");
    for entry in entries(&original).await.iter() {
        let mut entry = entry.clone();
        if entry.seq == 2 {
            entry.transition = progress(999);
            independent_seal(&mut entry);
        }
        j.put(&entry).await.unwrap();
    }
    store
        .put_opts(&j.checkpoint_key(6), cp.into(), PutMode::Create.into())
        .await
        .unwrap();
    store
        .put_opts(
            &j.checkpoint_index_key(1),
            head.into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let tail = Entry::new(&expected, progress(7), "writer").unwrap();
    j.put(&tail).await.unwrap();
    expected.apply(&tail).unwrap();
    assert_eq!(j.load(true).await.unwrap(), expected);
    assert!(matches!(j.scrub().await, Err(Error::RecoveryRequired(_))));
}

#[tokio::test]
async fn checkpoint_validates_its_own_digest_and_state_without_an_anchor_object() {
    let original_store = fs_store(scratch("checkpoint-proof"));
    let original = journal(original_store.clone(), "migration");
    let mut expected = history(&original, 5).await;
    let lifecycle = Entry::new(
        &expected,
        Transition::Lifecycle {
            event: Event::Advance {
                to: State::Assessed,
            },
        },
        "writer",
    )
    .unwrap();
    original.put(&lifecycle).await.unwrap();
    expected.apply(&lifecycle).unwrap();
    assert_eq!(expected.lifecycle.since_seq, 6);
    original.checkpoint(&expected).await.unwrap();
    let original_cp: serde_json::Value = serde_json::from_slice(
        &original_store
            .get(&original.checkpoint_key(6))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    let mut independently_sealed = original_cp.clone();
    seal_checkpoint_json(&mut independently_sealed);
    assert_eq!(original_cp["digest"], independently_sealed["digest"]);
    for mode in [
        "valid",
        "self-digest",
        "head-vs-state",
        "seq-vs-state",
        "snapshot-state",
        "index-digest",
        "state-applied-seq",
        "state-digest",
        "epoch",
        "audit-gap",
        "audit-key",
        "lifecycle-since",
        "recorded-head-digest",
        "head-seq",
        "index-ordinal",
        "index-seq",
        "index-checkpoint-digest",
        "index-journal-digest",
    ] {
        let store = fs_store(scratch(mode));
        let j = journal(store.clone(), "migration");
        let mut cp = original_cp.clone();
        match mode {
            "self-digest" => cp["digest"] = "invalid".into(),
            "head-vs-state" => {
                cp["journal_digest"] = "invalid".into();
                seal_checkpoint_json(&mut cp);
            }
            "seq-vs-state" => {
                cp["seq"] = 7.into();
                seal_checkpoint_json(&mut cp);
            }
            "snapshot-state" => {
                cp["state"]["owner"] = "invalid".into();
                seal_checkpoint_json(&mut cp);
            }
            "state-applied-seq" => {
                cp["state"]["applied_seq"] = 7.into();
                seal_checkpoint_json(&mut cp);
            }
            "state-digest" => {
                cp["state"]["digest"] = "invalid".into();
                seal_checkpoint_json(&mut cp);
            }
            "epoch" => {
                cp["state"]["epoch"] = 2.into();
                seal_checkpoint_json(&mut cp);
            }
            "audit-gap" => {
                cp["state"]["audit"].as_object_mut().unwrap().remove("6");
                seal_checkpoint_json(&mut cp);
            }
            "audit-key" => {
                let head = cp["state"]["audit"]
                    .as_object_mut()
                    .unwrap()
                    .remove("6")
                    .unwrap();
                cp["state"]["audit"]["7"] = head;
                seal_checkpoint_json(&mut cp);
            }
            "lifecycle-since" => {
                cp["state"]["lifecycle"]["since_seq"] = 7.into();
                seal_checkpoint_json(&mut cp);
            }
            "recorded-head-digest" | "head-seq" => {
                let mut head: Entry =
                    serde_json::from_value(cp["state"]["audit"]["6"].clone()).unwrap();
                if mode == "head-seq" {
                    head.seq = 7;
                } else {
                    head.transition = progress(999);
                }
                independent_seal(&mut head);
                cp["state"]["audit"]["6"] = serde_json::to_value(&head).unwrap();
                if mode == "head-seq" {
                    cp["state"]["digest"] = head.digest.clone().into();
                    cp["journal_digest"] = head.digest.into();
                }
                seal_checkpoint_json(&mut cp);
            }
            _ => {}
        }
        let seq = cp["seq"].as_u64().unwrap();
        let mut head = checkpoint_index_json(&cp, 1);
        if mode == "index-digest" {
            head["digest"] = "invalid".into();
        }
        match mode {
            "index-ordinal" => head["ordinal"] = 2.into(),
            "index-seq" => head["seq"] = 7.into(),
            "index-checkpoint-digest" => head["checkpoint_digest"] = "invalid".into(),
            "index-journal-digest" => head["journal_digest"] = "invalid".into(),
            _ => {}
        }
        if mode.starts_with("index-") && mode != "index-digest" {
            seal_checkpoint_index_json(&mut head);
        }
        put_json(
            &store,
            &j.checkpoint_key(if mode == "index-seq" { 7 } else { seq }),
            &cp,
        )
        .await;
        put_json(&store, &j.checkpoint_index_key(1), &head).await;
        if mode == "valid" {
            assert_eq!(j.load(true).await.unwrap(), expected);
            assert!(matches!(j.scrub().await, Err(Error::RecoveryRequired(_))));
        } else {
            assert!(
                matches!(j.load(true).await, Err(Error::RecoveryRequired(_))),
                "{mode}"
            );
        }
    }
}

#[tokio::test]
async fn newest_checkpoint_failure_never_falls_back_and_tail_uses_recorded_head() {
    let original_store = fs_store(scratch("newest-checkpoint"));
    let original = journal(original_store.clone(), "migration");
    let expected = history(&original, 6).await;
    let mut prior = Materialized::default();
    for entry in entries(&original).await.iter().take(3) {
        prior.apply(entry).unwrap();
    }
    original.checkpoint(&prior).await.unwrap();
    original.checkpoint(&expected).await.unwrap();
    for mode in [
        "newest-digest",
        "anchor-mismatch",
        "orphan-anchor-mismatch",
        "bad-tail-link",
        "bad-tail-seq",
    ] {
        let store = fs_store(scratch(mode));
        let j = journal(store.clone(), "migration");
        for entry in entries(&original).await.iter() {
            let mut entry = entry.clone();
            if matches!(mode, "anchor-mismatch" | "orphan-anchor-mismatch") && entry.seq == 6 {
                entry.transition = progress(999);
                independent_seal(&mut entry);
            }
            j.put(&entry).await.unwrap();
        }
        if mode == "anchor-mismatch" {
            assert!(
                matches!(
                    j.checkpoint(&expected).await,
                    Err(Error::RecoveryRequired(_))
                ),
                "publication must reject a different validly hashed anchor"
            );
        }
        for seq in [3, 6] {
            let mut cp: serde_json::Value = serde_json::from_slice(
                &original_store
                    .get(&original.checkpoint_key(seq))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
            )
            .unwrap();
            if mode == "newest-digest" && seq == 6 {
                cp["digest"] = "invalid".into();
            }
            put_json(&store, &j.checkpoint_key(seq), &cp).await;
        }
        for ordinal in [1, 2] {
            if mode == "orphan-anchor-mismatch" && ordinal == 2 {
                continue;
            }
            let bytes = original_store
                .get(&original.checkpoint_index_key(ordinal))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            store
                .put_opts(
                    &j.checkpoint_index_key(ordinal),
                    bytes.into(),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
        }
        if mode.starts_with("bad-tail") {
            let mut tail = Entry::new(&expected, progress(7), "writer").unwrap();
            if mode == "bad-tail-link" {
                tail.prev_digest = "invalid".into();
            } else {
                tail.seq = 8;
            }
            independent_seal(&mut tail);
            put_json(
                &store,
                &j.entry_key(7),
                &serde_json::to_value(tail).unwrap(),
            )
            .await;
        }
        if mode == "orphan-anchor-mismatch" {
            let recovered = j.load(true).await.unwrap();
            assert_eq!(recovered.applied_seq, 6);
            assert_eq!(recovered.progress["s"].1, "999");
            assert!(
                matches!(j.scrub().await, Err(Error::RecoveryRequired(_))),
                "a scrub must bind even an unpublished checkpoint to history"
            );
        } else {
            assert!(
                matches!(j.load(true).await, Err(Error::RecoveryRequired(_))),
                "{mode}"
            );
        }
    }
}

#[tokio::test]
async fn checkpoint_discovery_uses_bounded_gets_and_no_listing() {
    let counted = Arc::new(support::counting::CountingStore::new(fs_store(scratch(
        "checkpoint-discovery",
    ))));
    let j = journal(counted.clone(), "migration");
    let state = history(&j, 64).await;
    let mut cp = Materialized::default();
    for entry in entries(&j).await.iter() {
        cp.apply(entry).unwrap();
        if cp.applied_seq % 8 == 0 {
            j.checkpoint(&cp).await.unwrap();
        }
    }
    counted.reset();
    assert_eq!(j.load(true).await.unwrap(), state);
    let (gets, lists) = counted.counts();
    assert_eq!(lists, 0);
    assert!(gets <= 16, "discovery used {gets} GETs");
    // Publishing an older snapshot must not regress the discovery head.
    let mut old = Materialized::default();
    for entry in entries(&j).await.iter().take(8) {
        old.apply(entry).unwrap();
    }
    j.checkpoint(&old).await.unwrap();
    assert_eq!(j.load(true).await.unwrap(), state);
}

// Exact P2 checkpoint wire shape: all ten original Materialized fields, full
// audit, no D3 fields, and the independently sealed P2 discovery-head format.
async fn p2_legacy_fixture(
    name: &str,
    present: impl Fn(u64) -> bool,
) -> (
    std::path::PathBuf,
    Arc<dyn ObjectStore>,
    Journal,
    Materialized,
) {
    let root = scratch(name);
    let store = fs_store(&root);
    let j = Journal::new(store.clone(), "p2", "fixture", Duration::from_secs(3)).unwrap();
    let mut state = Materialized::default();
    let mut audit = std::collections::BTreeMap::new();
    for transition in [
        Transition::AcquireOwnership { epoch: 1 },
        Transition::Intent {
            effect: EffectKind::BufferPublication,
            payload: "published".into(),
        },
        Transition::EffectCompleted { intent_seq: 2 },
        progress(4),
        progress(5),
        progress(6),
    ] {
        let entry = Entry::new(&state, transition, "writer").unwrap();
        if present(entry.seq) {
            // Pretty JSON also proves the copy preserves original raw bytes.
            store
                .put_opts(
                    &j.entry_key(entry.seq),
                    serde_json::to_vec_pretty(&entry).unwrap().into(),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
        }
        state.apply(&entry).unwrap();
        audit.insert(entry.seq, entry);
    }
    let mut legacy = serde_json::to_value(&state).unwrap();
    for field in ["intent_roots", "verifier_ranges", "verifier_exceptions"] {
        assert!(legacy.as_object_mut().unwrap().remove(field).is_some());
    }
    legacy["audit"] = serde_json::to_value(audit).unwrap();
    assert_eq!(legacy.as_object().unwrap().len(), 10);
    let mut cp = serde_json::json!({"version":JOURNAL_FORMAT_VERSION,"seq":state.applied_seq,"journal_digest":state.digest,"state":legacy});
    seal_checkpoint_json(&mut cp);
    let digest = cp["digest"].clone();
    seal_checkpoint_json(&mut cp);
    assert_eq!(cp["digest"], digest);
    let head = checkpoint_index_json(&cp, 1);
    put_json(&store, &j.checkpoint_key(6), &cp).await;
    put_json(&store, &j.checkpoint_index_key(1), &head).await;
    (root, store, j, state)
}

async fn stored_object_bytes(
    store: &Arc<dyn ObjectStore>,
) -> std::collections::BTreeMap<Path, Vec<u8>> {
    use futures::TryStreamExt;
    let mut listing = store.list(None);
    let mut bytes = std::collections::BTreeMap::new();
    while let Some(meta) = listing.try_next().await.unwrap() {
        bytes.insert(
            meta.location.clone(),
            store
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec(),
        );
    }
    bytes
}

#[tokio::test]
async fn p2_checkpoint_requires_explicit_preserving_store_rebuild() {
    let (root, store, original, expected) = p2_legacy_fixture("legacy-p2-source", |_| true).await;
    println!("P2_LEGACY_SOURCE={}", root.display());
    let before = stored_object_bytes(&store).await;
    for checkpoint in [false, true] {
        let error = original.load(checkpoint).await.unwrap_err();
        assert!(matches!(error, Error::RecoveryRequired(_)));
        assert!(error.to_string().contains("intent_roots"), "{error}");
    }
    let replacement_store = fs_store(scratch("legacy-p2-replacement"));
    let replacement = Journal::new(
        replacement_store.clone(),
        "p2",
        "fixture",
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(original.namespace(), replacement.namespace());
    let copied = original
        .copy_entries_for_rebuild(&replacement)
        .await
        .unwrap();
    assert_eq!(copied, expected);
    assert_eq!(copied.audit.len(), 1);
    let raw = stored_object_bytes(&replacement_store).await;
    assert_eq!(raw.len(), 6, "no derived checkpoint/index is copied");
    for seq in 1..=6 {
        assert_eq!(
            raw[&replacement.entry_key(seq)],
            before[&original.entry_key(seq)]
        );
    }
    assert_eq!(replacement.load(false).await.unwrap(), expected);
    let local = Local::rebuild(scratch("legacy-p2-new-local").join("db"), &copied).unwrap();
    local.checkpoint(&replacement).await.unwrap();
    assert_eq!(replacement.load(true).await.unwrap(), expected);
    assert_eq!(replacement.scrub().await.unwrap(), expected);
    let mut owner = Coordinator::observer(replacement, local, "migrated").unwrap();
    assert_eq!(owner.acquire().await.unwrap(), 2);
    owner
        .resume_effect(2, |_| async { panic!("completed P2 effect was repeated") })
        .await
        .unwrap();
    assert_eq!(
        stored_object_bytes(&store).await,
        before,
        "original history, legacy checkpoint and discovery head remain byte-exact"
    );
    assert!(matches!(
        original.load(true).await,
        Err(Error::RecoveryRequired(_))
    ));
}

#[tokio::test]
async fn explicit_legacy_copy_rejects_incomplete_or_unsafe_destinations() {
    for gap in [true, false] {
        let (_, _, original, _) = p2_legacy_fixture("legacy-p2-incomplete", |seq| {
            if gap { seq != 4 } else { seq <= 3 }
        })
        .await;
        let store = fs_store(scratch("legacy-p2-incomplete-target"));
        let replacement =
            Journal::new(store.clone(), "p2", "fixture", Duration::from_secs(3)).unwrap();
        assert!(matches!(
            original.copy_entries_for_rebuild(&replacement).await,
            Err(Error::RecoveryRequired(_))
        ));
        assert!(stored_object_bytes(&store).await.is_empty());
    }
    let (_, _, original, _) = p2_legacy_fixture("legacy-p2-target-guards", |_| true).await;
    let store = fs_store(scratch("legacy-p2-renamed-target"));
    let renamed = Journal::new(store.clone(), "p2", "different", Duration::from_secs(3)).unwrap();
    assert!(matches!(
        original.copy_entries_for_rebuild(&renamed).await,
        Err(Error::Rejected(_))
    ));
    assert!(stored_object_bytes(&store).await.is_empty());
    let occupied = Journal::new(store.clone(), "p2", "fixture", Duration::from_secs(3)).unwrap();
    occupied
        .put(&original.entry(1).await.unwrap())
        .await
        .unwrap();
    let before = stored_object_bytes(&store).await;
    assert!(matches!(
        original.copy_entries_for_rebuild(&occupied).await,
        Err(Error::Rejected(_))
    ));
    assert_eq!(stored_object_bytes(&store).await, before);
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}

#[tokio::test]
async fn incremental_refresh_reads_only_anchor_and_suffix_and_matches_full_replay() {
    let counted = Arc::new(support::counting::CountingStore::new(fs_store(scratch(
        "refresh-io",
    ))));
    let j = journal(counted.clone(), "migration");
    let mut c = coordinator(j.clone(), "owner")
        .with_checkpoint_interval(100_000)
        .unwrap();
    c.acquire().await.unwrap();
    let mut state = c.state().clone();
    let mut random = 17u64;
    for n in 0..512 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let e = Entry::new(&state, support::progress(random), c.instance_id()).unwrap();
        j.put(&e).await.unwrap();
        state.apply(&e).unwrap();
        if n % 31 == 0 {
            c.refresh().await.unwrap();
            assert_eq!(c.state(), &j.load(true).await.unwrap());
        }
    }
    c.refresh().await.unwrap();
    counted.reset();
    c.refresh().await.unwrap();
    assert_eq!(counted.counts(), (2, 1)); // anchor + absent first checkpoint index
    let e = Entry::new(&state, support::progress(513), c.instance_id()).unwrap();
    j.put(&e).await.unwrap();
    counted.reset();
    c.refresh().await.unwrap();
    assert_eq!(counted.counts(), (4, 1)); // anchor, index, suffix, Local anchor
    assert_eq!(c.state(), &j.load(true).await.unwrap());
}

#[tokio::test]
async fn incremental_refresh_rejects_missing_anchor_gap_and_bad_digest() {
    use object_store::ObjectStoreExt;
    for fault in ["anchor", "gap", "digest", "checkpoint"] {
        let store = fs_store(scratch(fault));
        let j = journal(store.clone(), "migration");
        let mut c = coordinator(j.clone(), "owner");
        c.acquire().await.unwrap();
        c.commit(support::progress(2)).await.unwrap();
        c.refresh().await.unwrap();
        let e = Entry::new(c.state(), support::progress(3), c.instance_id()).unwrap();
        j.put(&e).await.unwrap();
        match fault {
            "anchor" => store.delete(&j.entry_key(2)).await.unwrap(),
            "gap" => {
                let mut state = c.state().clone();
                state.apply(&e).unwrap();
                let next = Entry::new(&state, support::progress(4), c.instance_id()).unwrap();
                j.put(&next).await.unwrap();
                store.delete(&j.entry_key(3)).await.unwrap();
            }
            "digest" => {
                let mut bad = e.clone();
                bad.digest = "bad".into();
                store
                    .put(&j.entry_key(3), serde_json::to_vec(&bad).unwrap().into())
                    .await
                    .unwrap();
            }
            "checkpoint" => {
                j.checkpoint(c.state()).await.unwrap();
                store
                    .put(&j.checkpoint_index_key(1), b"corrupt".to_vec().into())
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(c.refresh().await.is_err(), "{fault}");
        assert!(!c.is_owner(), "{fault}");
    }
}

#[tokio::test]
async fn incremental_refresh_rebases_checkpoint_and_fences_takeover() {
    let j = fresh_journal("refresh-rebase");
    let mut c = coordinator(j.clone(), "owner");
    c.acquire().await.unwrap();
    c.commit(support::progress(2)).await.unwrap();
    c.refresh().await.unwrap();
    j.checkpoint(c.state()).await.unwrap();
    c.refresh().await.unwrap();
    assert_eq!(c.state(), &j.load(true).await.unwrap());
    let mut other = coordinator(j.clone(), "other");
    other.acquire().await.unwrap();
    c.refresh().await.unwrap();
    assert!(!c.is_owner());
    assert_eq!(c.state(), other.state());
}

#[tokio::test]
#[ignore = "local refresh scalability measurement; run explicitly with --nocapture"]
async fn incremental_refresh_scalability() {
    use std::time::Instant;
    for count in [250, 2000, 10000] {
        let j = fresh_journal("refresh-bench");
        let mut c = coordinator(j.clone(), "owner")
            .with_checkpoint_interval(100_000)
            .unwrap();
        c.acquire().await.unwrap();
        let mut state = c.state().clone();
        for n in 1..count {
            let e = Entry::new(&state, support::progress(n), c.instance_id()).unwrap();
            j.put(&e).await.unwrap();
            state.apply(&e).unwrap();
        }
        c.refresh().await.unwrap();
        // A fresh observer has no process cache: this is the original
        // full-load + sync_verified refresh path, with setup outside timing.
        let mut baseline = Coordinator::observer(
            j.clone(),
            Local::rebuild(scratch("refresh-baseline").join("db"), c.state()).unwrap(),
            "baseline",
        )
        .unwrap();
        let start = Instant::now();
        baseline.refresh().await.unwrap();
        let full_empty = start.elapsed();
        let start = Instant::now();
        c.refresh().await.unwrap();
        let incremental_empty = start.elapsed();
        for n in count..count + 3 {
            let e = Entry::new(&state, support::progress(n), c.instance_id()).unwrap();
            j.put(&e).await.unwrap();
            state.apply(&e).unwrap();
        }
        let mut baseline = Coordinator::observer(
            j.clone(),
            Local::rebuild(scratch("refresh-baseline-tail").join("db"), c.state()).unwrap(),
            "baseline",
        )
        .unwrap();
        let start = Instant::now();
        baseline.refresh().await.unwrap();
        let full_tail = start.elapsed();
        let start = Instant::now();
        c.refresh().await.unwrap();
        println!(
            "refresh {count}: full_no_tail={full_empty:?} incremental_no_tail={incremental_empty:?} full_3_tail={full_tail:?} incremental_3_tail={:?}",
            start.elapsed()
        );
        assert_eq!(c.state(), &state);
    }
}
