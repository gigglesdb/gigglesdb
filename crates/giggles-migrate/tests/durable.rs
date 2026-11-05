use giggles_core::{CrashPoint, relational::Value as V};
use giggles_migrate::*;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
fn fresh() -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-migrate-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        ID.fetch_add(1, Ordering::Relaxed)
    ))
}
fn manifest() -> Manifest {
    Manifest {
        version: 1,
        cell_semantics: CellSemantics {
            timestamp_unit: TimestampUnit::MicrosecondsI64,
            expiry: ExpiryContract::AbsoluteMicrosecondsExclusive,
            tombstone_retention: TombstoneRetention::PermanentForRehearsal,
            tombstone_scopes: vec![
                TombstoneScope::Cell,
                TombstoneScope::Row,
                TombstoneScope::Partition,
                TombstoneScope::Range,
            ],
        },
        source: "s".into(),
        table: "messages".into(),
        schema_epoch: 1,
        topology_epoch: 2,
        partition_types: vec![ScalarType::Int],
        clustering_types: vec![ScalarType::Int],
        clustering_order: vec![Direction::Asc],
        columns: BTreeMap::from([("body".into(), ScalarType::Text)]),
        streams: vec![Stream {
            id: "a".into(),
            start: 0,
            end: 2,
        }],
        horizon_micros: 100,
        tie_policy: "reject_ambiguous".into(),
        unsupported_features: vec![],
        bounds: Bounds {
            max_events: 64,
            max_chunk_bytes: 48000,
            max_state_bytes: 48000,
            max_rows: 64,
        },
    }
}
fn event(i: u64) -> Event {
    Event {
        id: format!("e{i}"),
        source: "s".into(),
        stream: "a".into(),
        position: i,
        source_position: format!("source-token-{i}"),
        schema_epoch: 1,
        topology_epoch: 2,
        partition: vec![Scalar::Int(42)],
        mutation: Mutation::Put {
            key: vec![Scalar::Int(i as i64)],
            column: "body".into(),
            value: Scalar::Text(format!("v{i}")),
            timestamp: i as i64,
            expires_at: None,
        },
    }
}
fn chunk() -> Chunk {
    Chunk {
        id: "c1".into(),
        events: (0..3).map(event).collect(),
    }
}
fn expected(m: &Manifest) -> State {
    let mut s = State::default();
    s.apply(m, &chunk().events).unwrap();
    s
}
#[test]
fn real_destination_reopen_retry() {
    let p = fresh();
    let m = manifest();
    {
        let mut db = Importer::create(&p, m.clone()).unwrap();
        assert_eq!(db.status().unwrap().phase, "source-only-baseline");
        assert!(db.import(&chunk()).unwrap());
        assert_eq!(db.read_destination().unwrap().len(), 3);
        assert_eq!(db.status().unwrap().phase, "capture-complete");
    }
    let mut db = Importer::open(&p, m.clone()).unwrap();
    assert!(!db.import(&chunk()).unwrap());
    let report = db.verify(&expected(&m), 10).unwrap();
    assert!(report.equal);
    assert_eq!(report.actual[0].count, 3);
    assert_eq!(report.actual[0].first, vec![Scalar::Int(0)]);
    assert_eq!(report.actual[0].last, vec![Scalar::Int(2)]);
    assert_eq!(db.status().unwrap().chunks, 1);
    assert_eq!(db.status().unwrap().phase, "canary-ready");
    drop(db);
    let db = Importer::open(&p, m).unwrap();
    assert_eq!(db.status().unwrap().phase, "canary-ready");
    assert!(!db.status().unwrap().source_completeness_proven);
}
#[test]
fn permanent_chunk_mismatch() {
    let p = fresh();
    let m = manifest();
    let mut db = Importer::create(p, m).unwrap();
    db.import(&chunk()).unwrap();
    let before = db.read_destination().unwrap();
    let mut c = chunk();
    c.events.reverse();
    assert!(!db.import(&c).unwrap());
    c.events.pop();
    assert!(db.import(&c).unwrap_err().0.contains("chunk ID mismatch"));
    assert_eq!(db.read_destination().unwrap(), before);
    assert_eq!(db.status().unwrap().chunks, 1);
}
#[test]
fn stream_gap_topology_schema() {
    let m = manifest();
    let mut db = Importer::create(fresh(), m.clone()).unwrap();
    let mut c = chunk();
    c.events.retain(|e| e.position != 1);
    db.import(&c).unwrap();
    assert!(!db.status().unwrap().declared_cut_complete);
    let mut s = State::default();
    s.apply(&m, &c.events).unwrap();
    db.verify(&s, 10).unwrap();
    assert_eq!(db.status().unwrap().phase, "reconciled");
    for mode in 0..3 {
        let mut e = event(1);
        match mode {
            0 => e.topology_epoch += 1,
            1 => e.schema_epoch += 1,
            _ => e.stream = "unknown".into(),
        };
        assert!(
            db.import(&Chunk {
                id: format!("bad{mode}"),
                events: vec![e]
            })
            .is_err()
        );
    }
    assert_eq!(db.status().unwrap().chunks, 1);
    db.import(&Chunk {
        id: "gap".into(),
        events: vec![event(1)],
    })
    .unwrap();
    assert_eq!(db.status().unwrap().phase, "capture-complete");
    db.verify(&expected(&m), 10).unwrap();
    assert_eq!(db.status().unwrap().phase, "canary-ready");
}
#[test]
fn bounded_import_no_partial_checkpoint() {
    for mode in 0..4 {
        let mut m = manifest();
        match mode {
            0 => m.bounds.max_events = 2,
            1 => m.bounds.max_chunk_bytes = 1,
            2 => m.bounds.max_state_bytes = 64,
            _ => m.bounds.max_rows = 2,
        };
        let mut db = Importer::create(fresh(), m).unwrap();
        assert!(db.import(&chunk()).is_err(), "{mode}");
        assert_eq!(db.status().unwrap().chunks, 0);
        assert!(db.read_destination().unwrap().is_empty());
    }
}
#[test]
fn staged_abort_reopen() {
    let p = fresh();
    let m = manifest();
    {
        let mut db = Importer::create(&p, m.clone()).unwrap();
        assert!(
            db.import_with_stage_hook(&chunk(), || Err(Error("injected".into())))
                .is_err()
        );
        assert_eq!(db.status().unwrap().events, 0);
        assert!(db.read_destination().unwrap().is_empty());
    }
    let mut db = Importer::open(p, m).unwrap();
    assert!(db.import(&chunk()).unwrap());
    assert_eq!(db.status().unwrap().chunks, 1);
}
#[test]
fn manifest_mismatch_and_wrong_expected_cut() {
    let p = fresh();
    let m = manifest();
    let mut db = Importer::create(&p, m.clone()).unwrap();
    db.import(&chunk()).unwrap();
    assert!(db.verify(&State::default(), 10).is_err());
    drop(db);
    let mut wrong = m;
    wrong.horizon_micros += 1;
    assert!(Importer::open(p, wrong).is_err());
}
#[test]
fn crash_child() {
    let Ok(mode) = std::env::var("GD_MIGRATE_CRASH") else {
        return;
    };
    let p = std::env::var("GD_MIGRATE_DB").unwrap();
    let mut db = Importer::open(p, manifest()).unwrap();
    match mode.as_str() {
        "stage" => {
            db.import_with_stage_hook(&chunk(), || std::process::exit(86))
                .unwrap();
        }
        "before" => {
            db.set_crash_point(CrashPoint::BeforeCommit).unwrap();
            db.import(&chunk()).unwrap();
        }
        "after" => {
            db.set_crash_point(CrashPoint::AfterCommit).unwrap();
            db.import(&chunk()).unwrap();
        }
        "ack" => {
            db.set_crash_point(CrashPoint::BeforeAck).unwrap();
            db.import(&chunk()).unwrap();
        }
        _ => panic!("bad crash mode"),
    };
    std::process::exit(86);
}
#[test]
fn subprocess_crash_before_and_after_commit() {
    for mode in ["stage", "before", "after", "ack"] {
        let p = fresh();
        drop(Importer::create(&p, manifest()).unwrap());
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new(&exe)
            .args(["--exact", "crash_child", "--nocapture"])
            .env("GD_MIGRATE_CRASH", mode)
            .env("GD_MIGRATE_DB", &p)
            .output()
            .unwrap();
        std::fs::write(p.join(format!("{mode}.stdout")), &out.stdout).unwrap();
        std::fs::write(p.join(format!("{mode}.stderr")), &out.stderr).unwrap();
        std::fs::write(p.join(format!("{mode}.command")),format!("{} --exact crash_child --nocapture\nGD_MIGRATE_CRASH={mode}\nGD_MIGRATE_DB={}\nstatus={}\n",exe.display(),p.display(),out.status)).unwrap();
        assert_eq!(out.status.code(), Some(86), "{mode}: {out:?}");
        let mut db = Importer::open(&p, manifest()).unwrap();
        let committed = ["after", "ack"].contains(&mode);
        assert_eq!(
            db.status().unwrap().chunks,
            usize::from(committed),
            "{mode} {out:?}"
        );
        assert_eq!(
            db.read_destination().unwrap().len(),
            if committed { 3 } else { 0 }
        );
        assert_eq!(db.import(&chunk()).unwrap(), !committed);
        assert!(!db.import(&chunk()).unwrap());
        assert_eq!(db.status().unwrap().chunks, 1);
    }
}
fn mutate(db: &Importer, id: i64, row: &MaterializedRow) {
    db.sql_session()
        .execute(
            "UPDATE migration_rows SET payload=$1 WHERE id=$2",
            &[V::Bytes(serde_json::to_vec(row).unwrap()), V::Int(id)],
            None,
        )
        .unwrap();
}
#[test]
fn comparison_controls_missing_stale_resurrected_wrong_order() {
    for control in ["missing", "stale", "resurrected", "order"] {
        let m = manifest();
        let db = Importer::create(fresh(), m.clone()).unwrap();
        let mut c = chunk();
        c.events[2].mutation = Mutation::Tombstone {
            target: Scope::Row {
                key: vec![Scalar::Int(2)],
            },
            timestamp: 10,
        };
        let mut e = event(2);
        e.id = "old2".into();
        e.position = 3;
        c.events.push(e);
        let mut m2 = m.clone();
        m2.streams[0].end = 3;
        drop(db);
        let mut db = Importer::create(fresh(), m2.clone()).unwrap();
        db.import(&c).unwrap();
        let mut expected = State::default();
        expected.apply(&m2, &c.events).unwrap();
        assert!(db.verify(&expected, 10).unwrap().equal);
        let rows = db.read_destination().unwrap();
        match control {
            "missing" => {
                db.sql_session()
                    .execute(
                        "UPDATE migration_rows SET active=false WHERE id=1",
                        &[],
                        None,
                    )
                    .unwrap();
            }
            "stale" => {
                let mut r = rows[0].clone();
                r.cells.get_mut("body").unwrap().value = Scalar::Text("stale".into());
                mutate(&db, 0, &r);
            }
            "resurrected" => {
                let mut r = rows[1].clone();
                r.key = vec![Scalar::Int(2)];
                mutate(&db, 1, &r);
            }
            "order" => {
                mutate(&db, 0, &rows[1]);
                mutate(&db, 1, &rows[0]);
            }
            _ => unreachable!(),
        }
        let report = db.verify(&expected, 10);
        assert!(report.as_ref().map_or(true, |r| !r.equal), "{control}");
        assert!(
            db.status()
                .as_ref()
                .map_or(true, |s| s.phase != "canary-ready"),
            "{control}"
        );
    }
}
#[test]
fn partition_digest_boundaries_and_bounded_diffs() {
    let m = manifest();
    let s = expected(&m);
    let rows = s.rows(&m, 100).unwrap();
    let mut changed = rows.clone();
    changed[0].cells.get_mut("body").unwrap().timestamp = 999;
    changed[1].cells.get_mut("body").unwrap().timestamp = 998;
    let r = compare_rows(&m, &rows, &changed, 1).unwrap();
    assert!(!r.equal);
    assert_eq!(r.differences.len(), 1);
    assert!(r.differences_truncated);
    assert_ne!(r.expected[0].sha256, r.actual[0].sha256);
    assert!(compare_rows(&m, &rows, &rows, 0).is_err());
}
#[test]
fn inactive_slots_reused_without_resurrection() {
    let mut m = manifest();
    m.streams[0].end = 4;
    let mut db = Importer::create(fresh(), m.clone()).unwrap();
    db.import(&chunk()).unwrap();
    let mut e = event(3);
    e.mutation = Mutation::Tombstone {
        target: Scope::Partition,
        timestamp: 10,
    };
    db.import(&Chunk {
        id: "delete".into(),
        events: vec![e],
    })
    .unwrap();
    assert!(db.read_destination().unwrap().is_empty());
    let mut back = event(4);
    back.mutation = event(0).mutation;
    db.import(&Chunk {
        id: "backfill".into(),
        events: vec![back],
    })
    .unwrap();
    assert!(db.read_destination().unwrap().is_empty());
    let s = db.staged_state().unwrap();
    assert!(db.verify(&s, 10).unwrap().equal);
    assert_eq!(db.status().unwrap().phase, "canary-ready");
}
#[test]
fn bounded_range_comparison() {
    let m = manifest();
    let rows = expected(&m).rows(&m, 100).unwrap();
    let mut altered = rows.clone();
    altered[0].cells.get_mut("body").unwrap().value = Scalar::Text("outside".into());
    altered[1].cells.get_mut("body").unwrap().value = Scalar::Text("inside".into());
    let lower = Some(Bound {
        key: vec![Scalar::Int(0)],
        inclusive: false,
    });
    let upper = Some(Bound {
        key: vec![Scalar::Int(1)],
        inclusive: true,
    });
    let report = compare_range(
        &m,
        &vec![Scalar::Int(42)],
        lower.clone(),
        upper.clone(),
        &rows,
        &altered,
        10,
    )
    .unwrap();
    assert!(!report.equal);
    assert_eq!(report.differences.len(), 1);
    assert_eq!(report.expected[0].count, 1);
    assert_eq!(report.expected[0].first, vec![Scalar::Int(1)]);
    altered[1] = rows[1].clone();
    assert!(
        compare_range(
            &m,
            &vec![Scalar::Int(42)],
            lower,
            upper,
            &rows,
            &altered,
            10
        )
        .unwrap()
        .equal
    );
}
#[test]
fn multiple_streams_cannot_be_silently_skipped() {
    let mut m = manifest();
    m.streams.push(Stream {
        id: "b".into(),
        start: 8,
        end: 8,
    });
    let mut db = Importer::create(fresh(), m.clone()).unwrap();
    db.import(&chunk()).unwrap();
    assert!(!db.status().unwrap().declared_cut_complete);
    let mut state = expected(&m);
    db.verify(&state, 10).unwrap();
    assert_eq!(db.status().unwrap().phase, "reconciled");
    let mut e = event(0);
    e.id = "stream-b".into();
    e.stream = "b".into();
    e.position = 8;
    let c = Chunk {
        id: "b".into(),
        events: vec![e],
    };
    db.import(&c).unwrap();
    state.apply(&m, &c.events).unwrap();
    db.verify(&state, 10).unwrap();
    assert_eq!(db.status().unwrap().phase, "canary-ready");
}
#[test]
fn concurrent_owner_change_aborts_import_atomically() {
    let m = manifest();
    let mut db = Importer::create(fresh(), m).unwrap();
    let mut other = db.sql_session();
    let result = db.import_with_stage_hook(&chunk(), || {
        other.execute("CREATE TABLE unrelated (id BIGINT PRIMARY KEY)", &[], None)?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(db.read_destination().unwrap().is_empty());
    assert_eq!(db.status().unwrap().chunks, 0);
    assert!(db.import(&chunk()).unwrap());
}
#[test]
fn corrupt_staging_digest_rejects_reopen() {
    let p = fresh();
    let m = manifest();
    let mut db = Importer::create(&p, m.clone()).unwrap();
    db.import(&chunk()).unwrap();
    db.sql_session()
        .execute(
            "UPDATE migration_stage SET payload=$1 WHERE id=0",
            &[V::Bytes(serde_json::to_vec(&State::default()).unwrap())],
            None,
        )
        .unwrap();
    drop(db);
    assert!(Importer::open(p, m).is_err());
}
