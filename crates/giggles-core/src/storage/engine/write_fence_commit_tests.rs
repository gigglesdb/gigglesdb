use super::guard_tests::{fixture, request};
use super::*;
use crate::storage::engine::{kv_mem::MemKv, meta};

fn reopen(mem: Arc<MemKv>, token: Option<&[u8]>) -> Arc<Sequencer> {
    Sequencer::open_with_holder(
        Arc::new(Kv::new(mem)),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
        token,
    )
    .unwrap()
}
fn write() -> CommitRequest {
    let mut r = request(0);
    r.writes.insert(
        (Keyspace::Rows, b"fence-test".to_vec()),
        Some(b"value".to_vec()),
    );
    r
}
fn refused<T: std::fmt::Debug>(result: Result<T, TxnError>) {
    let error = crate::CoreError::from(result.unwrap_err());
    assert_eq!(error, crate::CoreError::WriteFenced { epoch: 1 });
    assert_eq!(crate::sqlstate(&error), "25006");
}

fn fenced_group(seq: &Sequencer, group: Vec<Message>) {
    let fence = FenceAccess {
        record: seq.write_fence_status().unwrap(),
        holder: false,
    };
    let mut hlc = HlcClock::new(seq.visible_ts());
    let mut log = RecentLog::new(1024);
    let mut features = load_features(seq.kv()).unwrap();
    let mut subversion = load_subversion(seq.kv()).unwrap();
    assert!(!process_group(
        group,
        seq.kv(),
        &ManualClock::new(10),
        &seq.shared,
        &mut hlc,
        &mut log,
        &mut features,
        &mut subversion,
        &TsSource::Hlc,
        &seq.served,
        &fence,
    ));
}

fn row_is_absent(seq: &Sequencer) {
    assert!(
        super::super::mvcc::read_at(seq.kv(), Keyspace::Rows, b"fence-test", Ts(u64::MAX))
            .unwrap()
            .is_none()
    );
}
#[test]
fn write_fence_lifecycle_restart_and_counter() {
    let (mem, seq) = fixture();
    assert_eq!(seq.install_write_fence(b"holder", "backup").unwrap(), 1);
    seq.submit(write()).unwrap(); // installer is the holder
    assert_eq!(
        seq.release_write_fence(b"wrong", 1),
        Err(TxnError::WriteFenced { epoch: 1 })
    );
    assert_eq!(
        seq.release_write_fence(b"holder", 0),
        Err(TxnError::WriteFenced { epoch: 1 })
    );
    drop(seq);
    let seq = reopen(mem.clone(), None);
    assert_eq!(seq.submit(write()), Err(TxnError::WriteFenced { epoch: 1 }));
    let mut raw = request(0);
    raw.raw.push(RawOp::XactCommitted { xid: 9 });
    assert_eq!(seq.submit(raw), Err(TxnError::WriteFenced { epoch: 1 }));
    let mut key = vec![2];
    key.extend_from_slice(&[0; 24]);
    assert_eq!(
        seq.write_counter(key, vec![], 0),
        Err(TxnError::WriteFenced { epoch: 1 })
    );
    seq.submit(request(0)).unwrap(); // reads remain allowed
    assert_eq!(
        seq.install_write_fence(b"wrong", "blocked"),
        Err(TxnError::WriteFenced { epoch: 1 })
    );
    let status = seq.write_fence_status().unwrap().unwrap();
    assert_eq!(status.label, "backup");
    assert!(status.changed_at > 0);
    drop(seq);
    let seq = reopen(mem.clone(), Some(b"holder"));
    seq.submit(write()).unwrap();
    seq.release_write_fence(b"holder", 1).unwrap();
    assert_eq!(
        seq.write_fence_status().unwrap().unwrap().state,
        FenceState::Released
    );
    drop(seq);
    let seq = reopen(mem, None);
    seq.submit(write()).unwrap();
    assert_eq!(seq.install_write_fence(b"next", "second").unwrap(), 2);
    assert_eq!(
        seq.release_write_fence(b"holder", 1),
        Err(TxnError::WriteFenced { epoch: 2 })
    );
    assert_eq!(seq.install_write_fence(b"next", "advance").unwrap(), 3);
    assert_eq!(
        seq.release_write_fence(b"next", 2),
        Err(TxnError::WriteFenced { epoch: 3 })
    );
}
#[test]
fn write_fence_rejects_individually_before_staging() {
    let (mem, seq) = fixture();
    seq.install_write_fence(b"holder", "test").unwrap();
    let mut receivers = Vec::new();
    let requests = [write(), request(0), {
        let mut r = request(0);
        r.raw.push(RawOp::XactCommitted { xid: 12 });
        r
    }];
    let group = requests
        .into_iter()
        .map(|request| {
            let (reply, rx) = mpsc::channel();
            receivers.push(rx);
            Message::Commit(Pending {
                request,
                guard: None,
                reply,
            })
        })
        .collect();
    fenced_group(&seq, group);
    let results: Vec<_> = receivers.into_iter().map(|r| r.recv().unwrap()).collect();
    refused(results[0].clone());
    assert!(results[1].is_ok());
    refused(results[2].clone());
    row_is_absent(&seq);
    drop(seq);
    let seq = reopen(Arc::new(mem.crash()), None);
    seq.submit(request(0)).unwrap();
    row_is_absent(&seq);
    assert_eq!(
        seq.kv()
            .get(Keyspace::Seq, &super::super::xact::marker_key(12))
            .unwrap(),
        None
    );
}

#[test]
fn counter_only_and_mixed_read_groups_preserve_durable_counter() {
    use crate::relational::{DataType, catalog_ext::SequenceDef};
    use crate::storage::{engine::catalog_codec, sequences::SeqCounter};

    let (mem, seq) = fixture();
    let definition = SequenceDef {
        name: "fenced_sequence".into(),
        data_type: DataType::BigInt,
        start: 1,
        increment: 1,
        min: 1,
        max: 99,
        cache: 4,
        cycle: false,
        owned_by: None,
        identity: false,
        incarnation: 7,
    };
    let mut create = request(64);
    create.writes.insert(
        (Keyspace::Catalog, catalog_codec::sequence_key(1)),
        Some(catalog_codec::encode_sequence(&definition).unwrap()),
    );
    seq.submit(create).unwrap();
    let key = catalog_codec::counter_key(1, 7);
    let encode = |value| {
        catalog_codec::encode_counter(
            &SeqCounter {
                incarnation: 7,
                last_value: value,
                is_called: true,
                log_hw: value,
                stamp: 3,
            },
            &definition,
        )
        .unwrap()
    };
    let before = encode(8);
    let after = encode(40);
    seq.write_counter(key.clone(), before.clone(), 0).unwrap();
    seq.install_write_fence(b"holder", "counter").unwrap();
    drop(seq);
    let seq = reopen(mem.clone(), None);
    // Also exercise the public direct counter entry, not just constructed groups.
    refused(seq.write_counter(key.clone(), after.clone(), 0));
    for with_read in [false, true] {
        let (reply, receive) = mpsc::channel();
        let mut group = vec![Message::Counter {
            key: key.clone(),
            value: after.clone(),
            features_or: 0,
            reply,
        }];
        let (reply, read) = mpsc::channel();
        if with_read {
            group.push(Message::Commit(Pending {
                request: request(0),
                guard: None,
                reply,
            }));
        }
        fenced_group(&seq, group);
        refused(receive.recv().unwrap());
        if with_read {
            read.recv().unwrap().unwrap();
        }
        assert_eq!(
            seq.kv().get(Keyspace::Seq, &key).unwrap(),
            Some(before.clone())
        );
    }
    seq.submit(request(0)).unwrap();
    drop(seq);
    let seq = reopen(Arc::new(mem.crash()), None);
    seq.release_write_fence(b"holder", 1).unwrap();
    assert_eq!(seq.kv().get(Keyspace::Seq, &key).unwrap(), Some(before));
}

#[test]
fn row_version_refill_is_protocol_only_and_statement_commit_is_fenced() {
    use crate::storage::engine::seq::SequenceAllocator;

    let (mem, seq) = fixture();
    seq.install_write_fence(b"holder", "row versions").unwrap();
    drop(seq);
    let seq = reopen(mem.clone(), None);
    let mut allocator = SequenceAllocator::new(seq.kv.clone(), 0);
    allocator.set_sequencer(seq.clone());
    // Crossing half a chunk triggers the real asynchronous sequencer refill.
    let range = allocator
        .reserve_range(3, 0, 1024 * 1_000_001 / 2 + 1)
        .unwrap();
    // FIFO status is a barrier behind the queued Highwater.
    seq.write_fence_status().unwrap();
    let key = [3, 0, 0, 0, 0, 0, 0, 0, 0];
    let floor = seq.kv().get(Keyspace::Seq, &key).unwrap().unwrap();
    assert!(u64::from_be_bytes(floor.clone().try_into().unwrap()) > range.end);
    assert_eq!(seq.highwater_puts(), 1);
    refused(seq.submit(write()));
    seq.submit(request(0)).unwrap();
    row_is_absent(&seq);
    drop(allocator);
    drop(seq);
    let seq = reopen(Arc::new(mem.crash()), None);
    seq.release_write_fence(b"holder", 1).unwrap();
    row_is_absent(&seq);
    assert_eq!(seq.kv().get(Keyspace::Seq, &key).unwrap(), Some(floor));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_implicit_commit_is_fenced_without_publishing() {
    use crate::{Admin, CoreError, Engine, Limits, operations::*, relational::Value};

    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "native-implicit-fence-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    admin
        .install_write_fence(b"holder", "native implicit")
        .unwrap();
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let insert = Request::Insert {
        table: "items".into(),
        values: vec![("id".into(), ValueExpr::Literal(Value::Int(1)))],
    };
    let mut session = admin.sql_session();
    // The owned native scope must use the implicit engine path without parsing.
    let error = crate::relational::with_sql_parser_forbidden(|| {
        session.execute_native(&insert, &Bindings::new())
    })
    .unwrap_err();
    assert_eq!(
        error,
        Error::Core(CoreError::WriteFenced { epoch: 1 }.to_string())
    );
    assert!(!session.in_transaction());
    // Assert the typed error at the exact implicit publication seam as well.
    session.begin_native_implicit().unwrap();
    session.execute_native(&insert, &Bindings::new()).unwrap();
    let error = session.commit_native_implicit().unwrap_err();
    assert_eq!(error, CoreError::WriteFenced { epoch: 1 });
    assert_eq!(crate::sqlstate(&error), "25006");
    session.rollback();
    assert!(
        admin
            .sql_session()
            .execute("SELECT * FROM items", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(session);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    admin.release_write_fence(b"holder", 1).unwrap();
    assert!(
        admin
            .sql_session()
            .execute("SELECT * FROM items", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn write_fence_limits_and_replicated_refusal() {
    let (_, seq) = fixture();
    assert_eq!(
        seq.install_write_fence(b"token", &"x".repeat(257)),
        Err(TxnError::WriteSetLimit)
    );
    assert!(seq.write_fence_status().unwrap().is_none());
    let mut state = FenceAccess::default();
    let mut features = meta::REPLICATED;
    let mut hlc = HlcClock::new(Ts(0));
    assert_eq!(
        process_fence(
            seq.kv(),
            FenceAction::Install {
                digest: holder_digest(b"token"),
                label: "x".into()
            },
            &mut state,
            &mut features,
            &mut hlc,
            &ManualClock::new(10)
        ),
        Err(TxnError::Unsupported)
    );
    assert!(state.record.is_none());
}
