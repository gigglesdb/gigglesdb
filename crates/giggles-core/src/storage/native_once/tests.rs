use super::*;
use std::{cell::Cell, path::PathBuf};

fn fresh(name: &str) -> PathBuf {
    let root = crate::test_artifact_root();
    fs::create_dir_all(&root).unwrap();
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    root.join(format!(
        "native-once-{name}-{:032x}",
        u128::from_be_bytes(nonce)
    ))
}
fn fixture() -> (PathBuf, Admin) {
    let path = fresh("db");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    (path, admin)
}
fn identity(admin: &Admin, key: u8) -> NativeOnceKey {
    NativeOnceKey {
        database_domain: admin.snapshot().unwrap().domain,
        identity: vec![key],
    }
}
fn owner(admin: &Admin, key: &NativeOnceKey) -> NativeOnceOwner {
    match admin
        .admit_native_once_engine(key.clone(), [1; 32])
        .unwrap()
    {
        NativeOnceAdmission::Owner(owner) => owner,
        NativeOnceAdmission::Replay(_) => panic!("new owner expected"),
    }
}
pub(super) fn clock(lower: u64, upper: u64) -> NativeResult<NativeClockLease> {
    Ok(NativeClockLease {
        lower_unix_seconds: lower,
        upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(5),
    })
}
fn bytes(admin: &Admin) -> Vec<u8> {
    encode(&records(&admin.inner.read().unwrap()).unwrap()).unwrap()
}
pub(super) fn terminal_payload() -> Vec<u8> {
    use crate::operations::once::{OnceDescription, OnceSchema};
    crate::operations::once::outcome::encode(
        &OnceDescription {
            catalog_epoch: 1,
            output: OnceSchema::Operation(vec![]),
        },
        &Err(crate::operations::Error::NotFound),
    )
    .unwrap()
}
fn payload(len: usize) -> Vec<u8> {
    use crate::operations::once::{OnceDescription, OnceSchema};
    crate::operations::once::outcome::encode(
        &OnceDescription {
            catalog_epoch: 1,
            output: OnceSchema::Operation(vec![relational::ColumnDef {
                id: 1,
                name: "value".into(),
                data_type: relational::DataType::Bytes,
                not_null: true,
            }]),
        },
        &Ok(crate::operations::once::OnceOutcome::Operation(
            crate::operations::Outcome {
                columns: vec!["value".into()],
                rows: vec![relational::SqlRow {
                    values: vec![relational::Value::Bytes(vec![0; len])],
                    version: 1,
                }],
                matched: 1,
                ..Default::default()
            },
        )),
    )
    .unwrap()
}
fn publish(
    admin: &Admin,
    key: &NativeOnceKey,
    sql: Option<&str>,
    mode: NativePublishMode,
) -> NativeResult<(NativeOnceRecord, bool)> {
    let owner = owner(admin, key);
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    if let Some(sql) = sql {
        session.execute(sql, &[], None).unwrap();
    }
    session.publish_native_once_engine(
        None,
        owner,
        [2; 32],
        terminal_payload(),
        10,
        mode,
        &mut || clock(100, 110),
    )
}

#[test]
fn success_terminal_and_replay_preserve_atomic_state_and_slot_cleanup() {
    let (_, admin) = fixture();
    admin.certify_server_time(900, 900).unwrap();
    let key = identity(&admin, 1);
    let (record, replayed) = publish(
        &admin,
        &key,
        Some("INSERT INTO items VALUES(1)"),
        NativePublishMode::Success,
    )
    .unwrap();
    assert!(!replayed);
    assert_eq!(record.retry_until_seconds, 120);
    assert_eq!(record.result, Some(terminal_payload()));
    assert_eq!(admin.inner.sql_transactions.load(Ordering::Acquire), 0);
    assert_eq!(
        admin.inner.read().unwrap().last_time,
        900,
        "native seconds never overwrite repair time"
    );
    let saved = bytes(&admin);
    assert_eq!(
        admin.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::Retained(record.clone())
    );
    match admin
        .admit_native_once_engine(key.clone(), [1; 32])
        .unwrap()
    {
        NativeOnceAdmission::Replay(old) => assert_eq!(old, record),
        _ => panic!("retained outcome expected"),
    }
    assert_eq!(
        bytes(&admin),
        saved,
        "lookup and replay never publish a new generation"
    );
    assert!(matches!(
        admin.admit_native_once_engine(key, [9; 32]),
        Err(NativeOnceError::IdempotencyConflict)
    ));

    let key = identity(&admin, 2);
    let owner = owner(&admin, &key);
    let before = admin.inner.read().unwrap();
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    session
        .execute("INSERT INTO items VALUES(2)", &[], None)
        .unwrap();
    session
        .execute("CREATE TABLE discarded(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    session.abort();
    session
        .publish_native_once_engine(
            None,
            owner,
            [2; 32],
            terminal_payload(),
            10,
            NativePublishMode::Terminal,
            &mut || clock(100, 110),
        )
        .unwrap();
    let mut after = admin.inner.read().unwrap();
    assert_eq!(after.sql.generation, before.sql.generation + 1);
    after.sql.generation = before.sql.generation;
    assert_eq!(
        after.sql, before.sql,
        "terminal publication preserves rows, schema and both allocators"
    );
    assert_eq!(after.view, before.view);
    assert_eq!(admin.inner.sql_transactions.load(Ordering::Acquire), 0);
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    assert!(!session.catalog_empty_engine().unwrap());
}

#[test]
fn ownership_is_shared_bounded_and_released_after_cancellation() {
    let path = fresh("owners");
    let admin = Admin::create(
        path,
        Limits {
            admitted: 1,
            selected_keys: 1,
        },
    )
    .unwrap();
    let key = identity(&admin, 1);
    let second = identity(&admin, 2);
    let held = owner(&admin, &key);
    let clone = admin.clone();
    assert_eq!(
        clone.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::InProgress
    );
    assert!(matches!(
        clone.admit_native_once_engine(key.clone(), [1; 32]),
        Err(NativeOnceError::InProgress)
    ));
    assert!(matches!(
        clone.admit_native_once_engine(key.clone(), [2; 32]),
        Err(NativeOnceError::IdempotencyConflict)
    ));
    assert!(matches!(
        clone.admit_native_once_engine(second, [1; 32]),
        Err(NativeOnceError::Core(CoreError::Busy))
    ));
    let cancelled = Cancellation::default();
    cancelled.cancel();
    cancelled.scope(|| drop(held));
    assert_eq!(
        admin.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::Absent
    );
    drop(owner(&admin, &key));
}

#[test]
fn unrelated_commit_conflicts_but_retained_receipt_wins_over_stale_generation() {
    let (_, admin) = fixture();
    let key = identity(&admin, 1);
    let held = owner(&admin, &key);
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    session
        .execute("INSERT INTO items VALUES(1)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES(2)", &[], None)
        .unwrap();
    let saved = bytes(&admin);
    assert_eq!(
        session.publish_native_once_engine(
            None,
            held,
            [2; 32],
            terminal_payload(),
            10,
            NativePublishMode::Success,
            &mut || panic!("conflict before clock")
        ),
        Err(NativeOnceError::Core(CoreError::Sql(
            relational::SqlError::Conflict
        )))
    );
    assert_eq!(bytes(&admin), saved);
    assert_eq!(
        admin.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::Absent
    );

    let held = owner(&admin, &key);
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    session
        .execute("INSERT INTO items VALUES(1)", &[], None)
        .unwrap();
    // Simulate a durable winner arriving after this owner's snapshot. Real
    // owners are exclusive; this isolates the defensive publication ordering.
    admin
        .inner
        .change(|state| {
            state.native_outcomes = Some(ledger_with(&key, Some(payload(0))));
            Ok(())
        })
        .unwrap();
    let saved = bytes(&admin);
    let (record, replayed) = session
        .publish_native_once_engine(
            None,
            held,
            [2; 32],
            terminal_payload(),
            10,
            NativePublishMode::Success,
            &mut || panic!("replay has no clock"),
        )
        .unwrap();
    assert!(replayed);
    assert_eq!(record.result, Some(payload(0)));
    assert_eq!(bytes(&admin), saved);
}

fn ledger_with(key: &NativeOnceKey, result: Option<Vec<u8>>) -> NativeOutcomes {
    NativeOutcomes {
        version: LEDGER_VERSION,
        upper_watermark_unix_seconds: 110,
        entries: BTreeMap::from([(
            key.clone(),
            Entry {
                record: NativeOnceRecord {
                    submitted_fingerprint: [1; 32],
                    accepted_fingerprint: [2; 32],
                    retry_until_seconds: 120,
                    result,
                },
                retention_seconds: 10,
                decided_lower_unix_seconds: 100,
                decided_upper_unix_seconds: 110,
            },
        )]),
    }
}

#[test]
fn explicit_compaction_is_strict_monotonic_and_never_reuses_identity() {
    let (_, admin) = fixture();
    let key = identity(&admin, 1);
    publish(&admin, &key, None, NativePublishMode::Success).unwrap();
    let saved = bytes(&admin);
    assert_eq!(
        admin
            .compact_native_once_engine(&mut || clock(120, 120))
            .unwrap(),
        0
    );
    assert_eq!(
        bytes(&admin),
        saved,
        "exact deadline retains payload without a new decision"
    );
    assert_eq!(
        admin.compact_native_once_engine(&mut || clock(100, 109)),
        Err(NativeOnceError::ClockUncertain)
    );
    assert_eq!(
        admin.compact_native_once_engine(&mut || Err(NativeOnceError::ClockUncertain)),
        Err(NativeOnceError::ClockUncertain)
    );
    assert_eq!(bytes(&admin), saved);
    assert_eq!(
        admin
            .compact_native_once_engine(&mut || clock(121, 121))
            .unwrap(),
        1
    );
    assert_eq!(
        admin.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::Expired {
            retry_until_seconds: 120
        }
    );
    match admin
        .admit_native_once_engine(key.clone(), [1; 32])
        .unwrap()
    {
        NativeOnceAdmission::Replay(record) => assert!(record.result.is_none()),
        _ => panic!("tombstone cannot become a new owner"),
    }
    assert!(matches!(
        admin.admit_native_once_engine(key, [8; 32]),
        Err(NativeOnceError::IdempotencyConflict)
    ));
    assert_eq!(
        admin
            .inner
            .read()
            .unwrap()
            .native_outcomes
            .unwrap()
            .entries
            .len(),
        1
    );
}

#[test]
fn clock_failure_and_both_decision_lease_checks_leave_handle_usable() {
    let (_, admin) = fixture();
    for mode in 0..4 {
        let key = identity(&admin, mode);
        let held = owner(&admin, &key);
        let mut session = admin.sql_session();
        session.begin_engine().unwrap();
        session
            .execute("INSERT INTO items VALUES(1)", &[], None)
            .unwrap();
        let saved = bytes(&admin);
        let result = session.publish_native_once_engine(
            None,
            held,
            [2; 32],
            terminal_payload(),
            10,
            NativePublishMode::Success,
            &mut || {
                let mut lease = clock(100, 110)?;
                match mode {
                    0 => lease.lower_unix_seconds = 111,
                    1 => lease.valid_until = Instant::now(),
                    2 => lease.valid_until = Instant::now() + Duration::from_secs(31),
                    3 => {
                        lease.lower_unix_seconds = u64::MAX;
                        lease.upper_unix_seconds = u64::MAX;
                    }
                    _ => unreachable!(),
                }
                Ok(lease)
            },
        );
        assert_eq!(result, Err(NativeOnceError::ClockUncertain));
        assert_eq!(bytes(&admin), saved);
    }
    for reject_at in [1, 2] {
        let saved = bytes(&admin);
        let mut state = admin.inner.read().unwrap();
        let calls = Cell::new(0);
        let cancellation = Cancellation::default();
        let result = cancellation.scope(|| {
            // Lease rejection persists an unchanged catalog snapshot.
            admin
                .inner
                .persist_checked(&mut state, None, true, &mut || {
                    calls.set(calls.get() + 1);
                    calls.get() != reject_at
                })
        });
        assert!(matches!(result, Err(GuardedPersistError::ClockExpired)));
        assert_eq!(calls.get(), reject_at);
        assert_eq!(bytes(&admin), saved);
        assert!(!admin.inner.fenced.load(Ordering::Acquire));
    }
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES(1)", &[], None)
        .unwrap();
}

#[test]
fn before_decision_pause_expires_the_real_clock_lease_without_fencing() {
    let (_, admin) = fixture();
    let key = identity(&admin, 1);
    let held = owner(&admin, &key);
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    session
        .execute("INSERT INTO items VALUES(1)", &[], None)
        .unwrap();
    let saved = bytes(&admin);
    let deadline = Arc::new(Mutex::new(None));
    let observed = Arc::new(AtomicBool::new(false));
    let end = deadline.clone();
    let hook_observed = observed.clone();
    admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == DecisionPoint::BeforeDecision {
            hook_observed.store(true, Ordering::Release);
            let until: Instant = end
                .lock()
                .unwrap()
                .expect("clock certified before publication");
            while Instant::now() < until {
                std::thread::yield_now();
            }
        }
    })));
    let result = session.publish_native_once_engine(
        None,
        held,
        [2; 32],
        terminal_payload(),
        10,
        NativePublishMode::Success,
        &mut || {
            let valid_until = Instant::now() + Duration::from_millis(20);
            *deadline.lock().unwrap() = Some(valid_until);
            Ok(NativeClockLease {
                lower_unix_seconds: 100,
                upper_unix_seconds: 110,
                valid_until,
            })
        },
    );
    admin.set_decision_hook(None);
    assert_eq!(result, Err(NativeOnceError::ClockUncertain));
    assert!(observed.load(Ordering::Acquire));
    assert_eq!(bytes(&admin), saved);
    assert_eq!(
        admin.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::Absent
    );
}

#[test]
fn crash_error_recovery_keeps_receipt_and_effects_in_the_same_decision() {
    for point in [
        CrashPoint::ErrorBeforeCommit,
        CrashPoint::ErrorAfterDecisionCommit,
    ] {
        let (path, admin) = fixture();
        let key = identity(&admin, 1);
        admin.set_crash_point(point).unwrap();
        assert!(matches!(
            publish(
                &admin,
                &key,
                Some("INSERT INTO items VALUES(1)"),
                NativePublishMode::Success
            ),
            Err(NativeOnceError::Core(CoreError::Storage(_)))
        ));
        assert_eq!(admin.snapshot(), Err(CoreError::Fenced));
        drop(admin);
        let reopened = Admin::open(path, Limits::default()).unwrap();
        let committed = point == CrashPoint::ErrorAfterDecisionCommit;
        assert_eq!(
            matches!(
                reopened.lookup_native_once_engine(&key).unwrap(),
                NativeOnceLookup::Retained(_)
            ),
            committed
        );
        let rows = reopened
            .sql_session()
            .execute("SELECT * FROM items LIMIT 1024", &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), usize::from(committed));
    }
}

#[test]
fn format_three_bytes_are_frozen_and_format_four_requires_native_namespace() {
    // Independent fixed-width pre-native fixture: metadata3 and empty SQL1.
    let mut metadata = 3u32.to_be_bytes().to_vec();
    metadata.extend(73u128.to_be_bytes());
    metadata.extend([0; 24]);
    metadata.push(0); // origin=None
    let mut relational = 1u32.to_be_bytes().to_vec();
    for number in [0u64, 0, 1, 1, 0, 0] {
        relational.extend(number.to_be_bytes());
    }
    let old = BTreeMap::from([(vec![0], metadata), (vec![5], relational)]);
    assert_eq!(records(&empty(73)).unwrap(), old);
    let old_bytes = encode(&old).unwrap();
    assert_eq!(
        replica::Image::from_bytes(old_bytes.clone())
            .unwrap()
            .bytes(),
        old_bytes
    );
    for version in [1u32, 2, 3] {
        let mut legacy = old.clone();
        legacy.get_mut(&vec![0]).unwrap()[..4].copy_from_slice(&version.to_be_bytes());
        if version == 1 {
            legacy.remove(&vec![5]);
        }
        let state = from_records(legacy).unwrap();
        assert!(state.native_outcomes.is_none());
        assert_eq!(records(&state).unwrap(), old);
    }
    let mut state = empty(73);
    state.native_outcomes = Some(NativeOutcomes::default());
    let native = records(&state).unwrap();
    assert_eq!(decode::<Metadata>(&native[&vec![0]]).unwrap().format, 4);
    assert!(
        from_records(native.clone())
            .unwrap()
            .native_outcomes
            .is_some()
    );
    let mut missing = native.clone();
    missing.remove(&vec![7]);
    assert!(matches!(from_records(missing), Err(CoreError::Corrupt(_))));
    let mut unexpected = old.clone();
    unexpected.insert(vec![7], native[&vec![7]].clone());
    assert!(matches!(
        from_records(unexpected),
        Err(CoreError::Corrupt(_))
    ));
    let mut future = native.clone();
    future.get_mut(&vec![0]).unwrap()[..4].copy_from_slice(&99u32.to_be_bytes());
    assert!(matches!(from_records(future), Err(CoreError::Unsupported)));
    assert_eq!(
        NativeOutcomes::from_bytes(&2u16.to_be_bytes()).err(),
        Some(CoreError::Unsupported)
    );
}

#[test]
fn replica_methods_reject_native_state_even_after_raw_deserialization() {
    let mut state = empty(73);
    state.native_outcomes = Some(NativeOutcomes::default());
    let bytes = encode(&records(&state).unwrap()).unwrap();
    assert_eq!(
        replica::Image::from_bytes(bytes.clone()).unwrap_err(),
        CoreError::Unsupported
    );
    let raw: replica::Image = serde_json::from_slice(&serde_json::to_vec(&bytes).unwrap()).unwrap();
    assert_eq!(raw.domain(), Err(CoreError::Unsupported));
    assert_eq!(raw.snapshot(), Err(CoreError::Unsupported));
    #[cfg(feature = "legacy-task")]
    assert_eq!(raw.accounting(TaskId(1)), Err(CoreError::Unsupported));
    assert_eq!(
        raw.row_partition(
            Key {
                tenant: TenantId(0),
                row: RowId(0)
            },
            None
        ),
        Err(CoreError::Unsupported)
    );
    #[cfg(feature = "legacy-task")]
    assert!(matches!(
        raw.evaluate(&replica::Command::Admin(
            br#"{"command":"catalog"}"#.to_vec()
        )),
        Err(CoreError::Unsupported)
    ));
}

#[test]
fn restore_preserves_inaccessible_old_domain_receipts_and_format_four_export() {
    let (path, admin) = fixture();
    let key = identity(&admin, 1);
    publish(&admin, &key, None, NativePublishMode::Success).unwrap();
    let exported = fresh("export");
    admin.export(&exported).unwrap();
    let export_bytes = fs::read(&exported).unwrap();
    assert_eq!(&export_bytes[8..12], &4u32.to_be_bytes());
    let restored = Admin::restore(&exported, fresh("restored"), Limits::default()).unwrap();
    assert_eq!(
        restored.lookup_native_once_engine(&key),
        Err(NativeOnceError::DomainMismatch)
    );
    assert!(matches!(
        restored.admit_native_once_engine(key.clone(), [1; 32]),
        Err(NativeOnceError::DomainMismatch)
    ));
    assert!(
        restored
            .inner
            .read()
            .unwrap()
            .native_outcomes
            .unwrap()
            .entries
            .contains_key(&key)
    );
    let mut session = restored.sql_session();
    session.begin_engine().unwrap();
    assert!(!session.catalog_empty_engine().unwrap());
    drop(admin);
    let reopened = Admin::open(path, Limits::default()).unwrap();
    assert!(matches!(
        reopened.lookup_native_once_engine(&key).unwrap(),
        NativeOnceLookup::Retained(_)
    ));
    assert_eq!(fs::read(exported).unwrap(), export_bytes);
}

#[test]
fn encoded_ledger_result_and_decoder_bounds_are_checked_without_identity_eviction() {
    let key = NativeOnceKey {
        database_domain: 73,
        identity: vec![1],
    };
    let mut ledger = ledger_with(&key, Some(payload(0)));
    let template = ledger.entries[&key].clone();
    for identity in 1..8 {
        ledger.entries.insert(
            NativeOnceKey {
                identity: vec![identity + 1],
                ..key.clone()
            },
            template.clone(),
        );
    }
    let overhead = codec().serialized_size(&ledger).unwrap() as usize;
    for (i, entry) in ledger.entries.values_mut().enumerate() {
        entry.record.result = Some(payload(if i < 7 {
            relational::MAX_RESULT_BYTES
        } else {
            relational::MAX_RESULT_BYTES - overhead
        }));
    }
    assert_eq!(ledger.to_bytes().unwrap().len(), MAX_NATIVE_LEDGER_BYTES);
    ledger.entries.values_mut().last().unwrap().record.result =
        Some(payload(relational::MAX_RESULT_BYTES - overhead + 1));
    assert_eq!(ledger.validate(), Err(CoreError::Limit));
    let mut oversized_result = template.record.clone();
    oversized_result.result = Some(vec![0; MAX_NATIVE_RESULT_BYTES + 1]);
    assert!(decode::<NativeOnceRecord>(&encode(&oversized_result).unwrap()).is_err());
    let mut empty_result = template.record.clone();
    empty_result.result = Some(Vec::new());
    let mut forged = encode(&empty_result).unwrap();
    let last = forged.len() - 8;
    forged[last..].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(decode::<NativeOnceRecord>(&forged).is_err());
    let mut many = NativeOutcomes::default();
    for i in 0..MAX_NATIVE_RECORDS {
        many.entries.insert(
            NativeOnceKey {
                database_domain: 73,
                identity: (i as u64).to_be_bytes().to_vec(),
            },
            template.clone(),
        );
    }
    many.upper_watermark_unix_seconds = 110;
    many.validate().unwrap();
    many.entries.insert(
        NativeOnceKey {
            database_domain: 74,
            identity: vec![0],
        },
        template,
    );
    assert_eq!(many.validate(), Err(CoreError::Limit));
    let mut forged = NativeOutcomes::default().to_bytes().unwrap();
    forged[10..18].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(NativeOutcomes::from_bytes(&forged).is_err());
}

#[test]
fn malformed_result_codec_rejects_before_publication_and_on_record_decode() {
    let (_, admin) = fixture();
    let key = identity(&admin, 1);
    let saved = bytes(&admin);
    let mut bad_magic = terminal_payload();
    bad_magic[0] = b'X';
    let mut bad_version = terminal_payload();
    bad_version[4] = 2;
    let mut truncated = terminal_payload();
    truncated.pop();
    for malformed in [vec![], bad_magic, bad_version, truncated] {
        let held = owner(&admin, &key);
        let mut session = admin.sql_session();
        session.begin_engine().unwrap();
        session
            .execute("INSERT INTO items VALUES(1)", &[], None)
            .unwrap();
        assert!(matches!(
            session.publish_native_once_engine(
                None,
                held,
                [2; 32],
                malformed.clone(),
                10,
                NativePublishMode::Success,
                &mut || clock(100, 110)
            ),
            Err(NativeOnceError::Core(CoreError::Corrupt(_)))
        ));
        assert_eq!(bytes(&admin), saved);
        let ledger = ledger_with(&key, Some(malformed));
        assert!(matches!(
            NativeOutcomes::from_bytes(&encode(&ledger).unwrap()),
            Err(CoreError::Corrupt(_))
        ));
    }
}

#[cfg(feature = "legacy-task")]
#[test]
fn native_format_preserves_repair_receipt_and_accounting_namespaces() {
    let (_, admin) = fixture();
    let repair_key = Key {
        tenant: TenantId(42),
        row: RowId(1),
    };
    admin
        .put(
            repair_key,
            Row {
                status: Status::Failed,
                failed_at: 10,
                attempt_count: 0,
                version: 0,
            },
        )
        .unwrap();
    admin
        .grant(Grant {
            task: TaskId(1),
            issuer: Principal(1),
            executor: Principal(2),
            approver: Principal(3),
            policy: Policy {
                tenant: TenantId(42),
                columns: std::collections::BTreeSet::from([Column::Status]),
                transitions: std::collections::BTreeSet::from([(Status::Failed, Status::Queued)]),
                incident: (0, 100),
                distinct_limit: 1,
                transition_limit: 1,
                approval_above: 1,
                valid_from: 0,
                expires: 1000,
            },
        })
        .unwrap();
    admin
        .provision_task_resources(
            TaskId(1),
            TaskResources {
                read_rows: 10,
                disclosed_rows: 10,
                encoded_bytes: 10000,
            },
        )
        .unwrap();
    let repair = admin.executor(TaskId(1), Principal(2)).unwrap();
    repair
        .execute(
            crate::Request {
                operation: OperationId(1),
                selection: Selection::Keys(vec![repair_key]),
                column: Column::Status,
                after: Status::Queued,
            },
            Time::Certified(20),
            None,
        )
        .unwrap()
        .unwrap();
    let before = records(&admin.inner.read().unwrap()).unwrap();
    let key = identity(&admin, 1);
    publish(&admin, &key, None, NativePublishMode::Success).unwrap();
    let after = records(&admin.inner.read().unwrap()).unwrap();
    for (key, value) in &before {
        if matches!(key[0], 1..=4 | 6) {
            assert_eq!(after.get(key), Some(value));
        }
    }
    assert_eq!(decode::<Metadata>(&after[&vec![0]]).unwrap().format, 4);
    let parsed = from_records(after).unwrap();
    assert_eq!(parsed.view, admin.snapshot().unwrap());
    assert_eq!(parsed.accounting.len(), 1);
    assert!(matches!(
        repair.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(Ok(_))
    ));
}

#[test]
fn permanent_tombstones_and_concurrent_owners_share_the_last_key_slot() {
    let (_, admin) = fixture();
    let first = identity(&admin, 1);
    let second = identity(&admin, 2);
    let mut ledger = NativeOutcomes {
        upper_watermark_unix_seconds: 110,
        ..Default::default()
    };
    let template = ledger_with(&first, None).entries.remove(&first).unwrap();
    for id in 0..MAX_NATIVE_RECORDS - 1 {
        ledger.entries.insert(
            NativeOnceKey {
                database_domain: first.database_domain,
                identity: (id as u64).to_be_bytes().to_vec(),
            },
            template.clone(),
        );
    }
    admin
        .inner
        .change(|state| {
            state.native_outcomes = Some(ledger);
            Ok(())
        })
        .unwrap();
    let held = owner(&admin, &first);
    let concurrent = admin.clone();
    let contender = second.clone();
    assert!(
        std::thread::spawn(move || matches!(
            concurrent.admit_native_once_engine(contender, [1; 32]),
            Err(NativeOnceError::RetentionUnavailable)
        ))
        .join()
        .unwrap()
    );
    assert_eq!(
        admin.lookup_native_once_engine(&first).unwrap(),
        NativeOnceLookup::InProgress
    );
    assert_eq!(
        admin.lookup_native_once_engine(&second).unwrap(),
        NativeOnceLookup::Absent
    );
    drop(held);
    let held = owner(&admin, &second);
    let mut session = admin.sql_session();
    session.begin_engine().unwrap();
    session
        .publish_native_once_engine(
            None,
            held,
            [2; 32],
            terminal_payload(),
            10,
            NativePublishMode::Success,
            &mut || clock(100, 110),
        )
        .unwrap();
    assert!(matches!(
        admin.admit_native_once_engine(first, [1; 32]),
        Err(NativeOnceError::RetentionUnavailable)
    ));
    let state = admin.inner.read().unwrap();
    let ledger = state.native_outcomes.unwrap();
    assert_eq!(ledger.entries.len(), MAX_NATIVE_RECORDS);
    assert_eq!(
        ledger
            .entries
            .values()
            .filter(|entry| entry.record.result.is_none())
            .count(),
        MAX_NATIVE_RECORDS - 1
    );
}

#[test]
fn once_replay_precedes_binding_after_a_table_name_disappears_and_is_reused() {
    use crate::operations::{
        Bindings, Engine, Error as OperationError, Request as OperationRequest, ValueExpr,
        once::{
            ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceDescription, OnceIdentity,
            OnceOutcome, OnceRequest, OnceSchema, OnceTime,
        },
    };
    let (_, admin) = fixture();
    let engine = Engine::new(admin.clone());
    let (original_epoch, original_tables) = admin.catalog().unwrap();
    let original_table = original_tables
        .iter()
        .find(|table| table.name == "items")
        .unwrap();
    let expected_description = OnceDescription {
        catalog_epoch: original_epoch,
        output: OnceSchema::Operation(original_table.columns.clone()),
    };
    let operation = OperationRequest::Returning {
        request: Box::new(OperationRequest::Insert {
            table: "items".into(),
            values: vec![("id".into(), ValueExpr::Literal(relational::Value::Int(1)))],
        }),
        projection: vec![],
    };
    let request = OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: admin.snapshot().unwrap().domain,
            tenant: 42,
            namespace: "application.catalog_replay".into(),
            key: b"original-table".to_vec(),
        },
        retention_seconds: 60,
        body: OnceBody::Operation(operation.clone()),
    };
    let bindings = Bindings::new();
    let initial_clock = || -> Result<OnceTime, OnceClockUnavailable> {
        Ok(OnceTime {
            lower_unix_seconds: 100,
            upper_unix_seconds: 110,
            valid_until: Instant::now() + Duration::from_secs(5),
        })
    };
    let no_clock = || -> Result<OnceTime, OnceClockUnavailable> {
        panic!("retained replay must not certify time")
    };
    let first = relational::with_sql_parser_forbidden(|| {
        engine.execute_once(&request, &bindings, &initial_clock)
    })
    .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.description, expected_description);
    let OnceSchema::Operation(columns) = &first.description.output else {
        panic!("operation schema expected")
    };
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].id, original_table.columns[0].id);
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[0].data_type, relational::DataType::BigInt);
    assert!(columns[0].not_null);
    let OnceOutcome::Operation(original) = first.outcome.as_ref().unwrap() else {
        panic!("operation result expected")
    };
    assert_eq!(original.rows.len(), 1);
    assert_eq!(original.rows[0].values, vec![relational::Value::Int(1)]);
    let original_id = original_table.id;

    // Private, test-only catalog surgery: preserve the actual table identity and
    // its rows while removing the original spelling from the live namespace.
    admin
        .inner
        .change(|state| {
            let mut table = Arc::make_mut(&mut state.sql.tables).remove("items").unwrap();
            assert_eq!(table.id, original_id);
            table.name = "archived".into();
            assert!(Arc::make_mut(&mut state.sql.tables).insert(table.name.clone(), table).is_none());
            state.sql.epoch = state.sql.epoch.checked_add(1).unwrap();
            state.sql.validate().map_err(CoreError::Sql)
        })
        .unwrap();
    let absent_name_state = bytes(&admin);
    assert!(admin.catalog().unwrap().0 > first.description.catalog_epoch);
    assert_eq!(
        relational::with_sql_parser_forbidden(|| engine.execute(&operation, &bindings)),
        // PG18: undefined table is 42P01; native errors carry SQL's SQLSTATE (R2 parity).
        Err(OperationError::Sql {
            sqlstate: "42P01".into()
        })
    );
    let replayed = relational::with_sql_parser_forbidden(|| {
        engine.execute_once(&request, &bindings, &no_clock)
    })
    .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.outcome, first.outcome);
    assert_eq!(replayed.description, first.description);
    assert_eq!(replayed.accepted_fingerprint, first.accepted_fingerprint);
    assert_eq!(replayed.retry_until_seconds, first.retry_until_seconds);
    assert_eq!(
        bytes(&admin),
        absent_name_state,
        "failed live binding and retained replay publish no generation"
    );

    admin
        .sql_session()
        .execute("CREATE TABLE items(id SMALLINT PRIMARY KEY)", &[], None)
        .unwrap();
    let tables = admin.catalog().unwrap().1;
    let replacement = tables.iter().find(|table| table.name == "items").unwrap();
    assert_eq!(
        replacement.columns[0].data_type,
        relational::DataType::SmallInt
    );
    assert_ne!(replacement.columns[0].id, columns[0].id);
    assert_eq!(
        tables
            .iter()
            .find(|table| table.name == "archived")
            .unwrap()
            .id,
        original_id
    );
    assert_ne!(
        tables
            .iter()
            .find(|table| table.name == "items")
            .unwrap()
            .id,
        original_id
    );
    let reused_name_state = bytes(&admin);
    let replayed = relational::with_sql_parser_forbidden(|| {
        engine.execute_once(&request, &bindings, &no_clock)
    })
    .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.outcome, first.outcome);
    assert_eq!(replayed.description, first.description);
    assert_eq!(replayed.accepted_fingerprint, first.accepted_fingerprint);
    assert_eq!(replayed.retry_until_seconds, first.retry_until_seconds);
    assert!(
        admin
            .sql_session()
            .execute("SELECT * FROM items LIMIT 1024", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM archived LIMIT 1024", &[], None)
            .unwrap()
            .rows,
        original.rows
    );
    assert_eq!(
        bytes(&admin),
        reused_name_state,
        "replay must not target the replacement table or publish a generation"
    );
}
