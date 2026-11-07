use super::*;
use crate::relational::{Prepared, Relational};
fn sql(s: &mut Relational, text: &str) {
    let p = Prepared::parse(text, 1, s.epoch).unwrap();
    s.execute(&p, &[], None).unwrap();
}
fn fixture(typed: bool, native: bool, rows: bool) -> State {
    let mut s = empty(123);
    sql(
        &mut s.sql,
        if typed {
            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT, extra NUMERIC)"
        } else {
            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)"
        },
    );
    if rows {
        sql(
            &mut s.sql,
            if typed {
                "INSERT INTO items VALUES (1,'a',1.25)"
            } else {
                "INSERT INTO items VALUES (1,'a')"
            },
        );
    }
    let t = s.sql.tables["items"].clone();
    s.sql
        .secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
        .unwrap();
    if native {
        s.native_outcomes = Some(Default::default());
    }
    s
}
fn metadata(r: &mut BTreeMap<Vec<u8>, Vec<u8>>, version: u32) {
    let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
    m.format = version;
    r.insert(vec![0], encode(&m).unwrap());
}
fn path(label: &str) -> std::path::PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!(
        "gd42-w2-{label}-{:032x}",
        u128::from_be_bytes(nonce)
    ))
}
#[test]
fn four_index_families_empty_descriptors_and_canonical_roundtrip() {
    for typed in [false, true] {
        for native in [false, true] {
            for rows in [false, true] {
                let s = fixture(typed, native, rows);
                let r = records(&s).unwrap();
                let m: Metadata = decode(&r[&vec![0]]).unwrap();
                assert_eq!(m.format, 7 + u32::from(native) + 2 * u32::from(typed));
                assert!(r.contains_key(&vec![8]));
                let restored = from_records(r.clone()).unwrap();
                assert_eq!(restored.sql, s.sql);
                assert_eq!(records(&restored).unwrap(), r);
                assert_eq!(
                    replica::Image::from_bytes(encode(&r).unwrap()).unwrap_err(),
                    CoreError::Unsupported
                );
            }
        }
    }
}
#[test]
fn missing_extra_malformed_forged_crosscatalog_and_old_envelopes_reject() {
    let r = records(&fixture(false, false, true)).unwrap();
    let mut bad = r.clone();
    bad.remove(&vec![8]);
    assert!(from_records(bad).is_err());
    for version in 1..=6 {
        let mut bad = r.clone();
        metadata(&mut bad, version);
        assert!(from_records(bad).is_err());
    }
    for version in [0, u32::MAX] {
        let mut bad = r.clone();
        metadata(&mut bad, version);
        bad.insert(vec![5], vec![255]);
        assert!(matches!(from_records(bad), Err(CoreError::Unsupported)));
    }
    for payload in [
        vec![],
        vec![255],
        {
            let mut p = r[&vec![8]].clone();
            p.push(0);
            p
        },
        {
            let mut p = r[&vec![8]].clone();
            *p.last_mut().unwrap() ^= 1;
            p
        },
    ] {
        let mut bad = r.clone();
        bad.insert(vec![8], payload);
        assert!(from_records(bad).is_err());
    }
    let mut other = fixture(false, false, true);
    sql(&mut other.sql, "UPDATE items SET tag='b' WHERE id=1");
    let mut bad = records(&other).unwrap();
    bad.insert(vec![8], r[&vec![8]].clone());
    assert!(from_records(bad).is_err());
    let mut foreign = empty(123);
    sql(
        &mut foreign.sql,
        "CREATE TABLE first (id BIGINT PRIMARY KEY)",
    );
    sql(
        &mut foreign.sql,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
    );
    sql(&mut foreign.sql, "INSERT INTO items VALUES (1,'a')");
    let t = foreign.sql.tables["items"].clone();
    foreign
        .sql
        .secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
        .unwrap();
    let mut bad = records(&foreign).unwrap();
    bad.insert(vec![8], r[&vec![8]].clone());
    assert!(from_records(bad).is_err());
    let mut bad = r.clone();
    bad.insert(
        vec![8],
        index_codec::encode(
            &crate::relational::secondary_index::Sidecar::default(),
            || false,
        )
        .unwrap(),
    );
    assert!(from_records(bad).is_err());
    let mut bad = r;
    bad.insert(vec![8, 0], vec![]);
    assert!(from_records(bad).is_err());
}
#[test]
fn real_combined_application_index_states_fail_all_paths_and_application_identity_survives() {
    for typed in [false, true] {
        for native in [false, true] {
            let mut s = fixture(typed, native, false);
            s.applications = Some(Default::default());
            assert_eq!(state_format(&s), Err(CoreError::Unsupported));
            assert_eq!(records(&s), Err(CoreError::Unsupported));
            assert_eq!(validate(&s), Err(CoreError::Unsupported));
            assert!(matches!(
                Admin::create_state(
                    path("combined"),
                    Limits::default(),
                    s.clone(),
                    RecoveryPoint::None
                ),
                Err(CoreError::Unsupported)
            ));
            let mut r = records(&fixture(typed, native, false)).unwrap();
            r.insert(vec![9], s.applications.as_ref().unwrap().bytes().unwrap());
            assert!(matches!(
                from_records(r.clone()),
                Err(CoreError::Unsupported)
            ));
            metadata(&mut r, 11 + u32::from(native) + 2 * u32::from(typed));
            assert!(matches!(from_records(r), Err(CoreError::Unsupported)));
            let mut app = empty(123);
            app.applications = Some(Default::default());
            if native {
                app.native_outcomes = Some(Default::default());
            }
            if typed {
                sql(
                    &mut app.sql,
                    "CREATE TABLE typed (id BIGINT PRIMARY KEY, extra NUMERIC)",
                );
            }
            let r = records(&app).unwrap();
            assert_eq!(
                state_format(&app).unwrap(),
                11 + u32::from(native) + 2 * u32::from(typed)
            );
            assert_eq!(records(&from_records(r.clone()).unwrap()).unwrap(), r);
        }
    }
}
#[test]
fn reopen_export_restore_fresh_domain_retains_index_epoch_ids_and_receipts() {
    for typed in [false, true] {
        for native in [false, true] {
            let s = fixture(typed, native, true);
            let source = path("source");
            let export = path("export");
            let dest = path("restore");
            let admin =
                Admin::create_state(&source, Limits::default(), s.clone(), RecoveryPoint::None)
                    .unwrap();
            admin.export(&export).unwrap();
            drop(admin);
            let reopened = Admin::open(&source, Limits::default()).unwrap();
            reopened
                .inner
                .inspect(|r| {
                    assert_eq!(r.sql, s.sql);
                    Ok(())
                })
                .unwrap();
            drop(reopened);
            let restored = Admin::restore(&export, &dest, Limits::default()).unwrap();
            restored
                .inner
                .inspect(|r| {
                    assert_ne!(r.view.domain, s.view.domain);
                    assert_eq!(r.origin, Some(s.view.domain));
                    assert_eq!(r.sql, s.sql);
                    assert_eq!(
                        r.native_outcomes.as_ref().map(|n| n.to_bytes().unwrap()),
                        s.native_outcomes.as_ref().map(|n| n.to_bytes().unwrap())
                    );
                    Ok(())
                })
                .unwrap();
        }
    }
}
#[test]
fn indexed_crud_rollback_cas_and_commit_error_boundaries_are_atomic() {
    let s = fixture(false, false, true);
    let dir = path("atomic");
    let admin = Admin::create_state(&dir, Limits::default(), s, RecoveryPoint::None).unwrap();
    let initial = admin.inner.read().unwrap();
    let mut session = admin.sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("UPDATE items SET tag='b' WHERE id=1", &[], None)
        .unwrap();
    session.rollback();
    assert_eq!(
        records(&admin.inner.read().unwrap()).unwrap(),
        records(&initial).unwrap()
    );
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("UPDATE items SET tag='b' WHERE id=1", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES (2,'c')", &[], None)
        .unwrap();
    assert!(session.commit_engine().is_err());
    let before = admin.inner.read().unwrap();
    admin
        .set_crash_point(CrashPoint::ErrorBeforeCommit)
        .unwrap();
    assert!(
        admin
            .sql_session()
            .execute("UPDATE items SET tag='d' WHERE id=1", &[], None)
            .is_err()
    );
    drop(session);
    drop(admin);
    let admin = Admin::open(&dir, Limits::default()).unwrap();
    assert_eq!(
        records(&admin.inner.read().unwrap()).unwrap(),
        records(&before).unwrap()
    );
    admin.set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
    assert!(
        admin
            .sql_session()
            .execute("DELETE FROM items WHERE id=2", &[], None)
            .is_err()
    );
    drop(admin);
    let admin = Admin::open(&dir, Limits::default()).unwrap();
    let after = admin.inner.read().unwrap();
    assert!(after.sql.generation > before.sql.generation);
    assert_ne!(records(&after).unwrap(), records(&before).unwrap());
    let r = records(&after).unwrap();
    assert_eq!(records(&from_records(r.clone()).unwrap()).unwrap(), r);
}
#[test]
fn cancellation_and_full_envelope_charge_include_sidecar_header() {
    let s = fixture(false, false, true);
    let cancel = crate::cancellation::Cancellation::default();
    cancel.cancel();
    assert_eq!(cancel.scope(|| records(&s)), Err(CoreError::Cancelled));
    let r = records(&s).unwrap();
    let encoded = encode(&r).unwrap().len();
    let mut r = r;
    let spare = MAX_BYTES as usize - encoded - 17;
    r.insert(vec![255], vec![0; spare]);
    envelope_bound(&r).unwrap();
    r.get_mut(&vec![255]).unwrap().push(0);
    assert_eq!(envelope_bound(&r), Err(CoreError::Limit));
}
fn write_export(path: &Path, version: u32, values: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let payload = encode(values).unwrap();
    let mut bytes = MAGIC.to_vec();
    bytes.extend(version.to_be_bytes());
    bytes.extend((payload.len() as u64).to_be_bytes());
    bytes.extend(Sha256::digest(&payload));
    bytes.extend(payload);
    fs::write(path, bytes).unwrap();
}
#[test]
fn combined_and_future_export_restore_reject_before_destination_creation() {
    let indexed = fixture(false, false, true);
    let mut r = records(&indexed).unwrap();
    let mut app = empty(123);
    app.applications = Some(Default::default());
    r.insert(vec![9], app.applications.as_ref().unwrap().bytes().unwrap());
    for version in [7, 11, u32::MAX] {
        metadata(&mut r, version);
        let export = path("badexport");
        let dest = path("badrestore");
        write_export(&export, version, &r);
        assert!(matches!(
            Admin::read_export(&export),
            Err(CoreError::Unsupported)
        ));
        assert!(matches!(
            Admin::restore(&export, &dest, Limits::default()),
            Err(CoreError::Unsupported)
        ));
        assert!(!dest.exists());
    }
}
#[test]
fn legacy_actual_record_migration_and_image_bytes_remain_canonical() {
    for version in 1..=6 {
        let mut s = empty(91);
        if version >= 2 {
            sql(&mut s.sql, "CREATE TABLE plain (id BIGINT PRIMARY KEY)");
        }
        if [4, 6].contains(&version) {
            s.native_outcomes = Some(Default::default());
        }
        if [5, 6].contains(&version) {
            sql(
                &mut s.sql,
                "CREATE TABLE typed (id BIGINT PRIMARY KEY, extra NUMERIC)",
            );
        }
        let mut r = records(&s).unwrap();
        if version == 1 {
            r.remove(&vec![5]);
        }
        metadata(&mut r, version);
        let parsed = from_records(r.clone()).unwrap();
        assert_eq!(parsed.sql, s.sql);
        let dir = path("legacy");
        let admin =
            Admin::create_state(&dir, Limits::default(), parsed, RecoveryPoint::None).unwrap();
        drop(admin);
        let admin = Admin::open(&dir, Limits::default()).unwrap();
        let export = path("legacyexport");
        admin.export(&export).unwrap();
        let dest = path("legacyrestore");
        Admin::restore(&export, &dest, Limits::default()).unwrap();
    }
    let s = empty(17);
    let r = records(&s).unwrap();
    // Frozen preindex namespace map for an ordinary state. No index slot or
    // alternate serializer may change these exact bytes.
    let expected = BTreeMap::from([
        (
            vec![0],
            encode(&Metadata {
                format: 3,
                domain: 17,
                schema: 0,
                last_time: 0,
                next_approval: 0,
                origin: None,
            })
            .unwrap(),
        ),
        (vec![5], encode(&s.sql).unwrap()),
    ]);
    assert_eq!(r, expected);
    let bytes = encode(&expected).unwrap();
    assert_eq!(replica::Image::new(17).unwrap().bytes(), bytes);
    assert_eq!(
        replica::Image::from_bytes(bytes.clone()).unwrap().bytes(),
        bytes
    );
}
#[test]
fn process_crash_child() {
    let Ok(dir) = std::env::var("GD42_W2_CRASH_DIR") else {
        return;
    };
    let mode = std::env::var("GD42_W2_CRASH_MODE").unwrap();
    let admin = Admin::open(dir, Limits::default()).unwrap();
    admin
        .set_crash_point(if mode == "before" {
            CrashPoint::BeforeCommit
        } else {
            CrashPoint::AfterCommit
        })
        .unwrap();
    admin
        .sql_session()
        .execute("UPDATE items SET tag='crashed' WHERE id=1", &[], None)
        .unwrap();
    panic!("crash seam failed to exit")
}
#[test]
fn actual_process_crashes_publish_neither_or_both_row_and_sidecar_generation() {
    for mode in ["before", "after"] {
        let dir = path(mode);
        let initial = fixture(false, false, true);
        let before = records(&initial).unwrap();
        let admin =
            Admin::create_state(&dir, Limits::default(), initial, RecoveryPoint::None).unwrap();
        drop(admin);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::secondary_index::tests::process_crash_child",
                "--nocapture",
            ])
            .env("GD42_W2_CRASH_DIR", &dir)
            .env("GD42_W2_CRASH_MODE", mode)
            .output()
            .unwrap();
        assert!(!result.status.success());
        let admin = Admin::open(&dir, Limits::default()).unwrap();
        let after = admin.inner.read().unwrap();
        let actual = records(&after).unwrap();
        if mode == "before" {
            assert_eq!(actual, before);
        } else {
            assert_ne!(actual[&vec![5]], before[&vec![5]]);
            assert_ne!(actual[&vec![8]], before[&vec![8]]);
            assert!(after.sql.generation > 0);
        }
        assert_eq!(
            records(&from_records(actual.clone()).unwrap()).unwrap(),
            actual
        );
    }
}

fn unique_fixture(
    typed: bool,
    native: bool,
    temporal: bool,
    populated: bool,
    mixed: bool,
) -> State {
    let mut s = empty(42123);
    let mut ddl = "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT".to_owned();
    if typed {
        ddl.push_str(", extra NUMERIC");
    }
    if temporal {
        ddl.push_str(", stamp DATE");
    }
    ddl.push(')');
    sql(&mut s.sql, &ddl);
    let t = s.sql.tables["items"].clone();
    s.sql
        .create_secondary_index_internal_engine(
            &t,
            s.sql.epoch,
            "unique_tag",
            &[t.columns[1].id],
            true,
            || false,
        )
        .unwrap();
    if mixed {
        s.sql
            .create_secondary_index_engine(
                &t,
                s.sql.epoch,
                "ordinary_tag",
                &[t.columns[1].id],
                || false,
            )
            .unwrap();
    }
    if populated {
        for (id, tag) in [(1, "a"), (2, "b")] {
            let values = format!("INSERT INTO items (id,tag) VALUES ({id},'{tag}')");
            sql(&mut s.sql, &values);
        }
    }
    if native {
        s.native_outcomes = Some(Default::default());
    }
    s
}
#[test]
fn unique_actual_writer_all_families_export_restore_reopen_fixtures() {
    let root = path("actual-unique-writer");
    fs::create_dir_all(&root).unwrap();
    println!("actual writer fixture root {}", root.display());
    let mut cases = 0;
    for typed in [false, true] {
        for native in [false, true] {
            for temporal in [false, true] {
                for populated in [false, true] {
                    for mixed in [false, true] {
                        let name = format!(
                            "t{}n{}d{}r{}m{}",
                            u8::from(typed),
                            u8::from(native),
                            u8::from(temporal),
                            u8::from(populated),
                            u8::from(mixed)
                        );
                        let s = unique_fixture(typed, native, temporal, populated, mixed);
                        let r = records(&s).unwrap();
                        assert_eq!(&r[&vec![8]][8..12], &2u32.to_le_bytes());
                        assert_eq!(records(&from_records(r.clone()).unwrap()).unwrap(), r);
                        fs::write(root.join(format!("{name}.records")), encode(&r).unwrap())
                            .unwrap();
                        let source = root.join(format!("{name}-source"));
                        let export = root.join(format!("{name}.gdexp"));
                        let dest = root.join(format!("{name}-restored"));
                        let admin = Admin::create_state(
                            &source,
                            Limits::default(),
                            s.clone(),
                            RecoveryPoint::None,
                        )
                        .unwrap();
                        admin.export(&export).unwrap();
                        Admin::verify_export(&export).unwrap();
                        drop(admin);
                        let reopened = Admin::open(&source, Limits::default()).unwrap();
                        assert_eq!(
                            records(&reopened.inner.read().unwrap()).unwrap()[&vec![8]],
                            r[&vec![8]]
                        );
                        drop(reopened);
                        let restored = Admin::restore(&export, &dest, Limits::default()).unwrap();
                        assert_eq!(
                            records(&restored.inner.read().unwrap()).unwrap()[&vec![8]],
                            r[&vec![8]]
                        );
                        if populated {
                            assert_eq!(
                                restored.sql_session().execute(
                                    "UPDATE items SET tag='a' WHERE id=2",
                                    &[],
                                    None
                                ),
                                Err(CoreError::Sql(relational::SqlError::Duplicate))
                            );
                        }
                        let mut combined = s.clone();
                        combined.applications = Some(Default::default());
                        assert!(matches!(records(&combined), Err(CoreError::Unsupported)));
                        let mut combined = r.clone();
                        combined.insert(vec![9], vec![]);
                        assert!(matches!(
                            from_records(combined),
                            Err(CoreError::Unsupported)
                        ));
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 32);
}
#[test]
fn unique_native_sql_conflicts_abort_and_whole_image_cas() {
    use crate::relational::Value;
    let s = unique_fixture(false, false, false, false, false);
    let t = s.sql.tables["items"].clone();
    let admin = Admin::create_state(
        path("unique-cas"),
        Limits::default(),
        s,
        RecoveryPoint::None,
    )
    .unwrap();
    let mut native = admin.sql_session();
    native.begin_engine().unwrap();
    native
        .insert_engine(&t, vec![Value::Int(1), Value::Text("a".into())])
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES (2,'a')", &[], None)
        .unwrap();
    assert!(native.commit_engine().is_err());
    native.rollback();
    let before = admin.inner.read().unwrap();
    assert_eq!(
        admin
            .sql_session()
            .insert_engine(&t, vec![Value::Int(3), Value::Text("a".into())]),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    );
    let after = admin.inner.read().unwrap();
    assert_eq!(after.sql, before.sql);
    assert_eq!(after.view, before.view);
    assert_eq!(after.accounting, before.accounting);
    let mut tx = admin.sql_session();
    tx.begin_engine().unwrap();
    tx.execute("INSERT INTO items VALUES (3,'c')", &[], None)
        .unwrap();
    assert_eq!(
        tx.execute("INSERT INTO items VALUES (4,'a')", &[], None),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    );
    assert!(tx.commit_engine().is_err());
    tx.rollback();
    assert_eq!(admin.inner.read().unwrap().sql, before.sql);
}
#[test]
fn unique_internal_catalog_whole_empty_and_receipt_refusal_before_ids() {
    for native in [false, true] {
        let mut s = empty(420);
        sql(
            &mut s.sql,
            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
        );
        let t = s.sql.tables["items"].clone();
        if native {
            s.native_outcomes = Some(Default::default());
        }
        let admin = Admin::create_state(
            path("unique-create"),
            Limits::default(),
            s,
            RecoveryPoint::None,
        )
        .unwrap();
        let before = admin.inner.read().unwrap();
        let mut tx = admin.sql_session();
        tx.begin_engine().unwrap();
        let result = tx.create_index_internal_engine(&t, "unique_tag", &[t.columns[1].id], true);
        if native {
            assert_eq!(result, Err(CoreError::Unsupported));
            tx.rollback();
            assert_eq!(admin.inner.read().unwrap().sql, before.sql);
        } else {
            result.unwrap();
            tx.commit_engine().unwrap();
            let after = admin.inner.read().unwrap();
            assert!(after.sql.secondary_descriptors_engine()[0].unique);
        }
    }
    let s = unique_fixture(false, false, false, true, false);
    let t = s.sql.tables["items"].clone();
    let admin = Admin::create_state(
        path("unique-populated-create"),
        Limits::default(),
        s,
        RecoveryPoint::None,
    )
    .unwrap();
    let before = admin.inner.read().unwrap();
    let mut tx = admin.sql_session();
    tx.begin_engine().unwrap();
    assert_eq!(
        tx.create_index_internal_engine(&t, "second", &[t.columns[1].id], true),
        Err(CoreError::Unsupported)
    );
    tx.rollback();
    assert_eq!(admin.inner.read().unwrap().sql, before.sql);
}

#[test]
fn unique_snapshot_context_checks_before_accepting_unpublished_image() {
    use crate::operations::{Bindings, Compare, Expr, Intent, Request, ValueExpr};
    use crate::relational::Value;
    let s = unique_fixture(false, false, false, true, false);
    let before = records(&s).unwrap();
    let eq = |field: &str, value: Value| {
        Expr::Compare(
            Box::new(Expr::Field(field.into())),
            Compare::Eq,
            Box::new(Expr::Value(ValueExpr::Literal(value))),
        )
    };
    let request = Request::Update {
        table: "items".into(),
        filter: eq("id", Value::Int(2)),
        guard: Some(eq("tag", Value::Text("b".into()))),
        values: vec![("tag".into(), ValueExpr::Literal(Value::Text("a".into())))],
        intent: Intent::One,
    };
    assert!(operation_adapter::execute(&s, &request, &Bindings::new()).is_err());
    assert_eq!(records(&s).unwrap(), before);
    let Request::Update { filter, guard, .. } = request else {
        unreachable!()
    };
    let request = Request::Update {
        table: "items".into(),
        filter,
        guard,
        values: vec![("tag".into(), ValueExpr::Literal(Value::Text("c".into())))],
        intent: Intent::One,
    };
    let expected = operation_adapter::execute(&s, &request, &Bindings::new()).unwrap();
    let cancelled = crate::cancellation::Cancellation::default();
    cancelled.cancel();
    let actual = cancelled
        .scope(|| operation_adapter::execute(&s, &request, &Bindings::new()))
        .unwrap();
    assert_eq!(records(&expected.0).unwrap(), records(&actual.0).unwrap());
    assert_eq!(expected.1, actual.1);
    assert_eq!(records(&s).unwrap(), before);
}
#[test]
fn unique_once_replay_and_terminal_receipt_protected_deltas() {
    use super::super::native_once::{
        NativeClockLease, NativeOnceAdmission, NativeOnceKey, NativePublishMode,
    };
    use crate::operations::once::{OnceDescription, OnceOutcome, OnceSchema};
    use std::time::{Duration, Instant};
    let s = unique_fixture(false, false, false, false, false);
    let epoch = s.sql.epoch;
    let admin = Admin::create_state(
        path("unique-once"),
        Limits::default(),
        s,
        RecoveryPoint::None,
    )
    .unwrap();
    let key = NativeOnceKey {
        database_domain: admin.snapshot().unwrap().domain,
        identity: vec![42],
    };
    let owner = match admin
        .admit_native_once_engine(key.clone(), [1; 32])
        .unwrap()
    {
        NativeOnceAdmission::Owner(o) => o,
        _ => panic!("owner"),
    };
    let mut tx = admin.sql_session();
    tx.begin_engine().unwrap();
    tx.execute("INSERT INTO items VALUES (1,'a')", &[], None)
        .unwrap();
    let description = OnceDescription {
        catalog_epoch: epoch,
        output: OnceSchema::Operation(vec![]),
    };
    let payload = crate::operations::once::outcome::encode(
        &description,
        &Ok(OnceOutcome::Operation(crate::operations::Outcome::default())),
    )
    .unwrap();
    let clock = || {
        Ok(NativeClockLease {
            lower_unix_seconds: 100,
            upper_unix_seconds: 110,
            valid_until: Instant::now() + Duration::from_secs(5),
        })
    };
    let (receipt, _) = tx
        .publish_native_once_engine(
            owner,
            [2; 32],
            payload,
            10,
            NativePublishMode::Success,
            &mut || clock(),
        )
        .unwrap();
    let committed = admin.inner.read().unwrap();
    let exact = records(&committed).unwrap();
    match admin
        .admit_native_once_engine(key.clone(), [1; 32])
        .unwrap()
    {
        NativeOnceAdmission::Replay(r) => assert_eq!(r, receipt),
        _ => panic!("replay"),
    };
    assert_eq!(records(&admin.inner.read().unwrap()).unwrap(), exact);
    let key = NativeOnceKey {
        database_domain: key.database_domain,
        identity: vec![43],
    };
    let owner = match admin.admit_native_once_engine(key, [3; 32]).unwrap() {
        NativeOnceAdmission::Owner(o) => o,
        _ => panic!("owner"),
    };
    let mut tx = admin.sql_session();
    tx.begin_engine().unwrap();
    assert_eq!(
        tx.execute("INSERT INTO items VALUES (2,'a')", &[], None),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    );
    tx.rollback();
    assert_eq!(records(&admin.inner.read().unwrap()).unwrap(), exact);
    // Duplicate is not a retainable terminal business error in the existing
    // once result format. A separate missing-row decision exercises terminal
    // receipt publication without widening that existing closed error set.
    let mut tx = admin.sql_session();
    tx.begin_engine().unwrap();
    assert!(
        tx.execute("SELECT * FROM items WHERE id=999", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    let payload = crate::operations::once::outcome::encode(
        &description,
        &Err(crate::operations::Error::NotFound),
    )
    .unwrap();
    tx.publish_native_once_engine(
        owner,
        [4; 32],
        payload,
        10,
        NativePublishMode::Terminal,
        &mut || clock(),
    )
    .unwrap();
    let after = admin.inner.read().unwrap();
    // Allowed: terminal decision ledger and whole-image generation. Protected:
    // all SQL business rows/indexes/IDs/versions/epoch, legacy view, accounting.
    let mut normalized = after.sql.clone();
    normalized.generation = committed.sql.generation;
    assert_eq!(normalized, committed.sql);
    assert_eq!(after.view, committed.view);
    assert_eq!(after.accounting, committed.accounting);
    assert!(after.sql.generation > committed.sql.generation);
    assert_ne!(
        after.native_outcomes.as_ref().unwrap().to_bytes().unwrap(),
        committed
            .native_outcomes
            .as_ref()
            .unwrap()
            .to_bytes()
            .unwrap()
    );
    assert_eq!(records(&after).unwrap()[&vec![8]], exact[&vec![8]]);
}
#[test]
fn unique_actual_crash_before_after_preserves_paired_bytes() {
    for mode in ["before", "after"] {
        let dir = path("unique-process-crash");
        let s = unique_fixture(false, false, false, true, true);
        let before = records(&s).unwrap();
        drop(Admin::create_state(&dir, Limits::default(), s, RecoveryPoint::None).unwrap());
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::secondary_index::tests::process_crash_child",
                "--nocapture",
            ])
            .env("GD42_W2_CRASH_DIR", &dir)
            .env("GD42_W2_CRASH_MODE", mode)
            .output()
            .unwrap();
        assert!(!child.status.success());
        let admin = Admin::open(&dir, Limits::default()).unwrap();
        let after = records(&admin.inner.read().unwrap()).unwrap();
        if mode == "before" {
            assert_eq!(after, before);
        } else {
            assert_ne!(after[&vec![5]], before[&vec![5]]);
            assert_ne!(after[&vec![8]], before[&vec![8]]);
        }
        assert_eq!(
            records(&from_records(after.clone()).unwrap()).unwrap(),
            after
        );
        assert_eq!(&after[&vec![8]][8..12], &2u32.to_le_bytes());
    }
}

#[path = "logical_archive_tests.rs"]
mod logical_archive_tests;
