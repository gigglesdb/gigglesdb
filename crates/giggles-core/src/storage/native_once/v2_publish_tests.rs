use super::*;
use crate::storage::engine::{kv::{Direction, Keyspace}, meta, txn::Isolation};
use std::{ops::Bound::Unbounded, path::PathBuf};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}
fn fixture() -> (Dir, Admin) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = crate::test_artifact_root();
    fs::create_dir_all(&root).unwrap();
    let dir = Dir(root.join(format!("t10-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE items(id BIGINT PRIMARY KEY, n BIGINT)", &[], None).unwrap();
    sql.execute("CREATE INDEX items_n ON items(n)", &[], None).unwrap();
    drop(sql);
    (dir, admin)
}
fn db(admin: &Admin) -> &crate::storage::v2::V2Db {
    let Backend::V2(db) = &admin.inner.backend else { panic!("v2") };
    db
}
fn key(admin: &Admin) -> NativeOnceKey {
    NativeOnceKey { database_domain: u128::from_be_bytes(db(admin).meta.domain), identity: vec![10] }
}
fn payload() -> Vec<u8> {
    use crate::operations::once::{OnceDescription, OnceOutcome, OnceSchema};
    crate::operations::once::outcome::encode(
        &OnceDescription { catalog_epoch: 1, output: OnceSchema::Operation(vec![]) },
        &Ok(OnceOutcome::Operation(crate::operations::Outcome::default())),
    ).unwrap()
}
fn publish(admin: &Admin, expired: bool) -> NativeResult<(NativeOnceRecord, bool)> {
    let NativeOnceAdmission::Owner(owner) = admin.admit_native_once_engine(key(admin), [1; 32])? else { panic!("owner") };
    let mut session = admin.sql_session();
    let setup = session.begin_once_engine().unwrap();
    session.execute("INSERT INTO items VALUES(1,10)", &[], None).unwrap();
    session.publish_native_once_engine(Some(setup), owner, [2; 32], payload(), 60, NativePublishMode::Success, &mut || {
        Ok(NativeClockLease { lower_unix_seconds: 100, upper_unix_seconds: 110,
            valid_until: if expired { Instant::now() } else { Instant::now() + Duration::from_secs(10) } })
    })
}
fn assert_row_and_index(admin: &Admin) {
    let rows = admin.sql_session().execute("SELECT id,n FROM items WHERE n=10", &[], None).unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![relational::Value::Int(1), relational::Value::Int(10)]);
    let mut txn = db(admin).begin(Isolation::RepeatableRead);
    let index = db(admin).catalog().snapshot(&mut txn).unwrap().parts.indexes.iter()
        .find(|index| index.name == "items_n").unwrap().id.to_be_bytes();
    let entries = txn.scan(Keyspace::Index, Unbounded, Unbounded, Direction::Forward, None).unwrap()
        .collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.iter().filter(|(key, _)| key.starts_with(&index)).count(), 1);
}

#[test]
fn success_reopens_row_index_identical_receipt_and_native_metadata() {
    let (dir, admin) = fixture();
    let identity = key(&admin);
    let (record, replayed) = publish(&admin, false).unwrap();
    assert!(!replayed);
    assert_eq!(record.result, Some(payload()));
    assert_eq!(admin.lookup_native_once_engine(&identity).unwrap(), NativeOnceLookup::Retained(record.clone()));
    assert_row_and_index(&admin);
    let metadata = meta::validate_meta(db(&admin).kv.as_ref()).unwrap();
    assert_eq!(metadata.subversion, 2);
    assert_ne!(metadata.features & meta::NATIVE_OUTCOMES, 0);
    assert_eq!(admin.inner.sql_transactions.load(Ordering::Acquire), 0);
    drop(admin);
    let reopened = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_row_and_index(&reopened);
    assert_eq!(reopened.lookup_native_once_engine(&identity).unwrap(), NativeOnceLookup::Retained(record));
}

#[test]
fn identical_admission_replays_and_different_submission_conflicts() {
    let (_dir, admin) = fixture();
    let (record, _) = publish(&admin, false).unwrap();
    assert!(matches!(admin.admit_native_once_engine(key(&admin), [1; 32]).unwrap(),
        NativeOnceAdmission::Replay(replayed) if replayed == record));
    assert!(matches!(admin.admit_native_once_engine(key(&admin), [3; 32]), Err(NativeOnceError::IdempotencyConflict)));
    assert_row_and_index(&admin);
}

#[test]
fn expired_publication_lease_leaves_no_row_or_key() {
    let (_dir, admin) = fixture();
    assert_eq!(publish(&admin, true), Err(NativeOnceError::ClockUncertain));
    assert!(admin.sql_session().execute("SELECT * FROM items", &[], None).unwrap().rows.is_empty());
    assert_eq!(admin.lookup_native_once_engine(&key(&admin)).unwrap(), NativeOnceLookup::Absent);
    assert_eq!(admin.inner.sql_transactions.load(Ordering::Acquire), 0);
    assert!(!admin.inner.fenced.load(Ordering::Acquire));
    publish(&admin, false).unwrap();
    assert_row_and_index(&admin);
}

#[test]
fn unrelated_commit_rejects_stale_once_without_consuming_key() {
    let (_dir, admin) = fixture();
    admin.sql_session().execute("CREATE TABLE other(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    let identity = key(&admin);
    let NativeOnceAdmission::Owner(owner) = admin.admit_native_once_engine(identity.clone(), [1; 32]).unwrap() else { panic!("owner") };
    let mut session = admin.sql_session();
    let setup = session.begin_once_engine().unwrap();
    session.execute("INSERT INTO items VALUES(1,10)", &[], None).unwrap();
    admin.sql_session().execute("INSERT INTO other VALUES(2)", &[], None).unwrap();
    let result = session.publish_native_once_engine(Some(setup), owner, [2; 32], payload(), 60,
        NativePublishMode::Success, &mut || Ok(NativeClockLease {
            lower_unix_seconds: 100, upper_unix_seconds: 110,
            valid_until: Instant::now() + Duration::from_secs(10),
        }));
    assert!(matches!(result, Err(NativeOnceError::Core(CoreError::Sql(relational::SqlError::Conflict)))));
    assert_eq!(admin.lookup_native_once_engine(&identity).unwrap(), NativeOnceLookup::Absent);
    assert_eq!(admin.sql_session().execute("SELECT * FROM items WHERE id=1", &[], None).unwrap().rows.len(), 0);
    publish(&admin, false).unwrap();
    assert_row_and_index(&admin);
}

#[test]
fn terminal_aborted_session_discards_sequences_features_and_prior_writes() {
    let (_dir, admin) = fixture();
    let index_entries = || {
        db(&admin).begin(Isolation::RepeatableRead).scan(Keyspace::Index, Unbounded, Unbounded,
            Direction::Forward, None).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    };
    let before_index = index_entries();
    let encoded = crate::operations::once::outcome::encode(
        &crate::operations::once::OnceDescription { catalog_epoch: 1,
            output: crate::operations::once::OnceSchema::Operation(vec![]) },
        &Err(crate::operations::Error::RequirementFailed)).unwrap();
    let before_meta = meta::validate_meta(db(&admin).kv.as_ref()).unwrap();
    let NativeOnceAdmission::Owner(owner) = admin.admit_native_once_engine(key(&admin), [1; 32]).unwrap()
        else { panic!("owner") };
    let mut session = admin.sql_session();
    let setup = session.begin_once_engine().unwrap();
    session.execute("INSERT INTO items VALUES(1,10)", &[], None).unwrap();
    let pending = session.pending_sequences.clone();
    pending.lock().unwrap().insert(u64::MAX, crate::storage::sequences::SeqCounter {
        incarnation: 1, last_value: 9, is_called: true, log_hw: 9, stamp: 1 });
    session.deferral.pending.entry((u64::MAX, u64::MAX)).or_default().insert(vec![1]);
    session.deferral.modes.insert(u64::MAX, true);
    let txn = session.v2_transaction.as_mut().unwrap();
    assert_eq!(before_meta.features & 16, 0);
    txn.require_features(16);
    let counter_key = crate::storage::engine::catalog_codec::counter_key(u64::MAX, 1);
    txn.put_counter(counter_key.clone(), vec![0; 25]).unwrap();
    txn.require_subversion(u32::MAX);
    assert!(session.execute("INSERT INTO missing VALUES(1)", &[], None).is_err());
    assert!(session.aborted);
    session.publish_native_once_engine(Some(setup), owner, [2; 32], encoded.clone(), 60,
        NativePublishMode::Terminal, &mut || Ok(NativeClockLease { lower_unix_seconds: 100,
            upper_unix_seconds: 110, valid_until: Instant::now() + Duration::from_secs(10) })).unwrap();
    assert_eq!(index_entries(), before_index);
    assert_eq!(admin.inner.sql_transactions.load(Ordering::Acquire), 0);
    assert!(admin.sql_session().execute("SELECT * FROM items WHERE n=10", &[], None).unwrap().rows.is_empty());
    let after_meta = meta::validate_meta(db(&admin).kv.as_ref()).unwrap();
    assert_eq!(after_meta.features, before_meta.features | meta::NATIVE_OUTCOMES);
    assert_eq!(after_meta.subversion, 2);
    assert!(pending.lock().unwrap().is_empty());
    assert!(db(&admin).begin(Isolation::RepeatableRead).get(Keyspace::Seq, &counter_key).unwrap().is_none());
    assert!(matches!(admin.lookup_native_once_engine(&key(&admin)).unwrap(),
        NativeOnceLookup::Retained(record) if record.result == Some(encoded)));
}

// Native defaults lack an xact callback; assign through the session slot to
// exercise the same guard ownership that clocked SQL supplies.
#[test]
fn publication_xact_marker_tracks_success_terminal_and_rejection() {
    use crate::storage::engine::xact::XactStatus;
    for (mode, stale, committed) in [
        (NativePublishMode::Success, false, true),
        (NativePublishMode::Terminal, false, true),
        (NativePublishMode::Success, true, false),
        (NativePublishMode::Terminal, true, false),
    ] {
        let (dir, admin) = fixture();
        admin.sql_session().execute("CREATE TABLE unrelated(id BIGINT PRIMARY KEY)", &[], None).unwrap();
        let identity = key(&admin);
        let NativeOnceAdmission::Owner(owner) = admin.admit_native_once_engine(identity.clone(), [1; 32]).unwrap()
            else { panic!("owner") };
        let mut session = admin.sql_session();
        let setup = session.begin_once_engine().unwrap();
        let guard = db(&admin).xacts.assign().unwrap();
        let xid = guard.id();
        *session.xact.lock().unwrap() = Some(guard);
        session.execute("INSERT INTO items VALUES(1,10)", &[], None).unwrap();
        if stale {
            admin.sql_session().execute("INSERT INTO unrelated VALUES(1)", &[], None).unwrap();
        }
        let result = session.publish_native_once_engine(Some(setup), owner, [2; 32], payload(), 60, mode,
            &mut || Ok(NativeClockLease { lower_unix_seconds: 100, upper_unix_seconds: 110,
                valid_until: Instant::now() + Duration::from_secs(10) }));
        if committed { assert!(result.is_ok(), "{result:?}"); }
        else { assert!(matches!(result, Err(NativeOnceError::Core(CoreError::Sql(relational::SqlError::Conflict))))); }
        let status = if committed { XactStatus::Committed } else { XactStatus::Aborted };
        assert_eq!(db(&admin).xacts.status(xid).unwrap(), Some(status));
        drop(admin);
        let reopened = Admin::open(&dir.0, Limits::default()).unwrap();
        assert_eq!(db(&reopened).xacts.status(xid).unwrap(), Some(status));
        assert_eq!(matches!(reopened.lookup_native_once_engine(&identity).unwrap(), NativeOnceLookup::Retained(_)), committed);
    }
}

#[test]
fn attached_xact_guarded_commit_writes_marker_and_rejection_does_not() {
    use crate::storage::engine::{once_guard::CommitGuard, xact::XactStatus};
    let (_dir, admin) = fixture();
    for expired in [false, true] {
        let guard = db(&admin).xacts.assign().unwrap();
        let xid = guard.id();
        let mut txn = db(&admin).begin(Isolation::RepeatableRead);
        txn.attach_xact(guard).unwrap();
        let result = txn.commit_guarded(CommitGuard::capture_unfenced(
            if expired { Instant::now() } else { Instant::now() + Duration::from_secs(10) }), |_| Ok(()));
        assert_eq!(result.is_ok(), !expired);
        assert_eq!(db(&admin).xacts.status(xid).unwrap(), Some(
            if expired { XactStatus::Aborted } else { XactStatus::Committed }));
    }
}
#[test]
fn lost_reply_receipt_and_xact_status_agree_after_reopen() {
    use crate::storage::engine::xact::XactStatus;
    for point in [CrashPoint::ErrorAfterCommit, CrashPoint::ErrorBeforeCommit] {
        let (dir, admin) = fixture();
        let identity = key(&admin);
        let NativeOnceAdmission::Owner(owner) = admin.admit_native_once_engine(identity.clone(), [1; 32]).unwrap()
            else { panic!("owner") };
        let mut session = admin.sql_session();
        let setup = session.begin_once_engine().unwrap();
        let guard = db(&admin).xacts.assign().unwrap();
        let xid = guard.id();
        *session.xact.lock().unwrap() = Some(guard);
        session.execute("INSERT INTO items VALUES(1,10)", &[], None).unwrap();
        admin.set_crash_point(point).unwrap();
        let result = session.publish_native_once_engine(Some(setup), owner, [2; 32], payload(), 60,
            NativePublishMode::Success, &mut || Ok(NativeClockLease {
                lower_unix_seconds: 100, upper_unix_seconds: 110,
                valid_until: Instant::now() + Duration::from_secs(10),
            }));
        let committed = point == CrashPoint::ErrorAfterCommit;
        assert_eq!(result, Err(NativeOnceError::Core(CoreError::Storage(
            if committed { "injected lost commit response" } else { "injected before commit" }.into()))));
        if committed {
            assert_eq!(admin.lookup_native_once_engine(&identity), Err(NativeOnceError::Core(CoreError::Fenced)));
            assert!(matches!(admin.admit_native_once_engine(identity.clone(), [1; 32]),
                Err(NativeOnceError::Core(CoreError::Fenced))));
        }
        drop(admin);
        let reopened = Admin::open(&dir.0, Limits::default()).unwrap();
        let lookup = reopened.lookup_native_once_engine(&identity).unwrap();
        if committed {
            assert!(matches!(lookup, NativeOnceLookup::Retained(record) if record.result == Some(payload())));
            assert_eq!(db(&reopened).xacts.status(xid).unwrap(), Some(XactStatus::Committed));
            assert_row_and_index(&reopened);
        } else {
            assert_eq!(lookup, NativeOnceLookup::Absent);
            assert_eq!(db(&reopened).xacts.status(xid).unwrap(), Some(XactStatus::Aborted));
            assert!(reopened.sql_session().execute("SELECT * FROM items", &[], None).unwrap().rows.is_empty());
        }
    }
}
