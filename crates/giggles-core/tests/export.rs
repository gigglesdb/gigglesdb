mod common;
use common::*;
use giggles_core::*;
#[test]
fn online_export_restore_fresh_fenced_authority() {
    let (_, a, e) = fixture(3);
    let d = e
        .execute(request(1, &[0]), Time::Certified(10), None)
        .unwrap();
    assert!(d.is_ok());
    let export = fresh("export");
    a.export(&export).unwrap();
    let snapshot = Admin::verify_export(&export).unwrap();
    assert_eq!(snapshot, a.snapshot().unwrap());
    // Export remains an earlier consistent cut while live writes continue.
    assert!(
        e.execute(request(2, &[1]), Time::Certified(11), None)
            .unwrap()
            .is_ok()
    );
    let dest = fresh("restore");
    let restored = Admin::restore(&export, &dest, Limits::default()).unwrap();
    let s = restored.snapshot().unwrap();
    assert_ne!(s.domain, snapshot.domain);
    assert_eq!(s.rows, snapshot.rows);
    assert_eq!(s.operations, snapshot.operations);
    assert!(s.tasks[&TaskId(1)].revoked);
    let re = restored.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(
        re.execute(request(3, &[2]), Time::Certified(12), None)
            .unwrap(),
        Err(Error::Revoked)
    );
    assert_eq!(
        re.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(d)
    );
    assert!(Admin::restore(&export, &dest, Limits::default()).is_err());
    assert!(a.export(&export).is_err());
    assert_eq!(
        restored.authority_metadata().unwrap().3,
        Some(snapshot.domain)
    );
}
#[test]
fn corrupt_truncated_extended_unknown_version_exports_reject() {
    let (_, a, _) = fixture(1);
    let export = fresh("valid-export");
    a.export(&export).unwrap();
    let valid = std::fs::read(&export).unwrap();
    for mode in 0..6 {
        let mut bytes = valid.clone();
        match mode {
            0 => bytes[55] ^= 1,
            1 => bytes.truncate(25),
            2 => bytes.push(0),
            3 => bytes[11] = 2,
            4 => bytes[0] = 0,
            5 => bytes[12..20].copy_from_slice(&u64::MAX.to_be_bytes()),
            _ => unreachable!(),
        }
        let file = fresh(&format!("corrupt-{mode}"));
        std::fs::write(&file, &bytes).unwrap();
        assert!(Admin::verify_export(&file).is_err());
        let dest = fresh("rejected-restore");
        assert!(Admin::restore(&file, &dest, Limits::default()).is_err());
        assert!(!dest.exists());
        assert_eq!(std::fs::read(file).unwrap(), bytes);
    }
}
#[test]
fn approval_ids_and_time_are_exported() {
    let (_, a, e) = fixture(1);
    let p = e.preview(&request(1, &[0]), Time::Certified(60)).unwrap();
    let id = a.approve(Principal(3), p, Time::Certified(60)).unwrap();
    let file = fresh("authority-export");
    a.export(&file).unwrap();
    let restored = Admin::restore(file, fresh("authority-restored"), Limits::default()).unwrap();
    let (_, time, next, _) = restored.authority_metadata().unwrap();
    assert_eq!(time, 60);
    assert_eq!(next, id.0 + 1);
    assert!(restored.snapshot().unwrap().approvals[&id].revoked);
}
#[test]
fn export_under_concurrent_writes_is_consistent() {
    let (_, a, e) = fixture(25);
    let writer = std::thread::spawn(move || {
        for n in 0..25 {
            e.execute(request(n, &[n]), Time::Certified(10), None)
                .unwrap()
                .unwrap();
        }
    });
    for _ in 0..8 {
        let file = fresh("online-export");
        a.export(&file).unwrap();
        let s = Admin::verify_export(file).unwrap();
        let changed = s
            .rows
            .values()
            .filter(|r| r.status == Status::Queued)
            .count();
        assert_eq!(changed, s.operations.len());
        assert_eq!(changed as u64, s.tasks[&TaskId(1)].transitions);
    }
    writer.join().unwrap();
}
