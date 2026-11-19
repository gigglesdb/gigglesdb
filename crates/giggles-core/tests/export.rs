mod common;
use common::*;
use giggles_core::*;

fn ordinary_fixture(n: u64) -> (std::path::PathBuf, Admin) {
    let p = fresh("ordinary-export");
    let a = Admin::create(&p, Limits::default()).unwrap();
    for i in 0..n {
        a.put(key(i), row()).unwrap();
    }
    (p, a)
}

#[test]
fn ordinary_corrupt_truncated_extended_unknown_version_exports_reject() {
    let (_, a) = ordinary_fixture(1);
    let export = fresh("valid-export");
    a.export(&export).unwrap();
    let valid = std::fs::read(&export).unwrap();
    for mode in 0..6 {
        let mut bytes = valid.clone();
        match mode {
            0 => bytes[55] ^= 1,
            1 => bytes.truncate(25),
            2 => bytes.push(0),
            3 => bytes[11] = 99,
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
fn ordinary_export_restore_fresh_identity() {
    let (_, a) = ordinary_fixture(3);
    let export = fresh("export");
    a.export(&export).unwrap();
    let snapshot = Admin::verify_export(&export).unwrap();
    assert_eq!(snapshot, a.snapshot().unwrap());
    // Export remains an earlier consistent cut while live writes continue.
    a.put(
        key(1),
        Row {
            status: Status::Queued,
            ..row()
        },
    )
    .unwrap();
    let dest = fresh("restore");
    let restored = Admin::restore(&export, &dest, Limits::default()).unwrap();
    let s = restored.snapshot().unwrap();
    assert_ne!(s.domain, snapshot.domain);
    assert_eq!(s.rows, snapshot.rows);
    assert_eq!(s.operations, snapshot.operations);
    assert!(Admin::restore(&export, &dest, Limits::default()).is_err());
    assert!(a.export(&export).is_err());
    assert_eq!(
        restored.authority_metadata().unwrap().3,
        Some(snapshot.domain)
    );
}

#[test]
fn ordinary_export_under_concurrent_writes_is_consistent() {
    let (_, a) = ordinary_fixture(25);
    a.sql_session()
        .execute(
            "CREATE TABLE correlated (tx BIGINT, part BIGINT, PRIMARY KEY (tx, part))",
            &[],
            None,
        )
        .unwrap();
    let writer_admin = a.clone();
    let writer = std::thread::spawn(move || {
        let mut sql = writer_admin.sql_session();
        for n in 0..25 {
            sql.execute("BEGIN", &[], None).unwrap();
            sql.execute(
                &format!("INSERT INTO correlated VALUES ({n}, 0), ({n}, 1)"),
                &[],
                None,
            )
            .unwrap();
            sql.execute("COMMIT", &[], None).unwrap();
            writer_admin
                .put(
                    key(n),
                    Row {
                        status: Status::Queued,
                        ..row()
                    },
                )
                .unwrap();
        }
    });
    for _ in 0..8 {
        let file = fresh("online-export");
        a.export(&file).unwrap();
        let s = Admin::verify_export(&file).unwrap();
        let changed = s
            .rows
            .values()
            .filter(|r| r.status == Status::Queued)
            .count();
        assert!(changed <= 25);
        assert_eq!(s.rows.len(), 25);
        let restored = Admin::restore(&file, fresh("online-restored"), Limits::default()).unwrap();
        assert_eq!(restored.snapshot().unwrap().rows, s.rows);
        let rows = restored
            .sql_session()
            .execute(
                "SELECT tx, part FROM correlated ORDER BY tx, part",
                &[],
                None,
            )
            .unwrap()
            .rows;
        for tx in 0..25 {
            let parts: Vec<_> = rows
                .iter()
                .filter(|r| r.values[0] == relational::Value::Int(tx))
                .map(|r| r.values[1].clone())
                .collect();
            assert!(
                parts.is_empty()
                    || parts == vec![relational::Value::Int(0), relational::Value::Int(1)],
                "export contains a partial SQL transaction {tx}: {parts:?}"
            );
        }
    }
    writer.join().unwrap();
}
