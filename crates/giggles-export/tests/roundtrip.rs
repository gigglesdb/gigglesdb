mod common;
use common::*;
use giggles_core::relational::Value;
use giggles_core::{Admin, Limits};
use giggles_export::*;

#[test]
fn legacy_rows_and_task_authority_reject() {
    let (p, a) = populated("legacy-denied");
    a.put(
        giggles_core::Key {
            tenant: giggles_core::TenantId(1),
            row: giggles_core::RowId(1),
        },
        giggles_core::Row {
            status: giggles_core::Status::Failed,
            failed_at: 1,
            attempt_count: 0,
            version: 0,
        },
    )
    .unwrap();
    assert!(export(&a, p.join("copy"), Bounds::default()).is_err());
    assert!(!p.join("copy").exists());
    let (_p, a) = db("authority-denied");
    a.grant(giggles_core::Grant {
        task: giggles_core::TaskId(1),
        issuer: giggles_core::Principal(1),
        executor: giggles_core::Principal(2),
        approver: giggles_core::Principal(3),
        policy: giggles_core::Policy {
            tenant: giggles_core::TenantId(1),
            columns: [giggles_core::Column::Status].into_iter().collect(),
            transitions: [(giggles_core::Status::Failed, giggles_core::Status::Queued)]
                .into_iter()
                .collect(),
            incident: (0, 10),
            distinct_limit: 1,
            transition_limit: 1,
            approval_above: 1,
            valid_from: 0,
            expires: 10,
        },
    })
    .unwrap();
    assert!(capture(&a, Bounds::default()).is_err());
}

#[test]
fn every_type_composite_roundtrip() {
    let (p, a) = db("types");
    let mut s = a.sql_session();
    s.execute("CREATE TABLE typed (z TEXT NOT NULL, k BYTEA NOT NULL, flag BOOLEAN, small SMALLINT, normal INTEGER, big BIGINT, body TEXT, blob BYTEA, PRIMARY KEY (k,z))",&[],None).unwrap();
    let mut cases = Vec::new();
    for (i, k) in [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![255],
    ]
    .into_iter()
    .enumerate()
    {
        for z in ["", "\0", "é", "e\u{301}", "雪😀"] {
            cases.push(vec![
                Value::Text(z.into()),
                Value::Bytes(k.clone()),
                Value::Bool(i % 2 == 0),
                Value::Int(if i % 2 == 0 {
                    i16::MIN as i64
                } else {
                    i16::MAX as i64
                }),
                Value::Int(if i % 2 == 0 {
                    i32::MIN as i64
                } else {
                    i32::MAX as i64
                }),
                Value::Int(if i % 2 == 0 { i64::MIN } else { i64::MAX }),
                Value::Text("\r\n雪\0😀\"\\".into()),
                Value::Bytes((0..=255).collect()),
            ]);
        }
    }
    cases.push(vec![
        Value::Text("null".into()),
        Value::Bytes(vec![2]),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ]);
    s.execute("BEGIN", &[], None).unwrap();
    for row in &cases {
        s.execute(
            "INSERT INTO typed VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            row,
            None,
        )
        .unwrap();
    }
    s.execute("COMMIT", &[], None).unwrap();
    drop(s);
    let archive = capture(&a, Bounds::default()).unwrap();
    let report = export(&a, p.join("copy.jsonl"), Bounds::default()).unwrap();
    assert_eq!(report.rows, cases.len());
    import(p.join("copy.jsonl"), p.join("new"), Bounds::default()).unwrap();
    let b = Admin::open(p.join("new"), Limits::default()).unwrap();
    assert_eq!(capture(&b, Bounds::default()).unwrap(), archive);
    assert_ne!(a.snapshot().unwrap().domain, b.snapshot().unwrap().domain);
    assert!(b.snapshot().unwrap().tasks.is_empty());
    assert!(b.snapshot().unwrap().approvals.is_empty());
    assert!(b.snapshot().unwrap().operations.is_empty());
}
#[test]
fn empty_database_and_tables() {
    let (p, a) = db("empty");
    for n in 0..2 {
        if n == 1 {
            a.sql_session()
                .execute("CREATE TABLE empty (id INTEGER PRIMARY KEY)", &[], None)
                .unwrap();
        }
        let file = p.join(format!("{n}.jsonl"));
        export(&a, &file, Bounds::default()).unwrap();
        import(file, p.join(format!("new{n}")), Bounds::default()).unwrap();
    }
}
#[test]
fn query_equivalence() {
    let (p, a) = populated("queries");
    export(&a, p.join("a"), Bounds::default()).unwrap();
    import(p.join("a"), p.join("new"), Bounds::default()).unwrap();
    let b = Admin::open(p.join("new"), Limits::default()).unwrap();
    for (sql, params) in [
        ("SELECT id,body FROM items ORDER BY id LIMIT 1024", vec![]),
        ("SELECT body FROM items WHERE id=$1", vec![Value::Int(2)]),
        ("SELECT COUNT(*),MIN(id),MAX(id),SUM(id) FROM items", vec![]),
        (
            "SELECT id FROM items WHERE id >= $1 ORDER BY id LIMIT 10",
            vec![Value::Int(-1)],
        ),
    ] {
        let x = a.sql_session().execute(sql, &params, None).unwrap();
        let y = b.sql_session().execute(sql, &params, None).unwrap();
        assert_eq!(x.columns, y.columns);
        assert_eq!(
            x.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
            y.rows.iter().map(|r| &r.values).collect::<Vec<_>>()
        );
    }
}
#[test]
fn fresh_domain_versions() {
    let (p, a) = populated("versions");
    let mut s = a.sql_session();
    let r = s
        .execute("SELECT id,body FROM items WHERE id=2", &[], None)
        .unwrap();
    s.execute(
        "UPDATE items SET body=$1 WHERE id=2",
        &[Value::Text("updated".into())],
        Some(r.rows[0].version),
    )
    .unwrap();
    let old = s
        .execute("SELECT id,body FROM items WHERE id=2", &[], None)
        .unwrap()
        .rows[0]
        .version;
    drop(s);
    export(&a, p.join("a"), Bounds::default()).unwrap();
    import(p.join("a"), p.join("new"), Bounds::default()).unwrap();
    let b = Admin::open(p.join("new"), Limits::default()).unwrap();
    let new = b
        .sql_session()
        .execute("SELECT id,body FROM items WHERE id=2", &[], None)
        .unwrap()
        .rows[0]
        .version;
    assert_ne!(old, new);
    assert_ne!(a.snapshot().unwrap().domain, b.snapshot().unwrap().domain);
}
#[test]
fn coherent_concurrent_dml_ddl() {
    let (_p, a) = populated("cut");
    a.sql_session()
        .execute("CREATE TABLE second (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let old = capture(&a, Bounds::default()).unwrap();
    let writer = a.clone();
    let cut = capture_with_hook(&a, Bounds::default(), &mut |point| {
        if point == Point::Snapshot {
            let writer = writer.clone();
            std::thread::spawn(move || {
                let mut s = writer.sql_session();
                s.execute("BEGIN", &[], None).unwrap();
                s.execute("INSERT INTO items VALUES (3,'later')", &[], None)
                    .unwrap();
                s.execute("INSERT INTO second VALUES (7)", &[], None)
                    .unwrap();
                s.execute("CREATE TABLE third (id BIGINT PRIMARY KEY)", &[], None)
                    .unwrap();
                s.execute("COMMIT", &[], None).unwrap();
            })
            .join()
            .unwrap();
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(cut, old);
    assert_ne!(capture(&a, Bounds::default()).unwrap(), old);
}
#[test]
fn catalog_race_rejects() {
    let (_p, a) = populated("catalog-race");
    let writer = a.clone();
    assert!(
        capture_with_hook(&a, Bounds::default(), &mut |point| {
            if point == Point::CatalogBefore {
                writer.sql_session().execute(
                    "CREATE TABLE later (id BIGINT PRIMARY KEY)",
                    &[],
                    None,
                )?;
            }
            Ok(())
        })
        .is_err()
    );
    assert!(capture(&a, Bounds::default()).is_ok());
}
#[test]
fn failed_import_retained_atomic_schema_and_rows() {
    let (p, a) = populated("atomic");
    export(&a, p.join("a"), Bounds::default()).unwrap();
    assert!(
        import_with_hook(
            p.join("a"),
            p.join("new"),
            Bounds::default(),
            &mut |pt| if pt == Point::ImportStaged {
                Err(Error::Injected)
            } else {
                Ok(())
            }
        )
        .is_err()
    );
    assert!(p.join("new/data.redb").is_file());
    assert!(!p.join("new/logical-import-complete.json").exists());
    let b = Admin::open(p.join("new"), Limits::default()).unwrap();
    assert!(b.catalog().unwrap().1.is_empty());
    assert!(import(p.join("a"), p.join("new"), Bounds::default()).is_err());
}
#[test]
fn invalid_input_no_target() {
    let p = temp("invalid-input");
    std::fs::write(p.join("a"), b"junk").unwrap();
    assert!(import(p.join("a"), p.join("new"), Bounds::default()).is_err());
    assert!(!p.join("new").exists());
}
#[test]
fn wrapper_catalog_denied() {
    for name in ["ledger_state", "outbox_internal_state"] {
        let (p, a) = db(name);
        a.sql_session()
            .execute(
                &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY)"),
                &[],
                None,
            )
            .unwrap();
        assert!(export(&a, p.join("a"), Bounds::default()).is_err());
        assert!(!p.join("a").exists());
    }
}
#[test]
fn exactly_max_rows_and_oversized_composite_reject() {
    let (_p, a) = db("row-cap");
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE many (a BIGINT NOT NULL,b BIGINT NOT NULL,PRIMARY KEY (a,b))",
        &[],
        None,
    )
    .unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    for n in 0..1024 {
        s.execute(
            "INSERT INTO many VALUES ($1,$2)",
            &[Value::Int(n / 512), Value::Int(n % 512)],
            None,
        )
        .unwrap();
    }
    s.execute("COMMIT", &[], None).unwrap();
    assert_eq!(capture(&a, Bounds::default()).unwrap().rows[0].len(), 1024);
    s.execute("INSERT INTO many VALUES (2,0)", &[], None)
        .unwrap();
    assert!(matches!(capture(&a, Bounds::default()), Err(Error::Limit)));
}
#[test]
fn same_prefix_overflow_reject() {
    let (_p, a) = db("suffix-cap");
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE many (a TEXT NOT NULL,b BIGINT NOT NULL,PRIMARY KEY (a,b))",
        &[],
        None,
    )
    .unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    for n in 0..1025 {
        s.execute(
            "INSERT INTO many VALUES ('same',$1)",
            &[Value::Int(n)],
            None,
        )
        .unwrap();
    }
    s.execute("COMMIT", &[], None).unwrap();
    assert!(matches!(capture(&a, Bounds::default()), Err(Error::Limit)));
}
#[test]
fn snapshot_failures_release_transaction_slots() {
    let (_p, a) = populated("slots");
    for _ in 0..20 {
        assert!(
            capture_with_hook(&a, Bounds::default(), &mut |pt| if pt == Point::Snapshot {
                Err(Error::Injected)
            } else {
                Ok(())
            })
            .is_err()
        );
    }
    assert!(capture(&a, Bounds::default()).is_ok());
}
#[test]
fn primary_key_type_matrix() {
    let (p, a) = db("pk-matrix");
    let mut s = a.sql_session();
    let cases = [
        ("Boolean", vec![Value::Bool(false), Value::Bool(true)]),
        (
            "SMALLINT",
            vec![
                Value::Int(i16::MIN as i64),
                Value::Int(0),
                Value::Int(i16::MAX as i64),
            ],
        ),
        (
            "INTEGER",
            vec![
                Value::Int(i32::MIN as i64),
                Value::Int(0),
                Value::Int(i32::MAX as i64),
            ],
        ),
        (
            "BIGINT",
            vec![
                Value::Int(i64::MIN),
                Value::Int(-1),
                Value::Int(0),
                Value::Int(i64::MAX),
            ],
        ),
        (
            "TEXT",
            vec![
                Value::Text("".into()),
                Value::Text("\0".into()),
                Value::Text("雪".into()),
            ],
        ),
        (
            "BYTEA",
            vec![
                Value::Bytes(vec![]),
                Value::Bytes(vec![0]),
                Value::Bytes(vec![255]),
            ],
        ),
    ];
    for (i, (kind, values)) in cases.into_iter().enumerate() {
        s.execute(
            &format!("CREATE TABLE key{i} (id {kind} PRIMARY KEY)"),
            &[],
            None,
        )
        .unwrap();
        for v in values {
            s.execute(&format!("INSERT INTO key{i} VALUES ($1)"), &[v], None)
                .unwrap();
        }
    }
    drop(s);
    let expected = capture(&a, Bounds::default()).unwrap();
    export(&a, p.join("copy"), Bounds::default()).unwrap();
    import(p.join("copy"), p.join("fresh"), Bounds::default()).unwrap();
    let b = Admin::open(p.join("fresh"), Limits::default()).unwrap();
    assert_eq!(capture(&b, Bounds::default()).unwrap(), expected);
}
#[test]
fn export_import_mutation_state_machine() {
    let (p, mut a) = populated("state-machine");
    for step in 0..12 {
        let mut s = a.sql_session();
        let old = s
            .execute("SELECT id,body FROM items WHERE id=2", &[], None)
            .unwrap();
        s.execute(
            "UPDATE items SET body=$1 WHERE id=2",
            &[if step % 3 == 0 {
                Value::Null
            } else {
                Value::Text(format!("step{step}雪"))
            }],
            Some(old.rows[0].version),
        )
        .unwrap();
        drop(s);
        let expected = capture(&a, Bounds::default()).unwrap();
        let file = p.join(format!("copy{step}"));
        let target = p.join(format!("db{step}"));
        export(&a, &file, Bounds::default()).unwrap();
        import(&file, &target, Bounds::default()).unwrap();
        let b = Admin::open(target, Limits::default()).unwrap();
        assert_eq!(capture(&b, Bounds::default()).unwrap(), expected);
        a = b;
    }
}
#[test]
fn capture_file_bounds_and_result_caps_reject_without_output() {
    let (p, a) = populated("capture-bounds");
    assert!(
        export(
            &a,
            p.join("small"),
            Bounds {
                file_bytes: 100,
                ..Bounds::default()
            }
        )
        .is_err()
    );
    assert!(!p.join("small").exists());
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE large (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    for n in 0..18 {
        s.execute(
            "INSERT INTO large VALUES ($1,$2)",
            &[Value::Int(n), Value::Text("x".repeat(60000))],
            None,
        )
        .unwrap();
    }
    s.execute("COMMIT", &[], None).unwrap();
    assert!(export(&a, p.join("large-copy"), Bounds::default()).is_err());
    assert!(!p.join("large-copy").exists());
}
