use super::*;
use relational::{Relational, SqlResult, SqlRow, Value};
use std::cell::Cell;
use std::path::PathBuf;

fn fixture() -> (Admin, SqlSession, PathBuf) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("gd42-batch-{:032x}", u128::from_be_bytes(nonce)));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE items (tenant BIGINT, id BIGINT, email TEXT, n INTEGER, PRIMARY KEY (tenant,id))", &[], None).unwrap();
    crate::operations::Engine::new(admin.clone())
        .execute_unique_index(&crate::operations::UniqueIndexRequest {
            table: "items".into(),
            name: "email_unique".into(),
            columns: vec!["email".into()],
        })
        .unwrap();
    session
        .execute("INSERT INTO items VALUES (1,1,'a',10)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO items VALUES (1,2,'b',20)", &[], None)
        .unwrap();
    session.execute("BEGIN", &[], None).unwrap();
    (admin, session, path)
}
fn bytes(s: &SqlSession) -> BTreeMap<Vec<u8>, Vec<u8>> {
    records(s.staged.as_ref().unwrap()).unwrap()
}
fn source(s: &SqlSession) -> &Relational {
    &s.staged.as_ref().unwrap().sql
}
fn rows(s: &Relational) -> &BTreeMap<Vec<u8>, SqlRow> {
    s.secondary_access_engine(&s.tables["items"], s.epoch, || false)
        .unwrap()
        .1
}
fn next(s: &Relational) -> u64 {
    let base = s.secondary_persistence_engine(|| false).unwrap().0;
    u64::from_le_bytes(base[28..36].try_into().unwrap())
}
fn row(id: i64, email: &str, n: i64, version: u64) -> SqlRow {
    SqlRow {
        values: vec![
            Value::Int(1),
            Value::Int(id),
            Value::Text(email.into()),
            Value::Int(n),
        ],
        version,
    }
}
fn key(id: i64) -> Vec<u8> {
    relational::encode_key(&[Value::Int(1), Value::Int(id)]).unwrap()
}
fn noop(s: &mut SqlSession) -> Result<((), SqlResult), CoreError> {
    let fence = s.batch_fence_engine().unwrap();
    s.apply_batch_engine(fence, &mut || false, |src, apply| {
        apply.apply(&[], next(src))
    })
}
fn refused(
    s: &SqlSession,
    before: &BTreeMap<Vec<u8>, Vec<u8>>,
    result: Result<((), SqlResult), CoreError>,
    error: SqlError,
) {
    assert_eq!(result, Err(CoreError::Sql(error)));
    assert!(s.aborted);
    assert_eq!(&bytes(s), before);
}
#[test]
fn fence_requires_active_transaction() {
    let (admin, mut s, _) = fixture();
    s.rollback();
    assert_eq!(
        s.batch_fence_engine(),
        Err(CoreError::Sql(SqlError::Transaction))
    );
    assert!(!s.aborted);
    let mut s = admin.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    let before = bytes(&s);
    s.abort();
    assert_eq!(
        s.batch_fence_engine(),
        Err(CoreError::Sql(SqlError::Aborted))
    );
    assert!(s.aborted);
    assert_eq!(bytes(&s), before);
}
#[test]
fn each_stale_fence_field_refuses() {
    for field in 0..6 {
        let (_, mut s, _) = fixture();
        let mut fence = s.batch_fence_engine().unwrap();
        match field {
            0 => fence.transaction_id ^= 1,
            1 => fence.staged_revision += 1,
            2 => fence.domain ^= 1,
            3 => fence.epoch += 1,
            4 => fence.generation += 1,
            _ => {
                s.execute("INSERT INTO items VALUES (1,3,'c',30)", &[], None)
                    .unwrap();
            }
        }
        let before = bytes(&s);
        let called = Cell::new(false);
        let result = s.apply_batch_engine(fence, &mut || false, |_, _| {
            called.set(true);
            Ok(())
        });
        refused(&s, &before, result, SqlError::StaleSchema);
        assert!(!called.get());
    }
}
#[test]
fn earlier_dml_visible_to_plan() {
    let (_, mut s, _) = fixture();
    s.execute("INSERT INTO items VALUES (1,3,'c',30)", &[], None)
        .unwrap();
    let fence = s.batch_fence_engine().unwrap();
    s.apply_batch_engine(fence, &mut || false, |src, apply| {
        assert_eq!(rows(src)[&key(3)].values[2], Value::Text("c".into()));
        apply.apply(&[], next(src))
    })
    .unwrap();
    assert!(!s.aborted);
}
#[test]
fn insert_update_batch_commits() {
    let (admin, mut s, path) = fixture();
    let revision = s.staged_revision;
    let generation = source(&s).generation;
    let fence = s.batch_fence_engine().unwrap();
    let (_, result) = s
        .apply_batch_engine(fence, &mut || false, |src, apply| {
            let old = &rows(src)[&key(1)];
            let mut updated = old.clone();
            updated.values[3] = Value::Int(11);
            updated.version = next(src);
            let inserted = row(3, "c", 30, next(src) + 1);
            let k1 = key(1);
            let k3 = key(3);
            apply.apply(
                &[
                    BatchRowChange {
                        table: src.tables["items"].id,
                        old: Some((&k1, old)),
                        new: Some((&k1, &updated)),
                    },
                    BatchRowChange {
                        table: src.tables["items"].id,
                        old: None,
                        new: Some((&k3, &inserted)),
                    },
                ],
                next(src) + 2,
            )
        })
        .unwrap();
    assert_eq!(
        result,
        SqlResult {
            affected: 2,
            ..Default::default()
        }
    );
    assert_eq!(s.staged_revision, revision + 1);
    assert_eq!(source(&s).generation, generation);
    assert!(!s.aborted);
    s.execute("COMMIT", &[], None).unwrap();
    let check = |admin: &Admin| {
        let mut fresh = admin.sql_session();
        fresh.execute("BEGIN", &[], None).unwrap();
        let actual = rows(source(&fresh));
        assert_eq!(actual.len(), 3);
        for (id, n) in [(1, 11), (2, 20), (3, 30)] {
            assert_eq!(actual[&key(id)].values[3], Value::Int(n));
        }
    };
    check(&admin);
    drop(s);
    drop(admin);
    let reopened = Admin::open(path, Limits::default()).unwrap();
    check(&reopened);
}
#[test]
fn plan_uses_exact_unique_lookup() {
    let (_, mut s, _) = fixture();
    let fence = s.batch_fence_engine().unwrap();
    s.apply_batch_engine(fence, &mut || false, |src, apply| {
        let table = &src.tables["items"];
        let sidecar = src.secondary_state_engine(|| false)?;
        let descriptor = sidecar
            .descriptors()
            .iter()
            .find(|d| d.name == "email_unique")
            .unwrap();
        let limits = relational::secondary_unique_lookup::Limits {
            bytes: 64 * 1024 * 1024,
            work: relational::secondary_index::MAX_WORK * 3,
            candidates: relational::secondary_index::MAX_CANDIDATES,
        };
        let (pk, old) = src
            .secondary_unique_lookup_engine(
                table,
                src.epoch,
                descriptor,
                &[Value::Text("b".into())],
                &limits,
                || false,
            )?
            .unwrap();
        assert_eq!(pk, key(2));
        let mut updated = old.clone();
        updated.values[3] = Value::Int(99);
        updated.version = next(src);
        apply.apply(
            &[BatchRowChange {
                table: table.id,
                old: Some((pk, old)),
                new: Some((pk, &updated)),
            }],
            next(src) + 1,
        )
    })
    .unwrap();
    assert_eq!(rows(source(&s))[&key(2)].values[3], Value::Int(99));
    assert!(!s.aborted);
}
fn swap(s: &mut SqlSession, duplicate: bool) -> Result<((), SqlResult), CoreError> {
    let fence = s.batch_fence_engine().unwrap();
    s.apply_batch_engine(fence, &mut || false, |src, apply| {
        let k1 = key(1);
        let k2 = key(2);
        let a = &rows(src)[&k1];
        let b = &rows(src)[&k2];
        let mut x = a.clone();
        let mut y = b.clone();
        x.values[2] = Value::Text("b".into());
        y.values[2] = Value::Text(if duplicate { "b" } else { "a" }.into());
        x.version = next(src);
        y.version = next(src) + 1;
        apply.apply(
            &[
                BatchRowChange {
                    table: src.tables["items"].id,
                    old: Some((&k1, a)),
                    new: Some((&k1, &x)),
                },
                BatchRowChange {
                    table: src.tables["items"].id,
                    old: Some((&k2, b)),
                    new: Some((&k2, &y)),
                },
            ],
            next(src) + 2,
        )
    })
}
#[test]
fn unique_swap_in_one_batch_succeeds() {
    let (_, mut s, _) = fixture();
    swap(&mut s, false).unwrap();
    assert!(!s.aborted);
    assert_eq!(rows(source(&s))[&key(1)].values[2], Value::Text("b".into()));
    s.rollback();
    s.execute("BEGIN", &[], None).unwrap();
    let before = bytes(&s);
    let result = swap(&mut s, true);
    refused(&s, &before, result, SqlError::Duplicate);
    s.execute("ROLLBACK", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    swap(&mut s, false).unwrap();
    assert!(!s.aborted);
}
#[test]
fn stale_old_witness_conflicts() {
    let (_, mut s, _) = fixture();
    let before = bytes(&s);
    let fence = s.batch_fence_engine().unwrap();
    let result = s.apply_batch_engine(fence, &mut || false, |src, apply| {
        let k = key(1);
        let mut old = rows(src)[&k].clone();
        old.values[3] = Value::Int(999);
        apply.apply(
            &[BatchRowChange {
                table: src.tables["items"].id,
                old: Some((&k, &old)),
                new: None,
            }],
            next(src),
        )
    });
    refused(&s, &before, result, SqlError::Conflict);
}
#[test]
fn empty_batch_is_one_bounded_noop() {
    let (_, mut s, _) = fixture();
    let before = bytes(&s);
    let revision = s.staged_revision;
    let version = next(source(&s));
    let (_, result) = noop(&mut s).unwrap();
    assert_eq!(result, SqlResult::default());
    assert_eq!(s.staged_revision, revision + 1);
    assert_eq!(next(source(&s)), version);
    assert_eq!(bytes(&s), before);
    assert!(!s.aborted);
}
#[test]
fn apply_misuse() {
    for twice in [false, true] {
        let (_, mut s, _) = fixture();
        let before = bytes(&s);
        let fence = s.batch_fence_engine().unwrap();
        let result = s.apply_batch_engine(fence, &mut || false, |src, apply| {
            if twice {
                apply.apply(&[], next(src))?;
                assert_eq!(apply.apply(&[], next(src)), Err(SqlError::Transaction));
                return Err(SqlError::Transaction);
            }
            Ok(())
        });
        refused(&s, &before, result, SqlError::Transaction);
    }
}
#[test]
fn failed_apply_still_consumes_call() {
    let (_, mut s, _) = fixture();
    let before = bytes(&s);
    let fence = s.batch_fence_engine().unwrap();
    let result = s.apply_batch_engine(fence, &mut || false, |src, apply| {
        assert_eq!(apply.apply(&[], next(src) - 1), Err(SqlError::Version));
        apply.apply(&[], next(src))
    });
    refused(&s, &before, result, SqlError::Transaction);
}
#[test]
fn plan_error_propagates() {
    let (_, mut s, _) = fixture();
    let before = bytes(&s);
    let fence = s.batch_fence_engine().unwrap();
    let result = s.apply_batch_engine(fence, &mut || false, |_, _| Err(SqlError::Conflict));
    refused(&s, &before, result, SqlError::Conflict);
}
#[test]
fn cancellation_fences() {
    for stage in 0..3 {
        let (_, mut s, _) = fixture();
        let before = bytes(&s);
        let fence = s.batch_fence_engine().unwrap();
        let planned = Cell::new(false);
        let applied = Cell::new(false);
        let calls = Cell::new(0);
        let result = s.apply_batch_engine(
            fence,
            &mut || {
                calls.set(calls.get() + 1);
                stage == 0 || (stage == 1 && applied.get()) || (stage == 2 && calls.get() == 2)
            },
            |src, apply| {
                planned.set(true);
                apply.apply(&[], next(src))?;
                applied.set(true);
                Ok(())
            },
        );
        refused(&s, &before, result, SqlError::Aborted);
        assert_eq!(planned.get(), stage != 0);
        assert_eq!(applied.get(), stage == 1);
    }
}
#[test]
fn revision_overflow_before_plan() {
    let (_, mut s, _) = fixture();
    s.staged_revision = u64::MAX;
    let fence = s.batch_fence_engine().unwrap();
    let before = bytes(&s);
    let called = Cell::new(false);
    let result = s.apply_batch_engine(fence, &mut || false, |_, _| {
        called.set(true);
        Ok(())
    });
    refused(&s, &before, result, SqlError::Overflow);
    assert!(!called.get());
}
#[test]
fn panic_in_plan_poisons_and_preserves() {
    let (_, mut s, _) = fixture();
    let before = bytes(&s);
    let fence = s.batch_fence_engine().unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| {
        s.apply_batch_engine::<()>(fence, &mut || false, |src, apply| {
            apply.apply(&[], next(src))?;
            panic!("plan panic");
        })
    }));
    assert!(result.is_err());
    assert!(s.aborted);
    assert_eq!(bytes(&s), before);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(!s.aborted);
    assert!(s.staged.is_none());
}
#[test]
fn wrapper_epoch_and_cap() {
    let (_, s, _) = fixture();
    let mut sql = source(&s).clone();
    let before = sql.clone();
    assert_eq!(
        sql.secondary_apply_batch_engine(sql.epoch + 1, &[], next(&sql), || false),
        Err(SqlError::StaleSchema)
    );
    assert_eq!(sql, before);
    let changes: Vec<_> = (0..=relational::MAX_ROWS)
        .map(|_| BatchRowChange {
            table: 0,
            old: None,
            new: None,
        })
        .collect();
    assert_eq!(
        sql.secondary_apply_batch_engine(sql.epoch, &changes, next(&sql), || false),
        Err(SqlError::Limit)
    );
    assert_eq!(sql, before);
    assert!(!s.aborted);
}
