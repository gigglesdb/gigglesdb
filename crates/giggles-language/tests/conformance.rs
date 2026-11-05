use giggles_core::{Admin, CrashPoint, Limits, relational::Value};
use giggles_language::*;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-native-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let p = path();
    let a = Admin::create(&p, Limits::default()).unwrap();
    a.sql_session().execute("create table items (id bigint primary key, state text not null, note text, n integer, flag boolean)",&[],None).unwrap();
    let e = Engine::new(a.clone());
    (p, a, e)
}
fn run(e: &Engine, s: &str) -> Result<Outcome> {
    e.execute_text(s, &Bindings::new())
}
fn insert(e: &Engine, id: i64, state: &str) {
    run(
        e,
        &format!("insert items set id={id}, state=\"{state}\", n={id}"),
    )
    .unwrap();
}
fn all(e: &Engine) -> Outcome {
    run(e, "items limit 1024").unwrap()
}

#[test]
fn read_order_projection_and_complete() {
    let (_, _, e) = fixture();
    for i in [3, 1, 2] {
        insert(&e, i, "ready")
    }
    let o = run(&e, "items order by state desc limit 2 select id, state").unwrap();
    assert_eq!(o.columns, ["id", "state"]);
    assert_eq!(
        o.rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        [Value::Int(1), Value::Int(2)]
    );
    assert!(!o.complete);
    assert_eq!(o.matched, 3);
    assert_eq!(run(&e, "items limit 0").unwrap().rows.len(), 0);
    assert!(!run(&e, "items limit 0").unwrap().complete);
    assert!(all(&e).complete);
    assert!(run(&e, "items where id>9 limit 0").unwrap().complete);
}
#[test]
fn typed_binding_and_empty_validation() {
    let (_, _, e) = fixture();
    let b = BTreeMap::from([("key".into(), Value::Int(1))]);
    assert_eq!(
        e.execute_text("items where id=$key", &b).unwrap().matched,
        0
    );
    for s in [
        "items where absent=1",
        "items where id=\"a\"",
        "items where id",
        "items order by absent",
        "items select absent",
        "update many items where true set n=2147483648 at most 2",
        "update many items where true set state=null at most 2",
    ] {
        assert!(run(&e, s).is_err(), "{s}")
    }
    assert!(e.execute_text("items", &b).is_err());
    assert!(run(&e, "items where id=$missing").is_err());
    assert!(run(&e, "items limit -1").is_err());
    assert!(run(&e, "items limit 1025").is_err());
}
#[test]
fn null_truth_tables() {
    let (_, _, e) = fixture();
    insert(&e, 1, "ready");
    for (p, expected) in [
        ("null", 0),
        ("note=null", 0),
        ("note is null", 1),
        ("note is not null", 0),
        ("not null", 0),
        ("true and null", 0),
        ("false or null", 0),
        ("true or null", 1),
        ("false and null", 0),
        ("not (false and null)", 1),
        ("not (true or null)", 0),
        ("null or true", 1),
        ("null and false", 0),
    ] {
        assert_eq!(
            run(&e, &format!("items where {p}")).unwrap().matched,
            expected,
            "{p}"
        )
    }
}
#[test]
fn singular_cardinality_precedes_guard() {
    let (_, _, e) = fixture();
    insert(&e, 1, "ready");
    insert(&e, 2, "done");
    let before = all(&e);
    for verb in [
        "update items where id>0 when state=\"ready\" set state=\"new\"",
        "delete items where id>0 when state=\"ready\"",
    ] {
        assert_eq!(run(&e, verb), Err(Error::AmbiguousMatch));
        assert_eq!(all(&e), before)
    }
    assert_eq!(
        run(&e, "update items where id=9 when false set state=\"new\""),
        Err(Error::NotFound)
    );
    for g in ["false", "null", "note=\"x\""] {
        assert_eq!(
            run(&e, &format!("delete items where id=1 when {g}")),
            Err(Error::ConditionFailed)
        );
        assert_eq!(all(&e), before)
    }
    let o = run(
        &e,
        "update items where id=1 when state=\"ready\" set state=\"new\"",
    )
    .unwrap();
    assert_eq!((o.matched, o.affected, o.changed), (1, 1, 1));
}
#[test]
fn bulk_cap_plus_one_atomic() {
    let (_, _, e) = fixture();
    for i in 1..=4 {
        insert(&e, i, "ready")
    }
    let before = all(&e);
    assert_eq!(
        run(
            &e,
            "update many items where true set state=\"done\" at most 3"
        ),
        Err(Error::TooManyMatches)
    );
    assert_eq!(all(&e), before);
    assert_eq!(
        run(&e, "delete many items where true at most 3"),
        Err(Error::TooManyMatches)
    );
    assert_eq!(all(&e), before);
    assert_eq!(
        run(
            &e,
            "update many items where true set state=\"done\" at most 4"
        )
        .unwrap()
        .changed,
        4
    );
    assert_eq!(
        run(&e, "delete many items where id>9 at most 0")
            .unwrap()
            .affected,
        0
    );
    assert_eq!(
        run(&e, "delete many items where true at most 0"),
        Err(Error::TooManyMatches)
    );
    assert_eq!(
        run(&e, "delete many items where true at most 4")
            .unwrap()
            .changed,
        4
    );
    assert!(all(&e).rows.is_empty());
}
#[test]
fn claim_consumption_and_empty() {
    let (_, _, e) = fixture();
    for i in [2, 3, 1] {
        insert(&e, i, "ready")
    }
    let before = all(&e);
    assert_eq!(
        run(
            &e,
            "claim items where state=\"ready\" order by id set note=\"x\""
        ),
        Err(Error::ClaimNotConsumed)
    );
    assert_eq!(all(&e), before);
    assert_eq!(
        run(
            &e,
            "claim items where state=\"ready\" order by id desc set state=\"done\""
        )
        .unwrap()
        .changed,
        1
    );
    assert_eq!(
        run(&e, "items where state=\"done\" select id")
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(3)]
    );
    assert_eq!(
        run(&e, "claim items where id=99 order by id set state=\"done\"")
            .unwrap()
            .affected,
        0
    );
    assert!(run(&e, "claim items where true order by id set state=\"done\"").is_err());
}
#[test]
fn no_op_versions() {
    let (_, _, e) = fixture();
    insert(&e, 1, "ready");
    let before = all(&e);
    let o = run(&e, "update items where id=1 set state=\"ready\"").unwrap();
    assert_eq!((o.matched, o.affected, o.changed), (1, 1, 0));
    assert_eq!(before, all(&e));
}
#[test]
fn assignment_overflow_and_injection() {
    let (_, _, e) = fixture();
    insert(&e, 1, "ready");
    let before = all(&e);
    for s in [
        "update items where id=1 set id=2",
        "update items where id=1 set n=2147483648",
        "update items where id=1 set n=-2147483649",
        "update items where id=1 set state=null",
        "update items where id=1 set state=\"done\", state=\"x\"",
        "update items where id=1 set absent=2",
        "update items where id=1 set state=\"done\"; delete many items where true at most 10",
    ] {
        assert!(run(&e, s).is_err(), "{s}");
        assert_eq!(all(&e), before)
    }
    let b = BTreeMap::from([(
        "payload".into(),
        Value::Text("x\"; delete many items where true at most 1024 --".into()),
    )]);
    e.execute_text("update items where id=1 set note=$payload", &b)
        .unwrap();
    assert_eq!(all(&e).rows.len(), 1);
    assert_eq!(all(&e).rows[0].values[2], b["payload"]);
}
#[test]
fn sql_native_shared_catalog() {
    let (_, a, e) = fixture();
    a.sql_session()
        .execute(
            "insert into items (id,state) values ($1,$2)",
            &[Value::Int(1), Value::Text("sql".into())],
            None,
        )
        .unwrap();
    assert_eq!(run(&e, "items where state=\"sql\"").unwrap().matched, 1);
    run(&e, "update items where id=1 set state=\"native\"").unwrap();
    let o = a
        .sql_session()
        .execute("select state from items where id=1", &[], None)
        .unwrap();
    assert_eq!(o.rows[0].values, [Value::Text("native".into())]);
}
#[test]
fn abort_preserves_state_and_reopen() {
    let (p, a, e) = fixture();
    insert(&e, 1, "ready");
    let before = all(&e);
    assert_eq!(
        run(&e, "delete items where id=1 when false"),
        Err(Error::ConditionFailed)
    );
    drop(e);
    drop(a);
    let a = Admin::open(p, Limits::default()).unwrap();
    assert_eq!(all(&Engine::new(a)), before);
}
#[test]
fn direct_ast_validation() {
    let (_, _, e) = fixture();
    let bad = Statement::Read {
        table: "items;drop".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    };
    assert!(e.execute(&bad, &Bindings::new()).is_err());
    let bad = Statement::Update {
        table: "items".into(),
        filter: Expr::Value(ValueExpr::Literal(Value::Bool(true))),
        guard: Some(Expr::Value(ValueExpr::Literal(Value::Bool(true)))),
        values: vec![("state".into(), ValueExpr::Literal(Value::Text("x".into())))],
        intent: Intent::Many {
            cap: ValueExpr::Literal(Value::Int(1)),
        },
    };
    assert_eq!(e.execute(&bad, &Bindings::new()), Err(Error::Unsupported));
    let mut x = Expr::Value(ValueExpr::Literal(Value::Bool(true)));
    for _ in 0..40 {
        x = Expr::Not(Box::new(x))
    }
    let bad = Statement::Read {
        table: "items".into(),
        filter: Some(x),
        order: vec![],
        limit: None,
        projection: vec![],
    };
    assert_eq!(e.execute(&bad, &Bindings::new()), Err(Error::Limit));
}
#[test]
fn nullable_and_bytes_bindings() {
    let (_, a, e) = fixture();
    a.sql_session()
        .execute(
            "create table blobs (id bigint primary key, payload bytea not null, enabled boolean)",
            &[],
            None,
        )
        .unwrap();
    let b = BTreeMap::from([("bytes".into(), Value::Bytes(vec![0, 255, 1]))]);
    e.execute_text("insert blobs set id=1,payload=$bytes", &b)
        .unwrap();
    assert_eq!(
        e.execute_text("blobs where payload=$bytes", &b)
            .unwrap()
            .matched,
        1
    );
    assert_eq!(run(&e, "blobs where enabled is null").unwrap().matched, 1);
}
#[test]
fn composite_primary_key_point_mutations() {
    let (_, a, e) = fixture();
    a.sql_session()
        .execute(
            "create table pairs (tenant bigint, id bigint, state text, primary key (tenant,id))",
            &[],
            None,
        )
        .unwrap();
    for tenant in 1..=2 {
        run(
            &e,
            &format!("insert pairs set tenant={tenant},id=1,state=\"ready\""),
        )
        .unwrap();
    }
    assert_eq!(
        run(&e, "update pairs where id=1 set state=\"x\""),
        Err(Error::AmbiguousMatch)
    );
    run(&e, "update pairs where tenant=1 and id=1 set state=\"x\"").unwrap();
    assert_eq!(run(&e, "pairs where state=\"ready\"").unwrap().matched, 1);
}
#[test]
fn null_sort_and_pk_ties() {
    let (_, _, e) = fixture();
    for i in 1..=3 {
        insert(&e, i, "ready")
    }
    run(&e, "update items where id=2 set note=\"x\"").unwrap();
    let asc = run(&e, "items order by note select id").unwrap();
    assert_eq!(
        asc.rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        [Value::Int(2), Value::Int(1), Value::Int(3)]
    );
    let desc = run(&e, "items order by note desc select id").unwrap();
    assert_eq!(desc.rows[0].values, [Value::Int(1)]);
}
#[test]
fn duplicate_insert_atomic() {
    let (_, _, e) = fixture();
    insert(&e, 1, "ready");
    let before = all(&e);
    assert!(run(&e, "insert items set id=1,state=\"bad\"").is_err());
    assert_eq!(all(&e), before);
    assert!(run(&e, "insert items set id=2").is_err());
    assert_eq!(all(&e), before);
}
#[test]
fn cardinality_beyond_default_page() {
    let (_, a, e) = fixture();
    let mut s = a.sql_session();
    s.execute("begin", &[], None).unwrap();
    for i in 0..101 {
        s.execute(
            "insert into items (id,state) values ($1,$2)",
            &[Value::Int(i), Value::Text("ready".into())],
            None,
        )
        .unwrap();
    }
    s.execute("commit", &[], None).unwrap();
    let before = all(&e);
    assert_eq!(run(&e, "items").unwrap().rows.len(), 100);
    assert!(!run(&e, "items").unwrap().complete);
    assert_eq!(
        run(
            &e,
            "update many items where true set state=\"done\" at most 100"
        ),
        Err(Error::TooManyMatches)
    );
    assert_eq!(
        run(&e, "update items where true when id=0 set state=\"done\""),
        Err(Error::AmbiguousMatch)
    );
    assert_eq!(all(&e), before);
    assert_eq!(
        run(
            &e,
            "update many items where true set state=\"done\" at most 101"
        )
        .unwrap()
        .changed,
        101
    );
}
#[test]
fn error_after_staged_row_rolls_back_everything() {
    let (_, _, e) = fixture();
    insert(&e, 1, "a");
    insert(&e, 2, "a");
    let b = BTreeMap::from([("large".into(), Value::Text("n".repeat(40_000)))]);
    e.execute_text("update items where id=2 set note=$large", &b)
        .unwrap();
    let before = all(&e);
    let failed = e.execute_text(
        "update many items where true set state=$large at most 2",
        &b,
    );
    assert!(failed.is_err());
    assert_eq!(all(&e), before);
}
#[test]
fn result_materialization_limit_fails_before_mutation() {
    let (_, a, e) = fixture();
    let mut s = a.sql_session();
    s.execute("begin", &[], None).unwrap();
    for i in 0..18 {
        s.execute(
            "insert into items (id,state,note) values ($1,$2,$3)",
            &[
                Value::Int(i),
                Value::Text("ready".into()),
                Value::Text("n".repeat(60_000)),
            ],
            None,
        )
        .unwrap();
    }
    s.execute("commit", &[], None).unwrap();
    assert!(run(&e, "items limit 1 select id").is_err());
    assert!(run(&e, "delete many items where true at most 18").is_err());
    let result = a
        .sql_session()
        .execute("select id from items limit 1024", &[], None)
        .unwrap();
    assert_eq!(result.rows.len(), 18);
}
#[test]
fn full_table_capacity_exact_cap() {
    let (_, a, e) = fixture();
    let mut s = a.sql_session();
    s.execute("begin", &[], None).unwrap();
    for i in 0..1024 {
        s.execute(
            "insert into items (id,state) values ($1,$2)",
            &[Value::Int(i), Value::Text("ready".into())],
            None,
        )
        .unwrap();
    }
    s.execute("commit", &[], None).unwrap();
    assert_eq!(all(&e).rows.len(), 1024);
    assert!(all(&e).complete);
    assert!(run(&e, "insert items set id=1024,state=\"x\"").is_err());
    assert_eq!(
        run(&e, "delete many items where true at most 1023"),
        Err(Error::TooManyMatches)
    );
    assert_eq!(
        run(&e, "delete many items where true at most 1024")
            .unwrap()
            .changed,
        1024
    );
}

#[test]
fn oversized_core_table_is_never_silently_truncated() {
    let (_, a, e) = fixture();
    let mut s = a.sql_session();
    s.execute("begin", &[], None).unwrap();
    for i in 0..1025 {
        s.execute(
            "insert into items (id,state) values ($1,$2)",
            &[Value::Int(i), Value::Text("ready".into())],
            None,
        )
        .unwrap();
    }
    s.execute("commit", &[], None).unwrap();
    for text in [
        "items limit 1",
        "update many items where true set state=\"done\" at most 1024",
        "delete many items where true at most 1024",
        "update items where id=1024 set state=\"done\"",
        "claim items where true order by id set state=\"done\"",
    ] {
        assert_eq!(run(&e, text), Err(Error::Limit), "{text}");
    }
    let rows = a
        .sql_session()
        .execute("select state from items where id=1024", &[], None)
        .unwrap()
        .rows;
    assert_eq!(rows[0].values, [Value::Text("ready".into())]);
}
#[test]
fn claim_nullable_postimage_consumes() {
    let (_, _, e) = fixture();
    insert(&e, 1, "ready");
    run(&e, "update items where id=1 set flag=true").unwrap();
    assert_eq!(
        run(&e, "claim items where flag=true order by id set flag=null")
            .unwrap()
            .changed,
        1
    );
    assert_eq!(
        run(&e, "claim items where flag=true order by id set flag=null")
            .unwrap()
            .affected,
        0
    );
}
#[test]
fn smallint_range_and_required_fields() {
    let (_, a, e) = fixture();
    a.sql_session()
        .execute(
            "create table tiny (id bigint primary key, n smallint not null)",
            &[],
            None,
        )
        .unwrap();
    for (id, n) in [(1, -32768), (2, 32767)] {
        run(&e, &format!("insert tiny set id={id},n={n}")).unwrap();
    }
    for s in [
        "insert tiny set id=3,n=32768",
        "insert tiny set id=3,n=-32769",
        "insert tiny set id=3,n=null",
        "insert tiny set id=3,n=true",
    ] {
        assert!(run(&e, s).is_err(), "{s}")
    }
    assert_eq!(run(&e, "tiny").unwrap().matched, 2);
}
#[test]
fn unknown_commit_reopen_both_boundaries() {
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (p, a, e) = fixture();
        insert(&e, 1, "ready");
        a.set_crash_point(point).unwrap();
        assert_eq!(
            run(&e, "update items where id=1 set state=\"done\""),
            Err(Error::Unknown)
        );
        drop(e);
        drop(a);
        let a = Admin::open(&p, Limits::default()).unwrap();
        let e = Engine::new(a);
        let want = if persisted { "done" } else { "ready" };
        assert_eq!(all(&e).rows[0].values[1], Value::Text(want.into()));
    }
}
#[test]
fn concurrent_claims_unique() {
    let (_, a, e) = fixture();
    for i in 0..16 {
        insert(&e, i, "ready")
    }
    let barrier = Arc::new(Barrier::new(4));
    let mut workers = vec![];
    for worker in 0..4 {
        let e = Engine::new(a.clone());
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            let mut success = Vec::new();
            for _ in 0..128 {
                match run(
                    &e,
                    &format!(
                        "claim items where state=\"ready\" order by id set state=\"worker{worker}\""
                    ),
                ) {
                    Ok(o) if o.affected == 0 => return success,
                    Ok(o) => {
                        assert_eq!(o.changed, 1);
                        let Value::Int(id) = o.rows[0].values[0] else {
                            panic!("claim ID")
                        };
                        assert_eq!(o.rows[0].values[1], Value::Text(format!("worker{worker}")));
                        success.push(id);
                    }
                    Err(Error::Conflict) => (),
                    Err(e) => panic!("{e:?}"),
                }
            }
            panic!("bounded caller retry exhausted")
        }))
    }
    let claimed: Vec<_> = workers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();
    assert_eq!(claimed.len(), 16);
    assert_eq!(
        claimed
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        16
    );
    assert_eq!(run(&e, "items where state=\"ready\"").unwrap().matched, 0);
}
#[test]
fn core_generation_conflict_negative_control() {
    let (_, a, e) = fixture();
    insert(&e, 1, "ready");
    let mut stale = a.sql_session();
    stale.execute("begin", &[], None).unwrap();
    stale
        .execute(
            "update items set state=$1 where id=$2",
            &[Value::Text("stale".into()), Value::Int(1)],
            None,
        )
        .unwrap();
    run(&e, "update items where id=1 set state=\"native\"").unwrap();
    assert_eq!(
        stale.execute("commit", &[], None).unwrap_err(),
        giggles_core::CoreError::Sql(giggles_core::relational::SqlError::Conflict)
    );
    stale.rollback();
    assert_eq!(all(&e).rows[0].values[1], Value::Text("native".into()));
}
#[test]
fn independent_model_generated_histories() {
    let (_, _, e) = fixture();
    let mut model = BTreeMap::<i64, String>::new();
    let mut rng = 0x49u64;
    for step in 0..160 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = ((rng >> 32) % 12) as i64;
        match step % 5 {
            0 => {
                let result = run(&e, &format!("insert items set id={key},state=\"ready\""));
                if let std::collections::btree_map::Entry::Vacant(entry) = model.entry(key) {
                    assert!(result.is_ok());
                    entry.insert("ready".into());
                } else {
                    assert!(result.is_err())
                }
            }
            1 => {
                let before = all(&e);
                let eligible: Vec<_> = model
                    .iter()
                    .filter(|(_, s)| *s == "ready")
                    .map(|(k, _)| *k)
                    .collect();
                let cap = (rng % 5) as usize;
                let result = run(
                    &e,
                    &format!(
                        "update many items where state=\"ready\" set state=\"done\" at most {cap}"
                    ),
                );
                if eligible.len() > cap {
                    assert_eq!(result, Err(Error::TooManyMatches));
                    assert_eq!(all(&e), before)
                } else {
                    assert_eq!(result.unwrap().changed, eligible.len());
                    for k in eligible {
                        model.insert(k, "done".into());
                    }
                }
            }
            2 => {
                let result = run(&e, &format!("delete items where id={key}"));
                if model.remove(&key).is_some() {
                    assert_eq!(result.unwrap().changed, 1)
                } else {
                    assert_eq!(result, Err(Error::NotFound))
                }
            }
            3 => {
                let eligible = model.iter().find(|(_, s)| *s == "ready").map(|(k, _)| *k);
                let result = run(
                    &e,
                    "claim items where state=\"ready\" order by id set state=\"claimed\"",
                )
                .unwrap();
                if let Some(k) = eligible {
                    assert_eq!(result.changed, 1);
                    model.insert(k, "claimed".into());
                } else {
                    assert_eq!(result.affected, 0)
                }
            }
            _ => {
                let result = run(
                    &e,
                    &format!(
                        "update items where id={key} when state=\"ready\" set state=\"ready\""
                    ),
                );
                match model.get(&key) {
                    None => assert_eq!(result, Err(Error::NotFound)),
                    Some(v) if v != "ready" => assert_eq!(result, Err(Error::ConditionFailed)),
                    Some(_) => assert_eq!(result.unwrap().changed, 0),
                }
            }
        }
        let actual: BTreeMap<_, _> = all(&e)
            .rows
            .into_iter()
            .map(|r| {
                let Value::Int(id) = r.values[0] else {
                    panic!()
                };
                let Value::Text(s) = &r.values[1] else {
                    panic!()
                };
                (id, s.clone())
            })
            .collect();
        assert_eq!(actual, model, "step {step}");
    }
}
