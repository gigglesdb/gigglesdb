//! Owned OVERLAY mutation and SQL-context checks without filesystem fixtures.
use super::*;
fn state() -> Relational {
    let mut s = Relational::default();
    let p = Prepared::parse(
        "CREATE TABLE overlay_input(id BIGINT PRIMARY KEY,txt TEXT,b BYTEA)",
        1,
        s.epoch,
    )
    .unwrap();
    s.execute_with_cancel(&p, &[], None, || false).unwrap();
    let t = s.tables["overlay_input"].clone();
    s.create_secondary_index_engine(&t, s.epoch, "overlay_txt", &[t.columns[1].id], || false)
        .unwrap();
    s
}
fn run(s: &mut Relational, sql: &str) -> R<SqlResult> {
    let p = Prepared::parse(sql, 1, s.epoch)?;
    s.execute_with_cancel(&p, &[], None, || false)
}
#[test]
fn overlay_insert_every_cancel_boundary_and_duplicate_preserve_complete_state() {
    let base = state();
    let p=Prepared::parse("INSERT INTO overlay_input VALUES(1,overlay('abcdef' PLACING 'X' FROM 2 FOR -1),overlay('\\x610062'::bytea,'\\xff'::bytea,2))",1,base.epoch).unwrap();
    let mut success = base.clone();
    let mut calls = 0;
    assert_eq!(
        success
            .execute_with_cancel(&p, &[], None, || {
                calls += 1;
                false
            })
            .unwrap()
            .affected,
        1
    );
    assert!(calls > 0);
    for stop in 1..=calls {
        let mut candidate = base.clone();
        let mut seen = 0;
        assert_eq!(
            candidate.execute_with_cancel(&p, &[], None, || {
                seen += 1;
                seen == stop
            }),
            Err(SqlError::Aborted),
            "callback={stop}"
        );
        assert_eq!(candidate, base, "complete state callback={stop}");
    }
    let before = success.clone();
    assert_eq!(
        success.execute_with_cancel(&p, &[], None, || false),
        Err(SqlError::Duplicate)
    );
    assert_eq!(success, before);
    success.secondary_validate_engine(|| false).unwrap();
}
#[test]
fn overlay_projection_order_aggregate_and_insert_contexts_are_typed() {
    let mut s = state();
    run(
        &mut s,
        "INSERT INTO overlay_input VALUES(1,'abc','\\x610062'::bytea)",
    )
    .unwrap();
    run(
        &mut s,
        "INSERT INTO overlay_input VALUES(2,'def','\\x640065'::bytea)",
    )
    .unwrap();
    for (sql, expected) in [
        (
            "SELECT overlay(txt PLACING 'X' FROM 2) FROM overlay_input ORDER BY id",
            vec![Value::Text("aXc".into()), Value::Text("dXf".into())],
        ),
        (
            "SELECT overlay(txt,'X',2) FROM overlay_input ORDER BY overlay(txt,'X',2)",
            vec![Value::Text("aXc".into()), Value::Text("dXf".into())],
        ),
        (
            "SELECT max(overlay(txt,'X',2)) FROM overlay_input",
            vec![Value::Text("dXf".into())],
        ),
        (
            "SELECT overlay(b,'\\xff'::bytea,2) FROM overlay_input ORDER BY id",
            vec![
                Value::Bytes(vec![97, 255, 98]),
                Value::Bytes(vec![100, 255, 101]),
            ],
        ),
    ] {
        let result = run(&mut s, sql).unwrap();
        assert_eq!(
            result
                .rows
                .into_iter()
                .map(|row| row.values[0].clone())
                .collect::<Vec<_>>(),
            expected,
            "{sql}"
        );
    }
    for sql in [
        "INSERT INTO overlay_input VALUES(3,overlay('abc','X',0),NULL)",
        "INSERT INTO overlay_input VALUES(3,overlay('abc','X',2147483647,1),NULL)",
    ] {
        let before = s.clone();
        assert!(run(&mut s, sql).is_err());
        assert_eq!(s, before);
        s.secondary_validate_engine(|| false).unwrap();
    }
}
#[test]
fn overlay_insert_admission_visits_each_child_before_effects() {
    let s = state();
    let huge = format!("'{}'", "x".repeat(65_537));
    let leaf = format!("concat({})", vec!["'1'"; 256].join(","));
    let wide = format!("concat({})", vec![leaf.as_str(); 20].join(","));
    for child in [huge, wide] {
        for position in 0..4 {
            let mut args = ["'abc'".to_string(), "'X'".into(), "1".into(), "1".into()];
            args[position] = child.clone();
            for sql in [
                format!(
                    "INSERT INTO overlay_input(id,txt) VALUES(1,overlay({},{},{},{}))",
                    args[0], args[1], args[2], args[3]
                ),
                format!(
                    "INSERT INTO overlay_input(id,txt) VALUES(1,overlay({} PLACING {} FROM {} FOR {}))",
                    args[0], args[1], args[2], args[3]
                ),
            ] {
                assert!(
                    matches!(Prepared::parse(&sql, 1, s.epoch), Err(SqlError::Limit)),
                    "position={position}"
                );
            }
        }
    }
}
