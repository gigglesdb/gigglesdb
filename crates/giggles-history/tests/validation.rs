mod common;
use common::*;
use giggles_history::*;
use std::io::Cursor;

#[test]
fn schema_roundtrip_and_invalid_versions() {
    let h = generate(5, 8).unwrap();
    let mut bytes = vec![];
    write_jsonl(&h, &mut bytes).unwrap();
    assert_eq!(h, parse_jsonl(Cursor::new(&bytes)).unwrap());
    let mut bad = h.clone();
    bad.version = 2;
    assert!(validate(&bad).is_err());
    assert!(write_jsonl(&bad, vec![]).is_err());
}
#[test]
fn malformed_jsonl_corpus() {
    let mut encoded = vec![];
    write_jsonl(&history(vec![]), &mut encoded).unwrap();
    let header = String::from_utf8(encoded).unwrap();
    for s in [
        String::new(),
        "\n".into(),
        "null\n".into(),
        "[]\n".into(),
        "{}\n".into(),
        "{\n".into(),
        format!("{header}{header}"),
        header.replace("\"version\":1", "\"version\":1,\"version\":1"),
        header.replace("\"version\":1", "\"version\":1,\"extra\":1"),
        header.replace("\"version\":1", "\"version\":2"),
        format!("{header}\n"),
    ] {
        assert!(parse_jsonl(Cursor::new(s.as_bytes())).is_err(), "{s}");
    }
    assert!(parse_jsonl(Cursor::new(vec![255, 254, 253])).is_err());
}
#[test]
fn line_and_total_input_bounds_and_nesting() {
    assert!(parse_jsonl(Cursor::new(vec![b' '; 1024 * 1024 + 1])).is_err());
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    assert!(parse_jsonl(Cursor::new(deep)).is_err());
    let mut encoded = vec![];
    write_jsonl(&history(vec![]), &mut encoded).unwrap();
    let mut h = history(vec![]);
    for i in 0..129 {
        h.attempts.push(writer(&format!("a{i}"), 0, 1, 0, 7));
    }
    assert!(validate(&h).is_err());
}
#[test]
fn public_schema_rejects_invalid_attempt_contracts() {
    let base = history(vec![reader("a", 0, 1, 0, None)]);
    for mode in 0..13 {
        let mut h = base.clone();
        let a = &mut h.attempts[0];
        match mode {
            0 => a.id.clear(),
            1 => a.logical_id = "x".repeat(129),
            2 => a.id = "bad\n".into(),
            3 => a.complete = Some(101),
            4 => a.invoke = 2,
            5 => a.complete = None,
            6 => a.decision_at = Some(1),
            7 => a.durable = Some(Outcome::Indeterminate),
            8 => {
                a.durable = Some(Outcome::Committed);
                a.decision_at = Some(101);
            }
            9 => a.observations.clear(),
            10 => a.observations.push(Observation::Read { value: None }),
            11 => a.observations = vec![Observation::Scan { rows: vec![] }],
            _ => a.ops = vec![Op::Read { key: 0 }; 257],
        }
        assert!(validate(&h).is_err(), "mode {mode}");
    }
}
#[test]
fn rejects_conflicting_retry_semantic_bodies_and_receipts() {
    let a = writer("a", 0, 1, 0, 7);
    let mut b = a.clone();
    b.id = "b".into();
    let mut h = history(vec![a, b]);
    assert!(validate(&h).is_ok());
    h.attempts[1].ops[0] = Op::Write { key: 0, value: 8 };
    assert!(validate(&h).is_err());
    h.attempts[1] = h.attempts[0].clone();
    assert!(validate(&h).is_err());
    h.attempts[1].id = "b".into();
    h.attempts[1].receipt = Some(Receipt {
        id: "r".into(),
        budget: "b".into(),
        cost: 1,
    });
    assert!(validate(&h).is_err());
}
#[test]
fn rejects_bad_scans_transfers_and_invariants() {
    for rows in [vec![(1, 1), (0, 0)], vec![(0, 0), (0, 1)]] {
        assert!(validate(&history(vec![scanner("a", 0, 1, 0, 2, rows)])).is_err());
    }
    for op in [
        Op::Scan { start: 0, end: 0 },
        Op::Scan { start: 2, end: 0 },
        Op::Transfer {
            from: 0,
            to: 0,
            amount: 1,
        },
        Op::Transfer {
            from: 0,
            to: 1,
            amount: 0,
        },
        Op::Transfer {
            from: 0,
            to: 1,
            amount: -1,
        },
    ] {
        assert!(validate(&history(vec![tx("a", 0, 1, vec![op], vec![])])).is_err());
    }
    let mut h = history(vec![]);
    h.invariants.conserved_keys = vec![0, 0];
    assert!(validate(&h).is_err());
    h.invariants.conserved_keys.clear();
    h.invariants.nonnegative_keys = vec![0, 0];
    assert!(validate(&h).is_err());
}
#[test]
fn arbitrary_bounded_byte_mutations_never_panic() {
    let mut bytes = vec![];
    write_jsonl(&generate(99, 3).unwrap(), &mut bytes).unwrap();
    for i in 0..bytes.len() {
        let mut mutant = bytes.clone();
        mutant[i] = if i % 2 == 0 { 0 } else { 255 };
        let _ = parse_jsonl(Cursor::new(mutant));
    }
    for len in 0..bytes.len() {
        let _ = parse_jsonl(Cursor::new(&bytes[..len]));
    }
}

#[test]
fn rejects_duplicate_initial_rows_and_nested_unknown_fields() {
    let mut encoded = vec![];
    write_jsonl(&generate(0, 1).unwrap(), &mut encoded).unwrap();
    let text = String::from_utf8(encoded).unwrap();
    for modified in [
        text.replace(
            "\"initial\":[[0,1000],[1,1000]]",
            "\"initial\":[[0,1000],[0,1000]]",
        ),
        text.replace("\"ops\":[", "\"unexpected\":1,\"ops\":["),
        text.replace(
            "\"record\":\"attempt\",",
            "\"record\":\"attempt\",\"extra\":1,",
        ),
    ] {
        assert!(parse_jsonl(Cursor::new(modified)).is_err());
    }
}

#[test]
fn model_key_operation_observation_and_receipt_bounds() {
    let mut h = history(vec![]);
    h.initial = (0..4097).map(|k| (k, 0)).collect();
    assert!(validate(&h).is_err());
    let mut h = history(
        (0..33)
            .map(|i| {
                tx(
                    &format!("a{i}"),
                    0,
                    1,
                    vec![Op::Write { key: 0, value: 0 }; 256],
                    vec![],
                )
            })
            .collect(),
    );
    assert!(validate(&h).is_err());
    h = history(vec![scanner(
        "a",
        0,
        1,
        0,
        4097,
        (0..4097).map(|k| (k, 0)).collect(),
    )]);
    assert!(validate(&h).is_err());
    h = history(
        (0..5)
            .map(|i| {
                scanner(
                    &format!("a{i}"),
                    0,
                    1,
                    0,
                    4096,
                    (0..4096).map(|k| (k, 0)).collect(),
                )
            })
            .collect(),
    );
    assert!(validate(&h).is_err());
    h = history(vec![writer("a", 0, 1, 0, 0)]);
    h.attempts[0].receipt = Some(Receipt {
        id: "".into(),
        budget: "b".into(),
        cost: 0,
    });
    assert!(validate(&h).is_err());
    h = history(vec![]);
    h.invariants.budgets = (0..129).map(|i| (format!("b{i}"), 1)).collect();
    assert!(validate(&h).is_err());
}
