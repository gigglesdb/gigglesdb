use super::*;
use crate::{
    buffer::format::SegmentRecord,
    event::{Event, EventKind, OpaqueValue, Position, Row, SourceIdentity, TxnId},
    mapping::{EncodedBytes, PgType},
    pg::pgoutput::Message,
};

fn name(n: &str) -> QualifiedName {
    QualifiedName {
        schema: "pg_catalog".into(),
        name: n.into(),
    }
}
fn table(directions: &[Direction]) -> TableKey {
    TableKey {
        table: "public.rows".into(),
        relation: QualifiedName {
            schema: "public".into(),
            name: "rows".into(),
        },
        columns: directions
            .iter()
            .enumerate()
            .map(|(i, d)| KeyColumn {
                name: format!("k{i}"),
                pg_type: PgType::Int4,
                sql_type: name("int4"),
                collation: None,
                direction: *d,
            })
            .collect(),
    }
}
fn key(i: i32) -> Key {
    vec![Value::Int4(i)]
}
fn row(i: i32) -> Row {
    [(
        "k0".into(),
        OpaqueValue {
            type_id: "int4".into(),
            bytes: Some(Value::Int4(i).canonical_bytes().unwrap()),
        },
    )]
    .into()
}
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "test".into(),
        stream: "slot".into(),
    }
}
fn event(kind: EventKind, old: i32, new: Option<i32>) -> Event {
    Event {
        source: source(),
        txn: TxnId(vec![1]),
        ordinal_in_txn: 0,
        position: Position::new("pg", 20u64.to_be_bytes()),
        schema_version: 1,
        kind,
        table: Some("public.rows".into()),
        key: row(old),
        before: None,
        after: new.map(row),
        unchanged_cols: BTreeSet::new(),
    }
}
fn marker(window: &mut Window, id: &ChunkId, wm: WatermarkKind, lsn: u64) -> Result<()> {
    let bytes = Watermark {
        wm,
        chunk: id.clone(),
        attempt: 0,
    }
    .encode()?;
    window.watermark(
        &Message::LogicalMessage {
            transactional: true,
            lsn,
            prefix: MESSAGE_PREFIX,
            content: &bytes,
        },
        lsn,
    )
}
fn finish(window: Window) -> Vec<i32> {
    let record = window.finish(source(), "pg", 1).unwrap();
    let bytes = record.canonical_bytes().unwrap();
    assert_eq!(SegmentRecord::from_canonical_bytes(&bytes).unwrap(), record);
    let SegmentRecord::ChunkRows(txn) = record else {
        panic!("not a chunk")
    };
    assert_eq!(txn.commit_position.bytes(), &30u64.to_be_bytes());
    txn.events
        .iter()
        .map(|e| {
            assert_eq!(e.position, txn.commit_position);
            let Value::Int4(i) =
                Value::from_canonical_bytes(e.key["k0"].bytes.as_ref().unwrap()).unwrap()
            else {
                panic!()
            };
            i
        })
        .collect()
}

#[test]
fn exclusion_insert_update_delete_and_changes_across_both_bounds() {
    let t = table(&[Direction::Asc]);
    let c = plan_chunks(&t, 10, &[key(10), key(20)]).unwrap()[1].clone();
    let id = c.id.clone();
    let mut w = Window::new(t, c, (10..20).map(row).collect(), 0).unwrap();
    w.event(&event(EventKind::Delete, 18, None)).unwrap(); // before low
    marker(&mut w, &id, WatermarkKind::Low, 10).unwrap();
    for e in [
        event(EventKind::Insert, 10, Some(10)),
        event(EventKind::Update, 11, Some(11)),
        event(EventKind::Delete, 12, None),
        event(EventKind::Update, 9, Some(13)),
        event(EventKind::Update, 14, Some(20)),
        event(EventKind::Update, 20, Some(15)),
        event(EventKind::Delete, 20, None),
        event(EventKind::Insert, 9, Some(9)),
    ] {
        w.event(&e).unwrap();
    }
    marker(&mut w, &id, WatermarkKind::High, 30).unwrap();
    w.event(&event(EventKind::Delete, 19, None)).unwrap(); // after high
    assert_eq!(finish(w), vec![16, 17, 18, 19]);
}

#[test]
fn high_boundary_belongs_only_to_next_chunk() {
    let t = table(&[Direction::Asc]);
    let plan = plan_chunks(&t, 1, &[key(10), key(20)]).unwrap();
    for (index, rows, expected) in [(1, vec![10, 19], vec![10, 19]), (2, vec![20, 21], vec![21])] {
        let c = plan[index].clone();
        let id = c.id.clone();
        let mut w = Window::new(t.clone(), c, rows.into_iter().map(row).collect(), 0).unwrap();
        marker(&mut w, &id, WatermarkKind::Low, 10).unwrap();
        w.event(&event(EventKind::Delete, 20, None)).unwrap();
        marker(&mut w, &id, WatermarkKind::High, 30).unwrap();
        assert_eq!(finish(w), expected);
    }
}

#[test]
fn empty_chunk_truncate_and_other_table() {
    let t = table(&[Direction::Asc]);
    let c = plan_chunks(&t, 2, &[]).unwrap().remove(0);
    let id = c.id.clone();
    let mut w = Window::new(t, c, vec![row(1)], 0).unwrap();
    marker(&mut w, &id, WatermarkKind::Low, 10).unwrap();
    let mut e = event(EventKind::Delete, 1, None);
    e.table = Some("other".into());
    w.event(&e).unwrap();
    e.kind = EventKind::Truncate {
        relations: vec!["other".into(), id.table.clone()],
        cascade: false,
        restart_identity: false,
    };
    w.event(&e).unwrap();
    marker(&mut w, &id, WatermarkKind::High, 30).unwrap();
    assert!(finish(w).is_empty());
}

#[test]
fn malformed_event_poison_prevents_partial_output() {
    let t = table(&[Direction::Asc]);
    let c = plan_chunks(&t, 2, &[]).unwrap().remove(0);
    let id = c.id.clone();
    let mut w = Window::new(t, c, vec![row(1)], 0).unwrap();
    marker(&mut w, &id, WatermarkKind::Low, 10).unwrap();
    assert!(w.event(&event(EventKind::Update, 1, None)).is_err());
    assert!(marker(&mut w, &id, WatermarkKind::High, 30).is_err());
    assert!(w.finish(source(), "pg", 1).is_err());
}

#[test]
fn watermark_codec_rejects_foreign_malformed_and_nontransactional() {
    let id = plan_chunks(&table(&[Direction::Asc]), 1, &[]).unwrap()[0]
        .id
        .clone();
    for kind in [WatermarkKind::Low, WatermarkKind::High] {
        let wm = Watermark {
            wm: kind,
            chunk: id.clone(),
            attempt: 0,
        };
        let bytes = wm.encode().unwrap();
        assert_eq!(
            Watermark::parse(&Message::LogicalMessage {
                transactional: true,
                lsn: 10,
                prefix: MESSAGE_PREFIX,
                content: &bytes
            })
            .unwrap(),
            wm
        );
        assert!(
            Watermark::parse(&Message::LogicalMessage {
                transactional: false,
                lsn: 10,
                prefix: MESSAGE_PREFIX,
                content: &bytes
            })
            .is_err()
        );
        assert!(
            Watermark::parse(&Message::LogicalMessage {
                transactional: true,
                lsn: 10,
                prefix: "foreign",
                content: &bytes
            })
            .is_err()
        );
        assert!(
            Watermark::parse(&Message::LogicalMessage {
                transactional: true,
                lsn: 10,
                prefix: crate::pg::ddl::MESSAGE_PREFIX,
                content: &bytes,
            })
            .is_err()
        );
    }
    for bad in [
        b"{}".as_slice(),
        b"{",
        b"{\"wm\":\"other\",\"chunk\":{}}",
        b"{\"wm\":\"low\",\"wm\":\"high\",\"chunk\":{}}",
    ] {
        assert!(
            Watermark::parse(&Message::LogicalMessage {
                transactional: true,
                lsn: 10,
                prefix: MESSAGE_PREFIX,
                content: bad
            })
            .is_err()
        );
    }
    let mut wm = Watermark {
        wm: WatermarkKind::Low,
        chunk: id,
        attempt: 0,
    };
    wm.chunk.plan = "foreign".into();
    assert!(wm.encode().is_err());
}

#[test]
fn watermarks_require_low_then_high_and_ignore_other_chunks() {
    let t = table(&[Direction::Asc]);
    let p = plan_chunks(&t, 1, &[key(2)]).unwrap();
    let id = p[0].id.clone();
    let mut w = Window::new(t.clone(), p[0].clone(), vec![], 0).unwrap();
    marker(&mut w, &p[1].id, WatermarkKind::High, 50).unwrap();
    assert!(w.finish(source(), "pg", 1).is_err());
    let mut w = Window::new(t.clone(), p[0].clone(), vec![], 0).unwrap();
    assert!(marker(&mut w, &id, WatermarkKind::High, 30).is_err());
    let mut w = Window::new(t, p[0].clone(), vec![], 0).unwrap();
    marker(&mut w, &id, WatermarkKind::Low, 10).unwrap();
    assert!(marker(&mut w, &id, WatermarkKind::High, 10).is_err());
}

#[test]
fn abandoned_attempt_markers_do_not_open_replacement_window() {
    let t = table(&[Direction::Asc]);
    let c = plan_chunks(&t, 2, &[]).unwrap().remove(0);
    let id = c.id.clone();
    let mut w = Window::new(t, c, vec![row(1), row(2)], 1).unwrap();
    marker(&mut w, &id, WatermarkKind::Low, 10).unwrap(); // old attempt 0
    w.event(&event(EventKind::Delete, 1, None)).unwrap();
    for (kind, lsn) in [(WatermarkKind::Low, 20), (WatermarkKind::High, 30)] {
        let bytes = Watermark {
            wm: kind,
            chunk: id.clone(),
            attempt: 1,
        }
        .encode()
        .unwrap();
        w.watermark(
            &Message::LogicalMessage {
                transactional: true,
                lsn: lsn - 1,
                prefix: MESSAGE_PREFIX,
                content: &bytes,
            },
            lsn,
        )
        .unwrap();
        if kind == WatermarkKind::Low {
            w.event(&event(EventKind::Delete, 2, None)).unwrap();
        }
    }
    assert_eq!(finish(w), vec![1]);
}

#[test]
fn marker_requires_its_transaction_commit_boundary() {
    let t = table(&[Direction::Asc]);
    let c = plan_chunks(&t, 2, &[]).unwrap().remove(0);
    let bytes = Watermark {
        wm: WatermarkKind::Low,
        chunk: c.id.clone(),
        attempt: 0,
    }
    .encode()
    .unwrap();
    let mut w = Window::new(t, c, vec![], 0).unwrap();
    assert!(
        w.watermark(
            &Message::LogicalMessage {
                transactional: true,
                lsn: 20,
                prefix: MESSAGE_PREFIX,
                content: &bytes
            },
            19
        )
        .is_err()
    );
}

#[test]
fn composite_row_predicates_and_desc_bounds() {
    for (d, lower, upper) in [(Direction::Asc, ">=", "<"), (Direction::Desc, "<=", ">")] {
        let t = table(&[d, d]);
        let p = plan_chunks(
            &t,
            2,
            &[
                vec![Value::Int4(2), Value::Int4(3)],
                vec![Value::Int4(5), Value::Int4(7)],
            ],
        )
        .unwrap();
        let q = t.read_query(&p[1]).unwrap();
        assert!(q.sql.contains(&format!("((\"k0\", \"k1\") {lower} ($1::text::\"pg_catalog\".\"int4\", $2::text::\"pg_catalog\".\"int4\"))")));
        assert!(q.sql.contains(&format!("((\"k0\", \"k1\") {upper} ($3::text::\"pg_catalog\".\"int4\", $4::text::\"pg_catalog\".\"int4\"))")));
        assert_eq!(q.parameters, vec!["2", "3", "5", "7"]);
    }
}

#[test]
fn mixed_desc_lexicographic_predicate() {
    let t = table(&[Direction::Asc, Direction::Desc]);
    let p = plan_chunks(&t, 2, &[vec![Value::Int4(2), Value::Int4(3)]]).unwrap();
    let q = t.read_query(&p[1]).unwrap();
    assert!(q.sql.contains("(\"k0\" > $1::text::\"pg_catalog\".\"int4\") OR (\"k0\" = $1::text::\"pg_catalog\".\"int4\" AND \"k1\" <= $2::text::\"pg_catalog\".\"int4\")"));
    assert!(q.sql.ends_with("ORDER BY \"k0\" ASC, \"k1\" DESC"));
}

#[test]
fn samples_preserve_source_collation_order_and_uuid_order() {
    let mut t = table(&[Direction::Asc]);
    t.columns[0].pg_type = PgType::Text {
        encoding: "UTF8".into(),
    };
    t.columns[0].sql_type = name("text");
    t.columns[0].collation = Some(name("C"));
    let text = |s: &str| {
        vec![Value::Text(EncodedBytes {
            bytes: s.as_bytes().to_vec(),
            encoding: "UTF8".into(),
        })]
    };
    let samples = vec![text("z"), text("A")]; // deliberately not Rust sorted
    let p = plan_chunks(&t, 1, &samples).unwrap();
    assert_eq!(p[1].lo, Some(text("z")));
    assert_eq!(p[1].hi, Some(text("A")));
    assert!(
        t.sampling_query(1)
            .unwrap()
            .sql
            .contains("COLLATE \"pg_catalog\".\"C\"")
    );
    assert!(
        t.read_query(&p[1])
            .unwrap()
            .sql
            .contains("$1::text::\"pg_catalog\".\"text\" COLLATE")
    );
    t.columns[0].pg_type = PgType::Uuid;
    t.columns[0].sql_type = name("uuid");
    t.columns[0].collation = None;
    assert!(plan_chunks(&t, 2, &[vec![Value::Uuid([255; 16])]]).is_ok());
}

#[test]
fn deterministic_plan_resume_and_new_plan_cannot_skip() {
    let t = table(&[Direction::Asc]);
    let p = plan_chunks(&t, 2, &[key(3), key(5)]).unwrap();
    assert_eq!(p, plan_chunks(&t, 2, &[key(3), key(5)]).unwrap());
    let journal = vec![
        ChunkTransition::ChunkStarted {
            chunk: p[0].clone(),
            attempt: 0,
        },
        ChunkTransition::ChunkDone {
            chunk: p[0].clone(),
            attempt: 0,
            high_lsn: 30,
        },
        ChunkTransition::ChunkStarted {
            chunk: p[1].clone(),
            attempt: 0,
        },
    ];
    let bytes = serde_json::to_vec(&journal).unwrap();
    assert_eq!(
        unfinished_chunks(
            &p,
            &serde_json::from_slice::<Vec<ChunkTransition>>(&bytes).unwrap()
        )
        .unwrap(),
        p[1..]
    );
    assert!(unfinished_chunks(&plan_chunks(&t, 2, &[key(4)]).unwrap(), &journal).is_err());
    assert!(
        unfinished_chunks(
            &p,
            &[ChunkTransition::ChunkDone {
                chunk: p[0].clone(),
                attempt: 0,
                high_lsn: 30
            }]
        )
        .is_err()
    );
    let mut journal = journal;
    journal.push(ChunkTransition::ChunkStarted {
        chunk: p[0].clone(),
        attempt: 1,
    });
    assert_eq!(unfinished_chunks(&p, &journal).unwrap(), p);
}

#[test]
fn invalid_keys_and_identifiers_fail_closed() {
    let mut t = table(&[Direction::Asc]);
    assert!(plan_chunks(&t, 0, &[]).is_err());
    assert!(plan_chunks(&t, 1, &[key(1), key(1)]).is_err());
    assert!(plan_chunks(&t, 1, &[vec![Value::Null]]).is_err());
    assert!(plan_chunks(&t, 1, &[vec![]]).is_err());
    let p = plan_chunks(&t, 1, &[]).unwrap();
    assert!(Window::new(t.clone(), p[0].clone(), vec![Row::new()], 0).is_err());
    t.relation.name = "x\"; DROP TABLE x;--".into();
    assert!(
        t.sampling_query(2)
            .unwrap()
            .sql
            .contains("\"x\"\"; DROP TABLE x;--\"")
    );
    t.columns[0].name = "\0".into();
    assert!(t.sampling_query(2).is_err());
}
