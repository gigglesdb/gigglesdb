use giggles_migrate::*;
use std::collections::BTreeMap;

fn manifest() -> Manifest {
    Manifest {
        version: 1,
        cell_semantics: CellSemantics {
            timestamp_unit: TimestampUnit::MicrosecondsI64,
            expiry: ExpiryContract::AbsoluteMicrosecondsExclusive,
            tombstone_retention: TombstoneRetention::PermanentForRehearsal,
            tombstone_scopes: vec![
                TombstoneScope::Cell,
                TombstoneScope::Row,
                TombstoneScope::Partition,
                TombstoneScope::Range,
            ],
        },
        source: "offline-source".into(),
        table: "messages".into(),
        schema_epoch: 7,
        topology_epoch: 2,
        partition_types: vec![ScalarType::Bytes],
        clustering_types: vec![ScalarType::Int],
        clustering_order: vec![Direction::Asc],
        columns: BTreeMap::from([
            ("body".into(), ScalarType::Text),
            ("other".into(), ScalarType::Bytes),
        ]),
        streams: vec![Stream {
            id: "s".into(),
            start: 0,
            end: 100,
        }],
        horizon_micros: 100,
        tie_policy: "reject_ambiguous".into(),
        unsupported_features: vec![],
        bounds: Bounds {
            max_events: 128,
            max_chunk_bytes: 48000,
            max_state_bytes: 48000,
            max_rows: 128,
        },
    }
}
fn put(id: u64, k: i64, t: i64, v: Scalar, expiry: Option<i64>) -> Event {
    Event {
        id: format!("e{id}"),
        source: "offline-source".into(),
        stream: "s".into(),
        position: id,
        source_position: format!("opaque\0/source/{id}"),
        schema_epoch: 7,
        topology_epoch: 2,
        partition: vec![Scalar::Bytes(vec![0, 255])],
        mutation: Mutation::Put {
            key: vec![Scalar::Int(k)],
            column: "body".into(),
            value: v,
            timestamp: t,
            expires_at: expiry,
        },
    }
}
fn text(id: u64, k: i64, t: i64, v: &str) -> Event {
    put(id, k, t, Scalar::Text(v.into()), None)
}
fn tomb(id: u64, t: i64, target: Scope) -> Event {
    let mut e = text(id, 0, t, "");
    e.mutation = Mutation::Tombstone {
        target,
        timestamp: t,
    };
    e
}
#[test]
fn manifest_rejects_unsupported_and_unknown() {
    let m = manifest();
    m.validate().unwrap();
    for feature in ["counters", "lwt", "collections", "static"] {
        let mut n = m.clone();
        n.unsupported_features.push(feature.into());
        assert!(n.validate().is_err());
    }
    let mut n = m.clone();
    n.version = 2;
    assert!(n.validate().is_err());
    n = m.clone();
    n.tie_policy = "release_guess".into();
    assert!(n.validate().is_err());
    let mut j = serde_json::to_value(m).unwrap();
    j["surprise"] = true.into();
    assert!(serde_json::from_value::<Manifest>(j).is_err());
}
#[test]
fn invalid_keys_epochs_streams_positions() {
    let m = manifest();
    let e = text(0, 0, 0, "x");
    for i in 0..8 {
        let mut e = e.clone();
        match i {
            0 => e.schema_epoch += 1,
            1 => e.topology_epoch += 1,
            2 => e.stream = "missing".into(),
            3 => e.position = 101,
            4 => e.source = "wrong".into(),
            5 => e.partition = vec![Scalar::Null],
            6 => e.partition = vec![],
            _ => e.partition = vec![Scalar::Text("wrong".into())],
        };
        assert!(validate_event(&m, &e).is_err(), "case {i}");
    }
}
#[test]
fn ttl_checked_extremes() {
    assert_eq!(expiry_from_ttl(7, 2).unwrap(), 2_000_007);
    assert_eq!(expiry_from_ttl(i64::MIN, 0).unwrap(), i64::MIN);
    for (ts, ttl) in [(i64::MAX, 1), (0, i64::MAX), (0, -1)] {
        assert!(expiry_from_ttl(ts, ttl).is_err());
    }
}
#[test]
fn manifest_time_and_tombstone_scope_contract() {
    let mut m = manifest();
    m.cell_semantics.tombstone_scopes = vec![TombstoneScope::Cell];
    m.validate().unwrap();
    assert!(validate_event(&m, &tomb(0, 1, Scope::Partition)).is_err());
    m.cell_semantics.tombstone_scopes.push(TombstoneScope::Cell);
    assert!(m.validate().is_err());
    let mut j = serde_json::to_value(manifest()).unwrap();
    j["cell_semantics"]["expiry"] = "relative_arrival_time".into();
    assert!(serde_json::from_value::<Manifest>(j).is_err());
    j = serde_json::to_value(manifest()).unwrap();
    j["cell_semantics"]["timestamp_unit"] = "guess".into();
    assert!(serde_json::from_value::<Manifest>(j).is_err());
}
#[test]
fn conflict_rejected_even_behind_delete_and_extreme_stream_position() {
    let mut m = manifest();
    m.streams[0].start = u64::MAX - 3;
    m.streams[0].end = u64::MAX;
    let mut events = vec![
        text(0, 0, 1, "a"),
        text(1, 0, 1, "b"),
        tomb(2, i64::MAX, Scope::Partition),
    ];
    for (i, e) in events.iter_mut().enumerate() {
        e.position = u64::MAX - i as u64;
    }
    permutations(&mut events, 0, &mut |events| {
        let mut s = State::default();
        assert!(s.apply(&m, events).is_err());
        assert!(s.events.is_empty());
    });
    let mut s = State::default();
    s.apply(&m, &events[2..]).unwrap();
    assert!(!s.declared_cut_complete(&m));
}
#[test]
fn null_missing_empty_deleted() {
    let m = manifest();
    let mut s = State::default();
    s.apply(
        &m,
        &[
            put(0, 0, 1, Scalar::Null, None),
            text(1, 1, 1, ""),
            tomb(
                2,
                2,
                Scope::Row {
                    key: vec![Scalar::Int(2)],
                },
            ),
            text(3, 2, 1, "gone"),
        ],
    )
    .unwrap();
    let rows = s.rows(&m, 100).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].cells["body"].value, Scalar::Null);
    assert!(!rows[0].cells.contains_key("other"));
    assert_eq!(rows[1].cells["body"].value, Scalar::Text("".into()));
    let mut empty = put(4, 3, 1, Scalar::Bytes(vec![]), None);
    if let Mutation::Put { column, .. } = &mut empty.mutation {
        *column = "other".into();
    }
    s.apply(&m, &[empty]).unwrap();
    assert_eq!(
        s.rows(&m, 100).unwrap()[2].cells["other"].value,
        Scalar::Bytes(vec![])
    );
}
#[test]
fn equal_tie_rejects_atomically() {
    let m = manifest();
    let mut s = State::default();
    s.apply(&m, &[text(0, 0, 3, "a")]).unwrap();
    let before = s.clone();
    assert!(
        s.apply(&m, &[text(1, 1, 4, "valid"), text(2, 0, 3, "b")])
            .is_err()
    );
    assert_eq!(s, before);
    s.apply(&m, &[text(3, 0, 3, "a")]).unwrap();
    let before = s.clone();
    assert!(
        s.apply(&m, &[put(4, 0, 3, Scalar::Text("a".into()), Some(100))])
            .is_err()
    );
    assert_eq!(s, before);
    let mut reused = text(3, 1, 9, "mismatch");
    assert!(s.apply(&m, &[reused.clone()]).is_err());
    reused.id = "newid".into();
    assert!(s.apply(&m, &[reused]).is_err());
}
#[test]
fn delayed_backfill_never_resurrects() {
    let m = manifest();
    let mut s = State::default();
    s.apply(
        &m,
        &[
            put(0, 0, 9, Scalar::Text("expired".into()), Some(100)),
            tomb(
                1,
                9,
                Scope::Row {
                    key: vec![Scalar::Int(1)],
                },
            ),
        ],
    )
    .unwrap();
    s.apply(&m, &[text(2, 0, 8, "old"), text(3, 1, 8, "old")])
        .unwrap();
    assert!(s.rows(&m, 100).unwrap().is_empty());
    let copy = s.clone();
    s.apply(&m, &copy.events.values().cloned().collect::<Vec<_>>())
        .unwrap();
    assert_eq!(s, copy);
    s.apply(&m, &[text(4, 1, 10, "fresh")]).unwrap();
    assert_eq!(s.rows(&m, 100).unwrap().len(), 1);
}
#[test]
fn mixed_ttl_horizon_edges() {
    let m = manifest();
    let mut s = State::default();
    s.apply(
        &m,
        &[
            put(0, 0, 1, Scalar::Text("x".into()), Some(100)),
            put(1, 1, 2, Scalar::Text("y".into()), Some(101)),
            put(2, 2, 3, Scalar::Text("z".into()), Some(i64::MIN)),
        ],
    )
    .unwrap();
    assert_eq!(s.rows(&m, 99).unwrap().len(), 2);
    assert_eq!(s.rows(&m, 100).unwrap().len(), 1);
    assert!(s.rows(&m, i64::MAX).unwrap().is_empty());
}
#[test]
fn timestamp_extremes() {
    let m = manifest();
    let mut s = State::default();
    s.apply(
        &m,
        &[
            text(0, 0, i64::MIN, "min"),
            text(1, 0, i64::MAX, "max"),
            tomb(
                2,
                i64::MAX,
                Scope::Cell {
                    key: vec![Scalar::Int(0)],
                    column: "body".into(),
                },
            ),
        ],
    )
    .unwrap();
    assert!(s.rows(&m, 100).unwrap().is_empty());
}
#[test]
fn scope_boundaries_composite_descending() {
    let mut m = manifest();
    m.clustering_types.push(ScalarType::Bytes);
    m.clustering_order = vec![Direction::Desc, Direction::Asc];
    let key = |n| vec![Scalar::Int(n), Scalar::Bytes(vec![0, 255])];
    let scope = Scope::Range {
        lower: Some(Bound {
            key: key(3),
            inclusive: false,
        }),
        upper: Some(Bound {
            key: key(1),
            inclusive: true,
        }),
    };
    assert!(!covers(&m, &scope, &key(3), "body"));
    assert!(covers(&m, &scope, &key(2), "body"));
    assert!(covers(&m, &scope, &key(1), "body"));
    assert!(!covers(&m, &scope, &key(0), "body"));
    let mut events = vec![];
    for i in 0..5 {
        let mut e = text(i, i as i64, 1, "x");
        if let Mutation::Put { key: k, .. } = &mut e.mutation {
            *k = key(i as i64);
        }
        events.push(e);
    }
    events.push(tomb(5, 1, scope));
    let mut s = State::default();
    s.apply(&m, &events).unwrap();
    let rows = s.rows(&m, 100).unwrap();
    assert_eq!(
        rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec![key(4), key(3), key(0)]
    );
    let mut bad = tomb(
        6,
        2,
        Scope::Range {
            lower: Some(Bound {
                key: key(0),
                inclusive: true,
            }),
            upper: Some(Bound {
                key: key(3),
                inclusive: true,
            }),
        },
    );
    assert!(validate_event(&m, &bad).is_err());
    bad.partition = vec![];
    assert!(validate_event(&m, &bad).is_err());
}
// Independent single-column oracle: enumerate each possible key, select maximum
// write timestamp, then independently evaluate all four delete scopes.
fn oracle(m: &Manifest, events: &[Event]) -> Vec<MaterializedRow> {
    let mut out = vec![];
    for k in -2..=3 {
        let key = vec![Scalar::Int(k)];
        let mut best: Option<Cell> = None;
        for e in events {
            if let Mutation::Put {
                key: ek,
                value,
                timestamp,
                expires_at,
                ..
            } = &e.mutation
                && *ek == key
                && best.as_ref().is_none_or(|c| c.timestamp < *timestamp)
            {
                best = Some(Cell {
                    value: value.clone(),
                    timestamp: *timestamp,
                    expires_at: *expires_at,
                });
            }
        }
        if let Some(c) = best {
            let mut dead = false;
            for e in events {
                if let Mutation::Tombstone { target, timestamp } = &e.mutation {
                    if *timestamp < c.timestamp {
                        continue;
                    }
                    let hit = match target {
                        Scope::Partition => true,
                        Scope::Row { key: k } | Scope::Cell { key: k, .. } => *k == key,
                        Scope::Range { lower, upper } => {
                            lower
                                .as_ref()
                                .is_none_or(|l| key > l.key || (key == l.key && l.inclusive))
                                && upper
                                    .as_ref()
                                    .is_none_or(|u| key < u.key || (key == u.key && u.inclusive))
                        }
                    };
                    dead |= hit;
                }
            }
            if !dead && c.expires_at.is_none_or(|t| t > m.horizon_micros) {
                out.push(MaterializedRow {
                    partition: events[0].partition.clone(),
                    key,
                    cells: BTreeMap::from([("body".into(), c)]),
                });
            }
        }
    }
    out
}
fn permutations(a: &mut [Event], i: usize, f: &mut impl FnMut(&[Event])) {
    if i == a.len() {
        f(a);
    } else {
        for j in i..a.len() {
            a.swap(i, j);
            permutations(a, i + 1, f);
            a.swap(i, j);
        }
    }
}
#[test]
fn permutation_matches_independent_oracle() {
    let m = manifest();
    let mut events = vec![
        text(0, -1, 1, "old"),
        text(1, -1, 3, "new"),
        text(2, 0, 2, "gone"),
        tomb(
            3,
            3,
            Scope::Row {
                key: vec![Scalar::Int(0)],
            },
        ),
        put(4, 1, 9, Scalar::Text("expired".into()), Some(100)),
        text(5, 1, 1, "backfill"),
    ];
    let expected = oracle(&m, &events);
    let mut count = 0;
    permutations(&mut events, 0, &mut |p| {
        let mut s = State::default();
        for e in p {
            s.apply(&m, std::slice::from_ref(e)).unwrap();
        }
        assert_eq!(s.rows(&m, 100).unwrap(), expected);
        s.apply(&m, p).unwrap();
        assert_eq!(s.rows(&m, 100).unwrap(), expected);
        count += 1;
    });
    assert_eq!(count, 720);
    // A deliberately broken arrival-order reducer resurrects the expired key.
    let mut naive = BTreeMap::new();
    for e in &events {
        if let Mutation::Put { key, value, .. } = &e.mutation {
            naive.insert(key.clone(), value.clone());
        }
    }
    assert_eq!(
        naive[&vec![Scalar::Int(1)]],
        Scalar::Text("backfill".into())
    );
    assert!(!expected.iter().any(|r| r.key == vec![Scalar::Int(1)]));
}
#[test]
fn cell_delete_isolation_and_partition_isolation() {
    let m = manifest();
    let mut a = text(0, 0, 5, "body");
    let mut b = put(1, 0, 4, Scalar::Bytes(vec![0, 255]), None);
    if let Mutation::Put { column, .. } = &mut b.mutation {
        *column = "other".into();
    }
    let c = tomb(
        2,
        5,
        Scope::Cell {
            key: vec![Scalar::Int(0)],
            column: "body".into(),
        },
    );
    let mut d = text(3, 0, 3, "other partition");
    d.partition = vec![Scalar::Bytes(vec![1])];
    let mut s = State::default();
    s.apply(&m, &[a.clone(), b, c, d]).unwrap();
    let rows = s.rows(&m, 100).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].cells.len(), 1);
    assert!(rows[0].cells.contains_key("other"));
    assert!(rows[1].cells.contains_key("body"));
    a.id = "new-body".into();
    a.position = 4;
    if let Mutation::Put { timestamp, .. } = &mut a.mutation {
        *timestamp = 6;
    }
    s.apply(&m, &[a]).unwrap();
    assert_eq!(s.rows(&m, 100).unwrap()[0].cells.len(), 2);
}
#[test]
fn all_scalar_key_bits_and_mixed_direction_order() {
    let mut m = manifest();
    m.partition_types = vec![
        ScalarType::Bool,
        ScalarType::Int,
        ScalarType::Text,
        ScalarType::Bytes,
    ];
    m.clustering_types = vec![ScalarType::Int, ScalarType::Text];
    m.clustering_order = vec![Direction::Desc, Direction::Asc];
    let mut events = vec![];
    for (i, (n, t)) in [(i64::MIN, "a\0"), (i64::MAX, "z"), (i64::MAX, "")]
        .into_iter()
        .enumerate()
    {
        let mut e = text(i as u64, 0, 1, "body");
        e.partition = vec![
            Scalar::Bool(false),
            Scalar::Int(i64::MIN),
            Scalar::Text("\0'雪".into()),
            Scalar::Bytes(vec![0, 128, 255]),
        ];
        if let Mutation::Put { key, .. } = &mut e.mutation {
            *key = vec![Scalar::Int(n), Scalar::Text(t.into())];
        }
        events.push(e);
    }
    let mut s = State::default();
    s.apply(&m, &events).unwrap();
    let rows = s.rows(&m, 100).unwrap();
    assert_eq!(
        rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec![
            vec![Scalar::Int(i64::MAX), Scalar::Text("".into())],
            vec![Scalar::Int(i64::MAX), Scalar::Text("z".into())],
            vec![Scalar::Int(i64::MIN), Scalar::Text("a\0".into())]
        ]
    );
    assert_eq!(rows[0].partition, events[0].partition);
}
#[test]
fn range_permutations_match_reference() {
    let m = manifest();
    for inclusive in [false, true] {
        let mut events = vec![
            text(0, -1, 1, "left"),
            text(1, 0, 1, "middle"),
            text(2, 1, 1, "right"),
            tomb(
                3,
                1,
                Scope::Range {
                    lower: Some(Bound {
                        key: vec![Scalar::Int(-1)],
                        inclusive,
                    }),
                    upper: Some(Bound {
                        key: vec![Scalar::Int(1)],
                        inclusive: false,
                    }),
                },
            ),
        ];
        let reference = oracle(&m, &events);
        permutations(&mut events, 0, &mut |events| {
            let mut s = State::default();
            for e in events {
                s.apply(&m, std::slice::from_ref(e)).unwrap();
            }
            assert_eq!(s.rows(&m, 100).unwrap(), reference);
        });
    }
}
#[test]
fn seeded_state_machine_reference() {
    let m = manifest();
    for seed in 0..32u64 {
        let mut rng = seed + 1;
        let mut events = vec![];
        for i in 0..24 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let k = (rng % 6) as i64 - 2;
            let e = if rng & 7 == 0 {
                tomb(i, i as i64, Scope::Partition)
            } else if rng & 3 == 0 {
                tomb(
                    i,
                    i as i64,
                    Scope::Cell {
                        key: vec![Scalar::Int(k)],
                        column: "body".into(),
                    },
                )
            } else {
                text(i, k, i as i64, "v")
            };
            events.push(e);
        }
        let expected = oracle(&m, &events);
        for i in (1..events.len()).rev() {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            events.swap(i, (rng as usize) % (i + 1));
        }
        let mut s = State::default();
        for e in &events {
            s.apply(&m, std::slice::from_ref(e)).unwrap();
            s.apply(&m, std::slice::from_ref(e)).unwrap();
        }
        assert_eq!(s.rows(&m, 100).unwrap(), expected, "seed {seed}");
    }
}
#[test]
fn canonical_chunk_digest_and_bounds() {
    let m = manifest();
    let a = Chunk {
        id: "c".into(),
        events: vec![text(0, 0, 1, "a"), text(1, 1, 1, "b")],
    };
    let mut b = a.clone();
    b.events.reverse();
    assert_eq!(chunk_digest(&a).unwrap(), chunk_digest(&b).unwrap());
    b.events.push(b.events[0].clone());
    assert_ne!(chunk_digest(&a).unwrap(), chunk_digest(&b).unwrap());
    let mut m = m;
    m.bounds.max_events = 1;
    let mut s = State::default();
    assert!(s.apply(&m, &a.events).is_err());
    assert!(s.events.is_empty());
}
#[test]
fn original_opaque_position_is_preserved_and_bound() {
    let m = manifest();
    let e = text(0, 0, 1, "value");
    let mut s = State::default();
    s.apply(&m, std::slice::from_ref(&e)).unwrap();
    assert_eq!(s.events[&e.id].source_position, "opaque\0/source/0");
    let mut altered = e.clone();
    altered.source_position = "changed opaque token".into();
    assert!(s.apply(&m, &[altered]).is_err());
    let mut empty = e;
    empty.source_position.clear();
    assert!(validate_event(&m, &empty).is_err());
}
