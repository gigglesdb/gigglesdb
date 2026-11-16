use super::*;
use crate::{
    event::{Position, SourceIdentity},
    mapping::{Array, ArrayDimension, EncodedBytes},
};

fn column(name: &str, pg_type: PgType, wire_type: Type) -> Column {
    Column {
        name: name.into(),
        type_id: name.into(),
        pg_type,
        wire_type,
    }
}
fn table() -> Table {
    Table {
        event_table: "source.table".into(),
        schema: "odd\"schema".into(),
        name: "select".into(),
        schema_version: 1,
        columns: vec![
            column("id", PgType::Int4, Type::INT4),
            column(
                "body",
                PgType::Text {
                    encoding: "UTF8".into(),
                },
                Type::TEXT,
            ),
            column("n", PgType::Float8, Type::FLOAT8),
        ],
        key: vec!["id".into()],
    }
}
fn opaque(name: &str, value: Value) -> OpaqueValue {
    OpaqueValue {
        type_id: name.into(),
        bytes: (value != Value::Null).then(|| value.canonical_bytes().unwrap()),
    }
}
fn row(id: i32) -> Row {
    BTreeMap::from([("id".into(), opaque("id", Value::Int4(id)))])
}
fn text(s: &str) -> OpaqueValue {
    opaque(
        "body",
        Value::Text(EncodedBytes {
            bytes: s.as_bytes().into(),
            encoding: "UTF8".into(),
        }),
    )
}
fn transaction(kind: EventKind, after: Option<Row>) -> AssembledTxn {
    let source = SourceIdentity {
        engine: "postgres".into(),
        system_id: "system".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        stream: "slot".into(),
    };
    let event = Event {
        source: source.clone(),
        txn: TxnId(vec![1]),
        ordinal_in_txn: 0,
        position: Position::new("pg/source", 99_u64.to_be_bytes()),
        schema_version: 1,
        kind: kind.clone(),
        table: Some("source.table".into()),
        key: row(1),
        before: None,
        after,
        unchanged_cols: Default::default(),
    };
    let mut event = event;
    if matches!(kind, EventKind::Truncate { .. }) {
        event.table = None;
        event.key.clear();
        event.after = None;
    }
    AssembledTxn::new(
        source,
        event.txn.clone(),
        event.position.clone(),
        Some(1_000_000_000_000_000),
        vec![event],
    )
    .unwrap()
}
fn reseal(txn: AssembledTxn) -> AssembledTxn {
    AssembledTxn::new_with_origin(
        txn.source,
        txn.txn,
        txn.commit_position,
        txn.commit_time,
        txn.source_origin,
        txn.events,
    )
    .unwrap()
}
fn context() -> PlanContext {
    PlanContext {
        positions: SourcePositions::native_pg("pg/source"),
        stream: "forward".into(),
        mseq: 7,
        origin: "skedaddle_test".into(),
        synthetic_commit_time: None,
    }
}
fn plan(txn: &AssembledTxn, state: RangeState) -> TransactionPlan {
    plan_transaction(txn, &[table()], &context(), |_, _| Ok(state)).unwrap()
}

fn administrative_transaction() -> AssembledTxn {
    // Same minimal REVOKE as R6b1's blocker probe, with the real pgoutput ID.
    let mut txn = transaction(EventKind::DdlBoundary, None);
    txn.txn = TxnId(
        [
            42_u32.to_be_bytes().as_slice(),
            99_u64.to_be_bytes().as_slice(),
        ]
        .concat(),
    );
    let record = super::super::ddl::DdlRecord {
        format: 1,
        installation: "test".into(),
        origin: Some("test".into()),
        server_major: 18,
        version: 1,
        tag: "REVOKE".into(),
        xid: "42".into(),
        query: "REVOKE INSERT ON public.items FROM app".into(),
        commands: vec![],
    };
    let event = &mut txn.events[0];
    event.txn = txn.txn.clone();
    event.table = None;
    event.key.clear();
    event.after = Some(Row::from([(
        "control".into(),
        OpaqueValue {
            type_id: "skedaddle.ddl.v1".into(),
            bytes: Some(super::super::ddl::encode(&record).unwrap()),
        },
    )]));
    reseal(txn)
}

#[test]
fn configured_internal_ddl_is_noop_with_original_progress_and_digest() {
    let txn = administrative_transaction();
    let original = txn.clone();
    let plan =
        plan_transaction_with_ddl(&txn, &[], &context(), "test", |_, _| panic!("no rows")).unwrap();
    assert_eq!(txn, original);
    assert_eq!(plan.statements.len(), 3); // origin setup plus normal progress only
    assert_eq!(plan.progress.txn, txn.txn);
    assert_eq!(plan.progress.digest, txn.digest);
    assert_eq!(plan.progress.mseq, context().mseq);
    assert_eq!(
        plan.progress.position,
        Position::new("pg/source", 99_u64.to_be_bytes())
    );
    assert_eq!(
        plan.statements[2].parameters[4].bytes(),
        Some(txn.digest.as_slice())
    );
    assert!(matches!(
        plan_transaction(&txn, &[], &context(), |_, _| panic!()),
        Err(ApplyError::DdlBoundary { ordinal: 0 })
    ));
    assert!(matches!(
        plan_transaction_with_ddl(&txn, &[], &context(), "foreign", |_, _| panic!()),
        Err(ApplyError::DdlBoundary { ordinal: 0 })
    ));
    let reverse =
        super::super::reverse::Config::new("test", 180000, vec![("public".into(), "items".into())])
            .unwrap();
    assert_eq!(
        reverse
            .plan_transaction(&txn, &[], 7, context().positions, |_, _| panic!())
            .unwrap()
            .progress
            .digest,
        txn.digest
    );
}

#[test]
fn internal_ddl_mixed_with_application_or_malformed_boundary_is_rejected() {
    for variant in 0..9 {
        let mut txn = administrative_transaction();
        let mut event = txn.events[0].clone();
        event.ordinal_in_txn = 1;
        event.schema_version = 2;
        let value = event.after.as_mut().unwrap().get_mut("control").unwrap();
        let mut record: super::super::ddl::DdlRecord =
            serde_json::from_slice(value.bytes.as_ref().unwrap()).unwrap();
        record.version = 2;
        match variant {
            0 => {
                record.origin = None;
                record.tag = "ALTER TABLE".into();
            }
            1 => {
                record.tag = "DROP TABLE".into();
            }
            2 => record.origin = Some("foreign".into()),
            3 => record.xid = "43".into(),
            4 => record.version = 3,
            5 => record.version = 1,
            6 => value.type_id = "skedaddle.stop.v1".into(),
            7 => event.table = Some("source.table".into()),
            _ => event.schema_version = 3,
        }
        event
            .after
            .as_mut()
            .unwrap()
            .get_mut("control")
            .unwrap()
            .bytes = Some(super::super::ddl::encode(&record).unwrap());
        txn.events.push(event);
        let txn = reseal(txn);
        assert!(
            matches!(
                plan_transaction_with_ddl(&txn, &[], &context(), "test", |_, _| panic!()),
                Err(ApplyError::DdlBoundary { ordinal: 1 })
            ),
            "variant {variant}"
        );
    }
    let mut txn = administrative_transaction();
    let value = txn.events[0]
        .after
        .as_mut()
        .unwrap()
        .get_mut("control")
        .unwrap();
    let mut record: super::super::ddl::DdlRecord =
        serde_json::from_slice(value.bytes.as_ref().unwrap()).unwrap();
    record.origin = None;
    value.bytes = Some(super::super::ddl::encode(&record).unwrap());
    assert!(matches!(
        plan_transaction_with_ddl(&reseal(txn), &[], &context(), "test", |_, _| panic!()),
        Err(ApplyError::DdlBoundary { ordinal: 0 })
    ));
}

#[test]
fn insert_shape_order_parameters_and_progress() {
    let mut after = row(1);
    after.insert("body".into(), text("Robert'); DROP TABLE x;--"));
    let txn = transaction(EventKind::Insert, Some(after));
    let plan = plan(&txn, RangeState::Completed);
    assert!(plan.session_setup.sql.contains("origin_session_setup"));
    assert_eq!(plan.statements.len(), 4);
    assert_eq!(
        plan.statements[0].sql,
        "SET LOCAL session_replication_role = replica"
    );
    assert_eq!(
        plan.statements[1].parameters[0].bytes(),
        Some(b"0/63".as_slice())
    );
    assert_eq!(
        plan.statements[1].parameters[1].bytes(),
        Some(
            (1_000_000_000_000_000_i64 - 946_684_800_000_000)
                .to_be_bytes()
                .as_slice()
        )
    );
    let insert = &plan.statements[2];
    assert_eq!(
        insert.sql,
        "INSERT INTO \"odd\"\"schema\".\"select\" (\"id\", \"body\") VALUES ($1, $2)"
    );
    assert!(!insert.sql.contains("Robert"));
    assert_eq!(
        insert.parameters[1].bytes(),
        Some(b"Robert'); DROP TABLE x;--".as_slice())
    );
    let progress = plan.statements.last().unwrap();
    assert!(progress.sql.contains("last_mseq"));
    assert_eq!(progress.parameters[2].bytes(), Some(txn.txn.0.as_slice()));
    assert_eq!(progress.parameters[4].bytes(), Some(txn.digest.as_slice()));
}

#[test]
fn copying_inserts_upsert_only_present_columns() {
    let plan = plan(
        &transaction(EventKind::Insert, Some(row(1))),
        RangeState::Copying,
    );
    assert!(
        plan.statements[2]
            .sql
            .ends_with("ON CONFLICT (\"id\") DO NOTHING")
    );
    assert!(!plan.statements[2].sql.contains("body"));
    assert!(plan.statements[2].check_rows(0).is_ok());
}

#[test]
fn unchanged_toast_is_never_written_in_update_or_fallback() {
    let mut after = row(1);
    after.insert("body".into(), text("must not bind"));
    let mut txn = transaction(EventKind::Update, Some(after));
    txn.events[0].unchanged_cols.insert("body".into());
    let plan = plan(&reseal(txn), RangeState::Copying);
    for statement in &plan.statements[2..4] {
        assert!(!statement.sql.contains("body"));
    }
    assert!(!plan.statements[2].if_previous_missing);
    let fallback = &plan.statements[3];
    assert!(fallback.should_execute(Some(0)).unwrap());
    assert!(!fallback.should_execute(Some(1)).unwrap());
    assert!(fallback.should_execute(None).is_err());
}

#[test]
fn key_change_updates_old_key_before_copying_fallback_with_new_key() {
    let mut after = row(2);
    after.insert("body".into(), text("new"));
    let txn = transaction(EventKind::Update, Some(after));
    let mut seen = false;
    let plan = plan_transaction(&txn, &[table()], &context(), |_, key| {
        assert_eq!(key, &row(1));
        seen = true;
        Ok(RangeState::Copying)
    })
    .unwrap();
    assert!(seen);
    assert_eq!(
        plan.statements[2].sql,
        "UPDATE \"odd\"\"schema\".\"select\" SET \"id\" = $1, \"body\" = $2 WHERE \"id\" = $3"
    );
    assert_eq!(
        plan.statements[2].parameters[0].bytes(),
        Some(b"2".as_slice())
    );
    assert_eq!(
        plan.statements[2].parameters[2].bytes(),
        Some(b"1".as_slice())
    );
    assert_eq!(
        plan.statements[3].parameters[0].bytes(),
        Some(b"2".as_slice())
    );
}

#[test]
fn completed_update_and_delete_missing_row_are_typed_faults() {
    for kind in [EventKind::Update, EventKind::Delete] {
        let txn = transaction(kind.clone(), (kind == EventKind::Update).then(|| row(1)));
        let plan = plan(&txn, RangeState::Completed);
        assert_eq!(plan.statements.len(), 4);
        assert_eq!(
            plan.statements[2].check_rows(0),
            Err(ApplyError::MissingRow {
                table: "source.table".into(),
                ordinal: 0
            })
        );
        assert!(plan.statements[2].check_rows(1).is_ok());
        assert_eq!(
            plan.statements[2].check_rows(2),
            Err(ApplyError::UnexpectedRowCount(2))
        );
    }
    let plan = plan(&transaction(EventKind::Delete, None), RangeState::Copying);
    assert!(plan.statements[2].check_rows(0).is_ok());
}

#[test]
fn empty_update_still_checks_existence() {
    let plan = plan(
        &transaction(EventKind::Update, Some(Row::new())),
        RangeState::Completed,
    );
    assert_eq!(
        plan.statements[2].sql,
        "SELECT 1 FROM \"odd\"\"schema\".\"select\" WHERE \"id\" = $1 FOR UPDATE"
    );
    assert!(matches!(
        plan.statements[2].check_rows(0),
        Err(ApplyError::MissingRow { .. })
    ));
}

#[test]
fn chunk_rows_overwrite_complete_rows_even_in_completed_range() {
    let mut after = row(1);
    after.insert("body".into(), text("snapshot"));
    after.insert("n".into(), opaque("n", Value::Null));
    let plan = plan(
        &transaction(EventKind::ChunkRows, Some(after)),
        RangeState::Completed,
    );
    assert!(
        plan.statements[2]
            .sql
            .contains("DO UPDATE SET \"body\" = EXCLUDED.\"body\", \"n\" = EXCLUDED.\"n\"")
    );
    assert!(matches!(
        plan_transaction(
            &transaction(EventKind::ChunkRows, Some(row(1))),
            &[table()],
            &context(),
            |_, _| Ok(RangeState::Copying)
        ),
        Err(ApplyError::Invalid("ChunkRows requires a complete row"))
    ));
}

#[test]
fn null_empty_and_omitted_are_distinct() {
    for value in [opaque("body", Value::Null), text("")] {
        let mut after = row(1);
        after.insert("body".into(), value.clone());
        let plan = plan(
            &transaction(EventKind::Update, Some(after)),
            RangeState::Completed,
        );
        assert_eq!(
            plan.statements[2].parameters[1].bytes(),
            if value.bytes.is_none() {
                None
            } else {
                Some(b"".as_slice())
            }
        );
        assert!(plan.statements[2].sql.contains("\"body\" = $2"));
    }
    assert!(
        !plan(
            &transaction(EventKind::Update, Some(row(1))),
            RangeState::Completed
        )
        .statements[2]
            .sql
            .contains("body")
    );
}

#[test]
fn float_nan_payload_and_array_bounds_use_binary_bind() {
    let col = column("n", PgType::Float8, Type::FLOAT8);
    for bits in [0xfff8_0000_0000_1234, (-0.0_f64).to_bits()] {
        let param = parameter(&col, &opaque("n", Value::Float8(bits))).unwrap();
        assert!(matches!(param.format(), Format::Binary));
        assert_eq!(param.bytes(), Some(bits.to_be_bytes().as_slice()));
    }
    let col = column(
        "n",
        PgType::Array(Box::new(PgType::Float4)),
        Type::FLOAT4_ARRAY,
    );
    let value = Value::Array(Array {
        element_type: Box::new(PgType::Float4),
        dimensions: vec![ArrayDimension {
            length: 2,
            lower_bound: -3,
        }],
        elements: vec![Value::Float4(0x7fc0_1234), Value::Null],
    });
    let param = parameter(&col, &opaque("n", value)).unwrap();
    let bytes = param.bytes().unwrap();
    assert_eq!(&bytes[8..12], &Type::FLOAT4.oid().to_be_bytes());
    assert_eq!(&bytes[16..20], &(-3_i32).to_be_bytes());
    assert_eq!(&bytes[24..28], &0x7fc0_1234_u32.to_be_bytes());
    assert_eq!(&bytes[28..], &(-1_i32).to_be_bytes());
}

#[test]
fn bind_checks_actual_destination_type_and_preserves_empty() {
    let col = column(
        "body",
        PgType::Text {
            encoding: "UTF8".into(),
        },
        Type::TEXT,
    );
    let param = parameter(&col, &text("")).unwrap();
    let mut out = BytesMut::new();
    assert!(matches!(
        param.to_sql_checked(&Type::TEXT, &mut out).unwrap(),
        IsNull::No
    ));
    assert!(out.is_empty());
    assert!(param.to_sql_checked(&Type::INT4, &mut out).is_err());
    let null = parameter(&col, &opaque("body", Value::Null)).unwrap();
    assert!(matches!(
        null.to_sql_checked(&Type::TEXT, &mut out).unwrap(),
        IsNull::Yes
    ));
}

#[test]
fn truncate_markers_and_ddl_are_explicit() {
    let plan = plan(
        &transaction(
            EventKind::Truncate {
                relations: vec!["source.table".into()],
                cascade: false,
                restart_identity: false,
            },
            None,
        ),
        RangeState::Completed,
    );
    assert_eq!(
        plan.statements[2].sql,
        "TRUNCATE TABLE \"odd\"\"schema\".\"select\""
    );
    for kind in [EventKind::Watermark, EventKind::FinalMarker] {
        assert_eq!(
            self::plan(&transaction(kind, None), RangeState::Completed)
                .statements
                .len(),
            3
        );
    }
    assert!(matches!(
        plan_transaction(
            &transaction(EventKind::DdlBoundary, None),
            &[table()],
            &context(),
            |_, _| Ok(RangeState::Completed)
        ),
        Err(ApplyError::DdlBoundary { ordinal: 0 })
    ));
}

#[test]
fn grouped_truncate_has_one_statement_in_source_order_and_preserves_origin() {
    let first = table();
    let mut second = first.clone();
    second.event_table = "source.other".into();
    second.name = "child".into();
    for cascade in [false, true] {
        for restart_identity in [false, true] {
            let mut txn = transaction(
                EventKind::Truncate {
                    relations: vec![second.event_table.clone(), first.event_table.clone()],
                    cascade,
                    restart_identity,
                },
                None,
            );
            txn.source_origin = Some(SourceOrigin {
                name: "original.publisher".into(),
                lsn: 1234,
            });
            let txn = reseal(txn);
            let encoded = txn.canonical_bytes().unwrap();
            let txn = AssembledTxn::from_canonical_bytes(&encoded).unwrap();
            let plan = plan_transaction(
                &txn,
                &[first.clone(), second.clone()],
                &context(),
                |_, _| panic!("truncate must bypass row lookup"),
            )
            .unwrap();
            let suffix = format!(
                "{}{}",
                if restart_identity {
                    " RESTART IDENTITY"
                } else {
                    ""
                },
                if cascade { " CASCADE" } else { "" }
            );
            assert_eq!(plan.statements.len(), 4);
            assert_eq!(
                plan.statements[2].sql,
                format!(
                    "TRUNCATE TABLE \"odd\"\"schema\".\"child\", \"odd\"\"schema\".\"select\"{suffix}"
                )
            );
            assert!(plan.statements[2].parameters.is_empty());
            assert_eq!(plan.source_origin, txn.source_origin);
            assert_eq!(plan.progress.digest, txn.digest);
            assert_eq!(
                plan.progress.position,
                Position::new("pg/source", 99_u64.to_be_bytes())
            );
            assert_eq!(
                plan.session_setup.parameters[0].bytes(),
                Some(context().origin.as_bytes())
            );
        }
    }
    let txn = transaction(
        EventKind::Truncate {
            relations: vec![first.event_table.clone(), second.event_table.clone()],
            cascade: false,
            restart_identity: false,
        },
        None,
    );
    assert!(matches!(
        plan_transaction(
            &txn,
            std::slice::from_ref(&first),
            &context(),
            |_, _| unreachable!()
        ),
        Err(ApplyError::Invalid("unknown truncate table"))
    ));
    second.schema_version += 1;
    assert!(matches!(
        plan_transaction(&txn, &[first, second], &context(), |_, _| unreachable!()),
        Err(ApplyError::Invalid("schema version mismatch"))
    ));
}

#[test]
fn malformed_canonical_values_types_and_digests_fail_closed() {
    let col = column("id", PgType::Int4, Type::INT4);
    assert!(parameter(&col, &opaque("wrong", Value::Int4(1))).is_err());
    assert!(parameter(&col, &opaque("id", Value::Int8(1))).is_err());
    assert!(
        parameter(
            &col,
            &OpaqueValue {
                type_id: "id".into(),
                bytes: Some(vec![])
            }
        )
        .is_err()
    );
    assert!(
        parameter(
            &col,
            &OpaqueValue {
                type_id: "id".into(),
                bytes: Some(Value::Null.canonical_bytes().unwrap())
            }
        )
        .is_err()
    );
    let mut txn = transaction(EventKind::Delete, None);
    txn.digest[0] ^= 1;
    assert!(matches!(
        plan_transaction(&txn, &[table()], &context(), |_, _| Ok(
            RangeState::Completed
        )),
        Err(ApplyError::InvalidTransaction)
    ));
}

#[test]
fn composite_keys_and_event_order_are_preserved() {
    let mut table = table();
    table.key.push("body".into());
    let mut txn = transaction(EventKind::Delete, None);
    txn.events[0].key.insert("body".into(), text("key"));
    let mut second = txn.events[0].clone();
    second.kind = EventKind::Insert;
    second.ordinal_in_txn = 1;
    second.after = Some(second.key.clone());
    txn.events.push(second);
    let plan = plan_transaction(&reseal(txn), &[table], &context(), |_, _| {
        Ok(RangeState::Completed)
    })
    .unwrap();
    assert_eq!(plan.statements.len(), 5);
    assert!(
        plan.statements[2]
            .sql
            .ends_with("WHERE \"id\" = $1 AND \"body\" = $2")
    );
    assert!(plan.statements[3].sql.starts_with("INSERT INTO"));
    assert!(
        plan.statements[4]
            .sql
            .starts_with("INSERT INTO _skedaddle.apply_progress")
    );
}

#[test]
fn chunk_rows_do_not_depend_on_range_lookup() {
    let mut after = row(1);
    after.insert("body".into(), text("snapshot"));
    after.insert("n".into(), opaque("n", Value::Null));
    plan_transaction(
        &transaction(EventKind::ChunkRows, Some(after)),
        &[table()],
        &context(),
        |_, _| Err(ApplyError::Invalid("must not classify chunk")),
    )
    .unwrap();
}

#[test]
fn null_and_empty_bytea_bind_distinctly() {
    let col = column("b", PgType::Bytea, Type::BYTEA);
    let empty = parameter(&col, &opaque("b", Value::Bytea(vec![]))).unwrap();
    assert_eq!(empty.bytes(), Some(b"\\x".as_slice()));
    assert_eq!(
        parameter(&col, &opaque("b", Value::Null)).unwrap().bytes(),
        None
    );
}

#[test]
fn synthetic_time_is_explicit_and_cannot_replace_missing_cdc_time() {
    let mut txn = transaction(EventKind::Watermark, None);
    txn.commit_time = None;
    let txn = reseal(txn);
    assert!(plan_transaction(&txn, &[], &context(), |_, _| Ok(RangeState::Copying)).is_err());
    let mut ctx = context();
    ctx.synthetic_commit_time = Some(1_700_000_000_000_000);
    assert!(plan_transaction(&txn, &[], &ctx, |_, _| Ok(RangeState::Copying)).is_ok());
    let mut txn = transaction(EventKind::Delete, None);
    txn.commit_time = None;
    assert!(
        plan_transaction(&reseal(txn), &[table()], &ctx, |_, _| Ok(
            RangeState::Copying
        ))
        .is_err()
    );
}

#[test]
fn invalid_keys_schema_and_identifiers_fail_closed() {
    let txn = transaction(EventKind::Delete, None);
    let mut t = table();
    t.schema.push('\0');
    assert!(plan_transaction(&txn, &[t], &context(), |_, _| Ok(RangeState::Completed)).is_err());
    for key in [
        Row::new(),
        BTreeMap::from([("id".into(), opaque("id", Value::Null))]),
    ] {
        let mut txn = txn.clone();
        txn.events[0].key = key;
        assert!(
            plan_transaction(&reseal(txn), &[table()], &context(), |_, _| Ok(
                RangeState::Completed
            ))
            .is_err()
        );
    }
    let mut t = table();
    t.schema_version = 2;
    assert!(plan_transaction(&txn, &[t], &context(), |_, _| Ok(RangeState::Completed)).is_err());
    let mut t = table();
    t.columns[0].wire_type = Type::TEXT;
    assert!(matches!(
        plan_transaction(&txn, &[t], &context(), |_, _| Ok(RangeState::Completed)),
        Err(ApplyError::Invalid("builtin wire descriptor"))
    ));
}

#[test]
fn ambiguous_commit_decision_table_requires_matching_digest() {
    let target = plan(&transaction(EventKind::Delete, None), RangeState::Copying).progress;
    let mut bad = target.clone();
    bad.digest[0] ^= 1;
    let mut earlier = target.clone();
    earlier.position = Position::new("pg/source", 97_u64.to_be_bytes());
    earlier.txn = TxnId(vec![2]);
    let mut later = target.clone();
    later.position = Position::new("pg/source", 100_u64.to_be_bytes());
    later.txn = TxnId(vec![3]);
    for n in [0, 1, 98, 99, 100, 200] {
        assert_eq!(
            resolve_commit(&target, None, n, None),
            Ok(CommitDecision::NotApplied)
        );
        assert_eq!(
            resolve_commit(&target, None, n, Some(&target)),
            Err(ApplyError::InconsistentProgress)
        );
    }
    for n in [97, 98, 99, 100, 200] {
        assert_eq!(
            resolve_commit(&target, Some(&earlier), n, None),
            Ok(CommitDecision::NotApplied)
        );
        assert_eq!(
            resolve_commit(&target, Some(&earlier), n, Some(&target)),
            Err(ApplyError::InconsistentProgress)
        );
        let mut contradictory = earlier.clone();
        contradictory.mseq = target.mseq + 1;
        assert_eq!(
            resolve_commit(&target, Some(&contradictory), n, None),
            Err(ApplyError::InconsistentProgress)
        );
    }
    for n in [99, 100, 200] {
        for receipt in [None, Some(&target)] {
            assert_eq!(
                resolve_commit(&target, Some(&target), n, receipt),
                Ok(CommitDecision::Applied)
            );
        }
        assert_eq!(
            resolve_commit(&target, Some(&bad), n, None),
            Err(ApplyError::DigestMismatch)
        );
        assert_eq!(
            resolve_commit(&target, Some(&target), n, Some(&bad)),
            Err(ApplyError::DigestMismatch)
        );
        for wrong in ["stream", "mseq", "txn"] {
            let mut latest = target.clone();
            match wrong {
                "stream" => latest.stream.push('x'),
                "mseq" => latest.mseq += 1,
                _ => latest.txn.0.push(2),
            }
            assert_eq!(
                resolve_commit(&target, Some(&latest), n, None),
                Err(ApplyError::InconsistentProgress)
            );
            assert_eq!(
                resolve_commit(&target, Some(&target), n, Some(&latest)),
                Err(ApplyError::InconsistentProgress)
            );
        }
    }
    for n in [100, 101, 200] {
        assert_eq!(
            resolve_commit(&target, Some(&later), n, None),
            Err(ApplyError::MissingDigestEvidence)
        );
        assert_eq!(
            resolve_commit(&target, Some(&later), n, Some(&target)),
            Ok(CommitDecision::Applied)
        );
        assert_eq!(
            resolve_commit(&target, Some(&later), n, Some(&bad)),
            Err(ApplyError::DigestMismatch)
        );
        let mut contradictory = later.clone();
        contradictory.mseq = target.mseq - 1;
        assert_eq!(
            resolve_commit(&target, Some(&contradictory), n, Some(&target)),
            Err(ApplyError::InconsistentProgress)
        );
    }
    for n in [0, 1, 98] {
        assert_eq!(
            resolve_commit(&target, Some(&target), n, None),
            Err(ApplyError::InconsistentProgress)
        );
    }
}

fn equal_h_pair() -> (Progress, Progress) {
    let mut h = Progress {
        stream: "wal".into(),
        mseq: 1,
        txn: TxnId(vec![1]),
        position: Position::new("pg/source", 20_u64.to_be_bytes()),
        digest: [1; 32],
        evidence: None,
    };
    h.evidence = Some(RecordEvidence::new(
        RecordKind::Transaction,
        [11; 32],
        [10; 32],
        &h,
    ));
    let mut rows = Progress {
        stream: "wal".into(),
        mseq: 2,
        txn: TxnId(vec![2]),
        position: Position::new("pg/source", 20_u64.to_be_bytes()),
        digest: [2; 32],
        evidence: None,
    };
    rows.evidence = Some(RecordEvidence::new(
        RecordKind::ChunkRows,
        [12; 32],
        [11; 32],
        &rows,
    ));
    (h, rows)
}

#[test]
fn equal_h_probe_new_chunk_rows_is_not_applied() {
    for origin in [20, 200] {
        let (h, rows) = equal_h_pair();
        assert_eq!(
            resolve_commit(&rows, Some(&h), origin, None),
            Ok(CommitDecision::NotApplied)
        );
        assert_eq!(
            resolve_commit(&rows, Some(&rows), origin, Some(&rows)),
            Ok(CommitDecision::Applied)
        );
        assert_eq!(
            resolve_commit(&rows, Some(&rows), origin, None),
            Ok(CommitDecision::Applied)
        );
        assert_eq!(
            resolve_commit(&h, Some(&h), origin, None),
            Ok(CommitDecision::Applied)
        );
    }
}

#[test]
fn equal_h_probe_transaction_replay_requires_authenticated_manifest_receipt() {
    for origin in [20, 200] {
        let (h, rows) = equal_h_pair();
        assert_eq!(
            resolve_commit(&h, Some(&rows), origin, Some(&h)),
            Ok(CommitDecision::Applied)
        );
        assert_eq!(
            resolve_commit(&h, Some(&rows), origin, None),
            Err(ApplyError::MissingDigestEvidence)
        );
        let mut legacy = h.clone();
        legacy.evidence = None;
        assert_eq!(
            resolve_commit(&h, Some(&rows), origin, Some(&legacy)),
            Err(ApplyError::MissingDigestEvidence)
        );
        let mut conflict = h.clone();
        conflict.digest[0] ^= 1;
        assert_eq!(
            resolve_commit(&h, Some(&rows), origin, Some(&conflict)),
            Err(ApplyError::DigestMismatch)
        );
        conflict = h.clone();
        conflict.txn.0.push(9);
        assert_eq!(
            resolve_commit(&h, Some(&rows), origin, Some(&conflict)),
            Err(ApplyError::InconsistentProgress)
        );
    }
}

#[test]
fn equal_h_invalid_ties_missing_predecessor_and_evidence_fail_closed() {
    for origin in [20, 200] {
        let (h, rows) = equal_h_pair();
        let mut second = rows.clone();
        second.mseq += 1;
        second.txn.0.push(3);
        second.evidence = Some(RecordEvidence::new(
            RecordKind::ChunkRows,
            [13; 32],
            [12; 32],
            &second,
        ));
        assert_eq!(
            resolve_commit(&second, Some(&rows), origin, None),
            Err(ApplyError::InconsistentProgress)
        );
        assert_eq!(
            resolve_commit(&rows, None, origin, None),
            Err(ApplyError::InconsistentProgress)
        );
        let mut earlier = h.clone();
        earlier.position = Position::new("pg/source", 19_u64.to_be_bytes());
        earlier.evidence = Some(RecordEvidence::new(
            RecordKind::Transaction,
            [11; 32],
            [10; 32],
            &earlier,
        ));
        assert_eq!(
            resolve_commit(&rows, Some(&earlier), origin - 1, None),
            Err(ApplyError::InconsistentProgress)
        );
        for invalid in 0..6 {
            let mut target = rows.clone();
            match invalid {
                0 => target.evidence = None,
                1 => target.evidence.as_mut().unwrap().kind = RecordKind::Transaction,
                2 => target.evidence.as_mut().unwrap().previous_manifest_digest[0] ^= 1,
                3 => target.mseq += 1,
                4 => target.mseq = h.mseq,
                _ => target.stream.push('x'),
            }
            assert_eq!(
                resolve_commit(&target, Some(&h), origin, None),
                Err(ApplyError::InconsistentProgress)
            );
        }
        let mut legacy = h.clone();
        legacy.evidence = None;
        assert_eq!(
            resolve_commit(&rows, Some(&legacy), origin, None),
            Err(ApplyError::InconsistentProgress)
        );
    }
}

#[test]
fn equal_h_never_ignores_existing_receipt_conflicts() {
    for origin in [20, 200] {
        let (h, rows) = equal_h_pair();
        for invalid in 0..5 {
            let mut receipt = rows.clone();
            match invalid {
                0 => receipt.digest[0] ^= 1,
                1 => receipt.txn.0.push(9),
                2 => receipt.mseq += 1,
                3 => receipt.evidence.as_mut().unwrap().kind = RecordKind::Transaction,
                _ => receipt.evidence.as_mut().unwrap().manifest_digest[0] ^= 1,
            }
            assert!(resolve_commit(&rows, Some(&h), origin, Some(&receipt)).is_err());
            assert!(resolve_commit(&rows, Some(&rows), origin, Some(&receipt)).is_err());
        }
        let mut conflicting = rows.clone();
        conflicting.digest[0] ^= 1;
        assert_eq!(
            resolve_commit(&conflicting, Some(&rows), origin, None),
            Err(ApplyError::DigestMismatch)
        );
    }
}

#[test]
fn equal_h_verified_proof_cannot_be_reused_with_changed_identity_or_digest() {
    for origin in [20, 200] {
        let (h, rows) = equal_h_pair();
        for field in 0..4 {
            let mut target = rows.clone();
            match field {
                0 => target.digest[0] ^= 1,
                1 => target.txn.0.push(3),
                2 => target.position = Position::new("pg/source", 21_u64.to_be_bytes()),
                _ => target.mseq += 1,
            }
            assert!(resolve_commit(&target, Some(&h), origin, None).is_err());
        }
    }
}

fn resolve_commit(
    target: &Progress,
    latest: Option<&Progress>,
    origin: u64,
    receipt: Option<&Progress>,
) -> Result<CommitDecision> {
    super::resolve_commit(&context().positions, target, latest, origin, receipt)
}

#[test]
fn native_pg_rejects_foreign_domain_before_planning_sql() {
    let mut txn = transaction(EventKind::Delete, None);
    txn.commit_position = Position::new("foreign", 99_u64.to_be_bytes());
    txn.events[0].position = txn.commit_position.clone();
    let txn = reseal(txn);
    assert!(matches!(
        plan_transaction(&txn, &[table()], &context(), |_, _| panic!(
            "must reject before classification or SQL"
        )),
        Err(ApplyError::Position(EventError::PositionDomainMismatch))
    ));
    let mut progress = plan(&transaction(EventKind::Delete, None), RangeState::Copying).progress;
    progress.position = txn.commit_position;
    assert_eq!(
        super::resolve_commit(&context().positions, &progress, None, 0, None),
        Err(ApplyError::Position(EventError::PositionDomainMismatch))
    );
}

// A variable-length decimal source cursor deliberately differs from byte order.
struct DecimalOrder;
impl PositionOrder for DecimalOrder {
    fn domain(&self) -> &str {
        "decimal"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        let parse = |v| {
            std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(EventError::InvalidEncoding("decimal"))
        };
        Ok(parse(a)?.cmp(&parse(b)?))
    }
}
fn opaque_positions() -> SourcePositions {
    SourcePositions::new(Arc::new(DecimalOrder), OriginPolicy::Opaque)
}
#[test]
fn opaque_planning_has_session_tag_without_xact_setup_or_timestamp_conversion() {
    for time in [None, Some(i64::MIN)] {
        let mut txn = transaction(EventKind::Delete, None);
        txn.commit_position = Position::new("decimal", b"1000");
        txn.events[0].position = txn.commit_position.clone();
        txn.commit_time = time;
        let txn = reseal(txn);
        let canonical = txn.canonical_bytes().unwrap();
        let mut context = context();
        context.positions = opaque_positions();
        let plan =
            plan_transaction(&txn, &[table()], &context, |_, _| Ok(RangeState::Copying)).unwrap();
        assert!(plan.session_setup.sql.contains("origin_session_setup"));
        assert!(
            plan.statements
                .iter()
                .all(|s| !s.sql.contains("xact_setup") && !s.sql.contains("origin_progress"))
        );
        assert!(
            plan.statements
                .last()
                .unwrap()
                .sql
                .contains("apply_progress_v3")
        );
        assert_eq!(plan.progress.position, txn.commit_position);
        assert_eq!(
            AssembledTxn::from_canonical_bytes(&canonical)
                .unwrap()
                .commit_time,
            time
        );
    }
}
#[test]
fn opaque_recovery_uses_bound_order_and_exact_receipts_without_native_high_water() {
    let positions = opaque_positions();
    let mut earlier = plan(&transaction(EventKind::Delete, None), RangeState::Copying).progress;
    earlier.position = Position::new("decimal", b"9");
    earlier.mseq = 1;
    let mut later = earlier.clone();
    later.position = Position::new("decimal", b"1000");
    later.mseq = 2;
    assert!(earlier.position.bytes() > later.position.bytes());
    for native in [0, u64::MAX] {
        assert_eq!(
            super::resolve_commit(&positions, &earlier, None, native, None),
            Ok(CommitDecision::NotApplied)
        );
        assert_eq!(
            super::resolve_commit(&positions, &later, Some(&earlier), native, None),
            Ok(CommitDecision::NotApplied)
        );
        assert_eq!(
            super::resolve_commit(&positions, &earlier, Some(&earlier), native, None),
            Ok(CommitDecision::Applied)
        );
        assert_eq!(
            super::resolve_commit(&positions, &earlier, Some(&later), native, None),
            Err(ApplyError::MissingDigestEvidence)
        );
        assert_eq!(
            super::resolve_commit(&positions, &earlier, Some(&later), native, Some(&earlier)),
            Ok(CommitDecision::Applied)
        );
        let mut conflict = earlier.clone();
        conflict.digest[0] ^= 1;
        assert_eq!(
            super::resolve_commit(&positions, &earlier, Some(&later), native, Some(&conflict)),
            Err(ApplyError::DigestMismatch)
        );
    }
    let mut equal_but_different = later.clone();
    equal_but_different.position = Position::new("decimal", b"01000");
    assert_eq!(
        super::resolve_commit(&positions, &later, Some(&equal_but_different), 0, None),
        Err(ApplyError::InconsistentProgress)
    );
    let mut wrong = earlier.clone();
    wrong.position = Position::new("wrong", b"9");
    assert_eq!(
        super::resolve_commit(&positions, &wrong, None, 0, None),
        Err(ApplyError::Position(EventError::PositionDomainMismatch))
    );
    let mut txn = transaction(EventKind::Delete, None);
    txn.commit_position = wrong.position.clone();
    txn.events[0].position = wrong.position;
    let txn = reseal(txn);
    let mut context = context();
    context.positions = positions;
    assert!(matches!(
        plan_transaction(&txn, &[table()], &context, |_, _| panic!(
            "wrong domain before classification"
        )),
        Err(ApplyError::Position(EventError::PositionDomainMismatch))
    ));
}
#[test]
fn opaque_equal_h_preserves_authenticated_pair_truth_table() {
    let positions = opaque_positions();
    let (mut h, mut rows) = equal_h_pair();
    h.position = Position::new("decimal", b"1000");
    rows.position = h.position.clone();
    for p in [&mut h, &mut rows] {
        let e = p.evidence.take().unwrap();
        p.evidence = Some(RecordEvidence::new(
            e.kind,
            e.manifest_digest,
            e.previous_manifest_digest,
            p,
        ));
    }
    assert_eq!(
        super::resolve_commit(&positions, &rows, Some(&h), 0, None),
        Ok(CommitDecision::NotApplied)
    );
    assert_eq!(
        super::resolve_commit(&positions, &h, Some(&rows), 0, Some(&h)),
        Ok(CommitDecision::Applied)
    );
    assert_eq!(
        super::resolve_commit(&positions, &h, Some(&rows), 0, None),
        Err(ApplyError::MissingDigestEvidence)
    );
    let mut conflict = rows.clone();
    conflict.digest[0] ^= 1;
    conflict.evidence = None;
    assert_eq!(
        super::resolve_commit(&positions, &rows, Some(&rows), 0, Some(&conflict)),
        Err(ApplyError::DigestMismatch)
    );
    assert_eq!(
        super::resolve_commit(&positions, &rows, None, 0, None),
        Err(ApplyError::InconsistentProgress)
    );
}
