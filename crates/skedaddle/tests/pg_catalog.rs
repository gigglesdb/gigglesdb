//! Pure catalog evidence tests plus H1-gated PostgreSQL order qualification.
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use skedaddle::{
    coordinator::{
        Materialized,
        chunk::{ChunkAttempt, ChunkCompletion, ChunkPlan, ChunkPlanRef, ChunkRowsRef},
    },
    definition::{ObjectKind, ObjectSelector},
    event::{OpaqueValue, Position, Row},
    mapping::Value,
    pg::{
        self,
        catalog::{self, NullOrder, RangeStatus, TableSelection},
        chunk::Direction,
        preflight::{Check, Observation, Snapshot},
        source::TypeResolver,
    },
    security::TlsPolicy,
};
use std::{cmp::Ordering, io::Write};
use tokio_postgres::types::{Kind, Type};

fn selection() -> TableSelection {
    TableSelection {
        schema: "public".into(),
        name: "rows".into(),
        columns: vec!["id".into(), "body".into()],
    }
}
fn object(s: &TableSelection) -> ObjectSelector {
    ObjectSelector {
        schema: s.schema.clone(),
        name: s.name.clone(),
        kind: ObjectKind::Table,
        exclude_columns: Vec::new(),
    }
}
fn type_row(name: &str, ty: &Type, typmod: i32) -> Json {
    json!({"column":name,"schema":ty.schema(),"name":ty.name(),"oid":ty.oid(),"typmod":typmod,"kind":"b","base_oid":0,"element_oid":match ty.kind(){Kind::Array(t)=>t.oid(),_=>0},"formatted":ty.name(),"not_null":false,"has_default":false,"generated":false,"identity":false})
}
fn key_column(name: &str) -> Json {
    json!({"column":name,"desc":false,"nulls_first":false,"collation_schema":null,"collation_name":null,"opclass_schema":"pg_catalog","opclass_name":"int8_ops","opclass_default":true,"opclass_input_oid":20,"opfamily_schema":"pg_catalog","opfamily_name":"integer_ops"})
}
fn snapshot(s: &TableSelection, rows: Vec<Json>, keys: Vec<Json>) -> Snapshot {
    let names: Vec<_> = keys.iter().map(|k| k["column"].clone()).collect();
    let index = json!({"replica_identity":"d","identity_index":false,"name":"rows_pkey","primary":true,"unique":true,"valid":true,"ready":true,"immediate":true,"not_null":true,"partial":false,"expression":false,"access_method":"btree","columns":names,"key_columns":keys});
    Snapshot {
        observations: vec![
            Observation {
                check: Check::ColumnTypes,
                object: Some(object(s)),
                sql: "test evidence".into(),
                rows,
                error: None,
            },
            Observation {
                check: Check::Key,
                object: Some(object(s)),
                sql: "test evidence".into(),
                rows: vec![index],
                error: None,
            },
        ],
    }
}
fn fixture() -> (TableSelection, Snapshot) {
    let s = selection();
    let snapshot = snapshot(
        &s,
        vec![
            type_row("id", &Type::INT8, -1),
            type_row("body", &Type::TEXT, -1),
        ],
        vec![key_column("id")],
    );
    (s, snapshot)
}
fn assemble(s: &TableSelection, snapshot: &Snapshot) -> catalog::TableCatalog {
    catalog::build(snapshot, snapshot, std::slice::from_ref(s), 7, true)
        .unwrap()
        .remove(0)
}
fn event_key(c: &catalog::TableCatalog, values: Vec<Value>) -> Row {
    c.key
        .columns
        .iter()
        .zip(values)
        .map(|(key, value)| {
            let column = c.columns.iter().find(|c| c.name == key.name).unwrap();
            (
                key.name.clone(),
                OpaqueValue {
                    type_id: column.source_type_id.clone(),
                    bytes: Some(value.canonical_bytes().unwrap()),
                },
            )
        })
        .collect()
}
fn planned(plan: &ChunkPlan) -> Materialized {
    let mut state = Materialized::default();
    state.chunk_plans.insert(
        plan.table.table.clone(),
        ChunkPlanRef {
            table: plan.table.table.clone(),
            plan: plan.ranges[0].id.plan.clone(),
            digest: "a".repeat(64),
            bytes: 1,
            chunks: plan.ranges.len(),
        },
    );
    state
}
#[test]
fn qualified_types_ids_typmods_and_resolver_are_consistent() {
    let (s, mut snapshot) = fixture();
    snapshot.observations[0].rows[1] = type_row("body", &Type::VARCHAR, 16);
    let c = assemble(&s, &snapshot);
    let column = &c.columns[1];
    assert_eq!(column.typmod, 16);
    assert_eq!(
        column.source_type_id,
        format!(
            "skva.v1:{:x}",
            Sha256::digest(serde_json::to_vec(&(&column.pg_type, 16i32)).unwrap())
        )
    );
    assert_eq!(c.destination.columns[1].type_id, column.source_type_id);
    assert_eq!(c.destination.columns[1].wire_type, Type::VARCHAR);
    let mut resolver = c.resolver();
    let relation = pg::pgoutput::Relation {
        id: 42,
        namespace: "public",
        name: "rows",
        replica_identity: pg::pgoutput::ReplicaIdentity::Default,
        columns: vec![],
    };
    let mut col = pg::pgoutput::RelationColumn {
        key: false,
        name: "body",
        type_oid: 1043,
        type_modifier: 16,
    };
    assert_eq!(
        resolver.resolve(&relation, &col).unwrap().pg_type,
        column.pg_type
    );
    col.type_modifier = -1;
    assert!(resolver.resolve(&relation, &col).is_err());
    col.type_modifier = 16;
    col.type_oid = 25;
    assert!(resolver.resolve(&relation, &col).is_err());
    col.type_oid = 1043;
    let unqualified = pg::pgoutput::Relation {
        namespace: "",
        ..relation
    };
    assert!(resolver.resolve(&unqualified, &col).is_err());
}
#[test]
fn every_qualified_builtin_mapping_type_assembles() {
    let types = [
        Type::BOOL,
        Type::INT2,
        Type::INT4,
        Type::INT8,
        Type::NUMERIC,
        Type::TEXT,
        Type::VARCHAR,
        Type::BPCHAR,
        Type::BYTEA,
        Type::UUID,
        Type::DATE,
        Type::TIME,
        Type::TIMETZ,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
        Type::INTERVAL,
        Type::JSON,
        Type::JSONB,
        Type::INT8_ARRAY,
        Type::TEXT_ARRAY,
    ];
    for ty in types {
        let (s, mut snapshot) = fixture();
        snapshot.observations[0].rows[1] = type_row("body", &ty, -1);
        assert_eq!(assemble(&s, &snapshot).columns[1].destination_wire, ty);
    }
}
#[test]
fn unsupported_and_unqualified_types_name_the_column_and_remediation() {
    let (s, mut snapshot) = fixture();
    for row in [
        json!({"column":"body","schema":"custom","name":"mood","oid":80000,"kind":"e","base_oid":0,"element_oid":0,"typmod":-1}),
        type_row("body", &Type::POINT, -1),
    ] {
        snapshot.observations[0].rows[1] = row;
        let err =
            catalog::build(&snapshot, &snapshot, std::slice::from_ref(&s), 0, true).unwrap_err();
        assert_eq!(err.column, "body");
        assert!(!err.sql_type.is_empty());
        assert!(!err.next_step.is_empty());
    }
    snapshot.observations[0].rows[1] = type_row("body", &Type::TEXT, -1);
    snapshot.observations[0].rows[1]["schema"] = json!("fake");
    assert!(catalog::build(&snapshot, &snapshot, &[s], 0, true).is_err());
}
#[test]
fn float_nan_payloads_need_capture_qualification() {
    let (s, mut snapshot) = fixture();
    for ty in [
        Type::FLOAT4,
        Type::FLOAT8,
        Type::FLOAT4_ARRAY,
        Type::FLOAT8_ARRAY,
    ] {
        snapshot.observations[0].rows[1] = type_row("body", &ty, -1);
        let error =
            catalog::build(&snapshot, &snapshot, std::slice::from_ref(&s), 0, true).unwrap_err();
        assert_eq!(error.column, "body");
        assert!(error.reason.contains("NaN"));
    }
}
#[test]
fn destination_is_independently_qualified_and_encoding_is_explicit() {
    let (s, snapshot) = fixture();
    let mut dest = snapshot.clone();
    dest.observations[0].rows[0]["oid"] = json!(80000);
    assert!(
        catalog::build(&snapshot, &dest, std::slice::from_ref(&s), 0, true)
            .unwrap_err()
            .reason
            .contains("qualified")
    );
    dest = snapshot.clone();
    dest.observations[0].rows[0]["typmod"] = json!(1);
    assert!(
        catalog::build(&snapshot, &dest, std::slice::from_ref(&s), 0, true)
            .unwrap_err()
            .reason
            .contains("typmod")
    );
    assert!(catalog::build(&snapshot, &snapshot, std::slice::from_ref(&s), 0, false).is_err());
    let partial = TableSelection {
        columns: vec!["id".into()],
        ..s
    };
    let mut snapshot = snapshot;
    snapshot.observations[0].rows[1]
        .as_object_mut()
        .unwrap()
        .remove("not_null");
    assert!(
        catalog::build(&snapshot, &snapshot, &[partial], 0, true)
            .unwrap_err()
            .reason
            .contains("not_null/has_default")
    );
}

#[test]
fn partial_projection_qualifies_only_selected_types_and_descriptors() {
    let (mut s, mut snapshot) = fixture();
    s.columns = vec!["id".into()];
    // Unsupported excluded type must never be qualified or decoded.
    snapshot.observations[0].rows[1] = type_row("body", &Type::POINT, -1);
    snapshot.observations[0].rows[1]["not_null"] = json!(false);
    snapshot.observations[0].rows[1]["has_default"] = json!(false);
    let c = assemble(&s, &snapshot);
    assert_eq!(c.columns.len(), 1);
    assert_eq!(c.destination.columns.len(), 1);
    assert_eq!(c.destination.key, ["id"]);
    let range = pg::chunk::plan_chunks(&c.key, 10, &[]).unwrap().remove(0);
    assert!(
        c.chunk_query(&range)
            .unwrap()
            .sql
            .starts_with("SELECT \"id\" FROM")
    );
    assert!(!c.chunk_query(&range).unwrap().sql.contains("body"));
}

#[test]
fn partial_projection_blocks_required_destination_identity_and_missing_keys() {
    let (mut s, mut snapshot) = fixture();
    s.columns = vec!["id".into()];
    let body = &mut snapshot.observations[0].rows[1];
    body["not_null"] = json!(true);
    body["has_default"] = json!(false);
    let err = catalog::build(&snapshot, &snapshot, &[s.clone()], 0, true).unwrap_err();
    assert_eq!(err.column, "body");
    assert!(err.reason.contains("NOT NULL"));
    assert!(!err.next_step.is_empty());
    snapshot.observations[0].rows[1]["has_default"] = json!(true);
    assert!(catalog::build(&snapshot, &snapshot, &[s.clone()], 0, true).is_ok());
    snapshot.observations[1].rows[0]["replica_identity"] = json!("f");
    let err = catalog::build(&snapshot, &snapshot, &[s.clone()], 0, true).unwrap_err();
    assert!(err.sql_type.contains("FULL"));
    snapshot.observations[1].rows[0]["replica_identity"] = json!("d");
    snapshot.observations[0].rows[0]["not_null"] = json!(false);
    snapshot.observations[0].rows[0]["has_default"] = json!(false);
    s.columns = vec!["body".into()];
    let err = catalog::build(&snapshot, &snapshot, &[s], 0, true).unwrap_err();
    assert_eq!(err.column, "id");
    assert_eq!(err.sql_type, "key");
    assert!(err.next_step.contains("key"));
}

#[test]
fn excluded_destination_behavior_uses_defaults_generation_and_identity() {
    let (mut s, source) = fixture();
    s.columns = vec!["id".into()];
    for supplying_field in ["has_default", "generated", "identity"] {
        let mut destination = source.clone();
        let body = &mut destination.observations[0].rows[1];
        body["not_null"] = json!(true);
        body[supplying_field] = json!(true);
        let c = catalog::build(&source, &destination, std::slice::from_ref(&s), 0, true)
            .unwrap()
            .remove(0);
        assert_eq!(c.destination.columns.len(), 1, "{supplying_field}");
        assert_eq!(c.destination.columns[0].name, "id");
    }
}

#[test]
fn column_behavior_requires_checked_boolean_evidence() {
    let (mut s, source) = fixture();
    s.columns = vec!["id".into()];
    for field in ["not_null", "has_default", "generated", "identity"] {
        for invalid in [None, Some(json!("false")), Some(json!(0)), Some(Json::Null)] {
            // Check both selected and excluded destination columns, and the
            // selected source column. Missing source behavior cannot authorize writes.
            for (source_side, row_index) in [(false, 0), (false, 1), (true, 0)] {
                let mut changed = source.clone();
                let row = changed.observations[0].rows[row_index]
                    .as_object_mut()
                    .unwrap();
                if let Some(value) = &invalid {
                    row.insert(field.into(), value.clone());
                } else {
                    row.remove(field);
                }
                let (src, dst) = if source_side {
                    (&changed, &source)
                } else {
                    (&source, &changed)
                };
                let err = catalog::build(src, dst, std::slice::from_ref(&s), 0, true).unwrap_err();
                assert!(err.reason.contains("checked"), "{field}: {err}");
                assert!(err.next_step.contains("Recollect"));
            }
        }
    }
}

#[test]
fn selected_generated_columns_block_before_apply_descriptors_exist() {
    let (s, source) = fixture();
    for source_side in [false, true] {
        let mut changed = source.clone();
        changed.observations[0].rows[1]["generated"] = json!(true);
        let (src, dst) = if source_side {
            (&changed, &source)
        } else {
            (&source, &changed)
        };
        let err = catalog::build(src, dst, std::slice::from_ref(&s), 0, true).unwrap_err();
        assert_eq!(err.column, "body");
        assert!(err.reason.contains("apply writes"));
        assert!(err.next_step.contains("Exclude"));
    }
}

#[test]
fn filtered_relation_consumes_full_tuples_without_decoding_excluded_values() {
    use pg::pgoutput::{Column, Message, OldTuple, Relation, RelationColumn, ReplicaIdentity};
    let (mut s, mut snapshot) = fixture();
    s.columns = vec!["id".into()];
    snapshot.observations[0].rows[1]["not_null"] = json!(false);
    snapshot.observations[0].rows[1]["has_default"] = json!(false);
    let c = assemble(&s, &snapshot);
    let mut mapper = pg::source::RelationMapper::new(c.resolver(), 7);
    let mut relation = Relation {
        id: 42,
        namespace: "public",
        name: "rows",
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            RelationColumn {
                key: true,
                name: "id",
                type_oid: 20,
                type_modifier: -1,
            },
            RelationColumn {
                key: false,
                name: "body",
                type_oid: 999999,
                type_modifier: -1,
            },
        ],
    };
    mapper.relation(&relation).unwrap();
    let insert = mapper
        .map(&Message::Insert {
            relation_id: 42,
            new: vec![Column::Text(b"1"), Column::Binary(b"secret sentinel")],
        })
        .unwrap()
        .remove(0);
    assert_eq!(insert.after.unwrap().keys().collect::<Vec<_>>(), ["id"]);
    let update = mapper
        .map(&Message::Update {
            relation_id: 42,
            old: Some(OldTuple::Key(vec![Column::Text(b"1"), Column::Null])),
            new: vec![Column::Text(b"2"), Column::UnchangedToast],
        })
        .unwrap()
        .remove(0);
    assert_eq!(
        Value::from_canonical_bytes(update.key["id"].bytes.as_ref().unwrap()).unwrap(),
        Value::Int8(1)
    );
    assert!(update.before.is_none());
    assert!(update.unchanged_cols.is_empty());
    assert_eq!(update.after.unwrap().len(), 1);
    let delete = mapper
        .map(&Message::Delete {
            relation_id: 42,
            old: OldTuple::Full(vec![Column::Text(b"2"), Column::Text(&[0xff])]),
        })
        .unwrap()
        .remove(0);
    assert_eq!(delete.before.unwrap().len(), 1);
    assert_eq!(delete.key.len(), 1);
    // FULL requires all columns even if a malformed descriptor omits key flags.
    relation.replica_identity = ReplicaIdentity::Full;
    assert!(mapper.relation(&relation).is_err());
    assert!(
        mapper
            .map(&Message::Insert {
                relation_id: 42,
                new: vec![]
            })
            .is_err()
    );
    relation.replica_identity = ReplicaIdentity::Default;
    relation.columns[1].key = true;
    assert!(mapper.relation(&relation).is_err());
    relation.columns[1].key = false;
    relation.columns.remove(0);
    assert!(mapper.relation(&relation).is_err());
    relation.namespace = "other";
    assert!(mapper.relation(&relation).is_err());
}
#[test]
fn non_btree_nondefault_and_nullable_keys_block() {
    let (s, snapshot) = fixture();
    for (field, value) in [("access_method", json!("hash")), ("not_null", json!(false))] {
        let mut bad = snapshot.clone();
        bad.observations[1].rows[0][field] = value;
        assert!(catalog::build(&bad, &bad, std::slice::from_ref(&s), 0, true).is_err());
    }
    let mut bad = snapshot.clone();
    bad.observations[1].rows[0]["key_columns"][0]["opclass_default"] = json!(false);
    assert!(
        catalog::build(&bad, &bad, std::slice::from_ref(&s), 0, true)
            .unwrap_err()
            .reason
            .contains("operator class")
    );
    let mut bad = snapshot.clone();
    bad.observations[1].rows[0]["key_columns"][0]["opclass_input_oid"] = json!(25);
    assert!(
        catalog::build(&bad, &bad, &[s], 0, true)
            .unwrap_err()
            .reason
            .contains("operator class")
    );
}
#[test]
fn selected_key_must_be_available_in_replica_identity_old_tuples() {
    let (s, mut snapshot) = fixture();
    let primary = snapshot.observations[1].rows[0].clone();
    let mut identity = primary.clone();
    identity["primary"] = json!(false);
    identity["identity_index"] = json!(true);
    identity["name"] = json!("rows_identity");
    identity["replica_identity"] = json!("i");
    identity["key_columns"][0]["desc"] = json!(true);
    snapshot.observations[1].rows[0]["replica_identity"] = json!("i");
    snapshot.observations[1].rows.push(identity);
    assert_eq!(
        assemble(&s, &snapshot).key.columns[0].direction,
        Direction::Desc
    );
    snapshot.observations[1].rows.pop();
    assert!(catalog::build(&snapshot, &snapshot, std::slice::from_ref(&s), 0, true).is_err());
    snapshot.observations[1].rows[0]["replica_identity"] = json!("n");
    assert!(catalog::build(&snapshot, &snapshot, &[s], 0, true).is_err());
}
#[test]
fn desc_nulls_first_metadata_and_composite_boundaries_follow_typed_order() {
    let s = selection();
    let mut id = key_column("id");
    id["desc"] = json!(true);
    id["nulls_first"] = json!(true);
    let mut uuid = key_column("body");
    uuid["opclass_input_oid"] = json!(2950);
    uuid["opclass_name"] = json!("uuid_ops");
    uuid["opfamily_name"] = json!("uuid_ops");
    let snapshot = snapshot(
        &s,
        vec![
            type_row("id", &Type::INT8, -1),
            type_row("body", &Type::UUID, -1),
        ],
        vec![id, uuid],
    );
    let c = assemble(&s, &snapshot);
    assert_eq!(c.index_columns[0].null_order, NullOrder::First);
    assert_eq!(c.key.columns[0].direction, Direction::Desc);
    let sample = vec![Value::Int8(0), Value::Uuid([128; 16])];
    let plan = ChunkPlan::new(c.key.clone(), 10, vec![sample.clone()]).unwrap();
    let classifier = c.range_classifier(&plan, "pg:test").unwrap();
    assert_eq!(
        classifier
            .compare_keys(&[Value::Int8(1), Value::Uuid([255; 16])], &sample)
            .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        classifier
            .compare_keys(&[Value::Int8(0), Value::Uuid([0; 16])], &sample)
            .unwrap(),
        Ordering::Less
    );
    assert!(
        classifier
            .compare_keys(&[Value::Null, Value::Uuid([0; 16])], &sample)
            .is_err()
    );
    // The first field decides order, but an invalid suffix must still fail.
    assert!(
        classifier
            .compare_keys(&[Value::Int8(1), Value::Null], &sample)
            .is_err()
    );
}
#[test]
fn collation_is_carried_but_text_classifier_requires_server_followup() {
    let s = selection();
    let mut text = key_column("body");
    text["opclass_input_oid"] = json!(25);
    text["collation_schema"] = json!("pg_catalog");
    text["collation_name"] = json!("C");
    text["opclass_name"] = json!("text_ops");
    text["opfamily_name"] = json!("text_ops");
    let snapshot = snapshot(
        &s,
        vec![
            type_row("id", &Type::INT8, -1),
            type_row("body", &Type::TEXT, -1),
        ],
        vec![key_column("id"), text],
    );
    let c = assemble(&s, &snapshot);
    assert_eq!(c.key.columns[1].collation.as_ref().unwrap().name, "C");
    let plan = ChunkPlan::new(c.key.clone(), 10, vec![]).unwrap();
    let err = c.range_classifier(&plan, "pg:test").unwrap_err();
    assert_eq!(err.column, "body");
    assert!(err.next_step.contains("server-side"));
}
#[test]
fn durable_done_does_not_complete_transactions_at_or_before_high() {
    let (s, snapshot) = fixture();
    let c = assemble(&s, &snapshot);
    let plan = ChunkPlan::new(c.key.clone(), 10, vec![vec![Value::Int8(0)]]).unwrap();
    let classifier = c.range_classifier(&plan, "pg:test").unwrap();
    let mut state = planned(&plan);
    let key = event_key(&c, vec![Value::Int8(-2)]);
    let position = |lsn: u64| Position::new("pg:test", lsn.to_be_bytes());
    assert_eq!(
        classifier.classify(&key, &position(101), &state).unwrap(),
        RangeStatus::NotYet
    );
    let range = plan.ranges[0].clone();
    let id = serde_json::to_string(&range.id).unwrap();
    state.chunk_attempts.insert(
        id.clone(),
        ChunkAttempt {
            chunk: range,
            attempt: 1,
            done: None,
        },
    );
    assert_eq!(
        classifier.classify(&key, &position(101), &state).unwrap(),
        RangeStatus::InWindow
    );
    state.chunk_attempts.get_mut(&id).unwrap().done = Some(ChunkCompletion {
        high_lsn: 100,
        chunk_rows: ChunkRowsRef {
            mseq: 1,
            manifest_digest: Sha256::digest(b"catalog test").into(),
        },
    });
    for lsn in [99, 100] {
        assert_eq!(
            classifier.classify(&key, &position(lsn), &state).unwrap(),
            RangeStatus::InWindow
        );
    }
    assert_eq!(
        classifier.classify(&key, &position(101), &state).unwrap(),
        RangeStatus::Done
    );
    assert!(
        classifier
            .classify(&key, &Position::new("other", 101u64.to_be_bytes()), &state)
            .is_err()
    );
    let upper = event_key(&c, vec![Value::Int8(0)]);
    assert_eq!(
        classifier.classify(&upper, &position(101), &state).unwrap(),
        RangeStatus::NotYet
    );
}

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
#[tokio::test]
async fn live_catalog_decoder_and_order_qualification() -> TestResult {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        let _ = writeln!(
            std::io::stderr().lock(),
            "SKIP pg_catalog: SKEDADDLE_TEST_CONFIG absent; PostgreSQL qualification NOT RUN; run cargo test --test pg_catalog with an active harness config"
        );
        return Ok(());
    };
    let cfg: Json = serde_json::from_slice(&std::fs::read(path)?)?;
    let dsn = cfg["pg_ddl"].as_str().ok_or("H1 pg_ddl missing")?;
    let ca = cfg["ca_pem"].as_str().ok_or("H1 ca_pem missing")?;
    let (mut client, driver, evidence) = pg::tls::connect(
        &pg::tls::Config::new(dsn.parse()?, ca),
        &TlsPolicy::Required,
    )
    .await?;
    assert!(evidence.encrypted());
    tokio::spawn(driver);
    client
        .batch_execute(pg::source::REQUIRED_SETTINGS_SQL)
        .await?;
    let encoding: String = client.query_one("SHOW client_encoding", &[]).await?.get(0);
    assert_eq!(encoding, "UTF8");
    let encoding: String = client.query_one("SHOW server_encoding", &[]).await?.get(0);
    assert_eq!(encoding, "UTF8");
    let schema = format!("k1_{}", uuid::Uuid::new_v4().simple());
    let tx = client.transaction().await?;
    tx.batch_execute(&format!("CREATE SCHEMA {schema};CREATE TABLE {schema}.rows(id int8 NOT NULL, body text COLLATE \"C\" NOT NULL, u uuid NOT NULL);CREATE UNIQUE INDEX rows_key ON {schema}.rows(id DESC NULLS FIRST,body,u);ALTER TABLE {schema}.rows REPLICA IDENTITY USING INDEX rows_key;INSERT INTO {schema}.rows SELECT i,b,u FROM unnest(ARRAY['-9223372036854775808'::int8,-1,0,1,9223372036854775807]) i CROSS JOIN unnest(ARRAY['','A','a','é']) b CROSS JOIN unnest(ARRAY['00000000-0000-0000-0000-000000000000'::uuid,'ffffffff-ffff-ffff-ffff-ffffffffffff']) u")).await?;
    let s = TableSelection {
        schema: schema.clone(),
        name: "rows".into(),
        columns: vec!["id".into(), "body".into(), "u".into()],
    };
    let snap = pg::preflight::collect_in(&tx, &[object(&s)]).await?;
    let c = assemble(&s, &snap);
    assert_eq!(c.index_columns[0].null_order, NullOrder::First);
    assert_eq!(c.key.columns[1].collation.as_ref().unwrap().name, "C");
    let plan = ChunkPlan::new(c.key.clone(), 10, vec![])?;
    assert!(
        c.range_classifier(&plan, "pg:live")
            .unwrap_err()
            .next_step
            .contains("server-side")
    );
    let mut decoder = c.decoder();
    for row in tx
        .query(
            &format!("SELECT * FROM {schema}.rows ORDER BY id DESC,body COLLATE \"C\",u"),
            &[],
        )
        .await?
    {
        assert_eq!(decoder(row)?.len(), 3);
    }
    // NULL keys remain an explicit blocked contract. PostgreSQL itself proves
    // nullable NULL/empty/case/accent ordering; this item never approximates it.
    tx.batch_execute(&format!("CREATE TABLE {schema}.nullable(id int8, body text COLLATE \"C\",u uuid); CREATE UNIQUE INDEX nullable_key ON {schema}.nullable(id DESC NULLS FIRST,body,u); INSERT INTO {schema}.nullable VALUES(NULL,NULL,NULL),(0,'',NULL),(0,'A',NULL),(0,'a',NULL),(0,'é',NULL)")).await?;
    let n = TableSelection {
        name: "nullable".into(),
        ..s.clone()
    };
    let ns = pg::preflight::collect_in(&tx, &[object(&n)]).await?;
    assert!(
        catalog::build(&ns, &ns, &[n], 0, true)
            .unwrap_err()
            .reason
            .contains("NULL")
    );
    let ordered=tx.query(&format!("SELECT id,body FROM {schema}.nullable ORDER BY id DESC NULLS FIRST,body COLLATE \"C\" NULLS FIRST"),&[]).await?;
    assert!(ordered[0].get::<_, Option<i64>>(0).is_none());
    let labels: Vec<String> = ordered.iter().skip(1).map(|r| r.get(1)).collect();
    assert_eq!(labels, vec!["", "A", "a", "é"]);
    // A collation-free composite subset is qualified against real ORDER BY.
    tx.batch_execute(&format!("CREATE TABLE {schema}.typed(id int8 NOT NULL,u uuid NOT NULL);CREATE UNIQUE INDEX typed_key ON {schema}.typed(id DESC NULLS FIRST,u ASC);ALTER TABLE {schema}.typed REPLICA IDENTITY USING INDEX typed_key;INSERT INTO {schema}.typed SELECT DISTINCT id,u FROM {schema}.rows")).await?;
    let typed_selection = TableSelection {
        schema: schema.clone(),
        name: "typed".into(),
        columns: vec!["id".into(), "u".into()],
    };
    let typed_snapshot = pg::preflight::collect_in(&tx, &[object(&typed_selection)]).await?;
    let typed = assemble(&typed_selection, &typed_snapshot);
    let rows = tx
        .query(
            &format!("SELECT id,u FROM {schema}.typed ORDER BY id DESC,u ASC"),
            &[],
        )
        .await?;
    let mut decode = typed.decoder();
    let mut keys = Vec::new();
    for row in rows {
        let row = decode(row)?;
        keys.push(
            typed
                .key
                .columns
                .iter()
                .map(|c| {
                    Value::from_canonical_bytes(row[&c.name].bytes.as_deref().unwrap()).unwrap()
                })
                .collect::<Vec<_>>(),
        );
    }
    let plan = ChunkPlan::new(typed.key.clone(), 1, keys.iter().skip(1).cloned().collect())?;
    let classifier = typed.range_classifier(&plan, "pg:live")?;
    for pair in keys.windows(2) {
        assert_eq!(classifier.compare_keys(&pair[0], &pair[1])?, Ordering::Less);
    }
    for key in keys {
        let row = event_key(&typed, key);
        assert_eq!(
            classifier.classify(
                &row,
                &Position::new("pg:live", 1u64.to_be_bytes()),
                &planned(&plan)
            )?,
            RangeStatus::NotYet
        );
    }
    // Exercise a non-C collation when installed; classifier still blocks.
    if let Some(row)=tx.query_opt("SELECT n.nspname,c.collname FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE c.collname NOT IN ('C','POSIX','default') AND c.collencoding IN (-1,pg_char_to_encoding('UTF8')) ORDER BY c.oid LIMIT 1",&[]).await? {
        let quote=|s:String|format!("\"{}\"",s.replace('"',"\"\""));
        let coll=format!("{}.{}",quote(row.get(0)),quote(row.get(1)));
        tx.batch_execute(&format!("CREATE TABLE {schema}.localized(body text COLLATE {coll} PRIMARY KEY)")).await?;
        let localized=TableSelection{schema:schema.clone(),name:"localized".into(),columns:vec!["body".into()]};
        let snap=pg::preflight::collect_in(&tx,&[object(&localized)]).await?;
        let c=assemble(&localized,&snap);let p=ChunkPlan::new(c.key.clone(),1,vec![])?;
        assert!(c.range_classifier(&p,"pg:live").is_err());
        tx.query(&format!("SELECT v FROM (VALUES(NULL::text),(''),('A'),('a'),('é')) t(v) ORDER BY v COLLATE {coll} NULLS FIRST"),&[]).await?;
    } else {let _ = writeln!(std::io::stderr().lock(), "SKIP pg_catalog non-C collation qualification: none installed");}
    tx.rollback().await?;
    Ok(())
}

/// This gate uses real endpoints, a separate owned read-only planner connection,
/// and the same chunk predicates as copy reads. No DB behavior is mocked.
#[tokio::test]
async fn live_server_planner_range_and_transaction_qualification() -> TestResult {
    use skedaddle::{
        engine::PgTransactionPlanner,
        event::{AssembledTxn, Event, EventKind, SourceIdentity, TxnId},
        pg::catalog::{ServerPlanner, ServerTable},
    };
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        let _ = writeln!(
            std::io::stderr().lock(),
            "SKIP pg_catalog server planner: SKEDADDLE_TEST_CONFIG absent; PostgreSQL source/destination ordering and transaction qualification NOT RUN; harness/run.ps1 -Phase pg-matrix -PgVersion 18"
        );
        return Ok(());
    };
    let cfg: Json = serde_json::from_slice(&std::fs::read(path)?)?;
    let ca = cfg["ca_pem"].as_str().ok_or("H1 ca_pem missing")?;
    async fn connect(
        dsn: &str,
        ca: &str,
    ) -> Result<tokio_postgres::Client, Box<dyn std::error::Error + Send + Sync>> {
        let (client, driver, evidence) = pg::tls::connect(
            &pg::tls::Config::new(dsn.parse()?, ca),
            &TlsPolicy::Required,
        )
        .await?;
        assert!(evidence.encrypted());
        tokio::spawn(driver);
        Ok(client)
    }
    let source_dsn = cfg["pg_ddl"].as_str().ok_or("H1 pg_ddl missing")?;
    let mut source = connect(source_dsn, ca).await?;
    let mut destination =
        connect(cfg["pg_apply"].as_str().ok_or("H1 pg_apply missing")?, ca).await?;
    source
        .batch_execute(pg::source::REQUIRED_SETTINGS_SQL)
        .await?;
    destination
        .batch_execute(pg::source::REQUIRED_SETTINGS_SQL)
        .await?;
    let schema = format!("k2b_{}", uuid::Uuid::new_v4().simple());
    source
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let result: TestResult = async {
        destination.batch_execute(&format!("CREATE SCHEMA {schema}")).await?;
        let mut definitions = vec![
            ("mixed".to_string(),"a varchar(30) COLLATE \"C\" NOT NULL,b numeric NOT NULL".to_string(),"(''::varchar,0),('A',1.0),('A',2),('a',1),('e',1),('\u{00e9}',1)".to_string(),vec!["a".to_string(),"b".to_string()],"a COLLATE \"C\" ASC,b DESC".to_string()),
            ("blank".into(),"a char(8) COLLATE \"C\" PRIMARY KEY".into(),"(''),('A'),('a'),('e'),('\u{00e9}')".into(),vec!["a".into()],"a COLLATE \"C\"".into()),
            ("numbers".into(),"a numeric PRIMARY KEY".into(),"(0),(1.0),(2)".into(),vec!["a".into()],"a".into()),
            ("intervals".into(),"a interval PRIMARY KEY".into(),"('0 days'),('1 mon'),('31 days')".into(),vec!["a".into()],"a".into()),
            ("zones".into(),"a timetz PRIMARY KEY".into(),"('00:00+00'),('01:00+01'),('01:00+00')".into(),vec!["a".into()],"a".into()),
        ];
        let candidates=source.query("SELECT n.nspname,c.collname FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE c.collprovider IN ('i','c') AND c.collname NOT IN ('C','POSIX','default') AND c.collisdeterministic AND c.collversion IS NOT NULL AND c.collversion=pg_catalog.pg_collation_actual_version(c.oid) AND c.collencoding IN (-1,6) ORDER BY c.oid",&[]).await?;
        let mut localized=false;
        for row in candidates {
            let namespace:String=row.get(0);let name:String=row.get(1);
            if destination.query_opt("SELECT 1 FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE n.nspname=$1 AND c.collname=$2",&[&namespace,&name]).await?.is_some() {
                let q=pg::chunk::QualifiedName{schema:namespace,name}.sql()?;
                definitions.push(("localized".into(),format!("a text COLLATE {q} PRIMARY KEY"),"(''),('A'),('a'),('e'),('\u{00e9}')".into(),vec!["a".into()],format!("a COLLATE {q}")));
                localized=true;break;
            }
        }
        if !localized { let _=writeln!(std::io::stderr().lock(),"SKIP pg_catalog server non-C ordering: no common versioned deterministic libc/ICU collation installed"); }
        let mut catalogs=Vec::new();let mut plans=Vec::new();let mut ordered=Vec::new();
        for (name,ddl,values,columns,order) in &definitions {
            let mut ddl=format!("CREATE TABLE {schema}.{name}({ddl})");
            if name == "mixed" { ddl.push_str(&format!("; CREATE UNIQUE INDEX mixed_key ON {schema}.mixed(a ASC,b DESC); ALTER TABLE {schema}.mixed REPLICA IDENTITY USING INDEX mixed_key")); }
            source.batch_execute(&ddl).await?;destination.batch_execute(&ddl).await?;
            source.batch_execute(&format!("INSERT INTO {schema}.{name} VALUES {values}")).await?;
            let s=TableSelection{schema:schema.clone(),name:name.clone(),columns:columns.clone()};
            let src=pg::preflight::collect(&mut source,&[object(&s)]).await?;
            let dst=pg::preflight::collect(&mut destination,&[object(&s)]).await?;
            let c=catalog::build(&src,&dst,&[s],7,true)?.remove(0);
            let rows=source.query(&format!("SELECT * FROM {schema}.{name} ORDER BY {order}"),&[]).await?.into_iter().map(c.decoder()).collect::<pg::chunk::Result<Vec<_>>>()?;
            let keys=rows.iter().map(|row|c.key.columns.iter().map(|k|Value::from_canonical_bytes(row[&k.name].bytes.as_deref().unwrap()).unwrap()).collect::<Vec<_>>()).collect::<Vec<_>>();
            let plan=ChunkPlan::new(c.key.clone(),1,keys.iter().skip(1).cloned().collect())?;
            let mut observed=Vec::new();
            for range in &plan.ranges { observed.extend(c.read_chunk(&mut source,range).await?); }
            assert_eq!(observed,rows,"chunk range predicates must partition actual ORDER BY exactly");
            catalogs.push(c);plans.push(plan);ordered.push(rows);
        }
        let tables=catalogs.iter().zip(&plans).map(|(c,p)|ServerTable::new(c,p,"pg:k2b")).collect::<Result<Vec<_>,_>>()?;
        let context=pg::apply::PlanContext{stream:"k2b".into(),mseq:0,origin:"k2b_origin".into(),synthetic_commit_time:None};
        // Deliberately use batches of two to exercise multiple queries.
        let mut planner=ServerPlanner::new(connect(source_dsn,ca).await?,&mut destination,tables,context,2).await?;
        assert!(planner.collation_evidence().iter().any(|e|e.name.name=="C" && e.source.fixed_byte_order && e.source==e.destination));
        if localized {assert!(planner.collation_evidence().iter().any(|e|!e.source.fixed_byte_order && e.source.actual_version.is_some()));}
        let mut state=Materialized::default();
        for plan in &plans { state.chunk_plans.extend(planned(plan).chunk_plans); }
        let pos=|lsn:u64|Position::new("pg:k2b",lsn.to_be_bytes());
        for ((c,plan),rows) in catalogs.iter().zip(&plans).zip(&ordered) {
            assert_eq!(planner.classify_keys(&c.key.table,rows,&pos(101),&state).await?,vec![RangeStatus::NotYet;rows.len()]);
            for range in &plan.ranges {
                let mut partial = planned(plan);
                partial.chunk_attempts.insert(serde_json::to_string(&range.id)?, ChunkAttempt {
                    chunk: range.clone(), attempt: 1, done: Some(ChunkCompletion {
                        high_lsn:100, chunk_rows:ChunkRowsRef { mseq:1, manifest_digest:Sha256::digest(b"server membership proof").into() }
                    })
                });
                let members=c.read_chunk(&mut source,range).await?;
                let expected=rows.iter().map(|r|if members.contains(r){RangeStatus::Done}else{RangeStatus::NotYet}).collect::<Vec<_>>();
                assert_eq!(planner.classify_keys(&c.key.table,rows,&pos(101),&partial).await?,expected,"server membership must match this exact chunk predicate");
                state.chunk_attempts.insert(serde_json::to_string(&range.id)?,ChunkAttempt{chunk:range.clone(),attempt:1,done:Some(ChunkCompletion{high_lsn:100,chunk_rows:ChunkRowsRef{mseq:1,manifest_digest:Sha256::digest(b"server planner").into()}})});
            }
            for lsn in [99,100,101] {
                assert_eq!(planner.classify_keys(&c.key.table,rows,&pos(lsn),&state).await?,vec![if lsn>100{RangeStatus::Done}else{RangeStatus::InWindow};rows.len()]);
            }
        }
        // Values with different canonical encodings but PostgreSQL-equal order
        // must remain in the exact same saved half-open range.
        for (table,left,right) in [("numbers","1.0","1.00"),("blank","A","A   "),("intervals","1 mon","30 days")] {
            let i=catalogs.iter().position(|c|c.selection.name==table).unwrap();let c=&catalogs[i];
            let values=[left,right].into_iter().map(|s|event_key(c,vec![Value::parse_pg_text(&c.key.columns[0].pg_type,Some(s.as_bytes())).unwrap()])).collect::<Vec<_>>();
            // Only the range containing left is completed, so equality cannot
            // pass merely because all ranges have the same classification.
            let mut partial=planned(&plans[i]);
            for range in &plans[i].ranges {
                let rows=c.read_chunk(&mut source,range).await?;
                let equal = if rows.is_empty() { false } else {
                    let q=c.chunk_query(range)?;
                    let mut params=q.parameters.clone();params.push(left.to_string());
                    let ty=c.key.columns[0].sql_type.sql()?;
                    let sql=format!("SELECT EXISTS(SELECT 1 FROM ({}) AS sample WHERE \"a\" = ${}::text::{ty})",q.sql,params.len());
                    let binds=params.iter().map(|p|p as &(dyn tokio_postgres::types::ToSql+Sync)).collect::<Vec<_>>();
                    source.query_one(&sql,&binds).await?.get::<_,bool>(0)
                };
                if equal { let id=serde_json::to_string(&range.id)?;partial.chunk_attempts.insert(id.clone(),state.chunk_attempts[&id].clone()); }
            }
            let statuses=planner.classify_keys(&c.key.table,&values,&pos(101),&partial).await?;
            assert_eq!(statuses,vec![RangeStatus::Done;2],"{table} PostgreSQL equality must share membership");
        }
        // Invoke the actual borrowing async planner with real old/new keys.
        let c=&catalogs[0];let rows=&ordered[0];
        let identity=SourceIdentity{engine:"pg".into(),system_id:"k2b".into(),lineage:"k2b".into(),db_name:"test".into(),slot:"test".into()};
        let txnid=TxnId(b"k2b".to_vec());
        for lsn in [100,101] {
            let event=Event{source:identity.clone(),txn:txnid.clone(),ordinal_in_txn:0,position:pos(lsn),schema_version:7,kind:EventKind::Update,table:Some(c.key.table.clone()),key:rows[0].clone(),before:Some(rows[0].clone()),after:Some(rows[1].clone()),unchanged_cols:Default::default()};
            let txn=AssembledTxn::new(identity.clone(),txnid.clone(),pos(lsn),Some(1_700_000_000_000_000),vec![event])?;
            let plan=PgTransactionPlanner::plan(&mut planner,&txn,9,&state).await?;
            assert_eq!(plan.progress.mseq,9);
            assert_eq!(plan.statements.iter().any(|s|s.if_previous_missing),lsn==100,"equal-H stays copying; post-H is completed");
        }
        // Invalid durable bounds remain semantic Apply failures through async.
        let mut wrong=state.clone();wrong.chunk_plans.get_mut(&c.key.table).unwrap().chunks+=1;
        assert!(matches!(planner.classify_keys(&c.key.table,&rows[..1],&pos(101),&wrong).await,Err(skedaddle::engine::PgPlannerError::Apply(_))));
        Ok(())
    }.await;
    // The test owns these schemas; cleanup runs even after a qualification error.
    let cleanup_source = source
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    let cleanup_destination = destination
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    result?;
    cleanup_source?;
    cleanup_destination?;
    Ok(())
}
