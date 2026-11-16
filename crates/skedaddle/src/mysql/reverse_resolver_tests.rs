use super::*;
use crate::{
    mapping::mysql::plan_column,
    mysql::schema::{Collation, ColumnDescriptor},
    pg::{
        pgoutput::{Column, Message, OldTuple, ReplicaIdentity},
        reverse::{Eligibility, MappingFailure, ReverseResolver, ValidatedMapping, Violation},
        source::RelationMapper,
    },
};

fn plan(ty: &str, name: &str) -> MysqlPgColumn {
    let data_type = ty.split(['(', ' ']).next().unwrap();
    let text = matches!(data_type, "text" | "enum" | "varchar");
    plan_column(&ColumnDescriptor {
        name: name.into(),
        ordinal: 1,
        data_type: data_type.into(),
        column_type: ty.into(),
        character_set: text.then(|| "utf8mb4".into()),
        collation: text.then(|| Collation {
            name: "utf8mb4_bin".into(),
            id: 46,
        }),
        nullable: true,
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    })
    .unwrap()
}
fn relation(plans: &[MysqlPgColumn]) -> Relation<'_> {
    Relation {
        id: 1,
        namespace: "PG\"Schema",
        name: "PG.Table",
        replica_identity: ReplicaIdentity::Default,
        columns: plans
            .iter()
            .enumerate()
            .map(|(i, p)| RelationColumn {
                name: &p.source.name,
                key: i == 0,
                type_oid: oid(&p.pg_type).unwrap(),
                type_modifier: p.type_modifier,
            })
            .collect(),
    }
}
fn resolver(plans: Vec<MysqlPgColumn>, eligibility: Eligibility) -> ReverseResolver<Resolver> {
    ReverseResolver::with_mapping(
        Resolver::new(BTreeMap::from([(
            "\"PG\"\"Schema\".\"PG.Table\"".into(),
            plans,
        )]))
        .unwrap(),
        eligibility,
        ValidatedMapping::boxed,
    )
}
fn check_value(ty: &str, text: Option<&[u8]>, valid: bool) {
    let p = plan(ty, "v");
    let parsed = Value::parse_pg_text(&p.pg_type, text).unwrap();
    let plans = [p];
    let r = relation(&plans);
    let latch = Eligibility::new(|_| Ok(()));
    let mapping = resolver(plans.to_vec(), latch.clone())
        .resolve(&r, &r.columns[0])
        .unwrap();
    if valid {
        assert_eq!(mapping.mapping.forward(&parsed).unwrap(), parsed);
        assert!(latch.eligible());
    } else {
        assert!(mapping.mapping.forward(&parsed).is_err());
        assert!(!latch.eligible());
        assert!(mapping.mapping.forward(&Value::Null).is_err());
    }
}
#[test]
fn signed_boundaries() {
    for n in ["-2147483648", "2147483647"] {
        check_value("int", Some(n.as_bytes()), true);
    }
    // INT UNSIGNED maps to PG int8: values above the MySQL range still parse.
    check_value("int unsigned", Some(b"4294967296"), false);
    check_value("tinyint", Some(b"128"), false);
    check_value("tinyint", Some(b"-129"), false);
}
#[test]
fn unsigned_boundaries() {
    for n in ["0", "4294967295"] {
        check_value("int unsigned", Some(n.as_bytes()), true);
    }
    check_value("int unsigned", Some(b"-1"), false);
    check_value("bigint unsigned", Some(b"18446744073709551615"), true);
    check_value("bigint unsigned", Some(b"18446744073709551616"), false);
}
#[test]
fn decimal_precision() {
    check_value("decimal(5,2)", Some(b"999.99"), true);
    check_value("decimal(5,2)", Some(b"1000.00"), false);
    check_value("decimal(5,2)", Some(b"1.001"), false);
}
#[test]
fn time_precision() {
    check_value("time(3)", Some(b"00:00:01.123"), true);
    check_value("time(3)", Some(b"00:00:01.123456"), false);
    check_value("datetime(3)", Some(b"2026-01-01 00:00:01.123"), true);
    check_value("datetime(3)", Some(b"2026-01-01 00:00:01.123456"), false);
}
#[test]
fn null_and_text() {
    check_value("text", None, true);
    check_value("text", Some("café".as_bytes()), true);
    check_value("varchar(3)", Some(b"four"), false);
    let mut p = plan("text", "v");
    p.source.nullable = false;
    assert!(InverseDomain(p).forward(&Value::Null).is_err());
}
#[test]
fn enum_label_and_permanent_journal_failure() {
    check_value("enum('one','two')", Some(b"one"), true);
    let p = plan("enum('one','two')", "v");
    let latch = Eligibility::new(|_| Err("offline".into()));
    let mapping = ValidatedMapping::boxed(Box::new(InverseDomain(p.clone())), latch.clone());
    let bad = Value::parse_pg_text(&p.pg_type, Some(b"other")).unwrap();
    assert!(matches!(
        mapping.forward(&bad),
        Err(MappingError::Unverified(_))
    ));
    assert_eq!(
        latch.first_violation(),
        Some(Violation::Value {
            reason: MappingFailure::OutOfDomain
        })
    );
    assert!(!latch.eligible());
    assert!(mapping.forward(&Value::Null).is_err());
}
#[test]
fn exact_type_and_typmod() {
    let plans = [plan("int", "id"), plan("varchar(3)", "v")];
    let mut r = relation(&plans);
    let latch = Eligibility::new(|_| Ok(()));
    let mut resolver = resolver(plans.to_vec(), latch.clone());
    r.columns[0].type_oid = 20;
    assert!(resolver.resolve(&r, &r.columns[0]).is_err());
    assert!(!latch.eligible());
    let mut r = relation(&plans);
    r.columns[1].type_modifier += 1;
    assert!(
        super::Resolver::new(BTreeMap::from([(
            "\"PG\"\"Schema\".\"PG.Table\"".into(),
            plans.to_vec()
        )]))
        .unwrap()
        .resolve(&r, &r.columns[1])
        .is_err()
    );
}
#[test]
fn canonical_identity_pk_change_and_unchanged_toast() {
    let plans = [plan("int unsigned", "id"), plan("text", "v")];
    let r = relation(&plans);
    let mut mapper = RelationMapper::new(resolver(plans.to_vec(), Eligibility::new(|_| Ok(()))), 1);
    mapper.relation(&r).unwrap();
    let events = mapper
        .map(&Message::Update {
            relation_id: 1,
            old: Some(OldTuple::Key(vec![Column::Text(b"1"), Column::Null])),
            new: vec![Column::Text(b"2"), Column::UnchangedToast],
        })
        .unwrap();
    let event = &events[0];
    assert_eq!(
        event.table.as_deref(),
        Some("\"PG\"\"Schema\".\"PG.Table\"")
    );
    assert!(event.before.is_none());
    assert_eq!(event.key["id"].type_id, plans[0].type_id);
    assert_eq!(
        Value::from_canonical_bytes(event.key["id"].bytes.as_ref().unwrap()).unwrap(),
        Value::Int8(1)
    );
    let after = event.after.as_ref().unwrap();
    assert_eq!(after["id"].type_id, plans[0].type_id);
    assert_eq!(
        Value::from_canonical_bytes(after["id"].bytes.as_ref().unwrap()).unwrap(),
        Value::Int8(2)
    );
    assert!(!after.contains_key("v"));
    assert_eq!(event.unchanged_cols, BTreeSet::from(["v".into()]));
}
#[test]
fn generated_projection_and_unknown_alias() {
    let mut plans = [plan("int", "id"), plan("text", "computed")];
    plans[1].generated = true;
    let mut r = relation(&plans);
    let mut resolver = resolver(plans.to_vec(), Eligibility::new(|_| Ok(())));
    assert_eq!(
        resolver.included_columns(&r).unwrap(),
        Some(BTreeSet::from(["id".into()]))
    );
    r.namespace = "wrong";
    assert!(resolver.included_columns(&r).is_err());
}
