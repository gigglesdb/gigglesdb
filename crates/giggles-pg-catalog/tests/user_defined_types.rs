use giggles_pg_catalog::*;
#[test]
fn user_type_and_enum_rows() {
    let mut snapshot = CatalogSnapshot::default();
    for (oid, name, kind, base, labels) in [
        (65540, "mood", 'e', 0, vec!["second".into(), "first".into()]),
        (65552, "positive", 'd', 23, vec![]),
        (65556, "pair", 'c', 0, vec![]),
    ] {
        snapshot.user_types.push(UserType {
            schema: "public".into(),
            oid,
            name: name.into(),
            kind,
            base,
            typmod: -1,
            not_null: kind == 'd',
            labels,
        });
    }
    let query = |sql: &str| execute(&plan(sql).unwrap().unwrap(), &snapshot, &[]).unwrap();
    assert_eq!(
        query("SELECT typname,typtype FROM pg_type WHERE typnamespace=2200 ORDER BY typname").rows,
        vec![
            vec![Value::Text("_mood".into()), Value::Text("b".into())],
            vec![Value::Text("mood".into()), Value::Text("e".into())],
            vec![Value::Text("pair".into()), Value::Text("c".into())],
            vec![Value::Text("positive".into()), Value::Text("d".into())]
        ]
    );
    assert_eq!(
        query("SELECT oid,typelem,typarray FROM pg_type WHERE oid IN (65540,65541) ORDER BY oid").rows,
        vec![
            vec![Value::Int(65540), Value::Int(0), Value::Int(65541)],
            vec![Value::Int(65541), Value::Int(65540), Value::Int(0)],
        ]
    );
    assert_eq!(
        query("SELECT enumlabel FROM pg_enum WHERE enumtypid=65540 ORDER BY enumsortorder").rows,
        vec![
            vec![Value::Text("second".into())],
            vec![Value::Text("first".into())]
        ]
    );
}

#[test]
fn user_type_namespace_matches_pg_namespace() {
    let mut snapshot = CatalogSnapshot::default();
    snapshot.schemas.insert("types_one".into(), (70000, 10));
    snapshot.user_types.push(UserType {
        schema: "types_one".into(),
        oid: 65540,
        name: "mood".into(),
        kind: 'e',
        base: 0,
        typmod: -1,
        not_null: false,
        labels: vec!["first".into()],
    });
    let p = plan("SELECT n.nspname,t.typname FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE t.typname='mood'").unwrap().unwrap();
    assert_eq!(
        execute(&p, &snapshot, &[]).unwrap().rows,
        vec![vec![
            Value::Text("types_one".into()),
            Value::Text("mood".into())
        ]]
    );
}

#[test]
fn growing_an_enum_keeps_catalog_label_oids_distinct() {
    let mut snapshot = CatalogSnapshot::default();
    for (oid, name, labels) in [
        (
            65540,
            "first",
            vec!["start".into(), "inserted".into(), "end".into()],
        ),
        (65548, "second", vec!["kept".into()]),
    ] {
        snapshot.user_types.push(UserType {
            schema: "public".into(),
            oid,
            name: name.into(),
            kind: 'e',
            base: 0,
            typmod: -1,
            not_null: false,
            labels,
        });
    }
    let sql = "SELECT oid,enumlabel FROM pg_enum ORDER BY enumtypid,enumsortorder";
    let rows = execute(&plan(sql).unwrap().unwrap(), &snapshot, &[])
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(65544), Value::Text("start".into())],
            vec![
                Value::Int(i64::from(u32::MAX)),
                Value::Text("inserted".into())
            ],
            vec![
                Value::Int(i64::from(u32::MAX - 4)),
                Value::Text("end".into())
            ],
            vec![Value::Int(65552), Value::Text("kept".into())],
        ]
    );
}
