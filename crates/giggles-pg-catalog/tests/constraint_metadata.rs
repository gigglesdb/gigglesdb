use giggles_pg_catalog::*;
fn run(s: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    execute(&plan(sql).unwrap().unwrap(), s, &[]).unwrap().rows
}
#[test]
fn pg18_check_fk_defaults_identity_and_dependencies() {
    use Value::{Text as S, Int as I, Bool as B};
    let mut s = CatalogSnapshot::default();
    s.schemas.insert("gui_sales".into(), (70000, 10));
    for (id, name, columns) in [(10, "parent", vec!["id"]), (20, "child", vec!["id", "parent_id", "amount", "note"])] {
        s.tables.push(TableDef { id, name: name.into(), primary_key: vec![0], columns: columns.iter().enumerate().map(|(i,n)| ColumnDef {
            id: id + i as u64, name: (*n).into(), data_type: ScalarType::Integer, not_null: i == 0,
        }).collect() });
        s.table_schemas.insert(id, "gui_sales".into());
    }
    s.column_defaults.insert(22, "7".into()); s.column_defaults.insert(23, "'sample'::text".into());
    s.column_identities.insert(20, 'd');
    s.constraints.push(ConstraintDef { id: 30, table: 20, name: "child_amount_check".into(), kind: 'c',
        columns: vec![22], expression: Some("(amount >= 0)".into()), validated: true, ..Default::default() });
    s.constraints.push(ConstraintDef { id: 31, table: 20, name: "child_parent_id_fkey".into(), kind: 'f', columns: vec![21],
        foreign_table: 10, foreign_columns: vec![10], foreign_key_target: Some(ForeignKeyTarget::PrimaryKey), update: 'a', delete: 'a', validated: true, ..Default::default() });
    assert_eq!(run(&s, "SELECT pg_get_constraintdef(oid),pg_get_constraintdef(oid,true),pg_get_expr(conbin,conrelid) FROM pg_constraint WHERE contype='c'"),
        vec![vec![S("CHECK ((amount >= 0))".into()), S("CHECK (amount >= 0)".into()), S("(amount >= 0)".into())]]);
    assert_eq!(run(&s, "SELECT pg_get_constraintdef(oid),confupdtype,confdeltype,confmatchtype,condeferrable,condeferred FROM pg_constraint WHERE contype='f'"),
        vec![vec![S("FOREIGN KEY (parent_id) REFERENCES gui_sales.parent(id)".into()),S("a".into()),S("a".into()),S("s".into()),B(false),B(false)]]);
    assert_eq!(run(&s, "SELECT adnum,pg_get_expr(adbin,adrelid) FROM pg_attrdef ORDER BY adnum"), vec![vec![I(3),S("7".into())],vec![I(4),S("'sample'::text".into())]]);
    assert_eq!(run(&s, "SELECT attidentity,atthasdef FROM pg_attribute WHERE attrelid=65616 AND attnum=1"), vec![vec![S("d".into()),B(false)]]);
    assert_eq!(run(&s, "SELECT count(*) FROM pg_constraint WHERE contype='u'"), vec![vec![I(0)]]);
    assert_eq!(run(&s, "SELECT deptype,refobjsubid FROM pg_depend WHERE classid=2606 AND objid=65656 ORDER BY deptype"), vec![vec![S("a".into()),I(3)],vec![S("n".into()),I(3)]]);
    assert_eq!(run(&s, "SELECT pg_get_expr(conbin,conrelid,true) FROM pg_constraint WHERE contype='c'"), vec![vec![S("amount >= 0".into())]]);
    let mut bad = s.clone(); bad.constraints[1].foreign_columns = vec![999];
    assert_eq!(execute(&plan("SELECT * FROM pg_constraint").unwrap().unwrap(), &bad, &[]).unwrap_err(), Error::InvalidSnapshot);
    s.constraints[1].update='c'; s.constraints[1].delete='n'; s.constraints[1].deferrable=true; s.constraints[1].deferred=true;
    assert_eq!(run(&s, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE contype='f'"),
        vec![vec![S("FOREIGN KEY (parent_id) REFERENCES gui_sales.parent(id) ON UPDATE CASCADE ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED".into())]]);
}

#[test]
fn pg18_unique_origin_rows_dependencies_definition_and_information_schema() {
    use Value::{Text as S, Int as I, Bool as B};
    let mut s = CatalogSnapshot::default();
    s.tables.push(TableDef { id: 10, name: "t".into(), primary_key: vec![], columns: vec![ColumnDef {
        id: 11, name: "label".into(), data_type: ScalarType::Text, not_null: false }] });
    for (id, name) in [(20, "uq"), (21, "plain")] {
        s.indexes.push(IndexDef { id, table: 10, name: name.into(), columns: vec![11], unique: true });
    }
    s.constraints.push(ConstraintDef { id: 30, table: 10, name: "uq".into(), kind: 'u', columns: vec![11],
        index: 20, validated: true, ..Default::default() });
    assert_eq!(run(&s, "SELECT conname,contype,conkey,conindid,condeferrable,condeferred,convalidated,confrelid,confkey,pg_get_constraintdef(oid) FROM pg_constraint WHERE contype='u'"),
        vec![vec![S("uq".into()),S("u".into()),Value::Array(vec![I(1)]),I(65616),B(false),B(false),B(true),I(0),Value::Null,S("UNIQUE (label)".into())]]);
    assert_eq!(run(&s, "SELECT classid,objid,refclassid,refobjid,deptype FROM pg_depend WHERE objid=65616"),
        vec![vec![I(1259),I(65616),I(2606),I(65656),S("i".into())]]);
    assert_eq!(run(&s, "SELECT constraint_name,constraint_type,is_deferrable,initially_deferred,enforced,nulls_distinct FROM information_schema.table_constraints WHERE constraint_type='UNIQUE'"),
        vec![vec![S("uq".into()),S("UNIQUE".into()),S("NO".into()),S("NO".into()),S("YES".into()),S("YES".into())]]);
    assert_eq!(run(&s, "SELECT constraint_name,column_name,ordinal_position,position_in_unique_constraint FROM information_schema.key_column_usage WHERE constraint_name='uq'"),
        vec![vec![S("uq".into()),S("label".into()),I(1),Value::Null]]);
    assert_eq!(run(&s, "SELECT connoinherit,conparentid,conperiod,conenforced,conislocal,coninhcount FROM pg_constraint WHERE contype='u'"),
        vec![vec![B(true),I(0),B(false),B(true),B(true),I(0)]]);
    assert_eq!(run(&s, "SELECT count(*) FROM pg_index WHERE indisunique"), vec![vec![I(2)]]);
}
