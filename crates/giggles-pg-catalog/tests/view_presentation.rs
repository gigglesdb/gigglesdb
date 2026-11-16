use giggles_pg_catalog::*;
fn run(s: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    execute(&plan(sql).unwrap().unwrap(), s, &[]).unwrap().rows
}
#[test]
fn views_have_typed_outputs_definitions_and_no_heap_objects() {
    use Value::{Int as I, Text as S, Bool as B, Null as N};
    let mut s = CatalogSnapshot::default();
    s.tables.push(TableDef { id: 10, name: "v".into(), primary_key: vec![],
        columns: vec![ColumnDef { id: 11, name: "amount".into(), data_type: ScalarType::Numeric, not_null: false }] });
    s.views.insert(10, "SELECT amount FROM public.t;".into());
    s.column_typmods.insert(11, 655366);
    s.comments.insert((10, 0), "view comment".into());
    assert_eq!(run(&s, "SELECT relkind,relam,relnatts,reltype,relhasindex,relfilenode,relreplident FROM pg_class WHERE relname='v'"),
        vec![vec![S("v".into()),I(0),I(1),I(65579),B(false),I(0),S("n".into())]]);
    assert_eq!(run(&s, "SELECT attnum,atttypid,atttypmod,attnotnull FROM pg_attribute WHERE attrelid=65576"),
        vec![vec![I(1),I(1700),I(655366),B(false)]]);
    assert_eq!(run(&s, "SELECT typtype,typrelid FROM pg_type WHERE oid=65579"), vec![vec![S("c".into()),I(65576)]]);
    for sql in ["SELECT * FROM pg_index WHERE indrelid=65576", "SELECT * FROM pg_constraint WHERE conrelid=65576",
        "SELECT * FROM pg_tables WHERE tablename='v'"] { assert!(run(&s, sql).is_empty()); }
    assert_eq!(run(&s, "SELECT schemaname,viewname,viewowner FROM pg_views"), vec![vec![S("public".into()),S("v".into()),S("giggles".into())]]);
    for arg in ["65576::oid", "65576::oid,false", "65576::oid,true", "65576::oid,40", "'public.v'", "'v',true"] {
        assert_eq!(run(&s, &format!("SELECT pg_get_viewdef({arg})")), vec![vec![S(" SELECT amount\n   FROM public.t;".into())]]);
        assert!(plan_gateway(&format!("SELECT pg_get_viewdef({arg})")).unwrap().is_some());
    }
    assert_eq!(run(&s, "SELECT pg_get_viewdef(1::oid),pg_get_viewdef(NULL::oid),obj_description(65576::oid)"),
        vec![vec![N,N,S("view comment".into())]]);
    assert_eq!(run(&s, "SELECT table_type,is_insertable_into FROM information_schema.tables WHERE table_name='v'"), vec![vec![S("VIEW".into()),S("NO".into())]]);
    assert_eq!(run(&s, "SELECT is_nullable,numeric_precision,numeric_scale,is_updatable FROM information_schema.columns WHERE table_name='v'"),
        vec![vec![S("YES".into()),I(10),I(2),S("NO".into())]]);
    assert_eq!(run(&s, "SELECT view_definition,check_option,is_updatable FROM information_schema.views"), vec![vec![S(s.views[&10].clone()),S("NONE".into()),S("NO".into())]]);
    s.user = "reader".into();
    assert_eq!(run(&s, "SELECT view_definition FROM information_schema.views"), vec![vec![N]]);
    let prepared = plan("SELECT pg_get_viewdef(65576::oid)").unwrap().unwrap();
    s.views.insert(10, "SELECT renamed FROM public.t;".into());
    assert_eq!(execute(&prepared, &s, &[]).unwrap().rows, vec![vec![S(" SELECT renamed\n   FROM public.t;".into())]]);
}
