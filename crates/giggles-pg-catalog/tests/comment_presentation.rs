use giggles_pg_catalog::*;
fn run(s: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    execute(&plan(sql).unwrap().unwrap(), s, &[]).unwrap().rows
}
#[test]
fn stable_comments_follow_visible_columns_and_description_overloads() {
    use Value::{Int as I, Text as S, Null as N};
    let mut s = CatalogSnapshot::default();
    s.tables.push(TableDef { id: 10, name: "t".into(), primary_key: vec![],
        columns: [11, 12].into_iter().map(|id| ColumnDef { id, name: format!("c{id}"),
            data_type: ScalarType::Integer, not_null: false }).collect() });
    s.comments.extend([((10, 0), "é'quoted".into()), ((10, 12), "column".into()),
        ((99, 0), "hidden".into())]);
    assert_eq!(run(&s, "SELECT objsubid,description FROM pg_description WHERE classoid=1259 ORDER BY objsubid"),
        vec![vec![I(0),S("é'quoted".into())],vec![I(2),S("column".into())]]);
    assert_eq!(run(&s, "SELECT obj_description(65576::oid),obj_description(65576::oid,'pg_class'),col_description(65576::oid,2),col_description(65576::oid,1),obj_description(65576::oid,'pg_type'),obj_description(1::oid),obj_description(NULL::oid),col_description(65576::oid,NULL)"),
        vec![vec![S("é'quoted".into()),S("é'quoted".into()),S("column".into()),N,N,N,N,N]]);
    assert_eq!(run(&s, "SELECT obj_description(2200::oid,'pg_namespace')"), vec![vec![S("standard public schema".into())]]);
    s.tables[0].columns.remove(0);
    assert_eq!(run(&s, "SELECT col_description(65576::oid,1),col_description(65576::oid,2)"),
        vec![vec![S("column".into()),N]]);
    s.tables.clear();
    assert_eq!(run(&s, "SELECT obj_description(65576::oid),col_description(65576::oid,1)"), vec![vec![N,N]]);
}
