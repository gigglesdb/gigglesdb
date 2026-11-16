use giggles_pg_catalog::*;
fn snapshot(sql: &str) -> CatalogSnapshot {
    CatalogSnapshot { tables: vec![TableDef { id: 10, name: "v".into(), primary_key: vec![], columns: vec![ColumnDef { id: 11, name: "id".into(), data_type: ScalarType::Integer, not_null: false }] },
        TableDef { id: 20, name: "vt".into(), primary_key: vec![], columns: vec![ColumnDef { id: 21, name: "id".into(), data_type: ScalarType::Integer, not_null: false }] }],
    views: std::collections::BTreeMap::from([(10,sql.into())]), ..Default::default() }
}
fn run(s: &CatalogSnapshot, args: &str) -> Result<Value,Error> {
    Ok(execute(&plan(&format!("SELECT pg_get_viewdef({args})"))?.unwrap(),s,&[])?.rows[0][0].clone())
}
#[test]
fn oracle_supported_shapes_both_modes() {
    for (sql, plain, pretty) in [
        ("SELECT id AS id, v AS v FROM public.vt", " SELECT id,\n    v\n   FROM vt;", " SELECT id,\n    v\n   FROM vt;"),
        ("SELECT id AS id, v AS label FROM public.vt WHERE id > 0", " SELECT id,\n    v AS label\n   FROM vt\n  WHERE (id > 0);", " SELECT id,\n    v AS label\n   FROM vt\n  WHERE id > 0;"),
        ("SELECT id,v,n,d,b FROM public.vt", " SELECT id,\n    v,\n    n,\n    d,\n    b\n   FROM vt;", " SELECT id,\n    v,\n    n,\n    d,\n    b\n   FROM vt;"),
        ("SELECT id,v FROM public.vt WHERE (id > 0 AND v <> 'x') OR b", " SELECT id,\n    v\n   FROM vt\n  WHERE (((id > 0) AND (v <> 'x'::text)) OR b);", " SELECT id,\n    v\n   FROM vt\n  WHERE id > 0 AND v <> 'x'::text OR b;"),
        ("SELECT id FROM public.vt WHERE NOT b AND v IS NULL", " SELECT id\n   FROM vt\n  WHERE ((NOT b) AND (v IS NULL));", " SELECT id\n   FROM vt\n  WHERE NOT b AND v IS NULL;"),
        ("SELECT id AS a, v AS bb FROM public.vt", " SELECT id AS a,\n    v AS bb\n   FROM vt;", " SELECT id AS a,\n    v AS bb\n   FROM vt;"),
        (r#"SELECT id AS "Id", v AS "select" FROM public.vt"#, " SELECT id AS \"Id\",\n    v AS \"select\"\n   FROM vt;", " SELECT id AS \"Id\",\n    v AS \"select\"\n   FROM vt;")
    ] {
        let s = snapshot(sql);
        for (arg,want) in [("'v'",plain),("'v',false",plain),("'v',true",pretty),("'v'::regclass,0",pretty),("65576::oid",plain)] {
            assert_eq!(run(&s,arg).unwrap(),Value::Text(want.into()),"{sql}: {arg}");
        }
    }
}
#[test]
fn calling_forms_search_path_and_wrap() {
    let mut s = snapshot("SELECT id AS id,v AS label FROM public.vt WHERE id>0");
    assert_eq!(run(&s,"'v'::regclass,40").unwrap(),Value::Text(" SELECT id, v AS label\n   FROM vt\n  WHERE id > 0;".into()));
    assert_eq!(run(&s,"0::oid").unwrap(),Value::Null);
    assert_eq!(run(&s,"'vt'").unwrap(),Value::Null);
    assert_eq!(run(&s,"'nope'"),Err(Error::UnknownRelation("nope".into())));
    s.search_path.clear();
    assert_eq!(run(&s,"'public.v',true").unwrap(),Value::Text(" SELECT id,\n    v AS label\n   FROM public.vt\n  WHERE id > 0;".into()));
}
