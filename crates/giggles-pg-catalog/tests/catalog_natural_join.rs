use giggles_pg_catalog::*;

fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("metadata query"),
        &CatalogSnapshot::default(),
        &[],
    )
}
fn ints(rows: &[&[i64]]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|r| r.iter().map(|&v| Value::Int(v)).collect())
        .collect()
}
fn names(r: &ResultSet) -> Vec<&str> {
    r.fields.iter().map(|f| f.name.as_str()).collect()
}

#[test]
fn natural_multikey_order_and_qualified_keys() {
    let prefix = "WITH l AS (SELECT 9 AS lx, 2 AS b, 1 AS a FROM pg_namespace WHERE oid=11),
        r AS (SELECT 1 AS a, 8 AS rx, 2 AS b FROM pg_namespace WHERE oid=11) ";
    let out = run(&format!(
        "{prefix} SELECT * FROM l NATURAL INNER JOIN r ORDER BY a"
    ))
    .unwrap();
    assert_eq!(names(&out), ["b", "a", "lx", "rx"]);
    assert_eq!(out.rows, ints(&[&[2, 1, 9, 8]]));
    assert_eq!(
        out.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        vec![PgType::Int4; 4]
    );
    assert_eq!(
        run(&format!(
            "{prefix} SELECT a,b,l.a,r.a,l.b,r.b FROM l NATURAL JOIN r ORDER BY r.a"
        ))
        .unwrap()
        .rows,
        ints(&[&[1, 2, 1, 1, 2, 2]])
    );
}

#[test]
fn natural_nonempty_catalog_ctes_and_chained_keys() {
    let sql = "WITH l AS (SELECT oid AS k, nspname AS left_name FROM pg_namespace WHERE oid=11),
        r AS (SELECT oid AS k, nspname AS right_name FROM pg_namespace WHERE oid=11),
        t AS (SELECT oid AS k, nspname AS third_name FROM pg_namespace WHERE oid=11)
        SELECT * FROM l NATURAL JOIN r NATURAL JOIN t ORDER BY k";
    let out = run(sql).unwrap();
    assert_eq!(names(&out), ["k", "left_name", "right_name", "third_name"]);
    assert_eq!(
        out.rows,
        vec![vec![
            Value::Int(11),
            Value::Text("pg_catalog".into()),
            Value::Text("pg_catalog".into()),
            Value::Text("pg_catalog".into())
        ]]
    );
    assert_eq!(out.fields[0].data_type, PgType::Oid);
    let qualified = run("WITH l AS (SELECT oid AS k FROM pg_namespace WHERE oid=11),
        r AS (SELECT oid AS k FROM pg_namespace WHERE oid=11),
        t AS (SELECT oid AS k FROM pg_namespace WHERE oid=11)
        SELECT k,l.k,r.k,t.k FROM l NATURAL JOIN r NATURAL JOIN t")
    .unwrap();
    assert_eq!(qualified.rows, ints(&[&[11, 11, 11, 11]]));
}

#[test]
fn natural_no_common_is_cross_and_null_never_matches() {
    let out = run(
        "WITH l AS (SELECT oid AS l FROM pg_namespace WHERE oid IN (11,2200)),
        r AS (SELECT oid AS r FROM pg_namespace WHERE oid IN (11,2200))
        SELECT * FROM l NATURAL JOIN r ORDER BY l,r",
    )
    .unwrap();
    assert_eq!(names(&out), ["l", "r"]);
    assert_eq!(
        out.rows,
        ints(&[&[11, 11], &[11, 2200], &[2200, 11], &[2200, 2200]])
    );
    let out = run(
        "WITH l AS (SELECT NULL::oid AS k FROM pg_namespace WHERE oid=11),
        r AS (SELECT NULL::oid AS k FROM pg_namespace WHERE oid=11)
        SELECT * FROM l NATURAL JOIN r",
    )
    .unwrap();
    assert_eq!(names(&out), ["k"]);
    assert!(out.rows.is_empty());
    let out = run("WITH l AS (SELECT oid AS k FROM pg_namespace WHERE oid=11),
        r AS (SELECT oid AS k FROM pg_namespace WHERE oid=2200)
        SELECT * FROM l NATURAL JOIN r")
    .unwrap();
    assert!(out.rows.is_empty());
}

#[test]
fn natural_rejects_ambiguous_types_and_excluded_forms() {
    for sql in [
        "WITH l AS (SELECT oid AS k,oid AS k FROM pg_namespace),r AS (SELECT oid AS k FROM pg_namespace) SELECT * FROM l NATURAL JOIN r",
        "WITH l AS (SELECT oid AS k FROM pg_namespace),r AS (SELECT oid AS k,oid AS k FROM pg_namespace) SELECT * FROM l NATURAL JOIN r",
        "WITH l AS (SELECT oid AS k FROM pg_namespace),r AS (SELECT nspname AS k FROM pg_namespace) SELECT * FROM l NATURAL JOIN r",
        "SELECT * FROM pg_namespace l NATURAL LEFT JOIN pg_namespace r",
        "SELECT * FROM pg_namespace l NATURAL RIGHT JOIN pg_namespace r",
        "SELECT * FROM pg_namespace l NATURAL FULL JOIN pg_namespace r",
        "SELECT * FROM pg_namespace l NATURAL JOIN pg_namespace r,pg_namespace t",
        "SELECT * FROM pg_namespace l,pg_namespace t NATURAL JOIN pg_namespace r",
        "SELECT * FROM pg_namespace l NATURAL JOIN LATERAL (SELECT oid FROM pg_namespace) r",
        "SELECT * FROM pg_namespace l NATURAL JOIN unnest(ARRAY[1]) r",
    ] {
        assert!(
            matches!(plan(sql), Err(Error::Unsupported)),
            "{sql}: {:?}",
            plan(sql)
        );
    }
}

#[test]
fn ordinary_using_and_on_controls_keep_physical_columns() {
    let out =
        run("SELECT * FROM pg_namespace l JOIN pg_namespace r USING (oid) WHERE l.oid=11").unwrap();
    assert_eq!(
        out.fields.len(),
        2 * run("SELECT * FROM pg_namespace").unwrap().fields.len()
    );
    assert_eq!(out.rows.len(), 1);
    assert!(matches!(
        plan("SELECT oid FROM pg_namespace l JOIN pg_namespace r USING (oid)"),
        Err(Error::AmbiguousColumn)
    ));
    assert_eq!(run("SELECT l.oid,r.oid FROM pg_namespace l JOIN pg_namespace r ON l.oid=r.oid WHERE l.oid=11").unwrap().rows,
        ints(&[&[11,11]]));
}

#[test]
fn exact_full_routines_replay() {
    let sql = "with languages as (select oid as lang_oid, lanname as lang\n                   from pg_catalog.pg_language),\n     routines as (select proname as r_name,\n                         prolang as lang_oid,\n                         oid as r_id,\n                         xmin as r_state_number,\n                         proargnames as arg_names,\n                         proargmodes as arg_modes,\n                         proargtypes::int[] as in_arg_types,\n                         proallargtypes::int[] as all_arg_types,\n                         pg_catalog.pg_get_expr(proargdefaults, 0) as arg_defaults,\n                         provariadic as arg_variadic_id,\n                         prorettype as ret_type_id,\n                         proretset as ret_set,\n                         prokind /* case when proiswindow then 'w'\n                                                when proisagg then 'a'\n                                                else 'f'\n                                           end */ as kind,\n                         provolatile as volatile_kind,\n                         proisstrict as is_strict,\n                         prosecdef as is_security_definer,\n                         proconfig as configuration_parameters,\n                         procost as cost,\n                         pg_catalog.pg_get_userbyid(proowner) as \"owner\",\n                         prorows as rows ,\n                         proleakproof as is_leakproof  ,\n                         proparallel as concurrency_kind \n                  from pg_catalog.pg_proc\n                  where pronamespace = $1::oid\n                    --  and proname in ( :[*f_names] )\n                    and not (prokind = 'a') /* proisagg */\n                    /* and pg_catalog.age(xmin) <= #TXAGE */)\nselect *\nfrom routines natural join languages\n";

    let p = plan(sql).unwrap().unwrap();
    let fields = describe(&p).unwrap();
    let descriptor = fields
        .iter()
        .map(|f| format!("{}:{:?}", f.name, f.data_type))
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(
        descriptor,
        "lang_oid:Oid r_name:Name r_id:Oid r_state_number:Xid arg_names:TextArray arg_modes:CharArray in_arg_types:Int4Array all_arg_types:Int4Array arg_defaults:Text arg_variadic_id:Oid ret_type_id:Oid ret_set:Bool kind:Char volatile_kind:Char is_strict:Bool is_security_definer:Bool configuration_parameters:TextArray cost:Float4 owner:Name rows:Float4 is_leakproof:Bool concurrency_kind:Char lang:Name"
    );
    for schema in [11, 2200] {
        let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(schema)]).unwrap();
        assert_eq!(out.fields, fields);
        assert!(out.rows.is_empty());
        println!(
            "full-RetrieveRoutines schema={schema} descriptor={descriptor} rows={:?}",
            out.rows
        );
    }
    assert!(run("SELECT * FROM pg_proc").unwrap().rows.is_empty());
    assert_eq!(run("SELECT lanname FROM pg_language ORDER BY oid").unwrap().rows,
        ["internal", "c", "sql", "plpgsql"].map(|name| vec![Value::Text(name.into())]));
}
