use giggles_pg_catalog::*;

fn run(sql: &str, parameters: &[Value]) -> ResultSet {
    let p = plan(sql).unwrap().unwrap();
    let fields = describe(&p).unwrap();
    let r = execute(&p, &CatalogSnapshot::default(), parameters).unwrap();
    assert_eq!(fields, r.fields);
    r
}

#[test]
fn oid_scalar_storage_order_unsigned_null_and_distinct() {
    let r = run(
        "SELECT unnest(ARRAY[4294967295::oid,2::oid,NULL::oid,2::oid]) AS element",
        &[],
    );
    assert_eq!(r.fields[0].data_type, PgType::Oid);
    assert_eq!(r.fields[0].data_type.oid(), 26);
    assert_eq!(r.fields[0].data_type.type_size(), 4);
    assert_eq!(
        r.rows,
        [
            vec![Value::Int(4294967295)],
            vec![Value::Int(2)],
            vec![Value::Null],
            vec![Value::Int(2)]
        ]
    );
    assert_eq!(
        run("SELECT DISTINCT unnest(ARRAY[2::oid,2::oid,1::oid])", &[]).rows,
        [vec![Value::Int(2)], vec![Value::Int(1)]]
    );
    assert!(run("SELECT unnest(NULL::oid[])", &[]).rows.is_empty());
    assert!(
        run("SELECT unnest(ARRAY(SELECT oid FROM pg_proc))", &[])
            .rows
            .is_empty()
    );
}

#[test]
fn catalog_oidvector_and_array_descriptors() {
    let r = run("SELECT unnest(proargtypes) AS arg FROM pg_proc", &[]);
    assert_eq!(r.fields[0].data_type, PgType::Oid);
    assert!(r.rows.is_empty());
    let r = run("SELECT unnest(proallargtypes) AS arg FROM pg_proc", &[]);
    assert_eq!(r.fields[0].data_type, PgType::Oid);
    assert!(r.rows.is_empty());
}

#[test]
fn length_dimensions_empty_null_and_vectors() {
    let r = run(
        "SELECT array_length(ARRAY[1,2],1),array_length(ARRAY[1],2),array_length(ARRAY[1],0),array_length(NULL::oid[],1),array_length(ARRAY(SELECT oid FROM pg_proc),1),array_length(ARRAY[1],NULL)",
        &[],
    );
    assert!(r.fields.iter().all(|f| f.data_type == PgType::Int4));
    assert_eq!(
        r.rows,
        [vec![
            Value::Int(2),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null
        ]]
    );
    let r = run(
        "SELECT array_length(proargtypes,1),array_length(proallargtypes,1) FROM pg_proc",
        &[],
    );
    assert!(r.fields.iter().all(|f| f.data_type == PgType::Int4));
    assert!(r.rows.is_empty());
    for sql in [
        "SELECT array_length(1,1)",
        "SELECT array_length(true,1)",
        "SELECT array_length('abc',1)",
        "SELECT array_length(ARRAY[1],true)",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}

#[test]
fn srf_contexts_and_multiple_distinct_expansions_stay_closed() {
    for sql in [
        "SELECT unnest(ARRAY[1])",
        "SELECT unnest(ARRAY[1::oid],ARRAY[2::oid])",
        "SELECT 1 WHERE unnest(ARRAY[1::oid])=1",
        "SELECT 1 ORDER BY unnest(ARRAY[1::oid])",
        "SELECT array_length(ARRAY[unnest(ARRAY[1::oid])],1)",
        "SELECT unnest(ARRAY[1::oid]),unnest(ARRAY[2::oid])",
        "SELECT unnest(ARRAY[1::oid])+1",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}

#[test]
fn legacy_int2_record_fields_and_ordinal_remain_exact() {
    let r = run(
        "SELECT (information_schema._pg_expandarray(ARRAY[7::int2,3::int2])).x,(information_schema._pg_expandarray(ARRAY[7::int2,3::int2])).n",
        &[],
    );
    assert_eq!(
        r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [PgType::Int2, PgType::Int4]
    );
    assert_eq!(
        r.rows,
        [
            vec![Value::Int(7), Value::Int(1)],
            vec![Value::Int(3), Value::Int(2)]
        ]
    );
    assert!(plan("SELECT information_schema._pg_expandarray(ARRAY[1::oid])").is_err());
}

#[test]
fn parameter_projection_guard_and_output_work_caps() {
    let p = plan("SELECT unnest($1::oid[])").unwrap().unwrap();
    assert_eq!(p.parameter_types(), [PgType::OidArray]);
    assert!(!p.allows_numeric_input_override());
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Array(vec![Value::Int(3), Value::Int(1)])]
        ),
        Err(Error::Unsupported)
    );
    assert!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Null])
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Array(vec![Value::Int(1); 4097])]
        )
        .is_err()
    );
    let p = plan(
        "SELECT unnest(ARRAY[1::oid,2::oid]) FROM pg_database CROSS JOIN generate_series(1,8192)",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]),
        Err(Error::Limit)
    );
}

#[test]
fn exact_captured_full_used_types_and_routine_argument_names() {
    // Exact retained wire SQL SHA256 bd147c9463ed4b1b334492d5f391841698a29b3422b5d7054bbe880672d8aeb6
    let r = run(
        r###"with schema_procs as (select prorettype, proargtypes, proallargtypes
                      from pg_catalog.pg_proc
                      where pronamespace = $1::oid
                        /* and pg_catalog.age(xmin) <= #TXAGE */ ),
     schema_opers as (select oprleft, oprright, oprresult
                      from pg_catalog.pg_operator
                      where oprnamespace = $2::oid
                        /* and pg_catalog.age(xmin) <= #TXAGE */ ),
     schema_aggregates as (select A.aggtranstype , A.aggmtranstype 
                           from pg_catalog.pg_aggregate A
                           join pg_catalog.pg_proc P
                             on A.aggfnoid = P.oid
                           where P.pronamespace = $3::oid
                           /* and (pg_catalog.age(A.xmin) <= #TXAGE or pg_catalog.age(P.xmin) <= #TXAGE) */),
     schema_arg_types as ( select prorettype as type_id
                           from schema_procs
                           union
                           select distinct unnest(proargtypes) as type_id
                           from schema_procs
                           union
                           select distinct unnest(proallargtypes) as type_id
                           from schema_procs
                           union
                           select oprleft as type_id
                           from schema_opers
                           where oprleft is not null
                           union
                           select oprright as type_id
                           from schema_opers
                           where oprright is not null
                           union
                           select oprresult as type_id
                           from schema_opers
                           where oprresult is not null
                           union
                           select aggtranstype::oid as type_id
                           from schema_aggregates
                           union
                           select aggmtranstype::oid as type_id
                           from schema_aggregates
                           
                           )
select type_id, pg_catalog.format_type(type_id, null) as type_spec
from schema_arg_types
where type_id <> 0 -- todo unclear how to frag
"###,
        &vec![Value::Int(2200); 3],
    );
    assert_eq!(
        r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [PgType::Oid, PgType::Text]
    );
    assert_eq!(
        r.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        ["type_id", "type_spec"]
    );
    assert!(r.rows.is_empty());
    // Exact retained wire SQL SHA256 13271e2a1d0c83d1d796659e7d0f1f0ca13d9bdd587c92ff7faa88aeb1073e12
    let r = run(
        r###"select pronamespace as schemaId,
       oid as majorOid,
       proargnames as argNames,
       proargmodes as argModes,
       array_length(proargtypes, 1) as nArgs
from pg_catalog.pg_proc
where pronamespace in ( $1 )
  --  and pg_catalog.age(xmin) <= #TXAGE
order by schemaId"###,
        &[Value::Int(2200)],
    );
    assert_eq!(
        r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [
            PgType::Oid,
            PgType::Oid,
            PgType::TextArray,
            PgType::CharArray,
            PgType::Int4
        ]
    );
    assert_eq!(
        r.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        ["schemaid", "majoroid", "argnames", "argmodes", "nargs"]
    );
    assert!(r.rows.is_empty());
}

#[test]
fn nonempty_metadata_oidvector_and_int2vector_lengths() {
    let snapshot = CatalogSnapshot {
        tables: vec![TableDef {
            id: 1,
            name: "keys".into(),
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "a".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    id: 2,
                    name: "b".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
            ],
            primary_key: vec![1, 0],
        }],
    };
    let p=plan("SELECT unnest(indcollation) AS element,array_length(indclass,1),array_length(indkey,1) FROM pg_index").unwrap().unwrap();
    let r = execute(&p, &snapshot, &[]).unwrap();
    assert_eq!(
        r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [PgType::Oid, PgType::Int4, PgType::Int4]
    );
    assert_eq!(
        r.rows,
        [
            vec![Value::Int(0), Value::Int(2), Value::Int(2)],
            vec![Value::Int(0), Value::Int(2), Value::Int(2)]
        ]
    );
    let p=plan("SELECT (information_schema._pg_expandarray(indkey)).x,(information_schema._pg_expandarray(indkey)).n FROM pg_index").unwrap().unwrap();
    assert_eq!(
        execute(&p, &snapshot, &[]).unwrap().rows,
        [
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(1), Value::Int(2)]
        ]
    );
}

#[test]
fn expansion_output_bytes_and_sort_work_remain_bounded() {
    let p=plan("SELECT unnest(ARRAY[1::oid,2::oid]),$1::text FROM pg_database CROSS JOIN generate_series(1,3)").unwrap().unwrap();
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Text("x".repeat(800_000))]
        ),
        Err(Error::Limit)
    );
    let order = vec!["1"; 32].join(",");
    let p=plan(&format!("SELECT unnest(ARRAY[1::oid,2::oid]) FROM pg_database CROSS JOIN generate_series(1,4096) ORDER BY {order}")).unwrap().unwrap();
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]),
        Err(Error::Limit)
    );
}
