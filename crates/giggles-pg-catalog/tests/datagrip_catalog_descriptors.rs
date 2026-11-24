use giggles_pg_catalog::*;
fn run(sql: &str) -> ResultSet {
    let p = plan(sql).expect("plan").expect("catalog");
    let fields = describe(&p).expect("describe");
    let result = execute(&p, &CatalogSnapshot::default(), &[]).expect("execute");
    assert_eq!(fields, result.fields);
    result
}
fn i(v: i64) -> Value {
    Value::Int(v)
}
fn s(v: &str) -> Value {
    Value::Text(v.into())
}
#[test]
fn exact_nominal_descriptors_and_attribute_type_links() {
    assert_eq!(
        (PgType::Regproc.oid(), PgType::Regproc.type_size()),
        (24, 4)
    );
    assert_eq!(
        (PgType::PgNodeTree.oid(), PgType::PgNodeTree.type_size()),
        (194, -1)
    );
    assert_eq!(PgType::Regproc.array_element(), None);
    assert_eq!(PgType::PgNodeTree.array_element(), None);
    for (relation, column, ty, oid, size) in [
        ("pg_aggregate", "aggfnoid", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggtransfn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggfinalfn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggcombinefn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggserialfn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggdeserialfn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggmtransfn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggminvtransfn", PgType::Regproc, 24, 4),
        ("pg_aggregate", "aggmfinalfn", PgType::Regproc, 24, 4),
        ("pg_amproc", "amproc", PgType::Regproc, 24, 4),
        ("pg_operator", "oprcode", PgType::Regproc, 24, 4),
        ("pg_operator", "oprrest", PgType::Regproc, 24, 4),
        ("pg_operator", "oprjoin", PgType::Regproc, 24, 4),
        ("pg_policy", "polqual", PgType::PgNodeTree, 194, -1),
        ("pg_policy", "polwithcheck", PgType::PgNodeTree, 194, -1),
        ("pg_proc", "prosupport", PgType::Regproc, 24, 4),
        ("pg_proc", "proargdefaults", PgType::PgNodeTree, 194, -1),
        ("pg_proc", "prosqlbody", PgType::PgNodeTree, 194, -1),
        ("pg_rewrite", "ev_qual", PgType::PgNodeTree, 194, -1),
        ("pg_rewrite", "ev_action", PgType::PgNodeTree, 194, -1),
        ("pg_trigger", "tgqual", PgType::PgNodeTree, 194, -1),
    ] {
        let result = run(&format!("select {column} from pg_catalog.{relation}"));
        // The 23 supported text-search operator OIDs come from PostgreSQL 18.6.
        let expected_rows = if relation == "pg_operator" { 23 } else { 0 };
        assert_eq!(result.rows.len(), expected_rows, "{relation}.{column}");
        assert_eq!(
            result.fields,
            vec![Field {
                name: column.into(),
                data_type: ty
            }]
        );
        let attrs = run(&format!(
            "select A.atttypid, A.attlen from pg_catalog.pg_attribute A join pg_catalog.pg_class C on A.attrelid = C.oid where C.relname = '{relation}' and A.attname = '{column}'"
        ));
        assert_eq!(attrs.rows, vec![vec![i(oid), i(size)]]);
    }
}
#[test]
fn primary_pg14_nominal_type_facts_are_explicit() {
    // pg_type.dat entries24/194 + pg_type.h default typtype=b. Node tree is
    // category Z and has no array, despite being an internal-only descriptor.
    let result = run(
        "select oid, typname, typlen, typtype, typcategory, typelem, typarray, typinput, typisdefined, typispreferred from pg_catalog.pg_type where oid in (24,194,1008) order by oid",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                i(24),
                s("regproc"),
                i(4),
                s("b"),
                s("N"),
                i(0),
                i(1008),
                i(44),
                Value::Bool(true),
                Value::Bool(false)
            ],
            vec![
                i(194),
                s("pg_node_tree"),
                i(-1),
                s("b"),
                s("Z"),
                i(0),
                i(0),
                i(195),
                Value::Bool(true),
                Value::Bool(false)
            ],
            vec![
                i(1008),
                s("_regproc"),
                i(-1),
                s("b"),
                s("A"),
                i(24),
                i(0),
                i(750),
                Value::Bool(true),
                Value::Bool(false)
            ],
        ]
    );
    assert_eq!(run("select oid, typname, typlen, typtype, typcategory, typelem, typarray from pg_catalog.pg_type where oid in (23,25,26) order by oid").rows, vec![
        vec![i(23),s("int4"),i(4),s("b"),s("N"),i(0),i(1007)],
        vec![i(25),s("text"),i(-1),s("b"),s("S"),i(0),i(1009)],
        vec![i(26),s("oid"),i(4),s("b"),s("N"),i(0),i(1028)],
    ]);
    assert!(run("select oid from pg_catalog.pg_proc").rows.is_empty());
}
#[test]
fn null_descriptors_do_not_add_casts_or_routine_resolution() {
    let result = run(
        "select P.prosupport, P.proargdefaults from pg_catalog.pg_namespace N left join pg_catalog.pg_proc P on N.oid = P.pronamespace where N.oid = 11",
    );
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        vec![PgType::Regproc, PgType::PgNodeTree]
    );
    assert_eq!(result.rows, vec![vec![Value::Null, Value::Null]]);
    assert!(matches!(
        plan("select 'x'::pg_node_tree"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan("select regprocin('x')"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan("select pg_node_tree_in('x')"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan("select * from pg_catalog.pg_unknown"),
        Err(Error::Unsupported)
    ));
    // Existing regproc cast still returns the existing Oid descriptor and
    // rejects missing routine resolution at execution. No binder change here.
    let p = plan("select 'missing_routine'::regproc").unwrap().unwrap();
    assert_eq!(describe(&p).unwrap()[0].data_type, PgType::Oid);
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]),
        Err(Error::Unsupported)
    );
}
