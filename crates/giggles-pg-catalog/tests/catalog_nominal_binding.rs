use giggles_pg_catalog::*;

fn run(sql: &str) -> ResultSet {
    let p = plan(sql).unwrap().expect("catalog plan");
    let fields = describe(&p).unwrap();
    let result = execute(&p, &CatalogSnapshot::default(), &[]).unwrap();
    assert_eq!(fields, result.fields);
    result
}

#[test]
fn nominal_equality_joins_describe_and_execute_empty_rows() {
    // PG14 pg_cast.dat: regproc/oid implicit binary aliases; pg_operator.dat:
    // =(oid,oid) and <>(oid,oid). This finite binder retains both descriptors.
    for predicate in [
        "a.aggfnoid=p.oid",
        "p.oid=a.aggfnoid",
        "a.aggfnoid<>p.oid",
        "p.oid<>a.aggfnoid",
    ] {
        let result = run(&format!(
            "SELECT a.aggfnoid,p.oid,{predicate} AS compared FROM pg_aggregate a JOIN pg_proc p ON {predicate}"
        ));
        assert_eq!(
            result
                .fields
                .iter()
                .map(|f| f.data_type)
                .collect::<Vec<_>>(),
            [PgType::Regproc, PgType::Oid, PgType::Bool]
        );
        assert!(result.rows.is_empty());
    }
}

#[test]
fn left_join_nominal_null_comparisons_preserve_sql_null_and_selection() {
    let source = "FROM pg_type t LEFT JOIN pg_proc p ON false WHERE t.oid=23";
    let result = run(&format!(
        "SELECT p.prosupport,t.oid,p.prosupport=t.oid AS eq,t.oid<>p.prosupport AS ne {source}"
    ));
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        [PgType::Regproc, PgType::Oid, PgType::Bool, PgType::Bool]
    );
    assert_eq!(
        result.rows,
        [vec![Value::Null, Value::Int(23), Value::Null, Value::Null]]
    );
    for predicate in ["p.prosupport=t.oid", "t.oid<>p.prosupport"] {
        assert!(
            run(&format!("SELECT t.oid {source} AND {predicate}"))
                .rows
                .is_empty()
        );
        assert_eq!(
            run(&format!("SELECT t.oid {source} AND ({predicate}) IS NULL")).rows,
            [vec![Value::Int(23)]]
        );
    }
}

#[test]
fn node_tree_null_pg_get_expr_retains_input_and_result_descriptors() {
    // PG14 pg_proc.dat OID1716 is pg_get_expr(pg_node_tree,oid)->text.
    let result = run(
        "SELECT p.proargdefaults,pg_get_expr(p.proargdefaults,t.oid) AS expr FROM pg_type t LEFT JOIN pg_proc p ON false WHERE t.oid=23",
    );
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        [PgType::PgNodeTree, PgType::Text]
    );
    assert_eq!(result.rows, [vec![Value::Null, Value::Null]]);
    let legacy = run("SELECT pg_get_expr(NULL::text,23::oid) AS expr FROM pg_type WHERE oid=23");
    assert_eq!(legacy.fields[0].data_type, PgType::Text);
    assert_eq!(legacy.rows, [vec![Value::Null]]);
    let empty = run("SELECT proargdefaults,pg_get_expr(proargdefaults,oid) AS expr FROM pg_proc");
    assert_eq!(
        empty.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [PgType::PgNodeTree, PgType::Text]
    );
    assert!(empty.rows.is_empty());
}

#[test]
fn pg_get_expr_wrong_types_arity_and_nonnull_deparser_stay_closed() {
    for arg in ["true", "1", "ARRAY[1]", "ARRAY['x']"] {
        assert!(
            matches!(
                plan(&format!("SELECT pg_get_expr({arg},23::oid) FROM pg_type")),
                Err(Error::Type)
            ),
            "{arg}"
        );
    }
    assert!(matches!(
        plan("SELECT pg_get_expr(proargdefaults,true) FROM pg_proc"),
        Err(Error::Type)
    ));
    assert!(matches!(
        plan("SELECT pg_get_expr(proargdefaults) FROM pg_proc"),
        Err(Error::Unsupported)
    ));
    let p = plan("SELECT pg_get_expr(proargdefaults,oid,true) FROM pg_proc")
        .unwrap()
        .unwrap();
    assert!(
        execute(&p, &CatalogSnapshot::default(), &[])
            .unwrap()
            .rows
            .is_empty()
    );
    let p = plan("SELECT pg_get_expr('encoded'::text,23::oid) FROM pg_type WHERE oid=23")
        .unwrap()
        .unwrap();
    assert_eq!(describe(&p).unwrap()[0].data_type, PgType::Text);
    assert!(matches!(
        execute(&p, &CatalogSnapshot::default(), &[]),
        Err(Error::Unsupported)
    ));
}

#[test]
fn nominal_binding_does_not_widen_other_expression_contracts() {
    for sql in [
        "SELECT a.aggfnoid+p.oid FROM pg_aggregate a,pg_proc p",
        "SELECT a.aggfnoid<p.oid FROM pg_aggregate a,pg_proc p",
        "SELECT p.oid>a.aggfnoid FROM pg_aggregate a,pg_proc p",
        "SELECT a.aggfnoid=p.pronargs FROM pg_aggregate a,pg_proc p",
        "SELECT a.aggfnoid IN (p.oid) FROM pg_aggregate a,pg_proc p",
        "SELECT coalesce(a.aggfnoid,p.oid) FROM pg_aggregate a,pg_proc p",
        "SELECT aggfnoid FROM pg_aggregate UNION SELECT oid FROM pg_proc",
        "SELECT upper(proargdefaults) FROM pg_proc",
    ] {
        assert!(matches!(plan(sql), Err(Error::Type)), "{sql}");
    }
    let p = plan("SELECT prosupport::oid FROM pg_proc")
        .unwrap()
        .unwrap();
    assert_eq!(describe(&p).unwrap()[0].data_type, PgType::Oid);
    // pg_node_tree's unsupported explicit I/O remains closed.
    assert!(matches!(
        plan("SELECT 1::pg_node_tree FROM pg_type"),
        Err(Error::Unsupported)
    ));
}

#[test]
fn nominal_parameter_inference_and_nonnull_input_stay_closed() {
    for (column, ty) in [
        ("prosupport", PgType::Regproc),
        ("proargdefaults", PgType::PgNodeTree),
    ] {
        let p = plan(&format!("SELECT {column} FROM pg_proc WHERE {column}=$1"))
            .unwrap()
            .unwrap();
        assert_eq!(p.parameter_types(), [ty]);
        assert_eq!(describe(&p).unwrap()[0].data_type, ty);
        assert!(
            execute(&p, &CatalogSnapshot::default(), &[Value::Null])
                .unwrap()
                .rows
                .is_empty()
        );
        for value in [Value::Int(0), Value::Text("x".into())] {
            assert!(matches!(
                execute(&p, &CatalogSnapshot::default(), &[value]),
                Err(Error::Unsupported)
            ));
        }
    }
}
