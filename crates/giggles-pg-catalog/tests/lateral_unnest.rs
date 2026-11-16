use giggles_pg_catalog::*;

fn compile(sql: &str) -> Result<Option<Plan>, Error> {
    // A catalog context keeps standalone FROM-function cases on this engine.
    plan(&format!(
        "WITH catalog_context AS (SELECT oid FROM pg_roles) {sql}"
    ))
}

fn run(sql: &str) -> ResultSet {
    let p = compile(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .unwrap();
    let result =
        execute(&p, &CatalogSnapshot::default(), &[]).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(describe(&p).unwrap(), result.fields);
    result
}

#[test]
fn explicit_lateral_supported_functions_match_implicit_forms() {
    for function in [
        "unnest(ARRAY[3,1]) WITH ORDINALITY k(x,n)",
        "pg_catalog.unnest(ARRAY[3,1]) WITH ORDINALITY k(x,n)",
        "\"pg_catalog\".\"unnest\"(ARRAY[3,1]) WITH ORDINALITY k(x,n)",
        "generate_series(1,2) k(x)",
        "generate_subscripts(ARRAY[3,1],1) k(x)",
        "\"generate_subscripts\"(ARRAY[3,1],1) k(x)",
        "aclexplode(NULL) k",
    ] {
        for prefix in [
            "FROM",
            "FROM pg_roles CROSS JOIN",
            "FROM pg_roles LEFT JOIN",
        ] {
            let suffix = if prefix.contains("LEFT") {
                " ON true"
            } else {
                ""
            };
            let implicit = format!("SELECT k.* {prefix} {function}{suffix}");
            let explicit = format!("SELECT k.* {prefix} LATERAL {function}{suffix}");
            assert_eq!(run(&explicit), run(&implicit), "{explicit}");
        }
    }
}

#[test]
fn zip_padding_types_aliases_and_ordinality() {
    use Value::{Int as I, Null as N, Text as S};
    for name in ["unnest", "pg_catalog.unnest"] {
        for lateral in ["", "LATERAL "] {
            let r = run(&format!(
                "SELECT * FROM {lateral}{name}(ARRAY[3::int2,1::int2],ARRAY['a'::text],ARRAY[9::oid,8::oid,7::oid]) WITH ORDINALITY k(x,y,z,n)"
            ));
            assert_eq!(
                r.fields
                    .iter()
                    .map(|f| (f.name.as_str(), f.data_type))
                    .collect::<Vec<_>>(),
                [
                    ("x", PgType::Int2),
                    ("y", PgType::Text),
                    ("z", PgType::Oid),
                    ("n", PgType::Int8)
                ]
            );
            assert_eq!(
                r.rows,
                [
                    vec![I(3), S("a".into()), I(9), I(1)],
                    vec![I(1), N, I(8), I(2)],
                    vec![N, N, I(7), I(3)]
                ]
            );
        }
    }
    assert_eq!(
        run("SELECT * FROM unnest(NULL::int[],ARRAY['a','b']) k(x,y)").rows,
        [vec![N, S("a".into())], vec![N, S("b".into())]]
    );
    assert!(
        run("SELECT * FROM unnest(NULL::int[],ARRAY(SELECT rolname::text FROM pg_roles WHERE false)) WITH ORDINALITY k(x,y,n)")
            .rows
            .is_empty()
    );
    assert_eq!(
        run("SELECT * FROM unnest(ARRAY[NULL::int,2],NULL::text[]) k(x,y)").rows,
        [vec![N, N], vec![I(2), N]]
    );
}

#[test]
fn correlated_zip_and_left_join_null_extension() {
    let r = run(
        "SELECT r.rolname,k.x,k.y,k.n FROM pg_roles r CROSS JOIN LATERAL unnest(ARRAY[r.oid],ARRAY[r.rolname::text]) WITH ORDINALITY k(x,y,n)",
    );
    assert!(!r.rows.is_empty());
    for row in r.rows {
        assert_eq!(row[0], row[2]);
        assert!(matches!(row[1], Value::Int(_)));
        assert_eq!(row[3], Value::Int(1));
    }
    let r = run(
        "SELECT k.* FROM pg_roles r LEFT JOIN LATERAL unnest(NULL::int[],NULL::text[]) WITH ORDINALITY k(x,y,n) ON true",
    );
    assert!(!r.rows.is_empty());
    assert!(r.rows.iter().all(|r| r == &vec![Value::Null; 3]));
    let r = run(
        "SELECT array_agg(k.x ORDER BY k.n) FROM pg_roles CROSS JOIN LATERAL unnest(ARRAY[1]) WITH ORDINALITY k(x,n)",
    );
    assert!(
        matches!(&r.rows[0][0], Value::Array(a) if !a.is_empty() && a.iter().all(|v| *v == Value::Int(1)))
    );
    assert_eq!(
        run("SELECT k.x FROM pg_roles r, LATERAL unnest(ARRAY[r.oid]) k(x)").rows,
        run("SELECT k.x FROM pg_roles r CROSS JOIN unnest(ARRAY[r.oid]) k(x)").rows
    );
}

#[test]
fn zip_parameter_types_and_invalid_inputs() {
    let p = compile("SELECT * FROM LATERAL unnest($1::int2[],$2::text[]) WITH ORDINALITY k(x,y,n)")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), [PgType::Int2Array, PgType::TextArray]);
    let r = execute(&p, &CatalogSnapshot::default(), &[Value::Null, Value::Null]).unwrap();
    assert!(r.rows.is_empty());
    for sql in [
        "SELECT * FROM unnest(1,ARRAY[2])",
        "SELECT * FROM unnest(ARRAY[1],ARRAY[2]) k(x)",
        "SELECT * FROM LATERAL unsupported_function(1)",
        "SELECT LATERAL unnest(ARRAY[1])",
        "SELECT 1,LATERAL unnest(ARRAY[1])",
    ] {
        assert!(compile(sql).is_err(), "{sql}");
    }
    assert_eq!(
        run("SELECT 'LATERAL unnest(ARRAY[1])' FROM pg_roles LIMIT 1").rows,
        [vec![Value::Text("LATERAL unnest(ARRAY[1])".into())]]
    );
}

#[test]
fn composite_fk_operator_join_retains_each_position() {
    let mut snapshot = CatalogSnapshot::default();
    for (id, name, columns) in [(10, "parent", ["a", "b"]), (20, "child", ["x", "y"])] {
        snapshot.tables.push(TableDef {
            id,
            name: name.into(),
            primary_key: if id == 10 { vec![0, 1] } else { vec![] },
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, name)| ColumnDef {
                    id: id + i as u64 + 1,
                    name: (*name).into(),
                    data_type: ScalarType::Integer,
                    not_null: id == 10,
                })
                .collect(),
        });
    }
    snapshot.constraints.push(ConstraintDef {
        id: 30,
        table: 20,
        name: "child_parent_fk".into(),
        kind: 'f',
        columns: vec![22, 21],
        foreign_table: 10,
        foreign_columns: vec![11, 12],
        update: 'a',
        delete: 'a',
        validated: true,
        ..Default::default()
    });
    let from = "FROM pg_constraint c CROSS JOIN LATERAL unnest(c.conkey,c.confkey,c.conpfeqop) WITH ORDINALITY k(child,parent,eqop,n)";
    for join in [
        "",
        " JOIN pg_operator o ON o.oid=k.eqop JOIN pg_namespace ns ON ns.oid=o.oprnamespace",
    ] {
        let sql = format!(
            "SELECT k.child,k.parent,k.eqop,k.n {from}{join} WHERE c.contype='f' ORDER BY k.n"
        );
        let result = execute(&plan(&sql).unwrap().unwrap(), &snapshot, &[]).unwrap();
        assert_eq!(
            result.rows,
            [
                vec![Value::Int(2), Value::Int(1), Value::Int(96), Value::Int(1)],
                vec![Value::Int(1), Value::Int(2), Value::Int(96), Value::Int(2)],
            ],
            "{sql}"
        );
    }
}
