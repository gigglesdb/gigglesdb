use giggles_pg_catalog::*;
fn run(expression: &str) -> Result<Value, Error> {
    let sql = format!("SELECT {expression} FROM pg_namespace WHERE oid=11");
    Ok(execute(&plan(&sql)?.unwrap(), &CatalogSnapshot::default(), &[])?.rows[0][0].clone())
}
#[test]
fn regex_are_subset_and_collation() {
    for expression in [
        "'hello' ~ '^h(ell|all)o$'",
        "'Hello' ~* '^hello$'",
        "'Hello' !~ '^hello$'",
        "'Hello' !~* '^world$'",
        "'abc' OPERATOR(pg_catalog.~) '^[a-c].*$' COLLATE pg_catalog.default",
        "'a.b' ~ '^a[.]b$'",
        "'a' COLLATE \"C\" = 'a'",
        "'a' = 'a' COLLATE \"default\"",
        "E'a\\nb' ~ '^a.b$'",
        "'abc' OPERATOR(pg_catalog.!~*) '^xyz$'",
    ] {
        assert_eq!(run(expression), Ok(Value::Bool(true)), "{expression}");
    }
    assert_eq!(run("NULL ~ '^x$'"), Ok(Value::Null));
    assert_eq!(run("'x' ~ ('^x$' COLLATE \"C\")"), Ok(Value::Bool(true)));
    for expression in [
        "'a' ~ '(?i)a'",
        "'a' ~ '[[:alpha:]]'",
        "'a' ~ 'a+'",
        "'a' ~ E'\\\\w'",
        "'a' = 'a' COLLATE \"en_US\"",
    ] {
        assert_eq!(run(expression), Err(Error::Unsupported), "{expression}");
    }
    assert!(run("1 = 1 COLLATE \"C\"").is_err());
    assert!(run("'a' COLLATE \"C\"").is_err());
}
#[test]
fn scalar_catalog_functions() {
    assert_eq!(run("pg_table_is_visible(11)"), Ok(Value::Null));
    assert_eq!(
        run("pg_table_is_visible('pg_class'::regclass)"),
        Ok(Value::Bool(true))
    );
    assert_eq!(run("pg_table_is_visible(NULL)"), Ok(Value::Null));
    assert_eq!(
        run("pg_encoding_to_char(6)"),
        Ok(Value::Text("UTF8".into()))
    );
    assert_eq!(run("pg_encoding_to_char(-1)"), Ok(Value::Text("".into())));
    assert_eq!(run("pg_encoding_to_char(NULL)"), Ok(Value::Null));
    assert_eq!(
        run("array_to_string(ARRAY['','x',NULL,''], ',')"),
        Ok(Value::Text(",x,".into()))
    );
    assert_eq!(
        run("array_to_string(ARRAY[1,NULL,2], ',', 'nil')"),
        Ok(Value::Text("1,nil,2".into()))
    );
    assert_eq!(
        run("array_to_string(ARRAY[true,false], ',')"),
        Ok(Value::Text("t,f".into()))
    );
    assert_eq!(run("array_length(ARRAY[1,2], 1)"), Ok(Value::Int(2)));
    assert_eq!(run("array_length(ARRAY[1,2], 2)"), Ok(Value::Null));
}
#[test]
fn primary_key_deparsers_quote_identifiers_and_refresh() {
    let snapshot = CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "Order".into(),
            columns: vec![ColumnDef {
                id: 11,
                name: "select".into(),
                data_type: ScalarType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    };
    let plan = plan("SELECT pg_get_indexdef(i.indexrelid,0,true), pg_get_constraintdef(c.oid,true) FROM pg_index i JOIN pg_constraint c ON c.conindid=i.indexrelid").unwrap().unwrap();
    let output = execute(&plan, &snapshot, &[]).unwrap();
    assert_eq!(
        output.rows,
        vec![vec![
            Value::Text(
                "CREATE UNIQUE INDEX \"Order_pkey\" ON \"Order\" USING btree (\"select\")".into()
            ),
            Value::Text("PRIMARY KEY (\"select\")".into())
        ]]
    );
    assert!(
        execute(&plan, &CatalogSnapshot::default(), &[])
            .unwrap()
            .rows
            .is_empty()
    );
}
#[test]
fn string_aggregate_null_and_empty_semantics() {
    assert_eq!(run("string_agg(NULL, ',')"), Ok(Value::Null));
    assert_eq!(run("string_agg('x', NULL)"), Ok(Value::Text("x".into())));
    assert_eq!(run("string_agg('', ',')"), Ok(Value::Text("".into())));
    let query = plan("SELECT string_agg(nspname, NULL) FROM pg_namespace WHERE oid IN (11,2200)")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&query, &CatalogSnapshot::default(), &[])
            .unwrap()
            .rows,
        vec![vec![Value::Text("pg_catalogpublic".into())]]
    );
}
#[test]
fn publishable_visibility_and_index_column_bounds() {
    let snapshot = CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "items".into(),
            columns: vec![ColumnDef {
                id: 11,
                name: "id".into(),
                data_type: ScalarType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    };
    let query = plan("SELECT pg_relation_is_publishable(i.indrelid),pg_relation_is_publishable(i.indexrelid),pg_get_indexdef(i.indexrelid,-1,true),pg_get_indexdef(i.indexrelid,2,true) FROM pg_index i").unwrap().unwrap();
    assert_eq!(
        execute(&query, &snapshot, &[]).unwrap().rows,
        vec![vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Text("".into()),
            Value::Text("".into())
        ]]
    );
    assert_eq!(
        run("pg_relation_is_publishable('pg_class'::regclass)"),
        Ok(Value::Bool(false))
    );
    assert_eq!(run("pg_relation_is_publishable(999999)"), Ok(Value::Null));
}

#[test]
fn deparser_uses_postgres_keyword_categories() {
    for name in ["check", "primary", "between", "authorization", "id", "name"] {
        let snapshot = CatalogSnapshot {
            tables: vec![TableDef {
                id: 7,
                name: "items".into(),
                columns: vec![ColumnDef {
                    id: 11,
                    name: name.into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                }],
                primary_key: vec![0],
            }],
            ..CatalogSnapshot::default()
        };
        let query = plan("SELECT pg_get_indexdef(indexrelid,0,true) FROM pg_index")
            .unwrap()
            .unwrap();
        let quoted = if matches!(name, "id" | "name") {
            name.to_owned()
        } else {
            format!("\"{name}\"")
        };
        assert_eq!(
            execute(&query, &snapshot, &[]).unwrap().rows,
            vec![vec![Value::Text(format!(
                "CREATE UNIQUE INDEX items_pkey ON items USING btree ({quoted})"
            ))]],
            "{name}"
        );
    }
}
