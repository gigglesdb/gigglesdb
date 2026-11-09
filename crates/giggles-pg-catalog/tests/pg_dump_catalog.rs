use giggles_pg_catalog::*;

fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("catalog query"),
        &CatalogSnapshot::default(),
        &[],
    )
}

#[test]
fn namespace_inventory_computes_defaults_without_fabricating_acl_metadata() {
    let result = run("SELECT n.tableoid, n.oid, n.nspname, n.nspowner, n.nspacl, acldefault('n', n.nspowner) AS acldefault FROM pg_namespace n").unwrap();
    assert_eq!(result.fields[4].data_type, PgType::AclItemArray);
    assert_eq!(result.fields[5].data_type, PgType::AclItemArray);
    assert_eq!(result.fields[5].data_type.oid(), 1034);
    assert_eq!(result.rows.len(), 3);
    for row in result.rows {
        let owner = if row[3] == Value::Int(6171) {
            "pg_database_owner"
        } else {
            assert_eq!(row[3], Value::Int(10));
            "giggles"
        };
        assert_eq!(row[0], Value::Int(2615));
        assert_eq!(row[4], Value::Null);
        assert_eq!(
            row[5],
            Value::Array(vec![Value::Text(format!("{owner}=UC/{owner}"))])
        );
    }
}

#[test]
fn schema_acl_defaults_are_strict_and_resolve_owner_names() {
    let result = run("SELECT pg_catalog.acldefault(NULL,10), acldefault('n',NULL), acldefault('n',6171), acldefault('n',4294967295), acldefault('n',0)").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Null,
            Value::Null,
            Value::Array(vec![Value::Text(
                "pg_database_owner=UC/pg_database_owner".into()
            )]),
            Value::Array(vec![Value::Text("4294967295=UC/4294967295".into())]),
            Value::Array(vec![Value::Text("=UC/0".into())]),
        ]]
    );
    assert!(
        result
            .fields
            .iter()
            .all(|f| f.data_type == PgType::AclItemArray)
    );
    assert!(matches!(
        run("SELECT acldefault('r',10)"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan("SELECT acldefault('n')"),
        Err(Error::Unsupported)
    ));
}

#[test]
fn schema_acl_defaults_quote_acl_role_names() {
    // ACL quoting differs from SQL identifier quoting: uppercase, leading
    // digits and keywords are safe; non-ASCII and punctuation require quotes.
    for (name, encoded) in [
        ("SELECT", "SELECT"),
        ("123owner", "123owner"),
        ("a b", "\"a b\""),
        ("a\"b", "\"a\"\"b\""),
        ("a\\b", "\"a\\b\""),
        ("é", "\"é\""),
    ] {
        let mut snapshot = CatalogSnapshot::default();
        snapshot.roles[0].name = name.into();
        let result = execute(
            &plan("SELECT acldefault('n',10)").unwrap().unwrap(),
            &snapshot,
            &[],
        )
        .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Value::Array(vec![Value::Text(format!(
                "{encoded}=UC/{encoded}"
            ))])]],
            "{name}"
        );
    }
}

#[test]
fn extension_inventory_matches_no_installed_extensions() {
    let result = run("SELECT x.tableoid, x.oid, x.extname, n.nspname, x.extrelocatable, x.extversion, x.extconfig, x.extcondition FROM pg_extension x JOIN pg_namespace n ON n.oid = x.extnamespace").unwrap();
    assert!(result.rows.is_empty());
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        vec![
            PgType::Oid,
            PgType::Oid,
            PgType::Name,
            PgType::Name,
            PgType::Bool,
            PgType::Text,
            PgType::OidArray,
            PgType::TextArray
        ]
    );
}

#[test]
fn tableoid_binds_as_oid_and_preserves_outer_join_nulls() {
    let result = run("SELECT n.tableoid,n.oid,n.nspname,x.tableoid FROM pg_namespace n LEFT JOIN pg_extension x ON n.oid=x.extnamespace WHERE n.nspname='public'").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(2615),
            Value::Int(2200),
            Value::Text("public".into()),
            Value::Null
        ]]
    );
    assert_eq!(result.fields[0].data_type, PgType::Oid);
    assert_eq!(result.fields[3].data_type, PgType::Oid);
    assert!(matches!(
        plan("SELECT tableoid FROM pg_namespace n JOIN pg_extension x ON true"),
        Err(Error::AmbiguousColumn)
    ));
    assert!(matches!(
        plan("SELECT pg_namespace.tableoid FROM pg_namespace n"),
        Err(Error::UnknownColumn)
    ));
    assert!(matches!(
        plan("SELECT n.tableoid FROM (SELECT oid FROM pg_namespace) n"),
        Err(Error::UnknownColumn)
    ));
}

#[test]
fn tableoid_is_hidden_from_stars_views_and_ordinary_attributes() {
    let result = run("SELECT * FROM pg_extension").unwrap();
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "oid",
            "extname",
            "extowner",
            "extnamespace",
            "extrelocatable",
            "extversion",
            "extconfig",
            "extcondition",
            "xmin"
        ]
    );
    let comma = run("SELECT * FROM pg_namespace n, pg_extension x").unwrap();
    assert!(!comma.fields.iter().any(|f| f.name == "tableoid"));
    let natural = run("SELECT * FROM pg_namespace n NATURAL JOIN pg_extension x").unwrap();
    assert!(!natural.fields.iter().any(|f| f.name == "tableoid"));
    for relation in ["pg_roles", "pg_indexes", "information_schema.tables"] {
        assert!(
            matches!(
                plan(&format!("SELECT tableoid FROM {relation}")),
                Err(Error::UnknownColumn)
            ),
            "{relation}"
        );
    }
    assert!(run("SELECT attname FROM pg_attribute WHERE attrelid=3079 AND attname='tableoid' AND attnum>0").unwrap().rows.is_empty());
}
