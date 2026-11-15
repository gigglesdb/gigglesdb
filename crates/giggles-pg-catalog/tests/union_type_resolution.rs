use giggles_pg_catalog::*;

fn run(sql: &str, ty: PgType, rows: Vec<Vec<Value>>) {
    let p = plan(sql).unwrap().expect("catalog query");
    let fields = describe(&p).unwrap();
    assert_eq!(fields[0].data_type, ty, "{sql}");
    let result = execute(&p, &CatalogSnapshot::default(), &[]).unwrap();
    assert_eq!(result.fields, fields);
    assert_eq!(result.rows, rows, "{sql}");
}
fn text(s: &str) -> Vec<Value> {
    vec![Value::Text(s.into())]
}

#[test]
fn name_and_unknown_literal_resolve_to_name_in_either_order() {
    run(
        "SELECT rolname FROM pg_roles WHERE oid=10 UNION ALL SELECT ''",
        PgType::Name,
        vec![text("giggles"), text("")],
    );
    run(
        "SELECT '' UNION ALL SELECT rolname FROM pg_roles WHERE oid=10",
        PgType::Name,
        vec![text(""), text("giggles")],
    );
}

#[test]
fn mixed_string_types_resolve_to_text() {
    for sql in [
        "SELECT rolname FROM pg_roles WHERE oid=10 UNION ALL SELECT 'other'::text",
        "SELECT 'giggles'::text FROM pg_database UNION ALL SELECT 'other'::varchar",
        "SELECT 'giggles'::text FROM pg_database UNION ALL SELECT 'other'::text",
    ] {
        run(sql, PgType::Text, vec![text("giggles"), text("other")]);
    }
}

#[test]
fn varchar_column_and_text_resolve_to_text() {
    let fields = vec![Field {
        name: "v".into(),
        data_type: PgType::Varchar,
    }];
    let relations = [("union_values".into(), fields.clone())].into();
    let p = plan_gateway_with_relations(
        "SELECT v FROM union_values UNION ALL SELECT 'text'::text FROM pg_database",
        &relations,
    )
    .unwrap()
    .unwrap();
    let mut snapshot = CatalogSnapshot::default();
    snapshot.user_relations.insert(
        "union_values".into(),
        ResultSet {
            fields,
            rows: vec![text("varchar")],
        },
    );
    let result = execute(&p, &snapshot, &[]).unwrap();
    assert_eq!(result.fields, describe(&p).unwrap());
    assert_eq!(result.fields[0].data_type, PgType::Text);
    assert_eq!(result.rows, vec![text("varchar"), text("text")]);
}

#[test]
fn integer_widening_preserves_values() {
    for sql in [
        "SELECT 1 FROM pg_database UNION ALL SELECT 2147483648",
        "SELECT 2147483648 FROM pg_database UNION ALL SELECT 1",
    ] {
        let rows = if sql.starts_with("SELECT 1 ") {
            vec![vec![Value::Int(1)], vec![Value::Int(2147483648)]]
        } else {
            vec![vec![Value::Int(2147483648)], vec![Value::Int(1)]]
        };
        run(sql, PgType::Int8, rows);
    }
}

#[test]
fn integer_and_numeric_coerce_result_values() {
    let n = |s| vec![Value::Numeric(giggles_pg_types::Numeric::parse(s).unwrap())];
    run(
        "SELECT 1 FROM pg_database UNION ALL SELECT '2.5'::numeric",
        PgType::Numeric,
        vec![n("1"), n("2.5")],
    );
    run(
        "SELECT '2.5'::numeric FROM pg_database UNION ALL SELECT 1",
        PgType::Numeric,
        vec![n("2.5"), n("1")],
    );
}

#[test]
fn numeric_and_float_widening_coerce_result_values() {
    run(
        "SELECT '1.25'::numeric FROM pg_database UNION ALL SELECT '2.5'::float4",
        PgType::Float4,
        ["1.25", "2.5"]
            .into_iter()
            .map(|s| vec![Value::Float4(giggles_pg_types::Float4::parse(s).unwrap())])
            .collect(),
    );
    // Widen the binary float4 value, rather than reinterpreting its shortest
    // decimal spelling as a different double precision value.
    let widened = f64::from(giggles_pg_types::Float4::parse("1.1").unwrap().value()).to_string();
    run(
        "SELECT '1.1'::float4 FROM pg_database UNION ALL SELECT '2.5'::float8",
        PgType::Float8,
        [widened.as_str(), "2.5"]
            .into_iter()
            .map(|s| vec![Value::Float8(giggles_pg_types::Float8::parse(s).unwrap())])
            .collect(),
    );
}

#[test]
fn null_arm_and_all_unknown() {
    run(
        "SELECT NULL FROM pg_database UNION ALL SELECT 7",
        PgType::Int4,
        vec![vec![Value::Null], vec![Value::Int(7)]],
    );
    run(
        "SELECT 7 FROM pg_database UNION ALL SELECT NULL",
        PgType::Int4,
        vec![vec![Value::Int(7)], vec![Value::Null]],
    );
    run(
        "SELECT NULL FROM pg_database UNION ALL SELECT 'x'",
        PgType::Text,
        vec![vec![Value::Null], text("x")],
    );
    run(
        "SELECT NULL FROM pg_database UNION ALL SELECT NULL",
        PgType::Text,
        vec![vec![Value::Null], vec![Value::Null]],
    );
}

#[test]
fn three_arm_resolution_and_first_label() {
    let sql = "SELECT 1 AS first FROM pg_database UNION ALL SELECT 2147483648 AS later UNION ALL SELECT '3.5'::numeric";
    let p = plan(sql).unwrap().unwrap();
    assert_eq!(describe(&p).unwrap()[0].name, "first");
    run(
        sql,
        PgType::Numeric,
        ["1", "2147483648", "3.5"]
            .into_iter()
            .map(|s| vec![Value::Numeric(giggles_pg_types::Numeric::parse(s).unwrap())])
            .collect(),
    );
    // A resolved inner UNION is text, no longer an unknown NULL literal.
    assert_eq!(
        plan("SELECT NULL FROM pg_database UNION ALL SELECT NULL UNION ALL SELECT 1").unwrap_err(),
        Error::Type
    );
}

#[test]
fn distinct_deduplicates_after_coercion() {
    for (operator, count) in [("UNION", 1), ("UNION ALL", 2)] {
        run(
            &format!("SELECT 1 FROM pg_database {operator} SELECT '1'::numeric"),
            PgType::Numeric,
            vec![
                vec![Value::Numeric(
                    giggles_pg_types::Numeric::parse("1").unwrap()
                )];
                count
            ],
        );
    }
}

#[test]
fn incompatible_types_and_width_still_fail() {
    for sql in [
        "SELECT true FROM pg_database UNION ALL SELECT 1",
        "SELECT 1 FROM pg_database UNION SELECT '1'::text",
        "SELECT 1 FROM pg_database UNION ALL SELECT 1,2",
    ] {
        assert_eq!(plan(sql).unwrap_err(), Error::Type, "{sql}");
    }
}

#[test]
fn unknown_input_is_parsed_as_the_selected_type() {
    run(
        "SELECT 1 FROM pg_database UNION ALL SELECT '2'",
        PgType::Int4,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]],
    );
    assert_eq!(
        plan("SELECT 1 FROM pg_database UNION ALL SELECT 'bad'").unwrap_err(),
        Error::Type
    );
}

#[test]
fn aclexplode_from_forms_reuse_projection_evaluator() {
    let snapshot = CatalogSnapshot::default();
    let projected = plan("SELECT (aclexplode(acldefault('r',10))).grantee FROM pg_database")
        .unwrap()
        .unwrap();
    let expected = execute(&projected, &snapshot, &[]).unwrap();
    assert_eq!(expected.rows, vec![vec![Value::Int(10)]; 8]);
    for function in [
        "aclexplode",
        "LATERAL aclexplode",
        "LATERAL pg_catalog.aclexplode",
    ] {
        let sql = format!(
            "SELECT x.grantor,x.grantee,x.privilege_type,x.is_grantable FROM pg_database CROSS JOIN {function}(acldefault('r',10)) x"
        );
        let p = plan(&sql).unwrap().unwrap();
        let result = execute(&p, &snapshot, &[]).unwrap();
        assert_eq!(result.fields, describe(&p).unwrap());
        assert_eq!(
            result
                .fields
                .iter()
                .map(|f| f.data_type)
                .collect::<Vec<_>>(),
            vec![PgType::Oid, PgType::Oid, PgType::Text, PgType::Bool]
        );
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| vec![r[1].clone()])
                .collect::<Vec<_>>(),
            expected.rows
        );
        assert!(
            result
                .rows
                .iter()
                .all(|r| r[0] == Value::Int(10) && r[3] == Value::Bool(false))
        );
    }
    run(
        "SELECT x.grantee FROM pg_class c LEFT JOIN LATERAL aclexplode(c.relacl) x ON true WHERE c.relname='pg_class'",
        PgType::Oid,
        vec![vec![Value::Null]],
    );
}

#[test]
fn full_save_with_nonlogin_role_and_database_owner_membership() {
    let mut snapshot = CatalogSnapshot::default();
    let mut nonlogin = snapshot.roles[0].clone();
    nonlogin.oid = 10000;
    nonlogin.name = "e2c_nonlogin".into();
    nonlogin.login = false;
    nonlogin.superuser = false;
    nonlogin.bypassrls = false;
    snapshot.roles.push(nonlogin);
    // The Save query explicitly materializes PostgreSQL's implicit membership
    // of the database owner in pg_database_owner. Explicit pg_auth_members
    // rows cannot yet be supplied through CatalogSnapshot.
    snapshot.database_owner = 10000;
    snapshot.tables.push(TableDef {
        id: 1,
        name: "e1_a".into(),
        columns: vec![ColumnDef {
            id: 2,
            name: "id".into(),
            data_type: ScalarType::BigInt,
            not_null: true,
        }],
        primary_key: vec![0],
    });
    let sql = r#"SELECT 'role',rolname,rolcanlogin::text,rolsuper::text,rolbypassrls::text,'','','','' FROM pg_roles UNION ALL SELECT 'member',pg_get_userbyid(member),pg_get_userbyid(roleid),'','','','','','' FROM pg_auth_members UNION ALL SELECT 'member',pg_get_userbyid(datdba),'pg_database_owner','','','','','','' FROM pg_database WHERE datname=current_database() UNION ALL SELECT 'table',n.nspname,c.relname,pg_get_userbyid(c.relowner),'','','','','' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid IN (E'"public"."e1_a"'::regclass) UNION ALL SELECT 'grant',n.nspname,c.relname,pg_get_userbyid(c.relowner),'',CASE WHEN x.grantee=0 THEN 'public' ELSE 'role' END,CASE WHEN x.grantee=0 THEN '' ELSE pg_get_userbyid(x.grantee) END,pg_get_userbyid(x.grantor),x.privilege_type||':'||x.is_grantable::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) x WHERE c.oid IN (E'"public"."e1_a"'::regclass) AND x.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE') UNION ALL SELECT 'grant',n.nspname,c.relname,pg_get_userbyid(c.relowner),a.attname,CASE WHEN x.grantee=0 THEN 'public' ELSE 'role' END,CASE WHEN x.grantee=0 THEN '' ELSE pg_get_userbyid(x.grantee) END,pg_get_userbyid(x.grantor),x.privilege_type||':'||x.is_grantable::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped CROSS JOIN LATERAL aclexplode(a.attacl) x WHERE c.oid IN (E'"public"."e1_a"'::regclass) AND x.privilege_type IN ('INSERT','UPDATE')"#;
    let p = plan(sql).unwrap().unwrap();
    let result = execute(&p, &snapshot, &[]).unwrap();
    assert_eq!(result.fields, describe(&p).unwrap());
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        vec![
            PgType::Text,
            PgType::Name,
            PgType::Text,
            PgType::Text,
            PgType::Text,
            PgType::Text,
            PgType::Text,
            PgType::Text,
            PgType::Text
        ]
    );
    assert_eq!(result.rows.len(), 24);
    let row = |cells: &[&str]| {
        cells
            .iter()
            .map(|s| Value::Text((*s).into()))
            .collect::<Vec<_>>()
    };
    assert!(result.rows.contains(&row(&[
        "role",
        "e2c_nonlogin",
        "false",
        "false",
        "false",
        "",
        "",
        "",
        ""
    ])));
    assert!(result.rows.contains(&row(&[
        "member",
        "e2c_nonlogin",
        "pg_database_owner",
        "",
        "",
        "",
        "",
        "",
        ""
    ])));
    assert!(result.rows.contains(&row(&[
        "table", "public", "e1_a", "giggles", "", "", "", "", ""
    ])));
    for privilege in [
        "INSERT:false",
        "UPDATE:false",
        "DELETE:false",
        "TRUNCATE:false",
    ] {
        assert!(result.rows.contains(&row(&[
            "grant", "public", "e1_a", "giggles", "", "role", "giggles", "giggles", privilege
        ])));
    }
}
