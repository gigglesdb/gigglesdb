use giggles_pg_catalog::*;
fn snapshot() -> CatalogSnapshot {
    let mut s = CatalogSnapshot::default();
    let mut app = s.roles[0].clone();
    app.oid = 42;
    app.name = "app".into();
    app.superuser = false;
    s.roles.push(app);
    s.user = "app".into();
    s.tables.push(TableDef {
        id: 7,
        name: "t".into(),
        columns: vec![ColumnDef {
            id: 11,
            name: "id".into(),
            data_type: ScalarType::Integer,
            not_null: false,
        }],
        primary_key: vec![],
    });
    s
}
fn grant(grantee: u32, privileges: Vec<AclPrivilege>, grant_option: bool) -> AclEntry {
    AclEntry {
        grantee,
        grantor: 10,
        privileges,
        grant_option,
    }
}
fn run(s: &CatalogSnapshot, expression: &str) -> Result<Value, Error> {
    let sql = format!("SELECT {expression}");
    let p = plan_gateway(&sql)?.expect("privilege function catalog route");
    Ok(execute(&p, s, &[])?.rows[0][0].clone())
}
fn yes(s: &CatalogSnapshot, expression: &str, expected: bool) {
    assert_eq!(
        run(s, expression).unwrap(),
        Value::Bool(expected),
        "{expression}"
    );
}
#[test]
fn owner_and_superuser_hold_all_table_privileges() {
    let mut s = snapshot();
    yes(&s, "has_table_privilege('t','UPDATE')", false);
    s.table_owners.insert(7, 42);
    for p in [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
        "MAINTAIN WITH GRANT OPTION",
    ] {
        yes(&s, &format!("has_table_privilege('t','{p}')"), true);
    }
    s.table_owners.clear();
    yes(&s, "has_table_privilege('giggles','t','UPDATE')", true);
    yes(&s, "has_table_privilege(10::oid,'t','UPDATE')", true);
}
#[test]
fn direct_public_and_revoked_grants() {
    let mut s = snapshot();
    s.table_acls.insert(
        7,
        vec![grant(
            42,
            vec![AclPrivilege::Select, AclPrivilege::Insert],
            false,
        )],
    );
    yes(&s, "has_table_privilege('t','SELECT')", true);
    yes(&s, "has_table_privilege('app','public.t','INSERT')", true);
    yes(&s, "has_table_privilege(42::oid,65564::oid,'INSERT')", true);
    yes(&s, "has_table_privilege('t','UPDATE')", false);
    yes(&s, "has_table_privilege('t','DELETE, insert')", true);
    s.table_acls
        .insert(7, vec![grant(0, vec![AclPrivilege::Update], false)]);
    yes(&s, "has_table_privilege('t','UPDATE')", true);
    yes(&s, "has_table_privilege('public','t','UPDATE')", true);
    yes(&s, "has_table_privilege('t','SELECT')", false);
    s.table_acls.insert(7, vec![]);
    yes(&s, "has_table_privilege('t','UPDATE')", false);
    s.table_acls.clear();
    yes(&s, "has_table_privilege('t','SELECT')", false);
}
#[test]
fn grant_option_and_invalid_names() {
    let mut s = snapshot();
    s.table_acls.insert(
        7,
        vec![
            grant(42, vec![AclPrivilege::Select], false),
            grant(0, vec![AclPrivilege::Update], true),
        ],
    );
    yes(
        &s,
        "has_table_privilege('t',' select WITH GRANT OPTION ')",
        false,
    );
    yes(
        &s,
        "has_table_privilege('t','select with grant option, update with grant option')",
        true,
    );
    yes(&s, "has_table_privilege('t','UPDATE')", true);
    let e = run(&s, "has_table_privilege('t','SELECT, invalid')").unwrap_err();
    assert_eq!(
        e,
        Error::InvalidPrivilege("unrecognized privilege type: \"invalid\"".into())
    );
    assert_eq!(e.to_string(), "unrecognized privilege type: \"invalid\"");
    assert!(matches!(
        run(&s, "has_table_privilege('hidden','RULE')"),
        Err(Error::InvalidPrivilege(_))
    ));
    assert!(matches!(
        run(&s, "has_column_privilege('t','id','DELETE')"),
        Err(Error::InvalidPrivilege(_))
    ));
}
#[test]
fn missing_hidden_null_and_quoted_resolution() {
    let mut s = snapshot();
    yes(&s, "has_table_privilege('missing','SELECT')", false);
    assert_eq!(
        run(&s, "has_table_privilege(999::oid,'SELECT')").unwrap(),
        Value::Null
    );
    assert_eq!(
        run(&s, "has_table_privilege(NULL::text,'SELECT')").unwrap(),
        Value::Null
    );
    assert_eq!(
        run(&s, "has_table_privilege('t',NULL::text)").unwrap(),
        Value::Null
    );
    s.user = "giggles".into();
    s.tables[0].name = "T.with.dot".into();
    yes(
        &s,
        "has_table_privilege('public.\"T.with.dot\"','SELECT')",
        true,
    );
    yes(&s, "has_table_privilege('T.with.dot','SELECT')", false);
    s.schemas.insert("custom".into(), (70000, 10));
    s.table_schemas.insert(7, "custom".into());
    yes(&s, "has_table_privilege('\"T.with.dot\"','SELECT')", false);
    s.search_path = vec!["custom".into()];
    yes(&s, "has_table_privilege('\"T.with.dot\"','SELECT')", true);
    s.tables.clear();
    s.table_schemas.clear();
    yes(
        &s,
        "has_table_privilege('custom.\"T.with.dot\"','SELECT')",
        false,
    );
    assert_eq!(
        run(&s, "has_table_privilege(65564::oid,'SELECT')").unwrap(),
        Value::Null
    );
}
#[test]
fn column_forms_table_and_column_grants() {
    let mut s = snapshot();
    s.table_acls
        .insert(7, vec![grant(42, vec![AclPrivilege::Select], false)]);
    yes(&s, "has_column_privilege('t','id','SELECT')", true);
    yes(
        &s,
        "has_column_privilege('app','t',1::smallint,'SELECT')",
        true,
    );
    yes(
        &s,
        "has_column_privilege(42::oid,65564::oid,1::smallint,'SELECT')",
        true,
    );
    yes(&s, "has_column_privilege('t','id','UPDATE')", false);
    s.column_acls.insert(
        (7, 11),
        vec![
            grant(42, vec![AclPrivilege::Update], true),
            grant(0, vec![AclPrivilege::References], false),
        ],
    );
    yes(
        &s,
        "has_column_privilege('t','id','UPDATE WITH GRANT OPTION')",
        true,
    );
    yes(&s, "has_column_privilege('t','id','REFERENCES')", true);
    yes(&s, "has_column_privilege('t','id','INSERT')", false);
    yes(
        &s,
        "has_column_privilege('t','id','INSERT, REFERENCES')",
        true,
    );
    assert_eq!(
        run(&s, "has_column_privilege('t','absent','SELECT')"),
        Err(Error::UnknownColumn)
    );
    assert_eq!(
        run(&s, "has_column_privilege('t',99::smallint,'SELECT')").unwrap(),
        Value::Null
    );
    assert_eq!(
        run(&s, "has_column_privilege(999::oid,'id','SELECT')").unwrap(),
        Value::Null
    );
    yes(&s, "has_column_privilege('hidden','id','SELECT')", false);
}
#[test]
fn acl_columns_preserve_null_empty_and_quoted_roles() {
    let mut s = snapshot();
    let sql = "SELECT relacl FROM pg_catalog.pg_class WHERE relname='t'";
    let p = plan(sql).unwrap().unwrap();
    assert_eq!(execute(&p, &s, &[]).unwrap().rows, vec![vec![Value::Null]]);
    s.table_acls.insert(7, vec![]);
    assert_eq!(
        execute(&p, &s, &[]).unwrap().rows,
        vec![vec![Value::Array(vec![])]]
    );
    s.roles[1].name = "a=\"/b".into();
    s.user = s.roles[1].name.clone();
    s.table_acls
        .insert(7, vec![grant(42, vec![AclPrivilege::Select], true)]);
    yes(
        &s,
        "has_table_privilege('t','SELECT WITH GRANT OPTION')",
        true,
    );
    let p = plan("SELECT a.grantee,a.privilege_type,a.is_grantable FROM pg_catalog.pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.relname='t'").unwrap().unwrap();
    assert_eq!(
        execute(&p, &s, &[]).unwrap().rows,
        vec![vec![
            Value::Int(42),
            Value::Text("SELECT".into()),
            Value::Bool(true)
        ]]
    );
    s.column_acls
        .insert((7, 11), vec![grant(42, vec![AclPrivilege::Update], false)]);
    let p = plan("SELECT attacl FROM pg_catalog.pg_attribute WHERE attrelid=65564 AND attnum=1")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &s, &[]).unwrap().rows,
        vec![vec![Value::Array(vec![Value::Text(
            "\"a=\"\"/b\"=w/giggles".into()
        )])]]
    );
}
#[test]
fn invalid_acl_inputs_are_rejected() {
    let mut s = snapshot();
    s.table_acls.insert(8, vec![]);
    assert_eq!(
        run(&s, "has_table_privilege('t','SELECT')"),
        Err(Error::InvalidSnapshot)
    );
    s.table_acls.clear();
    s.column_acls.insert((7, 12), vec![]);
    assert_eq!(
        run(&s, "has_table_privilege('t','SELECT')"),
        Err(Error::InvalidSnapshot)
    );
    s.column_acls.clear();
    s.table_acls
        .insert(7, vec![grant(999, vec![AclPrivilege::Select], false)]);
    assert_eq!(
        run(&s, "has_table_privilege('t','SELECT')"),
        Err(Error::InvalidSnapshot)
    );
    s.table_acls.clear();
    s.column_acls
        .insert((7, 11), vec![grant(42, vec![AclPrivilege::Delete], false)]);
    assert_eq!(
        run(&s, "has_table_privilege('t','SELECT')"),
        Err(Error::InvalidSnapshot)
    );
}

#[test]
fn role_privilege_signatures_lists_and_strictness() {
    let mut s = snapshot();
    s.role_privileges.insert(("app".into(), "app".into()), [true, true, true, false]);
    for sql in ["pg_has_role('app','app','MEMBER')", "pg_has_role(42::oid,'app','USAGE')",
        "pg_has_role('app',42::oid,'SET')", "pg_has_role(42::oid,42::oid,'MEMBER')",
        "pg_has_role('app','USAGE')", "pg_has_role(42::oid,'SET')",
        "pg_has_role('app','app',' usage , MEMBER WITH ADMIN OPTION')"] {
        yes(&s, sql, true);
    }
    for sql in ["pg_has_role('app','app','MEMBER WITH ADMIN OPTION')",
        "pg_has_role('app','app','SET WITH GRANT OPTION')", "pg_has_role(999999::oid,'app','USAGE')",
        "pg_has_role('app',999999::oid,'USAGE')"] { yes(&s, sql, false); }
    assert!(matches!(run(&s, "pg_has_role('app','app',' bogus ')") , Err(Error::InvalidPrivilege(message)) if message == "unrecognized privilege type: \"bogus\""));
    for privilege in ["", "MEMBER,", "SELECT"] {
        let sql = format!("pg_has_role('app','app','{privilege}')");
        assert!(matches!(run(&s, &sql), Err(Error::InvalidPrivilege(_))));
    }
    assert!(matches!(run(&s, "pg_has_role('public','app','MEMBER')"), Err(Error::UnknownRole(_))));
    assert_eq!(run(&s, "pg_has_role(NULL,'app','MEMBER')").unwrap(), Value::Null);
}

#[test]
fn role_privilege_unnest_route() {
    let mut s = snapshot();
    s.role_privileges.insert(("app".into(), "app".into()), [true, true, true, false]);
    let sql = "SELECT pg_has_role('app', r, 'MEMBER') FROM unnest(ARRAY['app']) r";
    let p = plan_gateway(sql).unwrap().expect("role privilege unnest route");
    assert_eq!(execute(&p, &s, &[]).unwrap().rows, vec![vec![Value::Bool(true)]]);
}
