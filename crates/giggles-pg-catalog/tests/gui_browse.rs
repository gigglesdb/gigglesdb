use giggles_pg_catalog::*;

fn run(sql: &str, snapshot: &CatalogSnapshot) -> ResultSet {
    execute(&plan(sql).unwrap().expect("catalog"), snapshot, &[]).unwrap()
}

#[test]
fn qualified_wildcard_excludes_system_columns_and_preserves_duplicates() {
    let snapshot = CatalogSnapshot::default();
    let result = run(
        "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname",
        &snapshot,
    );
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["oid", "oid", "nspname", "nspowner", "nspacl", "description"]
    );
    assert!(!result.rows.is_empty());
    assert!(
        result
            .rows
            .iter()
            .all(|r| r[0] == r[1] && r[5] == Value::Null)
    );
    assert_eq!(
        plan("SELECT missing.* FROM pg_namespace n").unwrap_err(),
        Error::UnknownColumn
    );
}

#[test]
fn pgadmin_connection_catalog_queries() {
    let mut snapshot = CatalogSnapshot {
        database: "postgres".into(),
        backend_pid: 123,
        ..Default::default()
    };
    let result = run(
        "SELECT db.oid, db.datname, has_database_privilege(db.oid,'CREATE'), db.dattablespace FROM pg_catalog.pg_database db WHERE db.datname=current_database()",
        &snapshot,
    );
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(16384),
            Value::Text(snapshot.database.clone()),
            Value::Bool(true),
            Value::Int(1663)
        ]]
    );
    assert_eq!(run("SELECT gss_authenticated, encrypted FROM pg_catalog.pg_stat_gssapi WHERE pid=pg_backend_pid()", &snapshot).rows, vec![vec![Value::Bool(false), Value::Bool(false)]]);
    assert_eq!(run("SELECT CASE WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0 THEN 'pgd' WHEN (SELECT count(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END AS type", &snapshot).rows, vec![vec![Value::Null]]);
    assert_eq!(run("SELECT CASE WHEN usesuper AND pg_is_in_recovery() THEN pg_is_wal_replay_paused() ELSE FALSE END FROM pg_user WHERE usename=current_user", &snapshot).rows, vec![vec![Value::Bool(false)]]);
    assert_eq!(run("SELECT t.spcname, pg_tablespace_location(t.oid), d.description FROM pg_database b JOIN pg_tablespace t ON t.oid=b.dattablespace JOIN pg_shdescription d ON d.objoid=b.oid AND d.classoid='pg_database'::regclass", &snapshot).rows, vec![vec![Value::Text("pg_default".into()), Value::Text(String::new()), Value::Text("default administrative connection database".into())]]);
    snapshot.roles[0].superuser = false;
    snapshot.database_owner = 999;
    let mut owner = snapshot.roles[0].clone();
    owner.oid = 999;
    owner.name = "owner".into();
    snapshot.roles.push(owner);
    assert_eq!(run("SELECT has_database_privilege(16384::oid,'CREATE'), has_database_privilege(16384::oid,'CONNECT'), has_database_privilege(0::oid,'CREATE')", &snapshot).rows, vec![vec![Value::Bool(false), Value::Bool(true), Value::Null]]);
}

#[test]
fn pgadmin_role_recursion_stops_at_absent_memberships() {
    let result = run(
        "SELECT roles.rolname, 'pg_signal_backend'=ANY(ARRAY(WITH RECURSIVE cte AS (SELECT pg_roles.oid,pg_roles.rolname FROM pg_roles WHERE pg_roles.oid=roles.oid UNION ALL SELECT m.roleid,pgr.rolname FROM cte cte_1 JOIN pg_auth_members m ON m.member=cte_1.oid JOIN pg_roles pgr ON pgr.oid=m.roleid) SELECT rolname FROM cte)) FROM pg_roles roles WHERE rolname=current_user",
        &CatalogSnapshot::default(),
    );
    assert_eq!(
        result.rows,
        vec![vec![Value::Text("giggles".into()), Value::Bool(false)]]
    );
    assert!(plan("WITH RECURSIVE cte AS (SELECT oid FROM pg_roles UNION ALL SELECT oid FROM cte) SELECT * FROM cte").is_err());
}
