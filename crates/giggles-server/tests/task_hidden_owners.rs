#![cfg(feature = "engine-rocksdb")]
#[path = "r3_support/mod.rs"]
mod support;
use support::R3Fixture;
use giggles_core::roles::{NewRole, MembershipOptions, BOOTSTRAP_GRANTOR};
use tokio_postgres::{Client, SimpleQueryMessage};

async fn answer(c: &Client, expression: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let source = if expression.contains(" FROM ") { "" } else { " FROM pg_class LIMIT 1" };
    c.simple_query(&format!("SELECT {expression}{source}")).await
        .map(|rows| rows.into_iter().filter_map(|row| match row {
            SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect()),
            _ => None,
        }).collect()).map_err(|e| e.code().map_or_else(|| e.to_string(), |s| s.code().to_owned()))
}
fn role(f: &R3Fixture, name: &str) -> u32 {
    f.admin().roles().create(NewRole {name: name.into(), ..Default::default()}).unwrap();
    f.admin().roles().get(name).unwrap().unwrap().oid
}
async fn schema_fixture() -> (R3Fixture, Client, u32) {
    let f = R3Fixture::start().await;
    let hidden = role(&f, "h");
    role(&f, "r");
    let mut ddl = f.admin().sql_session();
    ddl.set_ddl_owner(Some("h"));
    ddl.execute("CREATE COLLATION unused FROM \"C\"", &[], None).unwrap();
    ddl.execute("CREATE COLLATION used FROM \"C\"", &[], None).unwrap();
    ddl.execute("ALTER TABLE shipments ALTER COLUMN customer_email TYPE text COLLATE used", &[], None).unwrap();
    let extension = ddl.prepare("CREATE EXTENSION \"uuid-ossp\"").unwrap();
    extension.set_extension_owner(hidden);
    ddl.execute_prepared(&extension, &[], None).unwrap();
    f.admin().roles().set_table_owner(support::TABLE, "r").unwrap();
    f.create_inc_436();
    let task = f.agent().await;
    (f, task, hidden)
}
#[tokio::test]
async fn catalog_role_columns() {
    let (_f, c, h) = schema_fixture().await;
    for (table, column) in [
        ("pg_namespace", "nspowner"), ("pg_class", "relowner"), ("pg_type", "typowner"), ("pg_proc", "proowner"),
        ("pg_collation", "collowner"), ("pg_extension", "extowner"), ("pg_database", "datdba"),
        ("pg_roles", "oid"), ("pg_authid", "oid"), ("pg_user", "usesysid"),
        ("pg_auth_members", "roleid"), ("pg_auth_members", "member"), ("pg_auth_members", "grantor"),
    ] {
        let rows = answer(&c, &format!("{column} FROM {table}")).await.unwrap();
        assert!(rows.iter().all(|r| r[0] != Some(h.to_string())), "{table}.{column}");
    }
    for (table, column) in [("pg_tables", "tableowner"), ("information_schema.schemata", "schema_owner")] {
        let rows = answer(&c, &format!("{column} FROM {table}")).await.unwrap();
        assert!(rows.iter().all(|r| r[0].as_deref() != Some("h")), "{table}.{column}");
    }
    let rows = answer(&c, "collname, pg_get_userbyid(collowner) FROM pg_collation WHERE collname IN ('used','unused')").await.unwrap();
    assert_eq!(rows.len(), 1, "only the shown column's collation is retained");
    assert_eq!(rows[0][0].as_deref(), Some("used"));
    assert_ne!(rows[0][1].as_deref(), Some("h"));
}
#[tokio::test]
async fn hidden_schema_privileges() {
    let (_f, c, h) = schema_fixture().await;
    for p in ["CREATE", "USAGE"] {
        assert_eq!(answer(&c, &format!("has_schema_privilege({h}::oid,'public','{p}')")).await,
                   answer(&c, &format!("has_schema_privilege(4294967000::oid,'public','{p}')")).await, "{p}");
        assert_eq!(answer(&c, &format!("has_schema_privilege('h','public','{p}')")).await,
                   answer(&c, &format!("has_schema_privilege('absent','public','{p}')")).await);

    }
}
#[tokio::test]
async fn hidden_role_resolution_and_inquiries() {
    let (_f, c, h) = schema_fixture().await;
    // Unknown OID displays preserve the numeric input, but never resolve a name.
    assert_eq!(answer(&c, &format!("pg_get_userbyid({h}::oid)")).await.unwrap(), vec![vec![Some(format!("unknown (OID={h})"))]]);
    assert_eq!(answer(&c, "pg_get_userbyid(4294967000::oid)").await.unwrap(), vec![vec![Some("unknown (OID=4294967000)".into())]]);
    let hidden_cast = answer(&c, &format!("{h}::regrole::text")).await;
    let absent_cast = answer(&c, "4294967000::regrole::text").await;
    match (&hidden_cast, &absent_cast) {
        (Ok(hrows), Ok(arows)) => { assert_eq!(hrows, &vec![vec![Some(h.to_string())]]); assert_eq!(arows, &vec![vec![Some("4294967000".into())]]); }
        _ => assert_eq!(hidden_cast, absent_cast), // R3 rejects regrole casts before catalog evaluation.
    }
    for expr in ["to_regrole('{}')", "'{}'::regrole", "has_database_privilege('{}','giggles','CREATE')", "has_type_privilege('{}','int4','USAGE')"] {
        assert_eq!(answer(&c, &expr.replace("{}", "h")).await, answer(&c, &expr.replace("{}", "absent")).await, "{expr}");
    }
    for expr in ["has_database_privilege({}::oid,'giggles','CREATE')", "has_type_privilege({}::oid,'int4','USAGE')"] {
        assert_eq!(answer(&c, &expr.replace("{}", &h.to_string())).await, answer(&c, &expr.replace("{}", "4294967000")).await, "{expr}");
    }
}
async fn chain_answers(grants: bool) -> Vec<Result<Vec<Vec<Option<String>>>, String>> {
    let f = R3Fixture::start().await;
    let h = role(&f, "h"); role(&f, "r");
    f.admin().roles().set_table_owner(support::TABLE, "r").unwrap();
    let bootstrap = "pg_monitor".to_owned();
    let target_oid = f.admin().roles().get(&bootstrap).unwrap().unwrap().oid;
    if grants {
        for (parent, member) in [("h", "r"), (bootstrap.as_str(), "h")] {
            f.admin().roles().grant_membership_options(parent, member, BOOTSTRAP_GRANTOR, MembershipOptions {admin:Some(true), inherit:Some(true), set:Some(true)}).unwrap();
        }
    }
    f.create_inc_436();
    let c = f.agent().await;
    let rows = answer(&c, "roleid, member, grantor FROM pg_auth_members").await.unwrap();
    assert!(rows.iter().flatten().all(|v| v.as_deref() != Some(h.to_string().as_str())));
    let mut results = vec![];
    for p in ["MEMBER", "USAGE", "SET", "MEMBER WITH ADMIN OPTION"] {
        for target in [format!("{target_oid}::oid"), format!("'{bootstrap}'")] {
            results.push(answer(&c, &format!("pg_has_role('r',{target},'{p}')")).await);
        }
    }
    results
}
#[tokio::test]
async fn hidden_membership_chain() {
    let baseline = chain_answers(false).await;
    assert!(baseline.iter().all(|r| r.as_ref().is_ok_and(|rows| rows == &vec![vec![Some("f".into())]])), "{baseline:?}");
    assert_eq!(chain_answers(true).await, baseline);
}
#[tokio::test]
async fn ordinary_membership_over_name_cap() {
    let f = R3Fixture::start().await;
    // catalog_roles starts its byte budget with the presentation bootstrap name.
    let mut bytes: usize = "giggles".len() + f.admin().roles().list().unwrap().iter().map(|r| r.name.len()).sum::<usize>();
    let mut i = 0;
    let target = loop {
        let name = format!("z{i:04}{}", "x".repeat(58));
        let oid = role(&f, &name);
        bytes += name.len();
        if bytes > 32768 { break (name, oid); }
        i += 1;
    };
    let (name, target) = target;
    let member = f.admin().roles().get(support::CONTROLLER).unwrap().unwrap().oid;
    f.admin().roles().grant_membership_options(&name, support::CONTROLLER, BOOTSTRAP_GRANTOR, MembershipOptions::default()).unwrap();
    let c = f.controller().await;
    assert_eq!(answer(&c, &format!("pg_has_role({member}::oid,{target}::oid,'MEMBER')")).await.unwrap(), vec![vec![Some("t".into())]]);
}

async fn acl_answers(c: &Client) -> Vec<Vec<Vec<Option<String>>>> {
    let mut out = vec![];
    for expr in [
        "relacl IS NULL, relacl FROM pg_class WHERE relname = 'shipments'",
        "attacl FROM pg_attribute ORDER BY attnum",
        "count(*) FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.relname = 'shipments'",
        "has_table_privilege('r','shipments','SELECT'), has_column_privilege('r','shipments','status','SELECT')",
        "has_table_privilege('repair_agent','shipments','SELECT'), has_column_privilege('repair_agent','shipments','status','SELECT')",
    ] { out.push(answer(c, expr).await.unwrap_or_else(|e| panic!("{expr}: {e}"))); }
    out
}
#[tokio::test]
async fn hidden_grants_leave_no_acl_footprint() {
    use giggles_core::roles::TablePrivilege::{Select, Insert, Update, Delete};
    let f = R3Fixture::start().await;
    role(&f, "r"); role(&f, "h");
    f.admin().roles().set_table_owner(support::TABLE, "r").unwrap();
    f.create_inc_436();
    let c = f.agent().await;
    // Remove fixture grants only here, after task admission.
    for name in [support::CONTROLLER, support::EXECUTOR, support::APPROVER, support::APP] {
        f.admin().roles().revoke_table(support::TABLE, name, &[Select, Insert, Update, Delete]).unwrap();
    }
    for retained in [false, true] {
        if retained { f.admin().roles().grant_table(support::TABLE, support::EXECUTOR, &[Select]).unwrap(); }
        let baseline = acl_answers(&c).await;
        let casts = answer(&c, "relacl::text FROM pg_class WHERE relname = 'shipments'").await;
        let column_casts = answer(&c, "attacl::text FROM pg_attribute ORDER BY attnum").await;
        assert_eq!(baseline[0][0][0].as_deref(), Some(if retained { "f" } else { "t" }));
        f.admin().roles().grant_table(support::TABLE, "h", &[Select]).unwrap();
        assert_eq!(acl_answers(&c).await, baseline, "retained grant={retained}");
        assert_eq!(answer(&c, "relacl::text FROM pg_class WHERE relname = 'shipments'").await, casts);
        assert_eq!(answer(&c, "attacl::text FROM pg_attribute ORDER BY attnum").await, column_casts);
        f.admin().roles().revoke_table(support::TABLE, "h", &[Select]).unwrap();
    }
}
#[tokio::test]
async fn hidden_fillers_preserve_task_identities() {
    let f = R3Fixture::start().await;
    let owner = role(&f, "r");
    let executor = f.admin().roles().get(support::EXECUTOR).unwrap().unwrap().oid;
    f.admin().roles().set_table_owner(support::TABLE, "r").unwrap();
    f.create_inc_436();
    let c = f.agent().await;
    let query = "oid, rolname FROM pg_roles WHERE rolname = 'repair_agent'";
    let owner_query = "relowner, pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'shipments'";
    let baseline = answer(&c, query).await.unwrap();
    let owners = answer(&c, owner_query).await.unwrap();
    assert_eq!(baseline[0][0], Some(executor.to_string()));
    assert_eq!(owners, vec![vec![Some(owner.to_string()), Some("r".into())]]);
    for i in 0..520 { role(&f, &format!("a{i:04}{}", "x".repeat(58))); }
    assert_eq!(answer(&c, query).await.unwrap(), baseline);
    assert_eq!(answer(&c, owner_query).await.unwrap(), owners);
}

const MEMBERSHIP_COLUMNS: &str = "oid, roleid, member, grantor, admin_option, inherit_option, set_option FROM pg_auth_members ORDER BY 1";

#[tokio::test]
async fn membership_oids_ignore_hidden_edges() {
    let f = R3Fixture::start().await;
    role(&f, "r");
    f.admin().roles().set_table_owner(support::TABLE, "r").unwrap();
    f.admin().roles().grant_membership("pg_monitor", "r").unwrap();
    f.create_inc_436();
    let before = answer(&f.agent().await, MEMBERSHIP_COLUMNS).await.unwrap();
    // The bootstrap identity is synthetic and the executor cannot have edges.
    // Thus no retained-to-retained edge is possible in the real R3 fixture.
    assert!(before.is_empty());
    role(&f, "a_hidden");
    f.admin().roles().grant_membership("a_hidden", "r").unwrap();
    let after = answer(&f.agent().await, MEMBERSHIP_COLUMNS).await.unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn membership_oids_ordinary_registry_order() {
    let f = R3Fixture::start().await;
    let r = role(&f, "r");
    let hidden = role(&f, "a_hidden");
    f.admin().roles().grant_membership("pg_monitor", "r").unwrap();
    f.admin().roles().grant_membership("a_hidden", "r").unwrap();
    let c = f.controller().await;
    let rows = answer(&c, MEMBERSHIP_COLUMNS).await.unwrap();
    let roles = f.admin().roles();
    let edges = roles.edges().unwrap();
    assert_eq!(rows.len(), edges.len());
    for (n, (row, edge)) in rows.iter().zip(&edges).enumerate() {
        assert_eq!(row[0], Some((100000 + n).to_string()));
        assert_eq!(row[1], Some(roles.get(&edge.role).unwrap().unwrap().oid.to_string()));
        assert_eq!(row[2], Some(roles.get(&edge.member).unwrap().unwrap().oid.to_string()));
    }
    let hidden_at = rows.iter().position(|row| row[1] == Some(hidden.to_string()) && row[2] == Some(r.to_string())).unwrap();
    let retained_at = rows.iter().position(|row| row[1].as_deref() == Some("3373") && row[2] == Some(r.to_string())).unwrap();
    assert!(hidden_at < retained_at);
}

#[tokio::test]
async fn hidden_bootstrap_name_leaves_no_footprint() {
    use giggles_core::roles::TablePrivilege::{Select, Insert, Update, Delete};
    for remove_fixture_grants in [false, true] {
        let f = R3Fixture::start().await;
        role(&f, "r");
        f.admin().roles().set_table_owner(support::TABLE, "r").unwrap();
        f.create_inc_436();
        let c = f.agent().await;
        if remove_fixture_grants {
            for name in [support::CONTROLLER, support::EXECUTOR, support::APPROVER, support::APP] {
                f.admin().roles().revoke_table(support::TABLE, name, &[Select, Insert, Update, Delete]).unwrap();
            }
        }
        let baseline = acl_answers(&c).await;
        let flags = answer(&c, "oid, rolname, rolsuper, rolcreaterole, rolcanlogin FROM pg_roles WHERE oid = 10").await.unwrap();
        let memberships = answer(&c, MEMBERSHIP_COLUMNS).await.unwrap();
        role(&f, "giggles");
        f.admin().roles().grant_table(support::TABLE, "giggles", &[Select]).unwrap();
        f.admin().roles().grant_membership("giggles", "r").unwrap();
        assert_eq!(acl_answers(&c).await, baseline, "remove fixture grants={remove_fixture_grants}");
        assert_eq!(answer(&c, "oid, rolname, rolsuper, rolcreaterole, rolcanlogin FROM pg_roles WHERE oid = 10").await.unwrap(), flags);
        assert_eq!(answer(&c, MEMBERSHIP_COLUMNS).await.unwrap(), memberships);
    }
}

#[tokio::test]
async fn stored_bootstrap_owner_is_presented_as_ten() {
    let f = R3Fixture::start().await;
    role(&f, "giggles");
    f.admin().roles().set_table_owner(support::TABLE, "giggles").unwrap();
    f.create_inc_436();
    let c = f.agent().await;
    assert_eq!(answer(&c, "relowner, pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'shipments'").await.unwrap(),
        vec![vec![Some("10".into()), Some("giggles".into())]]);
    assert!(!answer(&c, "c.relname, r.rolname, n.nspname FROM pg_class c JOIN pg_roles r ON c.relowner = r.oid JOIN pg_namespace n ON c.relnamespace = n.oid").await.unwrap().is_empty());
}

/// A real task on a non-public schema must hide that schema's unrelated owner.
#[tokio::test]
async fn non_public_task_hides_schema_owner() {
    use giggles_core::roles::{SchemaPrivilege, TablePrivilege};
    let f = R3Fixture::start().await;
    let h = role(&f, "h");
    f.sql("CREATE SCHEMA s AUTHORIZATION h");
    let ordinary_owner = answer(&f.controller().await,
        "nspowner, pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 's'").await.unwrap();
    assert_eq!(ordinary_owner, vec![vec![Some(h.to_string()), Some("h".into())]]);
    f.sql("CREATE TABLE s.t (merchant_id bigint, id bigint, status text, failed_at timestamptz, attempt_count int, customer_email text, card_token text, PRIMARY KEY (merchant_id, id))");
    f.sql("INSERT INTO s.t VALUES (42, 1, 'failed', '2026-11-05 17:01:00+00', 0, 'private', 'secret')");
    f.admin().roles().set_table_owner("s\0t", support::CONTROLLER).unwrap();
    for name in [support::CONTROLLER, support::EXECUTOR] {
        f.admin().roles().grant_schema("s", name, &[SchemaPrivilege::Usage]).unwrap();
        f.admin().roles().grant_table("s\0t", name, &[TablePrivilege::Select, TablePrivilege::Update]).unwrap();
    }
    let create = support::inc_436_sql(&support::agent_support::timestamptz(f.expires_ms))
        .replace("ON shipments", "ON s.t");
    f.controller().await.simple_query(&create).await.unwrap();
    let c = f.agent().await;
    assert!(!answer(&c, "id FROM s.t").await.unwrap().is_empty());
    let owners = answer(&c, "nspowner, pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 's'").await.unwrap();
    assert_eq!(owners.len(), 1);
    assert_ne!(owners[0][0], Some(h.to_string()));
    assert_eq!(owners[0][0].as_deref(), Some("10"));
    assert_ne!(owners[0][1].as_deref(), Some("h"));
    let names = answer(&c, "schema_owner FROM information_schema.schemata WHERE schema_name = 's'").await.unwrap();
    assert_eq!(names.len(), 1);
    assert_ne!(names[0][0].as_deref(), Some("h"));
    for p in ["CREATE", "USAGE"] {
        assert_eq!(answer(&c, &format!("has_schema_privilege({h}::oid,'s','{p}')")).await,
            answer(&c, &format!("has_schema_privilege(4294967000::oid,'s','{p}')")).await, "{p}");
        assert_eq!(answer(&c, &format!("has_schema_privilege('h','s','{p}')")).await,
            answer(&c, &format!("has_schema_privilege('absent','s','{p}')")).await, "{p}");
    }
    for (table, column) in [
        ("pg_namespace", "nspowner"), ("pg_class", "relowner"), ("pg_type", "typowner"), ("pg_proc", "proowner"),
        ("pg_collation", "collowner"), ("pg_extension", "extowner"), ("pg_database", "datdba"),
        ("pg_roles", "oid"), ("pg_authid", "oid"), ("pg_user", "usesysid"),
        ("pg_auth_members", "roleid"), ("pg_auth_members", "member"), ("pg_auth_members", "grantor"),
    ] {
        let rows = answer(&c, &format!("{column} FROM {table}")).await.unwrap();
        assert!(rows.iter().all(|r| r[0] != Some(h.to_string())), "{table}.{column}");
    }
    for (table, column) in [("pg_tables", "tableowner"), ("information_schema.schemata", "schema_owner")] {
        let rows = answer(&c, &format!("{column} FROM {table}")).await.unwrap();
        assert!(rows.iter().all(|r| r[0].as_deref() != Some("h")), "{table}.{column}");
    }
}
