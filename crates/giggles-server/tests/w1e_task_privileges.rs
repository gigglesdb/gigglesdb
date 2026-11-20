#![cfg(feature = "engine-rocksdb")]
#[path = "r3_support/mod.rs"]
mod support;
use support::R3Fixture;
async fn hidden_sequence(owner_retained: bool) {
    let f = R3Fixture::start().await;
    f.create_inc_436();
    f.sql("CREATE SEQUENCE w1e_hidden_sequence");
    let owner = "w1e_sequence_owner";
    f.admin().roles().create(giggles_core::roles::NewRole { name: owner.into(), ..Default::default() }).unwrap();
    f.admin().roles().set_table_owner("w1e_hidden_sequence", owner).unwrap();
    if owner_retained { f.admin().roles().set_table_owner(support::TABLE, owner).unwrap(); }
    let task = f.agent().await;
    let rows = task.simple_query("SELECT relname FROM pg_class").await.unwrap();
    for row in &rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = row {
            assert_ne!(row.get(0), Some("w1e_hidden_sequence"));
        }
    }
    let mut states = vec![];
    for name in ["w1e_hidden_sequence", "w1e_nonexistent_sequence"] {
        let e = task.simple_query(&format!("SELECT has_sequence_privilege('{name}','USAGE') FROM pg_class LIMIT 1")).await.unwrap_err();
        states.push(e.code().unwrap().code().to_owned());
    }
    assert_eq!(states[0], states[1]);
}
#[tokio::test]
async fn hidden_sequence_retained_owner() { hidden_sequence(true).await; }
#[tokio::test]
async fn hidden_sequence_removed_owner() { hidden_sequence(false).await; }

async fn hidden_role_inquiry(privilege: &str) {
    let f = R3Fixture::start().await;
    f.create_inc_436();
    for name in ["w1e_retained_owner", "w1e_hidden_parent"] {
        f.admin().roles().create(giggles_core::roles::NewRole { name: name.into(), ..Default::default() }).unwrap();
    }
    f.admin().roles().set_table_owner(support::TABLE, "w1e_retained_owner").unwrap();
    f.admin().roles().grant_membership_options("w1e_hidden_parent", "w1e_retained_owner", giggles_core::roles::BOOTSTRAP_GRANTOR, giggles_core::roles::MembershipOptions { admin: Some(true), inherit: Some(true), set: Some(true) }).unwrap();
    let hidden = f.admin().roles().get("w1e_hidden_parent").unwrap().unwrap().oid;
    let task = f.agent().await;
    let mut results = vec![];
    for oid in [hidden, 4294967000] {
        let sql = format!("SELECT pg_has_role('w1e_retained_owner', {oid}::oid, '{privilege}') FROM pg_class LIMIT 1");
        let rows = task.simple_query(&sql).await.unwrap();
        results.push(rows.iter().find_map(|r| if let tokio_postgres::SimpleQueryMessage::Row(r) = r { r.get(0).map(str::to_owned) } else { None }).unwrap());
    }
    assert_eq!(results[0], results[1], "{privilege}");
    let rows = task.simple_query("SELECT roleid, member FROM pg_auth_members").await.unwrap();
    assert!(!rows.iter().any(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_))));
}
#[tokio::test]
async fn hidden_role_member() { hidden_role_inquiry("MEMBER").await; }
#[tokio::test]
async fn hidden_role_usage() { hidden_role_inquiry("USAGE").await; }
#[tokio::test]
async fn hidden_role_set() { hidden_role_inquiry("SET").await; }
#[tokio::test]
async fn hidden_role_member_admin() { hidden_role_inquiry("MEMBER WITH ADMIN OPTION").await; }
#[tokio::test]
async fn hidden_role_usage_admin() { hidden_role_inquiry("USAGE WITH ADMIN OPTION").await; }
#[tokio::test]
async fn hidden_role_set_admin() { hidden_role_inquiry("SET WITH ADMIN OPTION").await; }
