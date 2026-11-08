use giggles_core::{
    Admin, Limits, SqlSession,
    roles::{NewRole, TablePrivilege},
};
use std::path::PathBuf;

struct Database {
    admin: Option<Admin>,
    path: PathBuf,
}

impl Database {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "owner-key-hygiene-{:032x}",
            u128::from_be_bytes(nonce)
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        for name in ["alice", "bob"] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    ..NewRole::default()
                })
                .unwrap();
        }
        Self {
            admin: Some(admin),
            path,
        }
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn seed(&self, name: &str) {
        let roles = self.admin().roles();
        roles.set_table_owner(name, "alice").unwrap();
        roles
            .grant_table(name, "bob", &[TablePrivilege::Select])
            .unwrap();
    }

    fn cleared(&self, name: &str) {
        let roles = self.admin().roles();
        assert_eq!(roles.table_owner(name).unwrap(), None, "{name}");
        assert!(
            roles.table_privileges(name, "bob").unwrap().is_empty(),
            "{name}"
        );
    }

    fn preserved(&self, name: &str) {
        let roles = self.admin().roles();
        assert_eq!(
            roles.table_owner(name).unwrap().as_deref(),
            Some("alice"),
            "{name}"
        );
        assert!(
            roles
                .table_privileges(name, "bob")
                .unwrap()
                .contains(&TablePrivilege::Select),
            "{name}"
        );
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

fn sql(session: &mut SqlSession, text: &str) {
    session.execute(text, &[], None).unwrap();
}

#[test]
fn native_create_clears_stale_table_and_sequence_authority() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    db.seed("fresh");
    sql(&mut session, "CREATE TABLE fresh (id BIGINT PRIMARY KEY)");
    db.cleared("fresh");
    db.seed("fresh_seq");
    sql(&mut session, "CREATE SEQUENCE fresh_seq");
    db.cleared("fresh_seq");
}

#[test]
fn native_drop_clears_table_and_owned_sequence_authority() {
    for managed in [false, true] {
        let db = Database::new();
        let mut session = db.admin().sql_session();
        sql(&mut session, "CREATE TABLE doomed (id BIGINT PRIMARY KEY)");
        sql(&mut session, "CREATE SEQUENCE owned OWNED BY doomed.id");
        db.seed("doomed");
        db.seed("owned");
        session.set_authority_managed(managed);
        sql(&mut session, "DROP TABLE doomed");
        db.cleared("doomed");
        db.cleared("owned");
    }
}

#[test]
fn admin_drop_clears_table_and_owned_sequence_authority() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    sql(&mut session, "CREATE TABLE doomed (id BIGINT PRIMARY KEY)");
    sql(&mut session, "CREATE SEQUENCE owned OWNED BY doomed.id");
    db.seed("doomed");
    db.seed("owned");
    assert!(db.admin().drop_table("doomed", false).unwrap());
    db.cleared("doomed");
    db.cleared("owned");
}

#[test]
fn managed_commit_preserves_authority_written_before_publication() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    session.set_authority_managed(true);
    sql(&mut session, "BEGIN");
    sql(&mut session, "CREATE TABLE fresh (id BIGINT PRIMARY KEY)");
    db.seed("fresh");
    sql(&mut session, "COMMIT");
    db.preserved("fresh");
}

#[test]
fn managed_same_name_replacement_preserves_authority_written_before_publication() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    sql(&mut session, "CREATE TABLE reused (id int)");
    session.set_authority_managed(true);
    sql(&mut session, "BEGIN");
    sql(&mut session, "DROP TABLE reused");
    sql(&mut session, "CREATE TABLE reused (id int)");
    db.seed("reused");
    sql(&mut session, "COMMIT");
    db.preserved("reused");
}

#[test]
fn unmanaged_same_name_replacement_clears_authority_written_before_publication() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    sql(&mut session, "CREATE TABLE reused (id int)");
    sql(&mut session, "BEGIN");
    sql(&mut session, "DROP TABLE reused");
    sql(&mut session, "CREATE TABLE reused (id int)");
    db.seed("reused");
    sql(&mut session, "COMMIT");
    db.cleared("reused");
}

#[test]
fn if_not_exists_preserves_existing_relation_authority() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    sql(
        &mut session,
        "CREATE TABLE existing (id BIGINT PRIMARY KEY)",
    );
    sql(&mut session, "CREATE SEQUENCE existing_seq");
    db.seed("existing");
    db.seed("existing_seq");
    sql(
        &mut session,
        "CREATE TABLE IF NOT EXISTS existing (id BIGINT PRIMARY KEY)",
    );
    sql(&mut session, "CREATE SEQUENCE IF NOT EXISTS existing_seq");
    sql(
        &mut session,
        "CREATE TABLE IF NOT EXISTS existing_seq (id BIGINT PRIMARY KEY)",
    );
    sql(&mut session, "CREATE SEQUENCE IF NOT EXISTS existing");
    db.preserved("existing");
    db.preserved("existing_seq");
}

#[test]
fn rename_and_reused_source_preserve_only_the_surviving_identity() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    sql(
        &mut session,
        "CREATE TABLE original (id BIGINT PRIMARY KEY)",
    );
    db.seed("original");
    sql(&mut session, "BEGIN");
    sql(&mut session, "ALTER TABLE original RENAME TO renamed");
    sql(
        &mut session,
        "CREATE TABLE original (id BIGINT PRIMARY KEY)",
    );
    sql(&mut session, "COMMIT");
    db.cleared("original");
    db.preserved("renamed");
}

#[test]
fn rename_into_dropped_name_retains_surviving_authority() {
    let db = Database::new();
    let mut session = db.admin().sql_session();
    sql(
        &mut session,
        "CREATE TABLE original (id BIGINT PRIMARY KEY)",
    );
    sql(&mut session, "CREATE TABLE doomed (id BIGINT PRIMARY KEY)");
    db.seed("original");
    db.admin().roles().set_table_owner("doomed", "bob").unwrap();
    db.admin()
        .roles()
        .grant_table("doomed", "alice", &[TablePrivilege::Insert])
        .unwrap();
    sql(&mut session, "BEGIN");
    sql(&mut session, "DROP TABLE doomed");
    sql(&mut session, "ALTER TABLE original RENAME TO doomed");
    sql(&mut session, "COMMIT");
    db.cleared("original");
    db.preserved("doomed");
    assert!(
        db.admin()
            .roles()
            .table_privileges("doomed", "alice")
            .unwrap()
            .is_empty()
    );
}
