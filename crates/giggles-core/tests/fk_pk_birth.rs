mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::SqlError};

fn exercise(engine: Engine) {
    let path = common::fresh("fk-pk-birth");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE early(a int PRIMARY KEY)",
        "CREATE UNIQUE INDEX early_s ON early(a)",
        "CREATE TABLE same_a(a int PRIMARY KEY, UNIQUE(a))",
        "CREATE TABLE same_b(a int PRIMARY KEY,b int UNIQUE)",
        "CREATE TABLE plain(a int)",
        "ALTER TABLE plain ADD PRIMARY KEY(a)",
        "CREATE TABLE nonunique(a int)",
        "CREATE INDEX nonunique_s ON nonunique(a)",
        "ALTER TABLE nonunique ADD PRIMARY KEY(a)",
    ] {
        session.execute(sql, &[], None).unwrap();
        let metadata = session.primary_key_birth_metadata().unwrap();
        assert!(metadata.0.is_empty(), "{sql}");
        assert!(metadata.2 < 8, "{sql}");
    }
    // Cross both secondary ownership kinds with both PK naming forms.
    for (table, secondary, primary) in [
        ("i_default", "CREATE UNIQUE INDEX id_s ON i_default(a)", "ADD PRIMARY KEY(a)"),
        ("i_named", "CREATE UNIQUE INDEX in_s ON i_named(a)", "ADD CONSTRAINT in_pk PRIMARY KEY(a)"),
        ("u_default", "ALTER TABLE u_default ADD CONSTRAINT ud_u UNIQUE(a)", "ADD PRIMARY KEY(a)"),
        ("u_named", "ALTER TABLE u_named ADD CONSTRAINT un_u UNIQUE(a)", "ADD CONSTRAINT un_pk PRIMARY KEY(a)"),
    ] {
        session.execute(&format!("CREATE TABLE {table}(a int NOT NULL)"), &[], None).unwrap();
        session.execute(secondary, &[], None).unwrap();
        session.execute(&format!("INSERT INTO {table} VALUES(1),(2)"), &[], None).unwrap();
        let before = session.primary_key_birth_metadata().unwrap();
        session.execute("BEGIN", &[], None).unwrap();
        session.execute(&format!("ALTER TABLE {table} {primary}"), &[], None).unwrap();
        assert_eq!(session.primary_key_birth_metadata().unwrap().0.len(), before.0.len() + 1);
        session.execute("ROLLBACK", &[], None).unwrap();
        assert_eq!(session.primary_key_birth_metadata().unwrap(), before);
        session.execute(&format!("ALTER TABLE {table} {primary}"), &[], None).unwrap();
        let id = admin.catalog().unwrap().1.iter().find(|t| t.name == table).unwrap().id;
        let birth = session.primary_key_birth_metadata().unwrap().0[&id];
        let index = admin.secondary_index_catalog().unwrap().1.into_iter().find(|d| d.table == id).unwrap();
        assert!(birth > index.id);
        assert_eq!(session.primary_key_birth_metadata().unwrap().2, 8);
    }
    // Failure with a UNIQUE on another column must not publish a birth or consume IDs.
    session.execute("CREATE TABLE bad(a int,b int UNIQUE)", &[], None).unwrap();
    session.execute("INSERT INTO bad VALUES(1,1),(1,2)", &[], None).unwrap();
    let before = session.primary_key_birth_metadata().unwrap();
    assert!(session.execute("ALTER TABLE bad ADD PRIMARY KEY(a)", &[], None).is_err());
    assert_eq!(session.primary_key_birth_metadata().unwrap(), before);
    session.execute("ALTER TABLE i_default RENAME TO renamed", &[], None).unwrap();
    // Constraint rename is not implemented by either engine's SQL grammar.
    assert_eq!(session.execute("ALTER TABLE renamed RENAME CONSTRAINT i_default_pkey TO renamed_pk", &[], None),
        Err(CoreError::Sql(SqlError::Syntax)));
    assert_eq!(session.primary_key_birth_metadata().unwrap().0, before.0);
    let before = session.primary_key_birth_metadata().unwrap();
    drop(session);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    assert_eq!(session.primary_key_birth_metadata().unwrap(), before);
    let id = admin.catalog().unwrap().1.iter().find(|t| t.name == "renamed").unwrap().id;
    session.execute("DROP TABLE renamed", &[], None).unwrap();
    assert!(!session.primary_key_birth_metadata().unwrap().0.contains_key(&id));
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_pk_birth_lifecycle() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_pk_birth_lifecycle() { exercise(Engine::V2); }
