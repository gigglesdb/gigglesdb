use super::*;

// The pre-cache implementation, deliberately independent of the cache builder.
fn reference(session: &SqlSession) -> Vec<(String, Option<String>)> {
    let owners = |s: &MetadataState<'_>| {
        let mut tables = std::collections::HashMap::with_capacity(s.sql.tables.len());
        for table in s.sql.tables.values() {
            tables.entry(table.id).or_insert(table);
        }
        Ok(s.sql
            .catalog_ext()
            .sequences
            .values()
            .map(|d| {
                let owner = d
                    .owned_by
                    .and_then(|(id, _)| tables.get(&id).map(|t| s.sql.table_key(t)));
                (d.name.clone(), owner)
            })
            .collect())
    };
    match &session.staged {
        Some(s) => owners(&MetadataState::borrow(s)),
        None => session.inspect_metadata(owners),
    }
    .unwrap()
}
fn check(session: &SqlSession) -> Arc<SequenceOwners> {
    let shared = session.sequence_owners_shared().unwrap();
    assert_eq!(&*shared, reference(session));
    assert_eq!(&*shared, session.sequence_owners().unwrap());
    shared
}
fn run(session: &mut SqlSession, sql: &str) {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

#[test]
fn committed_and_private_ownership_equivalence() {
    for engine in [Engine::Legacy, Engine::V2] {
        if engine == Engine::V2 && !cfg!(feature = "engine-rocksdb") {
            continue;
        }
        std::fs::create_dir_all(crate::test_artifact_root()).unwrap();
        let path = crate::test_artifact_root().join(format!(
            "sequence-owners-{:?}-{}-{}",
            engine,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY)",
            "CREATE SEQUENCE s",
            "ALTER SEQUENCE s OWNED BY t.id",
            "ALTER SEQUENCE s OWNED BY NONE",
            "CREATE TABLE serial_owner (id SERIAL PRIMARY KEY)",
            "DROP TABLE serial_owner",
            "CREATE TABLE cols (id INT PRIMARY KEY, serial_col SERIAL, identity_col INT GENERATED ALWAYS AS IDENTITY)",
            "ALTER TABLE cols DROP COLUMN serial_col",
            "ALTER TABLE cols DROP COLUMN identity_col",
            "CREATE SCHEMA ns",
            "CREATE TABLE ns.owned (id SERIAL PRIMARY KEY)",
            "ALTER TABLE ns.owned RENAME TO renamed",
        ] {
            run(&mut session, sql);
            let owners = check(&session);
            let other = admin.sql_session();
            assert_eq!(&*owners, &*check(&other), "{sql}");
            if engine == Engine::V2 {
                assert!(Arc::ptr_eq(&owners, &check(&other)), "{sql}");
            }
        }
        // ADD of sequence-producing columns is not implemented at this revision.
        for sql in [
            "ALTER TABLE t ADD COLUMN serial_col SERIAL",
            "ALTER TABLE t ADD COLUMN identity_col INT GENERATED ALWAYS AS IDENTITY",
        ] {
            let before = check(&session);
            assert!(session.execute(sql, &[], None).is_err(), "{sql}");
            assert_eq!(&*check(&session), &*before);
            assert_eq!(&*check(&session), &*check(&admin.sql_session()));
        }
        let before = check(&session);
        run(&mut session, "BEGIN");
        if engine == Engine::V2 {
            assert!(Arc::ptr_eq(&check(&session), &before));
        }
        run(&mut session, "SAVEPOINT sp");
        run(&mut session, "ALTER SEQUENCE s OWNED BY t.id");
        let private = check(&session);
        assert!(!Arc::ptr_eq(&private, &before));
        assert_ne!(&*private, &*before);
        assert!(private.iter().any(|(name, owner)| name == "s" && owner.as_deref() == Some("t")));
        assert_eq!(&*check(&admin.sql_session()), &*before);
        run(&mut session, "ROLLBACK TO SAVEPOINT sp");
        assert_eq!(&*check(&session), &*before);
        run(&mut session, "ALTER SEQUENCE s OWNED BY t.id");
        run(&mut session, "ROLLBACK");
        assert_eq!(&*check(&session), &*before);
        let mut writer = admin.sql_session();
        run(&mut writer, "ALTER SEQUENCE s OWNED BY t.id");
        assert_eq!(&*check(&session), &*check(&admin.sql_session()));
        assert_ne!(&*check(&session), &*before);
        session.set_isolation(SqlIsolation::RepeatableRead);
        run(&mut session, "BEGIN");
        run(&mut session, "SELECT * FROM t");
        let pinned = check(&session);
        run(&mut writer, "ALTER SEQUENCE s OWNED BY NONE");
        // Catalog visibility follows the existing fresh metadata reader, even
        // when it differs from a newly opened session at the latest snapshot.
        check(&session);
        check(&admin.sql_session());
        if engine == Engine::V2 {
            assert_eq!(&*check(&session), &*pinned);
        }
        run(&mut session, "ROLLBACK");
        assert_eq!(&*check(&session), &*check(&admin.sql_session()));
        // Schema rename parses, but execution is unsupported at this revision.
        let before = check(&session);
        let error = session.execute("ALTER SCHEMA ns RENAME TO renamed_ns", &[], None).unwrap_err();
        assert!(matches!(error, CoreError::Sql(relational::SqlError::Unsupported)), "{error:?}");
        assert_eq!(&*check(&session), &*before);
        assert_eq!(&*check(&session), &*check(&admin.sql_session()));
        run(&mut session, "DROP SEQUENCE s");
        let after = check(&session);
        assert!(!after.iter().any(|(name, _)| name == "s"));
        assert_eq!(&*after, &*check(&admin.sql_session()));
        drop(writer);
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn legacy_sequence_owner_updates_are_transaction_scoped() {
    let root = crate::test_artifact_root();
    fs::create_dir_all(&root).unwrap();
    let mut token = [0u8; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!(
        "legacy-sequence-owners-{:032x}",
        u128::from_be_bytes(token)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    let roles = admin.roles();
    for name in ["bob", "carol"] {
        roles
            .create(roles::NewRole {
                name: name.into(),
                ..Default::default()
            })
            .unwrap();
    }
    let mut writer = admin.sql_session();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY)",
        "CREATE TABLE u (id INT PRIMARY KEY)",
        "CREATE SEQUENCE s",
    ] {
        run(&mut writer, sql);
    }
    roles.set_table_owner("t", "bob").unwrap();
    roles.set_table_owner("u", "carol").unwrap();

    for sql in [
        "BEGIN",
        "ALTER SEQUENCE s OWNED BY t.id",
        "SAVEPOINT sp",
        "ALTER SEQUENCE s OWNED BY u.id",
    ] {
        run(&mut writer, sql);
    }
    assert_eq!(writer.table_owner("s").unwrap().as_deref(), Some("carol"));
    run(&mut writer, "ROLLBACK TO SAVEPOINT sp");
    assert_eq!(writer.table_owner("s").unwrap().as_deref(), Some("bob"));
    run(&mut writer, "COMMIT");
    assert_eq!(roles.table_owner("s").unwrap().as_deref(), Some("bob"));

    roles.set_table_owner("s", "carol").unwrap();
    assert_eq!(
        writer.sequence_call_engine("nextval", "s", None, true).unwrap(),
        1
    );
    assert_eq!(roles.table_owner("s").unwrap().as_deref(), Some("carol"));

    let mut reader = admin.sql_session();
    reader.set_isolation(SqlIsolation::RepeatableRead);
    run(&mut reader, "BEGIN");
    assert!(reader.staged_sequence_owner("s").is_none());
    assert_eq!(
        reader.sequence_ddl_authority_key("s").unwrap().as_deref(),
        Some("t")
    );
    run(&mut writer, "ALTER SEQUENCE s OWNED BY u.id");
    assert_eq!(
        reader.sequence_ddl_authority_key("s").unwrap().as_deref(),
        Some("u")
    );
    run(&mut reader, "ROLLBACK");

    // Exercise the persistence boundary directly while retaining its input state.
    run(&mut writer, "BEGIN");
    run(&mut writer, "ALTER SEQUENCE s OWNED BY t.id");
    let mut staged = writer.staged.as_ref().unwrap().clone();
    assert!(!staged.sequence_owner_updates.is_empty());
    {
        let _guard = admin.inner.gate.lock_cancellable().unwrap();
        assert!(
            admin
                .inner
                .persist_checked(&mut staged, None, true, &mut || true)
                .is_ok()
        );
    }
    assert_eq!(roles.table_owner("s").unwrap().as_deref(), Some("bob"));
    drop(reader);
    drop(writer);
    drop(roles);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
    assert!(
        staged.sequence_owner_updates.is_empty(),
        "successful legacy persistence retained staged sequence owners: {:?}",
        staged.sequence_owner_updates
    );
}
