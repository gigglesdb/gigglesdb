use super::*;
use crate::relational::{Prepared, Relational, triggers};

fn catalog(ddls: &[&str]) -> Relational {
    let mut state = Relational::default();
    for ddl in ddls {
        let prepared = Prepared::parse(ddl, 1, state.epoch).unwrap();
        state.execute(&prepared, &[], None).unwrap();
    }
    state
}

#[test]
fn collects_assignments_for_each_bound_update_target() {
    let state = catalog(&[
        "CREATE TABLE p(id int PRIMARY KEY, n int)",
        "CREATE TABLE q(id int PRIMARY KEY, n int)",
    ]);
    for (sql, expected) in [
        ("UPDATE public.p SET n=1 WHERE id=1", vec!["n"]),
        ("UPDATE p SET n=n+1 WHERE id=1", vec!["n"]),
        (
            "INSERT INTO p VALUES(1,2) ON CONFLICT(id) DO UPDATE SET n=excluded.n",
            vec!["n"],
        ),
        (
            "WITH d AS (UPDATE q SET id=2 RETURNING n) \
             INSERT INTO p SELECT 1,n FROM d ON CONFLICT(id) DO UPDATE SET n=excluded.n",
            vec!["n"],
        ),
        ("UPDATE p SET n=q.n FROM q WHERE p.id=q.id", vec!["n"]),
        (
            "WITH a AS (UPDATE p SET n=1 RETURNING id), \
                   b AS (UPDATE q SET n=2 RETURNING id) \
             UPDATE p SET id=2 FROM a WHERE p.id=a.id",
            vec!["id", "n"],
        ),
        (
            "MERGE INTO p USING q ON p.id=q.id \
             WHEN MATCHED AND q.n=1 THEN UPDATE SET n=q.n \
             WHEN MATCHED THEN UPDATE SET id=q.id \
             WHEN NOT MATCHED THEN INSERT(id,n) VALUES(q.id,q.n)",
            vec!["id", "n"],
        ),
    ] {
        let prepared = Prepared::parse(sql, 1, state.epoch)
            .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
            .bind_schemas(&state)
            .unwrap();
        assert_eq!(
            prepared.update_columns("p"),
            Some(expected.into_iter().collect()),
            "{sql}",
        );
        if sql.starts_with("WITH d") {
            assert_eq!(prepared.update_columns("q"), Some(BTreeSet::from(["id"])));
        }
    }
}

#[test]
fn component_lock_requires_a_writing_action_and_potential_key_change() {
    let state = catalog(&[
        "CREATE TABLE p(id int PRIMARY KEY, n int)",
        "CREATE TABLE c(pid int REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
        "CREATE TABLE generated_parent(id int PRIMARY KEY, n int, \
            g int GENERATED ALWAYS AS (n + 1) STORED UNIQUE)",
        "CREATE TABLE generated_child(g int REFERENCES generated_parent(g) ON UPDATE SET NULL)",
        "CREATE TABLE no_action_parent(id int PRIMARY KEY, n int)",
        "CREATE TABLE no_action_child(pid int REFERENCES no_action_parent(id))",
    ]);
    let ext = state.catalog_ext();
    let p = &state.tables["p"];
    let nonkey = BTreeSet::from(["n"]);
    let key = BTreeSet::from(["id"]);
    assert!(!requires_fk_component_lock(
        ext,
        ext,
        p,
        "UPDATE",
        Some(&nonkey)
    ));
    assert!(requires_fk_component_lock(
        ext,
        ext,
        p,
        "UPDATE",
        Some(&key)
    ));
    assert!(requires_fk_component_lock(ext, ext, p, "UPDATE", None));
    assert!(requires_fk_component_lock(ext, ext, p, "DELETE", None));
    assert!(!requires_fk_component_lock(ext, ext, p, "INSERT", None));
    assert!(requires_fk_component_lock(
        ext,
        ext,
        &state.tables["generated_parent"],
        "UPDATE",
        Some(&key),
    ));
    assert!(!requires_fk_component_lock(
        ext,
        ext,
        &state.tables["no_action_parent"],
        "UPDATE",
        None,
    ));

    let mut live = ext.clone();
    let trigger = triggers::Trigger {
        id: 1,
        table: p.id,
        name: "rewrite_key".into(),
        routine: 1,
        events: triggers::UPDATE,
        before: true,
        row: true,
        enabled: b'D',
        update_columns: vec![],
        args: vec![],
        when: None,
        when_columns: vec![],
    };
    live.triggers
        .insert(p.id, BTreeMap::from([(trigger.name.clone(), trigger)]));
    assert!(!requires_fk_component_lock(
        ext,
        &live,
        p,
        "UPDATE",
        Some(&nonkey)
    ));
    live.triggers
        .get_mut(&p.id)
        .unwrap()
        .get_mut("rewrite_key")
        .unwrap()
        .enabled = b'O';
    assert!(requires_fk_component_lock(
        ext,
        &live,
        p,
        "UPDATE",
        Some(&nonkey)
    ));
}

#[test]
fn delete_paths_follow_only_writing_actions() {
    let state = catalog(&[
        "CREATE TABLE owner(id int PRIMARY KEY)",
        "CREATE TABLE p(id int PRIMARY KEY, owner int REFERENCES owner(id))",
        "CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE)",
        "CREATE TABLE leaf(id int PRIMARY KEY, c int REFERENCES c(id) ON DELETE CASCADE)",
        "CREATE TABLE a(id int PRIMARY KEY, p int UNIQUE REFERENCES p(id) ON DELETE SET NULL)",
        "CREATE TABLE update_leaf(id int PRIMARY KEY, a int REFERENCES a(p) ON UPDATE CASCADE)",
        "CREATE TABLE delete_leaf(id int PRIMARY KEY, a int REFERENCES a(id) ON DELETE CASCADE)",
        "CREATE TABLE other_key(id int PRIMARY KEY, a int REFERENCES a(id) ON UPDATE CASCADE)",
        "CREATE TABLE witness(id int PRIMARY KEY, p int REFERENCES p(id))",
        "CREATE TABLE tree(id int PRIMARY KEY, p int REFERENCES tree(id) ON DELETE CASCADE)",
    ]);
    let id = |name: &str| state.tables[name].id;
    assert_eq!(
        delete_action_targets(state.catalog_ext(), [id("p")]),
        (
            ["c", "leaf", "a", "update_leaf"]
                .into_iter()
                .map(id)
                .collect(),
            ["a", "update_leaf"].into_iter().map(id).collect(),
        ),
    );
    assert_eq!(
        delete_action_targets(state.catalog_ext(), [id("tree")]),
        (BTreeSet::from([id("tree")]), BTreeSet::new()),
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn local_delete_and_protected_delete_hold_their_respective_table_locks() {
    use engine::locks::LockKey;
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "fk-delete-lock-set-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let db = admin.test_v2();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE owner(id int PRIMARY KEY)",
        "CREATE TABLE p(id int PRIMARY KEY, owner int REFERENCES owner(id))",
        "CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE)",
        "CREATE TABLE leaf(id int PRIMARY KEY, c int REFERENCES c(id) ON DELETE CASCADE)",
        "CREATE TABLE witness(id int PRIMARY KEY, p int REFERENCES p(id))",
        "INSERT INTO owner VALUES(1)",
        "INSERT INTO p VALUES(1,1)",
        "INSERT INTO c VALUES(1,1)",
        "INSERT INTO leaf VALUES(1,1)",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    let assert_modes = |txn: &mut Transaction, expected: &[(&str, Option<LockMode>)]| {
        let tables = db.catalog().snapshot(txn).unwrap();
        let engine::txn::TxnServices::Local { locks, .. } = txn.services() else {
            unreachable!()
        };
        for (name, held) in expected {
            let key = LockKey::Table(tables.parts.tables[*name].id);
            for mode in [
                LockMode::AccessShare,
                LockMode::RowShare,
                LockMode::RowExclusive,
                LockMode::Share,
                LockMode::AccessExclusive,
            ] {
                assert_eq!(
                    locks.holds_mode(txn.id(), key.clone(), mode),
                    *held == Some(mode),
                    "{name}: {mode:?}"
                );
            }
        }
    };
    for sql in [
        "DELETE FROM p WHERE id=1",
        "WITH gone AS (DELETE FROM p WHERE id=1 RETURNING id) SELECT id FROM gone",
        "MERGE INTO p USING (SELECT 1 AS id) AS input ON p.id=input.id WHEN MATCHED THEN DELETE",
    ] {
        session.execute("BEGIN", &[], None).unwrap();
        session
            .execute(sql, &[], None)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_modes(
            session.v2_transaction.as_mut().unwrap(),
            &[
                ("p", Some(LockMode::RowExclusive)),
                ("c", Some(LockMode::RowExclusive)),
                ("leaf", Some(LockMode::RowExclusive)),
                ("owner", None),
                ("witness", None),
            ],
        );
        assert!(
            session
                .execute("SELECT id FROM leaf", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
        session.execute("ROLLBACK", &[], None).unwrap();
    }
    let run = ProtectedRun {
        txn: db.begin(Isolation::Serializable),
        effects: Default::default(),
    };
    let prepared = session.prepare("DELETE FROM p WHERE id=1").unwrap();
    let (mut run, result) = session.execute_protected_v2(&db, run, &prepared, &[], None);
    result.unwrap();
    assert_modes(
        &mut run.txn,
        &[
            ("p", Some(LockMode::RowExclusive)),
            ("c", Some(LockMode::Share)),
            ("leaf", Some(LockMode::Share)),
            ("owner", Some(LockMode::Share)),
            ("witness", Some(LockMode::Share)),
        ],
    );
    drop(run);
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("ALTER TABLE p ADD COLUMN extra int", &[], None)
        .unwrap();
    assert_modes(
        session.v2_transaction.as_mut().unwrap(),
        &[
            ("p", Some(LockMode::AccessExclusive)),
            ("c", Some(LockMode::AccessExclusive)),
            ("leaf", Some(LockMode::AccessExclusive)),
            ("owner", Some(LockMode::AccessExclusive)),
            ("witness", Some(LockMode::AccessExclusive)),
        ],
    );
    session.execute("ROLLBACK", &[], None).unwrap();
    drop((session, db, admin));
    std::fs::remove_dir_all(path).unwrap();
}
