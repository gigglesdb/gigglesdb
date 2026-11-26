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
