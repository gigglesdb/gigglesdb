use super::*;
use crate::{Admin, Limits, SqlSession};
use std::sync::Arc;

fn with_session(test: impl FnOnce(&mut SqlSession)) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "enum-order-scope-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    test(&mut admin.sql_session());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn snapshot(session: &mut SqlSession) -> Relational {
    let fence = session.batch_fence_engine().unwrap();
    let (state, _) = session
        .apply_batch_engine(fence, &mut || false, |src, apply| {
            apply.apply(&[], src.next_version_engine())?;
            Ok(src.clone())
        })
        .unwrap();
    state
}

fn legacy_types(state: &Relational) -> BTreeMap<u64, TypeDef> {
    state
        .tables
        .values()
        .flat_map(|table| &table.columns)
        .filter_map(|column| {
            let ty = state.column_type(column.id)?;
            matches!(ty, TypeDef::Enum { .. }).then(|| (column.id, ty.clone()))
        })
        .collect()
}

fn assert_scope(state: &Relational, count: usize) {
    let expected = legacy_types(state);
    assert_eq!(expected.len(), count);
    let _scope = Scope::enter(state);
    TYPES.with(|slot| assert_eq!(*slot.borrow(), expected));
}

#[test]
fn scope_matches_full_column_scan_after_private_ddl() {
    with_session(|session| {
        run(session, "BEGIN");
        run(
            session,
            "CREATE TABLE plain (id int PRIMARY KEY, v text DEFAULT 'plain')",
        );
        assert_scope(&snapshot(session), 0);

        run(session, "CREATE DOMAIN positive AS int CHECK (VALUE > 0)");
        run(
            session,
            "CREATE TABLE domain_items (id int PRIMARY KEY, v positive)",
        );
        assert_scope(&snapshot(session), 0);
        run(session, "CREATE TYPE mood AS ENUM ('z','b')");
        assert_scope(&snapshot(session), 0);

        run(session, "CREATE TABLE items (id int PRIMARY KEY, v mood)");
        run(session, "CREATE TABLE other (id int PRIMARY KEY, v mood)");
        let before_drop = snapshot(session);
        assert_scope(&before_drop, 2);
        let items = before_drop
            .tables
            .values()
            .find(|table| table.name == "items")
            .unwrap();
        let dropped_id = items
            .columns
            .iter()
            .find(|column| column.name == "v")
            .unwrap()
            .id;
        let metadata = before_drop.ext.columns[&dropped_id].clone();

        run(session, "ALTER TABLE items DROP COLUMN v");
        let mut after_drop = snapshot(session);
        assert_scope(&after_drop, 1);
        Arc::make_mut(&mut after_drop.ext)
            .columns
            .insert(dropped_id, metadata.clone());
        assert_scope(&after_drop, 1);

        run(session, "DROP TABLE other");
        let mut after_drop = snapshot(session);
        assert_scope(&after_drop, 0);
        Arc::make_mut(&mut after_drop.ext)
            .columns
            .insert(dropped_id, metadata);
        assert_scope(&after_drop, 0);

        run(session, "CREATE TABLE arrays (id int PRIMARY KEY, v mood[])");
        assert_scope(&snapshot(session), 1);
        run(session, "DROP TABLE arrays");
        assert_scope(&snapshot(session), 0);
        run(session, "COMMIT");
    });
}

fn assert_label_order(mut select: impl FnMut(&str) -> SqlResult) {
    let rows = |result: SqlResult| {
        result
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>()
    };
    let text = |label: &str| Value::Text(label.into());
    assert_eq!(
        rows(select(
            "SELECT v, v < 'm', greatest(v, 'm') FROM items ORDER BY v"
        )),
        vec![
            vec![text("z"), Value::Bool(true), text("m")],
            vec![text("m"), Value::Bool(false), text("m")],
            vec![text("b"), Value::Bool(false), text("b")],
            vec![text("a"), Value::Bool(false), text("a")],
        ]
    );
    assert_eq!(
        rows(select("SELECT min(v), max(v) FROM items")),
        vec![vec![text("z"), text("a")]]
    );
}

#[test]
fn scope_reads_updated_labels_from_the_private_catalog() {
    with_session(|session| {
        run(session, "CREATE TYPE mood AS ENUM ('z','b')");
        run(session, "BEGIN");
        run(session, "CREATE TABLE items (id int PRIMARY KEY, v mood)");
        run(session, "INSERT INTO items VALUES (1,'b'),(2,'z')");
        let mut state = snapshot(session);
        let original = legacy_types(&state);
        let _outer = Scope::enter(&state);
        let mut execute = |sql: &str| {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&prepared, &[], None).unwrap()
        };
        execute("ALTER TYPE mood ADD VALUE 'm' BEFORE 'b'");
        execute("ALTER TYPE mood ADD VALUE 'a' AFTER 'b'");
        execute("INSERT INTO items VALUES (3,'a'),(4,'m')");
        assert_label_order(execute);
        assert_scope(&state, 1);
        TYPES.with(|slot| assert_eq!(*slot.borrow(), original));
        run(session, "ROLLBACK");
    });
}

#[test]
fn sql_uses_enum_label_order_inside_transaction_and_after_commit() {
    with_session(|session| {
        run(session, "BEGIN");
        run(session, "CREATE TYPE mood AS ENUM ('z','m','b','a')");
        run(session, "CREATE TABLE items (id int PRIMARY KEY, v mood)");
        run(
            session,
            "INSERT INTO items VALUES (1,'b'),(2,'z'),(3,'a'),(4,'m')",
        );
        assert_label_order(|sql| run(session, sql));
        run(session, "COMMIT");
        assert_label_order(|sql| run(session, sql));
    });
}
