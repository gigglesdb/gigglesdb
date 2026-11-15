//! Static shared-write admission, independent of storage locking.
use super::*;

fn run(state: &mut Relational, sql: &str) {
    let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    state.execute(&prepared, &[], None).unwrap();
}
fn fixture(ddl: &str) -> Relational {
    let mut state = Relational::default();
    run(&mut state, ddl);
    state
}
fn users() -> Relational {
    fixture("CREATE TABLE users(id bigserial PRIMARY KEY, email text UNIQUE, name text)")
}
fn prepared(state: &Relational, sql: &str) -> Prepared {
    Prepared::parse(sql, 1, state.epoch).unwrap()
}
fn plan(state: &Relational, sql: &str) -> Option<UpsertLockPlan> {
    prepared(state, sql).upsert_lock_plan(state, state.secondary.descriptors())
}

#[test]
fn upsert_classifier_unique_target_actions() {
    let state = users();
    let descriptor = &state.secondary.descriptors()[0];
    for action in ["DO NOTHING", "DO UPDATE SET name=excluded.name"] {
        let result = plan(&state, &format!("INSERT INTO users(email,name) VALUES ('a','A') ON CONFLICT (email) {action}"));
        assert_eq!(result, Some(UpsertLockPlan { arbiter: Arbiter::Unique(descriptor.id), rows: 1 }));
        let name = split_key(&descriptor.name).1;
        assert!(plan(&state, &format!("INSERT INTO users(email,name) VALUES ('a','A') ON CONFLICT ON CONSTRAINT {name} {action}")).is_some());
    }
}

#[test]
fn upsert_classifier_unsafe_shapes() {
    let state = users();
    for sql in [
        "INSERT INTO users(email) VALUES ('a') ON CONFLICT DO NOTHING",
        "INSERT INTO users(email) VALUES ('a') ON CONFLICT (email) DO UPDATE SET email=excluded.email",
        "INSERT INTO users(email) VALUES ('a') ON CONFLICT (email) DO UPDATE SET id=1",
        "INSERT INTO users(email) SELECT email FROM users ON CONFLICT (email) DO NOTHING",
        "INSERT INTO users(email) VALUES ('a') ON CONFLICT (name) DO NOTHING",
        "INSERT INTO users(id,email) VALUES (1,'a') ON CONFLICT (id) DO NOTHING",
    ] { assert!(plan(&state, sql).is_none(), "{sql}"); }
    let mut state = state;
    run(&mut state, "CREATE UNIQUE INDEX users_name_idx ON users(name)");
    assert!(plan(&state, "INSERT INTO users(email) VALUES ('a') ON CONFLICT (email) DO NOTHING").is_none());
}

#[test]
fn upsert_classifier_row_bound_and_append() {
    let state = users();
    for count in [200, 201] {
        let rows = (0..count).map(|n| format!("('{n}')")).collect::<Vec<_>>().join(",");
        let sql = format!("INSERT INTO users(email) VALUES {rows}");
        assert_eq!(prepared(&state, &sql).plain_values_append(&state), count == 200);
        let result = plan(&state, &format!("{sql} ON CONFLICT (email) DO NOTHING"));
        assert_eq!(result.is_some(), count == 200);
        if let Some(result) = result { assert_eq!(result.rows, count); }
    }
}

#[test]
fn upsert_classifier_primary_and_defaults() {
    let state = fixture("CREATE TABLE users(id bigint PRIMARY KEY, name text)");
    assert_eq!(plan(&state, "INSERT INTO users(id,name) VALUES (1,'a') ON CONFLICT (id) DO NOTHING"),
        Some(UpsertLockPlan { arbiter: Arbiter::PrimaryKey, rows: 1 }));
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_none());
    let name = state.primary_key_constraint_name(state.tables.values().next().unwrap()).unwrap();
    assert!(plan(&state, &format!("INSERT INTO users(id) VALUES (1) ON CONFLICT ON CONSTRAINT {name} DO NOTHING")).is_some());
    let state = fixture("CREATE TABLE users(id bigint DEFAULT 1 PRIMARY KEY, name text)");
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_none());
    let mut sequence = fixture("CREATE SEQUENCE user_ids");
    run(&mut sequence, "CREATE TABLE users(id bigint DEFAULT nextval('user_ids') PRIMARY KEY, name text)");
    assert!(plan(&sequence, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_some());
    let state = fixture("CREATE TABLE users(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text)");
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_some());
}

#[test]
fn upsert_classifier_foreign_keys() {
    let state = fixture("CREATE TABLE users(id bigserial PRIMARY KEY, parent bigint REFERENCES users(id), name text)");
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_none());
    let mut state = fixture("CREATE TABLE parents(id bigint PRIMARY KEY)");
    run(&mut state, "CREATE TABLE users(id bigserial PRIMARY KEY, parent bigint REFERENCES parents(id), name text)");
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO UPDATE SET parent=1").is_none());
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO UPDATE SET name=excluded.name").is_some());
    let mut state = fixture("CREATE TABLE parents(id bigint PRIMARY KEY)");
    run(&mut state, "CREATE TABLE users(id bigserial PRIMARY KEY, parent bigint REFERENCES parents(id) DEFERRABLE, name text)");
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_none());
    let mut state = fixture("CREATE TABLE users(id bigserial PRIMARY KEY, name text)");
    run(&mut state, "CREATE TABLE children(id bigint PRIMARY KEY, parent bigint REFERENCES users(id) DEFERRABLE)");
    assert!(plan(&state, "INSERT INTO users(name) VALUES ('a') ON CONFLICT (id) DO NOTHING").is_none());
}

#[test]
fn upsert_classifier_update_rule_two() {
    let state = users();
    let indexes = state.secondary.descriptors();
    let update = prepared(&state, "UPDATE users SET email=$1 WHERE id=1");
    let params = [Value::Text("a".into())];
    assert!(update.mutation_lock_keys(&state, &params, indexes).is_none());
    // Rule 2 tightens lock admission without changing the point publisher.
    assert!(point_keys(&state, &update, &params, indexes).is_some());
    assert!(prepared(&state, "UPDATE users SET name='A' WHERE id=1")
        .mutation_lock_keys(&state, &[], indexes).is_some());
}
