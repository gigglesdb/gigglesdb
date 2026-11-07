use super::catalog_ext::AutoName;
use super::*;

fn execute(state: &mut Relational, sql: &str) -> R<SqlResult> {
    let prepared = Prepared::parse(sql, 1u128, state.epoch)?;
    state.execute(&prepared, &[], None)
}

fn check(state: &mut Relational, table: &str, name: &str) -> R<()> {
    state.add_constraint(
        state.tables[table].id,
        Constraint {
            id: state.next_id,
            name: name.into(),
            kind: ConstraintKind::Check {
                expr: "true".into(),
                not_valid: false,
            },
        },
    )
}

#[test]
fn explicit_constraint_names_are_local_and_duplicates_are_atomic() {
    let mut state = Relational::default();
    execute(&mut state, "CREATE TABLE t1 (v INTEGER)").unwrap();
    execute(&mut state, "CREATE TABLE t2 (v INTEGER)").unwrap();
    check(&mut state, "t1", "shared").unwrap();
    check(&mut state, "t2", "shared").unwrap();
    check(&mut state, "t1", "t2").unwrap();
    let before = state.clone();
    assert_eq!(
        check(&mut state, "t1", "shared"),
        Err(SqlError::DuplicateObject)
    );
    assert_eq!(state, before);
    assert!(state.constraint_name_taken(state.tables["t1"].id, "shared"));
    assert!(!state.constraint_name_taken(state.tables["t2"].id, "t2"));
}

#[test]
fn automatic_constraint_names_check_database_wide_constraints() {
    let mut state = Relational::default();
    execute(&mut state, "CREATE TABLE t1 (v INTEGER)").unwrap();
    execute(&mut state, "CREATE TABLE t2 (v INTEGER)").unwrap();
    check(&mut state, "t2", "t1_v_key").unwrap();
    assert_eq!(
        state.choose_constraint_name("t1", &["v", "w"], AutoName::Key),
        "t1_v_w_key"
    );
    for (kind, name) in [
        (AutoName::Check, "t1_v_check"),
        (AutoName::Fkey, "t1_v_fkey"),
    ] {
        check(&mut state, "t2", name).unwrap();
        assert_eq!(
            state.choose_constraint_name("t1", &["v"], kind),
            format!("{name}1")
        );
    }
    let table = state.tables["t2"].clone();
    for (kind, name) in [
        (AutoName::Check, "other_v_check"),
        (AutoName::Fkey, "other_v_fkey"),
    ] {
        state
            .create_secondary_index_internal_engine(
                &table,
                state.epoch,
                name,
                &[table.columns[0].id],
                true,
                || false,
            )
            .unwrap();
        assert_eq!(
            state.choose_constraint_name("other", &["v"], kind),
            format!("{name}1")
        );
    }
    execute(&mut state, "CREATE TABLE relation_v_key (v INTEGER)").unwrap();
    execute(&mut state, "CREATE TABLE relation_pkey (v INTEGER)").unwrap();
    assert_eq!(
        state.choose_constraint_name("relation", &["v"], AutoName::Key),
        "relation_v_key1"
    );
    assert_eq!(
        state.choose_constraint_name("relation", &["v"], AutoName::Pkey),
        "relation_pkey1"
    );
    assert_eq!(
        state.choose_constraint_name("t1", &["v"], AutoName::Key),
        "t1_v_key1"
    );
    assert_eq!(
        state.choose_constraint_name("t1", &["v", "w"], AutoName::Pkey),
        "t1_pkey"
    );
    assert_eq!(
        state.choose_constraint_name("t1", &["v", "w"], AutoName::Fkey),
        "t1_v_w_fkey"
    );
    assert_eq!(
        state.choose_constraint_name("t1", &[], AutoName::Check),
        "t1_check"
    );
    execute(&mut state, "CREATE TABLE t1_check (v INTEGER)").unwrap();
    assert_eq!(
        state.choose_constraint_name("t1", &[], AutoName::Check),
        "t1_check"
    );
}

#[test]
fn relation_names_include_all_indexes_and_sequences() {
    let mut state = Relational::default();
    execute(&mut state, "CREATE TABLE t (v INTEGER)").unwrap();
    let table = state.tables["t"].clone();
    for (name, unique) in [("ordinary", false), ("t_v_key", true)] {
        state
            .create_secondary_index_internal_engine(
                &table,
                state.epoch,
                name,
                &[table.columns[0].id],
                unique,
                || false,
            )
            .unwrap();
    }
    state
        .add_sequence(SequenceDef {
            name: "seq".into(),
            data_type: DataType::BigInt,
            start: 1,
            increment: 1,
            min: 1,
            max: i64::MAX,
            cache: 1,
            cycle: false,
            owned_by: None,
            identity: false,
            incarnation: 1,
        })
        .unwrap();
    for name in ["t", "ordinary", "t_v_key", "seq"] {
        assert!(state.relation_name_taken(name), "{name}");
    }
    assert!(!state.relation_name_taken("unused"));
    assert!(!state.constraint_name_taken(table.id, "ordinary"));
    assert!(state.constraint_name_taken(table.id, "t_v_key"));
    assert_eq!(
        state.choose_constraint_name("t", &["v"], AutoName::Key),
        "t_v_key1"
    );
    let before = state.clone();
    assert_eq!(
        execute(&mut state, "CREATE TABLE ordinary (v INTEGER)").unwrap_err(),
        SqlError::DuplicateTable
    );
    assert_eq!(state, before);
}

#[test]
fn automatic_names_balance_and_clip_bytes_before_collision_suffixes() {
    let mut state = Relational::default();
    execute(&mut state, "CREATE TABLE t (v INTEGER)").unwrap();
    let table = "a".repeat(60);
    let name = state.choose_constraint_name(&table, &[], AutoName::Pkey);
    assert_eq!(name, format!("{}_pkey", "a".repeat(58)));
    let name = state.choose_constraint_name(&table, &[&"b".repeat(60)], AutoName::Key);
    assert_eq!(name, format!("{}_{}_key", "a".repeat(29), "b".repeat(29)));
    check(&mut state, "t", &name).unwrap();
    let collision = state.choose_constraint_name(&table, &[&"b".repeat(60)], AutoName::Key);
    assert_eq!(
        collision,
        format!("{}_{}_key1", "a".repeat(29), "b".repeat(28))
    );
    assert_eq!(collision.len(), 63);
    let unicode = "é".repeat(30);
    let name = state.choose_constraint_name(&unicode, &[&unicode], AutoName::Key);
    assert_eq!(name, format!("{}_{}_key", "é".repeat(14), "é".repeat(14)));
    check(&mut state, "t", &name).unwrap();
    let collision = state.choose_constraint_name(&unicode, &[&unicode], AutoName::Key);
    assert!(collision.ends_with("_key1"));
    assert!(collision.len() <= 63);
}
