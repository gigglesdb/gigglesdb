//! Private catalog fixtures for ALTER behavior whose CREATE SQL is not admitted.
use super::super::*;

fn run(state: &mut Relational, sql: &str) -> R<SqlResult> {
    let prepared = Prepared::parse(sql, 1, state.epoch)?;
    state.execute(&prepared, &[], None)
}

#[test]
fn same_table_check_name_collision_is_duplicate_object_42710() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE t(id int PRIMARY KEY,v int)").unwrap();
    let table = state.tables["t"].clone();
    state
        .add_constraint(
            table.id,
            Constraint {
                id: state.next_id,
                name: "same".into(),
                kind: ConstraintKind::Check {
                    expr: "v > 0".into(),
                    not_valid: false,
                },
            },
        )
        .unwrap();
    let before = state.clone();
    assert_eq!(
        run(&mut state, "ALTER TABLE t ADD CONSTRAINT same UNIQUE(v)"),
        Err(SqlError::DuplicateObject)
    );
    assert_eq!(state, before);
}

#[test]
fn drop_rejects_foreign_key_source_and_non_primary_target_columns_atomically() {
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE parent(id int PRIMARY KEY,code int)",
    )
    .unwrap();
    run(
        &mut state,
        "CREATE UNIQUE INDEX parent_code ON parent(code)",
    )
    .unwrap();
    run(
        &mut state,
        "CREATE TABLE child(id int PRIMARY KEY,parent_code int)",
    )
    .unwrap();
    let parent = state.tables["parent"].clone();
    let child = state.tables["child"].clone();
    state
        .add_constraint(
            child.id,
            Constraint {
                id: state.next_id,
                name: "child_parent_fk".into(),
                kind: ConstraintKind::ForeignKey {
                    columns: vec![child.columns[1].id],
                    ref_table: parent.id,
                    ref_columns: vec![parent.columns[1].id],
                    on_delete: catalog_ext::Action::NoAction,
                    on_update: catalog_ext::Action::NoAction,
                    not_valid: false,
                    deferral: Deferral::NotDeferrable,
                    target: crate::relational::catalog_ext::ReferencedKey::Unknown,
                },
            },
        )
        .unwrap();
    let before = state.clone();
    // Intentional bounded 0A000 rule, including a target not protected by its PK.
    for sql in [
        "ALTER TABLE child DROP COLUMN parent_code",
        "ALTER TABLE parent DROP COLUMN code",
    ] {
        assert_eq!(run(&mut state, sql), Err(SqlError::Unsupported));
        assert_eq!(state, before);
    }
}

#[test]
fn whole_statement_syntax_precedes_target_and_action_policy() {
    for target in ["missing", "deliveries", "public.deliveries", "absent.t"] {
        for actions in [
            "ADD COLUMN n int,",
            "ADD COLUMN giggles_rowid int, ALTER COLUMN n SET",
            "ADD COLUMN n numeric(0), ALTER COLUMN n SET",
            "DROP COLUMN absent garbage",
        ] {
            let sql = format!("ALTER TABLE {target} {actions}");
            assert_eq!(
                Prepared::alter_table_target(&sql),
                Err(SqlError::Syntax),
                "{sql}"
            );
        }
    }
    assert_eq!(
        Prepared::alter_table_target("/* comment */ ALTER TABLE missing ADD COLUMN n int"),
        Ok(Some("missing".into()))
    );
    assert_eq!(
        Prepared::alter_table_target(
            "/* comment */ ALTER TABLE t ADD COLUMN n text DEFAULT 'unterminated"
        ),
        Err(SqlError::Syntax)
    );
    assert_eq!(
        Prepared::alter_table_target("SELECT 'unterminated"),
        Ok(None)
    );
}

#[test]
fn relation_then_column_then_unique_semantic_errors() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE t(id int PRIMARY KEY,v int)").unwrap();
    assert_eq!(
        run(&mut state, "ALTER TABLE missing DROP COLUMN absent"),
        Err(SqlError::UnknownTable)
    );
    assert_eq!(
        run(&mut state, "ALTER TABLE deliveries DROP COLUMN absent"),
        Err(SqlError::Reserved)
    );
    assert_eq!(
        run(&mut state, "ALTER TABLE t ADD CONSTRAINT t UNIQUE(absent)"),
        Err(SqlError::UnknownColumn)
    );
}

#[test]
fn missing_add_datatype_is_syntax_before_target_resolution() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE t(id int PRIMARY KEY,v int)").unwrap();
    for target in ["t", "missing", "deliveries", "public.deliveries"] {
        for actions in [
            "ADD COLUMN x",
            "ADD COLUMN x;",
            "ADD COLUMN x, DROP COLUMN absent",
            "ADD CONSTRAINT c",
            "ADD CONSTRAINT c;",
            "ADD CONSTRAINT c, DROP COLUMN absent",
        ] {
            let sql = format!("ALTER TABLE {target} {actions}");
            assert_eq!(
                Prepared::alter_table_target(&sql),
                Err(SqlError::Syntax),
                "{sql}"
            );
            assert_eq!(run(&mut state, &sql), Err(SqlError::Syntax), "{sql}");
        }
    }
}

#[test]
fn hidden_rowid_rejection_follows_complete_parse_and_table_lookup() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE heap(v int)").unwrap();
    let before = state.clone();
    assert_eq!(
        run(&mut state, "ALTER TABLE heap DROP COLUMN giggles_rowid"),
        Err(SqlError::Reserved)
    );
    assert_eq!(state, before);
    assert_eq!(
        run(&mut state, "ALTER TABLE missing DROP COLUMN giggles_rowid"),
        Err(SqlError::UnknownTable)
    );
    assert_eq!(
        run(&mut state, "ALTER TABLE heap DROP COLUMN giggles_rowid,"),
        Err(SqlError::Syntax)
    );
}

#[test]
fn supported_add_constraints_are_admitted_before_resolution() {
    for target in ["t", "missing", "deliveries"] {
        for action in [
            "ADD CHECK(v>0)",
            "ADD PRIMARY KEY(v)",
            "ADD FOREIGN KEY(v) REFERENCES parent(v)",
        ] {
            let sql = format!("ALTER TABLE {target} {action}");
            assert!(Prepared::alter_table_target(&sql).is_ok(), "{sql}");
        }
    }
}
