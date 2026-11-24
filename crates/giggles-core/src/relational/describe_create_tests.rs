use super::*;

fn create(state: &mut Relational, name: &str, typmods: Vec<Option<i32>>) -> R<SqlResult> {
    let mut prepared = Prepared::parse(
        &format!("CREATE TABLE {name} (v NUMERIC, plain INTEGER)"),
        1,
        state.epoch,
    )?;
    let Statement::Create {
        typmods: target, ..
    } = prepared.statement.get_mut()
    else {
        unreachable!()
    };
    *target = typmods;
    state.execute(&prepared, &[], None)
}

#[test]
fn create_typmods_validate_before_any_mutation() {
    let mut state = Relational::default();
    let before = state.clone();
    let valid = (10 << 16) + 2 + 4;
    for typmods in [
        vec![Some(-1), None],
        vec![Some(valid), Some(4)],
        vec![Some(valid)],
    ] {
        assert_eq!(
            create(&mut state, "bad", typmods),
            Err(SqlError::InvalidParameterValue)
        );
        assert_eq!(state, before);
    }
    let mut timestamp =
        Prepared::parse("CREATE TABLE bad_time (v TIMESTAMP)", 1, state.epoch).unwrap();
    let Statement::Create { typmods, .. } = timestamp.statement.get_mut() else {
        unreachable!()
    };
    *typmods = vec![Some(7)];
    assert_eq!(
        state.execute(&timestamp, &[], None),
        Err(SqlError::InvalidParameterValue)
    );
    assert_eq!(state, before);
    create(&mut state, "good", vec![Some(valid), None]).unwrap();
    assert_eq!(
        state.column_typmod(state.tables["good"].columns[0].id),
        valid
    );
    assert_eq!(state.column_typmod(state.tables["good"].columns[1].id), -1);
    state.validate_ext().unwrap();
}

#[test]
fn descriptions_preserve_only_plain_column_typmods() {
    let mut state = Relational::default();
    let typmod = (10 << 16) + 2 + 4;
    create(&mut state, "t", vec![Some(typmod), None]).unwrap();
    for (sql, expected) in [
        ("SELECT * FROM t", vec![typmod, -1]),
        ("SELECT v AS alias FROM t", vec![typmod]),
        ("SELECT v AS aliased, plain FROM t", vec![typmod, -1]),
        (
            "SELECT CAST(v AS NUMERIC), coalesce(v, v) FROM t",
            vec![-1, -1],
        ),
        ("SELECT v + 0 FROM t", vec![-1]),
        ("SELECT count(*) FROM t", vec![-1]),
        ("EXPLAIN SELECT v FROM t", vec![-1]),
        ("INSERT INTO t VALUES (1, 2)", vec![]),
        (
            "INSERT INTO t VALUES (1, 2), (3, 4) RETURNING v",
            vec![typmod],
        ),
        ("INSERT INTO t VALUES (1 + 2, 2) RETURNING v", vec![typmod]),
        ("UPDATE t SET plain = plain + 1 RETURNING v", vec![typmod]),
        (
            "INSERT INTO t VALUES (1, 2) RETURNING v, v + 0, plain",
            vec![typmod, -1, -1],
        ),
        ("UPDATE t SET plain = 2 RETURNING v", vec![typmod]),
        ("DELETE FROM t RETURNING v", vec![typmod]),
    ] {
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        let description = prepared.describe(&state).unwrap();
        assert_eq!(
            description.typmods.len(),
            description.columns.len(),
            "{sql}"
        );
        assert_eq!(description.typmods, expected, "{sql}");
    }
}
