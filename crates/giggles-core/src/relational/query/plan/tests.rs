use super::*;
fn run(state: &mut Relational, sql: &str) -> SqlResult {
    let p = Prepared::parse(sql, 1, state.epoch).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    state
        .execute(&p, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn values(sql: &str) -> Vec<Vec<Value>> {
    run(&mut Relational::default(), sql)
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
#[test]
fn set_precedence_and_operand_pagination() {
    use Value::Int as I;
    for (sql, expected) in [
        (
            "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2 ORDER BY 1",
            vec![vec![I(1)], vec![I(2)]],
        ),
        (
            "SELECT 1 UNION SELECT 2 EXCEPT SELECT 1 ORDER BY 1",
            vec![vec![I(2)]],
        ),
        (
            "(SELECT 3 AS n UNION SELECT 1 ORDER BY n LIMIT 1) UNION ALL (SELECT 2 ORDER BY 1 LIMIT 1) ORDER BY n DESC OFFSET 1 LIMIT 1",
            vec![vec![I(1)]],
        ),
        (
            "((VALUES(3),(1) ORDER BY 1 LIMIT 1) LIMIT 1) UNION SELECT 2 ORDER BY 1",
            vec![vec![I(1)], vec![I(2)]],
        ),
    ] {
        assert_eq!(values(sql), expected, "{sql}");
    }
}
#[test]
fn multiset_nulls_and_counts() {
    use Value::{Int as I, Null as N};
    for (op, expected) in [
        ("UNION", vec![vec![I(1)], vec![I(2)], vec![N]]),
        (
            "UNION ALL",
            vec![
                vec![I(1)],
                vec![I(1)],
                vec![I(1)],
                vec![I(2)],
                vec![N],
                vec![N],
                vec![N],
            ],
        ),
        ("INTERSECT", vec![vec![I(1)], vec![N]]),
        ("INTERSECT ALL", vec![vec![I(1)], vec![N]]),
        ("EXCEPT", vec![]),
        ("EXCEPT ALL", vec![vec![I(1)], vec![N]]),
    ] {
        assert_eq!(
            values(&format!(
                "VALUES(1),(1),(NULL),(NULL) {op} VALUES(1),(2),(NULL) ORDER BY 1 NULLS LAST"
            )),
            expected,
            "{op}"
        );
    }
    assert_eq!(
        values("VALUES(NULL),(NULL) INTERSECT ALL VALUES(NULL),(NULL),(NULL)"),
        vec![vec![N], vec![N]]
    );
}
#[test]
fn values_scalar_semantics_aliases_table_and_select_all() {
    use Value::{Int as I, Text as T};
    assert_eq!(
        values("VALUES(2,upper('b')),(1,'a'||'!') ORDER BY 1"),
        vec![vec![I(1), T("a!".into())], vec![I(2), T("B".into())]]
    );
    assert_eq!(
        values(
            "SELECT v.id+1 AS n,upper(v.label) FROM (VALUES(2,'b'),(1,'a')) AS v(id,label) WHERE id>0 ORDER BY id USING >"
        ),
        vec![vec![I(3), T("B".into())], vec![I(2), T("A".into())]]
    );
    assert_eq!(
        values("VALUES(1),(2) ORDER BY column1+1 USING >"),
        vec![vec![I(2)], vec![I(1)]]
    );
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE plan_t(id int PRIMARY KEY, v text)",
    );
    run(&mut state, "INSERT INTO plan_t VALUES(1,'a'),(2,'a')");
    assert_eq!(run(&mut state, "TABLE plan_t ORDER BY id").rows.len(), 2);
    assert_eq!(
        run(&mut state, "SELECT ALL v FROM plan_t ORDER BY v,id")
            .rows
            .len(),
        2
    );
}
#[test]
fn set_types_names_and_errors() {
    let state = Relational::default();
    for (sql, ty) in [
        ("SELECT '1' AS first_name UNION SELECT 2", DataType::Integer),
        (
            "SELECT 1::smallint UNION SELECT 2::bigint",
            DataType::BigInt,
        ),
        ("SELECT NULL UNION SELECT 'x'", DataType::Text),
        ("VALUES(1::smallint),(2::bigint)", DataType::BigInt),
        ("SELECT 1::numeric UNION SELECT 2", DataType::Numeric),
    ] {
        let p = Prepared::parse(sql, 1, 0).unwrap();
        let d = p
            .describe(&state)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(d.columns[0].data_type, ty, "{sql}");
    }
    for (sql, ty) in [
        ("VALUES(1::real),(2::numeric)", DataType::Real),
        ("VALUES('a'::varchar),('b'::text)", DataType::Varchar),
        (
            "SELECT '2020-01-01'::date UNION SELECT '2020-01-02'::timestamp",
            DataType::Timestamp,
        ),
    ] {
        let p = Prepared::parse(sql, 1, 0).unwrap();
        assert_eq!(
            p.describe(&state).unwrap().columns[0].data_type,
            ty,
            "{sql}"
        );
    }
    let result = run(
        &mut Relational::default(),
        "SELECT '1' AS first_name UNION SELECT 2 AS second_name ORDER BY first_name",
    );
    assert_eq!(result.columns, vec!["first_name"]);
    assert_eq!(result.rows[0].values, vec![Value::Int(1)]);
    for (sql, error) in [
        ("SELECT true UNION SELECT 1", SqlError::Type),
        ("SELECT 1 UNION SELECT 1,2", SqlError::Syntax),
        ("VALUES(1),(1,2)", SqlError::Syntax),
        (
            "(SELECT NULL UNION SELECT NULL) UNION SELECT 1",
            SqlError::Type,
        ),
        (
            "SELECT 1 UNION SELECT 2 ORDER BY 2",
            SqlError::InvalidColumnReference,
        ),
    ] {
        let result = Prepared::parse(sql, 1, 0).and_then(|p| p.describe(&state));
        assert!(
            matches!(result,Err(ref e) if *e == error),
            "{sql}: {result:?}"
        );
    }
}
#[test]
fn read_and_authorization_dependencies_and_schema_binding() {
    let mut state = Relational::default();
    run(&mut state, "CREATE SCHEMA plan_s");
    run(&mut state, "CREATE TABLE plan_s.t(id int PRIMARY KEY)");
    run(&mut state, "INSERT INTO plan_s.t VALUES(7)");
    let p = Prepared::parse(
        "TABLE plan_s.t UNION SELECT id FROM plan_s.t",
        1,
        state.epoch,
    )
    .unwrap();
    assert!(p.is_read());
    assert!(matches!(p.v2_class(), V2Class::Read));
    assert_eq!(p.read_tables(), vec!["plan_s\0t"]);
    assert_eq!(p.authorization_tables().unwrap(), p.read_tables());
    assert_eq!(
        state.execute(&p, &[], None).unwrap().rows[0].values,
        vec![Value::Int(7)]
    );
    let p = Prepared::parse("SELECT * FROM (VALUES(1)) v(x)", 1, state.epoch).unwrap();
    assert!(p.read_tables().is_empty());
    assert!(p.authorization_tables().unwrap().is_empty());
    assert!(
        p.describe(&state)
            .unwrap()
            .columns
            .iter()
            .all(|c| c.id == 0)
    );
}
#[test]
fn prepared_parameters_and_null_ordering() {
    let mut state = Relational::default();
    let p = Prepared::parse("SELECT $1 AS x UNION SELECT 2 ORDER BY 1 LIMIT $2", 1, 0).unwrap();
    assert_eq!(
        p.describe(&state).unwrap().parameters,
        vec![DataType::Integer, DataType::BigInt]
    );
    assert_eq!(
        state
            .execute(&p, &[Value::Int(1), Value::Int(1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
    let p = Prepared::parse("VALUES($1),($2) ORDER BY 1", 1, 0)
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt), Some(DataType::BigInt)])
        .unwrap();
    assert_eq!(
        p.describe(&state).unwrap().parameters,
        vec![DataType::BigInt, DataType::BigInt]
    );
    assert_eq!(
        values("VALUES(1),(NULL),(2) ORDER BY 1 DESC"),
        vec![vec![Value::Null], vec![Value::Int(2)], vec![Value::Int(1)]]
    );
    assert_eq!(
        values("VALUES(1),(NULL),(2) ORDER BY 1 USING > NULLS LAST"),
        vec![vec![Value::Int(2)], vec![Value::Int(1)], vec![Value::Null]]
    );
}
#[test]
fn cancellation_and_retention_are_statement_scoped() {
    let mut cancel = || true;
    let state = Relational::default();
    let mut p = Parser {
        tokens: lex("VALUES(1) UNION SELECT 2").unwrap(),
        at: 0,
        params: BTreeSet::new(),
        relations: vec![],
    };
    let plan = Plan::parse(&mut p).unwrap();
    assert_eq!(
        plan.execute(
            &state,
            &scan::LegacyRows(&state),
            0,
            &[],
            &[],
            None,
            &mut cancel
        ),
        Err(SqlError::Aborted)
    );
    let mut cancel = || false;
    let execution = Execution::new(&mut cancel, &[]);
    *execution.retention.borrow_mut() = scan::Retention::new(1);
    assert!(matches!(
        plan.run(
            &state,
            &scan::LegacyRows(&state),
            &[],
            &[],
            None,
            &execution
        ),
        Err(SqlError::Limit)
    ));
    assert_eq!(execution.retention.borrow().used(), 0);
}
