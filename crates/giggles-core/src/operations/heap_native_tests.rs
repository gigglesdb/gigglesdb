//! SQL-only heap boundaries and keyed controls, GD-0058 WO-B3n.
use super::*;
use crate::relational::{Prepared, Relational};
use crate::storage::State;

fn state() -> State {
    let mut state = State {
        applications: None,
        accounting: BTreeMap::new(),
        native_outcomes: None,
        sql: Relational::default(),
        view: crate::Snapshot {
            domain: 1,
            schema: 0,
            rows: BTreeMap::new(),
            tasks: BTreeMap::new(),
            operations: BTreeMap::new(),
            approvals: BTreeMap::new(),
        },
        last_time: 0,
        next_approval: 0,
        origin: None,
    };
    for sql in [
        "CREATE TABLE heap (id BIGINT, n BIGINT)",
        "CREATE TABLE keyed (id BIGINT PRIMARY KEY, n BIGINT)",
        "INSERT INTO heap VALUES (1, 10)",
        "INSERT INTO keyed VALUES (1, 10)",
    ] {
        let prepared = Prepared::parse(sql, 1, state.sql.epoch).unwrap();
        state.sql.execute(&prepared, &[], None).unwrap();
    }
    state
}
fn equal(name: &str, value: Value) -> Expr {
    Expr::Compare(
        Box::new(Expr::Field(name.into())),
        Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Literal(value))),
    )
}
fn read(table: &str) -> Request {
    Request::Read {
        table: table.into(),
        filter: Some(equal("id", Value::Int(1))),
        order: vec![],
        limit: None,
        projection: vec![],
    }
}

#[test]
fn heap_typed_mutations_and_row_version_reject_while_keyed_succeed() {
    let mut state = state().sql;
    let heap = state.tables["heap"].clone();
    assert_eq!(
        Prepared::insert_engine(&heap, vec![], 1, &state).err(),
        Some(SqlError::Unsupported)
    );
    assert_eq!(
        Prepared::insert_engine(&heap, vec![Value::Text("x".repeat(crate::relational::MAX_ROW_BYTES + 1))], 1, &state).err(),
        Some(SqlError::Unsupported)
    );
    assert_eq!(
        Prepared::update_engine(&heap, vec![], vec![], 1, &state).err(),
        Some(SqlError::Unsupported)
    );
    assert_eq!(
        Prepared::delete_engine(&heap, vec![], 1, &state).err(),
        Some(SqlError::Unsupported)
    );
    assert_eq!(state.row_version_engine(&heap, &[]), Err(SqlError::Unsupported));
    let keyed = state.tables["keyed"].clone();
    let prepared = Prepared::insert_engine(&keyed, vec![Value::Int(2), Value::Int(20)], 1, &state).unwrap();
    assert_eq!(state.execute(&prepared, &[], None).unwrap().affected, 1);
    let version = state.row_version_engine(&keyed, &[Value::Int(2)]).unwrap();
    let prepared = Prepared::update_engine(&keyed, vec![Value::Int(2)], vec![(1, Value::Int(30))], 1, &state).unwrap();
    assert_eq!(state.execute(&prepared, &[], Some(version)).unwrap().affected, 1);
    let version = state.row_version_engine(&keyed, &[Value::Int(2)]).unwrap();
    let prepared = Prepared::delete_engine(&keyed, vec![Value::Int(2)], 1, &state).unwrap();
    assert_eq!(state.execute(&prepared, &[], Some(version)).unwrap().affected, 1);
    let mut stale = heap;
    stale.id += 1;
    assert_eq!(Prepared::delete_engine(&stale, vec![], 1, &state).err(), Some(SqlError::StaleSchema));
}

#[test]
fn heap_snapshot_point_filters_reject_while_keyed_read_and_guarded_write_succeed() {
    let state = state();
    let bindings = Bindings::new();
    assert!(matches!(state_adapter::execute(&state, &read("heap"), &bindings), Err(Error::Unsupported)));
    let (_, result) = state_adapter::execute(&state, &read("keyed"), &bindings).unwrap();
    assert_eq!(result.rows[0].values, [Value::Int(1), Value::Int(10)]);
    for (table, expected) in [("heap", false), ("keyed", true)] {
        let request = Request::Update {
            table: table.into(),
            filter: equal("id", Value::Int(1)),
            guard: Some(equal("n", Value::Int(10))),
            values: vec![("n".into(), ValueExpr::Literal(Value::Int(20)))],
            intent: Intent::One,
        };
        let result = state_adapter::execute(&state, &request, &bindings);
        if expected {
            assert_eq!(result.unwrap().1.changed, 1);
        } else {
            assert!(matches!(result, Err(Error::Unsupported)));
        }
    }
}

#[test]
fn heap_secondary_selector_rejects_before_optional_fallback() {
    let state = state();
    let filter = Bound::Compare(Box::new(Bound::Field(0)), Compare::Eq, Box::new(Bound::Value(Value::Int(1))));
    let heap = &state.sql.tables["heap"];
    let keyed = &state.sql.tables["keyed"];
    assert!(secondary_selector::choose(heap, &filter).is_none());
    assert!(secondary_selector::choose(keyed, &filter).is_some());
    // Snapshot execution has no live session: keyed selection retains fallback.
    struct NoSession;
    impl Context for NoSession {
        type Input = ();
        fn checkpoint(&self) -> std::result::Result<(), CoreError> { Ok(()) }
        fn catalog_engine(&mut self) -> std::result::Result<(u64, Vec<TableDef>), CoreError> { Err(CoreError::Unsupported) }
        fn consume_complete_engine(&mut self, _: (), _: &TableDef) -> std::result::Result<Vec<SqlRow>, CoreError> { Err(CoreError::Unsupported) }
        fn insert_engine(&mut self, _: &TableDef, _: Vec<Value>) -> std::result::Result<crate::relational::SqlResult, CoreError> { Err(CoreError::Unsupported) }
        fn update_engine(&mut self, _: &TableDef, _: Vec<Value>, _: Vec<(usize, Value)>, _: u64) -> std::result::Result<crate::relational::SqlResult, CoreError> { Err(CoreError::Unsupported) }
        fn delete_engine(&mut self, _: &TableDef, _: Vec<Value>, _: u64) -> std::result::Result<crate::relational::SqlResult, CoreError> { Err(CoreError::Unsupported) }
        fn row_version_engine(&mut self, _: &TableDef, _: &[Value]) -> std::result::Result<u64, CoreError> { Err(CoreError::Unsupported) }
        fn scan(&mut self, _: Scan) -> std::result::Result<crate::relational::SqlResult, CoreError> { Err(CoreError::Unsupported) }
    }
    assert_eq!(secondary_selector::read(&mut NoSession, heap, &filter, vec![]), Err(Error::Unsupported));
    assert_eq!(secondary_selector::read(&mut NoSession, keyed, &filter, vec![]), Ok(vec![]));
}
