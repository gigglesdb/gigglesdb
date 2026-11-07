use super::*;
use crate::operations::{Compare, Engine, Error, Expr, Intent, Value as V, ValueExpr};
use crate::relational::{Prepared, with_sql_parser_forbidden};
use crate::{Admin, Cancellation, Limits};

fn eq(name: &str, value: V) -> Expr {
    Expr::Compare(
        Box::new(Expr::Field(name.into())),
        Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Literal(value))),
    )
}
fn read(id: i64) -> Request {
    Request::Read {
        table: "items".into(),
        filter: Some(eq("id", V::Int(id))),
        order: vec![],
        limit: None,
        projection: vec![],
    }
}
fn update(value: V, expected: i64) -> Request {
    Request::Update {
        table: "items".into(),
        filter: eq("id", V::Int(1)),
        guard: Some(eq("n", V::Int(expected))),
        values: vec![("n".into(), ValueExpr::Literal(value))],
        intent: Intent::One,
    }
}
fn fixture(count: usize) -> (Admin, State) {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let admin = Admin::create(
        root.join(format!("state-adapter-{:032x}", u128::from_be_bytes(token))),
        Limits::default(),
    )
    .unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)",
            &[],
            None,
        )
        .unwrap();
    session.begin_engine().unwrap();
    let mut state = session.staged.as_ref().unwrap().clone();
    for id in 1..=count {
        let table = state.sql.tables["items"].clone();
        let prepared = Prepared::insert_engine(
            &table,
            vec![V::Int(id as i64), V::Int(10)],
            state.view.domain,
            &state.sql,
        )
        .unwrap();
        state.sql.execute(&prepared, &[], None).unwrap();
    }
    session.staged = Some(state.clone());
    session.commit_engine().unwrap();
    (admin, state)
}
fn bytes(state: &State) -> Vec<u8> {
    bincode::serialize(state).unwrap()
}

#[test]
fn differential_read_write_and_cas_failures() {
    for request in [
        read(1),
        read(2),
        update(V::Int(11), 10),
        update(V::Int(10), 10),
        update(V::Int(11), 9),
        update(V::Null, 10),
        update(V::Text("bad".into()), 10),
    ] {
        let (admin, state) = fixture(1);
        let before = bytes(&state);
        let pure = with_sql_parser_forbidden(|| execute(&state, &request, &Bindings::new()));
        let actual = with_sql_parser_forbidden(|| {
            Engine::new(admin.clone()).execute(&request, &Bindings::new())
        });
        assert_eq!(
            pure.as_ref().map(|(_, out)| out),
            actual.as_ref(),
            "{request:?}"
        );
        assert_eq!(bytes(&state), before);
        if let Ok((next, _)) = pure {
            let pure_rows = execute(&next, &read(1), &Bindings::new()).unwrap().1;
            assert_eq!(
                pure_rows,
                Engine::new(admin)
                    .execute(&read(1), &Bindings::new())
                    .unwrap()
            );
        }
    }
}
#[test]
fn exact_fences_and_versions() {
    let (admin, state) = fixture(1);
    let engine = Engine::new(admin);
    let request = update(V::Int(11), 10);
    let description = engine
        .prepare_bound(&request, &Bindings::new())
        .unwrap()
        .description()
        .clone();
    let version = execute(&state, &read(1), &Bindings::new()).unwrap().1.rows[0].version;
    assert!(
        execute_fenced(
            &state,
            &request,
            &Bindings::new(),
            &description,
            Some(version)
        )
        .is_ok()
    );
    assert!(matches!(
        execute_fenced(
            &state,
            &request,
            &Bindings::new(),
            &description,
            Some(version + 1)
        ),
        Err(Error::Conflict)
    ));
    assert!(execute_fenced(&state, &request, &Bindings::new(), &description, None).is_err());
    for fence in 0..6 {
        let mut changed = description.clone();
        match fence {
            0 => changed.database_domain += 1,
            1 => changed.catalog_epoch += 1,
            2 => changed.table.id += 1,
            3 => changed.table.columns[0].id += 1,
            4 => changed.contract_version += 1,
            _ => changed.columns.push(changed.table.columns[0].clone()),
        }
        assert!(
            execute_fenced(&state, &request, &Bindings::new(), &changed, Some(version)).is_err()
        );
    }
}
#[test]
fn deterministic_replay_ignores_ambient_cancellation() {
    let (_, state) = fixture(1);
    let request = update(V::Int(11), 10);
    let first = execute(&state, &request, &Bindings::new()).unwrap();
    let cancelled = Cancellation::default();
    cancelled.cancel();
    let second = cancelled
        .scope(|| with_sql_parser_forbidden(|| execute(&state, &request, &Bindings::new())))
        .unwrap();
    assert_eq!(bytes(&first.0), bytes(&second.0));
    assert_eq!(first.1, second.1);
}
#[test]
fn unsupported_grammar_unknown_fields_and_bounds_fail_closed() {
    let (_, state) = fixture(1);
    let before = bytes(&state);
    let mut requests = vec![update(V::Int(11), 10)];
    if let Request::Update { values, .. } = &mut requests[0] {
        values[0].0 = "missing".into();
    }
    let mut parameter_limit = read(1);
    if let Request::Read { limit, .. } = &mut parameter_limit {
        *limit = Some(ValueExpr::Parameter("cap".into()));
    }
    requests.push(parameter_limit);
    let mut scan = read(1);
    if let Request::Read { filter, .. } = &mut scan {
        *filter = None;
    }
    requests.push(scan);
    let mut function = read(1);
    if let Request::Read { filter, .. } = &mut function {
        *filter = Some(Expr::Call {
            function: crate::operations::ScalarFunction::Abs,
            arguments: vec![Expr::Value(ValueExpr::Literal(V::Int(1)))],
        });
    }
    requests.push(function);
    for request in requests {
        assert!(execute(&state, &request, &Bindings::new()).is_err());
        assert_eq!(bytes(&state), before);
    }
    let mut bindings = Bindings::new();
    for i in 0..=crate::operations::MAX_BINDINGS {
        bindings.insert(format!("p{i}"), V::Null);
    }
    assert!(matches!(
        execute(&state, &read(1), &bindings),
        Err(Error::Limit)
    ));
}
#[test]
fn complete_relation_n_and_n_plus_one_match_session() {
    for count in [crate::operations::MAX_ROWS, crate::operations::MAX_ROWS + 1] {
        let (admin, state) = fixture(count);
        let pure = execute(&state, &read(1), &Bindings::new());
        let actual = Engine::new(admin).execute(&read(1), &Bindings::new());
        assert_eq!(pure.as_ref().map(|(_, out)| out), actual.as_ref());
        assert_eq!(pure.is_ok(), count == crate::operations::MAX_ROWS);
    }
}

#[test]
fn overflow_is_atomic() {
    let (_, mut state) = fixture(1);
    // Relational's canonical bincode header is format:u32 followed by
    // generation, epoch, next_id, next_version:u64. Inject allocator overflow
    // without adding a production mutation/test hook to that private allocator.
    let mut encoded = bincode::serialize(&state.sql).unwrap();
    encoded[28..36].copy_from_slice(&u64::MAX.to_le_bytes());
    state.sql = bincode::deserialize(&encoded).unwrap();
    let before = bytes(&state);
    assert!(execute(&state, &update(V::Int(11), 10), &Bindings::new()).is_err());
    assert_eq!(before, bytes(&state));
}
