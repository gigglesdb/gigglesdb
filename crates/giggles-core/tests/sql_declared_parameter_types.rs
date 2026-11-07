use giggles_core::{Admin, CoreError, Limits, relational::*};

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("sql-declared-parameter-types");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

#[test]
fn declared_bytea_position_binds_describes_validates_and_executes() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "SELECT position($1 in $2)",
        "SELECT position($1::bytea in CAST($2 AS bytea))",
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
            .unwrap();
        let description = session.describe(&prepared).unwrap();
        assert_eq!(description.parameters, vec![DataType::Bytes; 2], "{sql}");
        assert_eq!(description.columns[0].data_type, DataType::Integer);
        session.validate_immutable_constants(&prepared).unwrap();
        for (needle, source, expected) in [
            (
                Value::Bytes(vec![0, 255]),
                Value::Bytes(vec![1, 0, 255, 2]),
                Value::Int(2),
            ),
            (
                Value::Bytes(vec![255, 0]),
                Value::Bytes(vec![1, 0, 255, 2]),
                Value::Int(0),
            ),
            (Value::Null, Value::Bytes(vec![1, 0, 255, 2]), Value::Null),
            (Value::Bytes(vec![0, 255]), Value::Null, Value::Null),
        ] {
            assert_eq!(
                session
                    .execute_prepared(&prepared, &[needle, source], None)
                    .unwrap()
                    .rows[0]
                    .values,
                vec![expected],
                "{sql}"
            );
        }
    }
}

#[test]
fn position_defaults_to_text_and_declarations_select_byte_or_character_offsets() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for declarations in [vec![], vec![None, None], vec![Some(DataType::Text); 2]] {
        let prepared = session
            .prepare("SELECT position($1 in $2)")
            .unwrap()
            .with_parameter_types(&declarations)
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Text; 2]
        );
        assert_eq!(
            session
                .execute_prepared(
                    &prepared,
                    &[Value::Text("z".into()), Value::Text("\u{e9}z".into())],
                    None,
                )
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(2)]
        );
    }
    let prepared = session
        .prepare("SELECT position($1 in $2)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[
                    Value::Bytes(b"z".to_vec()),
                    Value::Bytes("\u{e9}z".as_bytes().to_vec())
                ],
                None,
            )
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(3)]
    );
}

#[test]
fn declaration_prefixes_holes_clones_and_reset_preserve_query_inference() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for declarations in [
        vec![Some(DataType::Bytes)],
        vec![None, Some(DataType::Bytes)],
    ] {
        let prepared = session
            .prepare("SELECT position($1 in $2)")
            .unwrap()
            .with_parameter_types(&declarations)
            .unwrap();
        assert_eq!(prepared.parameter_count(), 2);
        let cloned = prepared.clone();
        assert_eq!(
            session.describe(&cloned).unwrap().parameters,
            vec![DataType::Bytes; 2]
        );
        assert_eq!(
            session
                .execute_prepared(
                    &cloned,
                    &[Value::Bytes(vec![0]), Value::Bytes(vec![1, 0])],
                    None,
                )
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(2)]
        );
        let reset = cloned.with_parameter_types(&[]).unwrap();
        assert_eq!(
            session.describe(&reset).unwrap().parameters,
            vec![DataType::Text; 2]
        );
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Bytes; 2]
        );
        let independent = session.prepare("SELECT position($1 in $2)").unwrap();
        assert_eq!(
            session.describe(&independent).unwrap().parameters,
            vec![DataType::Text; 2]
        );
    }
}

#[test]
fn declarations_are_input_types_and_explicit_casts_preserve_them() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let prepared = session
        .prepare("SELECT $1::bigint,$1 LIMIT $1")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    let description = session.describe(&prepared).unwrap();
    assert_eq!(description.parameters, vec![DataType::Integer]);
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![DataType::BigInt, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(&prepared, &[Value::Int(1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1), Value::Int(1)]
    );
    session
        .execute(
            "CREATE TABLE declared_inputs(id integer primary key)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO declared_inputs VALUES(1)", &[], None)
        .unwrap();
    let prepared = session
        .prepare("SELECT count($1) FROM declared_inputs")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes]
    );
    assert_eq!(
        session
            .execute_prepared(&prepared, &[Value::Bytes(vec![0])], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
}

#[test]
fn declared_type_conflicts_return_existing_binder_errors_at_every_query_entry() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE declared_inputs(id integer primary key)",
            &[],
            None,
        )
        .unwrap();
    for (sql, declarations, expected) in [
        (
            "SELECT upper($1)",
            vec![Some(DataType::Bytes)],
            SqlError::Type,
        ),
        (
            "SELECT id FROM declared_inputs WHERE id=$1",
            vec![Some(DataType::Bytes)],
            SqlError::Type,
        ),
        (
            "SELECT position($1 in $2)",
            vec![Some(DataType::Bytes), Some(DataType::Text)],
            SqlError::UndefinedFunction,
        ),
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&declarations)
            .unwrap();
        assert!(
            matches!(session.describe(&prepared), Err(CoreError::Sql(error)) if error == expected),
            "{sql}"
        );
        assert!(
            matches!(session.validate_immutable_constants(&prepared), Err(CoreError::Sql(error)) if error == expected),
            "{sql}"
        );
        let values = declarations.iter().map(|_| Value::Null).collect::<Vec<_>>();
        assert!(
            matches!(session.execute_prepared(&prepared, &values, None), Err(CoreError::Sql(error)) if error == expected),
            "{sql}"
        );
    }
}

#[test]
fn declaration_length_checks_precede_statement_kind_and_nonqueries_ignore_types() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE declared_inputs(id integer primary key)",
            &[],
            None,
        )
        .unwrap();
    for sql in [
        "SELECT $1::integer",
        "INSERT INTO declared_inputs VALUES($1)",
        "BEGIN",
    ] {
        let prepared = session.prepare(sql).unwrap();
        assert!(matches!(
            prepared
                .clone()
                .with_parameter_types(&vec![None; MAX_PARAMS + 1]),
            Err(SqlError::Limit)
        ));
        assert!(matches!(
            prepared
                .clone()
                .with_parameter_types(&vec![None; prepared.parameter_count() + 1]),
            Err(SqlError::Parameters)
        ));
    }
    let prepared = session
        .prepare("INSERT INTO declared_inputs VALUES($1)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert!(matches!(
        session.describe(&prepared),
        Err(CoreError::Sql(SqlError::Type))
    ));
    assert!(matches!(
        session.execute_prepared(&prepared, &[Value::Int(1)], None),
        Err(CoreError::Sql(SqlError::Type))
    ));
    let prepared = session
        .prepare("INSERT INTO declared_inputs VALUES(abs($1::integer))")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Integer]
    );
    session
        .execute_prepared(&prepared, &[Value::Int(-2)], None)
        .unwrap();
}

#[cfg(feature = "test-sql-parser-trap")]
#[test]
fn declaring_parameter_types_obeys_the_sql_frontend_guard() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let prepared = session.prepare("SELECT $1::integer").unwrap();
    assert!(
        std::panic::catch_unwind(|| {
            with_sql_parser_forbidden(|| prepared.with_parameter_types(&[]))
        })
        .is_err()
    );
}
