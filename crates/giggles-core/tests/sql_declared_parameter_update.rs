use giggles_core::{Admin, CoreError, Limits, relational::*};

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("sql-declared-parameter-update");
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
fn undeclared_update_position_defaults_to_text() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,0)", &[], None)
        .unwrap();
    for empty_declaration in [false, true] {
        let prepared = session
            .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
            .unwrap();
        let prepared = if empty_declaration {
            prepared.with_parameter_types(&[]).unwrap()
        } else {
            prepared
        };
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Text; 2]
        );
        assert_eq!(
            session
                .execute_prepared(
                    &prepared,
                    &[Value::Text("z".into()), Value::Text("éz".into())],
                    None
                )
                .unwrap()
                .affected,
            1
        );
        assert_eq!(
            session
                .execute("SELECT n FROM t WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(2)]
        );
    }
}
#[test]
fn declared_bytea_update_position_and_nulls() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,0)", &[], None)
        .unwrap();
    let prepared = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes; 2]
    );
    for (needle, haystack, expected) in [
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
                .execute_prepared(&prepared, &[needle, haystack], None)
                .unwrap()
                .affected,
            1
        );
        assert_eq!(
            session
                .execute("SELECT n FROM t WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![expected]
        );
    }
}

#[test]
fn update_declaration_prefix_holes_searched_and_returning() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n) VALUES(1,0),(2,0)", &[], None)
        .unwrap();
    let values = [Value::Bytes(vec![0, 255]), Value::Bytes(vec![1, 0, 255, 2])];
    let prepared = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=$3")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes, DataType::Bytes, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[values[0].clone(), values[1].clone(), Value::Int(1)],
                None
            )
            .unwrap()
            .affected,
        1
    );
    let text = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
        .unwrap()
        .with_parameter_types(&[None, None])
        .unwrap();
    assert_eq!(
        session.describe(&text).unwrap().parameters,
        vec![DataType::Text; 2]
    );
    assert_eq!(
        session
            .execute_prepared(
                &text,
                &[Value::Text("z".into()), Value::Text("az".into())],
                None
            )
            .unwrap()
            .affected,
        1
    );
    let searched = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE n IS NOT NULL OR id > 0")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&searched).unwrap().parameters,
        vec![DataType::Bytes; 2]
    );
    assert_eq!(
        session
            .execute_prepared(&searched, &values, None)
            .unwrap()
            .affected,
        2
    );
    assert!(
        session
            .execute("SELECT n FROM t ORDER BY id", &[], None)
            .unwrap()
            .rows
            .iter()
            .all(|row| row.values == vec![Value::Int(2)])
    );
    let returning = session
        .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1 RETURNING n")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes), Some(DataType::Bytes)])
        .unwrap();
    let description = session.describe(&returning).unwrap();
    assert_eq!(description.parameters, vec![DataType::Bytes; 2]);
    assert_eq!(description.columns.len(), 1);
    assert_eq!(description.columns[0].name, "n");
    assert_eq!(description.columns[0].data_type, DataType::Integer);
    assert_eq!(
        session
            .execute_prepared(&returning, &values, None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2)]
    );
}

#[test]
fn plain_update_declarations_keep_column_conflicts_and_length_checks() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,n INT,b BYTEA,s TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t(id,n,s) VALUES(1,0,'unchanged')", &[], None)
        .unwrap();
    let prepared = session
        .prepare("UPDATE t SET b=$1 WHERE id=$2")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Bytes)])
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Bytes, DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(
                &prepared,
                &[Value::Bytes(vec![0, 255]), Value::Int(1)],
                None
            )
            .unwrap()
            .affected,
        1
    );
    assert_eq!(
        session
            .execute("SELECT b FROM t WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Bytes(vec![0, 255])]
    );
    for (sql, types, values) in [
        (
            "UPDATE t SET s=$1 WHERE id=1",
            vec![Some(DataType::Bytes)],
            vec![Value::Bytes(vec![0])],
        ),
        (
            "UPDATE t SET s=$1 WHERE id=$2",
            vec![Some(DataType::Text), Some(DataType::Bytes)],
            vec![Value::Text("changed".into()), Value::Bytes(vec![0])],
        ),
    ] {
        let conflict = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&types)
            .unwrap();
        assert!(matches!(
            session.describe(&conflict),
            Err(CoreError::Sql(SqlError::Type))
        ));
        assert!(matches!(
            session.execute_prepared(&conflict, &values, None),
            Err(CoreError::Sql(SqlError::Type))
        ));
        assert_eq!(
            session
                .execute("SELECT s FROM t WHERE id=1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text("unchanged".into())]
        );
    }
    assert!(matches!(
        session
            .prepare("UPDATE t SET n=position($1 in $2) WHERE id=1")
            .unwrap()
            .with_parameter_types(&[
                Some(DataType::Bytes),
                Some(DataType::Bytes),
                Some(DataType::Bytes)
            ]),
        Err(SqlError::Parameters)
    ));
}
