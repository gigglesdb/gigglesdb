mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{MAX_TABLE_COLUMNS, MAX_TARGET_COLUMNS, Value, take_sql_error_message},
    sqlstate,
};

fn columns(width: usize) -> String {
    (1..=width)
        .map(|i| format!("c{i} int"))
        .collect::<Vec<_>>()
        .join(",")
}

fn values(width: usize) -> String {
    (1..=width)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn assert_limit(sql: &mut SqlSession, statement: &str, message: &str) {
    let error = sql.execute(statement, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), "54011", "{error:?}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message));
}

fn engines() -> impl Iterator<Item = Engine> {
    [Engine::Legacy, Engine::V2]
        .into_iter()
        .filter(|engine| *engine == Engine::Legacy || cfg!(feature = "engine-rocksdb"))
}

#[test]
fn full_width_row_survives_update_and_reopen() {
    for engine in engines() {
        let path = common::fresh("wide-table-reopen");
        let mut expected: Vec<_> = (1..=MAX_TABLE_COLUMNS as i64).map(Value::Int).collect();
        {
            let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut sql = admin.sql_session();
            sql.execute(
                &format!(
                    "CREATE TABLE wide ({}, PRIMARY KEY (c1))",
                    columns(MAX_TABLE_COLUMNS)
                ),
                &[],
                None,
            )
            .unwrap();
            sql.execute(
                &format!("INSERT INTO wide VALUES ({})", values(MAX_TABLE_COLUMNS)),
                &[],
                None,
            )
            .unwrap();
            let all = sql.execute("SELECT * FROM wide", &[], None).unwrap();
            assert_eq!(all.columns.len(), MAX_TABLE_COLUMNS);
            assert_eq!(all.rows[0].values, expected);
            let selected = sql
                .execute("SELECT c1,c800,c1600 FROM wide", &[], None)
                .unwrap();
            assert_eq!(
                selected.rows[0].values,
                [Value::Int(1), Value::Int(800), Value::Int(1600)]
            );
            let updated = sql
                .execute("UPDATE wide SET c1600=42 WHERE c1=1 RETURNING *", &[], None)
                .unwrap();
            expected[MAX_TABLE_COLUMNS - 1] = Value::Int(42);
            assert_eq!(updated.rows[0].values, expected);
        }
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let all = admin
            .sql_session()
            .execute("SELECT * FROM wide", &[], None)
            .unwrap();
        assert_eq!(all.rows[0].values, expected);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn table_and_target_list_limits_are_independent() {
    const TABLE_MESSAGE: &str = "tables can have at most 1600 columns";
    const TARGET_MESSAGE: &str = "target lists can have at most 1664 entries";
    for engine in engines() {
        let path = common::fresh("wide-table-limits");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        let ddl = format!("CREATE TABLE too_wide ({})", columns(MAX_TABLE_COLUMNS + 1));
        assert_limit(&mut sql, &ddl, TABLE_MESSAGE);
        sql.execute(
            &format!("CREATE TABLE heap ({})", columns(MAX_TABLE_COLUMNS - 1)),
            &[],
            None,
        )
        .unwrap();
        sql.execute("INSERT INTO heap(c1) VALUES (1)", &[], None)
            .unwrap();
        sql.execute("ALTER TABLE heap ADD COLUMN c1600 int", &[], None)
            .unwrap();
        assert_limit(
            &mut sql,
            "ALTER TABLE heap ADD COLUMN c1601 int",
            TABLE_MESSAGE,
        );
        let with_hidden = sql.execute("SELECT * FROM heap", &[], None).unwrap();
        assert_eq!(with_hidden.rows[0].values.len(), MAX_TABLE_COLUMNS);
        assert_eq!(
            with_hidden.rows[0].values[MAX_TABLE_COLUMNS - 1],
            Value::Null
        );
        let select = format!("SELECT {}", values(MAX_TARGET_COLUMNS));
        let projection = sql.execute(&select, &[], None).unwrap();
        assert_eq!(projection.columns.len(), MAX_TARGET_COLUMNS);
        assert_eq!(
            projection.rows[0].values[MAX_TARGET_COLUMNS - 1],
            Value::Int(1664)
        );
        let over = format!("{select},1665");
        assert_limit(&mut sql, &over, TARGET_MESSAGE);
        let expanded = format!(
            "SELECT heap.*,{} FROM heap",
            values(MAX_TARGET_COLUMNS - MAX_TABLE_COLUMNS + 1)
        );
        assert_limit(&mut sql, &expanded, TARGET_MESSAGE);
        let ctas = "CREATE TABLE too_wide AS SELECT heap.*, 0 AS extra FROM heap";
        assert_limit(&mut sql, ctas, TABLE_MESSAGE);
        sql.execute("CREATE TABLE copied AS SELECT * FROM heap", &[], None)
            .unwrap();
        let copy = sql
            .execute("SELECT c1,c1600 FROM copied", &[], None)
            .unwrap();
        assert_eq!(copy.rows[0].values, [Value::Int(1), Value::Null]);

        drop(sql);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn secondary_and_unique_index_columns_beyond_sixty_four() {
    for engine in engines() {
        let path = common::fresh("wide-table-indexes");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        sql.execute(
            &format!(
                "CREATE TABLE wide ({}, PRIMARY KEY (c1), UNIQUE (c70))",
                columns(70)
            ),
            &[],
            None,
        )
        .unwrap();
        sql.execute("CREATE INDEX wide_c65 ON wide (c65)", &[], None)
            .unwrap();
        sql.execute(
            "INSERT INTO wide (c1,c65,c70) VALUES (1,65,70),(2,66,71)",
            &[],
            None,
        )
        .unwrap();
        let result = sql
            .execute("SELECT c70 FROM wide WHERE c65=65", &[], None)
            .unwrap();
        assert_eq!(result.rows[0].values, [Value::Int(70)]);
        let error = sql
            .execute("INSERT INTO wide (c1,c65,c70) VALUES (3,67,70)", &[], None)
            .unwrap_err();
        assert_eq!(sqlstate(&error), "23505");
        let _ = take_sql_error_message();

        drop(sql);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
