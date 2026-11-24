mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{Value, take_sql_error_detail},
    sqlstate,
};

fn run(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn rejects(session: &mut SqlSession, sql: &str, state: &str) {
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), state, "{sql}: {error:?}");
}

fn lifecycle(engine: Engine) {
    let path = common::fresh("generated-columns");
    let limits = Limits::default();
    let admin = Admin::create_engine(&path, limits, engine).unwrap();
    let mut sql = admin.sql_session();
    run(
        &mut sql,
        "CREATE TABLE documents (id int PRIMARY KEY, title text DEFAULT '', body text,
            n int, tags text[], details jsonb,
            summary text GENERATED ALWAYS AS (coalesce(title,'') || ' ' || coalesce(body,'')) STORED,
            n2 int GENERATED ALWAYS AS (n * 2),
            n3 bigint GENERATED ALWAYS AS (n::bigint + 1) STORED)",
    );
    run(
        &mut sql,
        "CREATE INDEX documents_tags ON documents USING GIN (tags)",
    );
    run(
        &mut sql,
        "CREATE INDEX documents_details ON documents USING GIN (details jsonb_path_ops)",
    );
    assert_eq!(
        run(
            &mut sql,
            "INSERT INTO documents(id,title,body,n,tags,details)
            VALUES (1,'Ada','Lovelace',5,'{math}','{\"born\":1815}') RETURNING summary,n2,n3"
        ),
        vec![vec![
            Value::Text("Ada Lovelace".into()),
            Value::Int(10),
            Value::Int(6)
        ]]
    );
    assert_eq!(
        run(
            &mut sql,
            "UPDATE documents SET title='Grace',body='Hopper',n=6 WHERE id=1 RETURNING summary,n2"
        ),
        vec![vec![Value::Text("Grace Hopper".into()), Value::Int(12)]]
    );
    run(
        &mut sql,
        "INSERT INTO documents(id,n,summary,n2) VALUES (2,8,DEFAULT,DEFAULT)",
    );
    run(&mut sql, "INSERT INTO documents(id,n) SELECT 3,9");
    run(
        &mut sql,
        "INSERT INTO documents(id,n) VALUES (1,7) ON CONFLICT (id) DO UPDATE SET n=excluded.n",
    );
    assert_eq!(
        run(&mut sql, "SELECT n2,n3 FROM documents WHERE id=1"),
        vec![vec![Value::Int(14), Value::Int(8)]]
    );
    run(
        &mut sql,
        "UPDATE documents SET summary=DEFAULT,n2=DEFAULT WHERE id=1",
    );
    rejects(
        &mut sql,
        "UPDATE documents SET summary='manual' WHERE id=1",
        "428C9",
    );
    assert_eq!(
        take_sql_error_detail().as_deref(),
        Some("Column \"summary\" is a generated column.")
    );
    rejects(
        &mut sql,
        "INSERT INTO documents(id,summary) OVERRIDING SYSTEM VALUE VALUES (4,'manual')",
        "428C9",
    );
    let (_copy, prepared) = sql.prepare_copy("COPY documents(id,n) FROM STDIN").unwrap();
    let clock = giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: None,
        wall: None,
    };
    let (prepared, rows) = sql
        .normalize_copy_rows_at(&prepared, vec![vec![Value::Int(4), Value::Int(10)]], &clock)
        .unwrap();
    sql.ingest_copy_at(&prepared, rows, &clock).unwrap();
    assert_eq!(
        run(&mut sql, "SELECT n2,n3 FROM documents WHERE id=4"),
        vec![vec![Value::Int(20), Value::Int(11)]]
    );
    rejects(
        &mut sql,
        "ALTER TABLE documents ALTER COLUMN n TYPE bigint",
        "0A000",
    );
    rejects(&mut sql, "ALTER TABLE documents DROP COLUMN n", "2BP01");
    assert!(
        take_sql_error_detail()
            .unwrap()
            .contains("column n3 of table documents")
    );
    rejects(
        &mut sql,
        "ALTER TABLE documents ALTER COLUMN n2 DROP EXPRESSION",
        "0A000",
    );
    run(
        &mut sql,
        "ALTER TABLE documents ADD COLUMN label text GENERATED ALWAYS AS (upper(title)) STORED",
    );
    run(
        &mut sql,
        "ALTER TABLE documents ALTER COLUMN summary SET EXPRESSION AS (upper(title))",
    );
    run(&mut sql, "ALTER TABLE documents RENAME COLUMN n TO amount");
    drop(sql);
    drop(admin);

    let admin = Admin::open(&path, limits).unwrap();
    let mut sql = admin.sql_session();
    assert_eq!(
        run(
            &mut sql,
            "SELECT summary,label,n2,n3 FROM documents WHERE n2=14 ORDER BY n2"
        ),
        vec![vec![
            Value::Text("GRACE".into()),
            Value::Text("GRACE".into()),
            Value::Int(14),
            Value::Int(8)
        ]]
    );
    assert_eq!(
        sql.catalog_metadata()
            .unwrap()
            .presentation
            .generated_columns
            .len(),
        4
    );
    run(
        &mut sql,
        "ALTER TABLE documents ALTER COLUMN summary DROP EXPRESSION",
    );
    assert_eq!(
        run(&mut sql, "SELECT summary FROM documents WHERE id=1"),
        vec![vec![Value::Text("GRACE".into())]]
    );
    run(&mut sql, "UPDATE documents SET summary='manual' WHERE id=1");
    run(&mut sql, "DROP INDEX documents_tags");
    run(&mut sql, "ALTER TABLE documents DROP COLUMN details");
    run(&mut sql, "ALTER TABLE documents DROP COLUMN amount CASCADE");
    assert_eq!(
        sql.catalog_metadata()
            .unwrap()
            .presentation
            .generated_columns
            .len(),
        1
    );
    assert!(
        sql.catalog_metadata()
            .unwrap()
            .presentation
            .gin_indexes
            .is_empty()
    );
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn diagnostics(engine: Engine) {
    let path = common::fresh("generated-errors");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut sql = admin.sql_session();
    run(
        &mut sql,
        "CREATE TABLE lazy_virtual (id int PRIMARY KEY, quotient int GENERATED ALWAYS AS (1 / id) VIRTUAL)",
    );
    run(&mut sql, "INSERT INTO lazy_virtual (id) VALUES (0)");
    assert_eq!(
        run(&mut sql, "SELECT id FROM lazy_virtual"),
        vec![vec![Value::Int(0)]]
    );
    rejects(&mut sql, "SELECT quotient FROM lazy_virtual", "22012");
    for (statement, state) in [
        (
            "CREATE TABLE bad1 (a int, b int GENERATED ALWAYS AS (b + 1) STORED)",
            "42P17",
        ),
        (
            "CREATE TABLE bad2 (a int, b int GENERATED ALWAYS AS (a + 1) STORED, c int GENERATED ALWAYS AS (b + 1) STORED)",
            "42P17",
        ),
        (
            "CREATE TABLE bad3 (a int, b int GENERATED ALWAYS AS (random()::int) STORED)",
            "42P17",
        ),
        (
            "CREATE TABLE bad4 (a int, b int GENERATED ALWAYS AS (a + 1) STORED DEFAULT 5)",
            "42601",
        ),
        (
            "CREATE TABLE bad5 (a int, b int GENERATED ALWAYS AS (count(a)) STORED)",
            "42803",
        ),
        (
            "CREATE TABLE bad6 (a int, b int GENERATED ALWAYS AS (nosuch + 1) STORED)",
            "42703",
        ),
    ] {
        rejects(&mut sql, statement, state);
    }
    run(
        &mut sql,
        "CREATE TABLE bad7 (a int PRIMARY KEY GENERATED ALWAYS AS (1) STORED)",
    );
    run(
        &mut sql,
        "CREATE TABLE ok8 (a int, b int GENERATED ALWAYS AS (a + 1) STORED PRIMARY KEY)",
    );
    run(
        &mut sql,
        "CREATE TABLE ok9 (a int, b int GENERATED ALWAYS AS (a + 1) STORED NOT NULL)",
    );
    rejects(&mut sql, "INSERT INTO ok9 (a) VALUES (NULL)", "23502");
    assert_eq!(
        take_sql_error_detail().as_deref(),
        Some("Failing row contains (null, null).")
    );
    run(
        &mut sql,
        "CREATE TABLE ok10 (a int, b int GENERATED ALWAYS AS (a * 2) STORED CHECK (b < 10))",
    );
    rejects(&mut sql, "INSERT INTO ok10 (a) VALUES (6)", "23514");
    assert_eq!(
        take_sql_error_detail().as_deref(),
        Some("Failing row contains (6, 12).")
    );
    run(
        &mut sql,
        "CREATE TABLE ok11 (a int, b text GENERATED ALWAYS AS (a::text) STORED)",
    );
    run(&mut sql, "CREATE UNIQUE INDEX ok11_b ON ok11 (b)");
    rejects(&mut sql, "INSERT INTO ok11 (a) VALUES (1), (1)", "23505");
    assert_eq!(
        take_sql_error_detail().as_deref(),
        Some("Key (b)=(1) already exists.")
    );
    run(
        &mut sql,
        "CREATE TABLE ok12 (a int, b int GENERATED ALWAYS AS (a) STORED)",
    );
    rejects(
        &mut sql,
        "ALTER TABLE ok12 ALTER COLUMN b SET DEFAULT 1",
        "42601",
    );
    run(&mut sql, "ALTER TABLE ok12 ALTER COLUMN b TYPE bigint");
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn generated_columns_legacy_lifecycle() {
    lifecycle(Engine::Legacy);
}
#[test]
fn generated_columns_legacy_diagnostics() {
    diagnostics(Engine::Legacy);
}
#[test]
#[cfg(feature = "engine-rocksdb")]
fn generated_columns_v2_lifecycle() {
    lifecycle(Engine::V2);
}
#[test]
#[cfg(feature = "engine-rocksdb")]
fn generated_columns_v2_diagnostics() {
    diagnostics(Engine::V2);
}
