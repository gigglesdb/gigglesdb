use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, SqlResult, Value},
};

mod common;

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    run(session, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn fails(session: &mut SqlSession, sql: &str, error: SqlError) {
    assert_eq!(
        session.execute(sql, &[], None),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
}

fn with_db(engine: Engine, exercise: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("fk-cross-type");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    exercise(&mut admin.sql_session());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn no_action(session: &mut SqlSession) {
    for (i, (parent, child, key)) in [
        ("numeric(10,2)", "int", "1"),
        ("numeric", "int", "1"),
        ("numeric(10,2)", "bigint", "1"),
        ("numeric(10,2)", "smallint", "1"),
        ("float8", "int", "1"),
        ("float8", "float4", "1.5"),
        ("float4", "float8", "1.5"),
    ]
    .into_iter()
    .enumerate()
    {
        let p = format!("p{i}");
        let c = format!("c{i}");
        run(
            session,
            &format!("CREATE TABLE {p}(k {parent} PRIMARY KEY)"),
        );
        run(
            session,
            &format!("CREATE TABLE {c}(id int PRIMARY KEY, r {child} REFERENCES {p})"),
        );
        if i < 5 && i % 2 == 0 {
            run(session, &format!("CREATE INDEX {c}_r ON {c}(r)"));
        }
        run(session, &format!("INSERT INTO {p} VALUES ({key})"));
        run(session, &format!("INSERT INTO {c} VALUES (1,{key})"));
        if i == 1 {
            run(session, &format!("UPDATE {p} SET k=1.000"));
            assert_eq!(
                rows(session, &format!("SELECT k::text FROM {p}")),
                vec![vec![Value::Text("1.000".into())]]
            );
        }
        for sql in [
            format!("INSERT INTO {c} VALUES (2,9)"),
            format!("UPDATE {c} SET r=9 WHERE id=1"),
            format!("INSERT INTO {c} VALUES (1,{key}) ON CONFLICT (id) DO UPDATE SET r=9"),
            format!("UPDATE {p} SET k=2"),
            format!("DELETE FROM {p}"),
            format!(
                "MERGE INTO {p} USING (SELECT 1) s ON k={key} WHEN MATCHED THEN UPDATE SET k=2"
            ),
            format!("MERGE INTO {p} USING (SELECT 1) s ON k={key} WHEN MATCHED THEN DELETE"),
        ] {
            fails(session, &sql, SqlError::ForeignKeyViolation);
        }
        assert_eq!(
            rows(
                session,
                &format!("SELECT count(*) FROM {c} JOIN {p} ON r=k")
            ),
            vec![vec![Value::Int(1)]],
            "{parent} <- {child}",
        );
    }
}

fn deferred(session: &mut SqlSession) {
    run(session, "CREATE TABLE p(k numeric(10,2) PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE c(id int PRIMARY KEY, r int REFERENCES p DEFERRABLE INITIALLY DEFERRED)",
    );
    run(session, "INSERT INTO p VALUES (1)");
    run(session, "INSERT INTO c VALUES (1,1)");

    run(session, "BEGIN");
    run(session, "UPDATE p SET k=2");
    fails(session, "COMMIT", SqlError::ForeignKeyViolation);
    assert_eq!(
        rows(session, "SELECT count(*) FROM c JOIN p ON r=k"),
        vec![vec![Value::Int(1)]]
    );

    run(session, "BEGIN");
    run(session, "DELETE FROM p");
    fails(
        session,
        "SET CONSTRAINTS ALL IMMEDIATE",
        SqlError::ForeignKeyViolation,
    );
    run(session, "ROLLBACK");

    run(session, "BEGIN");
    run(session, "UPDATE p SET k=2");
    run(session, "UPDATE c SET r=2");
    run(session, "COMMIT");
    assert_eq!(rows(session, "SELECT r FROM c"), vec![vec![Value::Int(2)]]);
}

fn key_images_and_assignment(session: &mut SqlSession) {
    run(session, "CREATE TABLE p(k numeric PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE c(r numeric(10,2) REFERENCES p ON UPDATE RESTRICT)",
    );
    run(session, "INSERT INTO p VALUES (1.5)");
    run(session, "INSERT INTO c VALUES (1.50)");
    fails(session, "UPDATE p SET k=1.50", SqlError::RestrictViolation);

    run(session, "DROP TABLE c");
    run(
        session,
        "CREATE TABLE c(r numeric(10,2) REFERENCES p ON UPDATE CASCADE)",
    );
    run(session, "INSERT INTO c VALUES (1.50)");
    run(session, "UPDATE p SET k=2.5");
    assert_eq!(
        rows(session, "SELECT r::text FROM c"),
        vec![vec![Value::Text("2.50".into())]]
    );
    fails(
        session,
        "UPDATE p SET k=2.501",
        SqlError::ForeignKeyViolation,
    );

    run(session, "CREATE TABLE pn(k numeric(10,2) PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE cn(r int REFERENCES pn ON UPDATE CASCADE)",
    );
    run(session, "INSERT INTO pn VALUES (1.00)");
    run(session, "INSERT INTO cn VALUES (1)");
    fails(
        session,
        "UPDATE pn SET k=1.10",
        SqlError::ForeignKeyViolation,
    );
    run(session, "UPDATE pn SET k=2.00");
    assert_eq!(rows(session, "SELECT r FROM cn"), vec![vec![Value::Int(2)]]);

    run(session, "CREATE TABLE pr(k real PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE cr(r int REFERENCES pr ON UPDATE CASCADE)",
    );
    run(session, "INSERT INTO pr VALUES (16777216)");
    run(session, "INSERT INTO cr VALUES (16777217)");
    fails(session, "DELETE FROM pr", SqlError::ForeignKeyViolation);
    run(session, "UPDATE pr SET k=3");
    assert_eq!(rows(session, "SELECT r FROM cr"), vec![vec![Value::Int(3)]]);

    run(session, "CREATE TABLE pd(k numeric PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE cd(r int REFERENCES pd ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED)",
    );
    run(session, "INSERT INTO pd VALUES (1)");
    run(session, "INSERT INTO cd VALUES (1)");
    fails(session, "UPDATE pd SET k='Infinity'", SqlError::Unsupported);
    run(session, "BEGIN");
    run(session, "UPDATE pd SET k=1.1");
    fails(session, "COMMIT", SqlError::ForeignKeyViolation);
    assert_eq!(
        rows(session, "SELECT count(*) FROM cd JOIN pd ON r=k"),
        vec![vec![Value::Int(1)]],
    );

    run(session, "CREATE TABLE pi(k int PRIMARY KEY)");
    fails(
        session,
        "CREATE TABLE ci(r numeric REFERENCES pi)",
        SqlError::Type,
    );
    run(session, "CREATE TABLE ci(r numeric)");
    fails(
        session,
        "ALTER TABLE ci ADD FOREIGN KEY (r) REFERENCES pi",
        SqlError::Type,
    );
}

fn multipage_actions(session: &mut SqlSession, indexed: bool) {
    run(session, "CREATE TABLE p(k numeric(10,2) PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE c(id int PRIMARY KEY, r int REFERENCES p ON UPDATE CASCADE)",
    );
    if indexed {
        run(session, "CREATE INDEX c_r ON c(r)");
    }
    run(session, "INSERT INTO p VALUES (1), (3)");
    // V2 FK reads take 200 physical rows per page. Keep the same
    // scenario below Legacy's 1024-row bound.
    for start in (0..450).step_by(50) {
        let rows = (start..start + 50)
            .map(|id| format!("({id},1)"))
            .collect::<Vec<_>>()
            .join(",");
        run(session, &format!("INSERT INTO c VALUES {rows}"));
    }
    run(session, "INSERT INTO c VALUES (450,3), (451,NULL)");
    run(session, "UPDATE p SET k=2 WHERE k=1");
    for (predicate, count) in [("r=1", 0), ("r=2", 450), ("r=3", 1), ("r IS NULL", 1)] {
        assert_eq!(
            rows(
                session,
                &format!("SELECT count(*) FROM c WHERE {predicate}")
            ),
            vec![vec![Value::Int(count)]],
            "{predicate}, indexed={indexed}",
        );
    }

    run(session, "ALTER TABLE c DROP CONSTRAINT c_r_fkey");
    run(
        session,
        "ALTER TABLE c ADD FOREIGN KEY (r) REFERENCES p DEFERRABLE INITIALLY DEFERRED",
    );
    run(session, "BEGIN");
    run(session, "UPDATE p SET k=1 WHERE k=2");
    // Repair the references in the first two pages. COMMIT must still see
    // the invalid references on the final page.
    run(session, "UPDATE c SET r=1 WHERE id<400");
    fails(session, "COMMIT", SqlError::ForeignKeyViolation);
    assert_eq!(
        rows(session, "SELECT count(*) FROM c WHERE r=2"),
        vec![vec![Value::Int(450)]],
        "indexed={indexed}",
    );
}

fn scenarios(engine: Engine) {
    with_db(engine, no_action);
    with_db(engine, deferred);
    with_db(engine, key_images_and_assignment);
}

#[test]
fn legacy_scenarios() {
    scenarios(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_scenarios() {
    scenarios(Engine::V2);
}

#[test]
fn legacy_multipage_actions() {
    for indexed in [false, true] {
        with_db(Engine::Legacy, |session| {
            multipage_actions(session, indexed)
        });
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_multipage_actions() {
    for indexed in [false, true] {
        with_db(Engine::V2, |session| multipage_actions(session, indexed));
    }
}
