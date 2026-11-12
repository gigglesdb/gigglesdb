mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits,
    relational::{SqlError, StatementClock, Value},
};

fn run(admin: &Admin, sql: &str) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute_at(sql, &[], None, &StatementClock { unavailable: None, transaction_us: 0, statement_us: 0, zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)), wall: None })
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}

#[test]
fn precision_rounds_all_writes_and_survives_reopen_in_both_engines() {
    #[cfg(feature = "engine-rocksdb")]
    let engines = [Engine::Legacy, Engine::V2];
    #[cfg(not(feature = "engine-rocksdb"))]
    let engines = [Engine::Legacy];
    for engine in engines {
        let path = common::fresh("temporal-column-precision");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        for ty in ["timestamp", "timestamptz", "time", "timetz"] {
            let input = if ty.starts_with("timestamp") {
                "2000-01-01 00:00:00.123500"
            } else {
                "00:00:00.123500"
            };
            for precision in 0..=7 {
                let modifier = precision.min(6);
                run(
                    &admin,
                    &format!(
                        "CREATE TABLE t (id int PRIMARY KEY, v {ty}({precision}) DEFAULT '{input}')"
                    ),
                );
                let warnings = giggles_core::relational::take_sql_warnings();
                assert_eq!(warnings.len(), usize::from(precision > 6));
                // PG18.6 pg186.log: positive ties round up, including p=0..6.
                let fraction = ["", ".1", ".12", ".124", ".1235", ".1235", ".1235"][modifier as usize];
                let prefix = if ty.starts_with("timestamp") { "2000-01-01 " } else { "" };
                let expected = run(&admin, &format!("SELECT CAST('{prefix}00:00:00{fraction}' AS {ty})"))[0].clone();
                run(&admin, &format!("INSERT INTO t VALUES (1,'{input}')"));
                run(&admin, "INSERT INTO t (id) VALUES (2)");
                run(&admin, "INSERT INTO t VALUES (3,NULL)");
                run(&admin, &format!("UPDATE t SET v='{input}' WHERE id=3"));
                let mut session = admin.sql_session();
                let prepared = session.prepare("INSERT INTO t VALUES (4,$1)").unwrap();
                let value = run(&admin, &format!("SELECT CAST('{input}' AS {ty})"))[0][0].clone();
                session.execute_prepared(&prepared, &[value], None).unwrap();
                let (_, copy) = session.prepare_copy("COPY t FROM STDIN").unwrap();
                let clock = StatementClock {
                    unavailable: None,
                    transaction_us: 0,
                    statement_us: 0,
                    zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
                    wall: None,
                };
                let (copy, rows) = session
                    .normalize_copy_rows_at(
                        &copy,
                        vec![vec![Value::Text("5".into()), Value::Text(input.into())]],
                        &clock,
                    )
                    .unwrap();
                session.ingest_copy_at(&copy, rows, &clock).unwrap();
                assert_eq!(
                    run(&admin, "SELECT v FROM t ORDER BY id"),
                    vec![expected.clone(); 5],
                    "{engine:?} {ty}({precision})"
                );
                run(
                    &admin,
                    &format!("ALTER TABLE t ADD COLUMN added {ty}({precision}) DEFAULT '{input}'"),
                );
                assert_eq!(
                    run(&admin, "SELECT added FROM t ORDER BY id"),
                    vec![expected; 5]
                );
                assert!(
                    admin
                        .catalog_typmods()
                        .unwrap()
                        .1
                        .values()
                        .all(|m| *m == modifier)
                );
                run(&admin, "DROP TABLE t");
            }
        }
        run(&admin, "CREATE TABLE persisted (v timestamp(3))");
        run(&admin, "CREATE DOMAIN rounded AS time(0) CHECK (VALUE <> make_time(0,0,1))");
        run(&admin, "CREATE TABLE domain_values (v rounded)");
        assert_eq!(
            admin.sql_session().execute("INSERT INTO domain_values VALUES ('00:00:00.6')", &[], None),
            Err(CoreError::Sql(SqlError::CheckViolation)),
            "{engine:?} domain CHECK must see the rounded value"
        );
        run(&admin, "INSERT INTO domain_values VALUES ('00:00:00.4')");
        assert_eq!(run(&admin, "SELECT v FROM domain_values"), run(&admin, "SELECT CAST('00:00:00' AS time)"));
        run(&admin, "CREATE TABLE insert_paths (id int PRIMARY KEY, v timestamp(3))");
        run(&admin, "INSERT INTO insert_paths SELECT 1,CAST('2000-01-01 00:00:00.123500' AS timestamp)");
        assert_eq!(run(&admin, "SELECT v FROM insert_paths"), run(&admin, "SELECT CAST('2000-01-01 00:00:00.124' AS timestamp)"));
        run(&admin, "INSERT INTO insert_paths VALUES (1,'2000-01-01 00:00:00') ON CONFLICT (id) DO UPDATE SET v=CAST('2000-01-01 00:00:00.456500' AS timestamp)");
        assert_eq!(run(&admin, "SELECT v FROM insert_paths"), run(&admin, "SELECT CAST('2000-01-01 00:00:00.457' AS timestamp)"));
        run(
            &admin,
            "INSERT INTO persisted VALUES ('1999-12-31 23:59:59.876500'),('1999-12-31 23:59:59.876501'),('infinity'),('-infinity'),(NULL)",
        );
        let expected = run(
            &admin,
            "SELECT CAST('1999-12-31 23:59:59.876' AS timestamp)",
        );
        assert_eq!(run(&admin, "SELECT v FROM persisted LIMIT 1"), expected);
        let before = run(&admin, "SELECT v FROM persisted");
        assert_eq!(before[1], run(&admin, "SELECT CAST('1999-12-31 23:59:59.877' AS timestamp)")[0]);
        assert_eq!(before[4], vec![Value::Null]);
        let typmods = admin.catalog_typmods().unwrap();
        drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(admin.catalog_typmods().unwrap(), typmods);
        assert_eq!(run(&admin, "SELECT v FROM persisted"), before);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn invalid_temporal_modifiers_match_pg186_sqlstates() {
    let path = common::fresh("temporal-modifier-errors");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    for ty in ["timestamp", "time", "timestamptz", "timetz"] {
        let alias = matches!(ty, "timestamptz" | "timetz");
        for modifier in ["-1", "1,2"] {
            let sql = format!("CREATE TABLE invalid (v {ty}({modifier}))");
            let expected = if alias { SqlError::InvalidParameterValue } else { SqlError::Syntax };
            assert_eq!(admin.sql_session().execute(&sql, &[], None), Err(CoreError::Sql(expected)), "{sql}");
            let message = giggles_core::relational::take_sql_error_message();
            if alias && modifier == "1,2" {
                assert_eq!(message.as_deref(), Some("invalid type modifier"));
            }
        }
    }
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
