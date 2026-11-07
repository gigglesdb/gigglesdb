mod common;
use giggles_core::{
    Admin, CoreError, Limits,
    relational::{MAX_SQL, Numeric, SqlError, Value},
};
#[test]
fn quoted_routines_preserve_case_and_do_not_enable_special_forms() {
    let a = Admin::create(common::fresh("quoted-routines"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for sql in [
        "SELECT pg_catalog.\"extract\"('year', date '2000-01-01')",
        "SELECT \"extract\"('year', date '2000-01-01')",
        "SELECT \"pg_catalog\".\"extract\"('year', date '2000-01-01')",
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap().rows[0].values,
            vec![Value::Numeric(Numeric::parse("2000").unwrap())]
        );
    }
    assert_eq!(
        s.execute("SELECT \"abs\"(-2)", &[], None).unwrap().rows[0].values,
        vec![Value::Int(2)]
    );
    for sql in [
        "SELECT pg_catalog.\"EXTRACT\"('year', date '2000-01-01')",
        "SELECT \"Abs\"(-2)",
        "SELECT \"ab\"\"s\"(-2)",
        "SELECT \"coalesce\"(NULL,1)",
        "SELECT pg_catalog.\"coalesce\"(NULL,1)",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::UndefinedFunction)),
            "{sql}"
        );
    }
    assert_eq!(
        s.execute("SELECT \"PG_CATALOG\".\"abs\"(-2)", &[], None),
        Err(CoreError::Sql(SqlError::InvalidSchema))
    );
    // No routine exists in public: PG 42883 (core binds UndefinedFunction).
    assert_eq!(
        s.execute("SELECT public.\"abs\"(-2)", &[], None),
        Err(CoreError::Sql(SqlError::UndefinedFunction))
    );
    assert!(
        s.prepare("SELECT \"extract\"(year FROM date '2000-01-01')")
            .is_err()
    );
    assert!(s.prepare("SELECT \"substring\"('abc' FROM 1)").is_err());
    assert_eq!(
        s.execute(
            "SELECT extract(year FROM date '2000-01-01'), coalesce(NULL,1)",
            &[],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        vec![
            Value::Numeric(Numeric::parse("2000").unwrap()),
            Value::Int(1)
        ]
    );
}
#[test]
fn quoted_identifiers_are_accepted_for_objects_and_are_bounded() {
    let a = Admin::create(common::fresh("quoted-object-rejection"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    for sql in [
        "CREATE TABLE \"q\"(id BIGINT PRIMARY KEY)",
        "CREATE TABLE q(\"id\" BIGINT PRIMARY KEY)",
        "SELECT \"id\" FROM items",
        "SELECT id FROM \"items\"",
    ] {
        let a = Admin::create(common::fresh("quoted-object-acceptance"), Limits::default()).unwrap();
        let mut s = a.sql_session();
        s.execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert!(s.execute(sql, &[], None).is_ok(), "{sql}");
        if sql.starts_with("CREATE") {
            let (_, tables) = a.catalog().unwrap();
            let q = tables.iter().find(|table| table.name == "q").unwrap();
            assert_eq!(q.name, "q");
            assert_eq!(q.columns.len(), 1);
            assert_eq!(q.columns[0].name, "id");
        }
    }
    for sql in [
        "CREATE INDEX \"ix\" ON items(id)",
        "CREATE INDEX ix ON items(\"id\")",
    ] {
        let a = Admin::create(common::fresh("quoted-index-acceptance"), Limits::default()).unwrap();
        let mut s = a.sql_session();
        s.execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert!(s.prepare(sql).is_ok(), "{sql}");
        assert!(s.execute(sql, &[], None).is_ok(), "{sql}");
        let (_, tables) = a.catalog().unwrap();
        let items = tables.iter().find(|table| table.name == "items").unwrap();
        let (_, indexes) = a.secondary_index_catalog().unwrap();
        assert!(
            indexes
                .iter()
                .any(|index| index.table == items.id && index.name == "ix"),
            "{sql}"
        );
    }
    for sql in ["SELECT \"\"()", "SELECT \"abs", "SELECT \"ab\0s\"(1)"] {
        assert_eq!(
            s.prepare(sql).err(),
            Some(CoreError::Sql(SqlError::Syntax)),
            "{sql:?}"
        );
    }
    let at_limit = format!("SELECT \"{}\"()", "a".repeat(MAX_SQL - 11));
    assert_eq!(at_limit.len(), MAX_SQL);
    assert!(s.prepare(&at_limit).is_ok());
    assert_eq!(
        s.execute(&at_limit, &[], None),
        Err(CoreError::Sql(SqlError::UndefinedFunction))
    );
    assert_eq!(
        s.prepare(&(at_limit + " ")).err(),
        Some(CoreError::Sql(SqlError::Limit))
    );
    let token_limit = format!("SELECT {}", "\"a\", ".repeat(4097));
    assert_eq!(
        s.prepare(&token_limit).err(),
        Some(CoreError::Sql(SqlError::Limit))
    );
}
