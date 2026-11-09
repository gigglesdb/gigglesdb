//! Relation names are globally unique, a documented PostgreSQL divergence.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, Value},
};
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
#[test]
fn schema_resolution_drop_and_reopen() {
    let path = std::env::temp_dir().join(format!("user-schemas-{}", std::process::id()));
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE SCHEMA a AUTHORIZATION giggles");
        run(&mut s, "CREATE SCHEMA IF NOT EXISTS a");
        assert_eq!(
            s.execute("CREATE SCHEMA a", &[], None),
            Err(CoreError::DuplicateSchema)
        );
        run(&mut s, "CREATE SCHEMA b");
        run(&mut s, "CREATE TABLE a.t(id int PRIMARY KEY)");
        error(
            &mut s,
            "CREATE TABLE b.t(id int PRIMARY KEY)",
            E::DuplicateTable,
        );
        error(&mut s, "INSERT INTO t VALUES(1)", E::UnknownTable);
        error(&mut s, "INSERT INTO public.t VALUES(1)", E::UnknownTable);
        error(&mut s, "SELECT * FROM b.t", E::UnknownTable);
        s.set_search_path("missing,a,public", "giggles");
        run(&mut s, "INSERT INTO t VALUES(1)");
        assert_eq!(
            s.execute("SELECT a.t.id FROM a.t", &[], None).unwrap().rows[0].values,
            vec![Value::Int(1)]
        );
        run(&mut s, "CREATE TABLE a.source(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO a.source VALUES(2)");
        run(&mut s, "CREATE TABLE a.sink(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO a.sink SELECT id FROM a.source");
        error(
            &mut s,
            "INSERT INTO a.sink SELECT id FROM public.source",
            E::UnknownTable,
        );
        run(&mut s, "CREATE INDEX ti ON a.t(id)");
        run(&mut s, "ALTER TABLE a.t ADD COLUMN n int");
        run(&mut s, "UPDATE a.t SET n=2 WHERE id=1");
        run(
            &mut s,
            "CREATE TABLE b.child(id int PRIMARY KEY, parent int REFERENCES a.t(id))",
        );
        run(&mut s, "INSERT INTO b.child VALUES(1,1)");
        error(&mut s, "DROP SCHEMA a", E::DependentObjects);
        s.set_search_path("public", "giggles");
        error(&mut s, "SELECT * FROM t", E::UnknownTable);
        run(&mut s, "BEGIN");
        run(&mut s, "CREATE SCHEMA undone");
        run(&mut s, "ROLLBACK");
        assert!(!s.schema_metadata().unwrap().schemas.contains_key("undone"));
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert!(s.schema_metadata().unwrap().schemas.contains_key("a"));
        error(&mut s, "SELECT * FROM t", E::UnknownTable);
        run(&mut s, "SELECT * FROM a.t");
        s.set_search_path("a", "giggles");
        run(&mut s, "CREATE TABLE implicit(id int PRIMARY KEY)");
        assert_eq!(s.relation_schema("implicit").unwrap(), "a");
        let (_, p) = s.prepare_copy("COPY a.t(id) FROM STDIN").unwrap();
        s.describe(&p).unwrap();
        assert!(matches!(
            s.prepare_copy("COPY b.t(id) FROM STDIN"),
            Err(CoreError::Sql(E::UnknownTable))
        ));
        let prepared = s.prepare("SELECT * FROM t").unwrap();
        s.set_search_path("public", "giggles");
        assert!(matches!(
            s.execute_prepared(&prepared, &[], None),
            Err(CoreError::Sql(E::UnknownTable))
        ));
        run(&mut s, "DROP SCHEMA a CASCADE");
        run(&mut s, "DROP SCHEMA IF EXISTS a");
        error(&mut s, "DROP SCHEMA a", E::InvalidSchema);
        run(&mut s, "DROP SCHEMA b CASCADE");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn v4_public_logical_columns_upgrade_to_v5_and_reopen() {
    use giggles_core::relational::DataType;
    let path = std::env::temp_dir().join(format!("schema-logical-v4-v5-{}", std::process::id()));
    let values = r#"(1,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11','{"a":1}','{1,NULL,3}')"#;
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(
            &mut s,
            "CREATE TABLE legacy_logicals(id int PRIMARY KEY, u uuid, j jsonb, a int[])",
        );
        run(
            &mut s,
            &format!("INSERT INTO legacy_logicals VALUES {values}"),
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert_eq!(s.relation_schema("legacy_logicals").unwrap(), "public");
        assert_eq!(
            admin.catalog().unwrap().1[0].columns[1].data_type,
            DataType::Uuid
        );
        run(&mut s, "CREATE SCHEMA typed_s");
        run(
            &mut s,
            "CREATE TABLE typed_s.typed(id int PRIMARY KEY, u uuid, j jsonb, a int[])",
        );
        run(
            &mut s,
            &format!("INSERT INTO typed_s.typed VALUES {values}"),
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        s.set_search_path("typed_s,public", "giggles");
        let expected = s
            .execute("SELECT u,j,a FROM public.legacy_logicals", &[], None)
            .unwrap();
        let actual = s.execute("SELECT u,j,a FROM typed", &[], None).unwrap();
        assert_eq!(
            actual.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
            expected.rows.iter().map(|r| &r.values).collect::<Vec<_>>()
        );
        assert_eq!(
            actual.rows[0].values[0],
            Value::Text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into())
        );
        run(&mut s, "DROP SCHEMA typed_s CASCADE");
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert_eq!(s.relation_schema("legacy_logicals").unwrap(), "public");
        run(&mut s, "SELECT u,j,a FROM legacy_logicals");
    }
    std::fs::remove_dir_all(path).unwrap();
}
