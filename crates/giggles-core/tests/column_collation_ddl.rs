mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};
fn engines() -> Vec<Engine> {
    [Engine::Legacy, #[cfg(feature = "engine-rocksdb")] Engine::V2].to_vec()
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| {
            panic!(
                "{sql}: {e:?} message {:?}",
                giggles_core::relational::take_sql_error_message()
            )
        })
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn setup(s: &mut SqlSession) {
    run(
        s,
        "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
    );
    run(
        s,
        r#"CREATE TABLE c1(id int PRIMARY KEY,a text COLLATE "und-x-icu",b text COLLATE "C",c text COLLATE ci,d varchar(10) COLLATE "de-x-icu",e text)"#,
    );
    run(
        s,
        "INSERT INTO c1 VALUES(1,'b','b','ABC','ä','x'),(2,'B','B','abc','a','y'),(3,'a','a','Abc','z','z')",
    );
}

#[test]
fn ddl_domains_errors_dependencies_restart() {
    for engine in engines() {
        let path = common::fresh("column-collation-ddl");
        {
            let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            assert_eq!(
                s.execute(
                    r#"CREATE TABLE bad(id int PRIMARY KEY,v int COLLATE "C")"#,
                    &[],
                    None
                ),
                Err(CoreError::Sql(E::Type))
            );
            assert_eq!(
                s.execute(
                    "CREATE TABLE bad(id int PRIMARY KEY,v text COLLATE nonexistent)",
                    &[],
                    None
                ),
                Err(CoreError::Sql(E::UndefinedObject))
            );
            run(&mut s, "CREATE DOMAIN words AS varchar(10)");
            run(
                &mut s,
                r#"CREATE TABLE dom(id int PRIMARY KEY,v words COLLATE "C")"#,
            );
            run(&mut s, "ALTER TABLE dom ADD COLUMN n words COLLATE ci");
            run(
                &mut s,
                r#"ALTER TABLE dom ALTER COLUMN v TYPE text COLLATE "und-x-icu""#,
            );
            assert!(
                run(
                    &mut s,
                    "SELECT pg_collation_for(v),pg_collation_for(n) FROM dom"
                )
                .is_empty()
            );
            run(&mut s, "INSERT INTO dom VALUES(1,'b','ABC')");
            assert_eq!(
                run(
                    &mut s,
                    "SELECT pg_collation_for(v),pg_collation_for(n) FROM dom"
                ),
                vec![vec![V::Text("\"und-x-icu\"".into()), V::Text("ci".into())]]
            );
            assert_eq!(
                s.execute("DROP COLLATION ci", &[], None),
                Err(CoreError::Sql(E::DependentObjects))
            );
            assert!(
                giggles_core::relational::take_sql_error_detail()
                    .unwrap()
                    .contains("column c of table c1 depends on collation ci")
            );
            run(&mut s, "ALTER COLLATION ci RENAME TO ci2");
            run(&mut s, "CREATE SCHEMA moved");
            run(&mut s, "ALTER COLLATION ci2 SET SCHEMA moved");
        }
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        assert_eq!(
            run(&mut s, "SELECT count(DISTINCT c) FROM c1"),
            vec![vec![V::Int(1)]]
        );
        assert_eq!(s.collation_oids().unwrap().len(), 6);
        run(&mut s, "DROP COLLATION moved.ci2 CASCADE");
        assert_eq!(
            s.catalog_metadata()
                .unwrap()
                .tables
                .iter()
                .find(|t| t.name == "c1")
                .unwrap()
                .columns
                .iter()
                .filter(|c| c.name == "c")
                .count(),
            0
        );
        run(&mut s, "DROP TABLE c1");
        run(&mut s, "DROP TABLE dom");
    }
}
#[test]
fn create_as_and_select_into_preserve_collations() {
    for engine in engines() {
        let a = Admin::create_engine(
            common::fresh("column-collation-ctas"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut s = a.sql_session();
        setup(&mut s);
        run(&mut s, "CREATE TABLE c5 AS SELECT a,b FROM c1");
        assert_eq!(
            run(
                &mut s,
                "SELECT pg_collation_for(a),pg_collation_for(b) FROM c5 LIMIT 1"
            ),
            vec![vec![
                V::Text("\"und-x-icu\"".into()),
                V::Text("\"C\"".into())
            ]]
        );
        assert!(run(&mut s, "SELECT a INTO c6 FROM c1").is_empty());
        assert_eq!(
            run(&mut s, "SELECT count(*) FROM c6"),
            vec![vec![V::Int(3)]]
        );
    }
}

#[test]
fn cascade_primary_and_last_column_and_character_types() {
    for engine in engines() {
        let a = Admin::create_engine(
            common::fresh("column-collation-cascade"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut s = a.sql_session();
        run(
            &mut s,
            "CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false)",
        );
        run(
            &mut s,
            "CREATE TABLE words(v varchar(10) COLLATE ci PRIMARY KEY,n char(5) COLLATE ci)",
        );
        run(&mut s, "INSERT INTO words VALUES('abc','ABC')");
        assert_eq!(
            s.execute("SELECT v=n FROM words", &[], None),
            Err(CoreError::Sql(E::Unsupported))
        );
        run(
            &mut s,
            "ALTER TABLE words ALTER COLUMN n TYPE text COLLATE ci",
        );
        run(&mut s, "DROP COLLATION ci CASCADE");
        assert!(
            s.catalog_metadata()
                .unwrap()
                .tables
                .iter()
                .find(|t| t.name == "words")
                .unwrap()
                .logical_columns()
                .next()
                .is_none()
        );
    }
}
