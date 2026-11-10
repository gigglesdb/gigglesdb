use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, StatementClock, Value},
};

fn path(label: &str) -> std::path::PathBuf {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!(
        "per-schema-{label}-{:x}",
        u128::from_le_bytes(nonce)
    ))
}
fn run(session: &mut SqlSession, sql: &str) -> giggles_core::relational::SqlResult {
    session
        .execute_at(
            sql,
            &[],
            None,
            &StatementClock {
                unavailable: None,
                transaction_us: 100,
                statement_us: 100,
                zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
                wall: None,
            },
        )
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn value(session: &mut SqlSession, sql: &str) -> i64 {
    let rows = run(session, sql).rows;
    let Value::Int(n) = rows[0].values[0] else {
        panic!("{rows:?}")
    };
    n
}

#[test]
fn qualified_relations_search_path_ddl_and_reopen() {
    let path = path("tables");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            "CREATE SCHEMA s1",
            "CREATE SCHEMA s2",
            "CREATE TABLE public.t(id int PRIMARY KEY, n int)",
            "CREATE TABLE s1.t(id int PRIMARY KEY, n int)",
            "CREATE TABLE s2.t(id int PRIMARY KEY, n int)",
            "INSERT INTO public.t VALUES(1,30)",
            "INSERT INTO s1.t VALUES(1,10)",
            "INSERT INTO s2.t VALUES(1,20)",
            "CREATE INDEX ti ON s1.t(n)",
            "CREATE INDEX ti ON s2.t(n)",
        ] {
            run(&mut s, sql);
        }
        s.set_search_path("missing,s2,s1,public", "giggles");
        assert_eq!(value(&mut s, "SELECT n FROM t"), 20);
        assert_eq!(value(&mut s, "SELECT n FROM public.t"), 30);
        let (_, copy) = s.prepare_copy("COPY public.t(id,n) FROM STDIN").unwrap();
        s.ingest_copy(&copy, vec![vec![Value::Int(2), Value::Int(99)]])
            .unwrap();
        assert_eq!(value(&mut s, "SELECT n FROM public.t WHERE id=2"), 99);
        assert_eq!(value(&mut s, "SELECT count(*) FROM s2.t"), 1);
        assert_eq!(value(&mut s, "SELECT t.n FROM s1.t"), 10);
        let prepared = s.prepare("SELECT n FROM t").unwrap();
        s.set_search_path("s1,s2,public", "giggles");
        assert_eq!(
            s.execute_prepared(&prepared, &[], None).unwrap().rows[0].values,
            vec![Value::Int(10)]
        );
        run(&mut s, "UPDATE s2.t SET n=21 WHERE id=1 RETURNING n");
        run(&mut s, "ALTER TABLE s2.t ADD COLUMN extra int");
        run(&mut s, "ALTER TABLE s2.t RENAME TO renamed");
        run(&mut s, "CREATE TABLE implicit(id int)");
        assert_eq!(s.relation_schema("implicit").unwrap(), "s1");
        assert_eq!(value(&mut s, "SELECT n FROM s2.renamed"), 21);
        assert!(matches!(
            s.execute("CREATE TABLE s1.t(id int)", &[], None),
            Err(CoreError::Sql(SqlError::DuplicateTable))
        ));
        assert_eq!(
            admin
                .catalog()
                .unwrap()
                .1
                .iter()
                .filter(|t| t.name == "t")
                .count(),
            2
        );
        assert!(
            admin
                .secondary_index_catalog()
                .unwrap()
                .1
                .iter()
                .all(|index| index.name == "ti")
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert_eq!(value(&mut s, "SELECT n FROM s1.t"), 10);
        assert_eq!(value(&mut s, "SELECT n FROM s2.renamed"), 21);
        run(&mut s, "DROP SCHEMA s1 CASCADE");
        assert_eq!(value(&mut s, "SELECT n FROM s2.renamed"), 21);
        assert_eq!(value(&mut s, "SELECT n FROM public.t"), 30);
        run(&mut s, "DELETE FROM s2.renamed WHERE id=1");
        run(&mut s, "DROP TABLE s2.renamed");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sequences_and_constraints_share_only_their_schema_namespace() {
    let path = path("sequences");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            "CREATE SCHEMA a",
            "CREATE SCHEMA b",
            "CREATE SEQUENCE a.seq",
            "CREATE SEQUENCE b.seq",
            "CREATE TABLE a.t(id serial PRIMARY KEY, n int CONSTRAINT nk UNIQUE)",
            "CREATE TABLE b.t(id serial PRIMARY KEY, n int CONSTRAINT nk UNIQUE)",
            "INSERT INTO a.t(n) VALUES(10)",
            "INSERT INTO b.t(n) VALUES(20)",
        ] {
            run(&mut s, sql);
        }
        assert_eq!(value(&mut s, "SELECT nextval('a.seq')"), 1);
        assert_eq!(value(&mut s, "SELECT nextval('b.seq')"), 1);
        run(&mut s, "CREATE SEQUENCE shared");
        run(
            &mut s,
            "CREATE TABLE defaults_public(id bigint DEFAULT nextval('shared'))",
        );
        run(&mut s, "CREATE SEQUENCE a.shared");
        s.set_search_path("a,b,public", "giggles");
        run(&mut s, "INSERT INTO defaults_public DEFAULT VALUES");
        assert_eq!(value(&mut s, "SELECT nextval('public.shared')"), 2);
        run(
            &mut s,
            "CREATE TABLE manual(id bigint DEFAULT nextval('seq'))",
        );
        run(&mut s, "INSERT INTO manual DEFAULT VALUES");
        assert_eq!(value(&mut s, "SELECT id FROM manual"), 2);
        run(
            &mut s,
            "ALTER TABLE manual ALTER COLUMN id SET DEFAULT nextval('b.seq')",
        );
        run(&mut s, "INSERT INTO manual DEFAULT VALUES");
        s.set_search_path("b,a,public", "giggles");
        assert_eq!(value(&mut s, "SELECT nextval('seq')"), 3);
        run(&mut s, "ALTER SEQUENCE a.seq RESTART WITH 8");
        assert_eq!(value(&mut s, "SELECT nextval('a.seq')"), 8);
        run(&mut s, "DROP SEQUENCE a.seq");
        assert_eq!(value(&mut s, "SELECT nextval('b.seq')"), 4);
        run(&mut s, "DROP SCHEMA a CASCADE");
        assert_eq!(value(&mut s, "SELECT n FROM b.t"), 20);
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(value(&mut admin.sql_session(), "SELECT n FROM b.t"), 20);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn public_only_database_retains_legacy_names_after_reopen() {
    let path = path("legacy");
    let before;
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE TABLE legacy(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO legacy VALUES(7)");
        before = admin.catalog().unwrap();
        assert!(admin.catalog_schema_metadata().unwrap().tables.is_empty());
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(before, admin.catalog().unwrap());
        assert!(admin.catalog_schema_metadata().unwrap().tables.is_empty());
        assert_eq!(
            value(&mut admin.sql_session(), "SELECT id FROM public.legacy"),
            7
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
