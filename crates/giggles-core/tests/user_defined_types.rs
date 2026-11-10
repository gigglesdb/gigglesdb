mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn error(s: &mut SqlSession, sql: &str, expected: SqlError) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
#[test]
fn enum_domain_composite_survive_reopen() {
    let path = common::fresh("user-types");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE TYPE mood AS ENUM ('second','first')");
        run(
            &mut s,
            "CREATE DOMAIN positive AS integer NOT NULL CHECK (VALUE > 0)",
        );
        run(&mut s, "CREATE TYPE pair AS (a int, b text)");
        run(
            &mut s,
            "CREATE TABLE items (id int PRIMARY KEY, mood mood, n positive)",
        );
        run(&mut s, "INSERT INTO items VALUES (1, 'first', 2)");
        run(&mut s, "INSERT INTO items VALUES (2, 'second', 3)");
        run(&mut s, "UPDATE items SET n = 0 WHERE id = 999");
        run(&mut s, "UPDATE items SET n = NULL WHERE id = 999");
        error(
            &mut s,
            "INSERT INTO items VALUES (3, 'missing', 4)",
            SqlError::InvalidTextRepresentation,
        );
        error(
            &mut s,
            "INSERT INTO items VALUES (3, 'first', 0)",
            SqlError::CheckViolation,
        );
        error(
            &mut s,
            "INSERT INTO items VALUES (3, 'first', NULL)",
            SqlError::Null,
        );
        error(
            &mut s,
            "UPDATE items SET n = -1 WHERE id = 1",
            SqlError::CheckViolation,
        );
        error(
            &mut s,
            "UPDATE items SET mood = 'missing' WHERE id = 1",
            SqlError::InvalidTextRepresentation,
        );
        error(
            &mut s,
            "UPDATE items SET n = NULL WHERE id = 1",
            SqlError::Null,
        );
        error(
            &mut s,
            "CREATE TYPE mood AS ENUM ('other')",
            SqlError::DuplicateObject,
        );
        error(
            &mut s,
            "DROP TYPE mood RESTRICT",
            SqlError::DependentObjects,
        );
        error(&mut s, "DROP DOMAIN positive", SqlError::DependentObjects);
        error(
            &mut s,
            "CREATE TABLE composite_column (p pair)",
            SqlError::Unsupported,
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert_eq!(
            run(&mut s, "SELECT mood FROM items ORDER BY mood"),
            vec![
                vec![Value::Text("second".into())],
                vec![Value::Text("first".into())]
            ]
        );
        assert_eq!(
            run(&mut s, "SELECT mood FROM items ORDER BY mood DESC"),
            vec![
                vec![Value::Text("first".into())],
                vec![Value::Text("second".into())]
            ]
        );
        assert_eq!(
            run(&mut s, "SELECT ROW(1, 'x')::pair"),
            vec![vec![Value::Text("(1,x)".into())]]
        );
        let p = s.prepare("SELECT ROW(1, 'x')::pair").unwrap();
        let d = s.describe(&p).unwrap();
        let types = s.user_type_catalog().unwrap();
        let id = *types
            .types
            .iter()
            .find(|(_, ty)| ty.name() == "pair")
            .unwrap()
            .0;
        assert_eq!(
            d.type_oids,
            vec![giggles_core::relational::type_oid(id).unwrap()]
        );
        error(
            &mut s,
            "INSERT INTO items VALUES (3, 'first', 0)",
            SqlError::CheckViolation,
        );
        run(&mut s, "DROP TABLE items");
        run(&mut s, "DROP TYPE mood");
        run(&mut s, "DROP DOMAIN positive");
        run(&mut s, "DROP TYPE pair");
        run(&mut s, "DROP TYPE IF EXISTS missing");
        run(&mut s, "DROP DOMAIN IF EXISTS missing");
    }
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn domain_null_semantics_and_record_escaping() {
    let path = common::fresh("user-type-null");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(
            &mut s,
            "CREATE DOMAIN d AS int CONSTRAINT positive CHECK (VALUE > 0)",
        );
        run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, v d)");
        run(&mut s, "INSERT INTO t VALUES (1, NULL)");
        run(
            &mut s,
            "CREATE DOMAIN required AS int CHECK (VALUE > 0) NOT NULL",
        );
        run(
            &mut s,
            "CREATE TABLE required_t (id int PRIMARY KEY, v required)",
        );
        error(
            &mut s,
            "INSERT INTO required_t VALUES (1, NULL)",
            SqlError::Null,
        );
        run(&mut s, "CREATE TYPE pair AS (a int, b text)");
        for (sql, expected) in [
            ("SELECT ROW(NULL, '')::pair", "(,\"\")"),
            ("SELECT ROW(1, 'a,b')::pair", "(1,\"a,b\")"),
            ("SELECT ROW(1, 'a\"b')::pair", "(1,\"a\"\"b\")"),
        ] {
            assert_eq!(run(&mut s, sql), vec![vec![Value::Text(expected.into())]]);
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn v6_user_types_and_v4_logical_columns_reopen_together() {
    let path = common::fresh("combined-v4-v6");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE TYPE mood AS ENUM ('second', 'first')");
        run(
            &mut s,
            "CREATE DOMAIN positive AS integer CHECK (VALUE > 0)",
        );
        run(
            &mut s,
            "CREATE TABLE combined (id int PRIMARY KEY, m mood, n positive, u uuid, j jsonb, a int[], b bigint[], c text[])",
        );
        run(&mut s, "CREATE DOMAIN token AS uuid NOT NULL");
        run(
            &mut s,
            "CREATE TYPE logical_pair AS (u uuid, label varchar(4))",
        );
        run(&mut s, "CREATE TABLE tokens (id int PRIMARY KEY, v token)");
        run(
            &mut s,
            "INSERT INTO tokens VALUES (1, '12345678-1234-5678-1234-567812345678')",
        );
        run(
            &mut s,
            "INSERT INTO combined VALUES (1, 'first', 1, '12345678-1234-5678-1234-567812345678', '{\"a\":1}', '{1,2}', '{3,4}', '{x,y}')",
        );
        run(
            &mut s,
            "INSERT INTO combined SELECT 2, 'second', 2, u, j, a, b, c FROM combined WHERE id=1",
        );
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert_eq!(
            run(&mut s, "SELECT v FROM tokens"),
            vec![vec![Value::Text(
                "12345678-1234-5678-1234-567812345678".into()
            )]]
        );
        error(
            &mut s,
            "INSERT INTO tokens VALUES (2, NULL)",
            SqlError::Null,
        );
        assert_eq!(
            run(
                &mut s,
                "SELECT ROW('12345678-1234-5678-1234-567812345678','x')::logical_pair"
            ),
            vec![vec![Value::Text(
                "(12345678-1234-5678-1234-567812345678,x)".into()
            )]]
        );
        let rows = run(&mut s, "SELECT m,n,u,j,a,b,c FROM combined ORDER BY m");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Text("second".into()));
        assert_eq!(rows[0][1], Value::Int(2));
        assert_eq!(&rows[0][2..], &rows[1][2..]);
        assert_eq!(
            rows[0][2],
            Value::Text("12345678-1234-5678-1234-567812345678".into())
        );
        assert_eq!(rows[0][4], Value::Text("{1,2}".into()));
        error(
            &mut s,
            "INSERT INTO combined (id,m,n) VALUES (3, 'first', 0)",
            SqlError::CheckViolation,
        );
        run(&mut s, "DROP TABLE combined");
        run(&mut s, "DROP TYPE mood");
        run(&mut s, "DROP DOMAIN positive");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn types_resolve_in_schemas_with_global_names_and_reopen() {
    let path = common::fresh("schema-user-types");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE SCHEMA types_one");
        run(&mut s, "CREATE SCHEMA types_two");
        s.set_search_path("types_one, public", "giggles");
        run(&mut s, "CREATE TYPE mood AS ENUM ('second','first')");
        run(
            &mut s,
            "CREATE DOMAIN positive AS integer CHECK (VALUE > 0)",
        );
        run(&mut s, "CREATE TYPE pair AS (a int, b text)");
        run(
            &mut s,
            "CREATE TABLE typed (id int PRIMARY KEY, m mood, n positive)",
        );
        run(&mut s, "INSERT INTO typed VALUES (1,'first',1)");
        run(&mut s, "INSERT INTO typed VALUES (2,'second',2)");
        error(
            &mut s,
            "CREATE TYPE types_two.mood AS ENUM ('x')",
            SqlError::DuplicateObject,
        );
        s.set_search_path("types_two, public", "giggles");
        error(
            &mut s,
            "CREATE TABLE hidden_type (v mood)",
            SqlError::UndefinedObject,
        );
        run(
            &mut s,
            "CREATE TABLE qualified (id int PRIMARY KEY, m types_one.mood)",
        );
        run(&mut s, "INSERT INTO qualified VALUES (1,'second')");
        error(
            &mut s,
            "DROP TYPE types_one.mood",
            SqlError::DependentObjects,
        );
        error(
            &mut s,
            "DROP TYPE types_two.mood",
            SqlError::UndefinedObject,
        );
        error(
            &mut s,
            "DROP SCHEMA types_one RESTRICT",
            SqlError::DependentObjects,
        );
        run(&mut s, "CREATE TYPE types_two.other AS ENUM ('x')");
        run(&mut s, "DROP SCHEMA types_two CASCADE");
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        assert_eq!(
            run(&mut s, "SELECT ROW(1,'x')::types_one.pair"),
            vec![vec![Value::Text("(1,x)".into())]]
        );
        error(&mut s, "SELECT ROW(1,'x')::pair", SqlError::UndefinedObject);
        s.set_search_path("types_one, public", "giggles");
        assert_eq!(
            run(&mut s, "SELECT m FROM typed ORDER BY m"),
            vec![
                vec![Value::Text("second".into())],
                vec![Value::Text("first".into())]
            ]
        );
        assert_eq!(
            run(&mut s, "SELECT ROW(1,'x')::pair"),
            vec![vec![Value::Text("(1,x)".into())]]
        );
        run(&mut s, "DROP SCHEMA types_one CASCADE");
        assert!(s.user_type_catalog().unwrap().types.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}
