mod common;
use giggles_core::relational::SqlError;
use giggles_core::{Admin, CoreError, Engine, Limits};
fn exercise(engine: Engine) {
    let path = common::fresh("check_schema");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE SCHEMA p",
        "CREATE TABLE p.c3(id bigint PRIMARY KEY, balance integer CHECK(balance>=0))",
        "CREATE TABLE p.c4(id bigint PRIMARY KEY, balance integer, CHECK(balance>=0))",
        "CREATE TABLE p.c5(id bigint PRIMARY KEY, balance integer CONSTRAINT ck CHECK(balance>=0))",
        "CREATE TABLE p.c7(id bigint PRIMARY KEY, balance integer)",
        "ALTER TABLE p.c7 ADD CONSTRAINT ck7 CHECK(balance>=0)",
        "CREATE TABLE public.c6(id bigint PRIMARY KEY, balance integer CHECK(balance>=0))",
        "CREATE TABLE c2(id bigint PRIMARY KEY, balance integer CHECK(balance>=0))",
        "CREATE TABLE public.c3(id bigint PRIMARY KEY, balance integer CHECK(balance<=0))",
    ] {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for table in ["p.c3", "p.c4", "p.c5", "p.c7", "public.c6", "c2"] {
        s.execute(&format!("INSERT INTO {table} VALUES (1,1)"), &[], None)
            .unwrap();
        for sql in [
            format!("INSERT INTO {table} VALUES (2,-1)"),
            format!("UPDATE {table} SET balance=-1 WHERE id=1"),
        ] {
            assert_eq!(
                s.execute(&sql, &[], None),
                Err(CoreError::Sql(SqlError::CheckViolation)),
                "{sql}"
            );
        }
    }
    s.execute("UPDATE p.c3 SET balance=balance-1 WHERE id=1", &[], None)
        .unwrap();
    s.execute("INSERT INTO public.c3 VALUES (1,-1)", &[], None)
        .unwrap();
    assert_eq!(
        s.execute("INSERT INTO public.c3 VALUES (2,1)", &[], None),
        Err(CoreError::Sql(SqlError::CheckViolation))
    );
    s.execute(
        "CREATE TABLE p.bad(id bigint PRIMARY KEY, balance integer)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO p.bad VALUES (1,-1)", &[], None)
        .unwrap();
    assert_eq!(
        s.execute("ALTER TABLE p.bad ADD CHECK(balance>=0)", &[], None),
        Err(CoreError::Sql(SqlError::CheckViolation))
    );
}
#[test]
fn legacy() {
    exercise(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() {
    exercise(Engine::V2);
}
