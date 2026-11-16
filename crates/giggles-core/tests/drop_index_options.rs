mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};

fn run(s: &mut SqlSession, sql: &str) {
    let result = s
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    if sql.starts_with("DROP INDEX") {
        assert_eq!(result.affected, 0, "{sql}");
    }
}

fn error(s: &mut SqlSession, sql: &str, expected: SqlError) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

fn exercise(path: &std::path::Path, mut admin: Admin, views: bool) {
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE owner(id int PRIMARY KEY, u int CONSTRAINT owner_uq UNIQUE)",
        "CREATE TABLE named(id int CONSTRAINT named_pk PRIMARY KEY)",
        "CREATE SCHEMA other",
        "CREATE TABLE other.owner(id int CONSTRAINT owner_pk PRIMARY KEY)",
        "CREATE TABLE plain(id int)",
        "CREATE UNIQUE INDEX standalone_u ON plain(id)",
    ] {
        run(&mut s, sql);
    }
    s.execute("INSERT INTO plain VALUES(7)", &[], None).unwrap();
    if views {
        run(&mut s, "CREATE VIEW plain_view AS SELECT id FROM plain");
    }
    for reopened in [false, true] {
        if reopened {
            drop(s);
            drop(admin);
            admin = Admin::open(path, Limits::default()).unwrap();
            s = admin.sql_session();
            run(&mut s, "DROP INDEX persisted CASCADE");
            error(&mut s, "DROP INDEX persisted", SqlError::UndefinedObject);
        }
        // Explicit public names use the same catalog key as bare public names.
        for sql in [
            "CREATE TABLE public.qualified(id int)",
            "CREATE TABLE other.qualified(id int)",
            "CREATE INDEX qualified_i ON public.qualified(id)",
            "CREATE INDEX qualified_i ON other.qualified(id)",
            "DROP INDEX public.qualified_i",
            "DROP INDEX other.qualified_i",
            "CREATE INDEX qualified_i ON public.qualified(id)",
            "CREATE INDEX qualified_i ON other.qualified(id)",
        ] {
            run(&mut s, sql);
        }
        s.set_search_path("other,public", "giggles");
        run(&mut s, "DROP INDEX qualified_i");
        error(&mut s, "DROP INDEX other.qualified_i", SqlError::UndefinedObject);
        run(&mut s, "DROP INDEX public.qualified_i");
        error(&mut s, "DROP INDEX public.qualified_i", SqlError::UndefinedObject);
        run(&mut s, "DROP INDEX IF EXISTS public.qualified_i");
        run(&mut s, "DROP TABLE public.qualified");
        run(&mut s, "DROP TABLE other.qualified");
        s.set_search_path("public", "giggles");
        for option in ["", " CASCADE", " RESTRICT"] {
            run(&mut s, "CREATE INDEX plain_i ON plain(id)");
            run(&mut s, &format!("DROP INDEX plain_i{option};"));
            error(&mut s, "DROP INDEX plain_i", SqlError::UndefinedObject);
            run(&mut s, &format!("DROP INDEX IF EXISTS plain_i{option}"));
            error(
                &mut s,
                &format!("DROP INDEX missing{option}"),
                SqlError::UndefinedObject,
            );
            for name in ["owner_pkey", "public.owner_pkey", "owner_uq", "named_pk", "other.owner_pk"] {
                // PostgreSQL 18.6 refuses constraint indexes with 2BP01 for every option.
                for optional in ["", " IF EXISTS"] {
                    error(
                        &mut s,
                        &format!("DROP INDEX{optional} {name}{option}"),
                        SqlError::DependentObjects,
                    );
                }
            }
        }
        s.set_search_path("other,public", "giggles");
        error(
            &mut s,
            "DROP INDEX owner_pk CASCADE",
            SqlError::DependentObjects,
        );
        s.set_search_path("public", "giggles");
        for option in ["", " RESTRICT", " CASCADE"] {
            run(&mut s, &format!("DROP INDEX standalone_u{option}"));
            error(&mut s, "DROP INDEX standalone_u", SqlError::UndefinedObject);
            run(&mut s, "CREATE UNIQUE INDEX standalone_u ON plain(id)");
        }
        error(&mut s, "INSERT INTO plain VALUES(7)", SqlError::Duplicate);
        error(
            &mut s,
            "DROP INDEX CONCURRENTLY standalone_u",
            SqlError::Unsupported,
        );
        error(
            &mut s,
            "DROP INDEX standalone_u CASCADE RESTRICT",
            SqlError::Syntax,
        );
        assert_eq!(
            s.execute("SELECT id FROM plain", &[], None).unwrap().rows[0].values,
            [Value::Int(7)]
        );
        if views {
            assert_eq!(
                s.execute("SELECT id FROM plain_view", &[], None)
                    .unwrap()
                    .rows[0]
                    .values,
                [Value::Int(7)]
            );
        }
        if !reopened {
            run(&mut s, "CREATE INDEX persisted ON plain(id)");
        }
    }
    // The pre-existing bare standalone UNIQUE drop remains supported.
    run(&mut s, "DROP INDEX standalone_u");
    s.execute("INSERT INTO plain VALUES(7)", &[], None).unwrap();
    drop(s);
    drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    error(&mut s, "DROP INDEX standalone_u", SqlError::UndefinedObject);
    error(&mut s, "DROP INDEX owner_pkey", SqlError::DependentObjects);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_options_constraints_and_reopen() {
    let path = common::fresh("drop-index-options-legacy");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    exercise(&path, admin, false);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_options_constraints_views_and_reopen() {
    let path = common::fresh("drop-index-options-v2");
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    exercise(&path, admin, true);
}
