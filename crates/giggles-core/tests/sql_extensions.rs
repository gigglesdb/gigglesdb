mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
#[test]
fn lifecycle_and_oracles() {
    let a = Admin::create_engine(
        common::fresh("extensions"),
        Limits::default(),
        Engine::Legacy,
    )
    .unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        s.execute("SELECT uuid_nil()", &[], None),
        Err(CoreError::Sql(E::UndefinedFunction))
    );
    run(&mut s, "CREATE EXTENSION \"uuid-ossp\"");
    for (sql, expected) in [
        (
            "SELECT uuid_nil()::text",
            "00000000-0000-0000-0000-000000000000",
        ),
        (
            "SELECT uuid_generate_v3(uuid_ns_dns(),'example.com')::text",
            "9073926b-929f-31c2-abc9-fad77ae3e8eb",
        ),
        (
            "SELECT uuid_generate_v5(uuid_ns_url(),'http://x')::text",
            "c059942f-567e-54eb-aeb5-5fd9830bce43",
        ),
        (
            "SELECT uuid_generate_v3(uuid_nil(),'')::text",
            "4ae71336-e44b-39bf-b9d2-752e234818a5",
        ),
        (
            "SELECT uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8','example.com')::text",
            "cfbff0d1-9375-5685-968c-48ce8b15ae17",
        ),
    ] {
        assert_eq!(run(&mut s, sql), vec![vec![V::Text(expected.into())]]);
    }
    run(&mut s, "DROP EXTENSION \"uuid-ossp\"");
    assert_eq!(
        s.execute("SELECT uuid_nil()", &[], None),
        Err(CoreError::Sql(E::UndefinedFunction))
    );
    run(&mut s, "CREATE EXTENSION \"uuid-ossp\"");
}

#[test]
fn errors_notices_and_schemas() {
    use giggles_core::relational::{
        take_sql_error_hint as hint, take_sql_error_message as message, take_sql_notices as notices,
    };
    let a = Admin::create_engine(
        common::fresh("extensions-errors"),
        Limits::default(),
        Engine::Legacy,
    )
    .unwrap();
    let mut s = a.sql_session();
    for (sql, code, expected) in [
        (
            "CREATE EXTENSION nope",
            E::Unsupported,
            "extension \"nope\" is not available",
        ),
        (
            "CREATE EXTENSION \"uuid-ossp\" VERSION '9.9'",
            E::InvalidParameterValue,
            "extension \"uuid-ossp\" has no installation script nor update path for version \"9.9\"",
        ),
        (
            "CREATE EXTENSION \"uuid-ossp\" SCHEMA nosuch",
            E::InvalidSchema,
            "schema \"nosuch\" does not exist",
        ),
        (
            "CREATE EXTENSION plpgsql",
            E::DuplicateObject,
            "extension \"plpgsql\" already exists",
        ),
        (
            "DROP EXTENSION nope",
            E::UndefinedObject,
            "extension \"nope\" does not exist",
        ),
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(code)),
            "{sql}"
        );
        assert_eq!(message().as_deref(), Some(expected));
        if sql == "CREATE EXTENSION nope" {
            assert_eq!(
                hint(),
                Some(
                    "The extension must first be installed on the system where PostgreSQL is running."
                )
            );
        }
    }
    run(&mut s, "CREATE SCHEMA e");
    run(
        &mut s,
        "CREATE EXTENSION \"uuid-ossp\" WITH VERSION '1.1' SCHEMA e CASCADE",
    );
    assert_eq!(
        s.execute("SELECT uuid_nil()", &[], None),
        Err(CoreError::Sql(E::UndefinedFunction))
    );
    run(&mut s, "SELECT e.uuid_nil()");
    assert_eq!(
        s.execute("CREATE EXTENSION \"uuid-ossp\"", &[], None),
        Err(CoreError::Sql(E::DuplicateObject))
    );
    s.set_search_path("e, public", "giggles");
    run(&mut s, "SELECT uuid_nil()");
    s.set_search_path("public", "giggles");
    run(&mut s, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
    assert_eq!(
        notices(),
        vec![(
            "extension \"uuid-ossp\" already exists, skipping".into(),
            None,
            Some("42710")
        )]
    );
    run(&mut s, "DROP EXTENSION IF EXISTS nope, \"uuid-ossp\"");
    assert_eq!(
        notices(),
        vec![(
            "extension \"nope\" does not exist, skipping".into(),
            None,
            Some("00000")
        )]
    );
    run(&mut s, "CREATE EXTENSION \"uuid-ossp\" SCHEMA pg_catalog");
    run(&mut s, "SELECT pg_catalog.uuid_nil()");
    run(&mut s, "SELECT uuid_nil()");
}
#[test]
fn defaults_restrict_cascade_and_text_is_not_a_dependency() {
    use giggles_core::relational::{
        take_sql_error_detail as detail, take_sql_error_hint as hint, take_sql_notices as notices,
    };
    let a = Admin::create_engine(
        common::fresh("extensions-defaults"),
        Limits::default(),
        Engine::Legacy,
    )
    .unwrap();
    let mut s = a.sql_session();
    run(&mut s, "CREATE EXTENSION \"uuid-ossp\"");
    run(
        &mut s,
        "CREATE TABLE t (id INT PRIMARY KEY, u UUID DEFAULT uuid_generate_v4(), note TEXT DEFAULT 'uuid_nil()')",
    );
    assert_eq!(
        s.execute("DROP EXTENSION \"uuid-ossp\"", &[], None),
        Err(CoreError::Sql(E::DependentObjects))
    );
    assert_eq!(
        detail().as_deref(),
        Some("default value for column u of table t depends on function uuid_generate_v4()")
    );
    assert_eq!(
        hint(),
        Some("Use DROP ... CASCADE to drop the dependent objects too.")
    );
    run(&mut s, "DROP EXTENSION \"uuid-ossp\" CASCADE");
    assert_eq!(
        notices(),
        vec![(
            "drop cascades to default value for column u of table t".into(),
            None,
            Some("00000")
        )]
    );
    run(&mut s, "INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        run(&mut s, "SELECT u,note FROM t"),
        vec![vec![V::Null, V::Text("uuid_nil()".into())]]
    );
}
#[test]
fn uuid_versions_variants_and_strictness() {
    let a = Admin::create_engine(
        common::fresh("extensions-bits"),
        Limits::default(),
        Engine::Legacy,
    )
    .unwrap();
    let mut s = a.sql_session();
    run(&mut s, "CREATE EXTENSION \"uuid-ossp\"");
    for (function, version) in [
        ("uuid_generate_v1", 1),
        ("uuid_generate_v1mc", 1),
        ("uuid_generate_v4", 4),
    ] {
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let row = run(&mut s, &format!("SELECT {function}()"));
            let V::Text(text) = &row[0][0] else {
                panic!("uuid text")
            };
            let digits = text.replace('-', "");
            let bytes = (0..32)
                .step_by(2)
                .map(|i| u8::from_str_radix(&digits[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(bytes[6] >> 4, version);
            assert_eq!(bytes[8] >> 6, 2);
            // GigglesDB policy: v1 also uses a multicast node, unlike PG18 host MACs.
            // See docs/extensions.md.
            if version == 1 {
                assert_eq!(bytes[10] & 1, 1);
            }
            assert!(seen.insert(bytes.clone()));
        }
    }
    assert_eq!(
        run(
            &mut s,
            "SELECT uuid_generate_v3(NULL,'x'), uuid_generate_v5(uuid_nil(),NULL)"
        ),
        vec![vec![V::Null, V::Null]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT uuid_generate_v3(uuid_nil(),''::varchar)::text"
        ),
        vec![vec![V::Text("4ae71336-e44b-39bf-b9d2-752e234818a5".into())]]
    );
}
#[test]
fn restart_and_database_isolation() {
    let path = common::fresh("extensions-restart");
    {
        let a = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        run(&mut a.sql_session(), "CREATE EXTENSION \"uuid-ossp\"");
        let other = Admin::create_engine(
            common::fresh("extensions-other"),
            Limits::default(),
            Engine::Legacy,
        )
        .unwrap();
        assert_eq!(
            other.sql_session().execute("SELECT uuid_nil()", &[], None),
            Err(CoreError::Sql(E::UndefinedFunction))
        );
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    run(&mut a.sql_session(), "SELECT uuid_nil()");
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_restart_persists_extension() {
    let path = common::fresh("extensions-v2");
    {
        let a = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        run(&mut a.sql_session(), "CREATE EXTENSION \"uuid-ossp\"");
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(&mut s, "SELECT uuid_nil()");
}

#[test]
fn extension_routing_uses_registry_without_granting_table_free_access() {
    use giggles_core::relational::{BUILTIN_EXTENSIONS, Prepared};
    for extension in BUILTIN_EXTENSIONS {
        for function in extension.functions {
            let sql = format!("SELECT {}()", function.name);
            assert!(Prepared::extension_routine_call(&sql), "{sql}");
            assert!(
                !Prepared::rejects_unsupported_table_free_scalar(&sql),
                "{sql}"
            );
        }
    }
    assert!(!Prepared::extension_routine_call("SELECT 'uuid_nil()'"));
    assert!(!Prepared::extension_routine_call(
        "SELECT 1 /* uuid_nil() */"
    ));
    assert!(Prepared::extension_routine_call(
        "SELECT length(public.uuid_nil()::text)"
    ));
}

#[test]
fn absent_extension_argument_diagnostics() {
    use giggles_core::relational::{take_sql_error_message as message, take_sql_error_hint as hint};
    let a = Admin::create_engine(common::fresh("extensions-absent"), Limits::default(), Engine::Legacy).unwrap();
    let mut s = a.sql_session();
    for (args, types) in [
        ("'6ba7b810-9dad-11d1-80b4-00c04fd430c8','x'", "unknown, unknown"),
        ("'6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,'x'::text", "uuid, text"),
        ("1,'x'::varchar", "integer, character varying"),
    ] {
        for function in ["uuid_generate_v3", "uuid_generate_v5"] {
            assert_eq!(s.execute(&format!("SELECT {function}({args})"), &[], None), Err(CoreError::Sql(E::UndefinedFunction)));
            assert_eq!(message(), Some(format!("function {function}({types}) does not exist")));
            assert_eq!(hint(), Some("No function matches the given name and argument types. You might need to add explicit type casts."));
        }
    }
}

fn schema_extension_lifecycle(engine: Engine) {
    use giggles_core::relational::{take_sql_error_message as message, take_sql_error_detail as detail, take_sql_error_hint as hint, take_sql_notices as notices};
    let path = common::fresh("extensions-schema-drop");
    {
        let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE SCHEMA e");
        run(&mut s, "CREATE EXTENSION \"uuid-ossp\" SCHEMA e");
        run(&mut s, "ALTER SCHEMA e RENAME TO f");
        run(&mut s, "SELECT f.uuid_nil()");
        assert_eq!(s.execute("DROP SCHEMA f", &[], None), Err(CoreError::Sql(E::DependentObjects)));
        assert_eq!(message().as_deref(), Some("cannot drop schema f because other objects depend on it"));
        assert_eq!(detail().as_deref(), Some("extension uuid-ossp depends on schema f"));
        assert_eq!(hint(), Some("Use DROP ... CASCADE to drop the dependent objects too."));
        run(&mut s, "DROP SCHEMA f CASCADE");
        assert_eq!(notices(), vec![("drop cascades to extension uuid-ossp".into(), None, Some("00000"))]);
        run(&mut s, "CREATE SCHEMA e");
        run(&mut s, "CREATE EXTENSION \"uuid-ossp\" SCHEMA e");
        run(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, u UUID DEFAULT e.uuid_generate_v4())");
        run(&mut s, "ALTER SCHEMA e RENAME TO g");
        run(&mut s, "INSERT INTO t (id) VALUES (0)");
        run(&mut s, "DELETE FROM t WHERE id=0");
        run(&mut s, "DROP SCHEMA g CASCADE");
        assert_eq!(notices(), vec![("drop cascades to 2 other objects".into(), Some("drop cascades to extension uuid-ossp\ndrop cascades to default value for column u of table t".into()), Some("00000"))]);
        run(&mut s, "INSERT INTO t (id) VALUES (1)");
        assert_eq!(run(&mut s, "SELECT u FROM t"), vec![vec![V::Null]]);
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    assert_eq!(s.execute("SELECT uuid_nil()", &[], None), Err(CoreError::Sql(E::UndefinedFunction)));
    run(&mut s, "CREATE TABLE after_restart (id INT PRIMARY KEY)");
    run(&mut s, "CREATE EXTENSION \"uuid-ossp\"");
    run(&mut s, "SELECT uuid_nil()");
}
#[test]
fn schema_extension_dependencies() { schema_extension_lifecycle(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_schema_extension_dependencies_restart() { schema_extension_lifecycle(Engine::V2); }
