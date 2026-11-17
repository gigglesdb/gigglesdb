mod common;
use giggles_core::{Admin, Limits, SqlSession, relational::Value as V};
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
#[test]
fn lifecycle_and_restart() {
    let path = common::fresh("user-collations");
    {
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        run(
            &mut s,
            "CREATE COLLATION r1 (provider=icu,locale='de@collation=phonebook')",
        );
        let c = s.user_collations().unwrap();
        assert_eq!(c.len(), 1);
        assert!(c[0].0 >= 16384);
        assert_eq!(c[0].1.locale.as_deref(), Some("de-u-co-phonebk"));
        assert_eq!(c[0].1.version.as_deref(), Some("153.128.46"));
        run(&mut s, "COMMENT ON COLLATION r1 IS 'hello'");
        run(&mut s, "ALTER COLLATION r1 OWNER TO postgres");
        run(&mut s, "ALTER COLLATION r1 RENAME TO r2");
        run(&mut s, "CREATE SCHEMA rs");
        run(&mut s, "ALTER COLLATION r2 SET SCHEMA rs");
        assert_eq!(
            run(&mut s, "SELECT 'a' COLLATE rs.r2 < 'b'"),
            vec![vec![V::Bool(true)]]
        );
        assert!(s.execute("SELECT 'a' COLLATE r2", &[], None).is_err());
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let c = s.user_collations().unwrap();
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].1.name, "r2");
    assert_eq!(c[0].1.schema, "rs");
    assert_eq!(c[0].1.owner, "postgres");
    assert_eq!(c[0].1.comment.as_deref(), Some("hello"));
    assert!(s.execute("DROP SCHEMA rs", &[], None).is_err());
    assert_eq!(
        giggles_core::relational::take_sql_error_detail().as_deref(),
        Some("collation rs.r2 depends on schema rs")
    );
    s.set_search_path("rs", "giggles");
    assert_eq!(
        run(&mut s, "SELECT 'a' COLLATE r2 < 'b'"),
        vec![vec![V::Bool(true)]]
    );
    run(&mut s, "DROP COLLATION rs.r2");
    run(&mut s, "DROP SCHEMA rs");
}
#[test]
fn nondeterministic_comparison_and_copy() {
    let a = Admin::create(common::fresh("collation-nd"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE COLLATION n (provider=icu,locale='und-u-ks-level1',deterministic=false)",
    );
    assert_eq!(
        run(&mut s, "SELECT 'a' COLLATE n = 'A'"),
        vec![vec![V::Bool(true)]]
    );
    assert_eq!(
        run(&mut s, r"SELECT U&'\00e9' COLLATE n = U&'e\0301'"),
        vec![vec![V::Bool(true)]]
    );
    assert_eq!(
        run(&mut s, r"SELECT U&'\00e9' = U&'e\0301'"),
        vec![vec![V::Bool(false)]]
    );
    run(&mut s, "CREATE COLLATION n2 FROM n");
    assert_eq!(
        run(&mut s, "SELECT 'a' COLLATE n2 = 'A'"),
        vec![vec![V::Bool(true)]]
    );
    assert_eq!(
        s.execute("SELECT 'a' COLLATE n = 'A' COLLATE n2", &[], None),
        Err(giggles_core::CoreError::Sql(
            giggles_core::relational::SqlError::CollationMismatch
        ))
    );
    assert!(
        !giggles_core::relational::take_sql_error_message()
            .unwrap()
            .contains('\0')
    );
    run(&mut s, "CREATE COLLATION c FROM \"C\"");
    assert_eq!(
        run(&mut s, "SELECT 'a' COLLATE c < 'B'"),
        vec![vec![V::Bool(false)]]
    );
}

#[test]
fn equality_groups_and_distinct() {
    equality_groups_for(giggles_core::Engine::Legacy);
    #[cfg(feature = "engine-rocksdb")]
    equality_groups_for(giggles_core::Engine::V2);
}
fn equality_groups_for(engine: giggles_core::Engine) {
    let a =
        Admin::create_engine(common::fresh("collation-groups"), Limits::default(), engine).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE COLLATION n (provider=icu,locale='und-u-ks-level1',deterministic=false)",
    );
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY,v text)");
    run(&mut s, "INSERT INTO t VALUES(1,'a'),(2,'A'),(3,'b')");
    assert_eq!(run(&mut s, "SELECT DISTINCT v COLLATE n FROM t").len(), 2);
    assert_eq!(
        run(
            &mut s,
            "SELECT v COLLATE n,count(*) FROM t GROUP BY v COLLATE n"
        )
        .len(),
        2
    );
}
#[test]
fn errors_options_and_optional_ddl() {
    use giggles_core::{CoreError, relational::SqlError as E};
    let a = Admin::create(common::fresh("collation-errors"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (sql, e) in [
        (
            "CREATE COLLATION x (provider=icu)",
            E::InvalidTableDefinition,
        ),
        (
            "CREATE COLLATION x (provider=bogus,locale='und')",
            E::InvalidTableDefinition,
        ),
        (
            "CREATE COLLATION x (provider=icu,locale='und',deterministic=maybe)",
            E::Syntax,
        ),
        (
            "CREATE COLLATION x (provider=builtin,locale='en')",
            E::WrongObjectType,
        ),
        (
            "CREATE COLLATION x (provider=libc,locale='de_DE.utf8')",
            E::InvalidParameterValue,
        ),
        (
            "CREATE COLLATION x (provider=icu,locale='und',rules='&&&')",
            E::InvalidParameterValue,
        ),
        ("DROP COLLATION nope", E::UndefinedObject),
        ("DROP COLLATION \"C\"", E::DependentObjects),
    ] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(e)), "{sql}");
    }
    run(
        &mut s,
        "CREATE COLLATION x (provider=builtin,locale='PG_UNICODE_FAST')",
    );
    assert_eq!(
        s.execute("CREATE COLLATION x FROM \"C\"", &[], None),
        Err(CoreError::Sql(E::DuplicateObject))
    );
    assert_eq!(
        giggles_core::relational::take_sql_error_message().as_deref(),
        Some("collation \"x\" already exists")
    );
    run(&mut s, "CREATE COLLATION y FROM \"C\"");
    assert_eq!(
        s.execute("ALTER COLLATION y RENAME TO x", &[], None),
        Err(CoreError::Sql(E::DuplicateObject))
    );
    assert_eq!(
        giggles_core::relational::take_sql_error_message().as_deref(),
        Some("collation \"x\" already exists in schema \"public\"")
    );
    run(&mut s, "DROP COLLATION y RESTRICT");
    run(
        &mut s,
        "CREATE COLLATION IF NOT EXISTS x (provider=icu,locale='und')",
    );
    assert_eq!(
        giggles_core::relational::take_sql_notices()[0].2,
        Some("42710")
    );
    assert_eq!(s.user_collations().unwrap().len(), 1);
    run(&mut s, "DROP COLLATION IF EXISTS missing,x");
    assert!(s.user_collations().unwrap().is_empty());
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_restart() {
    use giggles_core::Engine;
    let path = common::fresh("collation-v2");
    {
        let a = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = a.sql_session();
        run(
            &mut s,
            "CREATE COLLATION n (provider=icu,locale='und',deterministic=false)",
        );
        run(&mut s, "COMMENT ON COLLATION n IS 'persist'");
        run(&mut s, "ALTER COLLATION n OWNER TO postgres");
        run(&mut s, "CREATE SCHEMA saved_schema");
        run(&mut s, "ALTER COLLATION n SET SCHEMA saved_schema");
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        s.user_collations().unwrap()[0].1.comment.as_deref(),
        Some("persist")
    );
    let c = &s.user_collations().unwrap()[0].1;
    assert_eq!(c.owner, "postgres");
    assert_eq!(c.schema, "saved_schema");
    assert_eq!(c.version.as_deref(), Some("153.128"));
    assert!(!c.deterministic);
    assert_eq!(
        run(
            &mut s,
            "SELECT '\u{e9}' COLLATE saved_schema.n = 'e\u{301}'"
        ),
        vec![vec![V::Bool(true)]]
    );
}

#[test]
fn postgres18_option_rows_orders_and_equality_groups() {
    let a = Admin::create(common::fresh("collation-oracle"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(&mut s, "CREATE TABLE collref(id int PRIMARY KEY,v text)");
    let values = include_str!("fixtures/expression_collation_orders.txt")
        .lines()
        .filter_map(|l| l.strip_prefix("S|"))
        .map(|l| {
            let (id, v) = l.split_once('|').unwrap();
            format!("({id},'{}')", v.replace("'", "''"))
        })
        .collect::<Vec<_>>();
    for chunk in values.chunks(50) {
        run(
            &mut s,
            &format!("INSERT INTO collref VALUES{}", chunk.join(",")),
        );
    }
    for sql in include_str!("fixtures/user_collation/creates.sql").lines() {
        run(&mut s, sql);
    }
    let collations = s.user_collations().unwrap();
    assert_eq!(collations.len(), 29);
    for line in include_str!("fixtures/user_collation/expected.txt").lines() {
        let f: Vec<_> = line.split('|').collect();
        let label = f[1];
        let name = format!("r0c_{label}");
        match f[0] {
            "P" => {
                let c = &collations.iter().find(|(_, c)| c.name == name).unwrap().1;
                assert_eq!(
                    c.encoding,
                    if c.provider == "i" || label == "from_c" {
                        -1
                    } else {
                        6
                    },
                    "encoding {label}"
                );
                let empty = |s: &Option<String>| s.clone().unwrap_or_default();
                assert_eq!(
                    [
                        c.provider.clone(),
                        if c.deterministic { "t" } else { "f" }.into(),
                        empty(&c.collate),
                        empty(&c.ctype),
                        empty(&c.locale),
                        empty(&c.rules),
                        empty(&c.version)
                    ],
                    f[2..]
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                        .as_slice(),
                    "{label}"
                );
            }
            "O" => {
                let actual = run(
                    &mut s,
                    &format!("SELECT id FROM collref ORDER BY v COLLATE {name},id"),
                )
                .into_iter()
                .map(|r| match r[0] {
                    V::Int(n) => n.to_string(),
                    _ => panic!(),
                })
                .collect::<Vec<_>>()
                .join(",");
                assert_eq!(actual, f[2], "order {label}");
            }
            "G" => {
                let mut expected = std::collections::BTreeMap::new();
                for g in f[2].split(';').filter(|g| !g.is_empty()) {
                    let ids = g
                        .split(',')
                        .map(|id| id.parse::<i64>().unwrap())
                        .collect::<Vec<_>>();
                    expected.insert(ids[0], ids.len() as i64);
                }
                let actual = run(
                    &mut s,
                    &format!(
                        "SELECT min(id),count(*) FROM collref GROUP BY v COLLATE {name} HAVING count(*) > 1"
                    ),
                );
                let actual = actual
                    .into_iter()
                    .map(|r| match (&r[0], &r[1]) {
                        (V::Int(a), V::Int(b)) => (*a, *b),
                        _ => panic!(),
                    })
                    .collect::<std::collections::BTreeMap<_, _>>();
                assert_eq!(actual, expected, "groups {label}");
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn diagnostics_and_version_refresh() {
    use giggles_core::{
        CoreError,
        relational::{SqlError, take_sql_notices},
    };
    let a = Admin::create(common::fresh("collation-diagnostics"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE COLLATION versioned (provider=icu,locale='und',version='1.0')",
    );
    assert_eq!(take_sql_notices()[0].2, Some("01000"));
    assert_eq!(
        run(&mut s, "SELECT 'a' COLLATE versioned < 'b'"),
        vec![vec![V::Bool(true)]]
    );
    run(&mut s, "ALTER COLLATION versioned REFRESH VERSION");
    assert_eq!(
        take_sql_notices()[0].0,
        "changing version from 1.0 to 153.128"
    );
    run(&mut s, "ALTER COLLATION versioned REFRESH VERSION");
    assert_eq!(take_sql_notices()[0].0, "version has not changed");
    run(
        &mut s,
        "CREATE COLLATION legacy (provider=icu,locale='@colStrength=primary')",
    );
    assert_eq!(
        take_sql_notices()[0].0,
        "using standard form \"und-u-ks-level1\" for ICU locale \"@colStrength=primary\""
    );
    run(
        &mut s,
        "CREATE COLLATION unknown_language (provider=icu,locale='xx-nonsense-zzzz')",
    );
    let notices = take_sql_notices();
    assert_eq!(notices.len(), 2);
    assert!(notices.iter().all(|n| n.2 == Some("22023")));
    run(
        &mut s,
        "CREATE COLLATION libc_en (provider=libc,lc_collate='en_US.utf8',lc_ctype='en_US.utf8')",
    );
    assert_eq!(
        s.user_collations()
            .unwrap()
            .into_iter()
            .find(|(_, c)| c.name == "libc_en")
            .unwrap()
            .1
            .version
            .as_deref(),
        Some("2.41")
    );
    assert_eq!(
        s.execute("SELECT 'a' COLLATE libc_en < 'b'", &[], None),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    run(&mut s, "CREATE SCHEMA cascade_schema");
    run(&mut s, "CREATE COLLATION cascade_schema.n FROM \"C\"");
    run(&mut s, "DROP SCHEMA cascade_schema CASCADE");
    assert!(
        !s.user_collations()
            .unwrap()
            .iter()
            .any(|(_, c)| c.schema == "cascade_schema")
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_write_fence_with_user_and_column_collations_survives_restart_and_export() {
    use giggles_core::Engine;
    let path = common::fresh("collation-write-fence");
    let backup = common::fresh("collation-write-fence-export");
    let restored_path = common::fresh("collation-write-fence-restored");
    {
        let a = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)");
        run(&mut s, "CREATE TABLE t(id int PRIMARY KEY,v text COLLATE ci)");
        run(&mut s, "INSERT INTO t VALUES(1,'ABC')");
        a.install_write_fence(b"holder", "collations").unwrap();
    }
    {
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        assert_eq!(run(&mut s, "SELECT id FROM t WHERE v='abc'"), vec![vec![V::Int(1)]]);
        assert_eq!(s.execute("INSERT INTO t VALUES(2,'x')", &[], None), Err(giggles_core::CoreError::WriteFenced { epoch: 1 }));
        a.export(&backup).unwrap();
        let bytes = std::fs::read(&backup).unwrap();
        let features = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
        assert_eq!(features & ((1 << 23) | (1 << 24)), (1 << 23) | (1 << 24));
        assert_eq!(features & (1 << 22), 0);
    }
    {
        let a = Admin::open_as_fence_holder(&path, Limits::default(), b"holder").unwrap();
        run(&mut a.sql_session(), "INSERT INTO t VALUES(2,'abc')");
    }
    {
        let a = Admin::restore(&backup, &restored_path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        assert_eq!(run(&mut s, "SELECT id FROM t WHERE v='abc'"), vec![vec![V::Int(1)]]);
        run(&mut s, "INSERT INTO t VALUES(2,'abc')");
    }
    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(restored_path).unwrap();
    std::fs::remove_file(backup).unwrap();
}
