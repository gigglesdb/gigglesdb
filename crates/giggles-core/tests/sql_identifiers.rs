use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{Numeric, SqlError as E, SqlResult, Value as V},
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-sql-identifiers-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    test(&mut s);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
fn values(result: SqlResult) -> Vec<Vec<V>> {
    result.rows.into_iter().map(|r| r.values).collect()
}

#[test]
fn quoted_spaces_primary_keys_and_reserved_keyword_columns() {
    database(|s| {
        run(
            s,
            r#"CREATE TABLE "t space" ("row id" integer PRIMARY KEY, "display name" text)"#,
        );
        run(
            s,
            r#"INSERT INTO "t space" ("row id","display name") VALUES (1,'first')"#,
        );
        assert_eq!(
            values(run(
                s,
                r#"SELECT "display name" FROM "t space" WHERE "row id"=1"#
            )),
            vec![vec![V::Text("first".into())]]
        );
        run(
            s,
            r#"UPDATE "t space" SET "display name"='next' WHERE "row id"=1"#,
        );
        assert_eq!(
            run(
                s,
                r#"DELETE FROM "t space" WHERE "row id"=1 RETURNING "display name""#
            )
            .rows[0]
                .values,
            [V::Text("next".into())]
        );
        run(
            s,
            r#"CREATE TABLE keywords ("order" INTEGER PRIMARY KEY, "user" TEXT, "select" TEXT, "returning" TEXT, "where" TEXT, "limit" TEXT)"#,
        );
        run(
            s,
            r#"INSERT INTO keywords ("order","user","select","returning","where","limit") VALUES (1,'u','s','r','w','l')"#,
        );
        let result = run(
            s,
            r#"SELECT "order" AS "select","user","select","returning","where","limit" FROM keywords WHERE "order"=1"#,
        );
        assert_eq!(
            result.columns,
            ["select", "user", "select", "returning", "where", "limit"]
        );
        assert_eq!(
            result.rows[0].values,
            [
                V::Int(1),
                V::Text("u".into()),
                V::Text("s".into()),
                V::Text("r".into()),
                V::Text("w".into()),
                V::Text("l".into())
            ]
        );
    });
}

#[test]
fn mixed_case_is_preserved_and_unquoted_names_fold() {
    database(|s| {
        run(
            s,
            r#"CREATE TABLE "UserAccount" (id BIGINT PRIMARY KEY, "createdAt" TEXT)"#,
        );
        run(
            s,
            r#"INSERT INTO "UserAccount" (id,"createdAt") VALUES (1,'today')"#,
        );
        assert_eq!(
            run(s, r#"SELECT "createdAt" FROM "UserAccount" WHERE id=1"#).columns,
            ["createdAt"]
        );
        error(s, "SELECT * FROM UserAccount LIMIT 1", E::UnknownTable);
        error(s, r#"SELECT * FROM "useraccount" LIMIT 1"#, E::UnknownTable);
        error(
            s,
            r#"SELECT createdAt FROM "UserAccount" WHERE id=1"#,
            E::UnknownColumn,
        );
        run(s, "CREATE TABLE Foo_X (id BIGINT PRIMARY KEY)");
        run(s, "INSERT INTO FOO_X VALUES (2)");
        assert_eq!(
            values(run(s, r#"SELECT id FROM "foo_x" WHERE id=2"#)),
            vec![vec![V::Int(2)]]
        );
        run(s, r#"CREATE TABLE "Bar" (id BIGINT PRIMARY KEY)"#);
        error(s, "SELECT * FROM bar LIMIT 1", E::UnknownTable);
        run(s, "CREATE TABLE bar (id BIGINT PRIMARY KEY)");
        run(s, r#"INSERT INTO "Bar" VALUES (3)"#);
        run(s, "INSERT INTO BAR VALUES (4)");
        assert_eq!(
            values(run(s, r#"SELECT id FROM "Bar" LIMIT 1"#)),
            vec![vec![V::Int(3)]]
        );
        assert_eq!(
            values(run(s, "SELECT id FROM bar LIMIT 1")),
            vec![vec![V::Int(4)]]
        );
    });
}

#[test]
fn escaped_quotes_unicode_and_dollar_identifiers() {
    database(|s| {
        run(
            s,
            r#"CREATE TABLE "a""b" (id BIGINT PRIMARY KEY, "c""d" TEXT)"#,
        );
        run(s, r#"INSERT INTO "a""b" VALUES (1,'quoted')"#);
        assert_eq!(
            run(s, r#"SELECT "c""d" FROM "a""b" WHERE id=1"#).columns,
            ["c\"d"]
        );
        run(
            s,
            "CREATE TABLE caf\u{00e9} (id BIGINT PRIMARY KEY, a$b TEXT)",
        );
        let result = s
            .execute(
                "INSERT INTO caf\u{00e9} (id,a$b) VALUES ($1,$2) RETURNING a$b",
                &[V::Int(1), V::Text("coffee".into())],
                None,
            )
            .unwrap();
        assert_eq!(result.rows[0].values, [V::Text("coffee".into())]);
        assert_eq!(
            values(run(s, "SELECT caf\u{00e9}.a$b FROM caf\u{00e9} WHERE id=1")),
            vec![vec![V::Text("coffee".into())]]
        );
        for sql in [
            "CREATE TABLE \"\" (id BIGINT PRIMARY KEY)",
            "CREATE TABLE \"a\0b\" (id BIGINT PRIMARY KEY)",
            "CREATE TABLE malformed (\"\" BIGINT PRIMARY KEY)",
            "CREATE TABLE malformed (\"a\0b\" BIGINT PRIMARY KEY)",
        ] {
            error(s, sql, E::Syntax);
        }
    });
}

#[test]
fn identifiers_truncate_at_utf8_boundaries_for_tables_columns_aliases_and_indexes() {
    database(|s| {
        for (n, quoted) in [(0, false), (1, true)] {
            let table = format!("t{n}{}", "x".repeat(68));
            let column = format!("c{n}{}", "y".repeat(68));
            let table63 = &table[..63];
            let column63 = &column[..63];
            let q = |v: &str| if quoted { format!("\"{v}\"") } else { v.into() };
            run(
                s,
                &format!(
                    "CREATE TABLE {} ({} BIGINT PRIMARY KEY)",
                    q(&table),
                    q(&column)
                ),
            );
            run(
                s,
                &format!("INSERT INTO {} ({}) VALUES (1)", q(table63), q(column63)),
            );
            let result = run(
                s,
                &format!(
                    "SELECT {} FROM {} WHERE {}=1",
                    q(&column),
                    q(&table),
                    q(column63)
                ),
            );
            assert_eq!(result.columns, [column63]);
            assert_eq!(result.rows[0].values, [V::Int(1)]);
        }
        let prefix = "z".repeat(62);
        let long = format!("{prefix}\u{00e9}abcdef");
        run(
            s,
            &format!("CREATE TABLE \"{long}\" (\"{long}\" BIGINT PRIMARY KEY)"),
        );
        run(s, &format!("INSERT INTO \"{prefix}\" VALUES (7)"));
        let result = run(
            s,
            &format!("SELECT \"{long}\" AS \"{long}\" FROM \"{long}\" LIMIT 1"),
        );
        assert_eq!(result.columns, [prefix.as_str()]);
        assert_eq!(result.rows[0].values, [V::Int(7)]);
        let unquoted_prefix = "u".repeat(62);
        let unquoted_long = format!("{unquoted_prefix}\u{00e9}abcdef");
        run(
            s,
            &format!("CREATE TABLE {unquoted_long} ({unquoted_long} BIGINT PRIMARY KEY)"),
        );
        run(s, &format!("INSERT INTO {unquoted_prefix} VALUES (8)"));
        let alias = "a".repeat(70);
        let result = run(
            s,
            &format!(
                "SELECT {}.{unquoted_prefix} FROM {unquoted_long} AS {alias} LIMIT 1",
                &alias[..63]
            ),
        );
        assert_eq!(result.columns, [unquoted_prefix]);
        assert_eq!(result.rows[0].values, [V::Int(8)]);
        let index = "i".repeat(70);
        run(
            s,
            &format!("CREATE INDEX \"{index}\" ON \"{prefix}\" (\"{prefix}\")"),
        );
        error(
            s,
            &format!(
                "CREATE INDEX \"{}\" ON \"{prefix}\" (\"{prefix}\")",
                &index[..63]
            ),
            E::Duplicate,
        );
    });
}

#[test]
fn public_schema_is_accepted_through_every_statement_and_other_schemas_rejected() {
    database(|s| {
        for (n, schema) in [(0, "public"), (1, "\"public\"")] {
            let table = format!("{schema}.t{n}");
            run(
                s,
                &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"),
            );
            run(s, &format!("INSERT INTO {table} VALUES (1,'old')"));
            assert_eq!(
                run(
                    s,
                    &format!("SELECT {table}.id FROM {table} WHERE {table}.id=1")
                )
                .rows[0]
                    .values,
                [V::Int(1)]
            );
            run(s, &format!("CREATE INDEX ix{n} ON {table} (name)"));
            run(
                s,
                &format!("UPDATE {table} SET name='new' WHERE {table}.id=1 RETURNING {table}.name"),
            );
            assert_eq!(
                run(
                    s,
                    &format!("DELETE FROM {table} WHERE {table}.id=1 RETURNING {table}.name")
                )
                .rows[0]
                    .values,
                [V::Text("new".into())]
            );
        }
        run(
            s,
            "INSERT INTO public.t0 VALUES (pg_catalog.abs(-2),upper('scalar'))",
        );
        assert_eq!(
            run(s, "SELECT name FROM public.t0 WHERE id=2").rows[0].values,
            [V::Text("SCALAR".into())]
        );
        error(s, "CREATE TABLE foo.t (id BIGINT PRIMARY KEY)", E::InvalidSchema);
        // PG18.6: DDL on a missing schema is 3F000 (invalid_schema_name); DML/SELECT stay 42P01
        // (GD-0001 R2 SQL-FIXES, J:/gigglesdb-work/r2/sql-fixes/w2/matrix.tsv).
        error(s, "CREATE INDEX bad ON other.t0 (name)", E::InvalidSchema);
        for sql in [
            "INSERT INTO other.t0 VALUES (2,'x')",
            "SELECT * FROM other.t0 LIMIT 1",
            "UPDATE other.t0 SET name='x' WHERE id=1",
            "DELETE FROM other.t0 WHERE id=1",
            "SELECT other.t0.id FROM t0 LIMIT 1",
            "SELECT * FROM \"Public\".t0 LIMIT 1",
        ] {
            error(s, sql, E::UnknownTable);
        }
    });
}

#[test]
fn prisma_quoted_public_user_parameters_unique_index_and_upsert() {
    database(|s| {
        run(
            s,
            r#"CREATE TABLE "public"."User" ("id" BIGINT PRIMARY KEY, "email" TEXT)"#,
        );
        run(
            s,
            r#"CREATE UNIQUE INDEX "User_email_key" ON "public"."User" ("email")"#,
        );
        let result = s.execute(r#"INSERT INTO "public"."User" ("id","email") VALUES ($1,$2) RETURNING "public"."User"."id","public"."User"."email""#, &[V::Int(1), V::Text("a@example.test".into())], None).unwrap();
        assert_eq!(result.columns, ["id", "email"]);
        assert_eq!(
            result.rows[0].values,
            [V::Int(1), V::Text("a@example.test".into())]
        );
        let result = s.execute(r#"SELECT "public"."User"."id","public"."User"."email" FROM "public"."User" WHERE "public"."User"."id"=$1 LIMIT 1"#, &[V::Int(1)], None).unwrap();
        assert_eq!(result.rows[0].values[0], V::Int(1));
        let result = s.execute(r#"INSERT INTO "public"."User" ("id","email") VALUES ($1,$2) ON CONFLICT ("email") DO UPDATE SET "email"=excluded."email" RETURNING "id""#, &[V::Int(2), V::Text("a@example.test".into())], None).unwrap();
        assert_eq!(result.rows[0].values, [V::Int(1)]);
    });
}

#[test]
fn quoted_aliases_qualifiers_and_wrong_relation_errors() {
    database(|s| {
        run(
            s,
            "CREATE TABLE qualifier (id BIGINT PRIMARY KEY, name TEXT)",
        );
        run(s, "INSERT INTO qualifier VALUES (1,'one')");
        assert_eq!(run(s, r#"SELECT "select".name AS "returning" FROM public.qualifier AS "select" WHERE "select".id=1"#).columns, ["returning"]);
        assert_eq!(
            run(
                s,
                "SELECT qualifier.name FROM qualifier WHERE qualifier.id=1"
            )
            .rows[0]
                .values,
            [V::Text("one".into())]
        );
        assert_eq!(
            run(
                s,
                r#"SELECT "select".name FROM qualifier "select" WHERE "select".id=1"#
            )
            .rows[0]
                .values,
            [V::Text("one".into())]
        );
        run(
            s,
            r#"CREATE TABLE quoted_default (id BIGINT PRIMARY KEY, "default" TEXT)"#,
        );
        run(s, r#"INSERT INTO quoted_default VALUES (1,'before')"#);
        assert_eq!(run(s, r#"INSERT INTO quoted_default VALUES (1,'after') ON CONFLICT (id) DO UPDATE SET "default"=excluded."default" RETURNING "default""#).rows[0].values, [V::Text("after".into())]);
        error(
            s,
            "SELECT qualifier.missing FROM qualifier LIMIT 1",
            E::UnknownColumn,
        );
    });
}

#[test]
fn internal_reserved_identifiers_remain_rejected_when_quoted() {
    database(|s| {
        for sql in [
            r#"CREATE TABLE "pg_x" (id BIGINT PRIMARY KEY)"#,
            "CREATE TABLE pg_x (id BIGINT PRIMARY KEY)",
            r#"CREATE TABLE "giggles_rowid" (id BIGINT PRIMARY KEY)"#,
            r#"CREATE TABLE safe ("giggles_rowid" BIGINT PRIMARY KEY)"#,
        ] {
            error(s, sql, E::Reserved);
        }
        // Column names follow PostgreSQL 18.6, which accepts pg_-prefixed and
        // other internal-looking names as columns (AM-4); relation names stay reserved.
        run(s, "CREATE TABLE safe (pg_x BIGINT PRIMARY KEY)");
    });
}

#[test]
fn reserved_create_names_preserve_catalog_and_quoted_case_boundary() {
    for sql in [
        "CREATE TABLE t (giggles_rowid bigint primary key, x int)",
        r#"CREATE TABLE t ("giggles_rowid" bigint primary key, x int)"#,
        r#"CREATE TABLE "giggles" (id int primary key)"#,
        r#"CREATE TABLE "giggles_x" (id int primary key)"#,
        r#"CREATE TABLE "system_x" (id int primary key)"#,
        r#"CREATE TABLE "pg_x" (id int primary key)"#,
        r#"CREATE TABLE "repair_x" (id int primary key)"#,
        r#"CREATE TABLE "internal_x" (id int primary key)"#,
        r#"CREATE TABLE "tasks" (id int primary key)"#,
        r#"CREATE TABLE "receipts" (id int primary key)"#,
    ] {
        let path = path();
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        error(&mut s, sql, E::Reserved);
        let (_, tables) = admin.catalog().unwrap();
        assert!(tables.is_empty(), "{sql} left a table in the catalog");
        drop(s);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }

    // Quoted identifiers preserve case; heap detection recognizes only the exact
    // lowercase giggles_rowid, so this uppercase primary key remains user-defined.
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        r#"CREATE TABLE t3 ("GIGGLES_ROWID" bigint primary key)"#,
    );
    let (_, tables) = admin.catalog().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "t3");
    assert_eq!(tables[0].columns[0].name, "GIGGLES_ROWID");
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn quoted_case_and_spaces_survive_reopen() {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        r#"CREATE TABLE "Case Table" ("Key Id" BIGINT, "createdAt" TEXT, PRIMARY KEY ("Key Id"))"#,
    );
    run(&mut s, r#"INSERT INTO "Case Table" VALUES (1,'persisted')"#);
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(
        run(
            &mut s,
            r#"SELECT "createdAt" FROM "Case Table" WHERE "Key Id"=1"#
        )
        .rows[0]
            .values,
        [V::Text("persisted".into())]
    );
    error(
        &mut s,
        r#"SELECT createdAt FROM "Case Table" LIMIT 1"#,
        E::UnknownColumn,
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn quoted_extract_field_uses_identifier_grammar() {
    database(|s| {
        let result = run(s, r#"SELECT extract("YEAR" FROM DATE '2026-11-06')"#);
        assert_eq!(
            result.rows[0].values,
            [V::Numeric(Numeric::parse("2026").unwrap())]
        );
    });
}

#[test]
fn aliases_hide_base_names_and_resolve_only_the_visible_relation() {
    database(|s| {
        run(s, "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, y BIGINT)");
        run(s, "CREATE TABLE u (id BIGINT PRIMARY KEY, x BIGINT)");
        run(s, "INSERT INTO t VALUES (1,10,30)");
        run(s, "INSERT INTO u VALUES (1,20)");
        for sql in ["SELECT t.x FROM t AS a", "SELECT public.t.x FROM t AS a", "SELECT wrong.x FROM t", "INSERT INTO t VALUES (2,11,31) RETURNING wrong.x"] {
            error(s, sql, E::UnknownTable);
        }
        error(s, "SELECT t.y FROM t AS a JOIN u AS t ON a.id=t.id", E::UnknownColumn);
        assert_eq!(values(run(s, "SELECT t.x FROM t AS a JOIN u AS t ON a.id=t.id")), vec![vec![V::Int(20)]]);
        for sql in ["SELECT t.x FROM t", "SELECT public.t.x FROM public.t"] {
            assert_eq!(values(run(s, sql)), vec![vec![V::Int(10)]]);
        }
        assert_eq!(values(run(s, "UPDATE t SET x=12 WHERE id=1 RETURNING public.t.x")), vec![vec![V::Int(12)]]);
    });
}

fn quoted_database() -> (PathBuf, Admin, SqlSession) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(&mut session, r#"CREATE TABLE "My.Tbl" ("Col X" BIGINT PRIMARY KEY)"#);
    run(&mut session, r#"INSERT INTO "My.Tbl" VALUES (7)"#);
    (path, admin, session)
}

#[test]
fn sql_quoted_names_survive_native_scan() {
    let (path, admin, mut session) = quoted_database();
    let result = session.scan(giggles_core::relational::Scan {
        table: "My.Tbl".into(), projection: vec!["Col X".into()], prefix: vec![],
        lower: None, upper: None, reverse: false, limit: 10,
    }).unwrap();
    assert_eq!(result.columns, ["Col X"]);
    assert_eq!(values(result), vec![vec![V::Int(7)]]);
    drop(session); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sql_quoted_names_survive_admin_catalog() {
    let (path, admin, session) = quoted_database();
    let (_, tables) = admin.catalog().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "My.Tbl");
    assert_eq!(tables[0].columns[0].name, "Col X");
    drop(session); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sql_quoted_names_and_rows_survive_export_verify_restore() {
    let (source, admin, session) = quoted_database();
    let export = path();
    let destination = path();
    admin.export(&export).unwrap();
    Admin::verify_export(&export).unwrap();
    let restored = Admin::restore(&export, &destination, Limits::default()).unwrap();
    assert_eq!(restored.catalog().unwrap(), admin.catalog().unwrap());
    let mut restored_session = restored.sql_session();
    let result = run(&mut restored_session, r#"SELECT "Col X" FROM "My.Tbl""#);
    assert_eq!(result.columns, ["Col X"]);
    assert_eq!(values(result), vec![vec![V::Int(7)]]);
    drop(restored_session); drop(restored); drop(session); drop(admin);
    std::fs::remove_dir_all(source).unwrap();
    std::fs::remove_dir_all(destination).unwrap();
    std::fs::remove_file(export).unwrap();
}

#[test]
fn cross_join_alias_never_exposes_hidden_base_columns() {
    database(|s| {
        run(s, "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, y BIGINT)");
        run(s, "CREATE TABLE u (id BIGINT PRIMARY KEY, x BIGINT)");
        run(s, "INSERT INTO t VALUES (1,10,30)");
        run(s, "INSERT INTO u VALUES (1,20)");
        // Once CROSS JOIN is supported, t.y must resolve via alias t (u) and give
        // 42703, never a.y.
        assert!(matches!(
            s.execute("select t.y from t as a cross join u as t", &[], None),
            Err(CoreError::Sql(_))
        ));
    });
}
