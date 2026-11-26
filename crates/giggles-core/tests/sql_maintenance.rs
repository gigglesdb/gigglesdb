mod common;
use giggles_core::relational::{take_sql_error_message, take_sql_notices};
use giggles_core::{Admin, Engine, Limits, SqlSession, roles::NewRole, sqlstate};
fn each(mut f: impl FnMut(Engine)) {
    f(Engine::Legacy);
    #[cfg(feature = "engine-rocksdb")]
    f(Engine::V2);
}
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?} {:?}", take_sql_error_message()));
}
fn fail(s: &mut SqlSession, sql: &str, code: &str, message: &str) {
    let e = s.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&e), code, "{sql}: {e:?}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
}
#[test]
fn maintenance_options_targets_and_transactions() {
    each(|engine| {
        let a =
            Admin::create_engine(common::fresh("maintenance"), Limits::default(), engine).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, v int)");
        run(&mut s, "CREATE INDEX t_v ON t(v)");
        for sql in [
            "VACUUM",
            "VACUUM t",
            "VACUUM FULL FREEZE VERBOSE ANALYZE t",
            "VACUUM (ANALYZE, VERBOSE false, FULL true) t(v)",
            "VACUUM t,t",
            "VACUUM ONLY t",
            "VACUUM pg_class",
            "ANALYSE t",
            "ANALYZE (VERBOSE, SKIP_LOCKED) t(v,id)",
            "ANALYZE (BUFFER_USAGE_LIMIT 0) t",
            "VACUUM (DISABLE_PAGE_SKIPPING, SKIP_LOCKED, INDEX_CLEANUP auto, PROCESS_MAIN, PROCESS_TOAST, TRUNCATE, PARALLEL 2, SKIP_DATABASE_STATS, ONLY_DATABASE_STATS false, BUFFER_USAGE_LIMIT '256kB', FREEZE) t",
            "VACUUM (ONLY_DATABASE_STATS)",
            "REINDEX INDEX t_v",
            "REINDEX TABLE t",
            "REINDEX INDEX CONCURRENTLY t_v",
            "REINDEX (VERBOSE) TABLE t",
            "REINDEX (CONCURRENTLY) TABLE t",
            "REINDEX (TABLESPACE pg_default) INDEX t_v",
            "REINDEX SCHEMA public",
            "REINDEX DATABASE",
            "REINDEX SYSTEM",
            "CHECKPOINT",
            "ALTER TABLE t SET ACCESS METHOD heap",
            "ALTER TABLE t SET ACCESS METHOD DEFAULT",
            "ALTER TABLE t SET TABLESPACE pg_default",
        ] {
            run(&mut s, sql);
        }
        for (sql, code, msg) in [
            (
                "VACUUM t(v)",
                "0A000",
                "ANALYZE option must be specified when a column list is provided",
            ),
            (
                "VACUUM (ANALYZE) t(nope)",
                "42703",
                "column \"nope\" of relation \"t\" does not exist",
            ),
            (
                "ANALYZE t(nope)",
                "42703",
                "column \"nope\" of relation \"t\" does not exist",
            ),
            (
                "ANALYZE t(v,v)",
                "42701",
                "column \"v\" of relation \"t\" appears more than once",
            ),
            ("VACUUM nope", "42P01", "relation \"nope\" does not exist"),
            (
                "VACUUM (BOGUS) t",
                "42601",
                "unrecognized VACUUM option \"bogus\"",
            ),
            (
                "ANALYZE (FULL) t",
                "42601",
                "unrecognized ANALYZE option \"full\"",
            ),
            (
                "REINDEX (BOGUS) TABLE t",
                "42601",
                "unrecognized REINDEX option \"bogus\"",
            ),
            (
                "CLUSTER (BOGUS) t",
                "42601",
                "unrecognized CLUSTER option \"bogus\"",
            ),
            (
                "VACUUM (ANALYZE maybe) t",
                "42601",
                "analyze requires a Boolean value",
            ),
            (
                "VACUUM (INDEX_CLEANUP bogus) t",
                "42601",
                "index_cleanup requires a Boolean value",
            ),
            (
                "VACUUM (PARALLEL) t",
                "42601",
                "parallel option requires a value between 0 and 1024",
            ),
            (
                "VACUUM (PARALLEL -1) t",
                "42601",
                "parallel workers for vacuum must be between 0 and 1024",
            ),
            (
                "VACUUM (FULL,PARALLEL 2) t",
                "0A000",
                "VACUUM FULL cannot be performed in parallel",
            ),
            (
                "VACUUM (FULL,DISABLE_PAGE_SKIPPING) t",
                "0A000",
                "VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL",
            ),
            (
                "VACUUM (BUFFER_USAGE_LIMIT '1kB') t",
                "22023",
                "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
            ),
            (
                "VACUUM (ONLY_DATABASE_STATS) t",
                "0A000",
                "ONLY_DATABASE_STATS cannot be specified with a list of tables",
            ),
            ("REINDEX INDEX t", "42809", "\"t\" is not an index"),
            (
                "REINDEX TABLE t_v",
                "42809",
                "\"t_v\" is not a table or materialized view",
            ),
            (
                "REINDEX SCHEMA nope",
                "3F000",
                "schema \"nope\" does not exist",
            ),
            (
                "REINDEX SYSTEM CONCURRENTLY",
                "0A000",
                "cannot reindex system catalogs concurrently",
            ),
            (
                "CLUSTER t",
                "42704",
                "there is no previously clustered index for table \"t\"",
            ),
            (
                "CLUSTER t USING nope",
                "42704",
                "index \"nope\" for table \"t\" does not exist",
            ),
            (
                "ALTER TABLE t SET ACCESS METHOD btree",
                "55000",
                "access method \"btree\" is not of type TABLE",
            ),
            (
                "ALTER TABLE t SET ACCESS METHOD nope",
                "42704",
                "access method \"nope\" does not exist",
            ),
            (
                "ALTER TABLE t SET TABLESPACE nope",
                "42704",
                "tablespace \"nope\" does not exist",
            ),
            (
                "ALTER TABLE t SET TABLESPACE pg_global",
                "22023",
                "only shared relations can be placed in pg_global tablespace",
            ),
        ] {
            fail(&mut s, sql, code, msg);
        }
        for (sql, code, message) in [
            (
                "CHECKPOINT (BOGUS)",
                "42601",
                "syntax error at or near \"(\"",
            ),
            ("CHECKPOINT t", "42601", "syntax error at or near \"t\""),
            (
                "REINDEX DATABASE other",
                "0A000",
                "can only reindex the currently open database",
            ),
            (
                "VACUUM pg_no_such_relation",
                "42P01",
                "relation \"pg_no_such_relation\" does not exist",
            ),
        ] {
            fail(&mut s, sql, code, message);
        }
        for (sql, command) in [
            ("VACUUM t", "VACUUM"),
            ("VACUUM", "VACUUM"),
            ("VACUUM ANALYZE t", "VACUUM"),
            ("REINDEX TABLE CONCURRENTLY t", "REINDEX CONCURRENTLY"),
            ("REINDEX SCHEMA public", "REINDEX SCHEMA"),
            ("REINDEX DATABASE", "REINDEX DATABASE"),
            ("CLUSTER", "CLUSTER"),
        ] {
            run(&mut s, "BEGIN");
            fail(
                &mut s,
                sql,
                "25001",
                &format!("{command} cannot run inside a transaction block"),
            );
            run(&mut s, "ROLLBACK");
        }
        run(&mut s, "BEGIN");
        for sql in [
            "ANALYZE t",
            "CHECKPOINT",
            "REINDEX TABLE t",
            "REINDEX INDEX t_v",
            "CLUSTER t USING t_v",
        ] {
            run(&mut s, sql);
        }
        run(&mut s, "ROLLBACK");
        run(&mut s, "CREATE SCHEMA m");
        run(&mut s, "CREATE TABLE m.s (v int)");
        run(&mut s, "CREATE INDEX m_s_v ON m.s(v)");
        run(&mut s, "VACUUM m.s");
        run(&mut s, "CLUSTER m.s USING m_s_v");
        s.set_search_path("m", "giggles");
        run(&mut s, "ANALYZE s(v)");
    });
}
#[test]
fn maintenance_warns_nonowners_and_special_relations() {
    each(|engine| {
        let a = Admin::create_engine(
            common::fresh("maintenance-roles"),
            Limits::default(),
            engine,
        )
        .unwrap();
        a.roles()
            .create(NewRole {
                name: "plain".into(),
                ..Default::default()
            })
            .unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE p (v int)");
        run(&mut s, "CREATE INDEX p_v ON p(v)");
        if engine == Engine::V2 {
            run(&mut s, "CREATE VIEW tv AS SELECT v FROM p");
            take_sql_notices();
            run(&mut s, "VACUUM tv");
            assert_eq!(
                take_sql_notices(),
                vec![(
                    "skipping \"tv\" --- cannot vacuum non-tables or special system tables".into(),
                    None,
                    Some("01000")
                )]
            );
        }
        run(&mut s, "CREATE SEQUENCE sq");
        for (sql, msg) in [
            (
                "ANALYZE sq",
                "skipping \"sq\" --- cannot analyze non-tables or special system tables",
            ),
            (
                "VACUUM sq",
                "skipping \"sq\" --- cannot vacuum non-tables or special system tables",
            ),
        ] {
            take_sql_notices();
            run(&mut s, sql);
            assert_eq!(take_sql_notices(), vec![(msg.into(), None, Some("01000"))]);
        }
        s.set_ddl_owner(Some("plain"));
        for (sql, msg) in [
            ("VACUUM p", "permission denied to vacuum \"p\", skipping it"),
            (
                "ANALYZE p",
                "permission denied to analyze \"p\", skipping it",
            ),
            (
                "VACUUM ANALYZE p",
                "permission denied to vacuum \"p\", skipping it",
            ),
            ("VACUUM", "permission denied to vacuum \"p\", skipping it"),
            ("ANALYZE", "permission denied to analyze \"p\", skipping it"),
        ] {
            take_sql_notices();
            run(&mut s, sql);
            assert_eq!(take_sql_notices(), vec![(msg.into(), None, Some("01000"))]);
        }
        for (sql, msg) in [
            ("REINDEX TABLE p", "permission denied for table p"),
            ("REINDEX INDEX p_v", "permission denied for index p_v"),
            ("CLUSTER p USING p_v", "permission denied for table p"),
            (
                "CHECKPOINT",
                "permission denied to execute CHECKPOINT command",
            ),
        ] {
            fail(&mut s, sql, "42501", msg);
        }
        assert_eq!(
            giggles_core::relational::take_sql_error_detail().as_deref(),
            Some(
                "Only roles with privileges of the \"pg_checkpoint\" role may execute this command."
            )
        );
        a.roles().set_table_owner("p", "plain").unwrap();
        for sql in [
            "VACUUM p",
            "ANALYZE p",
            "REINDEX TABLE p",
            "REINDEX INDEX p_v",
            "CLUSTER p USING p_v",
        ] {
            take_sql_notices();
            run(&mut s, sql);
            assert!(take_sql_notices().is_empty(), "{sql}");
        }
    });
}

#[test]
fn maintenance_review_regressions() {
    each(|engine| {
        let a = Admin::create_engine(
            common::fresh("maintenance-review"),
            Limits::default(),
            engine,
        )
        .unwrap();
        a.roles()
            .create(NewRole {
                name: "ed".into(),
                ..Default::default()
            })
            .unwrap();
        a.roles()
            .create(NewRole {
                name: "dan".into(),
                ..Default::default()
            })
            .unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE t (id int PRIMARY KEY)");
        run(&mut s, "CREATE SCHEMA m");
        run(&mut s, "CREATE TABLE m.t (id int)");
        s.set_search_path("m", "giggles");
        run(&mut s, "VACUUM");
        run(&mut s, "ANALYZE");
        s.set_search_path("m, public", "giggles");
        s.set_ddl_owner(Some("dan"));
        for verb in ["VACUUM", "ANALYZE"] {
            take_sql_notices();
            run(&mut s, verb);
            let notes = take_sql_notices();
            assert_eq!(notes.len(), 2, "{verb}: {notes:?}");
            for (message, _, code) in notes {
                assert_eq!(
                    message,
                    format!(
                        "permission denied to {} \"t\", skipping it",
                        verb.to_ascii_lowercase()
                    )
                );
                assert_eq!(code, Some("01000"));
            }
        }
        for sql in ["REINDEX DATABASE", "REINDEX SYSTEM"] {
            fail(&mut s, sql, "42501", "must be owner of database postgres");
        }
        for verb in ["VACUUM", "ANALYZE"] {
            take_sql_notices();
            run(&mut s, &format!("{verb} pg_class"));
            assert_eq!(
                take_sql_notices(),
                vec![(
                    format!(
                        "permission denied to {} \"pg_class\", skipping it",
                        verb.to_ascii_lowercase()
                    ),
                    None,
                    Some("01000")
                )]
            );
        }
        s.set_ddl_owner(None);
        s.set_search_path("public", "giggles");
        a.roles()
            .grant_table(
                "t",
                "dan",
                &[
                    giggles_core::roles::TablePrivilege::Select,
                    giggles_core::roles::TablePrivilege::Insert,
                    giggles_core::roles::TablePrivilege::Update,
                    giggles_core::roles::TablePrivilege::Delete,
                ],
            )
            .unwrap();
        s.set_ddl_owner(Some("dan"));
        take_sql_notices();
        run(&mut s, "ANALYZE t");
        assert_eq!(take_sql_notices().len(), 1);
        for sql in [
            "BEGIN",
            "CREATE TABLE dt (i int)",
            "ANALYZE dt",
            "CREATE INDEX dti ON dt(i)",
            "REINDEX TABLE dt",
            "CLUSTER dt USING dti",
            "COMMIT",
        ] {
            take_sql_notices();
            run(&mut s, sql);
            assert!(take_sql_notices().is_empty(), "{engine:?}: {sql}");
        }
        s.set_ddl_owner(None);
        if engine == Engine::V2 {
            a.roles().grant_membership("pg_maintain", "ed").unwrap();
            s.set_ddl_owner(Some("ed"));
            for sql in [
                "VACUUM t",
                "ANALYZE t",
                "REINDEX TABLE t",
                "CLUSTER t USING t_pkey",
                "REINDEX DATABASE",
            ] {
                take_sql_notices();
                run(&mut s, sql);
                assert!(take_sql_notices().is_empty(), "{sql}");
            }
        }
        s.set_ddl_owner(None);
        if engine == Engine::V2 {
            run(&mut s, "CREATE VIEW rv AS SELECT id FROM t");
            s.set_ddl_owner(Some("dan"));
            for verb in ["VACUUM", "ANALYZE"] {
                take_sql_notices();
                run(&mut s, &format!("{verb} rv"));
                assert_eq!(
                    take_sql_notices(),
                    vec![(
                        format!(
                            "permission denied to {} \"rv\", skipping it",
                            verb.to_ascii_lowercase()
                        ),
                        None,
                        Some("01000")
                    )]
                );
            }
            s.set_ddl_owner(None);
        }
        for verb in ["VACUUM", "ANALYZE", "REINDEX TABLE", "CLUSTER"] {
            fail(
                &mut s,
                &format!("{verb} nosuch.r"),
                "3F000",
                "schema \"nosuch\" does not exist",
            );
        }
        for v in ["true", "false", "on", "off", "'TRUE'", "'Off'", "0", "1"] {
            run(&mut s, &format!("VACUUM (FULL {v}) t"));
        }
        for v in ["yes", "no", "2", "'1'"] {
            fail(
                &mut s,
                &format!("VACUUM (FULL {v}) t"),
                "42601",
                "full requires a Boolean value",
            );
        }
        for (sql, code, msg) in [
            (
                "VACUUM (ONLY_DATABASE_STATS, FREEZE)",
                "0A000",
                "ONLY_DATABASE_STATS cannot be specified with other VACUUM options",
            ),
            (
                "VACUUM (FULL, PROCESS_TOAST false) t",
                "0A000",
                "PROCESS_TOAST required with VACUUM FULL",
            ),
            (
                "VACUUM (FULL, BUFFER_USAGE_LIMIT '256kB') t",
                "0A000",
                "BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL",
            ),
            (
                "VACUUM (PARALLEL abc) t",
                "42601",
                "parallel requires an integer value",
            ),
            (
                "VACUUM (BUFFER_USAGE_LIMIT) t",
                "42601",
                "buffer_usage_limit requires a parameter",
            ),
            (
                "VACUUM (BUFFER_USAGE_LIMIT '256KB') t",
                "22023",
                "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
            ),
        ] {
            fail(&mut s, sql, code, msg);
        }
        assert_eq!(
            giggles_core::relational::take_sql_error_hint(),
            Some("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".")
        );
        run(&mut s, "VACUUM (BUFFER_USAGE_LIMIT 0) t");
        run(&mut s, "VACUUM (BUFFER_USAGE_LIMIT 256) t");
    });
}
