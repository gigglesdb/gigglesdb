mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{
        Value, take_sql_error_detail, take_sql_error_hint, take_sql_error_message, take_sql_notices,
    },
    roles::NewRole,
    sqlstate,
};

fn each_engine(mut test: impl FnMut(Engine)) {
    test(Engine::Legacy);
    #[cfg(feature = "engine-rocksdb")]
    test(Engine::V2);
}

fn failure(session: &mut SqlSession, sql: &str, state: &str, message: &str) {
    let error = session.execute(sql, &[], None).unwrap_err();
    let actual_message = take_sql_error_message();
    assert_eq!(sqlstate(&error), state, "{sql}: {error:?} {actual_message:?}");
    assert_eq!(actual_message.as_deref(), Some(message), "{sql}");
}

#[test]
fn comment_and_label_errors_resolve_objects_in_order() {
    each_engine(|engine| {
        let admin =
            Admin::create_engine(common::fresh("admin-comments"), Limits::default(), engine)
                .unwrap();
        for role in ["root", "plain"] {
            admin
                .roles()
                .create(NewRole {
                    name: role.into(),
                    superuser: role == "root",
                    ..Default::default()
                })
                .unwrap();
        }
        let mut session = admin.sql_session();
        session
            .execute("CREATE TABLE admin_row (i int)", &[], None)
            .unwrap();
        for role in ["root", "plain"] {
            session.set_ddl_owner(Some(role));
            for (sql, expected) in [
                (
                    "COMMENT ON TABLESPACE missing IS 'x'",
                    "tablespace \"missing\" does not exist",
                ),
                (
                    "COMMENT ON EXTENSION \"Missing\" IS NULL",
                    "extension \"Missing\" does not exist",
                ),
                (
                    "COMMENT ON LANGUAGE missing IS 'x'",
                    "language \"missing\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR missing LANGUAGE missing IS 'x'",
                    "type \"missing\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR int LANGUAGE missing IS 'x'",
                    "language \"missing\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR int LANGUAGE sql IS 'x'",
                    "transform for type integer language \"sql\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR admin_row LANGUAGE sql IS 'x'",
                    "transform for type admin_row language \"sql\" does not exist",
                ),
            ] {
                failure(&mut session, sql, "42704", expected);
            }
            for (sql, expected) in [
                (
                    "SECURITY LABEL ON TABLE missing IS NULL",
                    "no security label providers have been loaded",
                ),
                (
                    "SECURITY LABEL FOR selinux ON TABLE missing IS 'x'",
                    "security label provider \"selinux\" is not loaded",
                ),
                (
                    "SECURITY LABEL FOR other ON COLUMN admin_row.i IS 'x'",
                    "security label provider \"other\" is not loaded",
                ),
                (
                    "SECURITY LABEL ON FOREIGN DATA WRAPPER missing IS 'x'",
                    "no security label providers have been loaded",
                ),
            ] {
                failure(&mut session, sql, "22023", expected);
            }
        }
        for (sql, message) in [
            (
                "SECURITY LABEL ON BOGUS admin_row IS 'x'",
                "syntax error at or near \"BOGUS\"",
            ),
            (
                "SECURITY LABEL ON TABLE admin_row IS 'x' extra",
                "syntax error at or near \"extra\"",
            ),
        ] {
            failure(&mut session, sql, "42601", message);
        }
        failure(
            &mut session,
            "COMMENT ON LANGUAGE plpgsql IS 'x'",
            "42501",
            "must be owner of language plpgsql",
        );
        assert_eq!(
            sqlstate(
                &session
                    .execute(
                        "SECURITY LABEL FOR giggles ON TABLE missing IS NULL",
                        &[],
                        None
                    )
                    .unwrap_err()
            ),
            "0A000"
        );
    });
}

#[test]
fn tablespace_and_load_validate_without_mutating() {
    each_engine(|engine| {
        let admin = Admin::create_engine(
            common::fresh("admin-tablespaces"),
            Limits::default(),
            engine,
        )
        .unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "root".into(),
                superuser: true,
                ..Default::default()
            })
            .unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "plain".into(),
                ..Default::default()
            })
            .unwrap();
        let mut session = admin.sql_session();
        session.set_ddl_owner(Some("root"));
        for (sql, state, message) in [
            (
                "CREATE TABLESPACE s OWNER missing LOCATION 'rel'",
                "42704",
                "role \"missing\" does not exist",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/a''b'",
                "42602",
                "tablespace location cannot contain single quotes",
            ),
            (
                "CREATE TABLESPACE pg_bad LOCATION ''",
                "42P17",
                "tablespace location must be an absolute path",
            ),
            (
                "CREATE TABLESPACE pg_bad LOCATION '/abs' WITH (bogus=1)",
                "42939",
                "unacceptable tablespace name \"pg_bad\"",
            ),
        ] {
            failure(&mut session, sql, state, message);
        }
        assert_eq!(
            take_sql_error_detail().as_deref(),
            Some("The prefix \"pg_\" is reserved for system tablespaces.")
        );
        for (sql, state, message) in [
            (
                "DROP TABLESPACE pg_default",
                "42501",
                "permission denied for tablespace pg_default",
            ),
            (
                "ALTER TABLESPACE missing OWNER TO missing",
                "42704",
                "role \"missing\" does not exist",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/x' WITH (seq_page_cost=1, seq_page_cost=-1)",
                "22023",
                "parameter \"seq_page_cost\" specified more than once",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/x' WITH (foo.bar=1)",
                "22023",
                "unrecognized parameter namespace \"foo\"",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/x' WITH (seq_page_cost='1e400')",
                "22023",
                "invalid value for floating point option \"seq_page_cost\": 1e400",
            ),
            (
                "ALTER TABLESPACE pg_default RESET (seq_page_cost=1)",
                "42601",
                "RESET must not include values for parameters",
            ),
        ] {
            failure(&mut session, sql, state, message);
        }
        for value in ["1.5", "'010'", "'0x10'", "'1000.5'"] {
            failure(
                &mut session,
                &format!(
                    "CREATE TABLESPACE s LOCATION '/x' WITH (effective_io_concurrency={value}, seq_page_cost='0x10')"
                ),
                "58P01",
                "directory \"/x\" does not exist",
            );
        }
        failure(
            &mut session,
            "CREATE TABLESPACE s LOCATION '/x' WITH (seq_page_cost='0x0.1p-1022')",
            "58P01",
            "directory \"/x\" does not exist",
        );
        for value in ["09", "0x1p2"] {
            failure(
                &mut session,
                &format!("CREATE TABLESPACE s LOCATION '/x' WITH (effective_io_concurrency='{value}')"),
                "22023",
                &format!("invalid value for integer option \"effective_io_concurrency\": {value}"),
            );
        }
        let location = format!("/{}", "\u{e9}".repeat(512));
        failure(
            &mut session,
            &format!("CREATE TABLESPACE s LOCATION '{location}'"),
            "42P17",
            &format!("tablespace location \"{location}\" is too long"),
        );
        for sql in [
            "ALTER TABLESPACE pg_default OWNER TO root",
            "ALTER TABLESPACE pg_global OWNER TO CURRENT_USER",
            "LOAD 'plpgsql'",
            "LOAD '$libdir/plpgsql'",
        ] {
            session.execute(sql, &[], None).unwrap();
        }
        failure(
            &mut session,
            "CREATE TABLESPACE s LOCATION '/a//b/./c/'",
            "58P01",
            "directory \"/a/b/c\" does not exist",
        );
        failure(
            &mut session,
            "ALTER TABLESPACE pg_global SET (maintenance_io_concurrency=1001)",
            "22023",
            "value 1001 out of bounds for option \"maintenance_io_concurrency\"",
        );
        assert_eq!(
            take_sql_error_detail().as_deref(),
            Some("Valid values are between \"0\" and \"1000\".")
        );
        session
            .execute("DROP TABLESPACE IF EXISTS s", &[], None)
            .unwrap();
        assert_eq!(
            take_sql_notices(),
            [(
                "tablespace \"s\" does not exist, skipping".into(),
                None,
                Some("00000")
            )]
        );
        session.execute("BEGIN", &[], None).unwrap();
        failure(
            &mut session,
            "DROP TABLESPACE IF EXISTS s",
            "25001",
            "DROP TABLESPACE cannot run inside a transaction block",
        );
        assert!(session.is_aborted());
        session.execute("ROLLBACK", &[], None).unwrap();
        failure(
            &mut session,
            "LOAD ''",
            "58P01",
            "could not access file \"\": No such file or directory",
        );
        session.set_ddl_owner(Some("plain"));
        failure(
            &mut session,
            "LOAD '$libdir/plugins/a/b'",
            "42501",
            "access to library \"$libdir/plugins/a/b\" is not allowed",
        );
        failure(
            &mut session,
            "LOAD '$libdir/plugins/'",
            "58P01",
            "could not access file \"$libdir/plugins/\": No such file or directory",
        );
        failure(
            &mut session,
            "CREATE TABLESPACE s OWNER missing LOCATION 'rel'",
            "42501",
            "permission denied to create tablespace \"s\"",
        );
        assert_eq!(
            take_sql_error_hint(),
            Some("Must be superuser to create a tablespace.")
        );
    });
}

#[test]
fn export_snapshot_works_until_a_savepoint_is_open() {
    each_engine(|engine| {
        let admin =
            Admin::create_engine(common::fresh("admin-snapshot"), Limits::default(), engine)
                .unwrap();
        let mut session = admin.sql_session();
        let check_export = |session: &mut SqlSession| {
            let result = session
                .execute("SELECT pg_catalog.pg_export_snapshot()", &[], None)
                .unwrap();
            let [Value::Text(id)] = result.rows[0].values.as_slice() else {
                panic!("{result:?}");
            };
            let parts: Vec<_> = id.split('-').collect();
            let [high, low, ordinal] = parts.as_slice() else {
                panic!("{id}");
            };
            for part in [high, low] {
                assert_eq!(part.len(), 8, "{id}");
                assert!(
                    part.bytes()
                        .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b)),
                    "{id}"
                );
            }
            (format!("{high}-{low}"), ordinal.parse::<u32>().unwrap())
        };
        assert_eq!(check_export(&mut session).1, 1);
        assert!(!session.has_exported_snapshots());
        session.execute("BEGIN", &[], None).unwrap();
        assert!(!session.has_exported_snapshots());
        let (prefix, ordinal) = check_export(&mut session);
        assert_eq!(ordinal, 1);
        assert_eq!(check_export(&mut session), (prefix.clone(), 2));
        assert!(session.has_exported_snapshots());
        session.execute("SAVEPOINT s", &[], None).unwrap();
        failure(
            &mut session,
            "SELECT pg_export_snapshot()",
            "25001",
            "cannot export a snapshot from a subtransaction",
        );
        assert!(session.is_aborted());
        session.execute("ROLLBACK TO s", &[], None).unwrap();
        session.execute("RELEASE s", &[], None).unwrap();
        assert_eq!(check_export(&mut session), (prefix, 3));
        session.execute("ROLLBACK", &[], None).unwrap();
        assert!(!session.has_exported_snapshots());
        assert_eq!(check_export(&mut session).1, 1);
    });
}
