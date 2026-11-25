use super::session_settings_network::{Fixture, wire_cycle, wire_query};
use giggles_core::Engine;
use giggles_server::pgwire::{frame, z};
use tokio::{io::AsyncWriteExt, net::TcpStream};

type Message = (u8, Vec<u8>);

async fn fixture() -> Fixture {
    Fixture::with_engine(if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    })
    .await
}

async fn connect(fixture: &Fixture, role: &str) -> TcpStream {
    let mut stream = TcpStream::connect(fixture.pg.address).await.unwrap();
    let mut body = 196608u32.to_be_bytes().to_vec();
    z("user", &mut body);
    z(role, &mut body);
    z("database", &mut body);
    z("postgres", &mut body);
    body.push(0);
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(&body).await.unwrap();
    assert_eq!(ready(&wire_cycle(&mut stream).await), b'I');
    stream
}

async fn query(stream: &mut TcpStream, sql: &str, extended: bool) -> Vec<Message> {
    if !extended {
        return wire_query(stream, sql).await;
    }
    pipeline(stream, &[sql]).await
}

async fn pipeline(stream: &mut TcpStream, statements: &[&str]) -> Vec<Message> {
    let mut packet = Vec::new();
    for sql in statements {
        let mut parse = vec![0];
        z(sql, &mut parse);
        parse.extend(0u16.to_be_bytes());
        frame(b'P', &parse, &mut packet).unwrap();
        frame(b'B', &[0; 8], &mut packet).unwrap();
        frame(b'E', &[0; 5], &mut packet).unwrap();
    }
    frame(b'S', &[], &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    wire_cycle(stream).await
}

fn body(messages: &[Message], tag: u8) -> &[u8] {
    &messages
        .iter()
        .find(|m| m.0 == tag)
        .unwrap_or_else(|| panic!("missing {} in {messages:?}", tag as char))
        .1
}

fn field(mut body: &[u8], field: u8) -> Option<&str> {
    while let Some((&key, rest)) = body.split_first() {
        if key == 0 {
            return None;
        }
        let end = rest.iter().position(|&byte| byte == 0).unwrap();
        if key == field {
            return Some(std::str::from_utf8(&rest[..end]).unwrap());
        }
        body = &rest[end + 1..];
    }
    None
}

fn ready(messages: &[Message]) -> u8 {
    body(messages, b'Z')[0]
}

fn completed(messages: &[Message], tag: &str, status: u8) {
    assert!(!messages.iter().any(|m| m.0 == b'E'), "{messages:?}");
    assert_eq!(
        body(messages, b'C').strip_suffix(&[0]).unwrap(),
        tag.as_bytes()
    );
    assert_eq!(ready(messages), status);
}

fn error(
    messages: &[Message],
    state: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    status: u8,
) {
    let error = body(messages, b'E');
    assert_eq!(field(error, b'C'), Some(state), "{message}: {error:?}");
    assert_eq!(field(error, b'M'), Some(message));
    assert_eq!(field(error, b'D'), detail);
    assert_eq!(field(error, b'H'), hint);
    assert_eq!(ready(messages), status);
}

fn notice(messages: &[Message], severity: &str, state: &str, message: &str) {
    let notice = body(messages, b'N');
    assert_eq!(field(notice, b'S'), Some(severity));
    assert_eq!(field(notice, b'C'), Some(state));
    assert_eq!(field(notice, b'M'), Some(message));
    assert!(messages.iter().take_while(|m| m.0 != b'N').all(|m| m.0 != b'E'));
}

fn first_text(messages: &[Message]) -> &str {
    let row = body(messages, b'D');
    assert_eq!(u16::from_be_bytes(row[..2].try_into().unwrap()), 1);
    let len = u32::from_be_bytes(row[2..6].try_into().unwrap()) as usize;
    std::str::from_utf8(&row[6..6 + len]).unwrap()
}

#[tokio::test]
async fn comments_labels_tablespaces_and_load_use_catalog_and_role_order() {
    let fixture = fixture().await;
    let mut root = connect(&fixture, "owner").await;
    completed(
        &wire_query(&mut root, "CREATE ROLE plain LOGIN").await,
        "CREATE ROLE",
        b'I',
    );
    let mut plain = connect(&fixture, "plain").await;
    for extended in [false, true] {
        for conn in [&mut root, &mut plain] {
            for (sql, state, message) in [
                (
                    "COMMENT ON TABLESPACE nosuch IS 'x'",
                    "42704",
                    "tablespace \"nosuch\" does not exist",
                ),
                (
                    "COMMENT ON EXTENSION nosuch IS 'x'",
                    "42704",
                    "extension \"nosuch\" does not exist",
                ),
                (
                    "COMMENT ON LANGUAGE nosuch IS 'x'",
                    "42704",
                    "language \"nosuch\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR nosuchtype LANGUAGE sql IS 'x'",
                    "42704",
                    "type \"nosuchtype\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR int LANGUAGE nosuch IS 'x'",
                    "42704",
                    "language \"nosuch\" does not exist",
                ),
                (
                    "COMMENT ON TRANSFORM FOR int LANGUAGE sql IS 'x'",
                    "42704",
                    "transform for type integer language \"sql\" does not exist",
                ),
                (
                    "SECURITY LABEL ON TABLE nosuch IS 'x'",
                    "22023",
                    "no security label providers have been loaded",
                ),
                (
                    "SECURITY LABEL FOR selinux ON TABLE nosuch IS 'x'",
                    "22023",
                    "security label provider \"selinux\" is not loaded",
                ),
                (
                    "ALTER TABLESPACE nosuch RENAME TO pg_bad",
                    "42704",
                    "tablespace \"nosuch\" does not exist",
                ),
                (
                    "ALTER TABLESPACE nosuch OWNER TO nosuchrole",
                    "42704",
                    "role \"nosuchrole\" does not exist",
                ),
                (
                    "ALTER TABLESPACE pg_default OWNER TO nosuchrole",
                    "42704",
                    "role \"nosuchrole\" does not exist",
                ),
                (
                    "ALTER TABLESPACE nosuch OWNER TO owner",
                    "42704",
                    "tablespace \"nosuch\" does not exist",
                ),
                (
                    "ALTER TABLESPACE nosuch SET (bogus=1)",
                    "42704",
                    "tablespace \"nosuch\" does not exist",
                ),
                (
                    "ALTER TABLESPACE nosuch RESET (bogus)",
                    "42704",
                    "tablespace \"nosuch\" does not exist",
                ),
                (
                    "DROP TABLESPACE nosuch",
                    "42704",
                    "tablespace \"nosuch\" does not exist",
                ),
            ] {
                error(
                    &query(conn, sql, extended).await,
                    state,
                    message,
                    None,
                    None,
                    b'I',
                );
            }
            let dropped = query(conn, "DROP TABLESPACE IF EXISTS nosuch", extended).await;
            notice(
                &dropped,
                "NOTICE",
                "00000",
                "tablespace \"nosuch\" does not exist, skipping",
            );
            completed(&dropped, "DROP TABLESPACE", b'I');
            let row = query(
                conn,
                "SELECT count(*) FROM pg_tablespace WHERE spcname='nosuch'",
                extended,
            )
            .await;
            assert_eq!(first_text(&row), "0");
            for sql in [
                "CREATE TABLESPACE pg_bad OWNER nosuchrole LOCATION 'rel'",
                "DROP TABLESPACE IF EXISTS nosuch",
            ] {
                completed(&query(conn, "BEGIN", extended).await, "BEGIN", b'T');
                let command = if sql.starts_with("CREATE") {
                    "CREATE TABLESPACE"
                } else {
                    "DROP TABLESPACE"
                };
                error(
                    &query(conn, sql, extended).await,
                    "25001",
                    &format!("{command} cannot run inside a transaction block"),
                    None,
                    None,
                    b'E',
                );
                completed(&query(conn, "ROLLBACK", extended).await, "ROLLBACK", b'I');
            }
        }
        for (sql, message) in [
            (
                "COMMENT ON TABLESPACE pg_default IS 'x'",
                "must be owner of tablespace pg_default",
            ),
            (
                "COMMENT ON EXTENSION plpgsql IS 'x'",
                "must be owner of extension plpgsql",
            ),
            (
                "COMMENT ON LANGUAGE plpgsql IS 'x'",
                "must be owner of language plpgsql",
            ),
            (
                "ALTER TABLESPACE pg_default RENAME TO other",
                "permission denied for tablespace pg_default",
            ),
            (
                "ALTER TABLESPACE pg_default OWNER TO plain",
                "must be owner of tablespace pg_default",
            ),
            (
                "ALTER TABLESPACE pg_default SET (seq_page_cost=2)",
                "must be owner of tablespace pg_default",
            ),
            (
                "ALTER TABLESPACE pg_default RESET (seq_page_cost)",
                "must be owner of tablespace pg_default",
            ),
            (
                "DROP TABLESPACE pg_default",
                "must be owner of tablespace pg_default",
            ),
        ] {
            error(
                &query(&mut plain, sql, extended).await,
                "42501",
                message,
                None,
                None,
                b'I',
            );
        }
        error(
            &query(
                &mut plain,
                "CREATE TABLESPACE s OWNER nosuchrole LOCATION 'rel'",
                extended,
            )
            .await,
            "42501",
            "permission denied to create tablespace \"s\"",
            None,
            Some("Must be superuser to create a tablespace."),
            b'I',
        );
        for (sql, state, message) in [
            (
                "CREATE TABLESPACE s OWNER nosuchrole LOCATION 'rel'",
                "42704",
                "role \"nosuchrole\" does not exist",
            ),
            (
                "CREATE TABLESPACE s OWNER owner LOCATION '/a//b/./c/'",
                "58P01",
                "directory \"/a/b/c\" does not exist",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/a''b'",
                "42602",
                "tablespace location cannot contain single quotes",
            ),
            (
                "CREATE TABLESPACE pg_bad LOCATION 'relative/path'",
                "42P17",
                "tablespace location must be an absolute path",
            ),
            (
                "CREATE TABLESPACE s LOCATION ''",
                "42P17",
                "tablespace location must be an absolute path",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/x' WITH (bogus=1)",
                "22023",
                "unrecognized parameter \"bogus\"",
            ),
            (
                "CREATE TABLESPACE s LOCATION '/x' WITH (seq_page_cost=0, random_page_cost=2.5, effective_io_concurrency=1000, maintenance_io_concurrency=0)",
                "58P01",
                "directory \"/x\" does not exist",
            ),
            (
                "ALTER TABLESPACE pg_global SET (bogus=1)",
                "22023",
                "unrecognized parameter \"bogus\"",
            ),
            (
                "DROP TABLESPACE pg_default",
                "42501",
                "permission denied for tablespace pg_default",
            ),
            (
                "DROP TABLESPACE pg_global",
                "42501",
                "permission denied for tablespace pg_global",
            ),
        ] {
            error(
                &query(&mut root, sql, extended).await,
                state,
                message,
                None,
                None,
                b'I',
            );
        }
        for sql in [
            "CREATE TABLESPACE pg_bad LOCATION '/x' WITH (bogus=1)",
            "ALTER TABLESPACE pg_global RENAME TO pg_bad",
        ] {
            error(
                &query(&mut root, sql, extended).await,
                "42939",
                "unacceptable tablespace name \"pg_bad\"",
                Some("The prefix \"pg_\" is reserved for system tablespaces."),
                None,
                b'I',
            );
        }
        let max = "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000";
        error(
            &query(
                &mut root,
                "CREATE TABLESPACE s LOCATION '/x' WITH (seq_page_cost=-1)",
                extended,
            )
            .await,
            "22023",
            "value -1 out of bounds for option \"seq_page_cost\"",
            Some(&format!(
                "Valid values are between \"0.000000\" and \"{max}\"."
            )),
            None,
            b'I',
        );
        for sql in [
            "COMMENT ON TABLESPACE pg_global IS 'x'",
            "COMMENT ON EXTENSION plpgsql IS 'x'",
            "COMMENT ON LANGUAGE plpgsql IS 'x'",
            "SECURITY LABEL FOR giggles ON TABLE nosuch IS 'x'",
            "ALTER TABLESPACE pg_global RENAME TO s",
            "ALTER TABLESPACE pg_global OWNER TO plain",
            "ALTER TABLESPACE pg_global SET (seq_page_cost=2)",
            "ALTER TABLESPACE pg_global RESET (seq_page_cost)",
        ] {
            let reply = query(&mut root, sql, extended).await;
            assert_eq!(field(body(&reply, b'E'), b'C'), Some("0A000"), "{sql}");
            assert_eq!(ready(&reply), b'I');
        }
        for filename in ["", "missing.so", "/abs/missing.so", "$libdir/plugins/x"] {
            error(
                &query(&mut root, &format!("LOAD '{filename}'"), extended).await,
                "58P01",
                &format!("could not access file \"{filename}\": No such file or directory"),
                None,
                None,
                b'I',
            );
        }
        for conn in [&mut root, &mut plain] {
            completed(
                &query(conn, "ALTER TABLESPACE pg_global OWNER TO owner", extended).await,
                "ALTER TABLESPACE",
                b'I',
            );
        }
        for filename in ["plpgsql", "$libdir/plpgsql"] {
            completed(
                &query(&mut root, &format!("LOAD '{filename}'"), extended).await,
                "LOAD",
                b'I',
            );
        }
        for filename in ["", "missing.so", "$libdir/plugins/a/b", "plpgsql", "$libdir/plpgsql"] {
            error(
                &query(&mut plain, &format!("LOAD '{filename}'"), extended).await,
                "42501",
                &format!("access to library \"{filename}\" is not allowed"),
                None,
                None,
                b'I',
            );
        }
        for filename in ["$libdir/plugins/", "$libdir/plugins/x"] {
            error(
                &query(&mut plain, &format!("LOAD '{filename}'"), extended).await,
                "58P01",
                &format!("could not access file \"{filename}\": No such file or directory"),
                None,
                None,
                b'I',
            );
        }
        completed(&query(&mut root, "BEGIN", extended).await, "BEGIN", b'T');
        error(
            &query(&mut root, "LOAD 'inside'", extended).await,
            "58P01",
            "could not access file \"inside\": No such file or directory",
            None,
            None,
            b'E',
        );
        completed(
            &query(&mut root, "ROLLBACK", extended).await,
            "ROLLBACK",
            b'I',
        );
    }
    drop((root, plain));
    fixture.stop().await;
}

#[tokio::test]
async fn two_phase_and_snapshot_commands_report_transaction_state() {
    let fixture = fixture().await;
    let mut conn = connect(&fixture, "owner").await;
    completed(
        &wire_query(
            &mut conn,
            "CREATE TABLE prepare_rollback (id integer PRIMARY KEY)",
        )
        .await,
        "CREATE TABLE",
        b'I',
    );
    const IMPORT: &str = "SET TRANSACTION SNAPSHOT '00000000-00000000-0'";
    const ISOLATION: &str = "a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ";
    const BEFORE_QUERY: &str = "SET TRANSACTION SNAPSHOT must be called before any query";
    const DISABLED: &str = "prepared transactions are disabled";
    const HINT: &str = "Set \"max_prepared_transactions\" to a nonzero value.";
    for extended in [false, true] {
        assert_eq!(
            first_text(&query(&mut conn, "SHOW max_prepared_transactions", extended).await),
            "0"
        );
        let outside = query(&mut conn, "PREPARE TRANSACTION 'gid'", extended).await;
        notice(
            &outside,
            "WARNING",
            "25P01",
            "there is no transaction in progress",
        );
        completed(&outside, "ROLLBACK", b'I');
        completed(&query(&mut conn, "BEGIN", extended).await, "BEGIN", b'T');
        completed(
            &query(&mut conn, "SAVEPOINT s", extended).await,
            "SAVEPOINT",
            b'T',
        );
        completed(
            &query(
                &mut conn,
                "INSERT INTO prepare_rollback VALUES (1)",
                extended,
            )
            .await,
            "INSERT 0 1",
            b'T',
        );
        error(
            &query(&mut conn, "PREPARE TRANSACTION 'gid'", extended).await,
            "55000",
            DISABLED,
            None,
            Some(HINT),
            b'I',
        );
        let after = query(&mut conn, "SELECT 'after prepare'", extended).await;
        completed(&after, "SELECT 1", b'I');
        assert_eq!(first_text(&after), "after prepare");
        let rolled_back = query(&mut conn, "SELECT count(*) FROM prepare_rollback", extended).await;
        assert_eq!(first_text(&rolled_back), "0");
        let gid = "\u{e9}".repeat(100);
        completed(&query(&mut conn, "BEGIN", extended).await, "BEGIN", b'T');
        error(
            &query(&mut conn, &format!("PREPARE TRANSACTION '{gid}'"), extended).await,
            "22023",
            &format!("transaction identifier \"{gid}\" is too long"),
            None,
            None,
            b'I',
        );
        for (sql, message) in [
            (
                "COMMIT PREPARED 'gid'",
                "COMMIT PREPARED cannot run inside a transaction block",
            ),
            (
                "ROLLBACK PREPARED 'gid'",
                "ROLLBACK PREPARED cannot run inside a transaction block",
            ),
        ] {
            error(
                &query(&mut conn, sql, extended).await,
                "42704",
                "prepared transaction with identifier \"gid\" does not exist",
                None,
                None,
                b'I',
            );
            completed(&query(&mut conn, "BEGIN", extended).await, "BEGIN", b'T');
            error(
                &query(&mut conn, sql, extended).await,
                "25001",
                message,
                None,
                None,
                b'E',
            );
            let failed = query(&mut conn, sql, extended).await;
            assert_eq!(field(body(&failed, b'E'), b'C'), Some("25P02"));
            assert_eq!(ready(&failed), b'E');
            completed(
                &query(&mut conn, "PREPARE TRANSACTION 'gid'", extended).await,
                "ROLLBACK",
                b'I',
            );
        }

        let outside = query(&mut conn, IMPORT, extended).await;
        notice(
            &outside,
            "WARNING",
            "25P01",
            "SET TRANSACTION can only be used in transaction blocks",
        );
        error(&outside, "0A000", ISOLATION, None, None, b'I');
        for (begin, before, sql, state, message) in [
            ("BEGIN", None, IMPORT, "0A000", ISOLATION),
            (
                "BEGIN",
                Some(("SELECT 1", "SELECT 1")),
                IMPORT,
                "25001",
                BEFORE_QUERY,
            ),
            (
                "BEGIN ISOLATION LEVEL REPEATABLE READ",
                Some(("SAVEPOINT s", "SAVEPOINT")),
                IMPORT,
                "25001",
                BEFORE_QUERY,
            ),
            (
                "BEGIN ISOLATION LEVEL REPEATABLE READ",
                None,
                IMPORT,
                "42704",
                "snapshot \"00000000-00000000-0\" does not exist",
            ),
            (
                "BEGIN ISOLATION LEVEL SERIALIZABLE",
                None,
                "SET TRANSACTION SNAPSHOT '0000000a-00000000-0'",
                "22023",
                "invalid snapshot identifier: \"0000000a-00000000-0\"",
            ),
            (
                "BEGIN ISOLATION LEVEL REPEATABLE READ",
                None,
                "SET TRANSACTION SNAPSHOT ''",
                "XX000",
                "could not read file \"pg_snapshots/\": Is a directory",
            ),
        ] {
            completed(&query(&mut conn, begin, extended).await, "BEGIN", b'T');
            if let Some((sql, tag)) = before {
                completed(&query(&mut conn, sql, extended).await, tag, b'T');
            }
            error(
                &query(&mut conn, sql, extended).await,
                state,
                message,
                None,
                None,
                b'E',
            );
            completed(
                &query(&mut conn, "ROLLBACK", extended).await,
                "ROLLBACK",
                b'I',
            );
        }
        let invalid = query(&mut conn, "SET TRANSACTION SNAPSHOT 123", extended).await;
        assert_eq!(field(body(&invalid, b'E'), b'C'), Some("42601"));
        assert_eq!(ready(&invalid), b'I');

        for begin in [None, Some("BEGIN"), Some("BEGIN READ ONLY")] {
            if let Some(begin) = begin {
                completed(&query(&mut conn, begin, extended).await, "BEGIN", b'T');
            }
            let exported = query(&mut conn, "SELECT pg_export_snapshot()", extended).await;
            completed(
                &exported,
                "SELECT 1",
                if begin.is_some() { b'T' } else { b'I' },
            );
            let id = first_text(&exported);
            assert_eq!(id.split('-').map(str::len).collect::<Vec<_>>(), [8, 8, 1]);
            if begin.is_some() {
                completed(
                    &query(&mut conn, "SAVEPOINT s", extended).await,
                    "SAVEPOINT",
                    b'T',
                );
                error(
                    &query(&mut conn, "SELECT pg_export_snapshot()", extended).await,
                    "25001",
                    "cannot export a snapshot from a subtransaction",
                    None,
                    None,
                    b'E',
                );
                completed(
                    &query(&mut conn, "ROLLBACK", extended).await,
                    "ROLLBACK",
                    b'I',
                );
            }
        }
    }
    drop(conn);
    fixture.stop().await;
}

#[tokio::test]
async fn implicit_block_two_phase_errors_stop_and_rollback_batches() {
    let fixture = fixture().await;
    let mut conn = connect(&fixture, "owner").await;
    completed(
        &wire_query(
            &mut conn,
            "CREATE TABLE twophase_batch (id integer PRIMARY KEY)",
        )
        .await,
        "CREATE TABLE",
        b'I',
    );
    let insert = "INSERT INTO twophase_batch VALUES (1)";
    let prepare = "PREPARE TRANSACTION 'gid'";
    let unexpected = "SELECT 99";
    for (batch, preceding_insert) in [
        (
            wire_query(&mut conn, &format!("{insert}; {prepare}; {unexpected}")).await,
            true,
        ),
        (
            wire_query(&mut conn, &format!("{prepare}; {unexpected}")).await,
            false,
        ),
        (
            pipeline(&mut conn, &[insert, prepare, unexpected]).await,
            true,
        ),
    ] {
        notice(
            &batch,
            "WARNING",
            "25P01",
            "there is no transaction in progress",
        );
        error(
            &batch,
            "55000",
            "prepared transactions are disabled",
            None,
            Some("Set \"max_prepared_transactions\" to a nonzero value."),
            b'I',
        );
        let tags: Vec<_> = batch
            .iter()
            .filter(|m| m.0 == b'C')
            .map(|m| m.1.as_slice())
            .collect();
        assert_eq!(
            tags,
            if preceding_insert {
                vec![b"INSERT 0 1\0".as_slice()]
            } else {
                vec![]
            }
        );
        assert!(!batch.iter().any(|m| m.0 == b'D'));
        assert_eq!(
            first_text(&wire_query(&mut conn, "SELECT count(*) FROM twophase_batch").await),
            "0"
        );
    }

    let gid = "g".repeat(200);
    let prepare = format!("PREPARE TRANSACTION '{gid}'");
    for batch in [
        wire_query(&mut conn, &format!("{prepare}; {unexpected}")).await,
        pipeline(&mut conn, &[insert, &prepare, unexpected]).await,
    ] {
        notice(
            &batch,
            "WARNING",
            "25P01",
            "there is no transaction in progress",
        );
        error(
            &batch,
            "22023",
            &format!("transaction identifier \"{gid}\" is too long"),
            None,
            None,
            b'I',
        );
        assert!(!batch.iter().any(|m| m.0 == b'D'));
    }
    for command in ["COMMIT PREPARED", "ROLLBACK PREPARED"] {
        let sql = format!("{command} 'gid'");
        for (batch, successful) in [
            (
                wire_query(&mut conn, &format!("{sql}; {unexpected}")).await,
                0,
            ),
            (
                wire_query(&mut conn, &format!("{insert}; {sql}; {unexpected}")).await,
                1,
            ),
            (pipeline(&mut conn, &[insert, &sql, unexpected]).await, 1),
            (
                pipeline(&mut conn, &["SHOW work_mem", &sql, unexpected]).await,
                1,
            ),
        ] {
            error(
                &batch,
                "25001",
                &format!("{command} cannot run inside a transaction block"),
                None,
                None,
                b'I',
            );
            assert_eq!(batch.iter().filter(|m| m.0 == b'C').count(), successful);
            assert!(!batch.iter().any(|m| m.0 == b'N'));
        }
    }
    assert_eq!(
        first_text(&wire_query(&mut conn, "SELECT count(*) FROM twophase_batch").await),
        "0"
    );
    drop(conn);
    fixture.stop().await;
}

#[tokio::test]
async fn readonly_admin_commands_precede_provider_and_object_checks() {
    let fixture = fixture().await;
    let mut conn = connect(&fixture, "owner").await;
    for extended in [false, true] {
        for (tag, sql) in [
            (
                "CREATE TABLESPACE",
                "CREATE TABLESPACE s OWNER missing LOCATION 'bad'",
            ),
            (
                "ALTER TABLESPACE",
                "ALTER TABLESPACE missing OWNER TO missing",
            ),
            ("DROP TABLESPACE", "DROP TABLESPACE missing"),
            (
                "SECURITY LABEL",
                "SECURITY LABEL FOR other ON COLUMN missing.c IS 'x'",
            ),
            ("SECURITY LABEL", "SECURITY LABEL ON TABLE missing IS 'x'"),
        ] {
            let message = format!("cannot execute {tag} in a read-only transaction");
            completed(
                &query(&mut conn, "BEGIN READ ONLY", extended).await,
                "BEGIN",
                b'T',
            );
            error(
                &query(&mut conn, sql, extended).await,
                "25006",
                &message,
                None,
                None,
                b'E',
            );
            completed(
                &query(&mut conn, "ROLLBACK", extended).await,
                "ROLLBACK",
                b'I',
            );
            completed(
                &query(
                    &mut conn,
                    "SET default_transaction_read_only = on",
                    extended,
                )
                .await,
                "SET",
                b'I',
            );
            error(
                &query(&mut conn, sql, extended).await,
                "25006",
                &message,
                None,
                None,
                b'I',
            );
            error(
                &query(&mut conn, "LOAD 'missing'", extended).await,
                "58P01",
                "could not access file \"missing\": No such file or directory",
                None,
                None,
                b'I',
            );
            completed(
                &query(&mut conn, "RESET default_transaction_read_only", extended).await,
                "RESET",
                b'I',
            );
        }
    }
    drop(conn);
    fixture.stop().await;
}

#[tokio::test]
async fn admin_syntax_validation_and_row_type_diagnostics() {
    let fixture = fixture().await;
    let mut conn = connect(&fixture, "owner").await;
    completed(
        &wire_query(&mut conn, "CREATE TABLE admin_row_type (i int)").await,
        "CREATE TABLE",
        b'I',
    );
    for extended in [false, true] {
        for (sql, state, message) in [
            (
                "SECURITY LABEL ON BOGUS admin_row_type IS 'x'",
                "42601",
                "syntax error at or near \"BOGUS\"",
            ),
            (
                "SECURITY LABEL ON TABLE admin_row_type IS 'x' extra",
                "42601",
                "syntax error at or near \"extra\"",
            ),
            (
                "SECURITY LABEL FOR other ON COLUMN admin_row_type.i IS 'x'",
                "22023",
                "security label provider \"other\" is not loaded",
            ),
            (
                "COMMENT ON TRANSFORM FOR admin_row_type LANGUAGE sql IS 'x'",
                "42704",
                "transform for type admin_row_type language \"sql\" does not exist",
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
            error(
                &query(&mut conn, sql, extended).await,
                state,
                message,
                None,
                None,
                b'I',
            );
        }
        for value in ["1.5", "'010'", "'0x10'"] {
            let sql = format!(
                "CREATE TABLESPACE s LOCATION '/x' WITH (effective_io_concurrency={value}, seq_page_cost='0x10')"
            );
            error(
                &query(&mut conn, &sql, extended).await,
                "58P01",
                "directory \"/x\" does not exist",
                None,
                None,
                b'I',
            );
        }
        let location = format!("/{}", "a".repeat(1024));
        let sql = format!("CREATE TABLESPACE s LOCATION '{location}'");
        error(
            &query(&mut conn, &sql, extended).await,
            "42P17",
            &format!("tablespace location \"{location}\" is too long"),
            None,
            None,
            b'I',
        );
    }
    drop(conn);
    fixture.stop().await;
}

#[tokio::test]
async fn snapshot_isolation_quoting_and_export_counts() {
    let fixture = fixture().await;
    let mut conn = connect(&fixture, "owner").await;
    for extended in [false, true] {
        completed(
            &query(
                &mut conn,
                "SET default_transaction_isolation = 'repeatable read'",
                extended,
            )
            .await,
            "SET",
            b'I',
        );
        let outside = query(&mut conn, "SET TRANSACTION SNAPSHOT 'ABC'", extended).await;
        notice(
            &outside,
            "WARNING",
            "25P01",
            "SET TRANSACTION can only be used in transaction blocks",
        );
        error(
            &outside,
            "42704",
            "snapshot \"ABC\" does not exist",
            None,
            None,
            b'I',
        );
        completed(
            &query(&mut conn, "RESET default_transaction_isolation", extended).await,
            "RESET",
            b'I',
        );
        for (sql, state, message) in [
            (
                "SET LOCAL TRANSACTION SNAPSHOT 'ABC'",
                "0A000",
                "SET LOCAL TRANSACTION SNAPSHOT is not implemented",
            ),
            (
                "SET SESSION TRANSACTION SNAPSHOT 'ABC'",
                "42704",
                "snapshot \"ABC\" does not exist",
            ),
        ] {
            completed(
                &query(&mut conn, "BEGIN ISOLATION LEVEL REPEATABLE READ", extended).await,
                "BEGIN",
                b'T',
            );
            let reply = query(&mut conn, sql, extended).await;
            error(&reply, state, message, None, None, b'E');
            assert!(!reply.iter().any(|m| m.0 == b'N'));
            completed(
                &query(&mut conn, "ROLLBACK", extended).await,
                "ROLLBACK",
                b'I',
            );
        }
        for sql in [
            "PREPARE TRANSACTION E'x'",
            "-- lead\n/* next */ PREPARE TRANSACTION $$x;$$",
        ] {
            completed(&query(&mut conn, "BEGIN", extended).await, "BEGIN", b'T');
            error(
                &query(&mut conn, sql, extended).await,
                "55000",
                "prepared transactions are disabled",
                None,
                Some("Set \"max_prepared_transactions\" to a nonzero value."),
                b'I',
            );
        }
        completed(&query(&mut conn, "BEGIN", extended).await, "BEGIN", b'T');
        let first = query(&mut conn, "SELECT pg_export_snapshot()", extended).await;
        let second = query(&mut conn, "SELECT pg_export_snapshot()", extended).await;
        let prefix = first_text(&first).strip_suffix("-1").unwrap();
        assert_eq!(first_text(&second), format!("{prefix}-2"));
        error(
            &query(&mut conn, "PREPARE TRANSACTION 'exported'", extended).await,
            "0A000",
            "cannot PREPARE a transaction that has exported snapshots",
            None,
            None,
            b'I',
        );
        let fresh = query(&mut conn, "SELECT pg_export_snapshot()", extended).await;
        assert!(first_text(&fresh).ends_with("-1"));
        assert_ne!(first_text(&fresh), first_text(&first));
    }
    let isolation = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ";
    let import = "SET TRANSACTION SNAPSHOT 'ZZ'";
    let batch = wire_query(&mut conn, &format!("{isolation}; {import}")).await;
    error(
        &batch,
        "22023",
        "invalid snapshot identifier: \"ZZ\"",
        None,
        None,
        b'I',
    );
    assert!(!batch.iter().any(|m| m.0 == b'N'));
    let pipeline = pipeline(&mut conn, &[isolation, import]).await;
    notice(
        &pipeline,
        "WARNING",
        "25P01",
        "SET TRANSACTION can only be used in transaction blocks",
    );
    assert_eq!(pipeline.iter().filter(|m| m.0 == b'N').count(), 1);
    error(
        &pipeline,
        "22023",
        "invalid snapshot identifier: \"ZZ\"",
        None,
        None,
        b'I',
    );
    let queried = wire_query(&mut conn, &format!("SELECT 1; {import}")).await;
    error(
        &queried,
        "25001",
        "SET TRANSACTION SNAPSHOT must be called before any query",
        None,
        None,
        b'I',
    );
    assert!(!queried.iter().any(|m| m.0 == b'N'));
    drop(conn);
    fixture.stop().await;
}

#[tokio::test]
async fn simple_batch_error_discards_partial_result_frames() {
    let fixture = fixture().await;
    let mut conn = connect(&fixture, "owner").await;
    completed(
        &wire_query(&mut conn, "CREATE TABLE partial_result (i int PRIMARY KEY)").await,
        "CREATE TABLE",
        b'I',
    );
    completed(
        &wire_query(
            &mut conn,
            "INSERT INTO partial_result SELECT generate_series(1, 300)",
        )
        .await,
        "INSERT 0 300",
        b'I',
    );
    let reply = wire_query(
        &mut conn,
        "SELECT 17; SELECT 600 / (300 - i) FROM partial_result; SELECT 99",
    )
    .await;
    assert_eq!(field(body(&reply, b'E'), b'C'), Some("22012"), "{reply:?}");
    assert_eq!(first_text(&reply), "17");
    assert_eq!(reply.iter().map(|m| m.0).collect::<Vec<_>>(), b"TDCEZ");
    assert_eq!(ready(&reply), b'I');
    drop(conn);
    fixture.stop().await;
}
