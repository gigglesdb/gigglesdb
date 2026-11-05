use giggles_cli::*;
use giggles_core::{
    Admin, Limits,
    relational::{SqlResult, SqlRow, Value},
};
use std::{
    io::{self, Write},
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn long_cell_does_not_amplify_padding_across_a_thousand_rows() {
    let long = "x".repeat(64 * 1024 - 16);
    let mut rows = vec![
        SqlRow {
            values: vec![Value::Text(String::new())],
            version: 1,
        };
        1024
    ];
    rows[0].values[0] = Value::Text(long.clone());
    let result = SqlResult {
        columns: vec!["body".into()],
        rows,
        affected: 0,
    };
    let rendered = render_result(&result, OutputMode::Table);
    assert!(rendered.contains(&long), "long values must remain complete");
    assert!(
        rendered.len() < 256 * 1024,
        "alignment amplified bounded cells: {}",
        rendered.len()
    );
    assert_eq!(rendered.lines().count(), 1026);
}

#[test]
fn io_reader_failure() {
    struct Broken;
    impl io::Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("injected read failure"))
        }
    }
    assert_eq!(bounded_read(Broken, 20).unwrap_err().status, 4);
}

#[test]
fn atomic_flush_failure_rolls_back() {
    struct FlushFailure;
    impl Write for FlushFailure {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("injected flush failure"))
        }
    }
    let a = admin();
    let mut runner = Runner::new(&a, OutputMode::Json);
    assert_eq!(
        runner
            .batch(
                &["CREATE TABLE app (id BIGINT PRIMARY KEY)".into()],
                &[],
                None,
                true,
                &mut FlushFailure
            )
            .unwrap_err()
            .status,
        4
    );
    assert!(a.catalog().unwrap().1.is_empty());
}

#[test]
fn transaction_state_machine_matches_committed_model() {
    let a = admin();
    let mut runner = Runner::new(&a, OutputMode::Json);
    runner
        .execute(
            "CREATE TABLE app (id BIGINT PRIMARY KEY)",
            &[],
            None,
            &mut Vec::new(),
        )
        .unwrap();
    let mut committed = Vec::new();
    // Each cycle has a different boundary: COMMIT, ROLLBACK, EOF, or duplicate failure.
    for i in 0..24 {
        let mut out = Vec::new();
        runner.execute("BEGIN", &[], None, &mut out).unwrap();
        runner
            .execute(&format!("INSERT INTO app VALUES({i})"), &[], None, &mut out)
            .unwrap();
        match i % 4 {
            0 => {
                runner.execute("COMMIT", &[], None, &mut out).unwrap();
                committed.push(i);
            }
            1 => runner.execute("ROLLBACK", &[], None, &mut out).unwrap(),
            2 => {
                assert_eq!(runner.finish().unwrap_err().status, 3);
            }
            _ => {
                assert!(
                    runner
                        .execute(&format!("INSERT INTO app VALUES({i})"), &[], None, &mut out)
                        .is_err()
                );
                runner.execute("ROLLBACK", &[], None, &mut out).unwrap();
            }
        }
        let rows = a
            .sql_session()
            .execute("SELECT * FROM app LIMIT 100", &[], None)
            .unwrap()
            .rows;
        assert_eq!(
            rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
            committed.iter().map(|i| Value::Int(*i)).collect::<Vec<_>>()
        );
    }
}

#[test]
fn interactive_bounds_and_parser_errors_rollback() {
    for script in [
        "x".repeat(MAX_STATEMENT + 1),
        "BEGIN;\n--unsupported\n".into(),
        "BEGIN;\n.help extra\n".into(),
    ] {
        let a = admin();
        let mut r = Runner::new(&a, OutputMode::Json);
        assert_eq!(
            r.repl(
                &a,
                &mut script.as_bytes(),
                &mut Vec::new(),
                &mut Vec::new(),
                false
            )
            .unwrap_err()
            .status,
            2
        );
        r.finish().unwrap();
    }
}
fn admin() -> Admin {
    let p = std::env::temp_dir().join(format!(
        "giggles-cli-direct-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    Admin::create(p, Limits::default()).unwrap()
}
fn err(text: &str) {
    assert_eq!(split_script(text, false).unwrap_err().status, 2);
}
#[test]
fn quoted_semicolons_unicode() {
    assert_eq!(
        split_script("INSERT INTO t VALUES('雪;🙂'); SELECT 'a';", false).unwrap(),
        ["INSERT INTO t VALUES('雪;🙂')", "SELECT 'a'"]
    );
}
#[test]
fn doubled_quotes() {
    assert_eq!(
        split_script("SELECT 'a'';b';", false).unwrap(),
        ["SELECT 'a'';b'"]
    );
}
#[test]
fn multiline() {
    assert_eq!(
        split_script("SELECT\n'a\nb';\n", false).unwrap(),
        ["SELECT\n'a\nb'"]
    );
}
#[test]
fn empty_script() {
    assert!(split_script(" \r\n ;;;\t", false).unwrap().is_empty());
}
#[test]
fn incomplete_eof() {
    err("SELECT 'a");
    err("SELECT 'a'");
    assert_eq!(split_script("SELECT 'a'", true).unwrap(), ["SELECT 'a'"]);
}
#[test]
fn comment_rejection_outside_strings() {
    for s in ["SELECT --x;", "/* hi */;", "SELECT 1;-- hi"] {
        err(s);
    }
    assert_eq!(split_script("SELECT '-- /* */';", false).unwrap().len(), 1);
}
#[test]
fn exact_statement_limits() {
    assert!(split_script(&format!("{};", "x".repeat(MAX_STATEMENT)), false).is_ok());
    err(&format!("{};", "x".repeat(MAX_STATEMENT + 1)));
    err(&format!("'{}';", "雪".repeat(MAX_STATEMENT / 3)));
}
#[test]
fn exact_script_limits() {
    assert_eq!(
        split_script(&"x;".repeat(MAX_STATEMENTS), false)
            .unwrap()
            .len(),
        MAX_STATEMENTS
    );
    err(&"x;".repeat(MAX_STATEMENTS + 1));
    err(&" ".repeat(MAX_SCRIPT + 1));
}
#[test]
fn invalid_utf8_nul() {
    assert_eq!(bounded_read(&[0xff][..], 4).unwrap_err().status, 2);
    assert!(bounded_read(&b"a\0b"[..], 4).is_err());
    err("SELECT '\0';");
}
#[test]
fn bounded_reader_exact() {
    assert_eq!(bounded_read(&b"abcd"[..], 4).unwrap(), "abcd");
    assert!(bounded_read(&b"abcde"[..], 4).is_err());
}
#[test]
fn generated_literal_roundtrip() {
    let atoms = ["", "'", ";", "雪", "🙂", "--", "/*", "\n", "\\", "a"];
    for a in atoms {
        for b in atoms {
            let value = format!("{a}{b}").replace('\'', "''");
            let query = format!("SELECT '{value}'");
            assert_eq!(split_script(&format!("{query};"), false).unwrap(), [query]);
        }
    }
}
#[test]
fn params_typed_and_invalid() {
    let v = parse_params(r#"["Null",{"Bool":true},{"Int":-9},{"Text":"a';雪"},{"Bytes":[0,255]}]"#)
        .unwrap();
    assert_eq!(v.len(), 5);
    for s in [
        "[null]",
        "[1]",
        "{}",
        r#"[{"Int":1.5}]"#,
        r#"[{"Bytes":[256]}]"#,
        r#"[{"Int":9223372036854775808}]"#,
    ] {
        assert!(parse_params(s).is_err(), "{s}");
    }
    assert!(parse_params(&format!("[{}]", vec!["\"Null\""; 257].join(","))).is_err());
}
fn args(extra: &[&str]) -> Vec<String> {
    ["--open", "db"]
        .iter()
        .chain(extra)
        .map(|s| s.to_string())
        .collect()
}
#[test]
fn bad_arguments() {
    for a in [
        vec![],
        args(&[]),
        args(&["--stdin", "--repl"]),
        args(&["--sql", "x", "--atomic", "--atomic"]),
        args(&["--stdin", "--params-file", "x"]),
        args(&["--sql", "x", "--expected-version", "0"]),
        args(&["--sql", "x", "--expected-version", "-1"]),
        args(&["--repl", "--atomic"]),
        args(&["--stdin", "--wat"]),
        args(&["--sql"]),
    ] {
        assert_eq!(Options::parse(a).unwrap_err().status, 2);
    }
}
#[test]
fn typed_rendering() {
    let result = SqlResult {
        columns: vec!["a".into(), "b".into(), "c".into(), "d".into()],
        rows: vec![SqlRow {
            values: vec![
                Value::Null,
                Value::Text("".into()),
                Value::Bytes(vec![0, 255]),
                Value::Bool(false),
            ],
            version: 7,
        }],
        affected: 0,
    };
    let text = render_result(&result, OutputMode::Table);
    assert!(text.contains("NULL | \"\" | 0x00ff | false"));
    assert!(text.contains("@version"));
    let json: serde_json::Value =
        serde_json::from_str(&render_result(&result, OutputMode::Json)).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["result"]["rows"][0]["version"], 7);
    assert_eq!(json["result"]["rows"][0]["values"][0], "Null");
}
#[test]
fn control_escape_alignment() {
    let result = SqlResult {
        columns: vec!["x\x1b[31m".into()],
        rows: vec![SqlRow {
            values: vec![Value::Text("\n\r\t\x1b\"\\雪".into())],
            version: 1,
        }],
        affected: 0,
    };
    let s = render_result(&result, OutputMode::Table);
    assert!(!s.contains('\x1b'));
    assert!(!s.contains('\r'));
    assert!(!s.contains('\t'));
    let lines: Vec<_> = s.lines().collect();
    assert_eq!(lines[0].chars().count(), lines[1].chars().count());
}
#[test]
fn session_read_own_write_and_rollback() {
    let a = admin();
    let mut r = Runner::new(&a, OutputMode::Json);
    let mut out = Vec::new();
    r.execute("BEGIN", &[], None, &mut out).unwrap();
    r.execute(
        "CREATE TABLE app (id BIGINT PRIMARY KEY)",
        &[],
        None,
        &mut out,
    )
    .unwrap();
    r.execute("INSERT INTO app VALUES(1)", &[], None, &mut out)
        .unwrap();
    r.execute("SELECT * FROM app LIMIT 10", &[], None, &mut out)
        .unwrap();
    assert_eq!(r.finish().unwrap_err().status, 3);
    assert!(a.catalog().unwrap().1.is_empty());
}
#[test]
fn distinct_handle_conflict() {
    let a = admin();
    let mut x = a.sql_session();
    let mut y = a.sql_session();
    x.execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    x.execute("BEGIN", &[], None).unwrap();
    y.execute("INSERT INTO app VALUES(2)", &[], None).unwrap();
    x.execute("INSERT INTO app VALUES(1)", &[], None).unwrap();
    assert!(x.execute("COMMIT", &[], None).is_err());
    x.rollback();
    assert_eq!(
        y.execute("SELECT * FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        1
    );
}
#[test]
fn atomic_nested_negative() {
    let a = admin();
    let mut r = Runner::new(&a, OutputMode::Json);
    for control in ["BEGIN", "begin", "COMMIT", "ROLLBACK"] {
        assert_eq!(
            r.batch(&[control.into()], &[], None, true, &mut Vec::new())
                .unwrap_err()
                .status,
            2
        );
    }
}
struct FailWrite;
impl Write for FailWrite {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("injected output failure"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[test]
fn output_failure_rollback() {
    let a = admin();
    let mut r = Runner::new(&a, OutputMode::Json);
    assert_eq!(
        r.batch(
            &["CREATE TABLE app (id BIGINT PRIMARY KEY)".into()],
            &[],
            None,
            true,
            &mut FailWrite
        )
        .unwrap_err()
        .status,
        4
    );
    assert!(a.catalog().unwrap().1.is_empty());
}
#[test]
fn repl_schema_mode_and_pending_rollback() {
    let a = admin();
    let mut r = Runner::new(&a, OutputMode::Json);
    let mut out = Vec::new();
    let e = r
        .repl(
            &a,
            &mut &b"BEGIN;\nCREATE TABLE app (id BIGINT PRIMARY KEY);\n.quit\n"[..],
            &mut out,
            &mut Vec::new(),
            false,
        )
        .unwrap_err();
    assert_eq!(e.status, 3);
    assert!(a.catalog().unwrap().1.is_empty());
}
