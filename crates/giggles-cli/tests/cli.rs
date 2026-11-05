use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn typed_rows_remain_lossless_in_machine_output() {
    let f = fixture();
    let db = f.join("db");
    success(run(
        &db,
        true,
        &[
            "--sql",
            "CREATE TABLE app (id BIGINT PRIMARY KEY, flag BOOL, body TEXT, raw BYTEA)",
        ],
        b"",
    ));
    let params = f.join("typed.json");
    fs::write(
        &params,
        r#"[{"Int":-9223372036854775808},{"Bool":true},"Null",{"Bytes":[0,27,255]}]"#,
    )
    .unwrap();
    success(run(
        &db,
        false,
        &[
            "--sql",
            "INSERT INTO app VALUES($1,$2,$3,$4)",
            "--params-file",
            params.to_str().unwrap(),
        ],
        b"",
    ));
    let rows = select(&db);
    assert_eq!(rows[0]["values"][0]["Int"].as_i64(), Some(i64::MIN));
    assert_eq!(rows[0]["values"][1]["Bool"], true);
    assert_eq!(rows[0]["values"][2], "Null");
    assert_eq!(
        rows[0]["values"][3]["Bytes"],
        serde_json::json!([0, 27, 255])
    );
}

#[test]
fn bad_parameter_file_prevents_creation() {
    let f = fixture();
    let db = f.join("db");
    let p = f.join("bad.json");
    for bytes in [b"[1]".to_vec(), vec![255], vec![b' '; 65537]] {
        fs::write(&p, bytes).unwrap();
        code(
            &run(
                &db,
                true,
                &[
                    "--sql",
                    "CREATE TABLE app (id BIGINT PRIMARY KEY)",
                    "--params-file",
                    p.to_str().unwrap(),
                ],
                b"",
            ),
            2,
        );
        assert!(!db.exists());
    }
}

#[test]
fn script_statement_and_total_byte_limits() {
    for bytes in [
        "INSERT INTO app VALUES(1,'x');".repeat(257).into_bytes(),
        vec![b' '; 1024 * 1024 + 1],
    ] {
        let f = fixture();
        let db = f.join("db");
        let file = f.join("large.sql");
        fs::write(&file, bytes).unwrap();
        code(&run(&db, true, &["--file", file.to_str().unwrap()], b""), 2);
        assert!(!db.exists());
    }
}

#[test]
fn repl_count_bound_is_enforced_across_lines() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let input = "SELECT * FROM app LIMIT 0;\n".repeat(257);
    let r = run(&db, false, &["--repl"], input.as_bytes());
    code(&r, 2);
    assert_eq!(String::from_utf8(r.stdout).unwrap().lines().count(), 256);
}

#[test]
fn separate_process_handles_observe_only_committed_rows() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    success(run(
        &db,
        false,
        &["--stdin"],
        b"BEGIN; INSERT INTO app VALUES(1,'discard'); ROLLBACK; INSERT INTO app VALUES(2,'keep');",
    ));
    assert_eq!(select(&db)[0]["values"][0]["Int"], 2);
}
fn fixture() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "giggles-cli-subprocess-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p).unwrap();
    p
}
fn run(db: &Path, create: bool, args: &[&str], stdin: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-sql"))
        .arg(if create { "--create" } else { "--open" })
        .arg(db)
        .args(["--format", "json"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut pipe = child.stdin.take().unwrap(); // Bounded input is small enough for these fixtures; tolerate early rejected input.
    let _ = pipe.write_all(stdin);
    drop(pipe);
    child.wait_with_output().unwrap()
}
fn code(output: &Output, n: i32) {
    assert_eq!(
        output.status.code(),
        Some(n),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
fn success(output: Output) -> Vec<serde_json::Value> {
    code(&output, 0);
    assert!(output.stderr.is_empty(), "{:?}", output);
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
fn create(db: &Path) {
    success(run(
        db,
        true,
        &[
            "--sql",
            "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
        ],
        b"",
    ));
}
fn select(db: &Path) -> Vec<serde_json::Value> {
    success(run(
        db,
        false,
        &["--sql", "SELECT * FROM app LIMIT 100"],
        b"",
    ))[0]["result"]["rows"]
        .as_array()
        .unwrap()
        .clone()
}
#[test]
fn subprocess_create_reopen() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    success(run(
        &db,
        false,
        &["--sql", "INSERT INTO app VALUES(1,'雪;it''s')"],
        b"",
    ));
    let rows = select(&db);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["values"][1]["Text"], "雪;it's");
    assert!(rows[0]["version"].as_u64().unwrap() > 0);
}
#[test]
fn explicit_open_no_create() {
    let f = fixture();
    let db = f.join("missing");
    code(&run(&db, false, &["--stdin"], b""), 3);
    assert!(!db.exists());
}
#[test]
fn create_no_clobber() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    code(
        &run(
            &db,
            true,
            &["--sql", "CREATE TABLE other (id BIGINT PRIMARY KEY)"],
            b"",
        ),
        3,
    );
    assert!(select(&db).is_empty());
}
#[test]
fn stdin_file_scripts() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    success(run(
        &db,
        false,
        &["--stdin"],
        b"INSERT INTO app VALUES(1,'a');\nINSERT INTO app VALUES(2,'b');",
    ));
    let script = f.join("input.sql");
    fs::write(&script, "SELECT * FROM app LIMIT 10;\n").unwrap();
    let r = success(run(&db, false, &["--file", script.to_str().unwrap()], b""));
    assert_eq!(r[0]["result"]["rows"].as_array().unwrap().len(), 2);
}
#[test]
fn multiline_repl_transaction() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let r=success(run(&db,false,&["--repl"],b"BEGIN;\nINSERT INTO app\n VALUES(1,'hello;\nworld');\nSELECT * FROM app LIMIT 10;\nCOMMIT;\n.quit\n"));
    assert_eq!(r[2]["result"]["rows"].as_array().unwrap().len(), 1);
    assert_eq!(select(&db)[0]["values"][1]["Text"], "hello;\nworld");
}
#[test]
fn eof_rollback() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    for source in ["--repl", "--stdin"] {
        code(
            &run(
                &db,
                false,
                &[source],
                b"BEGIN;\nINSERT INTO app VALUES(1,'uncommitted');\n",
            ),
            3,
        );
        assert!(select(&db).is_empty());
    }
}
#[test]
fn eof_incomplete_repl_rolls_back() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    code(
        &run(
            &db,
            false,
            &["--repl"],
            b"BEGIN;\nINSERT INTO app VALUES(1,'uncommitted');\nSELECT 'bad",
        ),
        2,
    );
    assert!(select(&db).is_empty());
}
#[test]
fn atomic_vs_autocommit_failure() {
    for atomic in [false, true] {
        let f = fixture();
        let db = f.join("db");
        create(&db);
        let args = if atomic {
            vec!["--stdin", "--atomic"]
        } else {
            vec!["--stdin"]
        };
        code(&run(&db,false,&args,b"INSERT INTO app VALUES(1,'first'); INSERT INTO app VALUES(1,'duplicate'); INSERT INTO app VALUES(2,'never');"),3);
        assert_eq!(select(&db).len(), if atomic { 0 } else { 1 });
    }
}
#[test]
fn atomic_success_and_nested_rejection() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    success(run(
        &db,
        false,
        &["--stdin", "--atomic"],
        b"INSERT INTO app VALUES(1,'a'); INSERT INTO app VALUES(2,'b');",
    ));
    assert_eq!(select(&db).len(), 2);
    code(
        &run(
            &db,
            false,
            &["--stdin", "--atomic"],
            b"INSERT INTO app VALUES(3,'c'); BEGIN;",
        ),
        2,
    );
    assert_eq!(select(&db).len(), 2);
}
#[test]
fn params_and_cas() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let p = f.join("params.json");
    fs::write(&p, r#"[{"Int":1},{"Text":"'); DELETE FROM app; -- 雪"}]"#).unwrap();
    success(run(
        &db,
        false,
        &[
            "--sql",
            "INSERT INTO app VALUES($1,$2)",
            "--params-file",
            p.to_str().unwrap(),
        ],
        b"",
    ));
    let v = select(&db)[0]["version"].as_u64().unwrap();
    success(run(
        &db,
        false,
        &[
            "--sql",
            "UPDATE app SET body = 'new' WHERE id = 1",
            "--expected-version",
            &v.to_string(),
        ],
        b"",
    ));
    code(
        &run(
            &db,
            false,
            &[
                "--sql",
                "UPDATE app SET body = 'stale' WHERE id = 1",
                "--expected-version",
                &v.to_string(),
            ],
            b"",
        ),
        3,
    );
    assert_eq!(select(&db)[0]["values"][1]["Text"], "new");
}
#[test]
fn output_no_clobber_prevents_mutation() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let out = f.join("output.json");
    fs::write(&out, "keep").unwrap();
    code(
        &run(
            &db,
            false,
            &[
                "--sql",
                "INSERT INTO app VALUES(1,'a')",
                "--output",
                out.to_str().unwrap(),
            ],
            b"",
        ),
        4,
    );
    assert_eq!(fs::read_to_string(out).unwrap(), "keep");
    assert!(select(&db).is_empty());
}
#[test]
fn output_new_file() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let out = f.join("new.json");
    let r = run(
        &db,
        false,
        &[
            "--sql",
            "SELECT * FROM app LIMIT 10",
            "--output",
            out.to_str().unwrap(),
        ],
        b"",
    );
    code(&r, 0);
    assert!(r.stdout.is_empty());
    let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(out).unwrap()).unwrap();
    assert_eq!(v["version"], 1);
}
#[test]
fn input_output_collision() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let file = f.join("input.sql");
    fs::write(&file, "INSERT INTO app VALUES(1,'a');").unwrap();
    code(
        &run(
            &db,
            false,
            &[
                "--file",
                file.to_str().unwrap(),
                "--output",
                file.to_str().unwrap(),
            ],
            b"",
        ),
        4,
    );
    assert_eq!(
        fs::read_to_string(file).unwrap(),
        "INSERT INTO app VALUES(1,'a');"
    );
    assert!(select(&db).is_empty());
}
#[test]
fn invalid_binary_and_limits_preflight_before_create() {
    for bytes in [
        vec![255],
        b"SELECT '\0';".to_vec(),
        vec![b'x'; 65538],
        b"SELECT 'no semicolon'".to_vec(),
        b"--comment\n".to_vec(),
    ] {
        let f = fixture();
        let db = f.join("db");
        code(&run(&db, true, &["--stdin"], &bytes), 2);
        assert!(!db.exists());
    }
}
#[test]
fn missing_file_io_status() {
    let f = fixture();
    let db = f.join("db");
    code(
        &run(
            &db,
            true,
            &["--file", f.join("missing").to_str().unwrap()],
            b"",
        ),
        4,
    );
    assert!(!db.exists());
}
#[test]
fn bad_args_status() {
    for args in [
        vec![],
        vec!["--open", "missing", "--stdin", "--repl"],
        vec!["--unknown"],
        vec!["--help", "extra"],
    ] {
        let o = Command::new(env!("CARGO_BIN_EXE_giggles-sql"))
            .args(args)
            .output()
            .unwrap();
        code(&o, 2);
        assert!(o.stdout.is_empty());
        assert!(!o.stderr.is_empty());
    }
}
#[test]
fn help_without_database() {
    let o = Command::new(env!("CARGO_BIN_EXE_giggles-sql"))
        .arg("--help")
        .output()
        .unwrap();
    code(&o, 0);
    assert!(String::from_utf8_lossy(&o.stdout).contains("trusted local Admin"));
}
#[test]
fn schema_mode_quit() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let r = run(
        &db,
        false,
        &["--repl"],
        b".mode table\nSELECT * FROM app LIMIT 10;\n.mode json\n.schema\n.quit\nINVALID;\n",
    );
    code(&r, 0);
    let text = String::from_utf8(r.stdout).unwrap();
    assert!(text.contains("@version"));
    assert!(text.contains("\"catalog\""));
    assert!(!text.contains("INVALID"));
}
#[test]
fn unsafe_metacommand_rejected() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    code(&run(&db, false, &["--repl"], b".shell echo nope\n"), 2);
}
#[test]
fn empty_scripts_succeed() {
    let f = fixture();
    let db = f.join("db");
    let r = run(&db, true, &["--stdin"], b" \n;; ");
    code(&r, 0);
    assert!(r.stdout.is_empty());
    assert!(db.exists());
}
#[test]
fn stderr_separation() {
    let f = fixture();
    let db = f.join("db");
    create(&db);
    let r = run(
        &db,
        false,
        &["--sql", "SELECT * FROM missing LIMIT 10"],
        b"",
    );
    code(&r, 3);
    assert!(r.stdout.is_empty());
    assert!(!r.stderr.is_empty());
}
#[test]
fn single_sql_rejects_multiple_before_create() {
    let f = fixture();
    let db = f.join("db");
    code(&run(&db, true, &["--sql", "BEGIN; COMMIT;"], b""), 2);
    assert!(!db.exists());
}
