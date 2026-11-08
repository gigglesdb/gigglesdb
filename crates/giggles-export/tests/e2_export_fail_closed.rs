//! Refuse catalog metadata that the logical archive cannot preserve.
mod common;
use common::*;
use giggles_core::{Admin, CoreError, Limits};
use giggles_export::*;
use std::path::Path;

fn assert_refused(admin: &Admin, path: &Path) {
    let captured = capture(admin, Bounds::default());
    assert!(
        matches!(captured, Err(Error::Core(CoreError::Unsupported))),
        "capture: {captured:?}"
    );
    let output = path.join("copy.jsonl");
    let exported = export(admin, &output, Bounds::default());
    assert!(
        matches!(exported, Err(Error::Core(CoreError::Unsupported))),
        "export: {exported:?}"
    );
    assert!(!output.exists());
    assert!(!std::fs::read_dir(path).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|ext| ext == "staging")
    }));
}

fn refuses_sql(label: &str, statements: &[&str]) {
    let (path, admin) = db(label);
    let mut session = admin.sql_session();
    for sql in statements {
        session
            .execute(sql, &[], None)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    drop(session);
    assert_refused(&admin, &path);
}

fn assert_roundtrip(admin: &Admin, path: &Path) -> Archive {
    let expected = capture(admin, Bounds::default()).unwrap();
    let output = path.join("copy.jsonl");
    let report = export(admin, &output, Bounds::default()).unwrap();
    assert_eq!(
        report.rows,
        expected.rows.iter().map(Vec::len).sum::<usize>()
    );
    let target = path.join("imported");
    import(&output, &target, Bounds::default()).unwrap();
    let restored = Admin::open(target, Limits::default()).unwrap();
    assert_eq!(capture(&restored, Bounds::default()).unwrap(), expected);
    expected
}

#[test]
fn numeric_typmod_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-numeric-typmod",
        &[
            "CREATE TABLE t (id int PRIMARY KEY)",
            "ALTER TABLE t ADD COLUMN n numeric(10,2)",
        ],
    );
}

#[test]
fn timestamp_typmod_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-timestamp-typmod",
        &[
            "CREATE TABLE t (id int PRIMARY KEY)",
            "ALTER TABLE t ADD COLUMN ts timestamp(3)",
        ],
    );
}

#[test]
fn default_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-default",
        &["CREATE TABLE t (id int PRIMARY KEY, v int DEFAULT 5)"],
    );
}

#[test]
fn identity_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-identity",
        &["CREATE TABLE t (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v int)"],
    );
}

#[test]
fn serial_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-serial",
        &["CREATE TABLE t (id serial PRIMARY KEY, v int)"],
    );
}

#[test]
fn check_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-check",
        &["CREATE TABLE t (id int PRIMARY KEY, v int CHECK (v > 0))"],
    );
}

#[test]
fn foreign_key_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-foreign-key",
        &[
            "CREATE TABLE p (id int PRIMARY KEY)",
            "CREATE TABLE c (id int PRIMARY KEY, p int REFERENCES p(id))",
        ],
    );
}

#[test]
fn unique_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-unique",
        &["CREATE TABLE t (id int PRIMARY KEY, v int UNIQUE)"],
    );
}

#[test]
fn secondary_index_refuses_capture_and_export() {
    refuses_sql(
        "e2-refuse-secondary-index",
        &[
            "CREATE TABLE t (id int PRIMARY KEY, v int)",
            "CREATE INDEX t_v ON t(v)",
        ],
    );
}

#[test]
fn standalone_sequence_refuses_capture_and_export() {
    refuses_sql("e2-refuse-sequence", &["CREATE SEQUENCE s"]);
}

#[test]
fn plain_table_rows_roundtrip() {
    let (path, admin) = db("e2-plain-roundtrip");
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t (id int PRIMARY KEY, v text NOT NULL)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1,'one'),(2,'two')", &[], None)
        .unwrap();
    drop(session);
    let archive = assert_roundtrip(&admin, &path);
    assert_eq!(archive.tables.len(), 1);
    assert_eq!(archive.rows[0].len(), 2);
}

#[test]
fn dropping_only_default_restores_export() {
    let (path, admin) = db("e2-drop-default");
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE t (id int PRIMARY KEY, v int DEFAULT 5)",
            &[],
            None,
        )
        .unwrap();
    assert_refused(&admin, &path);
    session
        .execute("ALTER TABLE t ALTER COLUMN v DROP DEFAULT", &[], None)
        .unwrap();
    drop(session);
    assert_roundtrip(&admin, &path);
}

/// Golden archive bytes for a plain table, captured from the pre-change capture path
/// (main e5722d95, catalog() bracket). The fail-closed gate must not change them.
const PLAIN_ARCHIVE_GOLDEN: &str = "7b227265636f7264223a22686561646572222c22666f726d6174223a312c2270726f66696c65223a22676967676c65732d6c6f676963616c2d7631222c227461626c6573223a5b7b226e616d65223a2270222c22636f6c756d6e73223a5b7b226e616d65223a226964222c22646174615f74797065223a22496e7465676572222c226e6f745f6e756c6c223a747275657d2c7b226e616d65223a2276222c22646174615f74797065223a2254657874222c226e6f745f6e756c6c223a747275657d5d2c227072696d6172795f6b6579223a5b305d7d5d7d0a7b227265636f7264223a22726f77222c227461626c65223a302c2276616c756573223a5b7b2274797065223a22696e74222c2276616c7565223a2231227d2c7b2274797065223a2274657874222c2276616c7565223a2261227d5d7d0a7b227265636f7264223a22726f77222c227461626c65223a302c2276616c756573223a5b7b2274797065223a22696e74222c2276616c7565223a2232227d2c7b2274797065223a2274657874222c2276616c7565223a2262227d5d7d0a7b227265636f7264223a226d616e6966657374222c22736861323536223a2264313538356564643539333732613730353030323234396566323966623262306234613764653735323338393233316638373463393966653030333036616133222c227461626c655f726f7773223a5b325d2c22726f7773223a327d0a";
#[test]
fn plain_table_archive_bytes_are_unchanged() {
    let (_path, admin) = db("e2-export-golden-bytes");
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE p (id int PRIMARY KEY, v text NOT NULL)",
        "INSERT INTO p VALUES (1,'a'),(2,'b')",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    drop(session);
    let bytes = encode(
        &capture(&admin, Bounds::default()).unwrap(),
        Bounds::default(),
    )
    .unwrap();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, PLAIN_ARCHIVE_GOLDEN);
}
