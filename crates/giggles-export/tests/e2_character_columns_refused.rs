//! E2a′: the logical archive has no varchar/char identity or typmod, so capture and
//! export refuse those columns (0A000) instead of silently writing them as TEXT.
mod common;
use common::*;
use giggles_export::*;

#[test]
fn varchar_and_char_columns_refuse_capture_and_export() {
    for (label, ddl) in [
        (
            "e2-export-varchar",
            "CREATE TABLE v (id int PRIMARY KEY, s varchar(5))",
        ),
        (
            "e2-export-varchar-bare",
            "CREATE TABLE v (id int PRIMARY KEY, s varchar)",
        ),
        (
            "e2-export-char",
            "CREATE TABLE v (id int PRIMARY KEY, s char(3))",
        ),
        (
            "e2-export-bpchar",
            "CREATE TABLE v (id int PRIMARY KEY, s bpchar)",
        ),
    ] {
        let (p, a) = populated(label);
        let mut session = a.sql_session();
        session.execute(ddl, &[], None).unwrap();
        assert!(
            matches!(
                capture(&a, Bounds::default()),
                Err(Error::Core(giggles_core::CoreError::Unsupported))
            ),
            "{ddl}"
        );
        assert!(
            matches!(
                export(&a, p.join("copy"), Bounds::default()),
                Err(Error::Core(giggles_core::CoreError::Unsupported))
            ),
            "{ddl}"
        );
        assert!(!p.join("copy").exists());
    }
}
