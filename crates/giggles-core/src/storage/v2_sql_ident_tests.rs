use super::*;
use relational::Value;

#[test]
fn quoted_identifiers_with_spaces_support_dml() {
    let f = Fixture::new();
    f.execute(r#"CREATE TABLE "t space" ("row id" integer PRIMARY KEY, "display name" text)"#);
    f.execute(r#"INSERT INTO "t space" VALUES (1, 'Ada')"#);
    let mut sql = f.admin().sql_session();
    assert_eq!(
        values(
            &mut sql,
            r#"SELECT "display name" FROM "t space" WHERE "row id"=1"#
        ),
        vec![vec![Value::Text("Ada".into())]]
    );
    assert_eq!(
        sql.execute(
            r#"UPDATE "t space" SET "display name"='Grace' WHERE "row id"=1"#,
            &[],
            None
        )
        .unwrap()
        .affected,
        1
    );
    assert_eq!(
        values(
            &mut sql,
            r#"SELECT "row id", "display name" FROM "t space""#
        ),
        vec![vec![Value::Int(1), Value::Text("Grace".into())]]
    );
}

#[test]
fn export_restore_preserves_secondary_and_unique_indexes() {
    let f = Fixture::new();
    for sql in [
        "CREATE TABLE t (id integer PRIMARY KEY, bucket integer, email text)",
        "CREATE INDEX by_bucket ON t (bucket)",
        "CREATE UNIQUE INDEX by_email ON t (email)",
        "INSERT INTO t VALUES (1,10,'a'),(2,10,'b'),(3,20,'c'),(4,30,'d')",
        "DELETE FROM t WHERE id=2",
        "UPDATE t SET bucket=20,email='updated' WHERE id=1",
    ] {
        f.execute(sql);
    }
    let backup = f.path.join("backup");
    f.admin().export(&backup).unwrap();
    assert_eq!(&fs::read(&backup).unwrap()[..8], b"GDEXP002");
    let restored = Admin::restore(&backup, f.path.join("restored"), Limits::default()).unwrap();
    let mut original = f.admin().sql_session();
    let mut copy = restored.sql_session();
    for (query, expected) in [
        ("SELECT id FROM t WHERE bucket=10 ORDER BY id", vec![]),
        (
            "SELECT id FROM t WHERE bucket>=20 ORDER BY id",
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(3)],
                vec![Value::Int(4)],
            ],
        ),
        (
            "SELECT id FROM t WHERE email='updated'",
            vec![vec![Value::Int(1)]],
        ),
        ("SELECT id FROM t WHERE email='a'", vec![]),
        ("SELECT id FROM t WHERE email='b'", vec![]),
    ] {
        assert_eq!(values(&mut original, query), expected, "original: {query}");
        USED_INDEX_SCAN.set(false);
        assert_eq!(values(&mut copy, query), expected, "restored: {query}");
        assert!(
            USED_INDEX_SCAN.get(),
            "restored must narrow by index: {query}"
        );
    }
    assert!(matches!(
        copy.execute("INSERT INTO t VALUES (5,40,'updated')", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    ));
    // Deleted and superseded unique entries must not survive the export.
    copy.execute("INSERT INTO t VALUES (6,40,'a'),(7,40,'b')", &[], None)
        .unwrap();
}
