use super::*;
fn impact(db: &str, sql: &str, mode: u8) -> DdlImpact {
    classify(db, sql, &[("db".into(), "zoo".into())], mode)
}
#[test]
fn selected_statement_forms() {
    for sql in [
        "ALTER TABLE zoo ADD a INT",
        "RENAME TABLE other TO zoo, c TO d",
        "RENAME TABLE zoo TO other",
        "DROP TABLE IF EXISTS other, zoo",
        "TRUNCATE zoo",
        "TRUNCATE TABLE db.zoo",
        "CREATE INDEX ix ON zoo(a)",
        "CREATE UNIQUE INDEX ix ON zoo(a)",
        "CREATE FULLTEXT INDEX ix ON zoo(a)",
        "CREATE SPATIAL INDEX ix ON zoo(a)",
        "DROP INDEX ix ON zoo",
        "CREATE INDEX `ON` ON zoo(a)",
        "CREATE TABLE zoo (a INT DEFAULT 'it\\'s')",
        "CREATE TABLE zoo (a INT DEFAULT 'x''y')",
        "CREATE TABLE other LIKE zoo",
        "DROP DATABASE db",
        "DROP SCHEMA IF EXISTS db",
        "ALTER DATABASE db CHARACTER SET utf8mb4",
        "CREATE TRIGGER tr BEFORE INSERT ON zoo FOR EACH ROW SET @a=1",
        "DROP TRIGGER tr ON zoo",
    ] {
        assert!(
            matches!(impact("db", sql, 0), DdlImpact::Selected { tables, .. } if tables == [("db".into(), "zoo".into())]),
            "{sql}"
        );
    }
}
#[test]
fn unselected_forms_and_maintenance() {
    for sql in [
        "ALTER TABLE other ADD a INT",
        "RENAME TABLE other TO renamed, a TO b",
        "DROP TABLE a,b",
        "TRUNCATE TABLE other",
        "CREATE INDEX zoo ON other(a)",
        "DROP INDEX zoo ON other",
        "CREATE TABLE other LIKE a",
        "DROP DATABASE other",
        "ALTER DATABASE other",
        "CREATE TRIGGER zoo BEFORE INSERT ON other FOR EACH ROW SET @a=1",
        "DROP TRIGGER zoo ON other",
        "OPTIMIZE TABLE zoo",
        "ANALYZE TABLE zoo",
        "REPAIR TABLE zoo",
        "GRANT SELECT ON zoo TO user",
        "REVOKE SELECT ON zoo FROM user",
        "CREATE USER zoo",
        "DROP USER zoo",
        "CREATE ROLE zoo",
        "SET @a='zoo'",
        "ALTER TABLE _skedaddle.zoo ADD a INT",
    ] {
        // Unrecognized statements mentioning a selected name fail closed.
        if sql.starts_with("SET") {
            assert!(matches!(impact("db", sql, 0), DdlImpact::Unknown { .. }));
        } else {
            assert_eq!(impact("db", sql, 0), DdlImpact::Unselected, "{sql}");
        }
    }
}
#[test]
fn qualification_comments_quotes_and_case() {
    for sql in [
        "ALTER TABLE db.zoo ADD a INT",
        "/* lead */ ALTER -- comment\n TABLE `db`.`zoo` ADD a VARCHAR(2) DEFAULT 'x'",
        "#lead\nTRUNCATE TABLE db.zoo",
    ] {
        assert!(matches!(impact("", sql, 0), DdlImpact::Selected { .. }));
    }
    assert!(matches!(
        impact("", "ALTER TABLE zoo ADD a INT", 0),
        DdlImpact::Unknown { .. }
    ));
    assert_eq!(
        impact("other", "ALTER TABLE zoo ADD a INT", 0),
        DdlImpact::Unselected
    );
    assert_eq!(
        impact("db", "ALTER TABLE DB.ZOO ADD a INT", 0),
        DdlImpact::Unselected
    );
    for mode in [1, 2] {
        assert!(matches!(
            impact("", "ALTER TABLE DB.ZOO ADD a INT", mode),
            DdlImpact::Selected { .. }
        ));
    }
    assert!(matches!(
        classify(
            "",
            "TRUNCATE `d``b`.`z``oo`",
            &[("d`b".into(), "z`oo".into())],
            0
        ),
        DdlImpact::Selected { .. }
    ));
}
#[test]
fn malformed_and_unknown_fail_closed() {
    for sql in [
        "ALTER zoo ???",
        "ALTER TABLE `zoo",
        "/*!80000 ALTER TABLE zoo ADD a INT */",
        "UNRECOGNIZED zoo",
        "RENAME TABLE a TO b, zoo",
    ] {
        assert!(
            matches!(impact("db", sql, 0), DdlImpact::Unknown { .. }),
            "{sql}"
        );
    }
    assert_eq!(impact("db", "UNRECOGNIZED other", 0), DdlImpact::Unselected);
}
