use giggles_core::relational::Prepared;

#[test]
fn negative_scalar_classifier_preserves_authority_boundaries() {
    for (sql, expected) in [
        ("SELECT private_probe('never_log_this_secret')", true),
        ("SELECT pg_catalog.private_probe('x')", true),
        ("SELECT lower(private_probe('x'))", true),
        ("SELECT 1 WHERE private_probe('x')=1", true),
        ("SELECT 1 ORDER BY private_probe('x')", true),
        ("SELECT private_probe('sum(abs(-1))')", true),
        ("SELECT 1", false),
        ("SELECT lower('X')", false),
        ("SELECT abs(-1)", false),
        ("SELECT abs($1::integer)", false),
        (
            "SELECT date_trunc('nonsense',timestamptz '2000-01-01 UTC')",
            false,
        ),
        ("SELECT SUM(abs(-1))", false),
        ("SELECT COUNT(lower('x'))", false),
        ("SELECT MIN(private_probe('x'))", false),
        ("SELECT MAX(lower('x'))", false),
        ("SELECT COUNT(*)", false),
        ("SELECT sum(private_probe('x'))", false),
        ("SELECT coalesce(sum(abs(-1)),0)", false),
        ("SELECT private_probe(sum(abs(-1)))", false),
        ("SELECT pg_catalog.sum(abs(-1))", false),
        ("SELECT \"sum\"(abs(-1))", false),
        ("SELECT lower(id)", false),
        ("SELECT private_probe(id)", false),
        ("SELECT lower(body) FROM app WHERE id=1", false),
        ("SELECT private_probe(body) FROM other WHERE id=1", false),
        ("SELECT a.id FROM app a JOIN other b ON a.id=b.id", false),
        ("EXPLAIN SELECT private_probe('x')", false),
        ("SELECT lower((SELECT body FROM other))", false),
        (
            "WITH x AS (SELECT body FROM other) SELECT lower(body) FROM x",
            false,
        ),
        ("SELECT lower(", false),
        ("SELECT evil.lower('x')", false),
        ("SELECT substring(private_probe('x') from 1 for 1)", true),
        (
            "SELECT substring('abc' from private_probe('x') for 1)",
            true,
        ),
        (
            "SELECT substring('abc' from 1 for private_probe('x'))",
            true,
        ),
        ("SELECT substring('abc' from id for 1)", false),
        ("SELECT substring('abc' from sum(abs(-1)) for 1)", false),
    ] {
        assert_eq!(
            Prepared::rejects_unsupported_table_free_scalar(sql),
            expected,
            "{sql}"
        );
    }
}
