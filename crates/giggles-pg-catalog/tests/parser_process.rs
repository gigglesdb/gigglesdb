use giggles_pg_catalog::{Error, plan};

// A separate process makes recursive-parser/drop regressions report a test
// failure instead of taking down the parent test executable.
#[test]
fn parser_budget_child() {
    if std::env::var_os("GIGGLES_CATALOG_PARSER_CHILD").is_none() {
        return;
    }
    let plus = format!("SELECT {} FROM pg_class", vec!["1"; 12_000].join("+"));
    assert_eq!(plus.len(), 24_020);
    let cases = [
        plus,
        format!("SELECT 1{} FROM pg_class", "[1]".repeat(4_000)),
        format!(
            "{}SELECT oid FROM pg_class",
            "SELECT oid FROM pg_class UNION ALL ".repeat(1_000)
        ),
        format!("SELECT {}true FROM pg_class", "NOT ".repeat(4_000)),
        format!("SELECT 1{} FROM pg_class", "::int8".repeat(4_000)),
        format!("SELECT (1){} FROM pg_class", ".x".repeat(4_000)),
    ];
    for sql in cases {
        assert_eq!(plan(&sql).unwrap_err(), Error::Limit);
    }
    // Wide output siblings must not hide a deep expression in another item.
    let wide = vec!["1+1 AS x"; 127].join(",");
    for tail in [vec!["1"; 300].join("+"), format!("{}true", "NOT ".repeat(40)), format!("(1){}", ".x".repeat(300))] {
        assert_eq!(plan(&format!("SELECT {wide},{tail} FROM pg_class")).unwrap_err(), Error::Limit);
    }
    // Exercise trees immediately below and above the constructor budget too;
    // the smaller AST must remain safe to visit and destroy after rejection.
    for n in [32, 64, 128, 248, 255, 256, 257] {
        let sql = format!("SELECT {} FROM pg_class", vec!["1"; n].join("+"));
        let _ = plan(&sql);
        let sql = format!("SELECT (1){} FROM pg_class", ".x".repeat(n));
        let _ = plan(&sql);
    }
}
#[test]
fn deep_ast_construction_is_rejected_before_parser_in_subprocess() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "parser_budget_child", "--nocapture"])
        .env("GIGGLES_CATALOG_PARSER_CHILD", "1")
        .status()
        .unwrap();
    assert!(status.success(), "parser child exited {status}");
}
