mod common;
use giggles_core::{
    Admin, CoreError, Limits,
    relational::{SqlError, Value},
};
#[test]
fn server_sql_path_without_icu() {
    if std::env::var_os("GIGGLES_COLLATION_MISSING_ICU_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "server_sql_path_without_icu", "--nocapture"])
            .env("GIGGLES_COLLATION_MISSING_ICU_CHILD", "1")
            .env(
                "GIGGLES_ICU_DIR",
                std::env::temp_dir().join("absent-icu-collation-runtime"),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let a = Admin::create(common::fresh("missing-icu-collation"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for sql in [
        r#"SELECT 'a' COLLATE "C" < 'B'"#,
        r#"SELECT 'a' COLLATE "pg_unicode_fast" < 'B'"#,
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap().rows[0].values,
            vec![Value::Bool(false)]
        );
    }
    s.execute(r#"SELECT 'a' COLLATE "und-x-icu""#, &[], None)
        .unwrap();
    assert_eq!(
        s.execute(r#"SELECT 'a' COLLATE "und-x-icu" < 'B'"#, &[], None)
            .unwrap_err(),
        CoreError::Sql(SqlError::Unsupported)
    );
}
