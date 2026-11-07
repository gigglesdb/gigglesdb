fn fresh(name: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("dd68-immutable-planning");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{name}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}
use giggles_core::{
    Admin, CoreError, Limits,
    relational::{SqlError as E, Value},
};
#[test]
fn immutable_planning_matches_pg18_overload_and_short_circuit_controls() {
    let a = Admin::create(fresh("immutable-planning"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (sql, expected) in [
        ("SELECT make_time(24,1,0)", Some(E::DatetimeFieldOverflow)),
        (
            "SELECT make_time(24,1,0) LIMIT 0",
            Some(E::DatetimeFieldOverflow),
        ),
        (
            "SELECT date_trunc('nonsense',timestamp '2000-01-01')",
            Some(E::InvalidParameterValue),
        ),
        (
            "SELECT extract('nonsense' from timestamp '2000-01-01')",
            Some(E::InvalidParameterValue),
        ),
        (
            "SELECT date_trunc('nonsense',timestamptz '2000-01-01 UTC')",
            None,
        ),
        (
            "SELECT extract('nonsense' from timestamptz '2000-01-01 UTC')",
            None,
        ),
        ("SELECT make_timestamptz(2000,13,1,0,0,0,'UTC')", None),
        ("SELECT coalesce(1,abs(-2147483648))", None),
        (
            "SELECT coalesce(NULL::integer,abs(-2147483648))",
            Some(E::Overflow),
        ),
        ("SELECT coalesce($1::integer,abs(-2147483648))", None),
        ("SELECT abs($1::integer)", None),
        ("SELECT make_time(NULL,1,0)", None),
    ] {
        let p = s.prepare(sql).unwrap();
        let r = s.validate_immutable_constants(&p);
        match expected {
            Some(e) => assert_eq!(r.unwrap_err(), CoreError::Sql(e), "{sql}"),
            None => r.unwrap(),
        }
    }
    let p = s
        .prepare("SELECT coalesce($1::integer,abs(-2147483648))")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[Value::Int(1)], None).unwrap().rows[0].values,
        vec![Value::Int(1)]
    );
}
#[test]
fn planning_poisoning_rollback_domain_and_schema_checks() {
    let a = Admin::create(fresh("planning-authority"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    let p = s.prepare("SELECT make_time(24,1,0)").unwrap();
    assert!(s.validate_immutable_constants(&p).is_err());
    let q = s.prepare("SELECT 1").unwrap_err();
    assert_eq!(q, CoreError::Sql(E::Aborted));
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(s.validate_immutable_constants(&p).is_err());
    let p = s.prepare("SELECT 1").unwrap();
    let b = Admin::create(fresh("planning-other-domain"), Limits::default()).unwrap();
    assert_eq!(
        b.sql_session()
            .validate_immutable_constants(&p)
            .unwrap_err(),
        CoreError::Sql(E::StaleSchema)
    );
    s.execute(
        "CREATE TABLE planning_table(id BIGINT PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    assert_eq!(
        s.validate_immutable_constants(&p).unwrap_err(),
        CoreError::Sql(E::StaleSchema)
    );
}
