use giggles_core::{Admin, CoreError, Limits, relational::SqlError};
fn fresh() -> std::path::PathBuf {
    fresh_at(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
}

fn fresh_at(timestamp: u128) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("dd68-filter-planning");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "filter-{}-{timestamp}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

#[test]
fn fixture_paths_are_unique_with_a_coarse_clock() {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let paths = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..8)
                        .map(|_| {
                            let path = fresh_at(timestamp);
                            let admin = Admin::create(&path, Limits::default()).unwrap();
                            drop(admin);
                            path
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(paths.len(), 64);
}

#[test]
fn filter_planning_pg18_ordered_error_contract() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (query, error) in [
        ("SELECT 1 WHERE TRUE AND abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 AND TRUE", true),
        ("SELECT 1 WHERE FALSE AND abs(-2147483648)=0", false),
        ("SELECT 1 WHERE abs(-2147483648)=0 AND FALSE", true),
        ("SELECT 1 WHERE NULL::boolean AND abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 AND NULL::boolean", true),
        ("SELECT 1 WHERE TRUE OR abs(-2147483648)=0", false),
        ("SELECT 1 WHERE abs(-2147483648)=0 OR TRUE", true),
        ("SELECT 1 WHERE FALSE OR abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 OR FALSE", true),
        ("SELECT 1 WHERE NULL::boolean OR abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 OR NULL::boolean", true),
        ("SELECT 1 WHERE NOT NULL::boolean", false),
        ("SELECT 1 WHERE NOT TRUE", false),
        ("SELECT 1 WHERE NOT FALSE", false),
        ("SELECT 1 WHERE NULL::integer=1", false),
        ("SELECT 1 WHERE coalesce(1,abs(-2147483648))=1", false),
        (
            "SELECT 1 WHERE coalesce(NULL::integer,abs(-2147483648))=1",
            true,
        ),
        (
            "SELECT 1 WHERE FALSE AND (TRUE AND abs(-2147483648)=0)",
            false,
        ),
        (
            "SELECT 1 WHERE (FALSE AND TRUE) AND abs(-2147483648)=0",
            false,
        ),
        (
            "SELECT 1 WHERE NULL::boolean AND FALSE AND abs(-2147483648)=0",
            false,
        ),
    ] {
        let p = s.prepare(query).unwrap();
        let result = s.validate_immutable_constants(&p);
        if error {
            assert_eq!(
                result.unwrap_err(),
                CoreError::Sql(SqlError::Overflow),
                "{query}"
            );
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn filter_parameter_guard_keeps_validation_deferred() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for q in [
        "SELECT 1 WHERE $1::boolean AND abs(-2147483648)=0",
        "SELECT $1::integer WHERE abs(-2147483648)=0",
    ] {
        let p = s.prepare(q).unwrap();
        s.validate_immutable_constants(&p).unwrap();
    }
    let p = s
        .prepare("SELECT 1 WHERE $1::boolean AND abs(-2147483648)=0")
        .unwrap();
    assert!(
        s.execute_prepared(&p, &[giggles_core::relational::Value::Bool(false)], None)
            .unwrap()
            .rows
            .is_empty()
    );
}
#[test]
fn filter_planning_field_null_stability_controls() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE f(i INTEGER PRIMARY KEY)", &[], None)
        .unwrap();
    for (q, expected) in [
        ("SELECT 1 WHERE TRUE AND TRUE", None),
        ("SELECT 1 WHERE TRUE AND FALSE", None),
        ("SELECT 1 WHERE TRUE AND NULL::boolean", None),
        ("SELECT 1 WHERE FALSE AND TRUE", None),
        ("SELECT 1 WHERE FALSE AND FALSE", None),
        ("SELECT 1 WHERE FALSE AND NULL::boolean", None),
        ("SELECT 1 WHERE NULL::boolean AND TRUE", None),
        ("SELECT 1 WHERE NULL::boolean AND FALSE", None),
        ("SELECT 1 WHERE NULL::boolean AND NULL::boolean", None),
        ("SELECT 1 WHERE TRUE OR TRUE", None),
        ("SELECT 1 WHERE TRUE OR FALSE", None),
        ("SELECT 1 WHERE TRUE OR NULL::boolean", None),
        ("SELECT 1 WHERE FALSE OR TRUE", None),
        ("SELECT 1 WHERE FALSE OR FALSE", None),
        ("SELECT 1 WHERE FALSE OR NULL::boolean", None),
        ("SELECT 1 WHERE NULL::boolean OR TRUE", None),
        ("SELECT 1 WHERE NULL::boolean OR FALSE", None),
        ("SELECT 1 WHERE NULL::boolean OR NULL::boolean", None),
        (
            "SELECT 1 FROM f WHERE i=0 AND abs(-2147483648)=0 LIMIT 0",
            Some(SqlError::Overflow),
        ),
        ("SELECT 1 FROM f WHERE FALSE AND abs(i)=0 LIMIT 0", None),
        ("SELECT 1 FROM f WHERE abs(i)=0 AND FALSE LIMIT 0", None),
        (
            "SELECT 1 FROM f WHERE NOT (abs(-2147483648)=0 AND FALSE) LIMIT 0",
            Some(SqlError::Overflow),
        ),
        (
            "SELECT 1 FROM f WHERE date_trunc('nonsense',timestamp '2000-01-01') IS NULL LIMIT 0",
            Some(SqlError::InvalidParameterValue),
        ),
        (
            "SELECT 1 FROM f WHERE date_trunc('nonsense',timestamptz '2000-01-01 UTC') IS NULL LIMIT 0",
            None,
        ),
        (
            "SELECT 1 FROM f WHERE abs(-2147483648) IS NULL LIMIT 0",
            Some(SqlError::Overflow),
        ),
        (
            "SELECT 1 FROM f WHERE abs(-2147483648)=NULL::integer LIMIT 0",
            Some(SqlError::Overflow),
        ),
        (
            "SELECT 1 FROM f WHERE coalesce(NULL::integer,1)=1 LIMIT 0",
            None,
        ),
    ] {
        let p = s.prepare(q).unwrap();
        let v = s.validate_immutable_constants(&p);
        match expected {
            Some(e) => assert_eq!(v.unwrap_err(), CoreError::Sql(e), "{q}"),
            None => v.unwrap(),
        }
    }
}
