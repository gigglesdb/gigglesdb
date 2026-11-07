use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, StatementClock, Value},
};
use giggles_pg_types::temporal::SessionZone;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "dd68-cross-type-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn clock(chicago: bool) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(if chicago {
            SessionZone::named("America/Chicago").unwrap()
        } else {
            SessionZone::Fixed(0)
        }),
        wall: None,
    }
}
fn run(s: &mut SqlSession, c: &StatementClock, sql: &str) -> Vec<Vec<Value>> {
    s.execute_at(sql, &[], None, c)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn setup(s: &mut SqlSession, c: &StatementClock) {
    for sql in [
        "CREATE TABLE p(id INT PRIMARY KEY,s SMALLINT,i INT,b BIGINT,n NUMERIC,r REAL,f FLOAT8,d DATE,ts TIMESTAMP,tz TIMESTAMPTZ,tm TIME,ttz TIMETZ,iv INTERVAL)",
        "CREATE TABLE ki(k INT PRIMARY KEY,v INT)",
        "CREATE TABLE kb(k BIGINT PRIMARY KEY,v INT)",
        "CREATE TABLE kn(k NUMERIC PRIMARY KEY,v INT)",
        "CREATE TABLE kf(k FLOAT8 PRIMARY KEY,v INT)",
        "CREATE TABLE q(id INT PRIMARY KEY,i INT,n NUMERIC,f FLOAT8,d DATE,ts TIMESTAMP,tz TIMESTAMPTZ,tm TIME,ttz TIMETZ,iv INTERVAL)",
        "INSERT INTO ki VALUES(1,10),(2,20),(3,30)",
        "INSERT INTO kb VALUES(1,10),(9007199254740993,20),(3,30)",
        "INSERT INTO kn VALUES(1,10),(1.5,15),(3,30),(9007199254740993,40)",
        "INSERT INTO kf VALUES(1,10),(1.5,15),(9007199254740992,20)",
        "INSERT INTO p VALUES(1,1,1,1,1,1,1,'2024-01-02'::date,'2024-01-02 00:00'::timestamp,'2024-01-02 00:00+00'::timestamptz,'03:04:05'::time,'03:04:05+00'::timetz,'03:04:05'::interval)",
        "INSERT INTO p VALUES(2,2,2,9007199254740993,9007199254740993,2,9007199254740992,'infinity'::date,'infinity'::timestamp,'-infinity'::timestamptz,'00:00'::time,'00:00+02'::timetz,'1 day'::interval)",
        "INSERT INTO p VALUES(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
        "INSERT INTO p VALUES(4,1,1,2,1.5,1.5,1.5,'2024-01-01'::date,'2024-01-01 18:00'::timestamp,'2024-01-02 06:00+00'::timestamptz,'18:00'::time,'03:04:05-05'::timetz,'-1 hour'::interval)",
        "INSERT INTO p VALUES(5,3,3,3,3.0,3,3,'2024-01-03'::date,'2024-01-03 00:00'::timestamp,'2024-01-03 06:00+00'::timestamptz,'06:00'::time,'06:00+00'::timetz,'06:00'::interval)",
        "INSERT INTO q VALUES(10,1,1.5,1,'2024-01-02'::date,'2024-01-01 18:00'::timestamp,'2024-01-02 00:00+00'::timestamptz,'03:04:05'::time,'03:04:05+00'::timetz,'06:00'::interval)",
        "INSERT INTO q VALUES(11,3,3,9007199254740992,'2024-01-03'::date,'2024-01-02 00:00'::timestamp,'2024-01-02 06:00+00'::timestamptz,'06:00'::time,'06:00+00'::timetz,'03:04:05'::interval)",
        "INSERT INTO q VALUES(12,2,9007199254740993,1.5,'infinity'::date,'infinity'::timestamp,'-infinity'::timestamptz,NULL,NULL,NULL)",
    ] {
        run(s, c, sql);
    }
}
fn pairs(
    s: &mut SqlSession,
    c: &StatementClock,
    table: &str,
    (left, right): (&str, &str),
    outer: bool,
    group: bool,
    expected: &[(i64, Option<i64>)],
) {
    for reverse in [false, true] {
        let on = if reverse {
            format!("x.{right}=p.{left}")
        } else {
            format!("p.{left}=x.{right}")
        };
        let column = if group { "id" } else { "v" };
        let grouping = if group { " GROUP BY p.id,x.id" } else { "" };
        let sql = format!(
            "SELECT p.id,x.{column} FROM p {}JOIN {table} x ON {on}{grouping} ORDER BY p.id,x.{column}",
            if outer { "LEFT " } else { "" }
        );
        let expected: Vec<_> = expected
            .iter()
            .map(|(a, b)| vec![Value::Int(*a), b.map_or(Value::Null, Value::Int)])
            .collect();
        assert_eq!(run(s, c, &sql), expected, "{sql}");
    }
}
fn inner(
    s: &mut SqlSession,
    c: &StatementClock,
    t: &str,
    l: &str,
    r: &str,
    g: bool,
    e: &[(i64, i64)],
) {
    pairs(
        s,
        c,
        t,
        (l, r),
        false,
        g,
        &e.iter().map(|&(a, b)| (a, Some(b))).collect::<Vec<_>>(),
    );
}
#[test]
fn keyed_numeric_cross_type_joins() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock(false);
    setup(&mut s, &c);
    for (l, t, e) in [
        ("i", "kn", vec![(1, 10), (4, 10), (5, 30)]),
        ("n", "ki", vec![(1, 10), (5, 30)]),
        ("b", "ki", vec![(1, 10), (4, 20), (5, 30)]),
        ("s", "ki", vec![(1, 10), (2, 20), (4, 10), (5, 30)]),
        ("i", "kb", vec![(1, 10), (4, 10), (5, 30)]),
        ("n", "kb", vec![(1, 10), (2, 20), (5, 30)]),
        ("b", "kn", vec![(1, 10), (2, 40), (5, 30)]),
        ("i", "kf", vec![(1, 10), (4, 10)]),
        ("b", "kf", vec![(1, 10), (2, 20)]),
        ("n", "kf", vec![(1, 10), (2, 20), (4, 15)]),
    ] {
        inner(&mut s, &c, t, l, "k", false, &e);
    }
    pairs(
        &mut s,
        &c,
        "ki",
        ("n", "k"),
        true,
        false,
        &[
            (1, Some(10)),
            (2, None),
            (3, None),
            (4, None),
            (5, Some(30)),
        ],
    );
    for (l, t) in [("f", "ki"), ("f", "kn"), ("r", "kn")] {
        for on in [format!("p.{l}=x.k"), format!("x.k=p.{l}")] {
            let sql = format!("SELECT p.id,x.v FROM p JOIN {t} x ON {on}");
            assert_eq!(
                s.execute_at(&sql, &[], None, &c),
                Err(CoreError::Sql(SqlError::Unsupported)),
                "{sql}"
            );
        }
    }
}
#[test]
fn grouped_numeric_and_time_interval_joins() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock(false);
    setup(&mut s, &c);
    for (l, r, e) in [
        ("i", "n", vec![(5, 11)]),
        ("n", "i", vec![(1, 10), (5, 11)]),
        ("b", "n", vec![(2, 12), (5, 11)]),
        ("i", "f", vec![(1, 10), (4, 10)]),
        ("b", "f", vec![(1, 10), (2, 11)]),
        ("n", "f", vec![(1, 10), (2, 11), (4, 12)]),
        ("f", "i", vec![(1, 10), (5, 11)]),
        ("s", "i", vec![(1, 10), (2, 12), (4, 10), (5, 11)]),
        ("tm", "iv", vec![(1, 11), (5, 10)]),
        ("iv", "tm", vec![(1, 10), (5, 11)]),
    ] {
        inner(&mut s, &c, "q", l, r, true, &e);
    }
    pairs(
        &mut s,
        &c,
        "q",
        ("n", "i"),
        true,
        true,
        &[
            (1, Some(10)),
            (2, None),
            (3, None),
            (4, None),
            (5, Some(11)),
        ],
    );
    for on in ["p.i=q.n", "q.n=p.i"] {
        assert_eq!(
            run(
                &mut s,
                &c,
                &format!("SELECT count(*) FROM p JOIN q ON {on} GROUP BY q.id")
            ),
            vec![vec![Value::Int(1)]]
        );
        // A non-key join without grouping keeps the existing bounded-join
        // limit, exactly as a same-type non-key join does.
        let sql = format!("SELECT count(*) FROM p JOIN q ON {on}");
        assert_eq!(
            s.execute_at(&sql, &[], None, &c),
            Err(CoreError::Sql(SqlError::Unbounded)),
            "{sql}"
        );
    }
    for (l, r) in [("i", "d"), ("ttz", "iv")] {
        for on in [format!("p.{l}=q.{r}"), format!("q.{r}=p.{l}")] {
            let sql = format!("SELECT p.id,q.id FROM p JOIN q ON {on} GROUP BY p.id,q.id");
            assert_eq!(
                s.execute_at(&sql, &[], None, &c),
                Err(CoreError::Sql(SqlError::UndefinedFunction)),
                "{sql}"
            );
        }
    }
}
#[test]
fn grouped_temporal_joins_use_statement_zone() {
    for chicago in [false, true] {
        let f = Fixture::new();
        let mut s = f.session();
        let c = clock(chicago);
        setup(&mut s, &c);
        for (l, r, utc, chi) in [
            ("d", "ts", vec![(1, 11), (2, 12)], vec![(1, 11), (2, 12)]),
            ("d", "tz", vec![(1, 10)], vec![(1, 11)]),
            (
                "ts",
                "d",
                vec![(1, 10), (2, 12), (5, 11)],
                vec![(1, 10), (2, 12), (5, 11)],
            ),
            ("ts", "tz", vec![(1, 10)], vec![(1, 11), (4, 10)]),
            ("tz", "d", vec![(1, 10)], vec![(4, 10), (5, 11)]),
            ("tz", "ts", vec![(1, 11)], vec![(1, 10), (4, 11)]),
            ("tm", "ttz", vec![(1, 10), (5, 11)], vec![]),
        ] {
            inner(
                &mut s,
                &c,
                "q",
                l,
                r,
                true,
                if chicago { &chi } else { &utc },
            );
        }
        pairs(
            &mut s,
            &c,
            "q",
            ("d", "tz"),
            true,
            true,
            &[
                (1, Some(if chicago { 11 } else { 10 })),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
            ],
        );
    }
}

#[test]
fn grouped_float_join_preserves_multiple_matching_exact_keys() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock(false);
    setup(&mut s, &c);
    for (table, ty) in [("wide_int", "BIGINT"), ("wide_numeric", "NUMERIC")] {
        run(
            &mut s,
            &c,
            &format!("CREATE TABLE {table}(k {ty} PRIMARY KEY,v INT)"),
        );
        run(
            &mut s,
            &c,
            &format!("INSERT INTO {table} VALUES(9007199254740992,20),(9007199254740993,21)"),
        );
        for on in ["p.f=x.k", "x.k=p.f"] {
            let sql = format!(
                "SELECT p.id,x.v FROM p JOIN {table} x ON {on} GROUP BY p.id,x.v ORDER BY p.id,x.v"
            );
            assert_eq!(
                run(&mut s, &c, &sql),
                vec![
                    vec![Value::Int(2), Value::Int(20)],
                    vec![Value::Int(2), Value::Int(21)]
                ],
                "{sql}"
            );
        }
    }
}

#[test]
fn temporal_cross_type_join_requires_clock_and_zone() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock(false);
    setup(&mut s, &c);
    let mut no_zone = clock(false);
    no_zone.zone = None;
    for on in ["p.d=q.tz", "q.tz=p.d"] {
        let sql = format!("SELECT p.id,q.id FROM p JOIN q ON {on} GROUP BY p.id,q.id");
        assert_eq!(
            s.execute(&sql, &[], None),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{sql}: missing clock"
        );
        assert_eq!(
            s.execute_at(&sql, &[], None, &no_zone),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{sql}: missing zone"
        );
    }
}
