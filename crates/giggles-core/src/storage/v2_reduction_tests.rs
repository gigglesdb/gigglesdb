use super::*;
use relational::{Value::Int, scan};
use std::time::Instant;

fn seed(count: usize) -> Fixture {
    let f = Fixture::new();
    f.execute("CREATE TABLE big(id bigint PRIMARY KEY, v bigint, h bigint)");
    let entry = TableEntry {
        primary_index_birth: None,
        def: f.parts().tables["big"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    for start in (0..count).step_by(1000) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for id in start..(start + 1000).min(count) {
            f.db()
                .rows
                .insert(
                    &mut txn,
                    &entry,
                    vec![Int(id as i64), Int(id as i64 % 4), Int(id as i64)],
                    None,
                    &|| false,
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }
    f
}

fn exercise(count: usize) {
    let started = Instant::now();
    let f = seed(count);
    eprintln!("reduction rows={count} seed={:?}", started.elapsed());
    let mut s = f.admin().sql_session();
    for (sql, expected) in [
        (
            "SELECT count(*),count(h),min(h),max(h) FROM big".to_owned(),
            vec![vec![
                Int(count as i64),
                Int(count as i64),
                Int(0),
                Int(count as i64 - 1),
            ]],
        ),
        (
            "SELECT count(*),max(h) FROM big WHERE id < 100000".to_owned(),
            vec![vec![Int(100000), Int(99999)]],
        ),
        (
            "SELECT id FROM big ORDER BY h DESC LIMIT 20 OFFSET 5".to_owned(),
            (count as i64 - 25..count as i64 - 5)
                .rev()
                .map(|id| vec![Int(id)])
                .collect(),
        ),
        (
            "SELECT v % 4,count(*),min(h),max(h) FROM big GROUP BY v % 4 ORDER BY v % 4".to_owned(),
            (0..4)
                .map(|v| {
                    vec![
                        Int(v),
                        Int(count as i64 / 4),
                        Int(v),
                        Int(count as i64 - 4 + v),
                    ]
                })
                .collect(),
        ),
    ] {
        let started = Instant::now();
        assert_eq!(values(&mut s, &sql), expected, "{sql}");
        eprintln!("reduction rows={count} {sql}: {:?}", started.elapsed());
    }
    let started = Instant::now();
    for (sql, expected) in [
        ("SELECT sum(h) FROM big WHERE id < 10000", "49995000"),
        ("SELECT avg(h) FROM big WHERE id < 10000", "4999.5"),
    ] {
        assert_eq!(
            values(&mut s, sql),
            vec![vec![relational::Value::Numeric(
                giggles_pg_types::Numeric::parse(expected).unwrap()
            )]]
        );
    }
    eprintln!(
        "reduction rows={count} sum/avg 10k: {:?}",
        started.elapsed()
    );
    // R2-PGLIMITS-1/4: v2 SUM/AVG of a column and large GROUP BY no longer hit a
    // statement-wide cap (PostgreSQL streams aggregates and spills groups).
    let n = count as i128;
    let small = n.min(100_000);
    for (sql, expected) in [
        ("SELECT sum(h) FROM big".to_owned(), (n * (n - 1) / 2).to_string()),
        ("SELECT sum(h) FROM big WHERE id < 100000".to_owned(), (small * (small - 1) / 2).to_string()),
    ] {
        assert_eq!(
            values(&mut s, &sql),
            vec![vec![relational::Value::Numeric(
                giggles_pg_types::Numeric::parse(&expected).unwrap()
            )]],
            "{sql}"
        );
    }
    for sql in ["SELECT avg(h) FROM big", "SELECT avg(h) FROM big WHERE id < 100000"] {
        assert!(s.execute(sql, &[], None).is_ok(), "{sql}");
    }
    let grouped = scan::with_cursor_limit(128 * 1024, || s.execute(
        "SELECT id,count(*) FROM big GROUP BY id",
        &[],
        None
    ))
    .unwrap();
    assert_eq!(grouped.rows.len(), count);
    s.execute("BEGIN", &[], None).unwrap();
    s.execute(
        &format!("INSERT INTO big VALUES ({count},0,{count})"),
        &[],
        None,
    )
    .unwrap();
    s.execute("DELETE FROM big WHERE id=0", &[], None).unwrap();
    s.execute("UPDATE big SET h=-1 WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(
        values(&mut s, "SELECT count(*),min(h),max(h) FROM big"),
        vec![vec![Int(count as i64), Int(-1), Int(count as i64)]]
    );
    assert_eq!(
        values(&mut s, "SELECT id FROM big ORDER BY h DESC LIMIT 1"),
        vec![vec![Int(count as i64)]]
    );
    s.execute("ROLLBACK", &[], None).unwrap();
    let cancellation = crate::cancellation::Cancellation::new(Some(
        Instant::now() + std::time::Duration::from_millis(20),
    ));
    let result = cancellation.scope(|| s.execute("SELECT count(*) FROM big", &[], None));
    assert!(
        matches!(
            result,
            Err(CoreError::Cancelled | CoreError::Sql(SqlError::Aborted))
        ),
        "{result:?}"
    );
}

#[test]
fn million_rows() {
    exercise(1_000_000);
}

#[test]
#[ignore = "5M scale qualification"]
fn five_million_rows() {
    exercise(5_000_000);
}

#[test]
fn buffered_parity_and_secondary_range() {
    let f = seed(32);
    f.execute("UPDATE big SET h=NULL WHERE id=0");
    f.execute("CREATE INDEX by_h ON big(h)");
    let parts = f.parts();
    let mut image =
        relational::Relational::catalog_image(parts.tables, parts.ext, u64::MAX, 1).unwrap();
    for id in 0..32 {
        let p = relational::Prepared::parse(
            &format!(
                "INSERT INTO big VALUES ({id},{},{})",
                id % 4,
                if id == 0 {
                    "NULL".to_owned()
                } else {
                    id.to_string()
                }
            ),
            0,
            0,
        )
        .unwrap();
        image.execute(&p, &[], None).unwrap();
    }
    let mut s = f.admin().sql_session();
    for sql in [
        "SELECT count(*),count(h),sum(h),avg(h),min(h),max(h) FROM big WHERE h>=8",
        "SELECT v,count(*),min(h),max(h) FROM big WHERE h<16 GROUP BY v ORDER BY v",
        "SELECT id,h FROM big WHERE h>3 ORDER BY h DESC LIMIT 3 OFFSET 2",
        "SELECT count(*),count(h),min(h),max(h) FROM big",
        "SELECT count(*) FROM big WHERE id<0",
        "SELECT count(*) FROM big LIMIT 0",
        "SELECT sum(1/(h-h)) FROM big",
    ] {
        let p = relational::Prepared::parse(sql, 0, 0).unwrap();
        let expected = image.execute(&p, &[], None).map_err(CoreError::Sql);
        let result = s.execute(sql, &[], None);
        assert_eq!(result, expected, "{sql}");
    }
    assert!(super::super::v2_sql::USED_INDEX_SCAN.get());
}

#[test]
fn exists_probes_share_statement_snapshot() {
    use super::super::v2_sql::{AFTER_TABLE_SCAN, USED_INDEX_SCAN};
    struct Guard;
    impl Drop for Guard { fn drop(&mut self) { AFTER_TABLE_SCAN.with_borrow_mut(|h| *h = None); } }
    let f = Fixture::new();
    for sql in ["CREATE TABLE users(id int PRIMARY KEY)", "INSERT INTO users VALUES(1)",
        "CREATE TABLE orders(id int PRIMARY KEY,user_id int)",
        "CREATE INDEX orders_user ON orders(user_id)", "INSERT INTO orders VALUES(1,1)"] { f.execute(sql); }
    let admin = f.admin().clone();
    let mut first = true;
    AFTER_TABLE_SCAN.with_borrow_mut(|h| *h = Some(Box::new(move |name| {
        if name == "users" && first {
            first = false;
            let admin = admin.clone();
            std::thread::spawn(move || { admin.sql_session().execute("DELETE FROM orders", &[], None).unwrap(); }).join().unwrap();
        }
    })));
    let guard = Guard;
    USED_INDEX_SCAN.set(false);
    let mut s = f.admin().sql_session();
    let sql = "SELECT id FROM users u WHERE EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)";
    assert_eq!(values(&mut s, sql), vec![vec![Int(1)]]);
    assert!(USED_INDEX_SCAN.get());
    drop(guard);
    assert!(values(&mut s, sql).is_empty());
}
