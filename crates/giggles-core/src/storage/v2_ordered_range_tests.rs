//! Unlimited ORDER BY retains only matching rows from the statement snapshot.
use super::*;
use relational::{Value::Int, scan};
use std::time::Instant;

fn exercise(count: usize) {
    let f = Fixture::new();
    f.execute("CREATE TABLE big(id bigint PRIMARY KEY, v bigint, h bigint)");
    f.execute("CREATE INDEX big_v ON big(v)");
    f.execute("CREATE INDEX big_h ON big(h)");
    let entry = TableEntry {
        primary_index_birth: None,
        def: f.parts().tables["big"].clone(), heap: false, columns_ext: BTreeMap::new(),
    };
    let started = Instant::now();
    for start in (0..count).step_by(1000) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for id in start..(start + 1000).min(count) {
            f.db().rows.insert(&mut txn, &entry,
                vec![Int(id as i64), Int(id as i64), Int(id as i64)], None, &|| false).unwrap();
        }
        txn.commit().unwrap();
    }
    eprintln!("ordered range rows={count} seed={:?}", started.elapsed());
    let mut s = f.admin().sql_session();
    for (sql, ids) in [
        ("SELECT id,h FROM big WHERE v>=0 AND v<100 ORDER BY v", (0..100).collect::<Vec<_>>()),
        ("SELECT id,h FROM big WHERE v>=0 AND v<100 ORDER BY v DESC", (0..100).rev().collect()),
        ("SELECT id,h FROM big WHERE v>=0 AND v<100 ORDER BY v OFFSET 17", (17..100).collect()),
        ("SELECT id,h FROM big WHERE id>=0 AND id<100 ORDER BY h DESC", (0..100).rev().collect()),
    ] {
        super::super::reduction::SCANNED.set(0);
        let started = Instant::now();
        let expected = ids.into_iter().map(|id| vec![Int(id), Int(id)]).collect::<Vec<_>>();
        assert_eq!(values(&mut s, sql), expected, "{sql}");
        assert_eq!(super::super::reduction::SCANNED.get(), 100, "{sql}");
        eprintln!("ordered range rows={count} {sql}: {:?}", started.elapsed());
    }
    // The cap applies before OFFSET discards rows, and before result row limits.
    let started = Instant::now();
    assert_eq!(s.execute("SELECT id,h FROM big ORDER BY v OFFSET 1000000", &[], None),
        Err(CoreError::Sql(SqlError::Limit)));
    assert_eq!(scan::with_cursor_limit(64 * 1024, || s.execute(
        "SELECT id,h FROM big WHERE v<10000 ORDER BY v OFFSET 10000", &[], None)),
        Err(CoreError::Sql(SqlError::Limit)));
    eprintln!("ordered range rows={count} no-range/retention rejection: {:?}", started.elapsed());
    for (sql, length) in [
        ("SELECT id,h FROM big WHERE h=12345", 1),
        ("SELECT id,h FROM big WHERE v>=0 AND v<100", 100),
    ] {
        let p = s.prepare(sql).unwrap();
        let mut cursor = s.open_cursor(&p, &[]).unwrap();
        let rows = cursor.fetch(200, 1024 * 1024).unwrap();
        assert_eq!(rows.len(), length, "{sql}");
        assert_eq!(rows[0].values, if length == 1 { vec![Int(12345), Int(12345)] }
            else { vec![Int(0), Int(0)] });
        assert!(cursor.fetch(1, 1024).unwrap().is_empty());
    }
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("DELETE FROM big WHERE id=0", &[], None).unwrap();
    s.execute("UPDATE big SET v=200 WHERE id=1", &[], None).unwrap();
    s.execute(&format!("INSERT INTO big VALUES ({count},1,-1)"), &[], None).unwrap();
    let mut expected = vec![vec![Int(count as i64), Int(-1)]];
    expected.extend((2..100).map(|id| vec![Int(id), Int(id)]));
    assert_eq!(values(&mut s, "SELECT id,h FROM big WHERE v>=0 AND v<100 ORDER BY v"), expected);
    assert_eq!(values(&mut s, "SELECT id,h FROM big WHERE id>=0 AND id<3 ORDER BY h DESC"),
        vec![vec![Int(2), Int(2)], vec![Int(1), Int(1)]]);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(values(&mut s, "SELECT id,h FROM big WHERE v<2 ORDER BY v"),
        vec![vec![Int(0), Int(0)], vec![Int(1), Int(1)]]);
}

#[test]
fn million_rows() { exercise(1_000_000); }

#[test]
#[ignore = "5M ordered-range qualification"]
fn five_million_rows() { exercise(5_000_000); }

#[test]
fn small_buffered_and_legacy_parity() {
    let f = Fixture::new();
    let path = f.path.with_extension("legacy");
    let legacy = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    for sql in [
        "CREATE TABLE t(id bigint PRIMARY KEY,v bigint,h text)",
        "INSERT INTO t VALUES (1,NULL,'z'),(2,2,'a'),(3,1,'a'),(4,2,NULL),(5,3,'ä')",
        "CREATE INDEX tv ON t(v)",
    ] {
        f.execute(sql);
        legacy.sql_session().execute(sql, &[], None).unwrap();
    }
    for sql in [
        "SELECT id,h FROM t ORDER BY v,id",
        "SELECT id,h FROM t ORDER BY v DESC NULLS LAST,id",
        "SELECT id,h FROM t ORDER BY h NULLS FIRST,id OFFSET 1",
        "SELECT id,h FROM t WHERE v>=1 AND v<3 ORDER BY h,id",
        "SELECT id,h FROM t WHERE v>=1 AND v<3 ORDER BY h",
        "SELECT id,h FROM t WHERE v>=1 AND v<=2 ORDER BY v DESC,id",
        "SELECT id,h FROM t WHERE v>1 AND v<3 ORDER BY v,id",
        "SELECT id,h FROM t WHERE v<=3 AND v<=2 AND v>=2 ORDER BY v,id",
        "SELECT id,h FROM t WHERE v=2 AND v>2 ORDER BY v,id",
        "SELECT id,h FROM t WHERE v<1 AND v>3 ORDER BY v,id",
        "SELECT id,h FROM t WHERE v=2 AND v=1 ORDER BY v,id",
        "SELECT id,h FROM t WHERE v>=2 AND v<=2 ORDER BY v,id OFFSET 50",
        "SELECT id FROM t ORDER BY 1/(id-id)",
        "SELECT id FROM t ORDER BY nonexistent",
        "SELECT id FROM t ORDER BY h COLLATE \"C\"",
        "SELECT id FROM t ORDER BY v OFFSET -1",
    ] {
        let normalize = |r: Result<relational::SqlResult, CoreError>| r.map(|r|
            r.rows.into_iter().map(|r| r.values).collect::<Vec<_>>());
        assert_eq!(normalize(f.admin().sql_session().execute(sql, &[], None)),
            normalize(legacy.sql_session().execute(sql, &[], None)), "{sql}");
    }
    drop(legacy);
    fs::remove_dir_all(path).unwrap();
}
