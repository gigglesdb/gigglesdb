use super::*;
use relational::{DISABLE_SELECT_POINTS, SELECT_POINT_READS, Value};

fn rows(s: &mut SqlSession, p: &relational::Prepared, params: &[Value], disabled: bool) -> Vec<Vec<Value>> {
    DISABLE_SELECT_POINTS.set(disabled);
    SELECT_POINT_READS.set(0);
    let result = s.execute_prepared(p, params, None).unwrap().rows.into_iter().map(|r|r.values).collect();
    if !disabled {
        // NULL key comparisons deliberately preserve the scan safety fallback.
        if params.contains(&Value::Null) { assert_eq!(SELECT_POINT_READS.get(), 0); }
        else { assert!(SELECT_POINT_READS.get() > 0); }
    }
    DISABLE_SELECT_POINTS.set(false);
    SELECT_POINT_READS.set(0);
    result
}
#[test]
fn select_points_prepared_composite_order_and_own_writes() {
    use Value::{Int, Null};
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute("CREATE TABLE t(id bigint primary key, n bigint, s text)", &[], None).unwrap();
    s.execute("CREATE TABLE c(a text, b bigint, v bigint, primary key(a,b))", &[], None).unwrap();
    for i in 1..=50 { s.execute(&format!("INSERT INTO t VALUES({i},{i},'z')"), &[], None).unwrap(); }
    for i in 1..=20 { s.execute(&format!("INSERT INTO c VALUES('x',{i},{i})"), &[], None).unwrap(); }
    let p = s.prepare("SELECT id,n FROM t WHERE id IN ($1,$2,$3)").unwrap();
    for params in [vec![Int(3),Int(1),Int(2)], vec![Null,Int(5),Int(999)], vec![Int(50),Int(50),Int(10)]] {
        assert_eq!(rows(&mut s,&p,&params,false), rows(&mut s,&p,&params,true));
    }
    for sql in ["SELECT * FROM c WHERE a='x' AND b IN (1,2,3)", "SELECT * FROM t WHERE id IN (5,6,7) ORDER BY id DESC"] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(rows(&mut s,&p,&[],false), rows(&mut s,&p,&[],true));
    }
    for sql in ["BEGIN", "DELETE FROM t WHERE id=6", "INSERT INTO t VALUES (1000,1,'z')"] { s.execute(sql,&[],None).unwrap(); }
    let p = s.prepare("SELECT id FROM t WHERE id IN (5,6,1000)").unwrap();
    for disabled in [false,true] { assert_eq!(rows(&mut s,&p,&[],disabled),vec![vec![Int(5)],vec![Int(1000)]]); }
    s.execute("ROLLBACK",&[],None).unwrap();
}
#[test]
fn oid_zero_decoded_bigint_large_in_uses_points() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute("CREATE TABLE t(id bigint PRIMARY KEY)", &[], None).unwrap();
    let insert=format!("INSERT INTO t VALUES {}",(1..=2000).map(|i|format!("({i})")).collect::<Vec<_>>().join(","));
    s.execute(&insert,&[],None).unwrap();
    let sql=format!("SELECT id FROM t WHERE id IN ({})",(1..=1000).map(|i|format!("${i}")).collect::<Vec<_>>().join(","));
    let p=s.prepare(&sql).unwrap();
    let params=(1..=1000).map(Value::Int).collect::<Vec<_>>();
    DISABLE_SELECT_POINTS.set(false);
    SELECT_POINT_READS.set(0);
    assert_eq!(s.execute_prepared(&p,&params,None).unwrap().rows.len(),1000);
    assert_eq!(SELECT_POINT_READS.get(),1000);
    SELECT_POINT_READS.set(0);
}
#[test]
fn cursor_points_chunked_domains() {
    use relational::SELECT_FILTER_SKIPS;
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute("CREATE TABLE t(id bigint PRIMARY KEY, v bigint)", &[], None).unwrap();
    s.execute(&format!("INSERT INTO t VALUES {}", (1..=1000).map(|i| format!("({i},{})", i%5)).collect::<Vec<_>>().join(",")), &[], None).unwrap();
    let params = (501..=1500).map(Value::Int).collect::<Vec<_>>();
    let list = (1..=1000).map(|i|format!("${i}")).collect::<Vec<_>>().join(",");
    for own in [false, true] {
        if own { s.execute("BEGIN", &[], None).unwrap(); s.execute("INSERT INTO t VALUES(1001,3)", &[], None).unwrap(); }
        for suffix in ["", " ORDER BY id DESC", " LIMIT 10 OFFSET 5", " AND v = 3"] {
            let p = s.prepare(&format!("SELECT id FROM t WHERE id IN ({list}){suffix}")).unwrap();
            let mut results = Vec::new();
            for disabled in [false, true] {
                DISABLE_SELECT_POINTS.set(disabled);
                SELECT_POINT_READS.set(0);
                SELECT_FILTER_SKIPS.set(0);
                let mut cursor = s.open_cursor(&p, &params).unwrap();
                let mut rows = Vec::new();
                loop { let chunk = cursor.fetch(7, 100000).unwrap(); if chunk.is_empty() { break; } rows.extend(chunk.into_iter().map(|r|r.values)); }
                // LIMIT/OFFSET queries keep the scan for now (their WHERE is wrapped with the clause bundle); results must match.
                if !disabled && !suffix.contains("LIMIT") { assert!(SELECT_POINT_READS.get()>0, "own={own} suffix={suffix}"); assert_eq!(SELECT_FILTER_SKIPS.get()>0, !suffix.contains("v =")); }
                results.push(rows);
            }
            assert_eq!(results[0], results[1]);
        }
        if own { s.execute("ROLLBACK", &[], None).unwrap(); }
    }
    DISABLE_SELECT_POINTS.set(false);
}
