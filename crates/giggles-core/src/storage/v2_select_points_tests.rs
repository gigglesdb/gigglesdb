use super::*;
thread_local! {
 pub(crate) static SELECT_FILTER_SKIPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
 pub(crate) static SELECT_KEY_CAP: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
 pub(crate) static DISABLE_SELECT_POINTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
 pub(crate) static SELECT_POINT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[test]
fn finite_select_matches_scan() {
    let mut state = Relational::default();
    let mut run = |sql: &str| {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap_or_else(|e| panic!("parse failed ({e:?}), SQL bytes={}, tail={}", sql.len(), &sql[sql.len().saturating_sub(40)..]));
        state.execute_with_cancel(&p, &[], None, || false)
    };
    run("CREATE TABLE t(id bigint PRIMARY KEY, n bigint)").unwrap();
    run("INSERT INTO t VALUES(1,10),(2,20),(3,10)").unwrap();
    let mut cases = vec!["SELECT id FROM t WHERE id=1".to_string(),
        "SELECT id FROM t WHERE id IN(3,1,3,NULL)".into(),
        "SELECT id FROM t WHERE id IN(3,1) AND n=10".into(),
        "SELECT id FROM t WHERE id IN(3,1) OR n=20".into(),
        "SELECT id FROM t WHERE id IN(3,1) ORDER BY id DESC LIMIT 1 OFFSET 1".into(),
        "SELECT count(*) FROM t WHERE id IN(3,1)".into()];
    for n in [1,10,1000,2044] {
        cases.push(format!("SELECT id FROM t WHERE id IN({})", (1..=n).map(|i|i.to_string()).collect::<Vec<_>>().join(",")));
    }
    for sql in cases {
        DISABLE_SELECT_POINTS.set(false);
        SELECT_POINT_READS.set(0);
        let actual = run(&sql).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
        DISABLE_SELECT_POINTS.set(true);
        let expected = run(&sql).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
        DISABLE_SELECT_POINTS.set(false);
        assert_eq!(actual,expected,"{sql}");
    }
}

#[test]
fn select_points_boundary_16_17() {
    let mut state = Relational::default();
    SELECT_KEY_CAP.set(Some(16));
    for n in [16, 17] {
        let sql = format!("SELECT id FROM t WHERE id IN({})", (1..=n).map(|v|v.to_string()).collect::<Vec<_>>().join(","));
        if n == 16 {
            for ddl in ["CREATE TABLE t(id bigint PRIMARY KEY)", "INSERT INTO t VALUES(1),(5),(16),(17)"] {
                let p = Prepared::parse(ddl, 1, state.epoch).unwrap();
                state.execute_with_cancel(&p, &[], None, || false).unwrap();
            }
        }
        let p = Prepared::parse(&sql, 1, state.epoch).unwrap();
        DISABLE_SELECT_POINTS.set(false);
        SELECT_POINT_READS.set(0);
        let actual = state.execute_with_cancel(&p, &[], None, || false).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
        assert_eq!(SELECT_POINT_READS.get(), if n == 16 {16} else {0});
        DISABLE_SELECT_POINTS.set(true);
        let expected = state.execute_with_cancel(&p, &[], None, || false).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    SELECT_KEY_CAP.set(None);
    DISABLE_SELECT_POINTS.set(false);
    SELECT_POINT_READS.set(0);
}

#[test]
fn exact_domain_filter_skips() {
    let mut state = Relational::default();
    for sql in ["CREATE TABLE t(id bigint PRIMARY KEY,n bigint)",
        "INSERT INTO t VALUES(1,5),(2,6),(3,5)",
        "CREATE TABLE c(a bigint,b bigint,PRIMARY KEY(a,b))",
        "INSERT INTO c VALUES(1,1),(1,2),(2,1),(2,2)"] {
        let p = Prepared::parse(sql,1,state.epoch).unwrap();
        state.execute_with_cancel(&p,&[],None,||false).unwrap();
    }
    for (sql, params, skips, points) in [
        ("SELECT id FROM t WHERE id IN (1,2)",vec![],2,true),
        ("SELECT id FROM t WHERE id=$1",vec![Value::Int(1)],1,true),
        ("SELECT * FROM c WHERE a IN (1,2) AND b IN (1,2)",vec![],4,true),
        ("SELECT id FROM t WHERE id IN (1,2) AND n=5",vec![],0,true),
        ("SELECT id FROM t WHERE id IN (1,2) AND id IN (2,3)",vec![],0,true),
        ("SELECT id FROM t WHERE id IN (1,2) OR n=5",vec![],0,false),
        ("SELECT id FROM t WHERE NOT (id IN (1,2))",vec![],0,false),
    ] {
        let p=Prepared::parse(sql,1,state.epoch).unwrap();
        SELECT_FILTER_SKIPS.set(0);
        SELECT_POINT_READS.set(0);
        let actual=state.execute_with_cancel(&p,&params,None,||false).unwrap();
        assert_eq!(SELECT_FILTER_SKIPS.get(),skips,"{sql}");
        assert_eq!(SELECT_POINT_READS.get()>0,points,"{sql}");
        DISABLE_SELECT_POINTS.set(true);
        let expected=state.execute_with_cancel(&p,&params,None,||false).unwrap();
        DISABLE_SELECT_POINTS.set(false);
        assert_eq!(actual.rows,expected.rows,"{sql}");
    }
}
