//! F3: point-page equivalence, bounds, secondary witnesses, and visibility.
use super::*;
use relational::Value::{self, Int, Null, Text};

struct DisablePoints(bool);
impl DisablePoints {
    fn set(disabled: bool) -> Self { Self(DISABLE_MUTATION_POINTS.replace(disabled)) }
}
impl Drop for DisablePoints {
    fn drop(&mut self) { DISABLE_MUTATION_POINTS.set(self.0); }
}
fn reset_reads() { MUTATION_POINT_READS.set(0); MUTATION_ROWS_READ.set(0); }
fn values(result: relational::SqlResult) -> (u64, Vec<Vec<Value>>) {
    (result.affected, result.rows.into_iter().map(|r| r.values).collect())
}
fn contents(s: &mut SqlSession, table: &str) -> Vec<Vec<Value>> {
    values(s.execute(&format!("SELECT * FROM {table} ORDER BY id"), &[], None).unwrap()).1
}

#[test]
fn pk_mutations_match_scan_and_legacy_constraints_returning_and_indexes() {
    let f = Fixture::new();
    let scan = Fixture::new();
    let mut nonce = [0;16];
    getrandom::fill(&mut nonce).unwrap();
    let legacy_path = std::env::temp_dir().join(format!("f3-legacy-{:032x}",u128::from_be_bytes(nonce)));
    let legacy = Admin::create_engine(&legacy_path, Limits::default(), Engine::Legacy).unwrap();
    let mut sessions = [f.admin().sql_session(), scan.admin().sql_session(), legacy.sql_session()];
    let ddl = [
        "CREATE TABLE p(id bigint PRIMARY KEY)",
        "INSERT INTO p VALUES(1),(2)",
        "CREATE TABLE t(id bigint PRIMARY KEY,email text UNIQUE NOT NULL,n bigint CHECK(n>=0),p bigint REFERENCES p(id),d bigint REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO t VALUES(1,'one',10,1,1),(2,'two',20,2,2),(3,'three',30,1,1)",
        "CREATE TABLE c(id bigint,b bigint,v bigint,PRIMARY KEY(id,b))",
        "INSERT INTO c VALUES(1,1,10),(1,2,20),(2,1,30)",
    ];
    for (i, s) in sessions.iter_mut().enumerate() {
        let _guard = DisablePoints::set(i==1);
        for sql in &ddl { s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}")); }
    }
    let cases: Vec<(&str,Vec<Value>)> = vec![
        ("UPDATE t SET n=n+1 WHERE id=$1 RETURNING id,n",vec![Int(1)]),
        ("UPDATE t SET email='new' WHERE id=1 RETURNING email",vec![]),
        ("UPDATE t SET n=n+1 WHERE id IN(3,1,3,NULL,$1,999) RETURNING id,n",vec![Int(2)]),
        ("UPDATE t SET n=n+1 WHERE id=999 RETURNING id",vec![]),
        ("UPDATE t SET n=n+1 WHERE id=$1 RETURNING id",vec![Null]),
        ("UPDATE t SET n=n+1 WHERE id=NULL RETURNING id",vec![]),
        ("UPDATE t SET n=n+1 WHERE id=$1",vec![Text("invalid".into())]),
        ("UPDATE t SET n=n+1 WHERE id=1 AND n=10 RETURNING id",vec![]),
        ("UPDATE t SET n=n+1 WHERE id=1 AND n=0 RETURNING id",vec![]),
        ("UPDATE t SET n=-1 WHERE id=1",vec![]),
        ("UPDATE t SET email='two' WHERE id=1",vec![]),
        ("UPDATE t SET email='two' WHERE id=1 RETURNING 1/(n-10)",vec![]),
        ("UPDATE t SET email=NULL WHERE id=1",vec![]),
        ("UPDATE t SET p=999 WHERE id=1",vec![]),
        ("UPDATE t SET d=999 WHERE id=1",vec![]),
        ("UPDATE t SET p=2,d=2 WHERE id=1 RETURNING p,d",vec![]),
        ("DELETE FROM t WHERE id IN(3,1,1,NULL,999) RETURNING id,email",vec![]),
        ("DELETE FROM t WHERE id=$1 RETURNING id",vec![Null]),
        ("DELETE FROM t WHERE id=NULL RETURNING id",vec![]),
        ("DELETE FROM t WHERE id=999",vec![]),
        ("DELETE FROM t WHERE id=1 AND n=0 RETURNING id",vec![]),
        ("DELETE FROM t WHERE id=1 AND n=10 RETURNING id",vec![]),
        ("UPDATE c SET v=v+1 WHERE id=$1 AND b=$2 RETURNING id,b,v",vec![Int(1),Int(2)]),
        ("UPDATE c SET v=v+1 WHERE id IN(2,1,1) AND b IN(2,1) RETURNING id,b,v",vec![]),
        ("DELETE FROM c WHERE b=2 AND id=1 RETURNING id,b",vec![]),
    ];
    for (sql,params) in cases {
        let mut observations = Vec::new();
        for (i,s) in sessions.iter_mut().enumerate() {
            let _guard = DisablePoints::set(i==1);
            s.execute("BEGIN",&[],None).unwrap();
            let p = s.prepare(sql).unwrap();
            let result = s.execute_prepared(&p,&params,None).map(values);
            let expected_error = match sql {
                "UPDATE t SET n=n+1 WHERE id=$1" => Some(SqlError::Type),
                "UPDATE t SET n=-1 WHERE id=1" => Some(SqlError::CheckViolation),
                "UPDATE t SET email='two' WHERE id=1" => Some(SqlError::Duplicate),
                "UPDATE t SET email='two' WHERE id=1 RETURNING 1/(n-10)" => Some(SqlError::DivisionByZero),
                "UPDATE t SET email=NULL WHERE id=1" => Some(SqlError::Null),
                "UPDATE t SET p=999 WHERE id=1" => Some(SqlError::ForeignKeyViolation),
                _ => None,
            };
            match expected_error {
                Some(error) => assert_eq!(result, Err(CoreError::Sql(error)), "{sql}"),
                None => assert!(result.is_ok(), "{sql}: {result:?}"),
            }
            // PG18: a NULL equality operand is unknown, so it selects no rows.
            if matches!(sql, "UPDATE t SET n=n+1 WHERE id=$1 RETURNING id"
                | "UPDATE t SET n=n+1 WHERE id=NULL RETURNING id"
                | "DELETE FROM t WHERE id=$1 RETURNING id"
                | "DELETE FROM t WHERE id=NULL RETURNING id") {
                assert_eq!(result, Ok((0, vec![])), "{sql}");
            }
            let contents = if result.is_ok() {
                let rows = contents(s,"t");
                let composite = s.execute("SELECT * FROM c ORDER BY id,b",&[],None).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
                let mut indexed = Vec::new();
                for email in ["one","two","three","new"] {
                    indexed.push(values(s.execute("SELECT id FROM t WHERE email=$1",&[Text(email.into())],None).unwrap()).1);
                }
                Some((rows,composite,indexed))
            } else { None };
            // An initially deferred FK failure must still surface at COMMIT.
            let commit = if sql=="UPDATE t SET d=999 WHERE id=1" {
                let result=s.execute("COMMIT",&[],None).map(values);
                assert_eq!(result,Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
                Some(result)
            } else { None };
            if s.in_transaction() { s.execute("ROLLBACK",&[],None).unwrap(); }
            observations.push((result,contents,commit));
        }
        assert_eq!(observations[0],observations[1],"point/scan: {sql}");
        assert_eq!(observations[0],observations[2],"point/legacy: {sql}");
    }
    // Committed row/index changes, followed by reuse of the vacated witness.
    for (i,s) in sessions.iter_mut().enumerate() {
        let _guard = DisablePoints::set(i==1);
        s.execute("UPDATE t SET email='new' WHERE id=1",&[],None).unwrap();
        s.execute("DELETE FROM t WHERE id IN(2,3)",&[],None).unwrap();
        s.execute("INSERT INTO t VALUES(4,'two',40,1,1)",&[],None).unwrap();
    }
    let final_rows = sessions.iter_mut().map(|s| contents(s,"t")).collect::<Vec<_>>();
    assert_eq!(final_rows[0],final_rows[1]);
    assert_eq!(final_rows[0],final_rows[2]);
    for email in ["one","two","three","new"] {
        let indexed = sessions.iter_mut().map(|s| values(s.execute(
            "SELECT id FROM t WHERE email=$1", &[Text(email.into())], None).unwrap()).1).collect::<Vec<_>>();
        assert_eq!(indexed[0],indexed[1]);
        assert_eq!(indexed[0],indexed[2]);
    }
    drop(sessions);
    drop(legacy);
    fs::remove_dir_all(legacy_path).unwrap();
}

#[test]
fn pk_mutation_reads_only_target_keys_on_5000_rows() {
    let f = super::large_ddl::large_table(5_000,false,false);
    let mut s = f.admin().sql_session();
    let p = s.prepare("UPDATE t SET v=v+1 WHERE id=$1 RETURNING id,v").unwrap();
    reset_reads();
    assert_eq!(s.execute_prepared(&p,&[Int(2500)],None).unwrap().affected,1);
    assert_eq!(MUTATION_POINT_READS.get(),1);
    assert_eq!(MUTATION_ROWS_READ.get(),1);
    {
        let _guard=DisablePoints::set(true);
        reset_reads();
        assert_eq!(s.execute_prepared(&p,&[Int(2500)],None).unwrap().affected,1);
        assert_eq!(MUTATION_POINT_READS.get(),0);
        assert_eq!(MUTATION_ROWS_READ.get(),5000);
    }
    reset_reads();
    assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id IN(4999,0,2500,0) AND v>=0",&[],None).unwrap().affected,3);
    assert_eq!(MUTATION_POINT_READS.get(),3);
    assert_eq!(MUTATION_ROWS_READ.get(),3);
    reset_reads();
    assert_eq!(s.execute("DELETE FROM t WHERE id IN(4999,0,0,9999) RETURNING id",&[],None).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>(),vec![vec![Int(0)],vec![Int(4999)]]);
    assert_eq!(MUTATION_POINT_READS.get(),3);
    assert_eq!(MUTATION_ROWS_READ.get(),2);
    reset_reads();
    // PG18: NULL selects no rows; the conservative path reads existing pages.
    assert_eq!(values(s.execute_prepared(&p,&[Null],None).unwrap()),(0,vec![]));
    assert_eq!(MUTATION_POINT_READS.get(),0);
    assert_eq!(MUTATION_ROWS_READ.get(),4998);
    reset_reads();
    assert!(s.execute_prepared(&p,&[Text("wrong".into())],None).is_err());
    assert_eq!(MUTATION_POINT_READS.get(),0);
    assert_eq!(MUTATION_ROWS_READ.get(),0);
    reset_reads();
    assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id=2500 AND v=-1",&[],None).unwrap().affected,0);
    assert_eq!(MUTATION_POINT_READS.get(),1);
    assert_eq!(MUTATION_ROWS_READ.get(),1);
    reset_reads();
    assert_eq!(s.execute_prepared(&p,&[Int(99999)],None).unwrap().affected,0);
    assert_eq!(MUTATION_POINT_READS.get(),1);
    assert_eq!(MUTATION_ROWS_READ.get(),0);
    reset_reads();
    let list=(1..=201).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
    assert_eq!(s.execute(&format!("UPDATE t SET v=v WHERE id IN({list})"),&[],None).unwrap().affected,201);
    assert_eq!(MUTATION_POINT_READS.get(),0);
    assert_eq!(MUTATION_ROWS_READ.get(),4998);
}

#[test]
fn pk_mutation_keeps_unique_check_precedence_and_errorful_residuals() {
    let mut observed = Vec::new();
    for disabled in [false,true] {
        let _guard = DisablePoints::set(disabled);
        let f = Fixture::new();
        f.execute("CREATE TABLE t(id bigint PRIMARY KEY,u bigint UNIQUE,n bigint CHECK(n>=0))");
        f.execute("INSERT INTO t VALUES(0,0,1),(1,1,0),(400,400,1)");
        let mut s=f.admin().sql_session();
        reset_reads();
        let result=s.execute("UPDATE t SET u=CASE WHEN id=0 THEN 1 ELSE u END,n=CASE WHEN id=400 THEN -1 ELSE n END WHERE id IN(0,400)",&[],None);
        assert_eq!(MUTATION_POINT_READS.get(),0);
        assert_eq!(result,Err(CoreError::Sql(SqlError::Duplicate)));
        observed.push(result);
        // Non-key rows still evaluate an errorful residual before a later IN test.
        assert_eq!(s.execute("DELETE FROM t WHERE 1/n>0 AND id IN(0,400)",&[],None),Err(CoreError::Sql(SqlError::DivisionByZero)));
    }
    assert_eq!(observed[0],observed[1]);
}

#[test]
fn pk_mutation_visibility_own_writes_deleted_rows_and_uncommitted_writer() {
    for disabled in [false,true] {
        let _guard=DisablePoints::set(disabled);
        let f=Fixture::new();
        f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint)");
        f.execute("INSERT INTO t VALUES(1,10),(2,20)");
        let mut s=f.admin().sql_session();
        f.execute("DELETE FROM t WHERE id=1");
        assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id=1",&[],None).unwrap().affected,0);
        s.execute("BEGIN",&[],None).unwrap();
        s.execute("INSERT INTO t VALUES(3,30)",&[],None).unwrap();
        assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id=3 RETURNING v",&[],None).unwrap().rows[0].values,vec![Int(31)]);
        s.execute("DELETE FROM t WHERE id=3",&[],None).unwrap();
        assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id=3",&[],None).unwrap().affected,0);
        s.execute("ROLLBACK",&[],None).unwrap();
        let entry=TableEntry { def:f.parts().tables["t"].clone(),heap:false,columns_ext:BTreeMap::new() };
        let mut writer=f.db().begin(Isolation::ReadCommitted);
        f.db().rows.update(&mut writer,&entry,&[Int(2)],&[(1,Int(99))],None,None,&||false).unwrap();
        // Commit the other transaction after this statement reads its snapshot,
        // before its publisher validates the row witness. Both paths conflict.
        AFTER_MUTATION_PAGE.with_borrow_mut(|hook| *hook=Some(Box::new(move || {
            writer.commit().unwrap();
        })));
        assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id=2",&[],None),Err(CoreError::Sql(SqlError::Conflict)));
        assert!(AFTER_MUTATION_PAGE.with_borrow(|hook|hook.is_none()));
        assert_eq!(contents(&mut s,"t"),vec![vec![Int(2),Int(99)]]);
        // SQL writers serialize on the existing destination lock before choosing
        // their statement snapshot. An uncommitted SQL writer still makes this
        // request wait (and cancel), without reaching either mutation reader.
        let mut writer=f.admin().sql_session();
        writer.execute("BEGIN",&[],None).unwrap();
        writer.execute("UPDATE t SET v=111 WHERE id=2",&[],None).unwrap();
        reset_reads();
        let request=crate::cancellation::Cancellation::new(Some(
            std::time::Instant::now()+std::time::Duration::from_millis(100)));
        assert_eq!(request.scope(||s.execute("UPDATE t SET v=v+1 WHERE id=2",&[],None)),Err(CoreError::Cancelled));
        assert_eq!(MUTATION_POINT_READS.get(),0);
        assert_eq!(MUTATION_ROWS_READ.get(),0);
        writer.execute("ROLLBACK",&[],None).unwrap();
        assert_eq!(s.execute("UPDATE t SET v=v+1 WHERE id=2 RETURNING v",&[],None).unwrap().rows[0].values,vec![Int(100)]);
    }
}

#[test]
fn pk_mutation_preserves_returning_before_a_later_page_check() {
    for disabled in [false,true] {
        let _guard=DisablePoints::set(disabled);
        let f=super::large_ddl::large_table(500,false,false);
        f.execute("ALTER TABLE t ADD CONSTRAINT positive CHECK(v>=0)");
        reset_reads();
        assert_eq!(f.admin().sql_session().execute(
            "UPDATE t SET v=CASE WHEN id=400 THEN -1 ELSE v END WHERE id IN(0,400) RETURNING 1/v",
            &[],None),Err(CoreError::Sql(SqlError::DivisionByZero)));
        assert_eq!(MUTATION_POINT_READS.get(),0);
    }
}

#[test]
fn pk_mutation_composite_points_and_wide_page_fallback() {
    let f=Fixture::new();
    f.execute("CREATE TABLE c(a bigint,b bigint,v bigint,PRIMARY KEY(b,a))");
    f.execute("INSERT INTO c VALUES(1,1,10),(2,1,20),(1,2,30)");
    let mut s=f.admin().sql_session();
    reset_reads();
    assert_eq!(s.execute("UPDATE c SET v=v+1 WHERE a IN(2,1) AND b IN(2,1)",&[],None).unwrap().affected,3);
    assert_eq!(MUTATION_POINT_READS.get(),4);
    assert_eq!(MUTATION_ROWS_READ.get(),3);
    reset_reads();
    assert_eq!(s.execute("UPDATE c SET v=v+1 WHERE b=$1 AND a=$2 RETURNING a,b,v",&[Int(2),Int(1)],None).unwrap().rows[0].values,vec![Int(1),Int(2),Int(32)]);
    assert_eq!(MUTATION_POINT_READS.get(),1);
    assert_eq!(MUTATION_ROWS_READ.get(),1);
    f.execute("CREATE TABLE wide(id bigint PRIMARY KEY,v text,n bigint)");
    for id in 1..=6 {
        s.execute("INSERT INTO wide VALUES($1,$2,0)",&[Int(id),Text("x".repeat(50_000))],None).unwrap();
    }
    // Collecting sparse wide rows can exceed the conservative one-page budget.
    // Falling back before the callback must not double-apply any mutation.
    reset_reads();
    assert_eq!(s.execute("UPDATE wide SET n=n+1 WHERE id IN(1,2,3,4,5,6)",&[],None).unwrap().affected,6);
    assert!(MUTATION_ROWS_READ.get()>6);
    assert_eq!(s.execute("SELECT n FROM wide ORDER BY id",&[],None).unwrap().rows.into_iter().map(|r|r.values).collect::<Vec<_>>(),vec![vec![Int(1)];6]);
}
