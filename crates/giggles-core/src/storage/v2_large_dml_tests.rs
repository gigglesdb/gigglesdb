//! Paged DML regressions use real row-store publishers for large fixtures.
use super::*;
use relational::Value::Int;

fn row(f: &Fixture, id: i64) -> Option<engine::rows::Row> {
    let entry = TableEntry {
        def: f.parts().tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    f.db().rows.get(&mut txn, &entry, &[Int(id)]).unwrap()
}

#[test]
fn update_delete_200_000_with_and_without_indexes() {
    for index in [
        "",
        "CREATE INDEX ix ON t(id)",
        "CREATE UNIQUE INDEX ix ON t(v)",
    ] {
        let f = super::large_ddl::large_table(200_000, false, false);
        if index.contains("UNIQUE") {
            // PG18 succeeds for v + 1 with gaps; a dense UNIQUE sequence fails.
            f.execute("UPDATE t SET v = v * 2");
        }
        if !index.is_empty() {
            f.execute(index);
        }
        let mut session = f.admin().sql_session();
        for sql in [
            "UPDATE t SET v = v + 1",
            "UPDATE t SET v = v + 1 WHERE v >= 0",
        ] {
            session.execute("BEGIN", &[], None).unwrap();
            let before = row(&f, 199999).unwrap();
            let result = session.execute(sql, &[], None).unwrap();
            assert_eq!(result.affected, 200_000, "{index}: {sql}");
            // Other sessions see the original row until the statement commits.
            assert_eq!(row(&f, 199999).unwrap(), before);
            session.execute("ROLLBACK", &[], None).unwrap();
            assert_eq!(row(&f, 199999).unwrap(), before);
        }
        if index.contains("UNIQUE") {
            assert_eq!(
                session.execute("UPDATE t SET v = v + 400", &[], None),
                Err(CoreError::Sql(SqlError::Duplicate))
            );
        }
        for sql in ["DELETE FROM t WHERE v >= 0", "DELETE FROM t"] {
            session.execute("BEGIN", &[], None).unwrap();
            assert_eq!(session.execute(sql, &[], None).unwrap().affected, 200_000);
            session.execute("ROLLBACK", &[], None).unwrap();
            assert!(row(&f, 199999).is_some());
        }
        assert_eq!(
            session
                .execute("UPDATE t SET v = v + 1", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        assert_eq!(row(&f, 0).unwrap().values, vec![Int(0), Int(1)]);
        let expected = if index.contains("UNIQUE") {
            399999
        } else {
            200000
        };
        assert_eq!(
            row(&f, 199999).unwrap().values,
            vec![Int(199999), Int(expected)]
        );
        assert_eq!(
            session
                .execute("DELETE FROM t", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        assert!(row(&f, 0).is_none());
        assert!(row(&f, 199999).is_none());
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        assert!(
            txn.scan(
                Keyspace::Index,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
                None
            )
            .unwrap()
            .next()
            .is_none()
        );
    }
}

#[test]
fn dense_unique_increment_has_pg18_immediate_conflict() {
    let f = super::large_ddl::large_table(2050, false, false);
    f.execute("CREATE UNIQUE INDEX ix ON t(v)");
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("UPDATE t SET v = v + 1", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert_eq!(row(&f, 0).unwrap().values, vec![Int(0), Int(0)]);
}

#[test]
fn failure_on_last_page_rolls_back_200_000_updates() {
    let mut f = super::large_ddl::large_table(200_000, false, false);
    f.execute("CREATE UNIQUE INDEX ix ON t(v)");
    let first = row(&f, 0).unwrap();
    let last = row(&f, 199999).unwrap();
    assert_eq!(
        f.admin().sql_session().execute(
            "UPDATE t SET v = CASE WHEN id = 199999 THEN 200001 ELSE v + 200000 END",
            &[],
            None
        ),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert_eq!(row(&f, 0).unwrap(), first);
    assert_eq!(row(&f, 199999).unwrap(), last);
    f.reopen();
    assert_eq!(row(&f, 0).unwrap().values, first.values);
    assert_eq!(row(&f, 199999).unwrap().values, last.values);
}

#[test]
fn returning_exceeds_legacy_row_cap_and_keeps_output_budget() {
    let f = super::large_ddl::large_table(2050, false, true);
    let result = f
        .admin()
        .sql_session()
        .execute("UPDATE t SET v = v + 1 RETURNING id, v", &[], None)
        .unwrap();
    assert_eq!(result.rows.len(), 2050);
    assert_eq!(result.affected, 2050);
    let result = f
        .admin()
        .sql_session()
        .execute("DELETE FROM t RETURNING id", &[], None)
        .unwrap();
    assert_eq!(result.rows.len(), 2050);
    let f = super::large_ddl::large_table(20_000, false, false);
    let before = row(&f, 0).unwrap();
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("UPDATE t SET v = v + 1 RETURNING id, v", &[], None),
        Err(CoreError::Sql(SqlError::Limit))
    );
    assert_eq!(row(&f, 0).unwrap(), before);
}

fn count(f: &Fixture, name: &str) -> usize {
    let entry = TableEntry {
        def: f.parts().tables[name].clone(), heap: false, columns_ext: BTreeMap::new(),
    };
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    f.db().rows.scan(&mut txn, &entry, Bound::Unbounded, Bound::Unbounded,
        Direction::Forward, None).unwrap().map(Result::unwrap).count()
}

#[test]
fn insert_select_200_000_and_late_failure() {
    for index in [
        "",
        "CREATE INDEX ix ON dst(v)",
        "CREATE UNIQUE INDEX ix ON dst(v)",
    ] {
        let f = super::large_ddl::large_table(200_000, false, false);
        f.execute("CREATE TABLE dst (id bigint PRIMARY KEY, v bigint)");
        if !index.is_empty() {
            f.execute(index);
        }
        let mut s = f.admin().sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        assert_eq!(
            s.execute("INSERT INTO dst SELECT id,v FROM t", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        s.execute("ROLLBACK", &[], None).unwrap();
        assert_eq!(count(&f, "dst"), 0);
        // The final input conflicts with the first row after 999 successful pages.
        assert_eq!(
            s.execute(
                "INSERT INTO dst SELECT CASE WHEN id = 199999 THEN 0 ELSE id END,v FROM t",
                &[],
                None
            ),
            Err(CoreError::Sql(SqlError::Duplicate))
        );
        assert_eq!(count(&f, "dst"), 0);
        if index.contains("UNIQUE") {
            assert_eq!(
                s.execute(
                    "INSERT INTO dst SELECT id,CASE WHEN id = 199999 THEN 0 ELSE v END FROM t",
                    &[],
                    None
                ),
                Err(CoreError::Sql(SqlError::Duplicate))
            );
            assert_eq!(count(&f, "dst"), 0);
        }
        assert_eq!(
            s.execute("INSERT INTO dst SELECT id,v FROM t", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        assert_eq!(count(&f, "dst"), 200_000);
    }
}

#[test]
fn large_values_keeps_legacy_admission_and_atomicity() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v bigint)");
    f.execute("CREATE UNIQUE INDEX ix ON t(v)");
    let values = (0..2050)
        .map(|id| format!("({id},{id})"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO t VALUES {values}");
    let mut legacy = relational::Relational::default();
    let p = relational::Prepared::parse(
        "CREATE TABLE t (id bigint PRIMARY KEY, v bigint)",
        1,
        legacy.epoch,
    )
    .unwrap();
    legacy.execute(&p, &[], None).unwrap();
    let p = relational::Prepared::parse(&sql, 1, legacy.epoch).unwrap();
    assert_eq!(legacy.execute(&p, &[], None), Err(SqlError::Limit));
    let mut s = f.admin().sql_session();
    assert_eq!(
        s.execute(&format!("{sql},(2050,0)"), &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert_eq!(count(&f, "t"), 0);
    assert_eq!(s.execute(&sql, &[], None).unwrap().affected, 2050);
}

#[test]
fn insert_200_000_fk_children_and_late_missing_parent() {
    let f = super::large_ddl::large_table(200_000, false, false);
    f.execute("CREATE TABLE p (id bigint PRIMARY KEY)");
    f.execute("INSERT INTO p VALUES (1)");
    f.execute("CREATE TABLE c (id bigint PRIMARY KEY, pid bigint REFERENCES p(id))");
    let mut s = f.admin().sql_session();
    assert_eq!(
        s.execute(
            "INSERT INTO c SELECT id,CASE WHEN id = 199999 THEN 2 ELSE 1 END FROM t",
            &[],
            None
        ),
        Err(CoreError::Sql(SqlError::ForeignKeyViolation))
    );
    assert_eq!(count(&f, "c"), 0);
    // Immediate uniqueness precedes a missing-parent trigger queued earlier.
    assert_eq!(s.execute("INSERT INTO c SELECT CASE WHEN id = 199999 THEN 0 ELSE id END,CASE WHEN id = 0 THEN 2 ELSE 1 END FROM t", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate)));
    assert_eq!(count(&f, "c"), 0);
    assert_eq!(
        s.execute("INSERT INTO c SELECT id,1 FROM t", &[], None)
            .unwrap()
            .affected,
        200_000
    );
    assert_eq!(
        s.execute("SELECT count(*) FROM c", &[], None).unwrap().rows[0].values,
        vec![Int(200_000)]
    );
}

#[test]
#[ignore = "one-million-row DML qualification; run explicitly"]
fn million_row_update() {
    let f = super::large_ddl::large_table(1_000_000, false, false);
    let started = std::time::Instant::now();
    let result = f
        .admin()
        .sql_session()
        .execute("UPDATE t SET v = v + 1", &[], None)
        .unwrap();
    assert_eq!(result.affected, 1_000_000);
    assert_eq!(
        row(&f, 999999).unwrap().values,
        vec![Int(999999), Int(1_000_000)]
    );
    println!("1M UPDATE elapsed={:?}", started.elapsed());
    let memory = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Process -Id {}).PeakWorkingSet64", std::process::id()),
        ])
        .output()
        .unwrap();
    println!(
        "1M process peak RSS bytes={}",
        String::from_utf8_lossy(&memory.stdout).trim()
    );
}
