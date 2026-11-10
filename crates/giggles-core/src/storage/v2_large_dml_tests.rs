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
