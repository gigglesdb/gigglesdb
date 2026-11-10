//! ALTER row-layout changes share the statement's transactional row/index sink.
use super::*;
use relational::Value::{self, Int, Null};

fn rows(f: &Fixture, query: &str) -> Vec<Vec<Value>> {
    f.admin()
        .sql_session()
        .execute(query, &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn lookup(f: &Fixture, query: &str, expected: Vec<Vec<Value>>) {
    USED_INDEX_SCAN.set(false);
    assert_eq!(rows(f, query), expected, "{query}");
    assert!(USED_INDEX_SCAN.get(), "{query} must use the index");
}

#[test]
fn populated_add_drop_and_not_null_reopen_with_indexes() {
    // Exercise both explicit PKs and the hidden heap key, which moves when a
    // logical column is added. Drop a column before both indexed columns.
    for key in ["PRIMARY KEY", ""] {
        let mut f = Fixture::new();
        f.execute(&format!(
            "CREATE TABLE t(id bigint {key}, obsolete text, v bigint UNIQUE)"
        ));
        f.execute("CREATE INDEX iv ON t(v)");
        f.execute("INSERT INTO t VALUES(1,'old',10),(2,'older',20)");
        for (alter, query, expected) in [
            (
                "ADD COLUMN optional bigint",
                "SELECT optional FROM t ORDER BY id",
                vec![vec![Null], vec![Null]],
            ),
            (
                "ADD COLUMN score bigint NOT NULL DEFAULT 7",
                "SELECT score FROM t ORDER BY id",
                vec![vec![Int(7)], vec![Int(7)]],
            ),
            (
                "DROP COLUMN obsolete",
                "SELECT id,v,optional,score FROM t ORDER BY id",
                vec![
                    vec![Int(1), Int(10), Null, Int(7)],
                    vec![Int(2), Int(20), Null, Int(7)],
                ],
            ),
            (
                "ALTER COLUMN v SET NOT NULL",
                "SELECT v FROM t ORDER BY id",
                vec![vec![Int(10)], vec![Int(20)]],
            ),
        ] {
            f.execute(&format!("ALTER TABLE t {alter}"));
            let parts = f.parts();
            assert_eq!(rows(&f, query), expected);
            lookup(&f, "SELECT id FROM t WHERE v=20", vec![vec![Int(2)]]);
            f.reopen();
            assert_eq!(f.parts(), parts);
            assert_eq!(rows(&f, query), expected);
            lookup(&f, "SELECT id FROM t WHERE v=20", vec![vec![Int(2)]]);
        }
        assert_eq!(
            f.admin()
                .sql_session()
                .execute("INSERT INTO t(id,v) VALUES(3,20)", &[], None),
            Err(CoreError::Sql(SqlError::Duplicate))
        );
        assert_eq!(
            f.admin()
                .sql_session()
                .execute("INSERT INTO t(id,v) VALUES(3,NULL)", &[], None),
            Err(CoreError::Sql(SqlError::Null))
        );
        f.execute("INSERT INTO t(id,v) VALUES(3,30)");
        assert_eq!(
            rows(&f, "SELECT score FROM t WHERE id=3"),
            vec![vec![Int(7)]]
        );
        f.execute("UPDATE t SET v=40 WHERE id=2");
        lookup(&f, "SELECT id FROM t WHERE v=20", vec![]);
        lookup(&f, "SELECT id FROM t WHERE v=40", vec![vec![Int(2)]]);
    }
}

#[test]
fn each_alter_rolls_back_catalog_rows_and_indexes() {
    for key in ["PRIMARY KEY", ""] {
        for alter in [
            "ADD COLUMN extra bigint NOT NULL DEFAULT 7",
            "DROP COLUMN v",
            "ALTER COLUMN v SET NOT NULL",
        ] {
            for savepoint in [false, true] {
                let mut f = Fixture::new();
                f.execute(&format!("CREATE TABLE t(id bigint {key},v bigint UNIQUE)"));
                f.execute("CREATE INDEX iv ON t(v)");
                f.execute("INSERT INTO t VALUES(1,10),(2,20)");
                let before = f.parts();
                let expected = vec![vec![Int(1), Int(10)], vec![Int(2), Int(20)]];
                {
                    let mut session = f.admin().sql_session();
                    session.execute("BEGIN", &[], None).unwrap();
                    if savepoint {
                        session.execute("SAVEPOINT s", &[], None).unwrap();
                    }
                    session
                        .execute(&format!("ALTER TABLE t {alter}"), &[], None)
                        .unwrap();
                    session
                        .execute(
                            if savepoint {
                                "ROLLBACK TO s"
                            } else {
                                "ROLLBACK"
                            },
                            &[],
                            None,
                        )
                        .unwrap();
                    assert_eq!(
                        session
                            .execute("SELECT * FROM t ORDER BY id", &[], None)
                            .unwrap()
                            .rows
                            .into_iter()
                            .map(|row| row.values)
                            .collect::<Vec<_>>(),
                        expected,
                        "{alter}, savepoint={savepoint}"
                    );
                    // In particular, undo SET NOT NULL before committing.
                    session
                        .execute("INSERT INTO t VALUES(3,NULL)", &[], None)
                        .unwrap();
                    session
                        .execute("DELETE FROM t WHERE id=3", &[], None)
                        .unwrap();
                    if savepoint {
                        session.execute("COMMIT", &[], None).unwrap();
                    }
                }
                assert_eq!(f.parts(), before, "{alter}, savepoint={savepoint}");
                lookup(&f, "SELECT id FROM t WHERE v=20", vec![vec![Int(2)]]);
                f.reopen();
                assert_eq!(f.parts(), before);
                assert_eq!(rows(&f, "SELECT * FROM t ORDER BY id"), expected);
                lookup(&f, "SELECT id FROM t WHERE v=20", vec![vec![Int(2)]]);
            }
        }
    }
}

#[test]
fn null_validation_and_partial_rewrite_leave_no_changes() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint)");
    f.execute("CREATE INDEX iv ON t(v)");
    f.execute("INSERT INTO t VALUES(1,10),(2,NULL)");
    let before = f.parts();
    for alter in [
        "ALTER COLUMN v SET NOT NULL",
        "ADD COLUMN bad bigint NOT NULL",
        "ADD COLUMN bad bigint NOT NULL DEFAULT NULL",
        "ADD COLUMN extra bigint DEFAULT 7, ALTER COLUMN v SET NOT NULL",
    ] {
        assert_eq!(
            f.admin()
                .sql_session()
                .execute(&format!("ALTER TABLE t {alter}"), &[], None),
            Err(CoreError::Sql(SqlError::Null)),
            "{alter}"
        );
        assert_eq!(f.parts(), before);
    }
    // Fail after the first physical row write, after index maintenance, and
    // recover the statement savepoint inside an explicit transaction.
    {
        let mut session = f.admin().sql_session();
        session.execute("BEGIN", &[], None).unwrap();
        session.execute("SAVEPOINT s", &[], None).unwrap();
        FAIL_AFTER_ROW_WRITE.set(true);
        assert_eq!(
            session.execute("ALTER TABLE t DROP COLUMN v", &[], None),
            Err(CoreError::Cancelled)
        );
        session.execute("ROLLBACK TO s", &[], None).unwrap();
        session.execute("COMMIT", &[], None).unwrap();
    }
    f.reopen();
    assert_eq!(f.parts(), before);
    assert_eq!(
        rows(&f, "SELECT * FROM t ORDER BY id"),
        vec![vec![Int(1), Int(10)], vec![Int(2), Null]]
    );
    lookup(&f, "SELECT id FROM t WHERE v=10", vec![vec![Int(1)]]);
}

#[test]
fn dropped_column_removes_its_indexes_and_unique_constraint() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint UNIQUE,w bigint)");
    f.execute("CREATE INDEX iv ON t(v)");
    f.execute("CREATE INDEX ivw ON t(v,w)");
    f.execute("CREATE INDEX iw ON t(w)");
    f.execute("INSERT INTO t VALUES(1,10,100),(2,20,200)");
    f.execute("ALTER TABLE t DROP COLUMN v");
    let parts = f.parts();
    assert_eq!(parts.indexes.len(), 1);
    assert_eq!(parts.indexes[0].name, "iw");
    f.reopen();
    assert_eq!(f.parts(), parts);
    lookup(&f, "SELECT id FROM t WHERE w=200", vec![vec![Int(2)]]);
    f.execute("CREATE INDEX iv ON t(w)");
    f.execute("ALTER TABLE t ADD COLUMN v bigint");
    f.execute("INSERT INTO t VALUES(3,300,5),(4,400,5)");
    assert_eq!(
        rows(&f, "SELECT v FROM t WHERE id>=3 ORDER BY id"),
        vec![vec![Int(5)], vec![Int(5)]]
    );
}

#[test]
fn same_width_drop_add_uses_new_column_ids_for_indexes() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t(v bigint,w bigint UNIQUE)");
    f.execute("CREATE INDEX iv ON t(v)");
    f.execute("INSERT INTO t VALUES(10,100),(20,200)");
    f.execute("ALTER TABLE t DROP COLUMN v, ADD COLUMN v bigint DEFAULT 7");
    assert_eq!(f.parts().indexes.len(), 1);
    f.reopen();
    assert_eq!(
        rows(&f, "SELECT w,v FROM t ORDER BY w"),
        vec![vec![Int(100), Int(7)], vec![Int(200), Int(7)]]
    );
    lookup(&f, "SELECT v FROM t WHERE w=200", vec![vec![Int(7)]]);
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("INSERT INTO t(w) VALUES(200)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
}

#[test]
fn alter_results_match_legacy() {
    let f = Fixture::new();
    let path = f.path.with_extension("legacy");
    let legacy = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    for sql in [
        "CREATE TABLE t(id bigint PRIMARY KEY,v bigint UNIQUE,w bigint)",
        "INSERT INTO t VALUES(1,10,NULL),(2,20,30)",
        "CREATE INDEX iv ON t(v)",
        "ALTER TABLE t ADD COLUMN a bigint",
        "ALTER TABLE t ADD COLUMN b bigint DEFAULT 7",
        "ALTER TABLE t ADD COLUMN c bigint NOT NULL DEFAULT 9",
        "ALTER TABLE t ADD COLUMN bad bigint NOT NULL",
        "ALTER TABLE t ALTER COLUMN w SET NOT NULL",
        "ALTER TABLE t ALTER COLUMN v SET NOT NULL",
        "ALTER TABLE t DROP COLUMN w",
        "ALTER TABLE t DROP COLUMN v",
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t(id) VALUES(3)",
        "SELECT * FROM t ORDER BY id",
        "CREATE TABLE h(v bigint,w bigint UNIQUE)",
        "INSERT INTO h VALUES(10,100),(20,200)",
        "ALTER TABLE h ADD COLUMN a bigint DEFAULT 7",
        "ALTER TABLE h DROP COLUMN v",
        "ALTER TABLE h ALTER COLUMN w SET NOT NULL",
        "SELECT * FROM h ORDER BY w",
        "INSERT INTO h(w) VALUES(100)",
        "SELECT * FROM h WHERE w=200",
        "ALTER TABLE h DROP COLUMN a, ADD COLUMN a bigint DEFAULT 9",
        "SELECT * FROM h ORDER BY w",
    ] {
        let normalize = |result: Result<relational::SqlResult, CoreError>| {
            result.map(|result| {
                (
                    result
                        .rows
                        .into_iter()
                        .map(|row| row.values)
                        .collect::<Vec<_>>(),
                    result.affected,
                )
            })
        };
        assert_eq!(
            normalize(f.admin().sql_session().execute(sql, &[], None)),
            normalize(legacy.sql_session().execute(sql, &[], None)),
            "{sql}"
        );
    }
    drop(legacy);
    fs::remove_dir_all(path).unwrap();
}
