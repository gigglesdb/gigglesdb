mod common;

use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value as V, sqlstate};
use std::time::Instant;

fn execute(session: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn migrate(session: &mut SqlSession) {
    for first in (1..=300).step_by(25) {
        execute(session, "BEGIN");
        for n in first..first + 25 {
            execute(
                session,
                &format!(
                    "CREATE TABLE t{n} (id integer NOT NULL, org_id integer NOT NULL, name text NOT NULL, n integer, CONSTRAINT t{n}_pkey PRIMARY KEY(id))"
                ),
            );
            execute(
                session,
                &format!("CREATE INDEX t{n}_org_idx ON t{n}(org_id)"),
            );
            execute(
                session,
                &format!("CREATE UNIQUE INDEX t{n}_org_name ON t{n}(org_id, name)"),
            );
            // Seed referenced rows while earlier migrations are still small.
            if n == 1 {
                execute(session, "INSERT INTO t1 VALUES(1, 10, 'first', 1)");
            } else if n < 300 {
                execute(
                    session,
                    &format!("INSERT INTO t{n} VALUES({n}, {}, 'middle', {n})", n - 1),
                );
            }
            if n > 1 {
                execute(
                    session,
                    &format!(
                        "ALTER TABLE t{n} ADD CONSTRAINT t{n}_previous_fk FOREIGN KEY(org_id) REFERENCES t{}(id) ON DELETE CASCADE ON UPDATE CASCADE",
                        n - 1
                    ),
                );
            }
        }
        execute(session, "COMMIT");
    }
    execute(session, "BEGIN");
    let columns = (1..=20)
        .map(|n| format!("c{n} integer"))
        .collect::<Vec<_>>();
    execute(
        session,
        &format!(
            "CREATE TABLE wide(id integer PRIMARY KEY, {})",
            columns.join(", ")
        ),
    );
    for n in 1..=20 {
        execute(session, &format!("CREATE INDEX wide_{n} ON wide(c{n})"));
    }
    execute(session, "COMMIT");
}

fn check_catalog_and_rows(session: &mut SqlSession) {
    let catalog = session.catalog_metadata().unwrap();
    assert_eq!(catalog.tables.len(), 301);
    assert_eq!(catalog.indexes.len(), 620);
    assert_eq!(
        catalog
            .presentation
            .constraints
            .iter()
            .filter(|c| c.kind == 'f')
            .count(),
        299
    );
    for (sql, id) in [
        ("SELECT id FROM t1 WHERE org_id = 10", 1),
        (
            "SELECT id FROM t300 WHERE org_id = 299 AND name = 'tail'",
            300,
        ),
        ("SELECT id FROM wide WHERE c20 = 400", 2),
    ] {
        assert_eq!(execute(session, sql), [vec![V::Int(id)]], "{sql}");
    }
}

fn migration(backend: Engine) {
    let started = Instant::now();
    let path = common::fresh("sql-catalog-scale");
    {
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let mut session = admin.sql_session();
        migrate(&mut session);
        println!(
            "{backend:?} catalog and seed rows staged in {:?}",
            started.elapsed()
        );
        execute(&mut session, "BEGIN");
        execute(
            &mut session,
            "INSERT INTO t300 VALUES(300, 299, 'tail', 300)",
        );
        execute(
            &mut session,
            "INSERT INTO wide(id, c1, c20) VALUES(1, 10, 200), (2, 20, 400)",
        );
        execute(&mut session, "COMMIT");
        for (sql, expected) in [
            ("INSERT INTO t300 VALUES(301, 299, 'tail', 1)", "23505"),
            ("INSERT INTO t300 VALUES(301, 999, 'orphan', 1)", "23503"),
        ] {
            assert_eq!(
                sqlstate(&session.execute(sql, &[], None).unwrap_err()),
                expected,
                "{sql}"
            );
        }
        check_catalog_and_rows(&mut session);
        if backend == Engine::Legacy {
            let export_dir = common::fresh("sql-catalog-scale-export");
            std::fs::create_dir(&export_dir).unwrap();
            let export = export_dir.join("snapshot");
            admin.export(&export).unwrap();
            println!(
                "Legacy 301-table export bytes {}",
                export.metadata().unwrap().len()
            );
            for file in export_dir.read_dir().unwrap() {
                std::fs::remove_file(file.unwrap().path()).unwrap();
            }
            std::fs::remove_dir(&export_dir).unwrap();
        }
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    check_catalog_and_rows(&mut session);
    execute(&mut session, "DROP TABLE wide");
    for last in (25..=300).rev().step_by(25) {
        execute(&mut session, "BEGIN");
        for n in (last - 24..=last).rev() {
            execute(&mut session, &format!("DROP TABLE t{n}"));
        }
        execute(&mut session, "COMMIT");
    }
    assert!(admin.catalog().unwrap().1.is_empty());
    for name in ["t1", "t300", "wide"] {
        execute(
            &mut session,
            &format!("CREATE TABLE {name}(id integer PRIMARY KEY)"),
        );
        execute(&mut session, &format!("INSERT INTO {name} VALUES(1)"));
    }
    for first in (1..=150).step_by(25) {
        execute(&mut session, "BEGIN");
        for n in first..first + 25 {
            execute(&mut session, &format!("CREATE SCHEMA s{n}"));
            execute(
                &mut session,
                &format!("CREATE TABLE s{n}.items(id integer PRIMARY KEY)"),
            );
        }
        execute(&mut session, "COMMIT");
    }
    drop(session);
    drop(admin);

    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let catalog = session.catalog_metadata().unwrap();
    assert_eq!(catalog.schemas.schemas.len(), 150);
    assert_eq!(catalog.schemas.tables.len(), 150);
    assert_eq!(catalog.tables.len(), 153);
    execute(&mut session, "INSERT INTO s150.items VALUES(42)");
    assert_eq!(
        execute(&mut session, "SELECT id FROM s150.items"),
        [vec![V::Int(42)]]
    );
    assert_eq!(
        execute(&mut session, "SELECT id FROM t300"),
        [vec![V::Int(1)]]
    );
    println!("{backend:?} catalog-scale elapsed {:?}", started.elapsed());
}

#[test]
fn legacy_catalog_migration() {
    migration(Engine::Legacy);
}

#[test]
#[cfg(feature = "engine-rocksdb")]
fn v2_catalog_migration() {
    migration(Engine::V2);
}
