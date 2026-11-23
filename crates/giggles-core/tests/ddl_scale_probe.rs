mod common;

use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value as V, sqlstate};
#[cfg(feature = "engine-rocksdb")]
use std::time::Duration;
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

fn table(session: &mut SqlSession, name: &str, unique_composite: bool) {
    execute(session, &table_sql(name));
    execute(
        session,
        &format!("CREATE INDEX {name}_org_idx ON {name}(org_id)"),
    );
    let unique = if unique_composite { "UNIQUE " } else { "" };
    execute(
        session,
        &format!("CREATE {unique}INDEX {name}_org_name ON {name}(org_id, name)"),
    );
}

fn table_sql(name: &str) -> String {
    format!(
        "CREATE TABLE {name} (id integer NOT NULL, org_id integer NOT NULL, name text NOT NULL, n integer, CONSTRAINT {name}_pkey PRIMARY KEY(id))"
    )
}

fn grow(session: &mut SqlSession, start: usize, end: usize) {
    for first in (start..end).step_by(25) {
        execute(session, "BEGIN");
        for n in first..end.min(first + 25) {
            // At 348 total tables the UNIQUE readiness bound exceeds MAX_WORK.
            table(session, &format!("t{n}"), n < 347);
        }
        execute(session, "COMMIT");
    }
}

const ADD_INDEX: &str = "CREATE INDEX probe_n_idx ON probe(n)";
const DROP_INDEX: &str = "DROP INDEX probe_n_idx";
const ADD_COLUMN: &str = "ALTER TABLE probe ADD COLUMN extra integer";
const DROP_COLUMN: &str = "ALTER TABLE probe DROP COLUMN extra";
const ADD_FK: &str = "ALTER TABLE probe ADD CONSTRAINT probe_fk FOREIGN KEY(org_id) REFERENCES probe(id) ON DELETE CASCADE ON UPDATE CASCADE";
const DROP_FK: &str = "ALTER TABLE probe DROP CONSTRAINT probe_fk";

fn measure(session: &mut SqlSession, sql: &str, undo: &str) -> [f64; 5] {
    std::array::from_fn(|_| {
        let start = Instant::now();
        execute(session, sql);
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        execute(session, undo);
        ms
    })
}

fn print_samples(engine: Engine, size: usize, label: &str, mut samples: [f64; 5]) {
    samples.sort_by(f64::total_cmp);
    println!(
        "DDL {engine:?} {size} {label} median_ms={:.6} samples_ms={samples:?}",
        samples[2]
    );
}

fn matrix(engine: Engine) {
    let path = common::fresh("ddl-scale-probe");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        let mut built = 0;
        for size in [0, 50, 100, 200, 400] {
            if size > 0 && built == 0 {
                table(&mut session, "probe", true);
                built = 1;
            }
            grow(&mut session, built, size);
            built = size;
            let create = table_sql("added");
            print_samples(
                engine,
                size,
                "create_table",
                measure(&mut session, &create, "DROP TABLE added"),
            );

            // At zero the target-dependent statements need one temporary table.
            if size == 0 {
                table(&mut session, "probe", true);
            }
            for (label, sql, undo) in [
                ("create_index", ADD_INDEX, DROP_INDEX),
                ("add_column", ADD_COLUMN, DROP_COLUMN),
                ("add_fk", ADD_FK, DROP_FK),
            ] {
                print_samples(engine, size, label, measure(&mut session, sql, undo));
            }
            if size == 0 {
                execute(&mut session, "DROP TABLE probe");
            }
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[inline(never)]
fn profile_create(session: &mut SqlSession, sql: &str) {
    std::hint::black_box(execute(session, sql));
}

#[cfg(feature = "engine-rocksdb")]
#[inline(never)]
fn profile_drop(session: &mut SqlSession, sql: &str) {
    std::hint::black_box(execute(session, sql));
}

#[cfg(feature = "engine-rocksdb")]
fn profile(seconds: u64) {
    let size = std::env::var("DDL_SCALE_PROFILE_SIZE")
        .map(|value| value.parse().unwrap())
        .unwrap_or(200);
    let path = common::fresh("ddl-scale-profile");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        grow(&mut session, 0, size - 1);
        table(&mut session, "probe", true);
        let operation = std::env::var("DDL_SCALE_PROFILE_OP").unwrap_or_default();
        let create_table = table_sql("added");
        let (sql, undo) = if operation == "table" {
            (create_table.as_str(), "DROP TABLE added")
        } else {
            (ADD_INDEX, DROP_INDEX)
        };
        println!("PROFILE_READY pid={} tables={size}", std::process::id());
        let start = Instant::now();
        let mut count = 0;
        let mut create = Duration::ZERO;
        let mut drop = Duration::ZERO;
        let limit = std::env::var("DDL_SCALE_PROFILE_REPS")
            .map(|value| value.parse().unwrap())
            .unwrap_or(usize::MAX);
        while count < limit && start.elapsed() < Duration::from_secs(seconds) {
            let start_create = Instant::now();
            profile_create(&mut session, sql);
            create += start_create.elapsed();
            let start_drop = Instant::now();
            profile_drop(&mut session, undo);
            drop += start_drop.elapsed();
            count += 1;
        }
        println!(
            "PROFILE_DONE reps={count} create_ms={} drop_ms={}",
            create.as_secs_f64() * 1000.0,
            drop.as_secs_f64() * 1000.0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn check_migrated(session: &mut SqlSession, regular: usize) {
    let catalog = session.catalog_metadata().unwrap();
    assert_eq!(catalog.tables.len(), regular + 1);
    assert_eq!(catalog.indexes.len(), regular * 2 + 20);
    assert_eq!(
        catalog
            .presentation
            .constraints
            .iter()
            .filter(|c| c.kind == 'f')
            .count(),
        regular - 1
    );
    assert_eq!(
        execute(session, "SELECT id FROM t1 WHERE org_id = 10"),
        [vec![V::Int(1)]]
    );
    assert_eq!(
        execute(
            session,
            &format!(
                "SELECT id FROM t{regular} WHERE org_id = {} AND name = 'tail'",
                regular - 1
            )
        ),
        [vec![V::Int(regular as i64)]]
    );
    assert_eq!(
        execute(session, "SELECT id FROM wide WHERE c20 = 400"),
        [vec![V::Int(2)]]
    );
}

fn migration_200(engine: Engine) {
    let started = Instant::now();
    let regular = 199;
    let path = common::fresh("ddl-migration-200");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for first in (1..=regular).step_by(25) {
            execute(&mut session, "BEGIN");
            for n in first..=regular.min(first + 24) {
                table(&mut session, &format!("t{n}"), true);
                if n == 1 {
                    execute(&mut session, "INSERT INTO t1 VALUES(1, 10, 'first', 1)");
                } else if n < regular {
                    execute(
                        &mut session,
                        &format!("INSERT INTO t{n} VALUES({n}, {}, 'middle', {n})", n - 1),
                    );
                }
                if n > 1 {
                    execute(
                        &mut session,
                        &format!(
                            "ALTER TABLE t{n} ADD CONSTRAINT t{n}_previous_fk FOREIGN KEY(org_id) REFERENCES t{}(id) ON DELETE CASCADE ON UPDATE CASCADE",
                            n - 1
                        ),
                    );
                }
            }
            execute(&mut session, "COMMIT");
        }
        execute(&mut session, "BEGIN");
        let columns = (1..=20)
            .map(|n| format!("c{n} integer"))
            .collect::<Vec<_>>();
        execute(
            &mut session,
            &format!(
                "CREATE TABLE wide(id integer PRIMARY KEY, {})",
                columns.join(", ")
            ),
        );
        for n in 1..=20 {
            execute(
                &mut session,
                &format!("CREATE INDEX wide_{n} ON wide(c{n})"),
            );
        }
        execute(&mut session, "COMMIT");
        println!(
            "MIGRATION {engine:?} 200 staged_s={:.6}",
            started.elapsed().as_secs_f64()
        );

        execute(&mut session, "BEGIN");
        execute(
            &mut session,
            &format!(
                "INSERT INTO t{regular} VALUES({regular}, {}, 'tail', {regular})",
                regular - 1
            ),
        );
        execute(
            &mut session,
            "INSERT INTO wide(id, c1, c20) VALUES(1, 10, 200), (2, 20, 400)",
        );
        execute(&mut session, "COMMIT");
        for (sql, expected) in [
            (
                format!(
                    "INSERT INTO t{regular} VALUES({}, {}, 'tail', 1)",
                    regular + 1,
                    regular - 1
                ),
                "23505",
            ),
            (
                format!(
                    "INSERT INTO t{regular} VALUES({}, 999, 'orphan', 1)",
                    regular + 1
                ),
                "23503",
            ),
        ] {
            assert_eq!(
                sqlstate(&session.execute(&sql, &[], None).unwrap_err()),
                expected
            );
        }
        check_migrated(&mut session, regular);
        if engine == Engine::Legacy {
            let export = common::fresh("ddl-migration-200-export");
            std::fs::create_dir(&export).unwrap();
            admin.export(export.join("snapshot")).unwrap();
            std::fs::remove_dir_all(export).unwrap();
        }
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        check_migrated(&mut session, regular);
        execute(&mut session, "DROP TABLE wide");
        for first in (1..=regular)
            .step_by(25)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            execute(&mut session, "BEGIN");
            for n in (first..=regular.min(first + 24)).rev() {
                execute(&mut session, &format!("DROP TABLE t{n}"));
            }
            execute(&mut session, "COMMIT");
        }
        assert!(admin.catalog().unwrap().1.is_empty());
        for name in ["t1".to_string(), format!("t{regular}"), "wide".to_string()] {
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
    }
    {
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
            execute(&mut session, &format!("SELECT id FROM t{regular}")),
            [vec![V::Int(1)]]
        );
        println!(
            "MIGRATION {engine:?} 200 full_s={:.6}",
            started.elapsed().as_secs_f64()
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[ignore]
fn timings() {
    #[cfg(feature = "engine-rocksdb")]
    if let Ok(seconds) = std::env::var("DDL_SCALE_PROFILE_SECONDS") {
        profile(seconds.parse().unwrap());
        return;
    }
    matrix(Engine::Legacy);
    migration_200(Engine::Legacy);
    #[cfg(feature = "engine-rocksdb")]
    {
        matrix(Engine::V2);
        migration_200(Engine::V2);
    }
}
