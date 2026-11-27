//! Compare both planner routes. The reference shares the per-probe admission
//! and descriptor/type checks, whose precedence is tested directly.
use super::*;
use crate::{
    CoreError,
    relational::{
        Prepared, SqlResult, StatementBudget, take_sql_error_detail, take_sql_error_message,
    },
    sqlstate,
};
use std::{cell::Cell, fmt::Write, time::Instant};

thread_local! {
    static REFERENCE: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn reference_lookup() -> bool {
    REFERENCE.with(Cell::get)
}

fn with_reference<T>(reference: bool, run: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            REFERENCE.with(|value| value.set(self.0));
        }
    }
    let _restore = Restore(REFERENCE.with(|value| value.replace(reference)));
    run()
}

fn execute(state: &mut Relational, sql: &str) -> std::result::Result<SqlResult, CoreError> {
    let statement = Prepared::parse(sql, 1, state.epoch).map_err(CoreError::Sql)?;
    state.execute(&statement, &[], None).map_err(CoreError::Sql)
}

fn run(state: &mut Relational, sql: &str) -> SqlResult {
    execute(state, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

#[derive(Debug, PartialEq, Eq)]
enum Output {
    Rows(Vec<u8>),
    Error(&'static str, String, Option<String>),
}

fn observe(result: std::result::Result<SqlResult, CoreError>) -> Output {
    let message = take_sql_error_message();
    let detail = take_sql_error_detail();
    match result {
        Ok(result) => Output::Rows(bincode::serialize(&result).unwrap()),
        Err(error) => Output::Error(
            sqlstate(&error),
            message.unwrap_or_else(|| error.to_string()),
            detail,
        ),
    }
}

fn observed(state: &mut Relational, sql: &str, reference: bool) -> Output {
    with_reference(reference, || {
        take_sql_error_message();
        take_sql_error_detail();
        observe(execute(state, sql))
    })
}

fn fixture(kind: usize, count: usize) -> Relational {
    let mut state = Relational::default();
    state.enable_v2_unique_reporting();
    if kind == 2 {
        run(
            &mut state,
            "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
        );
    }
    let collation = if kind == 2 { " COLLATE ci" } else { "" };
    let constraint = if kind == 1 {
        ""
    } else {
        " CONSTRAINT items_email UNIQUE"
    };
    run(
        &mut state,
        &format!(
            "CREATE TABLE items (id INT PRIMARY KEY, email TEXT{collation}{constraint},
         code INT UNIQUE, n INT NOT NULL CHECK (n >= 0), live BOOLEAN)"
        ),
    );
    if kind == 1 {
        run(
            &mut state,
            "CREATE UNIQUE INDEX live_email ON items(email) WHERE live",
        );
    }
    let mut seed = String::from("INSERT INTO items VALUES ");
    for i in 0..count {
        if i > 0 {
            seed.push(',');
        }
        write!(seed, "({i},'key{i}',{i},{i},true)").unwrap();
    }
    run(&mut state, &seed);
    state
}

fn compare(source: &Relational, statement: &str) -> Output {
    let mut old = source.clone();
    let mut new = source.clone();
    let expected = observed(&mut old, statement, true);
    let actual = observed(&mut new, statement, false);
    assert_eq!(actual, expected, "{statement}");
    let select = "SELECT * FROM items ORDER BY id LIMIT 1024";
    assert_eq!(
        bincode::serialize(&run(&mut new, select)).unwrap(),
        bincode::serialize(&run(&mut old, select)).unwrap(),
        "{statement}",
    );
    assert_eq!(new, old, "{statement}");
    actual
}

fn random(seed: &mut u32) -> usize {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 17;
    *seed ^= *seed << 5;
    *seed as usize
}

#[test]
fn seeded_sql_batches_match_per_proposal_reference() {
    for (kind, mut seed) in [0x6c8e_9cf5, 0x405b_82f9, 0xa721_23cd]
        .into_iter()
        .enumerate()
    {
        let source = fixture(kind, 128);
        for size in [1, 10, 100] {
            let mut successes = 0;
            for trial in 0..36 {
                let mut sql = String::from("INSERT INTO items VALUES ");
                let start = random(&mut seed) % 128;
                for i in 0..size {
                    if i > 0 {
                        sql.push(',');
                    }
                    let r = random(&mut seed);
                    let key = if trial % 6 == 0 {
                        start
                    } else {
                        (i + start) % 128
                    };
                    let email = match r % 8 {
                        0 => "NULL".into(),
                        1 | 2 => format!("'miss{i}'"),
                        3 if kind == 2 => format!("'KEY{key}'"),
                        _ => format!("'key{key}'"),
                    };
                    let code = if trial % 6 == 1 { key } else { 256 + i };
                    let live = kind != 1 || !r.is_multiple_of(3);
                    let n = r % 32;
                    write!(sql, "({},{email},{code},{n},{live})", 256 + i).unwrap();
                }
                sql.push_str(" ON CONFLICT ");
                let target = match (kind, trial % 4) {
                    (_, 3) => "",
                    (1, _) => "(email) WHERE live",
                    (_, 1) => "ON CONSTRAINT items_email",
                    _ => "(email)",
                };
                sql.push_str(target);
                sql.push_str(match trial % 4 {
                    0 => " DO UPDATE SET n = excluded.n",
                    1 => " DO UPDATE SET n = excluded.n WHERE items.n < excluded.n",
                    _ => " DO NOTHING",
                });
                if trial % 5 != 0 {
                    sql.push_str(" RETURNING id, email, code, n");
                }
                if matches!(compare(&source, &sql), Output::Rows(_)) {
                    successes += 1;
                }
            }
            assert!(
                successes >= 12,
                "{kind} {size}: {successes} successful statements"
            );
        }
    }
}

#[test]
fn sql_error_identity_and_precedence_match_reference() {
    let source = fixture(0, 8);
    for (values, action, expected) in [
        (
            "(90,'key2',90,1,true),(91,'key2',91,2,true)",
            "n=excluded.n",
            "21000",
        ),
        ("(90,'key2',3,1,true)", "code=excluded.code", "23505"),
        (
            "(90,'key2',90,1,true),(91,'key2',91,NULL,true)",
            "n=excluded.n",
            "23502",
        ),
        (
            "(90,'key2',90,1,true),(91,'key2',91,-1,true)",
            "n=excluded.n",
            "23514",
        ),
    ] {
        let sql = format!(
            "INSERT INTO items VALUES {values} ON CONFLICT (email) DO UPDATE SET {action} RETURNING id"
        );
        let result = compare(&source, &sql);
        assert!(
            matches!(result, Output::Error(state, _, _) if state == expected),
            "{result:?}"
        );
    }
}

fn assert_scaling(count: usize, reference: bool) {
    let _budget = StatementBudget::enter(0, true);
    let mut source = fixture(0, count);
    let mut sql = String::from("INSERT INTO items VALUES ");
    for i in 0..count {
        if i > 0 {
            sql.push(',');
        }
        write!(sql, "({},'key{i}',{},0,true)", 2000 + i, 2000 + i).unwrap();
    }
    sql.push_str(" ON CONFLICT (email) DO UPDATE SET n = excluded.n RETURNING id");
    let before = Relational::secondary_validation_count();
    let start = Instant::now();
    let result = with_reference(reference, || run(&mut source, &sql));
    let elapsed = start.elapsed();
    let validations = Relational::secondary_validation_count() - before;
    assert_eq!(result.affected, count as u64);
    assert_eq!(result.rows.len(), count);
    assert_eq!(result.rows[0].values, [Value::Int(0)]);
    assert_eq!(
        result.rows[count - 1].values,
        [Value::Int(count as i64 - 1)]
    );
    assert!(
        (1..=2).contains(&validations),
        "validation cap: {count} rows used {validations} validations"
    );
    eprintln!("{count} rows: {validations} validation(s), {elapsed:?}");
}

#[test]
fn source_validation_is_constant_across_batch_sizes() {
    for count in [1, 10, 100, 1000] {
        assert_scaling(count, false);
    }
}

#[test]
#[should_panic(expected = "validation cap: 10 rows used")]
fn per_proposal_reference_fails_the_validation_cap() {
    assert_scaling(10, true);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn overlapping_repeatable_read_upserts_match_reference() {
    use crate::{Admin, Engine, Limits, SqlIsolation};
    use std::{path::PathBuf, sync::mpsc, thread, time::Duration};

    struct TestDir(PathBuf);
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let scenario = |reference| {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = TestDir(std::env::temp_dir().join(format!(
            "upsert-validation-{:032x}",
            u128::from_be_bytes(nonce)
        )));
        let admin = Admin::create_engine(&path.0, Limits::default(), Engine::V2).unwrap();
        let mut a = admin.sql_session();
        a.set_isolation(SqlIsolation::RepeatableRead);
        a.execute(
            "CREATE TABLE race (id INT PRIMARY KEY, email TEXT UNIQUE, n INT)",
            &[],
            None,
        )
        .unwrap();
        a.execute("INSERT INTO race VALUES (1, 'a', 0)", &[], None)
            .unwrap();
        let upsert = "INSERT INTO race VALUES (2, 'a', 1) ON CONFLICT (email)
                      DO UPDATE SET n = race.n + excluded.n RETURNING n";
        let output = thread::scope(|scope| {
            let (ready, pinned) = mpsc::channel();
            let (start, wait) = mpsc::channel();
            let admin = &admin;
            let worker = scope.spawn(move || {
                let mut b = admin.sql_session();
                b.set_isolation(SqlIsolation::RepeatableRead);
                b.execute("BEGIN", &[], None).unwrap();
                let initial = b
                    .execute("SELECT n FROM race WHERE id = 1", &[], None)
                    .unwrap();
                assert_eq!(initial.rows[0].values, [Value::Int(0)]);
                ready.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
                let result = with_reference(reference, || observe(b.execute(upsert, &[], None)));
                b.execute("ROLLBACK", &[], None).unwrap();
                result
            });
            pinned.recv_timeout(Duration::from_secs(10)).unwrap();
            a.execute("BEGIN", &[], None).unwrap();
            let first = with_reference(reference, || a.execute(upsert, &[], None)).unwrap();
            assert_eq!(first.rows[0].values, [Value::Int(1)]);
            start.send(()).unwrap();
            a.execute("COMMIT", &[], None).unwrap();
            worker.join().unwrap()
        });
        let final_rows = a
            .execute("SELECT * FROM race ORDER BY id", &[], None)
            .unwrap()
            .rows;
        assert_eq!(final_rows.len(), 1);
        assert_eq!(
            final_rows[0].values,
            [Value::Int(1), Value::Text("a".into()), Value::Int(1)]
        );
        output
    };
    let expected = scenario(true);
    assert!(
        matches!(expected, Output::Error("40001", _, _)),
        "{expected:?}"
    );
    assert_eq!(scenario(false), expected);
}
