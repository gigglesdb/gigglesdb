mod common;

use giggles_core::relational::{SqlResult, StatementClock, Value, encode_key};
use giggles_core::{Admin, Engine, Limits, SqlSession, sqlstate};
use giggles_pg_types::{Float4, Float8, Numeric};

const CLOCK: StatementClock = StatementClock {
    unavailable: None,
    transaction_us: 0,
    statement_us: 0,
    wall: None,
    zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
};

fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute_at(sql, &[], None, &CLOCK)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn rejects(s: &mut SqlSession, sql: &str, code: &str) {
    let error = s.execute_at(sql, &[], None, &CLOCK).unwrap_err();
    assert_eq!(sqlstate(&error), code, "{sql}: {error:?}");
}

fn ids(s: &mut SqlSession, sql: &str, expected: &[i64]) {
    let actual = run(s, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|&n| vec![Value::Int(n)])
            .collect::<Vec<_>>(),
        "{sql}"
    );
}

struct KeyCase {
    ty: &'static str,
    low: &'static str,
    mid: &'static str,
    high: &'static str,
    duplicate: &'static str,
    missing: &'static str,
}

// Key spellings and comparison order from the PostgreSQL 18.6 qualification.
const CASES: &[KeyCase] = &[
    KeyCase {
        ty: "char(3)",
        low: "''",
        mid: "'a'",
        high: "'b'",
        duplicate: "'a  '",
        missing: "'c'",
    },
    KeyCase {
        ty: "bpchar",
        low: "'x'",
        mid: "'x  y'",
        high: "'x y'",
        duplicate: "'x  y  '",
        missing: "'z'",
    },
    KeyCase {
        ty: "date",
        low: "'4713-01-01 BC'",
        mid: "'2024-02-29'",
        high: "'infinity'",
        duplicate: "'2024-02-29'",
        missing: "'1999-01-01'",
    },
    KeyCase {
        ty: "time",
        low: "'00:00'",
        mid: "'12:00'",
        high: "'24:00'",
        duplicate: "'12:00:00.000000'",
        missing: "'23:59:59.999999'",
    },
    KeyCase {
        ty: "timetz",
        low: "'12:00+01'",
        mid: "'13:00+01'",
        high: "'12:00+00'",
        duplicate: "'13:00:00+01'",
        missing: "'11:00-01'",
    },
    KeyCase {
        ty: "timestamp",
        low: "'-infinity'",
        mid: "'2024-01-01 00:00:00'",
        high: "'2024-01-01 00:00:00.000001'",
        duplicate: "'2024-01-01'",
        missing: "'infinity'",
    },
    KeyCase {
        ty: "timestamptz",
        low: "'2024-01-01 11:59:59+00'",
        mid: "'2024-01-01 12:00:00+00'",
        high: "'2024-01-01 12:00:00.5+00'",
        duplicate: "'2024-01-01 13:00:00+01'",
        missing: "'infinity'",
    },
];

fn key_lookups(s: &mut SqlSession, table: &str, case: &KeyCase, middle_id: i64) {
    let KeyCase {
        mid,
        high,
        duplicate,
        ..
    } = case;
    ids(
        s,
        &format!("SELECT id FROM {table} ORDER BY k"),
        &[1, middle_id, 3],
    );
    ids(
        s,
        &format!("SELECT id FROM {table} ORDER BY k DESC"),
        &[3, middle_id, 1],
    );
    ids(
        s,
        &format!("SELECT id FROM {table} WHERE k = {duplicate}"),
        &[middle_id],
    );
    ids(
        s,
        &format!("SELECT id FROM {table} WHERE k > {mid} ORDER BY k"),
        &[3],
    );
    ids(
        s,
        &format!("SELECT id FROM {table} WHERE k >= {mid} ORDER BY k"),
        &[middle_id, 3],
    );
    ids(
        s,
        &format!("SELECT id FROM {table} WHERE k BETWEEN {mid} AND {high} ORDER BY k"),
        &[middle_id, 3],
    );
}

fn ordinary_table(s: &mut SqlSession) {
    ids(s, "SELECT id FROM ordinary WHERE k = 'keep  '", &[31]);
    ids(s, "SELECT id FROM ordinary WHERE k = 'keep'", &[30]);
}

fn exercise(engine: Engine) {
    let path = common::fresh("key-types");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE ordinary(k text PRIMARY KEY, id int)");
    run(
        &mut s,
        "INSERT INTO ordinary VALUES ('keep', 30), ('keep  ', 31)",
    );
    for (i, case) in CASES.iter().enumerate() {
        let KeyCase {
            ty,
            low,
            mid,
            high,
            duplicate,
            missing,
        } = case;
        let table = format!("parent_{i}");
        let child = format!("child_{i}");
        let restricted = format!("restricted_{i}");
        run(
            &mut s,
            &format!("CREATE TABLE {table}(k {ty} PRIMARY KEY, id int)"),
        );
        run(
            &mut s,
            &format!("INSERT INTO {table} VALUES ({high}, 3), ({low}, 1), ({mid}, 2)"),
        );
        rejects(
            &mut s,
            &format!("INSERT INTO {table} VALUES ({duplicate}, 20)"),
            "23505",
        );
        key_lookups(&mut s, &table, case, 2);
        ids(
            &mut s,
            &format!(
                "INSERT INTO {table} VALUES ({duplicate}, 20) ON CONFLICT (k) DO UPDATE SET id = excluded.id RETURNING id"
            ),
            &[20],
        );
        ids(
            &mut s,
            &format!(
                "INSERT INTO {table} VALUES ({duplicate}, 21) ON CONFLICT DO NOTHING RETURNING id"
            ),
            &[],
        );
        rejects(
            &mut s,
            &format!("UPDATE {table} SET k = {high} WHERE k = {duplicate}"),
            "23505",
        );
        run(
            &mut s,
            &format!(
                "CREATE TABLE {child}(id int PRIMARY KEY, r {ty} REFERENCES {table} ON UPDATE CASCADE ON DELETE CASCADE)"
            ),
        );
        run(
            &mut s,
            &format!(
                "CREATE TABLE {restricted}(id int PRIMARY KEY, r {ty} REFERENCES {table} ON DELETE RESTRICT)"
            ),
        );
        run(
            &mut s,
            &format!("INSERT INTO {child} VALUES (1, {duplicate}), (2, {low})"),
        );
        run(
            &mut s,
            &format!("INSERT INTO {restricted} VALUES (1, {high})"),
        );
        rejects(
            &mut s,
            &format!("INSERT INTO {child} VALUES (3, {missing})"),
            "23503",
        );
        rejects(
            &mut s,
            &format!("DELETE FROM {table} WHERE k = {high}"),
            "23001",
        );
        run(
            &mut s,
            &format!("UPDATE {table} SET k = {missing} WHERE k = {duplicate}"),
        );
        if ty.starts_with("char") || *ty == "bpchar" {
            let rows = run(&mut s, &format!("SELECT r::text FROM {child} WHERE id = 1")).rows;
            assert_eq!(rows[0].values, [Value::Text(missing.trim_matches('\'').into())]);
        } else {
            ids(&mut s, &format!("SELECT id FROM {child} WHERE r = {missing}"), &[1]);
        }
        run(&mut s, &format!("DELETE FROM {table} WHERE k = {missing}"));
        ids(&mut s, &format!("SELECT id FROM {child} ORDER BY id"), &[2]);
        run(&mut s, &format!("INSERT INTO {table} VALUES ({mid}, 20)"));
    }
    for sql in [
        "INSERT INTO parent_0 VALUES (' ', 10)",
        "INSERT INTO parent_0 VALUES ('a ', 10)",
        "INSERT INTO parent_1 VALUES ('x   ', 10)",
        "INSERT INTO parent_2 VALUES ('infinity', 10)",
    ] {
        rejects(&mut s, sql, "23505");
    }
    run(
        &mut s,
        "CREATE TABLE facts(country char(2), day date, v int, PRIMARY KEY (country, day))",
    );
    run(
        &mut s,
        "INSERT INTO facts VALUES ('US', '2024-01-01', 1), ('US', '2023-12-31', 2), ('DE', '2024-01-01', 3)",
    );
    rejects(
        &mut s,
        "INSERT INTO facts VALUES ('US ', '2024-01-01', 4)",
        "23505",
    );
    ids(
        &mut s,
        "SELECT v FROM facts WHERE country = 'US ' AND day BETWEEN '2023-12-31' AND '2024-06-01' ORDER BY day",
        &[2, 1],
    );
    run(&mut s, "CREATE TABLE build_key(k char(2), d date)");
    run(
        &mut s,
        "INSERT INTO build_key VALUES ('a', '2024-01-01'), ('a ', '2024-01-02')",
    );
    rejects(&mut s, "ALTER TABLE build_key ADD PRIMARY KEY(k)", "23505");
    run(&mut s, "ALTER TABLE build_key ADD PRIMARY KEY(d)");
    run(&mut s, "CREATE TABLE build_char(k char(3))");
    run(&mut s, "INSERT INTO build_char VALUES ('x '), ('x y')");
    run(&mut s, "ALTER TABLE build_char ADD PRIMARY KEY(k)");
    rejects(&mut s, "INSERT INTO build_char VALUES ('x')", "23505");
    run(&mut s, "MERGE INTO build_char USING (SELECT 1) s ON k = 'x ' WHEN MATCHED THEN UPDATE SET k = 'z'");
    ids(&mut s, "SELECT length(k) FROM build_char WHERE k = 'z'", &[1]);
    ids(&mut s, "SELECT octet_length(k) FROM build_char WHERE k = 'z'", &[3]);
    ids(&mut s, "SELECT 1 FROM build_char WHERE k = 'z  '::text", &[]);
    ids(&mut s, "SELECT 1 FROM build_char WHERE k = 'z  '::varchar(5)", &[1]);
    run(
        &mut s,
        "CREATE TABLE codes(id int PRIMARY KEY, c char(4) UNIQUE)",
    );
    run(
        &mut s,
        "INSERT INTO codes VALUES (1, 'ab'), (3, NULL), (4, NULL), (5, 'ab c')",
    );
    rejects(&mut s, "INSERT INTO codes VALUES (2, 'ab  ')", "23505");
    ids(
        &mut s,
        "INSERT INTO codes VALUES (6, 'ab') ON CONFLICT (c) DO UPDATE SET id = 60 RETURNING id",
        &[60],
    );
    run(
        &mut s,
        "CREATE TABLE aliases(k date PRIMARY KEY, code bpchar)",
    );
    run(
        &mut s,
        "INSERT INTO aliases VALUES ('2024-01-01', 'x '), ('2024-01-02', NULL)",
    );
    run(&mut s, "ALTER TABLE aliases ADD UNIQUE(code)");
    run(
        &mut s,
        "CREATE UNIQUE INDEX aliases_bpchar ON aliases(code, k)",
    );
    rejects(
        &mut s,
        "INSERT INTO aliases VALUES ('2024-01-03', 'x')",
        "23505",
    );
    rejects(
        &mut s,
        "CREATE TABLE bad_span(k interval PRIMARY KEY)",
        "0A000",
    );
    run(&mut s, "CREATE TABLE bad_span(k interval)");
    rejects(&mut s, "ALTER TABLE bad_span ADD PRIMARY KEY(k)", "0A000");
    rejects(
        &mut s,
        "ALTER TABLE ordinary ALTER COLUMN k TYPE bpchar",
        "0A000",
    );
    ordinary_table(&mut s);
    drop(s);
    drop(admin);

    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for (i, case) in CASES.iter().enumerate() {
        key_lookups(&mut s, &format!("parent_{i}"), case, 20);
    }
    ids(
        &mut s,
        "SELECT v FROM facts WHERE country = 'US ' AND day = '2024-01-01'",
        &[1],
    );
    ids(&mut s, "SELECT id FROM codes WHERE c = 'ab '", &[60]);
    ordinary_table(&mut s);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_key_types() {
    exercise(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_key_types() {
    exercise(Engine::V2);
}

#[test]
fn old_row_key_tags_are_byte_identical() {
    let cases = [
        (Value::Null, vec![0]),
        (Value::Bool(true), vec![1, 1]),
        (
            Value::Int(-1),
            vec![2, 127, 255, 255, 255, 255, 255, 255, 255],
        ),
        (
            Value::Text("a\0 ".into()),
            vec![3, b'a', 0, 255, b' ', 0, 0],
        ),
        (Value::Bytes(vec![0, 1]), vec![4, 0, 255, 1, 0, 0]),
        (Value::Numeric(Numeric::parse("0.00").unwrap()), vec![5, 2]),
        (
            Value::Float4(Float4::from_bits((-0.0f32).to_bits())),
            vec![6, 128, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            Value::Float8(Float8::from_bits(0)),
            vec![7, 128, 0, 0, 0, 0, 0, 0, 0],
        ),
    ];
    for (value, expected) in cases {
        assert_eq!(encode_key(&[value]).unwrap(), expected);
    }
}

// PostgreSQL 18.6 (pgp-oracle-am6): UPDATE 3000; children=3000, orphans=0;
// DELETE 3000; children=0 (CASCADE). RESTRICT: UPDATE/DELETE ERROR 23001,
// children=3000, orphans=0, for both char(3) and date.
// More than one parent page and action-queue batch. Differently padded char
// spellings must use the same parent/child key in cascades and empty proofs.
#[cfg(feature = "engine-rocksdb")]
#[test]
fn paged_char_and_date_foreign_key_actions() {
    let path = common::fresh("key-types-paged-fk");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for (ty, key, moved, parent, child) in [
        (
            "char(3)",
            "lpad(g::text, 3, '0')",
            "upper(k::text)",
            "'a'",
            "'a  '",
        ),
        (
            "date",
            "DATE '2000-01-01' + g",
            "k + 10000",
            "DATE '1999-01-01'",
            "DATE '1999-01-01'",
        ),
    ] {
        for action in ["CASCADE", "RESTRICT"] {
            run(&mut s, &format!("CREATE TABLE p(k {ty} PRIMARY KEY)"));
            // Exercise secondary child prefixes, including empty-child batch proofs.
            run(
                &mut s,
                &format!(
                    "CREATE TABLE c(id int PRIMARY KEY, k {ty} REFERENCES p ON DELETE {action} ON UPDATE {action})"
                ),
            );
            run(&mut s, "CREATE INDEX child_key ON c(k)");
            let key = if ty == "char(3)" {
                // 2,999 distinct lowercase base-26 keys, plus the padded 'a'.
                "chr(98 + g / 676) || chr(97 + (g / 26) % 26) || chr(97 + g % 26)"
            } else {
                key
            };
            run(
                &mut s,
                &format!("INSERT INTO p SELECT {key} FROM generate_series(1, 2999) g"),
            );
            run(&mut s, &format!("INSERT INTO p VALUES ({parent})"));
            run(
                &mut s,
                "INSERT INTO c SELECT row_number() OVER (), k FROM p",
            );
            run(
                &mut s,
                &format!("UPDATE c SET k = {child} WHERE k = {parent}"),
            );
            if action == "RESTRICT" {
                rejects(&mut s, &format!("UPDATE p SET k = {moved}"), "23001");
                rejects(&mut s, "DELETE FROM p", "23001");
                ids(&mut s, "SELECT count(*) FROM p", &[3000]);
                ids(&mut s, "SELECT count(*) FROM c", &[3000]);
            } else {
                assert_eq!(
                    run(&mut s, &format!("UPDATE p SET k = {moved}")).affected,
                    3000
                );
                ids(&mut s, "SELECT count(*) FROM c", &[3000]);
            }
            ids(
                &mut s,
                "SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.k = c.k)",
                &[0],
            );
            if action == "CASCADE" {
                assert_eq!(run(&mut s, "DELETE FROM p").affected, 3000);
                ids(&mut s, "SELECT count(*) FROM c", &[0]);
            }
            run(&mut s, "DROP TABLE c");
            run(&mut s, "DROP TABLE p");
        }
    }
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
