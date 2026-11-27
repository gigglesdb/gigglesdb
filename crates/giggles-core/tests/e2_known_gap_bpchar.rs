mod common;
use common::fresh;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{DataType, SqlError, Value},
};

const ENGINES: &[Engine] = &[
    #[cfg(feature = "engine-rocksdb")]
    Engine::V2,
    Engine::Legacy,
];

#[test]
fn unsupported_bpchar_scalar_consumers_fail_closed() {
    let a = Admin::create(fresh("e2-bpchar-known-gaps"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE t (id int PRIMARY KEY, c char(3))", &[], None)
        .unwrap();
    s.execute("INSERT INTO t VALUES (1,'a')", &[], None)
        .unwrap();
    for sql in [
        // DD68 followup must qualify bpchar pattern semantics; task1 fails closed (0A000).
        "SELECT c LIKE 'a%' FROM t",
        // DD68 followup must qualify bpchar concatenation conversion; task1 fails closed (0A000).
        "SELECT c || 'x' FROM t",
        // DD68 followup must qualify bpchar function conversion; task1 fails closed (0A000).
        "SELECT upper(c) FROM t",
        // DD68 followup must qualify bpchar common-type resolution; task1 fails closed (0A000).
        "SELECT coalesce(c,'x') FROM t",
        // DD68 followup must qualify bpchar CASE type resolution; task1 fails closed (0A000).
        "SELECT CASE WHEN id=1 THEN c ELSE 'x' END FROM t",
        // DD68 followup must qualify bpchar set-operation semantics; task1 fails closed (0A000).
        "SELECT c FROM t UNION SELECT c FROM t",
        // DD68 followup must qualify bpchar concat() conversion; task1 fails closed (0A000).
        "SELECT concat(c, 'x') FROM t",
        // DD68 followup must qualify bpchar function conversion; task1 fails closed (0A000).
        "SELECT lower(c) FROM t",
        // DD68 followup must qualify bpchar greatest/least resolution; task1 fails closed (0A000).
        "SELECT greatest(c, 'b') FROM t",
        "SELECT least(c, 'b') FROM t",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{sql}"
        );
    }
}

fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn assert_ids(s: &mut SqlSession, sql: &str, expected: &[i64]) {
    assert_eq!(
        rows(s, sql),
        expected
            .iter()
            .map(|&id| vec![Value::Int(id)])
            .collect::<Vec<_>>(),
        "{sql}"
    );
}

#[test]
fn nested_distinct_and_distinct_on_use_bpchar_equality() {
    use Value::{Int, Text};

    for &engine in ENGINES {
        let admin =
            Admin::create_engine(fresh("e2-bpchar-distinct"), Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        rows(&mut s, "CREATE TABLE lg (id int PRIMARY KEY, b bpchar)");
        rows(
            &mut s,
            "INSERT INTO lg VALUES (1, 'x'), (2, 'x  '), (3, 'y'), (4, 'x ')",
        );

        // PostgreSQL 18.6, C locale, review distinct.pg D1/D5/D7/D15:
        // D1 | 2
        // D5 | 2
        // D7 | 2
        // D15 | 2
        // review distinct2.pg E6:
        //  b | id
        //  x |  1
        //  y |  3
        for indexed in [false, true] {
            if indexed {
                rows(&mut s, "CREATE INDEX lg_b ON lg (b)");
            }
            assert_ids(&mut s, "SELECT count(*) FROM (SELECT DISTINCT b FROM lg) z", &[2]);
            assert_ids(
                &mut s,
                "SELECT count(*) FROM (SELECT DISTINCT b FROM (VALUES \
                 ('x'::bpchar), ('x  '::bpchar), ('y'::bpchar)) v(b)) z",
                &[2],
            );
            assert_ids(
                &mut s,
                "SELECT count(*) FROM (SELECT DISTINCT ON (b) b, id FROM lg ORDER BY b, id) z",
                &[2],
            );
            assert_eq!(
                rows(&mut s, "SELECT DISTINCT ON (b) b, id FROM lg ORDER BY b, id"),
                vec![vec![Text("x".into()), Int(1)], vec![Text("y".into()), Int(3)]],
            );
        }
    }
}

#[test]
fn bpchar_scalar_consumers_follow_pg18() {
    use Value::{Bool, Int, Null, Text};

    for &engine in ENGINES {
        let admin =
            Admin::create_engine(fresh("e2-bpchar-scalars"), Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            "CREATE TABLE t (id int PRIMARY KEY, c char(3), b bpchar, v varchar(5), x text)",
            "INSERT INTO t VALUES (1,'a','a  ','a','a'), (2,'b ','b','b  ','b '), \
             (3,'','  ','',''), (4,NULL,NULL,NULL,NULL), (5,E'a\t','ab','ab','ab')",
        ] {
            rows(&mut s, sql);
        }

        // PostgreSQL 18.6, o2.sql.
        for (sql, expected) in [
            (
                "SELECT c = 'a', c = 'a  ', c <> 'b', c < 'b', c >= 'a' FROM t ORDER BY id",
                vec![
                    vec![Bool(true); 5],
                    vec![
                        Bool(false),
                        Bool(false),
                        Bool(false),
                        Bool(false),
                        Bool(true),
                    ],
                    vec![
                        Bool(false),
                        Bool(false),
                        Bool(true),
                        Bool(true),
                        Bool(false),
                    ],
                    vec![Null; 5],
                    vec![Bool(false), Bool(false), Bool(true), Bool(true), Bool(true)],
                ],
            ),
            (
                "SELECT b = 'a', b = c, b < 'ab' FROM t ORDER BY id",
                vec![
                    vec![Bool(true); 3],
                    vec![Bool(false), Bool(true), Bool(false)],
                    vec![Bool(false), Bool(true), Bool(true)],
                    vec![Null; 3],
                    vec![Bool(false); 3],
                ],
            ),
            (
                "SELECT c = x, x = c, c = v, v = c, c = 'a'::text, c = 'a '::varchar \
                 FROM t ORDER BY id",
                vec![
                    vec![Bool(true); 6],
                    vec![
                        Bool(false),
                        Bool(false),
                        Bool(true),
                        Bool(true),
                        Bool(false),
                        Bool(false),
                    ],
                    vec![
                        Bool(true),
                        Bool(true),
                        Bool(true),
                        Bool(true),
                        Bool(false),
                        Bool(false),
                    ],
                    vec![Null; 6],
                    vec![Bool(false); 6],
                ],
            ),
            (
                "SELECT a.id, bb.id FROM t a JOIN t bb ON a.c = bb.b ORDER BY 1, 2",
                vec![vec![Int(1); 2], vec![Int(2); 2], vec![Int(3); 2]],
            ),
            (
                "SELECT length(c), char_length(c), character_length(c), octet_length(c), \
                 length(b), octet_length(b) FROM t ORDER BY id",
                vec![
                    vec![Int(1), Int(1), Int(1), Int(3), Int(1), Int(3)],
                    vec![Int(1), Int(1), Int(1), Int(3), Int(1), Int(1)],
                    vec![Int(0), Int(0), Int(0), Int(3), Int(0), Int(2)],
                    vec![Null; 6],
                    vec![Int(2), Int(2), Int(2), Int(3), Int(2), Int(2)],
                ],
            ),
            (
                "SELECT c, count(*) FROM t GROUP BY c ORDER BY c COLLATE \"C\" NULLS FIRST",
                vec![
                    vec![Null, Int(1)],
                    vec![Text("   ".into()), Int(1)],
                    vec![Text("a  ".into()), Int(1)],
                    vec![Text("a\t ".into()), Int(1)],
                    vec![Text("b  ".into()), Int(1)],
                ],
            ),
            (
                "SELECT 'a'::char(3) IN ('z', 'a '::text), \
                 'a'::char(3) IN ('a '::text), \
                 'a'::char(3) NOT IN ('z'::varchar, 'a '::text), \
                 'a ' IN ('z', 'a'::bpchar), 'a ' IN ('z'::text, 'a'::bpchar)",
                vec![vec![
                    Bool(true),
                    Bool(false),
                    Bool(false),
                    Bool(true),
                    Bool(false),
                ]],
            ),
            (
                "SELECT count(DISTINCT c), count(DISTINCT b) FROM t",
                vec![vec![Int(4), Int(4)]],
            ),
            (
                "SELECT min(c), max(c) FROM t",
                vec![vec![Text("   ".into()), Text("b  ".into())]],
            ),
            (
                "SELECT min(c), max(c) FROM t WHERE id IN (1, 5)",
                vec![vec![Text("a  ".into()), Text("a\t ".into())]],
            ),
            (
                "SELECT length('a  '::bpchar), octet_length('a  '::bpchar), \
                 'a '::char(2) <= 'a'::bpchar, 'b'::char(3) > 'a '::varchar",
                vec![vec![Int(1), Int(3), Bool(true), Bool(true)]],
            ),
        ] {
            assert_eq!(rows(&mut s, sql), expected, "{engine:?}: {sql}");
        }
        for (sql, expected) in [
            (
                "SELECT id FROM t ORDER BY c COLLATE \"C\", id",
                &[3, 1, 5, 2, 4][..],
            ),
            (
                "SELECT id FROM t ORDER BY c DESC NULLS LAST, id",
                &[2, 5, 1, 3, 4],
            ),
            (
                "SELECT id FROM t WHERE c IN ('a', 'b  ') ORDER BY id",
                &[1, 2],
            ),
            ("SELECT id FROM t WHERE c = $$a $$ ORDER BY id", &[1]),
        ] {
            assert_ids(&mut s, sql, expected);
        }

        // The distinct/grouping comparators must merge differently padded
        // stored values and retain padding on the value returned to SQL.
        rows(&mut s, "INSERT INTO t VALUES (6, 'a ', 'a', 'a ', 'a ')");
        assert_eq!(
            rows(&mut s, "SELECT DISTINCT c FROM t WHERE id IN (1, 6)"),
            vec![vec![Text("a  ".into())]]
        );
        assert!(matches!(
            rows(&mut s, "SELECT DISTINCT b FROM t WHERE id IN (1, 6)").as_slice(),
            [row] if matches!(row.as_slice(), [Text(value)] if value.trim_end_matches(' ') == "a")
        ));
        assert_eq!(
            rows(
                &mut s,
                "SELECT count(*) FROM t WHERE id IN (1, 6) GROUP BY b"
            ),
            vec![vec![Int(2)]]
        );
        assert_eq!(
            rows(&mut s, "SELECT count(DISTINCT b) FROM t WHERE id IN (1, 6)"),
            vec![vec![Int(1)]]
        );
        assert_eq!(
            rows(&mut s, "SELECT DISTINCT * FROM t WHERE id = 1"),
            vec![vec![
                Int(1),
                Text("a  ".into()),
                Text("a  ".into()),
                Text("a".into()),
                Text("a".into())
            ]]
        );
    }
}

fn sorted_ids(rows: impl IntoIterator<Item = Vec<Value>>) -> Vec<i64> {
    let mut ids = rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Int(id)] => *id,
            _ => panic!("expected one integer column, got {row:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[test]
fn bpchar_queries_agree_with_and_without_keys() {
    use Value::{Int, Text};

    for &engine in ENGINES {
        let admin =
            Admin::create_engine(fresh("e2-bpchar-access"), Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        rows(&mut s, "CREATE TABLE heap (id int PRIMARY KEY, c char(3))");
        rows(&mut s, "CREATE TABLE keyed (id int, c char(3) PRIMARY KEY)");
        for table in ["heap", "keyed"] {
            rows(
                &mut s,
                &format!("INSERT INTO {table} VALUES (1,'a'),(2,'b'),(3,''),(5,E'a\t')"),
            );
        }

        for (table, add_index) in [("heap", false), ("heap", true), ("keyed", false)] {
            if add_index {
                rows(&mut s, "CREATE INDEX heap_c ON heap (c)");
            }
            for (predicate, expected) in [
                ("c = 'a  '", &[1][..]),
                ("c = 'a'::text", &[1]),
                ("c = 'a '::text", &[]),
                ("c = 'a '::varchar", &[1]),
                ("c <> 'a '::text", &[1, 2, 3, 5]),
                ("c > 'a '", &[2, 5]),
                ("c <= 'a '", &[1, 3]),
                ("c < 'a '::text", &[1, 3, 5]),
                ("c >= 'a '::text", &[2]),
                ("c BETWEEN 'a ' AND 'b  '", &[1, 2, 5]),
                ("c IN ('a ', 'b  ')", &[1, 2]),
                ("c IN ('a '::text, 'b  '::varchar)", &[1, 2]),
            ] {
                let sql = format!("SELECT id FROM {table} WHERE {predicate}");
                assert_eq!(
                    sorted_ids(rows(&mut s, &sql)),
                    expected,
                    "{engine:?}: {sql}"
                );
            }
            assert_ids(
                &mut s,
                &format!("SELECT id FROM {table} ORDER BY c COLLATE \"C\" DESC"),
                &[2, 5, 1, 3],
            );
            assert_eq!(
                rows(
                    &mut s,
                    &format!("SELECT min(c), max(c), count(DISTINCT c) FROM {table}")
                ),
                vec![vec![Text("   ".into()), Text("b  ".into()), Int(4)]]
            );
            assert_eq!(
                rows(
                    &mut s,
                    &format!("SELECT length(c), octet_length(c) FROM {table} WHERE c = 'a '")
                ),
                vec![vec![Int(1), Int(3)]]
            );

            for (op, ty, value, expected) in [
                ("=", DataType::Bpchar, "b", &[2][..]),
                ("=", DataType::Text, "b", &[2]),
                ("=", DataType::Text, "b ", &[]),
                ("=", DataType::Varchar, "b ", &[2]),
                ("<", DataType::Text, "a ", &[1, 3, 5]),
                (">=", DataType::Text, "a ", &[2]),
            ] {
                let sql = format!("SELECT id FROM {table} WHERE c {op} $1");
                let prepared = s
                    .prepare(&sql)
                    .unwrap()
                    .with_parameter_types(&[Some(ty)])
                    .unwrap();
                let actual = s
                    .execute_prepared(&prepared, &[Text(value.into())], None)
                    .unwrap_or_else(|error| panic!("{engine:?}: {sql}: {value:?}: {error:?}"))
                    .rows
                    .into_iter()
                    .map(|row| row.values)
                    .collect::<Vec<_>>();
                assert_eq!(sorted_ids(actual), expected, "{engine:?}: {sql}: {value:?}");
            }
            let sql = format!("UPDATE {table} SET id = id WHERE c = $1 RETURNING id");
            let prepared = s
                .prepare(&sql)
                .unwrap()
                .with_parameter_types(&[Some(DataType::Text)])
                .unwrap();
            assert!(
                s.execute_prepared(&prepared, &[Text("a ".into())], None)
                    .unwrap_or_else(|error| panic!("{engine:?}: {sql}: {error:?}"))
                    .rows
                    .is_empty()
            );
            assert_eq!(
                s.execute_prepared(&prepared, &[Text("a".into())], None)
                    .unwrap_or_else(|error| panic!("{engine:?}: {sql}: {error:?}"))
                    .rows[0]
                    .values,
                [Int(1)]
            );
        }
    }
}

#[test]
fn character_casts_follow_pg18() {
    use giggles_core::{
        SqlSession,
        relational::{DataType as T, Value},
    };

    fn check_query(
        session: &mut SqlSession,
        failures: &mut Vec<String>,
        sql: &str,
        expected_text: &str,
        expected_type: T,
        expected_typmod: i32,
    ) {
        let prepared = match session.prepare(sql) {
            Ok(prepared) => prepared,
            Err(error) => {
                failures.push(format!("{sql}: prepare: {error:?}"));
                return;
            }
        };
        match session.describe(&prepared) {
            Ok(description) => {
                let types: Vec<_> = description
                    .columns
                    .iter()
                    .map(|column| column.data_type.clone())
                    .collect();
                if types != [expected_type.clone()] {
                    failures.push(format!(
                        "{sql}: types {types:?}, expected [{expected_type:?}]"
                    ));
                }
                if description.typmods != [expected_typmod] {
                    failures.push(format!(
                        "{sql}: typmods {:?}, expected [{expected_typmod}]",
                        description.typmods
                    ));
                }
            }
            Err(error) => failures.push(format!("{sql}: describe: {error:?}")),
        }
        match session.execute_prepared(&prepared, &[], None) {
            Ok(result) => {
                let rows: Vec<_> = result.rows.iter().map(|row| row.values.clone()).collect();
                let expected = vec![vec![Value::Text(expected_text.into())]];
                if rows != expected {
                    failures.push(format!("{sql}: rows {rows:?}, expected {expected:?}"));
                }
            }
            Err(error) => failures.push(format!("{sql}: execute: {error:?}")),
        }
    }

    // PG18.6 explicit character casts (DD-0068 F19): silent truncation, char(n) padding, unbounded varchar/bpchar without a typmod.
    let a = Admin::create(fresh("e2-character-casts"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let mut failures = Vec::new();
    for (sql, text, ty, typmod) in [
        ("SELECT 'abcdef'::varchar(3)", "abc", T::Varchar, 7),
        ("SELECT 'abc'::character varying(2)", "ab", T::Varchar, 6),
        ("SELECT 'abc'::varchar", "abc", T::Varchar, -1),
        ("SELECT 'abc'::char(3)", "abc", T::Bpchar, 7),
        ("SELECT 'ab'::char(3)", "ab ", T::Bpchar, 7),
        ("SELECT 'abc'::bpchar", "abc", T::Bpchar, -1),
        ("SELECT CAST('abcdef' AS varchar(3))", "abc", T::Varchar, 7),
    ] {
        check_query(&mut s, &mut failures, sql, text, ty, typmod);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
