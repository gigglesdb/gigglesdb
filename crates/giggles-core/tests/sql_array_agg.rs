//! PostgreSQL 18.6 oracle for scalar array_agg, including the Archdocs query shape.
mod common;

use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, StatementClock, Value},
};
use giggles_pg_types::temporal::{DateOrder, DateStyle, DateStyleSettings, SessionZone};

fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("sql-array-agg");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    test(&mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn t(text: &str) -> Value {
    Value::Text(text.into())
}

fn run_zoned(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: Some(SessionZone::named("America/New_York").unwrap()),
        wall: None,
    };
    session
        .execute_at(sql, &[], None, &clock)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[test]
fn postgres_18_array_agg_oracle() {
    database(|session| {
        use Value::{Int, Null};

        let ddl = "CREATE TEMP TABLE t(id int, g int, s text, u uuid, b bigint, n numeric, j jsonb, ts timestamp, f bool);";
        // The test owns an isolated database, so an ordinary table supplies
        // the same rows when TEMP TABLE is outside the current SQL profile.
        if let Err(error) = session.execute(ddl, &[], None) {
            assert!(
                matches!(
                    error,
                    CoreError::Sql(SqlError::Syntax | SqlError::Unsupported)
                ),
                "{error:?}"
            );
            run(session, &ddl.replace("TEMP ", ""));
        }
        run(
            session,
            r#"INSERT INTO t VALUES (1,1,'b','00000000-0000-0000-0000-000000000002',20,1.5,'{"a":1}','2024-01-02 03:04:05',true),(2,1,'a','00000000-0000-0000-0000-000000000001',10,NULL,'[1]',NULL,false),(3,2,NULL,NULL,NULL,2,NULL,'2024-01-01',NULL),(4,2,'a b','00000000-0000-0000-0000-000000000001',30,3,'"x"','2024-01-03',true),(5,3,'c"q\','00000000-0000-0000-0000-000000000003',40,4,'null','2024-01-04',false);"#,
        );

        let cases = [
            (
                r#"SELECT array_agg(s) FROM t;"#,
                vec![vec![t(r#"{b,a,NULL,"a b","c\"q\\"}"#)]],
            ),
            (
                r#"SELECT array_agg(s ORDER BY id DESC) FROM t;"#,
                vec![vec![t(r#"{"c\"q\\","a b",NULL,a,b}"#)]],
            ),
            (
                r#"SELECT g, array_agg(s ORDER BY s NULLS FIRST) FROM t GROUP BY g ORDER BY g;"#,
                vec![
                    vec![Int(1), t("{a,b}")],
                    vec![Int(2), t(r#"{NULL,"a b"}"#)],
                    vec![Int(3), t(r#"{"c\"q\\"}"#)],
                ],
            ),
            (
                r#"SELECT array_agg(DISTINCT s) FROM t;"#,
                vec![vec![t(r#"{a,"a b",b,"c\"q\\",NULL}"#)]],
            ),
            (
                r#"SELECT array_agg(DISTINCT u) FROM t;"#,
                vec![vec![t(
                    "{00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000002,00000000-0000-0000-0000-000000000003,NULL}",
                )]],
            ),
            (
                r#"SELECT (array_agg(s ORDER BY id))[1], (array_agg(s ORDER BY id))[3], (array_agg(s ORDER BY id))[9] FROM t;"#,
                vec![vec![t("b"), Null, Null]],
            ),
            (
                r#"SELECT array_agg(u ORDER BY id), array_agg(b ORDER BY id), array_agg(id ORDER BY id) FROM t;"#,
                vec![vec![
                    t(
                        "{00000000-0000-0000-0000-000000000002,00000000-0000-0000-0000-000000000001,NULL,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000003}",
                    ),
                    t("{20,10,NULL,30,40}"),
                    t("{1,2,3,4,5}"),
                ]],
            ),
            (
                r#"SELECT array_agg(n ORDER BY id), array_agg(f ORDER BY id), array_agg(ts ORDER BY id) FROM t;"#,
                vec![vec![
                    t("{1.5,NULL,2,3,4}"),
                    t("{t,f,NULL,t,f}"),
                    t(
                        r#"{"2024-01-02 03:04:05",NULL,"2024-01-01 00:00:00","2024-01-03 00:00:00","2024-01-04 00:00:00"}"#,
                    ),
                ]],
            ),
            (
                r#"SELECT array_agg(j ORDER BY id) FROM t;"#,
                vec![vec![t(r#"{"{\"a\": 1}",[1],NULL,"\"x\"","null"}"#)]],
            ),
            (
                r#"SELECT array_agg(s) FROM t WHERE false;"#,
                vec![vec![Null]],
            ),
            (
                r#"SELECT g, array_agg(s) FILTER (WHERE s IS NOT NULL) FROM t GROUP BY g ORDER BY g;"#,
                vec![
                    vec![Int(1), t("{b,a}")],
                    vec![Int(2), t(r#"{"a b"}"#)],
                    vec![Int(3), t(r#"{"c\"q\\"}"#)],
                ],
            ),
            (
                r#"SELECT pg_typeof(array_agg(s)), pg_typeof(array_agg(u)), pg_typeof(array_agg(id)), pg_typeof(array_agg(b)), pg_typeof(array_agg(n)), pg_typeof(array_agg(f)), pg_typeof(array_agg(ts)), pg_typeof(array_agg(j)) FROM t;"#,
                vec![vec![
                    t("text[]"),
                    t("uuid[]"),
                    t("integer[]"),
                    t("bigint[]"),
                    t("numeric[]"),
                    t("boolean[]"),
                    t("timestamp without time zone[]"),
                    t("jsonb[]"),
                ]],
            ),
            (
                r#"SELECT array_length(array_agg(s), 1), cardinality(array_agg(DISTINCT s)) FROM t;"#,
                vec![vec![Int(5), Int(5)]],
            ),
            (
                r#"SELECT array_agg(DISTINCT s ORDER BY s DESC) FROM t;"#,
                vec![vec![t(r#"{NULL,"c\"q\\",b,"a b",a}"#)]],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(run(session, sql), expected, "{sql}");
        }
        for (sql, expected, message) in [
            // PostgreSQL returns {{1,2},{3,4}}. The flat array codec cannot
            // preserve a second dimension.
            (
                r#"SELECT array_agg(x) FROM (VALUES (ARRAY[1,2]),(ARRAY[3,4])) v(x);"#,
                SqlError::Unsupported,
                None,
            ),
            (
                r#"SELECT array_agg(DISTINCT s ORDER BY id) FROM t;"#,
                SqlError::InvalidColumnReference,
                Some(
                    "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
                ),
            ),
            (
                r#"SELECT array_agg(s, s) FROM t;"#,
                SqlError::UndefinedFunction,
                Some("function array_agg(text, text) does not exist"),
            ),
            (
                r#"SELECT array_agg() FROM t;"#,
                SqlError::UndefinedFunction,
                Some("function array_agg() does not exist"),
            ),
        ] {
            assert_eq!(
                session.execute(sql, &[], None),
                Err(CoreError::Sql(expected)),
                "{sql}"
            );
            if let Some(message) = message {
                assert_eq!(
                    giggles_core::relational::take_sql_error_message().as_deref(),
                    Some(message),
                    "{sql}"
                );
            }
        }
    });
}

#[test]
fn additional_element_types_and_ordering() {
    database(|session| {
        let cases = [
            (
                "SELECT array_agg(x), pg_typeof(array_agg(x)) FROM (VALUES (''::text),('NULL'),('null'),('a b'),(NULL)) v(x)",
                r#"{"","NULL","null","a b",NULL}"#,
                "text[]",
            ),
            (
                "SELECT array_agg(x), pg_typeof(array_agg(x)) FROM (VALUES ('2024-01-02 03:04:05+01'::timestamptz),(NULL)) v(x)",
                r#"{"2024-01-02 02:04:05+00",NULL}"#,
                "timestamp with time zone[]",
            ),
            (
                "SELECT array_agg(x), pg_typeof(array_agg(x)) FROM (VALUES ('2024-01-02'::date),(NULL)) v(x)",
                "{2024-01-02,NULL}",
                "date[]",
            ),
            (
                "SELECT array_agg(x), pg_typeof(array_agg(x)) FROM (VALUES ('hi'::varchar),(NULL)) v(x)",
                "{hi,NULL}",
                "character varying[]",
            ),
            (
                "SELECT array_agg(DISTINCT x::int2), pg_typeof(array_agg(x::int2)) FROM (VALUES (20),(2),(NULL),(2)) v(x)",
                "{2,20,NULL}",
                "smallint[]",
            ),
            (
                "SELECT array_agg(x::json), pg_typeof(array_agg(x::json)) FROM (VALUES ('{\"a\":1}'::text),('null'),(NULL)) v(x)",
                r#"{"{\"a\":1}","null",NULL}"#,
                "json[]",
            ),
            (
                "SELECT array_agg(x), pg_typeof(array_agg(x)) FROM (VALUES ('NaN'::real),(2.5::real),(NULL)) v(x)",
                "{NaN,2.5,NULL}",
                "real[]",
            ),
            (
                "SELECT array_agg(weights[1] ORDER BY lexeme), pg_typeof(array_agg(weights[1])) FROM unnest('a:1A b:2B'::tsvector) AS u(lexeme, positions, weights)",
                "{A,B}",
                r#""char"[]"#,
            ),
            (
                "SELECT array_agg(x), pg_typeof(array_agg(x)) FROM (VALUES ('simple'::regdictionary),(NULL)) v(x)",
                "{simple,NULL}",
                "regdictionary[]",
            ),
        ];
        for (sql, literal, ty) in cases {
            assert_eq!(run(session, sql), vec![vec![t(literal), t(ty)]], "{sql}");
        }
        assert_eq!(
            run(
                session,
                "SELECT array_agg(x ORDER BY x DESC NULLS LAST) FROM (VALUES (2::int),(NULL),(5),(2)) t(x)"
            ),
            vec![vec![t("{5,2,2,NULL}")]],
        );
        assert_eq!(
            run(
                session,
                r#"SELECT array_agg(DISTINCT j) FROM (VALUES ('[1]'::jsonb), ('[]'), ('false'), ('true'), ('10'), ('2'), ('"a"'), ('null'), ('{}'), ('{"z":1}'), ('{"aa":1}'), ('{"a":1}'), ('{"a":1,"b":2}'), ('[0,0]'), ('[2]'), ('[10]'), (NULL)) v(j)"#
            ),
            vec![vec![t(
                r#"{[],"null","\"a\"",2,10,false,true,[1],[2],[10],"[0, 0]","{}","{\"a\": 1}","{\"aa\": 1}","{\"z\": 1}","{\"a\": 1, \"b\": 2}",NULL}"#
            )]],
        );
    });
}

#[test]
fn grouped_archdocs_heading_query_and_outer_array_consumers() {
    database(|session| {
        let sql = r#"WITH normalized AS (
            SELECT * FROM (VALUES
                (1, 20, 2, 'Later'::text, 'b'::text, 'shared'::text),
                (2, 10, 2, 'Second', 'a', 'shared'),
                (3, 10, 1, 'Shared', 'a', 'shared'),
                (3, 10, 3, 'Third', NULL, 'shared'),
                (4, 15, 1, 'Other', 'skip', 'other')
            ) AS entries(id, number, ordinality, heading, outline, key)
        ), hints AS (
            SELECT (array_agg(heading ORDER BY number, ordinality))[1] AS heading,
                   count(DISTINCT id) AS count, array_agg(DISTINCT outline) AS outlines
            FROM normalized GROUP BY key HAVING count(DISTINCT id) >= 3
        )
        SELECT heading, count, outlines, outlines[2], cardinality(outlines)
        FROM hints"#;
        assert_eq!(
            run(session, sql),
            vec![vec![
                t("Shared"),
                Value::Int(3),
                t("{a,b,NULL}"),
                t("b"),
                Value::Int(3)
            ]]
        );
    });
}

#[test]
fn query_only_arrays_assign_to_durable_columns() {
    database(|session| {
        run(
            session,
            "CREATE TABLE src (id int, v varchar, si smallint, n numeric, b boolean, d date, ts timestamp, tz timestamptz, jb jsonb, r real)",
        );
        run(
            session,
            r#"INSERT INTO src VALUES
            (1,'a,b',2,2.50,true,'2024-01-02','2024-01-02 03:04:05.5','2024-01-02 03:04:05+00','{"b":1}',2.5),
            (2,'NULL',-3,-2.50,false,NULL,NULL,NULL,'[1,true]',3.5),
            (3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"#,
        );
        run(
            session,
            "CREATE TABLE dst (id int PRIMARY KEY, a text[], i int[], l bigint[], u uuid[])",
        );
        run(
            session,
            "INSERT INTO dst SELECT 1, array_agg(v ORDER BY id), array_agg(si ORDER BY id), array_agg(si ORDER BY id), NULL FROM src",
        );
        run(
            session,
            "INSERT INTO dst SELECT 2, array_agg(n ORDER BY id), array_agg(n ORDER BY id), array_agg(n ORDER BY id), NULL FROM src",
        );
        run(
            session,
            "INSERT INTO dst SELECT 3, array_agg(b ORDER BY id), array_agg(r ORDER BY id), array_agg(r ORDER BY id), NULL FROM src",
        );
        run(
            session,
            "INSERT INTO dst SELECT 4, array_agg(tz ORDER BY id), NULL, NULL, NULL FROM src",
        );
        run(
            session,
            "INSERT INTO dst SELECT 5, array_agg(v), array_agg(n), array_agg(si), NULL FROM src WHERE false",
        );
        run(
            session,
            "UPDATE dst SET a = (SELECT array_agg(v ORDER BY id) FROM src) WHERE id = 5",
        );
        run(
            session,
            "UPDATE dst SET (a, i) = (SELECT array_agg(b ORDER BY id), array_agg(n ORDER BY id) FROM src) WHERE id = 4",
        );
        assert_eq!(
            run(session, "SELECT a, i, l FROM dst ORDER BY id"),
            vec![
                vec![
                    t(r#"{"a,b","NULL",NULL}"#),
                    t("{2,-3,NULL}"),
                    t("{2,-3,NULL}")
                ],
                vec![t("{2.50,-2.50,NULL}"), t("{3,-3,NULL}"), t("{3,-3,NULL}")],
                vec![t("{true,false,NULL}"), t("{2,4,NULL}"), t("{2,4,NULL}")],
                vec![t("{true,false,NULL}"), t("{3,-3,NULL}"), Value::Null],
                vec![t(r#"{"a,b","NULL",NULL}"#), Value::Null, Value::Null],
            ],
        );
        let cases = [
            ("d", "{2024-01-02,NULL,NULL}"),
            ("ts", r#"{"2024-01-02 03:04:05.5",NULL,NULL}"#),
            ("tz", r#"{"2024-01-01 22:04:05-05",NULL,NULL}"#),
            ("jb", r#"{"{\"b\": 1}","[1, true]",NULL}"#),
            ("r", "{2.5,3.5,NULL}"),
            ("si", "{2,-3,NULL}"),
        ];
        for (i, (column, expected)) in cases.into_iter().enumerate() {
            assert_eq!(
                run_zoned(
                    session,
                    &format!(
                        "INSERT INTO dst (id,a) SELECT {}, array_agg({column} ORDER BY id) FROM src RETURNING a",
                        i + 10
                    )
                ),
                vec![vec![t(expected)]],
                "{column}",
            );
        }
        assert_eq!(
            run(
                session,
                r#"INSERT INTO dst (id,a) SELECT 20, array_agg('{"a":1,"a":2}'::json) RETURNING a"#
            ),
            vec![vec![t(r#"{"{\"a\":1,\"a\":2}"}"#)]],
        );
        assert_eq!(
            run(
                session,
                "INSERT INTO dst (id,a) SELECT 21, array_agg(weights[1]) FROM unnest('a:1A'::tsvector) AS u(lexeme, positions, weights) RETURNING a"
            ),
            vec![vec![t("{A}")]],
        );
        assert_eq!(
            run(
                session,
                "INSERT INTO dst SELECT 22, array_agg('simple'::regdictionary), array_agg('simple'::regdictionary), array_agg('simple'::regdictionary), NULL RETURNING a,i,l"
            ),
            vec![vec![t("{simple}"), t("{3765}"), t("{3765}")]],
        );
        for (sql, error) in [
            (
                "INSERT INTO dst (id,i) SELECT 30, array_agg(2147483647.5)",
                SqlError::Overflow,
            ),
            (
                "INSERT INTO dst (id,l) SELECT 30, array_agg(9223372036854775807.5)",
                SqlError::Overflow,
            ),
            (
                "INSERT INTO dst (id,i) SELECT 30, array_agg('NaN'::real)",
                SqlError::Overflow,
            ),
            (
                "INSERT INTO dst (id,i) SELECT 30, array_agg('NaN'::numeric)",
                SqlError::Unsupported,
            ),
            (
                "INSERT INTO dst (id,l) SELECT 30, array_agg('Infinity'::numeric)",
                SqlError::Unsupported,
            ),
            (
                "INSERT INTO dst (id,i) SELECT 30, array_agg(v) FROM src",
                SqlError::Type,
            ),
            (
                "UPDATE dst SET u = (SELECT array_agg(v) FROM src) WHERE id = 1",
                SqlError::Type,
            ),
        ] {
            assert_eq!(
                session.execute(sql, &[], None),
                Err(CoreError::Sql(error)),
                "{sql}"
            );
        }
        session.set_text_settings(
            DateStyleSettings {
                style: DateStyle::Sql,
                order: DateOrder::Dmy,
            },
            Default::default(),
        );
        assert_eq!(
            run_zoned(
                session,
                "INSERT INTO dst (id,a) SELECT 30, array_agg(d ORDER BY id) FROM src RETURNING a"
            ),
            vec![vec![t("{02/01/2024,NULL,NULL}")]],
        );
    });
}

#[test]
fn query_only_arrays_convert_to_json() {
    database(|session| {
        let cases = [
            (
                "SELECT to_jsonb(array_agg(n ORDER BY id)) FROM (VALUES (1,2.50),(2,-2.50),(3,NULL)) s(id,n)",
                "[2.50, -2.50, null]",
            ),
            (
                "SELECT jsonb_build_object('x', array_agg('2024-01-02 03:04:05+00'::timestamptz))",
                r#"{"x": ["2024-01-01T22:04:05-05:00"]}"#,
            ),
            (
                r#"SELECT json_build_object('x', array_agg('{"a":1,"a":2}'::json))"#,
                r#"{"x" : [{"a":1,"a":2}]}"#,
            ),
            (
                r#"SELECT row_to_json(ROW(array_agg('2024-01-02 03:04:05.5'::timestamp), array_agg('{"b":1}'::jsonb)))"#,
                r#"{"f1":["2024-01-02T03:04:05.5"],"f2":[{"b": 1}]}"#,
            ),
            (
                "SELECT json_agg(x) FROM (SELECT array_agg(b ORDER BY id) x FROM (VALUES (1,true),(2,false),(3,NULL)) s(id,b)) q",
                "[[true,false,null]]",
            ),
            (
                "SELECT jsonb_agg(x) FROM (SELECT array_agg(b ORDER BY id) x FROM (VALUES (1,true),(2,false),(3,NULL)) s(id,b)) q",
                "[[true, false, null]]",
            ),
            (
                "SELECT to_json(array_agg('2024-01-02'::date))",
                r#"["2024-01-02"]"#,
            ),
            ("SELECT to_json(array_agg('a,b'::varchar))", r#"["a,b"]"#),
            ("SELECT to_json(array_agg(2.5::real))", "[2.5]"),
            ("SELECT to_json(array_agg(2::smallint))", "[2]"),
            (
                "SELECT to_json(array_agg(weights[1])) FROM unnest('a:1A'::tsvector) AS u(lexeme, positions, weights)",
                r#"["A"]"#,
            ),
            (
                "SELECT to_json(array_agg('simple'::regdictionary))",
                r#"["simple"]"#,
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(run_zoned(session, sql), vec![vec![t(expected)]], "{sql}");
        }
    });
}

#[test]
fn query_only_arrays_concatenate() {
    database(|session| {
        let cases = [
            (
                "SELECT array_agg(n ORDER BY id) || 5.5 FROM (VALUES (1,2.50),(2,-2.50),(3,NULL)) s(id,n)",
                "{2.50,-2.50,NULL,5.5}",
            ),
            ("SELECT 5.5 || array_agg(2.50)", "{5.5,2.50}"),
            ("SELECT array_agg(2.50) || 5", "{2.50,5}"),
            ("SELECT array_agg(2.50) || array_agg(3.5)", "{2.50,3.5}"),
            ("SELECT array_agg(2.50) || '{3.50}'", "{2.50,3.50}"),
            ("SELECT array_agg(2.50) || NULL", "{2.50}"),
            ("SELECT array_agg(2.50) || NULL::numeric", "{2.50,NULL}"),
            ("SELECT array_agg(true) || false", "{t,f}"),
            (
                "SELECT array_agg('NULL'::varchar) || 'a,b'::varchar",
                r#"{"NULL","a,b"}"#,
            ),
            (
                "SELECT array_agg('NULL'::varchar) || 'a,b'::text",
                r#"{"NULL","a,b"}"#,
            ),
            (
                "SELECT array_agg('2024-01-02 03:04:05+00'::timestamptz) || '2024-01-02 04:04:05+00'::timestamptz",
                r#"{"2024-01-01 22:04:05-05","2024-01-01 23:04:05-05"}"#,
            ),
            (
                r#"SELECT array_agg('{"a":1,"a":2}'::json) || '[]'::json"#,
                r#"{"{\"a\":1,\"a\":2}",[]}"#,
            ),
            ("SELECT array_agg(2::smallint) || 3::smallint", "{2,3}"),
            ("SELECT array_agg(2.5::real) || 3.5::real", "{2.5,3.5}"),
            ("SELECT array_agg(2.5::numeric)::int[]", "{3}"),
        ];
        for (sql, expected) in cases {
            assert_eq!(run_zoned(session, sql), vec![vec![t(expected)]], "{sql}");
        }
    });
}

#[test]
fn unsupported_array_operations_keep_their_error_class() {
    database(|session| {
        for sql in [
            "SELECT array_agg(n) OVER (ORDER BY n) FROM (VALUES (2.5)) s(n)",
            "SELECT array_to_string(array_agg(2.5), '|')",
            "SELECT unnest(array_agg(2.5))",
            "SELECT 2.5 = ANY(array_agg(2.5))",
            "SELECT array_agg(2.5) && array_agg(2.5)",
            "SELECT array_append(array_agg(2.5), 5.5)",
            "SELECT width_bucket(2.5, array_agg(1.0))",
            "SELECT array_agg(2.5) || '{{1.0},{2.0}}'",
            "SELECT array_agg(true)::int[]",
            "SELECT CASE WHEN true THEN array_agg(1.5) ELSE array_agg(1) END",
            "SELECT array_agg(1.5) UNION ALL SELECT array_agg(1)",
            "SELECT array_agg('2024-01-02'::date) || '2024-01-02'::timestamp",
            "CREATE TABLE ctas AS SELECT array_agg(2.5) AS a",
            "CREATE VIEW va AS SELECT array_agg(2.5) AS a",
        ] {
            assert_eq!(
                session.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::Unsupported)),
                "{sql}"
            );
        }
        for sql in [
            "SELECT array_agg(true) || 1",
            "SELECT array_agg('[]'::json) || '[]'::text",
        ] {
            assert_eq!(
                session.execute(sql, &[], None),
                Err(CoreError::Sql(SqlError::UndefinedFunction)),
                "{sql}"
            );
        }
    });
}

#[test]
fn enum_array_metadata_and_label_order() {
    database(|session| {
        run(session, "CREATE TYPE mood AS ENUM ('z', 'a')");
        run(
            session,
            "CREATE TABLE samples(id int PRIMARY KEY, label mood)",
        );
        run(
            session,
            "INSERT INTO samples VALUES (1,'a'), (2,NULL), (3,'z'), (4,'a')",
        );
        assert_eq!(
            run(
                session,
                "SELECT array_agg(DISTINCT label), (array_agg(label ORDER BY label DESC))[1], pg_typeof(array_agg(label)) FROM samples"
            ),
            vec![vec![t("{z,a,NULL}"), Value::Null, t("mood[]")]],
        );
    });
}
