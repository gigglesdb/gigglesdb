mod common;

use common::fresh;
use giggles_core::relational::{DataType, Value};
use giggles_core::{Admin, Limits};

#[test]
fn scalars_over_rows_report_values_types_and_names() {
    let admin = Admin::create(fresh("pgparity-scalars-w06"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE samples(id integer PRIMARY KEY, n smallint, s text)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO samples VALUES (2, -3, 'abc'), (1, 5, 'éCLaiR'), (3, NULL, NULL)",
            &[],
            None,
        )
        .unwrap();
    let query = r"SELECT id, n & 3::smallint, n ^ 2, n BETWEEN SYMMETRIC 6 AND 1,
        nullif(n,5), num_nulls(n,s,NULL), width_bucket(n,0,10,5), ROW(n,s) < ROW(5,'z'),
        upper(s), initcap(s), s ^@ 'a', normalize(s,NFC) FROM samples ORDER BY id";
    let prepared = session.prepare(query).unwrap();
    let description = session.describe(&prepared).unwrap();
    use DataType::*;
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        [
            Integer,
            SmallInt,
            DoublePrecision,
            Boolean,
            SmallInt,
            Integer,
            Integer,
            Boolean,
            Text,
            Text,
            Boolean,
            Text
        ],
    );
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        [
            "id",
            "?column?",
            "?column?",
            "?column?",
            "nullif",
            "num_nulls",
            "width_bucket",
            "?column?",
            "upper",
            "initcap",
            "?column?",
            "normalize"
        ],
    );
    let result = session.execute_prepared(&prepared, &[], None).unwrap();
    let text = |s: &str| Value::Text(s.into());
    let float = |n: f64| Value::Float8(giggles_core::relational::Float8::from_bits(n.to_bits()));
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        [
            vec![
                Value::Int(1),
                Value::Int(1),
                float(25.0),
                Value::Bool(true),
                Value::Null,
                Value::Int(1),
                Value::Int(3),
                Value::Bool(false),
                text("ÉCLAIR"),
                text("Éclair"),
                Value::Bool(false),
                text("éCLaiR")
            ],
            vec![
                Value::Int(2),
                Value::Int(1),
                float(9.0),
                Value::Bool(false),
                Value::Int(-3),
                Value::Int(1),
                Value::Int(0),
                Value::Bool(true),
                text("ABC"),
                text("Abc"),
                Value::Bool(true),
                text("abc")
            ],
            vec![
                Value::Int(3),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int(3),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null
            ],
        ],
    );
    let result = session
        .execute(
            r"SELECT pg_typeof(nullif(1,1.0)), pg_typeof(|/ 4.0), 'a'||'b',
           U&'é' IS NFC NORMALIZED, U&'e\0301' IS NOT NORMALIZED, unistr('\D83D\DE00')",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        [
            text("numeric"),
            text("double precision"),
            text("ab"),
            Value::Bool(true),
            Value::Bool(true),
            text("😀")
        ]
    );
}

#[test]
fn review_between_and_nullif_checks_use_postgres_catalog_text() {
    let admin = Admin::create(fresh("pgparity-w06-checks"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute(
        "CREATE TABLE w3_checks (x int PRIMARY KEY,
         CONSTRAINT w3_ck_a CHECK (x BETWEEN 2 AND 8),
         CONSTRAINT w3_ck_b CHECK (x NOT BETWEEN 2 AND 8),
         CONSTRAINT w3_ck_c CHECK (x BETWEEN SYMMETRIC 2 AND 8),
         CONSTRAINT w3_ck_d CHECK (x NOT BETWEEN SYMMETRIC 2 AND 8),
         CONSTRAINT w3_ck_e CHECK (NULLIF(x,0) IS NOT NULL),
         CONSTRAINT w3_ck_g CHECK (num_nonnulls(x) = 1),
         CONSTRAINT w3_ck_h CHECK (x BETWEEN ASYMMETRIC 2 AND 8),
         CONSTRAINT w3_ck_i CHECK (x NOT BETWEEN ASYMMETRIC 2 AND 8))",
        &[], None,
    ).unwrap();
    let checks = admin.catalog_presentation().unwrap().constraints;
    let stored: std::collections::BTreeMap<_, _> = checks.into_iter()
        .filter(|constraint| constraint.kind == 'c')
        .map(|constraint| (constraint.name, format!("CHECK ({})", constraint.expression.unwrap())))
        .collect();
    let expected = [
        ("w3_ck_a", "CHECK (((x >= 2) AND (x <= 8)))"),
        ("w3_ck_b", "CHECK (((x < 2) OR (x > 8)))"),
        ("w3_ck_c", "CHECK ((((x >= 2) AND (x <= 8)) OR ((x >= 8) AND (x <= 2))))"),
        ("w3_ck_d", "CHECK ((((x < 2) OR (x > 8)) AND ((x < 8) OR (x > 2))))"),
        ("w3_ck_e", "CHECK ((NULLIF(x, 0) IS NOT NULL))"),
        ("w3_ck_g", "CHECK ((num_nonnulls(x) = 1))"),
        ("w3_ck_h", "CHECK (((x >= 2) AND (x <= 8)))"),
        ("w3_ck_i", "CHECK (((x < 2) OR (x > 8)))"),
    ];
    assert_eq!(
        stored,
        expected.into_iter().map(|(name, text)| (name.to_owned(), text.to_owned())).collect(),
    );
    session.execute(
        "CREATE TABLE w3_nullif (x int PRIMARY KEY CHECK (NULLIF(x,0) IS NOT NULL))",
        &[], None,
    ).unwrap();
    session.execute("INSERT INTO w3_nullif VALUES (1)", &[], None).unwrap();
    let error = session.execute("INSERT INTO w3_nullif VALUES (0)", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "23514");

    for expression in [
        "x ^ 2 > 0", "x & 1 > 0", "x | 1 > 0", "x # 1 > 0", "~x > 0",
        "x << 1 > 0", "x >> 1 > 0", "@ x > 0", "|/ x > 0", "||/ x > 0",
        "width_bucket(x,0,10,1) >= 0", "s ~~ 'x'", "s ^@ 'x'", "(x,1) < (2,3)",
        "normalize(s) = s", "normalize(s, NFD) = s", "s IS NORMALIZED",
        "s IS NFC NORMALIZED", "s IS NFD NORMALIZED", "s IS NFKC NORMALIZED",
        "s IS NFKD NORMALIZED", "s IS NOT NORMALIZED",
    ] {
        let sql = format!("CREATE TABLE unsupported_check (x int PRIMARY KEY, s text, CHECK ({expression}))");
        let error = session.execute(&sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "0A000", "{sql}: {error:?}");
    }
}

#[test]
fn review_array_order_is_element_wise_and_rejects_row_array_ordering() {
    let admin = Admin::create(fresh("pgparity-w06-array-order"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let result = session.execute(
        r#"SELECT ARRAY[10] < ARRAY[2], ARRAY[10] <= ARRAY[2],
         ARRAY[10] > ARRAY[2], ARRAY[10] >= ARRAY[2],
         '{"a,b"}'::text[] < '{a,b}'::text[],
         (1, ARRAY[1]) = (1, ARRAY[1]), (1, ARRAY[1]) <> (1, ARRAY[2]),
         (1, ARRAY[1]) IS DISTINCT FROM (1, ARRAY[1])"#,
        &[], None,
    ).unwrap();
    assert_eq!(
        result.rows[0].values,
        // GD-0092 S0: element-wise array ordering (PostgreSQL 18.6: f|f|t|t|f|t|t|f).
        [false, false, true, true, false, true, true, false].map(Value::Bool),
    );

    for expression in [
        "(ARRAY[10],2) < (ARRAY[2],1)",
        "(1, ARRAY['a']) <= (2, ARRAY['b'])",
        "(ARRAY[10]::bigint[],2) > (ARRAY[2]::bigint[],1)",
        "(1, ARRAY[1]) >= (2, ARRAY[2])",
        "(1, '{}'::jsonb) < (2, '{}'::jsonb)",
    ] {
        let sql = format!("SELECT {expression}");
        let error = session.execute(&sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "0A000", "{sql}: {error:?}");
    }

    for sql in [
        "CREATE TABLE array_check (id int PRIMARY KEY, a int[], b int[], label text, CHECK (a < b))",
        "INSERT INTO array_check VALUES (1, '{2}', '{10}', 'old'), (2, '{9,1}', '{10}', 'second')",
        "UPDATE array_check SET label = 'new' WHERE id = 1",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    // PG 18.6: '{10}' < '{2}' is false element-wise, so the CHECK rejects it.
    let error = session
        .execute("INSERT INTO array_check VALUES (3, '{10}', '{2}', 'bad')", &[], None)
        .unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "23514", "{error:?}");
    let result = session.execute("SELECT label FROM array_check ORDER BY a", &[], None).unwrap();
    assert_eq!(result.rows[0].values, [Value::Text("new".into())]);
    assert_eq!(result.rows[1].values, [Value::Text("second".into())]);
}

#[test]
fn review_restores_array_containment_and_nested_cast_rejections() {
    let admin = Admin::create(fresh("pgparity-w06-array-rejections"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute(
        "CREATE COLLATION w4_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        &[], None,
    ).unwrap();
    session.execute("CREATE TABLE nested_array (id int PRIMARY KEY, a text[])", &[], None).unwrap();

    for sql in [
        "SELECT ARRAY['A'] COLLATE w4_ci @> ARRAY['a']",
        "SELECT ARRAY['a'] <@ (ARRAY['A'] COLLATE w4_ci)",
        "SELECT ARRAY[1] @> ARRAY[1]",
        "INSERT INTO nested_array SELECT 1, ARRAY[ARRAY['a','b'],ARRAY['c','d']]::text[]",
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "0A000", "{sql}: {error:?}");
    }
    assert!(session.execute("SELECT a FROM nested_array", &[], None).unwrap().rows.is_empty());
}

#[test]
fn review_ilike_locale_and_pattern_collation_budget() {
    let admin = Admin::create(fresh("pgparity-w06-ilike"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute(
        "CREATE COLLATION w4_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        &[], None,
    ).unwrap();
    let result = session.execute(
        r#"SELECT U&'\0130' ILIKE 'i', U&'\00C9' COLLATE "C" ILIKE U&'\00E9',
         U&'\0130' COLLATE "POSIX" ILIKE 'i', U&'\0130' COLLATE "und-x-icu" ILIKE 'i',
         U&'\0130' COLLATE "pg_unicode_fast" ILIKE 'i', 'a' ILIKE ('A' COLLATE "en_US"),
         repeat('a', 4096) LIKE 'b%' COLLATE w4_ci,
         repeat('a', 4096) ILIKE 'B%' COLLATE w4_ci"#,
        &[], None,
    ).unwrap();
    assert_eq!(
        result.rows[0].values,
        [true, false, false, false, false, true, false, false].map(Value::Bool),
    );
}

#[test]
fn review_json_path_whitespace_and_operator_associativity() {
    let admin = Admin::create(fresh("pgparity-w06-json-path"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let result = session.execute(
        r#"SELECT '[1,2]'::jsonb #>> ARRAY[' 1'], '[1,2]'::jsonb #>> ARRAY['1 '],
         '{"a":{"b":"v"}}'::jsonb #> '{a}' ->> 'b',
         '{"a":{"b":"v"}}'::jsonb -> 'a' #>> '{b}'"#,
        &[], None,
    ).unwrap();
    let text = |s: &str| Value::Text(s.into());
    assert_eq!(result.rows[0].values, [text("2"), Value::Null, text("v"), text("v")]);
}

#[test]
fn review_width_bucket_common_type_and_wide_literals() {
    let admin = Admin::create(fresh("pgparity-w06-width-bucket"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let result = session.execute(
        "SELECT width_bucket(1.5,ARRAY[1,2]),
         width_bucket(16777216::real,ARRAY[16777217]),
         width_bucket(16777216::double precision,ARRAY[16777217]),
         width_bucket(1::numeric,-9e131071,9e131071,4),
         width_bucket(-1::numeric,-9e131071,9e131071,4),
         width_bucket(1::numeric,9e131071,-9e131071,4),
         width_bucket(1::numeric,'-9e131071'::numeric,'9e131071'::numeric,4),
         width_bucket(1::numeric,'9e131071'::numeric,'-9e131071'::numeric,4),
         width_bucket(0.99999999999999999999::numeric,0::numeric,2::numeric,2),
         num_nulls(VARIADIC ARRAY[1,NULL]), num_nulls(VARIADIC NULL::int[])",
        &[], None,
    ).unwrap();
    assert_eq!(result.rows[0].values, [
        Value::Int(1), Value::Int(1), Value::Int(0), Value::Int(3), Value::Int(2),
        Value::Int(2), Value::Int(3), Value::Int(2), Value::Int(1), Value::Int(1), Value::Null,
    ]);
}

#[test]
fn review_strict_nulls_follow_existing_sequence_evaluation() {
    let admin = Admin::create(fresh("pgparity-w06-strict-null"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let clock = giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 86_400_000_100,
        statement_us: 86_400_000_100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    };
    for (sequence, expression) in [
        ("w3_bare", "NULL::integer + nextval('w3_bare')"),
        ("w3_width", "width_bucket(NULL,nextval('w3_width'),10,4)"),
        ("w3_power", "NULL::integer ^ nextval('w3_power')"),
        ("w3_nullif", "NULLIF(NULL::integer,nextval('w3_nullif'))"),
    ] {
        session.execute_at(&format!("CREATE SEQUENCE {sequence}"), &[], None, &clock).unwrap();
        let result = session.execute_at(&format!("SELECT {expression}"), &[], None, &clock)
            .unwrap_or_else(|error| panic!("{expression}: {error:?}"));
        assert_eq!(result.rows[0].values, [Value::Null], "{expression}");
        let value = session.execute_at(&format!("SELECT currval('{sequence}')"), &[], None, &clock).unwrap();
        assert_eq!(value.rows[0].values, [Value::Int(1)], "{expression}");
    }
}

#[test]
fn review_starts_with_checks_collation_for_function_and_operator() {
    let admin = Admin::create(fresh("pgparity-w06-starts-with"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute(
        "CREATE COLLATION w3_nondet (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        &[], None,
    ).unwrap();
    let result = session.execute("SELECT 'Ab' ^@ 'A', starts_with('Ab', 'A')", &[], None).unwrap();
    assert_eq!(result.rows[0].values, [Value::Bool(true), Value::Bool(true)]);
    for sql in [
        "SELECT 'A' COLLATE w3_nondet ^@ 'a'",
        "SELECT starts_with('A' COLLATE w3_nondet, 'a')",
        "SELECT starts_with('A', 'a' COLLATE w3_nondet)",
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "0A000", "{sql}");
        assert_eq!(
            giggles_core::relational::take_sql_error_message().as_deref(),
            Some("nondeterministic collations are not supported for substring searches"),
            "{sql}",
        );
    }
}
