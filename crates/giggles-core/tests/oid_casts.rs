mod common;

use Value::{Bool, Int, Null};
use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{DataType, Value},
    sqlstate,
};

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn check_rows(session: &mut SqlSession, sql: &str, expected: &[Vec<Value>]) {
    let result = session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let actual = result
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{sql}");
}

fn check_error(session: &mut SqlSession, sql: &str, params: &[Value], state: &str) {
    let error = session.execute(sql, params, None).unwrap_err();
    assert_eq!(sqlstate(&error), state, "{sql}: {error:?}");
}

fn check_result_type(session: &mut SqlSession, sql: &str, column: usize, expected: &DataType) {
    let prepared = session
        .prepare(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let desc = session
        .describe(&prepared)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    assert_eq!(&desc.columns[column].data_type, expected, "{sql}");
}

fn verify_oid_cast_matrix(session: &mut SqlSession) {
    for (expression, expected, ty) in [
        (r"E'12\v'::oid", Int(12), DataType::Oid),
        (r"E' \t\n\r\v\f12 \t\n\r\v\f'::oid", Int(12), DataType::Oid),
        ("'010'::regclass::oid", Int(8), DataType::Oid),
        ("'5'::oid::integer", Int(5), DataType::Integer),
        ("'5'::oid::bigint", Int(5), DataType::BigInt),
        ("'5'::oid::varchar", text("5"), DataType::Varchar),
        ("'5'::oid::char(3)", text("5  "), DataType::Bpchar),
        ("'5'::oid::text", text("5"), DataType::Text),
        ("5::smallint::oid", Int(5), DataType::Oid),
        ("'5'::varchar::oid", Int(5), DataType::Oid),
        ("'5'::char(2)::oid", Int(5), DataType::Oid),
        ("'pg_class'::regclass::oid", Int(1259), DataType::Oid),
        ("'int4'::regtype::oid", Int(23), DataType::Oid),
        ("'5'::oid || 'x'", text("5x"), DataType::Text),
        ("'5'::oid = 5::bigint", Bool(true), DataType::Boolean),
        ("'5'::oid = 5::smallint", Bool(true), DataType::Boolean),
        ("'5'::oid IN (5, 6)", Bool(true), DataType::Boolean),
        ("coalesce(NULL::oid, 7)", Int(7), DataType::Oid),
        ("coalesce(NULL::oid, -1)", Int(4294967295), DataType::Oid),
        (
            "CASE WHEN true THEN '5'::oid ELSE 6 END",
            Int(5),
            DataType::Oid,
        ),
        (
            "CASE WHEN false THEN '5'::oid ELSE -1 END",
            Int(4294967295),
            DataType::Oid,
        ),
        ("'5'::oid::text::oid", Int(5), DataType::Oid),
        ("'+7'::oid", Int(7), DataType::Oid),
        ("'0X1f'::oid", Int(31), DataType::Oid),
        ("'-0'::oid", Int(0), DataType::Oid),
        ("'010'::oid::regconfig::text", text("8"), DataType::Text),
        (
            "'0xEA4'::oid::regconfig::text",
            text("simple"),
            DataType::Text,
        ),
        ("3748::oid::regconfig::text", text("simple"), DataType::Text),
        (
            "'3748'::oid::regconfig::text",
            text("simple"),
            DataType::Text,
        ),
        ("'english'::regconfig::oid", Int(13248), DataType::Oid),
        (
            "'0xEA4'::oid::regdictionary::text",
            text("3748"),
            DataType::Text,
        ),
    ] {
        let bare = format!("SELECT {expression}");
        let from_table = format!("SELECT id, {expression} FROM t ORDER BY id");
        check_rows(session, &bare, &[vec![expected.clone()]]);
        check_rows(
            session,
            &from_table,
            &[vec![Int(1), expected.clone()], vec![Int(2), expected]],
        );
        check_result_type(session, &bare, 0, &ty);
        check_result_type(session, &from_table, 1, &ty);
    }

    for (expression, state) in [
        ("'0x10'::regclass::oid", "0A000"),
        ("' 123'::regclass::oid", "0A000"),
        ("'123 '::regclass::oid", "0A000"),
        ("'123abc'::regclass::oid", "0A000"),
        ("'5'::oid::smallint", "42846"),
        ("'4294967295'::oid::smallint", "42846"),
        ("'5'::oid::numeric", "42846"),
        ("'5'::oid::real", "42846"),
        ("'5'::oid::double precision", "42846"),
        ("'5'::oid::boolean", "42846"),
        ("'5'::oid::bytea", "42846"),
        ("'5'::oid::jsonb", "42846"),
        ("'5'::oid::uuid", "42846"),
        ("'5'::oid::date", "42846"),
        ("5::numeric::oid", "42846"),
        ("5.0::oid", "42846"),
        ("5::real::oid", "42846"),
        ("true::oid", "42846"),
        ("'5'::jsonb::oid", "42846"),
        ("'5'::oid::name", "0A000"),
        ("'1259'::oid::regclass", "0A000"),
        ("'23'::oid::regtype", "0A000"),
        ("ARRAY['5'::oid, 6::oid]", "0A000"),
        ("'{5,6}'::oid[]", "0A000"),
        ("sum('5'::oid)", "42883"),
        ("'1e3'::oid", "22P02"),
        ("'0o17'::oid", "22P02"),
        ("'1_000'::oid", "22P02"),
        ("(-1::bigint)::oid::regconfig::text", "22003"),
        ("(-1::bigint)::oid::regdictionary::text", "22003"),
    ] {
        check_error(session, &format!("SELECT {expression}"), &[], state);
        check_error(
            session,
            &format!("SELECT id, {expression} FROM t"),
            &[],
            state,
        );
    }
    for (sql, column, expected) in [
        (
            "SELECT '5'::oid UNION SELECT 6 ORDER BY 1",
            0,
            vec![vec![Int(5)], vec![Int(6)]],
        ),
        (
            "SELECT '5'::oid UNION SELECT -1 ORDER BY 1",
            0,
            vec![vec![Int(5)], vec![Int(4294967295)]],
        ),
        (
            "SELECT id, '5'::oid FROM t UNION SELECT id, 6 FROM t ORDER BY 1, 2",
            1,
            vec![
                vec![Int(1), Int(5)],
                vec![Int(1), Int(6)],
                vec![Int(2), Int(5)],
                vec![Int(2), Int(6)],
            ],
        ),
    ] {
        check_rows(session, sql, &expected);
        check_result_type(session, sql, column, &DataType::Oid);
    }
}

fn verify_oid_casts(engine: Engine) {
    let path = common::fresh("oid-casts");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    check_rows(
        &mut session,
        "CREATE TABLE t (id integer PRIMARY KEY, a integer, stored bigint)",
        &[],
    );
    check_rows(
        &mut session,
        "INSERT INTO t VALUES (1,5,255),(2,6,NULL)",
        &[],
    );

    for (expression, expected) in [
        ("'255'::oid", Int(255)),
        ("255::oid", Int(255)),
        ("CAST('255' AS oid)", Int(255)),
        ("oid '255'", Int(255)),
        ("'255'::oid = 255", Bool(true)),
        ("'255'::oid < '300'::oid", Bool(true)),
        ("'0x10'::oid", Int(16)),
        ("'-1'::oid", Int(4294967295)),
        ("'4294967295'::oid", Int(4294967295)),
        ("' 12 '::oid", Int(12)),
        ("'010'::oid", Int(8)),
        ("'0b10'::oid", Int(2)),
        ("(-1)::oid", Int(4294967295)),
        ("5::bigint::oid", Int(5)),
        ("'255'::oid::text", text("255")),
        ("'255'::oid::integer", Int(255)),
        ("'3000000000'::oid::integer", Int(-1294967296)),
        ("'3000000000'::oid::bigint", Int(3000000000)),
        ("pg_typeof('255'::oid)", text("oid")),
        ("'5'::text::oid", Int(5)),
        ("'4294967295'::oid = -1", Bool(true)),
    ] {
        check_rows(
            &mut session,
            &format!("SELECT {expression}"),
            &[vec![expected.clone()]],
        );
        check_rows(
            &mut session,
            &format!("SELECT id, {expression} FROM t ORDER BY id"),
            &[vec![Int(1), expected.clone()], vec![Int(2), expected]],
        );
    }

    for (expression, state) in [
        ("'255'::oid + 1", "42883"),
        ("'4294967296'::oid", "22003"),
        ("'-4294967296'::oid", "22003"),
        ("'abc'::oid", "22P02"),
        ("''::oid", "22P02"),
        ("'12abc'::oid", "22P02"),
        ("4294967296::oid", "22003"),
        ("(-1::bigint)::oid", "22003"),
    ] {
        check_error(&mut session, &format!("SELECT {expression}"), &[], state);
        check_error(
            &mut session,
            &format!("SELECT id, {expression} FROM t"),
            &[],
            state,
        );
    }

    // Stored oid columns require a storage-format extension. Project an
    // oid-typed source from stored rows to exercise the same binder paths.
    const OID_ROWS: &str = "WITH typed AS (SELECT id, stored::oid AS o FROM t) ";
    for (sql, expected) in [
        (
            "SELECT id, a::oid FROM t ORDER BY id",
            vec![vec![Int(1), Int(5)], vec![Int(2), Int(6)]],
        ),
        (
            "SELECT id, o FROM typed WHERE o = '255'::oid",
            vec![vec![Int(1), Int(255)]],
        ),
        (
            "SELECT id, o FROM typed WHERE o = 255",
            vec![vec![Int(1), Int(255)]],
        ),
        (
            "SELECT id, o::text, o::integer FROM typed ORDER BY id",
            vec![
                vec![Int(1), text("255"), Int(255)],
                vec![Int(2), Null, Null],
            ],
        ),
        (
            "SELECT id FROM t WHERE '255'::oid > a::oid ORDER BY id",
            vec![vec![Int(1)], vec![Int(2)]],
        ),
        (
            "SELECT id, o::oid FROM typed ORDER BY id",
            vec![vec![Int(1), Int(255)], vec![Int(2), Null]],
        ),
        (
            "SELECT max('7'::oid), min(o) FROM typed",
            vec![vec![Int(7), Int(255)]],
        ),
        (
            "SELECT max('7'::oid), min('8'::oid)",
            vec![vec![Int(7), Int(8)]],
        ),
    ] {
        if sql.contains(" FROM typed") {
            let sql = format!("{OID_ROWS}{sql}");
            check_rows(&mut session, &sql, &expected);
        } else {
            check_rows(&mut session, sql, &expected);
        }
    }
    check_error(
        &mut session,
        &format!("{OID_ROWS}SELECT id, o + 1 FROM typed ORDER BY id"),
        &[],
        "42883",
    );

    check_rows(
        &mut session,
        "WITH typed AS (SELECT '4294967295'::oid AS o), signed AS (SELECT id, a - 6 AS val FROM t)
         SELECT s.id FROM signed s JOIN typed b ON s.val = b.o ORDER BY s.id",
        &[vec![Int(1)]],
    );

    for (sql, column) in [("SELECT '255'::oid", "oid"), ("SELECT a::oid FROM t", "a")] {
        let prepared = session.prepare(sql).unwrap();
        let desc = session.describe(&prepared).unwrap();
        assert_eq!(desc.columns[0].name, column);
        assert_eq!(desc.columns[0].data_type, DataType::Oid);
    }

    for sql in [
        "SELECT $1::oid",
        "SELECT CAST($1 AS oid)",
        "SELECT $1::oid::text",
        "SELECT $1::integer::oid",
        "SELECT $1::varchar(10)::integer::oid",
        "SELECT a FROM t WHERE a = $1::oid",
    ] {
        let error = session
            .prepare(sql)
            .and_then(|p| session.describe(&p))
            .unwrap_err();
        assert_eq!(sqlstate(&error), "0A000", "{sql}");
        check_error(&mut session, sql, &[Int(255)], "0A000");
    }
    for (sql, ty) in [
        ("SELECT $1::oid", DataType::Text),
        ("SELECT $1::oid", DataType::Integer),
        ("SELECT $1", DataType::Oid),
        ("SELECT $1", DataType::PgChar),
        ("SELECT $1", DataType::PgCharArray),
    ] {
        let result = session.prepare(sql).and_then(|p| {
            let p = p
                .with_parameter_types(&[Some(ty)])
                .map_err(giggles_core::CoreError::Sql)?;
            session.describe(&p)
        });
        assert_eq!(sqlstate(&result.unwrap_err()), "0A000", "{sql}");
    }

    verify_oid_cast_matrix(&mut session);
    verify_oid_review_cases(&mut session);

    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn verify_oid_review_cases(session: &mut SqlSession) {
    for comparison in [
        "a IN (SELECT '4294967295'::oid)",
        "a NOT IN (SELECT '4294967295'::oid)",
        "a::smallint IN (SELECT '4294967295'::oid)",
        "a::oid IN (SELECT -1)",
    ] {
        check_error(session, &format!("SELECT id FROM t WHERE {comparison}"), &[], "0A000");
    }
    check_rows(
        session,
        "SELECT -1 IN (SELECT '4294967295'::oid)",
        &[vec![Bool(true)]],
    );
    for (comparison, expected) in [
        ("a::oid IN (SELECT 5::oid)", vec![vec![Int(1)]]),
        ("a::oid NOT IN (SELECT 5::oid)", vec![vec![Int(2)]]),
        ("a::oid NOT IN (SELECT NULL::oid)", vec![]),
    ] {
        check_rows(session, &format!("SELECT id FROM t WHERE {comparison}"), &expected);
    }

    for sql in [
        "SELECT id FROM t WHERE a::oid = $1",
        "SELECT '5'::oid = $1",
        "SELECT COALESCE(NULL::oid, $1), id FROM t",
        "SELECT CASE WHEN id = 1 THEN a::oid ELSE $1 END FROM t",
        "SELECT id FROM t WHERE a::oid IN ($1, $2)",
        "SELECT $1::oid::regconfig, $1 FROM t",
        "SELECT $1, $1::oid::regconfig FROM t",
        "SELECT $1::oid::regconfig, COALESCE(NULL::oid, $1) FROM t",
        "SELECT $1::oid::regconfig FROM t WHERE '3748'::oid = $1",
        "SELECT $1::oid::regconfig FROM t WHERE a = $1",
    ] {
        let error = session.prepare(sql).and_then(|p| session.describe(&p)).unwrap_err();
        assert_eq!(sqlstate(&error), "0A000", "{sql}");
        check_error(session, &format!("PREPARE rejected AS {sql}"), &[], "0A000");
    }
    let explicit_integer = session
        .prepare("SELECT COALESCE(NULL::oid, $1)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    assert_eq!(sqlstate(&session.describe(&explicit_integer).unwrap_err()), "42804");

    for (sql, expected, oid) in [
        ("SELECT json_build_array('5'::oid)", r#"["5"]"#, 114),
        ("SELECT jsonb_build_array('5'::oid)", r#"["5"]"#, 3802),
        ("SELECT json_build_object('k', '5'::oid)", r#"{"k" : "5"}"#, 114),
        ("SELECT jsonb_build_object('k', (SELECT '5'::oid))", r#"{"k": "5"}"#, 3802),
        ("SELECT to_json(a::oid) FROM t WHERE id = 1", r#""5""#, 114),
        ("SELECT to_jsonb('4294967295'::oid)", r#""4294967295""#, 3802),
        ("SELECT row_to_json(ROW('5'::oid))", r#"{"f1":"5"}"#, 114),
        ("SELECT row_to_json(s) FROM (SELECT a::oid AS o FROM t WHERE id = 1) s", r#"{"o":"5"}"#, 114),
        ("SELECT row_to_json(s) FROM (SELECT '5'::oid AS o, 'true'::json AS j) s", r#"{"o":"5","j":true}"#, 114),
        ("SELECT json_agg((a - 6)::oid ORDER BY id) FROM t", r#"["4294967295", "0"]"#, 114),
        ("SELECT jsonb_agg(a::oid ORDER BY id) FROM t", r#"["5", "6"]"#, 3802),
        ("SELECT json_object_agg(id, a::oid ORDER BY id) FROM t", r#"{ "1" : "5", "2" : "6" }"#, 114),
        ("SELECT jsonb_object_agg(id, a::oid) FROM t", r#"{"1": "5", "2": "6"}"#, 3802),
        ("SELECT json_build_array('5'::oid)->0", r#""5""#, 114),
    ] {
        check_rows(session, sql, &[vec![text(expected)]]);
        let prepared = session.prepare(sql).unwrap();
        let description = session.describe(&prepared).unwrap();
        if oid == 114 {
            assert_eq!(description.type_oids.first(), Some(&oid), "{sql}");
        } else {
            assert_eq!(description.columns[0].data_type, DataType::Jsonb, "{sql}");
        }
    }
    for sql in [
        "SELECT json_typeof(json_build_array('5'::oid)->0)",
        "SELECT jsonb_typeof(jsonb_build_array('5'::oid)->0)",
    ] {
        check_rows(session, sql, &[vec![text("string")]]);
    }
}

#[test]
fn oid_casts_legacy() {
    verify_oid_casts(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn oid_casts_v2() {
    verify_oid_casts(Engine::V2);
}
