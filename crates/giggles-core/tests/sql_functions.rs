mod common;
use common::*;
use giggles_core::{
    relational::{DataType as T, SqlError as E, Value as V, *},
    *,
};
fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("sql-functions"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE f (id BIGINT PRIMARY KEY, s SMALLINT, i INTEGER, b BIGINT, txt TEXT)",
        &[],
        None,
    )
    .unwrap();
    (a, s)
}
fn run(s: &mut SqlSession, q: &str) -> SqlResult {
    s.execute(q, &[], None).unwrap()
}
fn error(s: &mut SqlSession, q: &str, e: E) {
    assert_eq!(s.execute(q, &[], None), Err(CoreError::Sql(e)), "{q}");
}
#[test]
fn nested_rows_literals_null_and_unicode() {
    let (_a, mut s) = db();
    run(&mut s, "INSERT INTO f VALUES(1,-7,-8,-9,'é🙂é')");
    let r = run(
        &mut s,
        "SELECT abs(s), abs(i), abs(b), length(txt), upper(lower('ABC')), substring(txt,2,2), coalesce(NULL,abs(i)), length(substring(txt,2)) FROM f",
    );
    assert_eq!(
        r.rows[0].values,
        vec![
            V::Int(7),
            V::Int(8),
            V::Int(9),
            V::Int(4),
            V::Text("ABC".into()),
            V::Text("🙂e".into()),
            V::Int(8),
            V::Int(3)
        ]
    );
    assert_eq!(run(&mut s,"SELECT length(NULL),lower(NULL),upper(NULL),substring(NULL,1),coalesce(NULL,NULL) FROM f").rows[0].values,vec![V::Null;5]);
    assert_eq!(run(&mut s,"SELECT substring(txt,0,2),substring(txt,-3,2),substring(txt,99),upper('ß'),lower('İ') FROM f").rows[0].values,vec![V::Text("é".into()),V::Text("".into()),V::Text("".into()),V::Text("ß".into()),V::Text("İ".into())]);
}
#[test]
fn describe_preserves_width_and_never_evaluates() {
    let (_a, mut s) = db();
    let p=s.prepare("SELECT abs(s),abs(i),abs(b),length(txt),coalesce(s,i),substring(txt,1,2) FROM f LIMIT 0").unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(
        d.columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![
            T::SmallInt,
            T::Integer,
            T::BigInt,
            T::Integer,
            T::Integer,
            T::Text
        ]
    );
    let p = s.prepare("SELECT abs(-2147483648) FROM f").unwrap();
    s.describe(&p).unwrap();
}
#[test]
fn schema_and_signature_errors_on_empty_and_limit_zero() {
    let (_a, mut s) = db();
    for suffix in ["", " LIMIT 0"] {
        for q in [
            "abs()",
            "length(txt,1)",
            "coalesce()",
            "substring(txt)",
            "length(i)",
            "abs(txt)",
            "lower(i)",
            "coalesce(txt,i)",
            "substring(txt,txt)",
        ] {
            error(&mut s, &format!("SELECT {q} FROM f{suffix}"), E::Type);
        }
        error(
            &mut s,
            &format!("SELECT coalesce(1,abs(missing)) FROM f{suffix}"),
            E::UnknownColumn,
        );
        error(
            &mut s,
            &format!("SELECT unknown(txt) FROM f{suffix}"),
            E::Unsupported,
        );
    }
}
#[test]
fn checked_width_overflow_and_short_circuit() {
    let (_a, mut s) = db();
    run(
        &mut s,
        "INSERT INTO f VALUES(1,-32768,-2147483648,-9223372036854775808,'x')",
    );
    for c in ["s", "i", "b"] {
        error(&mut s, &format!("SELECT abs({c}) FROM f"), E::Overflow);
    }
    error(&mut s, "SELECT abs(-2147483648) FROM f", E::Overflow);
    error(
        &mut s,
        "SELECT substring(txt,1,-1) FROM f",
        E::SubstringError,
    );
    assert_eq!(
        run(&mut s, "SELECT coalesce(1,abs(b)) FROM f").rows[0].values,
        vec![V::Int(1)]
    );
}
#[test]
fn projection_bounds_and_transaction_failure_are_atomic() {
    let (_a, mut s) = db();
    run(&mut s, "INSERT INTO f VALUES(1,1,1,1,'ok')");
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO f VALUES(2,2,2,2,'staged')");
    error(&mut s, "SELECT abs(-2147483648) FROM f", E::Overflow);
    error(&mut s, "SELECT id FROM f LIMIT 10", E::Aborted);
    run(&mut s, "ROLLBACK");
    assert_eq!(run(&mut s, "SELECT id FROM f LIMIT 10").rows.len(), 1);
    let (_a, mut s) = db();
    let text = "ß".repeat(MAX_ROW_BYTES / 2 - 40);
    s.execute("INSERT INTO f VALUES(1,1,1,1,$1)", &[V::Text(text)], None)
        .unwrap();
    // Repeated projections must fail even when LIMIT would discard their rows.
    let calls = vec!["upper(txt)"; 32].join(",");
    error(&mut s, &format!("SELECT {calls} FROM f LIMIT 0"), E::Limit);
}

#[test]
fn parser_depth_and_c_locale_preservation_are_bounded() {
    let (_a, mut s) = db();
    let deep = format!("SELECT {}1{} FROM f", "abs(".repeat(34), ")".repeat(34));
    error(&mut s, &deep, E::Limit);
    let text = "ΐ".repeat(25000);
    // C locale preserves U+0390 without expansion.
    // Store the shorter UTF-8 input through a parameter rather than SQL text.
    s.execute("INSERT INTO f VALUES(1,1,1,1,$1)", &[V::Text(text)], None)
        .unwrap();
    assert_eq!(
        run(&mut s, "SELECT upper(txt) FROM f").rows[0].values,
        vec![V::Text("ΐ".repeat(25000))]
    );
    let p = s.prepare("SELECT id,abs(i) FROM f").unwrap();
    let d = s.describe(&p).unwrap();
    assert!(d.columns[0].not_null);
    assert_ne!(d.columns[0].id, 0);
}

#[test]
fn unknown_abs_and_typed_null_metadata() {
    let (_a, mut s) = db();
    for q in ["abs(NULL)", "coalesce(1,abs(NULL))"] {
        let p = s.prepare(&format!("SELECT {q} FROM f LIMIT 0")).unwrap();
        assert_eq!(
            s.describe(&p).unwrap().columns[0].data_type,
            T::DoublePrecision
        );
    }
    run(&mut s, "INSERT INTO f VALUES(1,NULL,NULL,NULL,NULL)");
    let q = "SELECT abs(s),abs(i),abs(b),abs(coalesce(NULL,i)),abs(coalesce(NULL,1)) FROM f";
    let p = s.prepare(q).unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![T::SmallInt, T::Integer, T::BigInt, T::Integer, T::Integer]
    );
    assert_eq!(
        run(&mut s, q).rows[0].values,
        vec![V::Null, V::Null, V::Null, V::Null, V::Int(1)]
    );
}
#[test]
fn c_locale_ascii_and_nonascii_reference() {
    let (_a, mut s) = db();
    run(&mut s, "INSERT INTO f VALUES(1,1,1,1,'x')");
    assert_eq!(
        run(
            &mut s,
            "SELECT lower('ABCßİΣσςΐÉ'),upper('abcßİΣσςΐé') FROM f"
        )
        .rows[0]
            .values,
        vec![V::Text("abcßİΣσςΐÉ".into()), V::Text("ABCßİΣσςΐé".into())]
    );
}
#[test]
fn output_and_work_budgets_have_independent_boundaries() {
    let (_a, mut s) = db();
    // 32 projections of 32768 bytes exactly fill the 1 MiB output cap.
    s.execute(
        "INSERT INTO f VALUES(1,1,1,1,$1)",
        &[V::Text("a".repeat(32768))],
        None,
    )
    .unwrap();
    let columns = vec!["txt"; 32].join(",");
    assert_eq!(
        run(&mut s, &format!("SELECT {columns} FROM f LIMIT 1")).rows[0]
            .values
            .len(),
        32
    );
    // A scalar literal must not debit the output budget twice.
    let mixed = format!("{},lower('')", vec!["txt"; 31].join(","));
    assert_eq!(run(&mut s, &format!("SELECT {mixed} FROM f")).rows.len(), 1);
    let calls = vec!["lower(txt)"; 16].join(",");
    assert_eq!(run(&mut s, &format!("SELECT {calls} FROM f")).rows.len(), 1);
    let calls = vec!["lower(txt)"; 17].join(",");
    error(&mut s, &format!("SELECT {calls} FROM f LIMIT 0"), E::Limit);
}
#[test]
fn casts_and_prepared_projection_values() {
    let (_a, mut s) = db();
    let p = s
        .prepare("SELECT abs($1::smallint),upper($2),CAST($3 AS integer),$4::boolean")
        .unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(
        d.parameters,
        vec![T::SmallInt, T::Text, T::Integer, T::Boolean]
    );
    assert_eq!(
        d.columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![T::SmallInt, T::Text, T::Integer, T::Boolean]
    );
    assert_eq!(
        s.execute_prepared(
            &p,
            &[V::Int(-7), V::Text("abc".into()), V::Int(9), V::Null],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        vec![V::Int(7), V::Text("ABC".into()), V::Int(9), V::Null]
    );
    assert!(s.execute_prepared(&p, &[], None).is_err());
    assert!(s.execute_prepared(&p, &vec![V::Int(1); 5], None).is_err());
    assert!(
        s.execute_prepared(
            &p,
            &[V::Int(32768), V::Text("a".into()), V::Int(1), V::Bool(true)],
            None
        )
        .is_err()
    );
    assert_eq!(run(&mut s,"SELECT abs(NULL::integer),CAST('32767' AS smallint),CAST(7 AS text),CAST('true' AS boolean)").rows[0].values,vec![V::Null,V::Int(32767),V::Text("7".into()),V::Bool(true)]);
    for q in [
        "SELECT '32768'::smallint",
        "SELECT '9223372036854775808'::bigint",
        "SELECT 2147483648::integer",
    ] {
        error(&mut s, q, E::Overflow);
    }
    error(
        &mut s,
        "SELECT 'bad'::integer",
        E::InvalidTextRepresentation,
    );
    assert_eq!(
        run(&mut s, "SELECT abs(NULL)").rows[0].values,
        vec![V::Null]
    );
    let p = s.prepare("SELECT $1::integer,$1::text").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::Integer]);
    let p = s.prepare("SELECT abs($1)").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::DoublePrecision]);
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        T::DoublePrecision
    );
    assert!(s.prepare("SELECT $2::integer").is_err());
    let p = s.prepare("SELECT 'bad'::integer LIMIT 0").unwrap();
    assert!(s.describe(&p).is_err());
    let p = s.prepare("SELECT abs($1) FROM f WHERE i=$1").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::DoublePrecision]);
    assert!(
        s.execute_prepared(
            &p,
            &[V::Float8(Float8::from_bits((i32::MIN as f64).to_bits()))],
            None
        )
        .unwrap()
        .rows
        .is_empty()
    );
}
#[test]
fn cast_context_empty_input_limits_and_rollback() {
    let (_a, mut s) = db();
    for tail in ["", " LIMIT 0"] {
        let p = s
            .prepare(&format!(
                "SELECT abs(NULL::smallint),coalesce($1,i),substring($2,$3) FROM f{tail}"
            ))
            .unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(d.parameters, vec![T::Integer, T::Text, T::Integer]);
        assert_eq!(d.columns[0].data_type, T::SmallInt);
        assert!(
            s.execute_prepared(&p, &[V::Int(1), V::Text("abc".into()), V::Int(1)], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
    assert_eq!(
        run(
            &mut s,
            "SELECT CAST('abc' AS bytea),CAST(CAST('abc' AS bytea) AS text)"
        )
        .rows[0]
            .values,
        vec![V::Bytes(b"abc".to_vec()), V::Text("\\x616263".into())]
    );
    run(&mut s, "INSERT INTO f VALUES(1,1,1,1,'a')");
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO f VALUES(2,2,2,2,'b')");
    error(
        &mut s,
        "SELECT CAST('32768' AS smallint) FROM f",
        E::Overflow,
    );
    assert!(s.execute("SELECT * FROM f", &[], None).is_err());
    run(&mut s, "ROLLBACK");
    assert_eq!(run(&mut s, "SELECT id FROM f LIMIT 10").rows.len(), 1);
    let deep = format!("SELECT 1{}", "::integer".repeat(33));
    error(&mut s, &deep, E::Limit);
    let p = s.prepare("SELECT lower($1::text)").unwrap();
    assert!(
        s.execute_prepared(&p, &[V::Text("x".repeat(1024 * 1024 + 1))], None)
            .is_err()
    );
}
#[test]
fn cast_integer_edges_and_shared_parameter_conflicts() {
    let (_a, mut s) = db();
    for (ty, lo, hi) in [
        ("smallint", i16::MIN as i64, i16::MAX as i64),
        ("integer", i32::MIN as i64, i32::MAX as i64),
        ("bigint", i64::MIN, i64::MAX),
    ] {
        let q = format!("SELECT '{lo}'::{ty},'{hi}'::{ty},NULL::{ty}");
        assert_eq!(
            run(&mut s, &q).rows[0].values,
            vec![V::Int(lo), V::Int(hi), V::Null]
        );
    }
    let p = s.prepare("SELECT $1::text FROM f WHERE i=$1").unwrap();
    assert!(s.describe(&p).is_err());
    let p = s.prepare("SELECT $1::integer FROM f LIMIT $1").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::Integer]);
    let p = s
        .prepare("SELECT coalesce(NULL,$1,i),$1 FROM f LIMIT 0")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::Integer]);
    assert!(
        s.execute_prepared(&p, &[V::Text("bad".into())], None)
            .is_err()
    );
    let p = s.prepare("SELECT $1::bigint::smallint").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::BigInt]);
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(32768)], None),
        Err(CoreError::Sql(E::Overflow))
    );
}

#[test]
fn adversarial_cast_binding_and_unknown_literals() {
    let (_a, mut s) = db();
    let p = s
        .prepare("SELECT $1::integer,$1::text,$1::boolean")
        .unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(d.parameters, vec![T::Integer]);
    assert_eq!(
        d.columns
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.clone()))
            .collect::<Vec<_>>(),
        vec![
            ("int4", T::Integer),
            ("text", T::Text),
            ("bool", T::Boolean)
        ]
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(7)], None).unwrap().rows[0].values,
        vec![V::Int(7), V::Text("7".into()), V::Bool(true)]
    );
    assert!(
        s.execute_prepared(&p, &[V::Int(i32::MAX as i64 + 1)], None)
            .is_err()
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT coalesce('2',1),coalesce(1,'2'),coalesce(NULL,'3',1)"
        )
        .rows[0]
            .values,
        vec![V::Int(2), V::Int(1), V::Int(3)]
    );
    for suffix in ["", " FROM f", " FROM f LIMIT 0"] {
        error(
            &mut s,
            &format!("SELECT coalesce(1,'bad'){suffix}"),
            E::InvalidTextRepresentation,
        );
        let p = s.prepare(&format!("SELECT abs('1'){suffix}")).unwrap();
        assert_eq!(
            s.describe(&p).unwrap().columns[0].data_type,
            T::DoublePrecision
        );
        error(&mut s, &format!("SELECT true::smallint{suffix}"), E::Type);
    }
    assert_eq!(
        run(&mut s, r"SELECT '\x61 62'::bytea,'\141'::bytea").rows[0].values,
        vec![V::Bytes(b"ab".to_vec()), V::Bytes(b"a".to_vec())]
    );
    error(&mut s, r"SELECT '\x6 1'::bytea", E::InvalidParameterValue);
    error(
        &mut s,
        "SELECT '\u{a0}1'::integer",
        E::InvalidTextRepresentation,
    );
    error(&mut s, "SELECT '\u{a0}true'::boolean", E::Type);
    let p = s
        .prepare("SELECT txt::text,abs(i)::text FROM f LIMIT 0")
        .unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["txt", "abs"]
    );
}

#[test]
fn nested_cast_labels_and_untyped_abs() {
    let (_a, mut s) = db();
    let p = s
        .prepare("SELECT 1::integer::text, '1'::text::integer")
        .unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["text", "int4"]
    );
    let p = s.prepare("SELECT abs($1)").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::DoublePrecision]);
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        T::DoublePrecision
    );
    for (input, expected) in [
        (V::Null, V::Null),
        (float(-2.5), float(2.5)),
        (float(-0.0), float(0.0)),
        (float(f64::NEG_INFINITY), float(f64::INFINITY)),
        (float(f64::NAN), float(f64::NAN)),
    ] {
        assert_eq!(
            s.execute_prepared(&p, &[input], None).unwrap().rows[0].values,
            vec![expected]
        );
    }
}

#[test]
fn postgres18_integer_bytea_casts() {
    let (_a, mut s) = db();
    assert_eq!(run(&mut s,r"SELECT (-1::smallint)::bytea,1::integer::bytea,1::bigint::bytea,'\xff'::bytea::smallint,'\xffff'::bytea::smallint,''::bytea::integer").rows[0].values,vec![V::Bytes(vec![255,255]),V::Bytes(vec![0,0,0,1]),V::Bytes(vec![0,0,0,0,0,0,0,1]),V::Int(255),V::Int(-1),V::Int(0)]);
    error(&mut s, r"SELECT '\x000001'::bytea::smallint", E::Overflow);
}

fn float(v: f64) -> V {
    V::Float8(Float8::from_bits(v.to_bits()))
}
