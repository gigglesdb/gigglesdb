mod common;
use common::*;
use giggles_core::{
    relational::{DataType as T, SqlError as E, Value as V},
    *,
};
fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("sql-text"), Limits::default()).unwrap();
    let s = a.sql_session();
    (a, s)
}
#[test]
fn text_pg18_cases() {
    let (_a, mut s) = db();
    let cases = [
        ("char_length('é🙂é')", V::Int(4)),
        ("character_length('')", V::Int(0)),
        ("octet_length('é🙂')", V::Int(6)),
        ("bit_length('é🙂')", V::Int(48)),
        ("starts_with('é🙂','é')", V::Bool(true)),
        ("strpos('é🙂x','x')", V::Int(3)),
        ("strpos('abc','')", V::Int(1)),
        ("strpos('abc','z')", V::Int(0)),
        ("reverse('é🙂')", V::Text("🙂é".into())),
        ("repeat('é',3)", V::Text("ééé".into())),
        ("repeat('x',-1)", V::Text("".into())),
        ("replace('aaaa','aa','é')", V::Text("éé".into())),
        ("replace('a','','x')", V::Text("a".into())),
        ("translate('abcabc','aba','XY')", V::Text("XYcXYc".into())),
        ("translate('abc','abc','é')", V::Text("é".into())),
        ("left('é🙂abc',-2)", V::Text("é🙂a".into())),
        ("right('é🙂abc',-2)", V::Text("abc".into())),
        ("lpad('é',4,'🙂x')", V::Text("🙂x🙂é".into())),
        ("rpad('é',4,'🙂x')", V::Text("é🙂x🙂".into())),
        ("lpad('abc',5,'')", V::Text("abc".into())),
        ("rpad('abc',-1)", V::Text("".into())),
        ("btrim('abbaXba','ab')", V::Text("X".into())),
        ("ltrim('  a  ')", V::Text("a  ".into())),
        ("rtrim('  a  ')", V::Text("  a".into())),
        ("split_part('a,b,c',',',-2)", V::Text("b".into())),
        ("split_part('a,b',',',-3)", V::Text("".into())),
        ("split_part('abc','',-1)", V::Text("abc".into())),
        ("concat(true,1,NULL,'é')", V::Text("t1é".into())),
        ("concat(NULL,NULL)", V::Text("".into())),
        ("concat_ws(',',NULL,'',1)", V::Text(",1".into())),
        ("concat_ws(NULL,'a')", V::Null),
        ("substring('é🙂abc' FROM 2 FOR 2)", V::Text("🙂a".into())),
        ("substring('abc' FOR 2)", V::Text("ab".into())),
    ];
    for (q, v) in cases {
        assert_eq!(
            s.execute(&format!("SELECT {q}"), &[], None).unwrap().rows[0].values,
            vec![v],
            "{q}"
        );
    }
}
#[test]
fn nulls_signatures_metadata_and_prepared() {
    let (_a, mut s) = db();
    let strict = [
        "char_length(NULL)",
        "character_length(NULL)",
        "octet_length(NULL)",
        "bit_length(NULL)",
        "reverse(NULL)",
        "starts_with('a',NULL)",
        "strpos(NULL,'a')",
        "repeat(NULL,2)",
        "replace('a',NULL,'b')",
        "translate('a','a',NULL)",
        "left('a',NULL)",
        "right(NULL,2)",
        "lpad(NULL,2)",
        "rpad('a',2,NULL)",
        "btrim(NULL)",
        "ltrim('a',NULL)",
        "rtrim(NULL,'a')",
        "split_part(NULL,',',1)",
    ];
    for q in strict {
        assert_eq!(
            s.execute(&format!("SELECT {q}"), &[], None).unwrap().rows[0].values,
            vec![V::Null]
        );
    }
    for q in [
        "reverse(1)",
        "repeat('a',true)",
        "left('a',2147483648)",
        "concat()",
        "concat_ws()",
        "btrim()",
        "split_part('a',',')",
        "lpad('a')",
        "starts_with('a')",
        "translate('a','b')",
        "char_length('a',1)",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {q} LIMIT 0"), &[], None),
            Err(CoreError::Sql(E::Type)),
            "{q}"
        );
    }
    assert_eq!(
        s.execute("SELECT split_part('a',',',0)", &[], None),
        Err(CoreError::Sql(E::Type))
    );
    let p = s
        .prepare("SELECT repeat(reverse($1),$2),starts_with($1,'é'),char_length($1)")
        .unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(
        d.columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![T::Text, T::Boolean, T::Integer]
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Text("éx".into()), V::Int(2)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Text("xéxé".into()), V::Bool(true), V::Int(2)]
    );
}
#[test]
fn bounded_expansion() {
    let (_a, mut s) = db();
    for q in [
        "repeat('x',2147483647)",
        "lpad('x',2147483647,'🙂')",
        "rpad('x',2147483647)",
        "replace(repeat('a',32768),'a','aaa')",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {q}"), &[], None),
            Err(CoreError::Sql(E::Limit)),
            "{q}"
        );
    }
    assert_eq!(
        s.execute("SELECT char_length(repeat('x',65536))", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Int(65536)]
    );
    assert_eq!(
        s.execute("SELECT repeat('x',65537)", &[], None),
        Err(CoreError::Sql(E::Limit))
    );
}
#[test]
fn all_signatures_reject_arity_and_wrong_types_before_scan() {
    let (_a, mut s) = db();
    for (name, args) in [
        ("char_length", "1"),
        ("character_length", "1"),
        ("octet_length", "true"),
        ("bit_length", "1"),
        ("reverse", "true"),
        ("starts_with", "'a',1"),
        ("strpos", "1,'a'"),
        ("repeat", "'a',true"),
        ("replace", "'a','a',1"),
        ("translate", "'a',1,'a'"),
        ("left", "'a',false"),
        ("right", "1,2"),
        ("lpad", "'a',2,1"),
        ("rpad", "'a',true"),
        ("btrim", "'a',1"),
        ("ltrim", "1"),
        ("rtrim", "'a',true"),
        ("split_part", "'a',1,1"),
        ("concat_ws", "1,'a'"),
    ] {
        for query in [
            format!("SELECT {name}() LIMIT 0"),
            format!("SELECT {name}({args}) LIMIT 0"),
        ] {
            assert_eq!(
                s.execute(&query, &[], None),
                Err(CoreError::Sql(E::Type)),
                "{query}"
            );
        }
    }
    for q in ["octet_length('abc'::bytea)", "bit_length('abc'::bytea)"] {
        let result = s.execute(&format!("SELECT {q}"), &[], None).unwrap();
        let p = s.prepare(&format!("SELECT {q}")).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, T::Integer);
        assert_eq!(
            result.rows[0].values,
            vec![V::Int(if q.starts_with("bit") { 24 } else { 3 })]
        );
    }
    for q in [
        "left('abc',-2147483648)",
        "split_part('a,b',',',-2147483648)",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {q}"), &[], None).unwrap().rows[0].values,
            vec![V::Text("".into())]
        );
    }
    assert_eq!(
        s.execute("SELECT repeat('é',32768)", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Text("é".repeat(32768))]
    );
    assert_eq!(
        s.execute("SELECT repeat('é',32769)", &[], None),
        Err(CoreError::Sql(E::Limit))
    );
}

#[test]
fn pg18_right_int_min_and_concat_ws_min_arity() {
    let (_a, mut s) = db();
    assert_eq!(
        s.execute("SELECT right('abc',-2147483648)", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Text("abc".into())]
    );
    assert_eq!(
        s.execute("SELECT concat_ws(',') LIMIT 0", &[], None),
        Err(CoreError::Sql(E::Type))
    );
}

#[test]
fn concat_exact_output_limits_and_no_partial_result() {
    let (_a, mut s) = db();
    let p = s.prepare("SELECT concat($1::text,$1::text)").unwrap();
    for (len, succeeds) in [(32768, true), (32769, false)] {
        let arg = V::Text("x".repeat(len));
        let result = s.execute_prepared(&p, &[arg], None);
        if succeeds {
            assert_eq!(
                result.unwrap().rows[0].values,
                vec![V::Text("x".repeat(65536))]
            );
        } else {
            assert_eq!(result, Err(CoreError::Sql(E::Limit)));
        }
    }
    let p = s
        .prepare("SELECT concat_ws(',', $1::text, NULL, $1::text)")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[V::Text("x".repeat(32768))], None),
        Err(CoreError::Sql(E::Limit))
    );
    let p = s
        .prepare("SELECT concat($1::text,$1::text) LIMIT 0")
        .unwrap();
    assert!(
        s.execute_prepared(&p, &[V::Text("x".repeat(32769))], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        s.execute(
            "SELECT concat((-9223372036854775808)::bigint,0,true,false,'ab'::bytea)",
            &[],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        vec![V::Text("-92233720368547758080tf\\x6162".into())]
    );
}
