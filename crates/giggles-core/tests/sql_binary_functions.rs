mod common;
use common::*;
use giggles_core::{
    relational::{SqlError as E, Value as V},
    *,
};
#[test]
fn binary_values_metadata_nulls_and_errors() {
    let a = Admin::create(fresh("sql-binary"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (q, v) in [
        ("length('\\x00ff'::bytea)", V::Int(2)),
        ("reverse('\\x00ff80'::bytea)", V::Bytes(vec![128, 255, 0])),
        (
            "btrim('\\x00ff000100'::bytea,'\\x00ff'::bytea)",
            V::Bytes(vec![1]),
        ),
        (
            "ltrim('\\x000100'::bytea,'\\x00'::bytea)",
            V::Bytes(vec![1, 0]),
        ),
        (
            "rtrim('\\x000100'::bytea,'\\x00'::bytea)",
            V::Bytes(vec![0, 1]),
        ),
        ("substring('\\x000102'::bytea,0,2)", V::Bytes(vec![0])),
        ("substring('\\x000102'::bytea,-3,2)", V::Bytes(vec![])),
        ("substring('\\x000102'::bytea,2)", V::Bytes(vec![1, 2])),
        ("get_bit('\\x0180'::bytea,15)", V::Int(1)),
        ("get_byte('\\x0080'::bytea,1)", V::Int(128)),
        ("set_byte('\\x00'::bytea,0,-1)", V::Bytes(vec![255])),
        ("set_bit('\\x00'::bytea,7,1)", V::Bytes(vec![128])),
        ("encode('\\x00ff'::bytea,'HEX')", V::Text("00ff".into())),
        ("decode('YR==YQ==','base64')", V::Bytes(vec![97, 97])),
        (
            "encode(decode('YQ==','base64'),'escape')",
            V::Text("a".into()),
        ),
        ("decode('00 ff','hex')", V::Bytes(vec![0, 255])),
        (
            "decode('\\000\\377\\\\','escape')",
            V::Bytes(vec![0, 255, 92]),
        ),
        (
            "encode(sha256(''::bytea),'hex')",
            V::Text("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into()),
        ),
    ] {
        let r = s.execute(&format!("SELECT {q}"), &[], None).unwrap();
        assert_eq!(r.rows[0].values, vec![v], "{q}");
    }
    for q in [
        "get_byte(NULL,-1)",
        "set_bit(''::bytea,NULL,2)",
        "sha224(NULL)",
        "decode(NULL,'bad')",
        "reverse(NULL::bytea)",
        "btrim(NULL::bytea,''::bytea)",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {q}"), &[], None).unwrap().rows[0].values,
            vec![V::Null],
            "{q}"
        );
    }
    for q in [
        "get_byte(''::bytea,0)",
        "get_bit('\\x00'::bytea,-1)",
        "set_bit('\\x00'::bytea,0,2)",
        "btrim(''::bytea)",
        "sha256(1)",
        "sha512()",
        "get_byte('\\x00'::bytea,0::bigint)",
        "decode('a b','hex')",
        "decode('a','base64')",
        "decode('\\777','escape')",
        "decode('','bad')",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {q}"), &[], None),
            Err(CoreError::Sql(match q {
                "sha256(1)"
                | "get_byte('\\x00'::bytea,0::bigint)"
                | "btrim(''::bytea)"
                | "sha512()" => E::UndefinedFunction,
                "decode('\\777','escape')" => E::InvalidTextRepresentation,
                "decode('a b','hex')" | "decode('a','base64')" | "decode('','bad')" => {
                    E::InvalidParameterValue
                }
                _ => E::Type,
            })),
            "{q}"
        );
    }
    assert_eq!(
        s.execute("SELECT substring(''::bytea,1,-1)", &[], None),
        Err(CoreError::Sql(E::SubstringError))
    );
    let p = s
        .prepare("SELECT encode(reverse($1::bytea),'hex'), get_bit($1::bytea,$2)")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[V::Bytes(vec![1, 128]), V::Int(15)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Text("8001".into()), V::Int(1)]
    );
    assert!(
        s.execute("SELECT get_byte(''::bytea,-1) LIMIT 0", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn binary_escape_controls_and_second_argument_resolution() {
    let a = Admin::create(fresh("sql-binary-edges"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let r=s.execute("SELECT encode('\\x00091f7f80'::bytea,'escape'),btrim(NULL,'\\x00'::bytea),rtrim('abc','\\x63'::bytea)",&[],None).unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![
            V::Text("\\000\t\u{1f}\u{7f}\\200".into()),
            V::Null,
            V::Bytes(b"ab".to_vec())
        ]
    );
    let p = s.prepare("SELECT encode($1::bytea,'hex')").unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[V::Bytes(vec![0; 32768])], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Text("00".repeat(32768))]
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Bytes(vec![0; 32769])], None),
        Err(CoreError::Sql(E::Limit))
    );
}

#[test]
fn binary_review_aliases_and_codec_audit() {
    let a = Admin::create(fresh("binary-review"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (expr, value) in [
        ("substr('abc','2','1')", V::Text("b".into())),
        ("substr('abc',2)", V::Text("bc".into())),
        ("substr('\\x000102'::bytea,0,2)", V::Bytes(vec![0])),
        (
            "substr('\\x000102'::bytea,-2147483648,2147483647)",
            V::Bytes(vec![]),
        ),
        (
            "substr('\\x000102'::bytea,2147483647,2147483647)",
            V::Bytes(vec![]),
        ),
        ("substr(NULL::bytea,1,-1)", V::Null),
        ("substr('abc',NULL::integer,-1)", V::Null),
        ("decode('AA=A','base64')", V::Bytes(vec![0])),
        ("decode('AA==AAAA','base64')", V::Bytes(vec![0, 0])),
        ("decode('AA=A====','base64')", V::Bytes(vec![0, 0])),
        ("decode('AAA=AAAA','base64')", V::Bytes(vec![0, 0, 0, 0])),
        ("decode('YQ== Yg==','base64')", V::Bytes(b"ab".to_vec())),
        ("decode('Y Q = =','base64')", V::Bytes(b"a".to_vec())),
        ("set_byte('\\x00'::bytea,0,256)", V::Bytes(vec![0])),
    ] {
        let r = s.execute(&format!("SELECT {expr}"), &[], None).unwrap();
        assert_eq!(r.rows[0].values, vec![value], "{expr}");
    }
    for expr in [
        "decode('YQ=','base64')",
        "decode('YQ===','base64')",
        "decode('YQ==x','base64')",
        "decode('====','base64')",
        "get_bit('\\x00'::bytea,9223372036854775807)",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {expr}"), &[], None),
            Err(CoreError::Sql(if expr.starts_with("decode") {
                E::InvalidParameterValue
            } else {
                E::Type
            })),
            "{expr}"
        );
    }
}

#[test]
fn binary_alias_descriptors_and_prepared_nulls() {
    use giggles_core::relational::DataType as T;
    let a = Admin::create(fresh("binary-descriptors"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let p = s.prepare("SELECT substr($1::bytea,$2),substr('abc',1,2),substring(NULL::bytea,1),reverse(NULL::bytea),btrim(NULL::bytea,''::bytea)").unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(d.parameters, vec![T::Bytes, T::Integer]);
    assert_eq!(
        d.columns
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.clone()))
            .collect::<Vec<_>>(),
        vec![
            ("substr", T::Bytes),
            ("substr", T::Text),
            ("substring", T::Bytes),
            ("reverse", T::Bytes),
            ("btrim", T::Bytes)
        ]
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Null, V::Int(-1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Null, V::Text("ab".into()), V::Null, V::Null, V::Null]
    );
}
