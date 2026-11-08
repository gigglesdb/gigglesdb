use giggles_core::{Admin, CoreError, Limits, relational::*};
use giggles_pg_types::temporal::SessionZone;
use serde_json::Value as Json;
mod common;
struct Fixture {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let path = common::fresh("dd68-format");
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn clock(zone: &str) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(if zone == "UTC" {
            SessionZone::Fixed(0)
        } else {
            SessionZone::named(zone).unwrap()
        }),
        wall: None,
    }
}
fn datatype(oid: u64) -> DataType {
    match oid {
        25 => DataType::Text,
        23 => DataType::Integer,
        701 => DataType::DoublePrecision,
        _ => panic!("oid {oid}"),
    }
}
fn error(state: &str) -> SqlError {
    match state {
        "22023" => SqlError::InvalidParameterValue,
        "22004" => SqlError::NullValueNotAllowed,
        "22003" => SqlError::Overflow,
        "22P02" => SqlError::InvalidTextRepresentation,
        "42883" => SqlError::UndefinedFunction,
        "42P18" => SqlError::IndeterminateDatatype,
        _ => panic!("state {state}"),
    }
}
// PostgreSQL 18 captures cover UTC/Chicago and default/zero extra_float_digits.
#[test]
fn postgres_format_truth() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    for sql in &[
        "CREATE TABLE gt(id integer primary key, a integer, i2 smallint, b numeric, s text, d date, ts timestamp, tz timestamptz, f double precision)",
        "INSERT INTO gt VALUES (1, 1, 7, 2.5, 'a', '2024-07-01', '2024-07-01 10:00', '2024-07-01 12:00:00+00', 0.1), (2, 9, 3, 1, 'z', '2024-07-02', '2024-07-01 23:00', '2024-07-02 01:00:00+00', 1e16), (3, 4, NULL, NULL, NULL, NULL, '2024-07-01 10:00', NULL, NULL), (4, NULL, NULL, 4, 'm', '2024-07-01', NULL, '2024-06-30 23:00:00+00', 'NaN')",
    ] {
        s.execute(sql, &[], None).unwrap();
    }
    let cases: Json = serde_json::from_str(CASES).unwrap();
    for case in cases.as_array().unwrap() {
        let sql = case["sql"].as_str().unwrap();
        let c = clock(case["zone"].as_str().unwrap());
        s.set_extra_float_digits(if case["efd"] == "0" { 0 } else { 1 });
        let declarations: Vec<_> = case["decl"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| Some(datatype(v.as_u64().unwrap())))
            .collect();
        let mut reached_execution = false;
        let observed = (|| {
            let p = s
                .prepare(sql)?
                .with_parameter_types(&declarations)
                .map_err(CoreError::Sql)?;
            let d = s.describe(&p)?;
            if !case["name"].as_str().unwrap().starts_with("tab ") {
                assert_eq!(d.columns[0].data_type, DataType::Text, "{case}");
            }
            let params: Vec<_> = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if v.is_null() {
                        Value::Null
                    } else {
                        let input = v.as_str().unwrap();
                        match declarations.get(i).and_then(Option::as_ref) {
                            Some(DataType::Integer) => Value::Int(input.parse().unwrap()),
                            Some(DataType::DoublePrecision) => {
                                Value::Float8(giggles_pg_types::Float8::from_bits(
                                    input.parse::<f64>().unwrap().to_bits(),
                                ))
                            }
                            _ => Value::Text(input.into()),
                        }
                    }
                })
                .collect();
            reached_execution = true;
            s.execute_prepared_at(&p, &params, None, &c)
        })();
        let expected = &case["result"];
        assert_eq!(
            reached_execution,
            expected["phase"] == "execute",
            "unexpected error phase: {case}"
        );
        if expected["status"] == "error" {
            assert_eq!(
                observed.unwrap_err(),
                CoreError::Sql(error(expected["sqlstate"].as_str().unwrap())),
                "{case}"
            );
        } else {
            let rows: Vec<Vec<Json>> = observed
                .unwrap_or_else(|e| panic!("{case}: {e:?}"))
                .rows
                .into_iter()
                .map(|r| {
                    r.values
                        .into_iter()
                        .map(|v| match v {
                            Value::Null => Json::Null,
                            Value::Text(s) => Json::String(s),
                            Value::Int(n) => Json::String(n.to_string()),
                            other => panic!("{other:?}"),
                        })
                        .collect()
                })
                .collect();
            assert_eq!(
                serde_json::to_value(rows).unwrap(),
                expected["rows"],
                "{case}"
            );
        }
    }
}
const CASES: &str = r###"[
{"name":"fmt plain","zone":"UTC","efd":"default","sql":"SELECT format('abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abc"]],"status":"ok"}},
{"name":"fmt pct","zone":"UTC","efd":"default","sql":"SELECT format('100%% sure')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["100% sure"]],"status":"ok"}},
{"name":"fmt s","zone":"UTC","efd":"default","sql":"SELECT format('Hello %s', 'World')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["Hello World"]],"status":"ok"}},
{"name":"fmt s two","zone":"UTC","efd":"default","sql":"SELECT format('%s,%s', 1, 2.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1,2.5"]],"status":"ok"}},
{"name":"fmt extra args","zone":"UTC","efd":"default","sql":"SELECT format('x', 1, 2)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["x"]],"status":"ok"}},
{"name":"fmt null fmt","zone":"UTC","efd":"default","sql":"SELECT format(NULL, 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null fmt only","zone":"UTC","efd":"default","sql":"SELECT format(NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null text fmt","zone":"UTC","efd":"default","sql":"SELECT format(NULL::text, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt s null","zone":"UTC","efd":"default","sql":"SELECT format('[%s]', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["[]"]],"status":"ok"}},
{"name":"fmt I","zone":"UTC","efd":"default","sql":"SELECT format('%I', 'foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["foo"]],"status":"ok"}},
{"name":"fmt I upper","zone":"UTC","efd":"default","sql":"SELECT format('%I', 'Foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"Foo\""]],"status":"ok"}},
{"name":"fmt I space","zone":"UTC","efd":"default","sql":"SELECT format('%I', 'foo bar')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"foo bar\""]],"status":"ok"}},
{"name":"fmt I keyword","zone":"UTC","efd":"default","sql":"SELECT format('%I', 'select')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"select\""]],"status":"ok"}},
{"name":"fmt I unreserved kw","zone":"UTC","efd":"default","sql":"SELECT format('%I', 'abort')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abort"]],"status":"ok"}},
{"name":"fmt I dquote","zone":"UTC","efd":"default","sql":"SELECT format('%I', 'a\"b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"a\"\"b\""]],"status":"ok"}},
{"name":"fmt I int","zone":"UTC","efd":"default","sql":"SELECT format('%I', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"42\""]],"status":"ok"}},
{"name":"fmt I null","zone":"UTC","efd":"default","sql":"SELECT format('%I', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"fmt L","zone":"UTC","efd":"default","sql":"SELECT format('%L', 'O''Reilly')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'O''Reilly'"]],"status":"ok"}},
{"name":"fmt L null","zone":"UTC","efd":"default","sql":"SELECT format('%L', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NULL"]],"status":"ok"}},
{"name":"fmt L int","zone":"UTC","efd":"default","sql":"SELECT format('%L', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'42'"]],"status":"ok"}},
{"name":"fmt L bool","zone":"UTC","efd":"default","sql":"SELECT format('%L', true)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'t'"]],"status":"ok"}},
{"name":"fmt L tstz","zone":"UTC","efd":"default","sql":"SELECT format('%L', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-07-01 12:00:00+00'"]],"status":"ok"}},
{"name":"fmt L float","zone":"UTC","efd":"default","sql":"SELECT format('%L', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'0.3333333333333333'"]],"status":"ok"}},
{"name":"fmt sIL combo","zone":"UTC","efd":"default","sql":"SELECT format('INSERT INTO %I VALUES(%L, %s)', 'T', 'x', 7)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["INSERT INTO \"T\" VALUES('x', 7)"]],"status":"ok"}},
{"name":"fmt pos swap","zone":"UTC","efd":"default","sql":"SELECT format('%2$s %1$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["b a"]],"status":"ok"}},
{"name":"fmt pos repeat","zone":"UTC","efd":"default","sql":"SELECT format('%1$s %1$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a a"]],"status":"ok"}},
{"name":"fmt pos then seq","zone":"UTC","efd":"default","sql":"SELECT format('%1$s %s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a b c"]],"status":"ok"}},
{"name":"fmt pos 3 then seq","zone":"UTC","efd":"default","sql":"SELECT format('%3$s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos 0","zone":"UTC","efd":"default","sql":"SELECT format('%0$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt pos too far","zone":"UTC","efd":"default","sql":"SELECT format('%3$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos huge","zone":"UTC","efd":"default","sql":"SELECT format('%2147483648$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt pos max","zone":"UTC","efd":"default","sql":"SELECT format('%2147483647$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt w right","zone":"UTC","efd":"default","sql":"SELECT format('|%10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|       abc|"]],"status":"ok"}},
{"name":"fmt w left","zone":"UTC","efd":"default","sql":"SELECT format('|%-10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abc       |"]],"status":"ok"}},
{"name":"fmt w short","zone":"UTC","efd":"default","sql":"SELECT format('|%2s|', 'abcdef')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abcdef|"]],"status":"ok"}},
{"name":"fmt w multibyte","zone":"UTC","efd":"default","sql":"SELECT format('|%5s|', '??')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   ??|"]],"status":"ok"}},
{"name":"fmt w L","zone":"UTC","efd":"default","sql":"SELECT format('|%8L|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     'a'|"]],"status":"ok"}},
{"name":"fmt w I left","zone":"UTC","efd":"default","sql":"SELECT format('|%-8I|', 'Ab')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|\"Ab\"    |"]],"status":"ok"}},
{"name":"fmt w null","zone":"UTC","efd":"default","sql":"SELECT format('|%5s|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     |"]],"status":"ok"}},
{"name":"fmt w L null","zone":"UTC","efd":"default","sql":"SELECT format('|%6L|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  NULL|"]],"status":"ok"}},
{"name":"fmt w zero","zone":"UTC","efd":"default","sql":"SELECT format('|%0s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt w leading zero","zone":"UTC","efd":"default","sql":"SELECT format('|%05s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    a|"]],"status":"ok"}},
{"name":"fmt flag only","zone":"UTC","efd":"default","sql":"SELECT format('|%-s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt flag double","zone":"UTC","efd":"default","sql":"SELECT format('|%--5s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a    |"]],"status":"ok"}},
{"name":"fmt pos width","zone":"UTC","efd":"default","sql":"SELECT format('|%2$-6s|%1$4s|', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|b     |   a|"]],"status":"ok"}},
{"name":"fmt star","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    x|"]],"status":"ok"}},
{"name":"fmt star neg","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus","zone":"UTC","efd":"default","sql":"SELECT format('|%-*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus neg","zone":"UTC","efd":"default","sql":"SELECT format('|%-*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star null","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', NULL, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x|"]],"status":"ok"}},
{"name":"fmt star text","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', '4', 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star textcol","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', '4'::text, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star numeric","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', 4.0, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type integer: \"4.0\""}},
{"name":"fmt star int8","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', 4::int8, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star int2","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', 4::int2, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star big","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', 2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"value \"2147483648\" is out of range for type integer"}},
{"name":"fmt star minint","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', -2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt star n","zone":"UTC","efd":"default","sql":"SELECT format('|%*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n pos","zone":"UTC","efd":"default","sql":"SELECT format('|%1$*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     x|"]],"status":"ok"}},
{"name":"fmt star n pos neg","zone":"UTC","efd":"default","sql":"SELECT format('|%1$*2$s|', 'x', -6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x     |"]],"status":"ok"}},
{"name":"fmt star then seq","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|%s|', 3, 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  a|b|"]],"status":"ok"}},
{"name":"fmt star n then seq","zone":"UTC","efd":"default","sql":"SELECT format('|%*1$s|%s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star few","zone":"UTC","efd":"default","sql":"SELECT format('|%*s|', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n 0","zone":"UTC","efd":"default","sql":"SELECT format('|%*0$s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt width huge","zone":"UTC","efd":"default","sql":"SELECT format('|%2147483648s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt width 99999999999","zone":"UTC","efd":"default","sql":"SELECT format('|%99999999999s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"err too few","zone":"UTC","efd":"default","sql":"SELECT format('%s')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err too few 2","zone":"UTC","efd":"default","sql":"SELECT format('%s %s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err unrecognized","zone":"UTC","efd":"default","sql":"SELECT format('%z', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"z\""}},
{"name":"err upper S","zone":"UTC","efd":"default","sql":"SELECT format('%S', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"S\""}},
{"name":"err unterminated","zone":"UTC","efd":"default","sql":"SELECT format('abc %')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated pos","zone":"UTC","efd":"default","sql":"SELECT format('%1$', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated width","zone":"UTC","efd":"default","sql":"SELECT format('%10', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated flag","zone":"UTC","efd":"default","sql":"SELECT format('%-', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated star","zone":"UTC","efd":"default","sql":"SELECT format('%*', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err pos no dollar","zone":"UTC","efd":"default","sql":"SELECT format('%1s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a"]],"status":"ok"}},
{"name":"err no args","zone":"UTC","efd":"default","sql":"SELECT format()","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format() does not exist"}},
{"name":"err int fmt","zone":"UTC","efd":"default","sql":"SELECT format(1)","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer) does not exist"}},
{"name":"err int fmt 2","zone":"UTC","efd":"default","sql":"SELECT format(1, 'a')","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer, unknown) does not exist"}},
{"name":"err I null after ok","zone":"UTC","efd":"default","sql":"SELECT format('%s %I', 'a', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"err unterminated after null fmtarg","zone":"UTC","efd":"default","sql":"SELECT format('%s %', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"type bool","zone":"UTC","efd":"default","sql":"SELECT format('%s|%s', true, false)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["t|f"]],"status":"ok"}},
{"name":"type int2 int8","zone":"UTC","efd":"default","sql":"SELECT format('%s|%s', 7::int2, 9007199254740993::int8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["7|9007199254740993"]],"status":"ok"}},
{"name":"type numeric","zone":"UTC","efd":"default","sql":"SELECT format('%s|%s', 1.50, -0.000)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.50|0.000"]],"status":"ok"}},
{"name":"type float4","zone":"UTC","efd":"default","sql":"SELECT format('%s', '1.1'::float4)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.1"]],"status":"ok"}},
{"name":"type float8 third","zone":"UTC","efd":"default","sql":"SELECT format('%s', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["0.3333333333333333"]],"status":"ok"}},
{"name":"type float8 1e16","zone":"UTC","efd":"default","sql":"SELECT format('%s', 1e16::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1e+16"]],"status":"ok"}},
{"name":"type float8 nan inf","zone":"UTC","efd":"default","sql":"SELECT format('%s|%s', 'NaN'::float8, '-Infinity'::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NaN|-Infinity"]],"status":"ok"}},
{"name":"type bytea","zone":"UTC","efd":"default","sql":"SELECT format('%s', '\\x0aff'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\\x0aff"]],"status":"ok"}},
{"name":"type date","zone":"UTC","efd":"default","sql":"SELECT format('%s', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-02-29"]],"status":"ok"}},
{"name":"type time","zone":"UTC","efd":"default","sql":"SELECT format('%s|%s', '13:14:15.5'::time, '13:14:15+05:30'::timetz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["13:14:15.5|13:14:15+05:30"]],"status":"ok"}},
{"name":"type timestamp","zone":"UTC","efd":"default","sql":"SELECT format('%s', '2024-07-01 12:00:00.25'::timestamp)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 12:00:00.25"]],"status":"ok"}},
{"name":"type tstz","zone":"UTC","efd":"default","sql":"SELECT format('%s', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 12:00:00+00"]],"status":"ok"}},
{"name":"type tstz winter","zone":"UTC","efd":"default","sql":"SELECT format('%s', '2024-01-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-01-01 12:00:00+00"]],"status":"ok"}},
{"name":"type interval","zone":"UTC","efd":"default","sql":"SELECT format('%s', interval '1 year 2 mons 3 days 04:05:06.7')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1 year 2 mons 3 days 04:05:06.7"]],"status":"ok"}},
{"name":"type nested","zone":"UTC","efd":"default","sql":"SELECT format('<%s>', format('%s', 1))","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["<1>"]],"status":"ok"}},
{"name":"type concat","zone":"UTC","efd":"default","sql":"SELECT format('%s', 'a' || 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a1"]],"status":"ok"}},
{"name":"type unknown literal","zone":"UTC","efd":"default","sql":"SELECT format('%s', 'lit')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["lit"]],"status":"ok"}},
{"name":"type I numeric","zone":"UTC","efd":"default","sql":"SELECT format('%I', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"1.5\""]],"status":"ok"}},
{"name":"type L numeric","zone":"UTC","efd":"default","sql":"SELECT format('%L', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'1.5'"]],"status":"ok"}},
{"name":"type L date","zone":"UTC","efd":"default","sql":"SELECT format('%L', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-02-29'"]],"status":"ok"}},
{"name":"type L bytea","zone":"UTC","efd":"default","sql":"SELECT format('%L', '\\x0a'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["E'\\\\x0a'"]],"status":"ok"}},
{"name":"tab fmt where","zone":"UTC","efd":"default","sql":"SELECT id FROM gt WHERE format('%s-%s', a, s) = '1-a' ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["1"]],"status":"ok"}},
{"name":"tab fmt order","zone":"UTC","efd":"default","sql":"SELECT id FROM gt ORDER BY format('%s', s) DESC, id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["2"],["4"],["1"],["3"]],"status":"ok"}},
{"name":"tab fmt col fmtstr","zone":"UTC","efd":"default","sql":"SELECT id, format(s || ' %s', a) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","a 1"],["2","z 9"],["3",null],["4","m "]],"status":"ok"}},
{"name":"tab fmt tz","zone":"UTC","efd":"default","sql":"SELECT id, format('%s', tz), format('%L', f) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25,25],"rows":[["1","2024-07-01 12:00:00+00","'0.1'"],["2","2024-07-02 01:00:00+00","'1e+16'"],["3","","NULL"],["4","2024-06-30 23:00:00+00","'NaN'"]],"status":"ok"}},
{"name":"tab fmt star col","zone":"UTC","efd":"default","sql":"SELECT id, format('|%*s|', a, 'x') FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","|x|"],["2","|        x|"],["3","|   x|"],["4","|x|"]],"status":"ok"}},
{"name":"p fmt declared","zone":"UTC","efd":"default","sql":"SELECT format($1, $2)","params":["<%s>","x"],"decl":[25,25],"result":{"phase":"execute","param_oids":[25,25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt declared int arg","zone":"UTC","efd":"default","sql":"SELECT format($1, $2)","params":["<%s>","7"],"decl":[25,23],"result":{"phase":"execute","param_oids":[25,23],"col_oids":[25],"rows":[["<7>"]],"status":"ok"}},
{"name":"p fmt undeclared fmt","zone":"UTC","efd":"default","sql":"SELECT format($1, 'x')","params":["<%s>"],"decl":[],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt undeclared arg","zone":"UTC","efd":"default","sql":"SELECT format('%s', $1)","params":["x"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt undeclared star","zone":"UTC","efd":"default","sql":"SELECT format('%*s', $1, 'x')","params":["3"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt declared float8","zone":"UTC","efd":"default","sql":"SELECT format('%s|%L', $1, $1)","params":["0.1"],"decl":[701],"result":{"phase":"execute","param_oids":[701],"col_oids":[25],"rows":[["0.1|'0.1'"]],"status":"ok"}},
{"name":"p fmt null fmt param","zone":"UTC","efd":"default","sql":"SELECT format($1, 'x')","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"p fmt null arg","zone":"UTC","efd":"default","sql":"SELECT format('%L|%s', $1, $1)","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["NULL|"]],"status":"ok"}},
{"name":"fmt plain","zone":"UTC","efd":"0","sql":"SELECT format('abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abc"]],"status":"ok"}},
{"name":"fmt pct","zone":"UTC","efd":"0","sql":"SELECT format('100%% sure')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["100% sure"]],"status":"ok"}},
{"name":"fmt s","zone":"UTC","efd":"0","sql":"SELECT format('Hello %s', 'World')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["Hello World"]],"status":"ok"}},
{"name":"fmt s two","zone":"UTC","efd":"0","sql":"SELECT format('%s,%s', 1, 2.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1,2.5"]],"status":"ok"}},
{"name":"fmt extra args","zone":"UTC","efd":"0","sql":"SELECT format('x', 1, 2)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["x"]],"status":"ok"}},
{"name":"fmt null fmt","zone":"UTC","efd":"0","sql":"SELECT format(NULL, 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null fmt only","zone":"UTC","efd":"0","sql":"SELECT format(NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null text fmt","zone":"UTC","efd":"0","sql":"SELECT format(NULL::text, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt s null","zone":"UTC","efd":"0","sql":"SELECT format('[%s]', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["[]"]],"status":"ok"}},
{"name":"fmt I","zone":"UTC","efd":"0","sql":"SELECT format('%I', 'foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["foo"]],"status":"ok"}},
{"name":"fmt I upper","zone":"UTC","efd":"0","sql":"SELECT format('%I', 'Foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"Foo\""]],"status":"ok"}},
{"name":"fmt I space","zone":"UTC","efd":"0","sql":"SELECT format('%I', 'foo bar')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"foo bar\""]],"status":"ok"}},
{"name":"fmt I keyword","zone":"UTC","efd":"0","sql":"SELECT format('%I', 'select')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"select\""]],"status":"ok"}},
{"name":"fmt I unreserved kw","zone":"UTC","efd":"0","sql":"SELECT format('%I', 'abort')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abort"]],"status":"ok"}},
{"name":"fmt I dquote","zone":"UTC","efd":"0","sql":"SELECT format('%I', 'a\"b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"a\"\"b\""]],"status":"ok"}},
{"name":"fmt I int","zone":"UTC","efd":"0","sql":"SELECT format('%I', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"42\""]],"status":"ok"}},
{"name":"fmt I null","zone":"UTC","efd":"0","sql":"SELECT format('%I', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"fmt L","zone":"UTC","efd":"0","sql":"SELECT format('%L', 'O''Reilly')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'O''Reilly'"]],"status":"ok"}},
{"name":"fmt L null","zone":"UTC","efd":"0","sql":"SELECT format('%L', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NULL"]],"status":"ok"}},
{"name":"fmt L int","zone":"UTC","efd":"0","sql":"SELECT format('%L', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'42'"]],"status":"ok"}},
{"name":"fmt L bool","zone":"UTC","efd":"0","sql":"SELECT format('%L', true)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'t'"]],"status":"ok"}},
{"name":"fmt L tstz","zone":"UTC","efd":"0","sql":"SELECT format('%L', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-07-01 12:00:00+00'"]],"status":"ok"}},
{"name":"fmt L float","zone":"UTC","efd":"0","sql":"SELECT format('%L', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'0.333333333333333'"]],"status":"ok"}},
{"name":"fmt sIL combo","zone":"UTC","efd":"0","sql":"SELECT format('INSERT INTO %I VALUES(%L, %s)', 'T', 'x', 7)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["INSERT INTO \"T\" VALUES('x', 7)"]],"status":"ok"}},
{"name":"fmt pos swap","zone":"UTC","efd":"0","sql":"SELECT format('%2$s %1$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["b a"]],"status":"ok"}},
{"name":"fmt pos repeat","zone":"UTC","efd":"0","sql":"SELECT format('%1$s %1$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a a"]],"status":"ok"}},
{"name":"fmt pos then seq","zone":"UTC","efd":"0","sql":"SELECT format('%1$s %s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a b c"]],"status":"ok"}},
{"name":"fmt pos 3 then seq","zone":"UTC","efd":"0","sql":"SELECT format('%3$s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos 0","zone":"UTC","efd":"0","sql":"SELECT format('%0$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt pos too far","zone":"UTC","efd":"0","sql":"SELECT format('%3$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos huge","zone":"UTC","efd":"0","sql":"SELECT format('%2147483648$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt pos max","zone":"UTC","efd":"0","sql":"SELECT format('%2147483647$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt w right","zone":"UTC","efd":"0","sql":"SELECT format('|%10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|       abc|"]],"status":"ok"}},
{"name":"fmt w left","zone":"UTC","efd":"0","sql":"SELECT format('|%-10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abc       |"]],"status":"ok"}},
{"name":"fmt w short","zone":"UTC","efd":"0","sql":"SELECT format('|%2s|', 'abcdef')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abcdef|"]],"status":"ok"}},
{"name":"fmt w multibyte","zone":"UTC","efd":"0","sql":"SELECT format('|%5s|', '??')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   ??|"]],"status":"ok"}},
{"name":"fmt w L","zone":"UTC","efd":"0","sql":"SELECT format('|%8L|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     'a'|"]],"status":"ok"}},
{"name":"fmt w I left","zone":"UTC","efd":"0","sql":"SELECT format('|%-8I|', 'Ab')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|\"Ab\"    |"]],"status":"ok"}},
{"name":"fmt w null","zone":"UTC","efd":"0","sql":"SELECT format('|%5s|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     |"]],"status":"ok"}},
{"name":"fmt w L null","zone":"UTC","efd":"0","sql":"SELECT format('|%6L|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  NULL|"]],"status":"ok"}},
{"name":"fmt w zero","zone":"UTC","efd":"0","sql":"SELECT format('|%0s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt w leading zero","zone":"UTC","efd":"0","sql":"SELECT format('|%05s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    a|"]],"status":"ok"}},
{"name":"fmt flag only","zone":"UTC","efd":"0","sql":"SELECT format('|%-s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt flag double","zone":"UTC","efd":"0","sql":"SELECT format('|%--5s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a    |"]],"status":"ok"}},
{"name":"fmt pos width","zone":"UTC","efd":"0","sql":"SELECT format('|%2$-6s|%1$4s|', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|b     |   a|"]],"status":"ok"}},
{"name":"fmt star","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    x|"]],"status":"ok"}},
{"name":"fmt star neg","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus","zone":"UTC","efd":"0","sql":"SELECT format('|%-*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus neg","zone":"UTC","efd":"0","sql":"SELECT format('|%-*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star null","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', NULL, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x|"]],"status":"ok"}},
{"name":"fmt star text","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', '4', 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star textcol","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', '4'::text, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star numeric","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', 4.0, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type integer: \"4.0\""}},
{"name":"fmt star int8","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', 4::int8, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star int2","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', 4::int2, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star big","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', 2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"value \"2147483648\" is out of range for type integer"}},
{"name":"fmt star minint","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', -2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt star n","zone":"UTC","efd":"0","sql":"SELECT format('|%*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n pos","zone":"UTC","efd":"0","sql":"SELECT format('|%1$*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     x|"]],"status":"ok"}},
{"name":"fmt star n pos neg","zone":"UTC","efd":"0","sql":"SELECT format('|%1$*2$s|', 'x', -6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x     |"]],"status":"ok"}},
{"name":"fmt star then seq","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|%s|', 3, 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  a|b|"]],"status":"ok"}},
{"name":"fmt star n then seq","zone":"UTC","efd":"0","sql":"SELECT format('|%*1$s|%s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star few","zone":"UTC","efd":"0","sql":"SELECT format('|%*s|', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n 0","zone":"UTC","efd":"0","sql":"SELECT format('|%*0$s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt width huge","zone":"UTC","efd":"0","sql":"SELECT format('|%2147483648s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt width 99999999999","zone":"UTC","efd":"0","sql":"SELECT format('|%99999999999s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"err too few","zone":"UTC","efd":"0","sql":"SELECT format('%s')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err too few 2","zone":"UTC","efd":"0","sql":"SELECT format('%s %s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err unrecognized","zone":"UTC","efd":"0","sql":"SELECT format('%z', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"z\""}},
{"name":"err upper S","zone":"UTC","efd":"0","sql":"SELECT format('%S', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"S\""}},
{"name":"err unterminated","zone":"UTC","efd":"0","sql":"SELECT format('abc %')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated pos","zone":"UTC","efd":"0","sql":"SELECT format('%1$', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated width","zone":"UTC","efd":"0","sql":"SELECT format('%10', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated flag","zone":"UTC","efd":"0","sql":"SELECT format('%-', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated star","zone":"UTC","efd":"0","sql":"SELECT format('%*', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err pos no dollar","zone":"UTC","efd":"0","sql":"SELECT format('%1s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a"]],"status":"ok"}},
{"name":"err no args","zone":"UTC","efd":"0","sql":"SELECT format()","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format() does not exist"}},
{"name":"err int fmt","zone":"UTC","efd":"0","sql":"SELECT format(1)","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer) does not exist"}},
{"name":"err int fmt 2","zone":"UTC","efd":"0","sql":"SELECT format(1, 'a')","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer, unknown) does not exist"}},
{"name":"err I null after ok","zone":"UTC","efd":"0","sql":"SELECT format('%s %I', 'a', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"err unterminated after null fmtarg","zone":"UTC","efd":"0","sql":"SELECT format('%s %', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"type bool","zone":"UTC","efd":"0","sql":"SELECT format('%s|%s', true, false)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["t|f"]],"status":"ok"}},
{"name":"type int2 int8","zone":"UTC","efd":"0","sql":"SELECT format('%s|%s', 7::int2, 9007199254740993::int8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["7|9007199254740993"]],"status":"ok"}},
{"name":"type numeric","zone":"UTC","efd":"0","sql":"SELECT format('%s|%s', 1.50, -0.000)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.50|0.000"]],"status":"ok"}},
{"name":"type float4","zone":"UTC","efd":"0","sql":"SELECT format('%s', '1.1'::float4)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.1"]],"status":"ok"}},
{"name":"type float8 third","zone":"UTC","efd":"0","sql":"SELECT format('%s', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["0.333333333333333"]],"status":"ok"}},
{"name":"type float8 1e16","zone":"UTC","efd":"0","sql":"SELECT format('%s', 1e16::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1e+16"]],"status":"ok"}},
{"name":"type float8 nan inf","zone":"UTC","efd":"0","sql":"SELECT format('%s|%s', 'NaN'::float8, '-Infinity'::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NaN|-Infinity"]],"status":"ok"}},
{"name":"type bytea","zone":"UTC","efd":"0","sql":"SELECT format('%s', '\\x0aff'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\\x0aff"]],"status":"ok"}},
{"name":"type date","zone":"UTC","efd":"0","sql":"SELECT format('%s', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-02-29"]],"status":"ok"}},
{"name":"type time","zone":"UTC","efd":"0","sql":"SELECT format('%s|%s', '13:14:15.5'::time, '13:14:15+05:30'::timetz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["13:14:15.5|13:14:15+05:30"]],"status":"ok"}},
{"name":"type timestamp","zone":"UTC","efd":"0","sql":"SELECT format('%s', '2024-07-01 12:00:00.25'::timestamp)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 12:00:00.25"]],"status":"ok"}},
{"name":"type tstz","zone":"UTC","efd":"0","sql":"SELECT format('%s', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 12:00:00+00"]],"status":"ok"}},
{"name":"type tstz winter","zone":"UTC","efd":"0","sql":"SELECT format('%s', '2024-01-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-01-01 12:00:00+00"]],"status":"ok"}},
{"name":"type interval","zone":"UTC","efd":"0","sql":"SELECT format('%s', interval '1 year 2 mons 3 days 04:05:06.7')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1 year 2 mons 3 days 04:05:06.7"]],"status":"ok"}},
{"name":"type nested","zone":"UTC","efd":"0","sql":"SELECT format('<%s>', format('%s', 1))","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["<1>"]],"status":"ok"}},
{"name":"type concat","zone":"UTC","efd":"0","sql":"SELECT format('%s', 'a' || 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a1"]],"status":"ok"}},
{"name":"type unknown literal","zone":"UTC","efd":"0","sql":"SELECT format('%s', 'lit')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["lit"]],"status":"ok"}},
{"name":"type I numeric","zone":"UTC","efd":"0","sql":"SELECT format('%I', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"1.5\""]],"status":"ok"}},
{"name":"type L numeric","zone":"UTC","efd":"0","sql":"SELECT format('%L', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'1.5'"]],"status":"ok"}},
{"name":"type L date","zone":"UTC","efd":"0","sql":"SELECT format('%L', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-02-29'"]],"status":"ok"}},
{"name":"type L bytea","zone":"UTC","efd":"0","sql":"SELECT format('%L', '\\x0a'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["E'\\\\x0a'"]],"status":"ok"}},
{"name":"tab fmt where","zone":"UTC","efd":"0","sql":"SELECT id FROM gt WHERE format('%s-%s', a, s) = '1-a' ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["1"]],"status":"ok"}},
{"name":"tab fmt order","zone":"UTC","efd":"0","sql":"SELECT id FROM gt ORDER BY format('%s', s) DESC, id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["2"],["4"],["1"],["3"]],"status":"ok"}},
{"name":"tab fmt col fmtstr","zone":"UTC","efd":"0","sql":"SELECT id, format(s || ' %s', a) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","a 1"],["2","z 9"],["3",null],["4","m "]],"status":"ok"}},
{"name":"tab fmt tz","zone":"UTC","efd":"0","sql":"SELECT id, format('%s', tz), format('%L', f) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25,25],"rows":[["1","2024-07-01 12:00:00+00","'0.1'"],["2","2024-07-02 01:00:00+00","'1e+16'"],["3","","NULL"],["4","2024-06-30 23:00:00+00","'NaN'"]],"status":"ok"}},
{"name":"tab fmt star col","zone":"UTC","efd":"0","sql":"SELECT id, format('|%*s|', a, 'x') FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","|x|"],["2","|        x|"],["3","|   x|"],["4","|x|"]],"status":"ok"}},
{"name":"p fmt declared","zone":"UTC","efd":"0","sql":"SELECT format($1, $2)","params":["<%s>","x"],"decl":[25,25],"result":{"phase":"execute","param_oids":[25,25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt declared int arg","zone":"UTC","efd":"0","sql":"SELECT format($1, $2)","params":["<%s>","7"],"decl":[25,23],"result":{"phase":"execute","param_oids":[25,23],"col_oids":[25],"rows":[["<7>"]],"status":"ok"}},
{"name":"p fmt undeclared fmt","zone":"UTC","efd":"0","sql":"SELECT format($1, 'x')","params":["<%s>"],"decl":[],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt undeclared arg","zone":"UTC","efd":"0","sql":"SELECT format('%s', $1)","params":["x"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt undeclared star","zone":"UTC","efd":"0","sql":"SELECT format('%*s', $1, 'x')","params":["3"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt declared float8","zone":"UTC","efd":"0","sql":"SELECT format('%s|%L', $1, $1)","params":["0.1"],"decl":[701],"result":{"phase":"execute","param_oids":[701],"col_oids":[25],"rows":[["0.1|'0.1'"]],"status":"ok"}},
{"name":"p fmt null fmt param","zone":"UTC","efd":"0","sql":"SELECT format($1, 'x')","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"p fmt null arg","zone":"UTC","efd":"0","sql":"SELECT format('%L|%s', $1, $1)","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["NULL|"]],"status":"ok"}},
{"name":"fmt plain","zone":"America/Chicago","efd":"default","sql":"SELECT format('abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abc"]],"status":"ok"}},
{"name":"fmt pct","zone":"America/Chicago","efd":"default","sql":"SELECT format('100%% sure')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["100% sure"]],"status":"ok"}},
{"name":"fmt s","zone":"America/Chicago","efd":"default","sql":"SELECT format('Hello %s', 'World')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["Hello World"]],"status":"ok"}},
{"name":"fmt s two","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s,%s', 1, 2.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1,2.5"]],"status":"ok"}},
{"name":"fmt extra args","zone":"America/Chicago","efd":"default","sql":"SELECT format('x', 1, 2)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["x"]],"status":"ok"}},
{"name":"fmt null fmt","zone":"America/Chicago","efd":"default","sql":"SELECT format(NULL, 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null fmt only","zone":"America/Chicago","efd":"default","sql":"SELECT format(NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null text fmt","zone":"America/Chicago","efd":"default","sql":"SELECT format(NULL::text, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt s null","zone":"America/Chicago","efd":"default","sql":"SELECT format('[%s]', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["[]"]],"status":"ok"}},
{"name":"fmt I","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 'foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["foo"]],"status":"ok"}},
{"name":"fmt I upper","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 'Foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"Foo\""]],"status":"ok"}},
{"name":"fmt I space","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 'foo bar')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"foo bar\""]],"status":"ok"}},
{"name":"fmt I keyword","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 'select')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"select\""]],"status":"ok"}},
{"name":"fmt I unreserved kw","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 'abort')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abort"]],"status":"ok"}},
{"name":"fmt I dquote","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 'a\"b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"a\"\"b\""]],"status":"ok"}},
{"name":"fmt I int","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"42\""]],"status":"ok"}},
{"name":"fmt I null","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"fmt L","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', 'O''Reilly')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'O''Reilly'"]],"status":"ok"}},
{"name":"fmt L null","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NULL"]],"status":"ok"}},
{"name":"fmt L int","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'42'"]],"status":"ok"}},
{"name":"fmt L bool","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', true)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'t'"]],"status":"ok"}},
{"name":"fmt L tstz","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-07-01 07:00:00-05'"]],"status":"ok"}},
{"name":"fmt L float","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'0.3333333333333333'"]],"status":"ok"}},
{"name":"fmt sIL combo","zone":"America/Chicago","efd":"default","sql":"SELECT format('INSERT INTO %I VALUES(%L, %s)', 'T', 'x', 7)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["INSERT INTO \"T\" VALUES('x', 7)"]],"status":"ok"}},
{"name":"fmt pos swap","zone":"America/Chicago","efd":"default","sql":"SELECT format('%2$s %1$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["b a"]],"status":"ok"}},
{"name":"fmt pos repeat","zone":"America/Chicago","efd":"default","sql":"SELECT format('%1$s %1$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a a"]],"status":"ok"}},
{"name":"fmt pos then seq","zone":"America/Chicago","efd":"default","sql":"SELECT format('%1$s %s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a b c"]],"status":"ok"}},
{"name":"fmt pos 3 then seq","zone":"America/Chicago","efd":"default","sql":"SELECT format('%3$s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos 0","zone":"America/Chicago","efd":"default","sql":"SELECT format('%0$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt pos too far","zone":"America/Chicago","efd":"default","sql":"SELECT format('%3$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos huge","zone":"America/Chicago","efd":"default","sql":"SELECT format('%2147483648$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt pos max","zone":"America/Chicago","efd":"default","sql":"SELECT format('%2147483647$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt w right","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|       abc|"]],"status":"ok"}},
{"name":"fmt w left","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%-10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abc       |"]],"status":"ok"}},
{"name":"fmt w short","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%2s|', 'abcdef')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abcdef|"]],"status":"ok"}},
{"name":"fmt w multibyte","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%5s|', '??')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   ??|"]],"status":"ok"}},
{"name":"fmt w L","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%8L|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     'a'|"]],"status":"ok"}},
{"name":"fmt w I left","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%-8I|', 'Ab')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|\"Ab\"    |"]],"status":"ok"}},
{"name":"fmt w null","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%5s|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     |"]],"status":"ok"}},
{"name":"fmt w L null","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%6L|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  NULL|"]],"status":"ok"}},
{"name":"fmt w zero","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%0s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt w leading zero","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%05s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    a|"]],"status":"ok"}},
{"name":"fmt flag only","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%-s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt flag double","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%--5s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a    |"]],"status":"ok"}},
{"name":"fmt pos width","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%2$-6s|%1$4s|', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|b     |   a|"]],"status":"ok"}},
{"name":"fmt star","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    x|"]],"status":"ok"}},
{"name":"fmt star neg","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%-*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus neg","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%-*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star null","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', NULL, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x|"]],"status":"ok"}},
{"name":"fmt star text","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', '4', 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star textcol","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', '4'::text, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star numeric","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', 4.0, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type integer: \"4.0\""}},
{"name":"fmt star int8","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', 4::int8, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star int2","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', 4::int2, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star big","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', 2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"value \"2147483648\" is out of range for type integer"}},
{"name":"fmt star minint","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', -2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt star n","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n pos","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%1$*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     x|"]],"status":"ok"}},
{"name":"fmt star n pos neg","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%1$*2$s|', 'x', -6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x     |"]],"status":"ok"}},
{"name":"fmt star then seq","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|%s|', 3, 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  a|b|"]],"status":"ok"}},
{"name":"fmt star n then seq","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*1$s|%s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star few","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*s|', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n 0","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%*0$s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt width huge","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%2147483648s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt width 99999999999","zone":"America/Chicago","efd":"default","sql":"SELECT format('|%99999999999s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"err too few","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err too few 2","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s %s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err unrecognized","zone":"America/Chicago","efd":"default","sql":"SELECT format('%z', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"z\""}},
{"name":"err upper S","zone":"America/Chicago","efd":"default","sql":"SELECT format('%S', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"S\""}},
{"name":"err unterminated","zone":"America/Chicago","efd":"default","sql":"SELECT format('abc %')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated pos","zone":"America/Chicago","efd":"default","sql":"SELECT format('%1$', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated width","zone":"America/Chicago","efd":"default","sql":"SELECT format('%10', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated flag","zone":"America/Chicago","efd":"default","sql":"SELECT format('%-', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated star","zone":"America/Chicago","efd":"default","sql":"SELECT format('%*', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err pos no dollar","zone":"America/Chicago","efd":"default","sql":"SELECT format('%1s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a"]],"status":"ok"}},
{"name":"err no args","zone":"America/Chicago","efd":"default","sql":"SELECT format()","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format() does not exist"}},
{"name":"err int fmt","zone":"America/Chicago","efd":"default","sql":"SELECT format(1)","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer) does not exist"}},
{"name":"err int fmt 2","zone":"America/Chicago","efd":"default","sql":"SELECT format(1, 'a')","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer, unknown) does not exist"}},
{"name":"err I null after ok","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s %I', 'a', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"err unterminated after null fmtarg","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s %', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"type bool","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s|%s', true, false)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["t|f"]],"status":"ok"}},
{"name":"type int2 int8","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s|%s', 7::int2, 9007199254740993::int8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["7|9007199254740993"]],"status":"ok"}},
{"name":"type numeric","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s|%s', 1.50, -0.000)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.50|0.000"]],"status":"ok"}},
{"name":"type float4","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', '1.1'::float4)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.1"]],"status":"ok"}},
{"name":"type float8 third","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["0.3333333333333333"]],"status":"ok"}},
{"name":"type float8 1e16","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', 1e16::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1e+16"]],"status":"ok"}},
{"name":"type float8 nan inf","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s|%s', 'NaN'::float8, '-Infinity'::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NaN|-Infinity"]],"status":"ok"}},
{"name":"type bytea","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', '\\x0aff'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\\x0aff"]],"status":"ok"}},
{"name":"type date","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-02-29"]],"status":"ok"}},
{"name":"type time","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s|%s', '13:14:15.5'::time, '13:14:15+05:30'::timetz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["13:14:15.5|13:14:15+05:30"]],"status":"ok"}},
{"name":"type timestamp","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', '2024-07-01 12:00:00.25'::timestamp)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 12:00:00.25"]],"status":"ok"}},
{"name":"type tstz","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 07:00:00-05"]],"status":"ok"}},
{"name":"type tstz winter","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', '2024-01-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-01-01 06:00:00-06"]],"status":"ok"}},
{"name":"type interval","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', interval '1 year 2 mons 3 days 04:05:06.7')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1 year 2 mons 3 days 04:05:06.7"]],"status":"ok"}},
{"name":"type nested","zone":"America/Chicago","efd":"default","sql":"SELECT format('<%s>', format('%s', 1))","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["<1>"]],"status":"ok"}},
{"name":"type concat","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', 'a' || 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a1"]],"status":"ok"}},
{"name":"type unknown literal","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', 'lit')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["lit"]],"status":"ok"}},
{"name":"type I numeric","zone":"America/Chicago","efd":"default","sql":"SELECT format('%I', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"1.5\""]],"status":"ok"}},
{"name":"type L numeric","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'1.5'"]],"status":"ok"}},
{"name":"type L date","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-02-29'"]],"status":"ok"}},
{"name":"type L bytea","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L', '\\x0a'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["E'\\\\x0a'"]],"status":"ok"}},
{"name":"tab fmt where","zone":"America/Chicago","efd":"default","sql":"SELECT id FROM gt WHERE format('%s-%s', a, s) = '1-a' ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["1"]],"status":"ok"}},
{"name":"tab fmt order","zone":"America/Chicago","efd":"default","sql":"SELECT id FROM gt ORDER BY format('%s', s) DESC, id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["2"],["4"],["1"],["3"]],"status":"ok"}},
{"name":"tab fmt col fmtstr","zone":"America/Chicago","efd":"default","sql":"SELECT id, format(s || ' %s', a) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","a 1"],["2","z 9"],["3",null],["4","m "]],"status":"ok"}},
{"name":"tab fmt tz","zone":"America/Chicago","efd":"default","sql":"SELECT id, format('%s', tz), format('%L', f) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25,25],"rows":[["1","2024-07-01 07:00:00-05","'0.1'"],["2","2024-07-01 20:00:00-05","'1e+16'"],["3","","NULL"],["4","2024-06-30 18:00:00-05","'NaN'"]],"status":"ok"}},
{"name":"tab fmt star col","zone":"America/Chicago","efd":"default","sql":"SELECT id, format('|%*s|', a, 'x') FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","|x|"],["2","|        x|"],["3","|   x|"],["4","|x|"]],"status":"ok"}},
{"name":"p fmt declared","zone":"America/Chicago","efd":"default","sql":"SELECT format($1, $2)","params":["<%s>","x"],"decl":[25,25],"result":{"phase":"execute","param_oids":[25,25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt declared int arg","zone":"America/Chicago","efd":"default","sql":"SELECT format($1, $2)","params":["<%s>","7"],"decl":[25,23],"result":{"phase":"execute","param_oids":[25,23],"col_oids":[25],"rows":[["<7>"]],"status":"ok"}},
{"name":"p fmt undeclared fmt","zone":"America/Chicago","efd":"default","sql":"SELECT format($1, 'x')","params":["<%s>"],"decl":[],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt undeclared arg","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s', $1)","params":["x"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt undeclared star","zone":"America/Chicago","efd":"default","sql":"SELECT format('%*s', $1, 'x')","params":["3"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt declared float8","zone":"America/Chicago","efd":"default","sql":"SELECT format('%s|%L', $1, $1)","params":["0.1"],"decl":[701],"result":{"phase":"execute","param_oids":[701],"col_oids":[25],"rows":[["0.1|'0.1'"]],"status":"ok"}},
{"name":"p fmt null fmt param","zone":"America/Chicago","efd":"default","sql":"SELECT format($1, 'x')","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"p fmt null arg","zone":"America/Chicago","efd":"default","sql":"SELECT format('%L|%s', $1, $1)","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["NULL|"]],"status":"ok"}},
{"name":"fmt plain","zone":"America/Chicago","efd":"0","sql":"SELECT format('abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abc"]],"status":"ok"}},
{"name":"fmt pct","zone":"America/Chicago","efd":"0","sql":"SELECT format('100%% sure')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["100% sure"]],"status":"ok"}},
{"name":"fmt s","zone":"America/Chicago","efd":"0","sql":"SELECT format('Hello %s', 'World')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["Hello World"]],"status":"ok"}},
{"name":"fmt s two","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s,%s', 1, 2.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1,2.5"]],"status":"ok"}},
{"name":"fmt extra args","zone":"America/Chicago","efd":"0","sql":"SELECT format('x', 1, 2)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["x"]],"status":"ok"}},
{"name":"fmt null fmt","zone":"America/Chicago","efd":"0","sql":"SELECT format(NULL, 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null fmt only","zone":"America/Chicago","efd":"0","sql":"SELECT format(NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt null text fmt","zone":"America/Chicago","efd":"0","sql":"SELECT format(NULL::text, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"fmt s null","zone":"America/Chicago","efd":"0","sql":"SELECT format('[%s]', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["[]"]],"status":"ok"}},
{"name":"fmt I","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 'foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["foo"]],"status":"ok"}},
{"name":"fmt I upper","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 'Foo')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"Foo\""]],"status":"ok"}},
{"name":"fmt I space","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 'foo bar')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"foo bar\""]],"status":"ok"}},
{"name":"fmt I keyword","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 'select')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"select\""]],"status":"ok"}},
{"name":"fmt I unreserved kw","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 'abort')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["abort"]],"status":"ok"}},
{"name":"fmt I dquote","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 'a\"b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"a\"\"b\""]],"status":"ok"}},
{"name":"fmt I int","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"42\""]],"status":"ok"}},
{"name":"fmt I null","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"fmt L","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', 'O''Reilly')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'O''Reilly'"]],"status":"ok"}},
{"name":"fmt L null","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NULL"]],"status":"ok"}},
{"name":"fmt L int","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', 42)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'42'"]],"status":"ok"}},
{"name":"fmt L bool","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', true)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'t'"]],"status":"ok"}},
{"name":"fmt L tstz","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-07-01 07:00:00-05'"]],"status":"ok"}},
{"name":"fmt L float","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'0.333333333333333'"]],"status":"ok"}},
{"name":"fmt sIL combo","zone":"America/Chicago","efd":"0","sql":"SELECT format('INSERT INTO %I VALUES(%L, %s)', 'T', 'x', 7)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["INSERT INTO \"T\" VALUES('x', 7)"]],"status":"ok"}},
{"name":"fmt pos swap","zone":"America/Chicago","efd":"0","sql":"SELECT format('%2$s %1$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["b a"]],"status":"ok"}},
{"name":"fmt pos repeat","zone":"America/Chicago","efd":"0","sql":"SELECT format('%1$s %1$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a a"]],"status":"ok"}},
{"name":"fmt pos then seq","zone":"America/Chicago","efd":"0","sql":"SELECT format('%1$s %s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a b c"]],"status":"ok"}},
{"name":"fmt pos 3 then seq","zone":"America/Chicago","efd":"0","sql":"SELECT format('%3$s %s', 'a', 'b', 'c')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos 0","zone":"America/Chicago","efd":"0","sql":"SELECT format('%0$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt pos too far","zone":"America/Chicago","efd":"0","sql":"SELECT format('%3$s', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt pos huge","zone":"America/Chicago","efd":"0","sql":"SELECT format('%2147483648$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt pos max","zone":"America/Chicago","efd":"0","sql":"SELECT format('%2147483647$s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt w right","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|       abc|"]],"status":"ok"}},
{"name":"fmt w left","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%-10s|', 'abc')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abc       |"]],"status":"ok"}},
{"name":"fmt w short","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%2s|', 'abcdef')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|abcdef|"]],"status":"ok"}},
{"name":"fmt w multibyte","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%5s|', '??')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   ??|"]],"status":"ok"}},
{"name":"fmt w L","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%8L|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     'a'|"]],"status":"ok"}},
{"name":"fmt w I left","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%-8I|', 'Ab')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|\"Ab\"    |"]],"status":"ok"}},
{"name":"fmt w null","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%5s|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     |"]],"status":"ok"}},
{"name":"fmt w L null","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%6L|', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  NULL|"]],"status":"ok"}},
{"name":"fmt w zero","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%0s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt w leading zero","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%05s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    a|"]],"status":"ok"}},
{"name":"fmt flag only","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%-s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a|"]],"status":"ok"}},
{"name":"fmt flag double","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%--5s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|a    |"]],"status":"ok"}},
{"name":"fmt pos width","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%2$-6s|%1$4s|', 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|b     |   a|"]],"status":"ok"}},
{"name":"fmt star","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|    x|"]],"status":"ok"}},
{"name":"fmt star neg","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%-*s|', 5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star minus neg","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%-*s|', -5, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x    |"]],"status":"ok"}},
{"name":"fmt star null","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', NULL, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x|"]],"status":"ok"}},
{"name":"fmt star text","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', '4', 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star textcol","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', '4'::text, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star numeric","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', 4.0, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22P02","message":"invalid input syntax for type integer: \"4.0\""}},
{"name":"fmt star int8","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', 4::int8, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star int2","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', 4::int2, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|   x|"]],"status":"ok"}},
{"name":"fmt star big","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', 2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"value \"2147483648\" is out of range for type integer"}},
{"name":"fmt star minint","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', -2147483648, 'x')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt star n","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n pos","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%1$*2$s|', 'x', 6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|     x|"]],"status":"ok"}},
{"name":"fmt star n pos neg","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%1$*2$s|', 'x', -6)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|x     |"]],"status":"ok"}},
{"name":"fmt star then seq","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|%s|', 3, 'a', 'b')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["|  a|b|"]],"status":"ok"}},
{"name":"fmt star n then seq","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*1$s|%s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star few","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*s|', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"fmt star n 0","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%*0$s|', 3, 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"format specifies argument 0, but arguments are numbered from 1"}},
{"name":"fmt width huge","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%2147483648s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"fmt width 99999999999","zone":"America/Chicago","efd":"0","sql":"SELECT format('|%99999999999s|', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22003","message":"number is out of range"}},
{"name":"err too few","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err too few 2","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s %s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"too few arguments for format()"}},
{"name":"err unrecognized","zone":"America/Chicago","efd":"0","sql":"SELECT format('%z', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"z\""}},
{"name":"err upper S","zone":"America/Chicago","efd":"0","sql":"SELECT format('%S', 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unrecognized format() type specifier \"S\""}},
{"name":"err unterminated","zone":"America/Chicago","efd":"0","sql":"SELECT format('abc %')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated pos","zone":"America/Chicago","efd":"0","sql":"SELECT format('%1$', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated width","zone":"America/Chicago","efd":"0","sql":"SELECT format('%10', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated flag","zone":"America/Chicago","efd":"0","sql":"SELECT format('%-', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err unterminated star","zone":"America/Chicago","efd":"0","sql":"SELECT format('%*', 3)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"err pos no dollar","zone":"America/Chicago","efd":"0","sql":"SELECT format('%1s', 'a')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a"]],"status":"ok"}},
{"name":"err no args","zone":"America/Chicago","efd":"0","sql":"SELECT format()","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format() does not exist"}},
{"name":"err int fmt","zone":"America/Chicago","efd":"0","sql":"SELECT format(1)","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer) does not exist"}},
{"name":"err int fmt 2","zone":"America/Chicago","efd":"0","sql":"SELECT format(1, 'a')","params":[],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42883","message":"function format(integer, unknown) does not exist"}},
{"name":"err I null after ok","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s %I', 'a', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22004","message":"null values cannot be formatted as an SQL identifier"}},
{"name":"err unterminated after null fmtarg","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s %', NULL)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"status":"error","sqlstate":"22023","message":"unterminated format() type specifier"}},
{"name":"type bool","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s|%s', true, false)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["t|f"]],"status":"ok"}},
{"name":"type int2 int8","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s|%s', 7::int2, 9007199254740993::int8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["7|9007199254740993"]],"status":"ok"}},
{"name":"type numeric","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s|%s', 1.50, -0.000)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.50|0.000"]],"status":"ok"}},
{"name":"type float4","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', '1.1'::float4)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1.1"]],"status":"ok"}},
{"name":"type float8 third","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', (1/3.0)::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["0.333333333333333"]],"status":"ok"}},
{"name":"type float8 1e16","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', 1e16::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1e+16"]],"status":"ok"}},
{"name":"type float8 nan inf","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s|%s', 'NaN'::float8, '-Infinity'::float8)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["NaN|-Infinity"]],"status":"ok"}},
{"name":"type bytea","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', '\\x0aff'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\\x0aff"]],"status":"ok"}},
{"name":"type date","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-02-29"]],"status":"ok"}},
{"name":"type time","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s|%s', '13:14:15.5'::time, '13:14:15+05:30'::timetz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["13:14:15.5|13:14:15+05:30"]],"status":"ok"}},
{"name":"type timestamp","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', '2024-07-01 12:00:00.25'::timestamp)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 12:00:00.25"]],"status":"ok"}},
{"name":"type tstz","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', '2024-07-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-07-01 07:00:00-05"]],"status":"ok"}},
{"name":"type tstz winter","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', '2024-01-01 12:00:00+00'::timestamptz)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["2024-01-01 06:00:00-06"]],"status":"ok"}},
{"name":"type interval","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', interval '1 year 2 mons 3 days 04:05:06.7')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["1 year 2 mons 3 days 04:05:06.7"]],"status":"ok"}},
{"name":"type nested","zone":"America/Chicago","efd":"0","sql":"SELECT format('<%s>', format('%s', 1))","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["<1>"]],"status":"ok"}},
{"name":"type concat","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', 'a' || 1)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["a1"]],"status":"ok"}},
{"name":"type unknown literal","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', 'lit')","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["lit"]],"status":"ok"}},
{"name":"type I numeric","zone":"America/Chicago","efd":"0","sql":"SELECT format('%I', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["\"1.5\""]],"status":"ok"}},
{"name":"type L numeric","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', 1.5)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'1.5'"]],"status":"ok"}},
{"name":"type L date","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', '2024-02-29'::date)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["'2024-02-29'"]],"status":"ok"}},
{"name":"type L bytea","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L', '\\x0a'::bytea)","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[25],"rows":[["E'\\\\x0a'"]],"status":"ok"}},
{"name":"tab fmt where","zone":"America/Chicago","efd":"0","sql":"SELECT id FROM gt WHERE format('%s-%s', a, s) = '1-a' ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["1"]],"status":"ok"}},
{"name":"tab fmt order","zone":"America/Chicago","efd":"0","sql":"SELECT id FROM gt ORDER BY format('%s', s) DESC, id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23],"rows":[["2"],["4"],["1"],["3"]],"status":"ok"}},
{"name":"tab fmt col fmtstr","zone":"America/Chicago","efd":"0","sql":"SELECT id, format(s || ' %s', a) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","a 1"],["2","z 9"],["3",null],["4","m "]],"status":"ok"}},
{"name":"tab fmt tz","zone":"America/Chicago","efd":"0","sql":"SELECT id, format('%s', tz), format('%L', f) FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25,25],"rows":[["1","2024-07-01 07:00:00-05","'0.1'"],["2","2024-07-01 20:00:00-05","'1e+16'"],["3","","NULL"],["4","2024-06-30 18:00:00-05","'NaN'"]],"status":"ok"}},
{"name":"tab fmt star col","zone":"America/Chicago","efd":"0","sql":"SELECT id, format('|%*s|', a, 'x') FROM gt ORDER BY id","params":[],"decl":[],"result":{"phase":"execute","param_oids":[],"col_oids":[23,25],"rows":[["1","|x|"],["2","|        x|"],["3","|   x|"],["4","|x|"]],"status":"ok"}},
{"name":"p fmt declared","zone":"America/Chicago","efd":"0","sql":"SELECT format($1, $2)","params":["<%s>","x"],"decl":[25,25],"result":{"phase":"execute","param_oids":[25,25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt declared int arg","zone":"America/Chicago","efd":"0","sql":"SELECT format($1, $2)","params":["<%s>","7"],"decl":[25,23],"result":{"phase":"execute","param_oids":[25,23],"col_oids":[25],"rows":[["<7>"]],"status":"ok"}},
{"name":"p fmt undeclared fmt","zone":"America/Chicago","efd":"0","sql":"SELECT format($1, 'x')","params":["<%s>"],"decl":[],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["<x>"]],"status":"ok"}},
{"name":"p fmt undeclared arg","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s', $1)","params":["x"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt undeclared star","zone":"America/Chicago","efd":"0","sql":"SELECT format('%*s', $1, 'x')","params":["3"],"decl":[],"result":{"phase":"parse","status":"error","sqlstate":"42P18","message":"could not determine data type of parameter $1"}},
{"name":"p fmt declared float8","zone":"America/Chicago","efd":"0","sql":"SELECT format('%s|%L', $1, $1)","params":["0.1"],"decl":[701],"result":{"phase":"execute","param_oids":[701],"col_oids":[25],"rows":[["0.1|'0.1'"]],"status":"ok"}},
{"name":"p fmt null fmt param","zone":"America/Chicago","efd":"0","sql":"SELECT format($1, 'x')","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[[null]],"status":"ok"}},
{"name":"p fmt null arg","zone":"America/Chicago","efd":"0","sql":"SELECT format('%L|%s', $1, $1)","params":[null],"decl":[25],"result":{"phase":"execute","param_oids":[25],"col_oids":[25],"rows":[["NULL|"]],"status":"ok"}}
]"###;

#[test]
fn additional_format_boundaries_and_scan_order() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    for (sql, expected) in [
        (r"SELECT format('%L', '\x0a'::bytea)", r"E'\\x0a'"),
        ("SELECT format('|%5s|', 'é€')", "|   é€|"),
        ("SELECT format('|%1$*2$s|%s|', 'x', 6, 'z')", "|     x|6|"),
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap().rows[0].values,
            vec![Value::Text(expected.into())]
        );
    }
    for (sql, expected) in [
        ("SELECT format('%*z', 4.0)", SqlError::InvalidParameterValue),
        ("SELECT format('%*s', -2147483648)", SqlError::Overflow),
        ("SELECT format('%2147483647s', 'x')", SqlError::Limit),
        ("SELECT format('%*2147483648$s', 'x')", SqlError::Overflow),
        (
            "SELECT format('%*2147483647$s', 'x')",
            SqlError::InvalidParameterValue,
        ),
        ("SELECT format(NULL::integer)", SqlError::UndefinedFunction),
    ] {
        assert_eq!(
            s.execute(sql, &[], None).unwrap_err(),
            CoreError::Sql(expected),
            "{sql}"
        );
    }
}

#[test]
fn declared_timestamp_parameter_and_format_result_type() {
    let f = Fixture::new();
    let mut s = f.admin.as_ref().unwrap().sql_session();
    let timestamp = s
        .execute_at(
            "SELECT '2024-07-01 12:00+00'::timestamptz",
            &[],
            None,
            &clock("UTC"),
        )
        .unwrap()
        .rows
        .remove(0)
        .values
        .remove(0);
    let p = s
        .prepare("SELECT format('%s', $1)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::TimestampTz)])
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().columns[0].data_type, DataType::Text);
    for (zone, expected) in [
        ("UTC", "2024-07-01 12:00:00+00"),
        ("America/Chicago", "2024-07-01 07:00:00-05"),
    ] {
        assert_eq!(
            s.execute_prepared_at(&p, std::slice::from_ref(&timestamp), None, &clock(zone))
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text(expected.into())]
        );
    }
}
