//! DD-0068 embedded SQL probe. Each input gets a fresh database supplied by the runner.
//! This reports core errors verbatim; it does not invent pgwire SQLSTATE evidence.
use giggles_core::{
    Admin, Limits,
    relational::{
        DataType, Date, Float4, Float8, Interval, Numeric, PgTime, TimeTz, Timestamp, TimestampTz,
        Value,
    },
};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::io::{self, Read};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    setup: Vec<String>,
    query: String,
    #[serde(default)]
    params: Vec<Json>,
    #[serde(default)]
    exact_numeric: bool,
    #[serde(default)]
    describe_metadata: bool,
}

fn value(v: Value, exact: bool) -> Json {
    if exact {
        match &v {
            Value::Numeric(n) => return json!({"numeric_text":n.to_string()}),
            Value::Float4(n) => {
                return json!({"float4_bits":format!("{:08x}", n.value().to_bits())});
            }
            Value::Float8(n) => {
                return json!({"float8_bits":format!("{:016x}", n.value().to_bits())});
            }
            _ => {}
        }
    }
    match v {
        Value::Numeric(n) => {
            serde_json::from_str(&n.to_string()).unwrap_or_else(|_| json!(n.to_string()))
        }
        Value::Float4(n) => {
            if n.value().is_finite() {
                json!(n.value())
            } else {
                json!(n.to_text())
            }
        }
        Value::Float8(n) => {
            if n.value().is_finite() {
                json!(n.value())
            } else {
                json!(n.to_text())
            }
        }
        Value::Date(v) => json!(v.to_string()),
        Value::Time(v) => json!(v.to_string()),
        Value::TimeTz(v) => json!(v.to_string()),
        Value::Timestamp(v) => json!(v.to_string().replacen(' ', "T", 1)),
        Value::TimestampTz(v) => {
            let text = v.to_string().replacen(' ', "T", 1);
            // PG JSON datetime encoding always includes timezone minutes.
            json!(text.replace("+00", "+00:00"))
        }
        Value::Interval(v) => json!(v.to_string()),
        Value::Null => Json::Null,
        Value::Bool(v) => json!(v),
        Value::Int(v) => json!(v),
        Value::Text(v) => json!(v),
        Value::Bytes(v) => json!(format!(
            "\\x{}",
            v.iter().map(|b| format!("{b:02x}")).collect::<String>()
        )),
    }
}

fn type_name(t: &DataType) -> &'static str {
    match t {
        DataType::Date => "date",
        DataType::Time => "time without time zone",
        DataType::TimeTz => "time with time zone",
        DataType::Timestamp => "timestamp without time zone",
        DataType::TimestampTz => "timestamp with time zone",
        DataType::Interval => "interval",
        DataType::Boolean => "boolean",
        DataType::SmallInt => "smallint",
        DataType::Integer => "integer",
        DataType::BigInt => "bigint",
        DataType::Text => "text",
        DataType::Bytes => "bytea",
        DataType::Numeric => "numeric",
        DataType::Real => "real",
        DataType::DoublePrecision => "double precision",
        DataType::Varchar => "character varying",
        DataType::Bpchar => "character",
        DataType::Uuid => "uuid",
        DataType::Jsonb => "jsonb",
        DataType::IntArray => "integer[]",
        DataType::BigIntArray => "bigint[]",
        DataType::TextArray => "text[]",
        DataType::UuidArray => "uuid[]",
        DataType::RegType => "regtype",
        DataType::TsVector => "tsvector",
        DataType::TsQuery => "tsquery",
        DataType::RegConfig => "regconfig",
        DataType::RegDictionary => "regdictionary",
        DataType::Oid => "oid",
        DataType::PgChar => "\"char\"",
        DataType::RealArray => "real[]",
        DataType::PgCharArray => "\"char\"[]",
        DataType::SmallIntArray => "smallint[]",
        DataType::RegDictionaryArray => "regdictionary[]",
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("new database path required")?;
    if std::path::Path::new(&path).exists() {
        return Err("probe requires a new database path".into());
    }
    let mut input = String::new();
    io::stdin()
        .take(1024 * 1024 + 1)
        .read_to_string(&mut input)?;
    if input.len() > 1024 * 1024 {
        return Err("case too large".into());
    }
    let case: Case = serde_json::from_str(&input)?;
    let params = case
        .params
        .into_iter()
        .map(|v| match v {
            Json::Null => Ok(Value::Null),
            Json::Bool(v) => Ok(Value::Bool(v)),
            Json::String(v) => Ok(Value::Text(v)),
            Json::Number(v) => v
                .as_i64()
                .map(Value::Int)
                .ok_or("integer parameter out of range"),
            Json::Object(v) if v.len() == 2 => {
                let kind = v
                    .get("type")
                    .and_then(Json::as_str)
                    .ok_or("missing parameter type")?;
                let text = v
                    .get("value")
                    .and_then(Json::as_str)
                    .ok_or("typed parameter must be string")?;
                match kind {
                    "date" => Date::parse(text, &Default::default())
                        .map(Value::Date)
                        .map_err(|_| "invalid date"),
                    "time" | "time without time zone" => PgTime::parse(text)
                        .map(Value::Time)
                        .map_err(|_| "invalid time"),
                    "timetz" | "time with time zone" => TimeTz::parse(text, &Default::default())
                        .map(Value::TimeTz)
                        .map_err(|_| "invalid timetz"),
                    "timestamp" | "timestamp without time zone" => {
                        Timestamp::parse(text, &Default::default())
                            .map(Value::Timestamp)
                            .map_err(|_| "invalid timestamp")
                    }
                    "timestamptz" | "timestamp with time zone" => {
                        TimestampTz::parse(text, &Default::default())
                            .map(Value::TimestampTz)
                            .map_err(|_| "invalid timestamptz")
                    }
                    "interval" => Interval::parse(text)
                        .map(Value::Interval)
                        .map_err(|_| "invalid interval"),
                    "numeric" => Numeric::parse(text)
                        .map(Value::Numeric)
                        .map_err(|_| "invalid numeric"),
                    "real" => u32::from_str_radix(text, 16)
                        .map_err(|_| "invalid real bits")
                        .and_then(|b| {
                            Float4::from_binary(&b.to_be_bytes())
                                .map(Value::Float4)
                                .map_err(|_| "invalid real")
                        }),
                    "double precision" => u64::from_str_radix(text, 16)
                        .map_err(|_| "invalid float bits")
                        .and_then(|b| {
                            Float8::from_binary(&b.to_be_bytes())
                                .map(Value::Float8)
                                .map_err(|_| "invalid float")
                        }),
                    _ => Err("unsupported typed parameter"),
                }
            }
            _ => Err("unsupported parameter"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let admin = Admin::create(path, Limits::default())?;
    let mut session = admin.sql_session();
    for sql in case.setup {
        if let Err(e) = session.execute(&sql, &[], None) {
            println!(
                "{}",
                json!({"status":"setup_error", "core_error":format!("{e:?}")})
            );
            return Ok(());
        }
    }
    let mut metadata = None;
    let result = (|| {
        let prepared = session.prepare(&case.query)?;
        let description = session.describe(&prepared)?;
        if case.describe_metadata {
            metadata = Some(
                json!({"parameterTypes":description.parameters.iter().map(type_name).collect::<Vec<_>>(), "resultTypes":description.columns.iter().map(|c| type_name(&c.data_type)).collect::<Vec<_>>() }),
            );
        }
        let result = session.execute_prepared(&prepared, &params, None)?;
        Ok::<_, giggles_core::CoreError>(json!({
            "status":"ok",
            "columns":description.columns.iter().map(|c| json!({"name":c.name,"type":type_name(&c.data_type)})).collect::<Vec<_>>(),
            "rows":result.rows.into_iter().map(|r| r.values.into_iter().map(|v| value(v, case.exact_numeric)).collect::<Vec<_>>()).collect::<Vec<_>>()
        }))
    })();
    let mut output =
        result.unwrap_or_else(|e| json!({"status":"error","core_error":format!("{e:?}")}));
    if case.describe_metadata {
        output["description"] = metadata.unwrap_or(Json::Null);
    }
    println!("{output}");
    Ok(())
}
