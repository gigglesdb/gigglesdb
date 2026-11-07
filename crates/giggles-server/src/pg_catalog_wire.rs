//! Wire encoding for the read-only catalog adapter. Catalog values never become
//! executable application SQL or bypass the session's authority gate.
use crate::{pgwire, transport::MAX_RESPONSE};
use giggles_pg_catalog::{Field, PgType, ResultSet, Value};
type Result<T> = std::result::Result<T, &'static str>;
pub(crate) fn value_bytes(value: &Value) -> Result<usize> {
    let extra = match value {
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Numeric(v) => v.to_string().len(),
        Value::Array(values) | Value::Record(values) => {
            values.iter().try_fold(0usize, |sum, v| {
                sum.checked_add(value_bytes(v)?).ok_or("54000")
            })?
        }
        _ => 0,
    };
    let bytes = std::mem::size_of::<Value>()
        .checked_add(extra)
        .ok_or("54000")?;
    if bytes > MAX_RESPONSE {
        return Err("54000");
    }
    Ok(bytes)
}

pub(crate) fn error(error: giggles_pg_catalog::Error) -> &'static str {
    use giggles_pg_catalog::Error as E;
    match error {
        E::Syntax => "42601",
        E::MixedRelations | E::Mutation => "42501",
        E::UnknownColumn => "42703",
        E::AmbiguousColumn => "42702",
        E::Type => "42804",
        E::Parameters => "08P01",
        E::Limit => "54000",
        E::InvalidSnapshot => "55000",
        E::Unsupported => "0A000",
    }
}

pub(crate) fn snapshot(
    tables: Vec<giggles_core::relational::TableDef>,
) -> giggles_pg_catalog::CatalogSnapshot {
    use giggles_core::relational::DataType as C;
    use giggles_pg_catalog::ScalarType as P;
    giggles_pg_catalog::CatalogSnapshot {
        tables: tables
            .into_iter()
            .map(|table| giggles_pg_catalog::TableDef {
                id: table.id,
                name: table.name,
                primary_key: table.primary_key,
                columns: table
                    .columns
                    .into_iter()
                    .map(|column| giggles_pg_catalog::ColumnDef {
                        id: column.id,
                        name: column.name,
                        not_null: column.not_null,
                        data_type: match column.data_type {
                            C::Boolean => P::Boolean,
                            C::SmallInt => P::SmallInt,
                            C::Integer => P::Integer,
                            C::BigInt => P::BigInt,
                            C::Text => P::Text,
                            C::Bytes => P::Bytes,
                            C::Numeric => P::Numeric,
                            C::Real => P::Real,
                            C::DoublePrecision => P::DoublePrecision,
                            C::Date => P::Date,
                            C::Time => P::Time,
                            C::TimeTz => P::TimeTz,
                            C::Timestamp => P::Timestamp,
                            C::TimestampTz => P::TimestampTz,
                            C::Interval => P::Interval,
                        },
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn is_vector(ty: PgType) -> bool {
    matches!(ty, PgType::Int2Vector | PgType::OidVector)
}
fn element(ty: PgType) -> Option<PgType> {
    match ty {
        PgType::Int2Vector => Some(PgType::Int2),
        PgType::OidVector => Some(PgType::Oid),
        _ => ty.array_element(),
    }
}

pub(crate) fn parameter_type(oid: u32) -> Result<PgType> {
    Ok(match oid {
        16 => PgType::Bool,
        17 => PgType::Bytea,
        18 => PgType::Char,
        19 => PgType::Name,
        20 => PgType::Int8,
        21 => PgType::Int2,
        23 => PgType::Int4,
        25 | 1043 => PgType::Text,
        26 => PgType::Oid,
        1700 => PgType::Numeric,
        700 => PgType::Float4,
        701 => PgType::Float8,
        1005 => PgType::Int2Array,
        1007 => PgType::Int4Array,
        1016 => PgType::Int8Array,
        1000 => PgType::BoolArray,
        1001 => PgType::ByteaArray,
        1002 => PgType::CharArray,
        1003 => PgType::NameArray,
        22 => PgType::Int2Vector,
        30 => PgType::OidVector,
        1028 => PgType::OidArray,
        1009 => PgType::TextArray,
        _ => return Err("0A000"),
    })
}

pub(crate) fn decode(bytes: &[u8], ty: PgType, format: u16) -> Result<Value> {
    use giggles_core::relational::{DataType as T, Value as V};
    if bytes.len() > crate::transport::MAX_FRAME {
        return Err("54000");
    }
    if format > 1 {
        return Err("08P01");
    }
    if ty == PgType::Oid {
        return if format == 0 {
            std::str::from_utf8(bytes)
                .map_err(|_| "22021")?
                .parse::<u32>()
                .map(|v| Value::Int(v.into()))
                .map_err(|_| "22P02")
        } else {
            let bytes: [u8; 4] = bytes.try_into().map_err(|_| "22P03")?;
            Ok(Value::Int(u32::from_be_bytes(bytes).into()))
        };
    }
    if let Some(scalar) = element(ty) {
        if format == 0 {
            return Err("0A000");
        }
        let mut reader = pgwire::Reader { b: bytes, at: 0 };
        let dimensions = reader.u32()?;
        let nulls = reader.u32()?;
        if nulls > 1 || reader.u32()? != scalar.oid() {
            return Err("22P03");
        }
        if dimensions == 0 {
            reader.end()?;
            return Ok(Value::Array(vec![]));
        }
        if dimensions != 1 {
            return Err("0A000");
        }
        let count = reader.u32()? as usize;
        if count > 8192 {
            return Err("54000");
        }
        if reader.u32()? != u32::from(!is_vector(ty)) {
            return Err("0A000");
        }
        let mut values = Vec::new();
        for _ in 0..count {
            let length = reader.u32()?;
            values.push(if length == u32::MAX {
                if nulls == 0 || is_vector(ty) {
                    return Err("22P03");
                }
                Value::Null
            } else {
                decode(reader.take(length as usize)?, scalar, 1)?
            });
        }
        reader.end()?;
        return Ok(Value::Array(values));
    }
    // PostgreSQL internal "char" uses a zero byte for its empty value.
    // Text format renders that value as an empty string, not SQL NULL.
    if ty == PgType::Char && format == 1 && bytes == [0] {
        return Ok(Value::Text(String::new()));
    }
    let data_type = match ty {
        PgType::Bool => T::Boolean,
        PgType::Int2 => T::SmallInt,
        PgType::Int4 => T::Integer,
        PgType::Int8 => T::BigInt,
        PgType::Text | PgType::Name | PgType::Char => T::Text,
        PgType::Bytea => T::Bytes,
        PgType::Numeric => T::Numeric,
        PgType::Float4 => T::Real,
        PgType::Float8 => T::DoublePrecision,
        _ => return Err("0A000"),
    };
    if (ty == PgType::Char && (bytes.len() > 1 || (format == 1 && bytes.is_empty())))
        || (ty == PgType::Name && bytes.len() > 63)
    {
        return Err("22P03");
    }
    if matches!(data_type, T::Text) && bytes.contains(&0) {
        return Err("22021");
    }
    Ok(match pgwire::decode(bytes, &data_type, format)? {
        V::Null => Value::Null,
        V::Bool(v) => Value::Bool(v),
        V::Int(v) => Value::Int(v),
        V::Text(v) => Value::Text(v),
        V::Bytes(v) => Value::Bytes(v),
        V::Numeric(v) => Value::Numeric(v),
        V::Float4(v) => Value::Float4(v),
        V::Float8(v) => Value::Float8(v),
        V::Date(_)
        | V::Time(_)
        | V::TimeTz(_)
        | V::Timestamp(_)
        | V::TimestampTz(_)
        | V::Interval(_) => return Err("0A000"),
    })
}

pub(crate) fn description(fields: &[Field], formats: &[u16], out: &mut Vec<u8>) -> Result<()> {
    if fields.len() > 256 {
        return Err("54000");
    }
    if fields.is_empty() {
        return pgwire::frame(b'n', &[], out);
    }
    let mut bytes = (fields.len() as u16).to_be_bytes().to_vec();
    for (i, field) in fields.iter().enumerate() {
        if field.name.len() > 65536 || field.name.contains('\0') {
            return Err("54000");
        }
        if field.data_type == PgType::Record {
            return Err("0A000");
        }
        bytes.extend(field.name.as_bytes());
        bytes.push(0);
        bytes.extend(0u32.to_be_bytes());
        bytes.extend(0i16.to_be_bytes());
        bytes.extend(field.data_type.oid().to_be_bytes());
        let size = field.data_type.type_size();
        bytes.extend(size.to_be_bytes());
        bytes.extend((-1i32).to_be_bytes());
        bytes.extend(pgwire::format_at(formats, i).to_be_bytes());
        if bytes.len() > MAX_RESPONSE {
            return Err("54000");
        }
    }
    pgwire::frame(b'T', &bytes, out)
}

pub(crate) fn encode(value: &Value, ty: PgType, format: u16) -> Result<Option<Vec<u8>>> {
    encode_with_extra(value, ty, format, 0)
}
fn encode_with_extra(
    value: &Value,
    ty: PgType,
    format: u16,
    extra: i32,
) -> Result<Option<Vec<u8>>> {
    if format > 1 {
        return Err("08P01");
    }
    if *value == Value::Null {
        return Ok(None);
    }
    let bytes = match (value, ty) {
        (Value::Numeric(v), PgType::Numeric) => {
            if format == 1 {
                v.to_binary()
            } else {
                v.to_string().into_bytes()
            }
        }
        (Value::Float4(v), PgType::Float4) => {
            if format == 1 {
                v.to_binary().to_vec()
            } else {
                v.to_text_with_extra_float_digits(extra)
                    .map_err(|_| "22003")?
                    .into_bytes()
            }
        }
        (Value::Float8(v), PgType::Float8) => {
            if format == 1 {
                v.to_binary().to_vec()
            } else {
                v.to_text_with_extra_float_digits(extra)
                    .map_err(|_| "22003")?
                    .into_bytes()
            }
        }
        (Value::Bool(v), PgType::Bool) => {
            if format == 0 {
                if *v { b"t".to_vec() } else { b"f".to_vec() }
            } else {
                vec![u8::from(*v)]
            }
        }
        (Value::Int(v), PgType::Int2) => {
            let v = i16::try_from(*v).map_err(|_| "22003")?;
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Int(v), PgType::Int4) => {
            let v = i32::try_from(*v).map_err(|_| "22003")?;
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Int(v), PgType::Oid) => {
            let v = u32::try_from(*v).map_err(|_| "22003")?;
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Int(v), PgType::Int8) => {
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Text(v), PgType::Text | PgType::Name | PgType::Char) => {
            if v.contains('\0')
                || (ty == PgType::Char && v.len() > 1)
                || (ty == PgType::Name && v.len() > 63)
            {
                return Err("22021");
            }
            if v.len() > MAX_RESPONSE {
                return Err("54000");
            }
            if ty == PgType::Char && format == 1 && v.is_empty() {
                vec![0]
            } else {
                v.as_bytes().to_vec()
            }
        }
        (Value::Bytes(v), PgType::Bytea) => {
            if v.len() > MAX_RESPONSE / 2 {
                return Err("54000");
            }
            if format == 0 {
                format!("\\x{}", hex::encode(v)).into_bytes()
            } else {
                v.clone()
            }
        }
        (Value::Array(values), array) if element(array).is_some() => {
            if values.len() > 8192 {
                return Err("54000");
            }
            if is_vector(array) && values.contains(&Value::Null) {
                return Err("42804");
            }
            let scalar = element(array).unwrap();
            let mut encoded = Vec::new();
            if format == 0 {
                if !is_vector(array) {
                    encoded.push(b'{');
                }
                for (i, value) in values.iter().enumerate() {
                    if i != 0 {
                        encoded.push(if is_vector(array) { b' ' } else { b',' });
                    }
                    match encode(value, scalar, 0)? {
                        None => encoded.extend(b"NULL"),
                        Some(item) => {
                            if encoded
                                .len()
                                .checked_add(item.len().saturating_mul(2) + 3)
                                .ok_or("54000")?
                                > MAX_RESPONSE
                            {
                                return Err("54000");
                            }
                            // Always quote text, including the string "NULL".
                            if matches!(
                                scalar,
                                PgType::Text | PgType::Name | PgType::Char | PgType::Bytea
                            ) {
                                encoded.push(b'"');
                                for byte in item {
                                    if matches!(byte, b'"' | b'\\') {
                                        encoded.push(b'\\');
                                    }
                                    encoded.push(byte);
                                }
                                encoded.push(b'"');
                            } else {
                                encoded.extend(item);
                            }
                        }
                    }
                    if encoded.len() > MAX_RESPONSE {
                        return Err("54000");
                    }
                }
                if !is_vector(array) {
                    encoded.push(b'}');
                }
            } else {
                encoded.extend(i32::from(!values.is_empty()).to_be_bytes());
                encoded.extend(i32::from(values.contains(&Value::Null)).to_be_bytes());
                encoded.extend(scalar.oid().to_be_bytes());
                if !values.is_empty() {
                    encoded.extend((values.len() as i32).to_be_bytes());
                    encoded.extend(i32::from(!is_vector(array)).to_be_bytes());
                }
                for value in values {
                    match encode(value, scalar, 1)? {
                        None => encoded.extend((-1i32).to_be_bytes()),
                        Some(item) => {
                            if encoded.len().checked_add(item.len() + 4).ok_or("54000")?
                                > MAX_RESPONSE
                            {
                                return Err("54000");
                            }
                            encoded.extend((item.len() as i32).to_be_bytes());
                            encoded.extend(item);
                        }
                    }
                    if encoded.len() > MAX_RESPONSE {
                        return Err("54000");
                    }
                }
            }
            encoded
        }
        (_, PgType::Record) => return Err("0A000"),
        _ => return Err("42804"),
    };
    if bytes.len() > MAX_RESPONSE {
        return Err("54000");
    }
    Ok(Some(bytes))
}

pub(crate) fn retained_bytes(result: &ResultSet) -> Result<usize> {
    if result.rows.len() > giggles_pg_catalog::MAX_ROWS || result.fields.len() > 256 {
        return Err("54000");
    }
    let mut total = 0usize;
    for field in &result.fields {
        total = total.checked_add(field.name.len() + 32).ok_or("54000")?;
    }
    for row in &result.rows {
        if row.len() != result.fields.len() {
            return Err("XX000");
        }
        for (value, field) in row.iter().zip(&result.fields) {
            // Text representation bounds both escaping growth and retained data.
            let length = encode(value, field.data_type, 0)?.map_or(0, |b| b.len());
            total = total
                .checked_add(length + std::mem::size_of::<Value>() + 4)
                .ok_or("54000")?;
            if let Value::Array(values) = value {
                total = total
                    .checked_add(
                        values
                            .len()
                            .checked_mul(std::mem::size_of::<Value>())
                            .ok_or("54000")?,
                    )
                    .ok_or("54000")?;
            }
            if total > MAX_RESPONSE {
                return Err("54000");
            }
        }
    }
    Ok(total)
}

pub(crate) fn rows(
    extra: i32,
    result: &ResultSet,
    formats: &[u16],
    start: usize,
    end: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    let rows = result.rows.get(start..end).ok_or("XX000")?;
    for row in rows {
        if row.len() != result.fields.len() {
            return Err("XX000");
        }
        let mut bytes = (row.len() as u16).to_be_bytes().to_vec();
        for (i, (value, field)) in row.iter().zip(&result.fields).enumerate() {
            match encode_with_extra(value, field.data_type, pgwire::format_at(formats, i), extra)? {
                None => bytes.extend((-1i32).to_be_bytes()),
                Some(value) => {
                    bytes.extend((value.len() as i32).to_be_bytes());
                    bytes.extend(value);
                }
            }
            if bytes.len() > MAX_RESPONSE {
                return Err("54000");
            }
        }
        pgwire::frame(b'D', &bytes, out)?;
    }
    Ok(())
}

// Initialized when the first Service is constructed, stable for this process.
static STARTUP: std::sync::OnceLock<Option<i64>> = std::sync::OnceLock::new();
pub(crate) fn initialize_startup() {
    STARTUP.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok())
    });
}
pub(crate) fn execute(
    plan: &giggles_pg_catalog::Plan,
    snapshot: &giggles_pg_catalog::CatalogSnapshot,
    parameters: &[Value],
) -> std::result::Result<ResultSet, giggles_pg_catalog::Error> {
    giggles_pg_catalog::execute_with_startup(
        plan,
        snapshot,
        parameters,
        STARTUP.get().copied().flatten(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::{FromSql, Type};

    #[test]
    fn empty_internal_char_has_distinct_text_binary_and_null_encodings() {
        let empty = Value::Text(String::new());
        assert_eq!(encode(&empty, PgType::Char, 0).unwrap(), Some(vec![]));
        assert_eq!(encode(&empty, PgType::Char, 1).unwrap(), Some(vec![0]));
        assert_eq!(encode(&Value::Null, PgType::Char, 1).unwrap(), None);
        assert_eq!(decode(&[], PgType::Char, 0).unwrap(), empty);
        assert_eq!(decode(&[0], PgType::Char, 1).unwrap(), empty);
        assert!(decode(&[], PgType::Char, 1).is_err());
        assert!(decode(&[0], PgType::Char, 0).is_err());
        assert!(encode(&Value::Text("ab".into()), PgType::Char, 0).is_err());
        for format in [0, 1] {
            let v = Value::Text("r".into());
            let bytes = encode(&v, PgType::Char, format).unwrap().unwrap();
            assert_eq!(decode(&bytes, PgType::Char, format).unwrap(), v);
        }
    }
    #[test]
    fn oidvector_preserves_unsigned_elements_zero_origin_and_vector_syntax() {
        assert_eq!(parameter_type(30), Ok(PgType::OidVector));
        let value = Value::Array(vec![Value::Int(0), Value::Int(u32::MAX.into())]);
        assert_eq!(
            encode(&value, PgType::OidVector, 0).unwrap().unwrap(),
            b"0 4294967295"
        );
        let wire = encode(&value, PgType::OidVector, 1).unwrap().unwrap();
        assert_eq!(&wire[8..12], &26u32.to_be_bytes());
        assert_eq!(&wire[16..20], &0u32.to_be_bytes());
        assert_eq!(decode(&wire, PgType::OidVector, 1).unwrap(), value);
        let mut wrong = wire.clone();
        wrong[16..20].copy_from_slice(&1u32.to_be_bytes());
        assert_eq!(decode(&wrong, PgType::OidVector, 1), Err("0A000"));
        wrong = wire.clone();
        wrong[8..12].copy_from_slice(&23u32.to_be_bytes());
        assert_eq!(decode(&wrong, PgType::OidVector, 1), Err("22P03"));
        for value in [Value::Null, Value::Int(-1), Value::Int(4_294_967_296)] {
            assert!(encode(&Value::Array(vec![value]), PgType::OidVector, 1).is_err());
        }
        let empty = Value::Array(vec![]);
        let bytes = encode(&empty, PgType::OidVector, 1).unwrap().unwrap();
        assert_eq!(decode(&bytes, PgType::OidVector, 1).unwrap(), empty);
    }
    #[test]
    fn vectors_use_zero_based_binary_bounds_and_names_escape_as_arrays() {
        let value = Value::Array(vec![Value::Int(2), Value::Int(1)]);
        assert_eq!(
            encode(&value, PgType::Int2Vector, 0).unwrap().unwrap(),
            b"2 1"
        );
        let bytes = encode(&value, PgType::Int2Vector, 1).unwrap().unwrap();
        assert_eq!(&bytes[16..20], &0i32.to_be_bytes());
        assert_eq!(decode(&bytes, PgType::Int2Vector, 1).unwrap(), value);
        assert!(encode(&Value::Array(vec![Value::Null]), PgType::Int2Vector, 1).is_err());
        let names = Value::Array(vec![Value::Text("a,b".into()), Value::Text("NULL".into())]);
        assert_eq!(
            encode(&names, PgType::NameArray, 0).unwrap().unwrap(),
            br#"{"a,b","NULL"}"#
        );
    }
    #[test]
    fn actual_driver_decodes_unsigned_oid_and_signed_scalar_boundaries() {
        let oid = encode(&Value::Int(u32::MAX.into()), PgType::Oid, 1)
            .unwrap()
            .unwrap();
        assert_eq!(u32::from_sql(&Type::OID, &oid).unwrap(), u32::MAX);
        assert_eq!(encode(&Value::Int(-1), PgType::Oid, 0), Err("22003"));
        assert_eq!(encode(&Value::Int(i64::MAX), PgType::Int2, 0), Err("22003"));
        let small = encode(&Value::Int(i16::MIN.into()), PgType::Int2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(i16::from_sql(&Type::INT2, &small).unwrap(), i16::MIN);
    }
    #[test]
    fn actual_driver_decodes_array_nulls_empty_and_escaped_text() {
        let value = Value::Array(vec![
            Value::Text("NULL".into()),
            Value::Null,
            Value::Text("a,\"b\\c".into()),
            Value::Text("λ".into()),
        ]);
        let binary = encode(&value, PgType::TextArray, 1).unwrap().unwrap();
        assert_eq!(
            Vec::<Option<String>>::from_sql(&Type::TEXT_ARRAY, &binary).unwrap(),
            vec![
                Some("NULL".into()),
                None,
                Some("a,\"b\\c".into()),
                Some("λ".into())
            ]
        );
        assert_eq!(
            String::from_utf8(encode(&value, PgType::TextArray, 0).unwrap().unwrap()).unwrap(),
            "{\"NULL\",NULL,\"a,\\\"b\\\\c\",\"λ\"}"
        );
        let empty = encode(&Value::Array(vec![]), PgType::OidArray, 1)
            .unwrap()
            .unwrap();
        assert!(
            Vec::<u32>::from_sql(&Type::OID_ARRAY, &empty)
                .unwrap()
                .is_empty()
        );
        let oid = encode(
            &Value::Array(vec![Value::Int(u32::MAX.into())]),
            PgType::OidArray,
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            Vec::<u32>::from_sql(&Type::OID_ARRAY, &oid).unwrap(),
            vec![u32::MAX]
        );
    }
    #[test]
    fn wrong_types_nested_arrays_and_internal_records_fail_closed() {
        assert_eq!(
            encode(&Value::Text("true".into()), PgType::Bool, 0),
            Err("42804")
        );
        assert_eq!(
            encode(&Value::Record(vec![]), PgType::Record, 0),
            Err("0A000")
        );
        assert_eq!(
            encode(
                &Value::Array(vec![Value::Array(vec![])]),
                PgType::Int2Array,
                1
            ),
            Err("42804")
        );
        assert_eq!(
            encode(&Value::Text("a\0b".into()), PgType::Text, 0),
            Err("22021")
        );
        assert_eq!(encode(&Value::Null, PgType::Text, 2), Err("08P01"));
    }
    #[test]
    fn retained_budget_checks_shape_and_escaped_output_before_release() {
        let fields = vec![Field {
            name: "values".into(),
            data_type: PgType::TextArray,
        }];
        let valid = ResultSet {
            fields: fields.clone(),
            rows: vec![vec![Value::Array(vec![Value::Text("x".into())])]],
        };
        assert!(retained_bytes(&valid).unwrap() > 1);
        assert_eq!(
            retained_bytes(&ResultSet {
                fields: fields.clone(),
                rows: vec![vec![]]
            }),
            Err("XX000")
        );
        let huge = ResultSet {
            fields,
            rows: vec![vec![Value::Array(vec![Value::Text(
                "\\".repeat(MAX_RESPONSE / 2),
            )])]],
        };
        assert_eq!(retained_bytes(&huge), Err("54000"));
    }
}
