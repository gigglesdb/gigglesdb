//! Plain-column catalog observations; no whole-row derived references.
use postgres_protocol::types as wire;
use serde_json::{Value, json};
use tokio_postgres::{
    Row,
    types::{FromSql, Kind, Type},
};

// Decode only the closed set produced by the collector. Unknown wire types
// fail collection rather than being misreported as JSON strings.
struct Cell(Value);
impl<'a> FromSql<'a> for Cell {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let value = match *ty {
            Type::BOOL => json!(wire::bool_from_sql(raw)?),
            Type::INT2 => json!(wire::int2_from_sql(raw)?),
            Type::INT4 => json!(wire::int4_from_sql(raw)?),
            Type::INT8 => json!(wire::int8_from_sql(raw)?),
            Type::OID => json!(wire::oid_from_sql(raw)?),
            Type::FLOAT8 => json!(wire::float8_from_sql(raw)?),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
                json!(std::str::from_utf8(raw)?)
            }
            Type::JSON => serde_json::from_slice(raw)?,
            Type::JSONB if raw.first() == Some(&1) => serde_json::from_slice(&raw[1..])?,
            _ if matches!(ty.kind(), Kind::Array(_)) => {
                let cells = <Vec<Option<Cell>> as FromSql>::from_sql(ty, raw)?;
                Value::Array(
                    cells
                        .into_iter()
                        .map(|c| c.map_or(Value::Null, |c| c.0))
                        .collect(),
                )
            }
            _ => return Err("unsupported catalog JSON wire type".into()),
        };
        Ok(Self(value))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}
pub(crate) fn row_json(row: &Row) -> Result<Value, ()> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, column)| {
            row.try_get::<_, Option<Cell>>(i)
                .map(|cell| (column.name().to_owned(), cell.map_or(Value::Null, |c| c.0)))
                .map_err(|_| ())
        })
        .collect::<Result<serde_json::Map<_, _>, _>>()
        .map(Value::Object)
}

/// GigglesDB comparison contract, separate from PostgreSQL collation identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteOrderUtf8;
impl ByteOrderUtf8 {
    pub fn accepts_source_collation(name: &crate::pg::chunk::QualifiedName) -> bool {
        name.schema == "pg_catalog" && matches!(name.name.as_str(), "C" | "POSIX")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_order_capability_accepts_only_builtin_c_and_posix() {
        use crate::pg::chunk::QualifiedName;
        for (schema, name, accepted) in [
            ("pg_catalog", "C", true),
            ("pg_catalog", "POSIX", true),
            ("public", "C", false),
            ("pg_catalog", "default", false),
        ] {
            assert_eq!(
                ByteOrderUtf8::accepts_source_collation(&QualifiedName {
                    schema: schema.into(),
                    name: name.into()
                }),
                accepted
            );
        }
    }
    #[test]
    fn collector_wire_types_preserve_json_values_and_reject_unknowns() {
        for (ty, bytes, expected) in [
            (Type::BOOL, vec![1], json!(true)),
            (Type::INT8, 42_i64.to_be_bytes().to_vec(), json!(42)),
            (Type::TEXT, b"quote\"\\\n".to_vec(), json!("quote\"\\\n")),
            (
                Type::JSON,
                b"{\"key\":[null,1]}".to_vec(),
                json!({"key":[null,1]}),
            ),
            (Type::JSONB, b"\x01[true,2]".to_vec(), json!([true, 2])),
        ] {
            assert_eq!(Cell::from_sql(&ty, &bytes).unwrap().0, expected);
        }
        assert!(Cell::from_sql(&Type::BYTEA, &[1, 2]).is_err());
    }
}
