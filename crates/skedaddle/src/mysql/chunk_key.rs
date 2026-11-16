//! Persisted MySQL primary-key metadata. Identity bytes never define source order.
use super::schema::Collation;
use crate::{
    event::Row,
    mapping::{MappingError, Value},
    pg::chunk::{self, ChunkError, KeyIdentity},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};

/// The only qualified policy for now: ascending primary-key column order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MysqlKeyDirection {
    Asc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlKeyColumn {
    pub name: String,
    pub data_type: String,
    pub column_type: String,
    pub character_set: Option<String>,
    pub collation: Option<Collation>,
    /// MysqlPgColumn::type_id: the destination mapping identity, including typmod.
    pub type_id: String,
    pub direction: MysqlKeyDirection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlTableKey {
    pub event_table: String,
    pub schema: String,
    pub table: String,
    /// Primary-key order, not table ordinal order.
    pub columns: Vec<MysqlKeyColumn>,
}

#[derive(Debug)]
pub enum MysqlKeyError {
    Invalid(&'static str),
    UnsupportedKeyType { column: String, data_type: String },
    Mapping(MappingError),
}
impl fmt::Display for MysqlKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MySQL chunk key: {self:?}")
    }
}
impl std::error::Error for MysqlKeyError {}
impl From<MappingError> for MysqlKeyError {
    fn from(error: MappingError) -> Self {
        Self::Mapping(error)
    }
}
type Result<T> = std::result::Result<T, MysqlKeyError>;
fn required(value: &str) -> bool {
    !value.is_empty() && !value.contains('\0')
}
impl MysqlTableKey {
    pub fn validate(&self) -> Result<()> {
        if !required(&self.event_table)
            || !required(&self.schema)
            || !required(&self.table)
            || self.columns.is_empty()
        {
            return Err(MysqlKeyError::Invalid("table/key identity"));
        }
        let mut names = BTreeSet::new();
        for c in &self.columns {
            if !required(&c.name)
                || !required(&c.data_type)
                || !required(&c.column_type)
                || !required(&c.type_id)
                || !names.insert(&c.name)
            {
                return Err(MysqlKeyError::Invalid("column/mapping identity"));
            }
            // Fail closed until the MySQL predicate builder qualifies these orders.
            for kind in [&c.data_type, &c.column_type] {
                let base = kind
                    .split(['(', ' '])
                    .next()
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if matches!(
                    base.as_str(),
                    "enum"
                        | "set"
                        | "json"
                        | "float"
                        | "double"
                        | "real"
                        | "geometry"
                        | "point"
                        | "linestring"
                        | "polygon"
                        | "multipoint"
                        | "multilinestring"
                        | "multipolygon"
                        | "geometrycollection"
                        | "geomcollection"
                ) {
                    return Err(MysqlKeyError::UnsupportedKeyType {
                        column: c.name.clone(),
                        data_type: kind.clone(),
                    });
                }
            }
            match (&c.character_set, &c.collation) {
                (None, None) => {}
                (Some(cs), Some(co)) if required(cs) && required(&co.name) && co.id != 0 => {}
                _ => return Err(MysqlKeyError::Invalid("charset/collation identity")),
            }
        }
        Ok(())
    }
    pub(crate) fn validate_key(&self, key: &[Value]) -> Result<()> {
        if key.len() != self.columns.len() || key.iter().any(|v| matches!(v, Value::Null)) {
            return Err(MysqlKeyError::Invalid("NULL/incomplete key"));
        }
        for value in key {
            value.canonical_bytes()?;
        }
        Ok(())
    }
}
impl KeyIdentity for MysqlTableKey {
    fn key(&self, row: &Row) -> chunk::Result<(Vec<u8>, Row)> {
        self.validate().map_err(ChunkError::MysqlKey)?;
        let mut values = Vec::with_capacity(self.columns.len());
        let mut key = Row::new();
        for column in &self.columns {
            let opaque =
                row.get(&column.name)
                    .ok_or(ChunkError::MysqlKey(MysqlKeyError::Invalid(
                        "incomplete key",
                    )))?;
            if opaque.type_id != column.type_id {
                return Err(ChunkError::MysqlKey(MysqlKeyError::Invalid(
                    "mapped type identity",
                )));
            }
            let bytes = opaque
                .bytes
                .as_deref()
                .ok_or(ChunkError::MysqlKey(MysqlKeyError::Invalid("NULL key")))?;
            values.push(Value::from_canonical_bytes(bytes)?);
            key.insert(column.name.clone(), opaque.clone());
        }
        self.validate_key(&values).map_err(ChunkError::MysqlKey)?;
        Ok((chunk::key_bytes(&values)?, key))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn table() -> MysqlTableKey {
        MysqlTableKey {
            event_table: "rows".into(),
            schema: "db".into(),
            table: "rows".into(),
            columns: vec![MysqlKeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                column_type: "int unsigned".into(),
                character_set: None,
                collation: None,
                type_id: "int4".into(),
                direction: MysqlKeyDirection::Asc,
            }],
        }
    }
    #[test]
    fn unsupported_key_types_are_typed() {
        for kind in [
            "ENUM",
            "set",
            "json",
            "float",
            "double",
            "real",
            "geometry",
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
        ] {
            for descriptor in [false, true] {
                let mut t = table();
                if descriptor {
                    t.columns[0].column_type = kind.into();
                } else {
                    t.columns[0].data_type = kind.into();
                }
                assert!(
                    matches!(t.validate(), Err(MysqlKeyError::UnsupportedKeyType { .. })),
                    "{kind}"
                );
            }
        }
    }
    #[test]
    fn identities_require_complete_nonnull_mapped_keys() {
        use crate::event::OpaqueValue;
        let t = table();
        let mut row: Row = [(
            "id".into(),
            OpaqueValue {
                type_id: "int4".into(),
                bytes: Some(Value::Int4(7).canonical_bytes().unwrap()),
            },
        )]
        .into();
        assert!(t.key(&row).is_ok());
        assert!(t.key(&Row::new()).is_err());
        row.get_mut("id").unwrap().type_id = "foreign".into();
        assert!(t.key(&row).is_err());
        row.get_mut("id").unwrap().type_id = "int4".into();
        row.get_mut("id").unwrap().bytes = None;
        assert!(t.key(&row).is_err());
        row.get_mut("id").unwrap().bytes = Some(Value::Null.canonical_bytes().unwrap());
        assert!(t.key(&row).is_err());
    }
    #[test]
    fn invalid_metadata_and_direction_fail_closed() {
        let mut t = table();
        t.columns.push(t.columns[0].clone());
        assert!(t.validate().is_err());
        let mut t = table();
        t.columns[0].character_set = Some("utf8mb4".into());
        assert!(t.validate().is_err());
        assert!(serde_json::from_str::<MysqlKeyDirection>("\"Desc\"").is_err());
    }
}
