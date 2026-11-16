//! Strict decoding after STRUCT expansion by client.rs, or JSONB string rows.
use google_cloud_spanner::value::Timestamp;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DataChangeRecord {
    pub commit_timestamp: Timestamp,
    pub record_sequence: String,
    pub server_transaction_id: String,
    pub is_last_record_in_transaction_in_partition: bool,
    pub table_name: String,
    pub column_types: Vec<ColumnType>,
    pub mods: Vec<Mod>,
    pub mod_type: ModType,
    pub value_capture_type: ValueCaptureType,
    #[serde(deserialize_with = "integer")]
    pub number_of_records_in_transaction: u64,
    #[serde(deserialize_with = "integer")]
    pub number_of_partitions_in_transaction: u64,
    pub transaction_tag: String,
    pub is_system_transaction: bool,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ColumnType {
    pub name: String,
    #[serde(deserialize_with = "object")]
    pub r#type: Map<String, Value>,
    pub is_primary_key: bool,
    #[serde(deserialize_with = "integer")]
    pub ordinal_position: u64,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Mod {
    #[serde(deserialize_with = "object")]
    pub keys: Map<String, Value>,
    #[serde(deserialize_with = "object")]
    pub new_values: Map<String, Value>,
    #[serde(deserialize_with = "object")]
    pub old_values: Map<String, Value>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ModType {
    Insert,
    Update,
    Delete,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValueCaptureType {
    OldAndNewValues,
    NewValues,
    NewRow,
    NewRowAndOldValues,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct HeartbeatRecord {
    pub timestamp: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ChildPartitionsRecord {
    pub start_timestamp: Timestamp,
    pub record_sequence: String,
    pub child_partitions: Vec<ChildPartition>,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ChildPartition {
    pub token: String,
    pub parent_partition_tokens: Vec<String>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Record {
    Data(DataChangeRecord),
    Heartbeat(HeartbeatRecord),
    Children(ChildPartitionsRecord),
}
#[derive(Debug)]
pub enum DecodeError {
    UnknownKind(String),
    Shape,
    InvalidRecord(serde_json::Error),
}
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Spanner record: {self:?}")
    }
}
impl std::error::Error for DecodeError {}
fn integer<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    match Value::deserialize(d)? {
        Value::String(s) => s.parse().map_err(serde::de::Error::custom),
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected unsigned integer")),
        _ => Err(serde::de::Error::custom("expected unsigned integer")),
    }
}
fn object<'de, D: Deserializer<'de>>(d: D) -> Result<Map<String, Value>, D::Error> {
    let value = Value::deserialize(d)?;
    let value = if let Value::String(s) = value {
        serde_json::from_str(&s).map_err(serde::de::Error::custom)?
    } else {
        value
    };
    match value {
        Value::Object(m) => Ok(m),
        _ => Err(serde::de::Error::custom("expected JSON object")),
    }
}
/// A raw result row contains one GoogleSQL ARRAY<STRUCT> or PostgreSQL JSONB.
pub fn decode_row(row: &Value) -> Result<Vec<Record>, DecodeError> {
    fn expand(value: &Value, out: &mut Vec<Record>) -> Result<(), DecodeError> {
        match value {
            Value::String(s) => expand(
                &serde_json::from_str(s).map_err(DecodeError::InvalidRecord)?,
                out,
            ),
            Value::Array(items) if !items.is_empty() => {
                for item in items {
                    expand(item, out)?;
                }
                Ok(())
            }
            Value::Object(m) if !m.is_empty() => {
                for (kind, value) in m {
                    if ![
                        "data_change_record",
                        "heartbeat_record",
                        "child_partitions_record",
                    ]
                    .contains(&kind.as_str())
                    {
                        return Err(DecodeError::UnknownKind(kind.clone()));
                    }
                    let items = match value {
                        Value::Array(a) => a.clone(),
                        Value::Object(_) => vec![value.clone()],
                        _ => return Err(DecodeError::Shape),
                    };
                    for item in items {
                        let record = match kind.as_str() {
                            "data_change_record" => serde_json::from_value(item).map(Record::Data),
                            "heartbeat_record" => {
                                serde_json::from_value(item).map(Record::Heartbeat)
                            }
                            _ => serde_json::from_value(item).map(Record::Children),
                        }
                        .map_err(DecodeError::InvalidRecord)?;
                        out.push(record);
                    }
                }
                Ok(())
            }
            _ => Err(DecodeError::Shape),
        }
    }
    let mut records = Vec::new();
    expand(row, &mut records)?;
    if records.is_empty() {
        return Err(DecodeError::Shape);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recorded_dialects() {
        for fixture in [
            include_str!("../../tests/fixtures/spanner/google.json"),
            include_str!("../../tests/fixtures/spanner/postgres.json"),
        ] {
            let rows: Vec<Value> = serde_json::from_str(fixture).unwrap();
            let records: Vec<_> = rows.iter().flat_map(|r| decode_row(r).unwrap()).collect();
            assert!(records.iter().any(|r| matches!(r, Record::Data(_))));
            assert!(records.iter().any(|r| matches!(r, Record::Heartbeat(_))));
            assert!(records.iter().any(|r| matches!(r, Record::Children(_))));
            let data = records
                .iter()
                .find_map(|r| match r {
                    Record::Data(d) if d.mod_type == ModType::Update => Some(d),
                    _ => None,
                })
                .unwrap();
            assert_eq!(data.mods[0].keys["parent_id"], "101");
            assert_eq!(data.mods[0].new_values["payload"], "after");
            assert_eq!(data.mods[0].old_values["payload"], "before");
            assert_eq!(data.column_types[0].r#type["code"], "INT64");
            assert!(data.column_types[0].is_primary_key);
            assert_eq!(data.column_types[0].ordinal_position, 1);
            assert_eq!(
                data.value_capture_type,
                ValueCaptureType::NewRowAndOldValues
            );
        }
    }
    #[test]
    fn multi_partition_fault_and_strict_errors() {
        let row: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/spanner/multi-partition.json"
        ))
        .unwrap();
        let Record::Data(record) = decode_row(&row).unwrap().remove(0) else {
            panic!()
        };
        assert_eq!(record.number_of_partitions_in_transaction, 2);
        assert_eq!(record.number_of_records_in_transaction, 2);
        assert_eq!(record.transaction_tag, "fault-injection-multi-partition");
        assert!(matches!(
            decode_row(&serde_json::json!({"future_record":{}})),
            Err(DecodeError::UnknownKind(_))
        ));
        assert!(matches!(
            decode_row(&serde_json::json!({"heartbeat_record":{}})),
            Err(DecodeError::InvalidRecord(_))
        ));
        assert!(matches!(
            decode_row(&serde_json::json!({"data_change_record":{}})),
            Err(DecodeError::InvalidRecord(_))
        ));
        assert!(matches!(
            decode_row(&serde_json::json!({"child_partitions_record":{}})),
            Err(DecodeError::InvalidRecord(_))
        ));
        assert!(decode_row(&serde_json::json!({"heartbeat_record":[]})).is_err());
    }
}
