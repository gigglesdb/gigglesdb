//! Connector-owned binlog events with bounded fixture expansion and live size checks.
use super::client::{MysqlError, Result};
use mysql_common::{
    Value,
    binlog::{
        EventStreamReader,
        consts::{BinlogVersion::Version4, EventType},
        events::{Event, EventData, OptionalMetaExtractor, OptionalMetadataField, TableMapEvent},
        row::BinlogRow,
    },
    constants::ColumnType,
    proto::MySerialize,
};
use std::{
    collections::HashMap,
    io::{BufRead, Cursor, Read},
};

#[derive(Clone, Debug, PartialEq)]
pub enum MysqlValue {
    Null,
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    Decimal(String),
    DateTime(u16, u8, u8, u8, u8, u8, u32),
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
    Timestamp(String),
    Year(String),
    Bit(Vec<u8>),
    Binary(Vec<u8>),
    Text {
        bytes: Vec<u8>,
        collation: u16,
    },
    Json(serde_json::Value),
    Enum {
        ordinal: u64,
        label: Option<Vec<u8>>,
    },
    Set {
        bitmap: u64,
        labels: Option<Vec<Vec<u8>>>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MysqlColumn {
    pub name: Vec<u8>,
    pub column_type: u8,
    pub metadata: Vec<u8>,
    pub unsigned: Option<bool>,
    pub collation: Option<u16>,
    pub nullable: bool,
    pub labels: Option<Vec<Vec<u8>>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MysqlTable {
    pub id: u64,
    pub schema: Vec<u8>,
    pub table: Vec<u8>,
    pub columns: Vec<MysqlColumn>,
    /// Complete serialized table map, retaining every FULL optional metadata field.
    pub full_metadata: Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryKind {
    Begin,
    Commit,
    Ddl,
    Other,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowKind {
    Write,
    Update,
    Delete,
}
pub type RowImage = Vec<MysqlValue>;
#[derive(Clone, Debug, PartialEq)]
pub enum MysqlEvent {
    FormatDescription {
        server_version: String,
    },
    PreviousGtids(String),
    Gtid {
        sid: String,
        sequence: u64,
        tag: Option<String>,
        /// Immediate source commit time, Unix microseconds; never an ordering key.
        commit_time: Option<i64>,
    },
    AnonymousGtid {
        sid: String,
        sequence: u64,
        commit_time: Option<i64>,
    },
    Query {
        schema: Vec<u8>,
        sql: Vec<u8>,
        kind: QueryKind,
    },
    TableMap(MysqlTable),
    Rows {
        table_id: u64,
        kind: RowKind,
        images: Vec<(Option<RowImage>, Option<RowImage>)>,
    },
    Xid(u64),
    Rotate {
        file: Vec<u8>,
        offset: u64,
    },
    Heartbeat,
    TransactionPayload(Vec<MysqlEvent>),
    /// ROWS_QUERY, IGNORABLE and STOP carry no row or transaction state.
    Skipped(u8),
}
/// End coordinate of the wire event. All payload children share its end.
#[derive(Clone, Debug, PartialEq)]
pub struct PositionedEvent {
    pub event: MysqlEvent,
    pub file: String,
    pub log_pos: u64,
}
pub struct BinlogDecoder {
    reader: EventStreamReader,
    maps: HashMap<u64, TableMapEvent<'static>>,
    limit: usize,
    file: Option<String>,
}
impl BinlogDecoder {
    pub fn new(max_decompressed_bytes: usize) -> Self {
        Self {
            reader: EventStreamReader::new(Version4),
            maps: HashMap::new(),
            limit: max_decompressed_bytes,
            file: None,
        }
    }
    pub fn read(&mut self, input: &mut impl BufRead) -> Result<Option<MysqlEvent>> {
        let Some(event) = self.reader.read(input)? else {
            return Ok(None);
        };
        Self::verify_checksum(&event)?;
        self.decode(&event).map(Some)
    }
    /// Supply the initial on-disk filename; subsequent ROTATEs update it.
    pub fn read_positioned(
        &mut self,
        input: &mut impl BufRead,
        initial_file: &str,
    ) -> Result<Option<PositionedEvent>> {
        let Some(event) = self.reader.read(input)? else {
            return Ok(None);
        };
        Self::verify_checksum(&event)?;
        let decoded = self.decode(&event)?;
        if let MysqlEvent::Rotate { file, .. } = &decoded {
            self.file = Some(
                String::from_utf8(file.clone())
                    .map_err(|_| MysqlError::Invalid("binlog filename"))?,
            );
        }
        Ok(Some(PositionedEvent {
            event: decoded,
            file: self.file.get_or_insert_with(|| initial_file.into()).clone(),
            log_pos: u64::from(event.header().log_pos()),
        }))
    }
    fn verify_checksum(event: &Event) -> Result<()> {
        if let Some(checksum) = event
            .checksum()
            .filter(|_| event.footer().get_checksum_enabled())
        {
            let algorithm = event
                .footer()
                .get_checksum_alg()
                .map_err(|_| MysqlError::Invalid("checksum algorithm"))?
                .ok_or(MysqlError::Invalid("missing checksum algorithm"))?;
            if event.calc_checksum(algorithm).to_le_bytes() != checksum {
                return Err(MysqlError::Invalid("binlog checksum mismatch"));
            }
        }
        Ok(())
    }
    pub(crate) fn decode_stream_event(&mut self, event: &Event) -> Result<Option<MysqlEvent>> {
        Self::verify_checksum(event)?;
        if event.header().event_type_raw() == EventType::TRANSACTION_PAYLOAD_EVENT as u8 {
            let payload =
                event.read_event::<mysql_common::binlog::events::TransactionPayloadEvent<'_>>()?;
            // mysql_async has already decompressed this payload and emits its inner
            // events next. Check the declaration, without expanding it a second time.
            // M6 preflight must require max_binlog_cache_size <= this limit when
            // binlog_transaction_compression=ON: that server-side bound protects
            // memory before mysql_async's otherwise unbounded decompression.
            if payload.uncompressed_size() > self.limit as u64 {
                return Err(MysqlError::PayloadTooLarge { limit: self.limit });
            }
            return Ok(None);
        }
        self.decode(event).map(Some)
    }
    fn decode(&mut self, event: &Event) -> Result<MysqlEvent> {
        let raw = event.header().event_type_raw();
        Ok(match event.read_data()? {
            Some(EventData::FormatDescriptionEvent(f)) => MysqlEvent::FormatDescription {
                server_version: f.server_version().trim_end_matches('\0').into(),
            },
            Some(EventData::PreviousGtidsEvent(p)) => {
                let mut groups = Vec::new();
                for sid in p.sids() {
                    let mut s = uuid::Uuid::from_bytes(sid.uuid()).to_string();
                    if let Some(tag) = sid.tag() {
                        s.push(':');
                        s.push_str(&tag.to_string());
                    }
                    for i in sid.intervals() {
                        s.push_str(&format!(":{}-{}", i.start(), i.end() - 1));
                    }
                    groups.push(s);
                }
                MysqlEvent::PreviousGtids(groups.join(","))
            }
            Some(EventData::GtidEvent(g)) => MysqlEvent::Gtid {
                sid: uuid::Uuid::from_bytes(g.sid()).to_string(),
                sequence: g.gno(),
                tag: g.tag().map(ToString::to_string),
                commit_time: commit_time(g.immediate_commit_timestamp()),
            },
            Some(EventData::AnonymousGtidEvent(g)) => MysqlEvent::AnonymousGtid {
                sid: uuid::Uuid::from_bytes(g.0.sid()).to_string(),
                sequence: g.0.gno(),
                commit_time: commit_time(g.0.immediate_commit_timestamp()),
            },
            Some(EventData::QueryEvent(q)) => {
                let sql = q.query_raw();
                let first = sql
                    .split(|b| b.is_ascii_whitespace() || *b == b';')
                    .find(|s| !s.is_empty())
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let kind = match first.as_slice() {
                    b"BEGIN" => QueryKind::Begin,
                    b"COMMIT" => QueryKind::Commit,
                    b"CREATE" | b"ALTER" | b"DROP" | b"TRUNCATE" | b"RENAME" => QueryKind::Ddl,
                    _ => QueryKind::Other,
                };
                MysqlEvent::Query {
                    schema: q.schema_raw().to_vec(),
                    sql: sql.to_vec(),
                    kind,
                }
            }
            Some(EventData::TableMapEvent(map)) => {
                let table = table(&map)?;
                self.maps.insert(map.table_id(), map.into_owned());
                MysqlEvent::TableMap(table)
            }
            Some(EventData::RowsEvent(rows)) => {
                let map = self
                    .maps
                    .get(&rows.table_id())
                    .ok_or(MysqlError::Invalid("row without table map"))?;
                let columns = table(map)?.columns;
                let kind = match raw {
                    23 | 30 => RowKind::Write,
                    24 | 31 => RowKind::Update,
                    25 | 32 => RowKind::Delete,
                    _ => return Err(MysqlError::UnsupportedEvent(raw)),
                };
                let mut images = Vec::new();
                for pair in rows.rows(map) {
                    let (before, after) = pair?;
                    images.push((
                        before.map(|r| image(r, &columns)).transpose()?,
                        after.map(|r| image(r, &columns)).transpose()?,
                    ));
                }
                MysqlEvent::Rows {
                    table_id: rows.table_id(),
                    kind,
                    images,
                }
            }
            Some(EventData::XidEvent(x)) => MysqlEvent::Xid(x.xid),
            Some(EventData::RotateEvent(r)) => {
                self.maps.clear();
                MysqlEvent::Rotate {
                    file: r.name_raw().to_vec(),
                    offset: r.position(),
                }
            }
            Some(EventData::HeartbeatEvent) => MysqlEvent::Heartbeat,
            Some(EventData::TransactionPayloadEvent(payload)) => {
                if payload.uncompressed_size() > self.limit as u64 {
                    return Err(MysqlError::PayloadTooLarge { limit: self.limit });
                }
                let mut bytes = Vec::new();
                payload
                    .decompressed()?
                    .take((self.limit as u64).saturating_add(1))
                    .read_to_end(&mut bytes)?;
                if bytes.len() > self.limit {
                    return Err(MysqlError::PayloadTooLarge { limit: self.limit });
                }
                if payload.uncompressed_size() != 0
                    && payload.uncompressed_size() != bytes.len() as u64
                {
                    return Err(MysqlError::Invalid("payload size mismatch"));
                }
                let mut input = Cursor::new(bytes);
                let mut events = Vec::new();
                while let Some(inner) = self.reader.read_decompressed(&mut input)? {
                    events.push(self.decode(&inner)?);
                }
                MysqlEvent::TransactionPayload(events)
            }
            Some(
                EventData::RowsQueryEvent(_) | EventData::IgnorableEvent(_) | EventData::StopEvent,
            ) => MysqlEvent::Skipped(raw),
            _ => return Err(MysqlError::UnsupportedEvent(raw)),
        })
    }
}
fn commit_time(timestamp: u64) -> Option<i64> {
    (timestamp > 0)
        .then(|| i64::try_from(timestamp).ok())
        .flatten()
}
fn table(map: &TableMapEvent<'_>) -> Result<MysqlTable> {
    let meta = OptionalMetaExtractor::new(map.iter_optional_meta())?;
    let mut names = meta.iter_column_name();
    let mut unsigned = meta.iter_signedness();
    let mut charset = meta.iter_charset();
    let mut enum_charset = meta.iter_enum_and_set_charset();
    let (mut enums, mut sets) = (Vec::new(), Vec::new());
    for field in map.iter_optional_meta() {
        match field? {
            OptionalMetadataField::EnumStrValue(e) => {
                for labels in e.iter_values() {
                    enums.push(
                        labels?
                            .values()
                            .iter()
                            .map(|v| v.value_raw().to_vec())
                            .collect(),
                    );
                }
            }
            OptionalMetadataField::SetStrValue(s) => {
                for labels in s.iter_values() {
                    sets.push(
                        labels?
                            .values()
                            .iter()
                            .map(|v| v.value_raw().to_vec())
                            .collect(),
                    );
                }
            }
            _ => {}
        }
    }
    let (mut enums, mut sets) = (enums.into_iter(), sets.into_iter());
    let mut columns = Vec::new();
    for i in 0..map.columns_count() as usize {
        let ty = map
            .get_column_type(i)
            .map_err(|_| MysqlError::Invalid("column type"))?
            .ok_or(MysqlError::Invalid("missing column type"))?;
        columns.push(MysqlColumn {
            name: names
                .next()
                .transpose()?
                .ok_or(MysqlError::Invalid("FULL column names required"))?
                .name_raw()
                .to_vec(),
            column_type: ty as u8,
            metadata: map.get_column_metadata(i).unwrap_or_default().to_vec(),
            unsigned: if ty.is_numeric_type() {
                Some(
                    unsigned
                        .next()
                        .ok_or(MysqlError::Invalid("column signedness required"))?,
                )
            } else {
                None
            },
            collation: if ty.is_character_type() {
                charset.next().transpose()?
            } else if ty.is_enum_or_set_type() {
                enum_charset.next().transpose()?
            } else {
                None
            },
            nullable: map.null_bitmask()[i],
            labels: match ty {
                ColumnType::MYSQL_TYPE_ENUM => enums.next(),
                ColumnType::MYSQL_TYPE_SET => sets.next(),
                _ => None,
            },
        });
    }
    let mut full_metadata = Vec::new();
    map.serialize(&mut full_metadata);
    Ok(MysqlTable {
        id: map.table_id(),
        schema: map.database_name_raw().to_vec(),
        table: map.table_name_raw().to_vec(),
        columns,
        full_metadata,
    })
}
fn image(row: BinlogRow, columns: &[MysqlColumn]) -> Result<RowImage> {
    if row.len() != columns.len() {
        return Err(MysqlError::Invalid("FULL row image required"));
    }
    let mut out = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        let value = row
            .as_ref(i)
            .ok_or(MysqlError::Invalid("missing row column"))?
            .clone();
        let value =
            Value::try_from(value).map_err(|_| MysqlError::Invalid("partial or invalid JSON"))?;
        use ColumnType::*;
        let ty = ColumnType::try_from(column.column_type)
            .map_err(|_| MysqlError::Invalid("column type"))?;
        let value = match (ty, value) {
            (_, Value::NULL) => MysqlValue::Null,
            (MYSQL_TYPE_ENUM, Value::Int(n)) if n >= 0 => MysqlValue::Enum {
                ordinal: n as u64,
                label: if n == 0 {
                    Some(Vec::new())
                } else {
                    column
                        .labels
                        .as_ref()
                        .and_then(|l| l.get(n as usize - 1))
                        .cloned()
                },
            },
            (MYSQL_TYPE_SET, Value::Bytes(b)) => {
                if b.len() > 8 {
                    return Err(MysqlError::Invalid("SET bitmap"));
                }
                let bitmap = b
                    .iter()
                    .enumerate()
                    .fold(0u64, |n, (i, v)| n | (u64::from(*v) << (8 * i)));
                MysqlValue::Set {
                    bitmap,
                    labels: column.labels.as_ref().map(|l| {
                        l.iter()
                            .enumerate()
                            .filter(|(i, _)| *i < 64 && bitmap & (1u64 << i) != 0)
                            .map(|(_, s)| s.clone())
                            .collect()
                    }),
                }
            }
            (MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL, Value::Bytes(b)) => {
                MysqlValue::Decimal(text(b)?)
            }
            (MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2, Value::Bytes(b)) => {
                MysqlValue::Timestamp(text(b)?)
            }
            (MYSQL_TYPE_YEAR, Value::Bytes(b)) => MysqlValue::Year(text(b)?),
            (MYSQL_TYPE_BIT, Value::Bytes(b)) => MysqlValue::Bit(b),
            (MYSQL_TYPE_JSON, Value::Bytes(b)) => MysqlValue::Json(
                serde_json::from_slice(&b).map_err(|_| MysqlError::Invalid("JSON"))?,
            ),
            (MYSQL_TYPE_INT24, Value::Int(n)) if column.unsigned == Some(false) => {
                MysqlValue::Signed(i64::from(((n as i32) << 8) >> 8))
            }
            (_, Value::Int(n)) if column.unsigned == Some(true) => MysqlValue::Unsigned(n as u64),
            (_, Value::Int(n)) => MysqlValue::Signed(n),
            (_, Value::UInt(n)) => MysqlValue::Unsigned(n),
            (_, Value::Float(n)) => MysqlValue::Float(n),
            (_, Value::Double(n)) => MysqlValue::Double(n),
            (_, Value::Date(y, m, d, h, min, s, us)) => {
                MysqlValue::DateTime(y, m, d, h, min, s, us)
            }
            (_, Value::Time(negative, days, hours, minutes, seconds, micros)) => MysqlValue::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            },
            (_, Value::Bytes(bytes)) if column.collation.is_some_and(|c| c != 63) => {
                MysqlValue::Text {
                    bytes,
                    collation: column.collation.unwrap(),
                }
            }
            (_, Value::Bytes(bytes)) => MysqlValue::Binary(binary_bytes(column, bytes)?),
        };
        out.push(value);
    }
    Ok(out)
}
fn text(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|_| MysqlError::Invalid("ASCII scalar"))
}

// Field_string::pack strips the charset pad byte; binary collation uses 0x00.
// Restore only fixed STRING/BINARY cells, never VARCHAR/VARBINARY or BLOB.
fn binary_bytes(column: &MysqlColumn, mut bytes: Vec<u8>) -> Result<Vec<u8>> {
    if column.column_type == ColumnType::MYSQL_TYPE_STRING as u8 && column.collation == Some(63) {
        let [kind, low] = column.metadata.as_slice() else {
            return Err(MysqlError::Invalid("BINARY column metadata"));
        };
        if kind & 0xcf != (ColumnType::MYSQL_TYPE_STRING as u8) & 0xcf {
            return Err(MysqlError::Invalid("BINARY column metadata"));
        }
        let width = usize::from(*low) | (usize::from((kind & 0x30) ^ 0x30) << 4);
        if bytes.len() > width {
            return Err(MysqlError::Invalid("BINARY column length"));
        }
        bytes.resize(width, 0);
    }
    Ok(bytes)
}

#[cfg(test)]
mod binary_padding_tests {
    use super::*;
    fn column(width: u8) -> MysqlColumn {
        MysqlColumn {
            name: b"b".to_vec(),
            column_type: ColumnType::MYSQL_TYPE_STRING as u8,
            metadata: vec![ColumnType::MYSQL_TYPE_STRING as u8, width],
            unsigned: None,
            collation: Some(63),
            nullable: false,
            labels: None,
        }
    }
    #[test]
    fn fixed_binary_restores_trailing_zeroes_and_maps_to_bytea() {
        use crate::{
            mapping::{
                Value as PgValue,
                mysql::{forward, plan_column},
            },
            mysql::schema::ColumnDescriptor,
        };
        for width in [16, 32] {
            for zeros in [1, 4, width] {
                let decoded =
                    binary_bytes(&column(width), vec![0xab; usize::from(width - zeros)]).unwrap();
                let mut expected = vec![0xab; usize::from(width)];
                expected[usize::from(width - zeros)..].fill(0);
                assert_eq!(decoded, expected);
                let descriptor = ColumnDescriptor {
                    name: "b".into(),
                    ordinal: 1,
                    data_type: "binary".into(),
                    column_type: format!("binary({width})"),
                    character_set: None,
                    collation: None,
                    nullable: false,
                    generated: false,
                    column_default: None,
                    default_generated: false,
                    generation_expression: None,
                };
                assert_eq!(
                    forward(
                        &plan_column(&descriptor).unwrap(),
                        &MysqlValue::Binary(decoded)
                    )
                    .unwrap(),
                    PgValue::Bytea(expected)
                );
            }
        }
    }
    #[test]
    fn binary_padding_preserves_full_values_and_variable_width_cells() {
        let bytes = vec![0xab; 32];
        assert_eq!(binary_bytes(&column(32), bytes.clone()).unwrap(), bytes);
        for ty in [
            ColumnType::MYSQL_TYPE_VARCHAR,
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnType::MYSQL_TYPE_BLOB,
        ] {
            let mut c = column(32);
            c.column_type = ty as u8;
            assert_eq!(binary_bytes(&c, vec![1, 0]).unwrap(), vec![1, 0]);
        }
        let mut c = column(32);
        c.collation = Some(255);
        assert_eq!(binary_bytes(&c, vec![1]).unwrap(), vec![1]);
        c.collation = None;
        assert_eq!(binary_bytes(&c, vec![1]).unwrap(), vec![1]);
    }
    #[test]
    fn binary_padding_rejects_invalid_metadata_and_oversize_values() {
        assert!(binary_bytes(&column(16), vec![1; 17]).is_err());
        let mut c = column(32);
        c.metadata.clear();
        assert!(binary_bytes(&c, vec![]).is_err());
        c.metadata = vec![ColumnType::MYSQL_TYPE_ENUM as u8, 32];
        assert!(binary_bytes(&c, vec![]).is_err());
    }
}
