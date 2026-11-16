//! D-A3 capture. The batch and its front transaction survive cancelled appends.
use super::{
    assembly::Assembler,
    client,
    frontier::{Frontier, SpannerOrder, SpannerPosition, Timestamp},
    records::{ColumnType, ModType},
    stream::{Event as StreamEvent, PartitionReader, timestamp},
};
use crate::{
    buffer::writer::{BufferWriter, DurableManifestRef, WriterCheckpoint},
    definition::SpannerEndpoint,
    engine::{Capture, Captured},
    event::{
        AssembledTxn, Event, EventKind, OpaqueValue, Position, PositionOrder, Row, SourceIdentity,
        TxnId,
    },
    mapping::{self, PgType, Value},
};
use base64::Engine;
use std::{collections::VecDeque, sync::Arc, time::Instant};

#[derive(Debug)]
pub enum ConversionError {
    Unsupported(String),
    InvalidValue,
}
impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Spanner conversion: {self:?}")
    }
}
impl std::error::Error for ConversionError {}
fn recovery(e: impl std::fmt::Display) -> crate::coordinator::Error {
    crate::coordinator::Error::RecoveryRequired(e.to_string())
}
fn release_boundary(
    assembler: &mut Assembler,
    released_through: &mut Option<Timestamp>,
    boundary: Timestamp,
) -> Result<Option<Vec<super::assembly::Transaction>>, super::assembly::Error> {
    // Newly discovered children initially certify start-1. Consume their
    // inclusive replay without sending an older boundary to the assembler.
    if released_through.is_some_and(|previous| boundary < previous) {
        return Ok(None);
    }
    let transactions = assembler.release(boundary)?;
    *released_through = Some(boundary);
    Ok(Some(transactions))
}
fn batch_boundary(batch: &VecDeque<AssembledTxn>) -> crate::engine::Result<Option<Timestamp>> {
    batch
        .front()
        .map(|txn| {
            match SpannerPosition::decode(txn.commit_position.bytes()).map_err(recovery)? {
                SpannerPosition::Txn { commit_ts, .. } => Ok(commit_ts),
                _ => Err(recovery("invalid transaction position")),
            }
        })
        .transpose()
}
/// Temporary S5 typed conversion; S9 replaces this single function. Unsupported
/// types (even NULLs), and sub-microsecond TIMESTAMP values, block explicitly.
pub fn convert_value(
    column: &ColumnType,
    input: &serde_json::Value,
) -> Result<OpaqueValue, client::Error> {
    use ConversionError::*;
    let code = column
        .r#type
        .get("code")
        .and_then(|v| v.as_str())
        .ok_or(InvalidValue)?;
    let ty = match code {
        "INT64" => PgType::Int8,
        "STRING" => PgType::Text {
            encoding: "UTF8".into(),
        },
        "BOOL" => PgType::Bool,
        "FLOAT64" => PgType::Float8,
        "BYTES" => PgType::Bytea,
        "TIMESTAMP" => PgType::Timestamptz,
        "NUMERIC" => PgType::Numeric,
        "JSON" => PgType::Json,
        _ => return Err(Unsupported(code.into()).into()),
    };
    let value = if input.is_null() {
        Value::Null
    } else {
        match code {
            "BYTES" => Value::Bytea(
                base64::engine::general_purpose::STANDARD
                    .decode(input.as_str().ok_or(InvalidValue)?)?,
            ),
            "TIMESTAMP" => {
                let ts = timestamp(serde_json::from_value(input.clone())?);
                if ts % 1000 != 0 {
                    return Err(Unsupported("TIMESTAMP nanoseconds".into()).into());
                }
                Value::Timestamptz(mapping::Timestamp::Finite(i64::try_from(
                    ts / 1000 - 946_684_800_000_000i128,
                )?))
            }
            _ => {
                if !input.is_string()
                    && !(code == "BOOL" && input.is_boolean()
                        || code == "FLOAT64" && input.is_number())
                {
                    return Err(InvalidValue.into());
                }
                let text = input
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| input.to_string());
                Value::parse_pg_text(&ty, Some(text.as_bytes()))?
            }
        }
    };
    Ok(OpaqueValue {
        type_id: format!("spanner:{code}"),
        bytes: if value == Value::Null {
            None
        } else {
            Some(value.canonical_bytes()?)
        },
    })
}

pub type BufferedIdentity = (Vec<u8>, Vec<u8>);

/// TxnId is versioned JSON [1, server_transaction_id, record_sequences]. Keeping
/// the sequences in the buffered transaction permits exact filter restoration,
/// including multiple mods per record, without changing engine event formats.
pub fn buffered_identities(txn: &AssembledTxn) -> Result<Vec<BufferedIdentity>, client::Error> {
    let (version, id, sequences): (u8, String, Vec<String>) = serde_json::from_slice(&txn.txn.0)?;
    if version != 1 {
        return Err("unsupported Spanner transaction identity version".into());
    }
    Ok(sequences
        .into_iter()
        .map(|s| (id.as_bytes().to_vec(), s.into_bytes()))
        .collect())
}

pub struct SpannerCapture {
    reader: PartitionReader,
    assembler: Assembler,
    writer: BufferWriter,
    order: Arc<SpannerOrder>,
    source: SourceIdentity,
    batch: VecDeque<AssembledTxn>,
    boundary: Option<Timestamp>,
    released_through: Option<Timestamp>,
    last: Frontier,
    schema_guard: Option<super::schema::Guard>,
}
impl SpannerCapture {
    /// Reader and writer must belong to this endpoint/snapshot/order domain;
    /// restore the reader's DuplicateFilter using buffered_identities on restart.
    pub fn new(
        endpoint: &SpannerEndpoint,
        snapshot: Timestamp,
        reader: PartitionReader,
        writer: BufferWriter,
        capacity: usize,
    ) -> Result<Self, client::Error> {
        let order = Arc::new(SpannerOrder::new(
            &endpoint.project,
            &endpoint.instance,
            &endpoint.database,
            &endpoint.change_stream,
            snapshot,
        )?);
        let source = SourceIdentity {
            engine: "spanner".into(),
            system_id: format!("{}/{}", endpoint.project, endpoint.instance),
            lineage: snapshot.to_string(),
            db_name: endpoint.database.clone(),
            stream: endpoint.change_stream.clone(),
        };
        let last = reader.frontier().clone();
        Ok(Self {
            reader,
            assembler: Assembler::new(capacity)?,
            writer,
            order,
            source,
            batch: VecDeque::new(),
            boundary: None,
            released_through: None,
            last,
            schema_guard: None,
        })
    }
    pub fn source(&self) -> &SourceIdentity {
        &self.source
    }
    /// Attach the selected-table baseline collected by live source preflight.
    pub fn with_schema_guard(mut self, guard: super::schema::Guard) -> Self {
        self.schema_guard = Some(guard);
        self
    }
    pub fn buffered_records(&self) -> usize {
        self.assembler.buffered_records()
    }
    fn position(&self, frontier: Frontier) -> Position {
        Position::new(
            self.order.domain(),
            SpannerPosition::Frontier(frontier).encode(),
        )
    }
    fn transaction(
        &self,
        txn: super::assembly::Transaction,
    ) -> Result<AssembledTxn, client::Error> {
        let position = Position::new(
            self.order.domain(),
            SpannerPosition::Txn {
                commit_ts: txn.commit_ts,
                server_transaction_id: txn.server_transaction_id.as_bytes().to_vec(),
            }
            .encode(),
        );
        let id = TxnId(serde_json::to_vec(&(
            1u8,
            &txn.server_transaction_id,
            txn.records
                .iter()
                .map(|r| &r.record_sequence)
                .collect::<Vec<_>>(),
        ))?);
        let mut events = Vec::new();
        for record in txn.records {
            let row = |values: &serde_json::Map<String, serde_json::Value>| -> Result<Row, client::Error> {
                values.iter().map(|(name, input)| {
                    let col = record.column_types.iter().find(|c| &c.name == name).ok_or(ConversionError::InvalidValue)?;
                    Ok((name.clone(), convert_value(col, input)?))
                }).collect()
            };
            for modification in &record.mods {
                let key = row(&modification.keys)?;
                let image = |values| -> Result<Row, client::Error> {
                    let mut out = key.clone();
                    out.extend(row(values)?);
                    Ok(out)
                };
                events.push(Event {
                    source: self.source.clone(),
                    txn: id.clone(),
                    ordinal_in_txn: events.len() as u64,
                    position: position.clone(),
                    schema_version: 0,
                    kind: match record.mod_type {
                        ModType::Insert => EventKind::Insert,
                        ModType::Update => EventKind::Update,
                        ModType::Delete => EventKind::Delete,
                    },
                    table: Some(record.table_name.clone()),
                    key: key.clone(),
                    before: if record.mod_type == ModType::Insert {
                        None
                    } else {
                        Some(image(&modification.old_values)?)
                    },
                    after: if record.mod_type == ModType::Delete {
                        None
                    } else {
                        Some(image(&modification.new_values)?)
                    },
                    unchanged_cols: Default::default(),
                });
            }
        }
        Ok(AssembledTxn::new(
            self.source.clone(),
            id,
            position,
            Some(i64::try_from(txn.commit_ts.div_euclid(1000))?),
            events,
        )?)
    }
    fn durable(&self, boundary: Timestamp) -> crate::engine::Result<(Frontier, Position)> {
        let frontier = self.reader.frontier().clamped(boundary).map_err(recovery)?;
        let position = self.position(frontier.clone());
        if self
            .order
            .compare(self.position(self.last.clone()).bytes(), position.bytes())
            .map_err(recovery)?
            == std::cmp::Ordering::Greater
        {
            return Err(recovery("Spanner durable frontier regression"));
        }
        Ok((frontier, position))
    }
}
impl Capture for SpannerCapture {
    fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
        self.order.clone()
    }
    fn writer_checkpoint(&self) -> crate::engine::Result<Option<WriterCheckpoint>> {
        self.writer.checkpoint().map(Some).map_err(recovery)
    }
    /// JSON v1 binds the engine's source-bound frontier envelope to the writer's
    /// immutable manifest chain head: checkpoint version, next mseq, digest, epoch.
    fn buffered_marker(&self, batch: &Captured) -> crate::engine::Result<String> {
        let position = batch
            .durable_position
            .as_ref()
            .ok_or_else(|| recovery("missing frontier"))?;
        if position != &self.position(self.last.clone()) {
            return Err(recovery("stale Spanner marker"));
        }
        let checkpoint = self.writer.checkpoint().map_err(recovery)?;
        Ok(serde_json::json!({"version":1, "frontier":crate::engine::encode_buffered_position(&self.source, position)?,
            "writer":{"version":checkpoint.version,"next_mseq":checkpoint.next_mseq,"prev_digest":checkpoint.prev_digest,"last_epoch":checkpoint.last_epoch}}).to_string())
    }
    async fn step(&mut self) -> crate::engine::Result<Captured> {
        loop {
            if let Some(txn) = self.batch.front() {
                let commit_ts = batch_boundary(&self.batch)?.expect("nonempty batch");
                let (frontier, position) = self.durable(commit_ts)?;
                self.writer
                    .append(txn.clone(), Instant::now())
                    .await
                    .map_err(recovery)?;
                self.writer.seal().await.map_err(recovery)?;
                self.last = frontier;
                return Ok(Captured {
                    transaction: self.batch.pop_front(),
                    durable_position: Some(position),
                    ..Default::default()
                });
            }
            if let Some(boundary) = self.boundary.take()
                && boundary
                    >= self
                        .last
                        .resolved_ts()
                        .map_err(recovery)?
                        .unwrap_or(boundary)
            {
                let (frontier, position) = self.durable(boundary)?;
                if frontier != self.last {
                    self.last = frontier;
                    return Ok(Captured {
                        durable_position: Some(position),
                        ..Default::default()
                    });
                }
            }
            if let StreamEvent::Data(record, token) = self.reader.next().await.map_err(recovery)? {
                self.assembler.insert(token, record).map_err(recovery)?;
            }
            if let Some(boundary) = self.reader.certified_ts().map_err(recovery)? {
                if let Some(guard) = &mut self.schema_guard {
                    guard.check().await.map_err(recovery)?;
                }
                let Some(released) =
                    release_boundary(&mut self.assembler, &mut self.released_through, boundary)
                        .map_err(recovery)?
                else {
                    continue;
                };
                self.batch = released
                    .into_iter()
                    .map(|t| self.transaction(t))
                    .collect::<Result<_, _>>()
                    .map_err(recovery)?;
                self.boundary = Some(boundary);
            }
        }
    }
    async fn publish(
        &mut self,
        high: Position,
        rows: AssembledTxn,
    ) -> crate::engine::Result<DurableManifestRef> {
        if rows.source != self.source
            || high != rows.commit_position
            || high.order_domain() != self.order.domain()
        {
            return Err(recovery("snapshot publication binding mismatch"));
        }
        self.writer
            .append_chunk_rows(rows, Instant::now())
            .await
            .map(|(_, reference)| reference)
            .map_err(recovery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn child_certification_replay_preserves_release_high_water() {
        use super::super::records::{Record, decode_row};
        let rows: Vec<serde_json::Value> = serde_json::from_str(include_str!(
            "../../tests/fixtures/spanner/multi-partition-set.json"
        ))
        .unwrap();
        let mut records = rows.into_iter().map(|row| {
            let token = row["partition"].as_str().unwrap().as_bytes().to_vec();
            let Record::Data(record) = decode_row(&row["record"]).unwrap().remove(0) else {
                panic!("expected data")
            };
            (token, record)
        });
        let (token, record) = records.next().unwrap();
        let ts = timestamp(record.commit_timestamp);
        let mut assembler = Assembler::new(2).unwrap();
        let mut released = None;
        assert!(
            release_boundary(&mut assembler, &mut released, ts - 1)
                .unwrap()
                .unwrap()
                .is_empty()
        );
        assembler.insert(token, record).unwrap();
        assert!(
            release_boundary(&mut assembler, &mut released, ts - 2)
                .unwrap()
                .is_none()
        );
        assert_eq!(released, Some(ts - 1));
        assert!(release_boundary(&mut assembler, &mut released, ts).is_err());
        assert_eq!(
            released,
            Some(ts - 1),
            "failed releases must remain retryable"
        );
        for (token, record) in records {
            assembler.insert(token, record).unwrap();
        }
        let transactions = release_boundary(&mut assembler, &mut released, ts)
            .unwrap()
            .unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].records.len(), 2);
        assert_eq!(released, Some(ts));
    }
    #[test]
    fn typed_values_and_unsupported_null() {
        for (code, input) in [
            ("INT64", serde_json::json!("42")),
            ("STRING", serde_json::json!("")),
            ("BOOL", serde_json::json!(true)),
            ("FLOAT64", serde_json::json!("NaN")),
            ("BYTES", serde_json::json!("AAE=")),
            ("TIMESTAMP", serde_json::json!("2026-01-01T00:00:00Z")),
            ("NUMERIC", serde_json::json!("12.30")),
            ("JSON", serde_json::json!("{\"a\":1}")),
        ] {
            let col = ColumnType {
                name: "v".into(),
                r#type: serde_json::from_value(serde_json::json!({"code":code})).unwrap(),
                is_primary_key: false,
                ordinal_position: 1,
            };
            let value = convert_value(&col, &input).unwrap();
            assert_ne!(
                Value::from_canonical_bytes(value.bytes.as_ref().unwrap()).unwrap(),
                Value::Null
            );
            assert_eq!(
                convert_value(&col, &serde_json::Value::Null).unwrap().bytes,
                None
            );
        }
        let col: ColumnType = serde_json::from_value(serde_json::json!({"name":"v","type":{"code":"ARRAY"},"is_primary_key":false,"ordinal_position":1})).unwrap();
        assert!(matches!(
            convert_value(&col, &serde_json::Value::Null)
                .unwrap_err()
                .downcast_ref::<ConversionError>(),
            Some(ConversionError::Unsupported(_))
        ));
    }
    #[test]
    fn batch_emits_commit_clamps_before_resolved_progress() {
        let source = SourceIdentity {
            engine: "spanner".into(),
            system_id: "p/i".into(),
            lineage: "0".into(),
            db_name: "d".into(),
            stream: "s".into(),
        };
        let mut batch: VecDeque<_> = [(10, "a"), (10, "b"), (20, "c")]
            .into_iter()
            .map(|(ts, id)| {
                AssembledTxn::new(
                    source.clone(),
                    TxnId(id.as_bytes().to_vec()),
                    Position::new(
                        "test",
                        SpannerPosition::Txn {
                            commit_ts: ts,
                            server_transaction_id: id.as_bytes().to_vec(),
                        }
                        .encode(),
                    ),
                    None,
                    vec![],
                )
                .unwrap()
            })
            .collect();
        let mut raw = Frontier::new(0);
        raw.mark_reading(b"").unwrap();
        raw.advance(b"", 30).unwrap();
        for expected in [10, 10, 20] {
            let boundary = batch_boundary(&batch).unwrap().unwrap();
            assert_eq!(boundary, expected);
            assert_eq!(
                raw.clamped(boundary).unwrap().resolved_ts().unwrap(),
                Some(expected)
            );
            batch.pop_front();
        }
        assert_eq!(batch_boundary(&batch).unwrap(), None);
        assert_eq!(raw.clamped(30).unwrap().resolved_ts().unwrap(), Some(30));
    }
}
