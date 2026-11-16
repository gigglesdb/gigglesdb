use super::{ChunkError, ChunkId, ChunkRange, Key, MESSAGE_PREFIX, Result, TableKey, key_bytes};
use crate::{
    buffer::format::SegmentRecord,
    event::{AssembledTxn, Event, EventKind, Position, PositionOrder, Row, SourceIdentity, TxnId},
    mapping::Value,
    pg::pgoutput::Message,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Client;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatermarkKind {
    Low,
    High,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Watermark {
    pub wm: WatermarkKind,
    pub chunk: ChunkId,
    pub attempt: u64,
}
impl Watermark {
    pub const EMIT_SQL: &'static str = "SELECT pg_logical_emit_message(true, $1::text, $2::text)";
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.chunk.validate()?;
        Ok(serde_json::to_vec(self)?)
    }
    /// Reject foreign prefixes, nontransactional markers, invalid identities,
    /// duplicate/unknown fields and all malformed JSON. Other skedaddle control
    /// messages must be routed by the capture loop before calling this codec.
    pub fn parse(message: &Message<'_>) -> Result<Self> {
        let Message::LogicalMessage {
            transactional: true,
            prefix: MESSAGE_PREFIX,
            content,
            ..
        } = message
        else {
            return Err(ChunkError::Invalid("foreign or nontransactional watermark"));
        };
        if content.len() > 64 * 1024 {
            return Err(ChunkError::Invalid("watermark too large"));
        }
        let value: Self = serde_json::from_slice(content)?;
        value.chunk.validate()?;
        Ok(value)
    }
    /// Use a dedicated idle admin connection: this statement commits in its own
    /// implicit transaction. Never run inside the chunk's read transaction.
    pub async fn emit(&self, admin: &Client) -> Result<()> {
        let payload =
            String::from_utf8(self.encode()?).map_err(|_| ChunkError::Invalid("watermark UTF8"))?;
        admin
            .query_one(Self::EMIT_SQL, &[&MESSAGE_PREFIX, &payload])
            .await?;
        Ok(())
    }
}

/// Coordinator integration seam. Journal Started before any side effects and
/// Done only AFTER ChunkRows is durable. A mere observed high is not completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ChunkTransition {
    ChunkStarted {
        chunk: ChunkRange,
        attempt: u64,
    },
    ChunkDone {
        chunk: ChunkRange,
        attempt: u64,
        high_lsn: u64,
    },
}

/// Started without Done (including a second Started after a prior Done) is
/// redone. Unknown/stale plans and mismatched bounds fail instead of skipping.
pub fn unfinished_chunks(
    plan: &[ChunkRange],
    journal: &[ChunkTransition],
) -> Result<Vec<ChunkRange>> {
    let mut states = BTreeMap::new();
    for chunk in plan {
        chunk.id.validate()?;
        let id = serde_json::to_vec(&chunk.id)?;
        if states.insert(id, (chunk, None)).is_some() {
            return Err(ChunkError::Invalid("duplicate chunk in plan"));
        }
    }
    for transition in journal {
        let chunk = match transition {
            ChunkTransition::ChunkStarted { chunk, .. }
            | ChunkTransition::ChunkDone { chunk, .. } => chunk,
        };
        let (expected, state) = states
            .get_mut(&serde_json::to_vec(&chunk.id)?)
            .ok_or(ChunkError::Invalid("journal plan mismatch"))?;
        if *expected != chunk {
            return Err(ChunkError::Invalid("journal bounds mismatch"));
        }
        match transition {
            ChunkTransition::ChunkStarted { attempt, .. } => {
                if state.is_some_and(|(previous, _)| *attempt <= previous) {
                    return Err(ChunkError::Invalid("chunk attempt reused/out of order"));
                }
                *state = Some((*attempt, false));
            }
            ChunkTransition::ChunkDone {
                attempt, high_lsn, ..
            } => {
                if !state.is_some_and(|(started, _)| started == *attempt) || *high_lsn == 0 {
                    return Err(ChunkError::Invalid("Done without matching Started/high"));
                }
                *state = Some((*attempt, true));
            }
        }
    }
    let mut unfinished = Vec::new();
    for chunk in plan {
        let id = serde_json::to_vec(&chunk.id)?;
        if !states[&id].1.is_some_and(|(_, done)| done) {
            unfinished.push(chunk.clone());
        }
    }
    Ok(unfinished)
}

/// Canonical row-key seam. Implementations validate and return the key columns
/// plus their collision-free identity; the kernel never orders source values.
pub trait KeyIdentity: Send {
    fn key(&self, row: &Row) -> Result<(Vec<u8>, Row)>;
}
impl KeyIdentity for TableKey {
    fn key(&self, row: &Row) -> Result<(Vec<u8>, Row)> {
        let bytes = key_bytes(&row_key(self, row)?)?;
        let key = self
            .columns
            .iter()
            .map(|c| (c.name.clone(), row[&c.name].clone()))
            .collect();
        Ok((bytes, key))
    }
}

// Avoid ordering PG values: rows were already selected by PostgreSQL's range
// predicate. Removing all touched identities among these rows is exactly the
// intersection with [lo,hi), including key changes into/out of the range.
pub struct Window {
    table: String,
    identity: Box<dyn KeyIdentity>,
    chunk: ChunkRange,
    attempt: u64,
    rows: Vec<(Vec<u8>, Row)>,
    keys: BTreeSet<Vec<u8>>,
    touched: BTreeSet<Vec<u8>>,
    low: Option<Position>,
    high: Option<Position>,
    failed: bool,
}
impl Window {
    pub fn new(table: TableKey, chunk: ChunkRange, rows: Vec<Row>, attempt: u64) -> Result<Self> {
        table.read_query(&chunk)?;
        Self::with_keys(table.table.clone(), Box::new(table), chunk, rows, attempt)
    }
    pub fn with_keys(
        table: String,
        identity: Box<dyn KeyIdentity>,
        chunk: ChunkRange,
        rows: Vec<Row>,
        attempt: u64,
    ) -> Result<Self> {
        chunk.id.validate()?;
        if table != chunk.id.table {
            return Err(ChunkError::Invalid("chunk table identity"));
        }
        let mut unique = BTreeSet::new();
        let rows = rows
            .into_iter()
            .map(|row| {
                let (bytes, _) = identity.key(&row)?;
                if !unique.insert(bytes.clone()) {
                    return Err(ChunkError::Invalid("duplicate snapshot key"));
                }
                Ok((bytes, row))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            table,
            identity,
            chunk,
            attempt,
            rows,
            keys: unique,
            touched: BTreeSet::new(),
            low: None,
            high: None,
            failed: false,
        })
    }

    /// PG wrapper: parsing and logical-message versus COMMIT checks stay here.
    pub fn watermark(
        &mut self,
        message: &Message<'_>,
        commit_lsn: u64,
        order: &dyn PositionOrder,
    ) -> Result<()> {
        let result = (|| {
            let wm = Watermark::parse(message)?;
            if wm.chunk != self.chunk.id || wm.attempt != self.attempt {
                return Ok(());
            }
            let Message::LogicalMessage { lsn, .. } = message else {
                unreachable!()
            };
            if *lsn == 0 || commit_lsn < *lsn {
                return Err(ChunkError::Invalid("watermark commit boundary"));
            }
            self.marker(
                &wm,
                Position::new(order.domain(), commit_lsn.to_be_bytes()),
                order,
            )
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    /// Exact native COMMIT boundary, delivered after its transaction's events.
    pub fn marker(
        &mut self,
        wm: &Watermark,
        commit: Position,
        order: &dyn PositionOrder,
    ) -> Result<()> {
        let result = self.marker_inner(wm, commit, order);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn marker_inner(
        &mut self,
        wm: &Watermark,
        commit: Position,
        order: &dyn PositionOrder,
    ) -> Result<()> {
        if self.failed {
            return Err(ChunkError::Invalid("window poisoned; replay required"));
        }
        wm.chunk.validate()?;
        commit.compare(&commit, order)?;
        if wm.chunk != self.chunk.id || wm.attempt != self.attempt {
            return Ok(());
        }
        match wm.wm {
            WatermarkKind::Low if self.low.is_none() && self.high.is_none() => {
                self.low = Some(commit)
            }
            WatermarkKind::High if self.high.is_none() => {
                let low = self
                    .low
                    .as_ref()
                    .ok_or(ChunkError::Invalid("watermark order/replay mismatch"))?;
                if commit.compare(low, order)? != std::cmp::Ordering::Greater {
                    return Err(ChunkError::Invalid("watermark order/replay mismatch"));
                }
                self.high = Some(commit);
            }
            _ => return Err(ChunkError::Invalid("watermark order/replay mismatch")),
        }
        Ok(())
    }

    /// Caller supplies mapped COMMITTED events, including old Event.key and
    /// new after-key columns on key changes. No comparisons of text/UUID keys.
    /// Events before L and after H do not participate in exclusion.
    pub fn event(&mut self, event: &Event) -> Result<()> {
        let result = self.event_inner(event);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn event_inner(&mut self, event: &Event) -> Result<()> {
        if self.failed {
            return Err(ChunkError::Invalid("window poisoned; replay required"));
        }
        if self.low.is_none() || self.high.is_some() {
            return Ok(());
        }
        if let EventKind::Truncate { relations, .. } = &event.kind {
            if relations.contains(&self.table) {
                self.rows.clear();
            }
            return Ok(());
        }
        if event.table.as_deref() != Some(&self.table) {
            return Ok(());
        }
        if !matches!(
            event.kind,
            EventKind::Insert | EventKind::Update | EventKind::Delete
        ) {
            return Ok(());
        }
        self.touch(&event.key)?;
        if let Some(before) = &event.before {
            let mut old = event.key.clone();
            old.extend(before.clone());
            self.touch(&old)?;
        }
        if matches!(event.kind, EventKind::Insert | EventKind::Update) {
            let after = event
                .after
                .as_ref()
                .ok_or(ChunkError::Invalid("missing insert/update image"))?;
            let mut new = event.key.clone();
            new.extend(after.clone());
            self.touch(&new)?;
        }
        Ok(())
    }
    fn touch(&mut self, row: &Row) -> Result<()> {
        let (bytes, _) = self.identity.key(row)?;
        // Bound memory by the chunk, regardless of stream-window activity.
        if self.keys.contains(&bytes) {
            self.touched.insert(bytes);
        }
        Ok(())
    }

    /// One atomic buffer record containing zero or more row events at H.
    /// Source must be the same lineage as the capture loop. Call only after
    /// the transaction carrying H has committed in the decoded stream.
    pub fn finish(
        self,
        source: SourceIdentity,
        order_domain: &str,
        schema_version: u64,
    ) -> Result<SegmentRecord> {
        if self.failed {
            return Err(ChunkError::Invalid("window poisoned; replay required"));
        }
        let high = self
            .high
            .ok_or(ChunkError::Invalid("high watermark not observed"))?;
        if order_domain.is_empty() || high.order_domain() != order_domain {
            return Err(ChunkError::Invalid("position domain"));
        }
        let position = high;
        let mut identity = Sha256::new();
        identity.update(b"skedaddle-chunk-record-v1\0");
        identity.update(serde_json::to_vec(&self.chunk.id)?);
        identity.update(self.attempt.to_be_bytes());
        identity.update(position.bytes());
        let txn = TxnId(identity.finalize().to_vec());
        let mut events = Vec::new();
        for (bytes, row) in self.rows {
            if self.touched.contains(&bytes) {
                continue;
            }
            let (_, key) = self.identity.key(&row)?;
            events.push(Event {
                source: source.clone(),
                txn: txn.clone(),
                ordinal_in_txn: events.len() as u64,
                position: position.clone(),
                schema_version,
                kind: EventKind::ChunkRows,
                table: Some(self.table.clone()),
                key,
                before: None,
                after: Some(row),
                unchanged_cols: BTreeSet::new(),
            });
        }
        Ok(SegmentRecord::ChunkRows(AssembledTxn::new(
            source, txn, position, None, events,
        )?))
    }
}

fn row_key(table: &TableKey, row: &Row) -> Result<Key> {
    let key = table
        .columns
        .iter()
        .map(|c| {
            let opaque = row
                .get(&c.name)
                .ok_or(ChunkError::Invalid("missing key column"))?;
            if opaque.type_id.is_empty() {
                return Err(ChunkError::Invalid("missing key type identity"));
            }
            let bytes = opaque
                .bytes
                .as_deref()
                .ok_or(ChunkError::Invalid("NULL key"))?;
            Ok(Value::from_canonical_bytes(bytes)?)
        })
        .collect::<Result<Vec<_>>>()?;
    table.validate_key(&key)?;
    Ok(key)
}
