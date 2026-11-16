//! Bounded MySQL transaction assembly. TxnId is UTF-8 `uuid:sequence` in GTID
//! mode, or `file_sequence:end_offset` in file mode. Event positions equal the
//! commit boundary, ordinals start at zero, schema_version starts at one, and
//! AssembledTxn computes the canonical digest, following the PG conventions.
//!
//! Supply descriptors for selected data tables AND the _skedaddle control tables
//! being captured. An unknown control table is an error, never silently skipped.
//! GTID ordering uses source.system_id as its stable source/cluster identity.
use super::{
    binlog::{MysqlEvent as M, PositionedEvent, QueryKind, RowImage, RowKind},
    client::{MysqlError, Result},
    position::{MysqlOrder, MysqlPosition},
    schema::TableDescriptor,
    value::to_opaque,
};
use crate::event::{
    AssembledTxn, Event, EventKind, Position, PositionOrder, Row, SourceIdentity, TxnId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlRow {
    pub table: String,
    pub kind: EventKind,
    pub before: Option<Row>,
    pub after: Option<Row>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssemblerOutput {
    Txn {
        txn: AssembledTxn,
        control_rows: Vec<ControlRow>,
    },
    Progress(MysqlPosition),
    Ddl {
        schema: Vec<u8>,
        sql: Vec<u8>,
        position: MysqlPosition,
    },
}
pub struct Assembler {
    source: SourceIdentity,
    descriptors: BTreeMap<(String, String), Arc<TableDescriptor>>,
    maps: BTreeMap<u64, Option<Arc<TableDescriptor>>>,
    position: MysqlPosition,
    order: MysqlOrder,
    gtid_mode: bool,
    gtid: Option<String>,
    commit_time: Option<i64>,
    open: bool,
    begun: bool,
    rows: Vec<(Arc<TableDescriptor>, ControlRow)>,
    bytes: usize,
    cap: usize,
    failed: bool,
}
impl Assembler {
    pub fn new(
        source: SourceIdentity,
        descriptors: Vec<TableDescriptor>,
        start: MysqlPosition,
        gtid_mode: bool,
        max_row_bytes: usize,
    ) -> Result<Self> {
        if max_row_bytes == 0 || matches!(start, MysqlPosition::Gtid { .. }) != gtid_mode {
            return Err(MysqlError::Assembly("mode or limit"));
        }
        start.encode()?;
        let order = if gtid_mode {
            MysqlOrder::gtid(&source.system_id)?
        } else {
            let MysqlPosition::File { server_uuid, .. } = &start else {
                return Err(MysqlError::Assembly("file mode"));
            };
            MysqlOrder::file(server_uuid)?
        };
        let mut selected = BTreeMap::new();
        for d in descriptors {
            if d.primary_key.is_empty() {
                return Err(MysqlError::MissingPrimaryKey {
                    schema: d.schema,
                    table: d.table,
                });
            }
            let mut names = BTreeSet::new();
            if d.columns.is_empty()
                || d.columns
                    .iter()
                    .enumerate()
                    .any(|(i, c)| c.ordinal != i as u64 + 1 || !names.insert(&c.name))
                || d.primary_key.iter().any(|k| !names.contains(k))
            {
                return Err(MysqlError::Assembly("invalid descriptor"));
            }
            if selected
                .insert((d.schema.clone(), d.table.clone()), Arc::new(d))
                .is_some()
            {
                return Err(MysqlError::Assembly("duplicate descriptor"));
            }
        }
        Ok(Self {
            source,
            descriptors: selected,
            maps: BTreeMap::new(),
            position: start,
            order,
            gtid_mode,
            gtid: None,
            commit_time: None,
            open: false,
            begun: false,
            rows: Vec::new(),
            bytes: 0,
            cap: max_row_bytes,
            failed: false,
        })
    }
    /// An error poisons this assembler: reconnect/replay with a new instance.
    pub fn push(&mut self, item: PositionedEvent) -> Result<Vec<AssemblerOutput>> {
        if self.failed {
            return Err(MysqlError::Assembly("assembler poisoned"));
        }
        let mut outputs = Vec::new();
        let result = self.consume(item.event, &item.file, item.log_pos, &mut outputs);
        if result.is_err() {
            self.failed = true;
            self.rows.clear();
            self.maps.clear();
            self.bytes = 0;
        }
        result.map(|()| outputs)
    }
    fn consume(
        &mut self,
        event: M,
        file: &str,
        end: u64,
        outputs: &mut Vec<AssemblerOutput>,
    ) -> Result<()> {
        match event {
            M::Gtid {
                sid,
                sequence,
                tag,
                commit_time,
            } => {
                if tag.is_some() {
                    return Err(MysqlError::Assembly("tagged GTID"));
                }
                self.start()?;
                self.commit_time = commit_time;
                self.gtid = Some(format!("{sid}:{sequence}"));
                // Validate even when operating in file mode.
                let mut one = super::GtidSet::default();
                one.add(self.gtid.as_deref().ok_or(MysqlError::Assembly("GTID"))?)?;
            }
            M::AnonymousGtid { commit_time, .. } => {
                if self.gtid_mode {
                    return Err(MysqlError::Assembly("anonymous GTID in GTID mode"));
                }
                self.start()?;
                self.commit_time = commit_time;
            }
            M::Query { schema, sql, kind } => {
                if sql
                    .split(|b| b.is_ascii_whitespace())
                    .find(|s| !s.is_empty())
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"XA"))
                {
                    return Err(MysqlError::Assembly("XA unsupported"));
                }
                match kind {
                    QueryKind::Begin => {
                        if self.begun {
                            return Err(MysqlError::Assembly("nested BEGIN"));
                        }
                        if !self.open {
                            self.start()?;
                        }
                        self.begun = true;
                    }
                    QueryKind::Commit => self.commit(file, end, outputs)?,
                    QueryKind::Ddl if !self.begun => {
                        let position = self.boundary(file, end)?;
                        self.reset();
                        outputs.push(AssemblerOutput::Ddl {
                            schema,
                            sql,
                            position,
                        });
                    }
                    _ => return Err(MysqlError::Assembly("unsupported transactional query")),
                }
            }
            M::TableMap(t) => {
                if t.schema == b"_skedaddle"
                    && matches!(t.table.as_slice(), b"apply_receipts" | b"apply_progress")
                {
                    let d = super::control::apply_descriptors()
                        .into_iter()
                        .find(|d| d.table.as_bytes() == t.table)
                        .expect("owned control descriptor");
                    super::schema::compare_table_map(&d, &t)
                        .map_err(|_| MysqlError::Control("apply control table schema mismatch"))?;
                }
                let schema = String::from_utf8(t.schema)
                    .map_err(|_| MysqlError::Assembly("table schema UTF-8"))?;
                let table = String::from_utf8(t.table)
                    .map_err(|_| MysqlError::Assembly("table name UTF-8"))?;
                let descriptor = self.descriptors.get(&(schema.clone(), table)).cloned();
                if schema == "_skedaddle" && descriptor.is_none() {
                    return Err(MysqlError::Assembly("control descriptor required"));
                }
                if descriptor
                    .as_ref()
                    .is_some_and(|d| d.columns.len() != t.columns.len())
                {
                    return Err(MysqlError::Assembly("table map column count"));
                }
                self.maps.insert(t.id, descriptor);
            }
            M::Rows {
                table_id,
                kind,
                images,
            } => {
                if !self.open || !self.begun {
                    return Err(MysqlError::Assembly("ROWS outside transaction"));
                }
                let descriptor = self
                    .maps
                    .get(&table_id)
                    .ok_or(MysqlError::Assembly("ROWS without table map"))?
                    .clone();
                for (before, after) in images {
                    if (before.is_some(), after.is_some())
                        != match kind {
                            RowKind::Write => (false, true),
                            RowKind::Update => (true, true),
                            RowKind::Delete => (true, false),
                        }
                    {
                        return Err(MysqlError::Assembly("row image kind"));
                    }
                    if let Some(d) = &descriptor {
                        let before = before.as_ref().map(|r| image(d, r)).transpose()?;
                        let after = after.as_ref().map(|r| image(d, r)).transpose()?;
                        let size = before
                            .iter()
                            .chain(after.iter())
                            .flat_map(|r| r.iter())
                            .try_fold(64usize, |n, (k, v)| {
                                n.checked_add(
                                    k.len()
                                        + v.type_id.len()
                                        + v.bytes.as_ref().map_or(0, Vec::len)
                                        + 16,
                                )
                                .ok_or(MysqlError::TransactionTooLarge { limit: self.cap })
                            })?;
                        self.bytes = self
                            .bytes
                            .checked_add(size)
                            .ok_or(MysqlError::TransactionTooLarge { limit: self.cap })?;
                        if self.bytes > self.cap {
                            return Err(MysqlError::TransactionTooLarge { limit: self.cap });
                        }
                        let kind = match kind {
                            RowKind::Write => EventKind::Insert,
                            RowKind::Update => EventKind::Update,
                            RowKind::Delete => EventKind::Delete,
                        };
                        self.rows.push((
                            d.clone(),
                            ControlRow {
                                table: format!("{}.{}", d.schema, d.table),
                                kind,
                                before,
                                after,
                            },
                        ));
                    }
                }
            }
            M::Xid(_) => self.commit(file, end, outputs)?,
            M::TransactionPayload(events) => {
                for event in events {
                    self.consume(event, file, end, outputs)?;
                }
            }
            M::Rotate { .. } => {
                if self.open {
                    return Err(MysqlError::Assembly("ROTATE inside transaction"));
                }
                self.maps.clear();
            }
            M::FormatDescription { .. } | M::PreviousGtids(_) | M::Heartbeat | M::Skipped(_) => {}
        }
        Ok(())
    }
    fn start(&mut self) -> Result<()> {
        if self.open {
            return Err(MysqlError::Assembly("nested transaction"));
        }
        self.open = true;
        Ok(())
    }
    fn boundary(&mut self, file: &str, end: u64) -> Result<MysqlPosition> {
        if end < 4 || end > u64::from(u32::MAX) {
            return Err(MysqlError::Assembly("commit offset"));
        }
        match &mut self.position {
            MysqlPosition::Gtid {
                set,
                file: current,
                offset,
                ..
            } => {
                set.add(
                    self.gtid
                        .as_deref()
                        .ok_or(MysqlError::Assembly("missing GTID"))?,
                )?;
                *current = file.into();
                *offset = end;
            }
            MysqlPosition::File {
                file_seq, offset, ..
            } => {
                *file_seq = file
                    .rsplit_once('.')
                    .and_then(|(_, n)| n.parse().ok())
                    .ok_or(MysqlError::Assembly("binlog file sequence"))?;
                *offset = end;
            }
        }
        Ok(self.position.clone())
    }
    fn commit(&mut self, file: &str, end: u64, outputs: &mut Vec<AssemblerOutput>) -> Result<()> {
        if !self.open || !self.begun {
            return Err(MysqlError::Assembly("commit outside transaction"));
        }
        let position = self.boundary(file, end)?;
        if self.rows.is_empty() {
            outputs.push(AssemblerOutput::Progress(position));
        } else {
            let id = TxnId(
                if self.gtid_mode {
                    self.gtid
                        .clone()
                        .ok_or(MysqlError::Assembly("missing GTID"))?
                } else {
                    let MysqlPosition::File {
                        file_seq, offset, ..
                    } = &position
                    else {
                        return Err(MysqlError::Assembly("file mode"));
                    };
                    format!("{file_seq}:{offset}")
                }
                .into_bytes(),
            );
            let commit_position = Position::new(self.order.domain(), position.encode()?);
            let mut events = Vec::new();
            let mut control_rows = Vec::new();
            for (d, row) in std::mem::take(&mut self.rows) {
                if d.schema == "_skedaddle" {
                    control_rows.push(row);
                    continue;
                }
                // UPDATE/DELETE identify the old row; INSERT identifies the new.
                let key_image = row
                    .before
                    .as_ref()
                    .or(row.after.as_ref())
                    .ok_or(MysqlError::Assembly("empty image"))?;
                let key = d
                    .primary_key
                    .iter()
                    .map(|k| {
                        Ok((
                            k.clone(),
                            key_image
                                .get(k)
                                .ok_or(MysqlError::Assembly("primary key column"))?
                                .clone(),
                        ))
                    })
                    .collect::<Result<Row>>()?;
                events.push(Event {
                    source: self.source.clone(),
                    txn: id.clone(),
                    ordinal_in_txn: events.len() as u64,
                    position: commit_position.clone(),
                    schema_version: 1,
                    kind: row.kind,
                    table: Some(row.table),
                    key,
                    before: row.before,
                    after: row.after,
                    unchanged_cols: BTreeSet::new(),
                });
            }
            let txn = AssembledTxn::new(
                self.source.clone(),
                id,
                commit_position,
                self.commit_time,
                events,
            )?;
            outputs.push(AssemblerOutput::Txn { txn, control_rows });
        }
        self.reset();
        Ok(())
    }
    fn reset(&mut self) {
        self.open = false;
        self.begun = false;
        self.gtid = None;
        self.commit_time = None;
        self.bytes = 0;
        self.rows.clear();
        self.maps.clear();
    }
    pub fn finish(&self) -> Result<()> {
        if self.failed || self.open {
            Err(MysqlError::Assembly(
                "incomplete or failed transaction stream",
            ))
        } else {
            Ok(())
        }
    }
}
fn image(d: &TableDescriptor, row: &RowImage) -> Result<Row> {
    if d.columns.len() != row.len() {
        return Err(MysqlError::Assembly("FULL row image required"));
    }
    d.columns
        .iter()
        .zip(row)
        .map(|(c, v)| Ok((c.name.clone(), to_opaque(c, v)?)))
        .collect()
}
#[cfg(test)]
mod tests;
