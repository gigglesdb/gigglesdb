use crate::error::{Error, Result};
use protobuf::Message;
use raft::{GetEntriesContext, RaftState, Storage, StorageError, eraftpb::*};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;

const ENTRIES: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
const STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("state");
type StateTable = redb::ReadOnlyTable<&'static str, &'static [u8]>;
type EntriesTable = redb::ReadOnlyTable<u64, &'static [u8]>;

pub struct LogStore {
    db: Database,
    identity: ([u8; 16], u64),
}
fn decode<T: Message + Default>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<T> {
    Ok(match table.get(key)? {
        Some(v) => T::parse_from_bytes(v.value())?,
        None => T::default(),
    })
}
fn raft_error(e: Error) -> raft::Error {
    match e {
        Error::Raft(e) => e,
        e => StorageError::Other(Box::new(e)).into(),
    }
}
impl LogStore {
    pub fn open(dir: &Path, cluster: [u8; 16], node_id: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("raft.redb"))?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            tx.open_table(ENTRIES)?;
            let mut state = tx.open_table(STATE)?;
            // Identity is 16 cluster bytes followed by a little-endian u64 node id.
            let bytes = [cluster.as_slice(), &node_id.to_le_bytes()].concat();
            let old = state.get("identity")?.map(|v| v.value().to_vec());
            match old {
                Some(old) if old != bytes => {
                    return Err(Error::Identity("cluster or node differs".into()));
                }
                None => {
                    state.insert("identity", bytes.as_slice())?;
                }
                _ => {}
            }
        }
        tx.commit()?;
        Ok(Self {
            db,
            identity: (cluster, node_id),
        })
    }
    fn view(&self) -> Result<(EntriesTable, StateTable)> {
        let tx = self.db.begin_read()?;
        Ok((tx.open_table(ENTRIES)?, tx.open_table(STATE)?))
    }
    pub fn identity(&self) -> ([u8; 16], u64) {
        self.identity
    }
    pub fn persist(&self, entries: &[Entry], hard_state: Option<&HardState>) -> Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut log = tx.open_table(ENTRIES)?;
            let mut state = tx.open_table(STATE)?;
            if let Some(first) = entries.first() {
                let meta: SnapshotMetadata = decode(&state, "snapshot_meta")?;
                let last = log.last()?.map_or(meta.index, |(k, _)| k.value());
                if first.index <= meta.index
                    || first.index > last.saturating_add(1)
                    || entries
                        .windows(2)
                        .any(|w| w[0].index.checked_add(1) != Some(w[1].index))
                {
                    return Err(Error::Corrupt("non-contiguous or compacted append".into()));
                }
                log.retain_in(first.index.., |_, _| false)?;
                for entry in entries {
                    log.insert(entry.index, entry.write_to_bytes()?.as_slice())?;
                }
            }
            if let Some(hs) = hard_state {
                state.insert("hard_state", hs.write_to_bytes()?.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn compact(&self, to: u64) -> Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut log = tx.open_table(ENTRIES)?;
            let mut state = tx.open_table(STATE)?;
            let mut meta: SnapshotMetadata = decode(&state, "snapshot_meta")?;
            if to <= meta.index {
                return Ok(());
            }
            let bytes = log
                .get(to)?
                .ok_or_else(|| Error::Corrupt("compaction beyond log".into()))?;
            meta.term = Entry::parse_from_bytes(bytes.value())?.term;
            meta.index = to;
            drop(bytes);
            log.retain_in(..=to, |_, _| false)?;
            state.insert("snapshot_meta", meta.write_to_bytes()?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn install_snapshot_meta(
        &self,
        index: u64,
        term: u64,
        conf_state: &ConfState,
    ) -> Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut log = tx.open_table(ENTRIES)?;
            let mut state = tx.open_table(STATE)?;
            let mut meta: SnapshotMetadata = decode(&state, "snapshot_meta")?;
            if index <= meta.index {
                return Err(Error::Corrupt("stale snapshot".into()));
            }
            let matches = log
                .get(index)?
                .map(|bytes| Entry::parse_from_bytes(bytes.value()))
                .transpose()?
                .is_some_and(|entry| entry.term == term);
            if matches {
                log.retain_in(..=index, |_, _| false)?;
            } else {
                log.retain(|_, _| false)?;
            }
            meta.index = index;
            meta.term = term;
            let mut hs: HardState = decode(&state, "hard_state")?;
            hs.commit = hs.commit.max(index);
            state.insert("snapshot_meta", meta.write_to_bytes()?.as_slice())?;
            state.insert("conf_state", conf_state.write_to_bytes()?.as_slice())?;
            state.insert("hard_state", hs.write_to_bytes()?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn snapshot_meta(&self) -> Result<(u64, u64)> {
        let meta: SnapshotMetadata = decode(&self.view()?.1, "snapshot_meta")?;
        Ok((meta.index, meta.term))
    }
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        self.persist(entries, None)
    }
    pub fn set_hard_state(&self, hs: &HardState) -> Result<()> {
        self.persist(&[], Some(hs))
    }
    pub fn hard_state(&self) -> Result<HardState> {
        decode(&self.view()?.1, "hard_state")
    }
    pub fn conf_state(&self) -> Result<ConfState> {
        decode(&self.view()?.1, "conf_state")
    }
    pub fn set_conf_state(&self, cs: &ConfState) -> Result<()> {
        self.write_conf(cs, false)
    }
    pub fn initialize_voters(&self, voters: &[u64]) -> Result<()> {
        let mut cs = ConfState::default();
        cs.set_voters(voters.to_vec());
        self.write_conf(&cs, true)
    }
    fn write_conf(&self, cs: &ConfState, initialize: bool) -> Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut state = tx.open_table(STATE)?;
            if initialize {
                let old: ConfState = decode(&state, "conf_state")?;
                if old != *cs
                    && (old != ConfState::default()
                        || tx.open_table(ENTRIES)?.last()?.is_some()
                        || decode::<SnapshotMetadata>(&state, "snapshot_meta")?.index != 0)
                {
                    return Err(Error::Corrupt("cannot initialize voters".into()));
                }
            }
            state.insert("conf_state", cs.write_to_bytes()?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
}
impl Storage for LogStore {
    fn initial_state(&self) -> raft::Result<RaftState> {
        (|| {
            let (_, s) = self.view()?;
            Ok(RaftState::new(
                decode(&s, "hard_state")?,
                decode(&s, "conf_state")?,
            ))
        })()
        .map_err(raft_error)
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        (|| {
            let (log, s) = self.view()?;
            let meta: SnapshotMetadata = decode(&s, "snapshot_meta")?;
            if low <= meta.index {
                return Err(Error::Raft(StorageError::Compacted.into()));
            }
            let last = log.last()?.map_or(meta.index, |(k, _)| k.value());
            if low > high || high > last.saturating_add(1) {
                return Err(Error::Raft(StorageError::Unavailable.into()));
            }
            let mut out = Vec::new();
            let max_size = max_size.into().unwrap_or(u64::MAX);
            let mut size = 0u64;
            for i in low..high {
                let bytes = log
                    .get(i)?
                    .ok_or_else(|| Error::Corrupt("missing entry".into()))?;
                let entry = Entry::parse_from_bytes(bytes.value())?;
                let next_size = size.saturating_add(u64::from(entry.compute_size()));
                if !out.is_empty() && next_size > max_size {
                    break;
                }
                out.push(entry);
                size = next_size;
                if size >= max_size {
                    break;
                }
            }
            Ok(out)
        })()
        .map_err(raft_error)
    }
    fn term(&self, i: u64) -> raft::Result<u64> {
        (|| {
            let (log, s) = self.view()?;
            let meta: SnapshotMetadata = decode(&s, "snapshot_meta")?;
            if i == meta.index {
                return Ok(meta.term);
            }
            if i < meta.index {
                return Err(Error::Raft(StorageError::Compacted.into()));
            }
            let bytes = log
                .get(i)?
                .ok_or(Error::Raft(StorageError::Unavailable.into()))?;
            Ok(Entry::parse_from_bytes(bytes.value())?.term)
        })()
        .map_err(raft_error)
    }
    fn first_index(&self) -> raft::Result<u64> {
        (|| Ok(decode::<SnapshotMetadata>(&self.view()?.1, "snapshot_meta")?.index + 1))()
            .map_err(raft_error)
    }
    fn last_index(&self) -> raft::Result<u64> {
        (|| {
            let (log, s) = self.view()?;
            Ok(log
                .last()?
                .map(|(k, _)| k.value())
                .unwrap_or(decode::<SnapshotMetadata>(&s, "snapshot_meta")?.index))
        })()
        .map_err(raft_error)
    }
    fn snapshot(&self, _: u64, _: u64) -> raft::Result<Snapshot> {
        Err(StorageError::SnapshotTemporarilyUnavailable.into())
    }
}
