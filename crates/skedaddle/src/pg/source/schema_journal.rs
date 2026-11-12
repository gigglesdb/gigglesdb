//! Independent, journal-authenticated classifier checkpoints. The journal head
//! has a current object, one prepared successor, and a retryable delete intent.
//! Creation intent precedes object creation, so cancellation cannot lose its key.

use super::*;
use crate::{buffer::writer::WriterCheckpoint, coordinator::journal::Journal, engine::Runtime};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{io::Write, sync::Arc, time::Duration};

pub const MAX_SCHEMA_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
const HEAD_BYTES: usize = 16 * 1024;

/// D11b supplies the same authoritative journal/runtime and the SAME object
/// store used by that journal. `stream` identifies the capture's exclusive writer
/// (including its buffer prefix/migration/stream configuration).
pub struct SchemaJournalBinding {
    pub journal: Journal,
    pub runtime: Runtime,
    pub objects: Arc<dyn ObjectStore>,
    pub stream: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    namespace: String,
    stream: String,
    installation: String,
    source: [String; 5],
    domain: String,
}

fn source(s: &SourceIdentity) -> [String; 5] {
    [
        s.engine.clone(),
        s.system_id.clone(),
        s.lineage.clone(),
        s.db_name.clone(),
        s.slot.clone(),
    ]
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterBoundary {
    next: u64,
    digest: [u8; 32],
    epoch: u64,
    position: Option<Vec<u8>>,
    domain: Option<String>,
    source: Option<[String; 5]>,
    chunk: bool,
}
impl WriterBoundary {
    fn from(c: &WriterCheckpoint) -> Self {
        Self {
            next: c.next_mseq,
            digest: c.prev_digest,
            epoch: c.last_epoch,
            position: c.last_position.as_ref().map(|p| p.bytes().to_vec()),
            domain: c.last_position.as_ref().map(|p| p.order_domain().into()),
            source: c.last_record.as_ref().map(|r| source(&r.source)),
            chunk: c.last_record.as_ref().is_some_and(|r| r.chunk_rows),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    txn: Vec<u8>,
    digest: [u8; 32],
    position: u64,
    chunk: bool,
}
impl Record {
    fn from(txn: &AssembledTxn, chunk: bool) -> Result<Self> {
        Ok(Self {
            txn: txn.txn.0.clone(),
            digest: txn.digest,
            position: commit_lsn(txn)?,
            chunk,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    identity: Identity,
    nonce: String,
    previous: WriterBoundary,
    record: Option<Record>,
    end_lsn: u64,
    schemas: ddl::SchemaVersionsSnapshot,
    unknown: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    digest: String,
    durable: Option<WriterBoundary>,
    // Authenticated creation intent also identifies an unappended transaction
    // when cancellation occurred before its snapshot object was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous: Option<WriterBoundary>,
}
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    current: Option<Reference>,
    prepared: Option<Reference>,
    deleting: Option<Reference>,
}

struct Limited(Vec<u8>, usize);
impl Write for Limited {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(b.len()) > self.1 {
            return Err(std::io::Error::other("schema checkpoint size limit"));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize, cap: usize) -> Result<Vec<u8>> {
    let mut out = Limited(Vec::new(), cap);
    serde_json::to_writer(&mut out, value)
        .map_err(|_| invalid("schema checkpoint encoding/size limit"))?;
    Ok(out.0)
}
fn invalid(why: &'static str) -> Error {
    Error::RecoveryRequired(why)
}

pub(super) struct SchemaJournal {
    binding: SchemaJournalBinding,
    identity: Identity,
    epoch: u64,
    owner: String,
    pending: Option<(Reference, Vec<u8>)>,
    pub ready: bool,
}

impl SchemaJournal {
    pub async fn new(
        binding: SchemaJournalBinding,
        source_id: &SourceIdentity,
        domain: &str,
        installation: &str,
    ) -> Result<Self> {
        if binding.stream.is_empty() || binding.stream.len() > 1024 || domain.is_empty() {
            return Err(invalid("schema journal stream/domain binding"));
        }
        let state = binding.runtime.state().await;
        let verified = binding
            .journal
            .load(true)
            .await
            .map_err(Error::SchemaJournal)?;
        if state.epoch == 0
            || state.owner != verified.owner
            || state.epoch != verified.epoch
            || state.applied_seq != verified.applied_seq
            || state.digest != verified.digest
        {
            return Err(invalid("schema journal/runtime binding"));
        }
        Ok(Self {
            identity: Identity {
                namespace: binding.journal.namespace().into(),
                stream: binding.stream.clone(),
                installation: installation.into(),
                source: source(source_id),
                domain: domain.into(),
            },
            binding,
            epoch: state.epoch,
            owner: state.owner,
            pending: None,
            ready: false,
        })
    }

    fn stream(&self) -> String {
        format!("capture-schema:{}", self.identity.stream)
    }
    fn key(&self, r: &Reference) -> Result<Path> {
        if r.digest.len() != 64
            || !r
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("schema snapshot reference digest"));
        }
        Ok(Path::from(format!(
            "capture-schema/{:x}/{:x}/{}.json",
            Sha256::digest(self.identity.namespace.as_bytes()),
            Sha256::digest(self.identity.stream.as_bytes()),
            r.digest
        )))
    }
    async fn head(&self) -> Result<Head> {
        // Reading the authenticated journal also resolves a cancelled marker
        // whose durable append preceded the Runtime cache update.
        let state = self
            .binding
            .journal
            .load(true)
            .await
            .map_err(Error::SchemaJournal)?;
        if state.epoch != self.epoch || state.owner != self.owner {
            return Err(invalid("schema snapshot owner changed"));
        }
        let Some((marker, position)) = state.progress.get(&self.stream()) else {
            return Ok(Head::default());
        };
        if position != &self.identity.namespace || marker.len() > HEAD_BYTES {
            return Err(invalid("schema snapshot head binding/size"));
        }
        let head: Head =
            serde_json::from_str(marker).map_err(|_| invalid("schema snapshot head encoding"))?;
        let mut digests = BTreeSet::new();
        for r in [&head.current, &head.prepared, &head.deleting]
            .into_iter()
            .flatten()
        {
            self.key(r)?;
            if !digests.insert(&r.digest) {
                return Err(invalid("schema snapshot duplicate references"));
            }
        }
        if head.current.as_ref().is_some_and(|r| r.durable.is_none())
            || head.prepared.as_ref().is_some_and(|r| r.durable.is_some())
            || (head.prepared.is_some() && head.deleting.is_some())
        {
            return Err(invalid("schema snapshot head lifecycle"));
        }
        Ok(head)
    }
    async fn save(&self, head: &Head) -> Result<()> {
        let marker = String::from_utf8(encode(head, HEAD_BYTES)?)
            .map_err(|_| invalid("schema head UTF8"))?;
        self.binding
            .runtime
            .marker(&self.stream(), marker, self.identity.namespace.clone())
            .await
            .map_err(Error::SchemaJournal)?;
        if self.head().await? != *head {
            return Err(invalid("schema journal/runtime publication mismatch"));
        }
        Ok(())
    }
    async fn cleanup(&self, head: &mut Head) -> Result<()> {
        if let Some(r) = &head.deleting {
            // Re-fence the already durable delete intent before its effect.
            self.save(head).await?;
            let key = self.key(r)?;
            match tokio::time::timeout(Duration::from_secs(30), self.binding.objects.delete(&key))
                .await
            {
                Ok(Ok(())) | Ok(Err(object_store::Error::NotFound { .. })) => (),
                _ => return Err(invalid("schema snapshot delete unavailable")),
            }
            if self
                .binding
                .journal
                .read_bounded(&key, MAX_SCHEMA_SNAPSHOT_BYTES)
                .await
                .map_err(Error::SchemaJournal)?
                .is_some()
            {
                return Err(invalid("schema snapshot deletion store differs"));
            }
            head.deleting = None;
            self.save(head).await?;
        }
        Ok(())
    }
    async fn load(&self, r: &Reference) -> Result<Snapshot> {
        let bytes = self
            .binding
            .journal
            .read_bounded(&self.key(r)?, MAX_SCHEMA_SNAPSHOT_BYTES)
            .await
            .map_err(Error::SchemaJournal)?
            .ok_or(invalid("schema snapshot object missing"))?;
        if format!("{:x}", Sha256::digest(&bytes)) != r.digest {
            return Err(invalid("schema snapshot digest mismatch"));
        }
        let snapshot: Snapshot =
            serde_json::from_slice(&bytes).map_err(|_| invalid("schema snapshot encoding"))?;
        if snapshot.version != 1
            || snapshot.identity != self.identity
            || uuid::Uuid::parse_str(&snapshot.nonce).is_err()
        {
            return Err(invalid("foreign schema snapshot"));
        }
        if r.previous.as_ref().is_some_and(|p| p != &snapshot.previous) {
            return Err(invalid("schema snapshot creation intent mismatch"));
        }
        SchemaVersions::restore(&self.identity.installation, snapshot.schemas.clone())?;
        let previous = &snapshot.previous;
        if previous.next == 0 {
            if previous != &WriterBoundary::from(&WriterCheckpoint::default()) {
                return Err(invalid("schema snapshot genesis predecessor"));
            }
        } else if previous.source.as_ref() != Some(&self.identity.source)
            || previous.domain.as_deref() != Some(self.identity.domain.as_str())
            || previous.position.as_ref().is_none_or(|p| p.len() != 8)
            || previous.epoch == 0
        {
            return Err(invalid("schema snapshot predecessor identity"));
        }
        if let Some(record) = &snapshot.record {
            if snapshot.end_lsn < record.position || record.txn.is_empty() {
                return Err(invalid("schema snapshot record/end LSN"));
            }
            if let Some(prior) = &previous.position {
                let order = record.position.to_be_bytes().as_slice().cmp(prior);
                if order.is_lt() || (order.is_eq() && (!record.chunk || previous.chunk)) {
                    return Err(invalid("schema snapshot record order"));
                }
            }
        } else if previous.next != 0 {
            return Err(invalid("schema snapshot missing record"));
        }
        Ok(snapshot)
    }
    fn at_or_below(&self, earlier: &WriterBoundary, later: &WriterBoundary) -> bool {
        if earlier.next == later.next {
            return earlier == later;
        }
        if earlier.next == 0 {
            return earlier == &WriterBoundary::from(&WriterCheckpoint::default());
        }
        earlier.next < later.next
            && earlier.epoch <= later.epoch
            && earlier.source.as_ref() == Some(&self.identity.source)
            && later.source.as_ref() == Some(&self.identity.source)
            && earlier.domain.as_deref() == Some(self.identity.domain.as_str())
            && later.domain.as_deref() == Some(self.identity.domain.as_str())
            && earlier.position.as_ref().is_some_and(|p| {
                p.len() == 8
                    && later.position.as_ref().is_some_and(|q| {
                        q.len() == 8 && (p < q || (p == q && !earlier.chunk && later.chunk))
                    })
            })
    }
    fn predecessor(&self, head: &Head, snapshot: &Snapshot) -> bool {
        match &head.current {
            Some(current) => current
                .durable
                .as_ref()
                .is_some_and(|c| self.at_or_below(c, &snapshot.previous)),
            None => snapshot.previous.next == 0 || snapshot.previous.chunk,
        }
    }
    fn matches(&self, snapshot: &Snapshot, c: &WriterCheckpoint) -> bool {
        let Some(record) = &snapshot.record else {
            return snapshot.previous == WriterBoundary::from(c) && c.next_mseq == 0;
        };
        let Some(next) = snapshot.previous.next.checked_add(1) else {
            return false;
        };
        next <= c.next_mseq
            && self.at_or_below(&snapshot.previous, &WriterBoundary::from(c))
            && c.redelivery.iter().any(|r| {
                source(&r.source) == self.identity.source
                    && r.txn.0 == record.txn
                    && r.digest == record.digest
                    && r.position.order_domain() == self.identity.domain
                    && r.position.bytes() == record.position.to_be_bytes()
                    && r.chunk_rows == record.chunk
                    && r.manifest.as_ref().is_some_and(|m| {
                        m.mseq == snapshot.previous.next
                            && (next < c.next_mseq || m.manifest_digest == c.prev_digest)
                    })
            })
    }
    fn validate_current(
        &self,
        snapshot: &Snapshot,
        anchor: &WriterBoundary,
        c: &WriterCheckpoint,
    ) -> Result<()> {
        if !self.at_or_below(anchor, &WriterBoundary::from(c)) {
            return Err(invalid("schema snapshot ahead of recovered writer"));
        }
        if let Some(record) = &snapshot.record {
            let next = snapshot
                .previous
                .next
                .checked_add(1)
                .ok_or(invalid("schema snapshot boundary overflow"))?;
            if next > anchor.next
                || !self.at_or_below(&snapshot.previous, anchor)
                || anchor.source.as_ref() != Some(&self.identity.source)
                || anchor.domain.as_deref() != Some(self.identity.domain.as_str())
                || anchor.position.as_ref().is_none_or(|p| {
                    p.len() != 8 || record.position.to_be_bytes().as_slice() > p.as_slice()
                })
                || (next == anchor.next
                    && (anchor.position.as_deref()
                        != Some(record.position.to_be_bytes().as_slice())
                        || anchor.chunk != record.chunk))
                || c.redelivery.iter().any(|r| {
                    r.manifest
                        .as_ref()
                        .is_some_and(|m| m.mseq == snapshot.previous.next)
                        && (r.txn.0 != record.txn
                            || r.digest != record.digest
                            || source(&r.source) != self.identity.source
                            || r.position.order_domain() != self.identity.domain
                            || r.position.bytes() != record.position.to_be_bytes()
                            || r.chunk_rows != record.chunk
                            || (next == anchor.next
                                && r.manifest
                                    .as_ref()
                                    .is_some_and(|m| m.manifest_digest != anchor.digest)))
                })
            {
                return Err(invalid("schema snapshot durable boundary mismatch"));
            }
        } else if anchor.next != 0 {
            return Err(invalid("schema genesis boundary mismatch"));
        }
        Ok(())
    }
    // A complete, unpruned history consisting only of validated chunk records
    // cannot contain DDL. Other missing-history cases remain ambiguous and fail
    // closed; RetainedRedelivery deliberately does not expose event kinds.
    fn only_chunks(c: &WriterCheckpoint) -> bool {
        let manifests: BTreeSet<_> = c
            .redelivery
            .iter()
            .filter_map(|r| r.manifest.as_ref().map(|m| m.mseq))
            .collect();
        c.next_mseq > 0
            && c.redelivery_from.is_none()
            && c.redelivery.iter().all(|r| r.chunk_rows)
            && manifests.len() as u64 == c.next_mseq
            && manifests.first() == Some(&0)
            && manifests.last() == Some(&(c.next_mseq - 1))
    }
    async fn create_pending(&self) -> Result<()> {
        let (r, bytes) = self.pending.as_ref().expect("prepared schema bytes");
        self.binding
            .journal
            .create(&self.key(r)?, bytes.clone())
            .await
            .map_err(Error::SchemaJournal)
    }
    fn stage(&mut self, snapshot: Snapshot) -> Result<Reference> {
        let bytes = encode(&snapshot, MAX_SCHEMA_SNAPSHOT_BYTES)?;
        let r = Reference {
            previous: Some(snapshot.previous.clone()),
            digest: format!("{:x}", Sha256::digest(&bytes)),
            durable: None,
        };
        self.pending = Some((r.clone(), bytes));
        Ok(r)
    }
    pub fn needs_checkpoint(&self) -> bool {
        self.pending.is_none()
    }

    pub async fn restore(
        &mut self,
        c: &WriterCheckpoint,
        baseline: &SchemaVersions,
        baseline_lsn: u64,
    ) -> Result<(SchemaVersions, u64, bool)> {
        let mut head = self.head().await?;
        self.cleanup(&mut head).await?;
        if self
            .pending
            .as_ref()
            .is_some_and(|(r, _)| head.prepared.as_ref() == Some(r))
        {
            self.create_pending().await?;
        }
        let token = WriterBoundary::from(c);
        if token.next != 0
            && (token.source.as_ref() != Some(&self.identity.source)
                || token.domain.as_deref() != Some(self.identity.domain.as_str())
                || token.position.as_ref().is_none_or(|p| p.len() != 8)
                || token.epoch == 0)
        {
            return Err(invalid("schema recovered writer identity"));
        }
        let mut selected = if let Some(r) = &head.current {
            let snapshot = self.load(r).await?;
            self.validate_current(&snapshot, r.durable.as_ref().expect("current boundary"), c)?;
            Some(snapshot)
        } else {
            None
        };
        if let Some(r) = &head.prepared {
            // At the predecessor boundary the transaction did not become
            // durable. Do not GET an object whose create may have been cancelled.
            let unappended = r.previous.as_ref() == Some(&token)
                || (r.previous.is_none()
                    && head.current.as_ref().and_then(|r| r.durable.as_ref()) == Some(&token));
            if unappended {
                head.deleting = head.prepared.take();
                self.save(&head).await?;
                self.cleanup(&mut head).await?;
            } else {
                let snapshot = self.load(r).await?;
                if !self.predecessor(&head, &snapshot) || !self.matches(&snapshot, c) {
                    return Err(invalid("schema prepared/recovered boundary mismatch"));
                }
                // The retained manifest proves this prepared DDL is durable,
                // even when later DML advanced the recovered writer boundary.
                self.promote(c).await?;
                selected = Some(snapshot);
            }
        }
        let selected = match selected {
            Some(snapshot) => snapshot,
            None if c == &WriterCheckpoint::default() => {
                let snapshot = Snapshot {
                    version: 1,
                    identity: self.identity.clone(),
                    nonce: uuid::Uuid::new_v4().to_string(),
                    previous: token,
                    record: None,
                    end_lsn: baseline_lsn,
                    schemas: baseline.snapshot(),
                    unknown: false,
                };
                head.prepared = Some(self.stage(snapshot.clone())?);
                self.save(&head).await?;
                self.create_pending().await?;
                self.promote(c).await?;
                snapshot
            }
            None if Self::only_chunks(c) => {
                self.pending = None;
                return Ok((
                    baseline.clone(),
                    baseline_lsn.max(Self::writer_lsn(c)?),
                    false,
                ));
            }
            None => {
                return Err(invalid(
                    "schema snapshot missing for recovered writer boundary",
                ));
            }
        };
        let restored = SchemaVersions::restore(&self.identity.installation, selected.schemas)?;
        self.pending = None;
        Ok((
            restored,
            selected.end_lsn.max(Self::writer_lsn(c)?),
            selected.unknown,
        ))
    }

    fn writer_lsn(c: &WriterCheckpoint) -> Result<u64> {
        c.last_position.as_ref().map_or(Ok(0), |p| {
            let bytes = p
                .bytes()
                .try_into()
                .map_err(|_| invalid("schema writer LSN"))?;
            Ok(u64::from_be_bytes(bytes))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare(
        &mut self,
        schemas: &SchemaVersions,
        unknown: bool,
        txn: &AssembledTxn,
        end_lsn: u64,
        chunk: bool,
        previous: Option<WriterCheckpoint>,
    ) -> Result<()> {
        let mut head = self.head().await?;
        self.cleanup(&mut head).await?;
        let r = if let Some((r, _)) = &self.pending {
            r.clone()
        } else {
            if head.prepared.is_some() {
                return Err(invalid("unresolved prepared schema snapshot"));
            }
            let previous = previous.ok_or(invalid("schema writer boundary unavailable"))?;
            if !head
                .current
                .as_ref()
                .and_then(|r| r.durable.as_ref())
                .map_or_else(
                    || previous.next_mseq == 0 || Self::only_chunks(&previous),
                    |current| self.at_or_below(current, &WriterBoundary::from(&previous)),
                )
            {
                return Err(invalid("schema snapshot previous writer boundary mismatch"));
            }
            if source(&txn.source) != self.identity.source
                || txn.commit_position.order_domain() != self.identity.domain
            {
                return Err(invalid("schema transaction source/domain mismatch"));
            }
            self.stage(Snapshot {
                version: 1,
                identity: self.identity.clone(),
                nonce: uuid::Uuid::new_v4().to_string(),
                previous: WriterBoundary::from(&previous),
                record: Some(Record::from(txn, chunk)?),
                end_lsn,
                schemas: schemas.snapshot(),
                unknown,
            })?
        };
        if head
            .current
            .as_ref()
            .is_some_and(|current| current.digest == r.digest)
        {
            return Ok(());
        }
        if head
            .prepared
            .as_ref()
            .is_some_and(|prepared| prepared != &r)
        {
            return Err(invalid("schema prepared snapshot conflict"));
        }
        head.prepared = Some(r);
        self.save(&head).await?;
        self.create_pending().await
    }

    pub async fn promote(&mut self, c: &WriterCheckpoint) -> Result<()> {
        let mut head = self.head().await?;
        self.cleanup(&mut head).await?;
        let token = WriterBoundary::from(c);
        if head.current.as_ref().and_then(|r| r.durable.as_ref()) == Some(&token) {
            if head.prepared.is_some() {
                return Err(invalid("schema promotion before writer durability"));
            }
            // A cancelled promotion may already have published current. Finish
            // the in-memory lifecycle too, so the next DDL stages fresh bytes.
            self.pending = None;
            return Ok(());
        }
        let mut r = head
            .prepared
            .take()
            .ok_or(invalid("schema promotion without prepared snapshot"))?;
        let snapshot = self.load(&r).await?;
        if !self.predecessor(&head, &snapshot) || !self.matches(&snapshot, c) {
            return Err(invalid(
                "schema promotion before matching writer durability",
            ));
        }
        r.durable = Some(token);
        head.deleting = head.current.replace(r);
        self.save(&head).await?;
        self.cleanup(&mut head).await?;
        self.pending = None;
        Ok(())
    }
}
