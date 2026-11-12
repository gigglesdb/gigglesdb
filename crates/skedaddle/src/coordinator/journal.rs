//! Immutable create-only journal, checkpoint recovery, and explicit full scrub.
use super::{Error, Materialized, Result, Transition};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, path::Path};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn hash(value: &impl Serialize) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&serde_json::to_value(value)?)?)
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub seq: u64,
    pub epoch: u64,
    pub prev_digest: String,
    pub transition: Transition,
    /// Milliseconds since Unix epoch; seq, never clocks, establishes ordering.
    pub at: u64,
    pub coordinator_instance: String,
    pub digest: String,
}
impl Entry {
    pub fn new(state: &Materialized, transition: Transition, instance: &str) -> Result<Self> {
        let epoch = match &transition {
            Transition::AcquireOwnership { epoch } => *epoch,
            _ => state.epoch,
        };
        let mut entry = Self {
            seq: state
                .applied_seq
                .checked_add(1)
                .ok_or_else(|| Error::Rejected("journal seq exhausted".into()))?,
            epoch,
            prev_digest: state.digest.clone(),
            transition,
            at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::Rejected(e.to_string()))?
                .as_millis()
                .try_into()
                .map_err(|_| Error::Rejected("clock overflow".into()))?,
            coordinator_instance: instance.into(),
            digest: String::new(),
        };
        entry.digest = entry.computed_digest()?;
        Ok(entry)
    }
    fn computed_digest(&self) -> Result<String> {
        hash(&(
            self.seq,
            self.epoch,
            &self.prev_digest,
            &self.transition,
            self.at,
            &self.coordinator_instance,
        ))
    }
    pub fn validate(&self) -> Result<()> {
        if self.seq == 0
            || self.epoch == 0
            || self.coordinator_instance.is_empty()
            || self.digest != self.computed_digest()?
        {
            return Err(Error::RecoveryRequired(format!(
                "invalid entry/digest at seq {}",
                self.seq
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub seq: u64,
    pub journal_digest: String,
    pub state: Materialized,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_archive: Option<String>,
    pub digest: String,
}
impl Checkpoint {
    fn new(state: &Materialized, local_archive: Option<String>) -> Result<Self> {
        let mut cp = Self {
            seq: state.applied_seq,
            journal_digest: state.digest.clone(),
            state: state.clone(),
            local_archive,
            digest: String::new(),
        };
        cp.digest = cp.computed_digest()?;
        Ok(cp)
    }
    fn computed_digest(&self) -> Result<String> {
        match &self.local_archive {
            Some(archive) => hash(&(self.seq, &self.journal_digest, &self.state, archive)),
            None => hash(&(self.seq, &self.journal_digest, &self.state)),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.seq == 0
            || self.seq != self.state.applied_seq
            || self.journal_digest != self.state.digest
            || self.digest != self.computed_digest()?
        {
            return Err(Error::RecoveryRequired(
                "checkpoint digest/state mismatch".into(),
            ));
        }
        let (seq, head) = self
            .state
            .audit
            .last_key_value()
            .ok_or_else(|| Error::RecoveryRequired("checkpoint has no recorded head".into()))?;
        head.validate()?;
        if *seq != self.seq
            || head.seq != self.seq
            || head.digest != self.journal_digest
            || head.epoch != self.state.epoch
            || head.coordinator_instance != self.state.owner
            || self.state.lifecycle.since_seq > self.seq
        {
            return Err(Error::RecoveryRequired(
                "checkpoint state does not match its recorded head".into(),
            ));
        }
        Ok(())
    }
    fn validate_materialization(&self) -> Result<()> {
        self.validate()?;
        if matches!(
            self.state.lifecycle.state,
            super::State::DestinationActive
                | super::State::RollbackWindow
                | super::State::Finalized
        ) && self
            .state
            .verification
            .reverse
            .as_ref()
            .and_then(|r| r.proof.as_ref())
            .is_none()
        {
            return Err(Error::ReverseProof(super::reverse::ProofError::Missing));
        }
        super::budget::state(&self.state)?;
        super::local::projection(&self.state)?;
        for (seq, entry) in &self.state.audit {
            entry.validate()?;
            if *seq != entry.seq
                || *seq > self.seq
                || (*seq != self.seq
                    && !self.state.intent_roots.contains_key(seq)
                    && !self.state.intent_roots.values().any(|root| root == seq))
            {
                return Err(Error::RecoveryRequired(
                    "checkpoint contains a non-current entry".into(),
                ));
            }
        }
        for (current, root) in &self.state.intent_roots {
            if !self
                .state
                .audit
                .get(root)
                .is_some_and(|e| matches!(e.transition, Transition::Intent { .. }))
                || !self.state.audit.get(current).is_some_and(|e| {
                    matches!(
                        e.transition,
                        Transition::Intent { .. } | Transition::ResumeEffect { .. }
                    )
                })
            {
                return Err(Error::RecoveryRequired(
                    "checkpoint active intent is incomplete".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Immutable dense discovery index. It is outside the journal prefix, so finding
/// the newest checkpoint uses O(log checkpoints) GETs and never lists history.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointHead {
    ordinal: u64,
    seq: u64,
    checkpoint_digest: String,
    journal_digest: String,
    digest: String,
}
impl CheckpointHead {
    fn new(ordinal: u64, cp: &Checkpoint) -> Result<Self> {
        let mut head = Self {
            ordinal,
            seq: cp.seq,
            checkpoint_digest: cp.digest.clone(),
            journal_digest: cp.journal_digest.clone(),
            digest: String::new(),
        };
        head.digest = head.computed_digest()?;
        Ok(head)
    }
    fn computed_digest(&self) -> Result<String> {
        hash(&(
            self.ordinal,
            self.seq,
            &self.checkpoint_digest,
            &self.journal_digest,
        ))
    }
    fn validate(&self, ordinal: u64) -> Result<()> {
        if self.ordinal != ordinal || self.seq == 0 || self.digest != self.computed_digest()? {
            return Err(Error::RecoveryRequired(
                "checkpoint discovery record is corrupt".into(),
            ));
        }
        Ok(())
    }
}

pub struct Recovery {
    pub checkpoint: Option<Checkpoint>,
    pub state: Materialized,
}

#[derive(Clone)]
pub struct Journal {
    store: Arc<dyn ObjectStore>,
    root: Path,
    index: Path,
    put_timeout: Duration,
    pub(crate) chunk_buffer: Option<super::chunk::ChunkBuffer>,
}

/// Configure upstream's durable local backend. Refuse identifiable unfinished
/// uploads at startup: they may belong to a crashed OR still-running writer.
/// Never remove a competing writer's staging files. Call before sharing the store.
pub fn local_store(
    root: impl AsRef<std::path::Path>,
) -> Result<object_store::local::LocalFileSystem> {
    let store =
        object_store::local::LocalFileSystem::new_with_prefix(root.as_ref())?.with_fsync(true);
    check_local_staging(root.as_ref())?;
    Ok(store)
}

fn check_local_staging(root: &std::path::Path) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err(Error::RecoveryRequired(format!(
                "local object root contains a symlink: {}",
                entry.path().display()
            )));
        }
        if kind.is_dir() {
            check_local_staging(&entry.path())?;
        } else if entry
            .file_name()
            .to_string_lossy()
            .rsplit_once('#')
            .is_some_and(|(_, n)| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(Error::RecoveryRequired(format!(
                "unfinished local object-store staging file at {}; inspect possible crash orphan or active upload before retrying",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

impl Journal {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        migration_id: &str,
        put_timeout: Duration,
    ) -> Result<Self> {
        if migration_id.is_empty()
            || migration_id.contains(['/', '\\'])
            || matches!(migration_id, "." | "..")
            || put_timeout.is_zero()
        {
            return Err(Error::Rejected(
                "migration ID must be one path component and timeout must be positive".into(),
            ));
        }
        let prefix = prefix.trim_matches('/');
        let root = Path::parse(format!("{prefix}/journal/{migration_id}"))
            .map_err(|e| Error::Rejected(e.to_string()))?;
        let index = Path::parse(format!("{prefix}/checkpoint-index/{migration_id}"))
            .map_err(|e| Error::Rejected(e.to_string()))?;
        Ok(Self {
            store,
            root,
            index,
            put_timeout,
            chunk_buffer: None,
        })
    }
    pub fn entry_key(&self, seq: u64) -> Path {
        self.root.clone().join(format!("{seq:020}.json"))
    }
    pub fn migration_id(&self) -> &str {
        self.root.filename().expect("validated migration path")
    }
    pub fn namespace(&self) -> &str {
        self.root.as_ref()
    }
    pub fn checkpoint_key(&self, seq: u64) -> Path {
        self.root.clone().join(format!("checkpoint-{seq:020}.json"))
    }
    pub fn checkpoint_index_key(&self, ordinal: u64) -> Path {
        self.index.clone().join(format!("{ordinal:020}.json"))
    }
    pub(crate) async fn read(&self, key: &Path) -> Result<Option<Vec<u8>>> {
        match self.store.get(key).await {
            Ok(result) => Ok(Some(result.bytes().await?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) async fn read_bounded(&self, key: &Path, cap: usize) -> Result<Option<Vec<u8>>> {
        let result = match self.store.get(key).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if result.meta.size > cap as u64 {
            return Err(Error::Rejected(format!("object exceeds {cap} byte limit")));
        }
        let mut stream = result.into_stream();
        let mut bytes = Vec::new();
        while let Some(part) = stream.try_next().await? {
            if bytes.len().saturating_add(part.len()) > cap {
                return Err(Error::Rejected(format!("object exceeds {cap} byte limit")));
            }
            bytes.extend_from_slice(&part);
        }
        Ok(Some(bytes))
    }
    /// A timeout is never a vote: compare durable bytes at this exact key.
    pub(crate) async fn create(&self, key: &Path, bytes: Vec<u8>) -> Result<()> {
        let outcome = tokio::time::timeout(
            self.put_timeout,
            self.store
                .put_opts(key, bytes.clone().into(), PutMode::Create.into()),
        )
        .await;
        if matches!(outcome, Ok(Ok(_))) {
            return Ok(());
        }
        match tokio::time::timeout(self.put_timeout, self.read(key)).await {
            Ok(Ok(Some(found))) if found == bytes => Ok(()),
            Ok(Ok(Some(_))) => Err(Error::LostOwnership),
            Ok(Ok(None)) => match outcome {
                Ok(Err(error)) => Err(Error::Store(error)),
                _ => Err(Error::UnknownOutcome(format!(
                    "{key} not visible after timeout"
                ))),
            },
            Ok(Err(error)) => Err(Error::UnknownOutcome(format!(
                "{key}: reread failed: {error}"
            ))),
            Err(_) => Err(Error::UnknownOutcome(format!("{key}: reread timed out"))),
        }
    }
    pub async fn put(&self, entry: &Entry) -> Result<()> {
        entry.validate()?;
        self.validate_reverse(&entry.transition)?;
        if matches!(
            entry.transition,
            Transition::Lifecycle {
                event: super::Event::Advance {
                    to: super::State::DestinationActive
                        | super::State::RollbackWindow
                        | super::State::RollingBack
                        | super::State::Finalized
                }
            } | Transition::RollbackEligibilityEnded { .. }
                | Transition::ReverseSetup { .. }
                | Transition::ReverseConsistentPoint { .. }
                | Transition::VerificationInvalidated { .. }
                | Transition::VerificationCompleted { .. }
                | Transition::ChunkPlanned { .. }
                | Transition::ChunkStarted { .. }
                | Transition::ChunkDone { .. }
        ) {
            let state = self.load(true).await?;
            if entry.seq > state.applied_seq {
                state.projected(entry)?;
                self.validate_chunk_objects(&state, &entry.transition)
                    .await?;
            } else if self.entry(entry.seq).await? != *entry {
                return Err(Error::LostOwnership);
            }
        }
        self.create(&self.entry_key(entry.seq), serde_json::to_vec(entry)?)
            .await
    }

    /// One validated historical object; callers never consult the local cache
    /// for completed effects, audit history or historical status.
    pub async fn entry(&self, seq: u64) -> Result<Entry> {
        let bytes = self
            .read(&self.entry_key(seq))
            .await?
            .ok_or_else(|| Error::RecoveryRequired(format!("journal entry {seq} is missing")))?;
        let entry: Entry = decode(&bytes)?;
        entry.validate()?;
        if entry.seq != seq {
            return Err(Error::RecoveryRequired("journal entry key differs".into()));
        }
        Ok(entry)
    }

    /// Keyset pagination, bounded by both object count and approximately 1 MiB.
    /// The first entry may exceed that byte target (a normalized definition).
    /// `after` is exclusive; an existing anchor authenticates the page chain.
    pub async fn history_page(&self, after: u64, limit: usize) -> Result<Vec<Entry>> {
        if limit == 0 || limit > 128 {
            return Err(Error::TruncatedRead(
                "journal page bound must be 1..=128".into(),
            ));
        }
        let digest = if after == 0 {
            String::new()
        } else {
            self.entry(after).await?.digest
        };
        self.page_from_anchor(after, limit, digest).await
    }

    async fn page_from_anchor(
        &self,
        after: u64,
        limit: usize,
        mut digest: String,
    ) -> Result<Vec<Entry>> {
        let mut page = Vec::new();
        let mut total = 0;
        for offset in 1..=limit as u64 {
            let seq = after
                .checked_add(offset)
                .ok_or_else(|| Error::RecoveryRequired("journal seq exhausted".into()))?;
            let Some(bytes) = self.read(&self.entry_key(seq)).await? else {
                break;
            };
            if !page.is_empty() && total + bytes.len() > 1024 * 1024 {
                break;
            }
            let entry: Entry = decode(&bytes)?;
            entry.validate()?;
            if entry.seq != seq || entry.prev_digest != digest {
                return Err(Error::RecoveryRequired("journal page chain differs".into()));
            }
            total += bytes.len();
            digest = entry.digest.clone();
            page.push(entry);
        }
        Ok(page)
    }

    /// Audit/status pagination against a verified, stable head. An early gap
    /// is an error, never an apparently complete historical response.
    pub async fn history_page_through(
        &self,
        after: u64,
        limit: usize,
        through: u64,
    ) -> Result<Vec<Entry>> {
        if after > through {
            return Err(Error::Rejected(
                "history cursor is beyond the verified head".into(),
            ));
        }
        if after == through {
            if limit == 0 || limit > 128 {
                return Err(Error::TruncatedRead(
                    "journal page bound must be 1..=128".into(),
                ));
            }
            return Ok(Vec::new());
        }
        let mut page = self.history_page(after, limit).await?;
        page.retain(|entry| entry.seq <= through);
        let last = page.last().map_or(after, |entry| entry.seq);
        let expected = after.saturating_add(limit as u64).min(through);
        if last < expected && self.read(&self.entry_key(last + 1)).await?.is_none() {
            return Err(Error::RecoveryRequired(format!(
                "historical journal gap at {} before verified head {through}",
                last + 1
            )));
        }
        Ok(page)
    }

    /// Resolve an older journal's adoption of a pruned alias without retaining
    /// its entire adoption chain in Materialized. New owners adopt the current
    /// alias directly; full rebuild also accepts the older wire history.
    pub(crate) async fn apply_entry(&self, state: &mut Materialized, entry: &Entry) -> Result<()> {
        self.validate_reverse(&entry.transition)?;
        self.validate_chunk_objects(state, &entry.transition)
            .await?;
        if let Transition::ResumeEffect { intent_seq } = entry.transition
            && !state.intent_roots.contains_key(&intent_seq)
        {
            let prior = self.entry(intent_seq).await?;
            let mut root = prior.clone();
            loop {
                match root.transition {
                    Transition::Intent { .. } => break,
                    Transition::ResumeEffect { intent_seq } if intent_seq < root.seq => {
                        root = self.entry(intent_seq).await?
                    }
                    _ => {
                        return Err(Error::RecoveryRequired(
                            "invalid historical intent alias".into(),
                        ));
                    }
                }
            }
            if state.audit.get(&root.seq) != Some(&root)
                || !state.intent_roots.values().any(|seq| *seq == root.seq)
            {
                return Err(Error::Rejected(
                    "adoption has no active original intent".into(),
                ));
            }
            let mut next = state.clone();
            next.intent_roots.retain(|_, seq| *seq != root.seq);
            next.intent_roots.insert(intent_seq, root.seq);
            next.audit.insert(intent_seq, prior);
            next.apply(entry)?;
            *state = next;
            return Ok(());
        }
        state.apply(entry)
    }

    async fn verify_snapshot(&self, state: &Materialized) -> Result<()> {
        let mut expected = if let Some(head) = self.latest_head().await? {
            let saved = self.read_checkpoint(&head).await?;
            if saved.seq == state.applied_seq {
                if saved.state != *state {
                    return Err(Error::RecoveryRequired(
                        "checkpoint state differs at the same sequence".into(),
                    ));
                }
                return Ok(());
            }
            if saved.seq < state.applied_seq {
                saved.state
            } else {
                Materialized::default()
            }
        } else {
            Materialized::default()
        };
        while expected.applied_seq < state.applied_seq {
            let page = self
                .page_from_anchor(expected.applied_seq, 128, expected.digest.clone())
                .await?;
            if page.is_empty() {
                return Err(Error::RecoveryRequired(
                    "checkpoint prefix is incomplete".into(),
                ));
            }
            for entry in page {
                if entry.seq > state.applied_seq {
                    break;
                }
                self.apply_entry(&mut expected, &entry).await?;
            }
        }
        if expected != *state {
            return Err(Error::RecoveryRequired(
                "checkpoint differs from journal projection".into(),
            ));
        }
        Ok(())
    }

    pub async fn checkpoint(&self, state: &Materialized) -> Result<()> {
        self.write_checkpoint(state, None).await
    }
    pub(crate) async fn checkpoint_archive(
        &self,
        state: &Materialized,
        archive: String,
    ) -> Result<()> {
        self.write_checkpoint(state, Some(archive)).await
    }
    async fn write_checkpoint(&self, state: &Materialized, archive: Option<String>) -> Result<()> {
        let cp = Checkpoint::new(state, archive)?;
        cp.validate_materialization()?;
        if let Some(report) = &cp.state.verification.report {
            self.verification_report(report).await?;
        }
        self.verify_snapshot(state).await?;
        let anchor = self.read(&self.entry_key(cp.seq)).await?.ok_or_else(|| {
            Error::RecoveryRequired("checkpoint journal anchor missing during publication".into())
        })?;
        let anchor: Entry = decode(&anchor)?;
        anchor.validate()?;
        if anchor.seq != cp.seq || anchor.digest != cp.journal_digest {
            return Err(Error::RecoveryRequired(
                "checkpoint journal anchor differs".into(),
            ));
        }
        if let Some(prior) = self.latest_head().await?
            && prior.seq >= cp.seq
        {
            let saved = self.read_checkpoint(&prior).await?;
            if prior.seq == cp.seq && saved.state != cp.state {
                return Err(Error::RecoveryRequired(
                    "checkpoint state differs at the same sequence".into(),
                ));
            }
            return Ok(());
        }
        self.create(&self.checkpoint_key(cp.seq), serde_json::to_vec(&cp)?)
            .await?;
        // The checkpoint becomes discoverable only when its immutable index put
        // commits. A crash before publication leaves an unindexed derived object.
        for _ in 0..8 {
            let prior = self.latest_head().await?;
            if let Some(prior) = &prior
                && prior.seq >= cp.seq
            {
                self.read_checkpoint(prior).await?;
                return Ok(());
            }
            let ordinal = prior.map_or(Ok(1), |h| {
                h.ordinal
                    .checked_add(1)
                    .ok_or_else(|| Error::RecoveryRequired("checkpoint ordinal exhausted".into()))
            })?;
            let head = CheckpointHead::new(ordinal, &cp)?;
            match self
                .create(
                    &self.checkpoint_index_key(ordinal),
                    serde_json::to_vec(&head)?,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(Error::LostOwnership) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::LostOwnership)
    }
    async fn index_head(&self, ordinal: u64) -> Result<Option<CheckpointHead>> {
        let Some(bytes) = self.read(&self.checkpoint_index_key(ordinal)).await? else {
            return Ok(None);
        };
        let head: CheckpointHead = decode(&bytes)?;
        head.validate(ordinal)?;
        Ok(Some(head))
    }
    async fn latest_head(&self) -> Result<Option<CheckpointHead>> {
        let Some(mut head) = self.index_head(1).await? else {
            return Ok(None);
        };
        let mut lo = 1u64;
        let mut hi = 2u64;
        while let Some(next) = self.index_head(hi).await? {
            if next.seq <= head.seq {
                return Err(Error::RecoveryRequired("checkpoint index regressed".into()));
            }
            lo = hi;
            head = next;
            hi = hi.checked_mul(2).ok_or_else(|| {
                Error::RecoveryRequired("checkpoint index exceeds discovery bounds".into())
            })?;
        }
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            match self.index_head(mid).await? {
                Some(next) => {
                    if next.seq <= head.seq {
                        return Err(Error::RecoveryRequired("checkpoint index regressed".into()));
                    }
                    lo = mid;
                    head = next;
                }
                None => hi = mid,
            }
        }
        Ok(Some(head))
    }
    async fn read_checkpoint(&self, head: &CheckpointHead) -> Result<Checkpoint> {
        let bytes = self
            .read(&self.checkpoint_key(head.seq))
            .await?
            .ok_or_else(|| Error::RecoveryRequired("published checkpoint is missing".into()))?;
        let cp: Checkpoint = decode(&bytes)?;
        cp.validate_materialization()?;
        if let Some(report) = &cp.state.verification.report {
            self.verification_report(report).await?;
        }
        for plan in cp.state.chunk_plans.values() {
            let saved = self.chunk_plan(plan).await?;
            for attempt in cp
                .state
                .chunk_attempts
                .values()
                .filter(|a| a.chunk.id.table == plan.table)
            {
                if saved
                    .ranges
                    .get(usize::try_from(attempt.chunk.id.ordinal).map_err(super::chunk::invalid)?)
                    != Some(&attempt.chunk)
                {
                    return Err(super::chunk::invalid(
                        "checkpoint bounds differ from durable plan",
                    ));
                }
            }
        }
        if cp.seq != head.seq
            || cp.digest != head.checkpoint_digest
            || cp.journal_digest != head.journal_digest
        {
            return Err(Error::RecoveryRequired(
                "checkpoint differs from its discovery record".into(),
            ));
        }
        // Archived/deleted old journal objects are allowed during recovery. A
        // present anchor must match; failure never falls back to an older CP.
        if let Some(bytes) = self.read(&self.entry_key(cp.seq)).await? {
            let anchor: Entry = decode(&bytes)?;
            anchor.validate()?;
            if anchor.seq != cp.seq || anchor.digest != cp.journal_digest {
                return Err(Error::RecoveryRequired(
                    "checkpoint anchor digest mismatch".into(),
                ));
            }
        }
        Ok(cp)
    }
    pub async fn load(&self, use_checkpoint: bool) -> Result<Materialized> {
        // Recovery is a large state machine. Keep it heap-bound so optional
        // verification metadata cannot amplify nested coordinator future stacks.
        Ok(Box::pin(self.recover(use_checkpoint)).await?.state)
    }

    /// Explicit maintenance operation, never a recovery fallback. Stop all
    /// writers and retire write access to the old journal prefixes before activating the
    /// replacement. Both journal/discovery prefixes in the destination must be
    /// empty, and its namespace must stay identical for peer idempotency keys.
    /// Derived checkpoints are deliberately not decoded or copied. The complete
    /// raw chain is validated and copied byte-for-byte with create-only puts;
    /// source objects are never changed. A failed copy leaves evidence in the
    /// unactivated destination; retry with another empty destination.
    pub async fn copy_entries_for_rebuild(&self, destination: &Journal) -> Result<Materialized> {
        if self.namespace() != destination.namespace() {
            return Err(Error::Rejected(
                "replacement must preserve the journal namespace and effect identity".into(),
            ));
        }
        for prefix in [&destination.root, &destination.index] {
            if destination
                .store
                .list(Some(prefix))
                .try_next()
                .await?
                .is_some()
            {
                return Err(Error::Rejected(
                    "replacement journal and checkpoint-index prefixes must be empty".into(),
                ));
            }
        }
        let published = self.latest_head().await?;
        let mut last = 0;
        let mut count = 0u64;
        let mut listing = self.store.list(Some(&self.root));
        while let Some(meta) = listing.try_next().await? {
            let name = meta
                .location
                .filename()
                .ok_or_else(|| Error::RecoveryRequired("journal key has no filename".into()))?;
            if name.starts_with("checkpoint-") {
                continue;
            }
            let seq = parse_seq(name.strip_suffix(".json").ok_or_else(|| {
                Error::RecoveryRequired(format!("unexpected journal object {name}"))
            })?)?;
            if meta.location != self.entry_key(seq) {
                return Err(Error::RecoveryRequired(
                    "noncanonical raw journal key".into(),
                ));
            }
            last = last.max(seq);
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::RecoveryRequired("journal seq exhausted".into()))?;
        }
        if published.is_some_and(|head| head.seq > last) {
            return Err(Error::RecoveryRequired(
                "raw journal ends before its published checkpoint; restore the complete raw history".into(),
            ));
        }
        if count != last {
            return Err(Error::RecoveryRequired(
                "raw journal has a sequence gap".into(),
            ));
        }
        let mut state = Materialized::default();
        for seq in 1..=last {
            let bytes = self.read(&self.entry_key(seq)).await?.ok_or_else(|| {
                Error::RecoveryRequired("raw journal entry vanished during copy".into())
            })?;
            let entry: Entry = decode(&bytes)?;
            if entry.seq != seq {
                return Err(Error::RecoveryRequired(
                    "entry sequence differs from its key".into(),
                ));
            }
            self.apply_entry(&mut state, &entry).await?;
            if let Transition::VerificationCompleted { report } = &entry.transition {
                let object = self.verification_report(report).await?;
                if destination.publish_verification(&object).await? != **report {
                    return Err(Error::Rejected("copied verification differs".into()));
                }
            }
            if let Transition::ChunkPlanned { plan } = &entry.transition {
                let object = self.chunk_plan(plan).await?;
                if destination.publish_chunk_plan(&object).await? != *plan {
                    return Err(super::chunk::invalid("copied plan reference differs"));
                }
            }
            destination
                .create(&destination.entry_key(seq), bytes)
                .await?;
        }
        let next = last
            .checked_add(1)
            .ok_or_else(|| Error::RecoveryRequired("journal seq exhausted".into()))?;
        if self.read(&self.entry_key(next)).await?.is_some() {
            return Err(Error::RecoveryRequired(
                "source advanced during copy; stop writers and retry with another empty destination".into(),
            ));
        }
        Ok(state)
    }
    /// Checkpoint recovery does not LIST the journal. Read N+1 upward by key until
    /// NotFound, starting the tail chain from the recorded checkpoint head digest.
    pub async fn recover(&self, use_checkpoint: bool) -> Result<Recovery> {
        if use_checkpoint && let Some(head) = self.latest_head().await? {
            let checkpoint = self.read_checkpoint(&head).await?;
            let mut state = checkpoint.state.clone();
            loop {
                let seq = state
                    .applied_seq
                    .checked_add(1)
                    .ok_or_else(|| Error::RecoveryRequired("journal seq exhausted".into()))?;
                let Some(bytes) = self.read(&self.entry_key(seq)).await? else {
                    break;
                };
                let entry: Entry = decode(&bytes)?;
                if entry.seq != seq {
                    return Err(Error::RecoveryRequired(
                        "tail entry sequence differs from its key".into(),
                    ));
                }
                self.apply_entry(&mut state, &entry)
                    .await
                    .map_err(|e| Error::RecoveryRequired(format!("journal tail at {seq}: {e}")))?;
            }
            // P11a authority requires its actual producer entries, including
            // after a restart through Local::recover. Never trust injected cache
            // metadata; retain raw setup history throughout the fenced window.
            if state.verification.reverse.is_some() {
                return Box::pin(self.recover(false)).await;
            }
            return Ok(Recovery {
                checkpoint: Some(checkpoint),
                state,
            });
        }
        let state = self.scrub().await?;
        Ok(Recovery {
            checkpoint: None,
            state,
        })
    }
    /// Explicit full-history integrity check. Unlike checkpoint recovery, this
    /// lists and validates every immutable transition, including the old prefix.
    pub async fn scrub(&self) -> Result<Materialized> {
        let checkpoint_head = self.latest_head().await?;
        let mut listing = self.store.list(Some(&self.root));
        let mut sequences = std::collections::BTreeMap::new();
        let mut checkpoints = std::collections::BTreeMap::new();
        while let Some(meta) = listing.try_next().await? {
            let name = meta
                .location
                .filename()
                .ok_or_else(|| Error::RecoveryRequired("journal key has no filename".into()))?;
            if let Some(n) = name
                .strip_prefix("checkpoint-")
                .and_then(|s| s.strip_suffix(".json"))
            {
                let seq = parse_seq(n)?;
                if meta.location != self.checkpoint_key(seq) {
                    return Err(Error::RecoveryRequired(
                        "checkpoint key is outside its canonical location".into(),
                    ));
                }
                checkpoints.insert(seq, meta.location);
            } else {
                let n = name.strip_suffix(".json").ok_or_else(|| {
                    Error::RecoveryRequired(format!("unexpected journal object {name}"))
                })?;
                let seq = parse_seq(n)?;
                if meta.location != self.entry_key(seq) {
                    return Err(Error::RecoveryRequired(
                        "journal key is outside its canonical location".into(),
                    ));
                }
                if sequences.insert(seq, meta.location).is_some() {
                    return Err(Error::RecoveryRequired("duplicate sequence key".into()));
                }
            }
        }
        let mut state = Materialized::default();
        // Checkpoints are compared to independently replayed state at their
        // exact journal sequence; no historical Entries are retained.
        for (index, (seq, key)) in sequences.iter().enumerate() {
            if *seq != index as u64 + 1 {
                return Err(Error::RecoveryRequired(format!(
                    "journal gap before seq {seq}"
                )));
            }
            let bytes = self
                .read(key)
                .await?
                .ok_or_else(|| Error::RecoveryRequired("listed journal entry vanished".into()))?;
            let entry: Entry = decode(&bytes)?;
            if entry.seq != *seq {
                return Err(Error::RecoveryRequired(
                    "entry sequence differs from its key".into(),
                ));
            }
            self.apply_entry(&mut state, &entry)
                .await
                .map_err(|e| Error::RecoveryRequired(format!("journal replay at {seq}: {e}")))?;
            if let Some(key) = checkpoints.remove(seq) {
                let bytes = self
                    .read(&key)
                    .await?
                    .ok_or_else(|| Error::RecoveryRequired("listed checkpoint vanished".into()))?;
                let cp: Checkpoint = decode(&bytes)?;
                cp.validate_materialization()?;
                if let Some(report) = &cp.state.verification.report {
                    self.verification_report(report).await?;
                }
                if cp.seq != *seq || cp.state != state {
                    return Err(Error::RecoveryRequired(
                        "checkpoint differs from scrubbed journal projection".into(),
                    ));
                }
            }
        }
        if !checkpoints.is_empty() {
            return Err(Error::RecoveryRequired(
                "checkpoint has no matching scrubbed journal anchor".into(),
            ));
        }
        if let Some(head) = checkpoint_head {
            if head.seq > state.applied_seq {
                return Err(Error::RecoveryRequired(
                    "journal ends before its published checkpoint".into(),
                ));
            }
            self.read_checkpoint(&head).await?;
        }
        Ok(state)
    }
}
fn parse_seq(s: &str) -> Result<u64> {
    let seq = s
        .parse::<u64>()
        .map_err(|_| Error::RecoveryRequired("invalid sequence filename".into()))?;
    if s.len() != 20 || !s.bytes().all(|b| b.is_ascii_digit()) || seq == 0 {
        return Err(Error::RecoveryRequired(
            "noncanonical sequence filename".into(),
        ));
    }
    Ok(seq)
}
fn decode<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|e| Error::RecoveryRequired(format!("malformed journal object: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_requires_a_committed_nonzero_sequence() {
        let cp = Checkpoint::new(&Materialized::default(), None).unwrap();
        assert!(matches!(cp.validate(), Err(Error::RecoveryRequired(_))));
    }
    #[test]
    fn sequence_filename_contract() {
        assert_eq!(parse_seq("00000000000000000001").unwrap(), 1);
        for name in [
            "1",
            "+0000000000000000001",
            "00000000000000000000",
            "0000000000000000000x",
            "",
        ] {
            assert!(parse_seq(name).is_err(), "{name}");
        }
    }
}
