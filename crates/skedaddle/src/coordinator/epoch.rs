//! Ownership and journal-before-effect ordering, with PostgreSQL fencing.

use std::{future::Future, sync::Arc};

use super::exclusion::{self, ExclusionGuard, MigrationExclusion};

use super::{
    EffectKind, Error, Materialized, Result, Transition,
    journal::{Entry, Journal, VerifiedRefresh},
    local::Local,
};

fn exclusion_error(error: exclusion::Error) -> Error {
    match error {
        exclusion::Error::Lost => Error::LostOwnership,
        error => Error::Rejected(error.to_string()),
    }
}

#[derive(Clone, Debug)]
pub struct EffectToken {
    migration: String,
    authority_epoch: u64,
    epoch: u64,
    seq: u64,
    effect: EffectKind,
    payload: String,
}
impl EffectToken {
    pub fn migration(&self) -> &str {
        &self.migration
    }
    /// The current owner's fencing epoch. The effect key remains (epoch, seq).
    pub fn authority_epoch(&self) -> u64 {
        self.authority_epoch
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn seq(&self) -> u64 {
        self.seq
    }
    pub fn effect(&self) -> &EffectKind {
        &self.effect
    }
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

// Replace the old inline checkpoint interval with one pointer, preserving the
// Coordinator layout budget as well as Materialized and Transition layouts.
struct RefreshState {
    checkpoint_every: u64,
    verified: Option<Box<VerifiedRefresh>>,
    full_refresh_interval: std::time::Duration,
    last_scrub: std::time::Instant,
}

pub struct Coordinator {
    pub(super) journal: Journal,
    local: Local,
    instance: Box<str>,
    exclusion: Option<Arc<MigrationExclusion>>,
    owns: bool,
    refresh_state: Box<RefreshState>,
    #[cfg(feature = "test-crash")]
    crash: Option<super::CrashSeam>,
}

impl Coordinator {
    /// A freshly opened coordinator is always an observer, including after a crash.
    pub fn observer(journal: Journal, local: Local, instance: impl Into<String>) -> Result<Self> {
        let instance = instance.into();
        if instance.is_empty() {
            return Err(Error::Rejected("coordinator instance is empty".into()));
        }
        Ok(Self {
            journal,
            local,
            instance: format!("{instance}/{}", uuid::Uuid::new_v4()).into_boxed_str(),
            exclusion: None,
            owns: false,
            refresh_state: Box::new(RefreshState {
                checkpoint_every: 1000,
                verified: None,
                full_refresh_interval: std::time::Duration::from_secs(300),
                last_scrub: std::time::Instant::now(),
            }),
            #[cfg(feature = "test-crash")]
            crash: None,
        })
    }
    pub fn state(&self) -> &Materialized {
        self.local.state()
    }
    pub(super) fn gc_journal(&self) -> &Journal {
        &self.journal
    }
    pub fn instance_id(&self) -> &str {
        &self.instance
    }
    pub fn is_owner(&self) -> bool {
        self.owns
    }
    pub fn local(&self) -> &Local {
        &self.local
    }
    pub fn with_checkpoint_interval(mut self, transitions: u64) -> Result<Self> {
        if transitions == 0 {
            return Err(Error::Rejected(
                "checkpoint interval must be positive".into(),
            ));
        }
        self.refresh_state.checkpoint_every = transitions;
        Ok(self)
    }
    /// Maximum time between full authentications during refresh activity.
    /// Zero forces a full scrub on every refresh (also useful for diagnostics).
    pub fn with_full_refresh_interval(mut self, interval: std::time::Duration) -> Self {
        self.refresh_state.full_refresh_interval = interval;
        self
    }

    pub async fn checkpoint(&mut self) -> Result<()> {
        if !self.owns {
            return Err(Error::Observer);
        }
        self.refresh_state.verified = None;
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        self.refresh_state.verified = None;
        let result = self.local.checkpoint(&self.journal).await;
        if result.is_err() {
            self.owns = false;
        }
        result
    }
    pub async fn refresh(&mut self) -> Result<()> {
        Box::pin(self.refresh_at(std::time::Instant::now())).await
    }

    async fn refresh_at(&mut self, now: std::time::Instant) -> Result<()> {
        let epoch = self.state().epoch;
        // Historical immutable objects before the cached head are rechecked
        // at full boundaries, not on each refresh. Never seed from Local.
        // Keep this deadline independent of checkpoint/acquisition refreshes:
        // frequent rebases must not postpone old-prefix tampering detection.
        let periodic = now.saturating_duration_since(self.refresh_state.last_scrub)
            >= self.refresh_state.full_refresh_interval;
        let candidate = if periodic {
            self.journal.full_refresh(true).await
        } else if let Some(cached) = &self.refresh_state.verified {
            self.journal.refresh_suffix(cached).await
        } else {
            self.journal.full_refresh(false).await
        };
        let verified = match candidate {
            Ok(state) => state,
            Err(error) => {
                self.owns = false;
                self.refresh_state.verified = None;
                return Err(error);
            }
        };
        let result = self
            .local
            .sync_verified(&verified.state, &self.journal)
            .await;
        if result.is_err()
            || (self.owns && (self.state().owner != *self.instance || self.state().epoch != epoch))
        {
            self.owns = false;
        }
        if result.is_ok() {
            if periodic {
                self.refresh_state.last_scrub = now;
            }
            self.refresh_state.verified = Some(verified);
        } else {
            self.refresh_state.verified = None;
        }
        result
    }

    pub async fn acquire(&mut self) -> Result<u64> {
        self.owns = false;
        let guard = match self.exclusion.clone() {
            Some(exclusion) => Some(
                Box::pin(exclusion.acquire(self.journal.migration_id()))
                    .await
                    .map_err(exclusion_error)?,
            ),
            None => None,
        };
        self.acquire_inner(guard.as_ref()).await
    }

    pub fn with_exclusion(mut self, exclusion: Arc<MigrationExclusion>) -> Self {
        self.exclusion = Some(exclusion);
        self
    }

    /// Promote under an existing capability, without recursive acquisition.
    pub async fn acquire_with_guard(&mut self, guard: &ExclusionGuard) -> Result<u64> {
        self.owns = false;
        self.validate_guard(guard).map_err(exclusion_error)?;
        self.acquire_inner(Some(guard)).await
    }

    fn validate_guard(&self, guard: &ExclusionGuard) -> exclusion::Result<()> {
        guard.validate(
            self.exclusion
                .as_ref()
                .ok_or(exclusion::Error::NotConfigured)?,
            self.journal.migration_id(),
        )
    }

    /// Owned, borrow-free capability for an external closure. Refresh under the
    /// lock authenticates its captured instance and epoch. Keep it for the whole
    /// closure; nested promotion uses acquire_with_guard.
    pub async fn exclusive(&mut self) -> exclusion::Result<ExclusionGuard> {
        let exclusion = self
            .exclusion
            .clone()
            .ok_or(exclusion::Error::NotConfigured)?;
        if !self.owns {
            return Err(exclusion::Error::NotOwner);
        }
        let mut guard = Box::pin(exclusion.acquire(self.journal.migration_id())).await?;
        self.refresh().await.map_err(|_| exclusion::Error::Lost)?;
        if !self.owns {
            return Err(exclusion::Error::NotOwner);
        }
        guard.check_alive().await?;
        guard.bind_owner(&self.instance, self.state().epoch);
        Ok(guard)
    }

    async fn acquire_inner(&mut self, guard: Option<&ExclusionGuard>) -> Result<u64> {
        self.refresh_state.verified = None;
        if let Some(guard) = guard {
            guard.check_alive().await.map_err(exclusion_error)?;
        }
        self.refresh().await?;
        let epoch = self
            .state()
            .epoch
            .checked_add(1)
            .ok_or_else(|| Error::Rejected("epoch exhausted".into()))?;
        self.commit_inner(Transition::AcquireOwnership { epoch })
            .await?;
        if let Some(guard) = guard {
            guard.check_alive().await.map_err(exclusion_error)?;
        }
        self.owns = true;
        Ok(epoch)
    }

    /// Captured journal state makes racing owners compete for the same seq.
    /// A failed or uncertain put always revokes this process's write capability.
    pub(super) async fn commit_inner(&mut self, transition: Transition) -> Result<Entry> {
        self.journal.validate_reverse(&transition)?;
        let entry = Entry::new(self.state(), transition, &self.instance)?;
        self.local.validate_transition(&entry)?;
        self.journal
            .validate_chunk_objects(self.state(), &entry.transition)
            .await?;
        if let Err(error) = self.journal.put(&entry).await {
            self.owns = false;
            // Best effort refresh must never replace the original put failure.
            let _ = self.refresh().await;
            return Err(error);
        }
        #[cfg(feature = "test-crash")]
        self.crash_at(super::CrashSeam::AfterJournalPut);
        if let Err(error) = self.local.apply(&entry) {
            self.owns = false;
            return Err(error);
        }
        #[cfg(feature = "test-crash")]
        self.crash_at(super::CrashSeam::AfterLocalApply);
        if entry
            .seq
            .is_multiple_of(self.refresh_state.checkpoint_every)
        {
            self.refresh_state.verified = None;
            self.refresh().await?;
            if let Err(error) = self.local.checkpoint(&self.journal).await {
                self.owns = false;
                return Err(error);
            }
            self.refresh_state.verified = None;
        }
        Ok(entry)
    }

    pub async fn commit(&mut self, transition: Transition) -> Result<Entry> {
        if !self.owns {
            return Err(Error::Observer);
        }
        if matches!(&transition, Transition::Progress { stream, .. } if stream.starts_with("buffer-gc:"))
        {
            return Err(Error::Rejected(
                "buffer GC facts require the dedicated authority methods".into(),
            ));
        }
        if matches!(
            transition,
            Transition::AcquireOwnership { .. }
                | Transition::Intent { .. }
                | Transition::ResumeEffect { .. }
                | Transition::EffectCompleted { .. }
                | Transition::ReverseSetup { .. }
                | Transition::ReverseConsistentPoint { .. }
        ) {
            return Err(Error::Rejected(
                "ownership and effects require their dedicated methods".into(),
            ));
        }
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        self.commit_inner(transition).await
    }

    pub async fn effect<F, Fut>(
        &mut self,
        effect: EffectKind,
        payload: String,
        run: F,
    ) -> Result<()>
    where
        F: FnOnce(EffectToken) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if !self.owns {
            return Err(Error::Observer);
        }
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        let intent = self
            .commit_inner(Transition::Intent {
                effect: effect.clone(),
                payload: payload.clone(),
            })
            .await?;
        self.execute_intent(intent, run).await
    }

    /// Retry an unacknowledged effect in the same owned epoch.
    pub async fn retry_effect<F, Fut>(&mut self, seq: u64, run: F) -> Result<()>
    where
        F: FnOnce(EffectToken) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if !self.owns {
            return Err(Error::Observer);
        }
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        if seq == 0 || seq > self.state().applied_seq {
            return Err(Error::Rejected("intent is missing".into()));
        }
        let intent = self.journal.entry(seq).await?;
        if !matches!(
            intent.transition,
            Transition::Intent { .. } | Transition::ResumeEffect { .. }
        ) {
            return Err(Error::Rejected("not an effect intent".into()));
        }
        if intent.epoch != self.state().epoch || intent.coordinator_instance != *self.instance {
            return Err(Error::LostOwnership);
        }
        let root = self.original_intent(seq).await?;
        if self.effect_completed(root.seq).await? {
            return Ok(());
        }
        self.execute_intent(intent, run).await
    }

    async fn original_intent(&self, mut seq: u64) -> Result<Entry> {
        if seq == 0 || seq > self.state().applied_seq {
            return Err(Error::Rejected("intent is missing".into()));
        }
        loop {
            let entry = self.journal.entry(seq).await?;
            if self
                .state()
                .audit
                .get(&seq)
                .is_some_and(|known| known != &entry)
            {
                return Err(Error::RecoveryRequired(
                    "journal intent differs from retained active intent".into(),
                ));
            }
            match entry.transition {
                Transition::Intent { .. } => return Ok(entry),
                Transition::ResumeEffect { intent_seq } if intent_seq < seq => seq = intent_seq,
                _ => return Err(Error::Rejected("not an effect intent".into())),
            }
        }
    }

    async fn effect_completed(&self, root: u64) -> Result<bool> {
        let mut after = root;
        while after < self.state().applied_seq {
            let page = self
                .journal
                .history_page_through(after, 128, self.state().applied_seq)
                .await?;
            if page.is_empty() {
                return Err(Error::RecoveryRequired(
                    "effect history is incomplete".into(),
                ));
            }
            for entry in page {
                if entry.seq > self.state().applied_seq {
                    break;
                }
                after = entry.seq;
                if let Transition::EffectCompleted { intent_seq } = entry.transition
                    && self.original_intent(intent_seq).await?.seq == root
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Journal adoption before retrying an earlier owner's operation. Its original
    /// identity survives failover; only the database authority epoch advances.
    pub async fn resume_effect<F, Fut>(&mut self, seq: u64, run: F) -> Result<()>
    where
        F: FnOnce(EffectToken) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if !self.owns {
            return Err(Error::Observer);
        }
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        let root = self.original_intent(seq).await?;
        if self.effect_completed(root.seq).await? {
            return Ok(());
        }
        let seq = self
            .state()
            .intent_roots
            .iter()
            .find(|(_, value)| **value == root.seq)
            .map(|(seq, _)| *seq)
            .ok_or_else(|| Error::Rejected("active intent is missing".into()))?;
        let prior = self.journal.entry(seq).await?;
        if prior.epoch == self.state().epoch {
            return self.retry_effect(seq, run).await;
        }
        let adoption = self
            .commit_inner(Transition::ResumeEffect { intent_seq: seq })
            .await?;
        self.execute_intent(adoption, run).await
    }

    async fn execute_intent<F, Fut>(&mut self, intent: Entry, run: F) -> Result<()>
    where
        F: FnOnce(EffectToken) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if intent.epoch != self.state().epoch || intent.coordinator_instance != *self.instance {
            return Err(Error::LostOwnership);
        }
        let root = self.original_intent(intent.seq).await?;
        let Transition::Intent { effect, payload } = root.transition else {
            return Err(Error::Rejected("not an effect intent".into()));
        };
        // Recheck ownership after the durable intent and before touching a peer.
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        let outcome = run(EffectToken {
            migration: self.journal.namespace().into(),
            authority_epoch: intent.epoch,
            epoch: root.epoch,
            seq: root.seq,
            effect,
            payload,
        })
        .await;
        if matches!(
            outcome,
            Err(Error::LostOwnership | Error::UnknownOutcome(_))
        ) {
            self.owns = false;
        }
        outcome?;
        #[cfg(feature = "test-crash")]
        self.crash_at(super::CrashSeam::AfterEffect);
        self.commit_inner(Transition::EffectCompleted {
            intent_seq: intent.seq,
        })
        .await?;
        Ok(())
    }

    #[cfg(feature = "test-crash")]
    pub fn set_crash_seam(&mut self, seam: super::CrashSeam) {
        self.crash = Some(seam);
    }
    #[cfg(feature = "test-crash")]
    pub(super) fn crash_at(&self, seam: super::CrashSeam) {
        if self.crash == Some(seam) {
            std::process::exit(86);
        }
    }
}

/// Initialize only in explicit connector setup, on BOTH source and destination.
/// P10/P11 own fence/activation SQL; P2 supplies its transactional authority gate.
pub async fn initialize_pg_authority(client: &tokio_postgres::Client) -> Result<()> {
    client.batch_execute("BEGIN").await?;
    let result = async {
        let rows = client
            .query(crate::pg::ddl::ADMIN_INSTALLATION_SQL, &[])
            .await?
            .into_iter()
            .map(|row| vec![row.get::<_, String>(0)])
            .collect();
        let origin = crate::pg::ddl::administrative_origin_sql(&rows)
            .map_err(|e| Error::Rejected(e.to_string()))?;
        client.batch_execute(&format!("{origin}CREATE SCHEMA IF NOT EXISTS _skedaddle;
            CREATE TABLE IF NOT EXISTS _skedaddle.authority (migration text PRIMARY KEY, epoch bigint NOT NULL);
            CREATE TABLE IF NOT EXISTS _skedaddle.effects (migration text NOT NULL, epoch bigint NOT NULL, seq bigint NOT NULL, payload text NOT NULL, PRIMARY KEY(migration,epoch,seq)); COMMIT;")).await?;
        Ok(())
    }.await;
    if result.is_err() {
        let _ = client.batch_execute("ROLLBACK").await;
    }
    result
}

/// The authority update, idempotency ledger, and SQL effect share one PG transaction.
/// A stale epoch cannot mutate either the ledger or business state.
pub async fn apply_pg_effect(
    client: &mut tokio_postgres::Client,
    token: EffectToken,
) -> Result<()> {
    apply_pg_effect_inner(client, token, || async { Ok(()) }).await
}

/// Fault setup immediately before COMMIT, available only in test builds.
#[cfg(feature = "test-crash")]
pub async fn apply_pg_effect_with_commit_hook<F, Fut>(
    client: &mut tokio_postgres::Client,
    token: EffectToken,
    before_commit: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    apply_pg_effect_inner(client, token, before_commit).await
}

async fn apply_pg_effect_inner<F, Fut>(
    client: &mut tokio_postgres::Client,
    token: EffectToken,
    before_commit: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let epoch: i64 = token
        .epoch
        .try_into()
        .map_err(|_| Error::Rejected("PG epoch overflow".into()))?;
    let seq: i64 = token
        .seq
        .try_into()
        .map_err(|_| Error::Rejected("PG seq overflow".into()))?;
    let authority_epoch: i64 = token
        .authority_epoch
        .try_into()
        .map_err(|_| Error::Rejected("PG authority epoch overflow".into()))?;
    let tx = client.transaction().await?;
    let rows = tx
        .query(crate::pg::ddl::ADMIN_INSTALLATION_SQL, &[])
        .await?
        .into_iter()
        .map(|row| vec![row.get::<_, String>(0)])
        .collect();
    let origin = crate::pg::ddl::administrative_origin_sql(&rows)
        .map_err(|e| Error::Rejected(e.to_string()))?;
    if !origin.is_empty() {
        tx.batch_execute(&origin).await?;
    }
    tx.execute(
        "INSERT INTO _skedaddle.authority VALUES ($1,0) ON CONFLICT DO NOTHING",
        &[&token.migration],
    )
    .await?;
    if tx
        .execute(
            "UPDATE _skedaddle.authority SET epoch=$1 WHERE migration=$2 AND epoch <= $1",
            &[&authority_epoch, &token.migration],
        )
        .await?
        != 1
    {
        return Err(Error::LostOwnership);
    }
    let existing = tx
        .query_opt(
            "SELECT payload FROM _skedaddle.effects WHERE migration=$1 AND epoch=$2 AND seq=$3",
            &[&token.migration, &epoch, &seq],
        )
        .await?;
    if let Some(row) = existing {
        if row.get::<_, String>(0) != token.payload {
            return Err(Error::RecoveryRequired(
                "effect key reused with different SQL".into(),
            ));
        }
    } else {
        tx.batch_execute(&token.payload).await?;
        tx.execute(
            "INSERT INTO _skedaddle.effects VALUES ($1,$2,$3,$4)",
            &[&token.migration, &epoch, &seq, &token.payload],
        )
        .await?;
    }
    before_commit().await?;
    tx.commit()
        .await
        .map_err(|e| Error::UnknownOutcome(format!("PG COMMIT reply: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn poisoned_local_sync_never_publishes_trusted_candidate() {
        use std::time::Duration;
        let root =
            crate::test_root::temp_root().join(format!("refresh-poison-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        // TEMP may use the J: alias on Linux; confinement needs a native absolute root.
        let root = root.canonicalize().unwrap();
        let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap());
        let j = Journal::new(store, "p2", "poison", Duration::from_secs(2)).unwrap();
        let mut c =
            Coordinator::observer(j.clone(), Local::create(root.join("db")).unwrap(), "owner")
                .unwrap();
        c.acquire().await.unwrap();
        c.refresh().await.unwrap();
        let progress = |position: &str| Transition::Progress {
            stream: "s".into(),
            marker: "m".into(),
            position: position.into(),
        };
        let real = Entry::new(c.state(), progress("real"), c.instance_id()).unwrap();
        let forged = Entry::new(c.state(), progress("poison"), c.instance_id()).unwrap();
        j.put(&real).await.unwrap();
        c.local.apply(&forged).unwrap();
        assert!(c.refresh().await.is_err());
        assert!(c.refresh_state.verified.is_none());
        assert!(!c.is_owner());
        assert_ne!(c.state(), &j.load(true).await.unwrap());
    }

    #[tokio::test]
    async fn periodic_refresh_clock_detects_old_tampering_even_behind_checkpoint() {
        use object_store::ObjectStoreExt;
        use std::time::{Duration, Instant};
        let root =
            crate::test_root::temp_root().join(format!("refresh-clock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        // TEMP may use the J: alias on Linux; confinement needs a native absolute root.
        let root = root.canonicalize().unwrap();
        let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap());
        let j = Journal::new(store.clone(), "p2", "periodic", Duration::from_secs(2)).unwrap();
        let mut c =
            Coordinator::observer(j.clone(), Local::create(root.join("db")).unwrap(), "owner")
                .unwrap();
        c.acquire().await.unwrap();
        c.commit(Transition::Progress {
            stream: "s".into(),
            marker: "m".into(),
            position: "2".into(),
        })
        .await
        .unwrap();
        c.checkpoint().await.unwrap();
        c.refresh().await.unwrap();
        store
            .put(&j.entry_key(1), b"corrupt".to_vec().into())
            .await
            .unwrap();
        c.refresh().await.unwrap(); // Old immutable object is outside the suffix.
        assert!(
            c.refresh_at(Instant::now() + Duration::from_secs(301))
                .await
                .is_err()
        );
        assert!(!c.is_owner());
        assert!(c.refresh_state.verified.is_none());
    }

    #[tokio::test]
    async fn intent_executor_rejects_each_stale_capability_field() {
        use std::{
            sync::Arc,
            time::{Duration, SystemTime, UNIX_EPOCH},
        };
        let base = crate::test_root::temp_root();
        crate::test_root::assert_under_work_root(&base);
        let root = base.join(format!(
            "intent-unit-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&root)
                .unwrap()
                .with_automatic_cleanup(false)
                .with_fsync(true),
        );
        let journal = Journal::new(store, "p2", "intent", Duration::from_secs(2)).unwrap();
        let local = Local::create(root.join("db")).unwrap();
        let mut owner = Coordinator::observer(journal, local, "owner").unwrap();
        owner.acquire().await.unwrap();
        owner
            .effect(EffectKind::Fence, "intent".into(), |_| async { Ok(()) })
            .await
            .unwrap();
        let old_intent = owner.journal.entry(2).await.unwrap();
        owner.acquire().await.unwrap();
        assert!(matches!(
            owner
                .execute_intent(old_intent, |_| async { panic!("old epoch executed") })
                .await,
            Err(Error::LostOwnership)
        ));
        let foreign = Entry::new(
            owner.state(),
            Transition::Intent {
                effect: EffectKind::Fence,
                payload: "intent".into(),
            },
            "foreign",
        )
        .unwrap();
        assert!(matches!(
            owner
                .execute_intent(foreign, |_| async { panic!("foreign instance executed") })
                .await,
            Err(Error::LostOwnership)
        ));
    }
}

#[cfg(test)]
mod administrative_pg_tests {
    use super::*;
    use crate::pg::{
        ddl::{self, Classification, SchemaVersions, TriggerSql},
        pgoutput::{Decoder, Message},
    };
    use futures::FutureExt;
    use std::io::Write;

    #[tokio::test]
    async fn authority_and_effect_transactions_tag_live_capture_and_public_ddl_still_stops() {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            writeln!(std::io::stderr(), "LOUD SKIP authority_and_effect_transactions_tag_live_capture_and_public_ddl_still_stops: SKEDADDLE_TEST_CONFIG unset; requires disposable PG16/17/18 superuser with CREATEDB and logical WAL; no Docker started").unwrap();
            return;
        };
        let config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let dsn = std::env::var("SKEDADDLE_PG_ACTIVATE_DSN")
            .ok()
            .or_else(|| {
                config["env"]["SKEDADDLE_PG_ACTIVATE_DSN"]
                    .as_str()
                    .or_else(|| config["pg"].as_str())
                    .map(str::to_owned)
            })
            .expect("harness PG admin DSN");
        let ca = config["ca_pem"]
            .as_str()
            .or_else(|| config["replication"]["ca_pem"].as_str())
            .expect("harness CA");
        let postgres: tokio_postgres::Config = dsn.parse().unwrap();
        let connect = |postgres| crate::pg::tls::Config::new(postgres, ca);
        let (admin, driver, _) = crate::pg::tls::connect(
            &connect(postgres.clone()),
            &crate::security::TlsPolicy::Required,
        )
        .await
        .unwrap();
        tokio::spawn(driver);
        let db = format!(
            "d9_authority_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        );
        admin
            .batch_execute(&format!("CREATE DATABASE {db}"))
            .await
            .unwrap();
        let result = std::panic::AssertUnwindSafe(async {
            let mut postgres = postgres;
            postgres.dbname(&db);
            let (mut client, driver, _) = crate::pg::tls::connect(&connect(postgres), &crate::security::TlsPolicy::Required).await.map_err(|e| Error::Rejected(e.to_string()))?;
            tokio::spawn(driver);
            initialize_pg_authority(&client).await?; // Initial setup without installation.
            let id = format!("d9e_{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
            client.batch_execute(&format!("CREATE TABLE public.mapped(id int); CREATE PUBLICATION {id} FOR TABLE public.mapped;")).await?;
            client.simple_query(&format!("SELECT slot_name FROM pg_create_logical_replication_slot('{id}','pgoutput')")).await?;
            let version: String = client.query_one("SHOW server_version_num", &[]).await?.get(0);
            let trigger = TriggerSql::new(&id, version.parse().unwrap()).unwrap();
            client.batch_execute(&trigger.install).await?;
            let version: i64 = client.query_one(&format!("SELECT version FROM _skedaddle.skedaddle_ddl_{id}_version"), &[]).await?.get(0);
            let changes = format!("SELECT data FROM pg_logical_slot_get_binary_changes('{id}',NULL,NULL,'proto_version','1','publication_names','{id}','messages','true')");
            client.query(&changes, &[]).await?;
            let mut versions = SchemaVersions::new(id.clone(), version as u64, []).unwrap();
            initialize_pg_authority(&client).await?;
            let token = |seq, payload: &str| EffectToken { migration: "d9".into(), authority_epoch: 1, epoch: 1, seq, effect: EffectKind::Fence, payload: payload.into() };
            apply_pg_effect(&mut client, token(1, &format!("SELECT 1 / CASE WHEN current_setting('skedaddle.admin_origin',true)='{id}' THEN 1 ELSE 0 END; CREATE TABLE _skedaddle.d9_effect(id int)"))).await?;
            let mut records = vec![];
            for row in client.query(&changes, &[]).await? {
                let bytes: Vec<u8> = row.get(0);
                let message = Decoder::default().decode(&bytes).unwrap();
                if let Message::LogicalMessage { prefix: ddl::MESSAGE_PREFIX, .. } = message { records.push(ddl::decode(&message, &id).unwrap()); }
            }
            assert!(records.iter().any(|record| record.query.contains("CREATE SCHEMA")));
            assert!(records.iter().any(|record| record.query.contains("d9_effect")));
            let mut start = 0;
            while start < records.len() {
                let end = (start + 1..records.len()).find(|&i| records[i].xid != records[start].xid).unwrap_or(records.len());
                assert!(records[start..end].iter().all(|record| record.origin.as_deref() == Some(id.as_str())));
                assert!(matches!(versions.classify_transaction(&records[start..end]), Classification::Supported { .. }));
                start = end;
            }
            for (seq, payload) in [(2, "CREATE TABLE public.d9_public(id int)"), (3, "CREATE PUBLICATION d9_global FOR TABLE public.mapped")] {
                apply_pg_effect(&mut client, token(seq, payload)).await?;
                let mut records = vec![];
                for row in client.query(&changes, &[]).await? {
                    let bytes: Vec<u8> = row.get(0);
                    let message = Decoder::default().decode(&bytes).unwrap();
                    if let Message::LogicalMessage { prefix: ddl::MESSAGE_PREFIX, .. } = message { records.push(ddl::decode(&message, &id).unwrap()); }
                }
                assert!(!records.is_empty());
                assert!(records.iter().all(|record| record.origin.as_deref() == Some(id.as_str())));
                let mut next = versions.clone();
                assert!(matches!(next.classify_transaction(&records), Classification::SafeBoundaryStop(_)));
                // The public record stops apply, but the next test needs a fresh baseline.
                versions = SchemaVersions::new(id.clone(), records.last().unwrap().version, []).unwrap();
            }
            Ok::<(), Error>(())
        }).catch_unwind().await;
        admin
            .batch_execute(&format!("DROP DATABASE {db} WITH (FORCE)"))
            .await
            .unwrap();
        match result {
            Ok(result) => result.unwrap(),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}
