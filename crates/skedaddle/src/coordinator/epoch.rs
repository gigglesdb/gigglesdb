//! Ownership and journal-before-effect ordering, with PostgreSQL fencing.

use std::future::Future;

use super::{
    EffectKind, Error, Materialized, Result, Transition,
    journal::{Entry, Journal},
    local::Local,
};

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

pub struct Coordinator {
    journal: Journal,
    local: Local,
    instance: String,
    owns: bool,
    checkpoint_every: u64,
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
            instance: format!("{instance}/{}", uuid::Uuid::new_v4()),
            owns: false,
            checkpoint_every: 1000,
            #[cfg(feature = "test-crash")]
            crash: None,
        })
    }
    pub fn state(&self) -> &Materialized {
        self.local.state()
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
        self.checkpoint_every = transitions;
        Ok(self)
    }
    pub async fn checkpoint(&mut self) -> Result<()> {
        if !self.owns {
            return Err(Error::Observer);
        }
        self.refresh().await?;
        if !self.owns {
            return Err(Error::LostOwnership);
        }
        let result = self.local.checkpoint(&self.journal).await;
        if result.is_err() {
            self.owns = false;
        }
        result
    }
    pub async fn refresh(&mut self) -> Result<()> {
        let state = match self.journal.load(true).await {
            Ok(state) => state,
            Err(error) => {
                self.owns = false;
                return Err(error);
            }
        };
        if self.owns && (state.owner != self.instance || state.epoch != self.state().epoch) {
            self.owns = false;
        }
        let result = self.local.replay(&state);
        if result.is_err() {
            self.owns = false;
        }
        result
    }

    pub async fn acquire(&mut self) -> Result<u64> {
        self.owns = false;
        self.refresh().await?;
        let epoch = self
            .state()
            .epoch
            .checked_add(1)
            .ok_or_else(|| Error::Rejected("epoch exhausted".into()))?;
        self.commit_inner(Transition::AcquireOwnership { epoch })
            .await?;
        self.owns = true;
        Ok(epoch)
    }

    /// Captured journal state makes racing owners compete for the same seq.
    /// A failed or uncertain put always revokes this process's write capability.
    async fn commit_inner(&mut self, transition: Transition) -> Result<Entry> {
        let entry = Entry::new(self.state(), transition, &self.instance)?;
        self.local.validate_transition(&entry)?;
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
        if entry.seq.is_multiple_of(self.checkpoint_every)
            && let Err(error) = self.local.checkpoint(&self.journal).await
        {
            self.owns = false;
            return Err(error);
        }
        Ok(entry)
    }

    pub async fn commit(&mut self, transition: Transition) -> Result<Entry> {
        if !self.owns {
            return Err(Error::Observer);
        }
        if matches!(
            transition,
            Transition::AcquireOwnership { .. }
                | Transition::Intent { .. }
                | Transition::ResumeEffect { .. }
                | Transition::EffectCompleted { .. }
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
        let intent = self
            .state()
            .audit
            .get(&seq)
            .cloned()
            .ok_or_else(|| Error::Rejected("intent is missing".into()))?;
        if intent.epoch != self.state().epoch || intent.coordinator_instance != self.instance {
            return Err(Error::LostOwnership);
        }
        if self.state().audit.values().any(
            |e| matches!(e.transition,Transition::EffectCompleted{intent_seq} if intent_seq==seq),
        ) {
            return Ok(());
        }
        self.execute_intent(intent, run).await
    }

    fn original_intent(&self, mut seq: u64) -> Result<Entry> {
        loop {
            let entry = self
                .state()
                .audit
                .get(&seq)
                .ok_or_else(|| Error::Rejected("intent is missing".into()))?;
            match entry.transition {
                Transition::Intent { .. } => return Ok(entry.clone()),
                Transition::ResumeEffect { intent_seq } if intent_seq < seq => seq = intent_seq,
                _ => return Err(Error::Rejected("not an effect intent".into())),
            }
        }
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
        let root = self.original_intent(seq)?;
        for entry in self.state().audit.values() {
            if let Transition::EffectCompleted { intent_seq } = entry.transition
                && self.original_intent(intent_seq)?.seq == root.seq
            {
                return Ok(());
            }
        }
        let prior = &self.state().audit[&seq];
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
        if intent.epoch != self.state().epoch || intent.coordinator_instance != self.instance {
            return Err(Error::LostOwnership);
        }
        let root = self.original_intent(intent.seq)?;
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
    fn crash_at(&self, seam: super::CrashSeam) {
        if self.crash == Some(seam) {
            std::process::exit(86);
        }
    }
}

/// Initialize only in explicit connector setup, on BOTH source and destination.
/// P10/P11 own fence/activation SQL; P2 supplies its transactional authority gate.
pub async fn initialize_pg_authority(client: &tokio_postgres::Client) -> Result<()> {
    client.batch_execute("CREATE SCHEMA IF NOT EXISTS _skedaddle;
        CREATE TABLE IF NOT EXISTS _skedaddle.authority (migration text PRIMARY KEY, epoch bigint NOT NULL);
        CREATE TABLE IF NOT EXISTS _skedaddle.effects (migration text NOT NULL, epoch bigint NOT NULL, seq bigint NOT NULL, payload text NOT NULL, PRIMARY KEY(migration,epoch,seq));").await?;
    Ok(())
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
    async fn intent_executor_rejects_each_stale_capability_field() {
        use std::{
            path::PathBuf,
            sync::Arc,
            time::{Duration, SystemTime, UNIX_EPOCH},
        };
        let base = PathBuf::from(std::env::var_os("TEMP").expect("set task TEMP on J:"));
        assert!(base.starts_with("J:/gigglesdb-work"));
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
        let old_intent = owner.state().audit[&2].clone();
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
