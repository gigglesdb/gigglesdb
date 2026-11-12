//! Destination fence/activation, sequence synchronization and activation blockers.
//!
//! The caller durably stores every [`Intent`] before returning from the journal
//! callback and resumes the latest stored state. Database operations are
//! retryable; a post-activate command must also be idempotent (its stable key is
//! `SKEDADDLE_ACTIVATION_EPOCH`). Administrative changes outside this module must
//! be excluded while the fence is held. Observations prove the fence at the
//! recorded points, not the absence of an intervening privileged administrator.
//! Cutover orchestration and establishing the final boundary belong to callers.

use std::{
    collections::BTreeSet,
    fmt,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use super::fence::{
    Catalog, Connection, Grantee, Privilege, Rows, SavedState, Table, effective_writers,
    reachable_roles, restoration_sql,
};

mod catalog;
mod checks;
mod sequences;
pub use checks::{Blocker, BlockerKind, inspect_validity};
pub use sequences::{
    Sequence, SequenceName, SequenceScope, SequenceState, discover_sequences, read_sequence_states,
    sequence_sync_sql, sync_sequences,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookCommand {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub epoch: i64,
    pub applications: BTreeSet<String>,
    pub tables: Vec<Table>,
    pub timeout_ms: u64,
    pub post_activate: Option<HookCommand>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    Save,
    Fence,
    Drain,
    VerifyFence,
    Fenced,
    CheckActivation,
    Restore,
    VerifyActivation,
    PostActivate,
    Complete,
}

impl Step {
    fn next(self) -> Self {
        match self {
            Self::Save => Self::Fence,
            Self::Fence => Self::Drain,
            Self::Drain => Self::VerifyFence,
            Self::VerifyFence | Self::Fenced => Self::Fenced,
            Self::CheckActivation => Self::Restore,
            Self::Restore => Self::VerifyActivation,
            Self::VerifyActivation => Self::PostActivate,
            Self::PostActivate | Self::Complete => Self::Complete,
        }
    }
}

/// A complete catalog observation, retained for the abort proof. `ordinal`
/// orders repeated observations without depending on the wall clock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceEvidence {
    pub ordinal: usize,
    pub observed_at: String,
    pub catalog: Catalog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationFence {
    pub config: Config,
    pub next: Step,
    pub saved: Option<SavedState>,
    /// Optional destination provisioning manifest, distinct from the observed
    /// initial state. Allows roles to be born NOLOGIN without any write grants.
    pub activation_state: Option<SavedState>,
    pub evidence: Vec<FenceEvidence>,
    pub final_boundary: Option<String>,
    pub acknowledged_triggers: BTreeSet<String>,
    /// Process-local journal capability; native snapshots never restore authority.
    #[serde(skip)]
    reverse_proof: Option<Box<crate::coordinator::reverse::Authenticated>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub state: DestinationFence,
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    Database(String),
    Journal(String),
    Hook(String),
    FenceBreached(String),
    Blocked(Vec<Blocker>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "destination activation: {self:?}")
    }
}
impl std::error::Error for Error {}

impl DestinationFence {
    /// Attach a freshly replayed P11a capability under the cutover owner guard.
    /// Serialization intentionally discards it; restart must authenticate again.
    pub fn attach_reverse_proof(&mut self, proof: crate::coordinator::reverse::Authenticated) {
        self.reverse_proof = Some(Box::new(proof));
    }
    pub(crate) fn reverse_proof(&self) -> Option<&crate::coordinator::reverse::Authenticated> {
        self.reverse_proof.as_deref()
    }
    pub(crate) fn clear_reverse_proof(&mut self) {
        self.reverse_proof = None;
    }

    /// Existing roles and grants are saved and restored exactly. Provision new
    /// roles before calling; originally NOLOGIN roles remain NOLOGIN afterwards.
    pub fn new(config: Config) -> Result<Self, Error> {
        validate_config(&config)?;
        Ok(Self {
            config,
            next: Step::Save,
            saved: None,
            activation_state: None,
            evidence: vec![],
            final_boundary: None,
            acknowledged_triggers: BTreeSet::new(),
            reverse_proof: None,
        })
    }

    /// Use when application roles were provisioned NOLOGIN with no write ACLs.
    /// Save verifies that initial fence, then activation installs this complete
    /// desired ACL/login state. Owners, memberships and non-login attributes
    /// must already match: activation does not silently change those properties.
    pub fn with_activation_state(
        config: Config,
        activation_state: SavedState,
    ) -> Result<Self, Error> {
        validate_roles(&activation_state.catalog, &config.applications)?;
        let mut fence = Self::new(config)?;
        fence.activation_state = Some(activation_state);
        Ok(fence)
    }

    /// Pure SQL planning for the two mutating phases. Restoration includes
    /// grantors, grant options, columns, PUBLIC and inherited write grants.
    pub fn planned_sql(&self) -> Result<Option<String>, Error> {
        self.validate_resume()?;
        let timeout = self.timeout();
        match self.next {
            Step::Fence => Ok(Some(transaction(&self.fence_sql()?, timeout))),
            Step::Restore => Ok(Some(transaction(
                &restoration_sql(self.restoration_target()?, &self.config.applications)
                    .map_err(|e| Error::Invalid(e.to_string()))?,
                timeout,
            ))),
            _ => Ok(None),
        }
    }

    /// Stops at Fenced until begin_activation is explicitly requested. Failures
    /// retain the current step; replay of an ambiguous SQL commit is safe.
    pub fn execute_next(
        &mut self,
        connection: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
    ) -> Result<(), Error> {
        self.validate_resume()?;
        if matches!(self.next, Step::Fenced | Step::Complete) {
            return Ok(());
        }
        self.journal(journal)?;
        let timeout = self.timeout();
        match self.next {
            Step::Save => {
                if self.saved.is_none() {
                    let catalog = catalog::read(connection, &self.config.tables, timeout)?;
                    validate_roles(&catalog, &self.config.applications)?;
                    if let Some(activation) = &self.activation_state {
                        validate_fenced(&catalog, &self.config.applications)?;
                        self.assert_drained(connection)?;
                        validate_activation_state(
                            &catalog,
                            &activation.catalog,
                            &self.config.applications,
                        )?;
                    }
                    // Ownership, superuser and predefined write-role paths
                    // cannot be fenced by ACL revocation; never acknowledge them.
                    let mut without_acl = catalog.clone();
                    without_acl.grants.clear();
                    if !effective_writers(&without_acl).is_disjoint(&self.config.applications) {
                        return Err(Error::Invalid(
                            "application has an ownership, superuser or pg_write_all_data path"
                                .into(),
                        ));
                    }
                    self.saved = Some(SavedState {
                        catalog,
                        authority: None,
                    });
                }
                // Snapshot must survive a crash before the first ALTER/REVOKE.
                self.journal(journal)?;
            }
            Step::Fence | Step::Restore => {
                let origin = super::ddl::administrative_origin(connection, timeout)
                    .map_err(|e| Error::Database(e.to_string()))?;
                let sql = self.planned_sql()?.expect("SQL step");
                // Keep planning pure and preserve ActivationConnection's COMMIT suffix.
                let sql = sql.replacen("BEGIN; ", &format!("BEGIN; {origin}"), 1);
                query(connection, &sql, timeout)?;
            }
            Step::Drain => {
                query(
                    connection,
                    &format!(
                        "SELECT pg_terminate_backend(pid)::text FROM pg_stat_activity WHERE datname=current_database() AND usename IN ({}) AND pid<>pg_backend_pid()",
                        names(&self.config.applications)
                    ),
                    timeout,
                )?;
                self.assert_drained(connection)?;
            }
            Step::VerifyFence => self.observe(connection, journal)?,
            Step::CheckActivation => {
                self.observe(connection, journal)?;
                let blockers = inspect_validity(
                    connection,
                    &self.config.tables,
                    &self.acknowledged_triggers,
                    timeout,
                )?;
                let blocking: Vec<_> = blockers.into_iter().filter(Blocker::blocks).collect();
                if !blocking.is_empty() {
                    return Err(Error::Blocked(blocking));
                }
            }
            Step::VerifyActivation => {
                let current = catalog::read(connection, &self.config.tables, timeout)?;
                let saved = &self.restoration_target()?.catalog;
                let mut actual = current.grants.clone();
                let mut expected = saved.grants.clone();
                actual.sort();
                expected.sort();
                if actual != expected
                    || saved
                        .roles
                        .iter()
                        .filter(|r| self.config.applications.contains(&r.name))
                        .any(|r| !current.roles.contains(r))
                {
                    return Err(Error::Invalid(
                        "activation differs from saved roles/ACLs".into(),
                    ));
                }
            }
            Step::PostActivate => self.run_hook()?,
            Step::Fenced | Step::Complete => unreachable!(),
        }
        self.next = self.next.next();
        Ok(())
    }

    /// Record another fence observation immediately before abort, or during a
    /// long copy. No successful observation is discarded when activation starts.
    pub fn observe(
        &mut self,
        connection: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
    ) -> Result<(), Error> {
        self.validate_resume()?;
        if !matches!(
            self.next,
            Step::VerifyFence | Step::Fenced | Step::CheckActivation
        ) {
            return Err(Error::Invalid(
                "fence observation outside fenced interval".into(),
            ));
        }
        self.assert_drained(connection)?;
        let catalog = catalog::read(connection, &self.config.tables, self.timeout())?;
        validate_fenced(&catalog, &self.config.applications)?;
        let rows = query(connection, "SELECT clock_timestamp()::text", self.timeout())?;
        let evidence = FenceEvidence {
            ordinal: self.evidence.len(),
            observed_at: cell(
                rows.first()
                    .ok_or_else(|| Error::Invalid("missing clock".into()))?,
                0,
            )?
            .into(),
            catalog,
        };
        let mut recorded = self.clone();
        recorded.evidence.push(evidence);
        recorded.journal(journal)?;
        *self = recorded;
        Ok(())
    }

    /// Caller attests that capture, apply and verification passed at this final
    /// marker, sequences were synchronized, and reverse prerequisites are ready.
    /// Repeating the same request after an ambiguous outcome is a no-op.
    pub fn begin_activation(
        &mut self,
        final_boundary: String,
        acknowledged_triggers: BTreeSet<String>,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
    ) -> Result<(), Error> {
        self.validate_resume()?;
        if self.final_boundary.as_ref() == Some(&final_boundary)
            && self.acknowledged_triggers == acknowledged_triggers
        {
            return Ok(());
        }
        if self.next != Step::Fenced || self.evidence.is_empty() || final_boundary.is_empty() {
            return Err(Error::Invalid(
                "activation requires a verified fence and final boundary".into(),
            ));
        }
        let mut ready = self.clone();
        ready.final_boundary = Some(final_boundary);
        ready.acknowledged_triggers = acknowledged_triggers;
        ready.next = Step::CheckActivation;
        ready.journal(journal)?;
        *self = ready;
        Ok(())
    }

    fn saved(&self) -> Result<&SavedState, Error> {
        self.saved
            .as_ref()
            .ok_or_else(|| Error::Invalid("resume requires saved state".into()))
    }

    fn restoration_target(&self) -> Result<&SavedState, Error> {
        match &self.activation_state {
            Some(target) => Ok(target),
            None => self.saved(),
        }
    }

    fn validate_resume(&self) -> Result<(), Error> {
        validate_config(&self.config)?;
        if self.next != Step::Save {
            validate_roles(&self.saved()?.catalog, &self.config.applications)?;
            if let Some(target) = &self.activation_state {
                validate_activation_state(
                    &self.saved()?.catalog,
                    &target.catalog,
                    &self.config.applications,
                )?;
            }
        }
        if matches!(
            self.next,
            Step::Fenced
                | Step::CheckActivation
                | Step::Restore
                | Step::VerifyActivation
                | Step::PostActivate
                | Step::Complete
        ) && self.evidence.is_empty()
        {
            return Err(Error::Invalid("resume requires fence evidence".into()));
        }
        if matches!(
            self.next,
            Step::CheckActivation
                | Step::Restore
                | Step::VerifyActivation
                | Step::PostActivate
                | Step::Complete
        ) && self.final_boundary.as_ref().is_none_or(String::is_empty)
        {
            return Err(Error::Invalid("resume requires final boundary".into()));
        }
        Ok(())
    }

    fn journal(
        &self,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
    ) -> Result<(), Error> {
        journal(&Intent {
            state: self.clone(),
        })
        .map_err(Error::Journal)
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeout_ms)
    }

    fn fence_sql(&self) -> Result<String, Error> {
        let saved = self.saved()?;
        let reached: BTreeSet<_> = self
            .config
            .applications
            .iter()
            .flat_map(|r| reachable_roles(r, &saved.catalog.memberships))
            .collect();
        let mut sql: String = self
            .config
            .applications
            .iter()
            .map(|r| format!("ALTER ROLE {} NOLOGIN; ", ident(r)))
            .collect();
        for grant in &saved.catalog.grants {
            if matches!(&grant.grantee, Grantee::Role(r) if !reached.contains(r)) {
                continue;
            }
            let privilege = match grant.privilege {
                Privilege::Insert => "INSERT",
                Privilege::Update => "UPDATE",
                Privilege::Delete => "DELETE",
                Privilege::Truncate => "TRUNCATE",
            };
            let column = grant
                .column
                .as_ref()
                .map(|c| format!(" ({})", ident(c)))
                .unwrap_or_default();
            let grantee = match &grant.grantee {
                Grantee::Public => "PUBLIC".into(),
                Grantee::Role(r) => ident(r),
            };
            sql.push_str(&format!(
                "SET LOCAL ROLE {}; REVOKE {privilege}{column} ON TABLE {} FROM {grantee} CASCADE; RESET ROLE; ",
                ident(&grant.grantor), table_sql(&grant.table),
            ));
        }
        Ok(sql)
    }

    fn assert_drained(&self, connection: &mut impl Connection) -> Result<(), Error> {
        let rows = query(
            connection,
            &format!(
                "SELECT 'backend',pid::text FROM pg_stat_activity WHERE datname=current_database() AND usename IN ({}) UNION ALL SELECT 'prepared',gid FROM pg_prepared_xacts WHERE database=current_database()",
                names(&self.config.applications)
            ),
            self.timeout(),
        )?;
        if !rows.is_empty() {
            return Err(Error::FenceBreached(format!(
                "application sessions or prepared transactions remain: {rows:?}"
            )));
        }
        Ok(())
    }

    fn run_hook(&self) -> Result<(), Error> {
        let Some(hook) = &self.config.post_activate else {
            return Ok(());
        };
        let mut child = Command::new(&hook.program)
            .args(&hook.args)
            .env("SKEDADDLE_ACTIVATION_EPOCH", self.config.epoch.to_string())
            .spawn()
            .map_err(|e| Error::Hook(e.to_string()))?;
        let deadline = Instant::now() + self.timeout();
        loop {
            if let Some(status) = child.try_wait().map_err(|e| Error::Hook(e.to_string()))? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(Error::Hook(format!("post-activate exited {status}")))
                };
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Hook(
                    "post-activate timed out; retry with the same epoch".into(),
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn validate_config(config: &Config) -> Result<(), Error> {
    if config.epoch < 1
        || config.timeout_ms == 0
        || config.applications.is_empty()
        || config.tables.is_empty()
        || config.applications.iter().any(String::is_empty)
        || config
            .post_activate
            .as_ref()
            .is_some_and(|h| h.program.is_empty())
    {
        return Err(Error::Invalid(
            "positive epoch/timeout, roles, tables and nonempty hook required".into(),
        ));
    }
    Ok(())
}

fn validate_roles(catalog: &Catalog, applications: &BTreeSet<String>) -> Result<(), Error> {
    for name in applications {
        if catalog.roles.iter().filter(|r| &r.name == name).count() != 1 {
            return Err(Error::Invalid(format!(
                "application role missing or duplicated: {name}"
            )));
        }
    }
    Ok(())
}

fn validate_activation_state(
    initial: &Catalog,
    desired: &Catalog,
    applications: &BTreeSet<String>,
) -> Result<(), Error> {
    validate_roles(desired, applications)?;
    let mut roles = desired.roles.clone();
    for role in &mut roles {
        if applications.contains(&role.name) {
            role.login = initial
                .roles
                .iter()
                .find(|r| r.name == role.name)
                .ok_or_else(|| Error::Invalid("activation role missing".into()))?
                .login;
        }
    }
    // These catalogs are destination snapshots supplied by provisioning, with
    // only application login flags and write grants changed in the manifest.
    let roles_match =
        roles.len() == initial.roles.len() && roles.iter().all(|r| initial.roles.contains(r));
    let memberships_match = desired.memberships.len() == initial.memberships.len()
        && desired
            .memberships
            .iter()
            .all(|m| initial.memberships.contains(m));
    let tables_match = desired.tables.len() == initial.tables.len()
        && desired.tables.iter().all(|t| initial.tables.contains(t));
    if !roles_match
        || !memberships_match
        || !tables_match
        || initial.grants.iter().any(|g| !desired.grants.contains(g))
        || desired
            .grants
            .iter()
            .any(|g| !initial.tables.contains(&g.table))
    {
        return Err(Error::Invalid("activation manifest changes owners/memberships/role attributes or omits existing grants".into()));
    }
    Ok(())
}

fn validate_fenced(catalog: &Catalog, applications: &BTreeSet<String>) -> Result<(), Error> {
    validate_roles(catalog, applications)?;
    if catalog
        .roles
        .iter()
        .any(|r| applications.contains(&r.name) && r.login)
        || !effective_writers(catalog).is_disjoint(applications)
    {
        return Err(Error::FenceBreached(
            "application login or effective write privilege detected".into(),
        ));
    }
    Ok(())
}

fn query(connection: &mut impl Connection, sql: &str, timeout: Duration) -> Result<Rows, Error> {
    connection.query(sql, timeout).map_err(Error::Database)
}
fn cell(row: &[String], index: usize) -> Result<&str, Error> {
    row.get(index)
        .map(String::as_str)
        .ok_or_else(|| Error::Invalid("short catalog row".into()))
}
fn boolean(value: &str) -> Result<bool, Error> {
    match value {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(Error::Invalid("invalid boolean".into())),
    }
}
fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
fn names(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|v| literal(v))
        .collect::<Vec<_>>()
        .join(",")
}
fn table_sql(table: &Table) -> String {
    format!("{}.{}", ident(&table.schema), ident(&table.name))
}
fn selected(tables: &[Table]) -> String {
    tables
        .iter()
        .map(|t| format!("{}::regclass", literal(&table_sql(t))))
        .collect::<Vec<_>>()
        .join(",")
}
fn transaction(sql: &str, timeout: Duration) -> String {
    format!(
        "BEGIN; SET LOCAL statement_timeout='{}ms'; SET LOCAL lock_timeout='{}ms'; {sql} COMMIT;",
        timeout.as_millis().max(1),
        timeout.as_millis().max(1)
    )
}

#[cfg(test)]
mod tests;
