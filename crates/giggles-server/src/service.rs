use crate::clock::Clock;
use giggles_core::{
    relational::{Prepared, SqlResult, StatementClock, Value},
    *,
};
use giggles_core::task::{Page, Released, Unreleased};
use giggles_pg_types::temporal::SessionZone;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "role", deny_unknown_fields)]
pub enum Role {
    Task {
        domain: String,
        task: TaskId,
        principal: Principal,
    },
    Application {
        domain: String,
        tables: BTreeSet<String>,
    },
    Database { name: String },
}
impl Role {
    pub fn is_sql(&self) -> bool {
        matches!(self, Role::Application { .. } | Role::Database { .. })
    }
}
/// GD-0061 R3-312: the part of the catalog a task session sees
/// (`Session::task_catalog_view`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskCatalogView {
    /// The task's table.
    pub(crate) table: u64,
    /// Its schema.
    pub(crate) schema: String,
    /// Shown column IDs: readable, masked, and the storage-only heap key.
    pub(crate) columns: BTreeSet<u64>,
    /// Lower-cased names the task never sees: its hidden columns and every
    /// other relation. Text that contains one is dropped.
    pub(crate) hidden: Vec<String>,
}
impl TaskCatalogView {
    /// Whether `text` mentions a hidden column or another relation. A
    /// substring match, so it errs towards dropping.
    pub(crate) fn mentions_hidden(&self, text: &str) -> bool {
        let text = text.to_lowercase();
        self.hidden.iter().any(|name| text.contains(name.as_str()))
    }
    /// `table` with only the shown columns; its primary key is remapped, or
    /// cleared when a key column is hidden.
    pub(crate) fn filter_table(&self, mut table: relational::TableDef) -> relational::TableDef {
        let mut positions = Vec::with_capacity(table.columns.len());
        let mut next = 0;
        for column in &table.columns {
            if self.columns.contains(&column.id) {
                positions.push(Some(next));
                next += 1;
            } else {
                positions.push(None);
            }
        }
        table.primary_key = table
            .primary_key
            .iter()
            .map(|i| positions.get(*i).copied().flatten())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        table.columns.retain(|column| self.columns.contains(&column.id));
        table
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub fingerprint: String,
    pub role: Role,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Request {
    /// Relative cooperative decision budget, measured from frame receipt.
    /// It is never a certified time assertion or proof a previous attempt rolled back.
    Deadline {
        budget_ms: u64,
        request: Box<Request>,
    },
    Preview {
        request: giggles_core::Request,
    },
    Execute {
        request: giggles_core::Request,
        approval: Option<ApprovalId>,
    },
    RepairSql {
        operation: OperationId,
        sql: String,
        approval: Option<ApprovalId>,
    },
    Status {
        operation: OperationId,
    },
    Sql {
        sql: String,
        #[serde(default)]
        params: Vec<Value>,
    },
    Ping,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub version: u32,
    pub code: String,
    pub operation: Option<OperationId>,
    pub count: Option<usize>,
    pub digest: Option<String>,
    pub sql: Option<SqlResult>,
}
impl Response {
    pub fn code(code: &str) -> Self {
        Self {
            version: 1,
            code: code.into(),
            operation: None,
            count: None,
            digest: None,
            sql: None,
        }
    }
    pub fn error(e: &CoreError) -> Self {
        Self::code(sqlstate(e))
    }
    /// An `ok` response carrying a released SQL result. Server sources build
    /// `Response.sql` only here, so both API transports encode only released
    /// rows (GD-0061 R3-314; `tests/release_seal.rs` checks it).
    ///
    /// A released result is accepted:
    /// ```
    /// use giggles_core::{relational::SqlResult, task::Released};
    /// let response = giggles_server::Response::released_sql(Released::synthetic(SqlResult::default()));
    /// assert_eq!(response.code, "ok");
    /// ```
    /// A plain `SqlResult` does not compile (`encoders_accept_only_released`):
    /// ```compile_fail
    /// use giggles_core::relational::SqlResult;
    /// let response = giggles_server::Response::released_sql(SqlResult::default());
    /// ```
    pub fn released_sql(result: Released<SqlResult>) -> Self {
        Self {
            sql: Some(result.into_inner()),
            ..Self::code("ok")
        }
    }
    fn decision(operation: OperationId, d: Decision) -> Self {
        match d {
            Ok(r) => Self {
                operation: Some(operation),
                count: Some(r.effects.len()),
                digest: Some(digest(&r.effects)),
                ..Self::code("committed")
            },
            Err(e) => Self {
                operation: Some(operation),
                ..Self::error(&CoreError::Policy(e))
            },
        }
    }
}
/// The refusal of a plain exit in a task session, as core's plain exits give
/// it (GD-0061 R3-301/R3-310).
fn task_session_refusal() -> CoreError {
    CoreError::Task(giggles_core::task::TaskError::NotAllowedInTaskSession(
        giggles_core::task::StatementClass::Other,
    ))
}
fn digest(effects: &[Effect]) -> String {
    // Stable v1 digest; no raw before/after rows are returned.
    hex::encode(Sha256::digest(
        serde_json::to_vec(effects).expect("effect serialization"),
    ))
}
pub use giggles_core::sqlstate;

/// Whether `error` is transient: the cluster gateway is fenced or a range is unavailable
/// (GD-0075 Phase 11), a serialization failure or deadlock, or admission is busy. A session
/// reports it and stays open; it never means the role is gone.
pub(crate) fn transient(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::Sql(relational::SqlError::Conflict)
            | CoreError::Deadlock
            | CoreError::Storage(_)
            | CoreError::Busy
    )
}

/// The per-statement role recheck failed: a transient failure stays that failure (the
/// statement fails, the session stays); anything else is an authentication failure.
pub(crate) fn role_lookup_error(error: roles::RoleError) -> CoreError {
    match error {
        roles::RoleError::Core(error) if transient(&error) => error,
        _ => CoreError::Policy(Error::Authentication),
    }
}

pub type StatementGate = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

pub struct Service {
    pub(crate) statement_gate: std::sync::OnceLock<StatementGate>,
    pub(crate) advisory: crate::advisory::Locks,
    pub(crate) blocking: crate::blocking::Blocking,
    pub(crate) cancellations: crate::cancel::Registry,
    task_gate: std::sync::Mutex<()>,
    pub(crate) admin: Admin,
    pub(crate) clock: Clock,
    password_generations: std::sync::Mutex<BTreeMap<String, (u32, u64, u64)>>,
    pub(crate) authority_gate: std::sync::RwLock<()>,
    pub(crate) binding_changes: tokio::sync::watch::Sender<()>,
    bindings: std::sync::Mutex<Credentials>,
    // Lock order: `bindings` before `agent` whenever both are held.
    agent: std::sync::Mutex<crate::agent::bindings::AgentCredentials>,
    outcome_retention_ms: std::sync::atomic::AtomicU64,
    pub(crate) admission: crate::admission::Admission,
    /// GD-0061 R3-403: agent requests take a slot here before `Blocking::admit`.
    pub(crate) agent_admission: crate::agent::limits::AgentAdmission,
}
struct Credentials {
    next: u64,
    entries: BTreeMap<String, (Role, u64)>,
}
impl Service {
    pub fn new(admin: Admin, bindings: Vec<Binding>, clock: Clock) -> Result<Arc<Self>, CoreError> {
        Self::with_limits(
            admin,
            bindings,
            clock,
            crate::admission::ServiceLimits::default(),
        )
    }
    pub fn with_limits(
        admin: Admin,
        bindings: Vec<Binding>,
        clock: Clock,
        limits: crate::admission::ServiceLimits,
    ) -> Result<Arc<Self>, CoreError> {
        Self::with_agent(admin, bindings, Vec::new(), clock, limits)
    }
    /// A service with both binding maps. A fingerprint may appear in only one.
    pub fn with_agent(
        admin: Admin,
        bindings: Vec<Binding>,
        agent: Vec<crate::agent::AgentBinding>,
        clock: Clock,
        limits: crate::admission::ServiceLimits,
    ) -> Result<Arc<Self>, CoreError> {
        crate::pg_catalog_wire::initialize_startup();
        let map = Self::validate(&admin, bindings)?;
        let agent_map = crate::agent::bindings::validate(&admin, &map, agent)?;
        let mut next = 0u64;
        let entries = map
            .into_iter()
            .map(|(fp, role)| {
                next += 1;
                (fp, (role, next))
            })
            .collect();
        let agent_entries = agent_map
            .into_iter()
            .map(|(fp, entry)| {
                next += 1;
                (fp, (entry, next))
            })
            .collect();
        // GD-0061 R3-403: core's default ceiling, min(8, admitted / 2), and the
        // agent admission sized to match it.
        let agent_admission = crate::agent::limits::AgentAdmission::default();
        let mut agent_limits = agent_admission.limits();
        if let Some(n) = (1..=agent_limits.max_concurrent_statements)
            .rev()
            .find(|n| admin.set_agent_statement_ceiling(*n).is_ok())
        {
            agent_limits.max_concurrent_statements = n;
            agent_admission.apply(agent_limits);
        }
        Ok(Arc::new(Self {
            advisory: crate::advisory::Locks::default(),
            blocking: crate::blocking::Blocking::new(),
            cancellations: crate::cancel::Registry::default(),
            task_gate: std::sync::Mutex::new(()),
            statement_gate: std::sync::OnceLock::new(),
            password_generations: std::sync::Mutex::new(BTreeMap::new()),
            authority_gate: std::sync::RwLock::new(()),
            binding_changes: tokio::sync::watch::channel(()).0,
            bindings: std::sync::Mutex::new(Credentials { next, entries }),
            agent: std::sync::Mutex::new(crate::agent::bindings::AgentCredentials {
                next,
                entries: agent_entries,
            }),
            outcome_retention_ms: std::sync::atomic::AtomicU64::new(
                crate::agent::DEFAULT_OUTCOME_RETENTION_MS,
            ),
            admission: crate::admission::Admission::new(limits)?,
            agent_admission,
            admin,
            clock,
        }))
    }
    /// GD-0075 R5-103: per-role statement limits on this node (startup config).
    pub fn set_role_statement_limits(
        &self,
        limits: crate::admission::RoleStatementLimits,
    ) -> Result<(), CoreError> {
        self.admission.set_role_limits(limits)
    }
    /// GD-0061 R3-403: installs the `agent` load settings, at startup and on
    /// reload. Refuses (`CoreError::Limit`, prior settings kept) a ceiling of 0,
    /// above `MAX_BLOCKING / 2` or above half the engine's admitted operations.
    /// Chunk settings apply to task sessions opened afterwards.
    pub fn set_agent_limits(&self, limits: crate::agent::AgentLimits) -> Result<(), CoreError> {
        limits.validate().map_err(|_| CoreError::Limit)?;
        self.admin
            .set_agent_statement_ceiling(limits.max_concurrent_statements)?;
        self.agent_admission.apply(limits);
        Ok(())
    }
    /// The `agent` load settings in force.
    pub fn agent_limits(&self) -> crate::agent::AgentLimits {
        self.agent_admission.limits()
    }
    /// For tests: agent requests holding an agent slot now.
    #[doc(hidden)]
    pub fn agent_requests_admitted(&self) -> usize {
        self.agent_admission.held()
    }
    /// For tests: blocking-pool permits held now, by any listener.
    #[doc(hidden)]
    pub fn blocking_permits_held(&self) -> usize {
        self.blocking.in_use()
    }
    pub fn set_statement_gate(&self, gate: StatementGate) -> Result<(), StatementGate> {
        self.statement_gate.set(gate)
    }
    pub(crate) fn check_statement_gate(&self) -> Result<(), String> {
        self.statement_gate.get().map_or(Ok(()), |gate| gate())
    }
    fn validate(
        admin: &Admin,
        bindings: Vec<Binding>,
    ) -> Result<BTreeMap<String, Role>, CoreError> {
        if bindings.len() > 1024 {
            return Err(CoreError::Limit);
        }
        let domain = admin.authority_metadata()?.0.to_string();
        let mut map = BTreeMap::new();
        let mut owners: BTreeMap<String, Role> = BTreeMap::new();
        for b in bindings {
            if b.fingerprint.len() != 64
                || !b
                    .fingerprint
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(CoreError::Limit);
            }
            match &b.role {
                Role::Task { domain: bound, .. } | Role::Application { domain: bound, .. }
                    if bound != &domain =>
                {
                    return Err(CoreError::Policy(Error::Authentication));
                }
                Role::Database { name } => {
                    admin.roles().get(name)
                        .map_err(|_| CoreError::Policy(Error::Authentication))?
                        .filter(|role| role.login && !role.task_only)
                        .ok_or(CoreError::Policy(Error::Authentication))?;
                }
                _ => {}
            }
            if let Role::Application { tables, .. } = &b.role {
                if tables.is_empty() || tables.len() > 128 {
                    return Err(CoreError::Policy(Error::Scope));
                }
                for table in tables {
                    // Core currently represents multi-source plans with this
                    // reserved sentinel, never an authorizable table identity.
                    if table == "giggles_join" {
                        return Err(CoreError::Policy(Error::Scope));
                    }
                    if table.is_empty() || table.len() > 128 {
                        return Err(CoreError::Limit);
                    }
                    if owners.get(table).is_some_and(|owner| owner != &b.role) {
                        return Err(CoreError::Policy(Error::Scope));
                    }
                    owners.insert(table.clone(), b.role.clone());
                }
            }
            if map.insert(b.fingerprint, b.role).is_some() {
                return Err(CoreError::Policy(Error::Authentication));
            }
        }
        Ok(map)
    }
    /// Trusted local control only. Validates completely before an atomic replacement.
    /// Decisions already ordered before this lock are never undone. The agent map
    /// is kept as it is, and revalidated against the new ordinary map.
    pub fn reload_bindings(&self, bindings: Vec<Binding>) -> Result<u64, CoreError> {
        let agent = self.agent.lock().map_err(|_| CoreError::Fenced)?.bindings();
        self.reload_all_bindings(bindings, agent).map(|(next, _)| next)
    }
    /// Trusted local control only. Validates both maps completely, then swaps both
    /// under one write of the authority gate. An entry keeps its epoch only when
    /// it is unchanged; any change cuts that fingerprint's live sessions.
    pub fn reload_all_bindings(
        &self,
        bindings: Vec<Binding>,
        agent: Vec<crate::agent::AgentBinding>,
    ) -> Result<(u64, u64), CoreError> {
        let proposed = Self::validate(&self.admin, bindings)?;
        let proposed_agent = crate::agent::bindings::validate(&self.admin, &proposed, agent)?;
        let _gate = self.authority_gate.write().map_err(|_| CoreError::Fenced)?;
        let mut current = self.bindings.lock().map_err(|_| CoreError::Fenced)?;
        let mut current_agent = self.agent.lock().map_err(|_| CoreError::Fenced)?;
        let mut next = current.next.max(current_agent.next);
        let mut entries = BTreeMap::new();
        for (fp, role) in proposed {
            let epoch = match current.entries.get(&fp) {
                Some((old, epoch)) if old == &role => *epoch,
                _ => {
                    next = next.checked_add(1).ok_or(CoreError::Limit)?;
                    next
                }
            };
            entries.insert(fp, (role, epoch));
        }
        let mut agent_entries = BTreeMap::new();
        for (fp, entry) in proposed_agent {
            let epoch = match current_agent.entries.get(&fp) {
                Some((old, epoch)) if old == &entry => *epoch,
                _ => {
                    next = next.checked_add(1).ok_or(CoreError::Limit)?;
                    next
                }
            };
            agent_entries.insert(fp, (entry, epoch));
        }
        *current = Credentials { next, entries };
        *current_agent = crate::agent::bindings::AgentCredentials {
            next,
            entries: agent_entries,
        };
        drop(current_agent);
        drop(current);
        self.binding_changes.send_replace(());
        Ok((next, next))
    }
    /// Phase 5 exposure: which agent credentials may open `task`, as the first
    /// 8 hex of each fingerprint and its executor role.
    pub fn agent_bindings_for(
        &self,
        task: &giggles_core::task::definition::TaskName,
    ) -> Vec<(String, String)> {
        let Ok(current) = self.agent.lock() else {
            return Vec::new();
        };
        current
            .entries
            .iter()
            .filter(|(_, (entry, _))| entry.tasks.contains(task))
            .map(|(fp, (entry, _))| (fp[..8].to_owned(), entry.executor.clone()))
            .collect()
    }
    /// How long after a task's expiry its credential still connects for outcome
    /// lookup (D5). Server setting; defaults to 30 days.
    pub fn set_outcome_retention(&self, retention: std::time::Duration) {
        let ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        self.outcome_retention_ms
            .store(ms, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn cancel_bound(&self, session: &Session, pid: u32, secret: u32) {
        // Hold registry snapshot through cancellation; reload cannot revoke
        // between validation and signaling. Never wait for the decision gate.
        if session.agent {
            let Ok(current) = self.agent.lock() else {
                return;
            };
            if session.agent_entry_matches(&current) {
                self.cancellations
                    .cancel(&session.fingerprint, session.epoch, pid, secret);
            }
            return;
        }
        let Ok(current) = self.bindings.lock() else {
            return;
        };
        if current
            .entries
            .get(&session.fingerprint)
            .is_some_and(|(role, epoch)| role == &session.role && *epoch == session.epoch)
        {
            self.cancellations
                .cancel(&session.fingerprint, session.epoch, pid, secret);
        }
    }
    fn role_generation_matches(&self, role: &giggles_core::roles::CatalogRole, epoch: u64) -> bool {
        role.generation == epoch
            || self.password_generations.lock().is_ok_and(|aliases| {
                aliases.get(&role.name).is_some_and(|&(oid, first, last)| {
                    role.oid == oid && role.generation == last && (first..=last).contains(&epoch)
                })
            })
    }
    pub fn role_session(self: &Arc<Self>, name: &str) -> Result<Session, CoreError> {
        let role = self
            .admin
            .roles()
            .get(name)
            .map_err(|_| CoreError::Policy(Error::Authentication))?
            .filter(|role| role.login && !role.task_only)
            .ok_or(CoreError::Policy(Error::Authentication))?;
        let mut session = Session {
            surface: Surface::Native,
            backend_pid: 0,
            connection_closed: Default::default(),
            backend_ssl: false,
            service: self.clone(),
            fingerprint: format!("pgrole:{}", role.name),
            role: Role::Database { name: role.name },
            epoch: role.generation,
            fingerprint_bound: false,
            agent: false,
            in_protected: false,
            role_fence: Some((role.oid, role.generation)),
            scoped: false,
            connection: None,
            database: None,
            sql: {
                let mut sql = self.admin.sql_session();
                sql.set_authority_managed(true);
                sql
            },
            zone: Some(SessionZone::Fixed(0)),
            transaction_us: None,
            zone_setting: None,
            implicit_ddl: false,
            #[cfg(test)]
            control_calls: 0,
            #[cfg(test)]
            fast_control_calls: 0,
            sequence_owner_candidates: BTreeSet::new(),
            search_path: String::new(),
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        Ok(session)
    }
    pub fn session(self: &Arc<Self>, fingerprint: &str) -> Result<Session, CoreError> {
        let (role, epoch) = self
            .bindings
            .lock()
            .map_err(|_| CoreError::Fenced)?
            .entries
            .get(fingerprint)
            .cloned()
            .ok_or(CoreError::Policy(Error::Authentication))?;
        let role_fence = if let Role::Database { name } = &role {
            let role = self.admin.roles().get(name)
                .map_err(|_| CoreError::Policy(Error::Authentication))?
                .filter(|role| role.login && !role.task_only)
                .ok_or(CoreError::Policy(Error::Authentication))?;
            Some((role.oid, role.generation))
        } else {
            None
        };
        let mut session = Session {
            surface: Surface::Native,
            backend_pid: 0,
            connection_closed: Default::default(),
            backend_ssl: false,
            service: self.clone(),
            role,
            epoch,
            fingerprint_bound: true,
            agent: false,
            in_protected: false,
            role_fence,
            scoped: false,
            connection: None,
            database: None,
            fingerprint: fingerprint.to_owned(),
            sql: {
                let mut sql = self.admin.sql_session();
                sql.set_authority_managed(true);
                sql
            },
            zone: Some(SessionZone::Fixed(0)),
            transaction_us: None,
            zone_setting: None,
            implicit_ddl: false,
            #[cfg(test)]
            control_calls: 0,
            #[cfg(test)]
            fast_control_calls: 0,
            sequence_owner_candidates: BTreeSet::new(),
            search_path: String::new(),
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        Ok(session)
    }
}
impl Service {
    /// The agent accept loop's first check: the certificate is in the agent map.
    /// The executor role and epoch of an agent-map entry (R3-208 API identity).
    pub(crate) fn agent_entry(&self, fingerprint: &str) -> Option<(String, u64)> {
        let current = self.agent.lock().ok()?;
        current
            .entries
            .get(fingerprint)
            .map(|(entry, epoch)| (entry.executor.clone(), *epoch))
    }
    /// The role exists and is a login, task-only, non-superuser role.
    pub(crate) fn agent_executor_valid(&self, name: &str) -> bool {
        self.admin
            .roles()
            .get(name)
            .ok()
            .flatten()
            .is_some_and(|role| crate::agent::bindings::executor_role(&role))
    }
    pub(crate) fn agent_known(&self, fingerprint: &str) -> bool {
        self.agent
            .lock()
            .is_ok_and(|current| current.entries.contains_key(fingerprint))
    }
    /// A CancelRequest on the agent listener: it reaches only a backend that the
    /// same certificate opened under its current agent-map epoch.
    pub(crate) fn cancel_agent(&self, fingerprint: &str, pid: u32, secret: u32) {
        let Ok(current) = self.agent.lock() else {
            return;
        };
        if let Some((_, epoch)) = current.entries.get(fingerprint) {
            self.cancellations.cancel(fingerprint, *epoch, pid, secret);
        }
    }
    /// The first 8 hex of each agent binding whose tasks all exist and are all
    /// revoked or past outcome retention (design-2 section 3). A task that does
    /// not exist yet keeps its binding live. Empty when the clock is unavailable.
    pub fn stale_agent_bindings(&self) -> Vec<String> {
        use giggles_core::task::context::TaskDefinitions;
        let Ok(now) = crate::agent::wall_now(&self.clock) else {
            return Vec::new();
        };
        let retention = i64::try_from(
            self.outcome_retention_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        )
        .unwrap_or(i64::MAX);
        let entries: Vec<_> = match self.agent.lock() {
            Ok(current) => current
                .entries
                .iter()
                .map(|(fp, (entry, _))| (fp.clone(), entry.tasks.clone()))
                .collect(),
            Err(_) => return Vec::new(),
        };
        let sql = self.admin.sql_session();
        entries
            .into_iter()
            .filter(|(_, tasks)| {
                tasks.iter().all(|name| match sql.task_definition(name) {
                    Ok(Some(def)) => {
                        def.revoked || now.0 >= def.expires_at.0.saturating_add(retention)
                    }
                    _ => false,
                })
            })
            .map(|(fp, _)| fp[..8].to_owned())
            .collect()
    }
    /// An agent (task) session for a certificate in the agent map. `task` names
    /// the task; with no name the binding must list exactly one. Every refusal is
    /// `Policy(Authentication)`, so the client cannot tell the reasons apart; an
    /// unavailable clock refuses the same way.
    pub fn agent_session(
        self: &Arc<Self>,
        fingerprint: &str,
        task: Option<&str>,
    ) -> Result<Session, CoreError> {
        use giggles_core::task::{context::open_context, definition::TaskName};
        let refused = || CoreError::Policy(Error::Authentication);
        // 1. The fingerprint is in the agent map.
        let (entry, epoch) = self
            .agent
            .lock()
            .map_err(|_| CoreError::Fenced)?
            .entries
            .get(fingerprint)
            .cloned()
            .ok_or_else(refused)?;
        // 2. The task is selected.
        let name = match task {
            Some(name) => TaskName::new(name)
                .ok()
                .filter(|name| entry.tasks.contains(name))
                .ok_or_else(refused)?,
            None if entry.tasks.len() == 1 => entry.tasks.first().cloned().ok_or_else(refused)?,
            None => return Err(refused()),
        };
        // 3. The executor is a login, task-only role.
        let role = self
            .admin
            .roles()
            .get(&entry.executor)
            .map_err(|_| refused())?
            .filter(crate::agent::bindings::executor_role)
            .ok_or_else(refused)?;
        // 4. The task opens for this executor at this time.
        let mut digest = [0u8; 32];
        hex::decode_to_slice(fingerprint, &mut digest).map_err(|_| refused())?;
        let now = crate::agent::wall_now(&self.clock).map_err(|_| refused())?;
        let retention = self
            .outcome_retention_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut sql = self.admin.sql_session();
        sql.set_authority_managed(true);
        let ctx = open_context(&sql, &name, &role.name, digest, now, retention)
            .map_err(|_| refused())?;
        sql.bind_task(ctx)?;
        let chunk = self.agent_admission.limits();
        sql.set_disclosure_chunk(chunk.disclosure_chunk_rows, chunk.disclosure_chunk_bytes);
        let mut session = Session {
            surface: Surface::Native,
            backend_pid: 0,
            connection_closed: Default::default(),
            backend_ssl: false,
            service: self.clone(),
            fingerprint: fingerprint.to_owned(),
            role: Role::Database { name: role.name },
            epoch,
            fingerprint_bound: true,
            agent: true,
            in_protected: false,
            role_fence: Some((role.oid, role.generation)),
            scoped: false,
            connection: None,
            database: None,
            sql,
            zone: Some(SessionZone::Fixed(0)),
            transaction_us: None,
            zone_setting: None,
            implicit_ddl: false,
            #[cfg(test)]
            control_calls: 0,
            #[cfg(test)]
            fast_control_calls: 0,
            sequence_owner_candidates: BTreeSet::new(),
            search_path: String::new(),
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        Ok(session)
    }
}
/// DD-0068 F9: protocol surface of a session. Only pgwire opts into Postgres; every other surface stays Native (E939 42501), fail-closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Surface {
    #[default]
    Native,
    Postgres,
}
#[derive(Default)]
pub(crate) struct ApiPathState {
    pub(crate) transaction_start: Option<String>,
    pub(crate) sql_committed: bool,
    pub(crate) savepoints: Vec<(String, String)>,
}
pub struct Session {
    pub(crate) surface: Surface,
    pub(crate) backend_pid: i32,
    pub(crate) connection_closed: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) backend_ssl: bool,
    pub(crate) service: Arc<Service>,
    pub(crate) role: Role,
    pub(crate) database: Option<String>,
    pub(crate) fingerprint: String,
    // Fingerprint binding epoch, or the role generation for trusted pgwire sessions.
    pub(crate) epoch: u64,
    // Trusted pgwire sessions pin role identity without a certificate binding.
    fingerprint_bound: bool,
    // An agent (task) session: bound through the agent map, never the ordinary one.
    pub(crate) agent: bool,
    // Set only by `execute_protected` while the protected seam runs; the
    // `authorize` backstop lets a task session's UPDATE through only then.
    in_protected: bool,
    // Database role OID and generation, independent of any fingerprint binding epoch.
    pub(crate) role_fence: Option<(u32, u64)>,
    scoped: bool,
    pub(crate) connection: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    pub(crate) sql: SqlSession,
    zone: Option<SessionZone>,
    transaction_us: Option<i64>,
    zone_setting: Option<String>,
    implicit_ddl: bool,
    #[cfg(test)]
    pub(crate) control_calls: usize,
    #[cfg(test)]
    pub(crate) fast_control_calls: usize,
    sequence_owner_candidates: BTreeSet<String>,
    pub(crate) search_path: String,
    pub(crate) api_path: ApiPathState,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.abort();
        self.service.advisory.release(self.backend_pid, false);
    }
}
impl Session {
    pub(crate) fn zone(&self) -> Option<SessionZone> {
        self.zone
    }
    pub(crate) fn set_search_path(&mut self, path: &str) {
        let user = match &self.role {
            Role::Database { name } => name.as_str(),
            _ => "giggles",
        };
        self.sql.set_search_path(path, user);
        let names = relational::schema_search_path(path, "$user");
        self.search_path = if names.is_empty() { "\"\"".into() } else {
            names.iter().map(|name| {
                let mut chars = name.bytes();
                let bare = chars.next().is_some_and(|c| c.is_ascii_lowercase() || c == b'_')
                    && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'$');
                if bare { name.clone() } else { format!("\"{}\"", name.replace('"', "\"\"")) }
            }).collect::<Vec<_>>().join(", ")
        };
    }
    pub(crate) fn set_extra_float_digits(&mut self, value: i32) {
        self.sql.set_extra_float_digits(value);
    }
    /// The level of transactions this session begins next (BEGIN without a
    /// level, and statements outside a block): `default_transaction_isolation`.
    pub(crate) fn set_default_isolation(&mut self, settings: &crate::pg_settings::Settings) {
        self.sql
            .set_isolation(crate::pg_transaction::sql_isolation(settings.default_isolation));
    }
    fn reconcile_sequence_owners(&mut self) -> Result<(), CoreError> {
        if self.sql.in_transaction() || self.sequence_owner_candidates.is_empty() {
            return Ok(());
        }
        let schemas = self.sql.schema_metadata()?;
        let mut live: BTreeSet<_> = self
            .service
            .admin
            .sql_session()
            .sequence_owners()?
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        live.extend(self.service.admin.catalog()?.1.into_iter().map(|table| {
            relational::relation_key(
                schemas
                    .tables
                    .get(&table.id)
                    .map_or("public", String::as_str),
                &table.name,
            )
        }));
        let roles = self.service.admin.roles();
        for name in &self.sequence_owner_candidates {
            if name
                .split_once('\0')
                .is_some_and(|(schema, _)| !schemas.schemas.contains_key(schema))
            {
                continue;
            }
            if !live.contains(name) {
                roles
                    .forget_table(name)
                    .map_err(|_| CoreError::Policy(Error::Scope))?;
            }
        }
        self.sequence_owner_candidates.clear();
        Ok(())
    }
    pub(crate) fn set_time_zone(&mut self, name: &str) {
        if self.zone_setting.as_deref() == Some(name) { return; }
        self.zone = SessionZone::setting(name).ok().map(|(zone, _)| zone);
        self.zone_setting = Some(name.to_owned());
    }
    fn role_epoch(&self) -> u64 {
        self.role_fence.map_or(self.epoch, |(_, generation)| generation)
    }
    /// GD-0061 R3-707 (design-7 D7.1, "PostgreSQL listener only"): the
    /// database role of a session that is neither certificate bound nor an
    /// agent session; `None` for every other session.
    pub(crate) fn postgresql_listener_role(&self) -> Option<&str> {
        match &self.role {
            Role::Database { name } if !self.fingerprint_bound && !self.agent => Some(name),
            _ => None,
        }
    }
    /// The task this session is bound to; `None` for every non-agent session.
    pub fn task(&self) -> Option<&giggles_core::task::context::TaskContext> {
        self.sql.task()
    }
    /// Per-call admission for a task session (design-2 section 3, steps 2 and 3):
    /// the static profile, then the task's standing. A non-agent session is
    /// always admitted.
    pub fn admit(
        &mut self,
        class: giggles_core::task::StatementClass,
    ) -> Result<(), CoreError> {
        let admitted = giggles_core::task::admit::admit(self.sql.task(), class);
        #[cfg(test)]
        let admitted = admitted.or_else(|error| {
            if crate::agent::statement::test_route::enabled(class) {
                Ok(())
            } else {
                Err(error)
            }
        });
        admitted.map_err(CoreError::Task)?;
        if self.sql.task().is_none() {
            return Ok(());
        }
        let now = crate::agent::wall_now(&self.service.clock)?;
        let standing = self.sql.task_standing(now).map_err(CoreError::Task)?;
        crate::agent::standing_admits(standing, class).map_err(CoreError::Task)
    }
    /// GD-0061 R3-207: run one protected statement (direct write, preview,
    /// apply or outcome lookup). It admits the class, then hands the statement
    /// to `agent::statement::protected_seam` (R3-615 fills in the direct write;
    /// the other forms fail closed until Phase 7).
    pub(crate) fn execute_protected(
        &mut self,
        statement: &crate::agent::statement::ProtectedStatement,
        params: &[Value],
    ) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| {
            if session.sql.task().is_none() {
                return Err(CoreError::Policy(Error::Scope));
            }
            session.admit(statement.class())?;
            session.protected_scope(|session| {
                crate::agent::statement::protected_seam(session, statement, params)
            })
        })
    }
    /// R3-207/615: prepare the `UPDATE` wrapped by `OPERATION '<id>'` (or
    /// `PREVIEW`). Its class is admitted first; the `authorize` backstop then
    /// lets the wrapped UPDATE through because `in_protected` is set.
    pub(crate) fn prepare_protected(
        &mut self,
        class: giggles_core::task::StatementClass,
        sql: &str,
        declared_types: &[Option<relational::DataType>],
    ) -> Result<Prepared, CoreError> {
        if self.sql.task().is_none() {
            return Err(CoreError::Policy(Error::Scope));
        }
        self.admit(class)?;
        self.protected_scope(|session| {
            let prepared = session.prepare_with_parameter_types(sql, declared_types)?;
            // R3-303's column rules before anything describes the statement:
            // a hidden column and a nonexistent one get the same rule error,
            // never a bind error (42703) that tells them apart.
            session.sql.task_column_rules(&prepared)
        })
    }
    /// The only place `in_protected` is set. It is cleared on every exit,
    /// including an error or a panic out of `work` (the guard's drop).
    fn protected_scope<T>(
        &mut self,
        work: impl FnOnce(&mut Self) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        struct Reset<'a>(&'a mut Session);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.in_protected = false;
            }
        }
        self.in_protected = true;
        let reset = Reset(self);
        work(&mut *reset.0)
    }
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn in_protected(&self) -> bool {
        self.in_protected
    }
    /// Test hook: a panic inside the protected scope still clears the flag.
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn panic_in_protected_scope(&mut self) -> bool {
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.protected_scope(|session| -> Result<(), CoreError> {
                assert!(session.in_protected);
                panic!("protected scope test panic")
            })
        }));
        assert!(caught.is_err());
        self.in_protected
    }
    /// Backstop for every internal path of a task session (design-2 section 4,
    /// call site 5): the class must be admitted. An UPDATE passes only inside
    /// `execute_protected`; outside it, it needs an operation ID.
    pub(crate) fn admit_backstop(
        &mut self,
        class: giggles_core::task::StatementClass,
    ) -> Result<(), CoreError> {
        use giggles_core::task::{StatementClass, TaskError};
        if self.sql.task().is_none() {
            return Ok(());
        }
        if class == StatementClass::Update && !self.in_protected {
            return Err(CoreError::Task(TaskError::OperationIdRequired));
        }
        self.admit(class)
    }
    /// The agent-map entry still has this session's epoch and executor and
    /// still lists its task.
    fn agent_entry_matches(&self, current: &crate::agent::bindings::AgentCredentials) -> bool {
        let (Some(ctx), Role::Database { name }) = (self.sql.task(), &self.role) else {
            return false;
        };
        current
            .entries
            .get(&self.fingerprint)
            .is_some_and(|(entry, epoch)| {
                *epoch == self.epoch && &entry.executor == name && entry.tasks.contains(&ctx.name)
            })
    }
    /// Step 1 for an agent session: the executor role is unchanged (oid and
    /// generation), still a login task-only role, and the agent entry matches.
    fn check_agent_binding(&self) -> Result<(), CoreError> {
        let Role::Database { name } = &self.role else {
            return Err(CoreError::Policy(Error::Authentication));
        };
        self.service
            .admin
            .roles()
            .get(name)
            .map_err(role_lookup_error)?
            .filter(|role| {
                crate::agent::bindings::executor_role(role)
                    && self.role_fence.is_some_and(|(oid, generation)| {
                        role.oid == oid && role.generation == generation
                    })
            })
            .ok_or(CoreError::Policy(Error::Authentication))?;
        let current = self.service.agent.lock().map_err(|_| CoreError::Fenced)?;
        if self.agent_entry_matches(&current) {
            Ok(())
        } else {
            Err(CoreError::Policy(Error::Authentication))
        }
    }
    pub(crate) fn check_binding(&self) -> Result<(), CoreError> {
        if self.agent {
            return self.check_agent_binding();
        }
        if let Role::Database { name } = &self.role {
            self.service
                .admin
                .roles()
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| {
                    role.login
                        && !role.task_only
                        && self.role_fence.is_none_or(|(oid, _)| role.oid == oid)
                        && self.service.role_generation_matches(role, self.role_epoch())
                })
                .map(|_| ())
                .ok_or(CoreError::Policy(Error::Authentication))?;
            // Trusted pgwire sessions have no fingerprint binding to fence.
            if !self.fingerprint_bound {
                return Ok(());
            }
        }
        let credentials = self
            .service
            .bindings
            .lock()
            .map_err(|_| CoreError::Fenced)?;
        if credentials
            .entries
            .get(&self.fingerprint)
            .is_some_and(|(role, epoch)| *epoch == self.epoch && role == &self.role)
        {
            Ok(())
        } else {
            Err(CoreError::Policy(Error::Authentication))
        }
    }
    pub(crate) fn authority_scope<T>(
        &mut self,
        work: impl FnOnce(&mut Self) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        if self.scoped {
            self.check_binding()?;
            return work(self);
        }
        let service = self.service.clone();
        let _guard = loop {
            giggles_core::cancellation::check()?;
            match service.authority_gate.try_read() {
                Ok(guard) => break guard,
                Err(std::sync::TryLockError::Poisoned(_)) => return Err(CoreError::Fenced),
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
            }
        };
        self.check_binding()?;
        self.scoped = true;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(self)));
        self.scoped = false;
        match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    pub fn abort(&mut self) {
        self.service.advisory.release(self.backend_pid, true);
        self.sql.abort();
    }
    pub fn in_transaction(&self) -> bool {
        self.sql.in_transaction()
    }
    pub fn is_aborted(&self) -> bool {
        self.sql.is_aborted()
    }
    /// GD-0061 R3-110: one task statement (design-1 section 5) as this
    /// session's database role. Standalone like role DDL: 25001 inside a
    /// transaction block, 25P02 when aborted. A task session is refused by
    /// class (RoleStatement) before anything else; the agent path refuses it
    /// earlier, so this is the backstop. Errors keep their SQLSTATE and
    /// message.
    pub(crate) fn execute_task_ddl(
        &mut self,
        stmt: &giggles_core::task::statements::TaskStatement,
    ) -> Result<giggles_core::task::statements::TaskOutcome, giggles_core::task::catalog::TaskDdlError>
    {
        use crate::task_ddl::from_core;
        let fail = |sqlstate: &'static str, message: &str| giggles_core::task::catalog::TaskDdlError {
            sqlstate,
            message: message.to_owned(),
        };
        self.admit_backstop(giggles_core::task::StatementClass::RoleStatement)
            .map_err(from_core)?;
        let Role::Database { name } = &self.role else {
            return Err(fail("42501", "task statements need a database role"));
        };
        let name = name.clone();
        self.check_binding().map_err(from_core)?;
        if self.in_transaction() {
            return Err(fail("25001", "task statements cannot run inside a transaction block"));
        }
        if self.is_aborted() {
            return Err(fail(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        let actor = self
            .service
            .admin
            .roles()
            .get(&name)
            .map_err(|e| from_core(role_lookup_error(e)))?
            .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
            .ok_or_else(|| fail("28000", "role is not permitted to log in"))?;
        let now = crate::agent::wall_now(&self.service.clock).map_err(from_core)?;
        crate::task_ddl::run(self, &actor, stmt, now)
    }
    pub(crate) fn execute_admin(
        &mut self,
        stmt: &crate::auth::statements::AdminStatement,
    ) -> Result<&'static str, &'static str> {
        let class = match stmt {
            crate::auth::statements::AdminStatement::DropTable(..) => {
                giggles_core::task::StatementClass::Ddl
            }
            crate::auth::statements::AdminStatement::Error(_)
            | crate::auth::statements::AdminStatement::Create(..)
            | crate::auth::statements::AdminStatement::Alter(..)
            | crate::auth::statements::AdminStatement::Drop(..)
            | crate::auth::statements::AdminStatement::Grant { .. } => {
                giggles_core::task::StatementClass::RoleStatement
            }
        };
        self.admit_backstop(class).map_err(|error| sqlstate(&error))?;
        let Role::Database { name } = &self.role else {
            return Err("42501");
        };
        self.check_binding().map_err(|error| sqlstate(&error))?;
        if let crate::auth::statements::AdminStatement::DropTable(optional, names) = stmt
            && self.in_transaction()
            && self.sql.set_ddl_owner(Some(name))
        {
            let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
            let target = names.iter().map(|name| if let Some((schema, table)) = name.split_once('\0') {
                format!("{}.{}", quote(schema), quote(table))
            } else { quote(name) }).collect::<Vec<_>>().join(", ");
            let sql = format!("DROP TABLE {}{target}", if *optional { "IF EXISTS " } else { "" });
            self.run_control(&sql).map_err(|e| sqlstate(&e))?;
            return Ok("DROP TABLE");
        }
        if self.in_transaction() {
            return Err("25001");
        }
        if self.is_aborted() {
            return Err("25P02");
        }
        let actor = self
            .service
            .admin
            .roles()
            .get(name)
            .map_err(|e| sqlstate(&role_lookup_error(e)))?
            .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
            .ok_or("28000")?;
        // Serialize SQL password rotations and publish aliases before returning to wire checks.
        let mut aliases = self
            .service
            .password_generations
            .lock()
            .map_err(|_| "XX000")?;
        let target = match stmt {
            crate::auth::statements::AdminStatement::Alter(name, changes, None)
                if changes.password.is_some()
                    && changes.login.is_none()
                    && changes.superuser.is_none()
                    && changes.createrole.is_none() =>
            {
                self.service.admin.roles().get(name).map_err(|_| "XX000")?
            }
            _ => None,
        };
        let mut bound = stmt.clone();
        match &mut bound {
            crate::auth::statements::AdminStatement::DropTable(_, names)
            | crate::auth::statements::AdminStatement::Grant {
                schema: false,
                objects: names,
                ..
            } => {
                for name in names {
                    *name = self
                        .sql
                        .resolve_relation_reference(name)
                        .map_err(|e| sqlstate(&e))?;
                }
            }
            _ => {}
        }
        let result = crate::auth::statements::execute(&bound, &self.service.admin, &actor);
        // Even a multi-role DROP that returns an error may have committed a
        // preceding drop. Publish only after password aliases are ready.
        let result = (|| {
            let result = result?;
            if let Some(before) = target {
                let after = self
                    .service
                    .admin
                    .roles()
                    .get(&before.name)
                    .map_err(|_| "XX000")?
                    .ok_or("XX000")?;
                let mut expected = before.clone();
                expected.password = after.password.clone();
                expected.generation = after.generation;
                if expected == after && before.generation.checked_add(1) == Some(after.generation) {
                    let first = aliases
                        .get(&before.name)
                        .filter(|&&(oid, _, last)| oid == before.oid && last == before.generation)
                        .map_or(before.generation, |&(_, first, _)| first);
                    aliases.insert(before.name, (after.oid, first, after.generation));
                }
            }
            Ok(result)
        })();
        drop(aliases);
        self.service.binding_changes.send_replace(());
        result
    }
    /// Wire description of a prepared statement. DDL returns no rows and takes
    /// no parameters; core does not describe it, so report an empty shape.
    pub fn describe(&mut self, p: &Prepared) -> Result<relational::Description, CoreError> {
        if p.is_ddl() {
            return Ok(relational::Description {
                type_oids: vec![],
                typmods: vec![],
                parameters: vec![],
                columns: vec![],
            });
        }
        self.sql.describe(p)
    }
    /// A coherent committed catalog restricted to this live SQL session authority.
    /// Active SQL snapshots must agree with every returned table definition;
    /// metadata never falls back to an unrestricted owner query.
    pub(crate) fn catalog_snapshot(
        &mut self,
    ) -> Result<(u64, Vec<relational::TableDef>, Vec<giggles_core::IndexInfo>), CoreError> {
        self.admit_backstop(giggles_core::task::StatementClass::Catalog)?;
        self.authority_scope(|session| {
            if session.is_aborted() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if session.sql.task().is_some() {
                return session.task_catalog_snapshot();
            }
            let tables = match &session.role {
                Role::Application { domain, tables } => {
                    if session.service.admin.authority_metadata()?.0.to_string() != *domain {
                        return Err(CoreError::Policy(Error::Authentication));
                    }
                    Some(tables.clone())
                }
                Role::Database { name } => {
                    let role = session
                        .service
                        .admin
                        .roles()
                        .get(name)
                        .map_err(role_lookup_error)?
                        .filter(|role| {
                            role.login
                                && session.service.role_generation_matches(role, session.role_epoch())
                        })
                        .ok_or(CoreError::Policy(Error::Authentication))?;
                    if role.superuser {
                        None
                    } else {
                        Some(
                            session
                                .service
                                .admin
                                .roles()
                                .visible_tables(name)
                                .map_err(|_| CoreError::Policy(Error::Scope))?,
                        )
                    }
                }
                Role::Task { .. } => return Err(CoreError::Policy(Error::Scope)),
            };
            let metadata = session.sql.catalog_metadata()?;
            let epoch = metadata.presentation.epoch;
            let catalog = metadata.tables;
            let schemas = metadata.schemas;
            let visible = catalog
                .into_iter()
                .filter(|table| !session.sql.table_dropped_in_transaction(table.id))
                .filter(|table| {
                    tables.as_ref().is_none_or(|tables| {
                        tables.contains(&relational::relation_key(
                            schemas
                                .tables
                                .get(&table.id)
                                .map_or("public", String::as_str),
                            &table.name,
                        ))
                    })
                })
                .collect::<Vec<_>>();
            let indexes = metadata.indexes
                .into_iter()
                .filter(|index| visible.iter().any(|table| table.id == index.table))
                .collect();
            let (epoch, visible) = session.validate_catalog_snapshot(epoch, visible)?;
            Ok((epoch, visible, indexes))
        })
    }
    /// GD-0061 R3-312: what a task session's catalog shows. Only the task's
    /// table, with its readable and masked columns (and the storage-only heap
    /// key, which `pg_catalog_wire::snapshot` strips as for every session).
    /// Its primary key survives only when every key column is shown.
    pub(crate) fn task_catalog_view(&self) -> Result<TaskCatalogView, CoreError> {
        use giggles_core::task::{
            DenyRule, TaskError,
            columns::{ColumnMarks, effective_mode},
            context::TaskDefinitions,
            definition::ColumnMode,
        };
        let ctx = self.sql.task().ok_or(CoreError::Policy(Error::Scope))?;
        let def = self
            .sql
            .task_definition(&ctx.name)
            .map_err(CoreError::Task)?
            .filter(|def| def.id == ctx.task)
            .ok_or(CoreError::Policy(Error::Scope))?;
        if def.def_version != ctx.bound_def_version {
            return Err(CoreError::Task(TaskError::Denied(DenyRule::DefinitionChanged)));
        }
        let metadata = self.sql.catalog_metadata()?;
        let table = metadata
            .tables
            .iter()
            .find(|table| table.id == def.table)
            .ok_or(CoreError::Policy(Error::Scope))?;
        // Marks only turn a shown column into a withheld (masked) one; they
        // never show a hidden column, so the definition alone decides.
        let marks = ColumnMarks::new(std::iter::empty::<String>());
        let rowid = table.hidden_rowid();
        let mut columns = BTreeSet::new();
        let mut hidden = Vec::new();
        for (i, column) in table.columns.iter().enumerate() {
            if Some(i) == rowid
                || effective_mode(&def, &marks, &column.name).mode != ColumnMode::Hidden
            {
                columns.insert(column.id);
            } else {
                hidden.push(column.name.to_lowercase());
            }
        }
        hidden.extend(
            metadata
                .tables
                .iter()
                .filter(|other| other.id != table.id)
                .map(|other| other.name.to_lowercase()),
        );
        hidden.retain(|name| !name.is_empty());
        Ok(TaskCatalogView {
            table: table.id,
            schema: metadata
                .schemas
                .tables
                .get(&table.id)
                .cloned()
                .unwrap_or_else(|| "public".into()),
            columns,
            hidden,
        })
    }
    /// `catalog_snapshot` of a task session: the task table filtered by
    /// `task_catalog_view`, and its indexes on shown columns only.
    fn task_catalog_snapshot(
        &mut self,
    ) -> Result<(u64, Vec<relational::TableDef>, Vec<giggles_core::IndexInfo>), CoreError> {
        let view = self.task_catalog_view()?;
        let metadata = self.sql.catalog_metadata()?;
        let epoch = metadata.presentation.epoch;
        let visible = metadata
            .tables
            .into_iter()
            .filter(|table| table.id == view.table)
            .filter(|table| !self.sql.table_dropped_in_transaction(table.id))
            .map(|table| view.filter_table(table))
            .collect::<Vec<_>>();
        let indexes = metadata
            .indexes
            .into_iter()
            .filter(|index| {
                index.table == view.table
                    && index.columns.iter().all(|id| view.columns.contains(id))
                    && !view.mentions_hidden(&index.name)
            })
            .collect();
        // The generated `SELECT *` is expanded by the task's column rules, so
        // it describes exactly the shown columns.
        let (epoch, visible) = self.validate_catalog_snapshot(epoch, visible)?;
        Ok((epoch, visible, indexes))
    }
    #[allow(clippy::type_complexity)]
    pub(crate) fn catalog_snapshot_with_typmods(
        &mut self,
    ) -> Result<
        (
            u64,
            Vec<relational::TableDef>,
            Vec<giggles_core::IndexInfo>,
            BTreeMap<u64, i32>,
            relational::UserTypeCatalog,
        ),
        CoreError,
    > {
        self.admit_backstop(giggles_core::task::StatementClass::Catalog)?;
        self.authority_scope(|session| {
            let (epoch, tables, indexes) = session.catalog_snapshot()?;
            let metadata = session.sql.catalog_metadata()?;
            let mut typmods = metadata.typmods;
            if metadata.presentation.epoch != epoch {
                return Err(CoreError::Sql(relational::SqlError::Conflict));
            }
            typmods.retain(|id, _| {
                tables
                    .iter()
                    .any(|table| table.columns.iter().any(|column| column.id == *id))
            });
            let types = metadata.types;
            if types.epoch != epoch {
                return Err(CoreError::Sql(relational::SqlError::Conflict));
            }
            Ok((epoch, tables, indexes, typmods, types))
        })
    }
    fn validate_catalog_snapshot(
        &mut self,
        epoch: u64,
        visible: Vec<relational::TableDef>,
    ) -> Result<(u64, Vec<relational::TableDef>), CoreError> {
        if visible.len() > relational::MAX_TABLES {
            return Err(CoreError::Sql(relational::SqlError::Limit));
        }
        let schemas = self.sql.schema_metadata()?;
        let views = self.sql.catalog_metadata()?.presentation.views;
        for table in &visible {
            giggles_core::cancellation::check()?;
            if views.get(&table.id).is_some_and(|v| v.table == *table) { continue; }
            // Table names originate from the validated engine catalog, not
            // client SQL. Prepare/Describe do not access application rows.
            let schema = schemas
                .tables
                .get(&table.id)
                .map_or("public", String::as_str);
            let prepared = self.sql.prepare(&format!(
                "SELECT * FROM \"{}\".\"{}\" LIMIT 1",
                schema.replace('"', "\"\""),
                table.name.replace('"', "\"\"")
            ))?;
            let description = self.sql.describe(&prepared)?;
            if description.columns != table.logical_columns().cloned().collect::<Vec<_>>() {
                return Err(CoreError::Sql(relational::SqlError::Conflict));
            }
        }
        // A concurrent trusted DDL operation cannot splice two generations
        // into this snapshot or make validation certify a different catalog.
        if self.sql.catalog_metadata()?.presentation.epoch != epoch {
            return Err(CoreError::Sql(relational::SqlError::Conflict));
        }
        Ok((epoch, visible))
    }
    pub fn prepare(&mut self, sql: &str) -> Result<Prepared, CoreError> {
        self.prepare_with_parameter_types(sql, &[])
    }
    pub(crate) fn prepare_with_parameter_types(
        &mut self,
        sql: &str,
        declared_types: &[Option<relational::DataType>],
    ) -> Result<Prepared, CoreError> {
        self.prepare_parameter_types_inner(sql, declared_types, false)
    }
    pub(crate) fn prepare_unnamed(
        &mut self, sql: &str, declared_types: &[Option<relational::DataType>],
    ) -> Result<Prepared, CoreError> {
        self.prepare_parameter_types_inner(sql, declared_types, true)
    }
    fn prepare_parameter_types_inner(
        &mut self, sql: &str, declared_types: &[Option<relational::DataType>], unnamed: bool,
    ) -> Result<Prepared, CoreError> {
        fn declared(
            p: Prepared,
            types: &[Option<relational::DataType>],
        ) -> Result<Prepared, CoreError> {
            // Leave excess wire declarations to pgwire's protocol count check.
            let types = if types.len() > p.parameter_count() {
                &[]
            } else {
                types
            };
            p.with_parameter_types(types).map_err(CoreError::Sql)
        }
        fn parse_error(session: &mut Session, error: CoreError) -> CoreError {
            // DD-0068 E939: sessions without SQL authority get 42501 before core's parse-time routine classification can answer.
            if !session.role.is_sql()
                && matches!(
                    error,
                    CoreError::Sql(
                        relational::SqlError::Unsupported
                            | relational::SqlError::UndefinedFunction
                            | relational::SqlError::AmbiguousFunction
                    )
                )
            {
                session.sql.abort();
                CoreError::Policy(Error::Scope)
            } else {
                error
            }
        }
        self.authority_scope(|session| {
            // GD-0061: a task session admits the statement's class before the
            // prepare-time column rules run, as on the agent pgwire path (an
            // UPDATE without OPERATION is 42T04, not a column-rule error).
            if session.sql.task().is_some()
                && let Ok(class) = Prepared::task_class_of_sql(sql)
            {
                session.admit_backstop(class)?;
            }
            if relational::Copy::is_copy(sql) {
                session.authorize_copy_sql(sql)?;
                let result = relational::Copy::parse(sql).map_err(CoreError::Sql);
                session.abort();
                result?;
                return Err(CoreError::Sql(relational::SqlError::Unsupported));
            }
            session.reconcile_sequence_owners()?;
            let comment_target = Prepared::comment_table_target(sql).map_err(|error| {
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            if let Some(table) = comment_target { session.authorize_alter_table(&table)?; }
            // Parse the complete ALTER before checking authority, but do not bind
            // its table, columns, or defaults until ownership has been checked.
            let alter_target = Prepared::alter_table_target(sql).map_err(|error| {
                // Match SqlSession::prepare's failed-statement transaction
                // semantics even though ALTER parsing precedes core binding.
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            if let Some(table) = alter_target {
                session.authorize_alter_table(&table)?;
            }
            let drop_targets = Prepared::drop_table_targets(sql).map_err(|error| {
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            for table in drop_targets {
                session.authorize_alter_table(&table)?;
            }
            let sequence_targets = Prepared::sequence_ddl_targets(sql).map_err(|error| {
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            // CREATE uses schema authority; ALTER/DROP must check owner keys
            // before core can resolve a sequence or its dependencies.
            if !sequence_targets.is_empty() {
                let create = Prepared::sequence_ddl_is_create(sql).map_err(CoreError::Sql)?;
                if create {
                    session.authorize_create_sequence()?;
                } else {
                    for sequence in sequence_targets {
                        session.authorize_alter_table(&sequence)?;
                    }
                }
                if let Some(table) = Prepared::sequence_owned_by(sql).map_err(CoreError::Sql)? {
                    session.authorize_alter_table(&table)?;
                }
            }
            // REFERENCES requires ownership before binding can reveal whether
            // the referenced relation, columns, or candidate key exist.
            let references = Prepared::constraint_references(sql).map_err(|error| {
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            for table in references {
                session.authorize_alter_table(&table)?;
            }
            session.authorize_sequence_sql(sql)?;
            let p = if matches!(&session.role, Role::Database { .. }) {
                declared(
                    (if unnamed && session.sql.task().is_none() {
                        session.sql.prepare_unnamed(sql, declared_types)
                    } else {
                        session.sql.prepare(sql)
                    }).map_err(|error| parse_error(session, error))?,
                    declared_types,
                )?
            } else if session.in_transaction() && !session.is_aborted() {
                // Gateway DDL is forbidden. Bind reusable plans against the
                // committed catalog, then validate against the active snapshot.
                // Core's staged-schema handles retain their lifetime fence.
                let p = declared(
                    session
                        .service
                        .admin
                        .sql_session()
                        .prepare(sql)
                        .map_err(|error| parse_error(session, error))?,
                    declared_types,
                )?;
                // The committed-catalog session has no task: run the task's
                // column rules before describe binds the columns, as
                // SqlSession::prepare does (no 42703 vs rule-error oracle).
                let p = if session.sql.task().is_some() {
                    session
                        .sql
                        .task_column_rules(&p)
                        .inspect_err(|_| session.sql.abort())?
                } else {
                    p
                };
                session.sql.describe(&p)?;
                p
            } else {
                declared(
                    session
                        .sql
                        .prepare(sql)
                        .map_err(|error| parse_error(session, error))?,
                    declared_types,
                )?
            };
            session.authorize(&p)?;
            Ok(p)
        })
    }
    /// Check sequence authority before binding can resolve an object name.
    /// Ownership of the owning table is this profile's analogue of PostgreSQL
    /// sequence USAGE/UPDATE privileges; ordinary table grants do not confer it.
    pub(crate) fn authorize_sequence_sql(&mut self, sql: &str) -> Result<(), CoreError> {
        let dependencies = Prepared::sequence_dependencies(sql);
        if dependencies.is_empty() {
            return Ok(());
        }
        if self.sql.task().is_some() {
            // D7: sequences are never reachable from a task statement.
            return Err(CoreError::Task(giggles_core::task::TaskError::Denied(
                giggles_core::task::DenyRule::Sequence,
            )));
        }
        self.authority_scope(|session| session.authorize_sequences(&dependencies))
    }
    fn authorize_create_sequence(&mut self) -> Result<(), CoreError> {
        let allowed = if let Role::Database { name } = &self.role {
            let roles = self.service.admin.roles();
            let role = roles
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
                .ok_or(CoreError::Policy(Error::Authentication))?;
            role.superuser
                || roles
                    .schema_privileges("public", name)
                    .is_ok_and(|privileges| privileges.contains(&roles::SchemaPrivilege::Create))
        } else {
            false
        };
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        Ok(())
    }
    fn authorize_sequences(&mut self, dependencies: &[Option<String>]) -> Result<(), CoreError> {
        let sequences = self.sql.sequence_owners()?;
        let image = match &self.role {
            Role::Database { .. } => Some(self.service.admin.roles().image()
                .map_err(|_| CoreError::Policy(Error::Authentication))?),
            _ => None,
        };
        let allowed_table = |table: &str| -> bool {
            match (&self.role, &image) {
                (Role::Application { tables, .. }, _) => tables.contains(table),
                (Role::Database { name }, Some(image)) => {
                    self.sql
                        .table_authority_name(table)
                        .is_ok_and(|durable_name| {
                            self.sql
                                .table_owner_in(image, &durable_name)
                                .is_ok_and(|owner| owner.as_deref() == Some(name.as_str()))
                        })
                }
                _ => false,
            }
        };
        let allowed_standalone = |sequence: &str| -> bool {
            matches!(&self.role, Role::Database { .. }) && allowed_table(sequence)
        };
        let superuser = if let (Role::Database { name }, Some(image)) = (&self.role, &image) {
            image
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
                .ok_or(CoreError::Policy(Error::Authentication))?
                .superuser
        } else {
            false
        };
        let allowed_tables: BTreeSet<String> = if superuser {
            BTreeSet::new()
        } else {
            sequences
            .iter()
            .filter_map(|(sequence, table)| match table {
                Some(table) if allowed_table(table) => Some(table.clone()),
                None if allowed_standalone(sequence) => Some(sequence.clone()),
                _ => None,
            })
            .collect()
        };
        let live_context = superuser || !allowed_tables.is_empty();
        let allowed = live_context
            && dependencies.iter().all(|dependency| {
                if superuser {
                    return true;
                }
                // Dynamic names are checked against the same table scope by the
                // execution callback before any counter state is read or changed.
                let Some(name) = dependency else {
                    return true;
                };
                let Ok(name) = self.sql.normalize_sequence_reference(name) else {
                    return true;
                };
                sequences
                    .iter()
                    .find(|(sequence, _)| sequence == &name)
                    .is_none_or(|(sequence, table)| match table {
                        Some(table) => allowed_table(table),
                        None => allowed_standalone(sequence),
                    })
            });
        self.sql
            .set_sequence_tables((!superuser).then_some(allowed_tables));
        if dependencies.is_empty() {
            return Ok(());
        }
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        Ok(())
    }
    fn authorize_alter_table(&mut self, table: &str) -> Result<(), CoreError> {
        let resolved = self.sql.resolve_relation_reference(table)?;
        let table = resolved.as_str();
        let allowed = if let Role::Database { name } = &self.role {
            let roles = self.service.admin.roles();
            let role = roles
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
                .ok_or(CoreError::Policy(Error::Authentication))?;
            if role.superuser {
                true
            } else {
                // The durable key follows staged renames; missing names remain
                // ordinary absent ownership records and cannot leak 42P01.
                let name_at_commit = self.sql.table_authority_name(table)?;
                self.sql
                    .table_owner(&name_at_commit)
                    .map_err(|_| CoreError::Policy(Error::Scope))?
                    .as_deref()
                    == Some(name.as_str())
            }
        } else {
            false
        };
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        Ok(())
    }
    pub(crate) fn validate_access_share(&mut self, tables: &[String]) -> Result<(), CoreError> {
        self.admit_backstop(giggles_core::task::StatementClass::Lock)?;
        self.authority_scope(|session| {
            if session.is_aborted() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if !session.role.is_sql() {
                return Err(CoreError::Policy(Error::Scope));
            }
            for table in tables {
                let prepared = session
                    .sql
                    .prepare(&format!("SELECT * FROM {table} LIMIT 0"))?;
                let table = prepared
                    .table()
                    .ok_or(CoreError::Sql(relational::SqlError::UnknownTable))?;
                let allowed = match &session.role {
                    Role::Database { name } => {
                        let role = session
                            .service
                            .admin
                            .roles()
                            .get(name)
                            .map_err(role_lookup_error)?
                            .ok_or(CoreError::Policy(Error::Authentication))?;
                        // PG18 accepts any of SELECT/INSERT/UPDATE/DELETE,
                        // TRUNCATE or MAINTAIN; the latter two are not modeled.
                        role.superuser
                            || [
                                roles::TablePrivilege::Select,
                                roles::TablePrivilege::Insert,
                                roles::TablePrivilege::Update,
                                roles::TablePrivilege::Delete,
                            ]
                            .into_iter()
                            .any(|privilege| {
                                session
                                    .table_privilege_allowed(table, name, Some(privilege))
                                    .unwrap_or(false)
                            })
                    }
                    Role::Application { tables, .. } => tables.contains(table),
                    Role::Task { .. } => false,
                };
                if !allowed {
                    return Err(CoreError::Policy(Error::Scope));
                }
                session.sql.describe(&prepared)?;
            }
            // Serializable snapshots pin the dump's schema and reads against
            // concurrent DDL, so ACCESS SHARE needs validation, not a new lock.
            Ok(())
        })
    }

    fn table_privilege_allowed(
        &self,
        table: &str,
        name: &str,
        privilege: Option<roles::TablePrivilege>,
    ) -> Result<bool, CoreError> {
        let name_at_commit = self.sql.table_authority_name(table)?;
        let owner = self
            .sql
            .table_owner(&name_at_commit)
            .map_err(|_| CoreError::Policy(Error::Scope))?;
        Ok(owner.as_deref() == Some(name)
            || privilege.is_some_and(|privilege| {
                self.sql
                    .table_privileges(&name_at_commit, name)
                    .is_ok_and(|privileges| privileges.contains(&privilege))
            }))
    }
    pub(crate) fn table_privilege_superuser(&self) -> Result<bool, CoreError> {
        let Role::Database { name } = &self.role else {
            return Ok(false);
        };
        self.service
            .admin
            .roles()
            .get(name)
            .map_err(role_lookup_error)?
            .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
            .map(|role| role.superuser)
            .ok_or(CoreError::Policy(Error::Authentication))
    }
    fn table_requirements_allowed(
        &self,
        reqs: &[(String, roles::TablePrivilege)],
        superuser: bool,
    ) -> Result<bool, CoreError> {
        if matches!(&self.role, Role::Task { .. }) {
            return Ok(false);
        }
        for (table, privilege) in reqs {
            let allowed = match &self.role {
                Role::Application { tables, .. } => {
                    table != "giggles_join" && tables.contains(table)
                }
                Role::Database { name } => {
                    superuser || self.table_privilege_allowed(table, name, Some(*privilege))?
                }
                Role::Task { .. } => false,
            };
            if !allowed {
                return Ok(false);
            }
        }
        Ok(true)
    }
    /// Check ordered table requirements before binding or classifying a request.
    /// Names are SQL authority keys (including staged names); only authority
    /// metadata is read. Sequence and DDL authority remain separate checks.
    #[allow(dead_code)] // Entry point for the subsequent native integration.
    pub(crate) fn authorize_table_requirements(
        &mut self,
        reqs: &[(String, roles::TablePrivilege)],
    ) -> Result<(), CoreError> {
        self.authority_scope(|session| {
            let superuser = session.table_privilege_superuser()?;
            if !session.table_requirements_allowed(reqs, superuser)? {
                session.sql.abort();
                return Err(CoreError::Policy(Error::Scope));
            }
            Ok(())
        })
    }
    pub(crate) fn authorize_native_sequence(&mut self, name: &str) -> Result<(), CoreError> {
        self.authority_scope(|session| session.authorize_sequences(&[Some(name.to_owned())]))
    }
    pub(crate) fn authorize_native_catalog(&mut self, request: &giggles_core::operations::CatalogRequest) -> Result<(), CoreError> {
        use giggles_core::operations::CatalogRequest as C;
        // Same rules as SQL CREATE SEQUENCE / DROP SEQUENCE in `authorize`.
        match request {
            C::CreateSequence { .. } => return self.authorize_create_sequence(),
            C::DropSequence { name, .. } => return self.authorize_alter_table(name),
            _ => {}
        }
        if matches!(request, C::AddConstraint { .. } | C::DropConstraint { .. }) {
            let table = match request { C::AddConstraint { table, .. } | C::DropConstraint { table, .. } => table, _ => unreachable!() };
            self.authorize_alter_table(&self.sql.native_relation_reference(table)?)?;
            if let C::AddConstraint { constraint: giggles_core::operations::ConstraintSpec::ForeignKey { ref_table, .. }, .. } = request {
                self.authorize_alter_table(&self.sql.native_relation_reference(ref_table)?)?;
            }
            return Ok(());
        }
        let allowed = if !matches!(self.role, Role::Database { .. }) { false }
        else if self.table_privilege_superuser()? { true }
        else {
            let Role::Database { name } = &self.role else { unreachable!() };
            let table = match request {
                C::DropTable { table, .. } | C::AlterTable { table, .. } | C::DropView { name: table, .. } => Some(self.sql.native_relation_reference(table)?),
                C::DropIndex { name, .. } => self.sql.native_index_table(name)?,
                _ => None,
            };
            if let Some(table) = table {
                let key = self.sql.table_authority_name(&table)?;
                self.sql.table_owner(&key).map_err(|_| CoreError::Policy(Error::Scope))?.as_deref() == Some(name)
            } else {
                let schema = match request {
                    C::CreateTable { table, .. } | C::CreateView { name: table, .. } => self.sql.native_creation_schema(table)?,
                    C::CreateIndex { table, .. } => self.sql.relation_schema(&self.sql.native_relation_reference(table)?)?,
                    _ => self.sql.relation_schema("")?,
                };
                let create = self.sql.schema_metadata()?.schemas.get(&schema).is_some_and(|(_, owner)| owner == name)
                    || self.service.admin.roles().schema_privileges(&schema, name)
                        .map_err(|_| CoreError::Policy(Error::Scope))?.contains(&roles::SchemaPrivilege::Create);
                // SQL CREATE VIEW also needs SELECT on every relation the view reads.
                create && match request {
                    C::CreateView { read, .. } => {
                        let sources = match read {
                            relational::ViewRead::Single(read) => vec![read.table.as_str()],
                            relational::ViewRead::Two(read) => vec![read.left.table.as_str(), read.right.table.as_str()],
                        };
                        let mut reqs = Vec::new();
                        for table in sources {
                            reqs.push((self.sql.native_relation_reference(table).unwrap_or_else(|_| table.to_owned()), roles::TablePrivilege::Select));
                        }
                        self.table_requirements_allowed(&reqs, false)?
                    }
                    _ => true,
                }
            }
        };
        if !allowed { self.sql.abort(); return Err(CoreError::Policy(Error::Scope)); }
        Ok(())
    }
    fn authorize(&mut self, p: &Prepared) -> Result<(), CoreError> {
        self.admit_backstop(p.task_class())?;
        if p.is_collation_ddl() {
            let allowed = if let Role::Database { name } = &self.role {
                let roles = self.service.admin.roles();
                let role = roles.get(name).map_err(|_| CoreError::Policy(Error::Authentication))?
                    .filter(|r| r.login && self.service.role_generation_matches(r, self.role_epoch()))
                    .ok_or(CoreError::Policy(Error::Authentication))?;
                if let Some(owner) = p.collation_new_owner() && roles.get(owner).map_err(|_| CoreError::Policy(Error::Scope))?.is_none() {
                    return Err(CoreError::Sql(relational::SqlError::UndefinedObject));
                }
                let targets = self.sql.collation_authority(p)?;
                role.superuser || targets.iter().all(|(schema, owner)| match owner {
                    Some(owner) => owner == name && p.collation_new_owner().is_none_or(|o| o == name),
                    None => self.sql.schema_metadata().ok().is_some_and(|m| m.schemas.get(schema).is_some_and(|(_,o)| o == name))
                        || roles.schema_privileges(schema,name).is_ok_and(|v| v.contains(&roles::SchemaPrivilege::Create)),
                })
            } else { false };
            if !allowed {
                let targets = self.sql.collation_authority(p)?;
                p.collation_permission_error(targets.iter().find(|(_,o)| o.is_none()).map(|(s,_)| s.as_str()));
                self.sql.abort(); return Err(CoreError::Policy(Error::Scope));
            }
            return Ok(());
        }
        if let Some((schema, owner, create)) = p.schema_ddl() {
            let allowed = if let Role::Database { name } = &self.role {
                let roles = self.service.admin.roles();
                let role = roles
                    .get(name)
                    .map_err(role_lookup_error)?
                    .filter(|r| r.login && self.service.role_generation_matches(r, self.role_epoch()))
                    .ok_or(CoreError::Policy(Error::Authentication))?;
                if let Some(owner) = owner
                    && roles
                        .get(owner)
                        .map_err(|_| CoreError::Policy(Error::Scope))?
                        .is_none()
                {
                    return Err(CoreError::Sql(relational::SqlError::UndefinedObject));
                }
                role.superuser
                    || (!create
                        && self
                            .sql
                            .schema_metadata()?
                            .schemas
                            .get(schema)
                            .is_some_and(|(_, owner)| owner == name))
            } else {
                false
            };
            if !allowed {
                self.sql.abort();
                return Err(CoreError::Policy(Error::Scope));
            }
            return Ok(());
        }

        for table in p.constraint_reference_tables() {
            self.authorize_alter_table(&table)?;
        }
        if p.is_sequence_ddl() {
            if p.command() == "CREATE SEQUENCE" {
                self.authorize_create_sequence()?;
            } else {
                for sequence in p.sequence_ddl_names() {
                    self.authorize_alter_table(sequence)?;
                }
            }
            if let Some(table) = p.sequence_owned_by_table() {
                self.authorize_alter_table(table)?;
            }
            return Ok(());
        }
        self.authorize_sequences(&p.sequences())?;
        if p.sequence_only() {
            return Ok(());
        }
        // E939: collect every child dependency before table-free/routine classification.
        // Routine/sequence authority and classification keep their existing gates.
        let read_tables = p.authorization_tables().map_err(CoreError::Sql)?;
        let write_tables = p.write_tables();
        let update_table = p.authorization_update_table();
        let relation_free_plan = p.is_planner_statement()
            && read_tables.is_empty()
            && write_tables.is_empty();
        // Planner writes must reach the per-table read/write requirements below.
        let modifying_plan = p.is_planner_statement() && !write_tables.is_empty();
        let joined_read = !read_tables.is_empty()
            && matches!(p.command(), "SELECT" | "EXPLAIN")
            && p.is_multi_source_read();
        let allowed = if matches!(&self.role, Role::Application { .. }) && !p.is_ddl() {
            match p.table() {
                Some(_) if p.is_multi_source_read() => joined_read || relation_free_plan || modifying_plan,
                Some(table) => self.table_requirements_allowed(
                    &[(table.to_owned(), roles::TablePrivilege::Select)],
                    false,
                )?,
                None => true,
            }
        } else {
            false
        };
        let mut superuser = false;
        let allowed = if let Role::Database { name } = &self.role {
            if self.table_privilege_superuser()? {
                superuser = true;
                true
            } else if matches!(p.command(), "COMMENT" | "SECURITY LABEL") || p.is_alter_table()
                || (p.is_view_replacement() && self.sql.relation_exists(p.table().unwrap_or(""))?)
                || p.is_drop_table()
                || (p.command() == "DROP INDEX" && p.table().is_some())
            {
                let targets = if p.is_drop_table() { p.drop_tables().iter().map(String::as_str).collect() }
                    else { vec![p.table().ok_or(CoreError::Policy(Error::Scope))?] };
                let mut owns_all = true;
                for table in targets {
                    let name_at_commit = self.sql.table_authority_name(table)?;
                    owns_all &= self.sql.table_owner(&name_at_commit)
                        .map_err(|_| CoreError::Policy(Error::Scope))?.as_deref() == Some(name.as_str());
                }
                owns_all
            } else if p.is_ddl() {
                let schema = if matches!(p.command(), "CREATE TABLE" | "CREATE VIEW") {
                    self.sql.creation_schema(p)?
                } else {
                    self.sql.relation_schema(p.table().unwrap_or(""))?
                };
                self.sql
                    .schema_metadata()?
                    .schemas
                    .get(&schema)
                    .is_some_and(|(_, owner)| owner == name)
                    || self
                        .service
                        .admin
                        .roles()
                        .schema_privileges(&schema, name)
                        .map_err(|_| CoreError::Policy(Error::Scope))?
                        .contains(&roles::SchemaPrivilege::Create)
            } else if let Some(table) = p.table() {
                if p.is_multi_source_read() {
                    joined_read || relation_free_plan || modifying_plan
                } else {
                    let privilege = match p.command() {
                        "SELECT" | "EXPLAIN" => Some(roles::TablePrivilege::Select),
                        "INSERT" => Some(roles::TablePrivilege::Insert),
                        "UPDATE" => Some(roles::TablePrivilege::Update),
                        "DELETE" | "TRUNCATE TABLE" => Some(roles::TablePrivilege::Delete),
                        _ => None,
                    };
                    if let Some(privilege) = privilege {
                        self.table_requirements_allowed(&[(table.to_owned(), privilege)], false)?
                    } else {
                        self.table_privilege_allowed(table, name, None)?
                    }
                }
            } else {
                true
            }
        } else {
            allowed
        };
        // DD-0068 F9 (GD-0001 Q8): on the PostgreSQL wire, SQL roles reach bind for deferred-routine-only SELECTs, so 42883/3F000 surface as in PG 18; native surfaces keep 42501.
        let allowed = allowed
            || (self.surface == Surface::Postgres
                && self.role.is_sql()
                && p.deferred_routine_only());
        let mut allowed = allowed;
        if allowed && !superuser {
            let mut reqs: Vec<_> = read_tables.into_iter()
                .map(|table| (table, roles::TablePrivilege::Select))
                .collect();
            if let Some(table) = update_table {
                reqs.push((table.to_owned(), roles::TablePrivilege::Update));
            }
            for (table, command) in write_tables {
                let privilege = match command {
                    "INSERT" => roles::TablePrivilege::Insert,
                    "UPDATE" => roles::TablePrivilege::Update,
                    "DELETE" => roles::TablePrivilege::Delete,
                    _ => continue,
                };
                reqs.push((table, privilege));
            }
            allowed = self.table_requirements_allowed(&reqs, false)?;
        }
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        if p.command() == "TRUNCATE TABLE" && !superuser {
            for table in self.sql.truncate_tables(p)? {
                let permitted = match &self.role {
                    Role::Database { name } => self.table_privilege_allowed(
                        &table,
                        name,
                        Some(roles::TablePrivilege::Delete),
                    )?,
                    Role::Application { tables, .. } => tables.contains(&table),
                    _ => false,
                };
                if !permitted {
                    self.sql.abort();
                    return Err(CoreError::Policy(Error::Scope));
                }
            }
        }
        // Ordinary table/DDL authority precedes implicit default metadata:
        // denied sessions must not discover whether a target table exists.
        let implicit = self.sql.implicit_sequence_dependencies(p)?;
        self.authorize_sequences(&implicit)?;
        Ok(())
    }
    fn authorize_copy_sql(&mut self, sql: &str) -> Result<(), CoreError> {
        self.admit_backstop(giggles_core::task::StatementClass::Copy)?;
        if !self.role.is_sql() {
            self.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        let unrestricted = if let Role::Database { name } = &self.role {
            self.service
                .admin
                .roles()
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| role.login && self.service.role_generation_matches(role, self.role_epoch()))
                .ok_or(CoreError::Policy(Error::Authentication))?
                .superuser
        } else {
            false
        };
        let target = match relational::Copy::target_reference(sql) {
            Ok(Some(target)) => self.sql.resolve_relation_reference(&target)?,
            result => {
                self.abort();
                if !unrestricted {
                    return Err(CoreError::Policy(Error::Scope));
                }
                return Err(CoreError::Sql(
                    result.err().unwrap_or(relational::SqlError::Syntax),
                ));
            }
        };
        let allowed = match &self.role {
            Role::Application { tables, .. } => tables.contains(&target),
            Role::Database { name } => {
                let target = self.sql.table_authority_name(&target)?;
                unrestricted
                    || self
                        .sql
                        .table_owner(&target)
                        .is_ok_and(|owner| owner.as_deref() == Some(name.as_str()))
                    || self
                        .sql
                        .table_privileges(&target, name)
                        .is_ok_and(|privileges| privileges.contains(&roles::TablePrivilege::Insert))
            }
            _ => false,
        };
        if !allowed {
            self.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        Ok(())
    }
    pub(crate) fn prepare_copy(
        &mut self,
        sql: &str,
    ) -> Result<(relational::Copy, Prepared), CoreError> {
        self.authority_scope(|session| {
            session.authorize_copy_sql(sql)?;
            session.reconcile_sequence_owners()?;
            let (copy, prepared) = session.sql.prepare_copy(sql)?;
            session.authorize(&prepared)?;
            Ok((copy, prepared))
        })
    }
    pub(crate) fn copy_columns(
        &mut self,
        copy: &relational::Copy,
        prepared: &Prepared,
    ) -> Result<Vec<relational::ColumnDef>, CoreError> {
        self.authority_scope(|session| {
            session.authorize(prepared)?;
            session.sql.copy_columns(copy)
        })
    }
    pub(crate) fn copy_clock(&mut self) -> StatementClock {
        if !self.sql.in_transaction() {
            self.transaction_us = None;
        }
        let (statement_us, unavailable) = match self.service.clock.sql_micros() {
            Ok(sample) => (sample, None),
            Err(error) => (self.transaction_us.unwrap_or(0), Some(error)),
        };
        let transaction_us = self.transaction_us.unwrap_or(statement_us);
        if self.sql.in_transaction() && unavailable.is_none() {
            self.transaction_us = Some(transaction_us);
        }
        let service = self.service.clone();
        StatementClock {
            unavailable,
            transaction_us,
            statement_us,
            zone: self.zone,
            wall: Some(Arc::new(move || service.clock.sql_micros())),
        }
    }
    pub(crate) fn normalize_copy_rows(
        &mut self,
        prepared: &Prepared,
        rows: Vec<Vec<Value>>,
        clock: &StatementClock,
    ) -> Result<(Prepared, Vec<Vec<Value>>), CoreError> {
        self.authority_scope(|session| {
            session.reconcile_sequence_owners()?;
            session.authorize(prepared)?;
            session.sql.normalize_copy_rows_at(prepared, rows, clock)
        })
    }
    pub(crate) fn finish_copy_input(
        &mut self,
        authority: &Prepared,
        prepared: &Prepared,
        clock: &StatementClock,
    ) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| {
            session.authorize(authority)?;
            let result = session.sql.finish_copy_input_at(prepared, clock);
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            }
            result
        })
    }
    pub(crate) fn ingest_normalized_copy(
        &mut self,
        authority: &Prepared,
        prepared: &Prepared,
        rows: Vec<Vec<Value>>,
        clock: &StatementClock,
    ) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| {
            session.reconcile_sequence_owners()?;
            session.authorize(authority)?;
            let result = session.sql.ingest_copy_at(prepared, rows, clock);
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            }
            result
        })
    }
    #[cfg(test)]
    pub(crate) fn ingest_copy(
        &mut self,
        prepared: &Prepared,
        rows: Vec<Vec<Value>>,
    ) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| {
            session.reconcile_sequence_owners()?;
            session.authorize(prepared)?;
            let (statement_us, unavailable) = match session.service.clock.sql_micros() {
                Ok(sample) => (sample, None),
                Err(error) => (session.transaction_us.unwrap_or(0), Some(error)),
            };
            let transaction_us = session.transaction_us.unwrap_or(statement_us);
            let service = session.service.clone();
            let clock = StatementClock {
                unavailable,
                transaction_us,
                statement_us,
                zone: session.zone,
                wall: Some(Arc::new(move || service.clock.sql_micros())),
            };
            let result = session.sql.ingest_copy_at(prepared, rows, &clock);
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            } else if clock.unavailable.is_none() {
                session.transaction_us = Some(transaction_us);
            }
            result
        })
    }
    /// One chunk of `cursor`, held until `release_page` (GD-0061 R3-310). Every
    /// server caller releases it before encoding; there is no plain exit
    /// (R3-311 removed the last caller).
    pub(crate) fn fetch_cursor_unreleased(
        &mut self,
        cursor: &mut Cursor,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Unreleased<Vec<giggles_core::relational::SqlRow>>, CoreError> {
        if self.is_aborted() {
            return Err(CoreError::Sql(giggles_core::relational::SqlError::Aborted));
        }
        let result = cursor.fetch_unreleased(max_rows, max_bytes);
        if result.is_err() {
            self.abort();
        }
        result
    }
    /// Releases one page of this session's results (core
    /// `SqlSession::release_page`): a move outside a task session; in a task
    /// session the page is masked, checked and charged before it is handed
    /// over, and on any error nothing is. A release error is an execution
    /// error: it aborts an open transaction like any other (GD-0061 R3-311).
    pub fn release_page<T: Page>(
        &mut self,
        page: Unreleased<T>,
    ) -> Result<Released<T>, CoreError> {
        let released = self.sql.release_page(page);
        if released.is_err() {
            self.abort();
        }
        released
    }
    /// A native keyset page (`SqlSession::read_page`). It has no release twin
    /// (design-3 section 4), so a task session is refused here, before core
    /// runs anything (GD-0061 R3-313).
    pub(crate) fn read_native_page(
        &mut self,
        request: &giggles_core::operations::Request,
        after: Option<&[Value]>,
        bindings: &giggles_core::operations::Bindings,
    ) -> Result<giggles_core::operations::Result<giggles_core::operations::Page>, CoreError> {
        if self.sql.task().is_some() {
            return Err(task_session_refusal());
        }
        Ok(self.sql.read_page(request, after, bindings))
    }
    /// SQL text over the API (`Request::Sql`), held until `release_page`. In
    /// a task session the statement is prepared and admitted first, and a
    /// refusal there, which never reaches core, is logged once; the read-log
    /// text is set before the statement runs (GD-0061 R3-313).
    fn execute_api_sql(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        if self.sql.task().is_none() {
            return self.execute_sql_unreleased(sql, params);
        }
        self.authority_scope(|session| {
            let refused = |session: &Session, class, error: CoreError| {
                session
                    .sql
                    .log_refused_task_statement(sql, class, params.len(), &error);
                Err(error)
            };
            // `prepare` admits the statement's class itself, before it parses
            // the whole statement; the refusal is logged with that class.
            let p = match session.prepare(sql) {
                Ok(p) => p,
                Err(error) => {
                    let class = Prepared::task_class_of_sql(sql)
                        .unwrap_or(giggles_core::task::StatementClass::Other);
                    return refused(session, class, error);
                }
            };
            if let Err(error) = session.admit_backstop(p.task_class()) {
                return refused(session, p.task_class(), error);
            }
            session.sql.set_task_statement_text(sql);
            session.run_prepared(&p, params, false)
        })
    }
    /// Validity only (revoked, expired, definition changed), for re-serving
    /// rows of a page that was already released and charged. No-op outside a
    /// task session.
    pub fn still_valid(&self) -> Result<(), CoreError> {
        self.sql.still_valid()
    }
    pub fn open_cursor(&mut self, p: &Prepared, params: &[Value]) -> Result<Cursor, CoreError> {
        self.authority_scope(|session| {
            let bound = session.sql.bind_relation_context(p)?;
            session.reconcile_sequence_owners()?;
            session.authorize(&bound)?;
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            }
            let (statement_us, unavailable) = match session.service.clock.sql_micros() {
                Ok(sample) => (sample, None),
                Err(error) => (session.transaction_us.unwrap_or(0), Some(error)),
            };
            let transaction_us = session.transaction_us.unwrap_or(statement_us);
            let service = session.service.clone();
            let clock = StatementClock {
                unavailable,
                transaction_us,
                statement_us,
                zone: session.zone,
                wall: Some(Arc::new(move || service.clock.sql_micros())),
            };
            let result = session.sql.open_cursor_at(&bound, params, &clock);
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            } else if clock.unavailable.is_none() {
                session.transaction_us = Some(transaction_us);
            }
            result
        })
    }
    /// Copy the effective pgwire setting after its transaction has started.
    pub(crate) fn sync_replication_role(
        &mut self,
        settings: &crate::pg_settings::SessionSettings,
    ) -> Result<(), CoreError> {
        if self.sql.in_transaction() && !self.sql.is_aborted() {
            self.sql.set_replication_role_replica(settings.replication_role_replica())?;
        }
        Ok(())
    }
    /// Plain exit, for tests only: a task session is refused before the
    /// statement runs (GD-0061 R3-310). Server sources never call it
    /// (`clippy.toml`, `tests/release_seal.rs`, GD-0061 R3-314); they use
    /// `execute_prepared_unreleased` and `release_page`, or `explain_facts`.
    /// It stays public for the integration tests.
    pub fn execute_prepared(
        &mut self,
        p: &Prepared,
        params: &[Value],
    ) -> Result<SqlResult, CoreError> {
        self.run_prepared(p, params, true)?.untasked()
    }
    /// The plan facts of an `EXPLAIN` read the server prepared itself: the
    /// first value of the unreleased result, taken with `untasked`. A task
    /// session is refused before the statement runs, as by the plain exit,
    /// and no core row leaves this function (GD-0061 R3-314).
    pub(crate) fn explain_facts(&mut self, p: &Prepared) -> Result<Option<Value>, CoreError> {
        let result = self.run_prepared(p, &[], true)?.untasked()?;
        Ok(result.rows.into_iter().next().and_then(|row| row.values.into_iter().next()))
    }
    /// `execute_prepared`, with the result held until `release_page`.
    pub fn execute_prepared_unreleased(
        &mut self,
        p: &Prepared,
        params: &[Value],
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.run_prepared(p, params, false)
    }
    /// The body of both prepared exits. `plain` refuses a task session where
    /// the statement would reach core, after the same checks as before.
    fn run_prepared(
        &mut self,
        p: &Prepared,
        params: &[Value],
        plain: bool,
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.authority_scope(|session| {
            let bound = session.sql.bind_relation_context(p)?;
            let p = &bound;
            session.reconcile_sequence_owners()?;
            session.authorize(p)?;
            // Recovery must remain available even after the host clock latches.
            if session.sql.is_aborted() && p.command() == "ROLLBACK" {
                if plain && session.sql.task().is_some() {
                    return Err(task_session_refusal());
                }
                let result = session.sql.execute_prepared_unreleased(p, params, None);
                if !session.sql.in_transaction() {
                    session.transaction_us = None;
                    session.reconcile_sequence_owners()?;
                }
                return result;
            }
            // pgwire can roll core back directly; ignore a pin unless a block is open.
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            }
            let (statement_us, unavailable) = match session.service.clock.sql_micros() {
                Ok(sample) => (sample, None),
                Err(error) => (session.transaction_us.unwrap_or(0), Some(error)),
            };
            let transaction_us = session.transaction_us.unwrap_or(statement_us);
            let service = session.service.clone();
            let clock = StatementClock {
                unavailable,
                transaction_us,
                statement_us,
                zone: session.zone,
                wall: Some(Arc::new(move || service.clock.sql_micros())),
            };
            session.execute_with_ddl_authority(p.command(), p.table(), p.is_ddl(), p.is_sequence_ddl(),
                p.sequence_ddl_names().into_iter().map(str::to_owned).collect(), |session| {
                    if plain && session.sql.task().is_some() {
                        return Err(task_session_refusal());
                    }
                    let result = session.sql.execute_prepared_at_unreleased(p, params, None, &clock);
                    if !session.sql.in_transaction() { session.transaction_us = None; }
                    else if clock.unavailable.is_none() { session.transaction_us = Some(transaction_us); }
                    // DDL bookkeeping reads a plain result only; a task
                    // session runs no DDL (admission refuses the class).
                    result.map(|result| {
                        let affected = result.untasked_ref().is_some_and(|r| r.affected != 0);
                        (result, affected, true)
                    })
                })
        })
    }
    fn execute_with_ddl_authority<T>(
        &mut self, command: &str, table: Option<&str>, ddl: bool, sequence_ddl: bool,
        sequences: Vec<String>, execute: impl FnOnce(&mut Self) -> Result<(T, bool, bool), CoreError>,
    ) -> Result<T, CoreError> {
        if ddl && self.sql.in_transaction() { self.implicit_ddl = true; }
        let transactional = self.sql.set_ddl_owner(match &self.role { Role::Database { name } => Some(name), _ => None });
        let candidates = if ddl && !transactional { self.sql.sequence_owners()?.into_iter().map(|(name, _)| name).collect::<Vec<_>>() } else { vec![] };
        let preexisting = command == "CREATE TABLE" && table.map(|t| self.sql.relation_exists(t)).transpose()?.unwrap_or(false);
        // Native errors stay in the result envelope; failed execution must skip bookkeeping.
        let (result, affected, succeeded) = execute(self)?;
        if transactional || !succeeded { return Ok(result); }
        self.sequence_owner_candidates.extend(candidates.iter().cloned());
        self.sequence_owner_candidates.extend(sequences.iter().cloned());
        let roles = self.service.admin.roles();
        let mut created = if command == "CREATE SEQUENCE" && affected { sequences } else { vec![] };
        if command == "CREATE TABLE" && !preexisting { created.extend(table.map(str::to_owned)); }
        for name in created {
            roles.forget_table(&name).map_err(|_| CoreError::Policy(Error::Scope))?;
            if let Role::Database { name: owner } = &self.role {
                roles.set_table_owner(&name, owner).map_err(|_| CoreError::Policy(Error::Scope))?;
            }
        }
        if ddl && !sequence_ddl {
            for (sequence, table) in self.sql.sequence_owners()? {
                if candidates.contains(&sequence) { continue; }
                roles.forget_table(&sequence).map_err(|_| CoreError::Policy(Error::Scope))?;
                if let Some(table) = table {
                    let durable = self.sql.table_authority_name(&table)?;
                    if let Some(owner) = roles.table_owner(&durable).map_err(|_| CoreError::Policy(Error::Scope))? {
                        roles.set_table_owner(&sequence, &owner).map_err(|_| CoreError::Policy(Error::Scope))?;
                    }
                }
                self.sequence_owner_candidates.insert(sequence);
            }
        }
        self.reconcile_sequence_owners()?;
        Ok(result)
    }
    pub(crate) fn execute_native_sequence_catalog(&mut self, request: &giggles_core::operations::CatalogRequest)
        -> Result<Result<u64, giggles_core::operations::Error>, CoreError> {
        use giggles_core::operations::CatalogRequest as C;
        self.reconcile_sequence_owners()?;
        let (command, name) = match request {
            C::CreateSequence { name } => ("CREATE SEQUENCE", name),
            C::DropSequence { name, .. } => ("DROP SEQUENCE", name),
            _ => return Ok(Err(giggles_core::operations::Error::Unsupported)),
        };
        // SQL passes the statement's sequence names; a qualified native name is schema.name.
        let key = name.split_once('.').map_or_else(|| name.clone(), |(schema, name)| relational::relation_key(schema, name));
        self.execute_with_ddl_authority(command, None, true, true, vec![key], |session| {
            let result = session.sql.execute_sequence_catalog_native(request);
            let succeeded = result.is_ok();
            Ok((result, succeeded, succeeded))
        })
    }
    pub(crate) fn execute_native_catalog(&mut self, request: &giggles_core::operations::CatalogRequest)
        -> Result<Result<giggles_core::operations::CatalogOutcome, giggles_core::operations::Error>, CoreError> {
        use giggles_core::operations::CatalogRequest as C;
        self.reconcile_sequence_owners()?;
        let table = if let C::CreateTable { table, .. } = request {
            let schema = self.sql.native_creation_schema(table)?;
            let name = table.split_once('.').map_or(table.as_str(), |(_, name)| name);
            Some(relational::relation_key(&schema, name))
        } else { None };
        self.execute_with_ddl_authority(if table.is_some() { "CREATE TABLE" } else { "" }, table.as_deref(), true, false, vec![],
            |session| { let result = session.sql.execute_catalog_native(request); let succeeded = result.is_ok(); Ok((result, true, succeeded)) })
    }
    /// Plain exit, for tests only: a task session is refused before the
    /// statement runs (GD-0061 R3-310). Server sources never call it
    /// (`clippy.toml`, `tests/release_seal.rs`, GD-0061 R3-314); they use
    /// `execute_sql_unreleased` and `release_page`, or `run_control`. It
    /// stays public for the integration tests.
    pub fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| session.execute_sql_authorized(sql, params, true))?
            .untasked()
    }
    /// Open a pgwire implicit block, retaining the ordinary authorization scope.
    pub(crate) fn begin_implicit_block(&mut self) -> Result<(), CoreError> {
        self.implicit_block(false)
    }
    pub(crate) fn commit_implicit_block(&mut self) -> Result<(), CoreError> {
        self.implicit_block(true)
    }
    fn implicit_block(&mut self, commit: bool) -> Result<(), CoreError> {
        self.authority_scope(|session| {
            // Preserve task admission, legacy bookkeeping, aborted-state ordering,
            // and DDL/sequence-owner publication through the ordinary SQL path.
            if !session.sql.has_native_transaction_blocks() || session.agent || session.sql.task().is_some()
                || !session.role.is_sql() || session.sql.is_aborted()
                || !session.sequence_owner_candidates.is_empty() || (commit && session.implicit_ddl)
            {
                return session.run_control(if commit { "COMMIT" } else { "BEGIN" });
            }
            #[cfg(test)]
            { session.fast_control_calls += 1; }
            // Retain existing authorization checks using control syntax built without parsing.
            session.authorize(&SqlSession::block_control(commit))?;
            session.sql.set_ddl_owner(match &session.role {
                Role::Database { name } => Some(name), _ => None,
            });
            if !session.sql.in_transaction() { session.transaction_us = None; }
            let (statement_us, unavailable) = match session.service.clock.sql_micros() {
                Ok(sample) => (sample, None),
                Err(error) => (session.transaction_us.unwrap_or(0), Some(error)),
            };
            let transaction_us = session.transaction_us.unwrap_or(statement_us);
            let clock = StatementClock {
                unavailable, transaction_us, statement_us, zone: session.zone, wall: None,
            };
            let result = session.sql.control_block_at(commit, &clock);
            if !session.sql.in_transaction() { session.transaction_us = None; }
            else if clock.unavailable.is_none() { session.transaction_us = Some(transaction_us); }
            session.api_path.sql_committed = result.is_ok() && commit;
            if result.is_ok() {
                if !commit { session.implicit_ddl = false; }
                session.reconcile_sequence_owners()?;
            }
            result
        })
    }
    /// A statement the server issues itself (transaction control, the admin
    /// path's `DROP TABLE`), whose result is never encoded. It runs through
    /// the unreleased twin and is taken with `untasked`: a task session is
    /// refused before the statement runs, as by the plain exit, and the
    /// result is dropped here (GD-0061 R3-314).
    pub(crate) fn run_control(&mut self, sql: &str) -> Result<(), CoreError> {
        #[cfg(test)]
        { self.control_calls += 1; }
        self.authority_scope(|session| session.execute_sql_authorized(sql, &[], true))?
            .untasked()
            .map(drop)
    }
    /// `execute_sql`, with the result held until `release_page`.
    pub fn execute_sql_unreleased(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.authority_scope(|session| session.execute_sql_authorized(sql, params, false))
    }
    fn execute_sql_authorized(
        &mut self,
        sql: &str,
        params: &[Value],
        plain: bool,
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        // Core's aborted-state ROLLBACK escape must remain reachable.
        if self.sql.is_aborted()
            && sql
                .trim()
                .trim_end_matches(';')
                .eq_ignore_ascii_case("rollback")
        {
            if plain && self.sql.task().is_some() {
                return Err(task_session_refusal());
            }
            let result = self.sql.execute_unreleased(sql, params, None);
            if !self.sql.in_transaction() {
                self.transaction_us = None;
                self.reconcile_sequence_owners()?;
            }
            return result;
        }
        let p = self.prepare(sql)?;
        let result = self.run_prepared(&p, params, plain);
        self.api_path.sql_committed = result.is_ok() && p.command() == "COMMIT";
        result
    }
    fn executor(&self) -> Result<(Executor, (u64, u64)), CoreError> {
        self.check_binding()?;
        let Role::Task {
            domain,
            task,
            principal,
        } = &self.role
        else {
            return Err(CoreError::Policy(Error::Scope));
        };
        if self.service.admin.authority_metadata()?.0.to_string() != *domain {
            return Err(CoreError::Policy(Error::Authentication));
        }
        let now = self.service.clock.now(&self.service.admin)?;
        let e = self.service.admin.executor(*task, *principal)?;
        Ok((e, now))
    }
    pub fn handle(&mut self, request: Request) -> Response {
        self.handle_received(request, std::time::Instant::now())
    }
    /// All task transports use the same immutable-response reservation gate.
    pub(crate) fn handle_prepared_task(
        &mut self,
        request: Request,
        received: std::time::Instant,
    ) -> Response {
        self.handle_received(request, received)
    }
    pub fn handle_received(&mut self, request: Request, received: std::time::Instant) -> Response {
        self.authority_scope(|session| Ok(session.handle_authorized(request, received)))
            .unwrap_or_else(|error| Response::error(&error))
    }
    fn handle_authorized(&mut self, request: Request, received: std::time::Instant) -> Response {
        if self.service.check_statement_gate().is_err() {
            return Response::code("57P01");
        }
        let context = Cancellation::current().unwrap_or_default();
        context.scope(|| {
            let response = self.handle_scoped(request, received, &context);
            if !matches!(self.role, Role::Task { .. }) {
                return response;
            }
            // Freeze exactly the Response later encoded by both transports. A
            // 1024-byte allowance covers their bounded decision framing/control.
            let reserve = (|| {
                let size = serde_json::to_vec(&response)
                    .map_err(|_| CoreError::Limit)?
                    .len();
                let bytes = u64::try_from(size)
                    .ok()
                    .and_then(|n| n.checked_add(1024))
                    .ok_or(CoreError::Limit)?;
                let _gate = giggles_core::cancellation::lock(&self.service.task_gate)?;
                let (executor, time) = self.executor()?;
                executor.reserve_disclosure(bytes, time.0, time.1)
            })();
            if let Err(error) = reserve {
                // Fixed envelope: never echo operation, counts, digests, original
                // denial or remaining allowance when release is unauthorized.
                return Response::code(match error {
                    CoreError::Cancelled => "57014",
                    CoreError::Policy(Error::UncertainTime | Error::TimeRegression) => "55000",
                    _ => "42501",
                });
            }
            response
        })
    }
    fn handle_scoped(
        &mut self,
        request: Request,
        received: std::time::Instant,
        context: &Cancellation,
    ) -> Response {
        // GD-0061 R3-210 backstop (design-2 section 4, call site 5): on a task
        // session the framed surface admits only `Ping`. A `Sql` request goes
        // on to `authorize`, which admits its statement's own class (R3-208);
        // every other request is `Other`, refused before it is unwrapped.
        if self.sql.task().is_some() {
            let class = match &request {
                Request::Ping => Some(giggles_core::task::StatementClass::Show),
                Request::Sql { .. } => None,
                Request::Deadline { .. }
                | Request::Preview { .. }
                | Request::Execute { .. }
                | Request::RepairSql { .. }
                | Request::Status { .. } => Some(giggles_core::task::StatementClass::Other),
            };
            if let Some(class) = class
                && let Err(error) = self.admit_backstop(class)
            {
                self.abort();
                return Response::error(&error);
            }
        }
        let (request, budget) = match request {
            Request::Deadline { budget_ms, request } => {
                if budget_ms > 10_000 || matches!(*request, Request::Deadline { .. }) {
                    self.abort();
                    return Response::code("54000");
                }
                (*request, Some(std::time::Duration::from_millis(budget_ms)))
            }
            request => (request, None),
        };
        if let Some(budget) = budget {
            context.deadline(received + budget);
        }
        let operation = match &request {
            Request::Execute { request, .. } => Some(request.operation),
            Request::RepairSql { operation, .. } | Request::Status { operation } => {
                Some(*operation)
            }
            _ => None,
        };
        // Keep time issuance in decision order across both transports. Core
        // still revalidates revoke/expiry atomically; this gate adds no authority.
        let service = self.service.clone();
        let _gate = if matches!(self.role, Role::Task { .. }) {
            match giggles_core::cancellation::lock(&service.task_gate) {
                Ok(g) => Some(g),
                Err(e) => {
                    return Response {
                        operation,
                        ..Response::error(&e)
                    };
                }
            }
        } else {
            None
        };
        if context.check().is_err() {
            self.abort();
            return Response {
                operation,
                ..Response::code("57014")
            };
        }
        let result = (|| -> Result<Response, CoreError> {
            match request {
                Request::Deadline { .. } => unreachable!(),
                Request::Ping => Ok(Response::code("ok")),
                // Certificate-bound SQL roles are provisioned for future native
                // privilege enforcement; this does not expose the SQL interface.
                // An agent session's SQL reaches the task admission backstop in
                // `authorize` instead (GD-0061 R3-208).
                Request::Sql { .. }
                    if self.fingerprint_bound && self.role_fence.is_some() && !self.agent =>
                {
                    Err(CoreError::Policy(Error::Scope))
                }
                // GD-0061 R3-313: the result is released before the Response
                // that both transports encode is built.
                Request::Sql { sql, params } => {
                    let result = self.execute_api_sql(&sql, &params)?;
                    Ok(Response::released_sql(self.release_page(result)?))
                }
                Request::Preview { request } => {
                    let (e, time) = self.executor()?;
                    let p = e.preview_current(&request, time.0, time.1)?;
                    Ok(Response {
                        operation: Some(request.operation),
                        count: Some(p.effects.len()),
                        digest: Some(digest(&p.effects)),
                        ..Response::code("preview")
                    })
                }
                Request::Execute { request, approval } => {
                    let (e, time) = self.executor()?;
                    let op = request.operation;
                    Ok(Response::decision(
                        op,
                        e.execute_current(request, time.0, time.1, approval)?,
                    ))
                }
                Request::RepairSql {
                    operation,
                    sql,
                    approval,
                } => {
                    let (e, time) = self.executor()?;
                    let r = parse_repair(operation, &sql)?;
                    Ok(Response::decision(
                        operation,
                        e.execute_current(r, time.0, time.1, approval)?,
                    ))
                }
                Request::Status { operation } => {
                    let (e, time) = self.executor()?;
                    Ok(match e.status_current(operation, time.0, time.1)? {
                        OperationStatus::Absent => Response {
                            operation: Some(operation),
                            ..Response::code("absent")
                        },
                        OperationStatus::Decided(d) => Response::decision(operation, d),
                    })
                }
            }
        })();
        result.unwrap_or_else(|e| Response {
            operation,
            ..Response::error(&e)
        })
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    /// GD-0075 P11-5: a role recheck that fails transiently (a fenced gateway's 40001, an
    /// unavailable range's 08007) keeps its own SQLSTATE; other failures stay 28000.
    #[test]
    fn transient_role_lookup_failures_are_not_authentication_failures() {
        use giggles_core::roles::RoleError;
        let code = |error: RoleError| sqlstate(&role_lookup_error(error));
        let conflict = CoreError::Sql(relational::SqlError::Conflict);
        assert_eq!(code(RoleError::Core(conflict)), "40001");
        assert_eq!(code(RoleError::Core(CoreError::Deadlock)), "40P01");
        assert_eq!(code(RoleError::Core(CoreError::Storage("range".into()))), "08007");
        assert_eq!(code(RoleError::Core(CoreError::Fenced)), "28000");
        assert_eq!(code(RoleError::Missing), "28000");
        assert_eq!(code(RoleError::Invalid), "28000");
        assert!(transient(&CoreError::Busy));
        assert!(!transient(&CoreError::Policy(Error::Authentication)));
    }
    use std::time::{SystemTime, UNIX_EPOCH};
    fn fixture() -> (Arc<Service>, Vec<Binding>) {
        let path = crate::test_support::fixture_root().join(format!(
            "binding-unit-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        println!("retained_fixture={}", path.display());
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let bindings = vec![Binding {
            fingerprint: "11".repeat(32),
            role: Role::Application {
                domain: admin.authority_metadata().unwrap().0.to_string(),
                tables: BTreeSet::from(["app".into()]),
            },
        }];
        (
            Service::new(admin, bindings.clone(), Clock::new(0, 3600).unwrap()).unwrap(),
            bindings,
        )
    }
    #[test]
    fn table_requirements_match_sql_roles_and_live_grants() {
        use roles::TablePrivilege::{Delete, Insert, Select, Update};
        let (service, _) = fixture();
        let roles = service.admin.roles();
        for (name, superuser) in [("owner", false), ("root", true), ("reader", false)] {
            roles.create(roles::NewRole {
                name: name.into(), login: true, superuser,
                ..roles::NewRole::default()
            }).unwrap();
        }
        roles.set_table_owner("app", "owner").unwrap();
        roles.grant_table("app", "reader", &[Select]).unwrap();
        for name in ["owner", "root", "reader"] {
            let mut policy = service.role_session(name).unwrap();
            let mut sql = service.role_session(name).unwrap();
            for (privilege, statement) in [
                (Select, "SELECT id FROM app"),
                (Select, "EXPLAIN SELECT id FROM app"),
                (Insert, "INSERT INTO app VALUES (1)"),
                (Update, "UPDATE app SET id = 2"),
                (Delete, "DELETE FROM app"),
                (Delete, "TRUNCATE TABLE app"),
            ] {
                let expected = if name != "reader" || privilege == Select { Ok(()) }
                    else { Err(CoreError::Policy(Error::Scope)) };
                assert_eq!(policy.authorize_table_requirements(&[("app".into(), privilege)]), expected);
                assert_eq!(sql.prepare(statement).map(|_| ()), expected, "{name}: {statement}");
            }
        }
        let mut owner = service.role_session("owner").unwrap();
        owner.execute_sql("BEGIN", &[]).unwrap();
        owner.execute_sql("ALTER TABLE app RENAME TO renamed_app", &[]).unwrap();
        assert_eq!(
            owner.authorize_table_requirements(&[("renamed_app".into(), Insert)]),
            Ok(())
        );
        assert!(owner.prepare("INSERT INTO renamed_app VALUES (1)").is_ok());
        owner.execute_sql("ROLLBACK", &[]).unwrap();
        let mut policy = service.role_session("reader").unwrap();
        let mut sql = service.role_session("reader").unwrap();
        let reqs = [("app".into(), Insert)];
        let statement = "INSERT INTO app VALUES (1)";
        for grant in [false, true, false] {
            if grant { roles.grant_table("app", "reader", &[Insert]).unwrap(); }
            else { roles.revoke_table("app", "reader", &[Insert]).unwrap(); }
            let expected = if grant { Ok(()) } else { Err(CoreError::Policy(Error::Scope)) };
            assert_eq!(policy.authorize_table_requirements(&reqs), expected);
            assert_eq!(sql.prepare(statement).map(|_| ()), expected);
        }
        policy.execute_sql("BEGIN", &[]).unwrap();
        sql.execute_sql("BEGIN", &[]).unwrap();
        assert_eq!(policy.authorize_table_requirements(&reqs), sql.prepare(statement).map(|_| ()));
        assert!(policy.is_aborted() && sql.is_aborted());
        policy.execute_sql("ROLLBACK", &[]).unwrap();
        sql.execute_sql("ROLLBACK", &[]).unwrap();
    }
    #[test]
    fn table_requirements_preserve_sql_missing_table_and_child_read_precedence() {
        use roles::TablePrivilege::{Insert, Select};
        let (service, bindings) = fixture();
        let roles = service.admin.roles();
        for (name, superuser) in [("reader", false), ("root", true)] {
            roles.create(roles::NewRole {
                name: name.into(), login: true, superuser,
                ..roles::NewRole::default()
            }).unwrap();
        }
        let mut policy = service.role_session("reader").unwrap();
        let mut sql = service.role_session("reader").unwrap();
        for table in ["app", "missing"] {
            let reqs = [(table.into(), Select)];
            let statement = format!("SELECT id FROM {table}");
            let denied = Err(CoreError::Policy(Error::Scope));
            assert_eq!(policy.authorize_table_requirements(&reqs), denied);
            assert_eq!(sql.prepare(&statement).map(|_| ()), denied);
            assert_eq!(sql.execute_sql(&statement, &[]).unwrap_err(), CoreError::Policy(Error::Scope));
        }
        roles.grant_table("missing", "reader", &[Select]).unwrap();
        // Qualified public names retain this authority key even when absent;
        // unqualified absent names use SQL's deliberately unresolvable key.
        for name in ["reader", "root"] {
            let mut policy = service.role_session(name).unwrap();
            let mut sql = service.role_session(name).unwrap();
            assert_eq!(policy.authorize_table_requirements(&[("missing".into(), Select)]), Ok(()));
            assert!(sql.prepare("SELECT id FROM public.missing").is_ok());
            assert_eq!(sql.execute_sql("SELECT id FROM public.missing", &[]).unwrap_err(),
                CoreError::Sql(relational::SqlError::UnknownTable));
        }
        roles.grant_table("app", "reader", &[Insert]).unwrap();
        let statement = "INSERT INTO app SELECT id FROM app";
        let reqs = [("app".into(), Insert), ("app".into(), Select)];
        assert_eq!(policy.authorize_table_requirements(&reqs), sql.prepare(statement).map(|_| ()));
        assert_eq!(policy.authorize_table_requirements(&reqs), Err(CoreError::Policy(Error::Scope)));
        roles.grant_table("app", "reader", &[Select]).unwrap();
        assert_eq!(policy.authorize_table_requirements(&reqs), Ok(()));
        assert!(sql.prepare(statement).is_ok());
        let mut app = service.session(&bindings[0].fingerprint).unwrap();
        for table in ["app", "missing"] {
            assert_eq!(app.authorize_table_requirements(&[(table.into(), Select)]),
                app.prepare(&format!("SELECT id FROM {table}")).map(|_| ()));
        }
    }
    #[test]
    fn copy_authority_and_atomic_insert_use_existing_sql_policy() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        let (_, prepared) = session.prepare_copy("COPY app(id) FROM STDIN").unwrap();
        let result = session
            .ingest_copy(&prepared, vec![vec![Value::Int(1)], vec![Value::Int(2)]])
            .unwrap();
        assert_eq!(result.affected, 2);
        let error = session
            .ingest_copy(&prepared, vec![vec![Value::Int(3)], vec![Value::Int(1)]])
            .unwrap_err();
        assert_eq!(sqlstate(&error), "23505");
        assert!(
            session
                .execute_sql("SELECT id FROM app WHERE id = 3", &[])
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(
            sqlstate(
                &session
                    .prepare_copy("COPY secret_missing FROM STDIN")
                    .unwrap_err()
            ),
            "42501"
        );
        assert_eq!(
            sqlstate(
                &session
                    .prepare_copy("COPY secret_missing FROM STDIN WITH (FORMAT")
                    .unwrap_err()
            ),
            "42501"
        );
        assert_eq!(
            sqlstate(&CoreError::Sql(relational::SqlError::BadCopyFileFormat)),
            "22P04"
        );
    }
    #[test]
    fn application_copy_denies_target_errors_before_classification() {
        let (service, bindings) = fixture();
        let mut app = service.session(&bindings[0].fingerprint).unwrap();
        for sql in [
            "COPY (",
            "COPY \"unterminated",
            "COPY secret_schema.t FROM STDIN",
            "COPY \u{96ea} FROM STDIN",
            "COPY secret FROM STDIN WITH (FORMAT",
        ] {
            assert_eq!(
                sqlstate(&app.prepare_copy(sql).unwrap_err()),
                "42501",
                "{sql}"
            );
            assert_eq!(sqlstate(&app.prepare(sql).unwrap_err()), "42501", "{sql}");
            assert_eq!(
                sqlstate(&app.execute_sql(sql, &[]).unwrap_err()),
                "42501",
                "{sql}"
            );
            assert_eq!(
                app.handle(Request::Sql {
                    sql: sql.into(),
                    params: vec![]
                })
                .code,
                "42501",
                "{sql}"
            );
        }
        assert_eq!(
            sqlstate(&app.prepare("COPY app FROM STDIN").unwrap_err()),
            "0A000"
        );
    }
    #[test]
    fn database_copy_denies_target_errors_without_insert_authority() {
        let (service, _) = fixture();
        service
            .admin
            .roles()
            .create(roles::NewRole {
                name: "copy_reader".into(),
                login: true,
                superuser: false,
                ..roles::NewRole::default()
            })
            .unwrap();
        let mut reader = service.role_session("copy_reader").unwrap();
        for sql in [
            "COPY (",
            "COPY \"unterminated",
            "COPY secret_schema.t FROM STDIN",
            "COPY \u{96ea} FROM STDIN",
            "COPY app FROM STDIN WITH (FORMAT",
        ] {
            assert_eq!(
                sqlstate(&reader.prepare_copy(sql).unwrap_err()),
                "42501",
                "{sql}"
            );
            assert_eq!(
                sqlstate(&reader.prepare(sql).unwrap_err()),
                "42501",
                "{sql}"
            );
        }
    }
    #[test]
    fn native_task_copy_denies_before_malformed_statement_or_data() {
        let (service, mut bindings) = fixture();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        service
            .admin
            .grant(Grant {
                task: TaskId(91),
                issuer: Principal(90),
                executor: Principal(92),
                approver: Principal(93),
                policy: Policy {
                    tenant: TenantId(42),
                    columns: BTreeSet::from([Column::Status]),
                    transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                    incident: (100, 107),
                    distinct_limit: 20,
                    transition_limit: 20,
                    approval_above: 10,
                    valid_from: 1,
                    expires: now + 7200,
                },
            })
            .unwrap();
        service
            .admin
            .provision_task_resources(
                TaskId(91),
                TaskResources {
                    read_rows: 1000,
                    disclosed_rows: 1000,
                    encoded_bytes: 1_000_000,
                },
            )
            .unwrap();
        service.admin.executor(TaskId(91), Principal(92)).unwrap();
        bindings[0].role = Role::Task {
            domain: service.admin.authority_metadata().unwrap().0.to_string(),
            task: TaskId(91),
            principal: Principal(92),
        };
        service.reload_bindings(bindings.clone()).unwrap();
        let mut task = service.session(&bindings[0].fingerprint).unwrap();
        for sql in [
            "COPY app FROM STDIN",
            "COPY missing FROM STDIN WITH (FORMAT",
            "COPY (",
            "COPY \"unterminated",
            "COPY secret_schema.t FROM STDIN",
            "COPY app FROM STDIN; not-an-int",
        ] {
            assert_eq!(sqlstate(&task.execute_sql(sql, &[]).unwrap_err()), "42501");
            assert_eq!(
                task.handle(Request::Sql {
                    sql: sql.into(),
                    params: vec![]
                })
                .code,
                "42501"
            );
        }
        assert!(
            service
                .admin
                .task_resources(TaskId(91))
                .unwrap()
                .unwrap()
                .used
                .encoded_bytes
                > 0
        );
    }
    fn sql_instant(session: &mut Session, sql: &str) -> i64 {
        match session.execute_sql(sql, &[]).unwrap().rows[0].values[0] {
            Value::TimestampTz(value) => value.microseconds(),
            ref value => panic!("expected timestamptz, got {value:?}"),
        }
    }
    #[test]
    fn sql_time_tracks_host_and_pins_transaction_start() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        let first = sql_instant(&mut session, "SELECT now()");
        let host = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros(),
        )
        .unwrap()
            - 946_684_800_000_000;
        assert!(first.abs_diff(host) < 2_000_000);
        session.execute_sql("BEGIN", &[]).unwrap();
        let now = sql_instant(&mut session, "SELECT now()");
        let statement = sql_instant(&mut session, "SELECT statement_timestamp()");
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(sql_instant(&mut session, "SELECT now()"), now);
        assert!(sql_instant(&mut session, "SELECT statement_timestamp()") > statement);
        session.execute_sql("COMMIT", &[]).unwrap();
        assert!(sql_instant(&mut session, "SELECT now()") > now);
        session.execute_sql("BEGIN", &[]).unwrap();
        let stale = sql_instant(&mut session, "SELECT now()");
        session.sql.rollback();
        std::thread::sleep(std::time::Duration::from_millis(10));
        session.execute_sql("BEGIN", &[]).unwrap();
        assert!(sql_instant(&mut session, "SELECT now()") > stale);
        session.execute_sql("ROLLBACK", &[]).unwrap();
    }
    #[test]
    fn local_sql_time_uses_session_zone_and_unresolved_zones_stay_partial() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        session.set_time_zone("America/Chicago");
        let result = session
            .execute_sql("SELECT now(), CURRENT_DATE, LOCALTIMESTAMP", &[])
            .unwrap();
        let Value::TimestampTz(instant) = result.rows[0].values[0] else {
            panic!("expected instant")
        };
        let local = SessionZone::named("America/Chicago")
            .unwrap()
            .to_local(instant)
            .unwrap();
        assert_eq!(result.rows[0].values[2], Value::Timestamp(local));
        assert_eq!(
            result.rows[0].values[1],
            Value::Date(
                giggles_pg_types::temporal::Date::from_days(
                    i32::try_from(local.microseconds().div_euclid(86_400_000_000)).unwrap()
                )
                .unwrap()
            )
        );
        session.set_time_zone("unresolved/timezone");
        assert_eq!(
            sqlstate(&session.execute_sql("SELECT CURRENT_DATE", &[]).unwrap_err()),
            "0A000"
        );
        sql_instant(&mut session, "SELECT now()");
        session.set_time_zone("+03:00");
        assert!(matches!(session.zone, Some(SessionZone::Fixed(-10800))));
        for name in ["UTC", "etc/utc", "GMT"] {
            session.set_time_zone(name);
            assert_eq!(session.zone.unwrap().to_local(instant).unwrap().microseconds(), instant.microseconds());
        }
    }
    #[test]
    fn uncertain_sql_clock_only_denies_time_functions_and_rollback_escapes() {
        let (service, _) = fixture();
        service
            .admin
            .roles()
            .create(roles::NewRole {
                name: "owner".into(),
                login: true,
                superuser: true,
                ..roles::NewRole::default()
            })
            .unwrap();
        let mut session = service.role_session("owner").unwrap();
        service.clock.latch_uncertain();
        // GigglesDB-specific: PostgreSQL now() does not fail on host-clock uncertainty.
        assert_eq!(
            sqlstate(&session.execute_sql("SELECT now()", &[]).unwrap_err()),
            "55000"
        );
        assert!(!session.in_transaction());
        assert_eq!(
            session.execute_sql("SELECT 1", &[]).unwrap().rows[0].values,
            vec![Value::Int(1)]
        );
        session
            .execute_sql("CREATE TABLE uncertain_clock (id BIGINT PRIMARY KEY)", &[])
            .unwrap();
        session
            .execute_sql("INSERT INTO uncertain_clock VALUES (7)", &[])
            .unwrap();
        assert_eq!(
            session
                .execute_sql("SELECT id FROM uncertain_clock", &[])
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(7)]
        );
        session.execute_sql("BEGIN", &[]).unwrap();
        assert!(session.transaction_us.is_none());
        session.execute_sql("SELECT 1", &[]).unwrap();
        assert!(session.transaction_us.is_none());
        assert_eq!(
            sqlstate(&session.execute_sql("SELECT now()", &[]).unwrap_err()),
            "55000"
        );
        assert!(session.is_aborted());
        session.execute_sql("ROLLBACK", &[]).unwrap();
        assert!(!session.in_transaction());
        assert!(session.transaction_us.is_none());
        session.execute_sql("SELECT 1", &[]).unwrap();
        assert_eq!(
            sqlstate(&session.execute_sql("SELECT now()", &[]).unwrap_err()),
            "55000"
        );
    }
    #[test]
    fn prepared_rollback_recovers_after_clock_failure_without_manual_abort() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        session.execute_sql("BEGIN", &[]).unwrap();
        let rollback = session.prepare("ROLLBACK").unwrap();
        let query = session.prepare("SELECT now()").unwrap();
        service.clock.latch_uncertain();
        assert_eq!(
            sqlstate(&session.execute_prepared(&query, &[]).unwrap_err()),
            "55000"
        );
        assert!(session.is_aborted());
        assert_eq!(
            sqlstate(
                &session
                    .execute_prepared(&rollback, &[Value::Null])
                    .unwrap_err()
            ),
            "42804"
        );
        assert!(session.in_transaction());
        session.execute_prepared(&rollback, &[]).unwrap();
        assert!(!session.in_transaction());
        assert!(!session.is_aborted());
        assert!(session.transaction_us.is_none());
    }
    fn database_binding_fixture() -> (Arc<Service>, Vec<Binding>) {
        let (service, mut bindings) = fixture();
        service.admin.roles().create(roles::NewRole {
            name: "bound".into(), login: true, superuser: true,
            ..roles::NewRole::default()
        }).unwrap();
        // Make the binding counter deliberately independent of the role counter.
        service.reload_bindings(vec![]).unwrap();
        bindings[0].role = Role::Database { name: "bound".into() };
        service.reload_bindings(bindings.clone()).unwrap();
        (service, bindings)
    }
    #[test]
    fn database_fingerprint_binding_requires_existing_login_role() {
        let (service, mut bindings) = database_binding_fixture();
        let encoded = serde_json::to_string(&bindings[0]).unwrap();
        assert_eq!(serde_json::from_str::<Binding>(&encoded).unwrap(), bindings[0]);
        Service::validate(&service.admin, bindings.clone()).unwrap();
        let session = service.session(&bindings[0].fingerprint).unwrap();
        assert_ne!(session.epoch, session.role_epoch());
        session.check_binding().unwrap();
        service.admin.roles().create(roles::NewRole {
            name: "no_login".into(), ..roles::NewRole::default()
        }).unwrap();
        for name in ["missing", "no_login"] {
            bindings[0].role = Role::Database { name: name.into() };
            assert!(matches!(Service::validate(&service.admin, bindings.clone()),
                Err(CoreError::Policy(Error::Authentication))));
            assert!(matches!(service.reload_bindings(bindings.clone()),
                Err(CoreError::Policy(Error::Authentication))));
            session.check_binding().unwrap();
        }
    }
    #[test]
    fn database_fingerprint_binding_fences_role_lifetime_and_login() {
        for change in ["drop", "recreate", "nologin", "login_again", "alter"] {
            let (service, bindings) = database_binding_fixture();
            let session = service.session(&bindings[0].fingerprint).unwrap();
            let pgwire = service.role_session("bound").unwrap();
            let roles = service.admin.roles();
            match change {
                "drop" | "recreate" => {
                    roles.drop("bound").unwrap();
                    assert!(matches!(service.session(&bindings[0].fingerprint),
                        Err(CoreError::Policy(Error::Authentication))));
                    if change == "recreate" {
                        roles.create(roles::NewRole {
                            name: "bound".into(), login: true,
                            ..roles::NewRole::default()
                        }).unwrap();
                    }
                }
                "nologin" | "login_again" => {
                    roles.alter("bound", roles::RoleChanges {
                        login: Some(false), ..roles::RoleChanges::default()
                    }).unwrap();
                    assert!(matches!(service.session(&bindings[0].fingerprint),
                        Err(CoreError::Policy(Error::Authentication))));
                    if change == "login_again" {
                        roles.alter("bound", roles::RoleChanges {
                            login: Some(true), ..roles::RoleChanges::default()
                        }).unwrap();
                    }
                }
                _ => { roles.alter("bound", roles::RoleChanges {
                    superuser: Some(false), ..roles::RoleChanges::default()
                }).unwrap(); }
            }
            assert!(matches!(session.check_binding(),
                Err(CoreError::Policy(Error::Authentication))), "{change}");
            assert!(matches!(pgwire.check_binding(),
                Err(CoreError::Policy(Error::Authentication))), "{change}");
        }
    }
    #[test]
    fn database_fingerprint_binding_fences_reload_replace_and_readdition() {
        let (service, bindings) = database_binding_fixture();
        let old = service.session(&bindings[0].fingerprint).unwrap();
        let pgwire = service.role_session("bound").unwrap();
        service.reload_bindings(bindings.clone()).unwrap();
        old.check_binding().unwrap(); // An unchanged reload preserves the binding.
        service.admin.roles().create(roles::NewRole {
            name: "other".into(), login: true, ..roles::NewRole::default()
        }).unwrap();
        let mut replaced = bindings.clone();
        replaced[0].role = Role::Database { name: "other".into() };
        service.reload_bindings(replaced).unwrap();
        assert!(matches!(old.check_binding(), Err(CoreError::Policy(Error::Authentication))));
        service.reload_bindings(bindings.clone()).unwrap();
        assert!(matches!(old.check_binding(), Err(CoreError::Policy(Error::Authentication))));
        let fresh = service.session(&bindings[0].fingerprint).unwrap();
        fresh.check_binding().unwrap();
        service.reload_bindings(vec![]).unwrap();
        assert!(matches!(fresh.check_binding(), Err(CoreError::Policy(Error::Authentication))));
        service.reload_bindings(bindings).unwrap();
        assert!(matches!(fresh.check_binding(), Err(CoreError::Policy(Error::Authentication))));
        pgwire.check_binding().unwrap();
    }
    #[test]
    fn database_fingerprint_binding_preserves_password_aliases_without_exposing_sql() {
        let (service, bindings) = database_binding_fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        let pgwire = service.role_session("bound").unwrap();
        let identity = crate::api::ConnectionIdentity::verified(&service, &bindings[0].fingerprint).unwrap();
        for _ in 0..2 {
            let stmt = crate::auth::statements::parse("ALTER ROLE bound PASSWORD NULL").unwrap();
            session.execute_admin(&stmt).unwrap();
            session.check_binding().unwrap();
            pgwire.check_binding().unwrap();
            identity.check(&service).unwrap();
            assert_eq!(identity, crate::api::ConnectionIdentity::verified(
                &service, &bindings[0].fingerprint).unwrap());
        }
        assert_eq!(session.handle(Request::Sql {
            sql: "SELECT 1".into(), params: vec![]
        }).code, "42501");
        service.admin.roles().drop("bound").unwrap();
        service.admin.roles().create(roles::NewRole {
            name: "bound".into(), login: true, ..roles::NewRole::default()
        }).unwrap();
        assert!(identity.check(&service).is_err());
        assert!(matches!(pgwire.check_binding(),
            Err(CoreError::Policy(Error::Authentication))));
    }
    #[test]
    fn drop_index_requires_table_ownership_even_with_schema_create() {
        let (service, _) = fixture();
        let roles = service.admin.roles();
        for (name, superuser) in [("owner", true), ("reader", false)] {
            roles
                .create(roles::NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..roles::NewRole::default()
                })
                .unwrap();
        }
        let mut owner = service.role_session("owner").unwrap();
        owner
            .execute_sql("CREATE TABLE owned(id bigint PRIMARY KEY,v bigint)", &[])
            .unwrap();
        owner
            .execute_sql("CREATE INDEX iv ON owned(v)", &[])
            .unwrap();
        roles
            .grant_schema("public", "reader", &[roles::SchemaPrivilege::Create])
            .unwrap();
        let mut reader = service.role_session("reader").unwrap();
        assert_eq!(
            sqlstate(&reader.execute_sql("DROP INDEX iv", &[]).unwrap_err()),
            "42501"
        );
        assert_eq!(service.admin.secondary_index_catalog().unwrap().1.len(), 1);
        roles.set_table_owner("owned", "reader").unwrap();
        let mut reader = service.role_session("reader").unwrap();
        reader.execute_sql("DROP INDEX iv", &[]).unwrap();
        assert!(
            service
                .admin
                .secondary_index_catalog()
                .unwrap()
                .1
                .is_empty()
        );
    }
    #[test]
    fn database_catalog_snapshot_filters_owners_and_grants_and_rechecks_role() {
        let (service, _) = fixture();
        let roles = service.admin.roles();
        for (name, superuser) in [("owner", true), ("reader", false)] {
            roles
                .create(roles::NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..roles::NewRole::default()
                })
                .unwrap();
        }
        let mut owner = service.role_session("owner").unwrap();
        owner
            .execute_sql("CREATE TABLE owned (id BIGINT PRIMARY KEY)", &[])
            .unwrap();
        owner
            .execute_sql("CREATE TABLE hidden (id BIGINT PRIMARY KEY)", &[])
            .unwrap();
        assert_eq!(
            roles.table_owner("owned").unwrap().as_deref(),
            Some("owner")
        );
        roles.set_table_owner("owned", "reader").unwrap();
        roles
            .grant_table("app", "reader", &[roles::TablePrivilege::Select])
            .unwrap();
        roles
            .grant_table("missing", "reader", &[roles::TablePrivilege::Select])
            .unwrap();
        let catalog = service.admin.catalog().unwrap();
        let snapshot = owner.catalog_snapshot().unwrap();
        assert_eq!((snapshot.0, snapshot.1), catalog);
        let mut reader = service.role_session("reader").unwrap();
        assert_eq!(reader.fingerprint, "pgrole:reader");
        let visible = reader.catalog_snapshot().unwrap().1;
        assert_eq!(visible.len(), 2);
        assert!(
            visible
                .iter()
                .all(|table| table.name == "app" || table.name == "owned")
        );
        assert_eq!(reader.handle(Request::Ping).code, "ok");
        assert_eq!(
            reader
                .handle(Request::Status {
                    operation: OperationId(1)
                })
                .code,
            "42501"
        );
        assert!(reader.execute_sql("BEGIN", &[]).is_ok());
        assert!(reader.execute_sql("ROLLBACK", &[]).is_ok());
        assert!(reader.prepare("SELECT id FROM owned LIMIT 1").is_ok());
        roles
            .alter(
                "reader",
                roles::RoleChanges {
                    login: Some(false),
                    ..roles::RoleChanges::default()
                },
            )
            .unwrap();
        assert!(matches!(
            reader.catalog_snapshot(),
            Err(CoreError::Policy(Error::Authentication))
        ));
        assert!(matches!(
            service.role_session("reader"),
            Err(CoreError::Policy(Error::Authentication))
        ));
        assert!(matches!(
            service.role_session("missing"),
            Err(CoreError::Policy(Error::Authentication))
        ));
    }
    #[test]
    fn database_catalog_snapshot_accepts_heap_created_by_sql_session() {
        let (service, _) = fixture();
        service
            .admin
            .roles()
            .create(roles::NewRole {
                name: "owner".into(),
                login: true,
                superuser: true,
                ..roles::NewRole::default()
            })
            .unwrap();
        let mut owner = service.role_session("owner").unwrap();
        owner
            .execute_sql("CREATE TABLE heap (body TEXT)", &[])
            .unwrap();
        let catalog = service.admin.catalog().unwrap();
        assert!(catalog.1.iter().any(|table| table.hidden_rowid().is_some()));
        // K2c snapshots also carry index metadata; compare the epoch and tables.
        let tables = |(epoch, tables, _indexes)| (epoch, tables);
        assert_eq!(tables(owner.catalog_snapshot().unwrap()), catalog);
        owner.execute_sql("BEGIN", &[]).unwrap();
        assert_eq!(tables(owner.catalog_snapshot().unwrap()), catalog);
        owner.execute_sql("ROLLBACK", &[]).unwrap();
    }
    #[test]
    fn database_catalog_snapshot_checks_transaction_schema_and_aborted_state() {
        let (service, _) = fixture();
        service
            .admin
            .roles()
            .create(roles::NewRole {
                name: "owner".into(),
                login: true,
                superuser: true,
                ..roles::NewRole::default()
            })
            .unwrap();
        let mut owner = service.role_session("owner").unwrap();
        owner.execute_sql("BEGIN", &[]).unwrap();
        service
            .admin
            .sql_session()
            .execute("CREATE TABLE future (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert!(matches!(
            owner.catalog_snapshot(),
            Err(CoreError::Sql(relational::SqlError::UnknownTable))
        ));
        owner.sql.rollback();
        assert_eq!(owner.catalog_snapshot().unwrap().1.len(), 2);
        owner.execute_sql("BEGIN", &[]).unwrap();
        owner.abort();
        assert!(matches!(
            owner.catalog_snapshot(),
            Err(CoreError::Sql(relational::SqlError::Aborted))
        ));
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn catalog_snapshot_excludes_own_drop_and_restores_it_at_savepoint() {
        let path = crate::test_support::fixture_root().join(format!("catalog-own-drop-{}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
        admin.roles().create(roles::NewRole { name: "owner".into(), login: true, superuser: true, ..roles::NewRole::default() }).unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let mut session = service.role_session("owner").unwrap();
        session.execute_sql("CREATE TABLE gone (id INT PRIMARY KEY)", &[]).unwrap();
        session.execute_sql("CREATE INDEX gone_idx ON gone(id)", &[]).unwrap();
        for finish in ["ROLLBACK", "COMMIT"] {
            session.execute_sql("BEGIN", &[]).unwrap();
            session.execute_sql("SAVEPOINT keep", &[]).unwrap();
            session.execute_sql("DROP TABLE gone", &[]).unwrap();
            let (_, tables, indexes) = session.catalog_snapshot().unwrap();
            assert!(tables.is_empty() && indexes.is_empty());
            session.execute_sql("ROLLBACK TO keep", &[]).unwrap();
            assert_eq!(session.catalog_snapshot().unwrap().1.len(), 1);
            for sql in ["DROP TABLE gone", finish] { session.execute_sql(sql, &[]).unwrap(); }
        }
        assert!(session.catalog_snapshot().unwrap().1.is_empty());
    }
    #[test]
    fn metadata_snapshot_filters_real_definitions_without_reading_or_mutating_rows() {
        let (service, bindings) = fixture();
        service.admin.sql_session().execute(
            "CREATE TABLE hidden (tenant BIGINT, id INTEGER, body TEXT, PRIMARY KEY (tenant,id))",
            &[], None,
        ).unwrap();
        let before = service.admin.catalog().unwrap();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        let (epoch, visible, _) = session.catalog_snapshot().unwrap();
        assert_eq!(epoch, before.0);
        assert_eq!(visible.len(), 1);
        assert_eq!(
            visible[0],
            before.1.iter().find(|t| t.name == "app").unwrap().clone()
        );
        assert_eq!(service.admin.catalog().unwrap(), before);
        assert!(!session.in_transaction());
        session.execute_sql("BEGIN", &[]).unwrap();
        assert_eq!(session.catalog_snapshot().unwrap().1, visible);
        session.execute_sql("ROLLBACK", &[]).unwrap();
    }
    #[test]
    fn metadata_snapshot_denies_task_roles_and_does_not_invent_missing_tables() {
        let (service, mut bindings) = fixture();
        let domain = service.admin.authority_metadata().unwrap().0.to_string();
        bindings[0].role = Role::Task {
            domain: domain.clone(),
            task: TaskId(91),
            principal: Principal(92),
        };
        service.reload_bindings(bindings.clone()).unwrap();
        let mut task = service.session(&bindings[0].fingerprint).unwrap();
        assert!(matches!(
            task.catalog_snapshot(),
            Err(CoreError::Policy(Error::Scope))
        ));
        bindings[0].role = Role::Application {
            domain,
            tables: BTreeSet::from(["not_created".into()]),
        };
        service.reload_bindings(bindings.clone()).unwrap();
        let mut empty = service.session(&bindings[0].fingerprint).unwrap();
        assert!(empty.catalog_snapshot().unwrap().1.is_empty());
    }
    #[test]
    fn metadata_snapshot_fences_new_schema_during_transaction() {
        let (service, mut bindings) = fixture();
        let Role::Application { tables, .. } = &mut bindings[0].role else {
            unreachable!()
        };
        tables.insert("future".into());
        service.reload_bindings(bindings.clone()).unwrap();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        session.execute_sql("BEGIN", &[]).unwrap();
        let before = session.catalog_snapshot().unwrap().1;
        let mut owner = service.admin.sql_session();
        owner
            .execute("CREATE TABLE future (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert!(matches!(
            session.catalog_snapshot(),
            Err(CoreError::Sql(relational::SqlError::UnknownTable))
        ));
        session.sql.rollback();
        let after = session.catalog_snapshot().unwrap().1;
        assert_eq!(before.len(), 1);
        assert_eq!(after.len(), 2);
        assert!(after.iter().any(|table| table.name == "future"));
    }
    #[test]
    fn metadata_snapshot_rechecks_revocation_and_aborted_state() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        session.execute_sql("BEGIN", &[]).unwrap();
        session.abort();
        assert!(matches!(
            session.catalog_snapshot(),
            Err(CoreError::Sql(relational::SqlError::Aborted))
        ));
        session.sql.rollback();
        assert!(session.catalog_snapshot().is_ok());
        service.reload_bindings(vec![]).unwrap();
        assert!(matches!(
            session.catalog_snapshot(),
            Err(CoreError::Policy(Error::Authentication))
        ));
        service.reload_bindings(bindings).unwrap();
        assert!(session.catalog_snapshot().is_err());
    }
    #[test]
    fn sentinel_bindings_and_legacy_multi_source_plans_cannot_expand_scope() {
        let (service, mut bindings) = fixture();
        let mut admin = service.admin.sql_session();
        admin
            .execute(
                "CREATE TABLE secret (id BIGINT PRIMARY KEY, body TEXT)",
                &[],
                None,
            )
            .unwrap();
        admin
            .execute("INSERT INTO app VALUES (1)", &[], None)
            .unwrap();
        admin
            .execute("INSERT INTO secret VALUES (1,'hidden')", &[], None)
            .unwrap();
        let Role::Application { tables, .. } = &mut bindings[0].role else {
            unreachable!()
        };
        tables.insert("giggles_join".into());
        assert!(matches!(
            Service::validate(&service.admin, bindings.clone()),
            Err(CoreError::Policy(Error::Scope))
        ));
        assert!(service.reload_bindings(bindings.clone()).is_err());
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        assert!(
            session
                .prepare("SELECT body FROM secret WHERE id=1")
                .is_err()
        );
        // Defense in depth even if an older trusted binding existed in memory.
        session.role = bindings[0].role.clone();
        let plan = admin
            .prepare("SELECT s.body FROM app a JOIN secret s ON s.id=a.id WHERE a.id=1")
            .unwrap();
        assert_eq!(plan.table(), Some("giggles_join"));
        assert!(matches!(
            session.authorize(&plan),
            Err(CoreError::Policy(Error::Scope))
        ));
    }
    #[test]
    fn removal_readdition_never_resurrects_session_prepared_transaction_or_cancel_key() {
        let (service, bindings) = fixture();
        let mut old = service.session(&bindings[0].fingerprint).unwrap();
        let prepared = old.prepare("INSERT INTO app (id) VALUES (1)").unwrap();
        old.execute_sql("BEGIN", &[]).unwrap();
        old.execute_prepared(&prepared, &[]).unwrap();
        let backend = service
            .cancellations
            .register(&old.fingerprint, old.epoch)
            .unwrap();
        let active = backend.begin(true);
        service.reload_bindings(vec![]).unwrap();
        assert!(service.session(&old.fingerprint).is_err());
        assert!(old.execute_sql("COMMIT", &[]).is_err());
        assert!(old.execute_prepared(&prepared, &[]).is_err());
        assert_eq!(old.handle(Request::Ping).code, "28000");
        service.cancel_bound(&old, backend.pid, backend.secret);
        assert!(active.context.check().is_ok());
        service.reload_bindings(bindings).unwrap();
        let mut fresh = service.session(&old.fingerprint).unwrap();
        assert_ne!(old.epoch, fresh.epoch);
        service.cancel_bound(&fresh, backend.pid, backend.secret);
        assert!(active.context.check().is_ok());
        assert!(old.prepare("SELECT id FROM app LIMIT 1").is_err());
        assert!(
            fresh
                .execute_sql("SELECT id FROM app LIMIT 1", &[])
                .unwrap()
                .rows
                .is_empty()
        );
    }
    #[test]
    fn unchanged_overlap_rotation_preserves_only_exact_authority() {
        let (service, mut bindings) = fixture();
        let mut old = service.session(&bindings[0].fingerprint).unwrap();
        bindings.push(Binding {
            fingerprint: "22".repeat(32),
            role: bindings[0].role.clone(),
        });
        service.reload_bindings(bindings.clone()).unwrap();
        let mut new = service.session(&bindings[1].fingerprint).unwrap();
        assert!(
            old.execute_sql("INSERT INTO app (id) VALUES (1)", &[])
                .is_ok()
        );
        assert_eq!(
            new.execute_sql("SELECT id FROM app WHERE id=1", &[])
                .unwrap()
                .rows
                .len(),
            1
        );
        bindings.remove(0);
        service.reload_bindings(bindings).unwrap();
        assert!(old.execute_sql("DELETE FROM app WHERE id=1", &[]).is_err());
        assert_eq!(
            new.execute_sql("SELECT id FROM app WHERE id=1", &[])
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    #[test]
    fn invalid_reload_is_atomic_and_epoch_overflow_cannot_partially_replace() {
        let (service, bindings) = fixture();
        let mut old = service.session(&bindings[0].fingerprint).unwrap();
        let mut overlapping = bindings.clone();
        let mut other = bindings[0].clone();
        other.fingerprint = "22".repeat(32);
        if let Role::Application { tables, .. } = &mut other.role {
            tables.insert("other".into());
        }
        overlapping.push(other);
        let epoch = old.epoch;
        for invalid in [
            overlapping,
            vec![bindings[0].clone(), bindings[0].clone()],
            vec![Binding {
                fingerprint: "AA".repeat(32),
                role: bindings[0].role.clone(),
            }],
            vec![bindings[0].clone(); 1025],
        ] {
            assert!(service.reload_bindings(invalid).is_err());
            assert_eq!(old.handle(Request::Ping).code, "ok");
            assert_eq!(service.session(&old.fingerprint).unwrap().epoch, epoch);
        }
        service.bindings.lock().unwrap().next = u64::MAX;
        let mut replacement = bindings[0].clone();
        replacement.fingerprint = "33".repeat(32);
        assert!(service.reload_bindings(vec![replacement]).is_err());
        assert_eq!(old.handle(Request::Ping).code, "ok");
    }
    #[test]
    fn role_change_and_change_back_never_revives_old_privileges() {
        let (service, bindings) = fixture();
        let mut old = service.session(&bindings[0].fingerprint).unwrap();
        let mut changed = bindings.clone();
        if let Role::Application { tables, .. } = &mut changed[0].role {
            *tables = BTreeSet::from(["other".into()]);
        }
        service.reload_bindings(changed).unwrap();
        assert!(
            old.execute_sql("INSERT INTO app (id) VALUES (1)", &[])
                .is_err()
        );
        service.reload_bindings(bindings).unwrap();
        assert!(
            old.execute_sql("INSERT INTO app (id) VALUES (1)", &[])
                .is_err()
        );
        assert!(
            service
                .session(&old.fingerprint)
                .unwrap()
                .execute_sql("SELECT id FROM app LIMIT 1", &[])
                .unwrap()
                .rows
                .is_empty()
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn queued_blocking_work_rechecks_binding_before_any_side_effect() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        let pool = service.blocking.clone();
        // Pause the actual submitted worker before service execution.
        let (started, rx) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let job = tokio::spawn(async move {
            pool.run(move || {
                started.send(()).unwrap();
                wait.recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                session.handle(Request::Sql {
                    sql: "INSERT INTO app (id) VALUES (1)".into(),
                    params: vec![],
                })
            })
            .await
            .unwrap()
        });
        rx.await.unwrap();
        service.reload_bindings(vec![]).unwrap();
        release.send(()).unwrap();
        assert_eq!(job.await.unwrap().code, "28000");
        assert!(
            service
                .admin
                .sql_session()
                .execute("SELECT id FROM app LIMIT 1", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn detached_worker_retains_shared_session_admission_until_real_completion() {
        let (original, bindings) = fixture();
        let service = Service::with_limits(
            original.admin.clone(),
            bindings.clone(),
            Clock::new(0, 3600).unwrap(),
            crate::admission::ServiceLimits {
                sessions: 1,
                burst: 100,
                per_second: 100,
            },
        )
        .unwrap();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        session.connection = service.admission.connect();
        assert!(session.connection.is_some());
        let (started, rx) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let pool = service.blocking.clone();
        let job = tokio::spawn(async move {
            pool.run(move || {
                started.send(()).unwrap();
                wait.recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                assert_eq!(session.handle(Request::Ping).code, "ok");
                drop(session);
                finished.send(()).unwrap();
            })
            .await
        });
        rx.await.unwrap();
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert!(service.admission.connect().is_none());
        release.send(()).unwrap();
        done.await.unwrap();
        assert!(service.admission.connect().is_some());
    }
    #[test]
    fn unchanged_reload_preserves_existing_backend_and_changed_epoch_denies_it() {
        let (service, bindings) = fixture();
        let old = service.session(&bindings[0].fingerprint).unwrap();
        let backend = service
            .cancellations
            .register(&old.fingerprint, old.epoch)
            .unwrap();
        service.reload_bindings(bindings.clone()).unwrap();
        let active = backend.begin(true);
        service.cancel_bound(&old, backend.pid, backend.secret);
        assert_eq!(active.context.check(), Err(CoreError::Cancelled));
        drop(active);
        service.reload_bindings(vec![]).unwrap();
        service.reload_bindings(bindings).unwrap();
        let active = backend.begin(true);
        let new = service.session(&old.fingerprint).unwrap();
        service.cancel_bound(&new, backend.pid, backend.secret);
        assert!(active.context.check().is_ok());
    }
    #[test]
    fn cancelled_wait_for_reload_lease_returns_before_writer_releases_and_never_writes() {
        let (service, bindings) = fixture();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        let writer = service.authority_gate.write().unwrap();
        let context = Cancellation::default();
        let worker_context = context.clone();
        let (started, rx) = std::sync::mpsc::channel();
        let (finished, done) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            worker_context.scope(|| {
                started.send(()).unwrap();
                finished
                    .send(session.handle(Request::Sql {
                        sql: "INSERT INTO app (id) VALUES (1)".into(),
                        params: vec![],
                    }))
                    .unwrap();
            })
        });
        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        context.cancel();
        assert_eq!(
            done.recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .code,
            "57014"
        );
        drop(writer);
        worker.join().unwrap();
        assert!(
            service
                .admin
                .sql_session()
                .execute("SELECT id FROM app LIMIT 1", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
}
