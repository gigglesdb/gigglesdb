use crate::clock::Clock;
use giggles_core::{
    relational::{Prepared, SqlResult, StatementClock, Value},
    *,
};
use giggles_core::task::{Page, Released, Unreleased};
use giggles_pg_types::temporal::SessionZone;
use serde::{Deserialize, Serialize};
#[cfg(feature = "legacy-task")]
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{Arc, MutexGuard, atomic::{AtomicU64, Ordering}},
    time::Duration,
};

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "role", deny_unknown_fields)]
pub enum Role {
    #[cfg(feature = "legacy-task")]
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
    /// GD-0061 R3-1202: a certificate identity with no authority; answers Ping only.
    NoSql { domain: String },
}
impl<'de> Deserialize<'de> for Role {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "role", deny_unknown_fields)]
        enum Input {
            Task { domain: String, task: TaskId, principal: Principal },
            Application { domain: String, tables: BTreeSet<String> },
            Database { name: String },
            NoSql { domain: String },
        }
        match Input::deserialize(deserializer)? {
            Input::Task { domain, task, principal } => {
                #[cfg(feature = "legacy-task")]
                { Ok(Role::Task { domain, task, principal }) }
                #[cfg(not(feature = "legacy-task"))]
                {
                    let _ = (domain, task, principal);
                    Err(serde::de::Error::custom("role Task is the retired legacy task prototype; enable the legacy-task feature or bind the certificate to NoSql/Application/Database"))
                }
            }
            Input::Application { domain, tables } => Ok(Role::Application { domain, tables }),
            Input::Database { name } => Ok(Role::Database { name }),
            Input::NoSql { domain } => Ok(Role::NoSql { domain }),
        }
    }
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
    #[cfg(feature = "legacy-task")]
    Preview {
        request: giggles_core::Request,
    },
    #[cfg(feature = "legacy-task")]
    Execute {
        request: giggles_core::Request,
        approval: Option<ApprovalId>,
    },
    #[cfg(feature = "legacy-task")]
    RepairSql {
        operation: OperationId,
        sql: String,
        approval: Option<ApprovalId>,
    },
    #[cfg(feature = "legacy-task")]
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
pub(crate) type ApiSqlReplyCheck<'a> = dyn FnMut(
    Released<&SqlResult>, Option<&mut giggles_core::relational::spill::SpilledRows>, &[u32],
) -> Result<(), CoreError> + 'a;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub version: u32,
    pub code: String,
    pub operation: Option<OperationId>,
    pub count: Option<usize>,
    pub digest: Option<String>,
    pub sql: Option<SqlResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_types: Option<Vec<u32>>,
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
            sql_types: None,
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
        Self::released_sql_typed(result, Vec::new())
    }
    pub(crate) fn released_sql_typed(result: Released<SqlResult>, types: Vec<u32>) -> Self {
        Self {
            sql: Some(result.into_inner()),
            sql_types: Some(types),
            ..Self::code("ok")
        }
    }
    #[cfg(feature = "legacy-task")]
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
#[cfg(feature = "legacy-task")]
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

fn routine_ddl_error(sqlstate: &'static str, message: String) -> CoreError {
    CoreError::RoutineDdl(Box::new(giggles_core::relational::RoutineDiagnostic {
        sqlstate, message, detail: None, hint: None, context: None, position: None,
    }))
}

pub type StatementGate = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

const DURABILITY_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
struct DurabilityPath {
    awaiting: std::sync::atomic::AtomicU64,
    releases: std::sync::atomic::AtomicU64,
    asynchronous: std::sync::atomic::AtomicU64,
}
pub struct Service {
    durability: [DurabilityPath; 3],
    drain_entries: std::sync::atomic::AtomicU64,
    pgwire_live: Arc<giggles_core::DurableTracker>,
    pgwire_submitted: Arc<giggles_core::DurableTracker>,
    durable: Arc<giggles_core::DurableTracker>,
    pub(crate) pgwire_blocking_tasks: std::sync::atomic::AtomicU64,
    pub(crate) statement_gate: std::sync::OnceLock<StatementGate>,
    pub(crate) advisory: crate::advisory::Locks,
    pub(crate) blocking: crate::blocking::Blocking,
    pub(crate) cancellations: crate::cancel::Registry,
    #[cfg(feature = "legacy-task")]
    task_gate: std::sync::Mutex<()>,
    pub(crate) admin: Admin,
    pub(crate) clock: Clock,
    password_generations: std::sync::Mutex<PasswordGenerations>,
    password_generation_revision: AtomicU64,
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
type PasswordGenerations = BTreeMap<String, (u32, u64, u64)>;
struct Credentials {
    next: u64,
    entries: BTreeMap<String, (Role, u64)>,
}
pub(crate) struct LocalDurabilityWait(Arc<Service>);
impl Drop for LocalDurabilityWait {
    fn drop(&mut self) { self.0.durability[0].awaiting.fetch_sub(1, std::sync::atomic::Ordering::AcqRel); }
}
impl Service {
    pub(crate) fn local_durability_release(self: &Arc<Self>) -> LocalDurabilityWait {
        self.durability[0].awaiting.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.durability[0].releases.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        LocalDurabilityWait(self.clone())
    }
    pub fn role_pool_counts(&self) -> Vec<(usize, usize, usize)> { self.admission.role_pool_counts() }
    pub fn pgwire_blocking_live(&self) -> usize { self.pgwire_live.live() }
    pub fn durable_tracker(&self) -> Arc<giggles_core::DurableTracker> { self.durable.clone() }
    pub(crate) fn pgwire_submission_token(&self) -> giggles_core::LiveToken { self.pgwire_submitted.register() }
    pub(crate) fn pgwire_live_token(&self) -> giggles_core::LiveToken { self.pgwire_live.register() }
    /// Per-path durability counters, exported even before the first ticket.
    pub fn durability_metrics(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering::Acquire;
        let paths = ["local", "cluster", "nextval"];
        let values = |read: fn(&DurabilityPath) -> &std::sync::atomic::AtomicU64| {
            paths.iter().zip(&self.durability).map(|(path, metric)| {
                ((*path).to_owned(), serde_json::json!(read(metric).load(Acquire)))
            }).collect::<serde_json::Map<String, serde_json::Value>>()
        };
        serde_json::json!({
            "pgwire_blocking_live": self.pgwire_blocking_live(),
            "durable_completions_live": self.durable.live(),
            "statements_awaiting_durability": values(|m| &m.awaiting),
            "durable_releases_total": values(|m| &m.releases),
            "durable_async_total": values(|m| &m.asynchronous)
        })
    }
    pub fn stop_submissions(&self) { self.admin.stop_submissions(); }
    /// Keep the runtime alive until detached workers and durable tickets retire.
    #[doc(hidden)]
    pub fn drain_entries(&self) -> u64 {
        self.drain_entries.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn drain(&self) -> std::io::Result<()> {
        self.drain_with_timeout(DURABILITY_DRAIN_TIMEOUT).await
    }

    fn durability_pending(&self) -> bool {
        self.durable.live() != 0
            || self.durability.iter().any(|path| path.awaiting.load(Ordering::Acquire) != 0)
    }

    /// Call after joining the socket tasks, when the listener cannot submit new jobs.
    pub(crate) async fn wait_for_pgwire_work(&self, work: &DurableTracker) -> io::Result<()> {
        let started = tokio::time::Instant::now();
        let mut workers_since = None;
        while work.live() != 0 {
            let now = tokio::time::Instant::now();
            let worker_timed_out = if self.durability_pending() {
                workers_since = None;
                false
            } else {
                now.duration_since(*workers_since.get_or_insert(now)) >= crate::transport::IO_TIMEOUT
            };
            if worker_timed_out || now.duration_since(started) >= DURABILITY_DRAIN_TIMEOUT {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "PostgreSQL listener shutdown worker drain",
                ));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    /// Override the durability deadline for shutdown qualification tests.
    #[doc(hidden)]
    pub async fn drain_with_timeout(&self, durability_timeout: std::time::Duration) -> std::io::Result<()> {
        self.drain_entries.fetch_add(1, std::sync::atomic::Ordering::Release);
        let started = tokio::time::Instant::now();
        let mut report = started;
        let mut workers_since = None;
        loop {
            let now = tokio::time::Instant::now();
            let durable = self.durability_pending();
            let workers = self.pgwire_blocking_live() != 0
                || self.pgwire_submitted.live() != 0 || self.blocking.in_use() != 0;
            if !durable && !workers { return Ok(()); }
            if now.duration_since(started) >= durability_timeout {
                eprintln!("ERROR: service shutdown durability deadline exceeded: {}; abandoned statements were never acknowledged to clients", self.durability_metrics());
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "service shutdown durability drain"));
            }
            if durable {
                workers_since = None;
                if now.duration_since(report) >= std::time::Duration::from_secs(10) {
                    eprintln!("service shutdown waiting for durability: {}", self.durability_metrics());
                    report = now;
                }
            } else if now.duration_since(*workers_since.get_or_insert(now)) >= crate::transport::IO_TIMEOUT {
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "service shutdown worker drain"));
            }
            // Recheck durability during worker draining: a worker may register late.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    #[doc(hidden)]
    pub fn pgwire_blocking_tasks(&self) -> u64 {
        self.pgwire_blocking_tasks.load(std::sync::atomic::Ordering::Relaxed)
    }

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
            durability: std::array::from_fn(|_| DurabilityPath::default()),
            drain_entries: Default::default(),
            pgwire_live: Arc::default(),
            pgwire_submitted: Arc::default(),
            durable: Arc::default(),
            advisory: crate::advisory::Locks::default(),
            pgwire_blocking_tasks: std::sync::atomic::AtomicU64::new(0),
            blocking: crate::blocking::Blocking::new(),
            cancellations: crate::cancel::Registry::default(),
            #[cfg(feature = "legacy-task")]
            task_gate: std::sync::Mutex::new(()),
            statement_gate: std::sync::OnceLock::new(),
            password_generations: Default::default(),
            password_generation_revision: AtomicU64::new(0),
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
                Role::Application { domain: bound, .. }
                | Role::NoSql { domain: bound }
                    if bound != &domain =>
                {
                    return Err(CoreError::Policy(Error::Authentication));
                }
                #[cfg(feature = "legacy-task")]
                Role::Task { domain: bound, .. } if bound != &domain => {
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
        if session.credential.is_some() {
            if session.credential_live().unwrap_or(false) {
                self.cancellations
                    .cancel(&session.fingerprint, session.epoch, pid, secret);
            }
            return;
        }
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
    fn update_password_generations<T>(
        &self,
        aliases: &mut MutexGuard<'_, PasswordGenerations>,
        update: impl FnOnce(&mut PasswordGenerations) -> T,
    ) -> T {
        // Writers hold the alias mutex across both the catalog commit and
        // the map update. Publish an odd revision before either can occur.
        // Only publish another usable (even) revision once both are ready.
        // Saturation stays odd and permanently disables memoization.
        let revision = self.password_generation_revision.load(Ordering::Relaxed);
        self.password_generation_revision.store(revision.saturating_add(1), Ordering::Release);
        let result = update(aliases);
        self.password_generation_revision.store(revision.saturating_add(2), Ordering::Release);
        result
    }

    fn role_generation_matches(&self, role: &giggles_core::roles::CatalogRole, epoch: u64) -> bool {
        role.generation == epoch
            || self.password_generations.lock().is_ok_and(|aliases| {
                aliases.get(&role.name).is_some_and(|&(oid, first, last)| {
                    role.oid == oid && role.generation == last && (first..=last).contains(&epoch)
                })
            })
    }
    #[cfg(test)]
    fn native_identity_reference(
        self: &Arc<Self>, role: &Role, epoch: u64, fence: Option<(u32, u64)>,
        fingerprint: &str, bound: bool,
    ) -> Result<(), CoreError> {
        let mut session = if bound { self.session(fingerprint)? } else {
            let Role::Database { name } = role else { return Err(CoreError::Policy(Error::Authentication)); };
            self.role_session(name)?
        };
        if bound && (session.role != *role || session.epoch != epoch) {
            return Err(CoreError::Policy(Error::Authentication));
        }
        session.role_fence = fence;
        session.check_binding_reference()
    }
    // Preserve the constructor preflight before the shared retained-session
    // predicate: an unfenced role identity uses the generation read here,
    // whereas a certificate identity uses its binding epoch.
    pub(crate) fn check_native_identity(
        &self,
        role: &Role,
        epoch: u64,
        role_fence: Option<(u32, u64)>,
        fingerprint: &str,
        fingerprint_bound: bool,
    ) -> Result<(), CoreError> {
        // Only certificate identities may carry a non-database role.
        if !fingerprint_bound && !matches!(role, Role::Database { .. }) {
            return Err(CoreError::Policy(Error::Authentication));
        }
        let (current_role, current_epoch) = if fingerprint_bound {
            self.bindings.lock().map_err(|_| CoreError::Fenced)?
                .entries.get(fingerprint).cloned()
                .ok_or(CoreError::Policy(Error::Authentication))?
        } else {
            (role.clone(), epoch)
        };
        let (current_epoch, database) = if let Role::Database { name } = &current_role {
            let image = self
                .admin
                .roles()
                .image_for_role(name)
                .map_err(|_| CoreError::Policy(Error::Authentication))?;
            let generation = image
                .with_role(name, |current| {
                    current
                        .filter(|role| role.login && !role.task_only)
                        .map(|role| role.generation)
                })
                .map_err(|_| CoreError::Policy(Error::Authentication))?
                .ok_or(CoreError::Policy(Error::Authentication))?;
            let epoch = if fingerprint_bound { current_epoch } else { generation };
            (epoch, Some((name, image)))
        } else {
            (current_epoch, None)
        };
        if fingerprint_bound && (&current_role != role || current_epoch != epoch) {
            return Err(CoreError::Policy(Error::Authentication));
        }
        if let Some((name, image)) = database {
            self.check_role_binding_in(&image, name, current_epoch, role_fence)?;
            if !fingerprint_bound {
                return Ok(());
            }
        }
        self.check_fingerprint_binding(&current_role, current_epoch, fingerprint)
    }
    /// Revalidate ordinary role authority without constructing SQL session state.
    pub(crate) fn check_role_binding(
        &self,
        role: &Role,
        epoch: u64,
        role_fence: Option<(u32, u64)>,
        fingerprint: &str,
        fingerprint_bound: bool,
    ) -> Result<(), CoreError> {
        if let Role::Database { name } = role {
            let image = self.admin.roles().image_for_role(name).map_err(role_lookup_error)?;
            self.check_role_binding_in(&image, name, epoch, role_fence)?;
            // Trusted pgwire sessions have no fingerprint binding to fence.
            if !fingerprint_bound {
                return Ok(());
            }
        }
        self.check_fingerprint_binding(role, epoch, fingerprint)
    }
    fn check_role_binding_in(
        &self,
        image: &roles::RoleCatalogImage,
        name: &str,
        epoch: u64,
        role_fence: Option<(u32, u64)>,
    ) -> Result<(), CoreError> {
        image
            .with_role(name, |current| {
                current
                    .filter(|role| {
                        role.login
                            && !role.task_only
                            && role_fence.is_none_or(|(oid, _)| role.oid == oid)
                            && self.role_generation_matches(
                                role, role_fence.map_or(epoch, |(_, generation)| generation),
                            )
                    })
                    .map(|_| ())
            })
            .map_err(role_lookup_error)?
            .ok_or(CoreError::Policy(Error::Authentication))
    }
    fn check_fingerprint_binding(
        &self,
        role: &Role,
        epoch: u64,
        fingerprint: &str,
    ) -> Result<(), CoreError> {
        let credentials = self
            .bindings
            .lock()
            .map_err(|_| CoreError::Fenced)?;
        if credentials
            .entries
            .get(fingerprint)
            .is_some_and(|(bound_role, bound_epoch)| *bound_epoch == epoch && bound_role == role)
        {
            Ok(())
        } else {
            Err(CoreError::Policy(Error::Authentication))
        }
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
            pgwire_listener_work: None,
            backend_ssl: false,
            service: self.clone(),
            fingerprint: format!("pgrole:{}", role.name),
            effective_role: None,
            effective_session: None,
            role_notices: Vec::new(),
            role: Role::Database { name: role.name },
            epoch: role.generation,
            fingerprint_bound: false,
            agent: false,
            credential: None,
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
            search_path_input: None,
            authorization_memos: Default::default(),
            #[cfg(test)]
            authorization_hits: 0,
            #[cfg(test)]
            authorization_full_runs: 0,
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        session.sync_sequence_authority();
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
            pgwire_listener_work: None,
            backend_ssl: false,
            service: self.clone(),
            effective_role: None,
            effective_session: None,
            role_notices: Vec::new(),
            role,
            epoch,
            fingerprint_bound: true,
            agent: false,
            credential: None,
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
            search_path_input: None,
            authorization_memos: Default::default(),
            #[cfg(test)]
            authorization_hits: 0,
            #[cfg(test)]
            authorization_full_runs: 0,
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        session.sync_sequence_authority();
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
            pgwire_listener_work: None,
            backend_ssl: false,
            service: self.clone(),
            fingerprint: fingerprint.to_owned(),
            effective_role: None,
            effective_session: None,
            role_notices: Vec::new(),
            role: Role::Database { name: role.name },
            epoch,
            fingerprint_bound: true,
            agent: true,
            credential: None,
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
            search_path_input: None,
            authorization_memos: Default::default(),
            #[cfg(test)]
            authorization_hits: 0,
            #[cfg(test)]
            authorization_full_runs: 0,
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        session.sync_sequence_authority();
        Ok(session)
    }
    /// Resolve credential authority without constructing a SQL session.
    pub(crate) fn credential_identity(
        &self,
        secret: &str,
        now: giggles_core::task::definition::WallClockMs,
    ) -> Option<(
        String,
        String,
        u64,
        giggles_core::task::definition::TaskObjectId,
    )> {
        let matched = self.admin.tasks().authenticate(secret, now).ok()?;
        if !self.agent_executor_valid(&matched.executor) {
            return None;
        }
        Some((
            format!("sqlcred:{:016x}", matched.credential),
            matched.executor,
            matched.credential,
            matched.task,
        ))
    }

    /// Open a task session with a SQL-issued credential. Refusals deliberately
    /// disclose neither the secret nor which authentication check failed.
    pub fn credential_session(
        self: &Arc<Self>,
        secret: &str,
        user: Option<&str>,
        task: Option<&str>,
    ) -> Result<Session, CoreError> {
        use giggles_core::task::context::open_context;
        let refused = || CoreError::Policy(Error::Authentication);
        let now = crate::agent::wall_now(&self.clock).map_err(|_| refused())?;
        let matched = self
            .admin
            .tasks()
            .authenticate(secret, now)
            .map_err(|_| refused())?;
        let def = self
            .admin
            .tasks()
            .get_by_id(matched.task)
            .map_err(|_| refused())?
            .ok_or_else(refused)?;
        if task.is_some_and(|name| name != def.name.as_str()) {
            return Err(refused());
        }
        let role = self
            .admin
            .roles()
            .get(&matched.executor)
            .map_err(|_| refused())?
            .filter(crate::agent::bindings::executor_role)
            .ok_or_else(refused)?;
        if user.is_some_and(|name| name != role.name) {
            return Err(refused());
        }
        let id = matched.credential;
        let mut digest = [0u8; 32];
        digest[..8].copy_from_slice(b"sqlcred\0");
        digest[24..].copy_from_slice(&id.to_be_bytes());
        let retention = self
            .outcome_retention_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut sql = self.admin.sql_session();
        sql.set_authority_managed(true);
        let ctx = open_context(&sql, &def.name, &role.name, digest, now, retention)
            .map_err(|_| refused())?;
        sql.bind_task(ctx).map_err(|_| refused())?;
        let chunk = self.agent_admission.limits();
        sql.set_disclosure_chunk(chunk.disclosure_chunk_rows, chunk.disclosure_chunk_bytes);
        let mut session = Session {
            surface: Surface::Native,
            backend_pid: 0,
            connection_closed: Default::default(),
            pgwire_listener_work: None,
            backend_ssl: false,
            service: self.clone(),
            fingerprint: format!("sqlcred:{id:016x}"),
            effective_role: None,
            effective_session: None,
            role_notices: Vec::new(),
            role: Role::Database { name: role.name },
            epoch: id,
            fingerprint_bound: true,
            agent: true,
            credential: Some((matched.task, id)),
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
            search_path_input: None,
            authorization_memos: Default::default(),
            #[cfg(test)]
            authorization_hits: 0,
            #[cfg(test)]
            authorization_full_runs: 0,
            api_path: ApiPathState::default(),
        };
        session.set_search_path("\"$user\", \"public\"");
        session.sync_sequence_authority();
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
#[derive(Clone, Copy, PartialEq, Eq)]
struct AuthorizationContext {
    latest_catalog: u64,
    statement_catalog: u64,
    stamp: u64,
    schema_revision: u64,
    password_revision: u64,
}

struct AuthorizationMemo {
    context: AuthorizationContext,
    role: Role,
    effective_role: Option<Role>,
    effective_session: Option<String>,
    epoch: u64,
    role_fence: Option<(u32, u64)>,
    plan: Option<relational::AuthorizationPlanKey>,
    sequence_tables: Option<BTreeSet<String>>,
    sequence_defaults: Option<Arc<giggles_core::SequenceDefaultGrants>>,
}

pub struct Session {
    pub(crate) surface: Surface,
    pub(crate) backend_pid: i32,
    pub(crate) connection_closed: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) pgwire_listener_work: Option<Arc<giggles_core::DurableTracker>>,
    pub(crate) backend_ssl: bool,
    pub(crate) service: Arc<Service>,
    pub(crate) role: Role,
    effective_role: Option<Role>,
    effective_session: Option<String>,
    pub(crate) role_notices: Vec<(bool, String)>,
    pub(crate) database: Option<String>,
    pub(crate) fingerprint: String,
    // Fingerprint binding epoch, or the role generation for trusted pgwire sessions.
    pub(crate) epoch: u64,
    // Trusted pgwire sessions pin role identity without a certificate binding.
    fingerprint_bound: bool,
    // An agent (task) session: bound through the agent map or a SQL credential.
    pub(crate) agent: bool,
    credential: Option<(giggles_core::task::definition::TaskObjectId, u64)>,
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
    search_path_input: Option<Arc<str>>,
    // One SQL plan and one entry for each implicit control. Controls must
    // not evict the statement that runs between them.
    authorization_memos: [Option<AuthorizationMemo>; 3],
    #[cfg(test)]
    pub(crate) authorization_hits: usize,
    #[cfg(test)]
    pub(crate) authorization_full_runs: usize,
    pub(crate) api_path: ApiPathState,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.abort();
        self.service.advisory.release(self.backend_pid, false);
    }
}
// Virtual namespaces exposed by pg_namespace exist even without physical schema metadata.
pub(crate) fn is_builtin_catalog_schema(schema: &str) -> bool {
    matches!(schema, "pg_catalog" | "pg_toast" | "information_schema")
}

impl Session {
    pub(crate) fn set_postgres_surface(&mut self) {
        self.surface = Surface::Postgres;
        if !self.fingerprint_bound && !self.agent && matches!(self.role, Role::Database { .. }) {
            self.sql.enable_authorization_bindings();
        }
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn clear_sequence_default_grants(&mut self) {
        if let Some(memo) = &mut self.authorization_memos[0] {
            memo.sequence_defaults = None;
        }
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn has_sequence_default_grants(&self) -> bool {
        self.authorization_memos[0].as_ref().is_some_and(|memo| memo.sequence_defaults.is_some())
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn observe_sequence_default_execution(observe: impl FnOnce(bool) + 'static) {
        BEFORE_SEQUENCE_DEFAULT_EXECUTION.with_borrow_mut(|hook| *hook = Some(Box::new(observe)));
    }

    fn authority_role_live(&self, role: &roles::CatalogRole) -> bool {
        if self.effective_role.is_some() { !role.task_only }
        else { role.login && self.service.role_generation_matches(role, self.role_epoch()) }
    }
    pub(crate) fn authority_role(&self) -> &Role {
        self.effective_role.as_ref().unwrap_or(&self.role)
    }
    pub(crate) fn session_user_name(&self) -> Option<&str> {
        self.effective_session.as_deref().or(match &self.role { Role::Database { name } => Some(name), _ => None })
    }
    /// Only the database GUC path may acquire effective authority; login identity is immutable.
    pub(crate) fn check_role_setting(&self, name: crate::pg_settings::Show, value: Option<&str>) -> Result<(), &'static str> {
        self.check_binding().map_err(|e| sqlstate(&e))?;
        let Role::Database { name: login } = &self.role else { return Err("42501"); };
        if self.agent || self.sql.task().is_some() { return Err("42501"); }
        let roles = self.service.admin.roles();
        let authenticated = roles.get(login).map_err(|_| "XX000")?.ok_or("28000")?;
        if name == crate::pg_settings::Show::SessionAuthorization && !authenticated.superuser
            && value.is_some_and(|target| target != login) { return Err("42501"); }
        if let Some(value) = value {
            let target = roles.get(value).map_err(|_| "XX000")?.ok_or("22023")?;
            if target.task_only { return Err("42501"); }
            if name == crate::pg_settings::Show::Role {
                let session = self.session_user_name().ok_or("42501")?;
                let actor = roles.get(session).map_err(|_| "XX000")?.ok_or("42501")?;
                if !actor.superuser && !roles.can_set(session, value).map_err(|_| "XX000")? { return Err("42501"); }
            }
        }
        Ok(())
    }
    pub(crate) fn pin_role_settings(&self, settings: &mut crate::pg_settings::Settings) -> Result<(), &'static str> {
        let roles = self.service.admin.roles();
        settings.selected_role_oid = if settings.selected_role == "none" { None }
            else { Some(roles.get(&settings.selected_role).map_err(|_| "XX000")?.ok_or("22023")?.oid) };
        settings.session_authorization_oid = if settings.session_authorization.is_empty() { None }
            else { Some(roles.get(&settings.session_authorization).map_err(|_| "XX000")?.ok_or("22023")?.oid) };
        Ok(())
    }
    pub(crate) fn sync_role_settings(&mut self, settings: &mut crate::pg_settings::Settings) -> Result<(), &'static str> {
        let Role::Database { name: authenticated } = &self.role else { return Ok(()); };
        let roles = self.service.admin.roles();
        let auth = roles.get(authenticated).map_err(|_| "XX000")?.ok_or("28000")?;
        let mut session = (!settings.session_authorization.is_empty() && settings.session_authorization != *authenticated).then(|| settings.session_authorization.clone());
        if let Some(name) = session.as_ref() {
            let valid = !self.agent && self.sql.task().is_none() && roles.get(name).map_err(|_| "XX000")?
                .is_some_and(|role| !role.task_only && Some(role.oid) == settings.session_authorization_oid
                    && (auth.superuser || name == authenticated));
            if !valid {
                self.role_notices.push((true, format!("role \"{name}\" can no longer be set; role reset")));
                session = None;
                settings.session_authorization = authenticated.clone();
                settings.session_authorization_oid = Some(auth.oid);
                settings.selected_role = "none".into();
                settings.selected_role_oid = None;
            }
        }
        if settings.selected_role != "none" {
            let name = &settings.selected_role;
            let session_name = session.as_deref().unwrap_or(authenticated);
            let actor = roles.get(session_name).map_err(|_| "XX000")?;
            let valid = !self.agent && self.sql.task().is_none() && roles.get(name).map_err(|_| "XX000")?
                .is_some_and(|role| !role.task_only && Some(role.oid) == settings.selected_role_oid)
                && (actor.is_some_and(|role| role.superuser) || roles.can_set(session_name, name).map_err(|_| "XX000")?);
            if !valid {
                self.role_notices.push((true, format!("role \"{name}\" can no longer be set; role reset")));
                settings.selected_role = "none".into();
                settings.selected_role_oid = None;
            }
        }
        let current = if settings.selected_role == "none" { session.clone() } else { Some(settings.selected_role.clone()) };
        self.effective_session = session;
        self.effective_role = current.map(|name| Role::Database { name });
        Ok(())
    }
    pub(crate) fn zone(&self) -> Option<SessionZone> {
        self.zone
    }
    pub(crate) fn set_search_path(&mut self, path: &str) {
        let user = match &self.role {
            Role::Database { name } => name.as_str(),
            _ => "giggles",
        };
        self.sql.set_search_path(path, user);
        let input = self.sql.search_path_input().expect("search path was applied");
        if self.search_path_input.as_ref().is_some_and(|saved| Arc::ptr_eq(saved, input)) {
            return;
        }
        self.search_path_input = Some(input.clone());
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
    pub(crate) fn set_max_statement_tokens(&mut self, tokens: usize) {
        self.sql.set_max_statement_tokens(tokens);
    }
    pub(crate) fn set_max_value_bytes(&mut self, bytes: usize) {
        self.sql.set_max_value_bytes(bytes);
    }
    pub(crate) fn set_text_settings(&mut self, settings: &crate::pg_settings::Settings) {
        self.sql.set_extra_float_digits(settings.extra_float_digits);
        self.sql.set_text_settings(settings.datestyle, settings.bytea_output);
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
    /// Check the SQL credential without consulting the certificate map.
    fn credential_live(&self) -> Result<bool, CoreError> {
        let Some((task, id)) = self.credential else {
            return Ok(false);
        };
        let now = crate::agent::wall_now(&self.service.clock)
            .map_err(|_| CoreError::Policy(Error::Authentication))?;
        Ok(self.service.admin.tasks().credential_live(task, id, now))
    }
    /// The credential is live, or the certificate entry still matches this task.
    fn agent_entry_matches(&self, current: &crate::agent::bindings::AgentCredentials) -> bool {
        if self.credential.is_some() {
            return self.credential_live().unwrap_or(false);
        }
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
        if self.credential.is_some() {
            return if self.credential_live()? {
                Ok(())
            } else {
                Err(CoreError::Policy(Error::Authentication))
            };
        }
        let current = self.service.agent.lock().map_err(|_| CoreError::Fenced)?;
        if self.agent_entry_matches(&current) {
            Ok(())
        } else {
            Err(CoreError::Policy(Error::Authentication))
        }
    }
    #[cfg(test)]
    fn check_binding_reference(&self) -> Result<(), CoreError> {
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
    pub(crate) fn check_binding(&self) -> Result<(), CoreError> {
        if self.agent {
            return self.check_agent_binding();
        }
        self.service.check_role_binding(
            &self.role, self.epoch, self.role_fence, &self.fingerprint, self.fingerprint_bound,
        )
    }

    /// V2 checks every nextval/currval/setval at execution against the effective
    /// role (GD-0084 Q24). It is taken from the session identity on each authorized
    /// request, independently of the statement authorization memo, so SET ROLE,
    /// RESET ROLE and SET SESSION AUTHORIZATION apply from the next request.
    /// The sequence guard checks roles, memberships and owners against the
    /// statement's live authority, including any validated S2 default grants.
    pub(crate) fn sync_sequence_authority(&mut self) {
        use giggles_core::SequenceAuthority as A;
        let current = self.sql.sequence_authority();
        let unchanged = match (self.authority_role(), current) {
            (Role::Database { name }, A::Role(held)) => name == held,
            (Role::Application { tables, .. }, A::Tables(held)) => tables == held,
            (Role::Database { .. } | Role::Application { .. }, _) => false,
            (_, current) => *current == A::Denied,
        };
        if !unchanged {
            let next = match self.authority_role() {
                Role::Database { name } => A::Role(name.clone()),
                Role::Application { tables, .. } => A::Tables(tables.clone()),
                _ => A::Denied,
            };
            self.sql.set_sequence_authority(next);
        }
    }
    pub(crate) fn authority_scope<T>(
        &mut self,
        work: impl FnOnce(&mut Self) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        if self.scoped {
            self.check_binding()?;
            self.sync_sequence_authority();
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
        self.sync_sequence_authority();
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
        let _ = name;
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
        let Role::Database { name: current } = self.authority_role() else { return Err(fail("42501", "task statements need a database role")); };
        let actor = self.service.admin.roles().get(current).map_err(|e| from_core(role_lookup_error(e)))?
            .filter(|role| !role.task_only).ok_or_else(|| fail("42501", "role cannot acquire task authority"))?;
        let now = crate::agent::wall_now(&self.service.clock).map_err(from_core)?;
        let outcome = crate::task_ddl::run(self, &actor, stmt, now)?;
        if matches!(stmt, giggles_core::task::statements::TaskStatement::Revoke(_)
            | giggles_core::task::statements::TaskStatement::RevokeCredential(_)) {
            self.service.binding_changes.send_replace(());
        }
        Ok(outcome)
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
            | crate::auth::statements::AdminStatement::Grant { .. }
            | crate::auth::statements::AdminStatement::Membership { .. } => {
                giggles_core::task::StatementClass::RoleStatement
            }
        };
        self.admit_backstop(class).map_err(|error| sqlstate(&error))?;
        let Role::Database { .. } = &self.role else {
            return Err("42501");
        };
        self.check_binding().map_err(|error| sqlstate(&error))?;
        if let crate::auth::statements::AdminStatement::DropTable(optional, names) = stmt
            && self.in_transaction()
            && self.sql.set_ddl_owner(match self.effective_role.as_ref().unwrap_or(&self.role) { Role::Database { name } => Some(name), _ => None })
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
        let Role::Database { name: current } = self.authority_role() else { return Err("42501"); };
        let actor = self.service.admin.roles().get(current).map_err(|e| sqlstate(&role_lookup_error(e)))?
            .filter(|role| !role.task_only).ok_or("28000")?;
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
                    && changes.createrole.is_none()
                    && changes.inherit.is_none() =>
            {
                self.service.admin.roles().get(name).map_err(|_| "XX000")?
            }
            _ => None,
        };
        if let crate::auth::statements::AdminStatement::Drop(_, names) = stmt {
            let current = match self.authority_role() { Role::Database { name } => Some(name.as_str()), _ => None };
            if names.iter().any(|name| current == Some(name.as_str()) || self.session_user_name() == Some(name.as_str())) { return Err("55006"); }
        }
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
        if let crate::auth::statements::AdminStatement::Membership { members, grantor, .. } = &mut bound {
            for name in members.iter_mut().chain(grantor.iter_mut()) {
                *name = match name.as_str() {
                    "\0current_user" | "\0current_role" => actor.name.clone(),
                    "\0session_user" => self.session_user_name().ok_or("42501")?.to_owned(),
                    _ => name.clone(),
                };
            }
        }
        let mut execute = || crate::auth::statements::execute(
            &bound, &self.service.admin, &actor, &mut self.role_notices,
        );
        let result = if let Some(before) = target {
            self.service.update_password_generations(&mut aliases, |aliases| {
                let result = execute()?;
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
                Ok(result)
            })
        } else {
            execute()
        };
        // Even a multi-role DROP that returns an error may have committed a
        // preceding drop. Notify binding checks after password aliases are ready.
        drop(aliases);
        self.service.binding_changes.send_replace(());
        result
    }
    /// Wire description of a prepared statement. Ordinary DDL has an empty
    /// shape. CTAS may inherit parameters from its source query.
    pub fn describe(&mut self, p: &Prepared) -> Result<relational::Description, CoreError> {
        if p.is_ddl() && !p.is_create_as() {
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
            match session.authority_role() {
                Role::Application { domain, .. } => {
                    if session.service.admin.authority_metadata()?.0.to_string() != *domain {
                        return Err(CoreError::Policy(Error::Authentication));
                    }
                }
                Role::Database { name } => {
                    session.service.admin.roles().get(name).map_err(role_lookup_error)?
                        .filter(|role| session.authority_role_live(role))
                        .ok_or(CoreError::Policy(Error::Authentication))?;
                }
                _ => return Err(CoreError::Policy(Error::Scope)),
            }
            let tables = crate::pg_catalog_wire::authorized_tables(
                session.authority_role(), &session.service.admin.roles(), std::iter::empty())?;
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
        // Authorization prechecks lex SQL before SqlSession::prepare, so they
        // must use the same session budgets as the application statement.
        let _budgets = self.sql.statement_budgets();
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
            let alter_target = Prepared::alter_table_authority_target(sql).map_err(|error| {
                // Match SqlSession::prepare's failed-statement transaction
                // semantics even though ALTER parsing precedes core binding.
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            if let Some((table, changes_trigger)) = alter_target {
                if changes_trigger && matches!(session.authority_role(), Role::Database { .. })
                    && session.service.admin.engine() == giggles_core::Engine::V2
                {
                    session.authorize_alter_table(&table).map_err(|error| match error {
                        CoreError::Policy(Error::Scope) => routine_ddl_error("42501", format!(
                            "must be owner of table {}", table.rsplit('\0').next().unwrap_or(&table),
                        )),
                        error => error,
                    })?;
                } else {
                    session.authorize_alter_table(&table)?;
                }
            }
            let drop_targets = Prepared::drop_table_targets(sql).map_err(|error| {
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })?;
            for table in drop_targets {
                session.authorize_drop_target(&table)?;
            }
            if let Some(index) = Prepared::drop_index_target(sql).map_err(|error| {
                session.sql.abort();
                parse_error(session, CoreError::Sql(error))
            })? {
                session.authorize_drop_schema(&index)?;
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
                        if sql.trim_start().to_ascii_lowercase().starts_with("drop") {
                            session.authorize_drop_relation(&sequence, true)?;
                        } else {
                            session.authorize_alter_relation(&sequence, true)?;
                        }
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
                let mut committed = session.service.admin.sql_session();
                session.sql.copy_statement_budgets_to(&mut committed);
                let p = declared(
                    committed.prepare(sql)
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
        self.authorize_sequence_dependencies(&Prepared::sequence_calls_from_sql(sql))
    }
    pub(crate) fn authorize_sequence_dependencies(
        &mut self, dependencies: &[relational::SequenceDependency],
    ) -> Result<(), CoreError> {
        if dependencies.is_empty() {
            return Ok(());
        }
        if self.sql.task().is_some() {
            // D7: sequences are never reachable from a task statement.
            return Err(CoreError::Task(giggles_core::task::TaskError::Denied(
                giggles_core::task::DenyRule::Sequence,
            )));
        }
        self.authority_scope(|session| session.authorize_sequences(dependencies))
    }
    fn authorize_create_sequence(&mut self) -> Result<(), CoreError> {
        let allowed = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
            let roles = self.service.admin.roles();
            let role = roles
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| self.authority_role_live(role))
                .ok_or(CoreError::Policy(Error::Authentication))?;
            role.superuser
                || self.schema_privilege_allowed("public", name, roles::SchemaPrivilege::Create)?
        } else {
            false
        };
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        Ok(())
    }
    fn authorize_sequences(&mut self, dependencies: &[relational::SequenceDependency]) -> Result<(), CoreError> {
        if !dependencies.is_empty() && !self.role.is_sql() && self.sql.task().is_none() {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        let sequences = self.sql.sequence_owners_shared()?;
        let image = match self.effective_role.as_ref().unwrap_or(&self.role) {
            Role::Database { .. } => Some(self.service.admin.roles().image()
                .map_err(|_| CoreError::Policy(Error::Authentication))?),
            _ => None,
        };
        let allowed_table = |table: &str| -> bool {
            match (self.effective_role.as_ref().unwrap_or(&self.role), &image) {
                (Role::Application { tables, .. }, _) => tables.contains(table),
                (Role::Database { name }, Some(image)) => {
                    self.sql
                        .table_authority_name(table)
                        .is_ok_and(|durable_name| {
                            self.sql
                                .table_owner_in(image, &durable_name)
                                .is_ok_and(|owner| owner.as_ref().is_some_and(|owner| self.service.admin.roles().privileges_of(name).is_ok_and(|roles| roles.contains(owner))))
                        })
                }
                _ => false,
            }
        };
        let allowed_standalone = |sequence: &str| -> bool {
            matches!(self.effective_role.as_ref().unwrap_or(&self.role), Role::Database { .. }) && allowed_table(sequence)
        };
        let superuser = if let (Role::Database { name }, Some(image)) = (self.effective_role.as_ref().unwrap_or(&self.role), &image) {
            image
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| self.authority_role_live(role))
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
        let allowed = dependencies.iter().all(|dependency| {
            use relational::SequenceDependency::{Dynamic, LastValue, Named};
            // lastval resolves a session observation before checking the
            // resolved sequence's authority in the callback.
            if superuser || matches!(dependency, LastValue) {
                return true;
            }
            if !live_context {
                return false;
            }
            // Dynamic names still require a live sequence context. Their
            // resolved sequence is checked at execution.
            let name = match dependency {
                Named(name) => name,
                Dynamic | LastValue => return true,
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
    fn authorize_drop_schema(&mut self, key: &str) -> Result<(), CoreError> {
        if matches!(self.role, Role::Database { .. }) && self.sql.task().is_none() {
            let key = self.sql.resolve_relation_reference(key)?;
            let schema = key.split_once('\0').map_or("public", |(schema, _)| schema);
            if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role)
                && !self.table_privilege_superuser()?
                && (is_builtin_catalog_schema(schema) || self.sql.schema_metadata()?.schemas.contains_key(schema))
                && !self.schema_privilege_allowed(schema, name, roles::SchemaPrivilege::Usage)?
            {
                self.sql.abort();
                return Err(CoreError::Policy(Error::Scope));
            }
        }
        Ok(())
    }
    fn authorize_drop_target(&mut self, table: &str) -> Result<(), CoreError> {
        self.authorize_drop_relation(table, false)
    }
    fn authorize_drop_relation(&mut self, table: &str, sequence: bool) -> Result<(), CoreError> {
        // Ordinary database roles get PostgreSQL's missing-object diagnostics.
        // Task and gateway admission still runs before object lookup.
        if matches!(self.role, Role::Database { .. }) && self.sql.task().is_none() {
            let key = self.sql.resolve_relation_reference(table)?;
            self.authorize_drop_schema(&key)?;
            if !Prepared::reserved_relation_name(&key) && !self.sql.relation_exists(&key)? { return Ok(()); }
        }
        self.authorize_alter_relation(table, sequence)
    }
    fn authorize_alter_table(&mut self, table: &str) -> Result<(), CoreError> {
        self.authorize_alter_relation(table, false)
    }
    fn authorize_alter_relation(&mut self, table: &str, sequence: bool) -> Result<(), CoreError> {
        let resolved = self.sql.resolve_relation_reference(table)?;
        let table = resolved.as_str();
        let allowed = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
            let roles = self.service.admin.roles();
            let role = roles
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| self.authority_role_live(role))
                .ok_or(CoreError::Policy(Error::Authentication))?;
            if role.superuser {
                true
            } else {
                // The durable key follows staged renames; missing names remain
                // ordinary absent ownership records and cannot leak 42P01.
                let name_at_commit = self.sql.table_authority_name(table)?;
                if sequence {
                    let schema = name_at_commit.split_once('\0').map_or("public", |(schema, _)| schema);
                    self.schema_privilege_allowed(schema, name, roles::SchemaPrivilege::Usage)?
                        && match self.sql.sequence_ddl_authority_key(table)? {
                            Some(key) => self.relation_privilege_allowed(
                                &self.sql.table_authority_name(&key)?, name, None,
                            )?,
                            None => false,
                        }
                } else {
                    self.table_privilege_allowed(&name_at_commit, name, None)?
                }
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
                let allowed = match session.effective_role.as_ref().unwrap_or(&session.role) {
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
                    Role::NoSql { .. } => false,
                #[cfg(feature = "legacy-task")]
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

    fn schema_privilege_allowed(&self, schema: &str, name: &str, privilege: roles::SchemaPrivilege) -> Result<bool, CoreError> {
        let catalog = self.service.admin.roles();
        let privileges = catalog.privileges_of(name).map_err(role_lookup_error)?;
        let capabilities = catalog.builtin_capabilities_of(name).map_err(role_lookup_error)?;
        let metadata = self.sql.schema_metadata()?;
        Ok((schema == "public" && privilege == roles::SchemaPrivilege::Usage) || privileges.iter().any(|name| metadata.schemas.get(schema).is_some_and(|(_, owner)| owner == name)
            || (privilege == roles::SchemaPrivilege::Usage && capabilities.contains(name) && matches!(name.as_str(), "pg_read_all_data" | "pg_write_all_data"))
            || catalog.schema_privileges(schema, name).is_ok_and(|p| p.contains(&privilege))))
    }
    fn table_privilege_allowed(
        &self,
        table: &str,
        name: &str,
        privilege: Option<roles::TablePrivilege>,
    ) -> Result<bool, CoreError> {
        let name_at_commit = self.sql.table_authority_name(table)?;
        let schema = name_at_commit.split_once('\0').map_or("public", |(schema, _)| schema);
        if !self.schema_privilege_allowed(schema, name, roles::SchemaPrivilege::Usage)? { return Ok(false); }
        self.relation_privilege_allowed(&name_at_commit, name, privilege)
    }
    // Stored view sources are already bound: schema name lookup is not repeated.
    fn relation_privilege_allowed(&self, name_at_commit: &str, name: &str, privilege: Option<roles::TablePrivilege>) -> Result<bool, CoreError> {
        let owner = self
            .sql
            .table_owner(name_at_commit)
            .map_err(|_| CoreError::Policy(Error::Scope))?;
        let catalog = self.service.admin.roles();
        let roles = catalog.privileges_of(name).map_err(role_lookup_error)?;
        let capabilities = catalog.builtin_capabilities_of(name).map_err(role_lookup_error)?;
        Ok(roles.iter().any(|name| owner.as_ref() == Some(name)
            || privilege.is_some_and(|privilege| {
                (capabilities.contains(name) && name == "pg_read_all_data" && privilege == roles::TablePrivilege::Select)
                || (capabilities.contains(name) && name == "pg_write_all_data" && matches!(privilege, roles::TablePrivilege::Insert | roles::TablePrivilege::Update | roles::TablePrivilege::Delete))
                || self.sql.table_privileges(name_at_commit, name).is_ok_and(|p| p.contains(&privilege))
            })))
    }
    fn view_sources_allowed(&self, table: &str, depth: usize) -> Result<bool, CoreError> {
        let Some(sources) = self.sql.view_authorization_sources(table)? else { return Ok(true) };
        if depth >= 32 { return Ok(false); }
        let key = self.sql.table_authority_name(table)?;
        let owner = self.sql.table_owner(&key).map_err(role_lookup_error)?;
        // Core/API and legacy views have no recorded definer; retain invoker ACLs.
        let ownerless = owner.is_none();
        let owner = match owner {
            Some(owner) => owner,
            None => match self.effective_role.as_ref().unwrap_or(&self.role) {
                Role::Database { name } => name.clone(),
                _ => return Ok(false),
            },
        };
        let catalog = self.service.admin.roles();
        let Some(role) = catalog.get(&owner).map_err(role_lookup_error)? else { return Ok(false) };
        for source in sources {
            let allowed = role.superuser || if ownerless {
                self.table_privilege_allowed(&source, &owner, Some(roles::TablePrivilege::Select))?
            } else {
                self.relation_privilege_allowed(&self.sql.table_authority_name(&source)?, &owner, Some(roles::TablePrivilege::Select))?
            };
            if !allowed {
                return Ok(false);
            }
            if !self.view_sources_allowed(&source, depth + 1)? { return Ok(false); }
        }
        Ok(true)
    }
    pub(crate) fn table_privilege_superuser(&self) -> Result<bool, CoreError> {
        let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) else {
            return Ok(false);
        };
        self.service
            .admin
            .roles()
            .image_for_role(name)
            .map_err(role_lookup_error)?
            .with_role(name, |current| {
                current
                    .filter(|role| self.authority_role_live(role))
                    .map(|role| role.superuser)
            })
            .map_err(role_lookup_error)?
            .ok_or(CoreError::Policy(Error::Authentication))
    }
    fn table_requirements_allowed(
        &self,
        reqs: &[(String, roles::TablePrivilege)],
        superuser: bool,
        check_views: bool,
    ) -> Result<bool, CoreError> {
        let acting_role = self.effective_role.as_ref().unwrap_or(&self.role);
        if matches!(acting_role, Role::NoSql { .. }) {
            return Ok(false);
        }
        #[cfg(feature = "legacy-task")]
        if matches!(acting_role, Role::Task { .. }) {
            return Ok(false);
        }
        for (table, privilege) in reqs {
            let allowed = match self.effective_role.as_ref().unwrap_or(&self.role) {
                Role::Application { tables, .. } => {
                    table != "giggles_join" && tables.contains(table)
                }
                Role::Database { name } => {
                    (superuser || self.table_privilege_allowed(table, name, Some(*privilege))?)
                        && (!check_views || *privilege != roles::TablePrivilege::Select || self.view_sources_allowed(table, 0)?)
                }
                Role::NoSql { .. } => false,
                #[cfg(feature = "legacy-task")]
                Role::Task { .. } => false,
            };
            if !allowed {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub(crate) fn can_read_duplicate_key(
        &mut self,
        detail: &giggles_core::relational::DuplicateKeyDetail,
    ) -> bool {
        let table = giggles_core::relational::relation_key(&detail.schema, &detail.table);
        self.authority_scope(|session| {
            let Some(authority) = session.sql.table_authority_snapshot(&table, detail.table_id)
                .map_err(role_lookup_error)? else { return Ok(false) };
            match session.authority_role() {
                Role::Database { name } => session.duplicate_key_select_allowed(name, &authority),
                Role::Application { tables, .. } => Ok(table != "giggles_join" && tables.contains(&table)),
                _ => Ok(false),
            }
        }).unwrap_or(false)
    }

    pub(crate) fn can_show_written_check_row(
        &mut self,
        detail: &giggles_core::relational::DuplicateKeyDetail,
    ) -> bool {
        let table = giggles_core::relational::relation_key(&detail.schema, &detail.table);
        self.authority_scope(|session| {
            let Role::Database { name } = session.authority_role() else { return Ok(false) };
            let Some(authority) = session.sql.table_authority_snapshot(&table, detail.table_id)
                .map_err(role_lookup_error)? else { return Ok(false) };
            authority.roles.with_role(name, |role| role.is_some_and(|role| session.authority_role_live(role)))
                .map_err(role_lookup_error)
        }).unwrap_or(false)
    }

    fn duplicate_key_select_allowed(
        &self,
        name: &str,
        authority: &giggles_core::TableAuthoritySnapshot,
    ) -> Result<bool, CoreError> {
        use roles::{SchemaPrivilege::Usage, TablePrivilege::Select};

        let roles = &authority.roles;
        let superuser = roles.with_role(name, |role| role
            .filter(|role| self.authority_role_live(role))
            .map(|role| role.superuser))
            .map_err(role_lookup_error)?
            .ok_or(CoreError::Policy(Error::Authentication))?;
        if superuser {
            return Ok(true);
        }
        let privileges = roles.privileges_of(name).map_err(role_lookup_error)?;
        let capabilities = roles.builtin_capabilities_of(name).map_err(role_lookup_error)?;
        let schema_usage = authority.schema == "public" || privileges.iter().any(|name| {
            authority.schema_owner.as_ref() == Some(name)
                || (capabilities.contains(name) && matches!(name.as_str(), "pg_read_all_data" | "pg_write_all_data"))
                || roles.schema_privileges(&authority.schema, name).is_ok_and(|grants| grants.contains(&Usage))
        });
        Ok(schema_usage && (authority.grants.get("").is_some_and(|grants| grants.contains(&Select))
            || privileges.iter().any(|name| {
                authority.owner.as_ref() == Some(name)
                    || (name == "pg_read_all_data" && capabilities.contains(name))
                    || authority.grants.get(name).is_some_and(|grants| grants.contains(&Select))
            })))
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
            if !session.table_requirements_allowed(reqs, superuser, true)? {
                session.sql.abort();
                return Err(CoreError::Policy(Error::Scope));
            }
            Ok(())
        })
    }
    pub(crate) fn authorize_native_sequence(&mut self, name: &str) -> Result<(), CoreError> {
        self.authority_scope(|session| session.authorize_sequences(&[relational::SequenceDependency::Named(name.to_owned())]))
    }
    pub(crate) fn authorize_native_catalog(&mut self, request: &giggles_core::operations::CatalogRequest) -> Result<(), CoreError> {
        use giggles_core::operations::CatalogRequest as C;
        // Same rules as SQL CREATE SEQUENCE / DROP SEQUENCE in `authorize`.
        match request {
            C::CreateSequence { .. } => return self.authorize_create_sequence(),
            C::DropSequence { name, .. } => {
                let key = self.sql.native_relation_reference(name)?;
                return self.authorize_drop_relation(&key, true);
            }
            C::DropView { name, .. } => {
                let key = self.sql.native_relation_reference(name)?;
                return self.authorize_drop_target(&key);
            }
            C::DropTable { table, .. } => {
                let key = self.sql.native_relation_reference(table)?;
                return self.authorize_drop_target(&key);
            }
            C::DropIndex { name, .. } if matches!(self.role, Role::Database { .. }) && self.sql.task().is_none() => {
                // Schema USAGE precedes index existence, just as in SQL.
                let key = self.sql.native_relation_reference(name)?;
                self.authorize_drop_schema(&key)?;
                if let Some(table) = self.sql.native_index_table(name)? {
                    return self.authorize_alter_table(&table);
                }
                return Ok(());
            }
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
            let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) else { unreachable!() };
            let table = match request {
                C::DropTable { table, .. } | C::AlterTable { table, .. } | C::DropView { name: table, .. } => Some(self.sql.native_relation_reference(table)?),
                C::DropIndex { name, .. } => self.sql.native_index_table(name)?,
                _ => None,
            };
            if let Some(table) = table {
                let key = self.sql.table_authority_name(&table)?;
                self.table_privilege_allowed(&key, name, None)?
            } else {
                let schema = match request {
                    C::CreateTable { table, .. } | C::CreateView { name: table, .. } => self.sql.native_creation_schema(table)?,
                    C::CreateIndex { table, .. } => self.sql.relation_schema(&self.sql.native_relation_reference(table)?)?,
                    _ => self.sql.relation_schema("")?,
                };
                self.schema_privilege_allowed(&schema, name, roles::SchemaPrivilege::Create)?
            }
        };
        if !allowed { self.sql.abort(); return Err(CoreError::Policy(Error::Scope)); }
        Ok(())
    }
    fn authorization_context(&self) -> Option<AuthorizationContext> {
        // Only ordinary PostgreSQL database roles. Fingerprint and task
        // identities can depend on authority outside the SQL role catalog.
        if self.surface != Surface::Postgres || self.fingerprint_bound || self.agent
            || !matches!(self.role, Role::Database { .. })
        {
            return None;
        }
        let (latest_catalog, statement_catalog, schema_revision, stamp) =
            self.sql.authorization_catalog_key(&self.service.admin)?;
        // Read after the catalog observation so a published password change
        // also makes the preceding invalidation visible to this reader.
        let password_revision = self.service.password_generation_revision.load(Ordering::Acquire);
        if password_revision & 1 != 0 { return None; }
        Some(AuthorizationContext { latest_catalog, statement_catalog, stamp, schema_revision, password_revision })
    }

    fn authorize(&mut self, p: &Prepared) -> Result<(), CoreError> {
        self.authorize_for_execution(p).map(drop)
    }

    fn authorize_for_execution(&mut self, p: &Prepared) -> Result<Option<AuthorizationContext>, CoreError> {
        self.admit_backstop(p.task_class())?;
        let control = p.implicit_authorization_block();
        let slot = control.map_or(0, |commit| 1 + usize::from(commit));
        // A miss (including an ineligible plan or a preflight error) discards
        // execution-time grants even when the old authorization entry survives.
        let grants = self.authorization_memos[slot].as_mut().and_then(|memo| memo.sequence_defaults.take());
        let context = if p.has_authorization_binding() { self.authorization_context() } else { None };
        #[cfg(all(test, feature = "engine-rocksdb"))]
        if let Some(observe) = AUTHORIZATION_CONTEXT_OBSERVED.with_borrow_mut(Option::take) {
            observe(context.is_some());
        }
        if let Some(context) = context
            && let Some(memo) = &self.authorization_memos[slot]
            && memo.context == context
            && memo.role == self.role
            && memo.effective_role.eq(&self.effective_role)
            && memo.effective_session.eq(&self.effective_session)
            && memo.epoch == self.epoch
            && memo.role_fence == self.role_fence
            && (control.is_some() || memo.plan.as_ref().is_some_and(|key| key.matches(p, context.statement_catalog)))
            && self.sql.authorization_memo_preflight(p)?
            && self.authorization_context() == Some(context)
        {
            self.sql.set_sequence_tables(memo.sequence_tables.clone());
            self.authorization_memos[slot].as_mut().expect("authorization hit").sequence_defaults = grants;
            #[cfg(test)]
            { self.authorization_hits += 1; }
            return Ok(control.is_none().then_some(context));
        }

        #[cfg(test)]
        { self.authorization_full_runs += 1; }
        self.authorize_full(p)?;
        if let Some(context) = context
            && self.authorization_context() == Some(context)
        {
            let plan = p.authorization_plan_key(context.statement_catalog);
            if control.is_none() && plan.is_none() { return Ok(None); }
            self.authorization_memos[slot] = Some(AuthorizationMemo {
                context,
                role: self.role.clone(),
                effective_role: self.effective_role.clone(),
                effective_session: self.effective_session.clone(),
                epoch: self.epoch,
                role_fence: self.role_fence,
                plan,
                sequence_tables: self.sql.sequence_tables().clone(),
                sequence_defaults: None,
            });
            return Ok(control.is_none().then_some(context));
        }
        Ok(None)
    }

    fn authorize_full(&mut self, p: &Prepared) -> Result<(), CoreError> {
        if p.is_admin_utility() {
            return if matches!(self.authority_role(), Role::Database { .. }) {
                Ok(())
            } else {
                Err(CoreError::Policy(Error::Scope))
            };
        }
        if matches!(p.command(), "CREATE FUNCTION" | "DROP FUNCTION" | "CREATE TRIGGER" | "DROP TRIGGER" | "ALTER TRIGGER") {
            let result = self.authorize_routine_ddl(p);
            if result.is_err() { self.sql.abort(); }
            return result.map(drop);
        }
        if p.is_collation_ddl() {
            let allowed = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
                let roles = self.service.admin.roles();
                let role = roles.get(name).map_err(|_| CoreError::Policy(Error::Authentication))?
                    .filter(|role| self.authority_role_live(role))
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
        if let Some((create, names)) = p.extension_ddl() {
            let allowed = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
                let role = self.service.admin.roles().get(name).map_err(role_lookup_error)?
                    .filter(|role| self.authority_role_live(role))
                    .ok_or(CoreError::Policy(Error::Authentication))?;
                p.set_extension_owner(role.oid);
                role.superuser || (!create && names.iter().all(|n| self.sql.user_type_catalog().ok()
                    .and_then(|m| m.extensions.get(n).map(|e| e.owner == role.oid)).unwrap_or(true)))
            } else { false };
            if !allowed { self.sql.abort(); return Err(CoreError::Policy(Error::Scope)); }
            return Ok(());
        }
        if let Some((schema, owner, create)) = p.schema_ddl() {
            let allowed = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
                let roles = self.service.admin.roles();
                let role = roles
                    .get(name)
                    .map_err(role_lookup_error)?
                    .filter(|role| self.authority_role_live(role))
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
                    if p.command() == "DROP SEQUENCE" {
                        self.authorize_drop_relation(sequence, true)?;
                    } else {
                        self.authorize_alter_relation(sequence, true)?;
                    }
                }
            }
            if let Some(table) = p.sequence_owned_by_table() {
                self.authorize_alter_table(table)?;
            }
            return Ok(());
        }
        self.authorize_sequences(&p.sequence_calls())?;
        if p.sequence_only() {
            return Ok(());
        }
        // E939: collect every child dependency before table-free/routine classification.
        // Routine/sequence authority and classification keep their existing gates.
        let read_tables = p.authorization_tables().map_err(CoreError::Sql)?;
        let invoker_tables = p.invoker_authorization_tables().map_err(CoreError::Sql)?;
        let check_views = p.authorization_has_views();
        let read_tables = if matches!(self.role, Role::Database { .. }) && self.sql.task().is_none() {
            invoker_tables
        } else { read_tables };
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
                    false, check_views,
                )?,
                None => true,
            }
        } else {
            false
        };
        let mut superuser = false;
        let allowed = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
            if self.table_privilege_superuser()? {
                superuser = true;
                true
            } else if matches!(p.command(), "COMMENT" | "SECURITY LABEL") || p.is_alter_table()
                || (p.is_view_replacement() && self.sql.relation_exists(p.table().unwrap_or(""))?)
                || p.is_drop_table()
                || (matches!(p.command(), "DROP INDEX" | "ALTER INDEX") && p.table().is_some())
            {
                let targets = if p.is_drop_table() { p.drop_tables().iter().map(String::as_str).collect() }
                    else { vec![p.table().ok_or(CoreError::Policy(Error::Scope))?] };
                let mut owns_all = true;
                for table in targets {
                    if p.is_drop_table() && !self.sql.relation_exists(table)? { continue; }
                    let name_at_commit = self.sql.table_authority_name(table)?;
                    owns_all &= self.sql.table_owner(&name_at_commit)
                        .map_err(|_| CoreError::Policy(Error::Scope))?.as_ref().is_some_and(|owner| self.service.admin.roles().privileges_of(name).is_ok_and(|roles| roles.contains(owner)));
                }
                owns_all
            } else if matches!(p.command(), "DROP INDEX" | "ALTER INDEX") && p.table().is_none() {
                true
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
                    || self.schema_privilege_allowed(&schema, name, roles::SchemaPrivilege::Create)?
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
                        self.table_requirements_allowed(&[(table.to_owned(), privilege)], false, check_views)?
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
        if allowed && p.command() != "CREATE VIEW" {
            let mut reqs: Vec<_> = read_tables.into_iter()
                .map(|table| (table, roles::TablePrivilege::Select))
                .collect();
            for table in p.row_lock_tables() {
                reqs.push((table, roles::TablePrivilege::Update));
            }
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
            allowed = self.table_requirements_allowed(&reqs, superuser, check_views)?;
        }
        if !allowed {
            let mut error = CoreError::Policy(Error::Scope);
            if self.surface == Surface::Postgres
                && matches!(self.authority_role(), Role::Database { .. })
                && self.sql.task().is_none()
                && !p.is_multi_source_read()
                && p.has_trigger_call()
                && let Some(table) = p.table()
            {
                let (_, relation) = table.split_once('\0').unwrap_or(("public", table));
                error = routine_ddl_error("42501", format!("permission denied for table {relation}"));
            }
            for table in p.row_lock_tables() {
                if !self.table_requirements_allowed(&[(table.clone(), roles::TablePrivilege::Select),
                    (table.clone(), roles::TablePrivilege::Update)], false, check_views)? {
                    error = giggles_core::relational::row_lock_permission_error(&table);
                    break;
                }
            }
            self.sql.abort();
            return Err(error);
        }
        if p.command() == "TRUNCATE TABLE" && !superuser {
            for table in self.sql.truncate_tables(p)? {
                let permitted = match self.effective_role.as_ref().unwrap_or(&self.role) {
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
        let implicit = self.sql.implicit_sequence_dependencies(p)?
            .into_iter()
            .map(|reference| reference.map_or(
                relational::SequenceDependency::Dynamic,
                relational::SequenceDependency::Named,
            ))
            .collect::<Vec<_>>();
        self.authorize_sequences(&implicit)?;
        Ok(())
    }
    fn authorize_routine_ddl(&mut self, p: &Prepared) -> Result<Option<Prepared>, CoreError> {
        let Role::Database { name } = self.authority_role() else { return Err(CoreError::Policy(Error::Scope)); };
        let name = name.clone();
        let roles = self.service.admin.roles();
        let role = roles.get(&name).map_err(role_lookup_error)?
            .filter(|role| self.authority_role_live(role)).ok_or(CoreError::Policy(Error::Authentication))?;
        match p.command() {
            "CREATE FUNCTION" | "DROP FUNCTION" => {
                p.set_extension_owner(role.oid);
                if role.superuser { return Ok(None); }
                let schema = if p.command() == "CREATE FUNCTION" {
                    let schema = self.sql.creation_schema(p)?;
                    if !self.schema_privilege_allowed(&schema, &name, roles::SchemaPrivilege::Create)? {
                        return Err(routine_ddl_error("42501", format!("permission denied for schema {schema}")));
                    }
                    if p.creates_leakproof_routine() { return Err(routine_ddl_error("42501", "only superuser can define a leakproof function".into())); }
                    Some(schema)
                } else { None };
                let metadata = self.sql.catalog_metadata()?;
                for (routine, oid) in p.routine_owners(&metadata.presentation, schema.as_deref()) {
                    let allowed = roles.privileges_of(&name).map_err(role_lookup_error)?.iter()
                        .any(|owner| roles.get(owner).ok().flatten().is_some_and(|role| role.oid == oid));
                    if !allowed { return Err(routine_ddl_error("42501", format!("must be owner of function {routine}"))); }
                }
            }
            command => {
                if role.superuser { return Ok(None); }
                let table = p.table().ok_or(CoreError::Policy(Error::Scope))?;
                let table = self.sql.resolve_relation_reference(table)?;
                self.authorize_drop_schema(&table)?;
                let (schema, relation) = table.split_once('\0').unwrap_or(("public", &table));
                if p.is_drop_trigger() {
                    let metadata = self.sql.catalog_metadata()?;
                    let target = metadata.tables.iter().find(|target| {
                        target.name == relation
                            && metadata.schemas.tables.get(&target.id).map_or("public", String::as_str) == schema
                            && !metadata.presentation.views.contains_key(&target.id)
                    });
                    let allowed = if let Some(target) = target
                        && self.table_privilege_allowed(&table, &name, None)?
                    {
                        Some(target.id)
                    } else {
                        None
                    };
                    let mut restricted = p.clone();
                    restricted.restrict_drop_trigger_to_table(allowed);
                    return Ok(Some(restricted));
                }
                if !self.sql.relation_exists(&table)? { return Ok(None); }
                let privilege = (command == "CREATE TRIGGER").then_some(roles::TablePrivilege::Trigger);
                if !self.table_privilege_allowed(&table, &name, privilege)? {
                    let message = match command {
                        "ALTER TRIGGER" => format!("must be owner of table {relation}"),
                        _ => format!("permission denied for table {relation}"),
                    };
                    return Err(routine_ddl_error("42501", message));
                }
            }
        }
        Ok(None)
    }
    fn authorize_copy_sql(&mut self, sql: &str) -> Result<(), CoreError> {
        self.admit_backstop(giggles_core::task::StatementClass::Copy)?;
        if !self.role.is_sql() {
            self.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        let unrestricted = if let Role::Database { name } = self.effective_role.as_ref().unwrap_or(&self.role) {
            self.service
                .admin
                .roles()
                .get(name)
                .map_err(role_lookup_error)?
                .filter(|role| self.authority_role_live(role))
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
        let allowed = match self.effective_role.as_ref().unwrap_or(&self.role) {
            Role::Application { tables, .. } => tables.contains(&target),
            Role::Database { name } => unrestricted || self.table_privilege_allowed(&target, name, Some(roles::TablePrivilege::Insert))?,
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
    pub(crate) fn normalize_copy_cells(
        &mut self,
        prepared: &Prepared,
        input: giggles_core::relational::CopyInput,
        clock: &StatementClock,
    ) -> Result<(Prepared, Vec<Vec<Value>>, u64), CoreError> {
        self.authority_scope(|session| {
            session.reconcile_sequence_owners()?;
            session.authorize(prepared)?;
            session.sql.normalize_copy_cells_at(prepared, input, clock)
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
        let result = self.sql.fetch_cursor_unreleased(cursor, max_rows, max_bytes);
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
    /// Share pgwire settings admission and log refusals before API SQL executes.
    pub(crate) fn admit_api_sql_settings(&mut self, sql: &str, bindings: usize) -> Result<(), CoreError> {
        if self.task().is_none() {
            return Ok(());
        }
        if let Some((class, error)) = crate::pgwire::api_settings_refusal(sql, self) {
            self.sql.log_refused_task_statement(sql, class, bindings, &error);
            return Err(error);
        }
        Ok(())
    }
    /// SQL text over the API (`Request::Sql`), held until `release_page`. In
    /// a task session the statement is prepared and admitted first, and a
    /// refusal there, which never reaches core, is logged once; the read-log
    /// text is set before the statement runs (GD-0061 R3-313).
    fn execute_api_sql(
        &mut self,
        sql: &str,
        params: &[Value],
        check_reply: Option<&mut ApiSqlReplyCheck<'_>>,
    ) -> Result<(Unreleased<SqlResult>, Vec<u32>), CoreError> {
        if self.sql.task().is_none() {
            return self.authority_scope(|session| {
                session.execute_sql_authorized_typed(sql, params, false, check_reply)
            });
        }
        self.authority_scope(|session| {
            let refused = |session: &Session, class, error: CoreError| {
                session
                    .sql
                    .log_refused_task_statement(sql, class, params.len(), &error);
                Err(error)
            };
            session.admit_api_sql_settings(sql, params.len())?;
            // Server task DDL must be refused before core's SQL parser, as
            // on pgwire; CREATE TASK is not core SQL grammar.
            if crate::task_ddl::parse(sql).is_some() {
                let class = giggles_core::task::StatementClass::RoleStatement;
                if let Err(error) = session.admit_backstop(class) {
                    return refused(session, class, error);
                }
            }
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
            let types = crate::pgwire::result_type_oids(&session.describe(&p)?);
            let result = session.run_prepared(&p, params, false, None)?;
            Ok((result, types))
        })
    }
    /// Validity only (revoked, expired, definition changed), for re-serving
    /// rows of a page that was already released and charged. No-op outside a
    /// task session.
    pub fn still_valid(&self) -> Result<(), CoreError> {
        self.sql.still_valid()
    }
    /// Recheck fresh application-Parse authority without parsing or describing.
    /// Caller proves this is an unchanged SELECT, INSERT, UPDATE, or DELETE eligible for unnamed Parse reuse.
    pub(crate) fn reauthorize_unnamed_parse(&mut self, _sql: &str, dependencies: &[relational::SequenceDependency], p: &Prepared) -> Result<(), CoreError> {
        #[cfg(test)]
        if _sql.contains("r3_parse_source") && crate::pgwire::UNNAMED_REAUTHORIZE_FAIL.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(CoreError::Policy(Error::Scope));
        }
        // Match fresh Parse's budget scope for authorization of stored defaults.
        let _budgets = self.sql.statement_budgets();
        self.authority_scope(|session| {
            session.reconcile_sequence_owners()?;
            session.authorize_sequence_dependencies(dependencies)?;
            session.authorize(p)
        })
    }
    /// Same dependency and sequence authorization as fresh prepare/Execute.
    /// Called inside authority_scope before any portal dependency expansion/lock.
    pub(crate) fn authorize_prepared_bind(&mut self, p: &Prepared) -> Result<Prepared, CoreError> {
        let bound = self.sql.bind_relation_context(p)?;
        self.reconcile_sequence_owners()?;
        self.authorize(&bound)?;
        Ok(bound)
    }
    pub(crate) fn bind_prepared_relations(&mut self, p: &Prepared) -> Result<(), CoreError> {
        self.authority_scope(|session| {
            let mut retries = 0u32;
            loop {
                let observation = session.sql.prepared_bind_observation()?;
                // A cancelled wait must still recheck authority after committed
                // drift. Mask cancellation only for this bounded metadata check.
                let bound = if retries == 0 {
                    session.authorize_prepared_bind(p)?
                } else {
                    giggles_core::cancellation::Cancellation::default()
                        .scope(|| session.authorize_prepared_bind(p))?
                };
                // Drift can return before any lock wait checks cancellation.
                // Check the original request, after re-authorization, on every
                // retry so catalog churn cannot hide cancellation or a deadline.
                giggles_core::cancellation::check()?;
                if bound.is_ddl() || session.sql.lock_prepared_relations(&bound, observation)? { return Ok(()); }
                // Unrelated committed DDL is not a stale plan error. Retry until
                // stable, yielding first and then backing off to at most 8 ms.
                retries = (retries + 1).min(7);
                if retries <= 3 {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(1 << (retries - 4)));
                }
            }
        })
    }
    pub fn open_cursor(&mut self, p: &Prepared, params: &[Value]) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, true)
    }

    pub(crate) fn open_uncached_cursor(&mut self, p: &Prepared, params: &[Value]) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, false)
    }

    fn open_cursor_inner(
        &mut self,
        p: &Prepared,
        params: &[Value],
        use_plan_cache: bool,
    ) -> Result<Cursor, CoreError> {
        let open = if use_plan_cache {
            SqlSession::open_cursor_at
        } else {
            SqlSession::open_uncached_cursor_at
        };
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
            let result = if bound.has_privilege_inquiry() {
                let callback = crate::pg_catalog_wire::privilege_callback(session);
                session.sql.with_privilege_callback(callback, |sql| open(sql, &bound, params, &clock))
            } else {
                open(&mut session.sql, &bound, params, &clock)
            };
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            } else if clock.unavailable.is_none() {
                session.transaction_us = Some(transaction_us);
            }
            result
        })
    }
    /// Copy effective pgwire settings at statement execution.
    pub(crate) fn sync_execution_settings(
        &mut self,
        settings: &crate::pg_settings::SessionSettings,
    ) -> Result<(), CoreError> {
        self.sql.set_check_function_bodies(settings.check_function_bodies());
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
        self.run_prepared(p, params, true, None)?.untasked()
    }
    /// The plan facts of an `EXPLAIN` read the server prepared itself: the
    /// first value of the unreleased result, taken with `untasked`. A task
    /// session is refused before the statement runs, as by the plain exit,
    /// and no core row leaves this function (GD-0061 R3-314).
    pub(crate) fn explain_facts(&mut self, p: &Prepared) -> Result<Option<Value>, CoreError> {
        let result = self.run_prepared(p, &[], true, None)?.untasked()?;
        Ok(result.rows.into_iter().next().and_then(|row| row.values.into_iter().next()))
    }
    /// Inspect a mutation's access path under the mutation's own privileges.
    /// The synthetic SELECT is never a user read and releases only plan facts.
    pub(crate) fn explain_mutation_facts(
        &mut self, authority: &Prepared, sql: &str,
    ) -> Result<(String, Option<Value>), CoreError> {
        self.authority_scope(|session| {
            session.authorize(authority)?;
            if session.sql.task().is_some() { return Err(task_session_refusal()); }
            let p = session.sql.prepare(sql)?;
            if p.command() != "EXPLAIN" { return Err(CoreError::Sql(relational::SqlError::Unsupported)); }
            let table = p.table().ok_or(CoreError::Policy(Error::Scope))?.to_owned();
            let result = session.sql.execute_prepared_unreleased(&p, &[], None)?.untasked()?;
            Ok((table, result.rows.into_iter().next().and_then(|r| r.values.into_iter().next())))
        })
    }
    /// `execute_prepared`, with the result held until `release_page`.
    pub fn execute_prepared_unreleased(
        &mut self,
        p: &Prepared,
        params: &[Value],
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.run_prepared(p, params, false, None)
    }
    pub(crate) fn execute_prepared_checked_unreleased(
        &mut self,
        p: &Prepared,
        check_reply: &mut giggles_core::SqlReplyCheck<'_>,
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.run_prepared(p, &[], false, Some(check_reply))
    }
    /// The body of both prepared exits. `plain` refuses a task session where
    /// the statement would reach core, after the same checks as before.
    fn run_prepared(
        &mut self,
        p: &Prepared,
        params: &[Value],
        plain: bool,
        check_reply: Option<&mut giggles_core::SqlReplyCheck<'_>>,
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.clear_native_duplicate_key_detail();
        let result = self.authority_scope(|session| {
            let mut bound = session.sql.bind_relation_context(p)?;
            session.reconcile_sequence_owners()?;
            let authorization = if bound.is_drop_trigger() {
                session.admit_backstop(bound.task_class())?;
                if let Some(restricted) = session.authorize_routine_ddl(&bound).inspect_err(|_| session.sql.abort())? {
                    bound = restricted;
                }
                None
            } else {
                session.authorize_for_execution(&bound)?
            };
            let p = &bound;
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
                    let result = if p.has_privilege_inquiry() {
                        let callback = crate::pg_catalog_wire::privilege_callback(session);
                        session.sql.with_privilege_callback(callback, |sql| {
                            sql.execute_prepared_at_checked_unreleased(p, params, None, &clock, check_reply)
                        })
                    } else if let Some(context) = authorization.filter(|_| p.authorization_may_parse_defaults()) {
                        session.execute_with_sequence_defaults(p, params, &clock, context, check_reply)
                    } else {
                        session.sql.execute_prepared_at_checked_unreleased(p, params, None, &clock, check_reply)
                    };
                    if !session.sql.in_transaction() { session.transaction_us = None; }
                    else if clock.unavailable.is_none() { session.transaction_us = Some(transaction_us); }
                    result.map(|result| (result, true))
                })
        });
        if self.surface == Surface::Native {
            relational::retain_sql_duplicate_key_detail(|detail|
                matches!(result, Err(CoreError::Sql(
                    relational::SqlError::Duplicate | relational::SqlError::CheckViolation,
                ))) && self.can_read_duplicate_key(detail));
        }
        result
    }
    fn clear_native_duplicate_key_detail(&self) {
        if self.surface == Surface::Native {
            relational::retain_sql_duplicate_key_detail(|_| false);
        }
    }
    #[inline(never)]
    fn execute_with_sequence_defaults(
        &mut self,
        p: &Prepared,
        params: &[Value],
        clock: &StatementClock,
        context: AuthorizationContext,
        check_reply: Option<&mut giggles_core::SqlReplyCheck<'_>>,
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        let memo = self.authorization_memos[0].as_mut().expect("authorized statement");
        let previous = memo.sequence_defaults.take();
        #[cfg(all(test, feature = "engine-rocksdb"))]
        if let Some(observe) = BEFORE_SEQUENCE_DEFAULT_EXECUTION.with_borrow_mut(Option::take) {
            observe(previous.is_some());
        }
        let (result, grants) = self.sql.with_sequence_default_memo(
            giggles_core::SequenceDefaultContext {
                stamp: context.stamp,
                latest_catalog: context.latest_catalog,
                statement_catalog: context.statement_catalog,
            },
            previous,
            |sql| sql.execute_prepared_at_checked_unreleased(p, params, None, clock, check_reply),
        );
        if grants.is_some()
            && self.authorization_context() == Some(context)
            && let Some(memo) = &mut self.authorization_memos[0]
            && memo.context == context
            && memo.role == self.role
            && memo.effective_role.eq(&self.effective_role)
            && memo.effective_session.eq(&self.effective_session)
            && memo.epoch == self.epoch
            && memo.role_fence == self.role_fence
        {
            // This execution holds the session exclusively and cannot replace
            // its S2 entry. In particular, it ran the plan just authorized above.
            memo.sequence_defaults = grants;
        }
        result
    }

    fn execute_with_ddl_authority<T>(
        &mut self, command: &str, table: Option<&str>, ddl: bool, sequence_ddl: bool,
        sequences: Vec<String>, execute: impl FnOnce(&mut Self) -> Result<(T, bool), CoreError>,
    ) -> Result<T, CoreError> {
        if ddl && self.sql.in_transaction() { self.implicit_ddl = true; }
        let transactional = self.sql.set_ddl_owner(match self.effective_role.as_ref().unwrap_or(&self.role) { Role::Database { name } => Some(name), _ => None });
        let candidates = if ddl && !transactional { self.sql.sequence_owners()?.into_iter().map(|(name, _)| name).collect::<Vec<_>>() } else { vec![] };
        let preexisting = command == "CREATE TABLE" && table.map(|t| self.sql.relation_exists(t)).transpose()?.unwrap_or(false);
        // Native errors stay in the result envelope; failed execution must skip bookkeeping.
        let (result, succeeded) = execute(self)?;
        if transactional || !succeeded { return Ok(result); }
        self.sequence_owner_candidates.extend(candidates.iter().cloned());
        self.sequence_owner_candidates.extend(sequences);
        let roles = self.service.admin.roles();
        if command == "CREATE TABLE" && !preexisting && let Some(table) = table {
            roles.forget_table(table).map_err(|_| CoreError::Policy(Error::Scope))?;
            if let Role::Database { name: owner } = self.effective_role.as_ref().unwrap_or(&self.role) {
                roles.set_table_owner(table, owner).map_err(|_| CoreError::Policy(Error::Scope))?;
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
            Ok((result, succeeded))
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
            |session| { let result = session.sql.execute_catalog_native(request); let succeeded = result.is_ok(); Ok((result, succeeded)) })
    }
    /// Plain exit, for tests only: a task session is refused before the
    /// statement runs (GD-0061 R3-310). Server sources never call it
    /// (`clippy.toml`, `tests/release_seal.rs`, GD-0061 R3-314); they use
    /// `execute_sql_unreleased` and `release_page`, or `run_control`. It
    /// stays public for the integration tests.
    pub fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<SqlResult, CoreError> {
        self.clear_native_duplicate_key_detail();
        self.authority_scope(|session| session.execute_sql_authorized(sql, params, true, None))?
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
        self.authority_scope(|session| session.execute_sql_authorized(sql, &[], true, None))?
            .untasked()
            .map(drop)
    }
    /// `execute_sql`, with the result held until `release_page`.
    pub fn execute_sql_unreleased(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        self.clear_native_duplicate_key_detail();
        self.authority_scope(|session| session.execute_sql_authorized(sql, params, false, None))
    }
    fn execute_sql_authorized(
        &mut self,
        sql: &str,
        params: &[Value],
        plain: bool,
        check_reply: Option<&mut giggles_core::SqlReplyCheck<'_>>,
    ) -> Result<Unreleased<SqlResult>, CoreError> {
        let checking = check_reply.is_some();
        let mut check_reply = check_reply;
        let mut check = |result: Released<&SqlResult>, spill: Option<&mut relational::spill::SpilledRows>, _types: &[u32]| {
            if let Some(check) = &mut check_reply { check(result, spill) } else { Ok(()) }
        };
        self.execute_sql_authorized_typed(sql, params, plain, checking.then_some(&mut check)).map(|(result, _)| result)
    }
    fn execute_sql_authorized_typed(
        &mut self,
        sql: &str,
        params: &[Value],
        plain: bool,
        check_reply: Option<&mut ApiSqlReplyCheck<'_>>,
    ) -> Result<(Unreleased<SqlResult>, Vec<u32>), CoreError> {
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
            return result.map(|result| (result, Vec::new()));
        }
        let p = self.prepare(sql)?;
        let types = crate::pgwire::result_type_oids(&self.describe(&p)?);
        let checking = check_reply.is_some();
        let mut check_reply = check_reply;
        let mut check = |result: Released<&SqlResult>, spill: Option<&mut relational::spill::SpilledRows>| {
            if let Some(check) = &mut check_reply { check(result, spill, &types) } else { Ok(()) }
        };
        let result = self.run_prepared(&p, params, plain, checking.then_some(&mut check));
        self.api_path.sql_committed = result.is_ok() && p.command() == "COMMIT";
        result.map(|result| (result, types))
    }
    #[cfg(feature = "legacy-task")]
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
    #[cfg(feature = "legacy-task")]
    pub(crate) fn handle_prepared_task(
        &mut self,
        request: Request,
        received: std::time::Instant,
    ) -> Response {
        self.handle_received(request, received)
    }
    pub fn handle_received(&mut self, request: Request, received: std::time::Instant) -> Response {
        self.clear_native_duplicate_key_detail();
        self.handle_with_reply_check(request, received, None)
    }
    pub(crate) fn handle_with_reply_check(
        &mut self,
        request: Request,
        received: std::time::Instant,
        check_reply: Option<&mut ApiSqlReplyCheck<'_>>,
    ) -> Response {
        self.authority_scope(|session| {
            Ok(session.handle_authorized(request, received, check_reply))
        })
        .unwrap_or_else(|error| Response::error(&error))
    }
    fn handle_authorized(
        &mut self,
        request: Request,
        received: std::time::Instant,
        check_reply: Option<&mut ApiSqlReplyCheck<'_>>,
    ) -> Response {
        if self.service.check_statement_gate().is_err() {
            return Response::code("57P01");
        }
        let context = Cancellation::current().unwrap_or_default();
        context.scope(|| {
            let response = self.handle_scoped(request, received, &context, check_reply);
            #[cfg(feature = "legacy-task")]
            {
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
            }
            response
        })
    }
    fn handle_scoped(
        &mut self,
        request: Request,
        received: std::time::Instant,
        context: &Cancellation,
        check_reply: Option<&mut ApiSqlReplyCheck<'_>>,
    ) -> Response {
        if matches!(&self.role, Role::NoSql { .. }) {
            return Response::code(if matches!(request, Request::Ping) { "ok" } else { "42501" });
        }
        // GD-0061 R3-210 backstop (design-2 section 4, call site 5): on a task
        // session the framed surface admits only `Ping`. A `Sql` request goes
        // on to `authorize`, which admits its statement's own class (R3-208);
        // every other request is `Other`, refused before it is unwrapped.
        if self.sql.task().is_some() {
            let class = match &request {
                Request::Ping => Some(giggles_core::task::StatementClass::Show),
                Request::Sql { .. } => None,
                Request::Deadline { .. } => Some(giggles_core::task::StatementClass::Other),
                #[cfg(feature = "legacy-task")]
                Request::Preview { .. }
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
            #[cfg(feature = "legacy-task")]
            Request::Execute { request, .. } => Some(request.operation),
            #[cfg(feature = "legacy-task")]
            Request::RepairSql { operation, .. } | Request::Status { operation } => {
                Some(*operation)
            }
            _ => None,
        };
        // Keep time issuance in decision order across both transports. Core
        // still revalidates revoke/expiry atomically; this gate adds no authority.
        #[cfg(feature = "legacy-task")]
        let service = self.service.clone();
        #[cfg(feature = "legacy-task")]
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
                    let (result, types) = self.execute_api_sql(&sql, &params, check_reply)?;
                    Ok(Response::released_sql_typed(self.release_page(result)?, types))
                }
                #[cfg(feature = "legacy-task")]
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
                #[cfg(feature = "legacy-task")]
                Request::Execute { request, approval } => {
                    let (e, time) = self.executor()?;
                    let op = request.operation;
                    Ok(Response::decision(
                        op,
                        e.execute_current(request, time.0, time.1, approval)?,
                    ))
                }
                #[cfg(feature = "legacy-task")]
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
                #[cfg(feature = "legacy-task")]
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

#[cfg(all(test, feature = "engine-rocksdb"))]
type AuthorizationContextObserver = Box<dyn FnOnce(bool)>;

#[cfg(all(test, feature = "engine-rocksdb"))]
thread_local! {
    static AUTHORIZATION_CONTEXT_OBSERVED: std::cell::RefCell<Option<AuthorizationContextObserver>> =
        const { std::cell::RefCell::new(None) };
    static BEFORE_SEQUENCE_DEFAULT_EXECUTION: std::cell::RefCell<Option<AuthorizationContextObserver>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[test]
    fn api_sql_types_for_empty_select() {
        let (service, _) = fixture();
        service.admin.roles().create(roles::NewRole {
            name: "type_owner".into(), login: true, superuser: true,
            ..roles::NewRole::default()
        }).unwrap();
        let mut session = service.role_session("type_owner").unwrap();
        let execute = |session: &mut Session, sql: &str| session.handle(Request::Sql {
            sql: sql.into(), params: vec![],
        });
        let ddl = execute(&mut session, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
        assert_eq!(ddl.code, "ok");
        assert_eq!(ddl.sql_types, Some(vec![]));
        let reply = execute(&mut session, "SELECT id FROM t WHERE false");
        assert_eq!(reply.code, "ok");
        assert!(reply.sql.as_ref().unwrap().rows.is_empty());
        let reply = crate::api::codec::encode_reply(reply, crate::api::proto::TransactionState::Idle).unwrap();
        assert_eq!(reply.sql.unwrap().column_types, vec![23]);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_reuses_a_success_and_keeps_control_entries() {
        let service = authorization_fixture(Engine::V2);
        let mut session = service.role_session("owner").unwrap();
        session.set_postgres_surface();
        let sql = "SELECT id FROM t";
        let plan = session.prepare(sql).unwrap();
        assert_eq!(session.authorization_full_runs, 1);

        let mut fresh = service.role_session("owner").unwrap();
        let expected = fresh.execute_sql(sql, &[]).unwrap().rows;
        for _ in 0..3 {
            session.begin_implicit_block().unwrap();
            session.authorize_prepared_bind(&plan).unwrap();
            assert_eq!(session.execute_prepared(&plan, &[]).unwrap().rows, expected);
            session.commit_implicit_block().unwrap();
        }
        assert_eq!(session.authorization_full_runs, 3);
        assert_eq!(session.authorization_hits, 10);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_replays_only_column_default_grants() {
        let service = authorization_fixture(Engine::V2);
        let mut session = service.role_session("owner").unwrap();
        session.execute_sql("CREATE TABLE serial_t (id BIGSERIAL PRIMARY KEY)", &[]).unwrap();
        session.execute_sql("CREATE TABLE explicit_t (id BIGINT PRIMARY KEY)", &[]).unwrap();
        session.set_postgres_surface();

        let insert = session.prepare("INSERT INTO serial_t DEFAULT VALUES RETURNING id").unwrap();
        session.execute_prepared(&insert, &[]).unwrap();
        let first = session.authorization_memos[0].as_ref().unwrap().sequence_defaults.clone().unwrap();
        let hits = session.authorization_hits;
        session.execute_prepared(&insert, &[]).unwrap();
        let second = session.authorization_memos[0].as_ref().unwrap().sequence_defaults.as_ref().unwrap();
        assert_eq!(session.authorization_hits, hits + 1);
        assert!(Arc::ptr_eq(&first, second), "the second draw must use the recorded grant");

        for (sql, params) in [
            ("INSERT INTO explicit_t VALUES (nextval('serial_t_id_seq'))", vec![]),
            ("INSERT INTO explicit_t VALUES (nextval($1))", vec![Value::Text("serial_t_id_seq".into())]),
        ] {
            let insert = session.prepare(sql).unwrap();
            for _ in 0..2 {
                session.execute_prepared(&insert, &params).unwrap();
                assert!(session.authorization_memos[0].as_ref().unwrap().sequence_defaults.is_none());
            }
        }
    }

    #[test]
    fn authorization_memo_excludes_legacy() {
        let service = authorization_fixture(Engine::Legacy);
        let mut session = service.role_session("owner").unwrap();
        session.set_postgres_surface();
        let plan = session.prepare("SELECT id FROM t").unwrap();
        for _ in 0..2 {
            session.authorize_prepared_bind(&plan).unwrap();
        }
        assert_eq!(session.authorization_full_runs, 3);
        assert_eq!(session.authorization_hits, 0);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_excludes_task_sessions() {
        let service = credential_fixture();
        let mut session = service.agent_session(&"aa".repeat(32), None).unwrap();
        session.set_postgres_surface();
        assert!(session.task().is_some());
        let mut ordinary = service.role_session("incident_controller").unwrap();
        ordinary.set_postgres_surface();
        let plan = ordinary.prepare("SELECT id FROM shipments").unwrap();
        assert!(plan.has_authorization_binding());

        for _ in 0..2 {
            session.authorize(&plan).unwrap();
        }
        assert_eq!(session.authorization_full_runs, 2);
        assert_eq!(session.authorization_hits, 0);
        assert!(session.sql.authorization_catalog_key(&service.admin).is_none());
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_rechecks_the_latest_role_catalog_in_a_fixed_snapshot() {
        use roles::TablePrivilege::Select;
        let service = authorization_fixture(Engine::V2);
        let roles = service.admin.roles();
        roles.grant_table("t", "reader", &[Select]).unwrap();
        let mut reader = service.role_session("reader").unwrap();
        reader.set_postgres_surface();
        reader.sql.set_isolation(SqlIsolation::RepeatableRead);
        reader.begin_implicit_block().unwrap();
        // REPEATABLE READ acquires its snapshot on the first executed statement.
        reader.execute_sql("SELECT 1", &[]).unwrap();
        let hits = reader.authorization_hits;
        let plan = reader.prepare("SELECT id FROM t").unwrap();
        reader.authorize_prepared_bind(&plan).unwrap();
        assert_eq!(reader.authorization_hits, hits + 1);
        let context = reader.authorization_context().unwrap();

        roles.revoke_table("t", "reader", &[Select]).unwrap();
        let changed = reader.authorization_context().unwrap();
        assert_eq!(changed.statement_catalog, context.statement_catalog);
        assert_ne!(changed.latest_catalog, context.latest_catalog);
        assert!(matches!(reader.authorize_prepared_bind(&plan), Err(CoreError::Policy(Error::Scope))));
        assert_eq!(reader.authorization_hits, hits + 1);
        reader.sql.rollback();
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_checks_password_alias_revisions() {
        let service = authorization_fixture(Engine::V2);
        let mut reader = service.role_session("reader").unwrap();
        reader.set_postgres_surface();
        let old = reader.role_epoch();
        service.update_password_generations(&mut service.password_generations.lock().unwrap(), |aliases| {
            let role = service.admin.roles().alter("reader", roles::RoleChanges {
                password: Some(Some("new verifier".into())), ..Default::default()
            }).unwrap();
            assert!(reader.sql.authorization_catalog_key(&service.admin).is_some());
            assert!(reader.authorization_context().is_none(), "the catalog is visible before the alias");
            aliases.insert("reader".into(), (role.oid, old, role.generation));
        });

        let plan = reader.prepare("SELECT 1").unwrap();
        reader.authorize(&plan).unwrap();
        assert_eq!(reader.authorization_hits, 1);
        let before = reader.authorization_context().unwrap();
        {
            let _aliases = service.password_generations.lock().unwrap();
            assert!(reader.authorization_context() == Some(before));
        }
        service.update_password_generations(&mut service.password_generations.lock().unwrap(), BTreeMap::clear);
        let after = reader.authorization_context().unwrap();
        assert_eq!(before.latest_catalog, after.latest_catalog);
        assert_ne!(before.password_revision, after.password_revision);
        assert!(matches!(reader.authorize(&plan), Err(CoreError::Policy(Error::Authentication))));
        assert_eq!(reader.authorization_hits, 1);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_bypasses_private_catalog_until_rollback() {
        let service = authorization_fixture(Engine::V2);
        let mut session = service.role_session("owner").unwrap();
        session.set_postgres_surface();
        let plan = session.prepare("SELECT id FROM t").unwrap();
        session.authorize_prepared_bind(&plan).unwrap();
        assert_eq!(session.authorization_hits, 1);

        session.begin_implicit_block().unwrap();
        session.execute_sql("CREATE TABLE private_t (id BIGINT PRIMARY KEY)", &[]).unwrap();
        assert!(session.authorization_context().is_none());
        let runs = session.authorization_full_runs;
        for _ in 0..2 {
            session.authorize_prepared_bind(&plan).unwrap();
        }
        assert_eq!(session.authorization_full_runs, runs + 2);
        assert_eq!(session.authorization_hits, 1);
        session.sql.rollback();
        assert!(session.authorization_context().is_some());
        let same = session.execute_prepared(&plan, &[]).unwrap();
        assert_eq!(same.rows[0].values, [Value::Int(1)]);
        assert_eq!(session.authorization_hits, 2);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_restores_sequence_scope() {
        use roles::SchemaPrivilege::Create;

        let service = authorization_fixture(Engine::V2);
        service.admin.roles().create(roles::NewRole {
            name: "outsider".into(), login: true, ..Default::default()
        }).unwrap();
        service.admin.roles().grant_schema("public", "reader", &[Create]).unwrap();
        let mut owner = service.role_session("owner").unwrap();
        owner.execute_sql("CREATE SEQUENCE secret_seq", &[]).unwrap();
        let mut reader = service.role_session("reader").unwrap();
        reader.execute_sql("CREATE SEQUENCE mine", &[]).unwrap();

        for (mut session, expected) in [
            (owner, None),
            (reader, Some(BTreeSet::from(["mine".into()]))),
            (service.role_session("outsider").unwrap(), Some(BTreeSet::new())),
        ] {
            session.set_postgres_surface();
            let plan = session.prepare("SELECT 1").unwrap();
            session.authorize_full(&plan).unwrap();
            let full_scope = session.sql.sequence_tables().clone();
            assert_eq!(full_scope, expected);

            session.sql.set_sequence_tables(Some(BTreeSet::from(["unrelated".into()])));
            let hits = session.authorization_hits;
            session.authorize(&plan).unwrap();
            assert_eq!(session.authorization_hits, hits + 1);
            assert_eq!(session.sql.sequence_tables(), &full_scope);
        }
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_excludes_fingerprint_application_and_native_sessions() {
        let service = authorization_fixture(Engine::V2);
        service.reload_bindings(vec![
            Binding { fingerprint: "11".repeat(32), role: Role::Database { name: "owner".into() } },
            Binding {
                fingerprint: "22".repeat(32),
                role: Role::Application {
                    domain: service.admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["t".into()]),
                },
            },
        ]).unwrap();
        let mut ordinary = service.role_session("owner").unwrap();
        ordinary.set_postgres_surface();
        let plan = ordinary.prepare("SELECT id FROM t").unwrap();
        ordinary.authorize(&plan).unwrap();
        assert_eq!(ordinary.authorization_hits, 1);

        let mut fingerprint = service.session(&"11".repeat(32)).unwrap();
        fingerprint.set_postgres_surface();
        // Isolate the role-kind guard: an Application session that is neither
        // fingerprint bound nor on the native surface still never hits.
        let mut application = service.session(&"22".repeat(32)).unwrap();
        assert!(matches!(application.role, Role::Application { .. }));
        application.surface = Surface::Postgres;
        application.fingerprint_bound = false;
        let native = service.role_session("owner").unwrap();
        for mut session in [fingerprint, application, native] {
            for _ in 0..2 {
                session.authorize(&plan).unwrap();
            }
            assert_eq!(session.authorization_full_runs, 2);
            assert_eq!(session.authorization_hits, 0);
            assert!(session.authorization_memos[0].is_none());
        }
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_skips_catalog_publication() {
        let service = authorization_fixture(Engine::V2);
        let mut session = service.role_session("owner").unwrap();
        session.set_postgres_surface();
        let plan = session.prepare("SELECT id FROM t").unwrap();
        session.authorize(&plan).unwrap();
        assert_eq!(session.authorization_hits, 1);
        let original = session.authorization_memos[0].as_ref().unwrap().context;

        let pause = CommitSyncHook::new();
        service.admin.set_commit_sync_hook(Some(pause.clone()));
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| service.admin.roles().grant_table(
                "t", "reader", &[roles::TablePrivilege::Select],
            ));
            let entered = pause.wait_entered(std::time::Duration::from_secs(10));
            if !entered {
                pause.release(false);
            }
            assert!(entered, "catalog writer never reached the commit sync boundary");

            // Full authorization can wait for publication. Let it proceed once
            // the first memo lookup has sampled the actual publication window.
            let release = pause.clone();
            AUTHORIZATION_CONTEXT_OBSERVED.with_borrow_mut(|observe| {
                *observe = Some(Box::new(move |context_available| {
                    release.release(false);
                    assert!(!context_available);
                }));
            });
            session.authorize(&plan).unwrap();
            writer.join().unwrap().unwrap();
        });
        service.admin.set_commit_sync_hook(None);
        assert_ne!(session.authorization_context().unwrap().latest_catalog, original.latest_catalog);
        assert_eq!(session.authorization_hits, 1);
        assert_eq!(session.authorization_full_runs, 2);
        assert!(session.authorization_memos[0].as_ref().unwrap().context == original);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn authorization_memo_rechecks_stored_defaults_with_changed_budgets() {
        use relational::MAX_ROW_BYTES;

        let service = authorization_fixture(Engine::V2);
        service.role_session("owner").unwrap().execute_sql(
            "CREATE TABLE budget_t (id BIGINT PRIMARY KEY, value BIGINT DEFAULT 42)",
            &[],
        ).unwrap();

        for (tokens, bytes) in [
            (1, relational::DEFAULT_MAX_VALUE_BYTES),
            (relational::DEFAULT_MAX_STATEMENT_TOKENS, MAX_ROW_BYTES),
        ] {
            let mut session = service.role_session("owner").unwrap();
            session.set_postgres_surface();
            let _budgets = session.sql.statement_budgets();
            let plan = session.prepare("INSERT INTO budget_t(id) VALUES (1)").unwrap();
            session.authorize(&plan).unwrap();
            assert_eq!(session.authorization_hits, 1);
            assert_eq!(session.authorization_full_runs, 1);

            // Stored default syntax is thread-local. A fresh worker must lex
            // the default under its current budgets, despite the prior success.
            std::thread::spawn(move || {
                session.set_max_statement_tokens(tokens);
                session.set_max_value_bytes(bytes);
                let _budgets = session.sql.statement_budgets();
                let actual = session.authorize(&plan);
                let full = session.authorize_full(&plan);
                assert_eq!(actual.as_ref().map_err(sqlstate), full.as_ref().map_err(sqlstate));
                if tokens == 1 {
                    assert!(matches!(actual, Err(CoreError::Sql(relational::SqlError::Limit))));
                } else {
                    actual.unwrap();
                }
                assert_eq!(session.authorization_hits, 1);
                assert_eq!(session.authorization_full_runs, 2);
            }).join().unwrap();
        }
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn search_path_reuse_preserves_constants_validation_revision() {
        let service = authorization_fixture(Engine::V2);
        let mut session = service.role_session("owner").unwrap();
        session.set_search_path("public");
        let initial = session.sql.constants_validation_key().unwrap();
        session.set_search_path("public");
        assert_eq!(session.sql.constants_validation_key(), Some(initial));
        session.set_search_path("  public  ");
        assert_eq!(session.sql.constants_validation_key(), Some(initial));
        assert_eq!(session.search_path, "public");
        session.set_search_path("alternate, public");
        assert_ne!(session.sql.constants_validation_key().unwrap().3, initial.3);

        session.sql.set_search_path("elsewhere", "owner");
        session.set_search_path("alternate, public");
        assert!(session.sql.schema_in_search_path("alternate"));
        assert!(!session.sql.schema_in_search_path("elsewhere"));

        session.sql.set_max_statement_tokens(1);
        {
            let _budgets = session.sql.statement_budgets();
            session.set_search_path("alternate, public");
            assert_eq!(session.search_path, "\"\"");
            assert!(!session.sql.schema_in_search_path("alternate"));
        }
        session.set_search_path("alternate, public");
        assert!(session.sql.schema_in_search_path("alternate"));
        assert_eq!(session.search_path, "alternate, public");
    }

    const DUPLICATE_SQL: &str =
        "INSERT INTO duplicate_test VALUES ('00000000-0000-0000-0000-000000000001')";
    const KEY_DETAIL: &str =
        "Key (id)=(00000000-0000-0000-0000-000000000001) already exists.";
    const CREATE_DUPLICATE_TABLE: &str = "CREATE TABLE duplicate_test (id uuid PRIMARY KEY)";

    fn duplicate_key_test_engines() -> impl Iterator<Item = Engine> {
        [
            Engine::Legacy,
            #[cfg(feature = "engine-rocksdb")]
            Engine::V2,
        ]
        .into_iter()
    }

    #[test]
    fn duplicate_key_detail_rejects_replaced_table_identity() {
        use relational::{SqlErrorDetail, take_sql_diagnostic_detail};
        use roles::TablePrivilege::{Insert, Select};

        for engine in duplicate_key_test_engines() {
            let service = authorization_fixture(engine);
            let mut trusted = service.admin.sql_session();
            let mut reader = service.role_session("reader").unwrap();
            let roles = service.admin.roles();
            let capture = |trusted: &mut SqlSession| {
                trusted.execute(CREATE_DUPLICATE_TABLE, &[], None).unwrap();
                trusted.execute(DUPLICATE_SQL, &[], None).unwrap();
                assert_eq!(
                    trusted.execute(DUPLICATE_SQL, &[], None),
                    Err(CoreError::Sql(relational::SqlError::Duplicate)),
                );
                let Some(SqlErrorDetail::DuplicateKey(key)) = take_sql_diagnostic_detail() else {
                    panic!("expected trusted duplicate-key detail");
                };
                assert_eq!(key.values, KEY_DETAIL);
                key
            };

            let old = capture(&mut trusted);
            roles.grant_table("duplicate_test", "reader", &[Insert]).unwrap();
            let authority = reader.sql.table_authority_snapshot("duplicate_test", old.table_id)
                .unwrap().unwrap();
            assert!(!reader.can_read_duplicate_key(&old));

            trusted.execute("DROP TABLE duplicate_test", &[], None).unwrap();
            let replacement = capture(&mut trusted);
            roles.set_table_owner("duplicate_test", "reader").unwrap();
            roles.grant_table("duplicate_test", "reader", &[Select]).unwrap();
            assert_ne!(old.table_id, replacement.table_id);
            assert!(!reader.duplicate_key_select_allowed("reader", &authority).unwrap());
            assert!(!reader.can_read_duplicate_key(&old));
            assert!(reader.can_read_duplicate_key(&replacement));
        }
    }

    #[test]
    fn duplicate_key_detail_for_uncommitted_table_owner() {
        use relational::take_sql_error_detail;
        use roles::SchemaPrivilege::{Create, Usage};

        for engine in duplicate_key_test_engines() {
            let service = authorization_fixture(engine);
            let mut owner = service.role_session("owner").unwrap();
            owner.execute_sql("CREATE SCHEMA duplicate_schema", &[]).unwrap();
            let roles = service.admin.roles();
            roles.grant_schema("duplicate_schema", "reader", &[Create, Usage]).unwrap();
            let mut reader = service.role_session("reader").unwrap();
            reader.set_search_path("duplicate_schema, public");
            reader.execute_sql("BEGIN", &[]).unwrap();
            reader.execute_sql(CREATE_DUPLICATE_TABLE, &[]).unwrap();
            let table = relational::relation_key("duplicate_schema", "duplicate_test");
            let legacy = matches!(engine, Engine::Legacy);
            let registry_owner = if legacy { Some("reader") } else { None };
            assert_eq!(roles.table_owner(&table).unwrap().as_deref(), registry_owner);
            reader.execute_sql(DUPLICATE_SQL, &[]).unwrap();
            reader.execute_sql("SAVEPOINT before_duplicate", &[]).unwrap();
            assert_eq!(
                reader.execute_sql(DUPLICATE_SQL, &[]),
                Err(CoreError::Sql(relational::SqlError::Duplicate)),
            );
            let expected_detail = if legacy { None } else { Some(KEY_DETAIL) };
            assert_eq!(take_sql_error_detail().as_deref(), expected_detail);
            reader.execute_sql("ROLLBACK TO SAVEPOINT before_duplicate", &[]).unwrap();
            reader.execute_sql("COMMIT", &[]).unwrap();
            assert_eq!(roles.table_owner(&table).unwrap().as_deref(), Some("reader"));
        }
    }

    #[test]
    fn duplicate_key_detail_on_role_bound_session_sql_exits() {
        use relational::{SqlErrorDetail, take_sql_diagnostic_detail, take_sql_error_detail};
        use roles::TablePrivilege::{Insert, Select};

        for engine in duplicate_key_test_engines() {
            let service = authorization_fixture(engine);
            let mut owner = service.role_session("owner").unwrap();
            owner.execute_sql(CREATE_DUPLICATE_TABLE, &[]).unwrap();
            owner.execute_sql(DUPLICATE_SQL, &[]).unwrap();
            let roles = service.admin.roles();
            roles.grant_table("duplicate_test", "reader", &[Insert]).unwrap();
            let mut reader = service.role_session("reader").unwrap();

            let duplicate = Err(CoreError::Sql(relational::SqlError::Duplicate));
            assert_eq!(reader.execute_sql(DUPLICATE_SQL, &[]), duplicate);
            assert!(take_sql_error_detail().is_none());
            let prepared = reader.prepare(DUPLICATE_SQL).unwrap();
            assert!(matches!(reader.execute_prepared_unreleased(&prepared, &[]),
                Err(CoreError::Sql(relational::SqlError::Duplicate))));
            assert!(take_sql_diagnostic_detail().is_none());

            roles.grant_table("duplicate_test", "reader", &[Select]).unwrap();
            assert_eq!(reader.execute_sql(DUPLICATE_SQL, &[]), duplicate);
            assert_eq!(take_sql_error_detail().as_deref(), Some(KEY_DETAIL));
            assert!(matches!(reader.execute_prepared_unreleased(&prepared, &[]),
                Err(CoreError::Sql(relational::SqlError::Duplicate))));
            assert!(matches!(take_sql_diagnostic_detail(),
                Some(SqlErrorDetail::DuplicateKey(key)) if key.values == KEY_DETAIL));
        }
    }

    fn authorization_fixture(engine: Engine) -> Arc<Service> {
        static SERIAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = crate::test_support::fixture_root().join(format!(
            "authorization-memo-{}-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let admin = Admin::create_engine(path, Limits::default(), engine).unwrap();
        for (name, superuser) in [("owner", true), ("reader", false)] {
            admin.roles().create(roles::NewRole {
                name: name.into(), login: true, superuser, ..Default::default()
            }).unwrap();
        }
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let mut owner = service.role_session("owner").unwrap();
        owner.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)", &[]).unwrap();
        owner.execute_sql("INSERT INTO t VALUES (1)", &[]).unwrap();
        service
    }

    #[cfg(feature = "engine-rocksdb")]
    fn credential_fixture() -> Arc<Service> {
        use crate::agent::{AgentBinding, wall_now};
        use giggles_core::{
            Admin, Engine, Limits,
            roles::{NewRole, TablePrivilege},
            task::definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText},
        };
        let path = crate::test_support::fixture_root().join(format!(
            "agent-defchange-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin =
            Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
                &[],
                None,
            )
            .unwrap();
        let roles = admin.roles();
        for name in ["incident_controller", "agent_exec"] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    ..NewRole::default()
                })
                .unwrap();
        }
        roles.set_task_only("agent_exec", true).unwrap();
        roles
            .grant_table(
                "shipments",
                "incident_controller",
                &[TablePrivilege::Select],
            )
            .unwrap();
        roles
            .grant_table("shipments", "agent_exec", &[TablePrivilege::Select])
            .unwrap();
        let issuer = roles.get("incident_controller").unwrap().unwrap();
        let clock = crate::clock::Clock::new(0, 3600).unwrap();
        let now = wall_now(&clock).unwrap();
        admin
            .tasks()
            .create(
                &issuer,
                NewTask {
                    name: "inc-436".into(),
                    executor: "agent_exec".into(),
                    approver: None,
                    schema: None,
                    table: "shipments".into(),
                    visible_rows: PredicateText("merchant_id = 42".into()),
                    readable: vec!["id".into(), "status".into()],
                    masked: Vec::new(),
                    untrusted_text: Vec::new(),
                    allowed_write: None,
                    mutation_caps: MutationCaps::default(),
                    disclosure_caps: DisclosureCaps {
                        rows_read: 10,
                        rows_returned: 10,
                        bytes_returned: 4096,
                    },
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires: "2999-01-01 00:00:00+00".into(),
                },
                now,
            )
            .unwrap();
        let fp = "aa".repeat(32);
        crate::Service::with_agent(
            admin.clone(),
            vec![],
            vec![AgentBinding {
                fingerprint: fp.clone(),
                executor: "agent_exec".into(),
                tasks: vec!["inc-436".into()],
            }],
            clock,
            crate::admission::ServiceLimits::default(),
        )
        .unwrap()
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn issue_credential_task_ddl_backstop_refuses_without_writing() {
        let service = credential_fixture();
        let admin = &service.admin;
        let now = crate::agent::wall_now(&service.clock).unwrap();
        let fp = "aa".repeat(32);
        let mut session = service.agent_session(&fp, None).unwrap();
        let stmt = crate::task_ddl::parse("ISSUE CREDENTIAL FOR TASK \"inc-436\"").unwrap();
        assert!(crate::task_ddl::writes(&stmt));
        let def = admin.tasks().get("inc-436").unwrap().unwrap();
        let before = admin.tasks().credentials(def.id, now).unwrap();
        let error = session.execute_task_ddl(&stmt).unwrap_err();
        assert_eq!(error.sqlstate, "0A000");
        assert_eq!(admin.tasks().credentials(def.id, now).unwrap(), before);
    }

    #[cfg(feature = "engine-rocksdb")]
    fn issue_session_credential(
        service: &Service,
        expires: Option<giggles_core::task::definition::WallClockMs>,
    ) -> giggles_core::task::credential::IssuedCredential {
        let issuer = service
            .admin
            .roles()
            .get("incident_controller")
            .unwrap()
            .unwrap();
        let now = crate::agent::wall_now(&service.clock).unwrap();
        service
            .admin
            .tasks()
            .issue_credential(&issuer, "inc-436", expires, now)
            .unwrap()
    }

    #[cfg(feature = "engine-rocksdb")]
    fn assert_auth_refused<T>(result: Result<T, CoreError>) {
        assert!(matches!(
            result,
            Err(CoreError::Policy(Error::Authentication))
        ));
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn credential_session_opens_and_selects() {
        let service = credential_fixture();
        service
            .admin
            .sql_session()
            .execute(
                "INSERT INTO shipments VALUES (1, 42, 'ready'), (2, 99, 'hidden')",
                &[],
                None,
            )
            .unwrap();
        let issued = issue_session_credential(&service, None);
        let mut session = service
            .credential_session(issued.secret.expose(), Some("agent_exec"), Some("inc-436"))
            .unwrap();
        assert_eq!(session.fingerprint, format!("sqlcred:{:016x}", issued.id));
        assert_eq!(session.epoch, issued.id);
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(b"sqlcred\0");
        digest[24..].copy_from_slice(&issued.id.to_be_bytes());
        assert_eq!(session.sql.task().unwrap().credential_fingerprint, digest);
        assert!(session.check_binding().is_ok());
        let read = session
            .execute_sql_unreleased("SELECT id, status FROM shipments LIMIT 10", &[])
            .unwrap();
        let rows = session.release_page(read).unwrap().into_inner().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![Value::Int(1), Value::Text("ready".into())]
        );
        let backend = service
            .cancellations
            .register(&session.fingerprint, session.epoch)
            .unwrap();
        let active = backend.begin(true);
        service.cancel_bound(&session, backend.pid, backend.secret);
        assert_eq!(active.context.check(), Err(CoreError::Cancelled));
        drop(active);
        let issuer = service
            .admin
            .roles()
            .get("incident_controller")
            .unwrap()
            .unwrap();
        service
            .admin
            .tasks()
            .revoke_credential(&issuer, issued.id)
            .unwrap();
        let active = backend.begin(true);
        service.cancel_bound(&session, backend.pid, backend.secret);
        assert!(active.context.check().is_ok());
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn credential_session_refuses_wrong_user_task_secret() {
        let service = credential_fixture();
        let issued = issue_session_credential(&service, None);
        assert_auth_refused(service.credential_session(
            issued.secret.expose(),
            Some("other"),
            None,
        ));
        assert_auth_refused(service.credential_session(
            issued.secret.expose(),
            None,
            Some("other"),
        ));
        assert_auth_refused(service.credential_session("invalid", None, None));
        let mut wrong = issued.secret.expose().to_owned();
        let last = wrong.pop().unwrap();
        wrong.push(if last == '0' { '1' } else { '0' });
        assert_auth_refused(service.credential_session(&wrong, None, None));
        service.clock.latch_uncertain();
        assert_auth_refused(service.credential_session(issued.secret.expose(), None, None));
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn credential_session_refuses_revoked_and_expired() {
        use giggles_core::task::definition::WallClockMs;
        let service = credential_fixture();
        let issuer = service
            .admin
            .roles()
            .get("incident_controller")
            .unwrap()
            .unwrap();
        let issued = issue_session_credential(&service, None);
        service
            .admin
            .tasks()
            .revoke_credential(&issuer, issued.id)
            .unwrap();
        assert_auth_refused(service.credential_session(issued.secret.expose(), None, None));
        let now = crate::agent::wall_now(&service.clock).unwrap();
        let expiring = issue_session_credential(&service, Some(WallClockMs(now.0 + 500)));
        let live = service
            .credential_session(expiring.secret.expose(), None, None)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(550));
        assert_auth_refused(service.credential_session(expiring.secret.expose(), None, None));
        assert_auth_refused(live.check_binding());
        let issued = issue_session_credential(&service, None);
        service.admin.tasks().revoke(&issuer, "inc-436").unwrap();
        assert_auth_refused(service.credential_session(issued.secret.expose(), None, None));
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn native_identity_credential_expires_by_clock_alone() {
        use crate::api::{ConnectionIdentity, error::ApiError};
        use giggles_core::task::definition::WallClockMs;
        let service = credential_fixture();
        let now = crate::agent::wall_now(&service.clock).unwrap();
        let issued = issue_session_credential(&service, Some(WallClockMs(now.0 + 500)));
        let owner = ConnectionIdentity::verified_credential(&service, issued.secret.expose()).unwrap();
        assert_eq!(owner.check(&service), Ok(()));
        let unix_ms = u64::try_from(now.0 + 600).unwrap();
        service.clock.set_sql_wall(Box::new(move || std::time::Duration::from_millis(unix_ms)));
        assert_eq!(owner.check(&service), Err(ApiError::Unauthenticated));
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn credential_check_binding_fails_after_revoke() {
        let service = credential_fixture();
        let issuer = service
            .admin
            .roles()
            .get("incident_controller")
            .unwrap()
            .unwrap();
        let issued = issue_session_credential(&service, None);
        let session = service
            .credential_session(issued.secret.expose(), None, None)
            .unwrap();
        service
            .admin
            .tasks()
            .revoke_credential(&issuer, issued.id)
            .unwrap();
        assert_auth_refused(session.check_binding());
        let issued = issue_session_credential(&service, None);
        let session = service
            .credential_session(issued.secret.expose(), None, None)
            .unwrap();
        service.admin.tasks().revoke(&issuer, "inc-436").unwrap();
        assert_auth_refused(session.check_binding());
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn cert_session_unaffected() {
        let service = credential_fixture();
        let issued = issue_session_credential(&service, None);
        let issuer = service
            .admin
            .roles()
            .get("incident_controller")
            .unwrap()
            .unwrap();
        service
            .admin
            .tasks()
            .revoke_credential(&issuer, issued.id)
            .unwrap();
        let mut session = service.agent_session(&"aa".repeat(32), None).unwrap();
        assert!(session.credential.is_none());
        assert!(session.check_binding().is_ok());
        let read = session.execute_sql_unreleased("SELECT id FROM shipments LIMIT 1", &[]).unwrap();
        assert!(session.release_page(read).unwrap().into_inner().rows.is_empty());
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn privilege_inquiry_preserves_native_fence_after_sql_admission() {
        use giggles_core::{
            CrashPoint,
            operations::{self, Bindings, ValueExpr, once::{
                ONCE_CONTRACT_VERSION, OnceBody, OnceError, OnceIdentity, OnceRequest, OnceTime,
            }},
        };
        use std::time::{Duration, Instant};

        let service = authorization_fixture(Engine::V2);
        let mut session = service.role_session("owner").unwrap();
        session.set_postgres_surface();
        session.execute_sql("CREATE TABLE fence_writer (id BIGINT PRIMARY KEY)", &[]).unwrap();
        let text = "SELECT has_table_privilege('t','SELECT') FROM t";
        assert_eq!(session.execute_sql(text, &[]).unwrap().rows.len(), 1);
        let plan = session.prepare(text).unwrap();
        let callback = crate::pg_catalog_wire::privilege_callback(&session);

        let admin = service.admin.clone();
        let request = OnceRequest {
            contract_version: ONCE_CONTRACT_VERSION,
            identity: OnceIdentity {
                database_domain: admin.authority_metadata().unwrap().0,
                tenant: 0,
                namespace: "privilege-inquiry".into(),
                key: b"native-fence".to_vec(),
            },
            retention_seconds: 60,
            body: OnceBody::Operation(operations::Request::Insert {
                table: "fence_writer".into(),
                values: vec![("id".into(), ValueExpr::Literal(relational::Value::Int(1)))],
            }),
        };
        let callback: giggles_core::PrivilegeCallback = Arc::new(move |name, args, metadata| {
            // This is the first inquiry, inside the admitted SQL execution.
            admin.set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
            let result = operations::Engine::new(admin.clone()).execute_once(
                &request, &Bindings::new(), &|| Ok(OnceTime {
                    lower_unix_seconds: 100,
                    upper_unix_seconds: 101,
                    valid_until: Instant::now() + Duration::from_secs(10),
                }),
            );
            assert_eq!(result, Err(OnceError::Unknown));
            callback(name, args, metadata)
        });
        let clock = StatementClock {
            transaction_us: 100,
            statement_us: 100,
            unavailable: None,
            zone: None,
            wall: None,
        };
        let result = session.sql.with_privilege_callback(callback, |sql| {
            sql.execute_prepared_at(&plan, &[], None, &clock)
        });
        let error = result.unwrap_err();
        assert_eq!(sqlstate(&error), "08007");
        assert_eq!(error, CoreError::Fenced);
    }

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
        fixture_with_engine(giggles_core::Engine::Legacy)
    }

    fn fixture_with_engine(engine: giggles_core::Engine) -> (Arc<Service>, Vec<Binding>) {
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
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
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
    fn prepare_lastval_requires_sql_scope() {
        let (service, mut bindings) = fixture();
        let mut sql = service.session(&bindings[0].fingerprint).unwrap();
        let prepared = sql.prepare("SELECT lastval()").unwrap();
        let error = sql.execute_prepared(&prepared, &[]).unwrap_err();
        assert_eq!(sqlstate(&error), "55000");
        assert_eq!(
            relational::take_sql_error_message().as_deref(),
            Some("lastval is not yet defined in this session"),
        );

        bindings[0].role = Role::NoSql {
            domain: service.admin.authority_metadata().unwrap().0.to_string(),
        };
        service.reload_bindings(bindings.clone()).unwrap();
        let mut nosql = service.session(&bindings[0].fingerprint).unwrap();
        for query in ["SELECT 1", "SELECT lastval()"] {
            assert!(matches!(nosql.prepare(query), Err(CoreError::Policy(Error::Scope))));
        }
    }

    #[test]
    fn nosql_binding_allows_ping_only() {
        let role: Role = serde_json::from_str(r#"{"role":"NoSql","domain":"d"}"#).unwrap();
        assert_eq!(role, Role::NoSql { domain: "d".into() });
        assert!(!role.is_sql());
        assert_eq!(serde_json::to_value(&role).unwrap(), serde_json::json!({"role":"NoSql","domain":"d"}));

        let (service, mut bindings) = fixture();
        bindings[0].role = Role::NoSql {
            domain: service.admin.authority_metadata().unwrap().0.to_string(),
        };
        service.reload_bindings(bindings.clone()).unwrap();
        let mut session = service.session(&bindings[0].fingerprint).unwrap();
        assert_eq!(session.handle(Request::Ping).code, "ok");
        #[cfg(feature = "legacy-task")]
        let repair = giggles_core::Request {
            operation: OperationId(1),
            selection: Selection::Keys(vec![]),
            column: Column::Status,
            after: Status::Queued,
        };
        for request in [
            Request::Sql { sql: "invalid SQL".into(), params: vec![] },
            #[cfg(feature = "legacy-task")]
            Request::Preview { request: repair.clone() },
            #[cfg(feature = "legacy-task")]
            Request::Execute { request: repair, approval: None },
            #[cfg(feature = "legacy-task")]
            Request::RepairSql { operation: OperationId(1), sql: "invalid SQL".into(), approval: None },
            #[cfg(feature = "legacy-task")]
            Request::Status { operation: OperationId(1) },
            Request::Deadline { budget_ms: u64::MAX, request: Box::new(Request::Ping) },
        ] {
            assert_eq!(session.handle(request).code, "42501");
        }
        assert_eq!(session.handle(Request::Ping).code, "ok");
        bindings[0].role = Role::NoSql { domain: "wrong-domain".into() };
        assert!(matches!(
            Service::new(service.admin.clone(), bindings, Clock::new(0, 3600).unwrap()),
            Err(CoreError::Policy(Error::Authentication))
        ));
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn committed_catalog_preparation_inherits_statement_budgets() {
        let path = crate::test_support::fixture_root().join(format!("committed-budgets-{}-{}",
            std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
        admin.sql_session().execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None).unwrap();
        let binding = Binding { fingerprint: "12".repeat(32), role: Role::Application {
            domain: admin.authority_metadata().unwrap().0.to_string(),
            tables: BTreeSet::from(["app".into()]),
        }};
        let service = Service::new(admin, vec![binding.clone()], Clock::new(0, 3600).unwrap()).unwrap();
        let list = (1..=20).map(|n| n.to_string()).collect::<Vec<_>>().join(",");
        let sql = format!("SELECT id FROM app WHERE (id+id+id+id) IN ({list})");
        for transaction in [false, true] {
            for tokens in [64, 0, giggles_core::relational::DEFAULT_MAX_STATEMENT_TOKENS] {
                let mut session = service.session(&binding.fingerprint).unwrap();
                session.set_max_statement_tokens(tokens);
                if transaction { session.sql.execute("BEGIN", &[], None).unwrap(); }
                let result = session.prepare(&sql);
                if tokens != 64 { assert!(result.is_ok(), "{result:?}"); }
                else { assert!(matches!(result, Err(CoreError::Sql(giggles_core::relational::SqlError::Limit))), "{result:?}"); }
            }
        }
        drop(service);
        std::fs::remove_dir_all(path).unwrap();
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
        database_binding_fixture_with_engine(giggles_core::Engine::Legacy)
    }

    fn database_binding_fixture_with_engine(engine: giggles_core::Engine) -> (Arc<Service>, Vec<Binding>) {
        let (service, mut bindings) = fixture_with_engine(engine);
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
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn native_identity_task_only_reference() {
        let service = credential_fixture();
        let session = service.role_session("incident_controller").unwrap();
        service.admin.roles().set_task_only("incident_controller", true).unwrap();
        let reference = session.check_binding_reference();
        assert_eq!(reference, Err(CoreError::Policy(Error::Authentication)));
        assert_eq!(
            service.check_native_identity(
                &session.role, session.epoch, session.role_fence, &session.fingerprint, false,
            ),
            reference,
        );
    }
    #[test]
    fn native_identity_equivalence_matrix() {
        let engine = if cfg!(feature = "engine-rocksdb") {
            giggles_core::Engine::V2
        } else {
            giggles_core::Engine::Legacy
        };
        let (service, bindings) = database_binding_fixture_with_engine(engine);
        let fp = &bindings[0].fingerprint;
        let role = bindings[0].role.clone();
        let catalog = service.admin.roles().get("bound").unwrap().unwrap();
        let entry = service.bindings.lock().unwrap().entries[fp].clone();
        let fences = [None, Some((catalog.oid, catalog.generation)),
            Some((catalog.oid + 1, catalog.generation)), Some((catalog.oid, catalog.generation + 10))];
        let mut cases = 0;
        let states = ["normal", "missing", "nologin", "generation", "alias"];
        #[cfg(feature = "engine-rocksdb")]
        let states = states.into_iter().chain(["task_only"]);
        for state in states {
            // Restore the role for each row without depending on its allocated oid.
            let roles = service.admin.roles();
            if roles.get("bound").unwrap().is_none() {
                roles.create(roles::NewRole { name: "bound".into(), login: true,
                    ..roles::NewRole::default() }).unwrap();
            }
            roles.alter("bound", roles::RoleChanges { login: Some(true), ..Default::default() }).unwrap();

            match state {
                "missing" => roles.drop("bound").unwrap(),
                "nologin" => { roles.alter("bound", roles::RoleChanges { login: Some(false), ..Default::default() }).unwrap(); }
                "generation" | "alias" => { roles.alter("bound", roles::RoleChanges { superuser: Some(false), ..Default::default() }).unwrap(); }
                "task_only" => { roles.set_task_only("bound", true).unwrap(); }
                _ => {}
            }
            service.update_password_generations(&mut service.password_generations.lock().unwrap(), |aliases| {
                aliases.clear();
                if let Some(current) = roles.get("bound").unwrap().filter(|_| state == "alias") {
                    aliases.insert("bound".into(),
                        (current.oid, current.generation.saturating_sub(1), current.generation));
                }
            });
            let current = roles.get("bound").unwrap();
            let mut all_fences = fences.to_vec();
            if let Some(current) = current {
                all_fences.extend([Some((current.oid, current.generation)),
                    Some((current.oid, current.generation.saturating_sub(1))),
                    Some((current.oid, current.generation + 1)),
                    Some((current.oid, current.generation.saturating_sub(2)))]);
            }
            for binding in [None, Some(entry.clone()), Some((role.clone(), entry.1 + 1)),
                Some((Role::Application { domain: "other".into(), tables: BTreeSet::new() }, entry.1))] {
                let mut map = service.bindings.lock().unwrap();
                map.entries.remove(fp);
                if let Some(binding) = binding { map.entries.insert(fp.clone(), binding); }
                drop(map);
                for fence in &all_fences {
                    for bound in [false, true] {
                        let old = service.native_identity_reference(&role, entry.1, *fence, fp, bound);
                        let new = service.check_native_identity(&role, entry.1, *fence, fp, bound);
                        assert_eq!(new, old, "{state} {fence:?} bound={bound}");
                        cases += 1;
                    }
                }
            }
        }
        // Non-database roles must ignore role fences and still check bindings.
        let app = Role::Application { domain: "test".into(), tables: BTreeSet::new() };
        service.bindings.lock().unwrap().entries.insert(fp.clone(), (app.clone(), entry.1));
        assert_eq!(
            service.check_native_identity(&app, entry.1, fences[2], fp, true),
            service.native_identity_reference(&app, entry.1, fences[2], fp, true),
        );
        assert!(service.check_native_identity(&app, entry.1 + 1, None, fp, true).is_err());
        assert!(cases >= 250);
        let owner = crate::api::ConnectionIdentity::verified(&service, fp).unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = service.bindings.lock().unwrap();
            panic!("poison bindings for fail-closed equivalence");
        }));
        let reference = service.native_identity_reference(&app, entry.1, None, fp, true);
        assert_eq!(reference, Err(CoreError::Fenced));
        assert_eq!(service.check_native_identity(&app, entry.1, None, fp, true), reference);
        assert_eq!(owner.check(&service), Err(crate::api::error::ApiError::Unauthenticated));

        let (service, bindings) = database_binding_fixture();
        let fp = &bindings[0].fingerprint;
        let session = service.session(fp).unwrap();
        service.admin.set_crash_point(giggles_core::CrashPoint::ErrorBeforeCommit).unwrap();
        assert_eq!(
            service.admin.sql_session().execute("CREATE TABLE fence_probe (id int)", &[], None),
            Err(CoreError::Storage("injected before commit".into())),
        );
        assert!(matches!(
            service.admin.roles().get("bound"),
            Err(roles::RoleError::Core(CoreError::Fenced)),
        ));
        for bound in [false, true] {
            let reference = service.native_identity_reference(
                &session.role, session.epoch, session.role_fence, fp, bound,
            );
            assert_eq!(reference, Err(CoreError::Policy(Error::Authentication)));
            assert_eq!(
                service.check_native_identity(
                    &session.role, session.epoch, session.role_fence, fp, bound,
                ),
                reference,
                "fenced catalog, bound={bound}",
            );
        }
    }
    #[test]
    fn native_identity_rechecks_committed_role_changes() {
        let (service, bindings) = database_binding_fixture();
        let roles = service.admin.roles();
        let role = &bindings[0].role;
        let original = roles.get("bound").unwrap().unwrap();
        let check = |fence| service.check_native_identity(role, 0, fence, "", false);
        let original_fence = Some((original.oid, original.generation));
        let denied = Err(CoreError::Policy(Error::Authentication));

        assert_eq!(check(original_fence), Ok(()));
        assert_eq!(check(None), Ok(()));
        roles.alter("bound", roles::RoleChanges {
            login: Some(false), ..Default::default()
        }).unwrap();
        assert_eq!(check(None), denied);
        assert_eq!(check(original_fence), denied);

        roles.alter("bound", roles::RoleChanges {
            login: Some(true), ..Default::default()
        }).unwrap();
        assert_eq!(check(None), Ok(()));
        assert_eq!(check(original_fence), denied);
        let restored = roles.get("bound").unwrap().unwrap();
        assert_eq!(check(Some((restored.oid, restored.generation))), Ok(()));

        roles.drop("bound").unwrap();
        assert_eq!(check(None), denied);
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn native_identity_rechecks_committed_role_changes_v2() {
        let (service, bindings) = database_binding_fixture_with_engine(giggles_core::Engine::V2);
        let fp = &bindings[0].fingerprint;
        let session = service.session(fp).unwrap();
        let role = &session.role;
        let roles = service.admin.roles();
        let denied = Err(CoreError::Policy(Error::Authentication));
        let check_bound = |epoch, fence| service.check_native_identity(role, epoch, fence, fp, true);
        let check_unbound = || service.check_native_identity(role, 0, None, "", false);
        let fence_of = |role: roles::CatalogRole| Some((role.oid, role.generation));

        // Warm the image through the same check used after each commit.
        assert_eq!(check_bound(session.epoch, session.role_fence), Ok(()));
        assert_eq!(check_unbound(), Ok(()));
        roles.alter("bound", roles::RoleChanges {
            login: Some(false), ..Default::default()
        }).unwrap();
        assert_eq!(check_bound(session.epoch, session.role_fence), denied);
        assert_eq!(check_unbound(), denied);

        let restored = roles.alter("bound", roles::RoleChanges {
            login: Some(true), ..Default::default()
        }).unwrap();
        assert_eq!(check_unbound(), Ok(()));
        let restored_fence = fence_of(restored);
        assert_eq!(check_bound(session.epoch, restored_fence), Ok(()));
        assert_eq!(check_bound(session.epoch, session.role_fence), denied);

        let changed = roles.alter("bound", roles::RoleChanges {
            superuser: Some(false), ..Default::default()
        }).unwrap();
        assert_eq!(check_bound(session.epoch, restored_fence), denied);
        let current_fence = fence_of(changed);
        assert_eq!(check_bound(session.epoch, current_fence), Ok(()));

        service.reload_bindings(vec![]).unwrap();
        assert_eq!(check_bound(session.epoch, current_fence), denied);
        let new_epoch = service.reload_bindings(bindings.clone()).unwrap();
        assert_ne!(new_epoch, session.epoch);
        assert_eq!(check_bound(session.epoch, current_fence), denied);
        assert_eq!(check_bound(new_epoch, current_fence), Ok(()));

        let task_only = roles.set_task_only("bound", true).unwrap();
        assert_eq!(check_bound(new_epoch, current_fence), denied);
        // Use the new generation so only the task-only attribute explains denial.
        assert_eq!(check_bound(new_epoch, fence_of(task_only)), denied);
        assert_eq!(check_unbound(), denied);
        let ordinary = roles.set_task_only("bound", false).unwrap();
        let ordinary_fence = fence_of(ordinary);
        assert_eq!(check_bound(new_epoch, ordinary_fence), Ok(()));
        roles.drop("bound").unwrap();
        assert_eq!(check_bound(new_epoch, ordinary_fence), denied);
        assert_eq!(check_unbound(), denied);
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
    fn admin_drop_table_checks_schema_before_object_and_owner() {
        let (service, _) = fixture();
        let roles = service.admin.roles();
        for (name, superuser) in [("owner", true), ("reader", false)] {
            roles.create(roles::NewRole { name: name.into(), login: true, superuser, ..Default::default() }).unwrap();
        }
        let mut owner = service.role_session("owner").unwrap();
        owner.execute_sql("CREATE SCHEMA app", &[]).unwrap();
        owner.execute_sql("CREATE TABLE app.t(id BIGINT PRIMARY KEY)", &[]).unwrap();
        let mut reader = service.role_session("reader").unwrap();
        for (query, code) in [
            ("DROP TABLE hidden.missing", "3F000"),
            ("DROP TABLE missing", "42P01"),
            ("DROP TABLE app.missing", "42501"),
            ("DROP TABLE IF EXISTS app.missing", "42501"),
            ("DROP TABLE app.t", "42501"),
        ] {
            assert_eq!(reader.execute_admin(&crate::auth::statements::parse(query).unwrap()), Err(code), "{query}");
        }
        assert_eq!(reader.execute_admin(&crate::auth::statements::parse("DROP TABLE IF EXISTS hidden.missing").unwrap()), Ok("DROP TABLE"));
        assert_eq!(std::mem::take(&mut reader.role_notices), vec![(false, "schema \"hidden\" does not exist, skipping".into())]);
        roles.grant_schema("app", "reader", &[roles::SchemaPrivilege::Usage]).unwrap();
        assert_eq!(reader.execute_admin(&crate::auth::statements::parse("DROP TABLE app.missing").unwrap()), Err("42P01"));
        assert_eq!(reader.execute_admin(&crate::auth::statements::parse("DROP TABLE IF EXISTS app.missing").unwrap()), Ok("DROP TABLE"));
        assert_eq!(reader.execute_admin(&crate::auth::statements::parse("DROP TABLE app.t").unwrap()), Err("42501"));
        assert_eq!(std::mem::take(&mut reader.role_notices), vec![(false, "table \"missing\" does not exist, skipping".into())]);
        roles.set_table_owner("app\0t", "reader").unwrap();
        assert_eq!(reader.execute_admin(&crate::auth::statements::parse("DROP TABLE app.t").unwrap()), Ok("DROP TABLE"));
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
        #[cfg(feature = "legacy-task")]
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
    fn metadata_snapshot_denies_task_roles_and_does_not_invent_missing_tables() {
        let (service, mut bindings) = fixture();
        let domain = service.admin.authority_metadata().unwrap().0.to_string();
        bindings[0].role = Role::NoSql {
            domain: domain.clone(),
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

    #[cfg(feature = "legacy-task")]
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

}

#[cfg(all(test, not(feature = "legacy-task")))]
mod tests {
    #[test]
    fn task_binding_rejected_without_legacy_feature() {
        let error = serde_json::from_str::<Vec<super::Binding>>(
            r#"[{"fingerprint":"1111111111111111111111111111111111111111111111111111111111111111","role":{"role":"Task","domain":"d","task":1,"principal":1}}]"#,
        ).unwrap_err();
        assert!(error.to_string().contains("role Task is the retired legacy task prototype"), "{error}");
    }






}
