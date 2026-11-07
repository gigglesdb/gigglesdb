use crate::clock::Clock;
use giggles_core::{
    relational::{Prepared, SqlResult, StatementClock, Value},
    *,
};
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
    #[serde(skip)]
    Database { name: String },
}
impl Role {
    pub fn is_sql(&self) -> bool {
        matches!(self, Role::Application { .. } | Role::Database { .. })
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
fn digest(effects: &[Effect]) -> String {
    // Stable v1 digest; no raw before/after rows are returned.
    hex::encode(Sha256::digest(
        serde_json::to_vec(effects).expect("effect serialization"),
    ))
}
pub fn sqlstate(e: &CoreError) -> &'static str {
    use giggles_core::relational::SqlError as S;
    match e {
        CoreError::Policy(Error::Authentication | Error::UnknownTask) => "28000",
        CoreError::Policy(Error::Revoked | Error::Expired | Error::Scope) => "42501",
        CoreError::Policy(Error::UncertainTime | Error::TimeRegression) => "55000",
        CoreError::Policy(Error::Budget) => "54000",
        CoreError::Policy(
            Error::ApprovalRequired | Error::InvalidApproval | Error::StalePreview,
        ) => "GD001",
        CoreError::Policy(Error::IdConflict) => "GD002",
        CoreError::Policy(_) => "22023",
        CoreError::Sql(S::Unsupported | S::Reserved)
        | CoreError::Unsupported
        | CoreError::EngineUnsupported { .. } => "0A000",
        CoreError::Sql(S::Syntax) => "42601",
        CoreError::Sql(S::UnknownTable) => "42P01",
        CoreError::Sql(S::UnknownColumn) => "42703",
        CoreError::Sql(S::Duplicate) => "23505",
        CoreError::Sql(S::Null) => "23502",
        CoreError::Sql(S::Type | S::Parameters) => "42804",
        CoreError::Sql(S::Limit | S::Unbounded) | CoreError::Limit => "54000",
        CoreError::Sql(S::Version | S::Conflict) => "40001",
        CoreError::Sql(S::StaleSchema) => "0A000",
        CoreError::Sql(S::Transaction) => "25000",
        CoreError::Sql(S::Aborted) => "25P02",
        CoreError::Sql(S::Overflow) => "22003",
        CoreError::Sql(S::UndefinedFunction) => "42883",
        CoreError::Sql(S::InvalidParameterValue) => "22023",
        CoreError::Sql(S::InvalidDatetimeFormat) => "22007",
        CoreError::Sql(S::DatetimeFieldOverflow) => "22008",
        CoreError::Sql(S::IntervalFieldOverflow) => "22015",
        CoreError::Sql(S::AmbiguousFunction) => "42725",
        CoreError::Sql(S::GroupingError) => "42803",
        CoreError::Sql(S::IndeterminateDatatype) => "42P18",
        CoreError::Sql(S::AmbiguousParameter) => "42P08",
        CoreError::Sql(S::InvalidTextRepresentation) => "22P02",
        CoreError::Sql(S::SubstringError) => "22011",
        CoreError::Sql(S::CharacterNotInRepertoire) => "22021",
        CoreError::Sql(S::DuplicateTable) => "42P07",
        CoreError::Sql(S::DuplicateColumn) => "42701",
        CoreError::Sql(S::DuplicateObject) => "42710",
        CoreError::Sql(S::UndefinedObject) => "42704",
        CoreError::Sql(S::CheckViolation) => "23514",
        CoreError::Sql(S::ForeignKeyViolation) => "23503",
        CoreError::Sql(S::DependentObjects) => "2BP01",
        CoreError::Sql(S::GeneratedAlways) => "428C9",
        CoreError::Sql(S::ObjectNotInPrerequisiteState) => "55000",
        CoreError::Sql(S::SequenceLimit) => "2200H",
        CoreError::Sql(S::NoActiveTransaction) => "25P01",
        CoreError::Sql(S::InvalidSavepoint) => "3B001",
        CoreError::Sql(S::InvalidForeignKey) => "42830",
        CoreError::Sql(S::InvalidSchema) => "3F000",
        CoreError::Sql(S::DivisionByZero) => "22012",
        CoreError::Sql(S::InvalidEscapeSequence) => "22025",
        CoreError::Sql(S::InvalidTableDefinition) => "42P16",
        CoreError::Cancelled => "57014",
        CoreError::Busy => "53300",
        CoreError::Storage(_) | CoreError::Fenced => "08007",
        _ => "XX000",
    }
}
pub struct Service {
    pub(crate) blocking: crate::blocking::Blocking,
    pub(crate) cancellations: crate::cancel::Registry,
    task_gate: std::sync::Mutex<()>,
    pub(crate) admin: Admin,
    pub(crate) clock: Clock,
    pub(crate) authority_gate: std::sync::RwLock<()>,
    bindings: std::sync::Mutex<Credentials>,
    pub(crate) admission: crate::admission::Admission,
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
        crate::pg_catalog_wire::initialize_startup();
        let map = Self::validate(&admin, bindings)?;
        let mut next = 0u64;
        let entries = map
            .into_iter()
            .map(|(fp, role)| {
                next += 1;
                (fp, (role, next))
            })
            .collect();
        Ok(Arc::new(Self {
            blocking: crate::blocking::Blocking::new(),
            cancellations: crate::cancel::Registry::default(),
            task_gate: std::sync::Mutex::new(()),
            authority_gate: std::sync::RwLock::new(()),
            bindings: std::sync::Mutex::new(Credentials { next, entries }),
            admission: crate::admission::Admission::new(limits)?,
            admin,
            clock,
        }))
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
            if matches!(&b.role, Role::Database { .. }) {
                return Err(CoreError::Policy(Error::Authentication));
            }
            if b.fingerprint.len() != 64
                || !b
                    .fingerprint
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(CoreError::Limit);
            }
            let bound = match &b.role {
                Role::Task { domain, .. } | Role::Application { domain, .. } => domain,
                Role::Database { .. } => return Err(CoreError::Policy(Error::Authentication)),
            };
            if bound != &domain {
                return Err(CoreError::Policy(Error::Authentication));
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
    /// Decisions already ordered before this lock are never undone.
    pub fn reload_bindings(&self, bindings: Vec<Binding>) -> Result<u64, CoreError> {
        let proposed = Self::validate(&self.admin, bindings)?;
        let _gate = self.authority_gate.write().map_err(|_| CoreError::Fenced)?;
        let mut current = self.bindings.lock().map_err(|_| CoreError::Fenced)?;
        let mut next = current.next;
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
        *current = Credentials { next, entries };
        Ok(next)
    }
    pub(crate) fn cancel_bound(&self, session: &Session, pid: u32, secret: u32) {
        // Hold registry snapshot through cancellation; reload cannot revoke
        // between validation and signaling. Never wait for the decision gate.
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
    pub fn role_session(self: &Arc<Self>, name: &str) -> Result<Session, CoreError> {
        let role = self
            .admin
            .roles()
            .get(name)
            .map_err(|_| CoreError::Policy(Error::Authentication))?
            .filter(|role| role.login)
            .ok_or(CoreError::Policy(Error::Authentication))?;
        Ok(Session {
            service: self.clone(),
            fingerprint: format!("pgrole:{}", role.name),
            role: Role::Database { name: role.name },
            epoch: role.generation,
            scoped: false,
            connection: None,
            database: None,
            sql: self.admin.sql_session(),
            zone: Some(SessionZone::Fixed(0)),
            transaction_us: None,
        })
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
        Ok(Session {
            service: self.clone(),
            role,
            epoch,
            scoped: false,
            connection: None,
            database: None,
            fingerprint: fingerprint.to_owned(),
            sql: self.admin.sql_session(),
            zone: Some(SessionZone::Fixed(0)),
            transaction_us: None,
        })
    }
}
pub struct Session {
    pub(crate) service: Arc<Service>,
    pub(crate) role: Role,
    pub(crate) database: Option<String>,
    pub(crate) fingerprint: String,
    pub(crate) epoch: u64,
    scoped: bool,
    pub(crate) connection: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    pub(crate) sql: SqlSession,
    zone: Option<SessionZone>,
    transaction_us: Option<i64>,
}
impl Session {
    pub(crate) fn set_time_zone(&mut self, name: &str) {
        self.zone = if ["UTC", "Etc/UTC", "GMT", "Z"]
            .iter()
            .any(|alias| name.eq_ignore_ascii_case(alias))
        {
            Some(SessionZone::Fixed(0))
        } else {
            // No PostgreSQL POSIX-offset parser is exposed by pg-types.
            SessionZone::named(name).ok()
        };
    }
    pub(crate) fn check_binding(&self) -> Result<(), CoreError> {
        if let Role::Database { name } = &self.role {
            return self
                .service
                .admin
                .roles()
                .get(name)
                .map_err(|_| CoreError::Policy(Error::Authentication))?
                .filter(|role| role.login && role.generation == self.epoch)
                .map(|_| ())
                .ok_or(CoreError::Policy(Error::Authentication));
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
        self.sql.abort();
    }
    pub fn in_transaction(&self) -> bool {
        self.sql.in_transaction()
    }
    pub fn is_aborted(&self) -> bool {
        self.sql.is_aborted()
    }
    pub(crate) fn execute_admin(
        &mut self,
        stmt: &crate::auth::statements::AdminStatement,
    ) -> Result<&'static str, &'static str> {
        let Role::Database { name } = &self.role else {
            return Err("42501");
        };
        self.check_binding().map_err(|error| sqlstate(&error))?;
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
            .map_err(|_| "28000")?
            .filter(|role| role.login && role.generation == self.epoch)
            .ok_or("28000")?;
        crate::auth::statements::execute(stmt, &self.service.admin, &actor)
    }
    /// Wire description of a prepared statement. DDL returns no rows and takes
    /// no parameters; core does not describe it, so report an empty shape.
    pub fn describe(&mut self, p: &Prepared) -> Result<relational::Description, CoreError> {
        if p.is_ddl() {
            return Ok(relational::Description {
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
        self.authority_scope(|session| {
            if session.is_aborted() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
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
                        .map_err(|_| CoreError::Policy(Error::Authentication))?
                        .filter(|role| role.login && role.generation == session.epoch)
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
            let (epoch, catalog) = session.service.admin.catalog()?;
            let visible = catalog
                .into_iter()
                .filter(|table| {
                    tables
                        .as_ref()
                        .is_none_or(|tables| tables.contains(&table.name))
                })
                .collect::<Vec<_>>();
            let (index_epoch, indexes) = session.service.admin.secondary_index_catalog()?;
            if index_epoch != epoch {
                return Err(CoreError::Sql(relational::SqlError::Conflict));
            }
            let indexes = indexes
                .into_iter()
                .filter(|index| visible.iter().any(|table| table.id == index.table))
                .collect();
            let (epoch, visible) = session.validate_catalog_snapshot(epoch, visible)?;
            Ok((epoch, visible, indexes))
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
        for table in &visible {
            giggles_core::cancellation::check()?;
            // Table names originate from the validated engine catalog, not
            // client SQL. Prepare/Describe do not access application rows.
            let prepared = self
                .sql
                .prepare(&format!("SELECT * FROM {} LIMIT 1", table.name))?;
            let description = self.sql.describe(&prepared)?;
            if description.columns != table.logical_columns().cloned().collect::<Vec<_>>() {
                return Err(CoreError::Sql(relational::SqlError::Conflict));
            }
        }
        // A concurrent trusted DDL operation cannot splice two generations
        // into this snapshot or make validation certify a different catalog.
        if self.service.admin.catalog()?.0 != epoch {
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
            session.authorize_sequence_sql(sql)?;
            let p = if matches!(&session.role, Role::Database { .. }) {
                declared(
                    session
                        .sql
                        .prepare(sql)
                        .map_err(|error| parse_error(session, error))?,
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
        self.authority_scope(|session| session.authorize_sequences(&dependencies))
    }
    fn authorize_sequences(&mut self, dependencies: &[Option<String>]) -> Result<(), CoreError> {
        let sequences = self.sql.sequence_owners()?;
        let roles = self.service.admin.roles();
        let allowed_table = |table: &str| -> bool {
            match &self.role {
                Role::Application { tables, .. } => tables.contains(table),
                Role::Database { name } => {
                    self.sql
                        .table_authority_name(table)
                        .is_ok_and(|durable_name| {
                            roles
                                .table_owner(&durable_name)
                                .is_ok_and(|owner| owner.as_deref() == Some(name.as_str()))
                        })
                }
                _ => false,
            }
        };
        let superuser = if let Role::Database { name } = &self.role {
            roles
                .get(name)
                .map_err(|_| CoreError::Policy(Error::Authentication))?
                .filter(|role| role.login && role.generation == self.epoch)
                .ok_or(CoreError::Policy(Error::Authentication))?
                .superuser
        } else {
            false
        };
        let allowed_tables: BTreeSet<String> = sequences
            .iter()
            .filter_map(|(_, table)| table.as_ref().filter(|table| allowed_table(table)).cloned())
            .collect();
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
                    .is_none_or(|(_, table)| table.as_deref().is_some_and(allowed_table))
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
        let allowed = if let Role::Database { name } = &self.role {
            let roles = self.service.admin.roles();
            let role = roles
                .get(name)
                .map_err(|_| CoreError::Policy(Error::Authentication))?
                .filter(|role| role.login && role.generation == self.epoch)
                .ok_or(CoreError::Policy(Error::Authentication))?;
            if role.superuser {
                true
            } else {
                // The durable key follows staged renames; missing names remain
                // ordinary absent ownership records and cannot leak 42P01.
                let name_at_commit = self.sql.table_authority_name(table)?;
                roles
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
    fn authorize(&mut self, p: &Prepared) -> Result<(), CoreError> {
        self.authorize_sequences(&p.sequences())?;
        if p.sequence_only() {
            return Ok(());
        }
        let allowed = matches!(&self.role, Role::Application { tables,.. }
            if !p.is_ddl() && p.table().is_none_or(|t| t != "giggles_join" && tables.contains(t)));
        let allowed = if let Role::Database { name } = &self.role {
            let role = self
                .service
                .admin
                .roles()
                .get(name)
                .map_err(|_| CoreError::Policy(Error::Authentication))?
                .filter(|role| role.login && role.generation == self.epoch)
                .ok_or(CoreError::Policy(Error::Authentication))?;
            if role.superuser {
                true
            } else if p.is_alter_table() {
                let table = p.table().ok_or(CoreError::Policy(Error::Scope))?;
                let name_at_commit = self.sql.table_authority_name(table)?;
                self.service
                    .admin
                    .roles()
                    .table_owner(&name_at_commit)
                    .map_err(|_| CoreError::Policy(Error::Scope))?
                    .as_deref()
                    == Some(name.as_str())
            } else if p.is_ddl() {
                self.service
                    .admin
                    .roles()
                    .schema_privileges("public", name)
                    .map_err(|_| CoreError::Policy(Error::Scope))?
                    .contains(&roles::SchemaPrivilege::Create)
            } else if let Some(table) = p.table() {
                if table == "giggles_join" {
                    false
                } else {
                    let roles = self.service.admin.roles();
                    let name_at_commit = self.sql.table_authority_name(table)?;
                    let owner = roles
                        .table_owner(&name_at_commit)
                        .map_err(|_| CoreError::Policy(Error::Scope))?;
                    let privilege = match p.command() {
                        "SELECT" | "EXPLAIN" => Some(roles::TablePrivilege::Select),
                        "INSERT" => Some(roles::TablePrivilege::Insert),
                        "UPDATE" => Some(roles::TablePrivilege::Update),
                        "DELETE" => Some(roles::TablePrivilege::Delete),
                        _ => None,
                    };
                    owner.as_deref() == Some(name.as_str())
                        || privilege.is_some_and(|privilege| {
                            roles
                                .table_privileges(&name_at_commit, name)
                                .is_ok_and(|privileges| privileges.contains(&privilege))
                        })
                }
            } else {
                true
            }
        } else {
            allowed
        };
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        // Ordinary table/DDL authority precedes implicit default metadata:
        // denied sessions must not discover whether a target table exists.
        let implicit = self.sql.implicit_sequence_dependencies(p)?;
        self.authorize_sequences(&implicit)?;
        Ok(())
    }
    pub fn execute_prepared(
        &mut self,
        p: &Prepared,
        params: &[Value],
    ) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| {
            session.authorize(p)?;
            // Recovery must remain available even after the host clock latches.
            if session.sql.is_aborted() && p.command() == "ROLLBACK" {
                let result = session.sql.execute_prepared(p, params, None);
                if !session.sql.in_transaction() {
                    session.transaction_us = None;
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
            let result = session.sql.execute_prepared_at(p, params, None, &clock);
            if !session.sql.in_transaction() {
                session.transaction_us = None;
            } else if clock.unavailable.is_none() {
                session.transaction_us = Some(transaction_us);
            }
            let result = result?;
            if p.is_ddl()
                && !p.is_alter_table()
                && let (Role::Database { name }, Some(table)) = (&session.role, p.table())
            {
                session
                    .service
                    .admin
                    .roles()
                    .set_table_owner(table, name)
                    .map_err(|_| CoreError::Policy(Error::Scope))?;
            }
            Ok(result)
        })
    }
    pub fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<SqlResult, CoreError> {
        self.authority_scope(|session| session.execute_sql_authorized(sql, params))
    }
    fn execute_sql_authorized(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<SqlResult, CoreError> {
        // Core's aborted-state ROLLBACK escape must remain reachable.
        if self.sql.is_aborted()
            && sql
                .trim()
                .trim_end_matches(';')
                .eq_ignore_ascii_case("rollback")
        {
            let result = self.sql.execute(sql, params, None);
            if !self.sql.in_transaction() {
                self.transaction_us = None;
            }
            return result;
        }
        let p = self.prepare(sql)?;
        self.execute_prepared(&p, params)
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
                Request::Sql { sql, params } => Ok(Response {
                    sql: Some(self.execute_sql(&sql, &params)?),
                    ..Response::code("ok")
                }),
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
        assert!(session.zone.is_none());
        for name in ["UTC", "etc/utc", "GMT", "Z"] {
            session.set_time_zone(name);
            assert!(matches!(session.zone, Some(SessionZone::Fixed(0))));
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
    #[test]
    fn database_role_cannot_be_installed_as_a_fingerprint_binding() {
        let (service, mut bindings) = fixture();
        bindings[0].role = Role::Database {
            name: "owner".into(),
        };
        assert!(matches!(
            Service::validate(&service.admin, bindings.clone()),
            Err(CoreError::Policy(Error::Authentication))
        ));
        assert!(service.reload_bindings(bindings.clone()).is_err());
        assert!(serde_json::to_value(&bindings[0]).is_err());
        assert!(serde_json::from_str::<Role>(r#"{"role":"Database","name":"owner"}"#).is_err());
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
