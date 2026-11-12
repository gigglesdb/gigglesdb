//! Resumable D11 source fence. The caller durably journals each intent before
//! execution and retains the returned state. This module does not activate a
//! destination, resolve prepared transactions, or emit the final cutover marker.
//!
//! Connections must execute a query as one batch, fail on any SQL error, roll
//! back a failed batch, and enforce the supplied wall-clock timeout. Use a
//! dedicated administrative connection outside the application-role scope.
//! Run the DDL capture trigger install before fencing; it provisions metadata.

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    fmt, thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::event::{AssembledTxn, EventError, Position, PositionOrder};

pub type Rows = Vec<Vec<String>>;

const FENCE_METADATA_SQL: &str = "SELECT to_regclass('_skedaddle.fence_state') IS NOT NULL AND to_regclass('_skedaddle.authority') IS NOT NULL AND to_regclass('_skedaddle.effects') IS NOT NULL";

pub mod inventory;

/// A connector adapter supplies the existing authenticated PG transport.
pub trait Connection {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<Rows, String>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    PreFence,
    Save,
    NoLogin,
    Revoke,
    Terminate,
    Drain,
    Recheck,
    Authority,
    Complete,
}

impl Step {
    fn next(self) -> Self {
        match self {
            Self::PreFence => Self::Save,
            Self::Save => Self::NoLogin,
            Self::NoLogin => Self::Revoke,
            Self::Revoke => Self::Terminate,
            Self::Terminate => Self::Drain,
            Self::Drain => Self::Recheck,
            Self::Recheck => Self::Authority,
            Self::Authority | Self::Complete => Self::Complete,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub owner: String,
}

impl Table {
    fn sql(&self) -> String {
        format!("{}.{}", ident(&self.schema), ident(&self.name))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub login: bool,
    pub superuser: bool,
    pub bypass_rls: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub member: String,
    pub role: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Grantee {
    Public,
    Role(String),
}

impl Grantee {
    fn sql(&self) -> String {
        match self {
            Self::Public => "PUBLIC".into(),
            Self::Role(name) => ident(name),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Privilege {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Privilege {
    fn sql(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Grant {
    pub table: Table,
    pub column: Option<String>,
    pub grantee: Grantee,
    pub grantor: String,
    pub privilege: Privilege,
    pub grant_option: bool,
}

impl Grant {
    fn privilege_sql(&self) -> String {
        match &self.column {
            Some(column) => format!("{} ({})", self.privilege.sql(), ident(column)),
            None => self.privilege.sql().into(),
        }
    }

    fn restore_sql(&self) -> String {
        format!(
            "SET LOCAL ROLE {}; GRANT {} ON TABLE {} TO {}{}; RESET ROLE; ",
            ident(&self.grantor),
            self.privilege_sql(),
            self.table.sql(),
            self.grantee.sql(),
            if self.grant_option {
                " WITH GRANT OPTION"
            } else {
                ""
            }
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub roles: Vec<Role>,
    pub memberships: Vec<Membership>,
    pub tables: Vec<Table>,
    /// All write grants, including column grants and downstream grants that a
    /// CASCADE revoke might remove. SELECT and other privileges are untouched.
    pub grants: Vec<Grant>,
}

/// Conservatively includes both inherited and SET ROLE paths, even NOINHERIT.
/// Ignoring membership option restrictions may over-fence, never under-fence.
pub fn reachable_roles(role: &str, memberships: &[Membership]) -> BTreeSet<String> {
    let mut reached = BTreeSet::from([role.to_owned()]);
    loop {
        let before = reached.len();
        for membership in memberships {
            if reached.contains(&membership.member) {
                reached.insert(membership.role.clone());
            }
        }
        if reached.len() == before {
            return reached;
        }
    }
}

pub fn effective_writers(catalog: &Catalog) -> BTreeSet<String> {
    catalog
        .roles
        .iter()
        .filter(|role| {
            let reached = reachable_roles(&role.name, &catalog.memberships);
            catalog
                .roles
                .iter()
                .any(|r| r.superuser && reached.contains(&r.name))
                || reached.contains("pg_write_all_data")
                || catalog.tables.iter().any(|t| reached.contains(&t.owner))
                || catalog.grants.iter().any(|grant| match &grant.grantee {
                    Grantee::Public => true,
                    Grantee::Role(name) => reached.contains(name),
                })
        })
        .map(|r| r.name.clone())
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ResidualKind {
    Superuser,
    TableOwner,
    BypassRls,
    OutOfScopeWriter,
    InScopeWriter,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResidualPrincipal {
    pub role: String,
    pub kind: ResidualKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Residual {
    pub principal: ResidualPrincipal,
    pub acknowledged: bool,
}

pub fn classify_residuals(
    catalog: &Catalog,
    applications: &BTreeSet<String>,
    acknowledged: &BTreeSet<ResidualPrincipal>,
) -> Vec<Residual> {
    let writers = effective_writers(catalog);
    let mut result = Vec::new();
    for role in &catalog.roles {
        let reached = reachable_roles(&role.name, &catalog.memberships);
        let mut kinds = BTreeSet::new();
        if catalog
            .roles
            .iter()
            .any(|r| r.superuser && reached.contains(&r.name))
        {
            kinds.insert(ResidualKind::Superuser);
        }
        if catalog.tables.iter().any(|t| reached.contains(&t.owner)) {
            kinds.insert(ResidualKind::TableOwner);
        }
        if role.bypass_rls {
            kinds.insert(ResidualKind::BypassRls);
        }
        if writers.contains(&role.name) {
            kinds.insert(if applications.contains(&role.name) {
                ResidualKind::InScopeWriter
            } else {
                ResidualKind::OutOfScopeWriter
            });
        }
        for kind in kinds {
            let principal = ResidualPrincipal {
                role: role.name.clone(),
                kind,
            };
            result.push(Residual {
                acknowledged: acknowledged.contains(&principal),
                principal,
            });
        }
    }
    result
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub epoch: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedState {
    pub catalog: Catalog,
    pub authority: Option<Authority>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceConfig {
    pub epoch: i64,
    pub applications: BTreeSet<String>,
    pub tables: Vec<Table>,
    pub acknowledged: BTreeSet<ResidualPrincipal>,
    pub deadline_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    /// Coordinator migration namespace, supplied by the definition/cutover key.
    #[serde(default)]
    pub migration: String,
    pub config: FenceConfig,
    pub next: Step,
    pub saved: Option<SavedState>,
    pub drain_lsn: Option<u64>,
    pub residuals: Vec<Residual>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    Step(Box<Fence>),
    /// Includes the original state: journal this durably before saving it in PG.
    Save(Box<Fence>),
    Unfence(Box<Fence>),
}

#[derive(Debug)]
pub enum FenceError {
    Invalid(String),
    InstallRequired(&'static str),
    Database(String),
    Journal(String),
    Hook(String),
    DrainBlocked(Rows),
    ResidualBlocked(Vec<Residual>),
}

impl fmt::Display for FenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "source fence: {self:?}")
    }
}

impl std::error::Error for FenceError {}

impl Fence {
    pub fn new(config: FenceConfig) -> Result<Self, FenceError> {
        if config.epoch < 1
            || config.applications.is_empty()
            || config.tables.is_empty()
            || config.deadline_ms == 0
        {
            return Err(FenceError::Invalid(
                "positive epoch/deadline, roles and tables required".into(),
            ));
        }
        Ok(Self {
            migration: String::new(),
            config,
            next: Step::PreFence,
            saved: None,
            drain_lsn: None,
            residuals: vec![],
        })
    }

    /// Bind before journaling any fence step; retries cannot change namespace.
    pub fn bind_migration(&mut self, migration: &str) -> Result<(), FenceError> {
        if migration.is_empty() || (!self.migration.is_empty() && self.migration != migration) {
            return Err(FenceError::Invalid(
                "missing or conflicting migration".into(),
            ));
        }
        self.migration = migration.into();
        Ok(())
    }

    /// Runs one step. A failed operation leaves `next` unchanged. Replaying an
    /// intent is safe; pre-fence hooks must themselves support retry/idempotency.
    pub fn execute_next(
        &mut self,
        connection: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
        pre_fence: &mut impl FnMut() -> Result<(), String>,
    ) -> Result<(), FenceError> {
        if self.next == Step::Complete {
            return Ok(());
        }
        self.validate_resume()?;
        journal(&Intent::Step(Box::new(self.clone()))).map_err(FenceError::Journal)?;
        let timeout = Duration::from_millis(self.config.deadline_ms);
        match self.next {
            Step::PreFence => pre_fence().map_err(FenceError::Hook)?,
            Step::Save => self.save(connection, journal, timeout)?,
            Step::NoLogin => {
                let sql =
                    self.administrative_transaction(connection, &self.no_login_sql(), timeout)?;
                query(connection, &sql, timeout)?;
            }
            Step::Revoke => {
                let sql =
                    self.administrative_transaction(connection, &self.revoke_sql()?, timeout)?;
                query(connection, &sql, timeout)?;
            }
            Step::Terminate => {
                query(
                    connection,
                    &format!(
                        "SELECT pid::text, pg_terminate_backend(pid)::text FROM pg_stat_activity WHERE datname=current_database() AND usename IN ({}) AND pid<>pg_backend_pid()",
                        names(&self.config.applications)
                    ),
                    timeout,
                )?;
            }
            Step::Drain => self.drain(connection, timeout)?,
            Step::Recheck => {
                let catalog = read_catalog(connection, &self.config.tables, timeout)?;
                self.residuals = classify_residuals(
                    &catalog,
                    &self.config.applications,
                    &self.config.acknowledged,
                );
                if self.residuals.iter().any(|r| !r.acknowledged) {
                    return Err(FenceError::ResidualBlocked(self.residuals.clone()));
                }
            }
            Step::Authority => {
                // The final privilege statement and authority/epoch are atomic.
                let sql = format!(
                    "{} {} {}",
                    self.no_login_sql(),
                    self.revoke_sql()?,
                    authority_sql(&self.migration, self.config.epoch)
                );
                let sql = self.administrative_transaction(connection, &sql, timeout)?;
                query(connection, &sql, timeout)?;
            }
            Step::Complete => unreachable!(),
        }
        self.next = self.next.next();
        Ok(())
    }

    fn validate_resume(&self) -> Result<(), FenceError> {
        if self.migration.is_empty() {
            return Err(FenceError::Invalid(
                "migration must be bound before fencing".into(),
            ));
        }
        if !matches!(self.next, Step::PreFence | Step::Save) && self.saved.is_none() {
            return Err(FenceError::Invalid(
                "resume requires original saved state".into(),
            ));
        }
        if matches!(self.next, Step::Recheck | Step::Authority) && self.drain_lsn.is_none() {
            return Err(FenceError::Invalid("resume requires drain point".into()));
        }
        if self.next == Step::Authority && self.residuals.iter().any(|r| !r.acknowledged) {
            return Err(FenceError::ResidualBlocked(self.residuals.clone()));
        }
        Ok(())
    }

    fn save(
        &mut self,
        connection: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
        timeout: Duration,
    ) -> Result<(), FenceError> {
        let metadata = query(connection, FENCE_METADATA_SQL, timeout)?;
        if !metadata
            .first()
            .and_then(|row| row.first())
            .is_some_and(|v| v == "t" || v == "true")
        {
            return Err(FenceError::InstallRequired(
                "missing fence_state, authority or effects: run DDL capture trigger install before capture",
            ));
        }
        let stored = query(
            connection,
            &format!(
                "SELECT state FROM _skedaddle.fence_state WHERE migration={} AND epoch={}",
                literal(&self.migration),
                self.config.epoch
            ),
            timeout,
        )?;
        if let Some(row) = stored.first() {
            let original: Fence = serde_json::from_str(cell(row, 0)?)
                .map_err(|e| FenceError::Invalid(e.to_string()))?;
            if original.migration != self.migration
                || original.config != self.config
                || original.saved.is_none()
            {
                return Err(FenceError::Invalid(
                    "epoch already belongs to a different fence".into(),
                ));
            }
            self.saved = original.saved;
        } else if self.saved.is_none() {
            let catalog = read_catalog(connection, &self.config.tables, timeout)?;
            for name in &self.config.applications {
                if !catalog.roles.iter().any(|r| &r.name == name) {
                    return Err(FenceError::Invalid(format!(
                        "application role missing: {name}"
                    )));
                }
            }
            let rows = query(
                connection,
                &format!(
                    "SELECT epoch::text FROM _skedaddle.authority WHERE migration={}",
                    literal(&self.migration)
                ),
                timeout,
            )?;
            let authority = rows
                .first()
                .map(|r| {
                    Ok(Authority {
                        epoch: number(cell(r, 0)?)?,
                    })
                })
                .transpose()?;
            self.saved = Some(SavedState { catalog, authority });
        }
        journal(&Intent::Save(Box::new(self.clone()))).map_err(FenceError::Journal)?;
        let encoded =
            serde_json::to_string(self).map_err(|e| FenceError::Invalid(e.to_string()))?;
        query(
            connection,
            &format!(
                "INSERT INTO _skedaddle.fence_state(migration,epoch,state) VALUES({},{},{}) ON CONFLICT(migration,epoch) DO NOTHING",
                literal(&self.migration),
                self.config.epoch,
                literal(&encoded)
            ),
            timeout,
        )?;
        // A concurrent save must never silently replace this epoch's original.
        let rows = query(
            connection,
            &format!(
                "SELECT state FROM _skedaddle.fence_state WHERE migration={} AND epoch={}",
                literal(&self.migration),
                self.config.epoch
            ),
            timeout,
        )?;
        let stored: Fence = serde_json::from_str(cell(
            rows.first()
                .ok_or_else(|| FenceError::Invalid("saved fence missing".into()))?,
            0,
        )?)
        .map_err(|e| FenceError::Invalid(e.to_string()))?;
        if stored.migration != self.migration
            || stored.config != self.config
            || stored.saved != self.saved
        {
            return Err(FenceError::Invalid("conflicting saved fence state".into()));
        }
        Ok(())
    }

    /// Share the capture-installation lookup with every metadata DDL producer.
    fn administrative_transaction(
        &self,
        connection: &mut impl Connection,
        sql: &str,
        timeout: Duration,
    ) -> Result<String, FenceError> {
        let origin = super::ddl::administrative_origin(connection, timeout)
            .map_err(|error| FenceError::Invalid(error.0))?;
        Ok(transaction(&format!("{origin}{sql}"), timeout))
    }

    fn no_login_sql(&self) -> String {
        self.config
            .applications
            .iter()
            .map(|role| format!("ALTER ROLE {} NOLOGIN; ", ident(role)))
            .collect()
    }

    fn revoke_sql(&self) -> Result<String, FenceError> {
        let saved = self
            .saved
            .as_ref()
            .ok_or_else(|| FenceError::Invalid("saved state missing".into()))?;
        let reached: BTreeSet<_> = self
            .config
            .applications
            .iter()
            .flat_map(|r| reachable_roles(r, &saved.catalog.memberships))
            .collect();
        // Revoke as the original grantor, retaining accurate grant chains for
        // unfence. CASCADE's downstream removals are covered by the full save.
        Ok(saved
            .catalog
            .grants
            .iter()
            .filter(|grant| match &grant.grantee {
                Grantee::Public => true,
                Grantee::Role(role) => reached.contains(role),
            })
            .map(|grant| {
                format!(
                    "SET LOCAL ROLE {}; REVOKE {} ON TABLE {} FROM {} CASCADE; RESET ROLE; ",
                    ident(&grant.grantor),
                    grant.privilege_sql(),
                    grant.table.sql(),
                    grant.grantee.sql()
                )
            })
            .collect())
    }

    fn drain(
        &mut self,
        connection: &mut impl Connection,
        timeout: Duration,
    ) -> Result<(), FenceError> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(FenceError::DrainBlocked(vec![vec![
                    "deadline expired".into(),
                ]]));
            }
            let rows = query(
                connection,
                &format!(
                    "SELECT 'backend',pid::text,usename FROM pg_stat_activity WHERE datname=current_database() AND usename IN ({}) UNION ALL SELECT 'prepared',gid,owner::text FROM pg_prepared_xacts WHERE database=current_database()",
                    names(&self.config.applications)
                ),
                remaining,
            )?;
            if rows.is_empty() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(FenceError::DrainBlocked(vec![vec![
                        "deadline expired".into(),
                    ]]));
                }
                let rows = query(
                    connection,
                    "SELECT pg_current_wal_insert_lsn()::text",
                    remaining,
                )?;
                self.drain_lsn = Some(parse_lsn(cell(
                    rows.first()
                        .ok_or_else(|| FenceError::Invalid("drain LSN missing".into()))?,
                    0,
                )?)?);
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(FenceError::DrainBlocked(rows));
            }
            thread::sleep(
                Duration::from_millis(25).min(deadline.saturating_duration_since(Instant::now())),
            );
            if Instant::now() >= deadline {
                return Err(FenceError::DrainBlocked(rows));
            }
        }
    }

    /// Restores original login flags, all removed write grants (including
    /// grantors/options/columns). Authority epochs never rewind, including aborts.
    /// Keeps the original snapshot for retries.
    /// Call only before activation; activation eligibility belongs to the caller.
    pub fn unfence(
        &self,
        connection: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
    ) -> Result<(), FenceError> {
        let Some(saved) = &self.saved else {
            return Ok(());
        };
        if self.migration.is_empty() {
            return Err(FenceError::Invalid(
                "migration must be bound before unfencing".into(),
            ));
        }
        journal(&Intent::Unfence(Box::new(self.clone()))).map_err(FenceError::Journal)?;
        let timeout = Duration::from_millis(self.config.deadline_ms);
        let sql = format!(
            "{} {}",
            authority_sql(&self.migration, self.config.epoch),
            restoration_sql(saved, &self.config.applications)?
        );
        let sql = self.administrative_transaction(connection, &sql, timeout)?;
        query(connection, &sql, timeout)?;
        let current = read_catalog(connection, &self.config.tables, timeout)?;
        let mut actual = current.grants;
        let mut expected = saved.catalog.grants.clone();
        actual.sort();
        expected.sort();
        if actual != expected
            || saved
                .catalog
                .roles
                .iter()
                .filter(|r| self.config.applications.contains(&r.name))
                .any(|r| !current.roles.contains(r))
        {
            return Err(FenceError::Invalid(
                "unfence verification differs from saved state".into(),
            ));
        }
        Ok(())
    }
}

/// Restore grant-option providers before dependent grants. A cycle without an
/// owner/superuser root is rejected instead of guessing or changing grantors.
pub fn restoration_sql(
    saved: &SavedState,
    applications: &BTreeSet<String>,
) -> Result<String, FenceError> {
    let mut sql = String::new();
    let mut pending = saved.catalog.grants.clone();
    let mut ready: Vec<Grant> = Vec::new();
    while !pending.is_empty() {
        let Some(index) = pending.iter().position(|g| {
            g.grantor == g.table.owner
                || saved.catalog.roles.iter().any(|r| r.name == g.grantor && r.superuser)
                || ready.iter().any(|p| p.table == g.table && p.privilege == g.privilege && p.grant_option
                    && (p.column.is_none() || p.column == g.column)
                    && matches!(&p.grantee, Grantee::Role(r) if reachable_roles(&g.grantor, &saved.catalog.memberships).contains(r)))
        }) else {
            return Err(FenceError::Invalid("write grant chain has no restorable grantor".into()));
        };
        let grant = pending.remove(index);
        sql.push_str(&grant.restore_sql());
        ready.push(grant);
    }
    for role in &saved.catalog.roles {
        if applications.contains(&role.name) {
            sql.push_str(&format!(
                "ALTER ROLE {} {}; ",
                ident(&role.name),
                if role.login { "LOGIN" } else { "NOLOGIN" }
            ));
        }
    }
    Ok(sql)
}

fn read_catalog(
    connection: &mut impl Connection,
    tables: &[Table],
    timeout: Duration,
) -> Result<Catalog, FenceError> {
    let selected = tables
        .iter()
        .map(|t| format!("{}::regclass", literal(&t.sql())))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT 'role',rolname,rolcanlogin::text,rolsuper::text,rolbypassrls::text,'','','','' FROM pg_roles \
         UNION ALL SELECT 'member',pg_get_userbyid(member),pg_get_userbyid(roleid),'','','','','','' FROM pg_auth_members \
         UNION ALL SELECT 'member',pg_get_userbyid(datdba),'pg_database_owner','','','','','','' FROM pg_database WHERE datname=current_database() \
         UNION ALL SELECT 'table',n.nspname,c.relname,pg_get_userbyid(c.relowner),'','','','','' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid IN ({selected}) \
         UNION ALL SELECT 'grant',n.nspname,c.relname,pg_get_userbyid(c.relowner),'',CASE WHEN x.grantee=0 THEN 'public' ELSE 'role' END,CASE WHEN x.grantee=0 THEN '' ELSE pg_get_userbyid(x.grantee) END,pg_get_userbyid(x.grantor),x.privilege_type||':'||x.is_grantable::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) x WHERE c.oid IN ({selected}) AND x.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE') \
         UNION ALL SELECT 'grant',n.nspname,c.relname,pg_get_userbyid(c.relowner),a.attname,CASE WHEN x.grantee=0 THEN 'public' ELSE 'role' END,CASE WHEN x.grantee=0 THEN '' ELSE pg_get_userbyid(x.grantee) END,pg_get_userbyid(x.grantor),x.privilege_type||':'||x.is_grantable::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped CROSS JOIN LATERAL aclexplode(a.attacl) x WHERE c.oid IN ({selected}) AND x.privilege_type IN ('INSERT','UPDATE')"
    );
    let mut catalog = Catalog::default();
    for row in query(connection, &sql, timeout)? {
        match cell(&row, 0)? {
            "role" => catalog.roles.push(Role {
                name: cell(&row, 1)?.into(),
                login: boolean(cell(&row, 2)?)?,
                superuser: boolean(cell(&row, 3)?)?,
                bypass_rls: boolean(cell(&row, 4)?)?,
            }),
            "member" => catalog.memberships.push(Membership {
                member: cell(&row, 1)?.into(),
                role: cell(&row, 2)?.into(),
            }),
            "table" => catalog.tables.push(table_row(&row)?),
            "grant" => {
                let (privilege, option) = cell(&row, 8)?
                    .split_once(':')
                    .ok_or_else(|| FenceError::Invalid("invalid ACL result".into()))?;
                catalog.grants.push(Grant {
                    table: table_row(&row)?,
                    column: (!cell(&row, 4)?.is_empty()).then(|| row[4].clone()),
                    grantee: if cell(&row, 5)? == "public" {
                        Grantee::Public
                    } else {
                        Grantee::Role(cell(&row, 6)?.into())
                    },
                    grantor: cell(&row, 7)?.into(),
                    privilege: match privilege {
                        "INSERT" => Privilege::Insert,
                        "UPDATE" => Privilege::Update,
                        "DELETE" => Privilege::Delete,
                        "TRUNCATE" => Privilege::Truncate,
                        _ => return Err(FenceError::Invalid("unknown write privilege".into())),
                    },
                    grant_option: boolean(option)?,
                });
            }
            _ => return Err(FenceError::Invalid("unknown catalog row".into())),
        }
    }
    if catalog.tables.len() != tables.len() {
        return Err(FenceError::Invalid(
            "selected table missing or duplicated".into(),
        ));
    }
    Ok(catalog)
}

fn table_row(row: &[String]) -> Result<Table, FenceError> {
    Ok(Table {
        schema: cell(row, 1)?.into(),
        name: cell(row, 2)?.into(),
        owner: cell(row, 3)?.into(),
    })
}

fn cell(row: &[String], index: usize) -> Result<&str, FenceError> {
    row.get(index)
        .map(String::as_str)
        .ok_or_else(|| FenceError::Invalid("short query row".into()))
}

fn boolean(value: &str) -> Result<bool, FenceError> {
    match value {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(FenceError::Invalid("invalid boolean".into())),
    }
}

fn number(value: &str) -> Result<i64, FenceError> {
    value
        .parse()
        .map_err(|_| FenceError::Invalid("invalid epoch".into()))
}

fn query(
    connection: &mut impl Connection,
    sql: &str,
    timeout: Duration,
) -> Result<Rows, FenceError> {
    connection.query(sql, timeout).map_err(FenceError::Database)
}

fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
/// Same insert-at-zero and monotonic UPDATE gate as coordinator/epoch.rs's
/// apply_pg_effect_inner. This transport executes SQL batches rather than a
/// tokio-postgres transaction, so raise on a rejected UPDATE to roll back the
/// entire batch, including the final privilege statements.
fn authority_sql(migration: &str, epoch: i64) -> String {
    let migration = literal(migration);
    let body = format!(
        "BEGIN INSERT INTO _skedaddle.authority VALUES ({migration},0) ON CONFLICT DO NOTHING; UPDATE _skedaddle.authority SET epoch={epoch} WHERE migration={migration} AND epoch <= {epoch}; IF NOT FOUND THEN RAISE EXCEPTION 'stale source fence epoch'; END IF; END;"
    );
    format!("DO LANGUAGE plpgsql {};", literal(&body))
}
fn names(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|v| literal(v))
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

pub fn parse_lsn(value: &str) -> Result<u64, FenceError> {
    let (hi, lo) = value
        .split_once('/')
        .ok_or_else(|| FenceError::Invalid("invalid PG LSN".into()))?;
    let hi =
        u32::from_str_radix(hi, 16).map_err(|_| FenceError::Invalid("invalid PG LSN".into()))?;
    let lo =
        u32::from_str_radix(lo, 16).map_err(|_| FenceError::Invalid("invalid PG LSN".into()))?;
    Ok((u64::from(hi) << 32) | u64::from(lo))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FenceWindow {
    BeforeActivation,
    RollbackWindow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Breach {
    None,
    AbortBeforeActivation,
    CriticalAlert { rollback_eligible: bool },
}

/// Capture must include all origins. Only this stream's exact trusted Skedaddle
/// origin is exempt, never an arbitrary nonempty origin or a name prefix.
pub fn detect_breach(
    transaction: &AssembledTxn,
    drain: &Position,
    order: &(impl PositionOrder + ?Sized),
    skedaddle_origin: &str,
    window: FenceWindow,
) -> Result<Breach, EventError> {
    if transaction.commit_position.compare(drain, order)? != Ordering::Greater
        || transaction
            .source_origin
            .as_ref()
            .is_some_and(|o| !skedaddle_origin.is_empty() && o.name == skedaddle_origin)
    {
        return Ok(Breach::None);
    }
    Ok(match window {
        FenceWindow::BeforeActivation => Breach::AbortBeforeActivation,
        FenceWindow::RollbackWindow => Breach::CriticalAlert {
            rollback_eligible: false,
        },
    })
}

#[cfg(test)]
mod tests;
