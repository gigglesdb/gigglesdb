use crate::clock::Clock;
use giggles_core::{
    relational::{Prepared, SqlResult, Value},
    *,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
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
}
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        CoreError::Sql(S::Unsupported | S::Reserved) | CoreError::Unsupported => "0A000",
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
    bindings: BTreeMap<String, Role>,
}
impl Service {
    pub fn new(admin: Admin, bindings: Vec<Binding>, clock: Clock) -> Result<Arc<Self>, CoreError> {
        let domain = admin.authority_metadata()?.0.to_string();
        let mut map = BTreeMap::new();
        let mut owners = BTreeSet::new();
        for b in bindings {
            if hex::decode(&b.fingerprint).is_err() || b.fingerprint.len() != 64 {
                return Err(CoreError::Limit);
            }
            let bound = match &b.role {
                Role::Task { domain, .. } | Role::Application { domain, .. } => domain,
            };
            if bound != &domain {
                return Err(CoreError::Policy(Error::Authentication));
            }
            if let Role::Application { tables, .. } = &b.role {
                if tables.is_empty() {
                    return Err(CoreError::Policy(Error::Scope));
                }
                // Exact one-owner tables; tenant sharing needs a separate policy design.
                for table in tables {
                    if !owners.insert(table.clone()) {
                        return Err(CoreError::Policy(Error::Scope));
                    }
                }
            }
            if map.insert(b.fingerprint, b.role).is_some() {
                return Err(CoreError::Policy(Error::Authentication));
            }
        }
        Ok(Arc::new(Self {
            blocking: crate::blocking::Blocking::new(),
            cancellations: crate::cancel::Registry::default(),
            task_gate: std::sync::Mutex::new(()),
            admin,
            clock,
            bindings: map,
        }))
    }
    pub fn session(self: &Arc<Self>, fingerprint: &str) -> Result<Session, CoreError> {
        let role = self
            .bindings
            .get(fingerprint)
            .cloned()
            .ok_or(CoreError::Policy(Error::Authentication))?;
        Ok(Session {
            service: self.clone(),
            role,
            fingerprint: fingerprint.to_owned(),
            sql: self.admin.sql_session(),
        })
    }
}
pub struct Session {
    pub(crate) service: Arc<Service>,
    pub(crate) role: Role,
    pub(crate) fingerprint: String,
    pub(crate) sql: SqlSession,
}
impl Session {
    pub fn abort(&mut self) {
        self.sql.abort();
    }
    pub fn in_transaction(&self) -> bool {
        self.sql.in_transaction()
    }
    pub fn is_aborted(&self) -> bool {
        self.sql.is_aborted()
    }
    pub fn prepare(&mut self, sql: &str) -> Result<Prepared, CoreError> {
        let p = self.sql.prepare(sql)?;
        self.authorize(&p)?;
        Ok(p)
    }
    fn authorize(&mut self, p: &Prepared) -> Result<(), CoreError> {
        let allowed = matches!(&self.role, Role::Application { tables,.. }
            if !p.is_ddl() && p.table().is_none_or(|t| tables.contains(t)));
        if !allowed {
            self.sql.abort();
            return Err(CoreError::Policy(Error::Scope));
        }
        Ok(())
    }
    pub fn execute_prepared(
        &mut self,
        p: &Prepared,
        params: &[Value],
    ) -> Result<SqlResult, CoreError> {
        self.authorize(p)?;
        self.sql.execute_prepared(p, params, None)
    }
    pub fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<SqlResult, CoreError> {
        // Core's aborted-state ROLLBACK escape must remain reachable.
        if self.sql.is_aborted()
            && sql
                .trim()
                .trim_end_matches(';')
                .eq_ignore_ascii_case("rollback")
        {
            return self.sql.execute(sql, params, None);
        }
        let p = self.prepare(sql)?;
        self.execute_prepared(&p, params)
    }
    fn executor(&self) -> Result<(Executor, (u64, u64)), CoreError> {
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
    pub fn handle_received(&mut self, request: Request, received: std::time::Instant) -> Response {
        let context = Cancellation::current().unwrap_or_default();
        context.scope(|| self.handle_scoped(request, received, &context))
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
