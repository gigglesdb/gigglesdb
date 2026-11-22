//! What a task session carries (GD-0061 R3-202, design-2 section 3).
//!
//! `open_context` and `task_standing` read task definitions through the narrow
//! `TaskDefinitions` seam. `SqlSession` implements it with the committed task catalog
//! (R3-105); on the legacy engine there are no tasks, so every open is `Revoked`.

use super::definition::{RoleName, TaskDef, TaskName, TaskObjectId, WallClockMs};
use super::error::TaskError;
use crate::storage::SqlSession;
use crate::storage::CoreError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskContext {
    pub task: TaskObjectId,
    pub name: TaskName,
    pub executor: RoleName,
    pub bound_def_version: u64,
    pub credential_fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    Live,
    Expired,
}

/// Seam for the catalog load. Returns the current definition, revoked or not.
pub trait TaskDefinitions {
    fn task_definition(&self, name: &TaskName) -> Result<Option<TaskDef>, TaskError>;
}

impl TaskDefinitions for SqlSession {
    fn task_definition(&self, name: &TaskName) -> Result<Option<TaskDef>, TaskError> {
        let def = self.tasks().lookup(name.as_str())?;
        Ok(def.map(|def| def.as_ref().clone()))
    }
}

pub fn open_context(
    sql: &SqlSession,
    name: &TaskName,
    executor: &RoleName,
    fingerprint: [u8; 32],
    now: WallClockMs,
    outcome_retention_ms: u64,
) -> Result<TaskContext, TaskError> {
    open_context_from(sql, name, executor, fingerprint, now, outcome_retention_ms)
}

pub(crate) fn open_context_from(
    defs: &dyn TaskDefinitions,
    name: &TaskName,
    executor: &RoleName,
    fingerprint: [u8; 32],
    now: WallClockMs,
    outcome_retention_ms: u64,
) -> Result<TaskContext, TaskError> {
    let def = defs.task_definition(name)?.ok_or(TaskError::Revoked)?;
    if def.revoked {
        return Err(TaskError::Revoked);
    }
    if &def.executor != executor {
        return Err(TaskError::Revoked);
    }
    let retention = i64::try_from(outcome_retention_ms).unwrap_or(i64::MAX);
    if now.0 >= def.expires_at.0.saturating_add(retention) {
        return Err(TaskError::Expired);
    }
    Ok(TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: fingerprint,
    })
}

pub(crate) fn standing_from(
    defs: &dyn TaskDefinitions,
    ctx: &TaskContext,
    now: WallClockMs,
) -> Result<Standing, TaskError> {
    let def = match defs.task_definition(&ctx.name)? {
        Some(def) if !def.revoked && def.id == ctx.task => def,
        _ => return Err(TaskError::Revoked),
    };
    if def.def_version != ctx.bound_def_version {
        return Err(TaskError::Denied(super::error::DenyRule::DefinitionChanged));
    }
    if now.0 >= def.expires_at.0 {
        return Ok(Standing::Expired);
    }
    Ok(Standing::Live)
}

impl SqlSession {
    /// Binds the session to a task, once. A second call is `CoreError::Fenced`.
    pub fn bind_task(&mut self, ctx: TaskContext) -> Result<(), CoreError> {
        if self.task.is_some() {
            return Err(CoreError::Fenced);
        }
        // Task sessions run on a local v2 database only: a cluster gateway
        // (or a replicated store) is 0A000 before anything is bound.
        if self.is_v2_engine() {
            let db = self.v2_lease(crate::storage::Holder::TaskRun)?;
            if db.task_kernel_unsupported() {
                return Err(CoreError::Unsupported);
            }
            self.task_generation = Some(db.tag());
        }
        self.task_state = Some(std::sync::Arc::new(
            super::release::TaskSessionState::new(ctx.clone()),
        ));
        self.task = Some(ctx);
        Ok(())
    }

    pub fn task(&self) -> Option<&TaskContext> {
        self.task.as_ref()
    }

    /// Admission-time standing. The caller maps `Expired` to a refusal except for
    /// `OutcomeLookup`, `Show` and `Set`. A session with no task is `Live`.
    pub fn task_standing(&self, now: WallClockMs) -> Result<Standing, TaskError> {
        match &self.task {
            None => Ok(Standing::Live),
            Some(ctx) => standing_from(self, ctx, now),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct Fake(RefCell<Option<TaskDef>>);
    impl TaskDefinitions for Fake {
        fn task_definition(&self, name: &TaskName) -> Result<Option<TaskDef>, TaskError> {
            Ok(self.0.borrow().clone().filter(|d| &d.name == name))
        }
    }

    // A task over the worked example's columns. `deliveries` is reserved
    // (relational/ident.rs), so the table is `shipments(id, merchant_id, status,
    // failed_at, attempt_count, customer_email, card_token)`; only its id matters here.
    fn def() -> TaskDef {
        use crate::task::definition::{
            ColumnMode, DisclosureCaps, LoadLimits, MutationCaps, PredicateText,
        };
        use std::collections::{BTreeMap, BTreeSet};
        let mut columns: BTreeMap<String, ColumnMode> = ["id", "status", "failed_at", "attempt_count"]
            .iter()
            .map(|c| (c.to_string(), ColumnMode::Readable))
            .collect();
        columns.insert("customer_email".into(), ColumnMode::Masked { constant: None });
        TaskDef {
            id: TaskObjectId(9),
            name: name(),
            def_version: 3,
            issuer: "incident_controller".into(),
            executor: "agent_exec".into(),
            approver: None,
            table: 16,
            visible_rows: PredicateText("merchant_id = 42".into()),
            columns,
            untrusted_allowed: BTreeSet::new(),
            allowed_write: None,
            mutation_caps: MutationCaps::default(),
            disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
            limits: LoadLimits::default(),
            approval_threshold: 0,
            expires_at: WallClockMs(1_000),
            revoked: false,
            grants: BTreeMap::new(),
        }
    }

    /// A SQL session on a fresh database (v2 when compiled in).
    fn session() -> SqlSession {
        let root = crate::test_artifact_root();
        std::fs::create_dir_all(&root).unwrap();
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = root.join(format!("task-context-{:032x}", u128::from_be_bytes(token)));
        #[cfg(feature = "engine-rocksdb")]
        let engine = crate::storage::Engine::V2;
        #[cfg(not(feature = "engine-rocksdb"))]
        let engine = crate::storage::Engine::Legacy;
        crate::storage::Admin::create_engine(&path, crate::storage::Limits::default(), engine)
            .unwrap()
            .sql_session()
    }

    fn name() -> TaskName {
        TaskName::new("inc-436").unwrap()
    }

    #[test]
    fn open_context_refuses_wrong_executor_revoked_past_retention() {
        let f = Fake(RefCell::new(Some(def())));
        let ok = open_context_from(&f, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(500), 100)
            .unwrap();
        assert_eq!(ok.bound_def_version, 3);
        assert_eq!(ok.task, TaskObjectId(9));
        // Expired but within retention still opens.
        assert!(open_context_from(&f, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(1_050), 100).is_ok());
        assert!(matches!(
            open_context_from(&f, &name(), &"other".to_string(), [1; 32], WallClockMs(500), 100),
            Err(TaskError::Revoked)
        ));
        assert!(matches!(
            open_context_from(&f, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(1_100), 100),
            Err(TaskError::Expired)
        ));
        f.0.borrow_mut().as_mut().unwrap().revoked = true;
        assert!(matches!(
            open_context_from(&f, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(500), 100),
            Err(TaskError::Revoked)
        ));
        let missing = Fake(RefCell::new(None));
        assert!(matches!(
            open_context_from(&missing, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(500), 100),
            Err(TaskError::Revoked)
        ));
    }

    #[test]
    fn standing_revoked_expired_definition_changed() {
        let f = Fake(RefCell::new(Some(def())));
        let ctx = open_context_from(&f, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(0), 0).unwrap();
        assert_eq!(standing_from(&f, &ctx, WallClockMs(10)).unwrap(), Standing::Live);
        assert_eq!(standing_from(&f, &ctx, WallClockMs(1_000)).unwrap(), Standing::Expired);
        f.0.borrow_mut().as_mut().unwrap().def_version = 4;
        assert!(matches!(
            standing_from(&f, &ctx, WallClockMs(10)),
            Err(TaskError::Denied(crate::task::error::DenyRule::DefinitionChanged))
        ));
        f.0.borrow_mut().as_mut().unwrap().revoked = true;
        assert!(matches!(standing_from(&f, &ctx, WallClockMs(10)), Err(TaskError::Revoked)));
    }

    #[test]
    fn bind_task_twice_fails() {
        let f = Fake(RefCell::new(Some(def())));
        let ctx = open_context_from(&f, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(0), 0).unwrap();
        let mut sql = session();
        assert!(sql.task().is_none());
        sql.bind_task(ctx.clone()).unwrap();
        assert_eq!(sql.task(), Some(&ctx));
        assert!(matches!(sql.bind_task(ctx.clone()), Err(CoreError::Fenced)));
        assert_eq!(sql.task(), Some(&ctx));
        // No task of that name exists in this database, so the bound task reads as revoked.
        assert!(matches!(sql.task_standing(WallClockMs(0)), Err(TaskError::Revoked)));
        assert!(matches!(
            open_context(&sql, &name(), &"agent_exec".to_string(), [1; 32], WallClockMs(0), 0),
            Err(TaskError::Revoked)
        ));
        assert_eq!(session().task_standing(WallClockMs(0)).unwrap(), Standing::Live);
    }

    /// The session seam reads the real task catalog: a created task binds and
    /// stays live; once revoked, and again once purged, it reads as revoked.
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn created_task_binds_and_dropped_task_is_revoked() {
        use crate::storage::roles::{NewRole, TablePrivilege};
        use crate::task::definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText};
        let root = crate::test_artifact_root();
        std::fs::create_dir_all(&root).unwrap();
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = root.join(format!("task-context-db-{:032x}", u128::from_be_bytes(token)));
        let admin = crate::storage::Admin::create_engine(
            &path,
            crate::storage::Limits::default(),
            crate::storage::Engine::V2,
        )
        .unwrap();
        let sql = admin.sql_session();
        admin
            .sql_session()
            .execute("CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)", &[], None)
            .unwrap();
        let roles = admin.roles();
        for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
            roles.create(NewRole { name: name.into(), login: true, ..NewRole::default() }).unwrap();
            if task_only {
                roles.set_task_only(name, true).unwrap();
            }
        }
        roles.grant_table("shipments", "incident_controller", &[TablePrivilege::Select]).unwrap();
        let issuer = roles.get("incident_controller").unwrap().unwrap();
        // 2026-11-05 17:00:00 UTC; the task expires half an hour later.
        let now = WallClockMs(1_793_898_000_000);
        let tasks = admin.tasks();
        let def = tasks
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
                    disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires: "2026-11-05 17:30:00+00".into(),
                },
                now,
            )
            .unwrap();
        let executor = "agent_exec".to_string();
        let ctx = open_context(&sql, &name(), &executor, [7; 32], now, 0).unwrap();
        assert_eq!(ctx.task, def.id);
        assert_eq!(ctx.bound_def_version, 1);
        let mut agent = admin.sql_session();
        agent.bind_task(ctx.clone()).unwrap();
        assert_eq!(agent.task_standing(now).unwrap(), Standing::Live);
        assert_eq!(agent.task_standing(def.expires_at).unwrap(), Standing::Expired);
        // The wrong executor never binds.
        assert!(matches!(
            open_context(&sql, &name(), &"incident_controller".to_string(), [7; 32], now, 0),
            Err(TaskError::Revoked)
        ));
        tasks.revoke(&issuer, "inc-436").unwrap();
        assert!(matches!(agent.task_standing(now), Err(TaskError::Revoked)));
        assert!(matches!(open_context(&sql, &name(), &executor, [7; 32], now, 0), Err(TaskError::Revoked)));
        tasks.purge(def.id).unwrap();
        assert!(matches!(agent.task_standing(now), Err(TaskError::Revoked)));
        assert!(matches!(open_context(&sql, &name(), &executor, [7; 32], now, 0), Err(TaskError::Revoked)));
    }
}
