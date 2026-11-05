use std::collections::{BTreeMap, BTreeSet};

macro_rules! id {
    ($($name:ident),*) => {$ (
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
    )*};
}
id!(TenantId, RowId, TaskId, OperationId, Principal, ApprovalId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Status {
    Failed = 0,
    Queued = 1,
    Succeeded = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Column {
    Status = 0,
    FailedAt = 1,
    AttemptCount = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub tenant: TenantId,
    pub row: RowId,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub status: Status,
    pub failed_at: u64,
    pub attempt_count: u64,
    pub version: u64,
}

/// Typed grammar. Bounds are half-open; transition and column sets have no wildcards.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub tenant: TenantId,
    pub columns: BTreeSet<Column>,
    pub transitions: BTreeSet<(Status, Status)>,
    pub incident: (u64, u64),
    pub distinct_limit: u64,
    pub transition_limit: u64,
    pub approval_above: u64,
    pub valid_from: u64,
    pub expires: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub task: TaskId,
    pub issuer: Principal,
    pub executor: Principal,
    pub approver: Principal,
    pub policy: Policy,
}
/// Opaque model capability, not a cryptographic token. Only trusted grant issues one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    pub(crate) domain: u128,
    pub(crate) task: TaskId,
    pub(crate) executor: Principal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskState {
    pub grant: Grant,
    pub revoked: bool,
    pub touched: BTreeSet<Key>,
    pub transitions: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Time {
    Certified(u64),
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    Keys(Vec<Key>),
    /// Complete qualifying set, including phantom protection in exact previews.
    Predicate {
        tenant: TenantId,
        status: Status,
        from: u64,
        until: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub operation: OperationId,
    pub selection: Selection,
    pub column: Column,
    pub after: Status,
}
impl Request {
    /// Versioned, length-delimited binary payload. Key-list order is immaterial;
    /// multiplicity is preserved and duplicate keys are rejected by validation.
    pub fn canonical(&self) -> Vec<u8> {
        let mut out = b"GD-P0-REPAIR\x01".to_vec();
        fn num(out: &mut Vec<u8>, n: u64) {
            out.extend(n.to_be_bytes());
        }
        match &self.selection {
            Selection::Keys(keys) => {
                out.push(0);
                let mut keys = keys.clone();
                keys.sort();
                num(&mut out, keys.len() as u64);
                for k in keys {
                    num(&mut out, k.tenant.0);
                    num(&mut out, k.row.0);
                }
            }
            Selection::Predicate {
                tenant,
                status,
                from,
                until,
            } => {
                out.push(1);
                num(&mut out, tenant.0);
                out.push(*status as u8);
                num(&mut out, *from);
                num(&mut out, *until);
            }
        }
        out.push(self.column as u8);
        out.push(self.after as u8);
        out
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Effect {
    pub key: Key,
    pub before: Row,
    pub after: Row,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Preview {
    pub domain: u128,
    pub task: TaskId,
    pub request: Request,
    pub schema: u64,
    pub policy: Policy,
    pub dependencies: BTreeMap<Key, Row>,
    pub effects: Vec<Effect>,
    pub expires: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Approval {
    pub id: ApprovalId,
    pub approver: Principal,
    pub preview: Preview,
    pub revoked: bool,
    pub consumed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub domain: u128,
    pub task: TaskId,
    pub operation: OperationId,
    pub payload: Vec<u8>,
    pub issuer: Principal,
    pub executor: Principal,
    pub approver: Option<Principal>,
    pub schema: u64,
    pub decided_at: u64,
    pub effects: Vec<Effect>,
    pub charged_transitions: u64,
    pub lifetime_transitions: u64,
    pub lifetime_distinct: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Authentication,
    InvalidGrant,
    TaskExists,
    UnknownTask,
    Revoked,
    Expired,
    UncertainTime,
    TimeRegression,
    Scope,
    MissingRow,
    DuplicateKey,
    InvalidPredicate,
    Budget,
    ApprovalRequired,
    InvalidApproval,
    StalePreview,
    IdConflict,
    Overflow,
}
pub type Decision = Result<Receipt, Error>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Known(Decision),
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationStatus {
    Absent,
    Decided(Decision),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundDecision {
    pub payload: Vec<u8>,
    pub decision: Decision,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Put(Key, Row),
    Grant(Grant),
    Revoke(TaskId),
    Schema(u64),
    Approve(Approval),
    RevokeApproval(ApprovalId),
    Decision {
        task: TaskId,
        request: Request,
        time: Time,
        approval: Option<ApprovalId>,
        result: Decision,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub domain: u128,
    pub schema: u64,
    pub rows: BTreeMap<Key, Row>,
    pub tasks: BTreeMap<TaskId, TaskState>,
    pub operations: BTreeMap<(TaskId, OperationId), BoundDecision>,
    pub approvals: BTreeMap<ApprovalId, Approval>,
}

#[derive(Clone, Debug)]
pub struct Model {
    state: Snapshot,
    history: Vec<Event>,
    next_approval: u64,
    last_time: u64,
}
impl Model {
    /// Domain identifies a database generation. Trusted callers must use distinct
    /// domains for independent databases/restores. No wall-clock guarantee is made.
    pub fn new(domain: u128) -> Self {
        Self {
            state: Snapshot {
                domain,
                schema: 0,
                rows: BTreeMap::new(),
                tasks: BTreeMap::new(),
                operations: BTreeMap::new(),
                approvals: BTreeMap::new(),
            },
            history: vec![],
            next_approval: 0,
            last_time: 0,
        }
    }
    pub fn snapshot(&self) -> Snapshot {
        self.state.clone()
    }
    pub fn history(&self) -> &[Event] {
        &self.history
    }
    /// Trusted external application write; version is controlled by the model.
    pub fn put(&mut self, key: Key, mut row: Row) -> Result<(), Error> {
        row.version = match self.state.rows.get(&key) {
            Some(old) => old.version.checked_add(1).ok_or(Error::Overflow)?,
            None => 0,
        };
        self.state.rows.insert(key, row.clone());
        self.history.push(Event::Put(key, row));
        Ok(())
    }
    pub fn grant(&mut self, grant: Grant) -> Result<Credential, Error> {
        if self.state.tasks.contains_key(&grant.task) {
            return Err(Error::TaskExists);
        }
        let p = &grant.policy;
        if p.incident.0 >= p.incident.1
            || p.valid_from >= p.expires
            || grant.executor == grant.approver
            || grant.issuer == grant.executor
            || p.columns.is_empty()
            || p.transitions.is_empty()
        {
            return Err(Error::InvalidGrant);
        }
        let cred = Credential {
            domain: self.state.domain,
            task: grant.task,
            executor: grant.executor,
        };
        self.state.tasks.insert(
            grant.task,
            TaskState {
                grant: grant.clone(),
                revoked: false,
                touched: BTreeSet::new(),
                transitions: 0,
            },
        );
        self.history.push(Event::Grant(grant));
        Ok(cred)
    }
    pub fn revoke(&mut self, task: TaskId) -> Result<(), Error> {
        self.state
            .tasks
            .get_mut(&task)
            .ok_or(Error::UnknownTask)?
            .revoked = true;
        self.history.push(Event::Revoke(task));
        Ok(())
    }
    pub fn bump_schema(&mut self) -> Result<(), Error> {
        self.state.schema = self.state.schema.checked_add(1).ok_or(Error::Overflow)?;
        self.history.push(Event::Schema(self.state.schema));
        Ok(())
    }
    fn authenticate(&self, cred: &Credential) -> Result<&TaskState, Error> {
        let t = self
            .state
            .tasks
            .get(&cred.task)
            .ok_or(Error::Authentication)?;
        if cred.domain != self.state.domain || cred.executor != t.grant.executor {
            return Err(Error::Authentication);
        }
        Ok(t)
    }
    fn time(&mut self, time: Time) -> Result<u64, Error> {
        let Time::Certified(now) = time else {
            return Err(Error::UncertainTime);
        };
        if now < self.last_time {
            return Err(Error::TimeRegression);
        }
        self.last_time = now;
        Ok(now)
    }
    fn active(task: &TaskState, now: u64) -> Result<(), Error> {
        if task.revoked {
            return Err(Error::Revoked);
        }
        if now < task.grant.policy.valid_from || now >= task.grant.policy.expires {
            return Err(Error::Expired);
        }
        Ok(())
    }
    fn materialize(
        &self,
        task: &TaskState,
        request: &Request,
    ) -> Result<(BTreeMap<Key, Row>, Vec<Effect>), Error> {
        let p = &task.grant.policy;
        if request.column != Column::Status || !p.columns.contains(&request.column) {
            return Err(Error::Scope);
        }
        let keys = match &request.selection {
            Selection::Keys(keys) => {
                let unique: BTreeSet<_> = keys.iter().copied().collect();
                if unique.len() != keys.len() {
                    return Err(Error::DuplicateKey);
                }
                unique.into_iter().collect::<Vec<_>>()
            }
            Selection::Predicate {
                tenant,
                status,
                from,
                until,
            } => {
                if *tenant != p.tenant {
                    return Err(Error::Scope);
                }
                if from >= until {
                    return Err(Error::InvalidPredicate);
                }
                self.state
                    .rows
                    .iter()
                    .filter(|(k, r)| {
                        k.tenant == *tenant
                            && r.status == *status
                            && r.failed_at >= *from
                            && r.failed_at < *until
                    })
                    .map(|(k, _)| *k)
                    .collect()
            }
        };
        let mut deps = BTreeMap::new();
        let mut effects = vec![];
        for key in keys {
            if key.tenant != p.tenant {
                return Err(Error::Scope);
            }
            let row = self.state.rows.get(&key).ok_or(Error::MissingRow)?;
            if row.failed_at < p.incident.0 || row.failed_at >= p.incident.1 {
                return Err(Error::Scope);
            }
            deps.insert(key, row.clone());
            if row.status == request.after {
                continue;
            }
            if !p.transitions.contains(&(row.status, request.after)) {
                return Err(Error::Scope);
            }
            let mut after = row.clone();
            after.status = request.after;
            after.version = after.version.checked_add(1).ok_or(Error::Overflow)?;
            effects.push(Effect {
                key,
                before: row.clone(),
                after,
            });
        }
        Ok((deps, effects))
    }
    pub fn preview(
        &mut self,
        cred: &Credential,
        request: &Request,
        time: Time,
    ) -> Result<Preview, Error> {
        self.authenticate(cred)?;
        let now = self.time(time)?;
        let task = self.authenticate(cred)?;
        Self::active(task, now)?;
        let (dependencies, effects) = self.materialize(task, request)?;
        Self::budget(task, &effects)?;
        Ok(Preview {
            domain: self.state.domain,
            task: cred.task,
            request: request.clone(),
            schema: self.state.schema,
            policy: task.grant.policy.clone(),
            dependencies,
            effects,
            expires: task.grant.policy.expires,
        })
    }
    /// Trusted approver call. A client-provided preview must equal a fresh recomputation.
    pub fn approve(
        &mut self,
        approver: Principal,
        preview: Preview,
        time: Time,
    ) -> Result<ApprovalId, Error> {
        let task = self
            .state
            .tasks
            .get(&preview.task)
            .ok_or(Error::InvalidApproval)?;
        if task.grant.approver != approver || approver == task.grant.executor {
            return Err(Error::InvalidApproval);
        }
        let cred = Credential {
            domain: self.state.domain,
            task: preview.task,
            executor: task.grant.executor,
        };
        let current = self.preview(&cred, &preview.request, time)?;
        if current != preview {
            return Err(Error::StalePreview);
        }
        let id = ApprovalId(self.next_approval);
        self.next_approval = self.next_approval.checked_add(1).ok_or(Error::Overflow)?;
        let approval = Approval {
            id,
            approver,
            preview,
            revoked: false,
            consumed: false,
        };
        self.state.approvals.insert(id, approval.clone());
        self.history.push(Event::Approve(approval));
        Ok(id)
    }
    pub fn revoke_approval(&mut self, id: ApprovalId) -> Result<(), Error> {
        self.state
            .approvals
            .get_mut(&id)
            .ok_or(Error::InvalidApproval)?
            .revoked = true;
        self.history.push(Event::RevokeApproval(id));
        Ok(())
    }
    fn budget(task: &TaskState, effects: &[Effect]) -> Result<(BTreeSet<Key>, u64), Error> {
        let mut touched = task.touched.clone();
        for effect in effects {
            touched.insert(effect.key);
        }
        let count = task
            .transitions
            .checked_add(effects.len() as u64)
            .ok_or(Error::Overflow)?;
        if count > task.grant.policy.transition_limit
            || touched.len() as u64 > task.grant.policy.distinct_limit
        {
            return Err(Error::Budget);
        }
        Ok((touched, count))
    }
    fn decide(
        &mut self,
        cred: &Credential,
        request: &Request,
        time: Time,
        approval: Option<ApprovalId>,
    ) -> Decision {
        let now = self.time(time)?;
        let task = self.authenticate(cred)?;
        Self::active(task, now)?;
        let (dependencies, effects) = self.materialize(task, request)?;
        let (touched, count) = Self::budget(task, &effects)?;
        let mut approver = None;
        if effects.len() as u64 > task.grant.policy.approval_above || approval.is_some() {
            let id = approval.ok_or(Error::ApprovalRequired)?;
            let a = self
                .state
                .approvals
                .get(&id)
                .ok_or(Error::InvalidApproval)?;
            if a.revoked
                || a.consumed
                || a.preview.task != cred.task
                || a.preview.request.operation != request.operation
                || a.preview.request.canonical() != request.canonical()
                || a.approver != task.grant.approver
            {
                return Err(Error::InvalidApproval);
            }
            let p = &a.preview;
            if p.domain != self.state.domain
                || p.schema != self.state.schema
                || p.policy != task.grant.policy
                || now >= p.expires
                || p.dependencies != dependencies
                || p.effects != effects
            {
                return Err(Error::StalePreview);
            }
            approver = Some(a.approver);
        }
        let receipt = Receipt {
            domain: self.state.domain,
            task: cred.task,
            operation: request.operation,
            payload: request.canonical(),
            issuer: task.grant.issuer,
            executor: task.grant.executor,
            approver,
            schema: self.state.schema,
            decided_at: now,
            effects: effects.clone(),
            charged_transitions: effects.len() as u64,
            lifetime_transitions: count,
            lifetime_distinct: touched.len() as u64,
        };
        // All fallible checks precede this atomic serial decision.
        for e in effects {
            self.state.rows.insert(e.key, e.after);
        }
        let t = self
            .state
            .tasks
            .get_mut(&cred.task)
            .expect("authenticated task");
        t.touched = touched;
        t.transitions = count;
        if let Some(id) = approval {
            self.state
                .approvals
                .get_mut(&id)
                .expect("validated approval")
                .consumed = true;
        }
        Ok(receipt)
    }
    /// A terminal decision binds the ID even on rejection. Authentication failure
    /// and altered-payload conflict do not replace an existing binding.
    pub fn execute(
        &mut self,
        cred: &Credential,
        request: Request,
        time: Time,
        approval: Option<ApprovalId>,
        lose_reply: bool,
    ) -> Outcome {
        if let Err(e) = self.authenticate(cred) {
            return Outcome::Known(Err(e));
        }
        let key = (cred.task, request.operation);
        let payload = request.canonical();
        let result = if let Some(old) = self.state.operations.get(&key) {
            if old.payload != payload {
                Err(Error::IdConflict)
            } else {
                old.decision.clone()
            }
        } else {
            let result = self.decide(cred, &request, time, approval);
            self.state.operations.insert(
                key,
                BoundDecision {
                    payload,
                    decision: result.clone(),
                },
            );
            self.history.push(Event::Decision {
                task: cred.task,
                request,
                time,
                approval,
                result: result.clone(),
            });
            result
        };
        if lose_reply {
            Outcome::Unknown
        } else {
            Outcome::Known(result)
        }
    }
    /// Resolution remains available after task expiry/revocation to its executor.
    pub fn status(&self, cred: &Credential, id: OperationId) -> Result<OperationStatus, Error> {
        self.authenticate(cred)?;
        Ok(match self.state.operations.get(&(cred.task, id)) {
            Some(d) => OperationStatus::Decided(d.decision.clone()),
            None => OperationStatus::Absent,
        })
    }
}
