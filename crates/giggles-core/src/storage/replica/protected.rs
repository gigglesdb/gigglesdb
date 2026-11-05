//! Trusted, bounded prerequisite for a future distributed protected repair.
//!
//! This is an owner integration API, never an external request dispatcher. The
//! runtime authenticates the principal and implements [`TrustedRuntime`] from
//! committed state, not from caller assertions. It must lock the unique task
//! authority (including catalog, policy, revoke, approvals, receipts, counters and
//! capacity) and *every* row range/gap through one immutable decision. Currently
//! coverage is deliberately the entire typed repair keyspace, including empty
//! ranges. No SQL, generated values, triggers or relational catalog are supported.
//!
//! An authority image is row-free; row images are authority-free. Assembly is
//! disposable and uses the actual [`Executor::execute_current`] implementation.
//! No spendable authority is installed at a data participant. Staging returns no
//! receipt or effects to the user. Opaque bytes are privileged log/redo material,
//! may contain protected data, and must never be returned to an external caller.
//! Hashes bind canonical bytes; they do not authenticate them.
//!
//! Immediately before recording a decision the runtime must validate a *fresh*
//! bounded time interval and decision fence while retaining those locks. A pause,
//! term change or revoke invalidates an undecided validation. A decided commit
//! finalizes using its recorded decision-time authority, without retroactive
//! revoke/expiry. Core cannot prove locks, time freshness, or consensus facts.
//!
//! [`publish`] returns a bundle for atomic runtime application under that same
//! decision; it does not itself publish any distributed state. The runtime must
//! reserve participant redo/counter/image capacity before deciding, persist the
//! bundle with its applied index, retain immutable decisions/retry identities,
//! and never acknowledge or expose partial participant application. Exact whole
//! authority-image comparison rejects stale updates rather than overwriting
//! unrelated tasks. All undecided work on one image is therefore serialized.
//!
//! Business read/mutation accounting is included once in the committed authority
//! result. Failed/aborted speculative work is not a disclosure grant. Before
//! releasing each response the runtime must separately use the existing current
//! authority disclosure reservation, conservatively charging every delivery
//! attempt (including retries), and revalidate release. No output is authorized
//! merely by possessing this module's receipt.
//!
//! Only successful committed business work installs this API's read debit;
//! speculative errors and aborts leave the input unchanged. The runtime must
//! separately admit/bound speculative work and preserve conservative disclosure
//! accounting. Existing exact approvals must already reside at the authority.
//! Grant/approval issuance, authority migration and distributed TaskWrite wiring
//! are separate integrations, not provided by this mutation-staging interface.
use super::*;

/// At most 64 data participants, 1024 rows total, and a 16 MiB canonical body.
pub const MAX_PARTITIONS: usize = 64;
pub const MAX_ROWS: usize = 1024;
const VERSION: u32 = 1;

/// Trusted routing identity. Generation is the runtime's authority ownership
/// epoch, distinct from the kernel image's internal mutation generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Authority {
    pub location: u64,
    pub generation: u64,
    pub applied: u64,
    pub domain: u128,
    pub task: TaskId,
}

/// Data-only image at a locked committed applied position. Bounds are half-open.
#[derive(Clone, Debug)]
pub struct Partition {
    pub owner: u64,
    pub epoch: u64,
    pub applied: u64,
    pub start: Key,
    pub end: Option<Key>,
    pub image: Image,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct RangeBinding {
    pub owner: u64,
    pub epoch: u64,
    pub applied: u64,
    pub start: Key,
    pub end: Option<Key>,
    pub image: [u8; 32],
}

/// Exact evidence presented to the trusted verifier. Full images bind policy,
/// revoke state, approvals, dedupe, schema, rows, absence and monotonic clocks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Binding {
    pub version: u32,
    pub authority: Authority,
    pub authority_image: [u8; 32],
    pub schema: u64,
    pub policy: [u8; 32],
    pub revoked: bool,
    pub ranges: Vec<RangeBinding>,
    pub intent: Intent,
}

/// No SQL strings or time expressions enter this closed typed request grammar.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Intent {
    pub principal: Principal,
    pub request: Request,
    pub approval: Option<ApprovalId>,
    pub lower: u64,
    pub upper: u64,
    /// Runtime decision incarnation/fence, checked against committed authority.
    pub fence: u64,
}

/// Immutable transaction-home decision identity. Never reuse an identity for a
/// different validated digest. The task operation ID remains separately bound.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct DecisionId {
    pub home: u64,
    pub epoch: u64,
    pub transaction: u128,
}

/// Trusted decision-time interval and coordinator incarnation, rechecked as one
/// fence immediately before recording the immutable outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecisionTime {
    pub lower: u64,
    pub upper: u64,
    pub fence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Pending,
    Aborted,
    Committed,
}

/// Implement only in the trusted runtime. Neither a caller-provided boolean nor
/// a collection of unsigned client assertions is evidence. Each method must
/// resolve exact identities/digests from authenticated committed state.
pub trait TrustedRuntime {
    /// Verify unique authority placement/domain/generation/task, schema/policy,
    /// authenticated principal, exact immutable image provenance/applied indices,
    /// complete partition membership, and all authority/row/predicate locks.
    /// Empty ranges must carry the same gap/predicate protection as populated ones.
    fn verify_locked(&self, binding: &Binding) -> Result<(), CoreError>;
    /// Called at validation, immediately before decision. Verify fresh trusted
    /// bounds, live coordinator incarnation and decision eligibility. Runtime
    /// must forbid recording this validation after fence change or stale pause.
    fn verify_decision_fence(&self, binding: &Binding) -> Result<(), CoreError>;
    /// Resolve the immutable decision with exact validated digest and *complete*
    /// sealed participant set. A mismatched digest/identity must return an error.
    fn decision(&self, id: &DecisionId, validated: [u8; 32]) -> Result<Outcome, CoreError>;
}

#[derive(Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
struct Body {
    binding: Binding,
    after: Image,
    receipt: Receipt,
    row_effects: Vec<Effect>,
    delta: AuthorityDelta,
}

/// Canonical before-bound authority transition. Approval snapshots are exact,
/// including consumed/revoked flags and the full approved qualifying set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct AuthorityDelta {
    pub before: [u8; 32],
    pub after: [u8; 32],
    pub task_before: TaskState,
    pub task_after: TaskState,
    pub approval_before: Option<Approval>,
    pub approval_after: Option<Approval>,
    pub operation_before: Option<BoundDecision>,
    pub operation_after: BoundDecision,
    pub accounting_before: Option<TaskAccounting>,
    pub accounting_after: Option<TaskAccounting>,
    pub clock_before: u64,
    pub clock_after: u64,
    pub generation_before: u64,
    pub generation_after: u64,
}

/// Speculative, opaque, not a publication capability. No public Deserialize.
#[derive(Clone)]
pub struct Staged(Body);

#[derive(Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
struct ValidatedBody {
    body: Body,
    decision: DecisionId,
}

/// Revalidated decision-time redo. Restart decoding must re-run the evaluator
/// against retained immutable before images, then verify the decision at publish.
#[derive(Clone)]
pub struct Validated(ValidatedBody);

/// Privileged atomic application bundle, available only for a verified commit.
/// `row_effects` are the exact row CAS effects for data participants. An operation
/// retry retains the original receipt but has *no* new row effects or debit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    authority: Image,
    receipt: Receipt,
    row_effects: Vec<Effect>,
    delta: AuthorityDelta,
    decision: DecisionId,
    validated: [u8; 32],
}
impl Publication {
    pub fn authority(&self) -> &Image {
        &self.authority
    }
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub fn row_effects(&self) -> &[Effect] {
        &self.row_effects
    }
    pub fn delta(&self) -> &AuthorityDelta {
        &self.delta
    }
    pub fn decision(&self) -> &DecisionId {
        &self.decision
    }
    pub fn validated_digest(&self) -> [u8; 32] {
        self.validated
    }
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn image_state(image: &Image) -> Result<State, CoreError> {
    // Image derives Deserialize for older integration paths; always validate it
    // explicitly here rather than relying on its Rust type or deserialization.
    Image::from_bytes(image.bytes().to_vec())?;
    from_records(decode(image.bytes())?)
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>, CoreError> {
    let bytes = encode(value)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(CoreError::Limit);
    }
    Ok(bytes)
}
fn parse<T: DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T, CoreError> {
    if bytes.len() as u64 > MAX_BYTES {
        return Err(CoreError::Limit);
    }
    let value: T = decode(bytes)?;
    if canonical(&value)? != bytes {
        return Err(CoreError::Corrupt("noncanonical protected body".into()));
    }
    Ok(value)
}

fn assemble(
    authority: &Image,
    identity: Authority,
    partitions: &[Partition],
    mut intent: Intent,
) -> Result<(Image, Binding), CoreError> {
    if partitions.is_empty() || partitions.len() > MAX_PARTITIONS {
        return Err(CoreError::Limit);
    }
    if let Selection::Keys(keys) = &mut intent.request.selection {
        if keys.len() > MAX_ROWS {
            return Err(CoreError::Limit);
        }
        keys.sort();
    }
    let mut s = image_state(authority)?;
    if s.view.domain != identity.domain {
        return Err(CoreError::Policy(Error::Authentication));
    }
    if !s.view.rows.is_empty() || !s.sql.tables.is_empty() {
        return Err(CoreError::Unsupported);
    }
    let task = s
        .view
        .tasks
        .get(&identity.task)
        .ok_or(CoreError::Policy(Error::UnknownTask))?;
    if task.grant.executor != intent.principal {
        return Err(CoreError::Policy(Error::Authentication));
    }
    let mut binding = Binding {
        version: VERSION,
        authority: identity,
        authority_image: hash(authority.bytes()),
        schema: s.view.schema,
        policy: hash(&canonical(&task.grant)?),
        revoked: task.revoked,
        ranges: Vec::new(),
        intent,
    };
    let mut next = Some(Key {
        tenant: TenantId(0),
        row: RowId(0),
    });
    let mut owners = std::collections::BTreeSet::new();
    for p in partitions {
        if next != Some(p.start)
            || p.end.is_some_and(|end| p.start >= end)
            || !owners.insert(p.owner)
        {
            return Err(CoreError::Unsupported);
        }
        let rows = image_state(&p.image)?;
        let mut expected = empty(binding.authority.domain);
        expected.sql.generation = rows.sql.generation;
        expected.view.rows = rows.view.rows.clone();
        if canonical(&records(&expected)?)? != p.image.bytes() {
            return Err(CoreError::Unsupported);
        }
        for (key, row) in rows.view.rows {
            if key < p.start || p.end.is_some_and(|end| key >= end) {
                return Err(CoreError::Unsupported);
            }
            s.view.rows.insert(key, row);
            if s.view.rows.len() > MAX_ROWS {
                return Err(CoreError::Limit);
            }
        }
        binding.ranges.push(RangeBinding {
            owner: p.owner,
            epoch: p.epoch,
            applied: p.applied,
            start: p.start,
            end: p.end,
            image: hash(p.image.bytes()),
        });
        next = p.end;
    }
    if next.is_some() {
        return Err(CoreError::Unsupported);
    }
    Ok((Image(canonical(&records(&s)?)?), binding))
}

fn compute(authority: &Image, workspace: Image, binding: Binding) -> Result<Body, CoreError> {
    let intent = &binding.intent;
    let task = binding.authority.task;
    // A committed retry must not consume admission/read clocks, budget or an
    // approval again. It still needs authenticated current release separately.
    let mut before = image_state(authority)?;
    if before
        .view
        .operations
        .contains_key(&(task, intent.request.operation))
    {
        let receipt = policy::execute(
            &mut before,
            task,
            intent.request.clone(),
            Time::Certified(intent.upper),
            intent.approval,
        )
        .map_err(CoreError::Policy)?;
        return body(authority, authority.clone(), binding, receipt, Vec::new());
    }
    let (after, receipt) = workspace.with_admin(|a| {
        a.executor(task, intent.principal)?
            .execute_current(
                intent.request.clone(),
                intent.lower,
                intent.upper,
                intent.approval,
            )?
            .map_err(CoreError::Policy)
    })?;
    let mut after = image_state(&after)?;
    after.view.rows = before.view.rows;
    let after = Image(canonical(&records(&after)?)?);
    let effects = receipt.effects.clone();
    body(authority, after, binding, receipt, effects)
}

fn body(
    authority: &Image,
    after: Image,
    binding: Binding,
    receipt: Receipt,
    row_effects: Vec<Effect>,
) -> Result<Body, CoreError> {
    let before = image_state(authority)?;
    let next = image_state(&after)?;
    let task = binding.authority.task;
    let op = (task, binding.intent.request.operation);
    let approval = binding.intent.approval;
    let delta = AuthorityDelta {
        before: hash(authority.bytes()),
        after: hash(after.bytes()),
        task_before: before.view.tasks[&task].clone(),
        task_after: next.view.tasks[&task].clone(),
        approval_before: approval.and_then(|id| before.view.approvals.get(&id).cloned()),
        approval_after: approval.and_then(|id| next.view.approvals.get(&id).cloned()),
        operation_before: before.view.operations.get(&op).cloned(),
        operation_after: next
            .view
            .operations
            .get(&op)
            .cloned()
            .ok_or(CoreError::Fenced)?,
        accounting_before: before.accounting.get(&task).copied(),
        accounting_after: next.accounting.get(&task).copied(),
        clock_before: before.last_time,
        clock_after: next.last_time,
        generation_before: before.sql.generation,
        generation_after: next.sql.generation,
    };
    let body = Body {
        binding,
        after,
        receipt,
        row_effects,
        delta,
    };
    canonical(&body)?; // Reserve complete serializable redo before decision.
    Ok(body)
}

/// Stage without modifying any input or releasing any receipt/effect. Rejected
/// operations return an error and do not install speculative rejection receipts.
pub fn stage(
    authority: &Image,
    identity: Authority,
    partitions: &[Partition],
    intent: Intent,
    runtime: &impl TrustedRuntime,
) -> Result<Staged, CoreError> {
    let (workspace, binding) = assemble(authority, identity, partitions, intent)?;
    runtime.verify_locked(&binding)?;
    Ok(Staged(compute(authority, workspace, binding)?))
}

impl Staged {
    /// Privileged canonical prepared-log material; never a task response.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CoreError> {
        canonical(&self.0)
    }
    /// Recompute *all* effects and authority bytes against exact trusted inputs.
    pub fn verify_bytes(
        bytes: &[u8],
        authority: &Image,
        partitions: &[Partition],
        runtime: &impl TrustedRuntime,
    ) -> Result<Self, CoreError> {
        let body: Body = parse(bytes)?;
        let recomputed = stage(
            authority,
            body.binding.authority.clone(),
            partitions,
            body.binding.intent.clone(),
            runtime,
        )?;
        if recomputed.0 != body {
            return Err(CoreError::Fenced);
        }
        Ok(recomputed)
    }
    pub fn digest(&self) -> Result<[u8; 32], CoreError> {
        Ok(hash(&self.to_bytes()?))
    }
}

/// Recheck exact before images, eligibility and fresh interval/fence immediately
/// before decision. Revoke/schema/row/predicate changes reject the old stage.
/// `decision` and the resulting digest must be durably sealed together.
pub fn validate(
    staged: &Staged,
    authority: &Image,
    partitions: &[Partition],
    time: DecisionTime,
    decision: DecisionId,
    runtime: &impl TrustedRuntime,
) -> Result<Validated, CoreError> {
    let DecisionTime {
        lower,
        upper,
        fence,
    } = time;
    let mut intent = staged.0.binding.intent.clone();
    if lower < intent.lower || upper < intent.upper {
        return Err(CoreError::Policy(Error::TimeRegression));
    }
    intent.lower = lower;
    intent.upper = upper;
    intent.fence = fence;
    let (workspace, binding) = assemble(
        authority,
        staged.0.binding.authority.clone(),
        partitions,
        intent,
    )?;
    let mut prior = staged.0.binding.clone();
    prior.intent = binding.intent.clone();
    if prior != binding {
        return Err(CoreError::Fenced);
    }
    runtime.verify_locked(&binding)?;
    // Retries also must prove current eligibility before a *new* decision.
    let mut state = image_state(authority)?;
    Executor::current(&mut state, binding.authority.task, lower, upper)?;
    let body = compute(authority, workspace, binding)?;
    if body.receipt.effects != staged.0.receipt.effects {
        return Err(CoreError::Fenced);
    }
    let result = Validated(ValidatedBody { body, decision });
    result.to_bytes()?;
    // Materialization/encoding can be substantial near the bounded capacity.
    // Check freshness after all of that work, immediately before returning the
    // decision-time redo. The runtime still fences pauses after this callback.
    runtime.verify_decision_fence(&result.0.body.binding)?;
    Ok(result)
}

impl Validated {
    /// Privileged immutable redo, not authenticated merely by its encoding.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CoreError> {
        canonical(&self.0)
    }
    pub fn digest(&self) -> Result<[u8; 32], CoreError> {
        Ok(hash(&self.to_bytes()?))
    }
    /// Restart verification uses retained decision-time before images. It does
    /// not reacquire present-day eligibility or reinterpret an existing decision.
    /// `publish` still requires committed exact-digest evidence.
    pub fn verify_bytes(
        bytes: &[u8],
        authority: &Image,
        partitions: &[Partition],
    ) -> Result<Self, CoreError> {
        let value: ValidatedBody = parse(bytes)?;
        let b = &value.body.binding;
        let (workspace, binding) =
            assemble(authority, b.authority.clone(), partitions, b.intent.clone())?;
        let mut state = image_state(authority)?;
        Executor::current(
            &mut state,
            binding.authority.task,
            binding.intent.lower,
            binding.intent.upper,
        )?;
        if binding != *b || compute(authority, workspace, binding)? != value.body {
            return Err(CoreError::Fenced);
        }
        Ok(Self(value))
    }
}

/// Return an application bundle only for an exact immutable committed decision.
/// Pending is fenced; abort has no effects. An exact after-image retry returns
/// the same redo bundle, since data participants may still need it. The runtime
/// must dedupe this decision at every participant to avoid repeating effects.
/// Other current images reject without losing unrelated state. Retain the
/// decision-time before images for recovery verification.
pub fn publish(
    validated: &Validated,
    current: &Image,
    runtime: &impl TrustedRuntime,
) -> Result<Option<Publication>, CoreError> {
    let digest = validated.digest()?;
    match runtime.decision(&validated.0.decision, digest)? {
        Outcome::Pending => return Err(CoreError::Fenced),
        Outcome::Aborted => return Ok(None),
        Outcome::Committed => {}
    }
    image_state(current)?;
    let body = &validated.0.body;
    if hash(current.bytes()) != body.binding.authority_image && current != &body.after {
        return Err(CoreError::Fenced);
    }
    Ok(Some(Publication {
        authority: body.after.clone(),
        receipt: body.receipt.clone(),
        row_effects: body.row_effects.clone(),
        delta: body.delta.clone(),
        decision: validated.0.decision.clone(),
        validated: digest,
    }))
}

#[cfg(test)]
mod tests;
