//! Pure, in-memory application range projections. Home declarations describe
//! placement; they never provision authority or authorize runtime bootstrap.
use crate::operations::Value;
use crate::relational::projector_reconstruction::ProjectionCounters;
use crate::relational::{ColumnDef, DataType, TableDef};
use crate::storage::CoreError;
use crate::storage::replica::{
    Image,
    projected_admission::{AdmittedProjectedSource, SourceBudget},
};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
mod projector_budget;
pub(in crate::storage::replica) use projector_budget::ProjectorBudget;
use projector_budget::WirePlan;
use sha2::{Digest, Sha256};

pub const CONTRACT_VERSION: u32 = 1;
pub const MAX_PROJECTED_ROWS: usize = crate::relational::MAX_ROWS;
pub const MAX_PROJECTED_BYTES: usize = crate::relational::MAX_RESULT_BYTES;
pub const MAX_SOURCE_ROWS: usize = 65_536;

/// Closed identity for the existing engine key ordering, not a second codec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyContract {
    RelationalEncodeKeyV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnIdentity {
    pub id: u64,
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableIdentity {
    pub id: u64,
    pub name: String,
    pub columns: Vec<ColumnIdentity>,
    pub primary_key: Vec<usize>,
}

impl From<&TableDef> for TableIdentity {
    fn from(table: &TableDef) -> Self {
        Self {
            id: table.id,
            name: table.name.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| ColumnIdentity {
                    id: column.id,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    not_null: column.not_null,
                })
                .collect(),
            primary_key: table.primary_key.clone(),
        }
    }
}

impl TableIdentity {
    fn is_heap(&self) -> bool {
        self.primary_key.len() == 1
            && self.columns.get(self.primary_key[0]).is_some_and(|column| {
                column.name == crate::relational::HIDDEN_ROWID
                    && column.data_type == DataType::BigInt
                    && column.not_null
            })
    }

    fn definition(&self) -> TableDef {
        TableDef {
            id: self.id,
            name: self.name.clone(),
            columns: self
                .columns
                .iter()
                .map(|column| ColumnDef {
                    id: column.id,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    not_null: column.not_null,
                })
                .collect(),
            primary_key: self.primary_key.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteKey {
    pub values: Vec<Value>,
    /// Supplied encoding must equal the owner engine's canonical key bytes.
    pub encoded: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Bound {
    MinusInfinity,
    Key(CompleteKey),
    PlusInfinity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Homes {
    pub catalog: u128,
    pub authority: u128,
    pub operations: OperationHome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationHome {
    OriginalOwningGroup,
}

/// Every field is independently selected by the trusted caller on admission.
/// The source index is a trusted commitment: an Image does not contain it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedDescriptor {
    pub contract_version: u32,
    pub domain: u128,
    pub catalog_epoch: u64,
    pub table: TableIdentity,
    pub key_contract: KeyContract,
    pub range: u128,
    pub group: u128,
    pub ownership_epoch: u64,
    pub lower: Bound,
    pub upper: Bound,
    pub source_applied_index: u64,
    pub source_digest: [u8; 32],
    pub metadata: Metadata,
    pub limits: ProjectionLimits,
    pub homes: Homes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionLimits {
    pub rows: usize,
    pub bytes: usize,
}

impl ProjectionLimits {
    fn validate(self) -> Result<(), CoreError> {
        if self.rows == 0
            || self.rows > MAX_PROJECTED_ROWS
            || self.bytes == 0
            || self.bytes > MAX_PROJECTED_BYTES
        {
            return Err(CoreError::Limit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRequest {
    pub descriptor: ExpectedDescriptor,
    pub limits: ProjectionLimits,
}

/// All ten known storage classes have a disposition, including absent classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Disposition {
    RetainedMetadata,
    ProjectedTable,
    RequiredAbsent,
    EmptyApplicationHome,
}

pub const NAMESPACE_MANIFEST: [Disposition; 10] = [
    Disposition::RetainedMetadata,
    Disposition::RequiredAbsent, // repair rows
    Disposition::RequiredAbsent, // task grants
    Disposition::RequiredAbsent, // task operations
    Disposition::RequiredAbsent, // approvals
    Disposition::ProjectedTable,
    Disposition::RequiredAbsent, // accounting
    Disposition::RequiredAbsent, // native-once
    Disposition::RequiredAbsent, // secondary indexes
    Disposition::EmptyApplicationHome,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub generation: u64,
    pub schema: u64,
    pub last_time: u64,
    pub next_approval: u64,
    pub origin: Option<u128>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectedRow {
    pub key: Vec<u8>,
    pub values: Vec<Value>,
    pub version: u64,
}

/// Deserialization confers no validation. Call validate_projection before use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UntrustedProjection {
    pub descriptor: ExpectedDescriptor,
    pub metadata: Metadata,
    pub namespace_manifest: [Disposition; 10],
    pub limits: ProjectionLimits,
    pub rows: Vec<ProjectedRow>,
    pub digest: [u8; 32],
}

/// Constructed only by the projector/admission validator. No Deserialize impl.
#[derive(Clone, Debug)]
pub struct ValidatedProjection(UntrustedProjection);

impl ValidatedProjection {
    pub fn descriptor(&self) -> &ExpectedDescriptor {
        &self.0.descriptor
    }
    pub fn metadata(&self) -> &Metadata {
        &self.0.metadata
    }
    pub fn rows(&self) -> &[ProjectedRow] {
        &self.0.rows
    }
    pub fn as_untrusted(&self) -> &UntrustedProjection {
        &self.0
    }
}

/// Compare serialized scalar representations, including decimal display scale
/// and float payload bits. Semantic Value equality is insufficient here.
fn exact<T: Serialize>(left: &T, right: &T) -> Result<bool, CoreError> {
    Ok(crate::storage::encode(left)? == crate::storage::encode(right)?)
}

fn serialized_size<T: Serialize>(value: &T) -> Result<usize, CoreError> {
    let size = crate::storage::codec()
        .serialized_size(value)
        .map_err(|error| CoreError::Corrupt(error.to_string()))?;
    usize::try_from(size).map_err(|_| CoreError::Limit)
}

fn envelope_base_size(
    descriptor: &ExpectedDescriptor,
    limits: ProjectionLimits,
) -> Result<usize, CoreError> {
    // Struct fields and this tuple have identical fixed-int bincode layout.
    // Charge the empty row vector's length and the final digest as well.
    serialized_size(&(
        descriptor,
        &descriptor.metadata,
        NAMESPACE_MANIFEST,
        limits,
        Vec::<ProjectedRow>::new(),
        [0u8; 32],
    ))
}

fn add_size(total: &mut usize, additional: usize, cap: usize) -> Result<(), CoreError> {
    *total = total.checked_add(additional).ok_or(CoreError::Limit)?;
    if *total > cap {
        return Err(CoreError::Limit);
    }
    Ok(())
}

fn content_digest(projection: &UntrustedProjection) -> Result<[u8; 32], CoreError> {
    let bytes = crate::storage::encode(&(
        &projection.descriptor,
        &projection.metadata,
        &projection.namespace_manifest,
        projection.limits,
        &projection.rows,
    ))?;
    Ok(Sha256::digest(bytes).into())
}

fn validate_structure(
    descriptor: &ExpectedDescriptor,
    limits: ProjectionLimits,
) -> Result<(), CoreError> {
    limits.validate()?;
    if descriptor.limits != limits {
        return Err(CoreError::Unsupported);
    }
    if descriptor.contract_version != CONTRACT_VERSION
        || matches!(descriptor.lower, Bound::PlusInfinity)
        || matches!(descriptor.upper, Bound::MinusInfinity)
    {
        return Err(CoreError::Unsupported);
    }
    let table = &descriptor.table;
    if table.is_heap() {
        return Err(CoreError::Unsupported);
    }
    if table.id == 0
        || table.name.is_empty()
        || table.columns.is_empty()
        || table.columns.len() > crate::relational::MAX_COLUMNS
        || table.primary_key.is_empty()
        || table.primary_key.len() > table.columns.len()
    {
        return Err(CoreError::Unsupported);
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    for column in &table.columns {
        if column.id == 0
            || column.id == table.id
            || column.name.is_empty()
            || !ids.insert(column.id)
            || !names.insert(&column.name)
        {
            return Err(CoreError::Unsupported);
        }
    }
    let mut indices = std::collections::BTreeSet::new();
    for index in &table.primary_key {
        if *index >= table.columns.len()
            || !indices.insert(*index)
            || !table.columns[*index].not_null
        {
            return Err(CoreError::Unsupported);
        }
        if !matches!(
            table.columns[*index].data_type,
            DataType::Boolean
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Text
                | DataType::Bytes
                | DataType::Numeric
                | DataType::Real
                | DataType::DoublePrecision
        ) {
            return Err(CoreError::Unsupported);
        }
    }
    for bound in [&descriptor.lower, &descriptor.upper] {
        if let Bound::Key(key) = bound
            && (key.values.len() != table.primary_key.len()
                || key.values.iter().any(|value| matches!(value, Value::Null)))
        {
            return Err(CoreError::Unsupported);
        }
    }
    let base = envelope_base_size(descriptor, limits)?;
    if base > limits.bytes {
        return Err(CoreError::Limit);
    }
    Ok(())
}

/// Exhaustively inspect the canonical decoded state before selecting rows.
/// Unknown namespaces, including empty ones, were rejected by from_records;
/// replica_state also gates native-once/index families and checks roundtrip.
fn classify_source(state: &crate::storage::State) -> Result<Metadata, CoreError> {
    if !state.view.rows.is_empty()
        || !state.view.tasks.is_empty()
        || !state.view.operations.is_empty()
        || !state.view.approvals.is_empty()
        || !state.accounting.is_empty()
        || state.native_outcomes.is_some()
        || state.sql.has_secondary_indexes_engine()
        || state
            .applications
            .as_ref()
            .is_some_and(|ledger| !ledger.is_empty_readonly())
        || state.sql.tables.len() != 1
        || state.sql.contains_heap()
            || crate::storage::feature_envelope::feature_bits_present(state)? & crate::storage::feature_envelope::NEW_BITS != 0
    {
        return Err(CoreError::Unsupported);
    }
    Ok(Metadata {
        generation: state.sql.generation,
        schema: state.view.schema,
        last_time: state.last_time,
        next_approval: state.next_approval,
        origin: state.origin,
    })
}

fn admission_preflight(
    projection: &UntrustedProjection,
    expected: &ExpectedDescriptor,
) -> Result<(), CoreError> {
    validate_structure(expected, projection.limits)?;
    if projection.rows.len() > projection.limits.rows {
        return Err(CoreError::Limit);
    }
    // Size the borrowed wire object before any row/value clone. This includes
    // every descriptor, metadata, manifest, key, enum and vector overhead byte.
    if serialized_size(projection)? > projection.limits.bytes {
        return Err(CoreError::Limit);
    }
    if !exact(&projection.descriptor, expected)?
        || !exact(&projection.metadata, &expected.metadata)?
        || projection.namespace_manifest != NAMESPACE_MANIFEST
    {
        return Err(CoreError::Unsupported);
    }
    if content_digest(projection)? != projection.digest {
        return Err(CoreError::Corrupt("projection digest mismatch".into()));
    }
    Ok(())
}

fn bind_row_values(
    projection: &UntrustedProjection,
    expected: &ExpectedDescriptor,
) -> Result<(), CoreError> {
    let table = expected.table.definition();
    // Descriptive catalog-only binder context. It has no authority or source
    // rows and is never executed or published. Reuse the engine's whole-row
    // checker, rather than duplicating scalar/nullability/width semantics.
    let mut context = crate::relational::Relational::default();
    context.epoch = expected.catalog_epoch;
    context.tables.insert(table.name.clone(), table.clone());
    for row in &projection.rows {
        if row.version == 0 {
            return Err(CoreError::Unsupported);
        }
        crate::relational::Prepared::insert_engine(
            &table,
            row.values.clone(),
            expected.domain,
            &context,
        )
        .map_err(CoreError::Sql)?;
    }
    Ok(())
}

struct EncodedBounds {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
}

impl EncodedBounds {
    fn contains(&self, key: &[u8]) -> bool {
        self.lower
            .as_ref()
            .is_none_or(|lower| key >= lower.as_slice())
            && self
                .upper
                .as_ref()
                .is_none_or(|upper| key < upper.as_slice())
    }
}

fn bind_bounds(
    descriptor: &ExpectedDescriptor,
    bind: impl Fn(&[Value]) -> Result<Vec<u8>, CoreError>,
) -> Result<EncodedBounds, CoreError> {
    let bind_one = |bound: &Bound| -> Result<Option<Vec<u8>>, CoreError> {
        match bound {
            Bound::MinusInfinity | Bound::PlusInfinity => Ok(None),
            Bound::Key(key) => {
                let encoded = bind(&key.values)?;
                if encoded != key.encoded {
                    return Err(CoreError::Corrupt("noncanonical bound key".into()));
                }
                Ok(Some(encoded))
            }
        }
    };
    let bounds = EncodedBounds {
        lower: bind_one(&descriptor.lower)?,
        upper: bind_one(&descriptor.upper)?,
    };
    if let (Some(lower), Some(upper)) = (&bounds.lower, &bounds.upper)
        && lower >= upper
    {
        return Err(CoreError::Unsupported);
    }
    Ok(bounds)
}

/// Project from canonical source bytes and independently trusted commitments.
/// No runtime, authority, persistence or home activation is performed.
pub fn project(
    image: &super::super::Image,
    request: &ProjectionRequest,
) -> Result<ValidatedProjection, CoreError> {
    validate_structure(&request.descriptor, request.limits)?;
    if image.bytes().len() as u64 > crate::storage::MAX_BYTES {
        return Err(CoreError::Limit);
    }
    let descriptor = &request.descriptor;
    if <[u8; 32]>::from(Sha256::digest(image.bytes())) != descriptor.source_digest {
        return Err(CoreError::Corrupt("source digest mismatch".into()));
    }
    let source = super::super::replica_state_with_cancel(image.bytes(), &mut || false)?;
    project_decoded(&source, request, &mut ProjectionMode::Ordinary, &mut || {
        false
    })
    .map(|(projection, _)| projection)
}

/// Restricted consuming source handoff; no decoder or ordinary project call.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Admitted projector entry awaits the separately released ancestor consumer."
    )
)]
pub(in crate::storage::replica) fn project_admitted<'a>(
    source: &'a Image,
    certificate: AdmittedProjectedSource<'a>,
    request: &ProjectionRequest,
    source_budget: &mut SourceBudget,
    projector_budget: &mut ProjectorBudget,
    cancel: &mut impl FnMut() -> bool,
) -> Result<(ValidatedProjection, ProjectionCounters), CoreError> {
    if cancel() {
        return Err(CoreError::Cancelled);
    }
    projector_budget.begin()?;
    if !certificate.matches_source(source) {
        return Err(CoreError::Unsupported);
    }
    primitive_preflight(request)?;
    projector_budget.reserve()?;
    if cancel() {
        return Err(CoreError::Cancelled);
    }
    projector_budget.helper()?;
    validate_structure(&request.descriptor, request.limits)?;
    if cancel() {
        return Err(CoreError::Cancelled);
    }
    if <[u8; 32]>::from(Sha256::digest(source.bytes())) != request.descriptor.source_digest {
        return Err(CoreError::Corrupt("source digest mismatch".into()));
    }
    let state = certificate.decode_canonical(source_budget, cancel)?;
    let (projection, counters) = project_decoded(
        &state,
        request,
        &mut ProjectionMode::Admitted(projector_budget),
        cancel,
    )?;
    if cancel() {
        return Err(CoreError::Cancelled);
    }
    Ok((projection, counters.ok_or(CoreError::Unsupported)?))
}
/// Counts/closed variants are checked before any sizing/formatting/clone call.
fn primitive_preflight(request: &ProjectionRequest) -> Result<(), CoreError> {
    let d = &request.descriptor;
    let t = &d.table;
    if t.is_heap() {
        return Err(CoreError::Unsupported);
    }
    if t.columns.is_empty()
        || t.columns.len() > 8
        || t.primary_key.is_empty()
        || t.primary_key.len() > t.columns.len()
        || t.name.is_empty()
        || t.name.len() > 63
        || request.limits.rows == 0
        || request.limits.rows > 64
        || request.limits.bytes == 0
        || request.limits.bytes > MAX_PROJECTED_BYTES
    {
        return Err(CoreError::Limit);
    }
    if !matches!(d.key_contract, KeyContract::RelationalEncodeKeyV1)
        || !matches!(d.homes.operations, OperationHome::OriginalOwningGroup)
        || d.limits != request.limits
        || d.contract_version != CONTRACT_VERSION
        || matches!(d.lower, Bound::PlusInfinity)
        || matches!(d.upper, Bound::MinusInfinity)
    {
        return Err(CoreError::Unsupported);
    }
    for c in &t.columns {
        if c.name.is_empty() || c.name.len() > 63 {
            return Err(CoreError::Limit);
        }
        if !matches!(
            c.data_type,
            DataType::Boolean
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Text
                | DataType::Bytes
        ) {
            return Err(CoreError::Unsupported);
        }
    }
    if t.primary_key.iter().any(|i| *i >= t.columns.len()) {
        return Err(CoreError::Unsupported);
    }
    for bound in [&d.lower, &d.upper] {
        if let Bound::Key(key) = bound {
            if key.values.len() != t.primary_key.len() || key.encoded.len() > 4120 {
                return Err(CoreError::Limit);
            }
            for value in &key.values {
                match value {
                    Value::Bool(_) | Value::Int(_) => {}
                    Value::Text(s) if s.len() <= 256 => {}
                    Value::Bytes(b) if b.len() <= 256 => {}
                    Value::Text(_) | Value::Bytes(_) => return Err(CoreError::Limit),
                    _ => return Err(CoreError::Unsupported),
                }
            }
        }
    }
    Ok(())
}
enum ProjectionMode<'a> {
    Ordinary,
    Admitted(&'a mut ProjectorBudget),
}
struct ProjectionGate<'a> {
    budget: Option<RefCell<&'a mut ProjectorBudget>>,
    cancel: RefCell<&'a mut dyn FnMut() -> bool>,
    observed: Cell<bool>,
}
impl ProjectionGate<'_> {
    fn cancelled(&self) -> bool {
        if self.observed.get() {
            return true;
        }
        let result = (self.cancel.borrow_mut())();
        if result {
            self.observed.set(true)
        }
        result
    }
    fn tick(&self) -> Result<(), CoreError> {
        if self.cancelled() {
            return Err(CoreError::Cancelled);
        }
        if let Some(b) = &self.budget {
            b.borrow_mut().tick()?
        }
        Ok(())
    }
    fn helper(&self) -> Result<(), CoreError> {
        self.tick()?;
        if let Some(b) = &self.budget {
            b.borrow_mut().helper()?
        }
        Ok(())
    }
    fn row(&self) -> Result<(), CoreError> {
        self.tick()?;
        if let Some(b) = &self.budget {
            b.borrow_mut().row()?
        }
        Ok(())
    }
    fn sql<T>(&self, r: Result<T, crate::relational::SqlError>) -> Result<T, CoreError> {
        r.map_err(|e| {
            if e == crate::relational::SqlError::Aborted && self.observed.get() {
                CoreError::Cancelled
            } else {
                CoreError::Sql(e)
            }
        })
    }
}
/// One traversal for both entries. Only this private helper borrows owned State.
fn project_decoded(
    source: &crate::storage::State,
    request: &ProjectionRequest,
    mode: &mut ProjectionMode<'_>,
    cancel: &mut impl FnMut() -> bool,
) -> Result<(ValidatedProjection, Option<ProjectionCounters>), CoreError> {
    let budget = match mode {
        ProjectionMode::Ordinary => None,
        ProjectionMode::Admitted(b) => Some(RefCell::new(&mut **b)),
    };
    let gate = ProjectionGate {
        budget,
        cancel: RefCell::new(cancel),
        observed: Cell::new(false),
    };
    gate.tick()?;
    let descriptor = &request.descriptor;
    let plan = if gate.budget.is_some() {
        Some(WirePlan::new(request)?)
    } else {
        None
    };
    let metadata = classify_source(source)?;
    if source.view.domain != descriptor.domain || source.sql.epoch != descriptor.catalog_epoch {
        return Err(CoreError::Unsupported);
    }
    gate.helper()?;
    if !exact(&metadata, &descriptor.metadata)? {
        return Err(CoreError::Unsupported);
    }
    let counters = if gate.budget.is_some() {
        gate.helper()?;
        Some(gate.sql(source.sql.projector_counters_engine(
            descriptor.catalog_epoch,
            descriptor.metadata.generation,
            &mut || gate.cancelled(),
        ))?)
    } else {
        None
    };
    gate.helper()?;
    let bounds = bind_bounds(descriptor, |values| {
        gate.sql(
            source
                .sql
                .canonical_pk_engine(descriptor.table.id, values, || gate.cancelled()),
        )
    })?;
    gate.helper()?;
    let projection = gate.sql(source.sql.with_complete_table_engine(
        descriptor.table.id,
        || gate.cancelled(),
        |table, rows| {
            Ok((|| {
                gate.helper()?;
                if TableIdentity::from(table) != descriptor.table {
                    return Err(CoreError::Unsupported);
                }
                if rows.len() > MAX_SOURCE_ROWS || plan.is_some() && rows.len() > 64 {
                    return Err(CoreError::Limit);
                }
                let mut count = 0usize;
                gate.helper()?;
                let mut bytes = envelope_base_size(descriptor, request.limits)?;
                if let Some(plan) = &plan
                    && bytes != plan.base
                {
                    return Err(CoreError::Unsupported);
                }
                for (key, row) in rows {
                    gate.row()?;
                    if bounds.contains(key) {
                        if row.version == 0 {
                            return Err(CoreError::Unsupported);
                        }
                        count = count.checked_add(1).ok_or(CoreError::Limit)?;
                        if count > request.limits.rows {
                            return Err(CoreError::Limit);
                        }
                        gate.helper()?;
                        let size = serialized_size(&(key, &row.values, row.version))?;
                        if plan.is_some() {
                            let predicted = row.values.iter().try_fold(
                                24usize.checked_add(key.len()).ok_or(CoreError::Limit)?,
                                |n, v| {
                                    n.checked_add(projector_budget::value_wire(v)?)
                                        .ok_or(CoreError::Limit)
                                },
                            )?;
                            if size != predicted {
                                return Err(CoreError::Unsupported);
                            }
                        }
                        add_size(&mut bytes, size, request.limits.bytes)?;
                    }
                }
                if let Some(plan) = &plan
                    && bytes.checked_sub(32).ok_or(CoreError::Limit)? > plan.digest_max
                {
                    return Err(CoreError::Limit);
                }
                gate.helper()?;
                let mut selected = Vec::with_capacity(count);
                for (key, row) in rows {
                    gate.row()?;
                    if bounds.contains(key) {
                        selected.push(ProjectedRow {
                            key: key.clone(),
                            values: row.values.clone(),
                            version: row.version,
                        });
                    }
                }
                gate.helper()?;
                let mut wire = UntrustedProjection {
                    descriptor: descriptor.clone(),
                    metadata,
                    namespace_manifest: NAMESPACE_MANIFEST,
                    limits: request.limits,
                    rows: selected,
                    digest: [0; 32],
                };
                gate.helper()?;
                wire.digest = content_digest(&wire)?;
                gate.tick()?;
                Ok(ValidatedProjection(wire))
            })())
        },
    ))??;
    gate.tick()?;
    Ok((projection, counters))
}

/// Validate untrusted content against an independently selected descriptor.
/// Hashes detect mismatch; neither hash authenticates a bootstrap source.
/// Callers must establish trusted committed provenance before runtime use.
pub fn validate_projection(
    projection: &UntrustedProjection,
    expected: &ExpectedDescriptor,
) -> Result<ValidatedProjection, CoreError> {
    admission_preflight(projection, expected)?;
    bind_row_values(projection, expected)?;
    // Build an empty validated engine catalog solely for its existing PK binder.
    // Fresh local IDs are descriptive: exact source identity was checked above.
    let mut catalog = crate::relational::Relational::default();
    let table = expected.table.definition();
    let mut binding_columns = table.columns.clone();
    for column in &mut binding_columns {
        column.id = 0;
    }
    let create = crate::relational::Prepared::create_engine(
        table.name.clone(),
        binding_columns,
        table
            .primary_key
            .iter()
            .map(|index| table.columns[*index].name.clone())
            .collect(),
        expected.domain,
        &catalog,
    )
    .map_err(CoreError::Sql)?;
    catalog
        .execute(&create, &[], None)
        .map_err(CoreError::Sql)?;
    let table_id = catalog.tables[&table.name].id;
    let bind = |values: &[Value]| {
        catalog
            .canonical_pk_engine(table_id, values, || false)
            .map_err(CoreError::Sql)
    };
    let bounds = bind_bounds(expected, bind)?;
    let mut previous: Option<&[u8]> = None;
    let mut versions = std::collections::BTreeSet::new();
    for row in &projection.rows {
        let values: Vec<Value> = table
            .primary_key
            .iter()
            .map(|index| row.values[*index].clone())
            .collect();
        if bind(&values)? != row.key
            || !bounds.contains(&row.key)
            || previous.is_some_and(|key| key >= row.key.as_slice())
            || !versions.insert(row.version)
        {
            return Err(CoreError::Unsupported);
        }
        previous = Some(&row.key);
    }
    Ok(ValidatedProjection(projection.clone()))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admitted_tests {
    use super::*;
    use crate::relational::{Float8, Numeric, SqlRow};
    use crate::storage::replica::projected_admission::{
        SourceAdmissionLimits, admit_projected_source,
    };
    use std::collections::BTreeMap;
    pub(super) fn fixture(wide: bool) -> (Image, ProjectionRequest) {
        let image=Image::new(123).unwrap().with_admin(|a|{
            let mut session=a.sql_session();
            if wide{
                session.execute("CREATE TABLE items (a BYTEA NOT NULL,b BYTEA NOT NULL,c BYTEA NOT NULL,d BYTEA NOT NULL,e BYTEA NOT NULL,f BYTEA NOT NULL,g BYTEA NOT NULL,h BYTEA NOT NULL,PRIMARY KEY (a,b,c,d,e,f,g,h))",&[],None)?;
                for i in 0..64{let mut vs=vec![Value::Bytes(vec![0;256]);8];if let Value::Bytes(v)=&mut vs[0]{v[0]=i;}session.execute("INSERT INTO items (a,b,c,d,e,f,g,h) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",&vs,None)?;}
            }else{
                session.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, payload TEXT)",&[],None)?;
                session.execute("INSERT INTO items (id,payload) VALUES (1,'a')",&[],None)?;
                for value in ["b","c","d"]{session.execute("UPDATE items SET payload=$1 WHERE id=1",&[Value::Text(value.into())],None)?;}
            }
            Ok(())
        }).unwrap().0;
        // Trusted fixture inspection occurs OUTSIDE either admission boundary.
        let mut state =
            crate::storage::replica::replica_state_with_cancel(image.bytes(), &mut || false)
                .unwrap();
        let image = if wide {
            // Trusted fixture metadata extension, validated canonically before admission.
            let mut table = state.sql.tables.remove("items").unwrap();
            table.name = "t".to_owned() + &"x".repeat(62);
            for (i, column) in table.columns.iter_mut().enumerate() {
                column.name = char::from(b'a' + i as u8).to_string() + &"x".repeat(62);
            }
            state.sql.tables.insert(table.name.clone(), table);
            state.origin = Some(44);
            Image::from_bytes(
                crate::storage::encode(&crate::storage::records(&state).unwrap()).unwrap(),
            )
            .unwrap()
        } else {
            image
        };
        let limits = ProjectionLimits {
            rows: 64,
            bytes: MAX_PROJECTED_BYTES,
        };
        let mut descriptor = ExpectedDescriptor {
            contract_version: 1,
            domain: 123,
            catalog_epoch: state.sql.epoch,
            table: TableIdentity::from(state.sql.tables.values().next().unwrap()),
            key_contract: KeyContract::RelationalEncodeKeyV1,
            range: 1,
            group: 2,
            ownership_epoch: 3,
            lower: Bound::MinusInfinity,
            upper: Bound::PlusInfinity,
            source_applied_index: 7,
            source_digest: Sha256::digest(image.bytes()).into(),
            metadata: classify_source(&state).unwrap(),
            limits,
            homes: Homes {
                catalog: 2,
                authority: 2,
                operations: OperationHome::OriginalOwningGroup,
            },
        };
        if wide {
            let values = vec![Value::Bytes(vec![0; 256]); 8];
            let mut upper = values.clone();
            if let Value::Bytes(v) = &mut upper[0] {
                v[0] = 255;
            }
            descriptor.lower = Bound::Key(CompleteKey {
                encoded: crate::relational::encode_key(&values).unwrap(),
                values,
            });
            descriptor.upper = Bound::Key(CompleteKey {
                encoded: crate::relational::encode_key(&upper).unwrap(),
                values: upper,
            });
        }
        (image, ProjectionRequest { descriptor, limits })
    }
    fn source_budget() -> SourceBudget {
        SourceBudget::new(SourceAdmissionLimits::default())
    }
    pub(super) fn admitted(
        image: &Image,
        request: &ProjectionRequest,
        budget: &mut ProjectorBudget,
        cancel: &mut impl FnMut() -> bool,
    ) -> Result<(ValidatedProjection, ProjectionCounters), CoreError> {
        let mut source = source_budget();
        let certificate = admit_projected_source(
            image,
            &SourceAdmissionLimits::default(),
            &mut source,
            &mut || false,
        )?;
        project_admitted(image, certificate, request, &mut source, budget, cancel)
    }
    #[test]
    fn admitted_matches_ordinary_and_source_immutable() {
        let (image, request) = fixture(false);
        let original = image.bytes().to_vec();
        let ordinary = project(&image, &request).unwrap();
        let mut b = ProjectorBudget::new(Default::default());
        let (p, _) = admitted(&image, &request, &mut b, &mut || false).unwrap();
        assert_eq!(p.as_untrusted(), ordinary.as_untrusted());
        assert_eq!(image.bytes(), original);
        assert_eq!(b.costs().allocations, 904381);
        assert!(b.costs().actual_controls <= b.costs().work.controls);
    }
    #[test]
    fn exact_object_identity_before_consume() {
        let (image, request) = fixture(false);
        let duplicate = image.clone();
        let mut sb = source_budget();
        let cert = admit_projected_source(
            &image,
            &SourceAdmissionLimits::default(),
            &mut sb,
            &mut || false,
        )
        .unwrap();
        assert!(cert.matches_source(&image));
        assert!(!cert.matches_source(&duplicate));
        let mut calls = 0;
        let mut b = ProjectorBudget::new(Default::default());
        assert!(matches!(
            project_admitted(&duplicate, cert, &request, &mut sb, &mut b, &mut || {
                calls += 1;
                false
            }),
            Err(CoreError::Unsupported)
        ));
        assert_eq!(calls, 1);
        assert_eq!(b.costs().helpers, 0);
        assert!(b.costs().work.controls >= b.costs().actual_controls);
        assert_eq!(b.costs().allocations, 0);
    }
    #[test]
    fn phase_and_work_n_nplus1_before_helpers() {
        let (image, request) = fixture(false);
        let mut b = ProjectorBudget::new(Default::default());
        admitted(&image, &request, &mut b, &mut || false).unwrap();
        let c = b.costs();
        let caps = [
            c.allocations,
            c.phases[0],
            c.phases[1],
            c.phases[2],
            c.phases[3],
            c.work.serde,
            c.work.controls,
            c.work.bytes,
            c.work.comparisons,
            c.work.moves,
            c.work.values,
            c.work.rows,
        ];
        for (component, n) in caps.into_iter().enumerate() {
            for delta in [0, 1] {
                let mut budget = ProjectorBudget::limited(component, n - delta);
                let result = admitted(&image, &request, &mut budget, &mut || false);
                assert_eq!(result.is_ok(), delta == 0, "component{component}");
                if delta == 1 {
                    assert_eq!(budget.costs().helpers, 0);
                }
            }
        }
    }
    #[test]
    fn preflight_denies_unsupported_before_sizing() {
        let (image, base) = fixture(false);
        for value in [
            Value::Null,
            Value::Numeric(Numeric::parse("1.00").unwrap()),
            Value::Float8(Float8::from_bits(1)),
            Value::Float4(crate::relational::Float4::from_bits(1)),
            Value::Date(crate::relational::Date::from_days(0).unwrap()),
            Value::Text("x".repeat(257)),
            Value::Bytes(vec![0; 257]),
        ] {
            let mut request = base.clone();
            request.descriptor.lower = Bound::Key(CompleteKey {
                values: vec![value],
                encoded: vec![],
            });
            let mut b = ProjectorBudget::new(Default::default());
            assert!(admitted(&image, &request, &mut b, &mut || false).is_err());
            assert_eq!(b.costs().helpers, 0);
            assert!(b.costs().work.controls >= b.costs().actual_controls);
            assert_eq!(b.costs().allocations, 0);
        }
        for dt in [
            DataType::Numeric,
            DataType::Real,
            DataType::DoublePrecision,
            DataType::Date,
            DataType::Time,
            DataType::TimeTz,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Interval,
        ] {
            let mut request = base.clone();
            request.descriptor.table.columns[0].data_type = dt;
            let mut b = ProjectorBudget::new(Default::default());
            assert!(admitted(&image, &request, &mut b, &mut || false).is_err());
            assert_eq!(b.costs().helpers, 0);
            assert!(b.costs().work.controls >= b.costs().actual_controls);
        }
    }
    #[test]
    fn request_count_name_key_bounds_precede_helpers() {
        let (image, base) = fixture(false);
        for which in 0..8 {
            let mut q = base.clone();
            match which {
                0 => q.descriptor.table.name = "x".repeat(64),
                1 => q.descriptor.table.columns[0].name = "x".repeat(64),
                2 => q.descriptor.table.columns = vec![q.descriptor.table.columns[0].clone(); 9],
                3 => q.descriptor.table.primary_key = vec![0; 9],
                4 => q.descriptor.table.primary_key = vec![9],
                5 => {
                    q.limits.rows = 65;
                    q.descriptor.limits = q.limits
                }
                6 => {
                    q.descriptor.lower = Bound::Key(CompleteKey {
                        values: vec![Value::Int(1)],
                        encoded: vec![0; 4121],
                    })
                }
                _ => {
                    q.descriptor.lower = Bound::Key(CompleteKey {
                        values: vec![Value::Int(1); 2],
                        encoded: vec![],
                    })
                }
            }
            let mut b = ProjectorBudget::new(Default::default());
            assert!(admitted(&image, &q, &mut b, &mut || false).is_err());
            assert_eq!(b.costs().helpers, 0);
            assert!(b.costs().work.controls >= b.costs().actual_controls);
            assert_eq!(b.costs().allocations, 0);
        }
    }
    #[test]
    fn initial_mid_final_cancel_and_sticky_no_double_poll() {
        let (image, q) = fixture(false);
        let mut total = 0;
        let mut b = ProjectorBudget::new(Default::default());
        admitted(&image, &q, &mut b, &mut || {
            total += 1;
            false
        })
        .unwrap();
        for at in [1, 4, 10, total] {
            let mut n = 0;
            let mut b = ProjectorBudget::new(Default::default());
            assert!(matches!(
                admitted(&image, &q, &mut b, &mut || {
                    n += 1;
                    n == at
                }),
                Err(CoreError::Cancelled)
            ));
            assert_eq!(n, at);
        }
        let ambient = crate::Cancellation::default();
        ambient.cancel();
        ambient.scope(|| {
            let mut b = ProjectorBudget::new(Default::default());
            admitted(&image, &q, &mut b, &mut || false).unwrap();
            assert!(crate::cancellation::check().is_err());
        });
    }
    #[test]
    fn original_counters_exceed_empty_projection_max() {
        let (image, mut q) = fixture(false);
        q.descriptor.lower = Bound::Key(CompleteKey {
            values: vec![Value::Int(2)],
            encoded: crate::relational::encode_key(&[Value::Int(2)]).unwrap(),
        });
        let mut b = ProjectorBudget::new(Default::default());
        let (p, counters) = admitted(&image, &q, &mut b, &mut || false).unwrap();
        assert!(p.rows().is_empty());
        let rows = BTreeMap::from([(
            crate::relational::encode_key(&[Value::Int(1)]).unwrap(),
            SqlRow {
                values: vec![Value::Int(1), Value::Text("d".into())],
                version: 4,
            },
        )]);
        crate::relational::projector_reconstruction::ScratchRelation::reconstruct(
            &q.descriptor.table.definition(),
            &rows,
            &counters,
            &Default::default(),
            &mut || false,
        )
        .unwrap();
    }
    #[test]
    fn exhausted_counters_reject_only_admitted() {
        let (image, base) = fixture(false);
        for offset in [20, 28] {
            let mut q = base.clone();
            let mut records: BTreeMap<Vec<u8>, Vec<u8>> =
                crate::storage::decode(image.bytes()).unwrap();
            // Test fixture mutation of the fixed header emitted by the real
            // serializer, never a product wire parser or alternate type schema.
            records.get_mut(&vec![5]).unwrap()[offset..offset + 8]
                .copy_from_slice(&u64::MAX.to_be_bytes());
            let altered = Image::from_bytes(crate::storage::encode(&records).unwrap()).unwrap();
            q.descriptor.source_digest = Sha256::digest(altered.bytes()).into();
            assert!(project(&altered, &q).is_ok());
            let mut b = ProjectorBudget::new(Default::default());
            assert!(matches!(
                admitted(&altered, &q, &mut b, &mut || false),
                Err(CoreError::Sql(crate::relational::SqlError::Overflow))
            ));
        }
    }
    #[test]
    fn actual_wire_formula_after_source_admission() {
        let (image, q) = fixture(false);
        let mut b = ProjectorBudget::new(Default::default());
        let (p, _) = admitted(&image, &q, &mut b, &mut || false).unwrap();
        let plan = WirePlan::new(&q).unwrap();
        let rowbytes = p
            .rows()
            .iter()
            .map(|row| serialized_size(&(row.key.as_slice(), &row.values, row.version)).unwrap())
            .sum::<usize>();
        assert_eq!(
            serialized_size(p.as_untrusted()).unwrap(),
            plan.base + rowbytes
        );
        let tuple = (
            &p.0.descriptor,
            &p.0.metadata,
            &p.0.namespace_manifest,
            p.0.limits,
            &p.0.rows,
        );
        assert_eq!(serialized_size(&tuple).unwrap() + 32, plan.base + rowbytes);
    }
    #[test]
    fn maximum_profile_and_mismatched_small_request() {
        let (image, q) = fixture(true);
        let mut b = ProjectorBudget::new(Default::default());
        let (p, _) = admitted(&image, &q, &mut b, &mut || false).unwrap();
        assert_eq!(p.rows().len(), 64);
        assert!(serialized_size(p.as_untrusted()).unwrap() <= 416189);
        assert!(b.costs().actual_controls <= b.costs().work.controls);
        let mut small = q.clone();
        small.descriptor.lower = Bound::MinusInfinity;
        small.descriptor.upper = Bound::PlusInfinity;
        small.descriptor.table.columns.truncate(1);
        small.descriptor.table.primary_key = vec![0];
        let mut b = ProjectorBudget::new(Default::default());
        assert!(admitted(&image, &small, &mut b, &mut || false).is_err());
        assert_eq!(b.costs().allocations, 904381);
    }
    #[test]
    fn independent_ledgers_never_refund_on_failure() {
        let (image, mut q) = fixture(false);
        q.descriptor.domain += 1;
        let mut sb = source_budget();
        let cert = admit_projected_source(
            &image,
            &SourceAdmissionLimits::default(),
            &mut sb,
            &mut || false,
        )
        .unwrap();
        let mut b = ProjectorBudget::new(Default::default());
        assert!(project_admitted(&image, cert, &q, &mut sb, &mut b, &mut || false).is_err());
        assert_eq!(b.costs().allocations, 904381);
        assert!(matches!(
            admit_projected_source(
                &image,
                &SourceAdmissionLimits::default(),
                &mut sb,
                &mut || false
            ),
            Err(CoreError::Limit)
        ));
        let saved = b.costs();
        assert!(admitted(&image, &q, &mut b, &mut || false).is_err());
        assert_eq!(b.costs().allocations, saved.allocations);
    }
    #[test]
    fn admitted_encoded_limit_exact_and_mismatch_metadata() {
        let (image, q) = fixture(false);
        let mut b = ProjectorBudget::new(Default::default());
        let (p, _) = admitted(&image, &q, &mut b, &mut || false).unwrap();
        let bytes = serialized_size(p.as_untrusted()).unwrap();
        for delta in [0, 1] {
            let mut q = q.clone();
            q.limits.bytes = bytes - delta;
            q.descriptor.limits = q.limits;
            let mut b = ProjectorBudget::new(Default::default());
            assert_eq!(
                admitted(&image, &q, &mut b, &mut || false).is_ok(),
                delta == 0
            );
        }
        let (image, mut q) = fixture(true);
        q.descriptor.metadata.origin = None;
        let original = image.bytes().to_vec();
        let mut b = ProjectorBudget::new(Default::default());
        assert!(matches!(
            admitted(&image, &q, &mut b, &mut || false),
            Err(CoreError::Unsupported)
        ));
        assert_eq!(b.costs().allocations, 904381);
        assert_eq!(image.bytes(), original);
    }
    #[test]
    fn empty_source_row_map_uses_same_traversal() {
        let (image, mut q) = fixture(false);
        let image = image
            .with_admin(|a| {
                a.sql_session()
                    .execute("DELETE FROM items WHERE id=1", &[], None)?;
                Ok(())
            })
            .unwrap()
            .0;
        let state =
            crate::storage::replica::replica_state_with_cancel(image.bytes(), &mut || false)
                .unwrap();
        q.descriptor.metadata = classify_source(&state).unwrap();
        q.descriptor.source_digest = Sha256::digest(image.bytes()).into();
        let mut b = ProjectorBudget::new(Default::default());
        let (p, _) = admitted(&image, &q, &mut b, &mut || false).unwrap();
        assert!(p.rows().is_empty());
        assert_eq!(b.costs().actual_rows, 0);
        assert_eq!(
            p.as_untrusted(),
            project(&image, &q).unwrap().as_untrusted()
        );
    }
}
