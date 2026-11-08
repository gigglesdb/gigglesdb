//! Frozen v2 catalog wire types. Field and variant order are persisted format.
use super::catalog::TableEntry;
use crate::{
    CoreError,
    relational::{
        ColumnDef, DataType, MAX_COLUMNS, TableDef,
        catalog_ext::{
            Action, ColumnExt, Constraint, ConstraintKind, Deferral, Identity, SequenceDef,
            TypeDef, validate_typmod,
        },
        validate_create_definition,
    },
    storage::sequences::SeqCounter,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(message.into())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableEntryV2Dto {
    def: TableDefDto,
    heap: bool,
    columns_ext: BTreeMap<u64, ColumnExtDto>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TableDefDto {
    id: u64,
    name: String,
    columns: Vec<ColumnDefDto>,
    primary_key: Vec<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ColumnDefDto {
    id: u64,
    name: String,
    data_type: DataTypeDto,
    not_null: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum DataTypeDto {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Bytes,
    Numeric,
    Real,
    DoublePrecision,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
}
impl From<DataType> for DataTypeDto {
    fn from(v: DataType) -> Self {
        match v {
            DataType::Boolean => Self::Boolean,
            DataType::SmallInt => Self::SmallInt,
            DataType::Integer => Self::Integer,
            DataType::BigInt => Self::BigInt,
            DataType::Text => Self::Text,
            DataType::Bytes => Self::Bytes,
            DataType::Numeric => Self::Numeric,
            DataType::Real => Self::Real,
            DataType::DoublePrecision => Self::DoublePrecision,
            DataType::Date => Self::Date,
            DataType::Time => Self::Time,
            DataType::TimeTz => Self::TimeTz,
            DataType::Timestamp => Self::Timestamp,
            DataType::TimestampTz => Self::TimestampTz,
            DataType::Interval => Self::Interval,
        }
    }
}
impl From<DataTypeDto> for DataType {
    fn from(v: DataTypeDto) -> Self {
        match v {
            DataTypeDto::Boolean => Self::Boolean,
            DataTypeDto::SmallInt => Self::SmallInt,
            DataTypeDto::Integer => Self::Integer,
            DataTypeDto::BigInt => Self::BigInt,
            DataTypeDto::Text => Self::Text,
            DataTypeDto::Bytes => Self::Bytes,
            DataTypeDto::Numeric => Self::Numeric,
            DataTypeDto::Real => Self::Real,
            DataTypeDto::DoublePrecision => Self::DoublePrecision,
            DataTypeDto::Date => Self::Date,
            DataTypeDto::Time => Self::Time,
            DataTypeDto::TimeTz => Self::TimeTz,
            DataTypeDto::Timestamp => Self::Timestamp,
            DataTypeDto::TimestampTz => Self::TimestampTz,
            DataTypeDto::Interval => Self::Interval,
        }
    }
}
impl From<ColumnDef> for ColumnDefDto {
    fn from(v: ColumnDef) -> Self {
        let ColumnDef {
            id,
            name,
            data_type,
            not_null,
        } = v;
        Self {
            id,
            name,
            data_type: data_type.into(),
            not_null,
        }
    }
}
impl From<ColumnDefDto> for ColumnDef {
    fn from(v: ColumnDefDto) -> Self {
        let ColumnDefDto {
            id,
            name,
            data_type,
            not_null,
        } = v;
        Self {
            id,
            name,
            data_type: data_type.into(),
            not_null,
        }
    }
}
impl TryFrom<TableDef> for TableDefDto {
    type Error = CoreError;
    fn try_from(v: TableDef) -> Result<Self, CoreError> {
        let TableDef {
            id,
            name,
            columns,
            primary_key,
        } = v;
        Ok(Self {
            id,
            name,
            columns: columns.into_iter().map(Into::into).collect(),
            primary_key: primary_key
                .into_iter()
                .map(|i| u64::try_from(i).map_err(|_| corrupt("column offset overflow")))
                .collect::<Result<_, _>>()?,
        })
    }
}
impl TryFrom<TableDefDto> for TableDef {
    type Error = CoreError;
    fn try_from(v: TableDefDto) -> Result<Self, CoreError> {
        let TableDefDto {
            id,
            name,
            columns,
            primary_key,
        } = v;
        Ok(Self {
            id,
            name,
            columns: columns.into_iter().map(Into::into).collect(),
            primary_key: primary_key
                .into_iter()
                .map(|i| usize::try_from(i).map_err(|_| corrupt("column offset overflow")))
                .collect::<Result<_, _>>()?,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ColumnExtDto {
    typmod: Option<i32>,
    default: Option<String>,
    identity: Option<IdentityDto>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IdentityDto {
    always: bool,
    sequence_id: u64,
}
impl From<Identity> for IdentityDto {
    fn from(v: Identity) -> Self {
        let Identity {
            always,
            sequence_id,
        } = v;
        Self {
            always,
            sequence_id,
        }
    }
}
impl From<IdentityDto> for Identity {
    fn from(v: IdentityDto) -> Self {
        let IdentityDto {
            always,
            sequence_id,
        } = v;
        Self {
            always,
            sequence_id,
        }
    }
}
impl From<ColumnExt> for ColumnExtDto {
    fn from(v: ColumnExt) -> Self {
        let ColumnExt {
            typmod,
            default,
            identity,
        } = v;
        Self {
            typmod,
            default,
            identity: identity.map(Into::into),
        }
    }
}
impl From<ColumnExtDto> for ColumnExt {
    fn from(v: ColumnExtDto) -> Self {
        let ColumnExtDto {
            typmod,
            default,
            identity,
        } = v;
        Self {
            typmod,
            default,
            identity: identity.map(Into::into),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DeferralDto {
    NotDeferrable,
    DeferrableImmediate,
    DeferrableDeferred,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ActionDto {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}
impl From<Action> for ActionDto {
    fn from(v: Action) -> Self {
        match v {
            Action::NoAction => Self::NoAction,
            Action::Restrict => Self::Restrict,
            Action::Cascade => Self::Cascade,
            Action::SetNull => Self::SetNull,
            Action::SetDefault => Self::SetDefault,
        }
    }
}
impl From<ActionDto> for Action {
    fn from(v: ActionDto) -> Self {
        match v {
            ActionDto::NoAction => Self::NoAction,
            ActionDto::Restrict => Self::Restrict,
            ActionDto::Cascade => Self::Cascade,
            ActionDto::SetNull => Self::SetNull,
            ActionDto::SetDefault => Self::SetDefault,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConstraintDto {
    id: u64,
    name: Option<String>,
    kind: ConstraintKindDto,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ConstraintKindDto {
    Check {
        expr: String,
        not_valid: bool,
    },
    ForeignKey {
        columns: Vec<u64>,
        ref_table: u64,
        ref_columns: Vec<u64>,
        on_delete: ActionDto,
        on_update: ActionDto,
        not_valid: bool,
        deferral: DeferralDto,
    },
    PrimaryKey,
}
impl From<Deferral> for DeferralDto {
    fn from(v: Deferral) -> Self {
        match v {
            Deferral::NotDeferrable => Self::NotDeferrable,
            Deferral::DeferrableImmediate => Self::DeferrableImmediate,
            Deferral::DeferrableDeferred => Self::DeferrableDeferred,
        }
    }
}
impl From<DeferralDto> for Deferral {
    fn from(v: DeferralDto) -> Self {
        match v {
            DeferralDto::NotDeferrable => Self::NotDeferrable,
            DeferralDto::DeferrableImmediate => Self::DeferrableImmediate,
            DeferralDto::DeferrableDeferred => Self::DeferrableDeferred,
        }
    }
}
// The DTO layout was frozen with `deferral` before E1's live field landed
// (82ea06ab); mapping it changes no bytes.
impl From<ConstraintKind> for ConstraintKindDto {
    fn from(v: ConstraintKind) -> Self {
        match v {
            ConstraintKind::Check { expr, not_valid } => Self::Check { expr, not_valid },
            ConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                not_valid,
                deferral,
            } => Self::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete: on_delete.into(),
                on_update: on_update.into(),
                not_valid,
                deferral: deferral.into(),
            },
            ConstraintKind::PrimaryKey => Self::PrimaryKey,
        }
    }
}
impl TryFrom<ConstraintKindDto> for ConstraintKind {
    type Error = CoreError;
    fn try_from(v: ConstraintKindDto) -> Result<Self, CoreError> {
        Ok(match v {
            ConstraintKindDto::Check { expr, not_valid } => Self::Check { expr, not_valid },
            ConstraintKindDto::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                not_valid,
                deferral,
            } => Self::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete: on_delete.into(),
                on_update: on_update.into(),
                not_valid,
                deferral: deferral.into(),
            },
            ConstraintKindDto::PrimaryKey => Self::PrimaryKey,
        })
    }
}
impl From<Constraint> for ConstraintDto {
    fn from(v: Constraint) -> Self {
        let Constraint { id, name, kind } = v;
        Self {
            id,
            name: Some(name),
            kind: kind.into(),
        }
    }
}
impl TryFrom<ConstraintDto> for Constraint {
    type Error = CoreError;
    fn try_from(v: ConstraintDto) -> Result<Self, CoreError> {
        let name = v.name.ok_or_else(|| corrupt("missing constraint name"))?;
        Ok(Self {
            id: v.id,
            name,
            kind: v.kind.try_into()?,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceDefDto {
    name: String,
    data_type: DataTypeDto,
    start: i64,
    increment: i64,
    min: i64,
    max: i64,
    cache: i64,
    cycle: bool,
    owned_by: Option<(u64, u64)>,
    identity: bool,
    incarnation: u128,
}
impl From<SequenceDef> for SequenceDefDto {
    fn from(v: SequenceDef) -> Self {
        let SequenceDef {
            name,
            data_type,
            start,
            increment,
            min,
            max,
            cache,
            cycle,
            owned_by,
            identity,
            incarnation,
        } = v;
        Self {
            name,
            start,
            increment,
            min,
            max,
            cache,
            cycle,
            owned_by,
            identity,
            incarnation,
            data_type: data_type.into(),
        }
    }
}
impl From<SequenceDefDto> for SequenceDef {
    fn from(v: SequenceDefDto) -> Self {
        let SequenceDefDto {
            name,
            data_type,
            start,
            increment,
            min,
            max,
            cache,
            cycle,
            owned_by,
            identity,
            incarnation,
        } = v;
        Self {
            name,
            start,
            increment,
            min,
            max,
            cache,
            cycle,
            owned_by,
            identity,
            incarnation,
            data_type: data_type.into(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TypeDefDto {}
impl From<TypeDef> for TypeDefDto {
    fn from(v: TypeDef) -> Self {
        match v {}
    }
}
impl From<TypeDefDto> for TypeDef {
    fn from(v: TypeDefDto) -> Self {
        match v {}
    }
}
fn encode_version<T: Serialize>(version: u8, v: &T) -> Result<Vec<u8>, CoreError> {
    let mut out = vec![version];
    out.extend(super::super::encode(v)?);
    Ok(out)
}
fn decode_version<T: DeserializeOwned>(version: u8, v: &[u8]) -> Result<T, CoreError> {
    if v.first() != Some(&version) {
        return Err(corrupt("unsupported catalog version"));
    }
    super::super::decode(&v[1..])
}
impl TryFrom<(TableEntry, BTreeMap<u64, ColumnExt>)> for TableEntryV2Dto {
    type Error = CoreError;
    fn try_from((entry, ext): (TableEntry, BTreeMap<u64, ColumnExt>)) -> Result<Self, CoreError> {
        let TableEntry { def, heap, .. } = entry;
        Ok(Self {
            def: def.try_into()?,
            heap,
            columns_ext: ext.into_iter().map(|(k, v)| (k, v.into())).collect(),
        })
    }
}
impl TryFrom<TableEntryV2Dto> for (TableEntry, BTreeMap<u64, ColumnExt>) {
    type Error = CoreError;
    fn try_from(v: TableEntryV2Dto) -> Result<Self, CoreError> {
        let TableEntryV2Dto {
            def,
            heap,
            columns_ext,
        } = v;
        let columns_ext: BTreeMap<u64, ColumnExt> = columns_ext
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();
        Ok((
            TableEntry {
                def: def.try_into()?,
                heap,
                columns_ext: columns_ext.clone(),
            },
            columns_ext,
        ))
    }
}
pub(crate) fn encode_table(
    entry: &TableEntry,
    ext: &BTreeMap<u64, ColumnExt>,
) -> Result<Vec<u8>, CoreError> {
    encode_version(2, &TableEntryV2Dto::try_from((entry.clone(), ext.clone()))?)
}
pub(crate) fn decode_table(
    value: &[u8],
) -> Result<(TableEntry, BTreeMap<u64, ColumnExt>), CoreError> {
    let dto: TableEntryV2Dto = decode_version(2, value)?;
    if dto.columns_ext.len() > crate::relational::catalog_ext::MAX_EXT_COLUMNS
        || dto
            .columns_ext
            .values()
            .any(|v| v.default.as_ref().is_some_and(|s| s.len() > 65536))
    {
        return Err(corrupt("catalog column extension limit"));
    }
    let (entry, ext): (TableEntry, BTreeMap<u64, ColumnExt>) = dto.try_into()?;
    validate_definition(&entry.def)?;
    for (id, column_ext) in &ext {
        let column = entry
            .def
            .columns
            .iter()
            .find(|column| column.id == *id)
            .ok_or_else(|| corrupt("column extension references absent column"))?;
        if let Some(typmod) = column_ext.typmod {
            validate_typmod(&column.data_type, typmod)
                .map_err(|_| corrupt("invalid catalog column typmod"))?;
        }
    }
    Ok((entry, ext))
}
/// Per-entry checks only: catalog-wide id allocation and references remain the
/// caller's responsibility. The exact hidden heap column is a legacy exception to
/// ordinary identifier validation.
fn validate_definition(def: &TableDef) -> Result<(), CoreError> {
    if def.id == 0 || def.columns.is_empty() || def.columns.len() > MAX_COLUMNS {
        return Err(corrupt("invalid catalog table definition"));
    }
    let mut ids = BTreeSet::new();
    let mut offsets = BTreeSet::new();
    for column in &def.columns {
        if column.id == 0 || !ids.insert(column.id) {
            return Err(corrupt("invalid catalog column id"));
        }
    }
    for &offset in &def.primary_key {
        let column = def
            .columns
            .get(offset)
            .ok_or_else(|| corrupt("invalid primary key offset"))?;
        if !offsets.insert(offset) || !column.not_null {
            return Err(corrupt("invalid primary key column"));
        }
    }
    let columns: Vec<_> = def.logical_columns().cloned().collect();
    let keys: Vec<_> = def
        .primary_key
        .iter()
        .filter(|&&offset| def.hidden_rowid() != Some(offset))
        .map(|&offset| def.columns[offset].name.clone())
        .collect();
    validate_create_definition(&def.name, &columns, &keys)
        .map_err(|_| corrupt("invalid catalog table definition"))
}
pub(crate) fn encode_constraint(v: &Constraint, _table: &str) -> Result<Vec<u8>, CoreError> {
    encode_version(1, &ConstraintDto::from(v.clone()))
}
pub(crate) fn decode_constraint(v: &[u8], table: &TableDef) -> Result<Constraint, CoreError> {
    let dto: ConstraintDto = decode_version(1, v)?;
    if dto
        .name
        .as_ref()
        .is_some_and(|s| s.is_empty() || s.len() > 63)
    {
        return Err(corrupt("constraint name limit"));
    }
    match &dto.kind {
        ConstraintKindDto::Check { expr, .. } if expr.len() > 65536 => {
            return Err(corrupt("constraint expression limit"));
        }
        ConstraintKindDto::ForeignKey {
            columns,
            ref_columns,
            ..
        } if columns.len() > MAX_COLUMNS
            || ref_columns.len() > MAX_COLUMNS
            || columns
                .iter()
                .any(|id| !table.columns.iter().any(|column| column.id == *id)) =>
        {
            return Err(corrupt("invalid constraint columns"));
        }
        _ => {}
    }
    Constraint::try_from(dto)
}
pub(crate) fn encode_sequence(v: &SequenceDef) -> Result<Vec<u8>, CoreError> {
    encode_version(1, &SequenceDefDto::from(v.clone()))
}
pub(crate) fn decode_sequence(v: &[u8]) -> Result<SequenceDef, CoreError> {
    let dto: SequenceDefDto = decode_version(1, v)?;
    if dto.name.is_empty() || dto.name.len() > 63 {
        return Err(corrupt("sequence name limit"));
    }
    Ok(dto.into())
}
pub(crate) fn encode_type(v: TypeDef) -> Result<Vec<u8>, CoreError> {
    match v {}
}
pub(crate) fn decode_type(v: &[u8]) -> Result<TypeDef, CoreError> {
    Ok(decode_version::<TypeDefDto>(1, v)?.into())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogKind {
    Table,
    Name,
    Constraint,
    Sequence,
    Type,
    RoleBootstrap,
}
pub(crate) fn classify_catalog_key(key: &[u8]) -> Result<CatalogKind, CoreError> {
    Ok(match key.first() {
        Some(1) if key.len() == 9 => CatalogKind::Table,
        Some(3)
            if key.len() >= 3
                && key.len() <= 65
                && key.last() == Some(&0)
                && !key[1..key.len() - 1].contains(&0)
                && std::str::from_utf8(&key[1..key.len() - 1]).is_ok() =>
        {
            CatalogKind::Name
        }
        Some(4) if key.len() == 17 => CatalogKind::Constraint,
        Some(5) if key.len() == 9 => CatalogKind::Sequence,
        Some(6) if key.len() == 9 => CatalogKind::Type,
        // Existing role bootstrap fixture: engine/meta_tests.rs role().
        Some(7) if key.len() == 9 => CatalogKind::RoleBootstrap,
        _ => return Err(corrupt("invalid catalog user key")),
    })
}
fn object_key(kind: u8, id: u64) -> Vec<u8> {
    let mut key = vec![kind];
    key.extend(id.to_be_bytes());
    key
}
pub(crate) fn table_key(id: u64) -> Vec<u8> {
    object_key(1, id)
}
pub(crate) fn name_key(name: &str) -> Vec<u8> {
    let mut key = vec![3];
    key.extend(name.as_bytes());
    key.push(0);
    key
}
pub(crate) fn constraint_key(table: u64, id: u64) -> Vec<u8> {
    let mut key = object_key(4, table);
    key.extend(id.to_be_bytes());
    key
}
pub(crate) fn sequence_key(id: u64) -> Vec<u8> {
    object_key(5, id)
}
pub(crate) fn type_key(id: u64) -> Vec<u8> {
    object_key(6, id)
}
pub(crate) fn counter_key(id: u64, incarnation: u128) -> Vec<u8> {
    let mut key = object_key(2, id);
    key.extend(incarnation.to_be_bytes());
    key
}
/// Mirrors legacy sequence_counters.rs: line 48 checks lengths/prefix; lines
/// 52/57 bind incarnation; line 62 decodes a strict bool; line 63 requires
/// last == log_hw; lines 25-26 require both values inside definition bounds.
/// Caller contract: `def` is the live kind-5 definition at the latest committed
/// version for the seq_id in `key[1..9]` (legacy line 52 looks it up by that id).
pub(crate) fn decode_counter(
    key: &[u8],
    value: &[u8],
    def: &SequenceDef,
) -> Result<SeqCounter, CoreError> {
    if key.len() != 25 || key.first() != Some(&2) || value.len() != 25 {
        return Err(corrupt("invalid sequence counter length/prefix"));
    }
    let incarnation = u128::from_be_bytes(key[9..25].try_into().expect("checked length"));
    let (last_value, is_called, log_hw, stamp): (i64, bool, i64, u64) =
        super::super::decode(value)?;
    if incarnation != def.incarnation
        || last_value != log_hw
        || !(def.min..=def.max).contains(&last_value)
        || !(def.min..=def.max).contains(&log_hw)
    {
        return Err(corrupt("sequence counter invariant"));
    }
    Ok(SeqCounter {
        incarnation,
        last_value,
        is_called,
        log_hw,
        stamp,
    })
}
pub(crate) fn encode_counter(
    counter: &SeqCounter,
    def: &SequenceDef,
) -> Result<Vec<u8>, CoreError> {
    if counter.incarnation != def.incarnation
        || !(def.min..=def.max).contains(&counter.last_value)
        || !(def.min..=def.max).contains(&counter.log_hw)
        || (!counter.is_called && counter.last_value != counter.log_hw)
    {
        return Err(corrupt("sequence counter invariant"));
    }
    // Persist the entire durable reservation, exactly as the legacy writer does.
    let value = if counter.is_called {
        counter.log_hw
    } else {
        counter.last_value
    };
    super::super::encode(&(value, counter.is_called, counter.log_hw, counter.stamp))
}
#[cfg(test)]
#[path = "catalog_codec_tests.rs"]
mod tests;
