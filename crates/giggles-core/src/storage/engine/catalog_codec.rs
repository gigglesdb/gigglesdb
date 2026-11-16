//! Frozen v2 catalog wire types. Field and variant order are persisted format.
#[path = "catalog_view_codec.rs"]
pub(crate) mod sql_views;
use super::catalog::TableEntry;
use crate::{
    CoreError,
    relational::{
        ColumnDef, DataType, MAX_COLUMNS, TableDef,
        catalog_ext::{
            Action, ColumnExt, Constraint, ConstraintKind, Deferral, Identity, ReferencedKey, SequenceDef,
            TextKind, TypeDef, validate_typmod,
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
    // Appended tags 15..=21 require meta feature bit 10.
    Varchar,
    Bpchar,
    Uuid,
    Jsonb,
    IntArray,
    BigIntArray,
    TextArray,
    // Appended tag 22 requires meta feature bit 12.
    UserType(u64),
}
impl TryFrom<DataType> for DataTypeDto {
    type Error = CoreError;
    fn try_from(v: DataType) -> Result<Self, CoreError> {
        Ok(match v {
            DataType::RegType => return Err(CoreError::Sql(crate::relational::SqlError::Unsupported)),
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
            DataType::Varchar => Self::Varchar,
            DataType::Bpchar => Self::Bpchar,
            DataType::Uuid => Self::Uuid,
            DataType::Jsonb => Self::Jsonb,
            DataType::IntArray => Self::IntArray,
            DataType::BigIntArray => Self::BigIntArray,
            DataType::TextArray => Self::TextArray,
        })
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
            DataTypeDto::Varchar => Self::Varchar,
            DataTypeDto::Bpchar => Self::Bpchar,
            DataTypeDto::Uuid => Self::Uuid,
            DataTypeDto::Jsonb => Self::Jsonb,
            DataTypeDto::IntArray => Self::IntArray,
            DataTypeDto::BigIntArray => Self::BigIntArray,
            DataTypeDto::TextArray => Self::TextArray,
            // Resolved against the type catalog before a view is published.
            DataTypeDto::UserType(_) => Self::Text,
        }
    }
}
impl TryFrom<ColumnDef> for ColumnDefDto {
    type Error = CoreError;
    fn try_from(v: ColumnDef) -> Result<Self, CoreError> {
        let ColumnDef {
            id,
            name,
            data_type,
            not_null,
        } = v;
        Ok(Self {
            id,
            name,
            data_type: data_type.try_into()?,
            not_null,
        })
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
            columns: columns
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
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
impl TryFrom<ColumnExt> for ColumnExtDto {
    type Error = CoreError;
    fn try_from(v: ColumnExt) -> Result<Self, CoreError> {
        let ColumnExt {
            typmod,
            default,
            identity,
            text_kind: _, // Reconstructed from the column tag, never a new wire field.
            type_id: _,   // Stored in the enclosing column data-type tag.
        } = v;
        Ok(Self {
            typmod,
            default,
            identity: identity.map(Into::into),
        })
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
            // The enclosing table conversion restores this from its type tag.
            text_kind: None,
            type_id: None,
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
    Unique { columns: Vec<u64>, index: u64 },
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
                ..
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
            ConstraintKind::Unique { columns, index } => Self::Unique { columns, index },
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
                target: ReferencedKey::Unknown,
            },
            ConstraintKindDto::PrimaryKey => Self::PrimaryKey,
            ConstraintKindDto::Unique { columns, index } => Self::Unique { columns, index },
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
impl TryFrom<SequenceDef> for SequenceDefDto {
    type Error = CoreError;
    fn try_from(v: SequenceDef) -> Result<Self, CoreError> {
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
        Ok(Self {
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
            data_type: data_type.try_into()?,
        })
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
        let mut def: TableDefDto = def.try_into()?;
        for column in &mut def.columns {
            if let Some(id) = ext.get(&column.id).and_then(|ext| ext.type_id) {
                column.data_type = DataTypeDto::UserType(id);
            }
        }
        Ok(Self {
            def,
            heap,
            columns_ext: ext
                .into_iter()
                .map(|(k, v)| Ok((k, v.try_into()?)))
                .collect::<Result<_, CoreError>>()?,
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
        let type_ids: Vec<_> = def
            .columns
            .iter()
            .filter_map(|column| {
                if let DataTypeDto::UserType(id) = column.data_type {
                    Some((column.id, id))
                } else {
                    None
                }
            })
            .collect();
        let def: TableDef = def.try_into()?;
        let mut columns_ext: BTreeMap<u64, ColumnExt> = columns_ext
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();
        for (column, id) in type_ids {
            if id == 0 {
                return Err(corrupt("zero user type id"));
            }
            columns_ext.entry(column).or_default().type_id = Some(id);
        }
        for column in &def.columns {
            if let Some(kind) = text_kind(&column.data_type) {
                columns_ext.entry(column.id).or_default().text_kind = Some(kind);
            }
        }
        Ok((
            TableEntry {
                primary_index_birth: None,
                def,
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
    for (id, column_ext) in ext {
        let column = entry
            .def
            .columns
            .iter()
            .find(|column| column.id == *id)
            .ok_or_else(|| corrupt("column extension references absent column"))?;
        if column_ext.text_kind != text_kind(&column.data_type) {
            return Err(corrupt("column text kind/type mismatch"));
        }
    }
    let dto = TableEntryV2Dto::try_from((entry.clone(), ext.clone()))?;
    if let Some(birth) = entry.primary_index_birth {
        validate_primary_birth(entry, birth)?;
        encode_version(3, &(dto, birth))
    } else { encode_version(2, &dto) }
}
pub(crate) fn decode_table(
    value: &[u8],
) -> Result<(TableEntry, BTreeMap<u64, ColumnExt>), CoreError> {
    let (dto, birth): (TableEntryV2Dto, Option<u64>) = match value.first() {
        Some(2) => (decode_version(2, value)?, None),
        Some(3) => { let (dto, birth) = decode_version(3, value)?; (dto, Some(birth)) }
        _ => return Err(corrupt("unsupported table version")),
    };
    if dto.columns_ext.len() > crate::relational::catalog_ext::MAX_EXT_COLUMNS
        || dto
            .columns_ext
            .values()
            .any(|v| v.default.as_ref().is_some_and(|s| s.len() > 65536))
    {
        return Err(corrupt("catalog column extension limit"));
    }
    let (mut entry, ext): (TableEntry, BTreeMap<u64, ColumnExt>) = dto.try_into()?;
    if let Some(birth) = birth { validate_primary_birth(&entry, birth)?; }
    entry.primary_index_birth = birth;
    validate_definition(&entry.def)?;
    for (id, column_ext) in &ext {
        let column = entry
            .def
            .columns
            .iter()
            .find(|column| column.id == *id)
            .ok_or_else(|| corrupt("column extension references absent column"))?;
        if let Some(typmod) = column_ext.typmod.filter(|_| column_ext.type_id.is_none()) {
            validate_typmod(&column.data_type, typmod)
                .map_err(|_| corrupt("invalid catalog column typmod"))?;
        }
    }
    Ok((entry, ext))
}
fn validate_primary_birth(entry: &TableEntry, birth: u64) -> Result<(), CoreError> {
    if birth == 0 || entry.heap || entry.def.hidden_rowid().is_some() || entry.def.primary_key.is_empty() {
        return Err(corrupt("noncanonical primary index birth"));
    }
    Ok(())
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
    validate_relation_name(&def.name)?;
    validate_create_definition(crate::relational::split_key(&def.name).1, &columns, &keys)
        .map_err(|_| corrupt("invalid catalog table definition"))
}
pub(crate) fn encode_constraint(v: &Constraint, _table: &str) -> Result<Vec<u8>, CoreError> {
    let dto = ConstraintDto::from(v.clone());
    if let ConstraintKind::ForeignKey { target, .. } = v.kind
        && target != ReferencedKey::Unknown {
        validate_known_target(target)?;
        return encode_version(3, &(dto, target));
    }
    encode_version(if matches!(v.kind, ConstraintKind::Unique { .. }) { 2 } else { 1 }, &dto)
}
/// Decode the v3 target for the feature witness before its owning table is loaded.
pub(crate) fn constraint_key_dependency_target(v: &[u8]) -> Result<ReferencedKey, CoreError> {
    let (dto, target): (ConstraintDto, ReferencedKey) = decode_version(3, v)?;
    validate_known_target(target)?;
    if !matches!(dto.kind, ConstraintKindDto::ForeignKey { .. }) {
        return Err(corrupt("noncanonical constraint v3 kind"));
    }
    Ok(target)
}
pub(crate) fn decode_constraint(v: &[u8], table: &TableDef) -> Result<Constraint, CoreError> {
    let version = *v.first().ok_or_else(|| corrupt("missing constraint version"))?;
    if !matches!(version, 1..=3) { return Err(corrupt("unsupported constraint version")); }
    let (dto, target): (ConstraintDto, ReferencedKey) = if version == 3 {
        let (dto, target) = decode_version(3, v)?;
        validate_known_target(target)?;
        (dto, target)
    } else { (decode_version(version, v)?, ReferencedKey::Unknown) };
    if version == 3 && !matches!(dto.kind, ConstraintKindDto::ForeignKey { .. }) {
        return Err(corrupt("noncanonical constraint v3 kind"));
    }
    if (version == 2) != matches!(dto.kind, ConstraintKindDto::Unique { .. }) {
        return Err(corrupt("noncanonical constraint version"));
    }
    if let ConstraintKindDto::Unique { columns, index } = &dto.kind
        && (*index == 0 || columns.is_empty() || columns.len() > MAX_COLUMNS
            || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
            || columns.iter().any(|id| !table.columns.iter().any(|c| c.id == *id))) {
        return Err(corrupt("invalid unique constraint"));
    }
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
    let mut constraint = Constraint::try_from(dto)?;
    if let ConstraintKind::ForeignKey { target: saved, .. } = &mut constraint.kind { *saved = target; }
    Ok(constraint)
}
fn validate_known_target(target: ReferencedKey) -> Result<(), CoreError> {
    if matches!(target, ReferencedKey::Unknown | ReferencedKey::Secondary(0)) {
        return Err(corrupt("noncanonical known foreign key target"));
    }
    Ok(())
}
pub(crate) fn encode_sequence(v: &SequenceDef) -> Result<Vec<u8>, CoreError> {
    encode_version(1, &SequenceDefDto::try_from(v.clone())?)
}
pub(crate) fn decode_sequence(v: &[u8]) -> Result<SequenceDef, CoreError> {
    let dto: SequenceDefDto = decode_version(1, v)?;
    if validate_relation_name(&dto.name).is_err() {
        return Err(corrupt("sequence name limit"));
    }
    Ok(dto.into())
}
// Standalone legacy v6 TypeDef, prefixed with the independent record version.
pub(crate) fn encode_type(v: TypeDef) -> Result<Vec<u8>, CoreError> {
    encode_version(1, &v)
}
pub(crate) fn decode_type(v: &[u8]) -> Result<TypeDef, CoreError> {
    decode_version(1, v)
}
pub(crate) fn schema_key(name: &str) -> Vec<u8> {
    let mut key = vec![14];
    key.extend(name.as_bytes());
    key.push(0);
    key
}
pub(crate) fn encode_schema(v: &crate::relational::schemas::Schema) -> Result<Vec<u8>, CoreError> {
    encode_version(1, v)
}
pub(crate) fn decode_schema(v: &[u8]) -> Result<crate::relational::schemas::Schema, CoreError> {
    let schema: crate::relational::schemas::Schema = decode_version(1, v)?;
    if schema.id == 0 {
        return Err(corrupt("zero schema id"));
    }
    Ok(schema)
}
fn validate_relation_name(name: &str) -> Result<(), CoreError> {
    let (schema, bare) = crate::relational::split_key(name);
    if schema.is_empty()
        || schema.len() > 63
        || bare.is_empty()
        || bare.len() > 63
        || bare.contains('\0')
        || (name.contains('\0') && schema == "public")
    {
        return Err(corrupt("invalid catalog relation name"));
    }
    Ok(())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogKind {
    Table,
    Name,
    Constraint,
    Sequence,
    Type,
    RoleBootstrap,
    Role,
    RoleMembership,
    TableOwner,
    TableGrant,
    SchemaGrant,
    RoleOid,
    Schema,
    TypeDefinition,
    Index,
    SqlView,
    RelationComment,
    Task,
    /// Kind 21: untrusted-text column label (GD-0061 R3-307).
    ColumnLabel,
}
pub(crate) fn classify_catalog_key(key: &[u8]) -> Result<CatalogKind, CoreError> {
    Ok(match key.first() {
        Some(1) if key.len() == 9 => CatalogKind::Table,
        Some(3) if key.len() >= 3 && key.last() == Some(&0) => {
            let name = std::str::from_utf8(&key[1..key.len() - 1])
                .map_err(|_| corrupt("invalid catalog name UTF-8"))?;
            validate_relation_name(name)?;
            CatalogKind::Name
        }
        Some(14)
            if key.len() >= 3
                && key.len() <= 65
                && key.last() == Some(&0)
                && !key[1..key.len() - 1].contains(&0)
                && std::str::from_utf8(&key[1..key.len() - 1]).is_ok() =>
        {
            CatalogKind::Schema
        }
        Some(15) if key.len() == 9 => CatalogKind::TypeDefinition,
        Some(16) if key.len() == 9 => CatalogKind::Index,
        Some(17) if key.len() == 9 && key[1..9] != [0; 8] => CatalogKind::SqlView,
        Some(18) if key.len() == 17 && key[1..9] != [0; 8] => CatalogKind::RelationComment,
        Some(19) if key.len() == 9 && key[1..9] != [0; 8] => CatalogKind::Task,
        Some(kind) if *kind == super::catalog_label_codec::LABEL_KIND && key.len() == 17 && key[1..9] != [0; 8] => {
            CatalogKind::ColumnLabel
        }
        Some(4) if key.len() == 17 => CatalogKind::Constraint,
        Some(5) if key.len() == 9 => CatalogKind::Sequence,
        Some(6) if key.len() == 9 => CatalogKind::Type,
        // Existing role bootstrap fixture: engine/meta_tests.rs role().
        Some(7) if key.len() == 9 => CatalogKind::RoleBootstrap,
        Some(8..=13) => super::super::roles::engine::classify_key(key)?,
        _ => return Err(corrupt("invalid catalog user key")),
    })
}
fn object_key(kind: u8, id: u64) -> Vec<u8> {
    let mut key = vec![kind];
    key.extend(id.to_be_bytes());
    key
}
pub(crate) fn comment_key(relation: u64, column: u64) -> Vec<u8> {
    let mut key = object_key(18, relation);
    key.extend(column.to_be_bytes());
    key
}
/// Version 1: version byte, u32 BE UTF-8 byte length, then text (at most 64 KiB).
pub(crate) fn encode_comment(text: &str) -> Result<Vec<u8>, CoreError> {
    if text.len() > 65536 {
        return Err(corrupt("relation comment length limit"));
    }
    let mut value = vec![1];
    value.extend((text.len() as u32).to_be_bytes());
    value.extend(text.as_bytes());
    Ok(value)
}
pub(crate) fn decode_comment(value: &[u8]) -> Result<String, CoreError> {
    if value.first() != Some(&1) || value.len() < 5 {
        return Err(corrupt("unsupported or truncated relation comment"));
    }
    let len = u32::from_be_bytes(value[1..5].try_into().unwrap()) as usize;
    if len > 65536 || value.len() - 5 != len {
        return Err(corrupt("invalid relation comment length"));
    }
    String::from_utf8(value[5..].to_vec()).map_err(|_| corrupt("invalid comment UTF-8"))
}
pub(crate) fn validate_comments<'a>(
    comments: &BTreeMap<(u64, u64), String>,
    tables: impl Iterator<Item = &'a TableDef>,
    views: &BTreeMap<u64, sql_views::SqlViewDef>,
) -> Result<(), CoreError> {
    if comments.is_empty() {
        return Ok(());
    }
    let tables: BTreeMap<_, _> = tables.map(|table| (table.id, table)).collect();
    for &(relation, column) in comments.keys() {
        let valid = tables.get(&relation).is_some_and(|t| column == 0 || t.columns.iter().any(|c| c.id == column))
            || views.get(&relation).is_some_and(|v| column == 0 || v.columns.iter().any(|c| c.id == column));
        if relation == 0 || !valid {
            return Err(corrupt("comment column absent"));
        }
    }
    Ok(())
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
    object_key(15, id)
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

/// Text semantics are redundant with the appended v2 column tag.
fn text_kind(data_type: &DataType) -> Option<TextKind> {
    match data_type {
        DataType::Varchar => Some(TextKind::Varchar),
        DataType::Bpchar => Some(TextKind::Bpchar),
        _ => None,
    }
}

pub(crate) fn requires_text_backed_types(def: &TableDef) -> bool {
    def.columns.iter().any(|column| {
        matches!(
            column.data_type,
            DataType::Varchar
                | DataType::Bpchar
                | DataType::Uuid
                | DataType::Jsonb
                | DataType::IntArray
                | DataType::BigIntArray
                | DataType::TextArray
        )
    })
}

#[cfg(test)]
#[path = "catalog_codec_tests.rs"]
mod tests;
