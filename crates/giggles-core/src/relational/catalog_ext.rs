//! Bounded, versioned catalog metadata for forthcoming DDL.
use super::*;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use std::fmt;
pub(crate) const UNSUPPORTED_CATALOG_EXT: &str = "unsupported relational catalog extension";
const MAX_EXPR: usize = 65536;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadata {
    pub(crate) gin: BTreeMap<u64, gin_indexes::GinIndex>,
    pub(crate) generated: BTreeMap<u64, generated_columns::GeneratedColumn>,
}
impl TableMetadata {
    pub(crate) fn is_empty(&self) -> bool {
        self.gin.is_empty() && self.generated.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogExt {
    pub(crate) ext_version: u32,
    pub(crate) columns: BTreeMap<u64, ColumnExt>,
    pub(crate) constraints: BTreeMap<u64, Vec<Constraint>>,
    pub(crate) sequences: BTreeMap<u64, SequenceDef>,
    pub(crate) types: BTreeMap<u64, TypeDef>,
    pub(crate) added_enum_labels: BTreeMap<u64, AddedEnumLabels>,
    pub(crate) logical_kinds: BTreeMap<u64, LogicalKind>,
    pub(crate) schemas: BTreeMap<String, schemas::Schema>,
    pub(crate) table_schemas: BTreeMap<u64, String>,
    pub(crate) primary_index_birth: BTreeMap<u64, u64>,
    pub(crate) collations: BTreeMap<u64, super::collations::Collation>,
    pub(crate) extensions: BTreeMap<String, super::extensions::InstalledExtension>,
    pub(crate) column_collations: BTreeMap<u64, super::column_collations::Binding>,
    // Serialized in the feature-gated table metadata trailer.
    pub(crate) gin_indexes: BTreeMap<u64, gin_indexes::GinIndex>,
    pub(crate) generated_columns: BTreeMap<u64, generated_columns::GeneratedColumn>,
}
/// Initial labels use their original creation slots. Only ADD VALUE needs
/// an explicit identity; rename keeps the same ID under the new spelling.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AddedEnumLabels(#[serde(deserialize_with = "added_labels")] pub(crate) BTreeMap<String, u64>);

fn added_labels<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<String, u64>, D::Error> {
    #[derive(Deserialize)]
    struct Label(#[serde(deserialize_with = "name")] String);

    struct Labels;
    impl<'de> Visitor<'de> for Labels {
        type Value = BTreeMap<String, u64>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded enum label identities")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut labels = BTreeMap::new();
            while let Some((Label(label), id)) = map.next_entry::<Label, u64>()? {
                if labels.len() == MAX_COLUMNS || labels.insert(label, id).is_some() {
                    return Err(de::Error::custom("invalid enum label identities"));
                }
            }
            Ok(labels)
        }
    }
    d.deserialize_map(Labels)
}

impl Default for CatalogExt {
    fn default() -> Self {
        Self {
            ext_version: 1,
            columns: BTreeMap::new(),
            constraints: BTreeMap::new(),
            sequences: BTreeMap::new(),
            types: BTreeMap::new(),
            added_enum_labels: BTreeMap::new(),
            logical_kinds: BTreeMap::new(),
            schemas: BTreeMap::new(),
            table_schemas: BTreeMap::new(),
            primary_index_birth: BTreeMap::new(),
            collations: BTreeMap::new(),
            extensions: BTreeMap::new(),
            column_collations: BTreeMap::new(),
            gin_indexes: BTreeMap::new(),
            generated_columns: BTreeMap::new(),
        }
    }
}
impl CatalogExt {
    pub(crate) fn is_empty(&self) -> bool {
        self.namespace5_ext_is_empty() && self.generated_columns.is_empty() && self.gin_indexes.is_empty()
    }

    // Generated columns and GIN indexes have their own format-53 envelope maps
    // and V2 table trailers. They never contribute to the base extension.
    pub(crate) fn namespace5_ext_is_empty(&self) -> bool {
        self.extensions.is_empty() && self.column_collations.is_empty() && self.collations.is_empty()
            && self.columns.is_empty()
            && self.constraints.is_empty()
            && self.sequences.is_empty()
            && self.types.is_empty()
            && self.added_enum_labels.is_empty()
            && self.logical_kinds.is_empty()
            && self.schemas.is_empty()
            && self.table_schemas.is_empty()
            && self.primary_index_birth.is_empty()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct ColumnExt {
    pub(crate) typmod: Option<i32>,
    #[serde(deserialize_with = "optional_expr")]
    pub(crate) default: Option<String>,
    pub(crate) identity: Option<Identity>,
    pub(crate) text_kind: Option<TextKind>,
    pub(crate) type_id: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TextKind {
    Varchar,
    Bpchar,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LogicalKind {
    Uuid,
    Jsonb,
    IntArray,
    BigIntArray,
    TextArray,
    UuidArray,
    // Appended logical kinds require Legacy format-53 feature bit 37.
    TsVector,
    TsQuery,
}
// Frozen version-three column tuple; version six appends type_id.
#[derive(Serialize, Deserialize)]
struct ColumnV3 {
    typmod: Option<i32>,
    #[serde(deserialize_with = "optional_expr")]
    default: Option<String>,
    identity: Option<Identity>,
    text_kind: Option<TextKind>,
}
impl From<ColumnV3> for ColumnExt {
    fn from(c: ColumnV3) -> Self {
        Self {
            typmod: c.typmod,
            default: c.default,
            identity: c.identity,
            text_kind: c.text_kind,
            type_id: None,
        }
    }
}
// Frozen version-one/two column layout.
#[derive(Serialize, Deserialize)]
struct LegacyColumnExt {
    typmod: Option<i32>,
    #[serde(deserialize_with = "optional_expr")]
    default: Option<String>,
    identity: Option<Identity>,
}
impl From<LegacyColumnExt> for ColumnExt {
    fn from(c: LegacyColumnExt) -> Self {
        Self {
            typmod: c.typmod,
            default: c.default,
            identity: c.identity,
            text_kind: None,
            type_id: None,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub(crate) always: bool,
    pub(crate) sequence_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Constraint {
    pub(crate) id: u64,
    #[serde(deserialize_with = "name")]
    pub(crate) name: String,
    pub(crate) kind: ConstraintKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ConstraintKind {
    Check {
        #[serde(deserialize_with = "expression")]
        expr: String,
        not_valid: bool,
    },
    ForeignKey {
        #[serde(deserialize_with = "ids")]
        columns: Vec<u64>,
        ref_table: u64,
        #[serde(deserialize_with = "ids")]
        ref_columns: Vec<u64>,
        on_delete: Action,
        on_update: Action,
        not_valid: bool,
        deferral: Deferral,
        target: ReferencedKey,
    },
    PrimaryKey,
    Unique {
        #[serde(deserialize_with = "ids")]
        columns: Vec<u64>,
        index: u64,
    },
}
// Frozen v2-7 tuples: live FK additions must never change these bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConstraintV2To7 {
    pub(crate) id: u64,
    #[serde(deserialize_with = "name")]
    pub(crate) name: String,
    pub(crate) kind: ConstraintKindV2To7,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ConstraintKindV2To7 {
    Check {
        #[serde(deserialize_with = "expression")]
        expr: String,
        not_valid: bool,
    },
    ForeignKey {
        #[serde(deserialize_with = "ids")]
        columns: Vec<u64>,
        ref_table: u64,
        #[serde(deserialize_with = "ids")]
        ref_columns: Vec<u64>,
        on_delete: Action,
        on_update: Action,
        not_valid: bool,
        deferral: Deferral,
    },
    PrimaryKey,
    Unique {
        #[serde(deserialize_with = "ids")]
        columns: Vec<u64>,
        index: u64,
    },
}
impl From<ConstraintV2To7> for Constraint {
    fn from(c: ConstraintV2To7) -> Self {
        let kind = match c.kind {
            ConstraintKindV2To7::Check { expr, not_valid } => ConstraintKind::Check { expr, not_valid },
            ConstraintKindV2To7::ForeignKey { columns, ref_table, ref_columns, on_delete, on_update, not_valid, deferral } => ConstraintKind::ForeignKey {
                columns, ref_table, ref_columns, on_delete, on_update, not_valid, deferral, target: ReferencedKey::Unknown,
            },
            ConstraintKindV2To7::PrimaryKey => ConstraintKind::PrimaryKey,
            ConstraintKindV2To7::Unique { columns, index } => ConstraintKind::Unique { columns, index },
        };
        Self { id: c.id, name: c.name, kind }
    }
}
impl From<&Constraint> for ConstraintV2To7 {
    fn from(c: &Constraint) -> Self {
        let kind = match &c.kind {
            ConstraintKind::Check { expr, not_valid } => ConstraintKindV2To7::Check { expr: expr.clone(), not_valid: *not_valid },
            ConstraintKind::ForeignKey { columns, ref_table, ref_columns, on_delete, on_update, not_valid, deferral, .. } => ConstraintKindV2To7::ForeignKey {
                columns: columns.clone(), ref_table: *ref_table, ref_columns: ref_columns.clone(), on_delete: on_delete.clone(), on_update: on_update.clone(), not_valid: *not_valid, deferral: *deferral,
            },
            ConstraintKind::PrimaryKey => ConstraintKindV2To7::PrimaryKey,
            ConstraintKind::Unique { columns, index } => ConstraintKindV2To7::Unique { columns: columns.clone(), index: *index },
        };
        Self { id: c.id, name: c.name.clone(), kind }
    }
}
// Wire tags are frozen: Unknown=0, PrimaryKey=1, Secondary=2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) enum ReferencedKey {
    #[default]
    Unknown,
    PrimaryKey,
    Secondary(u64),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) enum Deferral {
    #[default]
    NotDeferrable,
    DeferrableImmediate,
    DeferrableDeferred,
}

// The version-one DTO is frozen: adding a field to its ForeignKey variant
// changes the bincode tuple layout, even with a serde default.
#[derive(Serialize, Deserialize)]
struct ConstraintV1 {
    id: u64,
    #[serde(deserialize_with = "name")]
    name: String,
    kind: ConstraintKindV1,
}
#[derive(Serialize, Deserialize)]
enum ConstraintKindV1 {
    Check {
        #[serde(deserialize_with = "expression")]
        expr: String,
        not_valid: bool,
    },
    ForeignKey {
        #[serde(deserialize_with = "ids")]
        columns: Vec<u64>,
        ref_table: u64,
        #[serde(deserialize_with = "ids")]
        ref_columns: Vec<u64>,
        on_delete: Action,
        on_update: Action,
        not_valid: bool,
    },
    PrimaryKey,
}
impl From<ConstraintV1> for Constraint {
    fn from(c: ConstraintV1) -> Self {
        let kind = match c.kind {
            ConstraintKindV1::Check { expr, not_valid } => {
                ConstraintKind::Check { expr, not_valid }
            }
            ConstraintKindV1::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                not_valid,
            } => ConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                not_valid,
                deferral: Deferral::NotDeferrable,
                target: ReferencedKey::Unknown,
            },
            ConstraintKindV1::PrimaryKey => ConstraintKind::PrimaryKey,
        };
        Self {
            id: c.id,
            name: c.name,
            kind,
        }
    }
}
impl TryFrom<&Constraint> for ConstraintV1 {
    type Error = &'static str;
    fn try_from(c: &Constraint) -> Result<Self, Self::Error> {
        let kind = match &c.kind {
            ConstraintKind::Check { expr, not_valid } => ConstraintKindV1::Check {
                expr: expr.clone(),
                not_valid: *not_valid,
            },
            ConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                not_valid,
                ..
            } => ConstraintKindV1::ForeignKey {
                columns: columns.clone(),
                ref_table: *ref_table,
                ref_columns: ref_columns.clone(),
                on_delete: on_delete.clone(),
                on_update: on_update.clone(),
                not_valid: *not_valid,
            },
            ConstraintKind::PrimaryKey => ConstraintKindV1::PrimaryKey,
            ConstraintKind::Unique { .. } => return Err("UNIQUE requires catalog extension v7"),
        };
        Ok(Self {
            id: c.id,
            name: c.name.clone(),
            kind,
        })
    }
}
pub(crate) fn validate_unique_indexes<'a>(
    constraints: impl Iterator<Item = (u64, &'a Constraint)>,
    indexes: impl Iterator<Item = &'a super::secondary_index::Descriptor>,
) -> R<()> {
    let indexes: BTreeMap<_, _> = indexes.map(|d| (d.id, d)).collect();
    for (table, constraint) in constraints {
        if let ConstraintKind::Unique { columns, index } = &constraint.kind {
            let d = indexes.get(index).ok_or(SqlError::Type)?;
            if d.table != table || !d.unique || d.predicate.is_some() || d.columns != *columns {
                return Err(SqlError::Type);
            }
        }
    }
    Ok(())
}
fn validate_births(ext: &CatalogExt, tables: &BTreeMap<u64, &TableDef>, next_id: u64, used: &mut BTreeSet<u64>) -> R<()> {
    for (table, birth) in &ext.primary_index_birth {
        let table = tables.get(table).ok_or(SqlError::Type)?;
        if table.hidden_rowid().is_some() || table.primary_key.is_empty()
            || *birth == 0 || *birth >= next_id || !used.insert(*birth) {
            return Err(SqlError::Type);
        }
    }
    Ok(())
}
/// Cross-index checks run only after the authoritative descriptors are loaded.
pub(crate) fn validate_key_targets<'a>(ext: &CatalogExt, tables: impl Iterator<Item = &'a TableDef>,
    indexes: impl Iterator<Item = &'a super::secondary_index::Descriptor>, next_id: u64) -> R<()> {
    if ext.primary_index_birth.is_empty() && !ext.constraints.values().flatten().any(|c|
        matches!(c.kind, ConstraintKind::ForeignKey { target, .. } if target != ReferencedKey::Unknown)) { return Ok(()); }
    let tables: BTreeMap<_, _> = tables.map(|t| (t.id, t)).collect();
    let indexes: BTreeMap<_, _> = indexes.map(|d| (d.id, d)).collect();
    let mut used: BTreeSet<_> = tables.values().flat_map(|t| std::iter::once(t.id).chain(t.columns.iter().map(|c| c.id)))
        .chain(indexes.keys().copied()).chain(ext.constraints.values().flatten().map(|c| c.id))
        .chain(ext.sequences.keys().copied()).chain(ext.schemas.values().map(|s| s.id)).collect();
    used.extend(ext.types.keys().copied());
    for id in ext.enum_label_ids()?.into_iter().map(|(_, id)| id) {
        if !used.insert(id) || id >= next_id {
            return Err(SqlError::Type);
        }
    }
    validate_births(ext, &tables, next_id, &mut used)?;
    for c in ext.constraints.values().flatten() {
        let ConstraintKind::ForeignKey { ref_table, ref_columns, target, .. } = &c.kind else { continue };
        if *target != ReferencedKey::Unknown {
            let remote = tables.get(ref_table).ok_or(SqlError::Type)?;
            if ref_columns.iter().any(|id| !remote.columns.iter().any(|c| c.id == *id)) { return Err(SqlError::Type); }
        }
        let key = match target {
            ReferencedKey::Unknown => continue,
            ReferencedKey::PrimaryKey => {
                let t = tables.get(ref_table).ok_or(SqlError::Type)?;
                if t.hidden_rowid().is_some() { return Err(SqlError::Type); }
                t.primary_key.iter().map(|i| t.columns.get(*i).map(|c| c.id).ok_or(SqlError::Type)).collect::<R<Vec<_>>>()?
            }
            ReferencedKey::Secondary(id) => {
                let d = indexes.get(id).ok_or(SqlError::Type)?;
                if *id == 0 || *id >= next_id || d.table != *ref_table || !d.unique || d.predicate.is_some() { return Err(SqlError::Type); }
                d.columns.clone()
            }
        };
        let refs: BTreeSet<_> = ref_columns.iter().copied().collect();
        if key.is_empty() || key.len() != ref_columns.len() || refs.len() != ref_columns.len()
            || key.into_iter().collect::<BTreeSet<_>>() != refs { return Err(SqlError::Type); }
    }
    Ok(())
}
impl CatalogExt {
    /// Return each label's object ID in enum order. An ADD VALUE never
    /// occupies or shifts any of the slots originally allocated by CREATE TYPE.
    pub(crate) fn enum_ids(&self, id: u64, labels: &[String]) -> R<Vec<u64>> {
        let added = self.added_enum_labels.get(&id);
        if let Some(added) = added
            && (added.0.is_empty() || added.0.keys().any(|label| !labels.contains(label)))
        {
            return Err(SqlError::Type);
        }
        let initial_count = labels.len() - added.map_or(0, |added| added.0.len());
        let last_initial = id.checked_add(initial_count as u64).ok_or(SqlError::Limit)?;
        let mut initial = id;
        labels.iter().map(|label| {
            let oid = if let Some(added_id) = added.and_then(|added| added.0.get(label)).copied() {
                if added_id <= last_initial {
                    return Err(SqlError::Type);
                }
                added_id
            } else {
                initial += 1;
                initial
            };
            super::user_types::type_oid(oid)?;
            Ok(oid)
        }).collect()
    }

    pub(crate) fn enum_label_ids(&self) -> R<Vec<(u64, u64)>> {
        if self.added_enum_labels.keys().any(|id| !matches!(self.types.get(id), Some(TypeDef::Enum { .. }))) {
            return Err(SqlError::Type);
        }
        let mut ids = Vec::new();
        for (&id, ty) in &self.types {
            if let TypeDef::Enum { labels, .. } = ty {
                ids.extend(self.enum_ids(id, labels)?.into_iter().map(|label| (id, label)));
            }
        }
        Ok(ids)
    }

    fn has_text_kind(&self) -> bool {
        self.columns.values().any(|c| c.text_kind.is_some())
    }
    pub(crate) fn has_key_dependency_identity(&self) -> bool {
        !self.primary_index_birth.is_empty() || self.constraints.values().flatten().any(|c| matches!(c.kind, ConstraintKind::ForeignKey { target, .. } if target != ReferencedKey::Unknown))
    }
    pub(crate) fn has_uuid_arrays(&self) -> bool {
        self.logical_kinds.values().any(|kind| *kind == LogicalKind::UuidArray)
            || self.types.values().any(TypeDef::has_uuid_arrays)
    }
    pub(crate) fn has_text_search_types(&self) -> bool {
        self.logical_kinds.values().any(|kind| matches!(kind, LogicalKind::TsVector | LogicalKind::TsQuery))
    }
    pub(crate) fn wire_version(&self) -> u32 {
        if !self.added_enum_labels.is_empty() {
            12
        } else if !self.extensions.is_empty() {
            11
        } else if !self.column_collations.is_empty() {
            10
        } else if !self.collations.is_empty() {
            9
        } else if self.has_key_dependency_identity() {
            8
        } else if self.constraints.values().flatten().any(|c| matches!(c.kind, ConstraintKind::Unique { .. })) {
            7
        } else if !self.types.is_empty() || self.columns.values().any(|c| c.type_id.is_some()) {
            6
        } else if !self.schemas.is_empty() || !self.table_schemas.is_empty() {
            5
        } else if !self.logical_kinds.is_empty() {
            4
        } else if self.has_text_kind() {
            3
        } else if self.has_deferrable() {
            2
        } else {
            1
        }
    }
    fn has_deferrable(&self) -> bool {
        self.constraints.values().flatten().any(|c| matches!(c.kind, ConstraintKind::ForeignKey { deferral, .. } if deferral != Deferral::NotDeferrable))
    }
}
impl Serialize for CatalogExt {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let version = self.wire_version();
        let mut out = serializer.serialize_tuple(match version {
            12 => 13,
            11 => 12,
            10 => 11,
            9 => 10,
            8 => 9,
            5..=7 => 8,
            4 => 6,
            _ => 5,
        })?;
        out.serialize_element(&version)?;
        if version >= 6 {
            out.serialize_element(&self.columns)?;
        } else if matches!(version, 3..=5) {
            let columns: BTreeMap<_, _> = self
                .columns
                .iter()
                .map(|(id, c)| {
                    (
                        *id,
                        ColumnV3 {
                            typmod: c.typmod,
                            default: c.default.clone(),
                            identity: c.identity.clone(),
                            text_kind: c.text_kind,
                        },
                    )
                })
                .collect();
            out.serialize_element(&columns)?;
        } else {
            let legacy: BTreeMap<_, _> = self
                .columns
                .iter()
                .map(|(id, c)| {
                    (
                        *id,
                        LegacyColumnExt {
                            typmod: c.typmod,
                            default: c.default.clone(),
                            identity: c.identity.clone(),
                        },
                    )
                })
                .collect();
            out.serialize_element(&legacy)?;
        }
        if version == 1 {
            let legacy: BTreeMap<_, Vec<_>> = self
                .constraints
                .iter()
                .map(|(id, cs)| Ok((*id, cs.iter().map(ConstraintV1::try_from).collect::<Result<Vec<_>, _>>()?)))
                .collect::<Result<_, &'static str>>().map_err(serde::ser::Error::custom)?;
            out.serialize_element(&legacy)?;
        } else if version <= 7 {
            let frozen: BTreeMap<_, Vec<_>> = self.constraints.iter()
                .map(|(id, cs)| (*id, cs.iter().map(ConstraintV2To7::from).collect())).collect();
            out.serialize_element(&frozen)?;
        } else {
            out.serialize_element(&self.constraints)?;
        }
        out.serialize_element(&self.sequences)?;
        out.serialize_element(&self.types)?;
        if version >= 4 {
            out.serialize_element(&self.logical_kinds)?;
        }
        if version >= 5 {
            out.serialize_element(&self.schemas)?;
            out.serialize_element(&self.table_schemas)?;
        }
        if version >= 8 { out.serialize_element(&self.primary_index_birth)?; }
        if version >= 9 { out.serialize_element(&self.collations)?; }
        if version >= 10 { out.serialize_element(&self.column_collations)?; }
        if version >= 11 { out.serialize_element(&self.extensions)?; }
        if version >= 12 { out.serialize_element(&self.added_enum_labels)?; }
        out.end()
    }
}
impl<'de> Deserialize<'de> for CatalogExt {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Columns(#[serde(deserialize_with = "column_map")] BTreeMap<u64, ColumnExt>);
        #[derive(Deserialize)]
        struct LegacyColumns(
            #[serde(deserialize_with = "legacy_column_map")] BTreeMap<u64, LegacyColumnExt>,
        );
        fn legacy_column_map<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<BTreeMap<u64, LegacyColumnExt>, D::Error> {
            catalog_map::<D, LegacyColumnExt>(d)
        }
        #[derive(Deserialize)]
        struct Sequences(#[serde(deserialize_with = "sequence_map")] BTreeMap<u64, SequenceDef>);
        #[derive(Deserialize)]
        struct Types(#[serde(deserialize_with = "type_map")] BTreeMap<u64, TypeDef>);
        #[derive(Deserialize)]
        struct LegacyConstraints(
            #[serde(deserialize_with = "legacy_constraints")] Vec<ConstraintV1>,
        );
        fn legacy_constraints<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<Vec<ConstraintV1>, D::Error> {
            catalog_vec::<D, ConstraintV1>(d)
        }
        #[derive(Deserialize)]
        struct LegacyMap(
            #[serde(deserialize_with = "legacy_map")] BTreeMap<u64, LegacyConstraints>,
        );
        fn legacy_map<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<BTreeMap<u64, LegacyConstraints>, D::Error> {
            catalog_map::<D, LegacyConstraints>(d)
        }
        #[derive(Deserialize)]
        struct FrozenConstraints(#[serde(deserialize_with = "frozen_constraints")] Vec<ConstraintV2To7>);
        fn frozen_constraints<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<ConstraintV2To7>, D::Error> {
            catalog_vec::<D, ConstraintV2To7>(d)
        }
        #[derive(Deserialize)]
        struct FrozenMap(#[serde(deserialize_with = "frozen_map")] BTreeMap<u64, FrozenConstraints>);
        fn frozen_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, FrozenConstraints>, D::Error> {
            catalog_map::<D, FrozenConstraints>(d)
        }
        #[derive(Deserialize)]
        struct LogicalKinds(
            #[serde(deserialize_with = "logical_kind_map")] BTreeMap<u64, LogicalKind>,
        );
        fn logical_kind_map<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<BTreeMap<u64, LogicalKind>, D::Error> {
            catalog_map::<D, LogicalKind>(d)
        }
        #[derive(Deserialize)]
        struct Schemas(#[serde(deserialize_with = "schema_map")] BTreeMap<String, schemas::Schema>);
        fn schema_map<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<BTreeMap<String, schemas::Schema>, D::Error> {
            struct SV;
            impl<'de> Visitor<'de> for SV {
                type Value = BTreeMap<String, schemas::Schema>;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("schema map")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                    let mut out = BTreeMap::new();
                    #[derive(Deserialize)]
                    struct Name(#[serde(deserialize_with = "name")] String);
                    while let Some(key) = a.next_key::<Name>()? {
                        if out.insert(key.0, a.next_value()?).is_some() {
                            return Err(de::Error::custom("invalid schemas"));
                        }
                    }
                    Ok(out)
                }
            }
            d.deserialize_map(SV)
        }
        #[derive(Deserialize)]
        struct Extensions(#[serde(deserialize_with = "extension_map")] BTreeMap<String, super::extensions::InstalledExtension>);
        #[derive(Deserialize)]
        struct Membership(#[serde(deserialize_with = "membership_map")] BTreeMap<u64, String>);
        fn membership_map<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<BTreeMap<u64, String>, D::Error> {
            #[derive(Deserialize)]
            struct Name(#[serde(deserialize_with = "name")] String);
            Ok(catalog_map::<D, Name>(d)?
                .into_iter()
                .map(|(id, name)| (id, name.0))
                .collect())
        }
        #[derive(Deserialize)]
        struct CurrentMap(#[serde(deserialize_with = "constraint_map")] BTreeMap<u64, Vec<Constraint>>);
        #[derive(Deserialize)]
        struct Births(#[serde(deserialize_with = "birth_map")] BTreeMap<u64, u64>);
        fn birth_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, u64>, D::Error> {
            catalog_map::<D, u64>(d)
        }
        #[derive(Deserialize)]
        struct ColumnCollations(#[serde(deserialize_with = "bindings_map")] BTreeMap<u64, super::column_collations::Binding>);
        fn bindings_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, super::column_collations::Binding>, D::Error> {
            catalog_map::<D, super::column_collations::Binding>(d)
        }
        #[derive(Deserialize)]
        struct Collations(#[serde(deserialize_with = "collation_map")] BTreeMap<u64, super::collations::Collation>);
        fn collation_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, super::collations::Collation>, D::Error> {
            catalog_map::<D, super::collations::Collation>(d)
        }
        #[derive(Deserialize)]
        struct EnumLabels(#[serde(deserialize_with = "enum_labels_map")] BTreeMap<u64, AddedEnumLabels>);
        fn enum_labels_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, AddedEnumLabels>, D::Error> {
            catalog_map::<D, AddedEnumLabels>(d)
        }
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = CatalogExt;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("versioned catalog extension")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<CatalogExt, A::Error> {
                macro_rules! next {
                    ($t:ty) => {
                        a.next_element::<$t>()?
                            .ok_or_else(|| de::Error::custom("truncated catalog extension"))?
                    };
                }
                let ext_version = next!(u32);
                if !matches!(ext_version, 1..=12) {
                    return Err(de::Error::custom(UNSUPPORTED_CATALOG_EXT));
                }
                let columns = if ext_version >= 6 {
                    next!(Columns).0
                } else if matches!(ext_version, 3..=5) {
                    #[derive(Deserialize)]
                    struct V3(#[serde(deserialize_with = "v3_map")] BTreeMap<u64, ColumnV3>);
                    fn v3_map<'de, D: serde::Deserializer<'de>>(
                        d: D,
                    ) -> Result<BTreeMap<u64, ColumnV3>, D::Error> {
                        catalog_map::<D, ColumnV3>(d)
                    }
                    next!(V3)
                        .0
                        .into_iter()
                        .map(|(id, c)| (id, c.into()))
                        .collect()
                } else {
                    next!(LegacyColumns)
                        .0
                        .into_iter()
                        .map(|(id, c)| (id, c.into()))
                        .collect()
                };
                let constraints = if ext_version == 1 {
                    next!(LegacyMap)
                        .0
                        .into_iter()
                        .map(|(id, cs)| (id, cs.0.into_iter().map(Constraint::from).collect()))
                        .collect()
                } else if ext_version <= 7 {
                    next!(FrozenMap).0.into_iter().map(|(id, cs)| (id, cs.0.into_iter().map(Constraint::from).collect())).collect()
                } else {
                    next!(CurrentMap).0
                };
                let out = CatalogExt {
                    ext_version,
                    columns,
                    constraints,
                    sequences: next!(Sequences).0,
                    types: if ext_version >= 6 {
                        next!(Types).0
                    } else {
                        #[derive(Deserialize)]
                        struct EmptyTypes(
                            #[serde(deserialize_with = "empty_types")] BTreeMap<u64, TypeDef>,
                        );
                        next!(EmptyTypes).0
                    },
                    logical_kinds: if ext_version >= 4 {
                        next!(LogicalKinds).0
                    } else {
                        BTreeMap::new()
                    },
                    schemas: if ext_version >= 5 {
                        next!(Schemas).0
                    } else {
                        BTreeMap::new()
                    },
                    table_schemas: if ext_version >= 5 {
                        next!(Membership).0
                    } else {
                        BTreeMap::new()
                    },
                    primary_index_birth: if ext_version >= 8 { next!(Births).0 } else { BTreeMap::new() },
                    collations: if ext_version >= 9 { next!(Collations).0 } else { BTreeMap::new() },
                    column_collations: if ext_version >= 10 { next!(ColumnCollations).0 } else { BTreeMap::new() },
                    extensions: if ext_version >= 11 { next!(Extensions).0 } else { BTreeMap::new() },
                    added_enum_labels: if ext_version >= 12 { next!(EnumLabels).0 } else { BTreeMap::new() },
                    gin_indexes: BTreeMap::new(),
                    generated_columns: BTreeMap::new(),
                };
                if ext_version != out.wire_version() {
                    return Err(de::Error::custom("noncanonical catalog extension version"));
                }
                Ok(out)
            }
        }
        d.deserialize_tuple(13, V)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)] // PostgreSQL's referential action names
pub(crate) enum Action {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceDef {
    #[serde(deserialize_with = "relation_name")]
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) start: i64,
    pub(crate) increment: i64,
    pub(crate) min: i64,
    pub(crate) max: i64,
    pub(crate) cache: i64,
    pub(crate) cycle: bool,
    pub(crate) owned_by: Option<(u64, u64)>,
    pub(crate) identity: bool,
    pub(crate) incarnation: u128,
}
pub type TypeField = (String, DataType, Option<i32>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeDef {
    Enum {
        #[serde(deserialize_with = "name")]
        name: String,
        #[serde(deserialize_with = "name")]
        schema: String,
        #[serde(deserialize_with = "type_labels")]
        labels: Vec<String>,
    },
    Domain {
        #[serde(deserialize_with = "name")]
        name: String,
        #[serde(deserialize_with = "name")]
        schema: String,
        #[serde(with = "type_def_scalar")]
        base: DataType,
        typmod: Option<i32>,
        not_null: bool,
        #[serde(deserialize_with = "optional_expr")]
        check: Option<String>,
        #[serde(deserialize_with = "name")]
        constraint: String,
    },
    Composite {
        #[serde(deserialize_with = "name")]
        name: String,
        #[serde(deserialize_with = "name")]
        schema: String,
        #[serde(
            serialize_with = "serialize_type_fields",
            deserialize_with = "type_fields"
        )]
        fields: Vec<TypeField>,
    },
}
impl TypeDef {
    pub(crate) fn has_uuid_arrays(&self) -> bool {
        match self {
            Self::Enum { .. } => false,
            Self::Domain { base, .. } => *base == DataType::UuidArray,
            Self::Composite { fields, .. } => fields.iter().any(|(_, ty, _)| *ty == DataType::UuidArray),
        }
    }
}
fn type_labels<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    struct Label(#[serde(deserialize_with = "name")] String);
    Ok(bounded_vec::<D, Label, MAX_COLUMNS>(d)?
        .into_iter()
        .map(|v| v.0)
        .collect())
}
// Row DataType deliberately aliases logical text kinds to Text on the wire.
// Type definitions have no column sidecar for their base/field types, so keep
// their own scalar tags: existing 0..=14 stay byte-identical; 15..=21 retain kinds.
mod type_def_scalar {
    use super::*;
    const TYPES: &[DataType] = &[
        DataType::Boolean,
        DataType::SmallInt,
        DataType::Integer,
        DataType::BigInt,
        DataType::Text,
        DataType::Bytes,
        DataType::Numeric,
        DataType::Real,
        DataType::DoublePrecision,
        DataType::Date,
        DataType::Time,
        DataType::TimeTz,
        DataType::Timestamp,
        DataType::TimestampTz,
        DataType::Interval,
        DataType::Varchar,
        DataType::Bpchar,
        DataType::Uuid,
        DataType::Jsonb,
        DataType::IntArray,
        DataType::BigIntArray,
        DataType::TextArray,
        DataType::UuidArray,
    ];
    pub(super) fn serialize<S: serde::Serializer>(ty: &DataType, s: S) -> Result<S::Ok, S::Error> {
        let tag = TYPES.iter().position(|t| t == ty).ok_or_else(|| {
            <S::Error as serde::ser::Error>::custom("unknown type-definition scalar")
        })? as u32;
        tag.serialize(s)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<DataType, D::Error> {
        let tag = u32::deserialize(d)?;
        TYPES
            .get(tag as usize)
            .cloned()
            .ok_or_else(|| de::Error::custom("unknown type-definition scalar"))
    }
}
fn serialize_type_fields<S: serde::Serializer>(
    fields: &[TypeField],
    s: S,
) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Field(
        String,
        #[serde(with = "type_def_scalar")] DataType,
        Option<i32>,
    );
    fields
        .iter()
        .map(|(name, ty, typmod)| Field(name.clone(), ty.clone(), *typmod))
        .collect::<Vec<_>>()
        .serialize(s)
}
fn type_fields<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<TypeField>, D::Error> {
    #[derive(Deserialize)]
    struct Field(
        #[serde(deserialize_with = "name")] String,
        #[serde(with = "type_def_scalar")] DataType,
        Option<i32>,
    );
    Ok(bounded_vec::<D, Field, MAX_COLUMNS>(d)?
        .into_iter()
        .map(|v| (v.0, v.1, v.2))
        .collect())
}
fn empty_types<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, TypeDef>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = BTreeMap<u64, TypeDef>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("empty legacy type map")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            if a.size_hint().is_some_and(|n| n > 0) || a.next_key::<u64>()?.is_some() {
                return Err(de::Error::custom(UNSUPPORTED_CATALOG_EXT));
            }
            Ok(BTreeMap::new())
        }
    }
    d.deserialize_map(V)
}

impl TypeDef {
    pub fn schema(&self) -> &str {
        match self {
            Self::Enum { schema, .. }
            | Self::Domain { schema, .. }
            | Self::Composite { schema, .. } => schema,
        }
    }
    pub(super) fn set_schema(&mut self, value: String) {
        match self {
            Self::Enum { schema, .. }
            | Self::Domain { schema, .. }
            | Self::Composite { schema, .. } => *schema = value,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Enum { name, .. } | Self::Domain { name, .. } | Self::Composite { name, .. } => {
                name
            }
        }
    }
    pub(crate) fn storage_type(&self) -> R<DataType> {
        match self {
            Self::Enum { .. } => Ok(DataType::Text),
            Self::Domain { base, .. } => Ok(base.clone()),
            Self::Composite { .. } => Err(SqlError::Unsupported),
        }
    }
}
fn bounded_vec<'de, D, T, const N: usize>(d: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct V<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for V<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded sequence")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            if a.size_hint().is_some_and(|n| n > N) {
                return Err(de::Error::custom("catalog sequence limit"));
            }
            let mut v = Vec::new();
            while let Some(x) = a.next_element()? {
                if v.len() == N {
                    return Err(de::Error::custom("catalog sequence limit"));
                }
                v.push(x);
            }
            Ok(v)
        }
    }
    d.deserialize_seq(V::<T, N>(std::marker::PhantomData))
}
// Ignore collection size hints: a malformed length cannot reserve memory or
// cause a loop without consuming encoded bytes. The storage codec caps the
// input byte length, and each catalog entry consumes bytes before insertion.
fn catalog_vec<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct V<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("catalog sequence")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = a.next_element()? {
                values.push(value);
            }
            Ok(values)
        }
    }
    d.deserialize_seq(V::<T>(std::marker::PhantomData))
}
fn catalog_map<'de, D, T>(d: D) -> Result<BTreeMap<u64, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct V<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
        type Value = BTreeMap<u64, T>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("catalog map")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            let mut v = BTreeMap::new();
            while let Some(k) = a.next_key()? {
                if v.insert(k, a.next_value()?).is_some() {
                    return Err(de::Error::custom("duplicate catalog key"));
                }
            }
            Ok(v)
        }
    }
    d.deserialize_map(V::<T>(std::marker::PhantomData))
}
fn string<'de, D: serde::Deserializer<'de>, const N: usize>(d: D) -> Result<String, D::Error> {
    String::from_utf8(bounded_vec::<D, u8, N>(d)?).map_err(de::Error::custom)
}
pub(super) fn name<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    string::<D, 63>(d)
}
fn relation_name<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    string::<D, 127>(d)
}
fn expression<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    string::<D, MAX_EXPR>(d)
}
fn optional_expr<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<String>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("optional expression")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            expression(d).map(Some)
        }
    }
    d.deserialize_option(V)
}
fn ids<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    bounded_vec::<D, u64, MAX_COLUMNS>(d)
}
fn column_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<u64, ColumnExt>, D::Error> {
    catalog_map::<D, ColumnExt>(d)
}
#[derive(Deserialize)]
struct Constraints(#[serde(deserialize_with = "constraints")] Vec<Constraint>);
fn constraints<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Constraint>, D::Error> {
    catalog_vec::<D, Constraint>(d)
}
fn constraint_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<u64, Vec<Constraint>>, D::Error> {
    Ok(catalog_map::<D, Constraints>(d)?
        .into_iter()
        .map(|(k, v)| (k, v.0))
        .collect())
}
fn sequence_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<u64, SequenceDef>, D::Error> {
    catalog_map::<D, SequenceDef>(d)
}
fn type_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, TypeDef>, D::Error> {
    catalog_map::<D, TypeDef>(d)
}
pub(crate) fn validate_typmod(ty: &DataType, t: i32) -> R<()> {
    let valid = match ty {
        DataType::Varchar | DataType::Bpchar => (5..=10_485_764).contains(&t),
        DataType::Numeric => {
            giggles_pg_types::typmod::NumericTypmod::from_typmod(t).is_some_and(|m| {
                giggles_pg_types::typmod::NumericTypmod::new(m.precision(), m.scale()).is_ok()
                    && m.to_typmod() == t
            })
        }
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz => {
            (0..=6).contains(&t)
        }
        DataType::Interval => {
            let range = (t >> 16) & 0x7fff;
            let prec = t & 0xffff;
            let masks = [
                4, 2, 8, 1024, 2048, 4096, 6, 1032, 3080, 7176, 3072, 7168, 6144,
            ];
            t >= 0
                && (range == 32767 || masks.contains(&range))
                && (prec == 65535 || (0..=6).contains(&prec))
                && (prec == 65535 || range == 32767 || range & 4096 != 0)
                && !(range == 32767 && prec == 65535)
        }
        _ => false,
    };
    if valid { Ok(()) } else { Err(SqlError::Type) }
}
/// Suffixes used by PostgreSQL's automatically selected constraint names.
#[derive(Clone, Copy)]
pub(crate) enum AutoName {
    Key,
    Pkey,
    Check,
    Fkey,
}

// Match makeObjectName: reserve separators and label, then balance byte lengths
// before clipping each identifier at a UTF-8 boundary.
fn constraint_object_name(table: &str, columns: &str, label: &str) -> String {
    let mut table_len = table.len();
    let mut columns_len = columns.len();
    let overhead = label.len() + 1 + usize::from(!columns.is_empty());
    while table_len + columns_len + overhead > 63 {
        if table_len > columns_len {
            table_len -= 1;
        } else {
            columns_len -= 1;
        }
    }
    while !table.is_char_boundary(table_len) {
        table_len -= 1;
    }
    while !columns.is_char_boundary(columns_len) {
        columns_len -= 1;
    }
    if columns.is_empty() {
        format!("{}_{}", &table[..table_len], label)
    } else {
        format!(
            "{}_{}_{}",
            &table[..table_len],
            &columns[..columns_len],
            label
        )
    }
}

impl Relational {
    pub(super) fn choose_sequence_name(&self, table: &str, column: &str) -> String {
        for attempt in 0u64.. {
            let label = if attempt == 0 {
                "seq".to_owned()
            } else {
                format!("seq{attempt}")
            };
            let (schema, table) = split_key(table);
            let name = relation_key(schema, &constraint_object_name(table, column, &label));
            if !self.relation_name_taken(&name) {
                return name;
            }
        }
        unreachable!("bounded catalog cannot exhaust sequence names")
    }
    pub(crate) fn relation_name_taken(&self, name: &str) -> bool {
        self.tables.contains_key(name)
            || self.view_names.contains_key(name)
            || self.secondary.descriptors().iter().any(|d| d.name == name)
            || self.gin_index(name).is_some()
            || self.sequence_by_name(name).is_some()
    }

    pub(crate) fn constraint_name_taken(&self, table: u64, name: &str) -> bool {
        self.table_constraints(table).iter().any(|c| c.name == name)
            || self
                .tables
                .values()
                .find(|t| t.id == table)
                .and_then(|t| constraint_primary_key::primary_key_name(self, t))
                .is_some_and(|n| n == name)
            || self
                .secondary
                .descriptors()
                .iter()
                .any(|d| d.table == table && d.unique && split_key(&d.name).1 == name)
    }

    pub(crate) fn choose_constraint_name(
        &self,
        table: &str,
        columns: &[&str],
        kind: AutoName,
    ) -> String {
        let (suffix, relation) = match kind {
            AutoName::Key => ("key", true),
            AutoName::Pkey => ("pkey", true),
            AutoName::Check => ("check", false),
            AutoName::Fkey => ("fkey", false),
        };
        let columns = if matches!(kind, AutoName::Pkey) {
            String::new()
        } else {
            columns.join("_")
        };
        for attempt in 0u64.. {
            let label = if attempt == 0 {
                suffix.to_owned()
            } else {
                format!("{suffix}{attempt}")
            };
            let (schema, table) = split_key(table);
            let name = constraint_object_name(table, &columns, &label);
            let key = relation_key(schema, &name);
            let constraint_taken = self
                .ext
                .constraints
                .iter()
                .filter(|(id, _)| self.table_schema(**id) == schema)
                .flat_map(|(_, constraints)| constraints)
                .any(|c| c.name == name)
                || self.tables.values().any(|t| {
                    self.table_schema(t.id) == schema
                        && constraint_primary_key::primary_key_name(self, t)
                            .is_some_and(|n| n == name)
                })
                || self
                    .secondary
                    .descriptors()
                    .iter()
                    .any(|d| d.unique && d.name == key);
            if !constraint_taken && (!relation || !self.relation_name_taken(&key)) {
                return name;
            }
        }
        unreachable!("bounded catalog cannot exhaust constraint names")
    }

    pub(crate) fn catalog_ext(&self) -> &CatalogExt {
        &self.ext
    }
    pub(super) fn normalize_ext(ext: &mut CatalogExt) {
        ext.columns.retain(|_, v| v != &ColumnExt::default());
        ext.constraints.retain(|_, v| !v.is_empty());
        ext.ext_version = ext.wire_version();
    }
    pub(crate) fn validate_unique_indexes(&self) -> R<()> {
        super::column_collations::validate_index_bindings(self.ext.as_ref(),self.secondary_descriptors_engine())?;
        validate_unique_indexes(
            self.ext.constraints.iter().flat_map(|(t, cs)| cs.iter().map(move |c| (*t, c))),
            self.secondary.descriptors().iter(),
        )?;
        validate_key_targets(self.ext.as_ref(), self.tables.values(), self.secondary.descriptors().iter(), self.next_id)
    }
    pub(crate) fn validate_ext(&self) -> R<()> {
        self.validate_ext_candidate(self.ext.as_ref(), self.next_id, None)
    }
    pub(super) fn validate_ext_candidate(
        &self,
        ext: &CatalogExt,
        next_id: u64,
        excluded_table: Option<u64>,
    ) -> R<()> {
        if !matches!(ext.ext_version, 1..=12) {
            return Err(SqlError::Unsupported);
        }
        self.validate_schemas(ext, next_id, excluded_table)?;
        if ext.ext_version != ext.wire_version() {
            return Err(SqlError::Type);
        }
        if self.format != 1 {
            return Err(SqlError::Type);
        }
        if let Some(excluded) = excluded_table
            && ext.constraints.values().flatten().any(|constraint| {
                matches!(&constraint.kind, ConstraintKind::ForeignKey { ref_table, .. } if *ref_table == excluded)
            }) {
                return Err(SqlError::Unsupported);
            }
        let tables: BTreeMap<_, _> = self
            .tables
            .values()
            .filter(|t| Some(t.id) != excluded_table)
            .map(|t| (t.id, t))
            .collect();
        let columns: BTreeMap<_, _> = self
            .tables
            .values()
            .filter(|t| Some(t.id) != excluded_table)
            .flat_map(|t| t.columns.iter().map(move |c| (c.id, (t.id, c))))
            .collect();
        let mut used: BTreeSet<_> = tables
            .keys()
            .chain(columns.keys())
            .copied()
            .chain(ext.schemas.values().map(|s| s.id))
            .collect();
        super::extensions::validate(ext)?;
        super::column_collations::validate_bindings(self, ext, excluded_table)?;
        let mut collation_names = BTreeSet::new();
        for (id, c) in &ext.collations {
            super::collations::validate(c)?;
            if *id == 0 || *id >= next_id || !used.insert(*id)
                || !collation_names.insert((&c.schema, &c.name))
                || self.secondary.descriptors().iter().any(|d| d.id == *id)
                || (c.schema != "public" && !ext.schemas.contains_key(&c.schema)) {
                return Err(SqlError::Type);
            }
            super::user_types::type_oid(*id)?;
        }
        let mut type_names = BTreeSet::new();
        for (id, ty) in &ext.types {
            if *id == 0 || *id >= next_id || !used.insert(*id) || !type_names.insert(ty.name()) {
                return Err(SqlError::Type);
            }
            super::user_types::type_oid(*id)?;
            super::user_types::validate_type(ty)?;
            if ty.schema() != "public" && !ext.schemas.contains_key(ty.schema()) {
                return Err(SqlError::InvalidSchema);
            }
        }
        let label_ids = ext.enum_label_ids()?;
        let indexes: BTreeSet<_> = if label_ids.is_empty() {
            BTreeSet::new()
        } else {
            self.secondary.descriptors().iter().map(|d| d.id).collect()
        };
        let extension_oids: BTreeSet<_> = ext.extensions.values().map(|extension| extension.oid).collect();
        for (_, id) in label_ids {
            if id >= next_id || !used.insert(id) || indexes.contains(&id)
                || extension_oids.contains(&super::user_types::type_oid(id)?)
            {
                return Err(SqlError::Type);
            }
        }
        for (id, (_, column)) in &columns {
            let expected = match column.data_type {
                DataType::RegType | DataType::RegConfig | DataType::RegDictionary | DataType::Oid
                | DataType::PgChar | DataType::RealArray | DataType::PgCharArray | DataType::SmallIntArray | DataType::RegDictionaryArray => return Err(SqlError::Unsupported),
                DataType::Varchar => Some(TextKind::Varchar),
                DataType::Bpchar => Some(TextKind::Bpchar),

                DataType::Uuid
                | DataType::Jsonb
                | DataType::IntArray
                | DataType::BigIntArray
                | DataType::TextArray
                | DataType::UuidArray
                | DataType::TsVector
                | DataType::TsQuery
                | DataType::Boolean
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Text
                | DataType::Bytes
                | DataType::Numeric
                | DataType::Real
                | DataType::DoublePrecision
                | DataType::Date
                | DataType::Time
                | DataType::TimeTz
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Interval => None,
            };
            let logical = match column.data_type {
                DataType::Uuid => Some(LogicalKind::Uuid),
                DataType::Jsonb => Some(LogicalKind::Jsonb),
                DataType::IntArray => Some(LogicalKind::IntArray),
                DataType::BigIntArray => Some(LogicalKind::BigIntArray),
                DataType::TextArray => Some(LogicalKind::TextArray),
                DataType::UuidArray => Some(LogicalKind::UuidArray),
                DataType::TsVector => Some(LogicalKind::TsVector),
                DataType::TsQuery => Some(LogicalKind::TsQuery),
                _ => None,
            };
            if ext.logical_kinds.get(id).copied() != logical {
                return Err(SqlError::Type);
            }
            if ext.columns.get(id).and_then(|c| c.text_kind) != expected {
                return Err(SqlError::Type);
            }
        }
        if ext.logical_kinds.keys().any(|id| !columns.contains_key(id)) {
            return Err(SqlError::Type);
        }
        for (id, e) in &ext.columns {
            let (_, c) = columns.get(id).ok_or(SqlError::Type)?;
            if let Some(id) = e.type_id {
                let ty = ext.types.get(&id).ok_or(SqlError::Type)?;
                // An enum binding paired with the existing TextArray logical kind
                // denotes a flat array of that enum. Scalar bindings retain their layout.
                if ty.storage_type()? != c.data_type
                    && !matches!((ty, &c.data_type), (TypeDef::Enum { .. }, DataType::TextArray))
                {
                    return Err(SqlError::Type);
                }
            }
            if let Some(t) = e.typmod {
                validate_typmod(&c.data_type, t)?;
            }
            if e.default.as_ref().is_some_and(|s| s.len() > MAX_EXPR) {
                return Err(SqlError::Type);
            }
            if let Some(i) = &e.identity
                && !ext
                    .sequences
                    .get(&i.sequence_id)
                    .is_some_and(|s| s.identity)
            {
                return Err(SqlError::Type);
            }
        }
        for (table, cs) in &ext.constraints {
            let local = tables.get(table).ok_or(SqlError::Type)?;
            let mut names = BTreeSet::new();
            for c in cs {
                if c.id == 0
                    || c.id >= next_id
                    || !used.insert(c.id)
                    || c.name.len() > 63
                    || c.name.is_empty()
                    || !names.insert(&c.name)
                {
                    return Err(SqlError::Type);
                }
                match &c.kind {
                    ConstraintKind::Check { expr, .. } => {
                        if expr.len() > MAX_EXPR {
                            return Err(SqlError::Type);
                        }
                    }
                    ConstraintKind::ForeignKey {
                        columns: cols,
                        ref_table,
                        ref_columns,
                        ..
                    } => {
                        let remote = tables.get(ref_table).ok_or(SqlError::Type)?;
                        if cols.is_empty()
                            || cols.len() > MAX_COLUMNS
                            || cols.len() != ref_columns.len()
                        {
                            return Err(SqlError::Type);
                        }
                        let mut seen = BTreeSet::new();
                        let mut refs = BTreeSet::new();
                        for (a, b) in cols.iter().zip(ref_columns) {
                            if !local.columns.iter().any(|c| c.id == *a)
                                || !remote.columns.iter().any(|c| c.id == *b)
                                || !seen.insert(a)
                                || !refs.insert(b)
                            {
                                return Err(SqlError::Type);
                            }
                        }
                    }
                    ConstraintKind::Unique { columns: cols, index } => {
                        if *index == 0 || *index >= next_id || *index == c.id
                            || cols.is_empty() || cols.len() > MAX_COLUMNS
                            || cols.iter().collect::<BTreeSet<_>>().len() != cols.len()
                            || cols.iter().any(|id| !local.columns.iter().any(|c| c.id == *id)) {
                            return Err(SqlError::Type);
                        }
                    }
                    ConstraintKind::PrimaryKey => {}
                }
            }
        }
        let mut names = BTreeSet::new();
        for (id, s) in &ext.sequences {
            if *id == 0 || *id >= next_id || !used.insert(*id) || !names.insert(&s.name) {
                return Err(SqlError::Type);
            }
            schema_binding::validate_key(&s.name)?;
            let schema = split_key(&s.name).0;
            if schema != "public" && !ext.schemas.contains_key(schema) {
                return Err(SqlError::Type);
            }
            if split_key(&s.name).1.len() > 63
                || !matches!(
                    s.data_type,
                    DataType::SmallInt | DataType::Integer | DataType::BigInt
                )
                || s.increment == 0
                || s.min > s.max
                || s.start < s.min
                || s.start > s.max
                || s.cache <= 0
            {
                return Err(SqlError::Type);
            }
            let (lo, hi) = match s.data_type {
                DataType::SmallInt => (i16::MIN as i64, i16::MAX as i64),
                DataType::Integer => (i32::MIN as i64, i32::MAX as i64),
                _ => (i64::MIN, i64::MAX),
            };
            if s.min < lo || s.max > hi {
                return Err(SqlError::Type);
            }
            if let Some((t, c)) = s.owned_by
                && !columns.get(&c).is_some_and(|(owner, _)| {
                    *owner == t
                        && ext.table_schemas.get(&t).map_or("public", String::as_str) == schema
                })
            {
                return Err(SqlError::Type);
            }
        }
        validate_births(ext, &tables, next_id, &mut used)?;
        let mut gin_names = BTreeSet::new();
        for (id, index) in &ext.gin_indexes {
            let table = tables.get(&index.table).ok_or(SqlError::Type)?;
            let schema = ext.table_schemas.get(&index.table).map_or("public", String::as_str);
            if *id == 0 || *id >= next_id || !used.insert(*id)
                || split_key(&index.name).0 != schema
                || !gin_names.insert(&index.name)
                || self.tables.contains_key(&index.name)
                || self.secondary.descriptors().iter().any(|d| d.id == *id || d.name == index.name)
            {
                return Err(SqlError::Type);
            }
            super::user_types::type_oid(*id)?;
            index.validate(table)?;
        }
        for (id, generation) in &ext.generated_columns {
            let (table, column) = columns.get(id).ok_or(SqlError::Type)?;
            if column.name == HIDDEN_ROWID || generation.sql.is_empty() || generation.sql.len() > MAX_EXPR
                || ext.columns.get(id).is_some_and(|ext| ext.default.is_some() || ext.identity.is_some())
                || !generation.references.windows(2).all(|w| w[0] < w[1])
                || generation.references.iter().any(|id| {
                    ext.generated_columns.contains_key(id) || columns.get(id).is_none_or(|(owner, _)| owner != table)
                })
            {
                return Err(SqlError::Type);
            }
        }
        Ok(())
    }
    fn require_column(&self, id: u64) -> R<()> {
        if self
            .tables
            .values()
            .any(|t| t.columns.iter().any(|c| c.id == id))
        {
            Ok(())
        } else {
            Err(SqlError::Type)
        }
    }
    pub(super) fn mutate_ext(
        &mut self,
        f: impl FnOnce(&mut CatalogExt, &mut u64) -> R<()>,
    ) -> R<()> {
        let mut ext = (*self.ext).clone();
        let mut next_id = self.next_id;
        f(&mut ext, &mut next_id)?;
        Self::normalize_ext(&mut ext);
        self.validate_ext_candidate(&ext, next_id, None)?;
        self.ext = Arc::new(ext);
        self.next_id = next_id;
        Ok(())
    }
    pub(crate) fn set_column_typmod(&mut self, id: u64, t: Option<i32>) -> R<()> {
        let col = self
            .tables
            .values()
            .flat_map(|t| &t.columns)
            .find(|c| c.id == id)
            .ok_or(SqlError::Type)?;
        if let Some(t) = t {
            validate_typmod(&col.data_type, t).map_err(|_| SqlError::InvalidParameterValue)?;
        }
        self.mutate_ext(|ext, _| {
            ext.columns.entry(id).or_default().typmod = t;
            Ok(())
        })
    }
    pub(crate) fn set_column_text_kind(&mut self, id: u64, kind: Option<TextKind>) -> R<()> {
        let mut candidate = self.clone();
        let column = std::sync::Arc::make_mut(&mut candidate.tables).values_mut()
            .flat_map(|t| &mut t.columns)
            .find(|c| c.id == id)
            .ok_or(SqlError::Type)?;
        if !matches!(
            column.data_type,
            DataType::Text | DataType::Varchar | DataType::Bpchar
        ) {
            return Err(SqlError::Type);
        }
        column.data_type = match kind {
            Some(TextKind::Varchar) => DataType::Varchar,
            Some(TextKind::Bpchar) => DataType::Bpchar,

            None => DataType::Text,
        };
        std::sync::Arc::make_mut(&mut candidate.ext).columns.entry(id).or_default().text_kind = kind;
        Self::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
        candidate.validate_ext()?;
        *self = candidate;
        Ok(())
    }
    pub(crate) fn set_column_default(&mut self, id: u64, v: Option<String>) -> R<()> {
        self.require_column(id)?;
        self.mutate_ext(|ext, _| {
            ext.columns.entry(id).or_default().default = v;
            Ok(())
        })
    }
    pub(crate) fn set_column_identity(&mut self, id: u64, v: Option<Identity>) -> R<()> {
        self.require_column(id)?;
        self.mutate_ext(|ext, _| {
            ext.columns.entry(id).or_default().identity = v;
            Ok(())
        })
    }
    pub(crate) fn add_constraint(&mut self, table: u64, c: Constraint) -> R<()> {
        // The metadata API may materialize the already implicit primary-key
        // name (for example while importing a frozen extension DTO). It does
        // not introduce a second logical constraint. SQL CREATE/ADD keeps the
        // default implicit through constraint_primary_key::record_name.
        let materializes_primary = matches!(c.kind, ConstraintKind::PrimaryKey)
            && self.tables.values().any(|t| {
                t.id == table && t.hidden_rowid().is_none() && c.name == format!("{}_pkey", t.name)
            })
            && !self.table_constraints(table).iter().any(|existing| {
                existing.name == c.name || matches!(existing.kind, ConstraintKind::PrimaryKey)
            })
            && !self
                .secondary
                .descriptors()
                .iter()
                .any(|index| index.name == c.name);
        if self.constraint_name_taken(table, &c.name) && !materializes_primary {
            return Err(SqlError::DuplicateObject);
        }
        self.mutate_ext(|ext, next_id| {
            if c.id != *next_id {
                return Err(SqlError::Type);
            }
            allocate(next_id)?;
            ext.constraints.entry(table).or_default().push(c);
            Ok(())
        })
    }
    pub(crate) fn remove_constraint(&mut self, table: u64, name: &str) -> R<()> {
        if !self.tables.values().any(|t| t.id == table) {
            return Err(SqlError::Type);
        }
        self.mutate_ext(|ext, _| {
            if let Some(cs) = ext.constraints.get_mut(&table) {
                cs.retain(|c| c.name != name)
            }
            Ok(())
        })
    }
    pub(crate) fn add_sequence(&mut self, v: SequenceDef) -> R<u64> {
        let mut id = 0;
        self.mutate_ext(|ext, next_id| {
            id = allocate(next_id)?;
            ext.sequences.insert(id, v);
            Ok(())
        })?;
        Ok(id)
    }
    pub(crate) fn remove_sequence(&mut self, id: u64) -> R<()> {
        self.mutate_ext(|ext, _| {
            ext.sequences.remove(&id);
            Ok(())
        })
    }
    pub(crate) fn drop_column_ext(&mut self, id: u64) -> R<()> {
        self.mutate_ext(|ext, _| {
            ext.columns.remove(&id);
            ext.column_collations.remove(&id);
            ext.logical_kinds.remove(&id);
            ext.sequences
                .retain(|_, v| !matches!(v.owned_by,Some((_,c)) if c==id));
            Ok(())
        })
    }
    pub(crate) fn column_typmod(&self, id: u64) -> i32 {
        self.ext
            .columns
            .get(&id)
            .and_then(|c| c.typmod)
            .unwrap_or(-1)
    }
    pub(crate) fn column_default(&self, id: u64) -> Option<&str> {
        self.ext.columns.get(&id).and_then(|c| c.default.as_deref())
    }
    pub(crate) fn column_identity(&self, id: u64) -> Option<&Identity> {
        self.ext.columns.get(&id).and_then(|c| c.identity.as_ref())
    }
    pub(crate) fn table_constraints(&self, id: u64) -> &[Constraint] {
        self.ext
            .constraints
            .get(&id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
    pub(crate) fn sequence_by_name(&self, name: &str) -> Option<(u64, &SequenceDef)> {
        self.ext
            .sequences
            .iter()
            .find(|(_, s)| s.name == name)
            .map(|(id, s)| (*id, s))
    }
}

#[cfg(test)]
mod logical_catalog_ext_tests {
    use super::*;
    use bincode::Options;
    fn codec() -> impl Options {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .reject_trailing_bytes()
    }
    #[test]
    fn catalog_ext_v4_frozen_layout() {
        let mut ext = CatalogExt::default();
        ext.logical_kinds.insert(7, LogicalKind::Uuid);
        Relational::normalize_ext(&mut ext);
        let bytes = codec().serialize(&ext).unwrap();
        let mut expected = 4u32.to_be_bytes().to_vec();
        for _ in 0..4 {
            expected.extend_from_slice(&0u64.to_be_bytes());
        }
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.extend_from_slice(&7u64.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(bytes, expected);
        assert_eq!(codec().deserialize::<CatalogExt>(&bytes).unwrap(), ext);
        assert_eq!(
            codec().serialize(&DataType::Uuid).unwrap(),
            codec().serialize(&DataType::Text).unwrap()
        );
        // A v3 payload cannot smuggle the new metadata through its frozen tail.
        let mut wrong = bytes;
        wrong[..4].copy_from_slice(&3u32.to_be_bytes());
        assert!(codec().deserialize::<CatalogExt>(&wrong).is_err());
    }
}

#[cfg(test)]
#[path = "fk_target_codec_tests.rs"]
mod fk_target_codec_tests;

pub(crate) fn extension_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<String, super::extensions::InstalledExtension>, D::Error> {
    struct SV;
    impl<'de> Visitor<'de> for SV {
        type Value = BTreeMap<String, super::extensions::InstalledExtension>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("extension map")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            let mut out = BTreeMap::new();
            #[derive(Deserialize)]
            struct Name(#[serde(deserialize_with = "name")] String);
            while let Some(key) = a.next_key::<Name>()? {
                if out.insert(key.0, a.next_value()?).is_some() {
                    return Err(de::Error::custom("invalid extensions"));
                }
            }
            Ok(out)
        }
    }
    d.deserialize_map(SV)
}
