//! Relation-bound value mapping. Catalog/typmod qualification is a caller input.
use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use super::{Error, Result};
use crate::{
    event::{EventKind, OpaqueValue, Row},
    mapping::{IdentityMapping, Mapping, PgType, Value},
    pg::{
        assembly::MappedEvent,
        pgoutput::{Column, Message, OldTuple, Relation, RelationColumn, Tuple},
    },
};

pub struct ColumnMapping {
    pub pg_type: PgType,
    pub mapping: Box<dyn Mapping>,
}
impl ColumnMapping {
    pub fn identity(pg_type: PgType) -> Self {
        Self {
            mapping: Box::new(IdentityMapping::new(pg_type.clone())),
            pg_type,
        }
    }
}

/// Resolve the actual catalog descriptor, including enum/domain identity and
/// typmod preflight. Unknown types must fail rather than become raw text.
pub trait TypeResolver {
    /// Qualified table projection. None preserves the whole-relation API.
    /// Excluded columns are consumed as wire slots without resolving or decoding.
    fn included_columns(&mut self, _relation: &Relation<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(None)
    }
    fn resolve(
        &mut self,
        relation: &Relation<'_>,
        column: &RelationColumn<'_>,
    ) -> Result<ColumnMapping>;
}
impl<F> TypeResolver for F
where
    F: FnMut(&Relation<'_>, &RelationColumn<'_>) -> Result<ColumnMapping>,
{
    fn resolve(
        &mut self,
        relation: &Relation<'_>,
        column: &RelationColumn<'_>,
    ) -> Result<ColumnMapping> {
        self(relation, column)
    }
}

struct CachedColumn {
    name: String,
    key: bool,
    type_id: String,
    value: ColumnMapping,
}
struct CachedRelation {
    table: String,
    columns: Vec<Option<CachedColumn>>,
}
pub struct RelationMapper<R> {
    resolver: R,
    relations: BTreeMap<u32, CachedRelation>,
    pub(super) schema_version: u64,
    mapped_versions: BTreeMap<u32, u64>,
}
impl<R: TypeResolver> RelationMapper<R> {
    pub fn new(resolver: R, schema_version: u64) -> Self {
        Self {
            resolver,
            relations: BTreeMap::new(),
            schema_version,
            mapped_versions: BTreeMap::new(),
        }
    }
    pub(super) fn set_mapped_versions(&mut self, versions: impl IntoIterator<Item = (u32, u64)>) {
        self.mapped_versions = versions.into_iter().collect();
    }

    fn version(&self, oid: u32) -> u64 {
        self.mapped_versions
            .get(&oid)
            .copied()
            .unwrap_or(self.schema_version)
    }
    pub fn relation(&mut self, relation: &Relation<'_>) -> Result<()> {
        // A failed replacement must not leave an older descriptor usable.
        self.relations.remove(&relation.id);
        let included = self.resolver.included_columns(relation)?;
        let mut columns = Vec::new();
        let mut names = BTreeSet::new();
        for column in &relation.columns {
            if !names.insert(column.name) {
                return Err(Error::RecoveryRequired("duplicate relation column"));
            }
            if included
                .as_ref()
                .is_some_and(|names| !names.contains(column.name))
            {
                if column.key
                    || relation.replica_identity == crate::pg::pgoutput::ReplicaIdentity::Full
                {
                    return Err(Error::RecoveryRequired(
                        "excluded replica identity column; include every replica identity column",
                    ));
                }
                columns.push(None);
                continue;
            }
            let value = self.resolver.resolve(relation, column)?;
            let descriptor = serde_json::to_vec(&(&value.pg_type, column.type_modifier))
                .map_err(|_| Error::RecoveryRequired("type descriptor encoding"))?;
            let hash = Sha256::digest(descriptor);
            let type_id = format!("skva.v1:{hash:x}");
            columns.push(Some(CachedColumn {
                name: column.name.into(),
                key: column.key,
                type_id,
                value,
            }));
        }
        if included.as_ref().is_some_and(|included| {
            included.is_empty() || included.iter().any(|name| !names.contains(name.as_str()))
        }) {
            return Err(Error::RecoveryRequired(
                "selected column absent from relation; recollect catalog evidence",
            ));
        }
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        self.relations.insert(
            relation.id,
            CachedRelation {
                table: format!("{}.{}", quote(relation.namespace), quote(relation.name)),
                columns,
            },
        );
        Ok(())
    }
    pub fn map(&self, message: &Message<'_>) -> Result<Vec<MappedEvent>> {
        if let Message::Truncate {
            cascade,
            restart_identity,
            relation_ids,
        } = message
        {
            let version = relation_ids
                .first()
                .map(|id| self.version(*id))
                .unwrap_or(self.schema_version);
            // A canonical grouped TRUNCATE has one version. Keep the group
            // atomic and fail closed if its descriptors need different versions.
            if relation_ids.iter().any(|id| self.version(*id) != version) {
                return Err(Error::RecoveryRequired(
                    "grouped truncate has different mapped versions",
                ));
            }
            let relations = relation_ids
                .iter()
                .map(|id| self.get(*id).map(|r| r.table.clone()))
                .collect::<Result<Vec<_>>>()?;
            return Ok(vec![MappedEvent {
                schema_version: version,
                kind: EventKind::Truncate {
                    relations,
                    cascade: *cascade,
                    restart_identity: *restart_identity,
                },
                table: None,
                key: Row::new(),
                before: None,
                after: None,
                unchanged_cols: BTreeSet::new(),
            }]);
        }
        let (id, kind, old, new) = match message {
            Message::Insert { relation_id, new } => {
                (*relation_id, EventKind::Insert, None, Some(new))
            }
            Message::Update {
                relation_id,
                old,
                new,
            } => (*relation_id, EventKind::Update, old.as_ref(), Some(new)),
            Message::Delete { relation_id, old } => {
                (*relation_id, EventKind::Delete, Some(old), None)
            }
            _ => return Err(Error::RecoveryRequired("unmapped source message")),
        };
        let relation = self.get(id)?;
        let (after, unchanged_cols) = new
            .map(|tuple| row(relation, tuple, false))
            .transpose()?
            .map_or((None, BTreeSet::new()), |(r, u)| (Some(r), u));
        let before = old
            .map(|old| match old {
                OldTuple::Key(t) => row(relation, t, true),
                OldTuple::Full(t) => row(relation, t, false),
            })
            .transpose()?;
        if before.as_ref().is_some_and(|(_, u)| !u.is_empty())
            || (kind == EventKind::Insert && !unchanged_cols.is_empty())
        {
            return Err(Error::RecoveryRequired(
                "unchanged TOAST outside new UPDATE tuple",
            ));
        }
        let old_row = before.map(|(r, _)| r);
        let key_row = old_row
            .as_ref()
            .or(after.as_ref())
            .ok_or(Error::RecoveryRequired("missing row image"))?;
        let key = relation
            .columns
            .iter()
            .flatten()
            .filter(|c| c.key)
            .map(|c| {
                key_row
                    .get(&c.name)
                    .cloned()
                    .map(|v| (c.name.clone(), v))
                    .ok_or(Error::RecoveryRequired("missing replica identity value"))
            })
            .collect::<Result<Row>>()?;
        // A replica-identity key tuple is not a complete before image: its
        // non-key NULL placeholders must never be mistaken for previous NULLs.
        let before = if matches!(old, Some(OldTuple::Full(_))) {
            old_row
        } else {
            None
        };
        Ok(vec![MappedEvent {
            schema_version: self.version(id),
            kind,
            table: Some(relation.table.clone()),
            key,
            before,
            after,
            unchanged_cols,
        }])
    }
    fn get(&self, id: u32) -> Result<&CachedRelation> {
        self.relations
            .get(&id)
            .ok_or(Error::RecoveryRequired("unknown relation ID"))
    }
}

fn row(
    relation: &CachedRelation,
    tuple: &Tuple<'_>,
    only_keys: bool,
) -> Result<(Row, BTreeSet<String>)> {
    if tuple.len() != relation.columns.len() {
        return Err(Error::RecoveryRequired(
            "tuple/relation column count mismatch",
        ));
    }
    let mut row = Row::new();
    let mut unchanged = BTreeSet::new();
    for (column, field) in relation.columns.iter().zip(tuple) {
        let Some(column) = column else { continue };
        if only_keys && !column.key {
            continue;
        }
        let text = match field {
            Column::Null => None,
            Column::Text(bytes) => Some(*bytes),
            Column::UnchangedToast => {
                unchanged.insert(column.name.clone());
                continue;
            }
            Column::Binary(_) => {
                return Err(Error::RecoveryRequired("unexpected binary pgoutput column"));
            }
        };
        let parsed = Value::parse_pg_text(&column.value.pg_type, text)?;
        let value = column.value.mapping.forward(&parsed)?;
        row.insert(
            column.name.clone(),
            OpaqueValue {
                type_id: column.type_id.clone(),
                bytes: if value == Value::Null {
                    None
                } else {
                    Some(value.canonical_bytes()?)
                },
            },
        );
    }
    Ok((row, unchanged))
}
