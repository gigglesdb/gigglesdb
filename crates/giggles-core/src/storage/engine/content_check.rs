//! Open-time witnesses for the monotone catalog and counter feature bits.
use super::{
    catalog_codec::{self, CatalogKind},
    codec,
    kv::{Direction, Keyspace, KvRead},
    meta::FormatError,
    mvcc,
};
use crate::task::records;
use std::{collections::BTreeMap, ops::Bound};

fn corrupt(error: impl std::fmt::Display) -> FormatError {
    FormatError::Corrupt(error.to_string())
}

/// Catalog keys tagged 0xF0.. are a cluster's own records in its system range (range directory,
/// gateways, DDL, transaction status, catalog generation; GD-0075 design.md Changes), never
/// catalog objects: they need no format bits, and catalog loads skip them. A single-node or R4
/// catalog load still refuses them (`classify_catalog_key`).
pub(crate) fn is_cluster_system_key(key: &[u8]) -> bool {
    key.first().is_some_and(|tag| *tag >= 0xF0)
}

/// New role kinds need their guard even when the physical value is a tombstone.
pub(crate) fn required_bits_for_catalog_key(key: &[u8]) -> Result<u64, FormatError> {
    if is_cluster_system_key(key) {
        Ok(0)
    } else if key.first().is_some_and(|kind| (8..=13).contains(kind)) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::super::roles::engine::FEATURE)
    } else if key.first().is_some_and(|kind| matches!(kind, 14 | 15)) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::SCHEMAS_AND_USER_TYPES)
    } else if key.first() == Some(&23) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::COLUMN_COLLATIONS)
    } else if key.first() == Some(&22) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::USER_COLLATIONS)
    } else if key.first() == Some(&16) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::SECONDARY_INDEXES)
    } else if key.first() == Some(&17) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::SQL_VIEWS)
    } else if key.first() == Some(&18) {
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::RELATION_COMMENTS)
    } else if key.first() == Some(&super::catalog_label_codec::LABEL_KIND) {
        // Tombstones and history too, like kind 19.
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        super::catalog_label_codec::required_bits(key).map_err(corrupt)
    } else if key.first().is_some_and(|kind| matches!(*kind, crate::task::catalog::TASK_KIND | crate::task::credential::CREDENTIAL_KIND)) {
        // Tombstones and history too: task catalog keys never hide from old binaries.
        catalog_codec::classify_catalog_key(key).map_err(corrupt)?;
        Ok(super::meta::TASK_CATALOG)
    } else {
        Ok(0)
    }
}

/// Required format bits for one live, unversioned catalog value.
pub(crate) fn required_bits_for_catalog_value(
    key: &[u8],
    value: &[u8],
) -> Result<u64, FormatError> {
    if is_cluster_system_key(key) {
        return Ok(0);
    }
    match catalog_codec::classify_catalog_key(key).map_err(corrupt)? {
        CatalogKind::Table => {
            let (entry, ext) = catalog_codec::decode_table(value).map_err(corrupt)?;
            let mut required = if ext.is_empty() { 0 } else { 64 };
            if entry.primary_index_birth.is_some() {
                required |= 64 | super::meta::KEY_DEPENDENCY_IDENTITY;
            }
            if ext.values().any(|column| column.typmod.is_some()) {
                required |= 128;
            }
            if catalog_codec::requires_text_backed_types(&entry.def) {
                required |= super::meta::TEXT_BACKED_COLUMN_TYPES;
            }
            if ext.values().any(|column| column.type_id.is_some()) || entry.def.name.contains('\0')
            {
                required |= super::meta::SCHEMAS_AND_USER_TYPES;
            }
            Ok(required)
        }
        CatalogKind::Constraint => {
            let extra = match value.first() {
                Some(2) => super::meta::UNIQUE_CONSTRAINT_IDENTITY | super::meta::SECONDARY_INDEXES,
                Some(3) => {
                    let target = catalog_codec::constraint_key_dependency_target(value).map_err(corrupt)?;
                    super::meta::KEY_DEPENDENCY_IDENTITY | if matches!(target,
                        crate::relational::catalog_ext::ReferencedKey::Secondary(_)) {
                        super::meta::SECONDARY_INDEXES
                    } else { 0 }
                }
                _ => 0,
            };
            Ok(64 | extra)
        }
        CatalogKind::ColumnCollation => {
            catalog_codec::decode_column_collation(value).map_err(corrupt)?;
            Ok(super::meta::COLUMN_COLLATIONS)
        }
        CatalogKind::Collation => {
            catalog_codec::decode_collation(value).map_err(corrupt)?;
            Ok(super::meta::USER_COLLATIONS)
        }
        CatalogKind::Sequence | CatalogKind::Type => Ok(64),
        // validate_meta treats a live kind-7 marker without bit 9 as Corrupt,
        // so the commit guard must refuse it too (DD-0068 X1).
        CatalogKind::RoleBootstrap => Ok(512),
        CatalogKind::Index => {
            super::indexes::decode_descriptor(value).map_err(corrupt)?;
            Ok(super::meta::SECONDARY_INDEXES)
        }
        CatalogKind::SqlView => {
            catalog_codec::sql_views::decode_view(value).map_err(corrupt)?;
            Ok(super::meta::SQL_VIEWS)
        }
        CatalogKind::RelationComment => {
            catalog_codec::decode_comment(value).map_err(corrupt)?;
            Ok(super::meta::RELATION_COMMENTS)
        }
        CatalogKind::TaskCredential => {
            crate::task::credential::decode_credential(value).map_err(corrupt)?;
            Ok(super::meta::TASK_CATALOG)
        }
        CatalogKind::Task => {
            crate::task::catalog::decode_task(key, value).map_err(corrupt)?;
            Ok(super::meta::TASK_CATALOG)
        }
        CatalogKind::ColumnLabel => {
            super::catalog_label_codec::decode_label(value).map_err(corrupt)?;
            super::catalog_label_codec::required_bits(key).map_err(corrupt)
        }
        CatalogKind::Name => Ok(if key[1..key.len() - 1].contains(&0) {
            super::meta::SCHEMAS_AND_USER_TYPES
        } else {
            0
        }),
        CatalogKind::Schema | CatalogKind::TypeDefinition => {
            Ok(super::meta::SCHEMAS_AND_USER_TYPES)
        }
        CatalogKind::Role
        | CatalogKind::RoleMembership
        | CatalogKind::TableOwner
        | CatalogKind::TableGrant
        | CatalogKind::SchemaGrant
        | CatalogKind::RoleOid => {
            super::super::roles::engine::validate_record(key, value).map_err(corrupt)?;
            Ok(super::super::roles::engine::FEATURE
                | if super::super::roles::engine::is_task_only_record(key, value) {
                    super::meta::TASK_CATALOG
                } else {
                    0
                })
        }
    }
}

/// True for Kernel user keys in the protected task namespaces `0x20..=0x2B`.
pub(crate) fn is_protected_kernel_key(key: &[u8]) -> bool {
    key.first()
        .is_some_and(|ns| (records::NS_MIN..=records::NS_MAX).contains(ns))
}

/// Commit gate for one Kernel write (GD-0061 D-B). Keys outside `0x20..=0x2B`
/// need no bit (legacy behaviour). A protected key needs PROTECTED_RECORDS, a
/// classifiable key and, for a live value, a value that decodes completely; a
/// replicated database refuses protected records outright.
pub(crate) fn required_bits_for_kernel_write(
    key: &[u8],
    value: Option<&[u8]>,
    next_features: u64,
) -> Result<u64, FormatError> {
    if !is_protected_kernel_key(key) {
        return Ok(0);
    }
    if next_features & super::meta::REPLICATED != 0 {
        return Err(FormatError::Unsupported(
            "protected records in a replicated database".into(),
        ));
    }
    let namespace = records::classify_key(key).map_err(corrupt)?;
    let mut bits = super::meta::PROTECTED_RECORDS;
    if let Some(value) = value {
        records::validate(key, value).map_err(corrupt)?;
        if namespace == records::Namespace::Outcome {
            bits |= once_payload_bits(value)?;
        }
    }
    Ok(bits)
}

/// The bits a native `once` receipt in a kind-1 `22` record needs, matching
/// what `native_content` asks of the same receipt in `[07]` (GD-0061 R3-625):
/// a valid GDOU payload, NATIVE_OUTCOMES, and WIDE_ONCE_RECEIPTS for a
/// version 2 receipt. (The typed/temporal rule compares the receipt with the
/// kernel metadata format, which this per-key gate does not see.) A blanked
/// payload and task outcomes need nothing more.
fn once_payload_bits(value: &[u8]) -> Result<u64, FormatError> {
    let record = records::OutcomeRecord::decode(value).map_err(corrupt)?;
    let records::OutcomePayload::Once(payload) = &record.payload else {
        return Ok(0);
    };
    if payload.is_empty() {
        return Ok(0);
    }
    use crate::operations::once::outcome;
    outcome::validate(payload).map_err(corrupt)?;
    let mut bits = super::meta::NATIVE_OUTCOMES;
    if outcome::is_wide(payload) {
        bits |= super::meta::WIDE_ONCE_RECEIPTS;
    }
    Ok(bits)
}

/// Raw Kernel entries in `0x20..0x2C` the open check inspects; full decoding
/// happens at read time.
pub(crate) const PROTECTED_OPEN_SCAN_LIMIT: usize = 4096;

/// Constant-cost open check of the protected Kernel namespaces: at most
/// [`PROTECTED_OPEN_SCAN_LIMIT`] entries plus one seek per unassigned range.
fn check_protected_kernel(kv: &dyn KvRead, features: u64) -> Result<(), FormatError> {
    if features & super::meta::PROTECTED_RECORDS != 0 && features & super::meta::REPLICATED != 0 {
        return Err(FormatError::Unsupported(
            "protected records in a replicated database".into(),
        ));
    }
    for item in kv
        .range(
            Keyspace::Kernel,
            Bound::Included(&[records::NS_MIN]),
            Bound::Excluded(&[records::NS_MAX + 1]),
            Direction::Forward,
        )
        .map_err(corrupt)?
        .take(PROTECTED_OPEN_SCAN_LIMIT)
    {
        let (key, value) = item.map_err(corrupt)?;
        if features & super::meta::PROTECTED_RECORDS == 0 {
            return Err(corrupt("content requires unset format feature"));
        }
        let (user, _) = codec::split_versioned(&key)
            .map_err(|error| FormatError::Corrupt(format!("{error:?}")))?;
        records::classify_key(user).map_err(corrupt)?;
        if let Some(value) = mvcc::decode(&value).map_err(corrupt)?
            && value.first() != Some(&records::VERSION)
        {
            return Err(corrupt("protected record version"));
        }
    }
    // Namespaces nobody assigns: one seek each, any entry is corrupt.
    // native_content::check owns the legacy namespaces below and the stamp above.
    use super::native_content::{LEGACY_KERNEL_END, STAMP_NAMESPACE};
    for (start, end) in [
        (LEGACY_KERNEL_END, records::NS_MIN),
        (records::NS_MAX + 1, STAMP_NAMESPACE),
    ] {
        let mut gap = kv
            .range(
                Keyspace::Kernel,
                Bound::Included(&[start]),
                Bound::Excluded(&[end]),
                Direction::Forward,
            )
            .map_err(corrupt)?;
        if gap.next().transpose().map_err(corrupt)?.is_some() {
            return Err(corrupt("unknown kernel namespace"));
        }
    }
    Ok(())
}

pub(crate) fn check(kv: &dyn KvRead, features: u64) -> Result<Vec<Vec<u8>>, FormatError> {
    check_protected_kernel(kv, features)?;
    let mut stale = Vec::new();
    let mut previous = None;
    let mut tables = BTreeMap::new();
    let mut sequences = BTreeMap::new();
    let mut constraints = Vec::new();
    let mut comments = BTreeMap::new();
    let mut views = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    let mut column_collations=BTreeMap::new();
    let mut collations=BTreeMap::new();
    let mut indexes=BTreeMap::new();
    let mut tasks = Vec::new();
    let mut credentials = BTreeMap::new();
    let mut labels = BTreeMap::new();
    let mut required = 0;
    for item in kv
        .range(
            Keyspace::Catalog,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .map_err(corrupt)?
    {
        let (key, value) = item.map_err(corrupt)?;
        let (user, _) = codec::split_versioned(&key)
            .map_err(|error| FormatError::Corrupt(format!("{error:?}")))?;
        // Classify even historical/tombstoned keys: unknown kinds cannot hide
        // behind a tombstone or behind a newer, valid physical version.
        let kind = catalog_codec::classify_catalog_key(user).map_err(corrupt)?;
        required |= required_bits_for_catalog_key(user)?;
        if previous.as_deref() == Some(user) {
            continue;
        }
        previous = Some(user.to_vec());
        let Some(value) = mvcc::decode(&value).map_err(corrupt)? else {
            continue;
        };
        required |= required_bits_for_catalog_value(user, value)?;
        match kind {
            CatalogKind::SqlView => {
                views.insert(u64::from_be_bytes(user[1..9].try_into().unwrap()), catalog_codec::sql_views::decode_view(value).map_err(corrupt)?);
            }
            CatalogKind::RelationComment => {
                comments.insert(
                    (
                        u64::from_be_bytes(user[1..9].try_into().unwrap()),
                        u64::from_be_bytes(user[9..17].try_into().unwrap()),
                    ),
                    catalog_codec::decode_comment(value).map_err(corrupt)?,
                );
            }
            CatalogKind::Table => {
                let (entry, _) = catalog_codec::decode_table(value).map_err(corrupt)?;
                tables.insert(
                    u64::from_be_bytes(user[1..9].try_into().unwrap()),
                    entry.def,
                );
            }
            CatalogKind::Name => {
                let _: [u8; 8] = value
                    .try_into()
                    .map_err(|_| corrupt("invalid catalog name id"))?;
            }
            CatalogKind::Constraint => {
                constraints.push((
                    u64::from_be_bytes(user[1..9].try_into().unwrap()),
                    value.to_vec(),
                ));
            }
            CatalogKind::Sequence => {
                sequences.insert(
                    u64::from_be_bytes(user[1..9].try_into().unwrap()),
                    catalog_codec::decode_sequence(value).map_err(corrupt)?,
                );
            }
            CatalogKind::Type | CatalogKind::TypeDefinition => {
                catalog_codec::decode_type(value)
                    .map(|_| ())
                    .map_err(corrupt)?;
            }
            CatalogKind::ColumnCollation => {column_collations.insert(u64::from_be_bytes(user[1..9].try_into().unwrap()),catalog_codec::decode_column_collation(value).map_err(corrupt)?);}
            CatalogKind::Collation => {collations.insert(u64::from_be_bytes(user[1..9].try_into().unwrap()),catalog_codec::decode_collation(value).map_err(corrupt)?);}
            CatalogKind::Index => {
                indexes.insert(u64::from_be_bytes(user[1..9].try_into().unwrap()),super::indexes::decode_descriptor(value).map_err(corrupt)?);
            }
            CatalogKind::TaskCredential => {
                let (task, id) = crate::task::credential::decode_credential_key(user).map_err(corrupt)?;
                if credentials.insert(id, task).is_some() {
                    return Err(corrupt("duplicate credential id"));
                }
            }
            CatalogKind::Task => {
                tasks.push(crate::task::catalog::decode_task(user, value).map_err(corrupt)?);
            }
            CatalogKind::ColumnLabel => {
                let owner = super::catalog_label_codec::decode_label_key(user).map_err(corrupt)?;
                labels.insert(owner, super::catalog_label_codec::decode_label(value).map_err(corrupt)?);
            }
            CatalogKind::Schema => {
                schemas.insert(String::from_utf8(user[1..user.len()-1].to_vec()).map_err(corrupt)?, catalog_codec::decode_schema(value).map_err(corrupt)?);
            }
            // The role-bootstrap gate and its existing opaque payload fixture
            // are owned by validate_meta.
            CatalogKind::RoleBootstrap
            | CatalogKind::Role
            | CatalogKind::RoleMembership
            | CatalogKind::TableOwner
            | CatalogKind::TableGrant
            | CatalogKind::SchemaGrant
            | CatalogKind::RoleOid => {}
        }
    }
    for t in tables.values().filter(|t|t.logical_columns().next().is_none()) {
        if !matches!(column_collations.get(&t.id),Some(crate::relational::column_collations::Binding::EmptyTable)) {return Err(corrupt("empty table without collation cascade witness"));}
    }
    for (id,b) in &column_collations {
        use crate::relational::column_collations::{Binding,Reference};
        let refs=match b {
            Binding::EmptyTable=>{
                let t=tables.get(id).ok_or_else(||corrupt("empty table witness owner absent"))?;
                if t.hidden_rowid().is_none() || t.logical_columns().next().is_some() {return Err(corrupt("invalid empty table witness"));}
                vec![]
            },
            Binding::Column(r)=>{
                let c=tables.values().flat_map(|t|&t.columns).find(|c|c.id==*id).ok_or_else(||corrupt("column collation owner absent"))?;
                if !matches!(c.data_type,crate::relational::DataType::Text|crate::relational::DataType::Varchar|crate::relational::DataType::Bpchar) {return Err(corrupt("noncollatable column"));}
                vec![r]
            },
            Binding::Index(rs)=>{
                let d=indexes.get(id).ok_or_else(||corrupt("index collation owner absent"))?;
                if rs.len()!=d.columns.len() {return Err(corrupt("index collation arity"));}
                rs.iter().flatten().collect()
            }
        };
        if refs.iter().any(|r|matches!(r,Reference::User(id) if !collations.contains_key(id))) {return Err(corrupt("collation dependency absent"));}
    }
    for (id, value) in constraints {
        let table = tables
            .get(&id)
            .ok_or_else(|| corrupt("constraint table is absent"))?;
        catalog_codec::decode_constraint(&value, table).map_err(corrupt)?;
    }
    catalog_codec::validate_comments(&comments, tables.values(), &views).map_err(corrupt)?;
    super::catalog_label_codec::validate_labels(&labels, tables.values()).map_err(corrupt)?;
    // Task cross rules: names are unique, and a live task's table exists.
    // Revoked tasks may outlive their table.
    let task_ids: std::collections::BTreeSet<_> = tasks.iter().map(|task| task.id.0).collect();
    if credentials.values().any(|task| !task_ids.contains(task)) {
        return Err(corrupt("credential task absent"));
    }
    let mut task_names = std::collections::BTreeSet::new();
    for task in &tasks {
        if !task_names.insert(task.name.as_str()) {
            return Err(corrupt("duplicate task name"));
        }
        if !task.revoked && !tables.contains_key(&task.table) {
            return Err(corrupt("live task table is absent"));
        }
    }
    let mut parts = super::catalog::CatalogParts { sql_views: views, ..Default::default() };
    parts.tables = tables.into_iter().map(|(id, t)| (id.to_string(), t)).collect();
    parts.ext.schemas = schemas;
    catalog_codec::sql_views::validate_catalog(&parts).map_err(corrupt)?;
    for item in kv
        .range(
            Keyspace::Seq,
            Bound::Included(&[2]),
            Bound::Excluded(&[3]),
            Direction::Forward,
        )
        .map_err(corrupt)?
    {
        let (key, value) = item.map_err(corrupt)?;
        if !super::commit::valid_counter_key(&key) {
            return Err(corrupt("invalid sequence counter key"));
        }
        required |= 256;
        let id = u64::from_be_bytes(key[1..9].try_into().unwrap());
        let definition = sequences
            .get(&id)
            .ok_or_else(|| corrupt("counter sequence is absent"))?;
        if u128::from_be_bytes(key[9..25].try_into().unwrap()) != definition.incarnation {
            stale.push(key);
            continue;
        }
        catalog_codec::decode_counter(&key, &value, definition).map_err(corrupt)?;
    }
    if required & !features != 0 {
        return Err(corrupt("content requires unset format feature"));
    }
    Ok(stale)
}

#[cfg(test)]
#[path = "content_check_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "protected_gate_tests.rs"]
mod protected_gate_tests;
