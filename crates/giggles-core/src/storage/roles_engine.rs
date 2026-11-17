//! Appended role catalog kinds. Existing catalog kinds and payloads stay intact.
//! Keys: 8/name\0, 9/role\0member\0, 10/table\0,
//! 11/table\0role\0, 12/schema\0role\0, 13 (next OID).
//! A table identity is either a public bare name or schema\0name. Qualified
//! kinds 10/11 have an additional 0 byte immediately after the kind, keeping
//! them prefix-free relative to bare-table keys without changing bare bytes.
//! Values: version byte 1 followed by the legacy value (JSON role/grant arrays,
//! UTF-8 owner, big-endian u32 next OID); membership has no payload.
use super::*;
use crate::storage::{
    engine::{
        catalog::CatalogParts,
        catalog_codec::CatalogKind,
        kv::{Direction, Keyspace},
        locks::{LockKey, LockMode},
        txn::{Isolation, SerializationReason, Transaction, TxnError, TxnServices},
    },
    v2::V2Db,
};
use std::{ops::Bound, time::Instant};

pub(crate) const FEATURE: u64 = 1 << 11;
const ROLE_LOCK: u64 = u64::MAX - 2;
const PREFIXES: [&[u8]; 6] = [
    b"role/", b"member/", b"owner/", b"tgrant/", b"sgrant/", NEXT_OID,
];

fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(message.into())
}

pub(crate) fn classify_key(key: &[u8]) -> Result<CatalogKind, CoreError> {
    let kind = *key.first().ok_or_else(|| corrupt("empty role key"))?;
    if kind == 13 {
        return if key.len() == 1 {
            Ok(CatalogKind::RoleOid)
        } else {
            Err(corrupt("invalid role OID key"))
        };
    }
    if !(8..=12).contains(&kind) || key.last() != Some(&0) {
        return Err(corrupt("invalid role catalog key"));
    }
    let qualified = matches!(kind, 10 | 11) && key[1] == 0;
    let start = if qualified { 2 } else { 1 };
    let body = key
        .get(start..key.len() - 1)
        .ok_or_else(|| corrupt("short qualified role key"))?;
    let name = std::str::from_utf8(body).map_err(|_| corrupt("invalid role catalog UTF-8"))?;
    let relation = |name: &str| {
        if name.contains('\0') != qualified {
            return Err(RoleError::Invalid);
        }
        validate_relation_identity(name)
    };
    let valid = match kind {
        10 => relation(name),
        11 => name
            .rsplit_once('\0')
            .ok_or(RoleError::Invalid)
            .and_then(|(table, role)| {
                relation(table)?;
                validate_name(role)
            }),
        9 | 12 => name
            .split_once('\0')
            .ok_or(RoleError::Invalid)
            .and_then(|(object, role)| {
                validate_name(object)?;
                validate_name(role)
            }),
        _ => validate_name(name),
    };
    valid.map_err(|_| corrupt("invalid role catalog names"))?;
    Ok(match kind {
        8 => CatalogKind::Role,
        9 => CatalogKind::RoleMembership,
        10 => CatalogKind::TableOwner,
        11 => CatalogKind::TableGrant,
        12 => CatalogKind::SchemaGrant,
        _ => unreachable!(),
    })
}

pub(crate) fn validate_record(key: &[u8], value: &[u8]) -> Result<(), CoreError> {
    let kind = classify_key(key)?;
    let payload = value
        .strip_prefix(&[1])
        .ok_or_else(|| corrupt("role record version"))?;
    match kind {
        CatalogKind::Role => {
            let role: CatalogRole =
                serde_json::from_slice(payload).map_err(|e| CoreError::Corrupt(e.to_string()))?;
            if role.name.as_bytes() != &key[1..key.len() - 1]
                || role.oid < FIRST_ROLE_OID
                || role.generation == 0
                || validate_verifier(role.password.as_deref()).is_err()
            {
                return Err(corrupt("invalid catalog role"));
            }
        }
        CatalogKind::RoleMembership if payload.is_empty() => {}
        CatalogKind::TableOwner => {
            let name =
                std::str::from_utf8(payload).map_err(|e| CoreError::Corrupt(e.to_string()))?;
            validate_name(name).map_err(|_| corrupt("invalid catalog owner"))?;
        }
        CatalogKind::TableGrant => {
            let _: BTreeSet<TablePrivilege> =
                serde_json::from_slice(payload).map_err(|e| CoreError::Corrupt(e.to_string()))?;
        }
        CatalogKind::SchemaGrant => {
            let _: BTreeSet<SchemaPrivilege> =
                serde_json::from_slice(payload).map_err(|e| CoreError::Corrupt(e.to_string()))?;
        }
        CatalogKind::RoleOid if payload.len() == 4 => {
            if u32::from_be_bytes(payload.try_into().unwrap()) < FIRST_ROLE_OID {
                return Err(corrupt("invalid next role OID"));
            }
        }
        _ => return Err(corrupt("invalid role catalog payload")),
    }
    Ok(())
}

/// Whether a validated role record carries the task-only attribute, which
/// additionally requires the TASK_CATALOG feature bit.
pub(crate) fn is_task_only_record(key: &[u8], value: &[u8]) -> bool {
    key.first() == Some(&8)
        && value
            .strip_prefix(&[1])
            .and_then(|payload| serde_json::from_slice::<CatalogRole>(payload).ok())
            .is_some_and(|role| role.task_only)
}

pub(super) fn encode_key(raw: &[u8]) -> Result<Vec<u8>, CoreError> {
    for (index, prefix) in PREFIXES.iter().enumerate() {
        if let Some(name) = raw.strip_prefix(*prefix) {
            let mut key = vec![8 + index as u8];
            if (index == 2 && name.contains(&0))
                || (index == 3 && name.iter().filter(|byte| **byte == 0).count() > 1)
            {
                key.push(0);
            }
            key.extend(name);
            if index != 5 {
                key.push(0);
            }
            classify_key(&key)?;
            return Ok(key);
        }
    }
    Err(corrupt("unknown role image key"))
}
/// The role image at `txn`'s snapshot, through tracked catalog reads (the
/// protected commit's in-transaction privilege recheck, GD-0061 R3-615).
pub(crate) fn load_txn(txn: &mut Transaction) -> Result<RoleTable, RoleError> {
    let mut image = RoleTable::default();
    for entry in txn
        .scan(
            Keyspace::Catalog,
            Bound::Included(&[8]),
            Bound::Excluded(&[14]),
            Direction::Forward,
            None,
        )
        .map_err(CoreError::from)?
    {
        let (key, value) = entry.map_err(CoreError::from)?;
        validate_record(&key, &value)?;
        let index = (key[0] - 8) as usize;
        let mut raw = PREFIXES[index].to_vec();
        if index != 5 {
            let start = if matches!(index, 2 | 3) && key[1] == 0 {
                2
            } else {
                1
            };
            raw.extend(&key[start..key.len() - 1]);
        }
        image.0.insert(raw, value[1..].to_vec());
    }
    Ok(image)
}
pub(super) fn image(db: &V2Db) -> Result<std::sync::Arc<super::RoleImage>, RoleError> {
    image_txn(db.catalog(), &mut db.begin(Isolation::ReadCommitted))
}

pub(super) fn image_txn(
    catalog: &crate::storage::engine::catalog::Catalog,
    txn: &mut Transaction,
) -> Result<std::sync::Arc<super::RoleImage>, RoleError> {
    txn.snapshot_ts();
    if let Some(error) = txn.snapshot_error() {
        return Err(CoreError::from(error.clone()).into());
    }
    if txn.catalog_writes().next().is_none() {
        let (snapshot, version) = catalog.binding_snapshot(txn)?;
        if version.is_some() {
            // V is the latest committed catalog version <= this transaction's T.
            // Every role write publishes a catalog version, so no role write can
            // commit in (V, T]. load_txn(T) therefore builds exactly V's image.
            return snapshot.roles(|| load_txn(txn));
        }
    }
    // Private catalog writes and old-snapshot fallbacks have no shared image.
    Ok(std::sync::Arc::new(super::RoleImage::new(load_txn(txn)?)))
}
fn lock(
    txn: &mut Transaction,
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    txn.acquire_lock(
        LockKey::Table(ROLE_LOCK),
        LockMode::AccessExclusive,
        deadline,
        cancel,
    )?;
    // The lock above is this gateway's only; across gateways role writers serialize on an
    // exclusive range sentinel (GD-0075 P7-1).
    if matches!(txn.services(), TxnServices::Cluster(_)) {
        txn.lock_sql_sentinel(ROLE_LOCK, LockMode::Exclusive, cancel)?;
    }
    Ok(())
}
/// Shared role lock for writers (tasks) that read roles but must not race a
/// role writer, which holds the lock in AccessExclusive mode.
pub(crate) fn lock_shared(
    txn: &mut Transaction,
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    txn.acquire_lock(LockKey::Table(ROLE_LOCK), LockMode::AccessShare, deadline, cancel)?;
    Ok(())
}
fn stage(txn: &mut Transaction, before: &RoleTable, after: &RoleTable) -> Result<(), RoleError> {
    super::task_only_invariants(after)?;
    for raw in before.0.keys().filter(|raw| !after.0.contains_key(*raw)) {
        txn.delete(Keyspace::Catalog, encode_key(raw)?)
            .map_err(CoreError::from)?;
    }
    for (raw, payload) in &after.0 {
        if before.0.get(raw) == Some(payload) {
            continue;
        }
        let key = encode_key(raw)?;
        let value = [vec![1], payload.clone()].concat();
        validate_record(&key, &value)?;
        txn.require_features(FEATURE);
        if is_task_only_record(&key, &value) {
            txn.require_features(crate::storage::engine::meta::TASK_CATALOG);
        }
        txn.put(Keyspace::Catalog, key, value)
            .map_err(CoreError::from)?;
    }
    Ok(())
}
pub(super) fn write<T>(
    db: &V2Db,
    check: impl FnOnce() -> Result<(), RoleError>,
    change: impl FnOnce(&mut RoleTable) -> Result<T, RoleError>,
) -> Result<T, RoleError> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    lock(&mut txn, None, &|| false)?;
    // ReadCommitted captures a snapshot at begin; refresh only this standalone
    // administrative transaction after waiting for the preceding role writer.
    txn.begin_statement();
    check()?;
    let before = load_txn(&mut txn)?;
    let mut after = before.clone();
    let result = change(&mut after)?;
    crate::task::catalog::check_role_change(&mut txn, &before, &after)?;
    stage(&mut txn, &before, &after)?;
    db.catalog().commit_ddl(txn).map_err(CoreError::from)?;
    Ok(result)
}

/// Keep table authority in the same DDL commit as renames, drops and name reuse.
pub(crate) fn reconcile_tables(
    txn: &mut Transaction,
    kv: &dyn crate::storage::engine::kv::KvRead,
    old: &CatalogParts,
    new: &CatalogParts,
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    let old_objects = authority_objects(old);
    let new_objects = authority_objects(new);
    if old_objects == new_objects {
        return Ok(());
    }
    let new_names: BTreeMap<_, _> = new_objects.iter().map(|(name, id)| (*id, name)).collect();
    let renames: Vec<_> = old_objects
        .iter()
        .filter_map(|(name, id)| {
            new_names
                .get(id)
                .filter(|next| **next != name)
                .map(|next| (name.clone(), (*next).clone()))
        })
        .collect();
    let mut clear: BTreeSet<_> = old_objects
        .keys()
        .chain(new_objects.keys())
        .filter(|name| old_objects.get(*name) != new_objects.get(*name))
        .cloned()
        .collect();
    clear.extend(renames.iter().map(|(_, new)| new.clone()));
    if clear.is_empty() {
        return Ok(());
    }
    lock(txn, deadline, cancel)?;
    // Never refresh the SQL transaction's snapshot halfway through DDL. A role
    // mutation since that snapshot may have introduced grants we cannot see.
    let snapshot = txn.snapshot_ts();
    for entry in kv.range(
        Keyspace::Catalog,
        Bound::Included(&[8]),
        Bound::Excluded(&[14]),
        Direction::Forward,
    )? {
        let (key, _) = entry?;
        let (_, ts) =
            crate::storage::engine::codec::split_versioned(&key).map_err(CoreError::from)?;
        if ts > snapshot {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict).into());
        }
    }
    let operation = || -> Result<(), RoleError> {
        let before = load_txn(txn)?;
        let mut after = before.clone();
        after.0.retain(|key, _| {
            let object = key.strip_prefix(b"owner/").or_else(|| {
                key.strip_prefix(b"tgrant/")
                    .map(|pair| pair.rsplitn(2, |b| *b == 0).nth(1).unwrap_or(pair))
            });
            !object
                .and_then(|name| std::str::from_utf8(name).ok())
                .is_some_and(|name| clear.contains(name))
        });
        for (old, new) in renames {
            for (key, value) in &before.0 {
                if *key == super::key(b"owner/", &old) {
                    after.0.insert(super::key(b"owner/", &new), value.clone());
                } else if let Some(role) =
                    key.strip_prefix(grant_key(b"tgrant/", &old, "").as_slice())
                {
                    let mut key = grant_key(b"tgrant/", &new, "");
                    key.extend(role);
                    after.0.insert(key, value.clone());
                }
            }
        }
        stage(txn, &before, &after)
    };
    operation().map_err(|error| match error {
        RoleError::Core(e) => e,
        _ => corrupt("invalid role image"),
    })
}

fn authority_objects(parts: &CatalogParts) -> BTreeMap<String, u64> {
    parts
        .tables
        .iter()
        .map(|(name, def)| (name.clone(), def.id))
        .chain(
            parts
                .ext
                .sequences
                .iter()
                .map(|(id, def)| (def.name.clone(), *id)),
        )
        .chain(parts.sql_views.iter().map(|(id, v)| {
            let schema = parts.ext.schemas.iter().find(|(_, s)| s.id == v.schema).map_or("public", |(n, _)| n.as_str());
            (crate::relational::relation_key(schema, &v.name), *id)
        }))
        .collect()
}

/// apply_diff already holds the role lock and has cleared obsolete authority.
/// Stage creator ownership in that same transaction before its implicit commit.
pub(crate) fn assign_created(
    txn: &mut Transaction,
    old: &CatalogParts,
    new: &CatalogParts,
    owner: Option<&str>,
) -> Result<(), CoreError> {
    let Some(owner) = owner else {
        return Ok(());
    };
    let old_ids: BTreeSet<_> = authority_objects(old).into_values().collect();
    let created: Vec<_> = authority_objects(new)
        .into_iter()
        .filter(|(_, id)| !old_ids.contains(id))
        .collect();
    if created.is_empty() {
        return Ok(());
    }
    let mut operation = || -> Result<(), RoleError> {
        let before = load_txn(txn)?;
        require_role(&before, owner)?;
        let mut after = before.clone();
        // Tables precede their dependent sequences when inheriting authority.
        for (name, id) in &created {
            if new.tables.values().any(|def| def.id == *id) || new.sql_views.contains_key(id) {
                after
                    .0
                    .insert(super::key(b"owner/", name), owner.as_bytes().to_vec());
            }
        }
        for (name, id) in &created {
            if let Some(sequence) = new.ext.sequences.get(id) {
                let inherited = sequence
                    .owned_by
                    .and_then(|(table_id, _)| {
                        let (table, _) = new.tables.iter().find(|(_, def)| def.id == table_id)?;
                        after.0.get(&super::key(b"owner/", table)).cloned()
                    })
                    .unwrap_or_else(|| owner.as_bytes().to_vec());
                after.0.insert(super::key(b"owner/", name), inherited);
            }
        }
        stage(txn, &before, &after)
    };
    operation().map_err(|error| match error {
        RoleError::Core(e) => e,
        _ => CoreError::Policy(crate::Error::Scope),
    })
}

pub(crate) fn own_owner(
    txn: &Transaction,
    table: &str,
) -> Result<Option<Option<String>>, CoreError> {
    txn.catalog_own(&encode_key(&super::key(b"owner/", table))?)
        .map(|value| {
            value
                .as_ref()
                .map(|v| {
                    String::from_utf8(v[1..].to_vec())
                        .map_err(|e| CoreError::Corrupt(e.to_string()))
                })
                .transpose()
        })
        .transpose()
}

pub(crate) fn own_grants(
    txn: &Transaction,
    table: &str,
    role: &str,
) -> Result<Option<BTreeSet<TablePrivilege>>, CoreError> {
    txn.catalog_own(&encode_key(&grant_key(b"tgrant/", table, role))?)
        .map(|value| {
            value.as_ref().map_or_else(
                || Ok(BTreeSet::new()),
                |v| serde_json::from_slice(&v[1..]).map_err(|e| CoreError::Corrupt(e.to_string())),
            )
        })
        .transpose()
}
