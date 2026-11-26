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
                if role.is_empty() { Ok(()) } else { validate_name(role) }
            }),
        9 => {
            let parts: Vec<_> = name.split('\0').collect();
            if !(2..=3).contains(&parts.len()) {
                Err(RoleError::Invalid)
            } else {
                parts.into_iter().try_for_each(validate_name)
            }
        }
        12 => name
            .split_once('\0')
            .ok_or(RoleError::Invalid)
            .and_then(|(object, role)| {
                validate_name(object)?;
                if role.is_empty() { Ok(()) } else { validate_name(role) }
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

/// Validate a role record and return feature bits required by its grant payload.
pub(crate) fn validate_record(key: &[u8], value: &[u8]) -> Result<u64, CoreError> {
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
        CatalogKind::RoleMembership => {
            let raw = [b"member/".as_slice(), &key[1..key.len() - 1]].concat();
            let mut table = RoleTable::default();
            table.0.insert(raw, payload.to_vec());
            membership_edges(&table).map_err(|e| corrupt(&e.to_string()))?;
        }
        CatalogKind::TableOwner => {
            let name =
                std::str::from_utf8(payload).map_err(|e| CoreError::Corrupt(e.to_string()))?;
            validate_name(name).map_err(|_| corrupt("invalid catalog owner"))?;
        }
        CatalogKind::TableGrant => {
            let privileges: BTreeSet<TablePrivilege> =
                serde_json::from_slice(payload).map_err(|e| CoreError::Corrupt(e.to_string()))?;
            if privileges.contains(&TablePrivilege::Trigger) {
                return Ok(crate::storage::engine::meta::USER_ROUTINES);
            }
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
    Ok(0)
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

pub(crate) fn membership_features(key: &[u8], value: &[u8]) -> u64 {
    let Some(payload) = value.strip_prefix(&[1]) else {
        return 0;
    };
    let raw = match key.first() {
        Some(8) => [b"role/".as_slice(), &key[1..key.len() - 1]].concat(),
        Some(9) => [b"member/".as_slice(), &key[1..key.len() - 1]].concat(),
        Some(11) => [b"tgrant/".as_slice(), &key[if key[1] == 0 { 2 } else { 1 }..key.len() - 1]].concat(),
        Some(12) => [b"sgrant/".as_slice(), &key[1..key.len() - 1]].concat(),
        _ => return 0,
    };
    if extended_record(&raw, payload) {
        crate::storage::engine::meta::ROLE_MEMBERSHIP
    } else {
        0
    }
}

/// Catalog kinds 8..=13 for the role image. GD-0084 Q40: kinds 8, 10, 11, 12
/// and 13 are prefix-free (NUL-free names, a leading 0 marks qualified
/// relations, and every key ends with one NUL); kind 9 is not (see
/// `roles_membership::membership_key`).
pub(crate) fn encode_key(raw: &[u8]) -> Result<Vec<u8>, CoreError> {
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
        apply_read_record(&mut image, &key, Some(&value))?;
    }
    // GD-0084 Q40: membership keys are not prefix-free (the bootstrap-grantor key
    // `9|role|00|member|00` is a proper prefix of the named-grantor key
    // `9|role|00|member|00|grantor|00`, and both can be live), so the TxnScan
    // overlay merge can re-emit a committed value that this transaction
    // overwrote or deleted. Reapplying the transaction's own role writes, which
    // are keyed like the image, makes it exact. A transaction without catalog
    // writes pays one seek into the overlay map; otherwise one pass over its
    // catalog writes.
    let own: Vec<_> = txn
        .catalog_writes()
        .filter(|(key, _)| matches!(key.first(), Some(8..=13)))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    for (key, value) in own {
        apply_read_record(&mut image, &key, value.as_deref())?;
    }
    Ok(image)
}
fn apply_read_record(image: &mut RoleTable, key: &[u8], value: Option<&[u8]>) -> Result<(), RoleError> {
    classify_key(key)?;
    let index = (key[0] - 8) as usize;
    let mut raw = PREFIXES[index].to_vec();
    if index != 5 {
        let start = if matches!(index, 2 | 3) && key[1] == 0 { 2 } else { 1 };
        raw.extend(&key[start..key.len() - 1]);
    }
    if let Some(value) = value {
        validate_record(key, value)?;
        image.0.insert(raw, value[1..].to_vec());
    } else {
        image.0.remove(&raw);
    }
    Ok(())
}

pub(super) fn image_with_catalog_writes(
    db: &V2Db,
    txn: &mut Transaction,
    private: Option<&Transaction>,
) -> Result<Arc<super::RoleImage>, RoleError> {
    if let Some(private) = private
        && private.catalog_writes().any(|(key, _)| matches!(key.first(), Some(8..=13)))
    {
        let mut table = load_txn(txn)?;
        for (key, value) in private.catalog_writes().filter(|(key, _)| matches!(key.first(), Some(8..=13))) {
            apply_read_record(&mut table, key, value.as_deref())?;
        }
        return Ok(Arc::new(super::RoleImage::new(table)));
    }
    image_txn(db.catalog(), txn)
}

pub(super) fn image(db: &V2Db) -> Result<std::sync::Arc<super::RoleImage>, RoleError> {
    // Pin the same fresh local timestamp as a READ COMMITTED transaction. Only
    // reuse an image already loaded for that committed catalog version: loading
    // through a later transaction could populate this version with newer roles.
    // Cluster gateways and cold/publication misses retain the routed txn path.
    if let Some((snapshot, _pin)) = db.catalog().current_local_metadata()?
        && let Some(image) = snapshot.cached_roles()
    {
        return Ok(image);
    }
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
    txn.acquire_lock(
        LockKey::Table(ROLE_LOCK),
        LockMode::AccessShare,
        deadline,
        cancel,
    )?;
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
        let grant_features = validate_record(&key, &value)?;
        let synthesized = synthesized_membership(after, raw);
        txn.require_features(FEATURE | grant_features | membership_features(&key, &value)
            | if synthesized { crate::storage::engine::meta::ROLE_MEMBERSHIP } else { 0 });
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

/// Keep authority in the same DDL commit as renames, drops, name reuse and sequence relinks.
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
    let relinked: Vec<_> = new.ext.sequences.iter().filter_map(|(id, next)| {
        let previous = old.ext.sequences.get(id)?;
        (next.owned_by != previous.owned_by).then_some((previous, next))
    }).collect();
    if old_objects == new_objects && relinked.is_empty() {
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
    if clear.is_empty() && relinked.is_empty() {
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
        for (previous, next) in relinked {
            let owner = match next.owned_by {
                Some((table, _)) => owner_by_table_id(new, table, &after),
                None => previous.owned_by.and_then(|(table, _)| owner_by_table_id(old, table, &before)),
            };
            let key = super::key(b"owner/", &next.name);
            if let Some(owner) = owner {
                after.0.insert(key, owner);
            } else {
                after.0.remove(&key);
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
            let schema = parts
                .ext
                .schemas
                .iter()
                .find(|(_, s)| s.id == v.schema)
                .map_or("public", |(n, _)| n.as_str());
            (crate::relational::relation_key(schema, &v.name), *id)
        }))
        .collect()
}

fn owner_by_table_id(parts: &CatalogParts, id: u64, roles: &RoleTable) -> Option<Vec<u8>> {
    let (name, _) = parts.tables.iter().find(|(_, table)| table.id == id)?;
    roles.0.get(&super::key(b"owner/", name)).cloned()
}

/// apply_diff already holds the role lock and has cleared obsolete authority.
/// Stage creator ownership in that same transaction before its implicit commit.
pub(crate) fn assign_created(
    txn: &mut Transaction,
    old: &CatalogParts,
    new: &CatalogParts,
    owner: Option<&str>,
) -> Result<(), CoreError> {
    let old_ids: BTreeSet<_> = authority_objects(old).into_values().collect();
    let created: Vec<_> = authority_objects(new)
        .into_iter()
        .filter(|(_, id)| !old_ids.contains(id)
            && (owner.is_some() || new.ext.sequences.get(id).is_some_and(|seq| seq.owned_by.is_some())))
        .collect();
    if created.is_empty() {
        return Ok(());
    }
    let mut operation = || -> Result<(), RoleError> {
        let before = load_txn(txn)?;
        let mut after = before.clone();
        // Tables precede their dependent sequences when inheriting authority.
        if let Some(owner) = owner {
            require_role(&before, owner)?;
            for (name, id) in &created {
                if !new.ext.sequences.contains_key(id) {
                    after.0.insert(super::key(b"owner/", name), owner.as_bytes().to_vec());
                }
            }
        }
        for (name, id) in &created {
            if let Some(sequence) = new.ext.sequences.get(id) {
                let effective_owner = match sequence.owned_by {
                    Some((table, _)) => owner_by_table_id(new, table, &after),
                    None => owner.map(|owner| owner.as_bytes().to_vec()),
                };
                if let Some(owner) = effective_owner {
                    after.0.insert(super::key(b"owner/", name), owner);
                }
            }
        }
        stage(txn, &before, &after)
    };
    operation().map_err(|error| match error {
        RoleError::Core(e) => e,
        _ => CoreError::Policy(crate::Error::Scope),
    })
}

/// The catalog key of `table`'s owner record.
pub(crate) fn owner_key(table: &str) -> Result<Vec<u8>, CoreError> {
    encode_key(&super::key(b"owner/", table))
}

/// Decode an owner record value as staged in a transaction (`None`: removed).
pub(crate) fn decode_owner(value: &Option<Vec<u8>>) -> Result<Option<String>, CoreError> {
    value
        .as_ref()
        .map(|v| {
            String::from_utf8(v[1..].to_vec())
                .map_err(|e| CoreError::Corrupt(e.to_string()))
        })
        .transpose()
}

pub(crate) fn own_owner(
    txn: &Transaction,
    table: &str,
) -> Result<Option<Option<String>>, CoreError> {
    txn.catalog_own(&owner_key(table)?).map(decode_owner).transpose()
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
