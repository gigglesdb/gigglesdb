//! Atomic persisted column identity bindings and private domain allocation.
use super::{DomainName, PolicyDocument, RouteDefault, Treatment};
use crate::{
    schema::{ColumnId, ColumnRef, Snapshot},
    state::{
        model::{self, RouteId, keys},
        schema::Table,
        store::{ReadTxn, Store},
    },
    value::ColumnType,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Supported token codec family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeFamily {
    /// Text codec.
    Text,
    /// Int4 codec.
    Int4,
    /// Int8 codec.
    Int8,
    /// UUID codec.
    Uuid,
    /// Date codec.
    Date,
    /// Timestamp codec.
    Timestamp,
    /// Timestamp with time zone codec.
    Timestamptz,
}
impl TypeFamily {
    /// Return the compatible token codec family, refusing unsupported types.
    pub fn of(ty: &ColumnType) -> Option<Self> {
        match ty {
            ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Bpchar(_) => Some(Self::Text),
            ColumnType::Int4 => Some(Self::Int4),
            ColumnType::Int8 => Some(Self::Int8),
            ColumnType::Uuid => Some(Self::Uuid),
            ColumnType::Date => Some(Self::Date),
            ColumnType::Timestamp => Some(Self::Timestamp),
            ColumnType::Timestamptz => Some(Self::Timestamptz),
            ColumnType::Int2
            | ColumnType::Bool
            | ColumnType::Numeric
            | ColumnType::Bytea
            | ColumnType::Json
            | ColumnType::Jsonb
            | ColumnType::Enum(_)
            | ColumnType::Array(_)
            | ColumnType::Other(_) => None,
        }
    }
}
/// Binding lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BindingStatus {
    /// Usable binding.
    Active,
    /// Binding requires policy repair.
    NeedsRepair,
    /// Identity is no longer present.
    Retired,
}
/// Persisted identity and its current domain membership.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingRecord {
    /// Captured stable identity.
    pub identity: ColumnId,
    /// Last observed column name.
    pub column: ColumnRef,
    /// Compatible codec family.
    pub family: Option<TypeFamily>,
    /// Allocated or declared token domain.
    pub domain: Option<DomainName>,
    /// Whether a link assigned this domain.
    #[serde(default)]
    pub linked: bool,
    /// Explicit Show declaration anchored to this identity.
    pub show_rule: Option<ColumnRef>,
    /// Current lifecycle state.
    pub status: BindingStatus,
    /// Initial allocation time.
    pub created_unix_ms: u64,
}
/// Route-local bindings indexed by identity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteBindings(BTreeMap<ColumnId, BindingRecord>);
impl RouteBindings {
    /// Mutable record access for unit tests in sibling modules.
    #[cfg(test)]
    pub(crate) fn get_mut(&mut self, identity: &ColumnId) -> Option<&mut BindingRecord> {
        self.0.get_mut(identity)
    }
    /// Look up a captured identity.
    pub fn get(&self, identity: &ColumnId) -> Option<&BindingRecord> {
        self.0.get(identity)
    }
    /// Iterate bindings in identity order.
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnId, &BindingRecord)> {
        self.0.iter()
    }
    /// Find records with the given last observed name.
    pub fn by_column<'a>(
        &'a self,
        column: &'a ColumnRef,
    ) -> impl Iterator<Item = &'a BindingRecord> {
        self.0
            .values()
            .filter(move |record| &record.column == column)
    }
}
/// Destination for an atomic domain move.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DomainTarget {
    /// A declared or retained existing name.
    Named(DomainName),
    /// One fresh name shared by this reconciliation group.
    Generated {
        /// Reconciliation-local group identifier.
        group: usize,
    },
    /// A fresh private allocation.
    FreshPrivate,
}
/// Atomic binding mutation request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BindingChange {
    /// Move an active identity into a linked or private domain.
    SetDomain {
        /// Stable identity.
        identity: ColumnId,
        /// New domain destination.
        target: DomainTarget,
        /// Whether this is a link membership.
        linked: bool,
    },
    /// Allocate a new identity binding.
    Create {
        /// Stable identity.
        identity: ColumnId,
        /// Last observed name.
        column: ColumnRef,
        /// Codec family.
        family: Option<TypeFamily>,
        /// Whether to allocate a private domain.
        want_domain: bool,
        /// Anchored Show declaration.
        show_rule: Option<ColumnRef>,
        /// Lifecycle state.
        status: BindingStatus,
    },
    /// Allocate a domain for an existing unallocated identity.
    AddDomain {
        /// Stable identity.
        identity: ColumnId,
    },
    /// Update mutable fields while preserving allocation.
    Update {
        /// Stable identity.
        identity: ColumnId,
        /// Last observed name.
        column: ColumnRef,
        /// Codec family.
        family: Option<TypeFamily>,
        /// Anchored Show declaration.
        show_rule: Option<ColumnRef>,
        /// Lifecycle state.
        status: BindingStatus,
    },
}
/// Data-free binding operation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BindingError {
    /// A declared domain name is already allocated in persistent state.
    #[error("declared domain name collides with an allocated name")]
    DeclaredNameCollision,
    /// Storage or entropy unavailable.
    #[error("binding store unavailable")]
    Store,
    /// Stored binding is malformed.
    #[error("binding record is corrupt")]
    Corrupt,
    /// Allocation retry limit exceeded.
    #[error("private domain namespace exhausted")]
    NameSpaceExhausted,
    /// Mutation references an absent or already allocated identity.
    #[error("binding identity is unknown or already allocated")]
    UnknownIdentity,
}
/// Refuse declared domain names that collide with persisted generated names.
pub fn check_declared_names(store: &dyn Store, doc: &PolicyDocument) -> Result<(), BindingError> {
    let txn = store.read().map_err(|_| BindingError::Store)?;
    let names = doc
        .policy
        .domains
        .keys()
        .chain(
            doc.policy
                .columns
                .values()
                .filter_map(|treatment| match treatment {
                    Treatment::Blind { domain } => Some(domain),
                    Treatment::Show | Treatment::Masked { .. } => None,
                }),
        )
        .chain(doc.links.iter().filter_map(|link| link.domain.as_ref()));
    for name in names {
        if txn
            .get(Table::DomainNames, &keys::domain_name(name))
            .map_err(|_| BindingError::Store)?
            .is_some()
        {
            return Err(BindingError::DeclaredNameCollision);
        }
    }
    Ok(())
}

/// Generate a private name with 50 bits of operating-system entropy.
pub fn generate_domain_name() -> Result<DomainName, BindingError> {
    let mut bytes = [0; 10];
    getrandom::fill(&mut bytes).map_err(|_| BindingError::Store)?;
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut name = String::from("f");
    for byte in bytes {
        name.push(ALPHABET[usize::from(byte & 31)] as char);
    }
    Ok(DomainName(name))
}
fn read_bindings(txn: &dyn ReadTxn, route: &RouteId) -> Result<RouteBindings, BindingError> {
    let mut records = BTreeMap::new();
    for (key, value) in txn
        .scan_prefix(Table::DomainBindings, &keys::domain_binding_prefix(route))
        .map_err(|_| BindingError::Store)?
    {
        let record: BindingRecord = model::decode(&value).map_err(|_| BindingError::Corrupt)?;
        if key != keys::domain_binding(route, &record.identity)
            || records.insert(record.identity.clone(), record).is_some()
        {
            return Err(BindingError::Corrupt);
        }
    }
    Ok(RouteBindings(records))
}
/// Load all route bindings, refusing any malformed record.
pub fn load(store: &dyn Store, route: &RouteId) -> Result<RouteBindings, BindingError> {
    read_bindings(
        store.read().map_err(|_| BindingError::Store)?.as_ref(),
        route,
    )
}
/// Atomically persist changes and return the committed route bindings.
pub fn apply(
    store: &dyn Store,
    route: &RouteId,
    changes: &[BindingChange],
    reserved: &BTreeSet<DomainName>,
) -> Result<RouteBindings, BindingError> {
    apply_with_generator(store, route, changes, reserved, generate_domain_name)
}
fn apply_with_generator(
    store: &dyn Store,
    route: &RouteId,
    changes: &[BindingChange],
    reserved: &BTreeSet<DomainName>,
    mut generate: impl FnMut() -> Result<DomainName, BindingError>,
) -> Result<RouteBindings, BindingError> {
    if changes.is_empty() {
        return load(store, route);
    }
    let mut txn = store.write().map_err(|_| BindingError::Store)?;
    let mut groups: BTreeMap<usize, DomainName> = BTreeMap::new();
    let mut bindings = read_bindings(txn.as_ref(), route)?;
    for change in changes {
        let record = match change {
            BindingChange::SetDomain {
                identity,
                target,
                linked,
            } => {
                let mut record = bindings
                    .get(identity)
                    .filter(|r| r.status == BindingStatus::Active)
                    .ok_or(BindingError::UnknownIdentity)?
                    .clone();
                let domain = match target {
                    DomainTarget::Named(name) => name.clone(),
                    DomainTarget::FreshPrivate => {
                        allocate_domain(txn.as_mut(), route, identity, reserved, &mut generate)?
                    }
                    DomainTarget::Generated { group } => {
                        if let Some(name) = groups.get(group) {
                            name.clone()
                        } else {
                            let name = allocate_domain(
                                txn.as_mut(),
                                route,
                                identity,
                                reserved,
                                &mut generate,
                            )?;
                            groups.insert(*group, name.clone());
                            name
                        }
                    }
                };
                record.domain = Some(domain);
                record.linked = *linked;
                record
            }
            BindingChange::Create {
                identity,
                column,
                family,
                want_domain,
                show_rule,
                status,
            } => {
                if bindings.get(identity).is_some() {
                    return Err(BindingError::UnknownIdentity);
                }
                let domain = if *want_domain {
                    Some(allocate_domain(
                        txn.as_mut(),
                        route,
                        identity,
                        reserved,
                        &mut generate,
                    )?)
                } else {
                    None
                };
                BindingRecord {
                    identity: identity.clone(),
                    column: column.clone(),
                    family: *family,
                    domain,
                    linked: false,
                    show_rule: show_rule.clone(),
                    status: *status,
                    created_unix_ms: store.now_ms(),
                }
            }
            BindingChange::AddDomain { identity } => {
                let mut record = bindings
                    .get(identity)
                    .filter(|r| r.domain.is_none())
                    .ok_or(BindingError::UnknownIdentity)?
                    .clone();
                record.domain = Some(allocate_domain(
                    txn.as_mut(),
                    route,
                    identity,
                    reserved,
                    &mut generate,
                )?);
                record
            }
            BindingChange::Update {
                identity,
                column,
                family,
                show_rule,
                status,
            } => {
                let mut record = bindings
                    .get(identity)
                    .ok_or(BindingError::UnknownIdentity)?
                    .clone();
                record.column = column.clone();
                record.family = *family;
                record.show_rule = show_rule.clone();
                record.status = *status;
                record
            }
        };
        if let Some(old) = bindings.get(&record.identity)
            && (old.domain != record.domain
                || record.status == BindingStatus::Retired
                || (old.linked
                    && old.status == BindingStatus::Active
                    && record.status == BindingStatus::NeedsRepair))
                && let Some(name) = &old.domain
                    && txn
                        .get(Table::DomainNames, &keys::domain_name(name))
                        .map_err(|_| BindingError::Store)?
                        .is_some()
                    {
                        txn.put(
                            Table::RetiredDomains,
                            &keys::domain_name(name),
                            route.as_str().as_bytes(),
                        )
                        .map_err(|_| BindingError::Store)?;
                    }
        txn.put(
            Table::DomainBindings,
            &keys::domain_binding(route, &record.identity),
            &model::encode(&record),
        )
        .map_err(|_| BindingError::Store)?;
        bindings.0.insert(record.identity.clone(), record);
    }
    txn.commit().map_err(|_| BindingError::Store)?;
    load(store, route)
}

fn allocate_domain(
    txn: &mut dyn crate::state::store::WriteTxn,
    route: &RouteId,
    identity: &ColumnId,
    reserved: &BTreeSet<DomainName>,
    generate: &mut impl FnMut() -> Result<DomainName, BindingError>,
) -> Result<DomainName, BindingError> {
    for _ in 0..16 {
        let name = generate()?;
        if reserved.contains(&name)
            || txn
                .get(Table::DomainNames, &keys::domain_name(&name))
                .map_err(|_| BindingError::Store)?
                .is_some()
            || txn
                .get(Table::RetiredDomains, &keys::domain_name(&name))
                .map_err(|_| BindingError::Store)?
                .is_some()
        {
            continue;
        }
        txn.put(
            Table::DomainNames,
            &keys::domain_name(&name),
            &keys::domain_binding(route, identity),
        )
        .map_err(|_| BindingError::Store)?;
        return Ok(name);
    }
    Err(BindingError::NameSpaceExhausted)
}

/// Compute identity-ordered binding changes without mutating storage.
pub fn reconcile(
    bindings: &RouteBindings,
    doc: &PolicyDocument,
    snapshot: &Snapshot,
) -> Vec<BindingChange> {
    reconcile_with_names(bindings, doc, snapshot, None)
}

fn reconcile_with_names(
    bindings: &RouteBindings,
    doc: &PolicyDocument,
    snapshot: &Snapshot,
    generated: Option<&BTreeSet<DomainName>>,
) -> Vec<BindingChange> {
    let mut columns = BTreeMap::new();
    for relation in &snapshot.relations {
        for column in &relation.columns {
            let c = ColumnRef {
                schema: relation.schema.clone(),
                table: relation.name.clone(),
                column: column.name.clone(),
            };
            if let Some(id) = snapshot.column_id(&c) {
                columns.insert(id.clone(), (c, &column.ty));
            }
        }
    }
    let mut changes: BTreeMap<ColumnId, Vec<BindingChange>> = BTreeMap::new();
    let anchored = |c: &ColumnRef| {
        bindings
            .iter()
            .any(|(_, r)| r.show_rule.as_ref() == Some(c))
    };
    for (identity, (column, ty)) in &columns {
        let in_link = doc.links.iter().any(|link| link.fields.contains(column));
        if doc.route_default == RouteDefault::Show
            && !in_link
            && !bindings.get(identity).is_some_and(|r| r.linked)
        {
            continue;
        }
        let family = TypeFamily::of(ty);
        let show = matches!(doc.policy.columns.get(column), Some(Treatment::Show));
        let default = !doc.policy.columns.contains_key(column)
            && !doc.json_paths.iter().any(|r| r.column == *column);
        if let Some(record) = bindings.get(identity) {
            let mut updated = record.clone();
            updated.column = column.clone();
            if record.family != family && record.status != BindingStatus::Retired {
                updated.status = BindingStatus::NeedsRepair;
            }
            updated.family = family;
            if show && record.show_rule.is_none() && !anchored(column) {
                updated.show_rule = Some(column.clone());
            }
            if updated != *record {
                changes
                    .entry(identity.clone())
                    .or_default()
                    .push(BindingChange::Update {
                        identity: identity.clone(),
                        column: updated.column,
                        family: updated.family,
                        show_rule: updated.show_rule,
                        status: updated.status,
                    });
            }
            if updated.status == BindingStatus::Active
                && record.domain.is_none()
                && family.is_some()
                && default
                && !in_link
                && !record.linked
            {
                changes
                    .entry(identity.clone())
                    .or_default()
                    .push(BindingChange::AddDomain {
                        identity: identity.clone(),
                    });
            }
        } else {
            let vanished: Vec<_> = bindings
                .iter()
                .filter(|(id, r)| {
                    !columns.contains_key(*id)
                        && r.status != BindingStatus::Retired
                        && (r.column == *column
                            || match identity {
                                ColumnId::Pg { .. } => false,
                                ColumnId::Spanner { .. } => {
                                    (r.column.schema == column.schema
                                        && r.column.table == column.table)
                                        || spanner_vanished_match(&r.identity, identity, snapshot)
                                }
                            })
                })
                .map(|(_, r)| r)
                .collect();
            let ambiguous = !vanished.is_empty();
            for r in vanished {
                changes.insert(
                    r.identity.clone(),
                    vec![BindingChange::Update {
                        identity: r.identity.clone(),
                        column: r.column.clone(),
                        family: r.family,
                        show_rule: r.show_rule.clone(),
                        status: BindingStatus::Retired,
                    }],
                );
            }
            changes
                .entry(identity.clone())
                .or_default()
                .push(BindingChange::Create {
                    identity: identity.clone(),
                    column: column.clone(),
                    family,
                    want_domain: !ambiguous && default && !in_link && family.is_some(),
                    show_rule: if !ambiguous && show && !anchored(column) {
                        Some(column.clone())
                    } else {
                        None
                    },
                    status: if ambiguous {
                        BindingStatus::NeedsRepair
                    } else {
                        BindingStatus::Active
                    },
                });
        }
    }
    for (identity, record) in bindings.iter() {
        // A visible column with missing identity metadata is unknown, not proven vanished.
        let name_still_present = snapshot.relations.iter().any(|relation| {
            relation.schema == record.column.schema
                && relation.name == record.column.table
                && relation
                    .columns
                    .iter()
                    .any(|column| column.name == record.column.column)
        });
        if !columns.contains_key(identity)
            && (!name_still_present || snapshot.column_id(&record.column).is_some())
            && record.status != BindingStatus::Retired
            && (doc.route_default == RouteDefault::Tokenize || record.linked)
        {
            changes.insert(
                identity.clone(),
                vec![BindingChange::Update {
                    identity: identity.clone(),
                    column: record.column.clone(),
                    family: record.family,
                    show_rule: record.show_rule.clone(),
                    status: BindingStatus::Retired,
                }],
            );
        }
    }
    let mut changes: Vec<_> = changes.into_values().flatten().collect();
    let mut projected = bindings.clone();
    for change in &changes {
        match change {
            BindingChange::Create {
                identity,
                column,
                family,
                show_rule,
                status,
                ..
            } => {
                projected.0.insert(
                    identity.clone(),
                    BindingRecord {
                        identity: identity.clone(),
                        column: column.clone(),
                        family: *family,
                        domain: None,
                        linked: false,
                        show_rule: show_rule.clone(),
                        status: *status,
                        created_unix_ms: 0,
                    },
                );
            }
            BindingChange::Update {
                identity,
                column,
                family,
                show_rule,
                status,
            } => {
                if let Some(record) = projected.0.get_mut(identity) {
                    record.column = column.clone();
                    record.family = *family;
                    record.show_rule = show_rule.clone();
                    record.status = *status;
                }
            }
            BindingChange::AddDomain { .. } => {}
            BindingChange::SetDomain { .. } => {}
        }
    }
    let mut linked_ids = BTreeSet::new();
    for (group, link) in doc.links.iter().enumerate() {
        let members: BTreeSet<_> = link
            .fields
            .iter()
            .filter_map(|c| snapshot.column_id(c))
            .filter(|id| {
                projected
                    .get(id)
                    .is_some_and(|r| r.status == BindingStatus::Active)
            })
            .cloned()
            .collect();
        linked_ids.extend(members.iter().cloned());
        let shared = members
            .iter()
            .next()
            .and_then(|id| projected.get(id))
            .and_then(|r| r.domain.clone());
        let retained = shared.filter(|name| {
            generated.is_none_or(|names| names.contains(name))
                && members.iter().all(|id| {
                    projected
                        .get(id)
                        .is_some_and(|r| r.linked && r.domain.as_ref() == Some(name))
                })
                && bindings
                    .iter()
                    .filter(|(_, r)| {
                        r.status == BindingStatus::Active && r.domain.as_ref() == Some(name)
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<BTreeSet<_>>()
                    == members
        });
        let target = link.domain.clone().or(retained);
        for identity in members {
            if projected
                .get(&identity)
                .is_some_and(|record| !record.linked || target.is_none() || record.domain != target)
            {
                changes.push(BindingChange::SetDomain {
                    identity,
                    target: target
                        .clone()
                        .map(DomainTarget::Named)
                        .unwrap_or(DomainTarget::Generated { group }),
                    linked: true,
                });
            }
        }
    }
    for (identity, record) in projected.iter() {
        if columns.contains_key(identity)
            && record.status == BindingStatus::Active
            && record.linked
            && !linked_ids.contains(identity)
        {
            changes.push(BindingChange::SetDomain {
                identity: identity.clone(),
                target: DomainTarget::FreshPrivate,
                linked: false,
            });
        }
    }
    changes
}

fn spanner_vanished_match(old: &ColumnId, new: &ColumnId, snapshot: &Snapshot) -> bool {
    match (old, new) {
        (
            ColumnId::Spanner {
                schema,
                table,
                column,
                spanner_type,
            },
            ColumnId::Spanner {
                column: new_column,
                spanner_type: new_type,
                ..
            },
        ) => {
            column == new_column
                && spanner_type == new_type
                && !snapshot
                    .relations
                    .iter()
                    .any(|r| r.schema == *schema && r.name == *table)
        }
        (ColumnId::Pg { .. }, _) | (_, ColumnId::Pg { .. }) => false,
    }
}

/// Route membership changes requiring session invalidation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipChange {
    /// Affected route.
    pub route: RouteId,
    /// Generated domains retired by this refresh.
    pub retired: Vec<DomainName>,
    /// Domain memberships added.
    pub joined: Vec<(DomainName, ColumnRef)>,
    /// Domain memberships removed.
    pub left: Vec<(DomainName, ColumnRef)>,
    /// Link fields withheld because identity is absent or needs repair.
    pub skipped: Vec<ColumnRef>,
}
impl MembershipChange {
    /// Whether no membership or skipped-field information changed.
    pub fn is_empty(&self) -> bool {
        !self.requires_reset() && self.skipped.is_empty()
    }
    /// Whether old session tokens must be discarded.
    pub fn requires_reset(&self) -> bool {
        !self.retired.is_empty() || !self.joined.is_empty() || !self.left.is_empty()
    }
}
/// Test whether a generated domain was retired.
pub fn is_retired(store: &dyn Store, name: &DomainName) -> Result<bool, BindingError> {
    Ok(store
        .read()
        .map_err(|_| BindingError::Store)?
        .get(Table::RetiredDomains, &keys::domain_name(name))
        .map_err(|_| BindingError::Store)?
        .is_some())
}
/// Load the generated domains retired by a route.
pub fn load_retired(
    store: &dyn Store,
    route: &RouteId,
) -> Result<BTreeSet<DomainName>, BindingError> {
    let mut names = BTreeSet::new();
    for (key, value) in store
        .read()
        .map_err(|_| BindingError::Store)?
        .scan_prefix(Table::RetiredDomains, b"")
        .map_err(|_| BindingError::Store)?
    {
        if value == route.as_str().as_bytes() {
            names.insert(DomainName(
                String::from_utf8(key).map_err(|_| BindingError::Corrupt)?,
            ));
        }
    }
    Ok(names)
}
/// Refresh bindings and report domain membership changes atomically.
pub fn refresh_membership(
    store: &dyn Store,
    route: &RouteId,
    doc: &PolicyDocument,
    snapshot: &Snapshot,
) -> Result<(RouteBindings, MembershipChange), BindingError> {
    check_declared_names(store, doc)?;
    let before = load(store, route)?;
    let read = store.read().map_err(|_| BindingError::Store)?;
    let mut generated = BTreeSet::new();
    for (key, _) in read
        .scan_prefix(Table::DomainNames, b"")
        .map_err(|_| BindingError::Store)?
    {
        generated.insert(DomainName(
            String::from_utf8(key).map_err(|_| BindingError::Corrupt)?,
        ));
    }
    drop(read);
    let retired_before = load_retired(store, route)?;
    // A retired name is never kept as a link's shared domain.
    generated.retain(|name| !retired_before.contains(name));
    let changes = reconcile_with_names(&before, doc, snapshot, Some(&generated));
    let mut reserved: BTreeSet<_> = doc.policy.domains.keys().cloned().collect();
    for treatment in doc.policy.columns.values() {
        match treatment {
            Treatment::Blind { domain } => {
                reserved.insert(domain.clone());
            }
            Treatment::Show | Treatment::Masked { .. } => {}
        }
    }
    reserved.extend(doc.links.iter().filter_map(|l| l.domain.clone()));
    let after = if changes.is_empty() {
        before.clone()
    } else {
        apply(store, route, &changes, &reserved)?
    };
    let mut change = MembershipChange {
        route: route.clone(),
        retired: load_retired(store, route)?
            .difference(&retired_before)
            .cloned()
            .collect(),
        joined: vec![],
        left: vec![],
        skipped: vec![],
    };
    for (identity, record) in before
        .iter()
        .filter(|(_, r)| r.status == BindingStatus::Active)
    {
        if let Some(domain) = &record.domain
            && !after.get(identity).is_some_and(|r| {
                r.status == BindingStatus::Active && r.domain.as_ref() == Some(domain)
            }) {
                change.left.push((domain.clone(), record.column.clone()));
            }
    }
    for (identity, record) in after
        .iter()
        .filter(|(_, r)| r.status == BindingStatus::Active)
    {
        if let Some(domain) = &record.domain
            && !before.get(identity).is_some_and(|r| {
                r.status == BindingStatus::Active && r.domain.as_ref() == Some(domain)
            }) {
                change.joined.push((domain.clone(), record.column.clone()));
            }
    }
    for column in doc.links.iter().flat_map(|l| &l.fields) {
        if !snapshot
            .column_id(column)
            .and_then(|id| after.get(id))
            .is_some_and(|r| r.status == BindingStatus::Active)
        {
            change.skipped.push(column.clone());
        }
    }
    change.skipped.sort();
    change.skipped.dedup();
    Ok((after, change))
}
/// Refresh persisted bindings without returning membership information.
pub fn refresh(
    store: &dyn Store,
    route: &RouteId,
    doc: &PolicyDocument,
    snapshot: &Snapshot,
) -> Result<RouteBindings, BindingError> {
    refresh_membership(store, route, doc, snapshot).map(|(bindings, _)| bindings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        state::{schema::STORE_FILE, store::RedbStore},
        token::keys::RootKey,
    };
    use redb::ReadableDatabase;
    use std::{path::PathBuf, sync::Arc};
    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let path = PathBuf::from(std::env::var_os("TEMP").unwrap())
                .join(format!("bindings-{}", generate_domain_name().unwrap().0));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn root() -> RootKey {
        RootKey::from_hex(&"01".repeat(32)).unwrap()
    }
    fn route() -> RouteId {
        RouteId::new("route").unwrap()
    }
    fn id(n: i16) -> ColumnId {
        ColumnId::Pg {
            relation_oid: 123,
            attnum: n,
        }
    }
    fn column() -> ColumnRef {
        ColumnRef {
            schema: "public".into(),
            table: "customers".into(),
            column: "email".into(),
        }
    }
    fn create(n: i16) -> BindingChange {
        BindingChange::Create {
            identity: id(n),
            column: column(),
            family: Some(TypeFamily::Text),
            want_domain: true,
            show_rule: None,
            status: BindingStatus::Active,
        }
    }
    fn name(s: &str) -> DomainName {
        DomainName(s.into())
    }
    fn document() -> PolicyDocument {
        PolicyDocument {
            file_readlog: None,
            file_snapshot_max_age_ms: None,
            policy: super::super::Policy {
                columns: BTreeMap::new(),
                domains: BTreeMap::new(),
                snapshot_max_age_ms: 60000,
                readlog: super::super::ReadLogSink::Stdout,
            },
            version: super::super::PolicyVersion::V2,
            route_default: RouteDefault::Tokenize,
            json_paths: vec![],
            links: vec![],
        }
    }
    fn snap(entries: Vec<(ColumnRef, Option<ColumnId>, ColumnType)>) -> Snapshot {
        use crate::schema::{Column, Relation, RelationKind};
        let mut relations = BTreeMap::new();
        let mut ids = BTreeMap::new();
        for (c, id, ty) in entries {
            if let Some(id) = id {
                ids.insert(c.clone(), id);
            }
            let r = relations
                .entry((c.schema.clone(), c.table.clone()))
                .or_insert_with(|| Relation {
                    schema: c.schema.clone(),
                    name: c.table.clone(),
                    kind: RelationKind::Table,
                    columns: vec![],
                    view_definition: None,
                    primary_key: vec![],
                });
            r.columns.push(Column {
                name: c.column,
                ty,
                nullable: true,
                generated: None,
                default: None,
            });
        }
        Snapshot::new(relations.into_values().collect(), vec![], vec![], 0).with_column_ids(ids)
    }
    fn pg(name: &str, n: i16, ty: ColumnType) -> Snapshot {
        snap(vec![(
            ColumnRef {
                column: name.into(),
                ..column()
            },
            Some(id(n)),
            ty,
        )])
    }
    fn spanner_id(name: &str, ty: &str) -> ColumnId {
        ColumnId::Spanner {
            schema: "public".into(),
            table: "customers".into(),
            column: name.into(),
            spanner_type: ty.into(),
        }
    }
    fn result(
        doc: &PolicyDocument,
        snapshot: &Snapshot,
        bindings: &RouteBindings,
        c: &ColumnRef,
    ) -> (
        super::super::effective::Effective,
        super::super::effective::Origin,
    ) {
        super::super::effective::effective(doc, snapshot, bindings, c).unwrap()
    }
    fn linked_snapshot(n: i16) -> Snapshot {
        snap(
            (1..=n)
                .map(|i| {
                    (
                        ColumnRef {
                            column: format!("c{i}"),
                            ..column()
                        },
                        Some(id(i)),
                        ColumnType::Text,
                    )
                })
                .collect(),
        )
    }
    fn link_fields(n: i16, domain: Option<DomainName>) -> super::super::LinkRule {
        super::super::LinkRule {
            fields: (1..=n)
                .map(|i| ColumnRef {
                    column: format!("c{i}"),
                    ..column()
                })
                .collect(),
            domain,
        }
    }
    fn domain(bindings: &RouteBindings, n: i16) -> DomainName {
        bindings.get(&id(n)).unwrap().domain.clone().unwrap()
    }
    #[test]
    fn generated_links_rotate_on_membership_changes_and_unlink() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let snapshot = linked_snapshot(3);
        let private = refresh(&store, &route(), &doc, &snapshot).unwrap();
        let mut seen: BTreeSet<_> = (1..=3).map(|n| domain(&private, n)).collect();
        doc.links.push(link_fields(2, None));
        let (linked, change) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert_eq!(domain(&linked, 1), domain(&linked, 2));
        assert!(seen.insert(domain(&linked, 1)));
        assert_eq!(
            change.retired.iter().cloned().collect::<BTreeSet<_>>(),
            (1..=2).map(|n| domain(&private, n)).collect()
        );
        assert_eq!(change.joined.len(), 2);
        assert_eq!(change.left.len(), 2);
        assert!(change.requires_reset());
        store.fail_commits(true);
        let (same, change) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert_eq!(same, linked);
        assert!(change.is_empty());
        store.fail_commits(false);
        doc.links[0] = link_fields(3, None);
        let (larger, change) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert!(seen.insert(domain(&larger, 1)));
        assert_eq!(domain(&larger, 1), domain(&larger, 3));
        assert!(change.retired.contains(&domain(&linked, 1)));
        doc.links[0] = link_fields(2, None);
        let (smaller, _) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert!(seen.insert(domain(&smaller, 1)));
        assert!(seen.insert(domain(&smaller, 3)));
        assert!(is_retired(&store, &domain(&larger, 1)).unwrap());
        doc.links.clear();
        let (unlinked, change) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert!(change.retired.contains(&domain(&smaller, 1)));
        for n in 1..=2 {
            assert!(seen.insert(domain(&unlinked, n)));
            assert!(!unlinked.get(&id(n)).unwrap().linked);
        }
        assert!(
            load_retired(&store, &route())
                .unwrap()
                .contains(&domain(&linked, 1))
        );
        // A vanished member also rotates the remaining generated group.
        doc.links.push(link_fields(2, None));
        let linked = refresh(&store, &route(), &doc, &snapshot).unwrap();
        let (remaining, change) =
            refresh_membership(&store, &route(), &doc, &linked_snapshot(1)).unwrap();
        assert_ne!(domain(&remaining, 1), domain(&linked, 1));
        assert!(change.retired.contains(&domain(&linked, 1)));
        assert_eq!(change.skipped, vec![link_fields(2, None).fields[1].clone()]);
    }
    #[test]
    fn declared_links_are_not_allocations_and_can_become_generated() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let snapshot = linked_snapshot(2);
        let before = refresh(&store, &route(), &doc, &snapshot).unwrap();
        let declared = name("shared");
        doc.links.push(link_fields(2, Some(declared.clone())));
        let (after, change) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert_eq!(domain(&after, 1), declared);
        assert_eq!(domain(&after, 2), declared);
        assert_eq!(change.retired.len(), 2);
        assert!(change.retired.contains(&domain(&before, 1)));
        assert!(!is_retired(&store, &declared).unwrap());
        assert!(
            store
                .read()
                .unwrap()
                .get(Table::DomainNames, &keys::domain_name(&declared))
                .unwrap()
                .is_none()
        );
        store.fail_commits(true);
        assert!(
            refresh_membership(&store, &route(), &doc, &snapshot)
                .unwrap()
                .1
                .is_empty()
        );
        store.fail_commits(false);
        doc.links[0].domain = None;
        let generated = refresh(&store, &route(), &doc, &snapshot).unwrap();
        assert_ne!(domain(&generated, 1), declared);
        assert_eq!(domain(&generated, 1), domain(&generated, 2));
        assert!(!is_retired(&store, &declared).unwrap());
    }
    #[test]
    fn link_collision_and_failed_commit_leave_all_tables_unchanged() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let snapshot = linked_snapshot(2);
        let before = refresh(&store, &route(), &doc, &snapshot).unwrap();
        let capture = || {
            Table::ALL
                .iter()
                .map(|t| store.read().unwrap().scan_prefix(*t, b"").unwrap())
                .collect::<Vec<_>>()
        };
        let old_tables = capture();
        doc.links.push(link_fields(2, Some(domain(&before, 1))));
        assert_eq!(
            refresh_membership(&store, &route(), &doc, &snapshot).unwrap_err(),
            BindingError::DeclaredNameCollision
        );
        assert_eq!(capture(), old_tables);
        doc.links[0].domain = None;
        store.fail_commits(true);
        assert_eq!(
            refresh_membership(&store, &route(), &doc, &snapshot).unwrap_err(),
            BindingError::Store
        );
        assert_eq!(capture(), old_tables);
        assert_eq!(load(&store, &route()).unwrap(), before);
        store.fail_commits(false);
        let (after, _) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        let mut generate = || Ok(domain(&before, 1));
        assert_eq!(
            apply_with_generator(
                &store,
                &route(),
                &[create(4)],
                &BTreeSet::new(),
                &mut generate
            )
            .unwrap_err(),
            BindingError::NameSpaceExhausted
        );
        assert_eq!(load(&store, &route()).unwrap(), after);
    }
    #[test]
    fn show_links_only_create_linked_records_and_skip_unusable_fields() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        doc.route_default = RouteDefault::Show;
        doc.links.push(link_fields(2, None));
        let snapshot = linked_snapshot(3);
        let (bindings, _) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        assert_eq!(bindings.iter().count(), 2);
        assert_eq!(domain(&bindings, 1), domain(&bindings, 2));
        assert!(bindings.get(&id(3)).is_none());
        let mut changed = snapshot.clone();
        changed.relations[0].columns[0].ty = ColumnType::Int4;
        let (bindings, change) = refresh_membership(&store, &route(), &doc, &changed).unwrap();
        assert_eq!(
            bindings.get(&id(1)).unwrap().status,
            BindingStatus::NeedsRepair
        );
        assert_eq!(change.skipped, vec![link_fields(2, None).fields[0].clone()]);
        assert_ne!(domain(&bindings, 1), domain(&bindings, 2));
        doc.links.clear();
        let bindings = refresh(&store, &route(), &doc, &changed).unwrap();
        assert!(!bindings.get(&id(2)).unwrap().linked);
        assert!(bindings.get(&id(3)).is_none());
    }
    #[test]
    fn spanner_table_rename_with_matching_column_is_withheld() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let old_id = spanner_id("email", "STRING");
        let before = refresh(
            &store,
            &route(),
            &doc,
            &snap(vec![(column(), Some(old_id.clone()), ColumnType::Text)]),
        )
        .unwrap();
        let old_domain = before.get(&old_id).unwrap().domain.clone().unwrap();
        let renamed = ColumnRef {
            table: "new_table".into(),
            ..column()
        };
        let new_id = ColumnId::Spanner {
            schema: renamed.schema.clone(),
            table: renamed.table.clone(),
            column: renamed.column.clone(),
            spanner_type: "STRING".into(),
        };
        let unrelated = ColumnRef {
            column: "b".into(),
            ..renamed.clone()
        };
        let unrelated_id = ColumnId::Spanner {
            schema: unrelated.schema.clone(),
            table: unrelated.table.clone(),
            column: unrelated.column.clone(),
            spanner_type: "INT64".into(),
        };
        doc.policy.columns.insert(renamed.clone(), Treatment::Show);
        let snapshot = snap(vec![
            (renamed, Some(new_id.clone()), ColumnType::Text),
            (unrelated, Some(unrelated_id.clone()), ColumnType::Int8),
        ]);
        let (bindings, change) = refresh_membership(&store, &route(), &doc, &snapshot).unwrap();
        let new = bindings.get(&new_id).unwrap();
        assert_eq!(new.status, BindingStatus::NeedsRepair);
        assert!(new.domain.is_none());
        assert!(new.show_rule.is_none());
        assert_eq!(
            bindings.get(&old_id).unwrap().status,
            BindingStatus::Retired
        );
        assert_eq!(change.retired, vec![old_domain]);
        assert_eq!(
            bindings.get(&unrelated_id).unwrap().status,
            BindingStatus::Active
        );
        assert!(bindings.get(&unrelated_id).unwrap().domain.is_some());
    }
    #[test]
    fn legacy_binding_json_and_retirement_table_upgrade() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let bindings = apply(&store, &route(), &[create(1)], &BTreeSet::new()).unwrap();
        let mut value = serde_json::to_value(bindings.get(&id(1)).unwrap()).unwrap();
        value.as_object_mut().unwrap().remove("linked");
        let old_bytes = serde_json::to_vec(&value).unwrap();
        let decoded: BindingRecord = model::decode(&old_bytes).unwrap();
        assert!(!decoded.linked);
        let mut txn = store.write().unwrap();
        txn.put(
            Table::DomainBindings,
            &keys::domain_binding(&route(), &id(1)),
            &old_bytes,
        )
        .unwrap();
        txn.commit().unwrap();
        drop(store);
        let db = redb::Database::create(dir.0.join(STORE_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        assert!(
            txn.delete_table(Table::RetiredDomains.definition())
                .unwrap()
        );
        txn.commit().unwrap();
        drop(db);
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        assert_eq!(store.meta().format_version, 1);
        assert_eq!(
            store
                .read()
                .unwrap()
                .get(
                    Table::DomainBindings,
                    &keys::domain_binding(&route(), &id(1))
                )
                .unwrap(),
            Some(old_bytes)
        );
        assert!(load_retired(&store, &route()).unwrap().is_empty());
        assert_eq!(
            load(&store, &route()).unwrap().get(&id(1)).unwrap(),
            &decoded
        );
        let change = BindingChange::Update {
            identity: id(1),
            column: column(),
            family: decoded.family,
            show_rule: None,
            status: BindingStatus::Retired,
        };
        apply(&store, &route(), &[change], &BTreeSet::new()).unwrap();
        assert!(is_retired(&store, decoded.domain.as_ref().unwrap()).unwrap());
    }
    #[test]
    fn first_snapshot_refresh_is_idempotent_and_durable() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let doc = document();
        let entries: Vec<_> = [
            ColumnType::Text,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Uuid,
            ColumnType::Date,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::Bool,
            ColumnType::Json,
            ColumnType::Jsonb,
        ]
        .into_iter()
        .enumerate()
        .map(|(n, ty)| {
            (
                ColumnRef {
                    column: format!("c{n}"),
                    ..column()
                },
                Some(id(n as i16)),
                ty,
            )
        })
        .collect();
        let snapshot = snap(entries);
        let bindings = refresh(&store, &route(), &doc, &snapshot).unwrap();
        assert_eq!(bindings.iter().count(), 10);
        for (_, r) in bindings.iter() {
            assert_eq!(r.status, BindingStatus::Active);
            assert_eq!(r.domain.is_some(), r.family.is_some());
        }
        assert!(reconcile(&bindings, &doc, &snapshot).is_empty());
        store.fail_commits(true);
        assert_eq!(
            bindings,
            refresh(&store, &route(), &doc, &snapshot).unwrap()
        );
        drop(store);
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        assert_eq!(bindings, load(&store, &route()).unwrap());
        let mut show = doc;
        show.route_default = RouteDefault::Show;
        assert!(reconcile(&RouteBindings::default(), &show, &snapshot).is_empty());
    }
    #[test]
    fn postgres_rename_keeps_domain_and_anchored_show() {
        use super::super::effective::{Effective, Origin};
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let before = refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Text)).unwrap();
        let domain = before.get(&id(1)).unwrap().domain.clone();
        doc.policy.columns.insert(column(), Treatment::Show);
        refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Text)).unwrap();
        let renamed = pg("renamed", 1, ColumnType::Text);
        let after = refresh(&store, &route(), &doc, &renamed).unwrap();
        assert_eq!(after.get(&id(1)).unwrap().domain, domain);
        assert_eq!(after.get(&id(1)).unwrap().column.column, "renamed");
        assert_eq!(
            result(
                &doc,
                &renamed,
                &after,
                &ColumnRef {
                    column: "renamed".into(),
                    ..column()
                }
            ),
            (Effective::Show, Origin::Explicit)
        );
    }
    #[test]
    fn show_grant_follows_identity_when_old_name_is_reused() {
        use super::super::effective::{Effective, Origin};
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let named = |n: &str| ColumnRef {
            column: n.into(),
            ..column()
        };
        let mut doc = document();
        doc.policy.columns.insert(named("email"), Treatment::Show);
        // email (attnum 1) is shown; notes (attnum 2) is tokenized.
        let first = snap(vec![
            (named("email"), Some(id(1)), ColumnType::Text),
            (named("notes"), Some(id(2)), ColumnType::Text),
        ]);
        refresh(&store, &route(), &doc, &first).unwrap();
        // Rename email -> contact, then notes -> email.
        let swapped = snap(vec![
            (named("contact"), Some(id(1)), ColumnType::Text),
            (named("email"), Some(id(2)), ColumnType::Text),
        ]);
        let bindings = refresh(&store, &route(), &doc, &swapped).unwrap();
        assert_eq!(bindings.get(&id(2)).unwrap().show_rule, None);
        assert_eq!(
            result(&doc, &swapped, &bindings, &named("contact")),
            (Effective::Show, Origin::Explicit)
        );
        assert_eq!(
            result(&doc, &swapped, &bindings, &named("email")).1,
            Origin::NeedsRepair
        );
        // Moving the rule to the new name keeps the grant on the same identity.
        doc.policy.columns.remove(&named("email"));
        doc.policy.columns.insert(named("contact"), Treatment::Show);
        let bindings = refresh(&store, &route(), &doc, &swapped).unwrap();
        assert_eq!(
            result(&doc, &swapped, &bindings, &named("contact")),
            (Effective::Show, Origin::Explicit)
        );
        assert!(matches!(
            result(&doc, &swapped, &bindings, &named("email")).0,
            Effective::BlindPrivate { .. }
        ));
    }
    #[test]
    fn postgres_recreate_withholds_show_and_never_reuses_names() {
        use super::super::effective::{Effective, Origin};
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let first = refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Text)).unwrap();
        let old_domain = first.get(&id(1)).unwrap().domain.clone().unwrap();
        doc.policy.columns.insert(column(), Treatment::Show);
        refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Text)).unwrap();
        let recreated = pg("email", 2, ColumnType::Text);
        let next = refresh(&store, &route(), &doc, &recreated).unwrap();
        assert_eq!(next.get(&id(1)).unwrap().status, BindingStatus::Retired);
        assert_eq!(next.get(&id(2)).unwrap().status, BindingStatus::NeedsRepair);
        assert_eq!(next.get(&id(2)).unwrap().show_rule, None);
        assert_eq!(
            result(&doc, &recreated, &next, &column()),
            (Effective::Masked { placeholder: None }, Origin::NeedsRepair)
        );
        let mut sequence = [old_domain, name("fzzzzzzzzzz")].into_iter();
        let next = apply_with_generator(&store, &route(), &[create(3)], &BTreeSet::new(), || {
            Ok(sequence.next().unwrap())
        })
        .unwrap();
        assert_eq!(next.get(&id(3)).unwrap().domain, Some(name("fzzzzzzzzzz")));
    }
    #[test]
    fn postgres_family_changes_require_repair_without_losing_domain() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let doc = document();
        let before = refresh(
            &store,
            &route(),
            &doc,
            &pg("email", 1, ColumnType::Varchar(Some(20))),
        )
        .unwrap();
        let compatible =
            refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Text)).unwrap();
        assert_eq!(before, compatible);
        let broken = refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Int8)).unwrap();
        assert_eq!(
            broken.get(&id(1)).unwrap().status,
            BindingStatus::NeedsRepair
        );
        assert_eq!(
            broken.get(&id(1)).unwrap().domain,
            before.get(&id(1)).unwrap().domain
        );
        let restored = refresh(&store, &route(), &doc, &pg("email", 1, ColumnType::Text)).unwrap();
        assert_eq!(
            restored.get(&id(1)).unwrap().status,
            BindingStatus::NeedsRepair
        );
    }
    #[test]
    fn spanner_changes_are_ambiguous_but_additions_are_active() {
        for (new_name, raw, ty) in [
            ("email", "INT64", ColumnType::Int8),
            ("renamed", "STRING", ColumnType::Text),
        ] {
            let dir = TestDir::new();
            let store = RedbStore::open(&dir.0, &root()).unwrap();
            let doc = document();
            let old_id = spanner_id("email", "STRING");
            refresh(
                &store,
                &route(),
                &doc,
                &snap(vec![(column(), Some(old_id.clone()), ColumnType::Text)]),
            )
            .unwrap();
            let new_id = spanner_id(new_name, raw);
            let changed = snap(vec![(
                ColumnRef {
                    column: new_name.into(),
                    ..column()
                },
                Some(new_id.clone()),
                ty,
            )]);
            let bindings = refresh(&store, &route(), &doc, &changed).unwrap();
            assert_eq!(
                bindings.get(&old_id).unwrap().status,
                BindingStatus::Retired
            );
            assert_eq!(
                bindings.get(&new_id).unwrap().status,
                BindingStatus::NeedsRepair
            );
        }
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let doc = document();
        let old_id = spanner_id("email", "STRING");
        let new_id = spanner_id("added", "STRING");
        refresh(
            &store,
            &route(),
            &doc,
            &snap(vec![(column(), Some(old_id.clone()), ColumnType::Text)]),
        )
        .unwrap();
        let added = snap(vec![
            (column(), Some(old_id), ColumnType::Text),
            (
                ColumnRef {
                    column: "added".into(),
                    ..column()
                },
                Some(new_id.clone()),
                ColumnType::Text,
            ),
        ]);
        let bindings = refresh(&store, &route(), &doc, &added).unwrap();
        assert_eq!(bindings.get(&new_id).unwrap().status, BindingStatus::Active);
        assert!(bindings.get(&new_id).unwrap().domain.is_some());
    }
    #[test]
    fn removing_mask_allocates_domain_and_add_domain_refuses_invalid_requests() {
        use super::super::effective::{Effective, Origin};
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        let snapshot = pg("email", 1, ColumnType::Text);
        doc.policy
            .columns
            .insert(column(), Treatment::Masked { placeholder: None });
        let before = refresh(&store, &route(), &doc, &snapshot).unwrap();
        assert!(before.get(&id(1)).unwrap().domain.is_none());
        doc.policy.columns.clear();
        assert!(matches!(
            reconcile(&before, &doc, &snapshot).as_slice(),
            [BindingChange::AddDomain { .. }]
        ));
        let after = refresh(&store, &route(), &doc, &snapshot).unwrap();
        assert_eq!(
            result(&doc, &snapshot, &after, &column()),
            (
                Effective::BlindPrivate {
                    domain: after.get(&id(1)).unwrap().domain.clone().unwrap()
                },
                Origin::RouteDefault
            )
        );
        for identity in [id(1), id(99)] {
            assert_eq!(
                apply(
                    &store,
                    &route(),
                    &[BindingChange::AddDomain { identity }],
                    &BTreeSet::new()
                )
                .unwrap_err(),
                BindingError::UnknownIdentity
            );
        }
        let unknown = snap(vec![(column(), None, ColumnType::Text)]);
        assert!(reconcile(&after, &doc, &unknown).is_empty());
        assert_eq!(
            result(&doc, &unknown, &after, &column()),
            (Effective::Masked { placeholder: None }, Origin::Unbound)
        );
    }
    #[test]
    fn generated_names_obey_both_rules_and_are_distinct() {
        let mut names = BTreeSet::new();
        for _ in 0..1000 {
            let n = generate_domain_name().unwrap();
            assert_eq!(n.0.len(), 11);
            assert!(n.0.starts_with('f'));
            assert!(
                n.0[1..]
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
            );
            // Same rule as the private policy::file::domain_name_valid validator.
            assert!(
                (1..=31).contains(&n.0.len())
                    && n.0.as_bytes()[0].is_ascii_lowercase()
                    && n.0
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            );
            assert!(names.insert(n));
        }
    }
    #[test]
    fn round_trip_reopen_and_update_preserves_allocation() {
        let dir = TestDir::new();
        let store = RedbStore::with_clock(&dir.0, &root(), Arc::new(|| 42)).unwrap();
        let before = apply(&store, &route(), &[create(1)], &BTreeSet::new()).unwrap();
        assert_eq!(before, load(&store, &route()).unwrap());
        assert_eq!(before.by_column(&column()).count(), 1);
        assert_eq!(before.iter().count(), 1);
        drop(store);
        let store = RedbStore::with_clock(&dir.0, &root(), Arc::new(|| 99)).unwrap();
        assert_eq!(before, load(&store, &route()).unwrap());
        let renamed = ColumnRef {
            column: "renamed".into(),
            ..column()
        };
        let change = BindingChange::Update {
            identity: id(1),
            column: renamed.clone(),
            family: None,
            show_rule: Some(renamed.clone()),
            status: BindingStatus::Retired,
        };
        let after = apply(&store, &route(), &[change], &BTreeSet::new()).unwrap();
        let record = after.get(&id(1)).unwrap();
        assert_eq!(record.domain, before.get(&id(1)).unwrap().domain);
        assert_eq!(record.created_unix_ms, 42);
        assert_eq!(record.column, renamed);
        assert_eq!(record.show_rule, Some(renamed));
        assert_eq!(record.family, None);
        assert_eq!(record.status, BindingStatus::Retired);
        let change = BindingChange::Update {
            identity: id(3),
            column: column(),
            family: None,
            show_rule: None,
            status: BindingStatus::Active,
        };
        assert_eq!(
            apply(&store, &route(), &[create(2), change], &BTreeSet::new()).unwrap_err(),
            BindingError::UnknownIdentity
        );
        assert_eq!(after, load(&store, &route()).unwrap());
        assert_eq!(
            store
                .read()
                .unwrap()
                .scan_prefix(Table::DomainNames, b"")
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn collisions_retry_reserved_persisted_and_current_names() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let persisted = name("faaaaaaaaaa");
        let reserved = name("fbbbbbbbbbb");
        let fresh = name("fcccccccccc");
        let second = name("fdddddddddd");
        let mut txn = store.write().unwrap();
        txn.put(
            Table::DomainNames,
            &keys::domain_name(&persisted),
            b"existing",
        )
        .unwrap();
        txn.commit().unwrap();
        let reserved = BTreeSet::from([reserved]);
        let mut sequence = [
            persisted.clone(),
            reserved.first().unwrap().clone(),
            fresh.clone(),
            fresh.clone(),
            second.clone(),
        ]
        .into_iter();
        let result =
            apply_with_generator(&store, &route(), &[create(1), create(2)], &reserved, || {
                Ok(sequence.next().unwrap())
            })
            .unwrap();
        assert_eq!(result.get(&id(1)).unwrap().domain, Some(fresh));
        assert_eq!(result.get(&id(2)).unwrap().domain, Some(second));
        let mut tries = 0;
        assert_eq!(
            apply_with_generator(&store, &route(), &[create(3)], &reserved, || {
                tries += 1;
                Ok(persisted.clone())
            })
            .unwrap_err(),
            BindingError::NameSpaceExhausted
        );
        assert_eq!(tries, 16);
        assert_eq!(result, load(&store, &route()).unwrap());
        assert_eq!(
            store
                .read()
                .unwrap()
                .scan_prefix(Table::DomainNames, b"")
                .unwrap()
                .len(),
            3
        );
    }
    #[test]
    fn failed_commit_leaves_neither_record_nor_name() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        store.fail_commits(true);
        assert_eq!(
            apply(&store, &route(), &[create(1)], &BTreeSet::new()).unwrap_err(),
            BindingError::Store
        );
        assert_eq!(load(&store, &route()).unwrap().iter().count(), 0);
        assert!(
            store
                .read()
                .unwrap()
                .scan_prefix(Table::DomainNames, b"")
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn upgrade_creates_missing_tables_without_changing_old_records() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let deployment = store.meta().deployment_id;
        let created = store.meta().created_unix_ms;
        let mut txn = store.write().unwrap();
        txn.put(Table::Principals, b"old-key", b"unchanged bytes")
            .unwrap();
        txn.commit().unwrap();
        drop(store);
        let db = redb::Database::create(dir.0.join(STORE_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        assert!(
            txn.delete_table(Table::DomainBindings.definition())
                .unwrap()
        );
        assert!(txn.delete_table(Table::DomainNames.definition()).unwrap());
        txn.commit().unwrap();
        drop(db);
        let wrong = RootKey::from_hex(&"02".repeat(32)).unwrap();
        assert!(RedbStore::open(&dir.0, &wrong).is_err());
        let db = redb::Database::create(dir.0.join(STORE_FILE)).unwrap();
        assert!(
            db.begin_read()
                .unwrap()
                .open_table(Table::DomainBindings.definition())
                .is_err()
        );
        drop(db);
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        assert_eq!(store.meta().format_version, 1);
        assert_eq!(store.meta().deployment_id, deployment);
        assert_eq!(store.meta().created_unix_ms, created);
        assert_eq!(
            store
                .read()
                .unwrap()
                .get(Table::Principals, b"old-key")
                .unwrap(),
            Some(b"unchanged bytes".to_vec())
        );
        assert_eq!(
            apply(&store, &route(), &[create(1)], &BTreeSet::new())
                .unwrap()
                .iter()
                .count(),
            1
        );
    }
    #[test]
    fn corrupt_records_are_refused_and_routes_are_isolated() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        apply(&store, &route(), &[create(1)], &BTreeSet::new()).unwrap();
        assert_eq!(
            load(&store, &RouteId::new("route-extra").unwrap())
                .unwrap()
                .iter()
                .count(),
            0
        );
        let mut txn = store.write().unwrap();
        txn.put(
            Table::DomainBindings,
            &keys::domain_binding(&route(), &id(2)),
            b"invalid",
        )
        .unwrap();
        txn.commit().unwrap();
        assert_eq!(load(&store, &route()).unwrap_err(), BindingError::Corrupt);
        assert_eq!(
            apply(&store, &route(), &[create(3)], &BTreeSet::new()).unwrap_err(),
            BindingError::Corrupt
        );
    }
    #[test]
    fn all_column_types_have_explicit_families() {
        use ColumnType::*;
        for (ty, family) in [
            (Text, TypeFamily::Text),
            (Varchar(Some(12)), TypeFamily::Text),
            (Bpchar(None), TypeFamily::Text),
            (Int4, TypeFamily::Int4),
            (Int8, TypeFamily::Int8),
            (Uuid, TypeFamily::Uuid),
            (Date, TypeFamily::Date),
            (Timestamp, TypeFamily::Timestamp),
            (Timestamptz, TypeFamily::Timestamptz),
        ] {
            assert_eq!(TypeFamily::of(&ty), Some(family));
        }
        for ty in [
            Int2,
            Bool,
            Numeric,
            Bytea,
            Json,
            Jsonb,
            Enum("enum".into()),
            Array(Box::new(Text)),
            Other("other".into()),
        ] {
            assert_eq!(TypeFamily::of(&ty), None);
        }
    }
    #[test]
    fn declared_names_refuse_generated_collisions_from_all_sources() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let bindings = apply(&store, &route(), &[create(1)], &BTreeSet::new()).unwrap();
        let generated = bindings.get(&id(1)).unwrap().domain.clone().unwrap();
        for source in 0..3 {
            let mut doc = document();
            match source {
                0 => {
                    doc.policy.domains.insert(
                        generated.clone(),
                        super::super::DomainSettings {
                            tokenize: super::super::TokenizeSetting::Off,
                            range: None,
                        },
                    );
                }
                1 => {
                    doc.policy.columns.insert(
                        column(),
                        Treatment::Blind {
                            domain: generated.clone(),
                        },
                    );
                }
                2 => {
                    doc.links.push(super::super::LinkRule {
                        fields: vec![],
                        domain: Some(generated.clone()),
                    });
                }
                _ => panic!("invalid test case"),
            }
            let error = check_declared_names(&store, &doc).unwrap_err();
            assert_eq!(error, BindingError::DeclaredNameCollision);
            assert!(!error.to_string().contains(&generated.0));
        }
        assert_eq!(load(&store, &route()).unwrap(), bindings);
    }
    #[test]
    fn declared_names_allow_noncolliding_names_and_empty_store() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let mut doc = document();
        doc.policy.domains.insert(
            name("declared"),
            super::super::DomainSettings {
                tokenize: super::super::TokenizeSetting::Off,
                range: None,
            },
        );
        assert_eq!(check_declared_names(&store, &doc), Ok(()));
        apply(&store, &route(), &[create(1)], &BTreeSet::new()).unwrap();
        assert_eq!(check_declared_names(&store, &doc), Ok(()));
        assert_eq!(check_declared_names(&store, &document()), Ok(()));
    }
}
