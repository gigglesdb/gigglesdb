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
/// Persisted identity and its immutable private domain allocation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingRecord {
    /// Captured stable identity.
    pub identity: ColumnId,
    /// Last observed column name.
    pub column: ColumnRef,
    /// Compatible codec family.
    pub family: Option<TypeFamily>,
    /// Allocated private domain.
    pub domain: Option<DomainName>,
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
/// Atomic binding mutation request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BindingChange {
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
    let mut txn = store.write().map_err(|_| BindingError::Store)?;
    let mut bindings = read_bindings(txn.as_ref(), route)?;
    for change in changes {
        let record = match change {
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
    match doc.route_default {
        RouteDefault::Show => return vec![],
        RouteDefault::Tokenize => {}
    }
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
                                    r.column.schema == column.schema
                                        && r.column.table == column.table
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
                    want_domain: !ambiguous && default && family.is_some(),
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
    changes.into_values().flatten().collect()
}

/// Refresh persisted bindings, avoiding a write when no reconciliation is needed.
pub fn refresh(
    store: &dyn Store,
    route: &RouteId,
    doc: &PolicyDocument,
    snapshot: &Snapshot,
) -> Result<RouteBindings, BindingError> {
    let bindings = load(store, route)?;
    let changes = reconcile(&bindings, doc, snapshot);
    if changes.is_empty() {
        return Ok(bindings);
    }
    let mut reserved: BTreeSet<_> = doc.policy.domains.keys().cloned().collect();
    for treatment in doc.policy.columns.values() {
        match treatment {
            Treatment::Blind { domain } => {
                reserved.insert(domain.clone());
            }
            Treatment::Show | Treatment::Masked { .. } => {}
        }
    }
    apply(store, route, &changes, &reserved)
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
}
