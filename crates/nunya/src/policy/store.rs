//! Atomic, immutable policy revisions with route ownership and retry safety.
use crate::state::{
    model::{RouteId, decode, encode},
    schema::Table,
    store::{ReadTxn, Store, StoreError},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Authority responsible for a route's policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Owner {
    /// Policy comes from its configured file.
    File,
    /// Policy comes from the revision service.
    Service,
}
/// Operation that produced a revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RevisionKind {
    /// Transfer file ownership to the service.
    Import,
    /// Apply a new document.
    Apply,
    /// Restore an earlier document as a new revision.
    Rollback {
        /// Revision restored.
        from: u64,
    },
}

/// NewRevision metadata.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRevision {
    /// Exported policy TOML.
    pub policy_toml: String,
    /// SHA-256 of the policy text.
    pub policy_sha256: String,
    /// Catalog fingerprint used for validation.
    pub catalog_fingerprint: String,
    /// Actor responsible for this revision.
    pub actor: String,
    /// Operation that created this revision.
    pub kind: RevisionKind,
    /// Optional route-scoped retry key.
    pub idempotency_key: Option<String>,
    /// Digest of the original request.
    pub request_digest: Option<String>,
}
impl std::fmt::Debug for NewRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewRevision")
            .field("policy_sha256", &self.policy_sha256)
            .finish_non_exhaustive()
    }
}

/// PolicyRevision metadata.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRevision {
    /// Monotonically increasing route revision.
    pub number: u64,
    /// Owning route.
    pub route: RouteId,
    /// Commit time in Unix milliseconds.
    pub created_unix_ms: u64,
    /// Exported policy TOML.
    pub policy_toml: String,
    /// SHA-256 of the policy text.
    pub policy_sha256: String,
    /// Catalog fingerprint used for validation.
    pub catalog_fingerprint: String,
    /// Actor responsible for this revision.
    pub actor: String,
    /// Operation that created this revision.
    pub kind: RevisionKind,
    /// Optional route-scoped retry key.
    pub idempotency_key: Option<String>,
    /// Digest of the original request.
    pub request_digest: Option<String>,
}
impl std::fmt::Debug for PolicyRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyRevision")
            .field("policy_sha256", &self.policy_sha256)
            .finish_non_exhaustive()
    }
}

/// RevisionSummary metadata.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionSummary {
    /// Monotonically increasing route revision.
    pub number: u64,
    /// Owning route.
    pub route: RouteId,
    /// Commit time in Unix milliseconds.
    pub created_unix_ms: u64,
    /// SHA-256 of the policy text.
    pub policy_sha256: String,
    /// Catalog fingerprint used for validation.
    pub catalog_fingerprint: String,
    /// Actor responsible for this revision.
    pub actor: String,
    /// Operation that created this revision.
    pub kind: RevisionKind,
    /// Optional route-scoped retry key.
    pub idempotency_key: Option<String>,
    /// Digest of the original request.
    pub request_digest: Option<String>,
}
impl std::fmt::Debug for RevisionSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RevisionSummary")
            .field("policy_sha256", &self.policy_sha256)
            .finish_non_exhaustive()
    }
}

impl From<&PolicyRevision> for RevisionSummary {
    fn from(r: &PolicyRevision) -> Self {
        Self {
            number: r.number,
            route: r.route.clone(),
            created_unix_ms: r.created_unix_ms,
            policy_sha256: r.policy_sha256.clone(),
            catalog_fingerprint: r.catalog_fingerprint.clone(),
            actor: r.actor.clone(),
            kind: r.kind.clone(),
            idempotency_key: r.idempotency_key.clone(),
            request_digest: r.request_digest.clone(),
        }
    }
}
/// Result of an atomic revision write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// New revision committed.
    Written(PolicyRevision),
    /// Previously committed request replayed.
    Replayed(PolicyRevision),
    /// Active revision differs from the expected number.
    Stale {
        /// Current active revision, if any.
        current: Option<PolicyRevision>,
    },
}
/// Data-free revision storage failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RevisionStoreError {
    /// Route cannot be represented by a Kubernetes DNS label.
    #[error("policy route is not a valid Kubernetes DNS label")]
    InvalidRoute,
    /// Serialized Kubernetes policy head exceeds one MiB.
    #[error("policy revision exceeds the 1 MiB Kubernetes head limit")]
    Oversized,
    /// Storage cannot be accessed.
    #[error("policy revision store unavailable")]
    Unavailable,
    /// Persisted metadata is invalid.
    #[error("policy revision store corrupt")]
    Corrupt,
    /// A retry key was used for different input.
    #[error("policy idempotency conflict")]
    IdempotencyConflict,
}
impl From<StoreError> for RevisionStoreError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Corrupt => Self::Corrupt,
            _ => Self::Unavailable,
        }
    }
}
/// Narrow durable policy backend shared by local and remote implementations.
pub trait RevisionStore: Send + Sync {
    /// Read the active revision.
    fn active(&self, route: &RouteId) -> Result<Option<PolicyRevision>, RevisionStoreError>;
    /// Compare and set the active revision, replaying retry keys first.
    fn write(
        &self,
        route: &RouteId,
        expected: Option<u64>,
        draft: NewRevision,
    ) -> Result<WriteOutcome, RevisionStoreError>;
    /// List immutable revisions in ascending order.
    fn history(&self, route: &RouteId) -> Result<Vec<RevisionSummary>, RevisionStoreError>;
    /// Read one immutable revision.
    fn get(
        &self,
        route: &RouteId,
        number: u64,
    ) -> Result<Option<PolicyRevision>, RevisionStoreError>;
    /// Return ownership to the configured file while retaining all revisions.
    fn release_to_file(&self, route: &RouteId) -> Result<(), RevisionStoreError>;
    /// Read route ownership, defaulting to file ownership.
    fn owner(&self, route: &RouteId) -> Result<Owner, RevisionStoreError>;
}
#[derive(Serialize, Deserialize)]
struct Head {
    active: u64,
    owner: Owner,
}
#[derive(Serialize, Deserialize)]
struct Retry {
    number: u64,
    request_digest: Option<String>,
}
/// Revision backend over the gateway's transactional state store.
pub struct LocalRevisionStore {
    store: Arc<dyn Store>,
}
impl LocalRevisionStore {
    /// Share the runtime's existing state store.
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}
// RouteId permits only lowercase ASCII letters, digits and hyphens, never NUL.
fn prefix(route: &RouteId) -> Vec<u8> {
    let mut k = route.as_str().as_bytes().to_vec();
    k.push(0);
    k
}
fn key(route: &RouteId, n: u64) -> Vec<u8> {
    let mut k = prefix(route);
    k.extend(n.to_be_bytes());
    k
}
fn record<T: serde::de::DeserializeOwned>(
    tx: &dyn ReadTxn,
    table: Table,
    key: &[u8],
) -> Result<Option<T>, RevisionStoreError> {
    tx.get(table, key)?
        .map(|v| decode(&v).map_err(|_| RevisionStoreError::Corrupt))
        .transpose()
}
fn head(tx: &dyn ReadTxn, route: &RouteId) -> Result<Option<Head>, RevisionStoreError> {
    record(tx, Table::PolicyHeads, route.as_str().as_bytes())
}
fn revision(
    tx: &dyn ReadTxn,
    route: &RouteId,
    n: u64,
) -> Result<Option<PolicyRevision>, RevisionStoreError> {
    let r: Option<PolicyRevision> = record(tx, Table::PolicyRevisions, &key(route, n))?;
    if r.as_ref()
        .is_some_and(|r| r.route != *route || r.number != n || n == 0)
    {
        return Err(RevisionStoreError::Corrupt);
    }
    Ok(r)
}
fn active(tx: &dyn ReadTxn, route: &RouteId) -> Result<Option<PolicyRevision>, RevisionStoreError> {
    head(tx, route)?
        .map(|h| revision(tx, route, h.active)?.ok_or(RevisionStoreError::Corrupt))
        .transpose()
}
impl RevisionStore for LocalRevisionStore {
    fn release_to_file(&self, route: &RouteId) -> Result<(), RevisionStoreError> {
        let mut tx = self.store.write()?;
        if let Some(mut h) = head(tx.as_ref(), route)? {
            h.owner = Owner::File;
            tx.put(Table::PolicyHeads, route.as_str().as_bytes(), &encode(&h))?;
        }
        tx.commit()?;
        Ok(())
    }

    fn active(&self, route: &RouteId) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        active(self.store.read()?.as_ref(), route)
    }
    fn owner(&self, route: &RouteId) -> Result<Owner, RevisionStoreError> {
        Ok(head(self.store.read()?.as_ref(), route)?.map_or(Owner::File, |h| h.owner))
    }
    fn get(&self, route: &RouteId, n: u64) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        revision(self.store.read()?.as_ref(), route, n)
    }
    fn history(&self, route: &RouteId) -> Result<Vec<RevisionSummary>, RevisionStoreError> {
        let tx = self.store.read()?;
        tx.scan_prefix(Table::PolicyRevisions, &prefix(route))?
            .into_iter()
            .map(|(k, v)| {
                let r: PolicyRevision = decode(&v).map_err(|_| RevisionStoreError::Corrupt)?;
                if r.route != *route || r.number == 0 || k != key(route, r.number) {
                    return Err(RevisionStoreError::Corrupt);
                }
                Ok(RevisionSummary::from(&r))
            })
            .collect()
    }
    fn write(
        &self,
        route: &RouteId,
        expected: Option<u64>,
        draft: NewRevision,
    ) -> Result<WriteOutcome, RevisionStoreError> {
        let mut tx = self.store.write()?;
        let retry_key = draft.idempotency_key.as_ref().map(|s| {
            let mut k = prefix(route);
            k.extend(s.as_bytes());
            k
        });
        if let Some(k) = &retry_key
            && let Some(retry) = record::<Retry>(tx.as_ref(), Table::PolicyIdempotency, k)?
        {
            if retry.request_digest != draft.request_digest {
                return Err(RevisionStoreError::IdempotencyConflict);
            }
            return Ok(WriteOutcome::Replayed(
                revision(tx.as_ref(), route, retry.number)?.ok_or(RevisionStoreError::Corrupt)?,
            ));
        }
        let current = active(tx.as_ref(), route)?;
        if current.as_ref().map(|r| r.number) != expected {
            return Ok(WriteOutcome::Stale { current });
        }
        let number = current
            .as_ref()
            .map_or(0, |r| r.number)
            .checked_add(1)
            .ok_or(RevisionStoreError::Corrupt)?;
        let owner = if draft.kind == RevisionKind::Import {
            Owner::Service
        } else {
            head(tx.as_ref(), route)?.map_or(Owner::File, |h| h.owner)
        };
        let r = PolicyRevision {
            number,
            route: route.clone(),
            created_unix_ms: self.store.now_ms(),
            policy_toml: draft.policy_toml,
            policy_sha256: draft.policy_sha256,
            catalog_fingerprint: draft.catalog_fingerprint,
            actor: draft.actor,
            kind: draft.kind,
            idempotency_key: draft.idempotency_key,
            request_digest: draft.request_digest,
        };
        if tx
            .get(Table::PolicyRevisions, &key(route, number))?
            .is_some()
        {
            return Err(RevisionStoreError::Corrupt);
        }
        tx.put(Table::PolicyRevisions, &key(route, number), &encode(&r))?;
        tx.put(
            Table::PolicyHeads,
            route.as_str().as_bytes(),
            &encode(&Head {
                active: number,
                owner,
            }),
        )?;
        if let Some(k) = retry_key {
            tx.put(
                Table::PolicyIdempotency,
                &k,
                &encode(&Retry {
                    number,
                    request_digest: r.request_digest.clone(),
                }),
            )?;
        }
        tx.commit()?;
        Ok(WriteOutcome::Written(r))
    }
}

/// Validate revision rows during the authority's exhaustive startup scan.
pub(crate) fn validate_record(
    table: Table,
    k: &[u8],
    bytes: &[u8],
) -> Result<(), RevisionStoreError> {
    let corrupt = || RevisionStoreError::Corrupt;
    match table {
        Table::PolicyRevisions => {
            let r: PolicyRevision = decode(bytes).map_err(|_| corrupt())?;
            if r.number == 0 || k != key(&r.route, r.number) {
                return Err(corrupt());
            }
        }
        Table::PolicyHeads => {
            let h: Head = decode(bytes).map_err(|_| corrupt())?;
            if h.active == 0 || std::str::from_utf8(k).ok().and_then(RouteId::new).is_none() {
                return Err(corrupt());
            }
        }
        Table::PolicyIdempotency => {
            let r: Retry = decode(bytes).map_err(|_| corrupt())?;
            let separator = k.iter().position(|b| *b == 0).ok_or_else(corrupt)?;
            if r.number == 0
                || std::str::from_utf8(&k[..separator])
                    .ok()
                    .and_then(RouteId::new)
                    .is_none()
                || std::str::from_utf8(&k[separator + 1..]).is_err()
            {
                return Err(corrupt());
            }
        }
        _ => return Err(corrupt()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fixture {
        path: std::path::PathBuf,
        store: Arc<LocalRevisionStore>,
    }
    impl Fixture {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let path = std::env::temp_dir().join(format!(
                "policy-store-{}",
                data_encoding::HEXLOWER.encode(&nonce)
            ));
            let root = crate::token::keys::RootKey::from_hex(&"01".repeat(32)).unwrap();
            let store = Arc::new(LocalRevisionStore::new(Arc::new(
                crate::state::store::RedbStore::open(&path, &root).unwrap(),
            )));
            Self { path, store }
        }
    }
    fn route() -> RouteId {
        RouteId::new("a").unwrap()
    }
    fn draft(key: Option<&str>, digest: &str) -> NewRevision {
        NewRevision {
            policy_toml: "secret-policy-text".into(),
            policy_sha256: "sha".into(),
            catalog_fingerprint: "catalog".into(),
            actor: "test".into(),
            kind: RevisionKind::Import,
            idempotency_key: key.map(str::to_owned),
            request_digest: Some(digest.into()),
        }
    }
    fn written(o: WriteOutcome) -> PolicyRevision {
        match o {
            WriteOutcome::Written(r) => r,
            _ => panic!("expected new revision"),
        }
    }
    #[test]
    fn release_keeps_history_and_allows_reimport() {
        let f = Fixture::new(); let s = &f.store; let r = route();
        let first = written(s.write(&r, None, draft(None, "one")).unwrap());
        s.release_to_file(&r).unwrap();
        assert_eq!(s.owner(&r).unwrap(), Owner::File);
        assert_eq!(s.active(&r).unwrap(), Some(first.clone()));
        assert_eq!(s.get(&r, 1).unwrap(), Some(first));
        assert_eq!(s.history(&r).unwrap().len(), 1);
        assert_eq!(written(s.write(&r, Some(1), draft(None, "two")).unwrap()).number, 2);
        assert_eq!(s.owner(&r).unwrap(), Owner::Service);
    }
    #[test]
    fn cas_history_owner_and_redacted_debug() {
        let f = Fixture::new();
        let s = &f.store;
        let r = route();
        assert!(f.path.exists());
        assert_eq!(s.owner(&r).unwrap(), Owner::File);
        assert!(s.active(&r).unwrap().is_none());
        let first = written(s.write(&r, None, draft(None, "one")).unwrap());
        assert_eq!(first.number, 1);
        assert_eq!(s.owner(&r).unwrap(), Owner::Service);
        assert!(!format!("{first:?}").contains("secret-policy-text"));
        assert!(matches!(
            s.write(&r, None, draft(None, "wrong")).unwrap(),
            WriteOutcome::Stale { .. }
        ));
        assert_eq!(s.history(&r).unwrap().len(), 1);
        let second = written(s.write(&r, Some(1), draft(None, "two")).unwrap());
        assert_eq!(second.number, 2);
        assert_eq!(s.get(&r, 1).unwrap(), Some(first));
        assert_eq!(s.active(&r).unwrap(), Some(second));
        assert_eq!(
            s.history(&r)
                .unwrap()
                .iter()
                .map(|r| r.number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(s.get(&r, 3).unwrap().is_none());
        assert!(s.history(&RouteId::new("a-b").unwrap()).unwrap().is_empty());
    }
    #[test]
    fn idempotency_precedes_stale_and_conflicts_are_atomic() {
        let f = Fixture::new();
        let r = route();
        let first = written(f.store.write(&r, None, draft(Some("key"), "one")).unwrap());
        assert_eq!(
            f.store.write(&r, None, draft(Some("key"), "one")).unwrap(),
            WriteOutcome::Replayed(first)
        );
        assert_eq!(
            f.store.write(&r, Some(1), draft(Some("key"), "two")),
            Err(RevisionStoreError::IdempotencyConflict)
        );
        assert_eq!(f.store.history(&r).unwrap().len(), 1);
    }
    #[test]
    fn concurrent_cas_has_one_winner() {
        let f = Fixture::new();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let s = f.store.clone();
                let b = barrier.clone();
                std::thread::spawn(move || {
                    b.wait();
                    s.write(&route(), None, draft(None, "same")).unwrap()
                })
            })
            .collect();
        let outcomes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| matches!(o, WriteOutcome::Written(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| matches!(o, WriteOutcome::Stale { .. }))
                .count(),
            1
        );
    }
    #[test]
    fn tables_only_appended_and_old_state_opens() {
        let original = [
            "principals",
            "principal_credentials",
            "grants",
            "sessions",
            "session_credentials",
            "scopes",
            "session_counters",
            "principal_counters",
            "idempotency",
            "domain_bindings",
            "domain_names",
            "retired_domains",
            "membership_resets",
        ];
        assert_eq!(
            Table::ALL[..13]
                .iter()
                .map(|t| t.name())
                .collect::<Vec<_>>(),
            original
        );
        let f = Fixture::new();
        // Simulate an older state database by removing only the new tables.
        let path = f.path.clone();
        drop(f);
        let db = redb::Database::create(path.join(crate::state::schema::STORE_FILE)).unwrap();
        let tx = db.begin_write().unwrap();
        for table in &Table::ALL[13..] {
            tx.delete_table(table.definition()).unwrap();
        }
        tx.commit().unwrap();
        drop(db);
        let root = crate::token::keys::RootKey::from_hex(&"01".repeat(32)).unwrap();
        let s = LocalRevisionStore::new(Arc::new(
            crate::state::store::RedbStore::open(&path, &root).unwrap(),
        ));
        assert_eq!(s.owner(&route()).unwrap(), Owner::File);
        assert!(s.active(&route()).unwrap().is_none());
        drop(s);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn corrupt_records_and_failed_commits_are_data_free() {
        let f = Fixture::new();
        let mut tx = f.store.store.write().unwrap();
        tx.put(Table::PolicyHeads, b"a", b"invalid secret data")
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(f.store.active(&route()), Err(RevisionStoreError::Corrupt));
        assert!(!format!("{:?}", f.store.active(&route())).contains("secret"));
    }
    #[test]
    fn catalog_fingerprint_is_order_independent_and_tracks_identity() {
        use crate::schema::{Column, ColumnId, ColumnRef, Relation, RelationKind, Snapshot};
        let relation = Relation {
            schema: "public".into(),
            name: "t".into(),
            kind: RelationKind::Table,
            columns: vec![
                Column {
                    name: "a".into(),
                    ty: crate::value::ColumnType::Text,
                    nullable: false,
                    generated: None,
                    default: None,
                },
                Column {
                    name: "b".into(),
                    ty: crate::value::ColumnType::Text,
                    nullable: false,
                    generated: None,
                    default: None,
                },
            ],
            view_definition: None,
            primary_key: vec![],
        };
        let a = Snapshot::new(vec![relation], vec![], vec![], 0);
        let mut b = a.clone();
        b.relations[0].columns.reverse();
        b.taken_at_unix_ms = 99;
        assert_eq!(
            crate::schema::catalog_fingerprint(&a),
            crate::schema::catalog_fingerprint(&b)
        );
        b.column_ids.insert(
            ColumnRef {
                schema: "public".into(),
                table: "t".into(),
                column: "a".into(),
            },
            ColumnId::Pg {
                relation_oid: 1,
                attnum: 1,
            },
        );
        assert_ne!(
            crate::schema::catalog_fingerprint(&a),
            crate::schema::catalog_fingerprint(&b)
        );
    }
}
