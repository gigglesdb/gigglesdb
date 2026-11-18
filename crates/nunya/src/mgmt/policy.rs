//! Administrative policy management boundary.
use super::{
    audit::{Actor, LifecycleKind, LifecycleRecord, PolicyAudit},
    auth::{Caller, Origin},
    routes::{MgmtContext, error, idempotency_key, json_response},
};
use crate::{
    policy::{
        Owner, PolicyDocument, PolicyRevision, RevisionSummary,
        bindings::RouteBindings,
        diff::{self, PolicyDiff},
    },
    schema::Snapshot,
    state::model::RouteId,
};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
/// Data-free policy operation refusal.
#[derive(Debug)]
pub enum PolicyError {
    /// Fixed public code and HTTP status.
    Code(u16, &'static str),
    /// Invalid draft messages.
    Invalid(Vec<String>),
    /// Revision precondition failure.
    Stale(Box<RevisionSummary>),
    /// Catalog precondition failure.
    CatalogChanged(String),
}
impl From<crate::reload::PolicyOpError> for PolicyError {
    fn from(e: crate::reload::PolicyOpError) -> Self {
        use crate::{policy::RevisionStoreError, reload::PolicyOpError as E};
        match e {
            E::UnknownRoute | E::MissingRevision => Self::Code(404, "not_found"),
            E::SharedPolicyFile => Self::Code(409, "shared_policy_file"),
            E::FileOwnerConfigured => Self::Code(409, "file_owner_configured"),
            E::AlreadyServiceOwned => Self::Code(409, "already_service_owned"),
            E::NotServiceOwned => Self::Code(409, "not_service_owned"),
            E::InfrastructureSetting => Self::Code(422, "infrastructure_setting"),
            E::Invalid(m) => Self::Invalid(m),
            E::Stale { current } => Self::Stale(Box::new(current)),
            E::CatalogChanged { current } => Self::CatalogChanged(current),
            E::Store(RevisionStoreError::IdempotencyConflict) => {
                Self::Code(409, "idempotency_conflict")
            }
            E::Unavailable
            | E::Store(RevisionStoreError::Unavailable | RevisionStoreError::Corrupt) => {
                Self::Code(503, "policy_unavailable")
            }
        }
    }
}
/// Result of an atomic management mutation or retry.
pub struct PolicyWrite {
    /// Published or replayed revision.
    pub revision: PolicyRevision,
    /// Whether this request was already committed.
    pub replayed: bool,
}
/// Runtime adapter for policy management.
pub trait PolicySource: Send + Sync {
    /// Actually enforced revision, if service-owned.
    fn published(&self, r: &RouteId) -> Result<Option<u64>, PolicyError> { Ok(self.active(r)?.map(|r| r.number)) }
    /// Validate a draft without persistence.
    fn validate(&self, r: &RouteId, toml: &str) -> Result<PolicyDocument, PolicyError> {
        let (doc, snapshot, bindings) = self.effective(r)?;
        let draft = parse(toml, &snapshot, &bindings).map_err(|messages| {
            if !messages.is_empty() && messages.iter().all(|m| (m.starts_with("readlog:") || m.starts_with("snapshot_max_age_ms:")) && m.ends_with("moved to the service config")) { PolicyError::Code(422, "infrastructure_setting") } else { PolicyError::Invalid(messages) }
        })?;
        if std::mem::discriminant(&draft.policy.readlog) != std::mem::discriminant(&doc.policy.readlog) || draft.policy.snapshot_max_age_ms != doc.policy.snapshot_max_age_ms { return Err(PolicyError::Code(422, "infrastructure_setting")); }
        let policy = crate::policy::effective::served_policy(&draft, &snapshot, &bindings);
        crate::policy::validate::validate_dependents(&policy, &snapshot).map_err(|e| PolicyError::Invalid(e.into_iter().map(|e| e.message).collect()))?;
        Ok(draft)
    }
    /// Policy owner.
    fn owner(&self, r: &RouteId) -> Result<Owner, PolicyError>;
    /// Active durable revision.
    fn active(&self, r: &RouteId) -> Result<Option<PolicyRevision>, PolicyError>;
    /// Ordered revision history.
    fn history(&self, r: &RouteId) -> Result<Vec<RevisionSummary>, PolicyError>;
    /// One historical revision.
    fn get(&self, r: &RouteId, n: u64) -> Result<Option<PolicyRevision>, PolicyError>;
    /// Published document, catalog and read-only bindings.
    fn effective(
        &self,
        r: &RouteId,
    ) -> Result<(Arc<PolicyDocument>, Arc<Snapshot>, RouteBindings), PolicyError>;
    /// Published catalog fingerprint.
    fn fingerprint(&self, r: &RouteId) -> Result<String, PolicyError>;
    /// Published policy source, including file ownership.
    fn export(&self, r: &RouteId) -> Result<String, PolicyError>;
    /// Import file ownership.
    fn import(&self, r: &RouteId, actor: &str, key: String) -> Result<PolicyWrite, PolicyError>;
    /// Publish a draft.
    fn apply(
        &self,
        r: &RouteId,
        actor: &str,
        toml: &str,
        expected: u64,
        fingerprint: &str,
        key: String,
    ) -> Result<PolicyWrite, PolicyError>;
    /// Restore a historical revision.
    fn rollback(
        &self,
        r: &RouteId,
        actor: &str,
        to: u64,
        expected: u64,
        key: String,
    ) -> Result<PolicyWrite, PolicyError>;
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Draft {
    policy_toml: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Apply {
    policy_toml: String,
    expected_revision: u64,
    catalog_fingerprint: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rollback {
    to: u64,
    expected_revision: u64,
}
/// Recognize only declared policy endpoints.
pub(super) fn endpoint(path: &str) -> Option<(RouteId, &str, Option<u64>)> {
    let p: Vec<_> = path.split('/').collect();
    let (id, op, n) = match p.as_slice() {
        ["", "v1", "routes", id, "policy"] => (*id, "", None),
        [
            "",
            "v1",
            "routes",
            id,
            "policy",
            op @ ("effective" | "validate" | "preview" | "apply" | "rollback" | "import"
            | "revisions" | "export"),
        ] => (*id, *op, None),
        ["", "v1", "routes", id, "policy", "revisions", n] => {
            (*id, "revisions", Some(n.parse().ok()?))
        }
        _ => return None,
    };
    Some((RouteId::new(id)?, op, n))
}
fn invalid(m: Vec<String>) -> Response<Bytes> {
    json_response(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"error":"invalid_policy","messages":m}),
    )
}
fn parse(t: &str, s: &Snapshot, b: &RouteBindings) -> Result<PolicyDocument, Vec<String>> {
    let d = crate::policy::file::parse_policy_document(t)
        .map_err(|e| e.into_iter().map(|e| e.message).collect::<Vec<_>>())?;
    crate::policy::validate::validate_document(&d, s, b)
        .map_err(|e| e.into_iter().map(|e| e.message).collect::<Vec<_>>())?;
    Ok(d)
}
fn refusal(e: PolicyError, d: &PolicyDiff) -> (&'static str, Response<Bytes>) {
    match e {
        PolicyError::Code(s, c) => (
            c,
            error(
                StatusCode::from_u16(s).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                c,
            ),
        ),
        PolicyError::Invalid(m) => ("invalid_policy", invalid(m)),
        PolicyError::Stale(current) => (
            "stale_revision",
            json_response(
                StatusCode::CONFLICT,
                json!({"error":"stale_revision","current":current,"diff":d}),
            ),
        ),
        PolicyError::CatalogChanged(current) => (
            "catalog_changed",
            json_response(
                StatusCode::CONFLICT,
                json!({"error":"catalog_changed","catalog_fingerprint":current}),
            ),
        ),
    }
}
/// Dispatch a recognized policy request after authentication.
pub(super) fn handle(
    ctx: &MgmtContext,
    caller: &Caller,
    origin: Origin,
    req: &Request<Bytes>,
    route: RouteId,
    op: &str,
    n: Option<u64>,
) -> Response<Bytes> {
    let mutation = matches!(op, "apply" | "rollback" | "import");
    let post = mutation || matches!(op, "validate" | "preview");
    if req.method().as_str() != if post { "POST" } else { "GET" } {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    }
    if !caller.admin {
        return error(StatusCode::FORBIDDEN, "forbidden");
    }
    let Some(source) = ctx.policy.as_ref() else {
        return error(StatusCode::NOT_FOUND, "not_found");
    };
    let result = (|| -> Result<Response<Bytes>, PolicyError> {
        let (doc, snapshot, bindings) = source.effective(&route)?;
        let active = source.active(&route)?;
        let fp = source.fingerprint(&route)?;
        let ok = |v: Value| json_response(StatusCode::OK, v);
        match op {
            "" => {
                return Ok(ok(
                    json!({"owner":match source.owner(&route)? {Owner::File=>"file",Owner::Service=>"service"},"published_revision":source.published(&route)?,"active_revision":active.as_ref().map(RevisionSummary::from),"catalog_fingerprint":fp}),
                ));
            }
            "effective" => {
                return Ok(ok(
                    json!({"columns":diff::views(&doc,&snapshot,&bindings,false).into_iter().map(|(c,v)|{let mut value=json!(v);value["schema"]=json!(c.schema);value["table"]=json!(c.table);value["column"]=json!(c.column);value["type"]=json!(snapshot.column(&c).map(|c|format!("{:?}",c.ty)));value}).collect::<Vec<_>>()}),
                ));
            }
            "revisions" => {
                return Ok(if let Some(n) = n {
                    match source.get(&route, n)? {
                        Some(r) => {
                            let mut v = json!(RevisionSummary::from(&r));
                            v["policy_toml"] = json!(r.policy_toml);
                            ok(v)
                        }
                        None => error(StatusCode::NOT_FOUND, "not_found"),
                    }
                } else {
                    ok(json!(source.history(&route)?))
                });
            }
            "export" => {
                return Ok(ok(
                    json!({"revision":active.as_ref().map(RevisionSummary::from),"policy_toml":source.export(&route)?}),
                ));
            }
            "validate" | "preview" => {
                let body: Draft = match serde_json::from_slice(req.body()) {
                    Ok(b) => b,
                    Err(_) => return Ok(error(StatusCode::BAD_REQUEST, "bad_request")),
                };
                let draft = source.validate(&route, &body.policy_toml)?;
                return Ok(if op == "validate" {
                    ok(json!({"valid":true,"catalog_fingerprint":fp}))
                } else {
                    let mut v = json!(diff::diff(&doc, &draft, &snapshot, &bindings));
                    v["from_revision"] = json!(active.as_ref().map(|r| r.number));
                    v["catalog_fingerprint"] = json!(fp);
                    ok(v)
                });
            }
            "apply" | "rollback" | "import" => {}
            _ => return Ok(error(StatusCode::NOT_FOUND, "not_found")),
        }
        let Some(key) = idempotency_key(req.headers()) else {
            return Ok(error(StatusCode::BAD_REQUEST, "idempotency_key_required"));
        };
        let actor = super::routes::idempotency_owner(caller, origin).to_hex();
        let mut delta = PolicyDiff::default();
        let mut draft_document = None;
        let outcome = match op {
            "apply" => {
                let b: Apply = match serde_json::from_slice(req.body()) {
                    Ok(b) => b,
                    Err(_) => return Ok(error(StatusCode::BAD_REQUEST, "bad_request")),
                };
                if let Ok(d) = parse(&b.policy_toml, &snapshot, &bindings) {
                    delta = diff::diff(&doc, &d, &snapshot, &bindings);
                    draft_document = Some(d);
                }
                source.apply(
                    &route,
                    &actor,
                    &b.policy_toml,
                    b.expected_revision,
                    &b.catalog_fingerprint,
                    key.into(),
                )
            }
            "rollback" => {
                let b: Rollback = match serde_json::from_slice(req.body()) {
                    Ok(b) => b,
                    Err(_) => return Ok(error(StatusCode::BAD_REQUEST, "bad_request")),
                };
                if let Some(r) = source.get(&route, b.to)?
                    && let Ok(d) = parse(&r.policy_toml, &snapshot, &bindings) {
                    delta = diff::diff(&doc, &d, &snapshot, &bindings);
                    draft_document = Some(d);
                }
                source.rollback(&route, &actor, b.to, b.expected_revision, key.into())
            }
            "import" => {
                if !req.body().is_empty()
                    && serde_json::from_slice::<Value>(req.body()).ok() != Some(json!({}))
                {
                    return Ok(error(StatusCode::BAD_REQUEST, "bad_request"));
                }
                source.import(&route, &actor, key.into())
            }
            _ => return Ok(error(StatusCode::NOT_FOUND, "not_found")),
        };
        let (revision, code, response) = match outcome {
            Ok(PolicyWrite {
                revision: r,
                replayed,
            }) => {
                if op != "import" && r.number > 1 {
                    let before = source.get(&route, r.number - 1)?.ok_or(PolicyError::Code(503, "policy_unavailable"))?;
                    let before = crate::policy::file::parse_policy_document(&before.policy_toml).map_err(|_| PolicyError::Code(503, "policy_unavailable"))?;
                    let after = crate::policy::file::parse_policy_document(&r.policy_toml).map_err(|_| PolicyError::Code(503, "policy_unavailable"))?;
                    delta = diff::diff(&before, &after, &snapshot, &bindings);
                }
                (Some(r.number),
                "active",
                ok(json!({"revision":RevisionSummary::from(&r),"replayed":replayed})),
            )},
            Err(e) => {
                if matches!(&e, PolicyError::Stale(_))
                    && let (Some(draft), Ok((current, catalog, bindings))) =
                        (draft_document.as_ref(), source.effective(&route)) {
                    delta = diff::diff(&current, draft, &catalog, &bindings);
                }
                let (code, response) = refusal(e, &delta);
                (None, code, response)
            }
        };
        // Publication precedes audit: if audit fails, a successful revision is already active.
        let record = LifecycleRecord {
            kind: match op {
                "import" => LifecycleKind::PolicyImported,
                "apply" => LifecycleKind::PolicyApplied,
                "rollback" => LifecycleKind::PolicyRolledBack,
                _ => return Ok(error(StatusCode::NOT_FOUND, "not_found")),
            },
            at_ms: ctx.authority.now_ms(),
            principal: caller.principal.map(|p| p.to_hex()).unwrap_or_default(),
            session: None,
            route: Some(route.as_str().into()),
            actor: match caller.principal {
                Some(p) => Actor::Admin(p.to_hex()),
                None if origin == Origin::Socket => Actor::Socket,
                None => Actor::ConfigAdmin,
            },
            reason: code,
            policy: Some(PolicyAudit {
                revision,
                outcome: code,
                summary: delta.summary,
                newly_visible: column_names(&delta.newly_visible),
                newly_masked: column_names(&delta.newly_masked),
            }),
        };
        if ctx.lifecycle.record(&record).is_err() {
            return Ok(error(StatusCode::SERVICE_UNAVAILABLE, "audit_unavailable"));
        }
        Ok(response)
    })();
    match result {
        Ok(r) => r,
        Err(e) => refusal(e, &PolicyDiff::default()).1,
    }
}
fn column_names(entries: &[Value]) -> Vec<Value> {
    entries
        .iter()
        .map(|v| json!({"schema":v["schema"],"table":v["table"],"column":v["column"]}))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mgmt::{
            audit::{AuditUnavailable, LifecycleSink},
            routes::{self, ConnectionSource, ConnectionView, StatusSource},
        },
        policy::{RevisionKind, file::parse_policy_document},
        state::{
            authority::{Authority, SessionLimits},
            store::RedbStore,
        },
        token::{SessionId, keys::RootKey},
    };
    use std::sync::{
        Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Fake {
        mode: AtomicUsize,
        calls: AtomicUsize,
    }
    impl Fake {
        fn revision(&self) -> PolicyRevision {
            PolicyRevision {
                number: 1,
                route: RouteId::new("route").unwrap(),
                created_unix_ms: 0,
                policy_toml: "version=2\ndefault='tokenize'\n".into(),
                policy_sha256: "hash".into(),
                catalog_fingerprint: "fingerprint".into(),
                actor: "actor".into(),
                kind: RevisionKind::Apply,
                idempotency_key: Some("key".into()),
                request_digest: None,
            }
        }
    }
    impl PolicySource for Fake {
        fn owner(&self, _: &RouteId) -> Result<Owner, PolicyError> {
            Ok(Owner::Service)
        }
        fn active(&self, _: &RouteId) -> Result<Option<PolicyRevision>, PolicyError> {
            Ok(Some(self.revision()))
        }
        fn history(&self, _: &RouteId) -> Result<Vec<RevisionSummary>, PolicyError> {
            Ok(vec![RevisionSummary::from(&self.revision())])
        }
        fn get(&self, _: &RouteId, n: u64) -> Result<Option<PolicyRevision>, PolicyError> {
            if self.mode.load(Ordering::SeqCst) == 6 && (1..=2).contains(&n) {
                let mut r = self.revision(); r.number = n;
                if n == 2 { r.policy_toml = "version=2\ndefault='show'\n".into(); }
                return Ok(Some(r));
            }
            Ok((n == 1).then(|| self.revision()))
        }
        fn effective(
            &self,
            r: &RouteId,
        ) -> Result<(Arc<PolicyDocument>, Arc<Snapshot>, RouteBindings), PolicyError> {
            if r.as_str() != "route" {
                return Err(PolicyError::Code(404, "not_found"));
            }
            use crate::{
                schema::{Column, Relation, RelationKind},
                value::ColumnType,
            };
            let s = Snapshot::new(
                vec![Relation {
                    schema: "public".into(),
                    name: "accounts".into(),
                    kind: RelationKind::Table,
                    columns: vec![Column {
                        name: "distinctive_column_name".into(),
                        ty: ColumnType::Text,
                        nullable: true,
                        generated: None,
                        default: None,
                    }],
                    primary_key: vec![],
                    view_definition: None,
                }],
                vec![],
                vec![],
                0,
            );
            Ok((
                Arc::new(parse_policy_document("version=2\ndefault='tokenize'\n").unwrap()),
                Arc::new(s),
                RouteBindings::default(),
            ))
        }
        fn fingerprint(&self, _: &RouteId) -> Result<String, PolicyError> {
            Ok("fingerprint".into())
        }
        fn export(&self, _: &RouteId) -> Result<String, PolicyError> {
            Ok(self.revision().policy_toml)
        }
        fn import(&self, r: &RouteId, a: &str, k: String) -> Result<PolicyWrite, PolicyError> {
            self.apply(r, a, "", 0, "", k)
        }
        fn rollback(
            &self,
            r: &RouteId,
            a: &str,
            _: u64,
            e: u64,
            k: String,
        ) -> Result<PolicyWrite, PolicyError> {
            self.apply(r, a, "", e, "", k)
        }
        fn apply(
            &self,
            _: &RouteId,
            _: &str,
            _: &str,
            _: u64,
            _: &str,
            _: String,
        ) -> Result<PolicyWrite, PolicyError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                0 => Err(PolicyError::Stale(Box::new(RevisionSummary::from(
                    &self.revision(),
                )))),
                1 => Ok(PolicyWrite {
                    revision: self.revision(),
                    replayed: true,
                }),
                6 => {
                    let mut revision = self.revision(); revision.number = 2;
                    revision.policy_toml = "version=2\ndefault='show'\n".into();
                    Ok(PolicyWrite { revision, replayed: self.calls.load(Ordering::SeqCst) > 1 })
                }
                2 => Err(PolicyError::Invalid(vec!["invalid draft".into()])),
                3 => Err(PolicyError::CatalogChanged("new_fingerprint".into())),
                4 => Err(PolicyError::Code(422, "infrastructure_setting")),
                _ => Err(PolicyError::Code(503, "policy_unavailable")),
            }
        }
    }
    #[derive(Default)]
    struct Sink {
        records: Mutex<Vec<LifecycleRecord>>,
        fail: AtomicBool,
    }
    impl LifecycleSink for Sink {
        fn record(&self, r: &LifecycleRecord) -> Result<(), AuditUnavailable> {
            self.records.lock().unwrap().push(r.clone());
            if self.fail.load(Ordering::SeqCst) {
                Err(AuditUnavailable)
            } else {
                Ok(())
            }
        }
    }
    struct Sources;
    impl ConnectionSource for Sources {
        fn list(&self) -> Vec<ConnectionView> {
            vec![]
        }
    }
    impl StatusSource for Sources {
        fn policy_revision(&self, _: &str) -> Option<String> {
            None
        }
        fn config_revision(&self) -> Option<String> {
            None
        }
        fn route_ids(&self) -> Vec<String> {
            vec!["route".into()]
        }
    }
    struct Fixture {
        ctx: MgmtContext,
        fake: Arc<Fake>,
        sink: Arc<Sink>,
        dir: std::path::PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir()
                .join(format!("policy-routes-{}", SessionId::random().to_hex()));
            std::fs::create_dir_all(&dir).unwrap();
            let store = Arc::new(
                RedbStore::open(&dir, &RootKey::from_hex(&"01".repeat(32)).unwrap()).unwrap(),
            );
            let authority = Authority::load(store, SessionLimits::default()).unwrap();
            let fake = Arc::new(Fake {
                mode: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
            });
            let sink = Arc::new(Sink::default());
            Self {
                ctx: MgmtContext {
                    authority,
                    admins: Arc::new(RwLock::new(vec![])),
                    connections: Arc::new(Sources),
                    status: Arc::new(Sources),
                    policy: Some(fake.clone()),
                    lifecycle: sink.clone(),
                },
                fake,
                sink,
                dir,
            }
        }
        fn request(&self, op: &str, body: Value, key: bool) -> Response<Bytes> {
            let method = if matches!(op, "validate" | "preview" | "apply" | "rollback" | "import") {
                "POST"
            } else {
                "GET"
            };
            let path = if op.is_empty() {
                "/v1/routes/route/policy".into()
            } else {
                format!("/v1/routes/route/policy/{op}")
            };
            let mut r = Request::builder().method(method).uri(path);
            if key {
                r = r.header("Idempotency-Key", "key");
            }
            let bytes = if op == "import" {
                Bytes::new()
            } else {
                Bytes::from(serde_json::to_vec(&body).unwrap())
            };
            routes::handle(&self.ctx, Origin::Socket, r.body(bytes).unwrap())
        }
        fn finish(self) {
            let dir = self.dir.clone();
            drop(self);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
    fn apply() -> Value {
        json!({"policy_toml":"version=2\ndefault='show'\n","expected_revision":0,"catalog_fingerprint":"fingerprint"})
    }
    #[test]
    fn authentication_and_authorization_cover_all_endpoints() {
        let f = Fixture::new();
        let caller = Caller {
            principal: None,
            name: "reader".into(),
            admin: false,
        };
        for op in [
            "",
            "effective",
            "validate",
            "preview",
            "apply",
            "rollback",
            "import",
            "revisions",
            "revisions/1",
            "export",
        ] {
            let path = if op.is_empty() {
                "/v1/routes/route/policy".into()
            } else {
                format!("/v1/routes/route/policy/{op}")
            };
            let method = if matches!(op, "validate" | "preview" | "apply" | "rollback" | "import") {
                "POST"
            } else {
                "GET"
            };
            let req = Request::builder()
                .method(method)
                .uri(path)
                .body(Bytes::new())
                .unwrap();
            assert_eq!(
                routes::handle(&f.ctx, Origin::Network, req.clone()).status(),
                401
            );
            let (r, op, n) = endpoint(req.uri().path()).unwrap();
            assert_eq!(
                handle(&f.ctx, &caller, Origin::Network, &req, r, op, n).status(),
                403
            );
        }
        f.finish();
    }
    #[test]
    fn authenticated_non_admin_principal_is_forbidden() {
        let f = Fixture::new();
        let principal = f
            .ctx
            .authority
            .create_principal(
                "reader",
                crate::state::model::PrincipalKind::Agent,
                false,
                "reader",
            )
            .unwrap();
        let verifier =
            crate::pgwire::auth::ScramVerifier::from_password("fixture", b"policy-auth", 1);
        f.ctx
            .authority
            .add_principal_credential(
                principal.id,
                crate::state::model::Verifier::try_from(verifier.to_pg_string()).unwrap(),
                None,
            )
            .unwrap();
        for op in [
            "",
            "effective",
            "validate",
            "preview",
            "apply",
            "rollback",
            "import",
            "revisions",
            "revisions/1",
            "export",
        ] {
            let path = if op.is_empty() {
                "/v1/routes/route/policy".to_owned()
            } else {
                format!("/v1/routes/route/policy/{op}")
            };
            let method = if matches!(op, "validate" | "preview" | "apply" | "rollback" | "import") {
                "POST"
            } else {
                "GET"
            };
            let req = Request::builder()
                .method(method)
                .uri(path)
                .header("Authorization", "Bearer nunya.reader.fixture")
                .body(Bytes::new())
                .unwrap();
            let r = routes::handle(&f.ctx, Origin::Network, req);
            assert_eq!(r.status(), StatusCode::FORBIDDEN, "{op}");
        }
        assert_eq!(f.fake.calls.load(Ordering::SeqCst), 0);
        assert!(f.sink.records.lock().unwrap().is_empty());
        f.finish();
    }
    #[test]
    fn absent_adapter_unknown_route_and_bad_requests_are_closed() {
        let mut f = Fixture::new();
        let r = Request::builder()
            .uri("/v1/routes/unknown/policy")
            .body(Bytes::new())
            .unwrap();
        assert_eq!(
            routes::handle(&f.ctx, Origin::Socket, r).status(),
            StatusCode::NOT_FOUND
        );
        let r = Request::builder()
            .uri("/v1/routes/route/policy/unknown")
            .body(Bytes::new())
            .unwrap();
        assert_eq!(
            routes::handle(&f.ctx, Origin::Socket, r).status(),
            StatusCode::NOT_FOUND
        );
        let r = Request::builder()
            .method("POST")
            .uri("/v1/routes/route/policy/apply")
            .header("Idempotency-Key", "key")
            .body(Bytes::from_static(b"{"))
            .unwrap();
        assert_eq!(
            routes::handle(&f.ctx, Origin::Socket, r).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(f.fake.calls.load(Ordering::SeqCst), 0);
        f.ctx.policy = None;
        assert_eq!(
            f.request("", Value::Null, false).status(),
            StatusCode::NOT_FOUND
        );
        f.finish();
    }
    #[test]
    fn stale_replay_and_key_requirement() {
        let f = Fixture::new();
        assert_eq!(f.request("apply", apply(), false).status(), 400);
        assert_eq!(f.fake.calls.load(Ordering::SeqCst), 0);
        let r = f.request("apply", apply(), true);
        assert_eq!(r.status(), 409);
        let v: Value = serde_json::from_slice(r.body()).unwrap();
        assert_eq!(v["current"]["number"], 1);
        assert_eq!(v["diff"]["summary"]["newly_visible"], 1);
        assert_eq!(f.sink.records.lock().unwrap().len(), 1);
        f.fake.mode.store(1, Ordering::SeqCst);
        let r = f.request("apply", apply(), true);
        assert_eq!(r.status(), 200);
        let v: Value = serde_json::from_slice(r.body()).unwrap();
        assert_eq!(v["revision"]["number"], 1);
        assert_eq!(v["replayed"], true);
        assert_eq!(f.sink.records.lock().unwrap().len(), 2);
        f.finish();
    }
    #[test]
    fn preview_and_validate_refuse_infrastructure_changes() {
        let f = Fixture::new();
        for op in ["validate", "preview"] {
            let response = f.request(op, json!({"policy_toml":"version=2\ndefault='show'\n[readlog]\nsink='file'\npath='other.log'\n"}), false);
            assert_eq!(response.status(), 422);
            assert_eq!(serde_json::from_slice::<Value>(response.body()).unwrap()["error"], "infrastructure_setting");
        }
        f.finish();
    }
    #[test]
    fn replay_audits_same_summary() {
        let f = Fixture::new(); f.fake.mode.store(6, Ordering::SeqCst);
        assert_eq!(f.request("apply", apply(), true).status(), 200);
        assert_eq!(f.request("apply", apply(), true).status(), 200);
        let records = f.sink.records.lock().unwrap();
        assert_eq!(serde_json::to_value(&records[0].policy).unwrap(), serde_json::to_value(&records[1].policy).unwrap());
        assert_eq!(serde_json::to_value(&records[0].policy).unwrap()["summary"]["newly_visible"], 1);
        drop(records); f.finish();
    }
    #[test]
    fn every_hub_outcome_is_audited_once_and_sink_failure_is_503() {
        let f = Fixture::new();
        for (mode, status, code) in [
            (0, 409, "stale_revision"),
            (1, 200, "active"),
            (2, 422, "invalid_policy"),
            (3, 409, "catalog_changed"),
            (4, 422, "infrastructure_setting"),
        ] {
            f.fake.mode.store(mode, Ordering::SeqCst);
            let count = f.sink.records.lock().unwrap().len();
            assert_eq!(f.request("apply", apply(), true).status(), status);
            let records = f.sink.records.lock().unwrap();
            assert_eq!(records.len(), count + 1);
            assert_eq!(
                records.last().unwrap().policy.as_ref().unwrap().outcome,
                code
            );
        }
        f.sink.fail.store(true, Ordering::SeqCst);
        f.fake.mode.store(1, Ordering::SeqCst);
        assert_eq!(f.request("apply", apply(), true).status(), 503);
        f.sink.fail.store(false, Ordering::SeqCst);
        for (op, body) in [
            ("rollback", json!({"to":1,"expected_revision":1})),
            ("import", Value::Null),
        ] {
            let count = f.sink.records.lock().unwrap().len();
            assert_eq!(f.request(op, body, true).status(), 200);
            assert_eq!(f.sink.records.lock().unwrap().len(), count + 1);
        }
        f.finish();
    }
    #[test]
    fn effective_preview_validation_and_response_shapes_are_value_free() {
        let f = Fixture::new();
        let fake_value = "DATABASE_VALUE_NEVER_PASSED_TO_ANY_ENDPOINT";
        for op in [
            "",
            "effective",
            "validate",
            "preview",
            "revisions",
            "revisions/1",
            "export",
        ] {
            let r = f.request(
                op,
                json!({"policy_toml":"version=2\ndefault='show'\n"}),
                false,
            );
            assert_eq!(r.status(), 200, "{op}");
            let text = std::str::from_utf8(r.body()).unwrap();
            assert!(!text.contains(fake_value));
            let v: Value = serde_json::from_slice(r.body()).unwrap();
            if op == "effective" {
                assert_eq!(v["columns"][0]["column"], "distinctive_column_name");
                assert_eq!(v["columns"][0]["origin"], "unbound");
            }
            if op == "preview" {
                assert_eq!(v["newly_visible"][0]["column"], "distinctive_column_name");
            }
        }
        assert!(f.sink.records.lock().unwrap().is_empty());
        assert_eq!(
            f.request("validate", json!({"policy_toml":"bad input"}), false)
                .status(),
            422
        );
        assert_eq!(
            f.request("validate", json!({"policy_toml":"","unknown":true}), false)
                .status(),
            400
        );
        f.finish();
    }
}
