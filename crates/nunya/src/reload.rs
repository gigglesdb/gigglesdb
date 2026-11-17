//! Shared policy reload coordination for every serving route.
use crate::{
    policy::Policy,
    schema::Snapshot,
    serve::{ReloadableLog, now, parse_policy_bytes, readlog_key},
    state::model::RouteId,
};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, atomic::AtomicBool},
    time::Duration,
};

type PolicyLock = RwLock<Arc<Policy>>;
type SnapshotLock = RwLock<Arc<Snapshot>>;

/// A serving route and its policy/catalog locks.
pub(crate) enum Route {
    Postgres(Arc<crate::pgwire::context::GatewayContext>),
    Spanner(Arc<crate::spanner::proxy::SpannerContext>),
    #[cfg(test)]
    Test(RouteId, PolicyLock, SnapshotLock),
}
impl Route {
    fn id(&self) -> &str {
        match self {
            Self::Postgres(c) => c.route.as_str(),
            Self::Spanner(c) => c.route.as_str(),
            #[cfg(test)]
            Self::Test(id, _, _) => id.as_str(),
        }
    }

    fn locks(&self) -> (&PolicyLock, &SnapshotLock) {
        match self {
            Self::Postgres(c) => (&c.policy, &c.snapshot),
            Self::Spanner(c) => (&c.policy, &c.snapshot),
            #[cfg(test)]
            Self::Test(_, p, s) => (p, s),
        }
    }
}
struct Unit {
    identity: PathBuf,
    path: PathBuf,
    revisions: Arc<RwLock<crate::serve::Revisions>>,
    policy: Arc<Policy>,
    routes: Vec<Route>,
    hash: String,
    applied: u64,
    last: &'static str,
    last_time: u64,
    attempts: u64,
    failures: u64,
}
struct State {
    units: Vec<Unit>,
    configured: Vec<(RouteId, usize)>,
}
impl State {
    fn unit(&self, id: &str) -> Result<usize, String> {
        self.configured
            .iter()
            .find(|(route, _)| route.as_str() == id)
            .map(|(_, unit)| *unit)
            .ok_or_else(|| "route policy unavailable".into())
    }
}
/// Policy units and their shared audit sink.
pub(crate) struct Hub {
    state: Mutex<State>,
    /// Service lifecycle and management audit log.
    pub(crate) log: ReloadableLog,
    watch_ms: Option<u64>,
}
fn hash(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(bytes))
}
fn parse(path: &Path, bytes: &[u8]) -> Result<Policy, String> {
    parse_policy_bytes(path, bytes).map_err(crate::serve::errors)
}
impl Hub {
    /// Report data-free readiness for every configured route.
    pub(crate) fn readiness(&self, audit_ok: bool, store_ok: bool) -> (bool, String) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let policy_available = !self.state.is_poisoned();
        let mut ready = policy_available && !state.configured.is_empty();
        let mut body = String::new();
        let at = now();
        for (id, unit) in &state.configured {
            let unit = &state.units[*unit];
            let route = unit.routes.iter().find(|r| r.id() == id.as_str());
            let mut policy_ok = policy_available;
            let catalog = match route {
                Some(route) => {
                    let (policy, snapshot) = route.locks();
                    match (policy.read(), snapshot.read()) {
                        (Ok(policy), Ok(snapshot)) => {
                            policy_ok &= Arc::ptr_eq(&policy, &unit.policy);
                            if at.saturating_sub(snapshot.taken_at_unix_ms)
                                > policy.snapshot_max_age_ms
                            {
                                "stale"
                            } else {
                                "ok"
                            }
                        }
                        (Err(_), Ok(_)) => {
                            policy_ok = false;
                            "unavailable"
                        }
                        (Ok(_), Err(_)) => "unavailable",
                        (Err(_), Err(_)) => {
                            policy_ok = false;
                            "unavailable"
                        }
                    }
                }
                None => "unavailable",
            };
            ready &= policy_ok && catalog == "ok" && audit_ok && store_ok;
            use std::fmt::Write;
            let available = |ok| if ok { "ok" } else { "unavailable" };
            let _ = writeln!(
                body,
                "route={} policy={} catalog={} audit={} store={}",
                id.as_str(),
                available(policy_ok),
                catalog,
                available(audit_ok),
                available(store_ok)
            );
        }
        (ready, body)
    }

    /// Load one unit for each distinct canonical policy file and configure its routes.
    pub(crate) fn new(
        routes: &[(RouteId, PathBuf)],
        watch_ms: Option<u64>,
    ) -> Result<Arc<Self>, String> {
        let mut units: Vec<Unit> = Vec::new();
        let mut configured = Vec::new();
        for (route, path) in routes {
            if configured.iter().any(|(id, _)| id == route) {
                return Err("duplicate policy route".into());
            }
            let identity = std::fs::canonicalize(path).map_err(|_| "policy file unavailable")?;
            let index = if let Some(index) = units.iter().position(|unit| unit.identity == identity) {
                index
            } else {
                let bytes = std::fs::read(path).map_err(|_| "policy file unavailable")?;
                let policy = Arc::new(parse(path, &bytes)?);
                if units
                    .first()
                    .is_some_and(|unit| readlog_key(&unit.policy.readlog) != readlog_key(&policy.readlog))
                {
                    return Err("policy files name different read logs".into());
                }
                let digest = hash(&bytes);
                units.push(Unit {
                    identity,
                    path: path.clone(),
                    policy,
                    routes: vec![],
                    revisions: Arc::new(RwLock::new(crate::serve::Revisions {
                        policy: digest.clone(),
                        config: String::new(),
                    })),
                    hash: digest,
                    applied: now(),
                    last: "none",
                    last_time: 0,
                    attempts: 0,
                    failures: 0,
                });
                units.len() - 1
            };
            configured.push((route.clone(), index));
        }
        let first = units.first().ok_or("route policy unavailable")?;
        let sink =
            crate::readlog::open_sink(&first.policy.readlog).map_err(|_| "read log unavailable")?;
        Ok(Arc::new(Self {
            log: ReloadableLog(
                Arc::new(RwLock::new(sink)),
                Arc::new(RwLock::new(crate::serve::Revisions::default())),
                Arc::new(AtomicBool::new(true)),
            ),
            state: Mutex::new(State { units, configured }),
            watch_ms,
        }))
    }
    /// Set the service config revision on every unit and the lifecycle log.
    pub(crate) fn set_config_revision(&self, config: &str) -> Result<(), String> {
        self.commit_config_revision(config, || Ok(()))
    }
    /// Check publication availability before applying configuration, then publish infallibly.
    pub(crate) fn commit_config_revision(
        &self, config: &str, apply: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        // Validate all locks before the only fallible applier. Release revision guards
        // during apply: principal revocation emits lifecycle records using these locks.
        {
            let _revisions = state.units.iter().map(|unit| {
                unit.revisions.write().map_err(|_| "revision lock unavailable")
            }).collect::<Result<Vec<_>, _>>()?;
            let _service = self.log.1.write().map_err(|_| "revision lock unavailable")?;
        }
        apply()?;
        let mut revisions: Vec<_> = state.units.iter().map(|unit| {
            unit.revisions.write().unwrap_or_else(std::sync::PoisonError::into_inner)
        }).collect();
        let mut service = self.log.1.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        for revision in &mut revisions { revision.config = config.into(); }
        service.config = config.into();
        Ok(())
    }
    /// Return the configured route's current policy.
    pub(crate) fn policy_for(&self, route: &RouteId) -> Result<Arc<Policy>, String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        Ok(state.units[state.unit(route.as_str())?].policy.clone())
    }
    /// Return a route's current revision, refusing unavailable locks or routes.
    pub(crate) fn policy_revision_for(&self, route: &RouteId) -> Option<String> {
        let state = self.state.lock().ok()?;
        state.units[state.unit(route.as_str()).ok()?]
            .revisions
            .read()
            .ok()
            .map(|r| r.policy.clone())
    }
    /// Share the audit sink while stamping this route's unit revisions.
    pub(crate) fn route_log(&self, route: &RouteId) -> Result<ReloadableLog, String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        Ok(ReloadableLog(
            self.log.0.clone(),
            state.units[state.unit(route.as_str())?].revisions.clone(),
            self.log.2.clone(),
        ))
    }
    /// Validate and install the configured unit policy before publishing a route.
    pub(crate) fn register(&self, route: Route) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let index = state.unit(route.id())?;
        let s = &mut state.units[index];
        let (policy, snapshot) = route.locks();
        crate::policy::validate::validate(
            &s.policy,
            &**snapshot.read().map_err(|_| "snapshot lock unavailable")?,
        )
        .map_err(crate::serve::errors)?;
        {
            let mut current = policy.write().map_err(|_| "policy lock unavailable")?;
            let revision = match &route {
                Route::Postgres(ctx) => Some(&ctx.policy_revision),
                Route::Spanner(ctx) => Some(&ctx.policy_revision),
                #[cfg(test)]
                Route::Test(..) => None,
            };
            if let Some(revision) = revision {
                *revision.write().map_err(|_| "revision lock unavailable")? = Some(s.hash.clone());
            }
            *current = s.policy.clone();
        }
        s.routes.push(route);
        Ok(())
    }
    /// Validate a fresh catalog against this route's policy unit.
    pub(crate) fn refresh(&self, route: &Route, snapshot: Snapshot) -> Result<(), String> {
        self.refresh_if(route, snapshot, || true)
    }
    /// Publish a catalog only if its settings are still current under the publication lock.
    pub(crate) fn refresh_if(&self, route: &Route, snapshot: Snapshot, current: impl FnOnce() -> bool) -> Result<(), String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        if !current() { return Ok(()); }
        let s = &state.units[state.unit(route.id())?];
        crate::policy::validate::validate(&s.policy, &snapshot).map_err(crate::serve::errors)?;
        *route
            .locks()
            .1
            .write()
            .map_err(|_| "snapshot lock unavailable")? = Arc::new(snapshot);
        Ok(())
    }
    fn apply(
        &self,
        unit: usize,
        candidate: Result<Policy, String>,
        digest: Option<String>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let independent = state.units.len() > 1;
        let s = &mut state.units[unit];
        s.attempts += 1;
        s.last_time = now();
        let result = (|| {
            let policy = Arc::new(candidate?);
            if independent && readlog_key(&policy.readlog) != readlog_key(&s.policy.readlog) {
                return Err("policy files name different read logs".into());
            }
            // Hold every policy and snapshot lock through validation and publication.
            let mut policies = Vec::new();
            let mut snapshots = Vec::new();
            for route in &s.routes {
                let (p, snap) = route.locks();
                policies.push(p.write().map_err(|_| "policy lock unavailable")?);
                snapshots.push(snap.read().map_err(|_| "snapshot lock unavailable")?);
            }
            let mut route_revisions = Vec::new();
            for route in &s.routes {
                let revision = match route {
                    Route::Postgres(ctx) => Some(&ctx.policy_revision),
                    Route::Spanner(ctx) => Some(&ctx.policy_revision),
                    #[cfg(test)]
                    Route::Test(..) => None,
                };
                if let Some(revision) = revision {
                    route_revisions.push(revision.write().map_err(|_| "revision lock unavailable")?);
                }
            }
            for snapshot in &snapshots {
                crate::policy::validate::validate(&policy, snapshot)
                    .map_err(crate::serve::errors)?;
            }
            let replacement = if readlog_key(&policy.readlog) != readlog_key(&s.policy.readlog) {
                Some(
                    crate::readlog::open_sink(&policy.readlog)
                        .map_err(|_| "read log unavailable")?,
                )
            } else {
                None
            };
            let mut log = self
                .log
                .0
                .write()
                .map_err(|_| "read log lock unavailable")?;
            let mut revisions = s
                .revisions
                .write()
                .map_err(|_| "revision lock unavailable")?;
            let revision = digest.as_ref().ok_or("policy digest unavailable")?;
            if let Some(replacement) = replacement {
                *log = replacement;
            }
            for current in &mut policies {
                **current = policy.clone();
            }
            for current in &mut route_revisions {
                **current = Some(revision.clone());
            }
            revisions.policy = revision.clone();
            Ok::<_, String>(policy)
        })();
        match result {
            Ok(policy) => {
                s.policy = policy;
                s.hash = digest.ok_or("policy digest unavailable")?;
                s.applied = now();
                s.last = "applied";
                Ok(())
            }
            Err(e) => {
                s.failures += 1;
                s.last = "rejected";
                Err(e)
            }
        }
    }
    fn attempt(&self, unit: usize, path: &Path, bytes: Result<Vec<u8>, std::io::Error>) {
        let digest = bytes.as_ref().ok().map(|b| hash(b));
        let candidate = bytes
            .map_err(|_| "policy file unavailable".to_owned())
            .and_then(|b| parse(path, &b));
        match self.apply(unit, candidate, digest) {
            Ok(()) => eprintln!("serve: policy reloaded"),
            Err(e) => eprintln!(
                "serve: policy reload failed: {}",
                e.replace(['\r', '\n'], " ")
            ),
        }
    }
    /// Report each configured route in configuration order.
    pub(crate) fn status(&self) -> String {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.configured.iter().map(|(id, index)| {
            let unit = &s.units[*index];
            format!("route={} policy_sha256={} policy_applied_unix_ms={} last_reload={} last_reload_unix_ms={} policy_watch={}\n",
                id.as_str(), unit.hash, unit.applied, unit.last, unit.last_time,
                self.watch_ms.map_or_else(|| "off".into(), |v| v.to_string()))
        }).collect()
    }
    /// Report per-route policy counters and existing PostgreSQL connection counters.
    pub(crate) fn metrics(&self) -> Vec<(String, u64)> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut values = Vec::new();
        for (id, index) in &s.configured {
            let unit = &s.units[*index];
            values.extend([
                (
                    format!("nunya_policy_reloads_total{{route=\"{}\"}}", id.as_str()),
                    unit.attempts,
                ),
                (
                    format!(
                        "nunya_policy_reload_failures_total{{route=\"{}\"}}",
                        id.as_str()
                    ),
                    unit.failures,
                ),
                (
                    format!("nunya_policy_applied_unix_ms{{route=\"{}\"}}", id.as_str()),
                    unit.applied,
                ),
            ]);
        }
        for route in s.units.iter().flat_map(|unit| &unit.routes) {
            if let Route::Postgres(ctx) = route {
                let m = ctx.metrics.snapshot();
                values.extend([
                    ("nunya_connections_total".into(), m.connections),
                    (
                        "nunya_statements_allowed_total".into(),
                        m.statements_allowed,
                    ),
                    ("nunya_bytes_total".into(), m.bytes),
                ]);
                values.extend(
                    crate::error::NyCode::ALL
                        .iter()
                        .zip(m.statements_refused)
                        .map(|(code, n)| (format!("nunya_refused_{}_total", code.sqlstate()), n)),
                );
            }
        }
        values
    }
    /// Reload every unit manually, or just changed units after watcher debounce.
    pub(crate) async fn run(&self, mut reload: tokio::sync::mpsc::Receiver<()>, config: Option<Arc<crate::config_reload::ConfigReloader>>) {
        let mut config_watcher = config.as_ref().map(|c| Watcher {
            attempted: c.revision(),
            previous: None, readable: true,
        });
        let mut files: Vec<_> = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .units
                .iter()
                .map(|unit| {
                    (
                        unit.path.clone(),
                        Watcher {
                            attempted: unit.hash.clone(),
                            previous: None,
                            readable: true,
                        },
                    )
                })
                .collect()
        };
        let mut interval =
            tokio::time::interval(Duration::from_millis(self.watch_ms.unwrap_or(3600000)));
        interval.tick().await;
        let hup = crate::serve::hangup();
        tokio::pin!(hup);
        let mut reload_open = true;
        loop {
            let manual = tokio::select! {
                message = reload.recv(), if reload_open => {
                    if message.is_none() { reload_open = false; continue; }
                    true
                }
                _ = &mut hup => { hup.set(crate::serve::hangup()); true }
                _ = interval.tick(), if self.watch_ms.is_some() => false,
            };
            let mut reload_config = false;
            if let (Some(config), Some(watcher)) = (&config, &mut config_watcher) {
                let bytes = std::fs::read(config.path());
                if manual {
                    if let Ok(bytes) = &bytes { watcher.attempted = hash(bytes); }
                    watcher.previous = None;
                }
                reload_config = manual || watcher.poll(bytes.as_deref().ok());
            }
            let mut attempts = Vec::new();
            for (index, (path, watcher)) in files.iter_mut().enumerate() {
                let bytes = std::fs::read(&*path);
                if manual {
                    if let Ok(bytes) = &bytes { watcher.attempted = hash(bytes); }
                    watcher.previous = None;
                }
                if manual || watcher.poll(bytes.as_deref().ok()) {
                    attempts.push((index, path.clone(), bytes));
                }
            }
            // A policy watcher trigger also rereads config, even when its bytes
            // have not changed. A config-only trigger leaves policy units alone.
            reload_config |= !attempts.is_empty();
            config_before_policy(config.as_deref().filter(|_| reload_config), || {
                for (index, path, bytes) in attempts { self.attempt(index, &path, bytes); }
            }).await;
        }
    }
}

// The callback keeps policy attempts independent while making trigger order explicit.
async fn config_before_policy(config: Option<&crate::config_reload::ConfigReloader>, attempt: impl FnOnce()) {
    if let Some(config) = config { config.reload().await; }
    attempt();
}

struct Watcher {
    attempted: String,
    previous: Option<String>,
    readable: bool,
}
impl Watcher {
    fn poll(&mut self, bytes: Option<&[u8]>) -> bool {
        let Some(bytes) = bytes else {
            if self.readable {
                eprintln!("serve: policy watch: file unavailable");
            }
            self.readable = false;
            self.previous = None;
            return false;
        };
        self.readable = true;
        let digest = hash(bytes);
        let trigger = digest != self.attempted && self.previous.as_ref() == Some(&digest);
        self.previous = Some(digest.clone());
        if trigger {
            self.attempted = digest;
        }
        trigger
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{ReadLogSink, Treatment},
        schema::ColumnRef,
    };
    use std::collections::BTreeMap;
    fn policy() -> Policy {
        Policy {
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: ReadLogSink::Stdout,
        }
    }
    fn id(name: &str) -> RouteId {
        RouteId::new(name).unwrap()
    }
    fn hub() -> Hub {
        let p = policy();
        Hub {
            log: ReloadableLog(
                Arc::new(RwLock::new(crate::readlog::open_sink(&p.readlog).unwrap())),
                Arc::new(RwLock::new(crate::serve::Revisions::default())),
                Arc::new(AtomicBool::new(true)),
            ),
            state: Mutex::new(State {
                units: vec![Unit {
                    identity: PathBuf::new(),
                    path: PathBuf::new(),
                    revisions: Arc::new(RwLock::new(crate::serve::Revisions {
                        policy: hash(b"initial"),
                        config: "config-test".into(),
                    })),
                    policy: Arc::new(p),
                    routes: vec![],
                    hash: hash(b"initial"),
                    applied: 1,
                    last: "none",
                    last_time: 0,
                    attempts: 0,
                    failures: 0,
                }],
                configured: vec![(id("postgres"), 0)],
            }),
            watch_ms: None,
        }
    }
    struct Files {
        dir: PathBuf,
        a: PathBuf,
        b: PathBuf,
    }
    impl Files {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let dir = std::env::temp_dir().join(format!(
                "nunya-units-{}",
                data_encoding::HEXLOWER.encode(&nonce)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let files = Self {
                a: dir.join("a.toml"),
                b: dir.join("b.toml"),
                dir,
            };
            files.write(&files.a, 60000);
            files.write(&files.b, 60001);
            files
        }
        fn write(&self, path: &Path, age: u64) {
            std::fs::write(
                path,
                format!("snapshot_max_age_ms={age}\n[readlog]\nsink='stdout'\n"),
            )
            .unwrap();
        }
        fn hub(&self) -> Arc<Hub> {
            Hub::new(
                &[(id("a"), self.a.clone()), (id("b"), self.b.clone())],
                None,
            )
            .unwrap()
        }
    }
    impl Drop for Files {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    fn test_route(name: &str, snapshot: Snapshot) -> Route {
        Route::Test(
            id(name),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(snapshot)),
        )
    }
    fn empty_snapshot() -> Snapshot {
        Snapshot::new(vec![], vec![], vec![], now())
    }
    async fn manual_reload(h: &Arc<Hub>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = h.clone();
        let task = tokio::spawn(async move { worker.run(rx, None).await });
        tx.send(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if h.state
                    .lock()
                    .unwrap()
                    .units
                    .iter()
                    .all(|u| u.attempts == 1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    #[test]
    fn separate_files_validate_only_their_own_catalog() {
        let files = Files::new();
        let bytes =
            "[readlog]\nsink='stdout'\n[[column]]\ntable='public.t'\nname='secret'\nmasked=true\n";
        std::fs::write(&files.a, bytes).unwrap();
        let snapshot = Snapshot::new(
            vec![crate::schema::Relation {
                schema: "public".into(),
                name: "t".into(),
                kind: crate::schema::RelationKind::Table,
                columns: vec![crate::schema::Column {
                    name: "secret".into(),
                    ty: crate::value::ColumnType::Text,
                    nullable: true,
                    generated: None,
                    default: None,
                }],
                view_definition: None,
                primary_key: vec![],
            }],
            vec![],
            vec![],
            now(),
        );
        let h = files.hub();
        h.register(test_route("a", snapshot.clone())).unwrap();
        h.register(test_route("b", empty_snapshot())).unwrap();
        assert!(h.readiness(true, true).0);
        let b = test_route("b", empty_snapshot());
        h.refresh(&b, empty_snapshot()).unwrap();
        let shared = Hub::new(
            &[
                (id("a"), files.a.clone()),
                (id("b"), files.dir.join(".").join("a.toml")),
            ],
            None,
        )
        .unwrap();
        assert_eq!(shared.state.lock().unwrap().units.len(), 1);
        shared.register(test_route("a", snapshot)).unwrap();
        assert!(shared.register(test_route("b", empty_snapshot())).is_err());
        assert!(
            shared
                .refresh(&test_route("b", empty_snapshot()), empty_snapshot())
                .is_err()
        );
        assert!(h.register(test_route("unknown", empty_snapshot())).is_err());
        assert!(
            h.refresh(&test_route("unknown", empty_snapshot()), empty_snapshot())
                .is_err()
        );
        assert!(h.policy_for(&id("unknown")).is_err());
        assert!(h.route_log(&id("unknown")).is_err());
        assert_eq!(h.policy_revision_for(&id("unknown")), None);
    }
    #[tokio::test]
    async fn independent_reload_per_unit_manual_also_applies_unchanged_bytes() {
        let files = Files::new();
        let h = files.hub();
        h.register(test_route("a", empty_snapshot())).unwrap();
        h.register(test_route("b", empty_snapshot())).unwrap();
        let a = h.policy_for(&id("a")).unwrap();
        let b = h.policy_for(&id("b")).unwrap();
        let old_a = h.policy_revision_for(&id("a"));
        let old_b = h.policy_revision_for(&id("b"));
        files.write(&files.a, 70000);
        manual_reload(&h).await;
        assert_ne!(h.policy_revision_for(&id("a")), old_a);
        assert_eq!(h.policy_revision_for(&id("b")), old_b);
        assert_eq!(a.snapshot_max_age_ms, 60000);
        assert_eq!(h.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
        assert_eq!(
            h.policy_for(&id("b")).unwrap().snapshot_max_age_ms,
            b.snapshot_max_age_ms
        );
        // A manual reload republishes B's identical bytes, so its Arc and attempt
        // counter change, while its effective policy and revision stay unchanged.
        let metrics = h.metrics();
        for name in ["a", "b"] {
            assert!(
                metrics.contains(&(format!("nunya_policy_reloads_total{{route=\"{name}\"}}"), 1))
            );
        }
        assert!(h.readiness(true, true).0);
        let state = h.state.lock().unwrap();
        for unit in &state.units {
            assert!(Arc::ptr_eq(
                &unit.policy,
                &unit.routes[0].locks().0.read().unwrap()
            ));
        }
    }
    #[tokio::test]
    async fn bad_edit_on_one_route_keeps_last_good_and_other_unaffected() {
        let files = Files::new();
        let h = files.hub();
        let old = h.policy_for(&id("a")).unwrap();
        let revision = h.policy_revision_for(&id("a"));
        std::fs::write(&files.a, [255]).unwrap();
        files.write(&files.b, 70000);
        manual_reload(&h).await;
        assert!(Arc::ptr_eq(&old, &h.policy_for(&id("a")).unwrap()));
        assert_eq!(h.policy_revision_for(&id("a")), revision);
        assert_eq!(h.policy_for(&id("b")).unwrap().snapshot_max_age_ms, 70000);
        assert!(
            h.status()
                .lines()
                .next()
                .unwrap()
                .contains("last_reload=rejected ")
        );
        assert!(
            h.status()
                .lines()
                .nth(1)
                .unwrap()
                .contains("last_reload=applied ")
        );
        assert!(
            h.metrics()
                .contains(&("nunya_policy_reload_failures_total{route=\"a\"}".into(), 1))
        );
        assert!(
            h.metrics()
                .contains(&("nunya_policy_reload_failures_total{route=\"b\"}".into(), 0))
        );
    }
    #[tokio::test]
    async fn readlog_spellings_agree_at_startup_and_reload() {
        let files = Files::new();
        let policy = |path: &str| format!("[readlog]\nsink='file'\npath='{path}'\n");
        std::fs::write(&files.a, policy("read.log")).unwrap();
        std::fs::write(
            &files.b,
            policy(&files.dir.join("read.log").to_string_lossy()),
        )
        .unwrap();
        let hub = files.hub();
        std::fs::write(&files.a, policy("./read.log")).unwrap();
        manual_reload(&hub).await;
        assert!(
            hub.state
                .lock()
                .unwrap()
                .units
                .iter()
                .all(|u| u.last == "applied")
        );
        let changed = parse(
            &files.a,
            b"[readlog]\nsink='file'\npath='read.log'\nmax_bytes=1\n",
        )
        .unwrap();
        assert!(hub.apply(0, Ok(changed), Some(hash(b"changed"))).is_err());
    }

    #[tokio::test]
    async fn unit_retains_named_path_for_reload() {
        let files = Files::new();
        let named = files.dir.join("./a.toml");
        let hub = Hub::new(&[(id("a"), named.clone())], None).unwrap();
        assert_eq!(hub.state.lock().unwrap().units[0].path, named);
        assert_eq!(hub.state.lock().unwrap().units[0].identity, std::fs::canonicalize(&named).unwrap());
        files.write(&files.a, 70000);
        manual_reload(&hub).await;
        assert_eq!(hub.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
    }

    #[tokio::test]
    async fn symlink_policy_uses_named_directory_at_startup_and_reload() {
        let files = Files::new();
        let target_dir = files.dir.join("target");
        std::fs::create_dir(&target_dir).unwrap();
        let target = target_dir.join("policy.toml");
        let named = files.dir.join("named.toml");
        std::fs::write(&target, "[readlog]\nsink='file'\npath='read.log'\n").unwrap();
        #[cfg(windows)]
        if let Err(error) = std::os::windows::fs::symlink_file(&target, &named) {
            if error.raw_os_error() == Some(1314) {
                eprintln!("symlink regression requires Windows symlink privilege (1314)");
                return;
            }
            panic!("cannot create policy symlink: {error}");
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &named).unwrap();
        let hub = Hub::new(&[(id("a"), named.clone()), (id("b"), target.clone())], None).unwrap();
        assert_eq!(hub.state.lock().unwrap().units.len(), 1);
        assert!(files.dir.join("read.log").exists());
        assert!(!target_dir.join("read.log").exists());
        std::fs::write(
            &target,
            "snapshot_max_age_ms=70000\n[readlog]\nsink='file'\npath='./read.log'\n",
        )
        .unwrap();
        manual_reload(&hub).await;
        assert_eq!(hub.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
        assert_eq!(hub.state.lock().unwrap().units[0].path, named);
        assert!(!target_dir.join("read.log").exists());
    }
    #[test]
    fn readlog_must_agree_across_units() {
        let files = Files::new();
        let h = files.hub();
        let revision = h.policy_revision_for(&id("a"));
        let bytes = b"[readlog]\nsink='file'\npath='audit.log'\n";
        std::fs::write(&files.a, bytes).unwrap();
        assert_eq!(files.hub_error(), "policy files name different read logs");
        let candidate = parse(&files.a, bytes).unwrap();
        assert_eq!(
            h.apply(0, Ok(candidate), Some(hash(bytes))).unwrap_err(),
            "policy files name different read logs"
        );
        assert_eq!(h.policy_revision_for(&id("a")), revision);
        assert_eq!(h.state.lock().unwrap().units[0].failures, 1);
        // Distinct files with the same resolved relative audit path agree.
        std::fs::write(&files.b, bytes).unwrap();
        let shared_sink = files.hub();
        drop(shared_sink);
        // A single canonical unit (two routes) may still replace the sink.
        files.write(&files.a, 60000);
        let single = Hub::new(
            &[(id("a"), files.a.clone()), (id("b"), files.a.clone())],
            None,
        )
        .unwrap();
        single
            .apply(0, Ok(parse(&files.a, bytes).unwrap()), Some(hash(bytes)))
            .unwrap();
        assert_eq!(
            single.policy_revision_for(&id("a")),
            single.policy_revision_for(&id("b"))
        );
        assert!(
            single
                .metrics()
                .contains(&("nunya_policy_reloads_total{route=\"b\"}".into(), 1))
        );
    }
    impl Files {
        fn hub_error(&self) -> String {
            match Hub::new(
                &[(id("a"), self.a.clone()), (id("b"), self.b.clone())],
                None,
            ) {
                Ok(_) => panic!("expected audit sink mismatch"),
                Err(e) => e,
            }
        }
    }
    #[test]
    fn route_log_stamps_its_own_route_revision() {
        use crate::readlog::ReadLog;
        let files = Files::new();
        let log = "[readlog]\nsink='file'\npath='audit.log'\n";
        std::fs::write(&files.a, format!("snapshot_max_age_ms=60000\n{log}")).unwrap();
        std::fs::write(&files.b, format!("snapshot_max_age_ms=60001\n{log}")).unwrap();
        let h = files.hub();
        h.set_config_revision("config").unwrap();
        assert_ne!(
            h.policy_revision_for(&id("a")),
            h.policy_revision_for(&id("b"))
        );
        for name in ["a", "b"] {
            let route_log = h.route_log(&id(name)).unwrap();
            assert!(Arc::ptr_eq(&route_log.0, &h.log.0));
            assert!(Arc::ptr_eq(&route_log.2, &h.log.2));
            route_log
                .write(&crate::readlog::ReadLogRecord {
                    ts_unix_ms: 1,
                    principal: crate::token::PrincipalId([1; 16]),
                    session: crate::token::SessionId([2; 16]),
                    scope: crate::token::ScopeId([3; 16]),
                    connection: None,
                    credential_id: None,
                    route: name.into(),
                    policy_revision: None,
                    config_revision: None,
                    statement_fingerprint: "test".into(),
                    statement: "test".into(),
                    outcome: crate::readlog::Outcome::Allowed,
                    rows: 0,
                    tokens_released: vec![],
                })
                .unwrap();
        }
        let bytes = std::fs::read_to_string(files.dir.join("audit.log")).unwrap();
        let records: Vec<serde_json::Value> = bytes
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        for (record, route) in records.iter().zip(["a", "b"]) {
            assert_eq!(
                record["policy_revision"],
                h.policy_revision_for(&id(route)).unwrap()
            );
            assert_eq!(record["config_revision"], "config");
        }
    }
    #[tokio::test]
    async fn watcher_debounces_each_unit_without_attempting_unchanged_routes() {
        let files = Files::new();
        let h = Hub::new(
            &[(id("a"), files.a.clone()), (id("b"), files.b.clone())],
            Some(5),
        )
        .unwrap();
        let b = h.policy_for(&id("b")).unwrap();
        files.write(&files.a, 70000);
        let worker = h.clone();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move { worker.run(rx, None).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if h.state.lock().unwrap().units[0].attempts == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(h.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
        assert!(Arc::ptr_eq(&b, &h.policy_for(&id("b")).unwrap()));
        let state = h.state.lock().unwrap();
        assert_eq!(state.units[0].attempts, 1);
        assert_eq!(state.units[1].attempts, 0);
    }
    #[test]
    fn two_route_status_and_metrics_format() {
        let files = Files::new();
        let h = files.hub();
        let state = h.state.lock().unwrap();
        let expected: String = state.configured.iter().map(|(route, index)| {
            let unit = &state.units[*index];
            format!("route={} policy_sha256={} policy_applied_unix_ms={} last_reload=none last_reload_unix_ms=0 policy_watch=off\n", route.as_str(), unit.hash, unit.applied)
        }).collect();
        drop(state);
        assert_eq!(h.status(), expected);
        assert_eq!(h.metrics().len(), 6);
        for name in ["a", "b"] {
            assert!(
                h.metrics()
                    .contains(&(format!("nunya_policy_reloads_total{{route=\"{name}\"}}"), 0))
            );
            assert!(h.metrics().contains(&(
                format!("nunya_policy_reload_failures_total{{route=\"{name}\"}}"),
                0
            )));
        }
    }
    #[test]
    fn postgres_readiness_tracks_loading_staleness_and_dependencies() {
        let h = hub();
        assert_eq!(
            h.readiness(true, true),
            (
                false,
                "route=postgres policy=ok catalog=unavailable audit=ok store=ok\n".into()
            )
        );
        h.register(Route::Test(
            id("postgres"),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], now()))),
        ))
        .unwrap();
        assert_eq!(
            h.readiness(true, true),
            (
                true,
                "route=postgres policy=ok catalog=ok audit=ok store=ok\n".into()
            )
        );
        assert!(!h.readiness(false, true).0);
        assert!(h.readiness(false, true).1.contains("audit=unavailable"));
        assert!(!h.readiness(true, false).0);
        assert!(h.readiness(true, false).1.contains("store=unavailable"));
        *h.state.lock().unwrap().units[0].routes[0]
            .locks()
            .1
            .write()
            .unwrap() = Arc::new(Snapshot::new(vec![], vec![], vec![], 0));
        assert_eq!(
            h.readiness(true, true),
            (
                false,
                "route=postgres policy=ok catalog=stale audit=ok store=ok\n".into()
            )
        );
    }

    #[tokio::test]
    async fn config_poll_is_independent_and_policy_triggers_reread_config() {
        let files = crate::config_reload::tests::Fixture::new();
        let route = id("pg");
        let hub = Hub::new(&[(route, files.dir.join("policy"))], Some(5)).unwrap();
        let startup = crate::config::Config::load(&files.path).unwrap();
        let config = Arc::new(crate::config_reload::ConfigReloader::new(startup, Arc::downgrade(&hub)));
        files.load(&files.text.replace("max_connections=256", "max_connections=257"));
        let worker = hub.clone(); let worker_config = config.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move { worker.run(rx, Some(worker_config)).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while config.last_reload() != "applied" { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert_eq!(hub.state.lock().unwrap().units[0].attempts, 0);
        assert!(config.metrics().contains(&("nunya_config_reloads_total".into(), 1)));
        std::fs::write(files.dir.join("policy"), "snapshot_max_age_ms=60001\n[readlog]\nsink='stdout'\n").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while hub.state.lock().unwrap().units[0].attempts < 1 { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert!(config.metrics().contains(&("nunya_config_reloads_total".into(), 2)));
        std::fs::write(&files.path, "invalid=[").unwrap();
        tx.send(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while hub.state.lock().unwrap().units[0].attempts < 2 { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert_eq!(config.last_reload(), "rejected");
        task.abort(); assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn manual_trigger_reloads_config_before_policy_attempt() {
        let files = crate::config_reload::tests::Fixture::new();
        let hub = files.hub();
        let startup = crate::config::Config::load(&files.path).unwrap();
        let config = crate::config_reload::ConfigReloader::new(startup, Arc::downgrade(&hub));
        let new = files.load(&files.text.replace("max_connections=256", "max_connections=257"));
        config_before_policy(Some(&config), || {
            assert_eq!(config.last_reload(), "applied");
            assert_eq!(hub.log.1.read().unwrap().config, new.revision);
            assert_eq!(hub.state.lock().unwrap().units[0].attempts, 0);
            hub.attempt(0, &files.dir.join("policy"), std::fs::read(files.dir.join("policy")));
        }).await;
        assert_eq!(hub.state.lock().unwrap().units[0].attempts, 1);
    }

    #[test]
    fn watcher_debounces_rejections_and_recovers_readability() {
        let mut w = Watcher {
            attempted: hash(b"initial"),
            previous: None,
            readable: true,
        };
        assert!(!w.poll(Some(b"initial")));
        assert!(!w.poll(Some(b"partial")));
        assert!(!w.poll(Some(b"new")));
        assert!(w.poll(Some(b"new")));
        // A rejection is still an attempt; this hash must not trigger again.
        assert!(!w.poll(Some(b"new")));
        assert!(!w.poll(None));
        assert!(!w.readable);
        assert!(!w.poll(None));
        assert!(!w.poll(Some(b"next")));
        assert!(w.readable);
        assert!(w.poll(Some(b"next")));
    }
    #[test]
    fn reload_is_all_or_nothing_and_late_registration_uses_latest() {
        let h = hub();
        // Distinct catalogs: a policy referring to a column found only in the first
        // route must fail without publishing to either route.
        let column = ColumnRef {
            schema: "public".into(),
            table: "t".into(),
            column: "secret".into(),
        };
        let mut candidate = policy();
        candidate
            .columns
            .insert(column, Treatment::Masked { placeholder: None });
        let first = Snapshot::new(
            vec![crate::schema::Relation {
                schema: "public".into(),
                name: "t".into(),
                kind: crate::schema::RelationKind::Table,
                columns: vec![crate::schema::Column {
                    name: "secret".into(),
                    ty: crate::value::ColumnType::Text,
                    nullable: true,
                    generated: None,
                    default: None,
                }],
                view_definition: None,
                primary_key: vec![],
            }],
            vec![],
            vec![],
            0,
        );
        let second = Snapshot::new(vec![], vec![], vec![], 0);
        assert!(crate::policy::validate::validate(&candidate, &first).is_ok());
        assert!(crate::policy::validate::validate(&candidate, &second).is_err());
        for snapshot in [first, second] {
            h.register(Route::Test(
                id("postgres"),
                RwLock::new(Arc::new(policy())),
                RwLock::new(Arc::new(snapshot)),
            ))
            .unwrap();
        }
        let mut valid = policy();
        valid.snapshot_max_age_ms = 12345;
        h.apply(0, Ok(valid.clone()), Some(hash(b"valid"))).unwrap();
        assert!(h.apply(0, Ok(candidate), Some(hash(b"invalid"))).is_err());
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let missing_parent = std::env::temp_dir().join(format!(
            "nunya-reload-missing-{}",
            data_encoding::HEXLOWER.encode(&nonce)
        ));
        let mut bad_log = policy();
        bad_log.readlog = ReadLogSink::File {
            path: missing_parent.join("readlog"),
            max_bytes: 1024,
        };
        assert!(h.apply(0, Ok(bad_log), Some(hash(b"bad-log"))).is_err());
        h.register(Route::Test(
            id("postgres"),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], 0))),
        ))
        .unwrap();
        let guard = h.state.lock().unwrap();
        let state = &guard.units[0];
        for route in &state.routes {
            assert_eq!(route.locks().0.read().unwrap().snapshot_max_age_ms, 12345);
        }
        assert_eq!(state.hash, hash(b"valid"));
        assert_eq!(state.failures, 2);
    }
    #[test]
    fn native_spanner_context_observes_reload_and_swappable_log() {
        use crate::{
            spanner::proxy::{SpannerContext, SpannerUpstream},
            token::{StandardCodec, keys::RootKey},
        };
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "nunya-reload-{}",
            data_encoding::HEXLOWER.encode(&nonce)
        ));
        let policy_path = dir.join("policy.toml");
        std::fs::create_dir_all(&dir).unwrap();
        let bytes = b"snapshot_max_age_ms = 60000\n[readlog]\nsink = 'stdout'\n";
        std::fs::write(&policy_path, bytes).unwrap();
        let h = Hub::new(
            &[
                (id("postgres"), policy_path.clone()),
                (id("spanner-native"), policy_path.clone()),
            ],
            Some(100),
        )
        .unwrap();
        h.set_config_revision("config-test").unwrap();
        assert_eq!(h.state.lock().unwrap().units[0].hash, hash(bytes));
        assert!(h.status().ends_with("policy_watch=100\n"));
        // Manual reloads apply unchanged bytes, and parse exactly the supplied
        // capture even if the path has already been replaced by another editor.
        std::fs::write(&policy_path, "invalid = [").unwrap();
        h.attempt(0, &policy_path, Ok(bytes.to_vec()));
        h.attempt(0, &policy_path, Ok(bytes.to_vec()));
        assert_eq!(h.state.lock().unwrap().units[0].attempts, 2);
        assert_eq!(h.state.lock().unwrap().units[0].hash, hash(bytes));
        let runtime = crate::state::runtime::StateRuntime::open(
            &dir.join("state"),
            &RootKey::from_hex(&"ab".repeat(32)).unwrap(),
            &crate::config::SessionSettings::default(),
            &[],
        )
        .unwrap();
        let ctx = Arc::new(SpannerContext {
            authority: runtime.authority.clone(),
            registry: runtime.registry.clone(),
            route: crate::state::model::RouteId::new("spanner-native").unwrap(),
            policy: RwLock::new(Arc::new(policy())),
            policy_revision: RwLock::new(None),
            snapshot: RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], 0))),
            root_key: RootKey::from_hex(&"ab".repeat(32)).unwrap(),
            readlog: Box::new(h.route_log(&id("spanner-native")).unwrap()),
            codec: StandardCodec,
            database: "test".into(),
            clock: Arc::new(now),
            upstream: SpannerUpstream::Emulator {
                endpoint: "http://127.0.0.1:1".into(),
            },
        });
        assert!(!h.readiness(true, true).0);
        *ctx.snapshot.write().unwrap() = Arc::new(Snapshot::new(vec![], vec![], vec![], now()));
        h.register(Route::Spanner(ctx.clone())).unwrap();
        assert_eq!(ctx.policy_revision.read().unwrap().as_deref(), Some(hash(bytes).as_str()));
        assert!(!h.readiness(true, true).0);
        h.register(Route::Test(
            id("postgres"),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], now()))),
        ))
        .unwrap();
        let (ready, body) = h.readiness(true, true);
        assert!(ready);
        assert_eq!(body.lines().count(), 2);
        assert!(body.starts_with("route=postgres "));
        assert!(body.contains("route=spanner-native "));
        let mut new = policy();
        new.snapshot_max_age_ms = 123;
        new.readlog = ReadLogSink::File {
            path: dir.join("audit.log"),
            max_bytes: 1024,
        };
        h.apply(0, Ok(new), Some("R2".into())).unwrap();
        assert_eq!(ctx.policy_revision.read().unwrap().as_deref(), Some("R2"));
        assert_eq!(ctx.policy.read().unwrap().snapshot_max_age_ms, 123);
        ctx.readlog
            .write(&crate::readlog::ReadLogRecord {
                ts_unix_ms: 1,
                principal: crate::token::PrincipalId([1; 16]),
                session: crate::token::SessionId([2; 16]),
                scope: crate::token::ScopeId([3; 16]),
                connection: None,
                credential_id: None,
                route: ctx.route.as_str().into(),
                policy_revision: None,
                config_revision: None,
                statement_fingerprint: "test".into(),
                statement: "test".into(),
                outcome: crate::readlog::Outcome::Allowed,
                rows: 0,
                tokens_released: vec![],
            })
            .unwrap();
        let logged: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("audit.log")).unwrap()).unwrap();
        assert_eq!(logged["policy_revision"], "R2");
        assert_eq!(logged["config_revision"], "config-test");
        let mut next = (*h.policy_for(&id("spanner-native")).unwrap()).clone();
        next.snapshot_max_age_ms = 124;
        h.apply(0, Ok(next), Some(hash(b"next"))).unwrap();
        ctx.readlog
            .write(&crate::readlog::ReadLogRecord {
                ts_unix_ms: 2,
                principal: crate::token::PrincipalId([1; 16]),
                session: crate::token::SessionId([2; 16]),
                scope: crate::token::ScopeId([3; 16]),
                connection: None,
                credential_id: None,
                route: ctx.route.as_str().into(),
                policy_revision: None,
                config_revision: None,
                statement_fingerprint: "test".into(),
                statement: "test".into(),
                outcome: crate::readlog::Outcome::Allowed,
                rows: 0,
                tokens_released: vec![],
            })
            .unwrap();
        let bytes = std::fs::read_to_string(dir.join("audit.log")).unwrap();
        let logged: serde_json::Value =
            serde_json::from_str(bytes.lines().last().unwrap()).unwrap();
        assert_eq!(logged["policy_revision"], hash(b"next"));
        assert_eq!(logged["config_revision"], "config-test");
        drop(h);
        drop(ctx);
        drop(runtime);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn startup_status_and_reload_metrics() {
        let h = hub();
        assert_eq!(
            h.status(),
            format!(
                "route=postgres policy_sha256={} policy_applied_unix_ms=1 last_reload=none last_reload_unix_ms=0 policy_watch=off\n",
                hash(b"initial")
            )
        );
        h.apply(0, Err("invalid".into()), None).unwrap_err();
        assert!(h.status().contains("last_reload=rejected "));
        let metrics = h.metrics();
        assert!(metrics.contains(&("nunya_policy_reloads_total{route=\"postgres\"}".into(), 1)));
        assert!(metrics.contains(&(
            "nunya_policy_reload_failures_total{route=\"postgres\"}".into(),
            1
        )));
        assert!(metrics.contains(&("nunya_policy_applied_unix_ms{route=\"postgres\"}".into(), 1)));
    }
    #[test]
    fn postgres_register_and_apply_publish_matching_policy_revision() {
        use crate::pgwire::{context::{GatewayContext, Metrics}, upstream::UpstreamConfig};
        let files = Files::new();
        let runtime = crate::state::runtime::StateRuntime::open(
            &files.dir.join("state"),
            &crate::token::keys::RootKey::from_hex(&"00".repeat(32)).unwrap(),
            &crate::config::SessionSettings::default(), &[],
        ).unwrap();
        let h = hub();
        let ctx = Arc::new(GatewayContext {
            authority: runtime.authority.clone(), registry: runtime.registry.clone(), route: id("postgres"),
            policy: RwLock::new(Arc::new(policy())), policy_revision: RwLock::new(None),
            snapshot: RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec!["public".into()], 0))),
            root_key: crate::token::keys::RootKey::from_hex(&"00".repeat(32)).unwrap(),
            readlog: Box::new(h.log.clone()),
            upstream: UpstreamConfig {
                host: "unused".into(), port: 5432, user: "unused".into(),
                password: zeroize::Zeroizing::new(String::new()), dbname: "unused".into(),
                tls: crate::pgwire::upstream::UpstreamTls::Disable, application_name: "nunya".into(), connect_timeout_ms: 100,
            },
            lowering: crate::dialect::pg::PgLowering,
            codec: crate::token::StandardCodec, clock: Arc::new(|| 0), cancel_map: Mutex::new(Default::default()),
            metrics: Metrics::default(), tls: Arc::new(std::sync::RwLock::new(None)), allow_plaintext: true,
        });
        h.register(Route::Postgres(ctx.clone())).unwrap();
        let (initial, revision) = ctx.policy_snapshot().unwrap();
        assert_eq!(revision, Some(hash(b"initial")));
        let mut next = policy();
        next.snapshot_max_age_ms = 42;
        h.apply(0, Ok(next), Some("R2".into())).unwrap();
        let (current, revision) = ctx.policy_snapshot().unwrap();
        assert_eq!(revision.as_deref(), Some("R2"));
        assert_eq!(current.snapshot_max_age_ms, 42);
        assert_eq!(initial.snapshot_max_age_ms, 60000);
    }

}
