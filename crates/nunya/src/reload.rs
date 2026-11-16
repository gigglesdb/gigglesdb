//! Shared policy reload coordination for every serving route.
use crate::{
    policy::Policy,
    schema::Snapshot,
    serve::{ReloadableLog, now, parse_policy_bytes},
};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

type PolicyLock = RwLock<Arc<Policy>>;
type SnapshotLock = RwLock<Arc<Snapshot>>;

pub(crate) enum Route {
    Postgres(Arc<crate::pgwire::context::GatewayContext>),
    Spanner(Arc<crate::spanner::proxy::SpannerContext>),
    #[cfg(test)]
    Test(PolicyLock, SnapshotLock),
}
impl Route {
    fn locks(&self) -> (&PolicyLock, &SnapshotLock) {
        match self {
            Self::Postgres(c) => (&c.policy, &c.snapshot),
            Self::Spanner(c) => (&c.policy, &c.snapshot),
            #[cfg(test)]
            Self::Test(p, s) => (p, s),
        }
    }
}
struct State {
    policy: Arc<Policy>,
    routes: Vec<Route>,
    hash: String,
    applied: u64,
    last: &'static str,
    last_time: u64,
    attempts: u64,
    failures: u64,
}
pub(crate) struct Hub {
    state: Mutex<State>,
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
    pub(crate) fn new(path: &Path, watch_ms: Option<u64>) -> Result<Arc<Self>, String> {
        let bytes = std::fs::read(path).map_err(|_| "policy file unavailable")?;
        let policy = Arc::new(parse(path, &bytes)?);
        let log = crate::readlog::open_sink(&policy.readlog).map_err(|_| "read log unavailable")?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                policy,
                routes: vec![],
                hash: hash(&bytes),
                applied: now(),
                last: "none",
                last_time: 0,
                attempts: 0,
                failures: 0,
            }),
            log: ReloadableLog(Arc::new(RwLock::new(log))),
            watch_ms,
        }))
    }
    pub(crate) fn policy(&self) -> Arc<Policy> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .policy
            .clone()
    }
    pub(crate) fn register(&self, route: Route) -> Result<(), String> {
        let mut s = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let (policy, snapshot) = route.locks();
        crate::policy::validate::validate(
            &s.policy,
            &**snapshot.read().map_err(|_| "snapshot lock unavailable")?,
        )
        .map_err(crate::serve::errors)?;
        *policy.write().map_err(|_| "policy lock unavailable")? = s.policy.clone();
        s.routes.push(route);
        Ok(())
    }
    pub(crate) fn refresh(&self, route: &Route, snapshot: Snapshot) -> Result<(), String> {
        let s = self.state.lock().map_err(|_| "policy lock unavailable")?;
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
        candidate: Result<Policy, String>,
        digest: Option<String>,
    ) -> Result<(), String> {
        let mut s = self.state.lock().map_err(|_| "policy lock unavailable")?;
        s.attempts += 1;
        s.last_time = now();
        let result = (|| {
            let policy = Arc::new(candidate?);
            // Hold every policy and snapshot lock through validation and publication.
            let mut policies = Vec::new();
            let mut snapshots = Vec::new();
            for route in &s.routes {
                let (p, snap) = route.locks();
                policies.push(p.write().map_err(|_| "policy lock unavailable")?);
                snapshots.push(snap.read().map_err(|_| "snapshot lock unavailable")?);
            }
            for snapshot in &snapshots {
                crate::policy::validate::validate(&policy, snapshot)
                    .map_err(crate::serve::errors)?;
            }
            let replacement = if policy.readlog != s.policy.readlog {
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
            if let Some(replacement) = replacement {
                *log = replacement;
            }
            for current in &mut policies {
                **current = policy.clone();
            }
            Ok::<_, String>(policy)
        })();
        match result {
            Ok(policy) => {
                s.policy = policy;
                s.hash = digest.unwrap();
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
    fn attempt(&self, path: &Path, bytes: Result<Vec<u8>, std::io::Error>) {
        let digest = bytes.as_ref().ok().map(|b| hash(b));
        let candidate = bytes
            .map_err(|_| "policy file unavailable".to_owned())
            .and_then(|b| parse(path, &b));
        match self.apply(candidate, digest) {
            Ok(()) => eprintln!("serve: policy reloaded"),
            Err(e) => eprintln!(
                "serve: policy reload failed: {}",
                e.replace(['\r', '\n'], " ")
            ),
        }
    }
    pub(crate) fn status(&self) -> String {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        format!(
            "policy_sha256={}\npolicy_applied_unix_ms={}\nlast_reload={}\nlast_reload_unix_ms={}\npolicy_watch={}\n",
            s.hash,
            s.applied,
            s.last,
            s.last_time,
            self.watch_ms
                .map_or_else(|| "off".into(), |v| v.to_string())
        )
    }
    pub(crate) fn metrics(&self) -> Vec<(String, u64)> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut values = vec![
            ("nunya_policy_reloads_total".into(), s.attempts),
            ("nunya_policy_reload_failures_total".into(), s.failures),
            ("nunya_policy_applied_unix_ms".into(), s.applied),
        ];
        for route in &s.routes {
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
    pub(crate) async fn run(&self, path: PathBuf, mut reload: tokio::sync::mpsc::Receiver<()>) {
        let mut watcher = Watcher {
            attempted: self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .hash
                .clone(),
            previous: None,
            readable: true,
        };
        let mut interval =
            tokio::time::interval(Duration::from_millis(self.watch_ms.unwrap_or(3600000)));
        interval.tick().await;
        let hup = crate::serve::hangup();
        tokio::pin!(hup);
        loop {
            tokio::select! {
                _ = async { tokio::select! { _ = reload.recv() => {}, _ = &mut hup => {} } } => {
                    hup.set(crate::serve::hangup());
                    let bytes = std::fs::read(&path);
                    if let Ok(bytes) = &bytes { watcher.attempted = hash(bytes); }
                    self.attempt(&path, bytes);
                }
                _ = interval.tick(), if self.watch_ms.is_some() => {
                    let bytes = std::fs::read(&path);
                    if watcher.poll(bytes.as_deref().ok()) { self.attempt(&path, bytes); }
                }
            }
        }
    }
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
    fn hub() -> Hub {
        let p = policy();
        Hub {
            log: ReloadableLog(Arc::new(RwLock::new(
                crate::readlog::open_sink(&p.readlog).unwrap(),
            ))),
            state: Mutex::new(State {
                policy: Arc::new(p),
                routes: vec![],
                hash: hash(b"initial"),
                applied: 1,
                last: "none",
                last_time: 0,
                attempts: 0,
                failures: 0,
            }),
            watch_ms: None,
        }
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
                RwLock::new(Arc::new(policy())),
                RwLock::new(Arc::new(snapshot)),
            ))
            .unwrap();
        }
        let mut valid = policy();
        valid.snapshot_max_age_ms = 12345;
        h.apply(Ok(valid.clone()), Some(hash(b"valid"))).unwrap();
        assert!(h.apply(Ok(candidate), Some(hash(b"invalid"))).is_err());
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
        assert!(h.apply(Ok(bad_log), Some(hash(b"bad-log"))).is_err());
        h.register(Route::Test(
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], 0))),
        ))
        .unwrap();
        let state = h.state.lock().unwrap();
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
        let h = Hub::new(&policy_path, Some(100)).unwrap();
        assert_eq!(h.state.lock().unwrap().hash, hash(bytes));
        assert!(h.status().ends_with("policy_watch=100\n"));
        // Manual reloads apply unchanged bytes, and parse exactly the supplied
        // capture even if the path has already been replaced by another editor.
        std::fs::write(&policy_path, "invalid = [").unwrap();
        h.attempt(&policy_path, Ok(bytes.to_vec()));
        h.attempt(&policy_path, Ok(bytes.to_vec()));
        assert_eq!(h.state.lock().unwrap().attempts, 2);
        assert_eq!(h.state.lock().unwrap().hash, hash(bytes));
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
            snapshot: RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], 0))),
            root_key: RootKey::from_hex(&"ab".repeat(32)).unwrap(),
            tasks: crate::task::TaskStore::open_dir(&dir).unwrap(),
            readlog: Box::new(h.log.clone()),
            standing: BTreeMap::new(),
            codec: StandardCodec,
            database: "test".into(),
            clock: Arc::new(now),
            upstream: SpannerUpstream::Emulator {
                endpoint: "http://127.0.0.1:1".into(),
            },
        });
        h.register(Route::Spanner(ctx.clone())).unwrap();
        let mut new = policy();
        new.snapshot_max_age_ms = 123;
        new.readlog = ReadLogSink::File {
            path: dir.join("audit.log"),
            max_bytes: 1024,
        };
        h.apply(Ok(new), Some(hash(b"native"))).unwrap();
        assert_eq!(ctx.policy.read().unwrap().snapshot_max_age_ms, 123);
        ctx.readlog
            .write(&crate::readlog::ReadLogRecord {
                ts_unix_ms: 1,
                task: None,
                credential: "test".into(),
                statement_fingerprint: "test".into(),
                statement: "test".into(),
                outcome: crate::readlog::Outcome::Allowed,
                rows: 0,
                tokens_released: vec![],
            })
            .unwrap();
        assert!(!std::fs::read(dir.join("audit.log")).unwrap().is_empty());
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
                "policy_sha256={}\npolicy_applied_unix_ms=1\nlast_reload=none\nlast_reload_unix_ms=0\npolicy_watch=off\n",
                hash(b"initial")
            )
        );
        h.apply(Err("invalid".into()), None).unwrap_err();
        assert!(h.status().contains("last_reload=rejected\n"));
        let metrics = h.metrics();
        assert!(metrics.contains(&("nunya_policy_reloads_total".into(), 1)));
        assert!(metrics.contains(&("nunya_policy_reload_failures_total".into(), 1)));
        assert!(metrics.contains(&("nunya_policy_applied_unix_ms".into(), 1)));
    }
}
