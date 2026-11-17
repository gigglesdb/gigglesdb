//! Strict configuration reload classification and publication framework.
use crate::{
    config::{Config, TlsSection},
    pgwire::{auth::ScramVerifier, upstream::UpstreamTls},
    reload::Hub,
};
use std::{
    path::{Path, PathBuf},
    sync::{Mutex, Weak},
};

/// Changed setting names, divided by whether a restart is required.
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct Diff {
    /// Settings accepted by the reload framework.
    pub(crate) reloadable: Vec<&'static str>,
    /// Settings retained until process restart.
    pub(crate) restart_only: Vec<&'static str>,
}
const RESTART_ONLY: &[&str] = &[
    "listen",
    "admin_listen",
    "root_key",
    "state_dir",
    "dialect",
    "allow_plaintext",
    "tls.enabled",
    "upstream",
    "upstream.route_id",
    "spanner_native",
    "spanner_native.route_id",
    "policy",
    "policy_watch_ms",
    "management.listen",
    "management.enabled",
    "management.tls.enabled",
];
fn tls_parts(t: &TlsSection) -> (&Path, &Path) {
    (&t.cert_file, &t.key_file)
}
fn verifier_parts(v: &ScramVerifier) -> (&[u8], u32, &[u8; 32], &[u8; 32]) {
    (&v.salt, v.iterations, &v.stored_key, &v.server_key)
}
fn upstream_tls_parts(t: &UpstreamTls) -> (u8, Option<&Path>) {
    match t {
        UpstreamTls::Disable => (0, None),
        UpstreamTls::Require => (1, None),
        UpstreamTls::VerifyFull { ca_file } => (2, Some(ca_file)),
    }
}
/// Classify every configuration field without exposing setting values.
pub(crate) fn classify(running: &Config, new: &Config) -> Diff {
    let Config {
        path: _,
        revision: _,
        listen: a_listen,
        admin_listen: a_admin_listen,
        postgres_policy: a_postgres_policy,
        spanner_policy: a_spanner_policy,
        root_key: a_root_key,
        state_dir: a_state_dir,
        schemas: a_schemas,
        search_path: a_search_path,
        snapshot_refresh_ms: a_snapshot_refresh_ms,
        policy_watch_ms: a_policy_watch_ms,
        allow_plaintext: a_allow_plaintext,
        tls: a_tls,
        upstream: a_upstream,
        spanner_native: a_spanner_native,
        dialect: a_dialect,
        max_connections: a_max_connections,
        session: a_session,
        management: a_management,
        postgres_route_id: a_postgres_route_id,
        spanner_route_id: a_spanner_route_id,
        standing: a_standing,
    } = running;
    let Config {
        path: _,
        revision: _,
        listen: b_listen,
        admin_listen: b_admin_listen,
        postgres_policy: b_postgres_policy,
        spanner_policy: b_spanner_policy,
        root_key: b_root_key,
        state_dir: b_state_dir,
        schemas: b_schemas,
        search_path: b_search_path,
        snapshot_refresh_ms: b_snapshot_refresh_ms,
        policy_watch_ms: b_policy_watch_ms,
        allow_plaintext: b_allow_plaintext,
        tls: b_tls,
        upstream: b_upstream,
        spanner_native: b_spanner_native,
        dialect: b_dialect,
        max_connections: b_max_connections,
        session: b_session,
        management: b_management,
        postgres_route_id: b_postgres_route_id,
        spanner_route_id: b_spanner_route_id,
        standing: b_standing,
    } = new;
    let mut diff = Diff::default();
    macro_rules! changed {
        ($class:ident, $name:literal, $condition:expr) => {
            if $condition {
                diff.$class.push($name);
            }
        };
    }
    changed!(restart_only, "listen", a_listen != b_listen);
    changed!(
        restart_only,
        "admin_listen",
        a_admin_listen != b_admin_listen
    );
    changed!(restart_only, "root_key", a_root_key != b_root_key);
    changed!(restart_only, "state_dir", a_state_dir != b_state_dir);
    changed!(restart_only, "dialect", a_dialect != b_dialect);
    changed!(
        restart_only,
        "allow_plaintext",
        a_allow_plaintext != b_allow_plaintext
    );
    changed!(
        restart_only,
        "policy_watch_ms",
        a_policy_watch_ms != b_policy_watch_ms
    );
    changed!(
        reloadable,
        "max_connections",
        a_max_connections != b_max_connections
    );
    changed!(reloadable, "schemas", a_schemas != b_schemas);
    changed!(reloadable, "search_path", a_search_path != b_search_path);
    changed!(
        reloadable,
        "snapshot_refresh_ms",
        a_snapshot_refresh_ms != b_snapshot_refresh_ms
    );
    changed!(reloadable, "session", a_session != b_session);
    changed!(
        restart_only,
        "policy",
        a_postgres_policy != b_postgres_policy || a_spanner_policy != b_spanner_policy
    );
    changed!(
        restart_only,
        "upstream.route_id",
        a_postgres_route_id != b_postgres_route_id
    );
    changed!(
        restart_only,
        "spanner_native.route_id",
        a_spanner_route_id != b_spanner_route_id
    );
    changed!(
        reloadable,
        "standing",
        !a_standing
            .iter()
            .map(|s| (&s.name, verifier_parts(&s.verifier)))
            .eq(b_standing
                .iter()
                .map(|s| (&s.name, verifier_parts(&s.verifier))))
    );
    match (a_tls, b_tls) {
        (Some(a), Some(b)) => changed!(reloadable, "tls", tls_parts(a) != tls_parts(b)),
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => diff.restart_only.push("tls.enabled"),
    }
    let upstream_parts = |u: &crate::pgwire::upstream::UpstreamConfig| {
        // Compare secrets directly; never format connection settings.
        (u.port, u.connect_timeout_ms)
    };
    let upstream_changed = match (a_upstream, b_upstream) {
        (Some(a), Some(b)) => {
            upstream_parts(a) != upstream_parts(b)
                || a.host != b.host
                || a.user != b.user
                || a.password != b.password
                || a.dbname != b.dbname
                || a.application_name != b.application_name
                || upstream_tls_parts(&a.tls) != upstream_tls_parts(&b.tls)
        }
        (None, None) => false,
        (Some(_), None) | (None, Some(_)) => true,
    };
    changed!(restart_only, "upstream", upstream_changed);
    match (a_spanner_native, b_spanner_native) {
        (Some(a), Some(b)) => {
            changed!(
                restart_only,
                "spanner_native",
                a.listen != b.listen
                    || a.database != b.database
                    || a.emulator_endpoint != b.emulator_endpoint
                    || a.endpoint != b.endpoint
            );
            changed!(
                reloadable,
                "spanner_native.snapshot_refresh_ms",
                a.snapshot_refresh_ms != b.snapshot_refresh_ms
            );
        }
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => diff.restart_only.push("spanner_native"),
    }
    match (a_management, b_management) {
        (Some(a), Some(b)) => {
            changed!(restart_only, "management.listen", a.listen != b.listen);
            changed!(
                reloadable,
                "management.admins",
                !a.admins
                    .iter()
                    .map(|s| (&s.name, verifier_parts(&s.verifier)))
                    .eq(b
                        .admins
                        .iter()
                        .map(|s| (&s.name, verifier_parts(&s.verifier))))
            );
            match (&a.tls, &b.tls) {
                (Some(a), Some(b)) => {
                    changed!(reloadable, "management.tls", tls_parts(a) != tls_parts(b))
                }
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => {
                    diff.restart_only.push("management.tls.enabled")
                }
            }
        }
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => diff.restart_only.push("management.enabled"),
    }
    diff
}
fn copy_verifier(v: &ScramVerifier) -> ScramVerifier {
    ScramVerifier {
        salt: v.salt.clone(),
        iterations: v.iterations,
        stored_key: v.stored_key,
        server_key: v.server_key,
    }
}
/// Copy startup settings without rereading a potentially edited file.
pub(crate) fn copy_config(c: &Config) -> Config {
    Config {
        path: c.path.clone(),
        revision: c.revision.clone(),
        listen: c.listen,
        admin_listen: c.admin_listen,
        postgres_policy: c.postgres_policy.clone(),
        spanner_policy: c.spanner_policy.clone(),
        root_key: c.root_key.clone(),
        state_dir: c.state_dir.clone(),
        schemas: c.schemas.clone(),
        search_path: c.search_path.clone(),
        snapshot_refresh_ms: c.snapshot_refresh_ms,
        policy_watch_ms: c.policy_watch_ms,
        allow_plaintext: c.allow_plaintext,
        tls: c.tls.clone(),
        upstream: c.upstream_config(),
        spanner_native: c.spanner_native.as_ref().map(|s| {
            crate::spanner_serve::SpannerNativeSection {
                listen: s.listen.clone(),
                database: s.database.clone(),
                emulator_endpoint: s.emulator_endpoint.clone(),
                endpoint: s.endpoint.clone(),
                snapshot_refresh_ms: s.snapshot_refresh_ms,
            }
        }),
        dialect: c.dialect,
        max_connections: c.max_connections,
        session: c.session.clone(),
        management: c
            .management
            .as_ref()
            .map(|m| crate::config::ManagementSection {
                listen: m.listen,
                tls: m.tls.clone(),
                admins: m
                    .admins
                    .iter()
                    .map(|a| crate::config::AdminSection {
                        name: a.name.clone(),
                        verifier: copy_verifier(&a.verifier),
                    })
                    .collect(),
            }),
        postgres_route_id: c.postgres_route_id.clone(),
        spanner_route_id: c.spanner_route_id.clone(),
        standing: c
            .standing
            .iter()
            .map(|s| crate::config::StandingSection {
                name: s.name.clone(),
                verifier: copy_verifier(&s.verifier),
            })
            .collect(),
    }
}
struct State {
    running: Config,
    last: &'static str,
    time: u64,
    restart_pending: Vec<&'static str>,
    applied: Vec<&'static str>,
    attempts: u64,
    failures: u64,
}
/// Serialized configuration reloads and data-free operational status.
pub(crate) struct ConfigReloader {
    path: PathBuf,
    hub: Weak<Hub>,
    state: Mutex<State>,
}
/// Prepared configuration; future appliers may add fallible resources here.
struct Prepared {
    config: Config,
    diff: Diff,
}
fn prepare(running: &Config, config: Config) -> Result<Prepared, &'static str> {
    Ok(Prepared {
        diff: classify(running, &config),
        config,
    })
}
impl ConfigReloader {
    /// Retain the startup configuration and weak publication target.
    pub(crate) fn new(config: Config, hub: Weak<Hub>) -> Self {
        Self {
            path: config.path.clone(),
            hub,
            state: Mutex::new(State {
                running: config,
                last: "none",
                time: 0,
                restart_pending: vec![],
                applied: vec![],
                attempts: 0,
                failures: 0,
            }),
        }
    }
    /// Last applied configuration digest used to seed polling.
    pub(crate) fn revision(&self) -> String {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .running
            .revision
            .clone()
    }
    /// Configuration path used by the debounce watcher.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    /// Read, prepare, and commit one complete config reload attempt.
    pub(crate) async fn reload(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.attempts += 1;
        state.time = crate::serve::now();
        let prepared = Config::load(&self.path)
            .map_err(|errors| errors.join("; "))
            .and_then(|new| prepare(&state.running, new).map_err(str::to_owned));
        let result = prepared.and_then(|prepared| {
            let hub = self.hub.upgrade().ok_or("reload service unavailable")?;
            // Revision publication is the only fallible commit operation today.
            hub.set_config_revision(&prepared.config.revision)?;
            self.commit(&mut state, prepared);
            Ok(())
        });
        if let Err(reason) = result {
            state.last = "rejected";
            state.failures += 1;
            eprintln!("serve: config reload rejected: {reason}");
        }
    }
    fn commit(&self, state: &mut State, prepared: Prepared) {
        let Prepared { config: new, diff } = prepared;
        state.restart_pending = diff.restart_only;
        state.applied = diff.reloadable;
        for name in &state.restart_pending {
            eprintln!(
                "serve: config reload: restart-only setting {name} changed; ignored until restart"
            );
        }
        let running = &mut state.running;
        running.revision = new.revision;
        running.max_connections = new.max_connections;
        running.schemas = new.schemas;
        running.search_path = new.search_path;
        running.snapshot_refresh_ms = new.snapshot_refresh_ms;
        running.standing = new.standing;
        running.session = new.session;
        if running.tls.is_some() && new.tls.is_some() {
            running.tls = new.tls;
        }
        if let (Some(a), Some(b)) = (&mut running.spanner_native, new.spanner_native) {
            a.snapshot_refresh_ms = b.snapshot_refresh_ms;
        }
        if let (Some(a), Some(b)) = (&mut running.management, new.management) {
            a.admins = b.admins;
            if a.tls.is_some() && b.tls.is_some() {
                a.tls = b.tls;
            }
        }
        state.last = "applied";
    }
    /// Current restart-only changes, by setting name.
    pub(crate) fn restart_pending(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .restart_pending
            .iter()
            .map(|s| (*s).into())
            .collect()
    }
    /// Most recent configuration reload outcome.
    pub(crate) fn last_reload(&self) -> String {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last
            .into()
    }
    /// One administration status line containing no configuration values.
    pub(crate) fn status(&self) -> String {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = if s.restart_pending.is_empty() {
            "none".into()
        } else {
            s.restart_pending.join(",")
        };
        format!(
            "config revision={} last_reload={} last_reload_unix_ms={} restart_pending={}\n",
            s.running.revision, s.last, s.time, pending
        )
    }
    /// Reload counters and a complete restart-only pending gauge set.
    pub(crate) fn metrics(&self) -> Vec<(String, u64)> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut metrics = vec![
            ("nunya_config_reloads_total".into(), s.attempts),
            ("nunya_config_reload_failures_total".into(), s.failures),
        ];
        metrics.extend(RESTART_ONLY.iter().map(|name| {
            (
                format!("nunya_config_restart_only_ignored{{setting=\"{name}\"}}"),
                u64::from(s.restart_pending.contains(name)),
            )
        }));
        metrics
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Arc;
    pub(crate) struct Fixture {
        pub(crate) dir: PathBuf,
        pub(crate) path: PathBuf,
        pub(crate) text: String,
    }
    impl Fixture {
        pub(crate) fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let dir = std::env::temp_dir().join(format!(
                "config-reload-{}",
                data_encoding::HEXLOWER.encode(&nonce)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("root"), [1; 32]).unwrap();
            std::fs::write(dir.join("root2"), [2; 32]).unwrap();
            std::fs::write(dir.join("password"), "initial-password").unwrap();
            std::fs::write(dir.join("password2"), "changed-secret-password").unwrap();
            for name in ["cert", "cert2"] {
                std::fs::write(
                    dir.join(name),
                    include_bytes!("../tests/fixtures/mgmt/localhost.crt"),
                )
                .unwrap();
            }
            for name in ["key", "key2"] {
                std::fs::write(
                    dir.join(name),
                    include_bytes!("../tests/fixtures/mgmt/localhost.key"),
                )
                .unwrap();
            }
            std::fs::write(
                dir.join("policy"),
                "snapshot_max_age_ms=60000\n[readlog]\nsink='stdout'\n",
            )
            .unwrap();
            let verifier = ScramVerifier::from_password("fixture", b"salt", 1).to_pg_string();
            let text = format!(
                r#"
listen='127.0.0.1:6432'
admin_listen='127.0.0.1:6480'
root_key_file='root'
state_dir='state'
policy='policy'
allow_plaintext=true
policy_watch_ms=100
max_connections=256
schemas=['public']
search_path=['public']
snapshot_refresh_ms=30000
standing=[{{name='agent',verifier='{verifier}'}}]
[tls]
cert_file='cert'
key_file='key'
[session]
max_lifetime='8h'
idle_timeout='1h'
[upstream]
host='localhost'
port=5432
user='nunya'
password_file='password'
dbname='app'
tls='disable'
connect_timeout_ms=5000
application_name='nunya'
route_id='pg'
[spanner_native]
listen='127.0.0.1:6433'
database='projects/p/instances/i/databases/d'
emulator_endpoint='http://127.0.0.1:9010'
snapshot_refresh_ms=30000
route_id='native'
[management]
listen='127.0.0.1:6490'
admin=[{{name='admin',verifier='{verifier}'}}]
[management.tls]
cert_file='cert'
key_file='key'
"#
            );
            let path = dir.join("config.toml");
            std::fs::write(&path, &text).unwrap();
            Self { dir, path, text }
        }
        pub(crate) fn load(&self, text: &str) -> Config {
            std::fs::write(&self.path, text).unwrap();
            Config::load(&self.path).unwrap()
        }
        pub(crate) fn hub(&self) -> Arc<Hub> {
            let id = crate::state::model::RouteId::new("pg").unwrap();
            let hub = Hub::new(&[(id, self.dir.join("policy"))], None).unwrap();
            hub.set_config_revision(&Config::load(&self.path).unwrap().revision)
                .unwrap();
            hub
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn edit(text: &str, path: &str, value: &str) -> String {
        let mut doc: toml::Value = toml::from_str(text).unwrap();
        let keys: Vec<_> = path.split('.').collect();
        let mut table = doc.as_table_mut().unwrap();
        for key in &keys[..keys.len() - 1] {
            table = table.get_mut(*key).unwrap().as_table_mut().unwrap();
        }
        let key = keys[keys.len() - 1];
        if value == "REMOVE" {
            table.remove(key);
        } else {
            let v: toml::Value = toml::from_str(&format!("value={value}")).unwrap();
            table.insert(key.into(), v["value"].clone());
        }
        toml::to_string(&doc).unwrap()
    }
    #[test]
    fn every_setting_is_classified_individually() {
        let f = Fixture::new();
        let running = f.load(&f.text);
        let cases = [
            ("listen", "listen", "'127.0.0.1:7432'", false),
            ("admin_listen", "admin_listen", "'127.0.0.1:7480'", false),
            ("root_key", "root_key_file", "'root2'", false),
            ("state_dir", "state_dir", "'state2'", false),
            ("dialect", "upstream.dialect", "'spanner-pg'", false),
            ("allow_plaintext", "allow_plaintext", "false", false),
            ("tls.enabled", "tls", "REMOVE", false),
            ("upstream.route_id", "upstream.route_id", "'pg2'", false),
            (
                "spanner_native",
                "spanner_native.listen",
                "'127.0.0.1:7433'",
                false,
            ),
            (
                "spanner_native",
                "spanner_native.database",
                "'projects/p/instances/i/databases/e'",
                false,
            ),
            (
                "spanner_native",
                "spanner_native.emulator_endpoint",
                "'http://127.0.0.1:9011'",
                false,
            ),
            (
                "spanner_native",
                "spanner_native.endpoint",
                "'https://spanner.googleapis.com'",
                false,
            ),
            (
                "spanner_native.route_id",
                "spanner_native.route_id",
                "'native2'",
                false,
            ),
            ("policy", "policy", "'other-policy'", false),
            ("policy", "upstream.policy", "'other-policy'", false),
            ("policy", "spanner_native.policy", "'other-policy'", false),
            ("policy_watch_ms", "policy_watch_ms", "200", false),
            (
                "management.listen",
                "management.listen",
                "'127.0.0.1:7490'",
                false,
            ),
            ("management.enabled", "management", "REMOVE", false),
            ("management.tls.enabled", "management.tls", "REMOVE", false),
            ("tls", "tls.cert_file", "'cert2'", true),
            ("tls", "tls.key_file", "'key2'", true),
            ("max_connections", "max_connections", "257", true),
            ("schemas", "schemas", "['other']", true),
            ("search_path", "search_path", "['other']", true),
            ("snapshot_refresh_ms", "snapshot_refresh_ms", "30001", true),
            (
                "spanner_native.snapshot_refresh_ms",
                "spanner_native.snapshot_refresh_ms",
                "30001",
                true,
            ),
            ("standing", "standing", "[]", true),
            ("session", "session.max_lifetime", "'9h'", true),
            ("session", "session.idle_timeout", "'2h'", true),
            ("management.admins", "management.admin", "[]", true),
            (
                "management.tls",
                "management.tls.cert_file",
                "'cert2'",
                true,
            ),
            ("management.tls", "management.tls.key_file", "'key2'", true),
            ("upstream", "upstream.host", "'other'", false),
            ("upstream", "upstream.port", "5433", false),
            ("upstream", "upstream.user", "'other'", false),
            ("upstream", "upstream.password_file", "'password2'", false),
            ("upstream", "upstream.dbname", "'other'", false),
            ("upstream", "upstream.tls", "'require'", false),
            ("upstream", "upstream.ca_file", "'cert'", false),
            ("upstream", "upstream.connect_timeout_ms", "5001", false),
            ("upstream", "upstream.application_name", "'other'", false),
        ];
        for (name, path, value, reloadable) in cases {
            // ca_file is effective only in verify-full mode.
            let base = if path == "upstream.ca_file" {
                edit(
                    &edit(&f.text, "upstream.tls", "'verify-full'"),
                    path,
                    "'cert2'",
                )
            } else {
                f.text.clone()
            };
            let a = if path == "upstream.ca_file" {
                f.load(&base)
            } else {
                copy_config(&running)
            };
            let b = f.load(&edit(&base, path, value));
            let diff = classify(&a, &b);
            assert_eq!(
                diff.reloadable,
                if reloadable { vec![name] } else { vec![] },
                "{path}"
            );
            assert_eq!(
                diff.restart_only,
                if reloadable { vec![] } else { vec![name] },
                "{path}"
            );
            // Section enable/disable and path replacements are symmetric.
            let reverse = classify(&b, &a);
            assert_eq!(reverse, diff, "{path}");
        }
        let mut same = copy_config(&running);
        same.revision = "ignored".into();
        same.path = PathBuf::from("ignored");
        assert_eq!(classify(&running, &same), Diff::default());
    }
    #[test]
    fn removed_route_sections_classify_effective_policy_and_identity_changes() {
        let f = Fixture::new();
        let both = f.load(&f.text);
        for (section, mut expected) in [
            ("upstream", vec!["upstream", "upstream.route_id", "policy"]),
            ("spanner_native", vec!["spanner_native", "spanner_native.route_id", "policy"]),
        ] {
            let one = f.load(&edit(&f.text, section, "REMOVE"));
            let mut diff = classify(&both, &one);
            assert!(diff.reloadable.is_empty());
            diff.restart_only.sort(); expected.sort();
            assert_eq!(diff.restart_only, expected);
            let mut reverse = classify(&one, &both);
            reverse.restart_only.sort();
            assert_eq!(reverse, diff);
        }
    }
    #[tokio::test]
    async fn revision_publication_failure_rejects_without_partial_changes() {
        let f = Fixture::new();
        let startup = f.load(&f.text);
        let hub = f.hub();
        let original = startup.revision.clone();
        let r = ConfigReloader::new(startup, Arc::downgrade(&hub));
        f.load(&edit(&f.text, "max_connections", "999"));
        let lock = hub.log.1.clone();
        let _ = std::thread::spawn(move || {
            let _guard = lock.write().unwrap();
            panic!("test revision lock poisoning");
        }).join();
        r.reload().await;
        let s = r.state.lock().unwrap();
        assert_eq!(s.last, "rejected");
        assert_eq!(s.running.max_connections, 256);
        assert_eq!(s.running.revision, original);
        // The earlier unit lock was acquired, but its revision was never changed.
        let route = crate::state::model::RouteId::new("pg").unwrap();
        assert_eq!(hub.route_log(&route).unwrap().1.read().unwrap().config, original);
    }
    #[tokio::test]
    async fn rejection_changes_only_attempt_metadata() {
        let f = Fixture::new();
        let startup = f.load(&f.text);
        let revision = startup.revision.clone();
        let hub = f.hub();
        let r = ConfigReloader::new(startup, Arc::downgrade(&hub));
        std::fs::write(&f.path, "secret = [").unwrap();
        r.reload().await;
        let s = r.state.lock().unwrap();
        assert_eq!(s.last, "rejected");
        assert_eq!(s.failures, 1);
        assert_eq!(s.attempts, 1);
        assert_eq!(s.running.revision, revision);
        assert_eq!(s.running.max_connections, 256);
        assert_eq!(hub.log.1.read().unwrap().config, revision);
    }
    #[tokio::test]
    async fn mixed_reload_retains_restart_changes_until_reverted_without_values() {
        let f = Fixture::new();
        let startup = f.load(&f.text);
        let hub = f.hub();
        let r = ConfigReloader::new(startup, Arc::downgrade(&hub));
        let edited = edit(
            &edit(
                &edit(&f.text, "listen", "'127.0.0.1:7432'"),
                "max_connections",
                "999",
            ),
            "upstream.password_file",
            "'password2'",
        );
        let new = f.load(&edited);
        for _ in 0..2 {
            r.reload().await;
            assert_eq!(r.restart_pending(), ["listen", "upstream"]);
            let s = r.state.lock().unwrap();
            assert_eq!(s.running.max_connections, 999);
            assert_eq!(
                s.applied,
                if s.attempts == 1 {
                    vec!["max_connections"]
                } else {
                    vec![]
                }
            );
            assert_eq!(s.running.listen.unwrap().port(), 6432);
            assert_eq!(
                s.running.upstream.as_ref().unwrap().password.as_str(),
                "initial-password"
            );
            assert_eq!(s.running.revision, new.revision);
            drop(s);
            assert_eq!(hub.log.1.read().unwrap().config, new.revision);
            assert!(r.status().contains("last_reload=applied"));
            assert!(r.status().contains("restart_pending=listen,upstream"));
            let text = format!("{}{:?}", r.status(), r.metrics());
            assert!(!text.contains("changed-secret-password"));
            assert!(!text.contains("127.0.0.1:7432"));
            for name in RESTART_ONLY {
                assert!(r.metrics().contains(&(
                    format!("nunya_config_restart_only_ignored{{setting=\"{name}\"}}"),
                    u64::from(["listen", "upstream"].contains(name))
                )));
            }
        }
        f.load(&f.text);
        r.reload().await;
        assert!(r.restart_pending().is_empty());
        assert!(r.status().contains("restart_pending=none"));
        assert!(
            r.metrics()
                .contains(&("nunya_config_reloads_total".into(), 3))
        );
        assert!(
            r.metrics()
                .contains(&("nunya_config_reload_failures_total".into(), 0))
        );
    }
}
