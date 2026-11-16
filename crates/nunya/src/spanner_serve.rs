//! Native Spanner listener lifecycle and validated snapshot refresh.
use crate::{
    pgwire::auth::ScramVerifier,
    policy::Policy,
    readlog::ReadLog,
    spanner::{
        catalog::CatalogRunner,
        proxy::{self, SpannerContext, SpannerUpstream},
    },
    task::TaskStore,
    token::{StandardCodec, keys::RootKey},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
/// Native Spanner transport settings.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpannerNativeSection {
    /// Agent listener address.
    pub listen: String,
    /// Fully qualified upstream database.
    pub database: String,
    /// Trusted plaintext emulator endpoint.
    pub emulator_endpoint: Option<String>,
    /// Trusted Google HTTPS endpoint, defaulting to Google's service.
    pub endpoint: Option<String>,
    /// Snapshot refresh period, defaulting to thirty seconds.
    pub snapshot_refresh_ms: Option<u64>,
}
/// Parse and validate native Spanner settings without exposing source values.
pub fn parse_section(value: &toml::Value) -> Result<SpannerNativeSection, String> {
    let section: SpannerNativeSection = value
        .clone()
        .try_into()
        .map_err(|_| "spanner: invalid configuration".to_owned())?;
    if section.listen.parse::<std::net::SocketAddr>().is_err()
        || section.database.is_empty()
        || section.snapshot_refresh_ms == Some(0)
        || section
            .emulator_endpoint
            .as_ref()
            .is_some_and(|e| !e.starts_with("http://"))
        || section
            .endpoint
            .as_ref()
            .is_some_and(|e| !e.starts_with("https://"))
    {
        return Err("spanner: invalid configuration".into());
    }
    Ok(section)
}
fn publish_snapshot(
    policy: &Policy,
    current: &RwLock<Arc<crate::schema::Snapshot>>,
    snapshot: crate::schema::Snapshot,
) -> Result<(), String> {
    if crate::policy::validate::validate(policy, &snapshot).is_ok() {
        *current
            .write()
            .map_err(|_| "spanner: snapshot unavailable".to_owned())? = Arc::new(snapshot);
    }
    Ok(())
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u64::MAX as u128) as u64)
}
async fn stopped(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}
/// Shared state assigned to a configured native Spanner route.
pub struct RouteState {
    /// Shared durable authority.
    pub authority: Arc<crate::state::authority::Authority>,
    /// Shared connection registry.
    pub registry: crate::state::runtime::Registry,
    /// Validated configured route identifier.
    pub route: crate::state::model::RouteId,
}
/// Serve native Spanner requests until shutdown, retaining old snapshots on refresh failures.
#[allow(clippy::too_many_arguments)]
pub async fn run_spanner(
    section: &SpannerNativeSection,
    policy: Arc<Policy>,
    root_key: RootKey,
    tasks: TaskStore,
    state: RouteState,
    readlog: Box<dyn ReadLog>,
    standing: BTreeMap<String, ScramVerifier>,
    _policy_path: PathBuf,
    tls: Option<tonic::transport::ServerTlsConfig>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    run_spanner_with_hub(
        section, policy, root_key, tasks, state, readlog, standing, None, tls, shutdown,
    )
    .await
}
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_spanner_with_hub(
    section: &SpannerNativeSection,
    policy: Arc<Policy>,
    root_key: RootKey,
    tasks: TaskStore,
    state: RouteState,
    readlog: Box<dyn ReadLog>,
    standing: BTreeMap<String, ScramVerifier>,
    hub: Option<Arc<crate::reload::Hub>>,
    tls: Option<tonic::transport::ServerTlsConfig>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    if *shutdown.borrow() {
        return Ok(());
    }
    let work = async {
        let addr = section
            .listen
            .parse()
            .map_err(|_| "spanner: invalid listener".to_owned())?;
        let period = section.snapshot_refresh_ms.unwrap_or(30000);
        if period == 0 {
            return Err("spanner: invalid refresh interval".into());
        }
        let upstream = match &section.emulator_endpoint {
            Some(endpoint) => SpannerUpstream::Emulator {
                endpoint: endpoint.clone(),
            },
            None => SpannerUpstream::Google {
                endpoint: section
                    .endpoint
                    .clone()
                    .unwrap_or_else(|| "https://spanner.googleapis.com".into()),
            },
        };
        let runner = CatalogRunner::new(&upstream, section.database.clone())
            .await
            .map_err(|_| "spanner: catalog unavailable".to_owned())?;
        let snapshot = crate::schema::spanner_gsql::load_gsql_snapshot(&runner, now())
            .await
            .map_err(|_| "spanner: catalog unavailable".to_owned())?;
        if hub.is_none() {
            crate::policy::validate::validate(&policy, &snapshot)
                .map_err(|_| "spanner: policy validation failed".to_owned())?;
        }
        let ctx = Arc::new(SpannerContext {
            authority: state.authority,
            registry: state.registry,
            route: state.route,
            policy: RwLock::new(policy),
            snapshot: RwLock::new(Arc::new(snapshot)),
            root_key,
            tasks,
            readlog,
            standing,
            codec: StandardCodec,
            database: section.database.clone(),
            clock: Arc::new(now),
            upstream,
        });
        if let Some(hub) = &hub {
            hub.register(crate::reload::Route::Spanner(ctx.clone()))?;
        }
        let svc = proxy::build(ctx.clone())
            .await
            .map_err(|_| "spanner: upstream unavailable".to_owned())?;
        let refresh = async {
            let mut interval = tokio::time::interval(Duration::from_millis(period));
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Ok(snapshot) =
                    crate::schema::spanner_gsql::load_gsql_snapshot(&runner, now()).await
                {
                    if let Some(hub) = &hub {
                        let _ = hub.refresh(&crate::reload::Route::Spanner(ctx.clone()), snapshot);
                        continue;
                    }
                    let policy = ctx
                        .policy
                        .read()
                        .map_err(|_| "spanner: policy unavailable".to_owned())?
                        .clone();
                    publish_snapshot(&policy, &ctx.snapshot, snapshot)?;
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        };
        tokio::select! {
            result = proxy::serve(addr, svc, tls) => result.map_err(|_| "spanner: listener failed".to_owned()),
            result = refresh => result,
        }
    };
    tokio::select! { result = work => result, _ = stopped(&mut shutdown) => Ok(()) }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn section_is_strict_and_errors_are_sanitized() {
        let base: toml::Value = toml::from_str(
            "listen='127.0.0.1:1234'\ndatabase='projects/p/instances/i/databases/d'",
        )
        .unwrap();
        assert!(parse_section(&base).is_ok());
        for suffix in [
            "\nsecret='DO_NOT_LEAK'",
            "\nsnapshot_refresh_ms=0",
            "\nendpoint='http://DO_NOT_LEAK'",
        ] {
            let mut value = base.clone();
            let extra: toml::Value = toml::from_str(suffix).unwrap();
            value
                .as_table_mut()
                .unwrap()
                .extend(extra.as_table().unwrap().clone());
            let error = parse_section(&value).err().unwrap();
            assert!(!error.contains("DO_NOT_LEAK"));
        }
    }
    #[test]
    fn invalid_refresh_retains_old_timestamp_and_valid_refresh_publishes() {
        use crate::{
            policy::{DomainName, ReadLogSink, Treatment},
            schema::{ColumnRef, Snapshot},
        };
        let mut policy = Policy {
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 1000,
            readlog: ReadLogSink::Stdout,
        };
        let original = Arc::new(Snapshot::new(vec![], vec![], vec![String::new()], 10));
        let current = RwLock::new(original.clone());
        policy.columns.insert(
            ColumnRef {
                schema: String::new(),
                table: "missing".into(),
                column: "secret".into(),
            },
            Treatment::Blind {
                domain: DomainName("secret".into()),
            },
        );
        publish_snapshot(
            &policy,
            &current,
            Snapshot::new(vec![], vec![], vec![String::new()], 20),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&original, &current.read().unwrap()));
        assert_eq!(current.read().unwrap().taken_at_unix_ms, 10);
        policy.columns.clear();
        publish_snapshot(
            &policy,
            &current,
            Snapshot::new(vec![], vec![], vec![String::new()], 30),
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&original, &current.read().unwrap()));
        assert_eq!(current.read().unwrap().taken_at_unix_ms, 30);
    }
}
