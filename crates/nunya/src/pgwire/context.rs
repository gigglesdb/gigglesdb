//! Shared, reloadable gateway state and data-free counters.
use super::upstream::UpstreamConfig;
use crate::{
    dialect::pg::PgLowering,
    error::NyCode,
    policy::Policy,
    readlog::ReadLog,
    schema::Snapshot,
    token::{StandardCodec, keys::RootKey},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
/// Counters shared by all gateway connections.
#[derive(Default)]
pub struct Metrics {
    /// Total accepted connections.
    pub connections: AtomicU64,
    /// Statements accepted by policy.
    pub statements_allowed: AtomicU64,
    /// Refusals indexed in NyCode::ALL order.
    pub statements_refused: [AtomicU64; NyCode::ALL.len()],
    /// Bytes read and written on agent transports.
    pub bytes: AtomicU64,
}
/// Plain counter values suitable for monitoring.
pub struct MetricsSnapshot {
    /// Total accepted connections.
    pub connections: u64,
    /// Total allowed statements.
    pub statements_allowed: u64,
    /// Refusals indexed in NyCode::ALL order.
    pub statements_refused: [u64; NyCode::ALL.len()],
    /// Total agent bytes.
    pub bytes: u64,
}
impl Metrics {
    /// Read counters without locking or exposing client data.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections: self.connections.load(Ordering::Relaxed),
            statements_allowed: self.statements_allowed.load(Ordering::Relaxed),
            statements_refused: std::array::from_fn(|i| {
                self.statements_refused[i].load(Ordering::Relaxed)
            }),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn refused(&self, code: NyCode) {
        if let Some(i) = NyCode::ALL.iter().position(|c| *c == code) {
            self.statements_refused[i].fetch_add(1, Ordering::Relaxed);
        }
    }
}
type CancelMap = HashMap<(i32, [u8; 4]), (i32, Vec<u8>)>;

/// Gateway resources and configuration shared by sessions.
pub struct GatewayContext {
    /// Shared durable session authority.
    pub authority: Arc<crate::state::authority::Authority>,
    /// Shared live connection registry.
    pub registry: crate::state::runtime::Registry,
    /// Validated route identifier.
    pub route: crate::state::model::RouteId,
    /// Reloadable policy.
    pub policy: RwLock<Arc<Policy>>,
    /// Reloadable schema snapshot.
    pub snapshot: RwLock<Arc<Snapshot>>,
    /// Root token encryption key.
    pub root_key: RootKey,
    /// Mandatory audit sink.
    pub readlog: Box<dyn ReadLog>,
    /// Upstream connection settings.
    pub upstream: UpstreamConfig,
    /// Trusted upstream search path.
    pub search_path: Vec<String>,
    /// SQL lowering implementation.
    pub lowering: PgLowering,
    /// Token codec.
    pub codec: StandardCodec,
    /// Millisecond Unix clock.
    pub clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// Random downstream cancellation keys mapped to upstream keys.
    pub cancel_map: Mutex<CancelMap>,
    /// Data-free monitoring counters.
    pub metrics: Metrics,
    /// Optional downstream TLS configuration.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Whether unencrypted agent connections are permitted.
    pub allow_plaintext: bool,
}
