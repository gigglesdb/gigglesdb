//! Monotonic, topology-neutral serving deadline; zero is always closed.
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
/// Kubernetes resource names use DNS-1123 subdomains, with bounded labels.
pub(crate) fn valid_lease_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}
static ANCHOR: OnceLock<Instant> = OnceLock::new();
/// Nanoseconds on the process-wide serving clock.
pub fn now_nanos() -> u64 {
    ANCHOR
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}
/// Cheap fail-closed release gate, constructed closed.
#[derive(Default, Debug)]
pub struct ServingGate {
    deadline: AtomicU64,
}
impl ServingGate {
    /// Whether the current monotonic time precedes the confirmed deadline.
    pub fn allow(&self) -> bool {
        self.allow_at(now_nanos())
    }
    fn allow_at(&self, now: u64) -> bool {
        now < self.deadline.load(Ordering::SeqCst)
    }
    #[cfg(all(test, feature = "kubernetes"))]
    pub(crate) fn allow_at_for_test(&self, now: u64) -> bool {
        self.allow_at(now)
    }
    /// Publish a deadline on the process-wide serving clock.
    pub fn extend_to(&self, deadline: u64) {
        self.deadline.store(deadline, Ordering::SeqCst);
    }
    /// Immediately stop serving.
    pub fn close(&self) {
        self.extend_to(0);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lease_names_follow_dns_subdomain_rules() {
        assert!(valid_lease_name("nunya-serving-pod.domain"));
        for name in [
            "",
            "nunya-serving-pod_1",
            "nunya-serving-Pod",
            "nunya-serving-pod.",
            "nunya-serving--",
        ] {
            assert!(!valid_lease_name(name));
        }
        assert!(!valid_lease_name(&"a".repeat(64)));
        assert!(!valid_lease_name(&format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "a".repeat(63),
            "a".repeat(63),
            "a".repeat(63)
        )));
    }
    #[test]
    fn closed_extend_close() {
        let g = ServingGate::default();
        assert!(!g.allow());
        g.extend_to(u64::MAX);
        assert!(g.allow());
        g.close();
        assert!(!g.allow());
    }
    #[test]
    fn strict_deadline() {
        let g = ServingGate::default();
        g.extend_to(10);
        assert!(g.allow_at(9));
        assert!(!g.allow_at(10));
        assert!(!g.allow_at(11));
    }
}
