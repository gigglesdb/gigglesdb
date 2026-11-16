//! Cluster-scoped crash hooks for durable-step recovery tests.
use std::{
    collections::HashMap,
    sync::{
        LazyLock, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

/// Every hook name, in durable-step order.
pub const POINTS: &[&str] = &[
    "log.persist.before",
    "log.persist.after",
    "node.commit.persisted",
    "node.apply.before",
    "node.apply.after",
    "node.conf.before",
    "node.conf.after",
    "node.admin.before",
    "node.admin.after",
    "snapshot.build.data",
    "snapshot.build.manifest",
    "snapshot.receive.chunk",
    "snapshot.receive.finish",
    "snapshot.install.pending",
    "slot.install.old",
    "slot.install.new",
    "snapshot.install.state",
    "log.compact.before",
];
struct Track {
    point: &'static str,
    nth: u64,
    hits: u64,
    fired: Vec<(&'static str, Option<u64>)>,
}
static TRACKS: LazyLock<Mutex<HashMap<[u8; 16], Track>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
// The initial entry reserves env initialization; a valid env arming keeps it.
static ARMED: AtomicUsize = AtomicUsize::new(1);
static ENV: OnceLock<Option<(&'static str, u64, AtomicU64)>> = OnceLock::new();
fn env() -> &'static Option<(&'static str, u64, AtomicU64)> {
    ENV.get_or_init(|| {
        let value = std::env::var("GIGGLES_FAILPOINT").ok();
        let parsed = value.as_deref().and_then(|v| {
            let (name, nth) = v
                .split_once(':')
                .map_or((v, Some(1)), |(p, n)| (p, n.parse().ok()));
            POINTS
                .iter()
                .copied()
                .find(|p| *p == name)
                .zip(nth.filter(|n| *n > 0))
                .map(|(p, n)| (p, n, AtomicU64::new(0)))
        });
        if parsed.is_none() {
            if let Some(value) = value {
                eprintln!("warning: ignoring invalid GIGGLES_FAILPOINT: {value}");
            }
            ARMED.fetch_sub(1, Ordering::Relaxed);
        }
        parsed
    })
}
/// Panic on the nth matching cluster hit, or abort on the nth env-mode hit.
pub fn hit(cluster: &[u8; 16], node: Option<u64>, point: &'static str) {
    if ARMED.load(Ordering::Relaxed) == 0 {
        return;
    }
    if let Some((p, nth, count)) = env()
        && *p == point
        && count.fetch_add(1, Ordering::Relaxed) == nth - 1
    {
        eprintln!("failpoint crash: {point}");
        std::process::abort();
    }
    let crash = {
        let mut tracks = TRACKS.lock().unwrap();
        if let Some(t) = tracks
            .get_mut(cluster)
            .filter(|t| t.nth > 0 && t.point == point)
        {
            t.hits = t.hits.saturating_add(1);
            if t.hits == t.nth {
                t.fired.push((point, node));
                true
            } else {
                false
            }
        } else {
            false
        }
    };
    if crash {
        panic!("failpoint crash: {point}");
    }
}
/// Replace this cluster's arming with a 1-based crash count; reject unknown points.
pub fn arm(cluster: [u8; 16], point: &'static str, nth: u64) {
    assert!(POINTS.contains(&point), "unknown failpoint: {point}");
    assert!(nth > 0, "failpoint count must be 1-based");
    let mut tracks = TRACKS.lock().unwrap();
    let t = tracks.entry(cluster).or_insert_with(|| Track {
        point,
        nth: 0,
        hits: 0,
        fired: Vec::new(),
    });
    if t.nth == 0 {
        ARMED.fetch_add(1, Ordering::Relaxed);
    }
    t.point = point;
    t.nth = nth;
    t.hits = 0;
}
/// Stop counting and firing for this cluster; preserve its recorded history.
pub fn disarm(cluster: [u8; 16]) {
    if let Some(t) = TRACKS.lock().unwrap().get_mut(&cluster)
        && t.nth > 0
    {
        t.nth = 0;
        ARMED.fetch_sub(1, Ordering::Relaxed);
    }
}
/// Return fired points and their node ids in firing order.
pub fn fired(cluster: [u8; 16]) -> Vec<(&'static str, Option<u64>)> {
    TRACKS
        .lock()
        .unwrap()
        .get(&cluster)
        .map_or_else(Vec::new, |t| t.fired.clone())
}
/// Hits counted for this cluster and point since arming.
pub fn hits(cluster: [u8; 16], point: &'static str) -> u64 {
    TRACKS
        .lock()
        .unwrap()
        .get(&cluster)
        .filter(|t| t.point == point)
        .map_or(0, |t| t.hits)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counting_isolation_disarm_and_history() {
        let c = [241; 16];
        let p = POINTS[0];
        arm(c, p, 2);
        hit(&[242; 16], Some(9), p);
        hit(&c, None, POINTS[1]);
        assert_eq!(hits(c, p), 0);
        hit(&c, None, p);
        assert_eq!(hits(c, p), 1);
        let crash = std::panic::catch_unwind(|| hit(&c, Some(7), p)).unwrap_err();
        assert_eq!(
            crash.downcast_ref::<String>().unwrap(),
            &format!("failpoint crash: {p}")
        );
        assert_eq!(hits(c, p), 2);
        assert_eq!(fired(c), vec![(p, Some(7))]);
        arm(c, POINTS[1], 1);
        assert_eq!(hits(c, p), 0);
        assert_eq!(hits(c, POINTS[1]), 0);
        assert!(std::panic::catch_unwind(|| hit(&c, None, POINTS[1])).is_err());
        assert_eq!(fired(c), vec![(p, Some(7)), (POINTS[1], None)]);
        disarm(c);
        hit(&c, None, POINTS[1]);
        assert_eq!(hits(c, POINTS[1]), 1);
        disarm(c);
    }
    #[test]
    fn unique_points() {
        assert_eq!(
            POINTS
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            POINTS.len()
        );
    }
    #[test]
    #[should_panic(expected = "unknown failpoint")]
    fn unknown_point() {
        arm([243; 16], "unknown", 1);
    }
}
