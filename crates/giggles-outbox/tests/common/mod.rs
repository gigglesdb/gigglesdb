#![allow(dead_code)]
use giggles_outbox::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
pub fn fresh(label: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-outbox-{label}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        N.fetch_add(1, Ordering::Relaxed)
    ))
}
pub fn db() -> (PathBuf, Outbox) {
    let p = fresh("fixture");
    let o = Outbox::create(&p, Limits::default()).unwrap();
    (p, o)
}
pub fn intent<'a>(stream: &'a str, id: &'a str, payload: &'a [u8]) -> EnqueueIntent<'a> {
    EnqueueIntent {
        stream,
        effect_id: id,
        destination_kind: "notification",
        payload,
    }
}
pub fn seed(o: &Outbox, id: &str) {
    let mut t = o.begin().unwrap();
    t.enqueue(intent("tenant", id, b"hello")).unwrap();
    assert_eq!(t.commit().unwrap(), CommitOutcome::Committed);
}
pub fn now(n: u64) -> TrustedTime {
    TrustedTime { lower: n, upper: n }
}
pub fn claim(o: &Outbox, id_owner: &str, time: u64) -> Vec<Delivery> {
    match o.claim("tenant", id_owner, now(time), 10, 32).unwrap() {
        ClaimOutcome::Committed(v) => v,
        ClaimOutcome::Unknown => panic!("unknown claim"),
    }
}
pub fn scan() -> Scan {
    Scan {
        table: "orders".into(),
        projection: vec![],
        prefix: vec![],
        lower: None,
        upper: None,
        reverse: false,
        limit: 128,
    }
}
pub fn schema(o: &Outbox) {
    let mut t = o.begin().unwrap();
    t.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)",
        &[],
        None,
    )
    .unwrap();
    t.commit().unwrap();
}
