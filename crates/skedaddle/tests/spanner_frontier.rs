//! D-A2 crash boundaries: real local buffer manifests and fsync journal objects.
#[path = "support/root.rs"]
mod test_root;

use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    coordinator::{
        Materialized, Transition,
        journal::{Entry, Journal, local_store},
    },
    engine::{decode_buffered_position, encode_buffered_position},
    event::{AssembledTxn, Position, PositionOrder, Redelivery, SourceIdentity, TxnId},
    spanner::frontier::{
        DuplicateFilter, Error, Frontier, Partition, SpannerOrder, SpannerPosition,
    },
};
use std::{
    path::Path,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};
fn order() -> SpannerOrder {
    SpannerOrder::new("p", "i", "d", "s", 0).unwrap()
}
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "spanner".into(),
        system_id: "p/i".into(),
        lineage: "0".into(),
        db_name: "d".into(),
        stream: "s".into(),
    }
}
fn position(p: SpannerPosition) -> Position {
    Position::new(order().domain(), p.encode())
}
fn txn(ts: i128) -> AssembledTxn {
    AssembledTxn::new(
        source(),
        TxnId(ts.to_be_bytes().to_vec()),
        position(SpannerPosition::Txn {
            commit_ts: ts,
            server_transaction_id: ts.to_be_bytes().to_vec(),
        }),
        None,
        vec![],
    )
    .unwrap()
}
fn child(t: &[u8], parents: &[&[u8]], ts: i128) -> Partition {
    Partition::registered(t.to_vec(), parents.iter().map(|p| p.to_vec()).collect(), ts)
}
fn journal(root: &Path) -> Journal {
    std::fs::create_dir_all(root).unwrap();
    Journal::new(
        Arc::new(local_store(root).unwrap()),
        "s2",
        "migration",
        Duration::from_secs(3),
    )
    .unwrap()
}
async fn writer(root: &Path) -> BufferWriter {
    for sub in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(sub)).unwrap();
    }
    let (tx, _rx) = mpsc::channel();
    BufferWriter::open_from_checkpoint(
        Arc::new(ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap()),
        WriterConfig {
            prefix: "s2".into(),
            migration_id: "migration".into(),
            stream: "s".into(),
            epoch: 1,
            segment_bytes: 65536,
            segment_transactions: 1,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1048576,
        },
        WriterCheckpoint::default(),
        Arc::new(order()),
        tx,
        root.join("scratch"),
    )
    .await
    .unwrap()
}
async fn put(j: &Journal, state: &mut Materialized, transition: Transition) {
    let entry = Entry::new(state, transition, "s2").unwrap();
    j.put(&entry).await.unwrap();
    state.apply(&entry).unwrap();
}
async fn marker(j: &Journal, state: &mut Materialized, w: &BufferWriter, f: &Frontier) {
    // Marker publication follows checkpoint durability for every released txn.
    let cp = w.checkpoint().unwrap();
    assert!(cp.redelivery.iter().all(|r| r.manifest.is_some()));
    if let Some(p) = cp.last_position {
        assert!(
            p.compare(&position(SpannerPosition::Frontier(f.clone())), &order())
                .unwrap()
                .is_lt()
        );
    }
    put(
        j,
        state,
        Transition::Progress {
            stream: "buffered".into(),
            marker: "manifest-durable".into(),
            position: encode_buffered_position(
                &source(),
                &position(SpannerPosition::Frontier(f.clone())),
            )
            .unwrap(),
        },
    )
    .await;
}
async fn crash_history(stage: u8) {
    let root = crate::test_root::scratch_root().join(format!("s2-{}", uuid::Uuid::new_v4()));
    crate::test_root::assert_under_work_root(&root);
    std::fs::create_dir_all(&root).unwrap();
    let j = journal(&root.join("journal"));
    let mut state = Materialized::default();
    put(&j, &mut state, Transition::AcquireOwnership { epoch: 1 }).await;
    let mut w = writer(&root).await;
    let mut f = Frontier::new(0);
    f.mark_reading(b"").unwrap();
    f.advance(b"", 10).unwrap();
    assert_eq!(
        w.append(txn(5), Instant::now()).await.unwrap(),
        Redelivery::New
    );
    marker(&j, &mut state, &w, &f).await;
    if stage >= 1 {
        f.register_children(b"", vec![child(b"a", &[b""], 10), child(b"b", &[b""], 10)])
            .unwrap();
        marker(&j, &mut state, &w, &f).await;
    }
    if stage >= 2 {
        f.finish(b"").unwrap();
        marker(&j, &mut state, &w, &f).await;
    }
    if stage >= 3 {
        f.mark_reading(b"a").unwrap();
        f.mark_reading(b"b").unwrap();
        f.advance(b"a", 20).unwrap();
        f.register_children(b"a", vec![child(b"m", &[b"a", b"b"], 20)])
            .unwrap();
        assert_eq!(f.mark_reading(b"m"), Err(Error::ParentsBehind));
        marker(&j, &mut state, &w, &f).await;
    }
    let saved = f.clone();
    if stage == 4 {
        f.advance(b"b", 20).unwrap();
        f.mark_reading(b"m").unwrap();
        f.finish(b"a").unwrap();
        f.finish(b"b").unwrap();
        f.advance(b"m", 30).unwrap();
        assert_eq!(
            w.append(txn(25), Instant::now()).await.unwrap(),
            Redelivery::New
        );
        assert_eq!(w.checkpoint().unwrap().next_mseq, 2);
        // Crash after the real manifest fsync, before publishing the new marker.
    }
    drop(w);
    drop(j);
    drop(state);
    drop(f);
    let j = journal(&root.join("journal"));
    let state = j.load(false).await.unwrap();
    let payload = &state.progress["buffered"].1;
    let p = decode_buffered_position(payload, &source(), order().domain()).unwrap();
    let SpannerPosition::Frontier(mut recovered) = SpannerPosition::decode(p.bytes()).unwrap()
    else {
        panic!("frontier expected")
    };
    assert_eq!(recovered, saved);
    assert_eq!(recovered.resolved_ts().unwrap(), Some(10));
    for token in saved.records().keys() {
        assert!(recovered.records().contains_key(token));
    }
    let mut w = writer(&root).await;
    let cp = w.checkpoint().unwrap();
    let mut filter = DuplicateFilter::restore(
        10,
        32,
        cp.redelivery
            .iter()
            .map(|r| (r.txn.0.clone(), b"0".to_vec())),
    )
    .unwrap();
    for ts in if stage == 4 {
        vec![5_i128, 25]
    } else {
        vec![5_i128]
    } {
        assert!(
            !filter
                .observe(
                    &recovered,
                    if stage == 4 { b"b" } else { b"" },
                    ts,
                    ts.to_be_bytes().to_vec(),
                    b"0".to_vec()
                )
                .unwrap()
        );
        assert_eq!(
            w.append(txn(ts), Instant::now()).await.unwrap(),
            Redelivery::Noop
        );
    }
    assert_eq!(filter.drops, if stage == 4 { 2 } else { 1 });
    assert_eq!(
        w.checkpoint().unwrap().next_mseq,
        if stage == 4 { 2 } else { 1 }
    );
    // A referenced but unregistered merge parent must block after journal reload.
    if stage >= 3 {
        recovered
            .register_children(b"a", vec![child(b"blocked", &[b"a", b"unregistered"], 20)])
            .unwrap();
        let mut state = state;
        put(
            &j,
            &mut state,
            Transition::Progress {
                stream: "buffered".into(),
                marker: "blocked".into(),
                position: encode_buffered_position(
                    &source(),
                    &position(SpannerPosition::Frontier(recovered)),
                )
                .unwrap(),
            },
        )
        .await;
        let state = journal(&root.join("journal")).load(false).await.unwrap();
        let p =
            decode_buffered_position(&state.progress["buffered"].1, &source(), order().domain())
                .unwrap();
        let SpannerPosition::Frontier(blocked) = SpannerPosition::decode(p.bytes()).unwrap() else {
            panic!()
        };
        assert_eq!(
            blocked.resolved_ts(),
            Err(Error::MissingPartition(b"unregistered".to_vec()))
        );
    }
    drop(w);
    drop(j);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn crash_parent_reading() {
    crash_history(0).await;
}
#[tokio::test]
async fn crash_child_registered() {
    crash_history(1).await;
}
#[tokio::test]
async fn crash_parent_finished() {
    crash_history(2).await;
}
#[tokio::test]
async fn crash_merge_waiting() {
    crash_history(3).await;
}
#[tokio::test]
async fn crash_duplicate_replay_before_marker() {
    crash_history(4).await;
}
