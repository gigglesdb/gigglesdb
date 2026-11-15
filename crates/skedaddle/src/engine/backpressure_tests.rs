//! Real Capture/admission/local-buffer integration. The duplex peer only sends
//! protocol bytes and decodes feedback; it implements no database behavior.
use super::*;
use crate::{
    buffer::reader::{Backpressure, BufferBounds},
    pg::{frame::FrameBuffer, pgoutput::StandbyStatusUpdate, replication::ReplicationError},
};
use std::{sync::Mutex, time::Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

struct DuplexTransport {
    io: DuplexStream,
    frames: FrameBuffer,
    reads: usize,
}
impl source::Transport for DuplexTransport {
    async fn next(&mut self, durable_lsn: u64) -> source::Result<Vec<u8>> {
        let feedback = StandbyStatusUpdate {
            write_lsn: durable_lsn,
            flush_lsn: durable_lsn,
            apply_lsn: durable_lsn,
            client_time: 0,
            reply_requested: false,
        }
        .encode_copy_data(durable_lsn)?;
        self.io
            .write_all(&feedback)
            .await
            .map_err(ReplicationError::from)?;
        loop {
            if let Some(frame) = self.frames.next_frame()? {
                assert_eq!(frame.tag, b'd');
                self.reads += 1;
                return Ok(frame.payload);
            }
            let byte = self.io.read_u8().await.map_err(ReplicationError::from)?;
            self.frames.push(&[byte])?;
        }
    }
}

async fn stored(f: &Fixture) -> Vec<AssembledTxn> {
    let mut reader = BufferReader::new(
        f.store.clone(),
        f.config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut records = Vec::new();
    while let Some(record) = reader.next_txn().await.unwrap() {
        records.push(record);
    }
    records
}

#[tokio::test]
async fn capture_backpressure_retains_duplex_input_for_each_independent_bound() {
    for bound in ["bytes", "transactions", "age"] {
        let f = Fixture::new().await;
        let (messages, mut receive) = mpsc::channel(16);
        for (lsn, wm, attempt) in [
            (20, WatermarkKind::Low, 1),
            (30, WatermarkKind::High, 1),
            (40, WatermarkKind::Low, 2),
        ] {
            marker(
                &messages,
                Watermark {
                    wm,
                    chunk: plan().ranges[0].id.clone(),
                    attempt,
                },
                lsn,
            )
            .await;
        }
        drop(messages);
        let (io, mut peer) = tokio::io::duplex(4096);
        let acknowledgments = Arc::new(Mutex::new(Vec::new()));
        let observed = acknowledgments.clone();
        let (queued, input_queued) = oneshot::channel();
        let peer = tokio::spawn(async move {
            // Queue ALL input before capture runs; the held transactions stay in
            // real duplex I/O, never in a test variable waiting to be appended.
            while let Some(payload) = receive.recv().await {
                let payload = payload.unwrap();
                let mut frame = vec![b'd'];
                frame.extend(((payload.len() + 4) as u32).to_be_bytes());
                frame.extend(payload);
                for fragment in frame.chunks(3) {
                    peer.write_all(fragment).await.unwrap();
                }
            }
            queued.send(()).unwrap();
            for _ in 0..9 {
                let mut ack = [0; 39];
                peer.read_exact(&mut ack).await.unwrap();
                assert_eq!(&ack[..6], &[b'd', 0, 0, 0, 38, b'r']);
                let write = u64::from_be_bytes(ack[6..14].try_into().unwrap());
                let flush = u64::from_be_bytes(ack[14..22].try_into().unwrap());
                let apply = u64::from_be_bytes(ack[22..30].try_into().unwrap());
                assert_eq!((write, apply), (flush, flush));
                observed.lock().unwrap().push(flush);
            }
        });
        input_queued.await.unwrap();
        let (_, unused) = mpsc::channel(1);
        let capture = f.capture(unused, 10).await.capture;
        let (_manual_gate, paused) = watch::channel(false);
        let mut capture = PgCapture::new(
            capture,
            DuplexTransport {
                io,
                frames: FrameBuffer::default(),
                reads: 0,
            },
            paused,
            f.journal_objects.clone(),
            serde_json::json!(["engine", "test", "wal"]).to_string(),
            Arc::new(Order),
            source(),
        );
        let mut committed = Vec::new();
        for _ in 0..3 {
            let batch = capture.step().await.unwrap();
            if let Some(txn) = batch.transaction {
                committed.push(txn);
            }
        }
        assert_eq!(committed.len(), 1);
        let charge = committed[0].canonical_bytes().unwrap().len() as u64;
        let capacity = Backpressure::new(BufferBounds {
            max_buffered_bytes: if bound == "bytes" {
                charge + 1
            } else {
                charge * 10
            },
            max_unapplied_txns: if bound == "transactions" { 1 } else { 10 },
            max_oldest_unapplied_age: Duration::from_secs(1),
        })
        .unwrap();
        // Account the actual manifest-durable record; admission publishes the
        // production pause signal consumed by PgCapture, not a manual bool.
        let now = Instant::now();
        let first = capacity.try_admit(charge, 1, now).unwrap();
        let blocked_at = if bound == "age" {
            now + Duration::from_secs(1)
        } else {
            now
        };
        if bound == "bytes" {
            assert!(capacity.try_admit(charge, 1, blocked_at).is_err());
        }
        assert!(capacity.refresh(blocked_at).paused, "{bound}");
        capture = capture.with_backpressure(&capacity);
        // Await the peer's feedback for the first durable record's input frames.
        tokio::time::timeout(Duration::from_secs(2), async {
            while acknowledgments.lock().unwrap().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), capture.step())
                    .await
                    .is_err()
            );
            assert_eq!(
                capture.transport.reads, 3,
                "{bound}: capture read while paused"
            );
            assert_eq!(*acknowledgments.lock().unwrap(), vec![10; 3]);
            assert_eq!(capture.capture.durable_lsn(), 21);
            assert_eq!(stored(&f).await, committed);
        }
        capacity.mark_applied(first, blocked_at).unwrap();
        if bound == "bytes" {
            // Apply releases count/age, but bytes stay charged until journaled GC.
            assert!(capacity.refresh(blocked_at).paused);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), capture.step())
                    .await
                    .is_err()
            );
            assert_eq!(capture.transport.reads, 3);
            assert_eq!(*acknowledgments.lock().unwrap(), vec![10; 3]);
            capacity.mark_deleted(first, blocked_at).unwrap();
        }
        assert!(!capacity.refresh(blocked_at).paused);
        for _ in 0..6 {
            let batch = tokio::time::timeout(Duration::from_secs(2), capture.step())
                .await
                .unwrap()
                .unwrap();
            if let Some(txn) = batch.transaction {
                committed.push(txn);
            }
        }
        peer.await.unwrap();
        assert_eq!(capture.transport.reads, 9);
        assert_eq!(capture.capture.durable_lsn(), 41);
        assert_eq!(
            *acknowledgments.lock().unwrap(),
            [vec![10; 3], vec![21; 3], vec![31; 3]].concat()
        );
        assert_eq!(committed.len(), 3);
        assert_eq!(
            stored(&f).await,
            committed,
            "{bound}: retained input changed or was dropped"
        );
        assert_eq!(
            committed
                .iter()
                .map(|t| u64::from_be_bytes(t.commit_position.bytes().try_into().unwrap()))
                .collect::<Vec<_>>(),
            vec![20, 30, 40]
        );
        if bound != "bytes" {
            capacity.mark_deleted(first, blocked_at).unwrap();
        }
    }
}
