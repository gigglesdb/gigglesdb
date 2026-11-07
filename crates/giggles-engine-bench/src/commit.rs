//! A single durable group writer. Visibility is published before acknowledgements.
use crate::{
    keys::{encode_key, encode_value},
    kv::{Backend, Result},
};
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub type Transaction = Vec<(Vec<u8>, Option<Vec<u8>>)>;
#[derive(Debug)]
pub struct CommitAck {
    pub timestamp: u64,
    pub latency: Duration,
}
#[derive(Debug, Default, serde::Serialize)]
pub struct GroupStats {
    pub groups: u64,
    pub transactions: u64,
    /// Number of groups observed at each transaction count, bounded by group_max.
    pub group_sizes: BTreeMap<usize, u64>,
}
struct Request {
    transaction: Transaction,
    submitted: Instant,
    ack: mpsc::Sender<std::result::Result<CommitAck, String>>,
}
enum Message {
    Commit(Request),
    Reset(mpsc::Sender<()>),
    Shutdown,
}
#[derive(Clone)]
pub struct CommitHandle {
    sender: mpsc::Sender<Message>,
}
pub struct GroupCommit {
    handle: CommitHandle,
    writer: Option<JoinHandle<Result<GroupStats>>>,
}
fn failure(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
fn remaining_wait(deadline: Instant, now: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(now)
        .filter(|remaining| !remaining.is_zero())
}
impl CommitHandle {
    pub fn submit(&self, transaction: Transaction) -> Result<CommitAck> {
        let submitted = Instant::now();
        if transaction
            .iter()
            .any(|(key, _)| key.len() != 17 || key[8] != 2)
        {
            return Err(failure("invalid logical user key"));
        }
        let (ack, received) = mpsc::channel();
        self.sender
            .send(Message::Commit(Request {
                transaction,
                submitted,
                ack,
            }))
            .map_err(|_| failure("group writer stopped"))?;
        let mut result = received
            .recv()
            .map_err(|_| failure("group writer stopped before acknowledgement"))?
            .map_err(failure)?;
        result.latency = submitted.elapsed();
        Ok(result)
    }
    /// Call after warmup clients have joined, before measured clients start.
    pub fn reset_stats(&self) -> Result<()> {
        let (ack, received) = mpsc::channel();
        self.sender
            .send(Message::Reset(ack))
            .map_err(|_| failure("group writer stopped"))?;
        received
            .recv()
            .map_err(|_| failure("group writer stopped before statistics reset"))?;
        Ok(())
    }
}
impl GroupCommit {
    pub fn start(
        backend: Arc<dyn Backend>,
        visible: Arc<AtomicU64>,
        group_max: usize,
        wait: Duration,
    ) -> Result<Self> {
        if group_max == 0 {
            return Err(failure("group maximum must be positive"));
        }
        let (sender, receiver) = mpsc::channel();
        let writer = thread::Builder::new()
            .name("group-commit".into())
            .spawn(move || {
                let mut stats = GroupStats::default();
                let mut timestamp = visible.load(Ordering::Acquire);
                let mut stop = false;
                while !stop {
                    let first = match receiver.recv() {
                        Ok(Message::Commit(request)) => request,
                        Ok(Message::Reset(ack)) => {
                            stats = GroupStats::default();
                            let _ = ack.send(());
                            continue;
                        }
                        Ok(Message::Shutdown) | Err(_) => break,
                    };
                    let deadline = Instant::now() + wait;
                    let mut group = vec![first];
                    while group.len() < group_max {
                        let Some(remaining) = remaining_wait(deadline, Instant::now()) else {
                            break;
                        };
                        match receiver.recv_timeout(remaining) {
                            Ok(Message::Commit(request)) => group.push(request),
                            Ok(Message::Reset(ack)) => {
                                stats = GroupStats::default();
                                let _ = ack.send(());
                            }
                            Ok(Message::Shutdown) => {
                                stop = true;
                                break;
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => break,
                            Err(mpsc::RecvTimeoutError::Disconnected) => {
                                stop = true;
                                break;
                            }
                        }
                    }
                    let mut batch = Vec::new();
                    let mut timestamps = Vec::with_capacity(group.len());
                    for request in &group {
                        timestamp = match timestamp.checked_add(1) {
                            Some(next) => next,
                            None => {
                                for request in &group {
                                    let _ =
                                        request.ack.send(Err("commit timestamp exhausted".into()));
                                }
                                return Err(failure("commit timestamp exhausted"));
                            }
                        };
                        timestamps.push(timestamp);
                        for (key, value) in &request.transaction {
                            batch.push((
                                encode_key(key, timestamp),
                                Some(encode_value(value.as_deref())),
                            ));
                        }
                    }
                    if let Err(error) = backend.write_batch(&batch, true) {
                        let message = error.to_string();
                        for request in group {
                            let _ = request.ack.send(Err(message.clone()));
                        }
                        return Err(failure(message));
                    }
                    visible.store(timestamp, Ordering::Release);
                    stats.groups += 1;
                    stats.transactions += group.len() as u64;
                    *stats.group_sizes.entry(group.len()).or_default() += 1;
                    for (request, timestamp) in group.into_iter().zip(timestamps) {
                        let _ = request.ack.send(Ok(CommitAck {
                            timestamp,
                            latency: request.submitted.elapsed(),
                        }));
                    }
                }
                Ok(stats)
            })?;
        Ok(Self {
            handle: CommitHandle { sender },
            writer: Some(writer),
        })
    }
    pub fn handle(&self) -> CommitHandle {
        self.handle.clone()
    }
    pub fn shutdown(mut self) -> Result<GroupStats> {
        let _ = self.handle.sender.send(Message::Shutdown);
        self.writer
            .take()
            .expect("writer exists")
            .join()
            .map_err(|_| failure("group writer panicked"))?
    }
}
impl Drop for GroupCommit {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = self.handle.sender.send(Message::Shutdown);
            let _ = writer.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{KvIter, WriteOp};
    use std::sync::{Barrier, Mutex};
    #[derive(Default)]
    struct Memory {
        batches: Mutex<Vec<Vec<WriteOp>>>,
        fail: bool,
    }
    impl Backend for Memory {
        fn write_batch(&self, ops: &[WriteOp], durable: bool) -> Result<()> {
            assert!(durable);
            if self.fail {
                return Err(failure("injected durable failure"));
            }
            self.batches.lock().unwrap().push(ops.to_vec());
            Ok(())
        }
        fn iter_from(&self, _: &[u8]) -> Result<KvIter<'_>> {
            Ok(Box::new(std::iter::empty()))
        }
        fn disk_bytes(&self) -> Result<u64> {
            Ok(0)
        }
        fn flush(&self) -> Result<()> {
            Ok(())
        }
    }
    #[test]
    fn grouped_durable_acknowledgements_publish_timestamps() {
        let backend = Arc::new(Memory::default());
        let visible = Arc::new(AtomicU64::new(40));
        let writer =
            GroupCommit::start(backend.clone(), visible.clone(), 4, Duration::from_secs(2))
                .unwrap();
        let barrier = Arc::new(Barrier::new(4));
        let clients: Vec<_> = (0..4)
            .map(|key| {
                let handle = writer.handle();
                let barrier = barrier.clone();
                let visible = visible.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let ack = handle
                        .submit(vec![(
                            crate::keys::encode_user_key(0, key.into()).to_vec(),
                            Some(vec![key]),
                        )])
                        .unwrap();
                    assert!(visible.load(Ordering::Acquire) >= ack.timestamp);
                    ack.timestamp
                })
            })
            .collect();
        let mut timestamps: Vec<_> = clients
            .into_iter()
            .map(|client| client.join().unwrap())
            .collect();
        timestamps.sort_unstable();
        assert_eq!(timestamps, vec![41, 42, 43, 44]);
        writer.handle().reset_stats().unwrap();
        assert_eq!(writer.shutdown().unwrap().transactions, 0);
        let batches = backend.batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 4);
    }
    #[test]
    fn durable_failure_never_publishes_or_acknowledges_success() {
        let visible = Arc::new(AtomicU64::new(7));
        let writer = GroupCommit::start(
            Arc::new(Memory {
                fail: true,
                ..Memory::default()
            }),
            visible.clone(),
            1,
            Duration::ZERO,
        )
        .unwrap();
        let handle = writer.handle();
        assert!(
            handle
                .submit(vec![(crate::keys::encode_user_key(0, 1).to_vec(), None)])
                .unwrap_err()
                .to_string()
                .contains("injected")
        );
        assert_eq!(visible.load(Ordering::Acquire), 7);
        assert!(writer.shutdown().is_err());
        assert!(handle.submit(vec![]).is_err());
    }
    #[test]
    fn deadline_budget_expires_without_being_extended() {
        let first = Instant::now();
        let deadline = first + Duration::from_micros(500);
        assert_eq!(
            remaining_wait(deadline, first),
            Some(Duration::from_micros(500))
        );
        assert_eq!(
            remaining_wait(deadline, first + Duration::from_micros(450)),
            Some(Duration::from_micros(50))
        );
        assert_eq!(remaining_wait(deadline, deadline), None);
        assert_eq!(
            remaining_wait(deadline, deadline + Duration::from_secs(1)),
            None
        );
    }
    #[test]
    fn pending_transactions_are_split_at_group_maximum() {
        let backend = Arc::new(Memory::default());
        let visible = Arc::new(AtomicU64::new(0));
        let writer =
            GroupCommit::start(backend.clone(), visible.clone(), 3, Duration::from_secs(30))
                .unwrap();
        let mut responses = Vec::new();
        // Enqueue without blocking on acknowledgements so all groups have pending work.
        for pk in 0..8 {
            let (ack, response) = mpsc::channel();
            writer
                .handle
                .sender
                .send(Message::Commit(Request {
                    transaction: vec![(
                        crate::keys::encode_user_key(0, pk).to_vec(),
                        Some(vec![1]),
                    )],
                    submitted: Instant::now(),
                    ack,
                }))
                .unwrap();
            responses.push(response);
        }
        // Shutdown flushes the final partial group without any timed wait in this test.
        let stats = writer.shutdown().unwrap();
        assert_eq!(stats.transactions, 8);
        assert_eq!(stats.groups, 3);
        assert_eq!(stats.group_sizes, BTreeMap::from([(2, 1), (3, 2)]));
        let timestamps: Vec<_> = responses
            .into_iter()
            .map(|response| response.recv().unwrap().unwrap().timestamp)
            .collect();
        assert_eq!(timestamps, (1..=8).collect::<Vec<_>>());
        assert_eq!(visible.load(Ordering::Acquire), 8);
        let batches = backend.batches.lock().unwrap();
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![3, 3, 2]
        );
        for (index, row) in batches.iter().flatten().enumerate() {
            assert_eq!(crate::keys::decode_key(&row.0).unwrap().1, index as u64 + 1);
        }
    }
}
