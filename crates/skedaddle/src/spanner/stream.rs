//! Concurrent bounded partition queries; the consumer alone owns the frontier.
//! Output preserves each partition's order. Cross-partition transaction ordering
//! belongs to the assembler at the resolved boundary.
use super::{
    client::{Client, Error as ClientError},
    frontier::{self, DuplicateFilter, Frontier, Partition, State, Timestamp, Token},
    records::{ChildPartition, DataChangeRecord, Record, decode_row},
};
use google_cloud_spanner::value::Timestamp as CloudTimestamp;
use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Clone, Debug)]
pub struct Config {
    pub concurrency: usize,
    pub channel_capacity: usize,
    pub query_window: Duration,
    pub heartbeat_ms: u32,
    pub max_retries: u32,
    pub backoff: Duration,
    pub max_backoff: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 8,
            channel_capacity: 64,
            query_window: Duration::from_secs(2),
            heartbeat_ms: 100,
            max_retries: 5,
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}
#[derive(Debug)]
pub enum Event {
    Data(DataChangeRecord, Token),
    Heartbeat(Token, Timestamp),
    Children(Token, Timestamp, Vec<ChildPartition>),
    PartitionEnd(Token),
}
#[derive(Debug)]
pub enum Error {
    Frontier(frontier::Error),
    InvalidConfig,
    InvalidToken,
    RecoveryRequired { token: Token, cause: ClientError },
    QueryFailed { token: Token, cause: ClientError },
    Decode(super::records::DecodeError),
    WorkerStopped,
    NoReadablePartition,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Spanner reader: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<frontier::Error> for Error {
    fn from(e: frontier::Error) -> Self {
        Self::Frontier(e)
    }
}
pub fn timestamp(ts: CloudTimestamp) -> Timestamp {
    i128::from(ts.seconds()) * 1_000_000_000 + i128::from(ts.nanos())
}
fn cloud(ts: Timestamp) -> Result<String, Error> {
    let seconds = i64::try_from(ts.div_euclid(1_000_000_000)).map_err(|_| Error::InvalidConfig)?;
    let ts = CloudTimestamp::new(seconds, ts.rem_euclid(1_000_000_000) as i32)
        .map_err(|_| Error::InvalidConfig)?;
    Ok(serde_json::to_value(ts)
        .map_err(|_| Error::InvalidConfig)?
        .as_str()
        .ok_or(Error::InvalidConfig)?
        .into())
}
fn recovery(error: &ClientError) -> bool {
    // The SDK preserves the service status. Retention and expired-token errors
    // are FAILED_PRECONDITION / OUT_OF_RANGE, sometimes INVALID_ARGUMENT.
    error
        .downcast_ref::<google_cloud_spanner::Error>()
        .and_then(|e| e.status())
        .is_some_and(|s| {
            let message = s.message.to_ascii_lowercase();
            message.contains("retention")
                || message.contains("expired")
                || message.contains("too old")
        })
}
enum Message {
    Record(Token, Record),
    Done(Token, Timestamp, Result<(), ClientError>),
}
fn query_start(p: &Partition, completed: &BTreeMap<Token, Timestamp>) -> Timestamp {
    completed
        .get(&p.token)
        .copied()
        .unwrap_or(p.processed_ts)
        .max(p.processed_ts)
}
fn certify(progress: &mut BTreeMap<Token, Timestamp>, token: Token, ts: Timestamp, data: bool) {
    let ts = if data { ts.saturating_sub(1) } else { ts };
    progress
        .entry(token)
        .and_modify(|old| *old = (*old).max(ts))
        .or_insert(ts);
}
fn can_finish(frontier: &Frontier, token: &[u8]) -> bool {
    let mut children = frontier
        .records()
        .values()
        .filter(|p| p.parents.contains(token))
        .peekable();
    children.peek().is_some()
        && children.all(|p| frontier.records()[token].processed_ts >= p.start_ts)
}
fn readable(
    frontier: &Frontier,
    active: &BTreeMap<Token, JoinHandle<()>>,
) -> Result<Vec<Partition>, Error> {
    frontier.resolved_ts()?;
    let mut ready: Vec<_> = frontier
        .records()
        .values()
        .filter(|p| {
            p.state != State::Finished
                && !active.contains_key(&p.token)
                && p.parents
                    .iter()
                    .all(|t| frontier.records()[t].processed_ts >= p.start_ts)
        })
        .cloned()
        .collect();
    ready.sort_by(|a, b| (a.processed_ts, &a.token).cmp(&(b.processed_ts, &b.token)));
    Ok(ready)
}

pub struct PartitionReader {
    client: Arc<Client>,
    config: Config,
    snapshot: Timestamp,
    frontier: Frontier,
    certified: BTreeMap<Token, Timestamp>,
    // Successful bounded queries consumed their whole interval, even if they
    // returned only data. Keep their next read bound separate from observed
    // progress: completion alone must never certify a durable frontier.
    completed_queries: BTreeMap<Token, Timestamp>,
    duplicates: DuplicateFilter,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    tasks: BTreeMap<Token, JoinHandle<()>>,
    retries: BTreeMap<Token, u32>,
}
impl PartitionReader {
    /// Pass Frontier::new(T) initially. On restart decode the persisted frontier
    /// and restore the filter from buffered identities, including those at the
    /// processed timestamp: that timestamp can end partway through a transaction.
    pub fn new(
        client: Arc<Client>,
        snapshot: Timestamp,
        frontier: Frontier,
        duplicates: DuplicateFilter,
        config: Config,
    ) -> Result<Self, Error> {
        if config.concurrency == 0
            || config.channel_capacity == 0
            || config.query_window.as_millis() <= u128::from(config.heartbeat_ms)
            || config.query_window > Duration::from_secs(300)
            || !(100..=300_000).contains(&config.heartbeat_ms)
            || config.backoff.is_zero()
            || config.backoff > config.max_backoff
        {
            return Err(Error::InvalidConfig);
        }
        frontier.resolved_ts()?;
        if frontier
            .records()
            .values()
            .any(|p| p.processed_ts < snapshot)
        {
            return Err(Error::InvalidConfig);
        }
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let certified = frontier
            .records()
            .iter()
            .map(|(t, p)| {
                (
                    t.clone(),
                    if p.state == State::Finished {
                        p.processed_ts
                    } else {
                        p.processed_ts.saturating_sub(1).max(snapshot)
                    },
                )
            })
            .collect();
        Ok(Self {
            client,
            config,
            snapshot,
            frontier,
            certified,
            completed_queries: BTreeMap::new(),
            duplicates,
            tx,
            rx,
            tasks: BTreeMap::new(),
            retries: BTreeMap::new(),
        })
    }
    pub fn frontier(&self) -> &Frontier {
        &self.frontier
    }
    pub fn duplicate_drops(&self) -> u64 {
        self.duplicates.drops
    }
    /// Data alone never certifies the rest of its commit timestamp. Children
    /// start conservatively, including on inclusive restart. Raw progress stays
    /// unchanged; only actual heartbeat timestamps certify, even after retries.
    pub fn certified_ts(&self) -> Result<Option<Timestamp>, Error> {
        self.frontier.resolved_ts()?;
        Ok(self
            .frontier
            .records()
            .values()
            .filter(|p| p.state != State::Finished)
            .map(|p| {
                self.certified
                    .get(&p.token)
                    .copied()
                    .unwrap_or(p.start_ts.saturating_sub(1).max(self.snapshot))
            })
            .min())
    }
    fn schedule(&mut self) -> Result<(), Error> {
        let ready = readable(&self.frontier, &self.tasks)?;
        for p in ready
            .into_iter()
            .take(self.config.concurrency.saturating_sub(self.tasks.len()))
        {
            let start_ts = query_start(&p, &self.completed_queries);
            let start = cloud(start_ts)?;
            let end_ts = start_ts
                .checked_add(self.config.query_window.as_nanos() as i128)
                .ok_or(Error::InvalidConfig)?;
            let end = cloud(end_ts)?;
            let token = if p.token.is_empty() {
                None
            } else {
                Some(String::from_utf8(p.token.clone()).map_err(|_| Error::InvalidToken)?)
            };
            if p.state == State::Registered {
                self.frontier.mark_reading(&p.token)?;
            }
            let (client, tx, key, heartbeat) = (
                self.client.clone(),
                self.tx.clone(),
                p.token.clone(),
                self.config.heartbeat_ms,
            );
            let attempt = self.retries.get(&key).copied().unwrap_or(0);
            let delay = if attempt == 0 {
                Duration::ZERO
            } else {
                self.config
                    .backoff
                    .saturating_mul(2u32.saturating_pow(attempt - 1))
                    .min(self.config.max_backoff)
            };
            self.tasks.insert(
                p.token,
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let result = async {
                        for row in client
                            .change_stream(token.as_deref(), &start, &end, heartbeat)
                            .await?
                        {
                            for record in decode_row(&row)? {
                                if tx.send(Message::Record(key.clone(), record)).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        Ok(())
                    }
                    .await;
                    let _ = tx.send(Message::Done(key, end_ts, result)).await;
                }),
            );
        }
        if self.tasks.is_empty() {
            return Err(Error::NoReadablePartition);
        }
        Ok(())
    }
    /// Cancellation-safe: after receiving a message all processing is synchronous
    /// until return. A cancelled pending receive neither consumes nor advances.
    /// Persist the exposed frontier only after downstream has buffered this event.
    pub async fn next(&mut self) -> Result<Event, Error> {
        loop {
            self.schedule()?;
            match self.rx.recv().await.ok_or(Error::WorkerStopped)? {
                Message::Record(token, Record::Data(record)) => {
                    let ts = timestamp(record.commit_timestamp);
                    if ts <= self.snapshot {
                        continue;
                    }
                    certify(&mut self.certified, token.clone(), ts, true);
                    if !self.duplicates.admit(
                        record.server_transaction_id.as_bytes().to_vec(),
                        record.record_sequence.as_bytes().to_vec(),
                    )? {
                        continue;
                    }
                    let processed = self.frontier.records()[&token].processed_ts;
                    self.frontier.advance(&token, ts.max(processed))?;
                    return Ok(Event::Data(record, token));
                }
                Message::Record(token, Record::Heartbeat(record)) => {
                    certify(
                        &mut self.certified,
                        token.clone(),
                        timestamp(record.timestamp),
                        false,
                    );
                    let ts = timestamp(record.timestamp)
                        .max(self.frontier.records()[&token].processed_ts);
                    self.frontier.advance(&token, ts)?;
                    return Ok(Event::Heartbeat(token, ts));
                }
                Message::Record(token, Record::Children(record)) => {
                    let start = timestamp(record.start_timestamp).max(self.snapshot);
                    let children = record
                        .child_partitions
                        .iter()
                        .map(|c| {
                            Partition::registered(
                                c.token.as_bytes().to_vec(),
                                if c.parent_partition_tokens.is_empty() {
                                    [Vec::new()].into()
                                } else {
                                    c.parent_partition_tokens
                                        .iter()
                                        .map(|p| p.as_bytes().to_vec())
                                        .collect()
                                },
                                start,
                            )
                        })
                        .collect();
                    self.frontier.register_children(&token, children)?;
                    self.frontier.advance(
                        &token,
                        start.max(self.frontier.records()[&token].processed_ts),
                    )?;
                    return Ok(Event::Children(token, start, record.child_partitions));
                }
                Message::Done(token, end_ts, result) => {
                    self.tasks.remove(&token);
                    match result {
                        Err(cause) => {
                            // Failed reads retry inclusively from observed progress.
                            self.completed_queries.remove(&token);
                            if recovery(&cause) {
                                return Err(Error::RecoveryRequired { token, cause });
                            }
                            let cause = match cause.downcast::<super::records::DecodeError>() {
                                Ok(e) => return Err(Error::Decode(*e)),
                                Err(e) => e,
                            };
                            let attempts = self.retries.entry(token.clone()).or_default();
                            *attempts += 1;
                            if *attempts > self.config.max_retries {
                                return Err(Error::QueryFailed { token, cause });
                            }
                        }
                        Ok(()) => {
                            self.retries.remove(&token);
                            // A clamped restart retains children ahead of this
                            // bounded query. Re-read up to their start before ending.
                            if can_finish(&self.frontier, &token) {
                                self.completed_queries.remove(&token);
                                self.frontier.finish(&token)?;
                                certify(
                                    &mut self.certified,
                                    token.clone(),
                                    self.frontier.records()[&token].processed_ts,
                                    false,
                                );
                                return Ok(Event::PartitionEnd(token));
                            }
                            // Done follows every record from this worker. Reuse
                            // the inclusive end as the next start, retaining any
                            // records exactly on the shared window boundary.
                            self.completed_queries.insert(token, end_ts);
                        }
                    }
                }
            }
        }
    }
}
impl Drop for PartitionReader {
    fn drop(&mut self) {
        for task in self.tasks.values() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::frontier::SpannerPosition;
    use super::*;
    #[test]
    fn completed_window_advances_queries_without_certifying_or_changing_restart() {
        let mut raw = Frontier::new(10);
        raw.mark_reading(b"").unwrap();
        raw.advance(b"", 20).unwrap();
        let mut certified = BTreeMap::new();
        certify(&mut certified, vec![], 20, true);
        let mut completed = BTreeMap::new();
        assert_eq!(query_start(&raw.records()[&vec![]], &completed), 20);

        // A data-only query over [10,30] ended successfully. Re-reading [20,30]
        // forever can keep returning the final data record without a heartbeat.
        completed.insert(vec![], 30);
        assert_eq!(query_start(&raw.records()[&vec![]], &completed), 30);
        assert_eq!(raw.resolved_ts().unwrap(), Some(20));
        assert_eq!(certified[&vec![]], 19, "completion is not certification");
        raw.advance(b"", 30).unwrap();
        assert_eq!(
            query_start(&raw.records()[&vec![]], &completed),
            30,
            "the common window endpoint stays inclusive"
        );

        // A partial failed read must retry its last timestamp inclusively.
        raw.advance(b"", 40).unwrap();
        completed.remove(&vec![]);
        assert_eq!(query_start(&raw.records()[&vec![]], &completed), 40);
        let SpannerPosition::Frontier(restored) =
            SpannerPosition::decode(&SpannerPosition::Frontier(raw).encode()).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            query_start(&restored.records()[&vec![]], &BTreeMap::new()),
            40,
            "restart discards in-memory query bounds and rereads inclusively"
        );
    }
    #[test]
    fn clamped_parent_rereads_before_finishing() {
        let mut raw = Frontier::new(0);
        raw.mark_reading(b"").unwrap();
        raw.advance(b"", 30).unwrap();
        raw.register_children(b"", vec![child(b"a", &[b""], 30)])
            .unwrap();
        raw.finish(b"").unwrap();
        let mut replay = raw.clamped(10).unwrap();
        assert!(!can_finish(&replay, b""));
        replay.advance(b"", 20).unwrap();
        assert!(!can_finish(&replay, b""));
        replay.advance(b"", 30).unwrap();
        assert!(can_finish(&replay, b""));
        replay.finish(b"").unwrap();
    }
    #[test]
    fn data_and_actual_heartbeat_certification() {
        let mut progress = BTreeMap::new();
        certify(&mut progress, vec![], 10, true);
        assert_eq!(progress[&vec![]], 9);
        certify(&mut progress, vec![], 8, false);
        assert_eq!(progress[&vec![]], 9);
        certify(&mut progress, vec![], 10, false);
        assert_eq!(progress[&vec![]], 10);
    }
    fn child(t: &[u8], parents: &[&[u8]], ts: Timestamp) -> Partition {
        Partition::registered(t.to_vec(), parents.iter().map(|p| p.to_vec()).collect(), ts)
    }
    #[test]
    fn scheduling_split_merge_and_restored_reading() {
        let mut f = Frontier::new(10);
        let active = BTreeMap::new();
        assert_eq!(readable(&f, &active).unwrap()[0].token, b"");
        f.mark_reading(b"").unwrap();
        f.advance(b"", 20).unwrap();
        f.register_children(b"", vec![child(b"a", &[b""], 20), child(b"b", &[b""], 20)])
            .unwrap();
        f.finish(b"").unwrap();
        f.mark_reading(b"a").unwrap();
        f.mark_reading(b"b").unwrap();
        f.advance(b"a", 30).unwrap();
        f.register_children(b"a", vec![child(b"m", &[b"a", b"b"], 30)])
            .unwrap();
        assert!(
            !readable(&f, &active)
                .unwrap()
                .iter()
                .any(|p| p.token == b"m")
        );
        f.advance(b"b", 30).unwrap();
        assert!(
            readable(&f, &active)
                .unwrap()
                .iter()
                .any(|p| p.token == b"m")
        );
        f.finish(b"a").unwrap();
        f.finish(b"b").unwrap();
        f.mark_reading(b"m").unwrap();
        let bytes = frontier::SpannerPosition::Frontier(f).encode();
        let frontier::SpannerPosition::Frontier(f) =
            frontier::SpannerPosition::decode(&bytes).unwrap()
        else {
            panic!()
        };
        assert_eq!(readable(&f, &active).unwrap().len(), 1);
        assert_eq!(readable(&f, &active).unwrap()[0].processed_ts, 30);
    }
    #[test]
    fn missing_parent_blocks_and_equal_timestamp_identities_survive() {
        let mut f = Frontier::new(0);
        f.mark_reading(b"").unwrap();
        f.register_children(b"", vec![child(b"m", &[b"", b"absent"], 0)])
            .unwrap();
        assert!(matches!(
            readable(&f, &BTreeMap::new()),
            Err(Error::Frontier(frontier::Error::MissingPartition(_)))
        ));
        let mut filter =
            DuplicateFilter::restore(3, 20, [(b"txn".to_vec(), b"1".to_vec())]).unwrap();
        assert!(!filter.admit(b"txn".to_vec(), b"1".to_vec()).unwrap());
        assert!(filter.admit(b"txn".to_vec(), b"2".to_vec()).unwrap());
        assert_eq!(filter.drops, 1);
        for ts in [-1, 0, 1, 1_234_567_890] {
            assert_eq!(
                timestamp(serde_json::from_value(serde_json::json!(cloud(ts).unwrap())).unwrap()),
                ts
            );
        }
    }
}
