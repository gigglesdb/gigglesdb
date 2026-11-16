//! Bounded cross-partition assembly. Call `release` only with a boundary that
//! certifies all records at/below it have arrived (not a partial data record's
//! timestamp). The caller must hand out the entire returned batch before making
//! that boundary durable. Released record storage transfers to the caller.
use super::{
    frontier::{Timestamp, Token},
    records::DataChangeRecord,
    stream::timestamp,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidCapacity,
    Capacity {
        limit: usize,
    },
    BoundaryRegression,
    RecoveryRequired {
        transaction_id: String,
        reason: &'static str,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Spanner assembly: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug)]
pub struct Transaction {
    pub commit_ts: Timestamp,
    pub server_transaction_id: String,
    /// Record-sequence order, independent of partition arrival order.
    pub records: Vec<DataChangeRecord>,
}
struct Pending {
    commit_ts: Timestamp,
    record_count: u64,
    partition_count: u64,
    partitions: BTreeSet<Token>,
    records: BTreeMap<String, DataChangeRecord>,
}

/// At most `capacity` unreleased data records, counting complete transactions
/// that await resolution. Overflow rejects the new record without mutation;
/// there is no eviction. This is a record-count bound, not a byte-size bound.
pub struct Assembler {
    pending: BTreeMap<String, Pending>,
    capacity: usize,
    buffered: usize,
    released_through: Option<Timestamp>,
    pub duplicate_drops: u64,
}
impl Assembler {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        Ok(Self {
            pending: BTreeMap::new(),
            capacity,
            buffered: 0,
            released_through: None,
            duplicate_drops: 0,
        })
    }
    pub fn buffered_records(&self) -> usize {
        self.buffered
    }

    /// Identity is (server_transaction_id, record_sequence), across partitions.
    /// After a certified boundary has been fully released, its inclusive replay
    /// is also discarded. No unbounded history of released identities is kept.
    pub fn insert(&mut self, token: Token, record: DataChangeRecord) -> Result<bool, Error> {
        let ts = timestamp(record.commit_timestamp);
        let id = &record.server_transaction_id;
        if self.released_through.is_some_and(|through| ts <= through)
            || self
                .pending
                .get(id)
                .is_some_and(|p| p.records.contains_key(&record.record_sequence))
        {
            self.duplicate_drops = self.duplicate_drops.saturating_add(1);
            return Ok(false);
        }
        let recovery = |reason| Error::RecoveryRequired {
            transaction_id: id.clone(),
            reason,
        };
        if id.is_empty()
            || record.record_sequence.is_empty()
            || record.number_of_records_in_transaction == 0
            || record.number_of_partitions_in_transaction == 0
            || record.number_of_partitions_in_transaction > record.number_of_records_in_transaction
        {
            return Err(recovery("invalid transaction metadata"));
        }
        if let Some(p) = self.pending.get(id) {
            if p.commit_ts != ts
                || p.record_count != record.number_of_records_in_transaction
                || p.partition_count != record.number_of_partitions_in_transaction
            {
                return Err(recovery("inconsistent transaction metadata"));
            }
            let partitions = p.partitions.len() as u64 + u64::from(!p.partitions.contains(&token));
            if p.records.len() as u64 >= p.record_count || partitions > p.partition_count {
                return Err(recovery("transaction exceeds declared counts"));
            }
        }
        if self.buffered == self.capacity {
            return Err(Error::Capacity {
                limit: self.capacity,
            });
        }
        let p = self.pending.entry(id.clone()).or_insert_with(|| Pending {
            commit_ts: ts,
            record_count: record.number_of_records_in_transaction,
            partition_count: record.number_of_partitions_in_transaction,
            partitions: BTreeSet::new(),
            records: BTreeMap::new(),
        });
        p.partitions.insert(token);
        p.records.insert(record.record_sequence.clone(), record);
        self.buffered += 1;
        Ok(true)
    }

    /// Validate the whole boundary before releasing anything. An incomplete
    /// transaction leaves all records intact and returns RecoveryRequired.
    pub fn release(&mut self, resolved_ts: Timestamp) -> Result<Vec<Transaction>, Error> {
        if self.released_through.is_some_and(|old| resolved_ts < old) {
            return Err(Error::BoundaryRegression);
        }
        let mut ready = Vec::new();
        for (id, p) in &self.pending {
            if p.commit_ts > resolved_ts {
                continue;
            }
            if p.records.len() as u64 != p.record_count
                || p.partitions.len() as u64 != p.partition_count
            {
                return Err(Error::RecoveryRequired {
                    transaction_id: id.clone(),
                    reason: "incomplete transaction at resolved boundary",
                });
            }
            ready.push((p.commit_ts, id.clone()));
        }
        ready.sort();
        let mut transactions = Vec::with_capacity(ready.len());
        for (commit_ts, id) in ready {
            let p = self
                .pending
                .remove(&id)
                .expect("validated pending transaction");
            self.buffered -= p.records.len();
            transactions.push(Transaction {
                commit_ts,
                server_transaction_id: id,
                records: p.records.into_values().collect(),
            });
        }
        self.released_through = Some(resolved_ts);
        Ok(transactions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spanner::records::{Record, decode_row};
    use serde_json::Value;

    fn fault() -> Vec<(Token, DataChangeRecord)> {
        let rows: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/fixtures/spanner/multi-partition-set.json"
        ))
        .unwrap();
        rows.into_iter()
            .map(|row| {
                let token = row["partition"].as_str().unwrap().as_bytes().to_vec();
                let Record::Data(record) = decode_row(&row["record"]).unwrap().remove(0) else {
                    panic!("expected data")
                };
                (token, record)
            })
            .collect()
    }
    #[test]
    fn two_partition_completeness_and_release_gating() {
        let records = fault();
        let ts = timestamp(records[0].1.commit_timestamp);
        let mut a = Assembler::new(2).unwrap();
        for (token, record) in records.into_iter().rev() {
            a.insert(token, record).unwrap();
        }
        assert!(a.release(ts - 1).unwrap().is_empty());
        assert_eq!(a.buffered_records(), 2);
        let txns = a.release(ts).unwrap();
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].records.len(), 2);
        assert_eq!(txns[0].records[0].record_sequence, "00000000");
        assert_eq!(txns[0].records[1].record_sequence, "00000001");
        assert_eq!(a.buffered_records(), 0);
        assert!(a.release(ts).unwrap().is_empty());
    }
    #[test]
    fn incomplete_boundary_is_atomic_and_recoverable() {
        let mut records = fault();
        let ts = timestamp(records[0].1.commit_timestamp);
        let mut a = Assembler::new(3).unwrap();
        let mut complete = records[0].1.clone();
        complete.server_transaction_id = "earlier-id".into();
        complete.number_of_records_in_transaction = 1;
        complete.number_of_partitions_in_transaction = 1;
        a.insert(b"p".to_vec(), complete).unwrap();
        let (token, record) = records.remove(0);
        a.insert(token, record).unwrap();
        assert!(matches!(a.release(ts), Err(Error::RecoveryRequired { .. })));
        assert_eq!(a.buffered_records(), 2);
        let (token, record) = records.remove(0);
        a.insert(token, record).unwrap();
        assert_eq!(a.release(ts).unwrap().len(), 2);
    }
    #[test]
    fn partition_count_is_required_independently_of_record_count() {
        let records = fault();
        let ts = timestamp(records[0].1.commit_timestamp);
        let mut a = Assembler::new(2).unwrap();
        for (_, record) in records {
            a.insert(b"same".to_vec(), record).unwrap();
        }
        assert!(matches!(a.release(ts), Err(Error::RecoveryRequired { .. })));
    }
    #[test]
    fn duplicates_do_not_consume_capacity_and_replay_is_dropped() {
        let records = fault();
        let ts = timestamp(records[0].1.commit_timestamp);
        let mut a = Assembler::new(2).unwrap();
        for (token, record) in &records {
            assert!(a.insert(token.clone(), record.clone()).unwrap());
            assert!(!a.insert(b"other".to_vec(), record.clone()).unwrap());
        }
        a.release(ts).unwrap();
        for (token, record) in records {
            assert!(!a.insert(token, record).unwrap());
        }
        assert_eq!(a.duplicate_drops, 4);
        assert_eq!(a.buffered_records(), 0);
    }
    #[test]
    fn cap_rejects_without_mutation_including_complete_unreleased_records() {
        assert!(matches!(Assembler::new(0), Err(Error::InvalidCapacity)));
        let records = fault();
        let mut a = Assembler::new(1).unwrap();
        a.insert(records[0].0.clone(), records[0].1.clone())
            .unwrap();
        assert_eq!(
            a.insert(records[1].0.clone(), records[1].1.clone()),
            Err(Error::Capacity { limit: 1 })
        );
        assert_eq!(a.buffered_records(), 1);
        let mut a = Assembler::new(2).unwrap();
        for (token, record) in &records {
            a.insert(token.clone(), record.clone()).unwrap();
        }
        let mut later = records[0].1.clone();
        later.server_transaction_id = "later".into();
        assert_eq!(
            a.insert(b"p".to_vec(), later),
            Err(Error::Capacity { limit: 2 })
        );
    }
    #[test]
    fn commit_order_and_bytewise_id_ties() {
        let mut a = Assembler::new(4).unwrap();
        let template = fault().remove(0).1;
        let ts = timestamp(template.commit_timestamp);
        for (id, delta) in [("z", 0), ("a", 1), ("A", 0), ("a", 0)] {
            let mut r = template.clone();
            r.server_transaction_id = format!("{id}{delta}");
            r.number_of_records_in_transaction = 1;
            r.number_of_partitions_in_transaction = 1;
            r.commit_timestamp = google_cloud_spanner::value::Timestamp::new(
                r.commit_timestamp.seconds() + delta,
                r.commit_timestamp.nanos(),
            )
            .unwrap();
            a.insert(b"p".to_vec(), r).unwrap();
        }
        let txns = a.release(ts + 1_000_000_000).unwrap();
        assert_eq!(
            txns.iter()
                .map(|t| t.server_transaction_id.as_str())
                .collect::<Vec<_>>(),
            ["A0", "a0", "z0", "a1"]
        );
        assert_eq!(a.release(ts).unwrap_err(), Error::BoundaryRegression);
    }
    #[test]
    fn inconsistent_metadata_and_excess_records_are_rejected_without_mutation() {
        let records = fault();
        let mut a = Assembler::new(4).unwrap();
        a.insert(records[0].0.clone(), records[0].1.clone())
            .unwrap();
        let mut bad = records[1].1.clone();
        bad.number_of_records_in_transaction += 1;
        assert!(matches!(
            a.insert(records[1].0.clone(), bad),
            Err(Error::RecoveryRequired { .. })
        ));
        a.insert(records[1].0.clone(), records[1].1.clone())
            .unwrap();
        let mut extra = records[1].1.clone();
        extra.record_sequence = "extra".into();
        assert!(matches!(
            a.insert(records[1].0.clone(), extra),
            Err(Error::RecoveryRequired { .. })
        ));
        assert_eq!(a.buffered_records(), 2);
    }
    #[test]
    fn recorded_real_dialects_assemble_completely() {
        for fixture in [
            include_str!("../../tests/fixtures/spanner/google.json"),
            include_str!("../../tests/fixtures/spanner/postgres.json"),
        ] {
            let rows: Vec<Value> = serde_json::from_str(fixture).unwrap();
            let mut a = Assembler::new(10).unwrap();
            let mut records = 0;
            for row in rows {
                for r in decode_row(&row).unwrap() {
                    if let Record::Data(r) = r {
                        records += 1;
                        a.insert(b"recorded-partition".to_vec(), r).unwrap();
                    }
                }
            }
            let txns = a.release(i128::MAX).unwrap();
            assert_eq!(txns.len(), 3);
            assert_eq!(txns.iter().map(|t| t.records.len()).sum::<usize>(), records);
            assert!(
                txns.windows(2)
                    .all(|w| (w[0].commit_ts, &w[0].server_transaction_id)
                        < (w[1].commit_ts, &w[1].server_transaction_id))
            );
        }
    }
}
