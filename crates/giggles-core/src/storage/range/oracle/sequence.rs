//! SQL sequences on the system range leader (GD-0075 R5-43; design.md Changes 2026-11-16 Track C).
//!
//! A gateway never keeps a sequence counter: every nextval/setval of a committed sequence is
//! evaluated here, against the sequence's latest committed definition in the system range's
//! catalog. As on one node (v2_sql_sequences.rs), the counter lives in memory and the stored
//! counter `Seq/[2] || id || incarnation` holds a reservation 32 steps ahead, written through
//! Raft before a value inside it is handed out. A new leader starts from the stored
//! reservation, so it hands out only values above every earlier one (a gap of at most 32, as
//! after a crash on one node), and values stay in one global order across gateways.
use std::collections::BTreeMap;

use super::{SystemRange, unknown};
use crate::storage::engine::{
    catalog_codec,
    kv::{Keyspace, KvRead, WriteBatch},
    mvcc,
    txn::TxnError,
};
use crate::storage::range_api::{RangeError, SequenceOp, SequenceReply};
use crate::storage::sequences::{self, SeqCounter};

fn corrupt(error: impl std::fmt::Display) -> RangeError {
    RangeError::Corrupt(error.to_string())
}

/// Live counters of this term, by sequence id.
pub(super) type Live = BTreeMap<u64, SeqCounter>;

impl SystemRange {
    pub(super) fn sequence_op(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        self.check_open()?;
        // One operation at a time: the stored reservation is written before the value is
        // returned, and the next operation starts from the counter this one left.
        let mut live = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        let key = catalog_codec::counter_key(id, incarnation);
        let def = mvcc::latest_version(
            &self.kv,
            Keyspace::Catalog,
            &catalog_codec::sequence_key(id),
        )
        .map_err(|e| corrupt(format!("{e:?}")))?
        .and_then(|(_, value)| value)
        .map(|bytes| catalog_codec::decode_sequence(&bytes))
        .transpose()
        .map_err(|e| corrupt(format!("{e:?}")))?
        .filter(|def| def.incarnation == incarnation);
        let Some(def) = def else {
            live.remove(&id);
            if op == SequenceOp::Forget
                && KvRead::get(&self.kv, Keyspace::Seq, &key)
                    .map_err(corrupt)?
                    .is_some()
            {
                let mut batch = WriteBatch::default();
                batch.delete(Keyspace::Seq, key);
                self.write(batch)?;
            }
            return Ok(SequenceReply::Gone);
        };
        let prior = match live.get(&id).filter(|c| c.incarnation == incarnation) {
            Some(counter) => Some(counter.clone()),
            None => KvRead::get(&self.kv, Keyspace::Seq, &key)
                .map_err(corrupt)?
                .map(|bytes| catalog_codec::decode_counter(&key, &bytes, &def))
                .transpose()
                .map_err(|e| corrupt(format!("{e:?}")))?,
        };
        let mut counter = prior.clone().unwrap_or_else(|| sequences::fresh(&def));
        let stamp = counter.stamp.saturating_add(1);
        let (value, always_write) = match op {
            SequenceOp::Forget => return Ok(SequenceReply::Value(counter.last_value)),
            SequenceOp::Next => match sequences::advance(&mut counter, &def, stamp) {
                Ok(value) => (value, false),
                Err(crate::relational::SqlError::SequenceLimit) => {
                    return Ok(SequenceReply::Limit);
                }
                Err(e) => return Err(corrupt(format!("{e:?}"))),
            },
            SequenceOp::Set { value, called } => {
                if value < def.min || value > def.max {
                    return Ok(SequenceReply::OutOfRange);
                }
                sequences::set(&mut counter, value, called, stamp);
                (value, true)
            }
        };
        // The same rule as one node (v2_sql_sequences.rs): write when the reservation moved,
        // on the first call, and on a cycle's wrap.
        let reservation_changed = prior.as_ref().is_none_or(|old| {
            !old.is_called
                || old.log_hw != counter.log_hw
                || (def.cycle
                    && if def.increment > 0 {
                        counter.last_value < old.last_value
                    } else {
                        counter.last_value > old.last_value
                    })
        });
        if always_write || reservation_changed {
            // Through the system engine's sequencer: it ORs `sequence_counters` (and the
            // `catalog_ext` the definition needs) into the range's feature bits, which it
            // owns, and re-checks the definition's incarnation in its commit order (DROP).
            let sequencer = self
                .sequencer
                .get()
                .ok_or(RangeError::NotLeader { hint: None })?;
            let value = catalog_codec::encode_counter(&counter, &def)
                .map_err(|e| corrupt(format!("{e:?}")))?;
            match sequencer.write_counter(key, value, 64) {
                Ok(()) => {}
                Err(TxnError::SequenceGone) => {
                    live.remove(&id);
                    return Ok(SequenceReply::Gone);
                }
                Err(TxnError::Fenced) => return Err(RangeError::NotLeader { hint: None }),
                Err(error) => return Err(unknown(format!("{error:?}"))),
            }
        }
        live.insert(id, counter);
        Ok(SequenceReply::Value(value))
    }
}

#[cfg(test)]
#[path = "sequence_tests.rs"]
mod tests;
