//! Track D's home records stored by the range engine (GD-0075 design.md 5;
//! range/home.rs `HomeLog`): `Meta/range/<id>/home/<txn BE8>`, written through the range's
//! sink in the engine's term, so a write after a leader change fails `NotLeader`.
use std::ops::Bound;
use std::sync::Weak;

use super::RangeEngine;
use crate::storage::engine::{
    kv::{Direction, Durability, Keyspace, KvRead, WriteBatch},
    meta::{RangeLocalKey, parse_range_local_key, range_local_key},
};
use crate::storage::range::home::HomeLog;
use crate::storage::range::local::{decode_applied, local_interval};
use crate::storage::range_api::{ClusterTxnId, RangeError};

/// A `HomeLog` over one engine; holds it weakly (the engine owns its `HomeRecords`).
pub(super) struct EngineHomeLog(pub(super) Weak<RangeEngine>);

impl EngineHomeLog {
    fn engine(&self) -> Result<std::sync::Arc<RangeEngine>, RangeError> {
        let engine = self
            .0
            .upgrade()
            .ok_or(RangeError::NotLeader { hint: None })?;
        engine.check_open()?;
        Ok(engine)
    }
}

fn corrupt(error: impl std::fmt::Display) -> RangeError {
    RangeError::Corrupt(error.to_string())
}

impl HomeLog for EngineHomeLog {
    fn get(&self, txn: ClusterTxnId) -> Result<Option<Vec<u8>>, RangeError> {
        let engine = self.engine()?;
        let key = range_local_key(engine.descriptor.id, &RangeLocalKey::Home(txn.0));
        KvRead::get(engine.kv.as_ref(), Keyspace::Meta, &key).map_err(corrupt)
    }

    fn put(&self, txn: ClusterTxnId, value: Option<Vec<u8>>) -> Result<(), RangeError> {
        let engine = self.engine()?;
        let key = range_local_key(engine.descriptor.id, &RangeLocalKey::Home(txn.0));
        let mut batch = WriteBatch::default();
        match value {
            Some(value) => batch.put(Keyspace::Meta, key, value),
            None => batch.delete(Keyspace::Meta, key),
        }
        engine
            .kv
            .write(batch, Durability::Sync)
            .map_err(|e| super::txn_error(e.into()))
    }

    fn list(&self) -> Result<Vec<ClusterTxnId>, RangeError> {
        let engine = self.engine()?;
        let (lo, hi) = local_interval(engine.descriptor.id);
        let mut out = Vec::new();
        for item in KvRead::range(
            engine.kv.as_ref(),
            Keyspace::Meta,
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
            Direction::Forward,
        )
        .map_err(corrupt)?
        {
            let (key, _) = item.map_err(corrupt)?;
            if let Some((_, RangeLocalKey::Home(txn))) = parse_range_local_key(&key) {
                out.push(ClusterTxnId(txn));
            }
        }
        Ok(out)
    }

    /// The range's applied log index: a record's incarnation is read before its own write
    /// applies at a higher index, and a new leader's applied index covers every applied write.
    fn fresh_incarnation(&self, _txn: ClusterTxnId) -> Result<u64, RangeError> {
        let engine = self.engine()?;
        let key = range_local_key(engine.descriptor.id, &RangeLocalKey::Applied);
        let applied = KvRead::get(engine.kv.as_ref(), Keyspace::Meta, &key).map_err(corrupt)?;
        Ok(match applied {
            None => 0,
            Some(bytes) => {
                decode_applied(&bytes)
                    .ok_or_else(|| corrupt("bad applied position"))?
                    .index
            }
        })
    }
}
