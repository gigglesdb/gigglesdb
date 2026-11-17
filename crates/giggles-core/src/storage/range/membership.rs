//! Atomic descriptor membership updates for a range's configuration entries.
use super::RangeStore;
use super::split::child_interval;
use crate::CoreError;
use crate::storage::engine::{
    kv::{Durability, Keyspace, KvBackend, WriteBatch},
    meta::{RangeLocalKey, range_local_key},
};
use crate::storage::range::local::{applied_key, encode_applied};
use crate::storage::range_api::{Epoch, RangeDescriptor, RangeId, ReplicaDesc};
use crate::storage::replicated::AppliedPosition;

impl RangeStore {
    /// Change replicas and consume the log position atomically; replay is a no-op.
    pub fn change_replicas(
        &self,
        range: RangeId,
        mut replicas: Vec<ReplicaDesc>,
        clear_lineage: bool,
        at: AppliedPosition,
    ) -> Result<Option<RangeDescriptor>, CoreError> {
        let lock = self.range_lock(range)?;
        let _guard = lock
            .lock()
            .map_err(|_| CoreError::Corrupt("range apply lock poisoned".into()))?;
        let _installs = self.installs_lock()?;
        let Some(mut descriptor) = self.next_entry(range, at)? else {
            return Ok(None);
        };
        replicas.sort();
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            applied_key(range),
            encode_applied(at).to_vec(),
        );
        if replicas_match(&replicas, &descriptor.replicas) {
            self.kv.write(batch, Durability::NoSync)?;
            return Ok(None);
        }
        descriptor.replicas = replicas;
        descriptor.epoch = Epoch(
            descriptor
                .epoch
                .0
                .checked_add(1)
                .ok_or_else(|| CoreError::Corrupt("range epoch overflow".into()))?,
        );
        let encoded = descriptor
            .encode()
            .map_err(|e| CoreError::Corrupt(format!("range descriptor: {e}")))?;
        batch.put(
            Keyspace::Meta,
            range_local_key(range, &RangeLocalKey::Descriptor),
            encoded,
        );
        if clear_lineage {
            let (lo, hi) = child_interval(range);
            batch.delete_range(Keyspace::Meta, lo, hi);
        }
        self.kv.write(batch, Durability::NoSync)?;
        Ok(Some(descriptor))
    }
}

fn replicas_match(sorted: &[ReplicaDesc], stored: &[ReplicaDesc]) -> bool {
    let mut stored = stored.to_vec();
    stored.sort();
    sorted == stored
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::kv::KvRead;
    use crate::storage::range::store::tests::{Dir, pos, two_ranges};
    use crate::storage::range_api::ReplicaRole;

    #[test]
    fn membership_is_atomic_replay_safe_and_clears_only_its_lineage() {
        let dir = Dir::new("range-membership");
        let store = two_ranges(&dir);
        let range = RangeId(2);
        let before = store.descriptor(range).unwrap().unwrap();
        let key = range_local_key(range, &RangeLocalKey::Child(8));
        let other = range_local_key(RangeId(1), &RangeLocalKey::Child(9));
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Meta, key.clone(), before.encode().unwrap());
        batch.put(Keyspace::Meta, other.clone(), before.encode().unwrap());
        batch.put(Keyspace::Rows, b"data".to_vec(), b"value".to_vec());
        store.kv.write(batch, Durability::NoSync).unwrap();
        let mut replicas = before.replicas.clone();
        replicas.push(ReplicaDesc {
            node: 4,
            role: ReplicaRole::Learner,
        });
        replicas.reverse();
        let new = store
            .change_replicas(range, replicas, false, pos(1, 1))
            .unwrap()
            .unwrap();
        assert_eq!(new.epoch.0, before.epoch.0 + 1);
        assert_eq!(
            (new.start.clone(), new.end.clone()),
            (before.start, before.end)
        );
        assert_eq!(
            store.kv.get(Keyspace::Rows, b"data").unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(store.children(range).unwrap().len(), 1);
        let mut unsorted = new.replicas.clone();
        unsorted.reverse();
        assert!(replicas_match(&new.replicas, &unsorted));
        assert!(
            store
                .change_replicas(range, new.replicas.clone(), true, pos(2, 1))
                .unwrap()
                .is_none()
        );
        assert_eq!(store.applied(range).unwrap(), Some(pos(2, 1)));
        assert_eq!(store.children(range).unwrap().len(), 1);
        assert!(
            store
                .change_replicas(range, vec![], true, pos(1, 1))
                .unwrap()
                .is_none()
        );
        assert_eq!(store.descriptor(range).unwrap(), Some(new.clone()));
        assert_eq!(store.descriptor(range).unwrap().unwrap().epoch, new.epoch);
        assert!(
            store
                .change_replicas(range, vec![], true, pos(4, 1))
                .is_err()
        );
        let mut invalid = new.replicas.clone();
        invalid.push(invalid[0]);
        assert!(
            store
                .change_replicas(range, invalid, true, pos(3, 1))
                .is_err()
        );
        assert_eq!(store.applied(range).unwrap(), Some(pos(2, 1)));
        let replicas = new.replicas.into_iter().filter(|r| r.node != 4).collect();
        store
            .change_replicas(range, replicas, true, pos(3, 1))
            .unwrap()
            .unwrap();
        assert!(store.children(range).unwrap().is_empty());
        assert!(store.kv.get(Keyspace::Meta, &other).unwrap().is_some());
    }
}
