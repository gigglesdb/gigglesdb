//! Node identities persisted as Catalog records in the system range.
use crate::error::{Error, Result};
use giggles_core::range_api::{
    ClusterTxnId, LeaseKind, RangeClient, RangeError, RangeSpace, RangeWrite, ReadDirection,
    ReadRequest, range_key,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    thread,
    time::Duration,
};

pub const REGISTRY_TAG: u8 = 0xF5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: u64,
    pub state: NodeState,
    pub address: String,
    pub fingerprint: String,
}

fn corrupt(what: &str) -> RangeError {
    RangeError::Corrupt(format!("registry record: {what}"))
}

impl NodeRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![
            1,
            match self.state {
                NodeState::Active => 1,
                NodeState::Retired => 2,
            },
        ];
        for text in [&self.address, &self.fingerprint] {
            out.extend_from_slice(
                &u32::try_from(text.len())
                    .expect("registry string too long")
                    .to_be_bytes(),
            );
            out.extend_from_slice(text.as_bytes());
        }
        out
    }

    /// The id is carried in the record key, not in the value.
    pub fn decode(id: u64, mut bytes: &[u8]) -> std::result::Result<Self, RangeError> {
        fn take<'a>(bytes: &mut &'a [u8], n: usize) -> std::result::Result<&'a [u8], RangeError> {
            if bytes.len() < n {
                return Err(corrupt("short input"));
            }
            let (head, tail) = bytes.split_at(n);
            *bytes = tail;
            Ok(head)
        }
        fn text(bytes: &mut &[u8]) -> std::result::Result<String, RangeError> {
            let n = u32::from_be_bytes(take(bytes, 4)?.try_into().unwrap()) as usize;
            String::from_utf8(take(bytes, n)?.to_vec()).map_err(|_| corrupt("invalid UTF-8"))
        }
        if take(&mut bytes, 1)? != [1] {
            return Err(corrupt("unknown version"));
        }
        let state = match take(&mut bytes, 1)? {
            [1] => NodeState::Active,
            [2] => NodeState::Retired,
            _ => return Err(corrupt("unknown state")),
        };
        let address = text(&mut bytes)?;
        let fingerprint = text(&mut bytes)?;
        if !bytes.is_empty() {
            return Err(corrupt("trailing bytes"));
        }
        Ok(Self {
            id,
            state,
            address,
            fingerprint,
        })
    }
}

pub fn record_write(record: &NodeRecord) -> RangeWrite {
    let mut key = vec![REGISTRY_TAG];
    key.extend_from_slice(&record.id.to_be_bytes());
    RangeWrite {
        space: RangeSpace::Catalog,
        key,
        value: Some(record.encode()),
    }
}

fn decode_record(key: &[u8], value: &[u8]) -> std::result::Result<Option<NodeRecord>, RangeError> {
    if key.len() != 17 || key[0] != REGISTRY_TAG {
        return Err(corrupt("invalid key"));
    }
    let payload = match value.split_first() {
        Some((0, payload)) => payload,
        Some((1, [])) => return Ok(None),
        _ => return Err(corrupt("invalid MVCC envelope")),
    };
    NodeRecord::decode(u64::from_be_bytes(key[1..9].try_into().unwrap()), payload).map(Some)
}

pub fn read_all(client: &dyn RangeClient) -> std::result::Result<Vec<NodeRecord>, RangeError> {
    let system = client.descriptor_for(&range_key(RangeSpace::Catalog, &[REGISTRY_TAG]))?;
    let mut req = ReadRequest {
        range: system.id,
        epoch: system.epoch,
        space: RangeSpace::Catalog,
        start: Bound::Included(vec![REGISTRY_TAG]),
        end: Bound::Excluded(vec![REGISTRY_TAG + 1]),
        dir: ReadDirection::Forward,
        ts: client.oracle().next(1)?,
        page_bytes: 4096,
    };
    let mut records = Vec::new();
    loop {
        let page = client.read(req.clone())?;
        for (key, value) in page.pairs {
            if let Some(record) = decode_record(&key, &value)? {
                records.push(record);
            }
        }
        match page.resume {
            Some(resume) => req.start = resume,
            None => return Ok(records),
        }
    }
}

/// Retry uncertain outcomes only after reading the registry again.
fn retry(error: RangeError, attempt: usize) -> Result<()> {
    if attempt < 39
        && matches!(
            error,
            RangeError::Unknown(_)
                | RangeError::Aborted(_)
                | RangeError::Conflict(_)
                | RangeError::NotLeader { .. }
                | RangeError::Unavailable(_)
                | RangeError::StaleEpoch { .. }
        )
    {
        thread::sleep(Duration::from_millis(50));
        Ok(())
    } else {
        Err(Error::Core(format!("registry update: {error}")))
    }
}

fn update(
    client: &dyn RangeClient,
    node: u64,
    mut pending: impl FnMut(Vec<NodeRecord>) -> Result<Option<NodeRecord>>,
) -> Result<bool> {
    let mut written = false;
    for attempt in 0..40 {
        let records = match read_all(client) {
            Ok(records) => records,
            Err(e) => {
                retry(e, attempt)?;
                continue;
            }
        };
        let Some(record) = pending(records)? else {
            return Ok(written);
        };
        let result = (|| {
            let counter = client.lease(LeaseKind::Xid, (1 << 32) | node, 1)?.start;
            let txn = ClusterTxnId::try_new(node, counter)
                .ok_or_else(|| corrupt("transaction ids exhausted"))?;
            let system = client.descriptor_for(&range_key(RangeSpace::Catalog, &[REGISTRY_TAG]))?;
            client.commit(
                system.id,
                system.epoch,
                txn,
                vec![record_write(&record)],
                None,
                None,
            )
        })();
        match result {
            Ok(_) => return Ok(true),
            Err(e) => {
                written |= matches!(e, RangeError::Unknown(_));
                retry(e, attempt)?;
            }
        }
    }
    unreachable!("last failed attempt returns an error")
}

pub fn enrol(client: &dyn RangeClient, node: u64, record: NodeRecord) -> Result<bool> {
    if record.state != NodeState::Active {
        return Err(Error::Identity("enrol requires an Active record".into()));
    }
    update(client, node, |records| {
        for existing in &records {
            let same_id = existing.id == record.id;
            let same_fp = existing.fingerprint == record.fingerprint;
            if existing.state == NodeState::Retired && (same_id || same_fp) {
                return Err(Error::Identity("node id or fingerprint is retired".into()));
            }
            if same_id && existing != &record {
                return Err(Error::Identity(
                    "active node id has a different address or fingerprint".into(),
                ));
            }
            if !same_id && same_fp {
                return Err(Error::Identity(
                    "fingerprint belongs to another active node id".into(),
                ));
            }
        }
        Ok((!records.contains(&record)).then(|| record.clone()))
    })
}

pub fn retire(client: &dyn RangeClient, node: u64, id: u64) -> Result<bool> {
    update(client, node, |records| {
        let mut record = records
            .into_iter()
            .find(|r| r.id == id)
            .ok_or_else(|| Error::Identity(format!("unknown node id {id}")))?;
        if record.state == NodeState::Retired {
            return Ok(None);
        }
        record.state = NodeState::Retired;
        Ok(Some(record))
    })
}

pub fn peers(records: &[NodeRecord], me: u64) -> (BTreeMap<u64, (String, String)>, BTreeSet<u64>) {
    let mut active = BTreeMap::new();
    let mut retired = BTreeSet::new();
    for record in records {
        match record.state {
            NodeState::Retired => {
                retired.insert(record.id);
            }
            NodeState::Active if record.id != me => {
                active.insert(
                    record.id,
                    (record.address.clone(), record.fingerprint.clone()),
                );
            }
            _ => {}
        }
    }
    (active, retired)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id: u64, state: NodeState) -> NodeRecord {
        NodeRecord {
            id,
            state,
            address: "addré".into(),
            fingerprint: "fp".into(),
        }
    }
    #[test]
    fn codec_round_trip_and_strict_refusals() {
        for state in [NodeState::Active, NodeState::Retired] {
            let record = record(7, state);
            let bytes = record.encode();
            assert_eq!(NodeRecord::decode(7, &bytes).unwrap(), record);
            for n in 0..bytes.len() {
                assert!(matches!(
                    NodeRecord::decode(7, &bytes[..n]),
                    Err(RangeError::Corrupt(_))
                ));
            }
            for (at, value) in [(0, 2), (1, 0), (1, 3), (6, 255), (16, 255)] {
                let mut bad = bytes.clone();
                bad[at] = value;
                assert!(matches!(
                    NodeRecord::decode(7, &bad),
                    Err(RangeError::Corrupt(_))
                ));
            }
            let mut bad = bytes.clone();
            bad.push(0);
            assert!(matches!(
                NodeRecord::decode(7, &bad),
                Err(RangeError::Corrupt(_))
            ));
            let write = record_write(&record);
            assert_eq!(
                write.key,
                [vec![REGISTRY_TAG], 7u64.to_be_bytes().to_vec()].concat()
            );
            let key = [write.key, vec![0; 8]].concat();
            assert_eq!(
                decode_record(&key, &[vec![0], bytes].concat()).unwrap(),
                Some(record)
            );
            assert_eq!(decode_record(&key, &[1]).unwrap(), None);
            for bad in [vec![], vec![2], vec![1, 0]] {
                assert!(matches!(
                    decode_record(&key, &bad),
                    Err(RangeError::Corrupt(_))
                ));
            }
            assert!(decode_record(&key[..16], &[1]).is_err());
            let mut bad_key = key;
            bad_key[0] = 0;
            assert!(decode_record(&bad_key, &[1]).is_err());
        }
    }
    #[test]
    fn peers_excludes_self_and_collects_retired() {
        let records = [
            record(1, NodeState::Active),
            record(2, NodeState::Active),
            record(3, NodeState::Retired),
            record(4, NodeState::Retired),
        ];
        assert_eq!(
            peers(&records, 1),
            (
                BTreeMap::from([(2, ("addré".into(), "fp".into()))]),
                BTreeSet::from([3, 4])
            )
        );
        assert_eq!(peers(&[], 1), (BTreeMap::new(), BTreeSet::new()));
        assert!(peers(&records, 3).1.contains(&3));
    }
}
