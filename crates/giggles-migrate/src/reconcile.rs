use crate::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, collections::BTreeMap};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub value: Scalar,
    pub timestamp: i64,
    pub expires_at: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedRow {
    pub partition: Key,
    pub key: Key,
    pub cells: BTreeMap<String, Cell>,
}
/// Retained provenance is also the permanent version/tombstone barrier store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub events: BTreeMap<String, Event>,
}
pub fn digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}
pub fn chunk_digest(chunk: &Chunk) -> Result<String> {
    let mut events = chunk
        .events
        .iter()
        .map(serde_json::to_vec)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    events.sort();
    digest(&("giggles-migrate-chunk-v1", &chunk.id, events))
}
pub fn row_order(m: &Manifest, a: &MaterializedRow, b: &MaterializedRow) -> Ordering {
    a.partition
        .cmp(&b.partition)
        .then_with(|| m.compare_clustering(&a.key, &b.key))
}
pub fn covers(m: &Manifest, scope: &Scope, key: &Key, column: &str) -> bool {
    match scope {
        Scope::Cell { key: k, column: c } => k == key && c == column,
        Scope::Row { key: k } => k == key,
        Scope::Partition => true,
        Scope::Range { lower, upper } => {
            lower
                .as_ref()
                .is_none_or(|b| match m.compare_clustering(key, &b.key) {
                    Ordering::Greater => true,
                    Ordering::Equal => b.inclusive,
                    _ => false,
                })
                && upper
                    .as_ref()
                    .is_none_or(|b| match m.compare_clustering(key, &b.key) {
                        Ordering::Less => true,
                        Ordering::Equal => b.inclusive,
                        _ => false,
                    })
        }
    }
}
impl State {
    pub fn apply(&mut self, m: &Manifest, events: &[Event]) -> Result<()> {
        m.validate()?;
        let mut next = self.clone();
        for e in events {
            validate_event(m, e)?;
            if let Some(old) = next.events.get(&e.id) {
                require(old == e, "permanent event ID mismatch")?;
            }
            for old in next.events.values() {
                require(
                    old.stream != e.stream || old.position != e.position || old == e,
                    "stream position reused",
                )?;
                if old.partition == e.partition
                    && let (
                        Mutation::Put {
                            key: a,
                            column: ac,
                            timestamp: at,
                            value: av,
                            expires_at: ae,
                        },
                        Mutation::Put {
                            key: b,
                            column: bc,
                            timestamp: bt,
                            value: bv,
                            expires_at: be,
                        },
                    ) = (&old.mutation, &e.mutation)
                {
                    require(
                        a != b || ac != bc || at != bt || (av == bv && ae == be),
                        "ambiguous equal timestamp cell conflict",
                    )?;
                }
            }
            next.events.insert(e.id.clone(), e.clone());
        }
        require(next.events.len() <= m.bounds.max_events, "event bound")?;
        require(
            serde_json::to_vec(&next)?.len() <= m.bounds.max_state_bytes,
            "state byte bound",
        )?;
        *self = next;
        Ok(())
    }
    pub fn rows(&self, m: &Manifest, horizon: i64) -> Result<Vec<MaterializedRow>> {
        let mut winners: BTreeMap<(Key, Key, String), Cell> = BTreeMap::new();
        for e in self.events.values() {
            if let Mutation::Put {
                key,
                column,
                value,
                timestamp,
                expires_at,
            } = &e.mutation
            {
                let slot = (e.partition.clone(), key.clone(), column.clone());
                if winners
                    .get(&slot)
                    .is_none_or(|old| *timestamp > old.timestamp)
                {
                    winners.insert(
                        slot,
                        Cell {
                            value: value.clone(),
                            timestamp: *timestamp,
                            expires_at: *expires_at,
                        },
                    );
                }
            }
        }
        let mut rows: BTreeMap<(Key, Key), BTreeMap<String, Cell>> = BTreeMap::new();
        for ((partition, key, column), cell) in winners {
            let deleted = self.events.values().any(|e| e.partition == partition && matches!(&e.mutation,Mutation::Tombstone {target,timestamp} if *timestamp >= cell.timestamp && covers(m,target,&key,&column)));
            if !deleted && cell.expires_at.is_none_or(|t| t > horizon) {
                rows.entry((partition, key))
                    .or_default()
                    .insert(column, cell);
            }
        }
        require(rows.len() <= m.bounds.max_rows, "materialized row bound")?;
        let mut rows: Vec<_> = rows
            .into_iter()
            .map(|((partition, key), cells)| MaterializedRow {
                partition,
                key,
                cells,
            })
            .collect();
        rows.sort_by(|a, b| row_order(m, a, b));
        Ok(rows)
    }
    /// Coverage of the declared offline cut only; does not establish source completeness.
    pub fn declared_cut_complete(&self, m: &Manifest) -> bool {
        m.streams.iter().all(|s| {
            (s.start..=s.end).all(|p| {
                self.events
                    .values()
                    .any(|e| e.stream == s.id && e.position == p)
            })
        })
    }
}
