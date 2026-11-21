//! Correctness backend. `visible` includes pending NoSync writes; `durable` is
//! the last successful Sync/flush image. Copy-on-write Arc maps let lazy cursors
//! retain a stable view without holding the mutex throughout iteration.
use super::kv::{
    Direction, Durability, Keyspace, KvBackend, KvError, KvIter, KvRead, Op, WriteBatch,
};
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, Mutex, MutexGuard},
};

type Map = BTreeMap<(Keyspace, Vec<u8>), Vec<u8>>;
enum Failure {
    Before(KvError),
    AfterApply,
}
#[derive(Default)]
struct State {
    durable: Map,
    visible: Arc<Map>,
    failure: Option<Failure>,
}

#[derive(Default)]
pub(crate) struct MemKv {
    state: Mutex<State>,
}

impl MemKv {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    fn lock(&self) -> Result<MutexGuard<'_, State>, KvError> {
        self.state
            .lock()
            .map_err(|_| KvError::Corrupt("memory KV mutex poisoned".into()))
    }
    /// Return a new process image containing only durable writes.
    pub(crate) fn crash(&self) -> Self {
        let state = self.state.lock().expect("memory KV mutex poisoned");
        Self {
            state: Mutex::new(State {
                durable: state.durable.clone(),
                visible: Arc::new(state.durable.clone()),
                failure: None,
            }),
        }
    }
    pub(crate) fn fail_next_write(&self, error: KvError) {
        self.state.lock().expect("memory KV mutex poisoned").failure = Some(Failure::Before(error));
    }
    pub(crate) fn fail_next_write_after_apply(&self) {
        self.state.lock().expect("memory KV mutex poisoned").failure = Some(Failure::AfterApply);
    }
}

fn get(map: &Map, ks: Keyspace, key: &[u8]) -> Option<Vec<u8>> {
    map.get(&(ks, key.to_vec())).cloned()
}

fn empty_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    match (start, end) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a >= b
        }
        _ => false,
    }
}

/// Each next() performs a tree seek and clones only the returned item. Owning
/// bounds and an Arc avoids a self-referential iterator or an eager result Vec.
struct Cursor {
    map: Arc<Map>,
    ks: Keyspace,
    start: Bound<(Keyspace, Vec<u8>)>,
    end: Bound<(Keyspace, Vec<u8>)>,
    dir: Direction,
    done: bool,
}
impl Iterator for Cursor {
    type Item = Result<(Vec<u8>, Vec<u8>), KvError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut range = self.map.range((self.start.clone(), self.end.clone()));
        let item = match self.dir {
            Direction::Forward => range.next(),
            Direction::Reverse => range.next_back(),
        };
        match item {
            Some(((ks, key), value)) if *ks == self.ks => {
                let next = Bound::Excluded((*ks, key.clone()));
                match self.dir {
                    Direction::Forward => self.start = next,
                    Direction::Reverse => self.end = next,
                }
                Some(Ok((key.clone(), value.clone())))
            }
            _ => {
                self.done = true;
                None
            }
        }
    }
}

fn range(
    map: Arc<Map>,
    ks: Keyspace,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    dir: Direction,
) -> KvIter<'static> {
    let done = empty_bounds(start, end);
    let start = match start {
        Bound::Included(key) => Bound::Included((ks, key.to_vec())),
        Bound::Excluded(key) => Bound::Excluded((ks, key.to_vec())),
        Bound::Unbounded => Bound::Included((ks, Vec::new())),
    };
    let end = match end {
        Bound::Included(key) => Bound::Included((ks, key.to_vec())),
        Bound::Excluded(key) => Bound::Excluded((ks, key.to_vec())),
        Bound::Unbounded => Keyspace::ALL
            .iter()
            .copied()
            .find(|next| *next > ks)
            .map(|next| Bound::Excluded((next, Vec::new())))
            .unwrap_or(Bound::Unbounded),
    };
    KvIter::new(Cursor {
        map,
        ks,
        start,
        end,
        dir,
        done,
    })
}

impl KvRead for MemKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(get(&self.lock()?.visible, ks, key))
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        Ok(range(self.lock()?.visible.clone(), ks, start, end, dir))
    }
}
struct Snapshot {
    map: Arc<Map>,
}
impl KvRead for Snapshot {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(get(&self.map, ks, key))
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        Ok(range(self.map.clone(), ks, start, end, dir))
    }
}

impl KvBackend for MemKv {
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        let mut state = self.lock()?;
        let failure = state.failure.take();
        if let Some(Failure::Before(error)) = failure {
            return Err(error);
        }
        let visible = Arc::make_mut(&mut state.visible);
        for op in batch.into_ops() {
            match op {
                Op::Put { ks, key, value } => {
                    visible.insert((ks, key), value);
                }
                Op::Delete { ks, key } => {
                    visible.remove(&(ks, key));
                }
                Op::DeleteRange { ks, start, end } if start < end => {
                    // Mutation cannot retain a range borrow, so collect just deletion keys.
                    let keys: Vec<_> = visible
                        .range((ks, start)..(ks, end))
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in keys {
                        visible.remove(&key);
                    }
                }
                Op::DeleteRange { .. } => {}
            }
        }
        if durability == Durability::Sync || matches!(failure, Some(Failure::AfterApply)) {
            state.durable = (*state.visible).clone();
        }
        if matches!(failure, Some(Failure::AfterApply)) {
            Err(KvError::Io("injected error after durable apply".into()))
        } else {
            Ok(())
        }
    }
    fn flush_barrier(&self) -> Result<(), KvError> {
        let mut state = self.lock()?;
        state.durable = (*state.visible).clone();
        Ok(())
    }
    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        // Deliberate O(n) snapshot copy: acceptable for this correctness/test backend.
        Ok(Arc::new(Snapshot {
            map: Arc::new((*self.lock()?.visible).clone()),
        }))
    }
    fn engine_name(&self) -> &'static str {
        "mem"
    }
}
