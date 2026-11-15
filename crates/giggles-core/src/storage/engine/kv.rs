//! Backend-independent KV seam. Keys are byte-lexicographic within a keyspace.
use std::{
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Keyspace {
    Meta,
    Catalog,
    Rows,
    Index,
    Kernel,
    Seq,
    Gc,
}

impl Keyspace {
    pub(crate) const ALL: [Self; 7] = [
        Self::Meta,
        Self::Catalog,
        Self::Rows,
        Self::Index,
        Self::Kernel,
        Self::Seq,
        Self::Gc,
    ];

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Catalog => "catalog",
            Self::Rows => "rows",
            Self::Index => "index",
            Self::Kernel => "kernel",
            Self::Seq => "seq",
            Self::Gc => "gc",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Put {
        ks: Keyspace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        ks: Keyspace,
        key: Vec<u8>,
    },
    /// Delete exactly `[start, end)`. Empty or reversed intervals do nothing.
    DeleteRange {
        ks: Keyspace,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

/// Operations execute in insertion order as one atomic batch.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct WriteBatch {
    ops: Vec<Op>,
}

const BATCH_WIRE_VERSION: u8 = 1;

impl WriteBatch {
    pub(crate) fn ops(&self) -> &[Op] {
        &self.ops
    }

    /// Versioned replication payload; does not change the storage format.
    pub(crate) fn encode(&self) -> Vec<u8> {
        fn field(out: &mut Vec<u8>, bytes: &[u8]) {
            let len = u32::try_from(bytes.len()).expect("batch field exceeds u32 length");
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(bytes);
        }
        let mut out = vec![BATCH_WIRE_VERSION];
        let count = u32::try_from(self.ops.len()).expect("batch exceeds u32 operation count");
        out.extend_from_slice(&count.to_be_bytes());
        for op in self.ops() {
            let (tag, ks, first, second) = match op {
                Op::Put { ks, key, value } => (1, ks, key, Some(value)),
                Op::Delete { ks, key } => (2, ks, key, None),
                Op::DeleteRange { ks, start, end } => (3, ks, start, Some(end)),
            };
            out.push(tag);
            out.push(Keyspace::ALL.iter().position(|item| item == ks).unwrap() as u8);
            field(&mut out, first);
            if let Some(bytes) = second {
                field(&mut out, bytes);
            }
        }
        let checksum = super::crc32c::crc32c(&out);
        out.extend_from_slice(&checksum.to_be_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, KvError> {
        fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], KvError> {
            if len > input.len() {
                return Err(KvError::Corrupt("truncated batch".into()));
            }
            let (value, rest) = input.split_at(len);
            *input = rest;
            Ok(value)
        }
        fn number(input: &mut &[u8]) -> Result<u32, KvError> {
            Ok(u32::from_be_bytes(take(input, 4)?.try_into().unwrap()))
        }
        fn field(input: &mut &[u8]) -> Result<Vec<u8>, KvError> {
            let len = number(input)? as usize;
            // Validate the slice before allocating from an untrusted length.
            Ok(take(input, len)?.to_vec())
        }
        let Some(&version) = bytes.first() else {
            return Err(KvError::Corrupt("empty batch".into()));
        };
        if version != BATCH_WIRE_VERSION {
            return Err(KvError::Corrupt(format!("unknown batch version {version}")));
        }
        if bytes.len() < 9 {
            return Err(KvError::Corrupt("truncated batch".into()));
        }
        let (payload, mut trailer) = bytes.split_at(bytes.len() - 4);
        if super::crc32c::crc32c(payload) != number(&mut trailer)? {
            return Err(KvError::Corrupt("batch checksum mismatch".into()));
        }
        let mut input = &payload[1..];
        let count = number(&mut input)? as usize;
        // Even an empty-key Delete needs six bytes; never reserve by count.
        if count > input.len() / 6 {
            return Err(KvError::Corrupt("batch operation count mismatch".into()));
        }
        let mut batch = Self::default();
        for _ in 0..count {
            let tag = take(&mut input, 1)?[0];
            let index = take(&mut input, 1)?[0] as usize;
            let ks = *Keyspace::ALL
                .get(index)
                .ok_or_else(|| KvError::Corrupt("unknown batch keyspace".into()))?;
            match tag {
                1 => batch.put(ks, field(&mut input)?, field(&mut input)?),
                2 => batch.delete(ks, field(&mut input)?),
                3 => batch.delete_range(ks, field(&mut input)?, field(&mut input)?),
                _ => return Err(KvError::Corrupt("unknown batch operation tag".into())),
            }
        }
        if !input.is_empty() {
            return Err(KvError::Corrupt(
                "trailing batch bytes or operation count mismatch".into(),
            ));
        }
        Ok(batch)
    }

    pub(crate) fn put(&mut self, ks: Keyspace, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(Op::Put { ks, key, value });
    }
    pub(crate) fn delete(&mut self, ks: Keyspace, key: Vec<u8>) {
        self.ops.push(Op::Delete { ks, key });
    }
    pub(crate) fn delete_range(&mut self, ks: Keyspace, start: Vec<u8>, end: Vec<u8>) {
        self.ops.push(Op::DeleteRange { ks, start, end });
    }
    pub(crate) fn len(&self) -> usize {
        self.ops.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    /// Payload bytes only, excluding operation/keyspace tags and allocation overhead.
    pub(crate) fn byte_size(&self) -> usize {
        self.ops
            .iter()
            .map(|op| match op {
                Op::Put { key, value, .. } => key.len() + value.len(),
                Op::Delete { key, .. } => key.len(),
                Op::DeleteRange { start, end, .. } => start.len() + end.len(),
            })
            .sum()
    }
    pub(crate) fn into_ops(self) -> Vec<Op> {
        self.ops
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Durability {
    /// Commit this batch and every preceding NoSync batch durably before returning.
    Sync,
    /// Atomically visible on success; durability waits for Sync or flush_barrier.
    NoSync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KvError {
    Io(String),
    Corrupt(String),
    Fenced,
    Closed,
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) => write!(f, "KV I/O: {message}"),
            Self::Corrupt(message) => write!(f, "KV corruption: {message}"),
            Self::Fenced => f.write_str("KV fenced"),
            Self::Closed => f.write_str("KV closed"),
        }
    }
}
impl std::error::Error for KvError {}
impl From<KvError> for crate::CoreError {
    fn from(error: KvError) -> Self {
        match error {
            KvError::Io(message) => Self::Storage(message),
            KvError::Corrupt(message) => Self::Corrupt(message),
            KvError::Fenced | KvError::Closed => Self::Fenced,
        }
    }
}

pub(crate) type KvPair = (Vec<u8>, Vec<u8>);
pub(crate) type KvIter<'a> = Box<dyn Iterator<Item = Result<KvPair, KvError>> + Send + 'a>;

pub(crate) trait KvRead: Send + Sync {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    /// Lazy: implementations must not materialize the whole result before iteration.
    /// Bounds apply in byte order for both directions; reversed/empty bounds are empty.
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError>;
    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.range(
            ks,
            Bound::Included(key),
            Bound::Unbounded,
            Direction::Forward,
        )?
        .next()
        .transpose()
    }
}

pub(crate) trait KvBackend: KvRead {
    /// An error can have an ambiguous outcome, including a fully durable commit.
    /// Callers must fence rather than retry the batch on the same handle.
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError>;
    /// Makes every preceding successful write durable. Errors are ambiguous too.
    fn flush_barrier(&self) -> Result<(), KvError>;
    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError>;
    fn engine_name(&self) -> &'static str;
}

/// Shared fence for ambiguous write/flush outcomes. Once fenced, subsequent calls
/// fail without consulting the backend. Already-returned snapshots/iterators retain
/// their own read view; calls in flight when another thread fences may finish.
pub(crate) struct Kv {
    backend: Arc<dyn KvBackend>,
    fenced: AtomicBool,
}

impl Kv {
    pub(crate) fn new(backend: Arc<dyn KvBackend>) -> Self {
        Self {
            backend,
            fenced: AtomicBool::new(false),
        }
    }
    pub(crate) fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }
    fn check(&self) -> Result<(), KvError> {
        if self.is_fenced() {
            Err(KvError::Fenced)
        } else {
            Ok(())
        }
    }
    pub(crate) fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.check()?;
        self.backend.get(ks, key)
    }
    pub(crate) fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.check()?;
        self.backend.range(ks, start, end, dir)
    }
    pub(crate) fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.check()?;
        self.backend.seek_ge(ks, key)
    }
    pub(crate) fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        self.check()?;
        let result = self.backend.write(batch, durability);
        self.fence_on_error(result)
    }
    pub(crate) fn flush_barrier(&self) -> Result<(), KvError> {
        self.check()?;
        let result = self.backend.flush_barrier();
        self.fence_on_error(result)
    }
    fn fence_on_error(&self, result: Result<(), KvError>) -> Result<(), KvError> {
        if result.is_err() {
            self.fenced.store(true, Ordering::Release);
        }
        result
    }
    pub(crate) fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.check()?;
        self.backend.snapshot()
    }
}

impl KvRead for Kv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Kv::get(self, ks, key)
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        Kv::range(self, ks, start, end, dir)
    }
    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        Kv::seek_ge(self, ks, key)
    }
}

#[cfg(test)]
#[path = "kv_codec_tests.rs"]
mod codec_tests;
