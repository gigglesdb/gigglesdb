//! Session-local state shared by volatile SQL functions, including cursor fetches.
use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use super::{R, SqlError};

#[derive(Default)]
pub(crate) struct SessionVolatile {
    random: Option<Prng>,
    uuid_ns: u128,
    subtransaction: bool,
    snapshot_exports: SnapshotExports,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SnapshotExports {
    serial: u64,
    count: u32,
}

impl SnapshotExports {
    fn next(&mut self) -> R<String> {
        static NEXT_SERIAL: AtomicU64 = AtomicU64::new(1);
        if self.count == 0 {
            self.serial = NEXT_SERIAL.fetch_add(1, Ordering::Relaxed);
        }
        self.count = self.count.checked_add(1).ok_or(SqlError::Limit)?;
        Ok(format!(
            "{:08X}-{:08X}-{}",
            self.serial >> 32,
            self.serial as u32,
            self.count
        ))
    }
}

thread_local! {
    static ACTIVE: RefCell<Option<Arc<Mutex<SessionVolatile>>>> = const { RefCell::new(None) };
    static STANDALONE_EXPORTS: Cell<Option<SnapshotExports>> = const { Cell::new(None) };
    // Direct relational execution has no SqlSession.
    static LOCAL: RefCell<SessionVolatile> = RefCell::new(SessionVolatile::default());
}

pub(crate) struct Scope<'a> {
    previous: Option<Arc<Mutex<SessionVolatile>>>,
    previous_exports: Option<SnapshotExports>,
    cursor_exports: Option<&'a mut SnapshotExports>,
    thread: PhantomData<Rc<()>>,
}

impl<'a> Scope<'a> {
    pub(crate) fn statement(session: &Arc<Mutex<SessionVolatile>>, in_transaction: bool) -> Self {
        Self::enter(
            session,
            (!in_transaction).then_some(SnapshotExports::default()),
            None,
        )
    }

    pub(crate) fn cursor(
        session: &Arc<Mutex<SessionVolatile>>,
        exports: Option<&'a mut SnapshotExports>,
    ) -> Self {
        Self::enter(session, exports.as_deref().copied(), exports)
    }

    fn enter(
        session: &Arc<Mutex<SessionVolatile>>,
        exports: Option<SnapshotExports>,
        cursor_exports: Option<&'a mut SnapshotExports>,
    ) -> Self {
        Self {
            previous: ACTIVE.replace(Some(session.clone())),
            previous_exports: STANDALONE_EXPORTS.replace(exports),
            cursor_exports,
            thread: PhantomData,
        }
    }
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        ACTIVE.replace(self.previous.take());
        let exports = STANDALONE_EXPORTS.replace(self.previous_exports);
        if let Some(cursor) = self.cursor_exports.as_deref_mut() {
            *cursor = exports.expect("standalone cursor export state");
        }
    }
}

impl SessionVolatile {
    pub(super) fn with<T>(eval: impl FnOnce(&mut Self) -> R<T>) -> R<T> {
        match ACTIVE.with(|active| active.borrow().clone()) {
            Some(session) => eval(&mut *session.lock().map_err(|_| SqlError::Unsupported)?),
            None => LOCAL.with(|state| eval(&mut state.borrow_mut())),
        }
    }

    pub(crate) fn set_subtransaction(session: &Mutex<Self>, open: bool) {
        session
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .subtransaction = open;
    }

    pub(crate) fn has_exported_snapshots(&self) -> bool {
        self.snapshot_exports.count != 0
    }

    pub(crate) fn end_transaction(&mut self) {
        self.subtransaction = false;
        self.snapshot_exports = SnapshotExports::default();
    }

    pub(super) fn export_snapshot(&mut self) -> R<super::Value> {
        if self.subtransaction {
            return Err(super::user_types::exact_sql_error(
                SqlError::ActiveSqlTransaction,
                "cannot export a snapshot from a subtransaction",
                None,
            ));
        }
        let id = if let Some(mut exports) = STANDALONE_EXPORTS.get() {
            let id = exports.next()?;
            STANDALONE_EXPORTS.set(Some(exports));
            id
        } else {
            self.snapshot_exports.next()?
        };
        Ok(super::Value::Text(id))
    }

    pub(super) fn seed(&mut self, seed: f64) {
        self.random = Some(Prng::seeded(seed));
    }

    pub(super) fn random(&mut self) -> R<&mut Prng> {
        if self.random.is_none() {
            let mut bytes = [0; 8];
            getrandom::fill(&mut bytes).map_err(|_| SqlError::Unsupported)?;
            self.random = Some(Prng::from_seed(u64::from_ne_bytes(bytes)));
        }
        Ok(self.random.as_mut().expect("initialized PRNG"))
    }

    pub(super) fn uuid_nanos(&mut self) -> R<u128> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SqlError::DatetimeFieldOverflow)?
            .as_nanos();
        // 245ns is one UUIDv7 sub-millisecond step at 12-bit precision.
        self.uuid_ns = now.max(self.uuid_ns + 245);
        Ok(self.uuid_ns)
    }
}
// PostgreSQL 18 pg_prng.c uses xoroshiro128** with SplitMix64 seeding.
pub(super) struct Prng(u64, u64);

impl Prng {
    fn from_seed(mut seed: u64) -> Self {
        fn splitmix(seed: &mut u64) -> u64 {
            *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut word = *seed;
            word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            word ^ (word >> 31)
        }
        Self(splitmix(&mut seed), splitmix(&mut seed))
    }

    fn seeded(seed: f64) -> Self {
        Self::from_seed((seed * ((1_u64 << 52) - 1) as f64) as i64 as u64)
    }

    fn next(&mut self) -> u64 {
        let Self(first, second) = *self;
        let mixed = first ^ second;
        self.0 = first.rotate_left(24) ^ mixed ^ (mixed << 16);
        self.1 = mixed.rotate_left(37);
        first.wrapping_mul(5).rotate_left(7).wrapping_mul(9)
    }

    pub(super) fn double(&mut self) -> f64 {
        (self.next() >> 12) as f64 / (1_u64 << 52) as f64
    }

    pub(super) fn inclusive(&mut self, upper: u64) -> u64 {
        if upper == 0 {
            return 0;
        }
        loop {
            let candidate = self.next() >> upper.leading_zeros();
            if candidate <= upper {
                return candidate;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_18_seeded_sequence() {
        let mut random = Prng::seeded(0.5);
        assert_eq!(random.double(), 0.9851677175347999);
        assert_eq!(random.double(), 0.825301858027981);
        let mut random = Prng::seeded(0.5);
        assert_eq!(random.inclusive(1), 1);
        assert_eq!(random.double(), 0.825301858027981);
    }
}
