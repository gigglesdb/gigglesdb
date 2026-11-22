//! Session-local state shared by volatile SQL functions, including cursor fetches.
use std::{
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use super::{R, SqlError};

#[derive(Default)]
pub(crate) struct SessionVolatile {
    random: Option<Prng>,
    uuid_ns: u128,
}

thread_local! {
    static ACTIVE: RefCell<Option<Arc<Mutex<SessionVolatile>>>> = const { RefCell::new(None) };
    // Direct relational execution has no SqlSession.
    static LOCAL: RefCell<SessionVolatile> = RefCell::new(SessionVolatile::default());
}

pub(crate) struct Scope {
    previous: Option<Arc<Mutex<SessionVolatile>>>,
    thread: PhantomData<Rc<()>>,
}

impl Scope {
    pub(crate) fn enter(session: &Arc<Mutex<SessionVolatile>>) -> Self {
        Self {
            previous: ACTIVE.replace(Some(session.clone())),
            thread: PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.replace(self.previous.take());
    }
}

impl SessionVolatile {
    pub(super) fn with<T>(eval: impl FnOnce(&mut Self) -> R<T>) -> R<T> {
        match ACTIVE.with(|active| active.borrow().clone()) {
            Some(session) => eval(&mut *session.lock().map_err(|_| SqlError::Unsupported)?),
            None => LOCAL.with(|state| eval(&mut state.borrow_mut())),
        }
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
