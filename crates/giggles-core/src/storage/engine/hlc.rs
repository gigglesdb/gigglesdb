use super::codec::CodecError;

/// High 48 bits are physical milliseconds; low 16 bits are logical ticks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Ts(pub(crate) u64);

impl Ts {
    pub(crate) fn inverted_be(self) -> [u8; 8] {
        (!self.0).to_be_bytes()
    }

    pub(crate) fn from_inverted_be(bytes: [u8; 8]) -> Self {
        Self(!u64::from_be_bytes(bytes))
    }
}

#[derive(Debug, Default)]
pub(crate) struct HlcClock {
    pub(crate) last: Ts,
}

impl HlcClock {
    pub(crate) fn new(last_persisted: Ts) -> Self {
        Self {
            last: last_persisted,
        }
    }

    pub(crate) fn next(&mut self, now_ms: u64) -> Result<Ts, CodecError> {
        if now_ms >= (1u64 << 48) {
            return Err(CodecError::Clock);
        }
        let next = if now_ms > self.last.0 >> 16 {
            now_ms << 16
        } else {
            self.last.0.checked_add(1).ok_or(CodecError::Clock)?
        };
        self.last = Ts(next);
        Ok(self.last)
    }
}
