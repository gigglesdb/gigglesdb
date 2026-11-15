//! ReadIndex round bookkeeping, independent of Raft and storage.
struct Round<W> {
    ctx: Vec<u8>,
    waiters: Vec<W>,
    ticks: u32,
}

pub struct ReadBarrier<W> {
    node_id: u64,
    seq: u64,
    retry_ticks: u32,
    flight: Option<Round<W>>,
    queued: Vec<W>,
    ready: Vec<(u64, Vec<W>)>,
}
impl<W> ReadBarrier<W> {
    /// A zero retry interval retries on every tick.
    pub fn new(node_id: u64, retry_ticks: u32) -> Self {
        Self {
            node_id,
            seq: 0,
            retry_ticks,
            flight: None,
            queued: Vec::new(),
            ready: Vec::new(),
        }
    }
    fn start(&mut self, waiters: Vec<W>) -> Vec<u8> {
        self.seq = self
            .seq
            .checked_add(1)
            .expect("read round sequence exhausted");
        let ctx = [self.node_id.to_be_bytes(), self.seq.to_be_bytes()].concat();
        self.flight = Some(Round {
            ctx: ctx.clone(),
            waiters,
            ticks: 0,
        });
        ctx
    }
    pub fn request(&mut self, w: W) -> Option<Vec<u8>> {
        if self.flight.is_some() {
            self.queued.push(w);
            None
        } else {
            Some(self.start(vec![w]))
        }
    }
    pub fn on_read_state(&mut self, ctx: &[u8], index: u64) -> Option<Vec<u8>> {
        if self.flight.as_ref().is_none_or(|r| r.ctx != ctx) {
            return None;
        }
        let round = self.flight.take().expect("matching round");
        self.ready.push((index, round.waiters));
        if self.queued.is_empty() {
            None
        } else {
            let waiters = std::mem::take(&mut self.queued);
            Some(self.start(waiters))
        }
    }
    pub fn on_applied(&mut self, applied_index: u64) -> Vec<W> {
        let mut released = Vec::new();
        self.ready.retain_mut(|(index, waiters)| {
            if *index <= applied_index {
                released.append(waiters);
                false
            } else {
                true
            }
        });
        released
    }
    pub fn tick(&mut self) -> Option<Vec<u8>> {
        let round = self.flight.as_mut()?;
        round.ticks = round.ticks.saturating_add(1);
        if round.ticks < self.retry_ticks {
            return None;
        }
        let mut waiters = self.flight.take().expect("in-flight round").waiters;
        waiters.append(&mut self.queued);
        Some(self.start(waiters))
    }
    pub fn fail_all(&mut self) -> Vec<W> {
        let mut failed = self.flight.take().map_or_else(Vec::new, |r| r.waiters);
        failed.append(&mut self.queued);
        for (_, mut waiters) in self.ready.drain(..) {
            failed.append(&mut waiters);
        }
        // Keep seq across resets so contexts from a previous term stay stale.
        failed
    }
    pub fn is_idle(&self) -> bool {
        self.flight.is_none() && self.queued.is_empty() && self.ready.is_empty()
    }
}
