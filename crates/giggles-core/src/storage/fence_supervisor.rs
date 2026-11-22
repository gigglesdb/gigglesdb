//! Opt-in crash recovery for a local RocksDB engine slot.

use std::time::Duration;

/// Automatic recovery settings for a local V2 RocksDB database.
#[derive(Clone, Copy, Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FenceRecovery {
    pub enabled: bool,
    #[serde(rename = "drain_timeout_seconds", deserialize_with = "seconds")]
    pub drain_timeout: Duration,
    #[serde(rename = "close_timeout_seconds", deserialize_with = "seconds")]
    pub close_timeout: Duration,
    #[serde(rename = "backoff_base_seconds", deserialize_with = "seconds")]
    pub backoff_base: Duration,
    #[serde(rename = "backoff_cap_seconds", deserialize_with = "seconds")]
    pub backoff_cap: Duration,
    pub backoff_jitter_percent: u8,
    pub max_attempts: u32,
    pub max_recoveries: usize,
    #[serde(rename = "flap_window_seconds", deserialize_with = "seconds")]
    pub flap_window: Duration,
    #[serde(rename = "stall_limit_seconds", deserialize_with = "seconds")]
    pub stall_limit: Duration,
    #[serde(rename = "stable_after_seconds", deserialize_with = "seconds")]
    pub stable_after: Duration,
    #[serde(rename = "stall_log_interval_seconds", deserialize_with = "seconds")]
    pub stall_log_interval: Duration,
}

fn seconds<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    <u64 as serde::Deserialize>::deserialize(deserializer).map(Duration::from_secs)
}

impl Default for FenceRecovery {
    fn default() -> Self {
        Self {
            enabled: false,
            drain_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(30),
            backoff_base: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(30),
            backoff_jitter_percent: 20,
            max_attempts: 5,
            max_recoveries: 5,
            flap_window: Duration::from_secs(3600),
            stall_limit: Duration::from_secs(300),
            stable_after: Duration::from_secs(60),
            stall_log_interval: Duration::from_secs(5),
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
impl FenceRecovery {
    pub(super) fn validate(self) -> Result<(), crate::CoreError> {
        if self.max_attempts == 0
            || self.max_recoveries == 0
            || self.flap_window.is_zero()
            || self.backoff_base > self.backoff_cap
            || self.backoff_jitter_percent > 100
            || self.stall_log_interval.is_zero()
        {
            return Err(crate::CoreError::Storage(
                "invalid fence recovery settings".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(feature = "engine-rocksdb")]
pub(super) use worker::{Wake, spawn};

#[cfg(feature = "engine-rocksdb")]
mod worker {
    use super::{Duration, FenceRecovery};
    use crate::storage::{
        Backend, Inner,
        engine::{
            diagnostics,
            kv::{FenceCause, FenceClass, FenceSite, FenceState},
        },
        engine_slot::{
            ClosedProof, DrainError, EngineSlot, Generation, RecoveryCandidate, RecoveryOpenRequest,
        },
        v2::{OpenFailure, retry_writer_lock},
    };
    use std::{
        collections::VecDeque,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, Condvar, Mutex, Weak,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread,
        time::Instant,
    };

    /// An event sequence closes the gap between checking a predicate and
    /// parking. The pending generation coalesces reports and retains the
    /// newest report across recovery of the previous generation.
    #[derive(Default)]
    pub(in crate::storage) struct Wake {
        sequence: Mutex<u64>,
        changed: Condvar,
        pending: AtomicU64,
        stopping: AtomicBool,
    }

    impl Wake {
        pub(in crate::storage) fn signal(&self) {
            let mut sequence = self.sequence.lock().unwrap_or_else(|p| p.into_inner());
            *sequence = sequence.wrapping_add(1);
            self.changed.notify_one();
        }

        pub(in crate::storage) fn fenced(&self, generation: u64) {
            self.pending.fetch_max(generation, Ordering::AcqRel);
            self.signal();
        }

        pub(in crate::storage) fn stop(&self) {
            self.stopping.store(true, Ordering::Release);
            self.signal();
        }

        fn stopping(&self) -> bool {
            self.stopping.load(Ordering::Acquire)
        }

        fn observe(&self) -> u64 {
            *self.sequence.lock().unwrap_or_else(|p| p.into_inner())
        }

        fn wait(&self, observed: u64, duration: Option<Duration>) {
            let sequence = self.sequence.lock().unwrap_or_else(|p| p.into_inner());
            let unchanged = |sequence: &mut u64| *sequence == observed;
            match duration {
                Some(duration) => drop(
                    self.changed
                        .wait_timeout_while(sequence, duration, unchanged),
                ),
                None => drop(self.changed.wait_while(sequence, unchanged)),
            }
        }
    }

    pub(in crate::storage) fn spawn(
        owner: Weak<Inner>,
        wake: Arc<Wake>,
        settings: FenceRecovery,
    ) -> std::io::Result<()> {
        launch(owner, wake, settings, SystemRuntime).map(drop)
    }

    fn launch(
        owner: Weak<Inner>,
        wake: Arc<Wake>,
        settings: FenceRecovery,
        runtime: impl Runtime + Send + 'static,
    ) -> std::io::Result<thread::JoinHandle<()>> {
        thread::Builder::new()
            .name("fence-recovery".into())
            .spawn(move || {
                Supervisor {
                    owner,
                    wake,
                    settings,
                    runtime,
                }
                .run()
            })
    }

    trait Runtime {
        fn now(&self) -> Instant;
        fn wait(&self, wake: &Wake, observed: u64, duration: Option<Duration>);
        fn jitter(&self) -> f64;

        fn open(&self, request: &RecoveryOpenRequest) -> Result<RecoveryCandidate, OpenFailure> {
            request.open()
        }

        #[cfg(test)]
        fn before_install(&self, _candidate: &RecoveryCandidate) {}

        #[cfg(test)]
        fn after_success_recorded(&self, _generation: u64) {}
    }

    struct SystemRuntime;

    impl Runtime for SystemRuntime {
        fn now(&self) -> Instant {
            Instant::now()
        }

        fn wait(&self, wake: &Wake, observed: u64, duration: Option<Duration>) {
            wake.wait(observed, duration);
        }

        fn jitter(&self) -> f64 {
            let mut bytes = [0; 4];
            match getrandom::fill(&mut bytes) {
                Ok(()) => 2.0 * f64::from(u32::from_le_bytes(bytes)) / f64::from(u32::MAX) - 1.0,
                Err(_) => 0.0,
            }
        }
    }

    struct Supervisor<R> {
        owner: Weak<Inner>,
        wake: Arc<Wake>,
        settings: FenceRecovery,
        runtime: R,
    }

    enum Abort {
        Stopping,
        GiveUp(FenceCause),
    }

    impl<R: Runtime> Supervisor<R> {
        fn with_slot<T>(&self, f: impl FnOnce(&EngineSlot) -> T) -> Option<T> {
            let owner = self.owner.upgrade()?;
            match &owner.backend {
                Backend::V2(slot) => Some(f(slot)),
                Backend::Legacy(_) => None,
            }
        }

        fn run(self) {
            let mut expected = 0;
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.run_loop(&mut expected))) {
                let message = payload.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                let message = format!("recovery supervisor panicked: {message}");
                if expected != 0 {
                    let cause = FenceCause::new(FenceSite::Open, FenceClass::NotRecoverable, &message);
                    self.with_slot(|slot| slot.give_up(expected, cause));
                }
                diagnostics::error(format!("generation={expected}: {message}"));
            }
        }

        fn run_loop(&self, active_generation: &mut u64) {
            let mut recoveries = VecDeque::new();
            let mut retry_level = 0u32;
            loop {
                let observed = self.wake.observe();
                if self.wake.stopping() {
                    return;
                }
                let expected = self.wake.pending.swap(0, Ordering::AcqRel);
                *active_generation = expected;
                if expected == 0 {
                    self.runtime.wait(&self.wake, observed, None);
                    continue;
                }
                let Some((generation, causes)) = self
                    .with_slot(|slot| slot.admit_recovery(expected, true))
                    .flatten()
                else {
                    continue;
                };
                let started = self.runtime.now();
                if recoveries
                    .back()
                    .is_none_or(|last| started.duration_since(*last) >= self.settings.stable_after)
                {
                    retry_level = 0;
                }
                while recoveries
                    .front()
                    .is_some_and(|at| started.duration_since(*at) >= self.settings.flap_window)
                {
                    recoveries.pop_front();
                }
                let cause = causes.cause().expect("admitted fence cause");
                let result = if recoveries.len() >= self.settings.max_recoveries {
                    let mut cause = cause;
                    cause.error_text =
                        format!("recovery flapping limit reached ({})", cause.error_text);
                    Err(Abort::GiveUp(cause))
                } else {
                    self.recover(generation, &causes, cause, &mut retry_level)
                };
                match result {
                    Ok(attempt) => {
                        let now = self.runtime.now();
                        recoveries.push_back(now);
                        #[cfg(test)]
                        self.runtime.after_success_recorded(expected + 1);
                        diagnostics::info(format!(
                            "storage recovered generation={} duration={:?} attempt={attempt}",
                            expected + 1,
                            now.duration_since(started),
                        ));
                    }
                    Err(Abort::GiveUp(cause)) => {
                        if let Some((cause, attempts)) = self
                            .with_slot(|slot| slot.give_up(expected, cause))
                            .flatten()
                        {
                            diagnostics::error(format!(
                                "recovery gave up generation={expected} site={:?} class={:?} attempts={attempts}: {}",
                                cause.site, cause.class, cause.error_text,
                            ));
                        }
                    }
                    Err(Abort::Stopping) => return,
                }
            }
        }

        fn check(&self, causes: &FenceState) -> Result<(), Abort> {
            if self.wake.stopping() {
                return Err(Abort::Stopping);
            }
            if let Some(cause) = causes
                .cause()
                .filter(|c| c.class != FenceClass::Recoverable)
            {
                return Err(Abort::GiveUp(cause));
            }
            Ok(())
        }

        fn recover(
            &self,
            generation: Arc<Generation>,
            causes: &FenceState,
            cause: FenceCause,
            retry_level: &mut u32,
        ) -> Result<u32, Abort> {
            let mut stall_left = self.settings.stall_limit;
            self.wait_for_drain(
                causes,
                &cause,
                "leases",
                self.settings.drain_timeout,
                &mut stall_left,
                || generation.drain(Duration::ZERO),
            )?;
            let closing = match generation.close_drained() {
                Ok(closing) => closing,
                Err(error) => {
                    self.check(causes)?;
                    let mut cause = cause;
                    cause.error_text = format!(
                        "drained engine could not close ({error}): {}",
                        cause.error_text
                    );
                    return Err(Abort::GiveUp(cause));
                }
            };
            closing.notify_recovery(&self.wake);
            let proof = self.wait_for_drain(
                causes,
                &cause,
                "close latch",
                self.settings.close_timeout,
                &mut stall_left,
                || {
                    closing
                        .wait(Duration::ZERO)
                        .map_err(|_| DrainError::TimedOut {
                            active: 0,
                            holders: Vec::new(),
                        })
                },
            )?;
            self.reopen(&proof, causes, cause, retry_level)
        }

        fn wait_for_drain<T>(
            &self,
            causes: &FenceState,
            cause: &FenceCause,
            phase: &str,
            initial: Duration,
            stall_left: &mut Duration,
            mut poll: impl FnMut() -> Result<T, DrainError>,
        ) -> Result<T, Abort> {
            let started = self.runtime.now();
            let mut next_log = Duration::ZERO;
            loop {
                let observed = self.wake.observe();
                self.check(causes)?;
                let (active, holders) = match poll() {
                    Ok(value) => {
                        let stalled = self
                            .runtime
                            .now()
                            .duration_since(started)
                            .saturating_sub(initial);
                        *stall_left = stall_left.saturating_sub(stalled);
                        return Ok(value);
                    }
                    Err(DrainError::TimedOut { active, holders }) => (active, holders),
                    Err(DrainError::Stopping) => return Err(Abort::Stopping),
                };
                let elapsed = self.runtime.now().duration_since(started);
                if elapsed < initial {
                    self.runtime
                        .wait(&self.wake, observed, Some(initial - elapsed));
                    continue;
                }
                let stalled = elapsed - initial;
                if stalled >= *stall_left {
                    let mut cause = cause.clone();
                    cause.error_text = format!(
                        "stalled drain at {phase} (active={active}, holders={holders:?}): {}",
                        cause.error_text
                    );
                    return Err(Abort::GiveUp(cause));
                }
                if stalled >= next_log {
                    diagnostics::info(format!(
                        "drain waiting generation={} phase={phase} active={active} holders={holders:?}",
                        cause.generation,
                    ));
                    next_log = stalled.saturating_add(self.settings.stall_log_interval);
                }
                self.runtime.wait(
                    &self.wake,
                    observed,
                    Some(next_log.min(*stall_left) - stalled),
                );
            }
        }

        fn reopen(
            &self,
            proof: &ClosedProof,
            causes: &FenceState,
            trigger: FenceCause,
            retry_level: &mut u32,
        ) -> Result<u32, Abort> {
            let mut cause = trigger;
            for attempt in 1..=self.settings.max_attempts {
                let backoff = if attempt == 1 {
                    Duration::ZERO
                } else {
                    self.backoff(*retry_level)
                };
                self.wait_delay(causes, backoff)?;
                let request = self
                    .with_slot(|slot| slot.reopen_request(proof, attempt))
                    .ok_or(Abort::Stopping)?
                    .map_err(|failure| Abort::GiveUp(failure.cause))?;
                self.check(causes)?;
                diagnostics::info(format!(
                    "recovery attempt {attempt} generation={} backoff={backoff:?}",
                    cause.generation,
                ));

                let deadline_start = self.runtime.now();
                let mut lock_delay = Duration::from_millis(30);
                let opened = retry_writer_lock(
                    || self.runtime.open(&request),
                    || {
                        let left = Duration::from_secs(10)
                            .saturating_sub(self.runtime.now().duration_since(deadline_start));
                        if left.is_zero() {
                            return false;
                        }
                        let waited = self.wait_delay(causes, lock_delay.min(left));
                        lock_delay = lock_delay.saturating_mul(2).min(Duration::from_secs(1));
                        waited.is_ok()
                    },
                );
                self.check(causes)?;
                let failure = match opened {
                    Ok(mut candidate) => {
                        #[cfg(test)]
                        self.runtime.before_install(&candidate);
                        self.check(causes)?;
                        let result = self.with_slot(|slot| slot.install_recovery(&mut candidate));
                        match result {
                            Some(Ok(())) => return Ok(attempt),
                            Some(Err(cause)) => cause,
                            None => return Err(Abort::Stopping),
                        }
                    }
                    Err(failure) => failure.cause,
                };
                self.check(causes)?;
                cause = FenceCause {
                    generation: cause.generation,
                    ..failure
                };
                if cause.class != FenceClass::Recoverable {
                    break;
                }
                *retry_level = retry_level.saturating_add(1);
            }
            Err(Abort::GiveUp(cause))
        }

        fn backoff(&self, level: u32) -> Duration {
            let mut delay = self.settings.backoff_base;
            if delay.is_zero() {
                return delay;
            }
            for _ in 0..level {
                if delay >= self.settings.backoff_cap {
                    break;
                }
                delay = delay.saturating_mul(2).min(self.settings.backoff_cap);
            }
            let factor = 1.0
                + self.runtime.jitter() * f64::from(self.settings.backoff_jitter_percent) / 100.0;
            Duration::try_from_secs_f64(delay.as_secs_f64() * factor).unwrap_or(Duration::MAX)
        }

        fn wait_delay(&self, causes: &FenceState, duration: Duration) -> Result<(), Abort> {
            let started = self.runtime.now();
            loop {
                let observed = self.wake.observe();
                self.check(causes)?;
                let left = duration.saturating_sub(self.runtime.now().duration_since(started));
                if left.is_zero() {
                    return Ok(());
                }
                self.runtime.wait(&self.wake, observed, Some(left));
            }
        }
    }

    #[cfg(test)]
    mod tests {
        include!("fence_supervisor_tests.rs");
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn unsupported_legacy_enable_and_disabled_noop() {
        use crate::{Admin, CoreError, Limits};
        let mut id = [0; 16];
        getrandom::fill(&mut id).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "fence-supervisor-legacy-{:032x}",
            u128::from_be_bytes(id)
        ));
        let admin = Admin::create(&dir, Limits::default()).unwrap();
        let disabled = admin.set_fence_recovery(FenceRecovery::default());
        let enabled = admin.set_fence_recovery(FenceRecovery {
            enabled: true,
            ..FenceRecovery::default()
        });
        drop(admin);
        std::fs::remove_dir_all(dir).unwrap();
        assert_eq!(disabled, Ok(()));
        assert_eq!(enabled, Err(CoreError::Unsupported));
    }

    #[test]
    fn defaults_and_partial_json() {
        assert_eq!(
            serde_json::from_str::<FenceRecovery>("{}").unwrap(),
            FenceRecovery::default()
        );
        let config: FenceRecovery =
            serde_json::from_str(r#"{"enabled":true,"drain_timeout_seconds":2,"max_attempts":3}"#)
                .unwrap();
        assert_eq!(
            config,
            FenceRecovery {
                enabled: true,
                drain_timeout: Duration::from_secs(2),
                max_attempts: 3,
                ..FenceRecovery::default()
            }
        );
        assert!(serde_json::from_str::<FenceRecovery>(r#"{"drain_timeout_second":2}"#).is_err());
    }
}
