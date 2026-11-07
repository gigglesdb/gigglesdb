use giggles_core::{Admin, CoreError, Error};
use std::{
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
type CertificationProbe = Box<dyn Fn(u64) + Send + Sync>;

/// Time accuracy is an operator assumption. A suspend or clock discontinuity
/// latches failure until restart under a corrected host clock.
pub struct Clock {
    state: Mutex<(Instant, u64, bool)>,
    skew: u64,
    max_pause: Duration,
    #[cfg(test)]
    after_certification: Mutex<Option<CertificationProbe>>,
}
impl Clock {
    pub fn new(skew_seconds: u64, max_pause_seconds: u64) -> Result<Self, CoreError> {
        if max_pause_seconds == 0 || max_pause_seconds > 3600 || skew_seconds > 60 {
            return Err(CoreError::Limit);
        }
        Ok(Self {
            state: Mutex::new((Instant::now(), wall()?, false)),
            skew: skew_seconds,
            max_pause: Duration::from_secs(max_pause_seconds),
            #[cfg(test)]
            after_certification: Mutex::new(None),
        })
    }
    pub fn now(&self, admin: &Admin) -> Result<(u64, u64), CoreError> {
        let mut s = self.state.lock().map_err(|_| uncertain())?;
        // Keep the monotonic anchor paired with the wall sample across certification.
        let sampled_at = Instant::now();
        let now = wall()?;
        let elapsed = s.0.elapsed();
        if s.2
            || elapsed > self.max_pause
            || now < s.1
            || now.abs_diff(s.1.saturating_add(elapsed.as_secs())) > self.skew.saturating_add(1)
        {
            s.2 = true;
            return Err(uncertain());
        }
        let (lower, upper) = interval(now, self.skew)?;
        match admin.certify_server_time(lower, upper) {
            Ok(()) => {}
            // A cancelled invocation is not evidence of an uncertain host clock.
            Err(CoreError::Cancelled) => return Err(CoreError::Cancelled),
            Err(_) => {
                s.2 = true;
                return Err(uncertain());
            }
        }
        #[cfg(test)]
        if let Some(probe) = self.after_certification.lock().unwrap().as_ref() {
            probe(now);
        }
        *s = (sampled_at, now, false);
        Ok((lower, upper))
    }
}
pub fn interval(now: u64, skew: u64) -> Result<(u64, u64), CoreError> {
    Ok((
        now.checked_sub(skew).ok_or_else(uncertain)?,
        now.checked_add(skew).ok_or_else(uncertain)?,
    ))
}
fn uncertain() -> CoreError {
    CoreError::Policy(Error::UncertainTime)
}
fn wall() -> Result<u64, CoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| uncertain())
}

#[cfg(test)]
mod tests {
    use super::*;
    static NEXT_CLOCK_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    fn admin() -> Admin {
        let dir = crate::test_support::fixture_root().join(format!(
            "clock-unit-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_CLOCK_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Admin::create(dir.join("db.redb"), Default::default()).unwrap()
    }
    #[test]
    fn cancelled_watermark_does_not_latch_clock_uncertainty() {
        let a = admin();
        let c = Clock::new(0, 10).unwrap();
        let request = giggles_core::Cancellation::default();
        assert!(request.cancel());
        assert_eq!(request.scope(|| c.now(&a)), Err(CoreError::Cancelled));
        assert!(c.now(&a).is_ok());
    }
    #[test]
    fn backward_clock_latches_uncertainty() {
        let a = admin();
        let c = Clock::new(0, 10).unwrap();
        c.state.lock().unwrap().1 = wall().unwrap() + 20;
        assert!(c.now(&a).is_err());
        c.state.lock().unwrap().1 = wall().unwrap();
        assert!(c.now(&a).is_err());
    }
    #[test]
    fn excessive_pause_latches_uncertainty() {
        let a = admin();
        let c = Clock::new(0, 1).unwrap();
        c.state.lock().unwrap().0 = Instant::now() - Duration::from_secs(2);
        assert!(c.now(&a).is_err());
    }
    #[test]
    fn forward_discontinuity_is_uncertain() {
        let a = admin();
        let c = Clock::new(0, 10).unwrap();
        c.state.lock().unwrap().1 = wall().unwrap() - 20;
        assert!(c.now(&a).is_err());
    }

    #[test]
    fn durable_certification_delay_preserves_the_clock_sample_pair() {
        let a = admin();
        let c = Clock::new(0, 3600).unwrap();
        *c.after_certification.lock().unwrap() = Some(Box::new(|sample| {
            let started = Instant::now();
            while wall().unwrap() < sample.checked_add(3).unwrap() {
                assert!(
                    started.elapsed() < Duration::from_secs(10),
                    "host wall failed to progress"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }));
        let sampled_before = Instant::now();
        let first = c.now(&a).unwrap();
        let after_wall = wall().unwrap();
        assert!(after_wall >= first.0 + 3);
        assert!(
            after_wall.abs_diff(first.0.saturating_add(sampled_before.elapsed().as_secs())) <= 1,
            "host wall discontinuity during regression"
        );
        *c.after_certification.lock().unwrap() = None;
        let next = c.now(&a).unwrap();
        assert!(next.0 >= after_wall);
        assert_eq!(next.0, next.1);
        assert!(c.now(&a).is_ok());
    }
}
