use giggles_core::{Admin, CoreError, Error};
use std::{
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Time accuracy is an operator assumption. A suspend or clock discontinuity
/// latches failure until restart under a corrected host clock.
pub struct Clock {
    state: Mutex<(Instant, u64, bool)>,
    skew: u64,
    max_pause: Duration,
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
        })
    }
    pub fn now(&self, admin: &Admin) -> Result<(u64, u64), CoreError> {
        let mut s = self.state.lock().map_err(|_| uncertain())?;
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
        if admin.certify_server_time(lower, upper).is_err() {
            s.2 = true;
            return Err(uncertain());
        }
        *s = (Instant::now(), now, false);
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
    fn admin() -> Admin {
        let dir = std::path::PathBuf::from("artifacts").join(format!(
            "clock-unit-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Admin::create(dir.join("db.redb"), Default::default()).unwrap()
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
}
