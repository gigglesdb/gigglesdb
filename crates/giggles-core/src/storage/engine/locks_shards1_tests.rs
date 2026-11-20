use super::*;
#[derive(Debug)]
struct LockManager(super::LockManager);
impl LockManager {
    fn new(mut config: LockConfig) -> Self {
        config.shards = 1;
        Self(super::LockManager::new(config))
    }
}
impl std::ops::Deref for LockManager {
    type Target = super::LockManager;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[path = "locks_tests.rs"]
mod tests;

#[path = "locks_deadlock_tests.rs"]
mod deadlock_tests;

#[path = "locks_interval_tests.rs"]
mod interval_tests;

#[path = "locks_wakeup_tests.rs"]
mod wakeup_tests;

#[path = "locks_row_tests.rs"]
mod row_tests;
