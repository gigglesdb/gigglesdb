//! Synchronous cutover entry points over the Runtime's sole coordinator.
//!
//! Lock order: take the Runtime cutover gate, then briefly take the coordinator
//! mutex to acquire/authenticate migration exclusion. Release the coordinator
//! mutex before calling user code. Keep the cutover gate and
//! the owned exclusion across that code and the final journal refresh. Nested
//! operations borrow HeldCutover and never acquire the exclusion again. They may
//! take the coordinator mutex while the exclusion is held. No external closure
//! runs with the coordinator mutex held.
//! The gate is nested in the existing health Arc, preserving Runtime's
//! eight-pointer layout. Another outer call waits at this gate without holding
//! the coordinator mutex, so it cannot obstruct the active closure's operations.
//!
//! Async work always runs on a joined OS thread with its own Tokio runtime;
//! neither block_on nor blocking_lock runs in the caller's entered runtime.
//! The user closure stays on its original thread and need not be Send. As with
//! any synchronous API, callers must not hold the coordinator mutex or require
//! their blocked thread to drive a separately spawned operation to completion.
//! Exclusion liveness is instantaneous evidence; database effects still need
//! their transactional authority-epoch fence after a PostgreSQL session loss.

use super::Runtime;
use crate::{
    coordinator::{self, ExclusionGuard, exclusion, reverse::Authenticated},
    cutover::{self, Authority, Key},
    pg::{activate, fence, reverse},
};

/// A cutover authority using the Runtime's existing coordinator and exclusion.
pub struct CutoverAuthority {
    runtime: Runtime,
}

/// Unforgeable, scoped permission for nested cutover operations. Only run_with
/// constructs it; it cannot outlive the outer exclusion guard. Borrow it as an
/// Authority (`let mut nested = held`) to follow rollback::Held's convention.
///
/// Runtime::setup_reverse requires this capability; there is deliberately no
/// public constructor or unguarded Runtime reverse-setup entry point.
pub struct HeldCutover<'a> {
    runtime: &'a Runtime,
    key: &'a Key,
    guard: &'a ExclusionGuard,
}

fn bridge<T: Send>(
    run: impl FnOnce(&tokio::runtime::Runtime) -> cutover::Result<T> + Send,
) -> cutover::Result<T> {
    std::thread::scope(|scope| {
        let worker = std::thread::Builder::new()
            .name("cutover-authority".into())
            .spawn_scoped(scope, || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| cutover::Error::Component(error.to_string()))?;
                run(&runtime)
            })
            .map_err(|error| cutover::Error::Component(error.to_string()))?;
        match worker.join() {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
}

fn exclusion_error(error: exclusion::Error) -> cutover::Error {
    match error {
        // cutover::Error is a closed enum. Keep missing configuration typed as
        // Invalid rather than hiding it in a Component string.
        exclusion::Error::NotConfigured => {
            cutover::Error::Invalid("migration exclusion is not configured")
        }
        _ => cutover::Error::LostOwnership,
    }
}

fn coordinator_error(error: coordinator::Error) -> cutover::Error {
    match error {
        coordinator::Error::LostOwnership | coordinator::Error::Observer => {
            cutover::Error::LostOwnership
        }
        coordinator::Error::ReverseProof(error) => cutover::Error::ReverseProof(error),
        error => cutover::Error::Component(error.to_string()),
    }
}

fn matches(guard: &ExclusionGuard, key: &Key) -> bool {
    guard.migration() == key.migration
        && guard.owner().is_some_and(|(owner, epoch)| {
            owner == key.coordinator && u64::try_from(key.epoch) == Ok(epoch)
        })
}

impl Runtime {
    pub fn cutover_authority(&self) -> CutoverAuthority {
        CutoverAuthority {
            runtime: self.clone(),
        }
    }

    /// Install reverse replication under the outer authority, returning only the
    /// real journal-authenticated proof. Connections must be movable to the
    /// joined bridge thread; their synchronous transports run outside an entered
    /// runtime, with a separately created runtime driving coordinator work.
    ///
    /// The capability must belong to this Runtime. Snapshot refresh runs even
    /// when setup fails after publishing an intent. Nested setup never acquires
    /// migration exclusion a second time. The coordinator mutex is held during
    /// setup itself; transports must not call back into this Runtime.
    pub fn setup_reverse(
        &self,
        held: &HeldCutover<'_>,
        config: &reverse::Config,
        destination: &mut (impl fence::Connection + Send),
        old_source: &mut (impl fence::Connection + Send),
        destination_fence: &mut activate::DestinationFence,
    ) -> cutover::Result<Authenticated> {
        if !std::sync::Arc::ptr_eq(&self.0, &held.runtime.0) {
            return Err(cutover::Error::LostOwnership);
        }
        bridge(|runtime| {
            runtime.block_on(held.check())?;
            let mut coordinator = runtime.block_on(self.0.lock());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                coordinator.setup_reverse(
                    runtime,
                    config,
                    destination,
                    old_source,
                    destination_fence,
                )
            }));
            let refreshed = runtime.block_on(coordinator.refresh());
            *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
            let owns = coordinator.is_owner();
            drop(coordinator);
            let alive = runtime.block_on(held.guard.check_alive());
            match result {
                Err(panic) => std::panic::resume_unwind(panic),
                Ok(_) if refreshed.is_err() || !owns || alive.is_err() => {
                    Err(cutover::Error::LostOwnership)
                }
                Ok(result) => result.map_err(coordinator_error),
            }
        })
    }
}

impl CutoverAuthority {
    /// Supply a borrow-only capability to adapters that need owner-scoped
    /// operations. The closure may return non-Send values and borrow its caller.
    pub fn run_with<T>(
        &mut self,
        key: &Key,
        run: impl FnOnce(&HeldCutover<'_>) -> cutover::Result<T>,
    ) -> cutover::Result<T> {
        let _serial = bridge(|runtime| Ok(runtime.block_on(self.runtime.2.cutover.lock())))?;
        let guard = bridge(|runtime| {
            runtime.block_on(async {
                let mut coordinator = self.runtime.0.lock().await;
                let guard = Box::pin(coordinator.exclusive()).await;
                *self.runtime.1.write().expect("engine snapshot poisoned") =
                    coordinator.state().clone();
                guard.map_err(exclusion_error)
            })
        })?;
        if !matches(&guard, key) {
            return Err(cutover::Error::LostOwnership);
        }
        let held = HeldCutover {
            runtime: &self.runtime,
            key,
            guard: &guard,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&held)));
        let completed = bridge(|runtime| runtime.block_on(held.check()))
            .map_err(|_| cutover::Error::LostOwnership);
        match result {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(result) => {
                completed?;
                result
            }
        }
        // guard is released on success, error, validation failure and unwind.
    }
}

impl Authority for CutoverAuthority {
    fn run<T>(
        &mut self,
        key: &Key,
        run: impl FnOnce() -> cutover::Result<T>,
    ) -> cutover::Result<T> {
        self.run_with(key, |_| run())
    }
}

impl HeldCutover<'_> {
    async fn check(&self) -> cutover::Result<()> {
        let alive = self.guard.check_alive().await;
        let mut coordinator = self.runtime.0.lock().await;
        let refreshed = coordinator.refresh().await;
        *self.runtime.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        if alive.is_err()
            || refreshed.is_err()
            || !coordinator.is_owner()
            || !matches(self.guard, self.key)
            || coordinator.state().owner != self.key.coordinator
            || u64::try_from(self.key.epoch) != Ok(coordinator.state().epoch)
        {
            return Err(cutover::Error::LostOwnership);
        }
        Ok(())
    }
}

impl Authority for &HeldCutover<'_> {
    fn run<T>(
        &mut self,
        key: &Key,
        run: impl FnOnce() -> cutover::Result<T>,
    ) -> cutover::Result<T> {
        if key != self.key {
            return Err(cutover::Error::LostOwnership);
        }
        bridge(|runtime| runtime.block_on(self.check()))?;
        let result = run();
        bridge(|runtime| runtime.block_on(self.check()))?;
        result
    }
}

#[cfg(test)]
#[path = "cutover_authority_tests.rs"]
mod tests;
