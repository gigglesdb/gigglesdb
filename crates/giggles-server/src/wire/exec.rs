//! Execution seam for the native wire.

use crate::api::{
    ApiRuntime, CallOwnership, Completion,
    codec::wire_seam::{WireCall, WireSession},
    error::ApiError,
};
use std::sync::Arc;

pub(crate) async fn run_call(
    runtime: &ApiRuntime,
    session: &mut WireSession,
    call: WireCall,
    ownership: Arc<CallOwnership>,
) -> Result<Completion, ApiError> {
    runtime.execute_wire(session, call, ownership).await
}

#[cfg(test)]
pub(super) struct CallHook {
    block_first: bool,
    released: tokio::sync::Notify,
    pub(super) calls: std::sync::atomic::AtomicUsize,
    pub(super) cancelled: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl CallHook {
    pub(super) fn new(block_first: bool) -> Self {
        Self {
            block_first,
            released: tokio::sync::Notify::new(),
            calls: Default::default(),
            cancelled: Default::default(),
        }
    }

    pub(super) fn release(&self) {
        self.released.notify_one();
    }

    pub(super) async fn before(&self, cancel: &giggles_core::Cancellation) -> Result<(), ApiError> {
        use std::{sync::atomic::Ordering::Relaxed, time::Duration};
        if self.calls.fetch_add(1, Relaxed) == 0 && self.block_first {
            loop {
                if cancel.check().is_err() {
                    self.cancelled.store(true, Relaxed);
                    return Err(ApiError::DeadlineExceeded);
                }
                tokio::select! {
                    _ = self.released.notified() => break,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            }
        }
        Ok(())
    }
}
