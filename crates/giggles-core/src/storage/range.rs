mod keys;
mod local;
mod store;
mod export;
mod kv;
pub(super) mod engine; // Track C: gateway tests and the in-process cluster open engines.
mod oracle;
pub(super) mod home; // Track D: the gateway coordinator (storage::cluster) and its tests use it.
pub(super) mod txn_record; // Track D: R5-53 resolver, tested with the coordinator's fake cluster.

// Track A (R5-03): the multi-range store, public through `replicated` for giggles-replication.
pub use store::{RangeApplyOutcome, RangeStore};
