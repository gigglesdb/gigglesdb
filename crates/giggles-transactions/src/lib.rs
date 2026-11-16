//! Deterministic rigorous two-phase locking and replicated transaction transitions.
//!
//! # Embedding contract
//!
//! This library is an implementation dependency, not a consensus or storage engine.
//! Call [`Machine::apply_committed`] only for entries the exact database/range/epoch
//! consensus group has committed. Persist its resulting snapshot **atomically with**
//! the applied index and any external effects before returning evidence or replies.
//! On a failed persistence attempt discard the tentative machine and replay from
//! durable state. Never serve an uncommitted clone through [`CommittedView`].
//!
//! Evidence is minted from applied machines and cannot be deserialized from client
//! messages. A trusted [`CommittedView`] resolves references through authenticated
//! consensus state plus a read barrier/applied-index check. It must never trust a
//! client's claim that a log index is committed. Snapshot input must be authenticated
//! and rollback-fenced by the embedding; the checksum only detects corruption.
//!
//! # Isolation and atomic observation
//!
//! All current reads, including absent keys and scans, must first acquire shared
//! point/predicate/gap locks. Keep a read transaction's locks until a durable home
//! outcome resolves it. Acquire *all* observation locks before exposing any rows.
//! A reader touching a fast-finalized range and a still-prepared range must block
//! on the latter, never return a partial transfer. Raw backing-store reads bypass
//! this contract. The planner must cover all indexes, uniqueness gaps, foreign keys,
//! catalog/policy dependencies and coarse intervals for unindexed predicates.
//!
//! Byte keys are lexicographically ordered, namespaced by the adapter (including
//! index/table/tenant). A gap is an explicit half-open interval, not inferred from
//! the current data. Writes require exclusive point coverage. Locks cannot be
//! released individually. Contended holders receive [`Error::MustRollback`]; the
//! caller must roll back/restart before prepare. Lock-free waiters enter a bounded
//! FIFO queue only when their transaction ID is greater than every conflicting
//! holder and queued predecessor. Thus every wait edge decreases one global order,
//! preventing cross-range cycles. FIFO is fair to admitted waiters; retrying clients
//! need bounded admission/backoff and have no starvation-free promise. Prepared work
//! cannot time out or unilaterally abort.
//!
//! # Protected operations and recovery
//!
//! Register and seal every participant, including the task authority, before begin.
//! Stage user mutations, task budget/membership, approval consumption and receipt
//! under the same operation digest and home decision. Lock and revalidate policy,
//! before-images and remaining budget before prepare; this crate cannot evaluate
//! them. Materialize deterministic effects and include them in the operation digest.
//! Stage each key's final value once; changing a staged value is a conflicting retry.
//! Acquire every participant's required locks before preparing any participant.
//! If a restart is required after any participant prepared, durably abort at the
//! home and finalize all participants before starting a new transaction identity.
//! A durable commit requires all matching prepares. Acknowledgement requires durable
//! redo at every participant and a durable home decision (waiting for every finalize
//! is the conservative integration). Recovery reads that home; absence, timeout or
//! coordinator death is never an abort certificate. Split/move must drain or retain
//! every prepared lock/intent and fence old epochs. No GC or epoch migration is
//! supplied: terminal records remain until admission caps reject new work.
//! Admission also reserves snapshot space for already registered votes, terminal
//! records and redo replies so resource exhaustion cannot evict recovery state.
//!
//! No SQL/core/distributed runtime integration, physical-time policy, production,
//! cloud or security qualification is implied.
//!
//! ```
//! use giggles_transactions::{DatabaseId, Domain, Limits, Machine};
//! let domain = Domain { database: DatabaseId([1; 16]), range: 7, epoch: 1 };
//! let machine = Machine::new(domain, Limits::default()).unwrap();
//! let bytes = machine.snapshot().unwrap();
//! let restored = Machine::restore(domain, Limits::default(), &bytes).unwrap();
//! assert_eq!(restored.applied_index(), 0);
//! ```
//!
//! Private evidence cannot be constructed from a remote claim:
//! ```compile_fail
//! use giggles_transactions::PrepareEvidence;
//! let forged = PrepareEvidence { reference: todo!() };
//! ```
//! A machine cannot bypass snapshot validation through public deserialization:
//! ```compile_fail
//! use giggles_transactions::Machine;
//! let forged: Machine = bincode::deserialize(&[]).unwrap();
//! ```

mod home;
mod machine;
mod types;
pub use home::*;
pub use machine::*;
pub use types::*;
