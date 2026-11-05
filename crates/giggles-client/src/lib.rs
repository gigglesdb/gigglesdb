#![doc = include_str!("../README.md")]
//! Bounded authenticated clients for the consensus native task protocol.
//!
//! Persist the business operation ID and exact request **before** calling execute.
//! Preview is advisory; the server revalidates current authority on every call.
//! An [`Outcome::Unknown`] requires status resolution with the original ID.
//! Status absence does not prove a pending proposal can never commit.
//! Dropping a call future cancels its transport and releases its admission permit;
//! cancellation after sending cannot cancel a decision already made by the server.
mod client;
mod config;
mod operation;
mod outcome;
mod transport;

pub use client::Client;
pub use config::{Config, Limits};
pub use giggles_core::{ApprovalId, OperationId, Request};
pub use operation::Operation;
pub use outcome::{ClientError, Denial, Outcome, Recorded, UnknownReason};
