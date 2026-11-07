//! v2 storage engine (GD-0054). Inactive until wired by F7.
#![allow(dead_code)]

pub(crate) mod codec;
pub(crate) mod crc32c;
pub(crate) mod hlc;
pub(crate) mod kv;
pub(crate) mod kv_mem;
pub(crate) mod locks;

#[cfg(test)]
pub(crate) mod kv_conformance;

#[cfg(test)]
mod tests;

pub(crate) mod commit;
pub(crate) mod mvcc;
pub(crate) mod txn;

pub(crate) mod ssi_validate;

#[cfg(test)]
mod history_tests;
