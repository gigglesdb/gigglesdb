//! v2 storage engine (GD-0054). Inactive until wired by F7.
#![allow(dead_code)]

pub(crate) mod codec;
pub(crate) mod crc32c;
pub(crate) mod hlc;
pub(crate) mod kv;
pub(crate) mod kv_mem;
#[cfg(feature = "engine-rocksdb")]
pub(crate) mod kv_rocksdb;
pub(crate) mod locks;

#[cfg(test)]
pub(crate) mod kv_conformance;

#[cfg(test)]
mod tests;

pub(crate) mod commit;
pub(crate) mod content_check;
pub(crate) mod gc;
pub(crate) mod mvcc;
pub(crate) mod once_guard;
pub(crate) mod txn;

pub(crate) mod catalog;
pub(crate) mod catalog_codec;
pub(crate) mod catalog_label_codec;
pub(crate) mod indexes;
pub(crate) mod rows;
pub(crate) mod seq;
pub(crate) mod xact;
pub(crate) mod ssi_validate;

#[cfg(test)]
mod history_tests;

#[cfg(test)]
mod task_kind_tests;

pub(crate) mod kernel;
pub(crate) mod meta;
pub(crate) mod write_fence;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "write_tripwire_tests.rs"]
mod write_tripwire_tests;
mod native_content;
