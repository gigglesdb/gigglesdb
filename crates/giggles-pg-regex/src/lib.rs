//! PostgreSQL 18.6 regular expressions, ported to Rust.
//!
//! `engine` is a port of Henry Spencer's regex engine as shipped in
//! PostgreSQL 18.6 (`src/backend/regex/*`, `src/include/regex/*`).
//! `api` is a port of the SQL-facing layer in
//! `src/backend/utils/adt/regexp.c` (regexp_* functions, SIMILAR TO
//! translation, substring-from-pattern). See NOTICE for the licences
//! that cover the ported code.

pub mod api;
pub mod engine;
