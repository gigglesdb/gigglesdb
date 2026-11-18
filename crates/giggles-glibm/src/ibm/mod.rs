// IBM Accurate Mathematical Library, Rust port of Debian glibc 2.41-12+deb13u4.
// Copyright (C) 2001-2025 Free Software Foundation, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later
// This is free software, distributed without warranty; see the GNU LGPL 2.1 or later.
//! Scalar binary64 IBM routines. Assumes round-to-nearest, ties-to-even.
//!
//! Source: Debian glibc 2.41-12+deb13u4, sysdeps/ieee754/dbl-64.
//! `s_sin`, `s_tan`, `e_asin`, `s_atan`, and `e_atan2` reproduce the
//! x86_64 FMA/AVX2 variants; `branred` reproduces the separate SSE2 helper.
//! Explicit `mul_add` calls preserve the image's instruction contractions,
//! including repeated products in range reduction. Do not replace them with
//! separate multiplication/addition, or fuse other operations during cleanup.
//! Tables use binary64 bit patterns, independent of the host's byte order.
//! asin/acos also reproduce the exported libm wrappers' domain-error NaN.
//! Floating-point exception flags, errno, and other rounding modes are outside
//! this module's interface. No platform transcendental routine is called.
//!
//! The style allowances below retain the upstream names, branch structure,
//! parenthesization, and scratch variables for comparison with the C routines.
//! Historical constants that are unused by the selected paths remain adjacent
//! to their tables. See COPYING.LIB for the upstream license.
#![allow(
    non_upper_case_globals,
    unused_parens,
    unused_mut,
    unused_assignments,
    dead_code
)]
#![allow(
    clippy::excessive_precision,
    clippy::approx_constant,
    clippy::needless_return,
    clippy::assign_op_pattern,
    clippy::let_and_return,
    clippy::collapsible_if,
    clippy::collapsible_else_if
)]
mod branred;
mod e_asin;
mod e_atan2;
mod s_atan;
mod s_sin;
mod s_tan;
pub use e_asin::{acos, asin};
pub use e_atan2::atan2;
pub use s_atan::atan;
pub use s_sin::{cos, sin};
pub use s_tan::tan;
fn hi(x: f64) -> i32 {
    (x.to_bits() >> 32) as i32
}
fn lo(x: f64) -> i32 {
    x.to_bits() as i32
}
fn set_hi(x: f64, h: i32) -> f64 {
    f64::from_bits((x.to_bits() & 0xffffffff) | ((h as u32 as u64) << 32))
}
fn set_lo(x: f64, l: i32) -> f64 {
    f64::from_bits((x.to_bits() & 0xffffffff00000000) | l as u32 as u64)
}
#[cfg(test)]
mod tests;
