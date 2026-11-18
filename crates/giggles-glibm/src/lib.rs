//! Portable scalar glibc math port. See README.md for qualification status.
// SPDX-License-Identifier: LGPL-2.1-or-later
#![allow(clippy::excessive_precision, clippy::approx_constant)]
mod basic;
mod data;
mod erf;
mod erf_data;
mod extended;
mod gamma;
mod gamma_data;
mod hyperbolic;
mod ibm_pending;
mod pow;
mod pow_data;
pub use basic::{cbrt, exp, log, log10};
pub use erf::{erf, erfc};
pub use gamma::{lgamma, tgamma};
pub use hyperbolic::{acosh, asinh, atanh, cosh, sinh, tanh};
pub use ibm_pending::{acos, asin, atan, atan2, cos, sin, tan};
pub use pow::pow;
/// Set to false when replacing ibm_pending with the qualified IBM module.
pub const IBM_PENDING: bool = true;

pub static IDENTITY: &std::ffi::CStr = c"giggles-glibm/1 glibc-2.41-12+deb13u4 x86_64-fma";
#[unsafe(no_mangle)]
pub extern "C" fn giggles_glibm_identity() -> *const std::ffi::c_char {
    IDENTITY.as_ptr()
}

macro_rules! unary_exports {
    ($($abi:ident => $f:ident),*) => {$(
        #[unsafe(no_mangle)]
        pub extern "C" fn $abi(x: f64) -> f64 { $f(x) }
    )*};
}
unary_exports!(giggles_glibm_cbrt=>cbrt,giggles_glibm_exp=>exp,giggles_glibm_log=>log,giggles_glibm_log10=>log10,
giggles_glibm_sin=>sin,giggles_glibm_cos=>cos,giggles_glibm_tan=>tan,giggles_glibm_asin=>asin,giggles_glibm_acos=>acos,giggles_glibm_atan=>atan,
giggles_glibm_sinh=>sinh,giggles_glibm_cosh=>cosh,giggles_glibm_tanh=>tanh,giggles_glibm_asinh=>asinh,giggles_glibm_acosh=>acosh,giggles_glibm_atanh=>atanh,
giggles_glibm_erf=>erf,giggles_glibm_erfc=>erfc,giggles_glibm_tgamma=>tgamma,giggles_glibm_lgamma=>lgamma);
#[unsafe(no_mangle)]
pub extern "C" fn giggles_glibm_pow(x: f64, y: f64) -> f64 {
    pow(x, y)
}
#[unsafe(no_mangle)]
pub extern "C" fn giggles_glibm_atan2(y: f64, x: f64) -> f64 {
    atan2(y, x)
}
