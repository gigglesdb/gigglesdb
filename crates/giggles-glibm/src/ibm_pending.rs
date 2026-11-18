//! Temporary ABI stubs. Replace with src/ibm when the parallel IBM port merges.
macro_rules! pending {
    ($($f:ident),*) => {$(pub fn $f(_x: f64) -> f64 { f64::NAN })*};
}
pending!(sin, cos, tan, asin, acos, atan);
pub fn atan2(_y: f64, _x: f64) -> f64 {
    f64::NAN
}
