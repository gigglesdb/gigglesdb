// Minimal round-to-nearest x87 significand emulation for gamma_product.
// No host extended-precision type or platform math is used.
// SPDX-License-Identifier: LGPL-2.1-or-later
#[derive(Clone, Copy)]
pub(crate) struct Ext {
    m: u64,
    e: i32,
    negative: bool,
}
fn round(v: u128, shift: u32) -> u128 {
    if shift == 0 {
        return v;
    }
    let q = v >> shift;
    let rem = v & ((1u128 << shift) - 1);
    let half = 1u128 << (shift - 1);
    q + u128::from(rem > half || (rem == half && q & 1 != 0))
}
impl Ext {
    pub fn from(x: f64) -> Self {
        if x == 0.0 {
            return Self {
                m: 0,
                e: 0,
                negative: false,
            };
        }
        let bits = x.to_bits();
        let exponent = ((bits >> 52) & 0x7ff) as i32;
        debug_assert!(exponent > 0 && exponent < 2047);
        Self {
            m: ((bits & 0x000fffffffffffff) | (1u64 << 52)) << 11,
            e: exponent - 1023 - 63,
            negative: x.is_sign_negative(),
        }
    }
    fn normalize(v: u128, e: i32, negative: bool) -> Self {
        if v == 0 {
            return Self::from(0.0);
        }
        let significant = 128 - v.leading_zeros();
        if significant <= 64 {
            let shift = 64 - significant;
            return Self {
                m: (v as u64) << shift,
                e: e - shift as i32,
                negative,
            };
        }
        let shift = significant - 64;
        let q = round(v, shift);
        if q == (1u128 << 64) {
            Self {
                m: 1u64 << 63,
                e: e + shift as i32 + 1,
                negative,
            }
        } else {
            Self {
                m: q as u64,
                e: e + shift as i32,
                negative,
            }
        }
    }
    pub fn add(self, b: Self) -> Self {
        if self.m == 0 {
            return b;
        }
        if b.m == 0 {
            return self;
        }
        let (a, b) = if self.e >= b.e { (self, b) } else { (b, self) };
        let shift = (a.e - b.e) as u32;
        if shift > 64 {
            return a;
        }
        let av = (a.m as u128) << shift;
        let bv = b.m as u128;
        if a.negative == b.negative {
            Self::normalize(av + bv, b.e, a.negative)
        } else if av >= bv {
            Self::normalize(av - bv, b.e, a.negative)
        } else {
            Self::normalize(bv - av, b.e, b.negative)
        }
    }
    pub fn mul(self, b: Self) -> Self {
        Self::normalize(
            self.m as u128 * b.m as u128,
            self.e + b.e,
            self.negative != b.negative,
        )
    }
    pub fn div(self, b: Self) -> Self {
        if self.m == 0 {
            return self;
        }
        let shift = if self.m >= b.m { 63 } else { 64 };
        let numerator = (self.m as u128) << shift;
        let den = b.m as u128;
        let q = numerator / den;
        let rem = numerator % den;
        let q = q + u128::from(2 * rem > den || (2 * rem == den && q & 1 != 0));
        Self::normalize(q, self.e - b.e - shift, self.negative != b.negative)
    }
    pub fn to_f64(self) -> f64 {
        if self.m == 0 {
            return 0.0;
        }
        let mut q = round(self.m as u128, 11) as u64;
        let mut e = self.e + 63;
        if q == 1u64 << 53 {
            q >>= 1;
            e += 1;
        }
        f64::from_bits(
            ((self.negative as u64) << 63) | (((e + 1023) as u64) << 52) | (q & 0x000fffffffffffff),
        )
    }
}
