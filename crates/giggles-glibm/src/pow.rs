// Derived from glibc 2.41 e_pow.c.
// Copyright (C) 2018-2025 Free Software Foundation, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later
use crate::{basic::exp_tail, pow_data::*};
fn integer(y: f64) -> i32 {
    let iy = y.to_bits();
    let e = ((iy >> 52) & 0x7ff) as i32;
    if e < 1023 {
        return 0;
    }
    if e > 1075 {
        return 2;
    }
    let bit = 1u64 << (1075 - e);
    if iy & (bit - 1) != 0 {
        0
    } else if iy & bit != 0 {
        1
    } else {
        2
    }
}
fn signaling(x: f64) -> bool {
    x.is_nan() && x.to_bits() & (1u64 << 51) == 0
}
pub fn pow(x: f64, y: f64) -> f64 {
    let mut ix = x.to_bits();
    let iy = y.to_bits();
    if y == 0.0 {
        return if signaling(x) {
            f64::from_bits(ix | (1u64 << 51))
        } else {
            1.0
        };
    }
    if x == 1.0 {
        return if signaling(y) {
            f64::from_bits(iy | (1u64 << 51))
        } else {
            1.0
        };
    }
    if y.is_nan() {
        return f64::from_bits((if x.is_nan() { ix } else { iy }) | (1u64 << 51));
    }
    if x.is_nan() {
        let mut bits = ix | (1u64 << 51);
        if x.is_sign_negative() && integer(y) == 1 {
            bits ^= 1u64 << 63;
        }
        return f64::from_bits(bits);
    }
    if y.is_infinite() {
        if x.abs() == 1.0 {
            return 1.0;
        }
        return if (x.abs() < 1.0) == y.is_sign_positive() {
            0.0
        } else {
            f64::INFINITY
        };
    }
    if x == 0.0 || x.is_infinite() {
        let mut x2 = x * x;
        if x.is_sign_negative() && integer(y) == 1 {
            x2 = -x2;
        }
        return if y.is_sign_negative() { 1.0 / x2 } else { x2 };
    }
    let mut negative = false;
    if x < 0.0 {
        let i = integer(y);
        if i == 0 {
            return f64::from_bits(0xfff8000000000000);
        }
        negative = i == 1;
        ix &= 0x7fffffffffffffff;
    }
    let topy = (iy >> 52) & 0x7ff;
    if !(0x3be..0x43e).contains(&topy) {
        if ix == 1.0f64.to_bits() {
            return 1.0;
        }
        if topy < 0x3be {
            return if ix > 1.0f64.to_bits() {
                1.0 + y
            } else {
                1.0 - y
            };
        }
        return if (ix > 1.0f64.to_bits()) == y.is_sign_positive() {
            f64::INFINITY
        } else {
            0.0
        };
    }
    if ix >> 52 == 0 {
        ix = (x.abs() * 4503599627370496.0)
            .to_bits()
            .wrapping_sub(52u64 << 52);
    }
    let tmp = ix.wrapping_sub(0x3fe6955500000000);
    let i = ((tmp >> 45) % 128) as usize;
    let z = f64::from_bits(ix.wrapping_sub(tmp & (0xfffu64 << 52)));
    let kd = ((tmp as i64) >> 52) as f64;
    let invc = POW_TAB[3 * i];
    let logc = POW_TAB[3 * i + 1];
    let logctail = POW_TAB[3 * i + 2];
    let r = z.mul_add(invc, -1.0);
    let t1 = kd.mul_add(f64::from_bits(0x3fe62e42fefa3800), logc);
    let t2 = t1 + r;
    let lo1 = kd.mul_add(f64::from_bits(0x3d2ef35793c76730), logctail);
    let lo2 = t1 - t2 + r;
    let a = POW_A;
    let ar = a[0] * r;
    let ar2 = r * ar;
    let ar3 = r * ar2;
    let hi = t2 + ar2;
    let lo3 = ar.mul_add(r, -ar2);
    let lo4 = t2 - hi + ar2;
    let p = ar3
        * ar2.mul_add(
            ar2.mul_add(r.mul_add(a[6], a[5]), r.mul_add(a[4], a[3])),
            r.mul_add(a[2], a[1]),
        );
    let lo = lo1 + lo2 + lo3 + lo4 + p;
    let h = hi + lo;
    let l = hi - h + lo;
    let ehi = y * h;
    let elo = y.mul_add(l, y.mul_add(h, -ehi));
    exp_tail(ehi, elo, negative)
}
