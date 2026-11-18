// Derived from glibc 2.41 s_cbrt.c, e_exp.c, e_log.c, e_log10.c.
// Copyright (C) 1997-2025 Free Software Foundation, Inc.
// e_log10.c: Copyright (C) 1993 Sun Microsystems, Inc. All rights reserved.
// Developed at SunPro, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this software is freely
// granted, provided that this notice is preserved.
// SPDX-License-Identifier: LGPL-2.1-or-later
use crate::data::*;

pub fn cbrt(x: f64) -> f64 {
    if !x.is_finite() || x == 0.0 {
        return x + x;
    }
    let mut a = x.abs();
    let mut correction = 0;
    if a < f64::MIN_POSITIVE {
        a *= 18014398509481984.0;
        correction = -54;
    }
    let xe = ((a.to_bits() >> 52) as i32) - 1022 + correction;
    let xm = f64::from_bits((a.to_bits() & 0x000fffffffffffff) | (1022u64 << 52));
    let u = 0.354895765043919860
        + (1.50819193781584896
            + (-2.11499494167371287
                + (2.44693122563534430
                    + (-1.83469277483613086
                        + (0.784932344976639262 - 0.145263899385486377 * xm) * xm)
                        * xm)
                    * xm)
                * xm)
            * xm;
    let t2 = u * u * u;
    let factors = [
        1.0 / 1.5874010519681994748,
        1.0 / 1.2599210498948731648,
        1.0,
        1.2599210498948731648,
        1.5874010519681994748,
    ];
    let ym = u * (t2 + 2.0 * xm) / (2.0 * t2 + xm) * factors[(2 + xe % 3) as usize];
    ym.copysign(x) * f64::from_bits(((1023 + xe / 3) as u64) << 52)
}

pub fn exp(x: f64) -> f64 {
    exp_tail(x, 0.0, false)
}
pub(crate) fn exp_tail(x: f64, xtail: f64, negative: bool) -> f64 {
    let sign = if negative { -1.0 } else { 1.0 };
    let top = (x.to_bits() >> 52) & 0x7ff;
    if top < 969 {
        return sign * (1.0 + x);
    }
    if top >= 1033 {
        if x == f64::NEG_INFINITY {
            return 0.0;
        }
        if top == 2047 {
            return 1.0 + x;
        }
        return sign * if x < 0.0 { 0.0 } else { f64::INFINITY };
    }
    let shift = 6755399441055744.0;
    let kd = x.mul_add(f64::from_bits(0x40671547652b82fe), shift);
    let ki = kd.to_bits();
    let kd = kd - shift;
    let r = kd.mul_add(f64::from_bits(0xbf762e42fefa0000), x);
    let r = kd.mul_add(f64::from_bits(0xbd0cf79abc9e3b3a), r);
    let r = r + xtail;
    let i = 2 * (ki % 128) as usize;
    let sb = EXP_TAB[i + 1].wrapping_add(ki.wrapping_add(if negative { 0x40000 } else { 0 }) << 45);
    let tail = f64::from_bits(EXP_TAB[i]);
    let r2 = r * r;
    let tmp = (r2 * r2).mul_add(
        r.mul_add(EXP_POLY[3], EXP_POLY[2]),
        r2.mul_add(r.mul_add(EXP_POLY[1], EXP_POLY[0]), tail + r),
    );
    if top < 1032 {
        let scale = f64::from_bits(sb);
        return scale.mul_add(tmp, scale);
    }
    if ki & 0x80000000 == 0 {
        let scale = f64::from_bits(sb.wrapping_sub(1009u64 << 52));
        return f64::from_bits(2032u64 << 52) * scale.mul_add(tmp, scale);
    }
    let scale = f64::from_bits(sb.wrapping_add(1022u64 << 52));
    let product = scale * tmp;
    let mut y = scale + product;
    if y.abs() < 1.0 {
        let lo = scale - y + product;
        let hi = sign + y;
        let lo = sign - hi + y + lo;
        y = (hi + lo) - sign;
        if y == 0.0 {
            y = 0.0f64.copysign(sign);
        }
    }
    f64::MIN_POSITIVE * y
}

pub fn log(x: f64) -> f64 {
    let mut ix = x.to_bits();
    if ix.wrapping_sub(0x3fee000000000000) < 0x3ff1090000000000 - 0x3fee000000000000 {
        if x == 1.0 {
            return 0.0;
        }
        let r = x - 1.0;
        let r2 = r * r;
        let r3 = r * r2;
        let b = LOG_B;
        let y = r3
            * ((r.mul_add(b[2], b[1]) + r2 * b[3])
                + r3 * ((r.mul_add(b[5], b[4]) + r2 * b[6])
                    + r3 * ((r.mul_add(b[8], b[7]) + r2 * b[9]) + r3 * b[10])));
        let w = r * 134217728.0;
        let rhi = r + w - w;
        let rlo = r - rhi;
        let w = rhi * rhi * b[0];
        let hi = r + w;
        let lo = (b[0] * rlo).mul_add(rhi + r, r - hi + w);
        return (y + lo) + hi;
    }
    let top = (ix >> 48) as u32;
    if top.wrapping_sub(0x10) >= 0x7ff0 - 0x10 {
        if x == 0.0 {
            return f64::NEG_INFINITY;
        }
        if x == f64::INFINITY {
            return x;
        }
        if x.is_nan() {
            return x + x;
        }
        if x < 0.0 {
            return f64::from_bits(0xfff8000000000000);
        }
        ix = (x * 4503599627370496.0).to_bits().wrapping_sub(52u64 << 52);
    }
    let tmp = ix.wrapping_sub(0x3fe6000000000000);
    let i = ((tmp >> 45) % 128) as usize;
    let k = ((tmp as i64) >> 52) as f64;
    let z = f64::from_bits(ix.wrapping_sub(tmp & (0xfffu64 << 52)));
    let r = z.mul_add(LOG_TAB[2 * i], -1.0);
    let w = k.mul_add(f64::from_bits(0x3fe62e42fefa3800), LOG_TAB[2 * i + 1]);
    let hi = w + r;
    let lo = k.mul_add(f64::from_bits(0x3d2ef35793c76730), w - hi + r);
    let r2 = r * r;
    let a = LOG_A;
    let p = r2.mul_add(r.mul_add(a[4], a[3]), r.mul_add(a[2], a[1]));
    (r * r2).mul_add(p, r2.mul_add(a[0], lo)) + hi
}

pub fn log10(mut x: f64) -> f64 {
    let mut hx = x.to_bits() as i64;
    let mut k = 0i64;
    if hx < 0x0010000000000000 {
        if x == 0.0 {
            return f64::NEG_INFINITY;
        }
        if hx < 0 {
            return if x.is_nan() { x + x } else { f64::NAN };
        }
        k -= 54;
        x *= 18014398509481984.0;
        hx = x.to_bits() as i64;
    }
    if hx >= 0x7ff0000000000000 {
        return x + x;
    }
    k += (hx >> 52) - 1023;
    let i = if k < 0 { 1 } else { 0 };
    x = f64::from_bits((hx as u64 & 0x000fffffffffffff) | ((0x3ff - i) << 52));
    let y = (k + i as i64) as f64;
    let z = y * 3.69423907715893078616e-13 + 4.34294481903251816668e-01 * log(x);
    z + y * 3.01029995663611771306e-01
}
