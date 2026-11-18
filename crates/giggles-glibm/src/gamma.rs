// Derived from glibc 2.41 e_lgamma_r.c, lgamma_neg.c, e_gamma_r.c,
// gamma_product.c, lgamma_product.c, e_exp2.c and math/mul_split.h.
// Copyright (C) 1997-2025 Free Software Foundation, Inc.
// e_lgamma_r.c: Copyright (C) 1993 Sun Microsystems, Inc. All rights reserved.
// Developed at SunPro, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this software is freely
// granted, provided that this notice is preserved.
// SPDX-License-Identifier: LGPL-2.1-or-later
use crate::{
    cos,
    data::EXP_TAB,
    exp,
    gamma_data::*,
    hyperbolic::{expm1, log1p},
    log, pow, sin,
};
const PI: f64 = std::f64::consts::PI;
// All floor/ceil/trunc operations below apply only to small finite arguments.
fn trunc(x: f64) -> f64 {
    (x as i64) as f64
}
fn floor(x: f64) -> f64 {
    let t = trunc(x);
    if t > x { t - 1.0 } else { t }
}
fn ceil(x: f64) -> f64 {
    -floor(-x)
}
fn split(x: f64, y: f64) -> (f64, f64) {
    let hi = x * y;
    let mut x1 = x * 134217729.0;
    let mut y1 = y * 134217729.0;
    x1 = (x - x1) + x1;
    y1 = (y - y1) + y1;
    let x2 = x - x1;
    let y2 = y - y1;
    (hi, (((x1 * y1 - hi) + x1 * y2) + x2 * y1) + x2 * y2)
}
fn sum(a: f64, b: f64) -> (f64, f64) {
    let hi = a + b;
    (hi, b - (hi - a))
}
fn mul(h1: f64, l1: f64, h2: f64, l2: f64) -> (f64, f64) {
    let (hi, lo) = split(h1, h2);
    let r = h1 * l2 + h2 * l1;
    let (hi, e) = sum(hi, r);
    (hi, lo - e)
}
fn div(h1: f64, l1: f64, h2: f64, l2: f64) -> (f64, f64) {
    let hi = h1 / h2;
    let (a, b) = split(hi, h2);
    let lo = ((h1 - a) - b) / h2;
    let r = (l1 * h2 - l2 * h1) / (h2 * h2);
    let (hi, e) = sum(hi, r);
    sum(hi, lo + e)
}
fn sinpi(x: f64) -> f64 {
    if x.abs() < 0.25 {
        return sin(PI * x);
    }
    let mut y = -x;
    let n;
    let z = floor(y);
    if z != y {
        y *= 0.5;
        y = 2.0 * (y - floor(y));
        n = (y * 4.0) as i32;
    } else {
        if y >= 9007199254740992.0 {
            return -0.0;
        }
        let z = if y < 4503599627370496.0 {
            y + 4503599627370496.0
        } else {
            z
        };
        let parity = (z.to_bits() & 1) as i32;
        y = parity as f64;
        n = parity << 2;
    }
    -match n {
        0 => sin(PI * y),
        1 | 2 => cos(PI * (0.5 - y)),
        3 | 4 => sin(PI * (1.0 - y)),
        5 | 6 => -cos(PI * (y - 1.5)),
        _ => sin(PI * (y - 2.0)),
    }
}
pub fn lgamma(x: f64) -> f64 {
    let ix = ((x.to_bits() >> 32) as u32) & 0x7fffffff;
    if ix >= 0x7ff00000 {
        return x * x;
    }
    if x == 0.0 {
        return f64::INFINITY;
    }
    if ix < 0x3b900000 {
        return -log(x.abs());
    }
    if x < 0.0 {
        if ix >= 0x43300000 {
            return f64::INFINITY;
        }
        if trunc(x) == x {
            return f64::INFINITY;
        }
        if x < -2.0 && x > -28.0 {
            return lgamma_neg(x);
        }
        let t = sinpi(x);
        if t == 0.0 {
            return f64::INFINITY;
        }
        return log(PI / (t * x).abs()) - lgamma_positive(-x);
    }
    lgamma_positive(x)
}
fn lgamma_positive(x: f64) -> f64 {
    if x == 1.0 || x == 2.0 {
        return 0.0;
    }
    let ix = (x.to_bits() >> 32) as u32;
    if ix < 0x40000000 {
        let (mut r, y, i) = if ix <= 0x3feccccc {
            let (y, i) = if ix >= 0x3fe76944 {
                (1.0 - x, 0)
            } else if ix >= 0x3fcda661 {
                (x - (TC - 1.0), 1)
            } else {
                (x, 2)
            };
            (-log(x), y, i)
        } else {
            let (y, i) = if ix >= 0x3ffbb4c3 {
                (2.0 - x, 0)
            } else if ix >= 0x3ff3b4c4 {
                (x - TC, 1)
            } else {
                (x - 1.0, 2)
            };
            (0.0, y, i)
        };
        match i {
            0 => {
                let z = y * y;
                let p1 = A0 + z * (A2 + z * (A4 + z * (A6 + z * (A8 + z * A10))));
                let p2 = z * (A1 + z * (A3 + z * (A5 + z * (A7 + z * (A9 + z * A11)))));
                let p = y * p1 + p2;
                r += p - 0.5 * y;
            }
            1 => {
                let z = y * y;
                let w = z * y;
                let p1 = T0 + w * (T3 + w * (T6 + w * (T9 + w * T12)));
                let p2 = T1 + w * (T4 + w * (T7 + w * (T10 + w * T13)));
                let p3 = T2 + w * (T5 + w * (T8 + w * (T11 + w * T14)));
                let p = z * p1 - (TT - w * (p2 + y * p3));
                r += TF + p;
            }
            _ => {
                let p1 = y * (U0 + y * (U1 + y * (U2 + y * (U3 + y * (U4 + y * U5)))));
                let p2 = 1.0 + y * (V1 + y * (V2 + y * (V3 + y * (V4 + y * V5))));
                r += -0.5 * y + p1 / p2;
            }
        }
        return r;
    }
    if ix < 0x40200000 {
        let i = x as i32;
        let y = x - i as f64;
        let p = y * (S0 + y * (S1 + y * (S2 + y * (S3 + y * (S4 + y * (S5 + y * S6))))));
        let q = 1.0 + y * (R1 + y * (R2 + y * (R3 + y * (R4 + y * (R5 + y * R6)))));
        let mut r = 0.5 * y + p / q;
        let mut z = 1.0;
        for n in (2..i).rev() {
            z *= y + n as f64;
        }
        if i >= 3 {
            r += log(z);
        }
        return r;
    }
    let t = log(x);
    if ix < 0x43900000 {
        let z = 1.0 / x;
        let y = z * z;
        let w = W0 + z * (W1 + y * (W2 + y * (W3 + y * (W4 + y * (W5 + y * W6)))));
        (x - 0.5) * (t - 1.0) + w
    } else {
        x * (t - 1.0)
    }
}
fn lg_sinpi(x: f64) -> f64 {
    if x <= 0.25 {
        sin(PI * x)
    } else {
        cos(PI * (0.5 - x))
    }
}
fn lg_cospi(x: f64) -> f64 {
    if x <= 0.25 {
        cos(PI * x)
    } else {
        sin(PI * (0.5 - x))
    }
}
fn lgamma_product(t: f64, x: f64, x_eps: f64, n: i32) -> f64 {
    let mut ret = 0.0;
    let mut eps = 0.0;
    for i in 0..n {
        let xi = x + i as f64;
        let quot = t / xi;
        let (mhi, mlo) = split(quot, xi);
        let quot_lo = (t - mhi - mlo) / xi - t * x_eps / (xi * xi);
        let (rhi, rlo) = split(ret, quot);
        let rpq = ret + quot;
        let rpq_eps = (ret - rpq) + quot;
        let nret = rpq + rhi;
        let nret_eps = (rpq - nret) + rhi;
        eps += rpq_eps + nret_eps + rlo + eps * quot + quot_lo + quot_lo * (ret + eps);
        ret = nret;
    }
    ret + eps
}
fn lgamma_neg(x: f64) -> f64 {
    let mut i = floor(-2.0 * x) as i32;
    if i & 1 == 0 && i as f64 == -2.0 * x {
        return f64::INFINITY;
    }
    let xn = (if i & 1 == 0 { -i / 2 } else { (-i - 1) / 2 }) as f64;
    i -= 4;
    let x0_hi = LGAMMA_ZEROS[2 * i as usize];
    let x0_lo = LGAMMA_ZEROS[2 * i as usize + 1];
    let xdiff = x - x0_hi - x0_lo;
    if i < 2 {
        let j = (floor(-8.0 * x) as usize) - 16;
        let xm = (-33 - 2 * j as i32) as f64 * 0.0625;
        let xa = x - xm;
        let deg = [10, 11, 12, 13, 13, 12, 11, 11][j];
        let end = [10, 22, 35, 49, 63, 76, 88, 100][j];
        let mut g = POLY_COEFF[end];
        for k in 1..=deg {
            g = g * xa + POLY_COEFF[end - k];
        }
        return log1p(g * xdiff / (x - xn));
    }
    let xd = (xn - x).abs();
    let x0d = (xn - x0_hi - x0_lo).abs();
    let log_sinpi_ratio = if x0d < xd * 0.5 {
        log(lg_sinpi(x0d) / lg_sinpi(xd))
    } else {
        let x0diff2 = (if i & 1 == 0 { xdiff } else { -xdiff }) * 0.5;
        let s = lg_sinpi(x0diff2);
        let c = lg_cospi(x0diff2);
        log1p(2.0 * s * (-s + c * (lg_cospi(xd) / lg_sinpi(xd))))
    };
    let mut y0 = 1.0 - x0_hi;
    let mut y0_eps = -x0_hi + (1.0 - y0) - x0_lo;
    let mut y = 1.0 - x;
    let mut y_eps = -x + (1.0 - y);
    let mut adj = 0.0;
    if i < 6 {
        let n = (7 - i) / 2;
        let nf = n as f64;
        let ny0 = y0 + nf;
        y0_eps += y0 - (ny0 - nf);
        y0 = ny0;
        let ny = y + nf;
        y_eps += y - (ny - nf);
        y = ny;
        adj = -log1p(lgamma_product(xdiff, y - nf, y_eps, n));
    }
    let e_hi = f64::from_bits(0x4005bf0a8b145769);
    let e_lo = f64::from_bits(0x3ca4d57ee2b1013a);
    let high = xdiff * log1p((y0 - e_hi - e_lo + y0_eps) / e_hi)
        + (y - 0.5 + y_eps) * log1p(xdiff / y)
        + adj;
    let y0r = 1.0 / y0;
    let yr = 1.0 / y;
    let y0r2 = y0r * y0r;
    let yr2 = yr * yr;
    let rdiff = -xdiff / (y * y0);
    let mut b = [0.0; 12];
    let mut d = rdiff;
    let mut e = rdiff * yr * (yr + y0r);
    b[0] = d * LGAMMA_COEFF[0];
    for j in 1..12 {
        let dn = d * y0r2 + e;
        let en = e * yr2;
        b[j] = dn * LGAMMA_COEFF[j];
        d = dn;
        e = en;
    }
    let mut low = 0.0;
    for j in (0..12).rev() {
        low += b[j];
    }
    log_sinpi_ratio + (high + low)
}
// gamma_positive only requests exp2 for a small exponent (|x| < 5).
fn exp2_small(x: f64) -> f64 {
    let shift = 6755399441055744.0 / 128.0;
    let kd = x + shift;
    let ki = kd.to_bits();
    let r = x - (kd - shift);
    let i = 2 * (ki % 128) as usize;
    let tail = f64::from_bits(EXP_TAB[i]);
    let r2 = r * r;
    let c = EXP2_POLY;
    let tmp = tail + r * c[0] + r2 * (c[1] + r * c[2]) + r2 * r2 * (c[3] + r * c[4]);
    let scale = f64::from_bits(EXP_TAB[i + 1].wrapping_add(ki << 45));
    scale + scale * tmp
}
fn gamma_product(x: f64, x_eps: f64, n: i32) -> (f64, f64) {
    use crate::extended::Ext;
    // x86_64 selects ldbl-96/gamma_product.c: round intermediate products
    // to the x87 64-bit significand, using portable integer arithmetic.
    let full = Ext::from(x).add(Ext::from(x_eps));
    let mut ret = full;
    for i in 1..n {
        ret = ret.mul(full.add(Ext::from(i as f64)));
    }
    let fret = ret.to_f64();
    let eps = ret.add(Ext::from(-fret)).div(Ext::from(fret)).to_f64();
    (fret, eps)
}
fn gamma_positive(x: f64) -> (f64, i32) {
    if x < 0.5 {
        return (exp(lgamma_positive(x + 1.0)) / x, 0);
    }
    if x <= 1.5 {
        return (exp(lgamma_positive(x)), 0);
    }
    if x < 6.5 {
        let n = ceil(x - 1.5);
        let xa = x - n;
        let (prod, eps) = gamma_product(xa, 0.0, n as i32);
        return ((exp(lgamma_positive(xa)) * prod) * (1.0 + eps), 0);
    }
    let mut eps = 0.0;
    let mut xe = 0.0;
    let mut xa = x;
    let mut prod = 1.0;
    if x < 12.0 {
        let n = ceil(12.0 - x);
        xa = x + n;
        xe = x - (xa - n);
        (prod, eps) = gamma_product(xa - n, xe, n as i32);
    }
    let xi = trunc(xa + 0.5);
    let xf = xa - xi;
    let mut exponent = ((xa.to_bits() >> 52) as i32) - 1022;
    let mut mant = f64::from_bits((xa.to_bits() & 0x000fffffffffffff) | (1022u64 << 52));
    if mant < std::f64::consts::FRAC_1_SQRT_2 {
        exponent -= 1;
        mant *= 2.0;
    }
    let adj = exponent * xi as i32;
    let (h1, l1) = split(pow(mant, xa), exp2_small(exponent as f64 * xf));
    let (h2, l2) = split(exp(-xa), (2.0 * PI / xa).sqrt());
    let (h1, l1) = mul(h1, l1, h2, l2);
    let (h1, mut l1) = div(h1, l1, prod, prod * eps);
    let mut exp_adj = xe * log(xa);
    let mut bsum = GAMMA_COEFF[5];
    let xa2 = xa * xa;
    for i in (0..5).rev() {
        bsum = bsum / xa2 + GAMMA_COEFF[i];
    }
    exp_adj += bsum / xa;
    l1 += h1 * expm1(exp_adj);
    (h1 + l1, adj)
}
fn scalbn(mut x: f64, mut n: i32) -> f64 {
    if n > 1023 {
        x *= f64::from_bits(2046u64 << 52);
        n -= 1023;
    }
    if n < -1022 {
        x *= f64::MIN_POSITIVE * 9007199254740992.0;
        n += 969;
    }
    x * f64::from_bits(((n + 1023) as u64) << 52)
}
pub fn tgamma(x: f64) -> f64 {
    if x == 0.0 {
        return 1.0 / x;
    }
    if x.is_nan() {
        return x + x;
    }
    if x == f64::NEG_INFINITY {
        return f64::NAN;
    }
    if x < 0.0 && (x.abs() >= 4503599627370496.0 || trunc(x) == x) {
        return f64::NAN;
    }
    if x >= 172.0 {
        return f64::INFINITY;
    }
    if x > 0.0 {
        let (r, n) = gamma_positive(x);
        return scalbn(r, n);
    }
    if x >= -f64::EPSILON / 4.0 {
        return 1.0 / x;
    }
    let tx = trunc(x);
    let negative = tx == 2.0 * trunc(tx / 2.0);
    if x <= -184.0 {
        return if negative { -0.0 } else { 0.0 };
    }
    let mut frac = tx - x;
    if frac > 0.5 {
        frac = 1.0 - frac;
    }
    let s = if frac <= 0.25 {
        sin(PI * frac)
    } else {
        cos(PI * (0.5 - frac))
    };
    let (g, n) = gamma_positive(-x);
    let (h1, l1) = split(s, g);
    let (h2, mut l2) = split(h1, x);
    l2 += l1 * x;
    let (h1, _) = div(PI, f64::from_bits(0x3ca1a62633145c07), h2, l2);
    let ret = scalbn(-h1, -n);
    if negative { -ret } else { ret }
}
