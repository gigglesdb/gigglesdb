// Derived from glibc 2.41 s_erf.c.
// Copyright (C) 1993 Sun Microsystems, Inc. All rights reserved.
// Developed at SunPro, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this software is freely
// granted, provided that this notice is preserved.
// Modified by Naohiko Shimizu/Tokai University, Japan 1997/08/25,
// for performance improvement on pipelined processors.
use crate::{erf_data::*, exp};
const ERX: f64 = 8.45062911510467529297e-01;
const EFX: f64 = 1.28379167095512586316e-01;
fn small(x: f64) -> f64 {
    let z = x * x;
    let z2 = z * z;
    let z4 = z2 * z2;
    let r1 = PP[0] + z * PP[1];
    let r2 = PP[2] + z * PP[3];
    let s1 = 1.0 + z * QQ[1];
    let s2 = QQ[2] + z * QQ[3];
    let s3 = QQ[4] + z * QQ[5];
    (r1 + z2 * r2 + z4 * PP[4]) / (s1 + z2 * s2 + z4 * s3)
}
fn near(x: f64) -> f64 {
    let s = x.abs() - 1.0;
    let s2 = s * s;
    let s4 = s2 * s2;
    let s6 = s4 * s2;
    let p1 = PA[0] + s * PA[1];
    let p2 = PA[2] + s * PA[3];
    let p3 = PA[4] + s * PA[5];
    let q1 = 1.0 + s * QA[1];
    let q2 = QA[2] + s * QA[3];
    let q3 = QA[4] + s * QA[5];
    (p1 + s2 * p2 + s4 * p3 + s6 * PA[6]) / (q1 + s2 * q2 + s4 * q3 + s6 * QA[6])
}
fn tail(x: f64, threshold: u32) -> f64 {
    let s = 1.0 / (x * x);
    let s2 = s * s;
    let s4 = s2 * s2;
    let s6 = s4 * s2;
    let (r, t) = if ((x.to_bits() >> 32) as u32) < threshold {
        let r1 = RA[0] + s * RA[1];
        let r2 = RA[2] + s * RA[3];
        let r3 = RA[4] + s * RA[5];
        let r4 = RA[6] + s * RA[7];
        let t1 = 1.0 + s * SA[1];
        let t2 = SA[2] + s * SA[3];
        let t3 = SA[4] + s * SA[5];
        let t4 = SA[6] + s * SA[7];
        (
            r1 + s2 * r2 + s4 * r3 + s6 * r4,
            t1 + s2 * t2 + s4 * t3 + s6 * t4 + (s4 * s4) * SA[8],
        )
    } else {
        let r1 = RB[0] + s * RB[1];
        let r2 = RB[2] + s * RB[3];
        let r3 = RB[4] + s * RB[5];
        let t1 = 1.0 + s * SB[1];
        let t2 = SB[2] + s * SB[3];
        let t3 = SB[4] + s * SB[5];
        let t4 = SB[6] + s * SB[7];
        (
            r1 + s2 * r2 + s4 * r3 + s6 * RB[6],
            t1 + s2 * t2 + s4 * t3 + s6 * t4,
        )
    };
    let z = f64::from_bits(x.to_bits() & 0xffffffff00000000);
    exp(-z * z - 0.5625) * exp((z - x) * (z + x) + r / t)
}
pub fn erf(x: f64) -> f64 {
    let ix = ((x.to_bits() >> 32) as u32) & 0x7fffffff;
    if ix >= 0x7ff00000 {
        return 1.0f64.copysign(x) + 1.0 / x;
    }
    if ix < 0x3feb0000 {
        if ix < 0x3e300000 {
            if ix < 0x00800000 {
                return 0.0625 * (16.0 * x + (16.0 * EFX) * x);
            }
            return x + EFX * x;
        }
        return x + x * small(x);
    }
    if ix < 0x3ff40000 {
        let p = near(x);
        return if x.is_sign_negative() {
            -ERX - p
        } else {
            ERX + p
        };
    }
    if ix >= 0x40180000 {
        return 1.0f64.copysign(x);
    }
    let a = x.abs();
    let r = tail(a, 0x4006db6e);
    if x.is_sign_negative() {
        r / a - 1.0
    } else {
        1.0 - r / a
    }
}
pub fn erfc(x: f64) -> f64 {
    let hx = (x.to_bits() >> 32) as i32;
    let ix = hx & 0x7fffffff;
    if ix >= 0x7ff00000 {
        return (if hx < 0 { 2.0 } else { 0.0 }) + 1.0 / x;
    }
    if ix < 0x3feb0000 {
        if ix < 0x3c700000 {
            return 1.0 - x;
        }
        let y = small(x);
        return if hx < 0x3fd00000 {
            1.0 - (x + x * y)
        } else {
            0.5 - (x * y + (x - 0.5))
        };
    }
    if ix < 0x3ff40000 {
        let p = near(x);
        return if hx >= 0 {
            (1.0 - ERX) - p
        } else {
            1.0 + (ERX + p)
        };
    }
    if ix < 0x403c0000 {
        if hx < 0 && ix >= 0x40180000 {
            return 2.0;
        }
        let a = x.abs();
        let r = tail(a, 0x4006db6d);
        return if hx > 0 { r / a } else { 2.0 - r / a };
    }
    if hx > 0 { 0.0 } else { 2.0 }
}
