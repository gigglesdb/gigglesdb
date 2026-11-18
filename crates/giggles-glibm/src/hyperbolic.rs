// Derived from glibc 2.41 s_log1p.c, s_expm1.c, s_asinh.c, e_acosh.c,
// e_atanh.c, e_sinh.c, e_cosh.c and s_tanh.c.
// Copyright (C) 2011-2025 Free Software Foundation, Inc.
// Copyright (C) 1993 Sun Microsystems, Inc. All rights reserved.
// Developed at SunPro, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this software is freely
// granted, provided that this notice is preserved.
// SPDX-License-Identifier: LGPL-2.1-or-later
use crate::{exp, log};
fn high(x: f64) -> u32 {
    (x.to_bits() >> 32) as u32
}
fn set_high(x: f64, h: u32) -> f64 {
    f64::from_bits((x.to_bits() & 0xffffffff) | ((h as u64) << 32))
}
fn scale(x: f64, k: i32) -> f64 {
    f64::from_bits(x.to_bits().wrapping_add((k as u64) << 52))
}
const LN2_HI: f64 = 6.93147180369123816490e-01;
const LN2_LO: f64 = 1.90821492927058770002e-10;
pub(crate) fn log1p(x: f64) -> f64 {
    let hx = high(x) as i32;
    let ax = hx & 0x7fffffff;
    let mut k = 1;
    let mut f = 0.0;
    let mut hu = 1;
    let mut c = 0.0;
    if hx < 0x3fda827a {
        if ax >= 0x3ff00000 {
            return if x == -1.0 {
                f64::NEG_INFINITY
            } else if x.is_nan() {
                x + x
            } else {
                f64::from_bits(0xfff8000000000000)
            };
        }
        if ax < 0x3e200000 {
            return if ax < 0x3c900000 {
                x
            } else {
                (-x).mul_add(x * 0.5, x)
            };
        }
        if hx > 0 || hx <= 0xbfd2bec3u32 as i32 {
            k = 0;
            f = x;
        }
    } else if hx >= 0x7ff00000 {
        return x + x;
    }
    if k != 0 {
        let mut u;
        if hx < 0x43400000 {
            u = 1.0 + x;
            hu = high(u);
            k = (hu >> 20) as i32 - 1023;
            c = if k > 0 { 1.0 - (u - x) } else { x - (u - 1.0) };
            c /= u;
        } else {
            u = x;
            hu = high(u);
            k = (hu >> 20) as i32 - 1023;
        }
        hu &= 0x000fffff;
        if hu < 0x6a09e {
            u = set_high(u, hu | 0x3ff00000);
        } else {
            k += 1;
            u = set_high(u, hu | 0x3fe00000);
            hu = (0x00100000 - hu) >> 2;
        }
        f = u - 1.0;
    }
    let hfsq = 0.5 * f * f;
    let kd = k as f64;
    if hu == 0 {
        if f == 0.0 {
            return if k == 0 {
                0.0
            } else {
                kd.mul_add(LN2_HI, kd.mul_add(LN2_LO, c))
            };
        }
        let r = hfsq * (-0.66666666666666666f64).mul_add(f, 1.0);
        return if k == 0 {
            f - r
        } else {
            kd * LN2_HI - ((r - kd.mul_add(LN2_LO, c)) - f)
        };
    }
    let s = f / (2.0 + f);
    let z = s * s;
    let z2 = z * z;
    let z4 = z2 * z2;
    let z6 = z4 * z2;
    let r2 = z.mul_add(2.857142874366239149e-01, 3.999999999940941908e-01);
    let r3 = z.mul_add(1.818357216161805012e-01, 2.222219843214978396e-01);
    let r4 = z.mul_add(1.479819860511658591e-01, 1.531383769920937332e-01);
    let r = z6.mul_add(
        r4,
        z4.mul_add(r3, z.mul_add(6.666666666666735130e-01, z2 * r2)),
    );
    if k == 0 {
        f - (hfsq - s * (hfsq + r))
    } else {
        kd.mul_add(
            LN2_HI,
            -((hfsq - (s * (hfsq + r) + kd.mul_add(LN2_LO, c))) - f),
        )
    }
}
pub(crate) fn expm1(mut x: f64) -> f64 {
    let hx = high(x) & 0x7fffffff;
    let neg = x.is_sign_negative();
    if hx >= 0x4043687a {
        if hx >= 0x7ff00000 {
            return if x.is_nan() {
                x + x
            } else if neg {
                -1.0
            } else {
                x
            };
        }
        if x > 7.09782712893383973096e2 {
            return f64::INFINITY;
        }
        if neg {
            return -1.0;
        }
    }
    let mut c = 0.0;
    let k;
    if hx > 0x3fd62e42 {
        let hi;
        let lo;
        if hx < 0x3ff0a2b2 {
            if !neg {
                hi = x - LN2_HI;
                lo = LN2_LO;
                k = 1;
            } else {
                hi = x + LN2_HI;
                lo = -LN2_LO;
                k = -1;
            }
        } else {
            k = x.mul_add(1.44269504088896338700, if neg { -0.5 } else { 0.5 }) as i32;
            hi = (-(k as f64)).mul_add(LN2_HI, x);
            lo = k as f64 * LN2_LO;
        }
        x = hi - lo;
        c = (hi - x) - lo;
    } else if hx < 0x3c900000 {
        return x;
    } else {
        k = 0;
    }
    let hfx = 0.5 * x;
    let hxs = x * hfx;
    let h2 = hxs * hxs;
    let h4 = h2 * h2;
    let r1 = hxs.mul_add(-3.33333333333331316428e-2, 1.0);
    let r2 = hxs.mul_add(-7.93650757867487942473e-5, 1.58730158725481460165e-3);
    let r3 = hxs.mul_add(-2.01099218183624371326e-7, 4.00821782732936239552e-6);
    let r1 = h4.mul_add(r3, h2.mul_add(r2, r1));
    let t = (-r1).mul_add(hfx, 3.0);
    let mut e = hxs * ((r1 - t) / (-x).mul_add(t, 6.0));
    if k == 0 {
        return x - x.mul_add(e, -hxs);
    }
    e = x.mul_add(e - c, -c);
    e -= hxs;
    if k == -1 {
        return 0.5 * (x - e) - 0.5;
    }
    if k == 1 {
        return if x < -0.25 {
            -2.0 * (e - (x + 0.5))
        } else {
            2.0f64.mul_add(x - e, 1.0)
        };
    }
    if k <= -2 || k > 56 {
        return scale(1.0 - (e - x), k) - 1.0;
    }
    if k < 20 {
        let t = set_high(1.0, 0x3ff00000 - (0x200000 >> k));
        scale(t - (e - x), k)
    } else {
        let t = set_high(1.0, ((0x3ff - k) << 20) as u32);
        scale((x - (e + t)) + 1.0, k)
    }
}
pub fn sinh(x: f64) -> f64 {
    let ix = high(x) & 0x7fffffff;
    if ix >= 0x7ff00000 {
        return x + x;
    }
    let h = 0.5f64.copysign(x);
    if ix < 0x40360000 {
        if ix < 0x3e300000 {
            return x;
        }
        let t = expm1(x.abs());
        return if ix < 0x3ff00000 {
            h * (2.0 * t - t * t / (t + 1.0))
        } else {
            h * (t + t / (t + 1.0))
        };
    }
    if ix < 0x40862e42 {
        return h * exp(x.abs());
    }
    if x.to_bits() & 0x7fffffffffffffff <= 0x408633ce8fb9f87d {
        let w = exp(0.5 * x.abs());
        return (h * w) * w;
    }
    x * 1.0e307
}
pub fn cosh(x: f64) -> f64 {
    let ix = high(x) & 0x7fffffff;
    if ix < 0x40360000 {
        if ix < 0x3fd62e43 {
            if ix < 0x3c800000 {
                return 1.0;
            }
            let t = expm1(x.abs());
            let w = 1.0 + t;
            return 1.0 + (t * t) / (w + w);
        }
        let t = exp(x.abs());
        return 0.5 * t + 0.5 / t;
    }
    if ix < 0x40862e42 {
        return 0.5 * exp(x.abs());
    }
    if x.to_bits() & 0x7fffffffffffffff <= 0x408633ce8fb9f87d {
        let w = exp(0.5 * x.abs());
        return (0.5 * w) * w;
    }
    if ix >= 0x7ff00000 {
        return x * x;
    }
    f64::INFINITY
}
pub fn tanh(x: f64) -> f64 {
    let ix = high(x) & 0x7fffffff;
    if ix >= 0x7ff00000 {
        return if x.is_sign_negative() {
            1.0 / x - 1.0
        } else {
            1.0 / x + 1.0
        };
    }
    if x == 0.0 {
        return x;
    }
    if ix < 0x3c800000 {
        return x * (1.0 + x);
    }
    let z = if ix >= 0x40360000 {
        1.0
    } else if ix >= 0x3ff00000 {
        let t = expm1(2.0 * x.abs());
        1.0 - 2.0 / (t + 2.0)
    } else {
        let t = expm1(-2.0 * x.abs());
        -t / (t + 2.0)
    };
    z.copysign(x)
}
pub fn asinh(x: f64) -> f64 {
    let ix = high(x) & 0x7fffffff;
    if ix < 0x3e300000 {
        return x;
    }
    let a = x.abs();
    let w = if ix > 0x41b00000 {
        if ix >= 0x7ff00000 {
            return x + x;
        }
        log(a) + std::f64::consts::LN_2
    } else if ix > 0x40000000 {
        log(2.0 * a + 1.0 / ((a * a + 1.0).sqrt() + a))
    } else {
        let t = a * a;
        log1p(a + t / (1.0 + (1.0 + t).sqrt()))
    };
    w.copysign(x)
}
pub fn acosh(x: f64) -> f64 {
    if x.is_nan() {
        return x + x;
    }
    if x < 1.0 {
        return f64::from_bits(0xfff8000000000000);
    }
    if x == 1.0 {
        return 0.0;
    }
    let hx = x.to_bits();
    if hx > 0x4000000000000000 {
        if hx >= 0x41b0000000000000 {
            return if x.is_infinite() {
                x + x
            } else {
                log(x) + std::f64::consts::LN_2
            };
        }
        return log(2.0 * x - 1.0 / (x + (x * x - 1.0).sqrt()));
    }
    let t = x - 1.0;
    log1p(t + (2.0 * t + t * t).sqrt())
}
pub fn atanh(x: f64) -> f64 {
    let a = x.abs();
    if x.is_nan() {
        return x + x;
    }
    if a > 1.0 {
        return f64::from_bits(0xfff8000000000000);
    }
    if a == 1.0 {
        return f64::INFINITY.copysign(x);
    }
    if a < f64::from_bits(0x3e30000000000000) {
        return x;
    }
    let t = if a < 0.5 {
        let t = a + a;
        0.5 * log1p(t + t * a / (1.0 - a))
    } else {
        0.5 * log1p((a + a) / (1.0 - a))
    };
    t.copysign(x)
}
