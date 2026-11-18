// IBM Accurate Mathematical Library, Rust port of Debian glibc 2.41-12+deb13u4.
// Copyright (C) 2001-2025 Free Software Foundation, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later
// This is free software, distributed without warranty; see the GNU LGPL 2.1 or later.
use super::{hi, set_hi};
const t576: f64 = f64::from_bits(0x63f0000000000000);
const tm600: f64 = f64::from_bits(0x1a70000000000000);
const tm24: f64 = f64::from_bits(0x3e70000000000000);
const big: f64 = f64::from_bits(0x4338000000000000);
const big1: f64 = f64::from_bits(0x4358000000000000);
const hp0: f64 = f64::from_bits(0x3ff921fb54442d18);
const hp1: f64 = f64::from_bits(0x3c91a62633145c07);
const mp1: f64 = f64::from_bits(0x3ff921fb58000000);
const mp2: f64 = f64::from_bits(0xbe4dde9740000000);
static toverp: [f64; 75] = [
    10680707.0_f64,
    7228996.0_f64,
    1387004.0_f64,
    2578385.0_f64,
    16069853.0_f64,
    12639074.0_f64,
    9804092.0_f64,
    4427841.0_f64,
    16666979.0_f64,
    11263675.0_f64,
    12935607.0_f64,
    2387514.0_f64,
    4345298.0_f64,
    14681673.0_f64,
    3074569.0_f64,
    13734428.0_f64,
    16653803.0_f64,
    1880361.0_f64,
    10960616.0_f64,
    8533493.0_f64,
    3062596.0_f64,
    8710556.0_f64,
    7349940.0_f64,
    6258241.0_f64,
    3772886.0_f64,
    3769171.0_f64,
    3798172.0_f64,
    8675211.0_f64,
    12450088.0_f64,
    3874808.0_f64,
    9961438.0_f64,
    366607.0_f64,
    15675153.0_f64,
    9132554.0_f64,
    7151469.0_f64,
    3571407.0_f64,
    2607881.0_f64,
    12013382.0_f64,
    4155038.0_f64,
    6285869.0_f64,
    7677882.0_f64,
    13102053.0_f64,
    15825725.0_f64,
    473591.0_f64,
    9065106.0_f64,
    15363067.0_f64,
    6271263.0_f64,
    9264392.0_f64,
    5636912.0_f64,
    4652155.0_f64,
    7056368.0_f64,
    13614112.0_f64,
    10155062.0_f64,
    1944035.0_f64,
    9527646.0_f64,
    15080200.0_f64,
    6658437.0_f64,
    6231200.0_f64,
    6832269.0_f64,
    16767104.0_f64,
    5075751.0_f64,
    3212806.0_f64,
    1398474.0_f64,
    7579849.0_f64,
    6349435.0_f64,
    12618859.0_f64,
    4703257.0_f64,
    12806093.0_f64,
    14477321.0_f64,
    2786137.0_f64,
    12875403.0_f64,
    9837734.0_f64,
    14528324.0_f64,
    13719321.0_f64,
    343717.0_f64,
];
const split: f64 = 134217729.0_f64;
pub fn branred(mut x: f64, mut a: &mut f64, mut aa: &mut f64) -> i32 {
    let mut i: i32 = 0;
    let mut k: i32 = 0;
    let mut u: f64 = 0.0;
    let mut gor: f64 = 0.0;
    let mut r: [f64; 6] = [0.0; 6];
    let mut s: f64 = 0.0;
    let mut t: f64 = 0.0;
    let mut sum: f64 = 0.0;
    let mut b: f64 = 0.0;
    let mut bb: f64 = 0.0;
    let mut sum1: f64 = 0.0;
    let mut sum2: f64 = 0.0;
    let mut b1: f64 = 0.0;
    let mut bb1: f64 = 0.0;
    let mut b2: f64 = 0.0;
    let mut bb2: f64 = 0.0;
    let mut x1: f64 = 0.0;
    let mut x2: f64 = 0.0;
    let mut t1: f64 = 0.0;
    let mut t2: f64 = 0.0;
    x *= tm600;
    t = (x * split);
    x1 = (t - (t - x));
    x2 = (x - x1);
    sum = 0_f64;
    u = x1;
    k = ((hi(u) >> 20_i32) & 2047_i32);
    k = ((k - 450_i32) / 24_i32);
    if (k < 0_i32) {
        k = 0_i32;
    }
    gor = t576;
    gor = set_hi(gor, (hi(gor) - ((k * 24_i32) << 20_i32)));
    i = 0_i32;
    while (i < 6_i32) {
        {
            r[(i) as usize] = ((x1 * toverp[(k + i) as usize]) * gor);
            gor *= tm24;
        }
        i += 1;
    }
    i = 0_i32;
    while (i < 3_i32) {
        {
            s = ((r[(i) as usize] + big) - big);
            sum += s;
            r[(i) as usize] -= s;
        }
        i += 1;
    }
    t = 0_f64;
    i = 0_i32;
    while (i < 6_i32) {
        t += r[(5_i32 - i) as usize];
        i += 1;
    }
    bb = ((((((r[(0_i32) as usize] - t) + r[(1_i32) as usize]) + r[(2_i32) as usize])
        + r[(3_i32) as usize])
        + r[(4_i32) as usize])
        + r[(5_i32) as usize]);
    s = ((t + big) - big);
    sum += s;
    t -= s;
    b = (t + bb);
    bb = ((t - b) + bb);
    s = ((sum + big1) - big1);
    sum -= s;
    b1 = b;
    bb1 = bb;
    sum1 = sum;
    sum = 0_f64;
    u = x2;
    k = ((hi(u) >> 20_i32) & 2047_i32);
    k = ((k - 450_i32) / 24_i32);
    if (k < 0_i32) {
        k = 0_i32;
    }
    gor = t576;
    gor = set_hi(gor, (hi(gor) - ((k * 24_i32) << 20_i32)));
    i = 0_i32;
    while (i < 6_i32) {
        {
            r[(i) as usize] = ((x2 * toverp[(k + i) as usize]) * gor);
            gor *= tm24;
        }
        i += 1;
    }
    i = 0_i32;
    while (i < 3_i32) {
        {
            s = ((r[(i) as usize] + big) - big);
            sum += s;
            r[(i) as usize] -= s;
        }
        i += 1;
    }
    t = 0_f64;
    i = 0_i32;
    while (i < 6_i32) {
        t += r[(5_i32 - i) as usize];
        i += 1;
    }
    bb = ((((((r[(0_i32) as usize] - t) + r[(1_i32) as usize]) + r[(2_i32) as usize])
        + r[(3_i32) as usize])
        + r[(4_i32) as usize])
        + r[(5_i32) as usize]);
    s = ((t + big) - big);
    sum += s;
    t -= s;
    b = (t + bb);
    bb = ((t - b) + bb);
    s = ((sum + big1) - big1);
    sum -= s;
    b2 = b;
    bb2 = bb;
    sum2 = sum;
    sum = (sum1 + sum2);
    b = (b1 + b2);
    bb = (if ((b1).abs() > (b2).abs()) {
        ((b1 - b) + b2)
    } else {
        ((b2 - b) + b1)
    });
    if (b > 0.5_f64) {
        b -= 1.0_f64;
        sum += 1.0_f64;
    } else {
        if (b < (-0.5_f64)) {
            b += 1.0_f64;
            sum -= 1.0_f64;
        }
    }
    s = (b + ((bb + bb1) + bb2));
    t = (((b - s) + bb) + (bb1 + bb2));
    b = (s * split);
    t1 = (b - (b - s));
    t2 = (s - t1);
    b = (s * hp0);
    bb = (((((t1 * mp1) - b) + (t1 * mp2)) + (t2 * mp1)) + (((t2 * mp2) + (s * hp1)) + (t * hp0)));
    s = (b + bb);
    t = ((b - s) + bb);
    (*a) = s;
    (*aa) = t;
    return ((sum as i32) & 3_i32);
}
