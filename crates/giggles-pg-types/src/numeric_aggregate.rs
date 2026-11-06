//! Decimal SUM arithmetic with precomputable allocation sizes.
use super::*;
use num_bigint::Sign;
const BASE: u64 = 1_000_000_000;
fn digits(n: &BigInt) -> usize {
    ((n.bits() as usize * 30103) / 100000 + 1).max(1)
}
fn layout(a: &BigInt, x: u32, b: &BigInt, y: u32) -> (usize, usize) {
    let scale = x.max(y);
    let width = (digits(a) + (scale - x) as usize).max(digits(b) + (scale - y) as usize) + 1;
    (width.div_ceil(9) + 1, width)
}
fn normalize(v: &[u32], mut len: usize) -> usize {
    while len > 1 && v[len - 1] == 0 {
        len -= 1;
    }
    len
}
fn decimal(n: &BigInt, shift: u32, out: &mut [u32]) -> usize {
    let mut len = 1;
    for limb in n.iter_u32_digits().rev() {
        let mut carry = u64::from(limb);
        for d in &mut out[..len] {
            let next = u64::from(*d) * (1u64 << 32) + carry;
            *d = (next % BASE) as u32;
            carry = next / BASE;
        }
        while carry != 0 {
            out[len] = (carry % BASE) as u32;
            len += 1;
            carry /= BASE;
        }
    }
    let power = 10u64.pow(shift % 9);
    let mut carry = 0;
    for d in &mut out[..len] {
        let next = u64::from(*d) * power + carry;
        *d = (next % BASE) as u32;
        carry = next / BASE;
    }
    if carry != 0 {
        out[len] = carry as u32;
        len += 1;
    }
    let groups = (shift / 9) as usize;
    if groups != 0 {
        out.copy_within(0..len, groups);
        out[..groups].fill(0);
        len += groups;
    }
    normalize(out, len)
}
fn subtract(a: &mut [u32], b: &[u32], len: usize) -> usize {
    let mut borrow = 0i64;
    for i in 0..len {
        let n = i64::from(a[i]) - i64::from(b[i]) - borrow;
        if n < 0 {
            a[i] = (n + BASE as i64) as u32;
            borrow = 1;
        } else {
            a[i] = n as u32;
            borrow = 0;
        }
    }
    debug_assert_eq!(borrow, 0);
    normalize(a, len)
}
impl Numeric {
    /// Allocation-free cumulative requested-byte bound for checked_add_aggregate.
    /// Each of the two coefficient arrays and the decimal/radix output buffers is bounded.
    pub fn aggregate_scratch_bound(&self, other: &Self) -> usize {
        match (&self.0, &other.0) {
            (
                Repr::Finite {
                    coefficient: a,
                    scale: x,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: y,
                },
            ) => {
                let (groups, width) = layout(a, *x, b, *y);
                // from_radix_be consumes numeric digits directly (no text normalization).
                // Four bits/digit bounds its preallocated limbs on 32- and 64-bit targets.
                2 * groups * std::mem::size_of::<u32>()
                    + width.min(MAX_DIGITS)
                    + width.min(MAX_DIGITS).div_ceil(8) * 4
                    + 8
            }
            _ => 0,
        }
    }
    /// Every i64 coefficient has at most 19 decimal digits and scale zero.
    /// BigInt::from uses an inline digit on 64-bit targets, at most an 8-byte pair on 32-bit.
    pub fn aggregate_from_i64(n: i64) -> Self {
        Self(Repr::Finite {
            coefficient: BigInt::from(n),
            scale: 0,
        })
    }
    /// Exact bounded decimal addition, without recursive BigInt multiplication/division or formatting.
    /// Callers admit aggregate_scratch_bound before entering this operation.
    pub fn checked_add_aggregate(&self, other: &Self) -> Result<Self, Error> {
        let (a, x, b, y) = match (&self.0, &other.0) {
            (
                Repr::Finite {
                    coefficient: a,
                    scale: x,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: y,
                },
            ) => (a, *x, b, *y),
            (Repr::NaN, _)
            | (_, Repr::NaN)
            | (Repr::Infinity, Repr::NegInfinity)
            | (Repr::NegInfinity, Repr::Infinity) => return Ok(Self(Repr::NaN)),
            (Repr::Infinity, _) | (_, Repr::Infinity) => return Ok(Self(Repr::Infinity)),
            _ => return Ok(Self(Repr::NegInfinity)),
        };
        let scale = x.max(y);
        if scale > MAX_SCALE {
            return Err(Error::Limit);
        }
        let (groups, _) = layout(a, x, b, y);
        let mut left = vec![0u32; groups];
        let mut right = vec![0u32; groups];
        let al = decimal(a, scale - x, &mut left);
        let bl = decimal(b, scale - y, &mut right);
        let (result, len, sign) = if a.sign() == b.sign() {
            let len = al.max(bl);
            let mut carry = 0u64;
            for i in 0..len {
                let n = u64::from(left[i]) + u64::from(right[i]) + carry;
                left[i] = (n % BASE) as u32;
                carry = n / BASE;
            }
            let len = if carry != 0 {
                left[len] = carry as u32;
                len + 1
            } else {
                len
            };
            (&left, normalize(&left, len), a.sign())
        } else {
            let ordering = al
                .cmp(&bl)
                .then_with(|| left[..al].iter().rev().cmp(right[..bl].iter().rev()));
            if ordering == Ordering::Less {
                let len = subtract(&mut right, &left, bl);
                (&right, len, b.sign())
            } else {
                let len = subtract(&mut left, &right, al);
                (&left, len, a.sign())
            }
        };
        let zero = len == 1 && result[0] == 0;
        let width = if zero {
            1
        } else {
            (len - 1) * 9 + result[len - 1].ilog10() as usize + 1
        };
        if width > MAX_DIGITS {
            return Err(Error::Limit);
        }
        let mut output = vec![0u8; width];
        let mut at = width;
        for (i, group) in result[..len].iter().enumerate() {
            let count = if i + 1 == len { width - i * 9 } else { 9 };
            let mut n = *group;
            for _ in 0..count {
                at -= 1;
                output[at] = (n % 10) as u8;
                n /= 10;
            }
        }
        let sign = if zero { Sign::NoSign } else { sign };
        let coefficient = BigInt::from_radix_be(sign, &output, 10).ok_or(Error::Syntax)?;
        // Width and scale were checked before construction; radix digits are normalized 0..9.
        Ok(Self(Repr::Finite { coefficient, scale }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aggregate_decimal_matches_existing_arithmetic() {
        for a in -32..=32 {
            for b in -32..=32 {
                for (x, y) in [(0, 0), (1, 0), (0, 4), (7, 3)] {
                    let a = Numeric::parse(&format!("{a}e-{x}")).unwrap();
                    let b = Numeric::parse(&format!("{b}e-{y}")).unwrap();
                    let got = a.checked_add_aggregate(&b).unwrap();
                    let expected = a.checked_add(&b).unwrap();
                    assert_eq!(got.to_string(), expected.to_string());
                    assert_eq!(got.scale(), expected.scale());
                    assert_eq!(got.to_binary(), expected.to_binary());
                }
            }
        }
    }
    #[test]
    fn aggregate_specials_carry_borrow_width_and_scale() {
        for a in [
            "NaN",
            "Infinity",
            "-Infinity",
            "0.000",
            "-0.00",
            "999999999999999999999999.99999",
            "-999999999999999999999999.99999",
            "0.00001",
        ] {
            for b in [
                "NaN",
                "Infinity",
                "-Infinity",
                "0.000",
                "-0.00",
                "0.00001",
                "-0.00001",
            ] {
                let a = Numeric::parse(a).unwrap();
                let b = Numeric::parse(b).unwrap();
                let got = a.checked_add_aggregate(&b).unwrap();
                let expected = a.checked_add(&b).unwrap();
                assert_eq!(got.to_string(), expected.to_string());
                assert_eq!(got.to_binary(), expected.to_binary());
            }
        }
        for n in [i64::MIN, i64::MAX, -1, 0, 1] {
            let got = Numeric::aggregate_from_i64(n);
            let expected = Numeric::parse(&n.to_string()).unwrap();
            assert_eq!(got.to_binary(), expected.to_binary());
            assert_eq!(got.scale(), Some(0));
        }
        let max = Numeric::parse(&"9".repeat(MAX_DIGITS)).unwrap();
        let one = Numeric::aggregate_from_i64(1);
        assert_eq!(max.checked_add_aggregate(&one).unwrap_err(), Error::Limit);
        let tiny = Numeric::parse(&format!("0.{}1", "0".repeat(MAX_SCALE as usize - 1))).unwrap();
        let got = tiny.checked_add_aggregate(&tiny).unwrap();
        assert_eq!(got.scale(), Some(MAX_SCALE));
        assert!(got.to_string().ends_with('2'));
        let mixed = one.checked_add_aggregate(&tiny).unwrap();
        assert_eq!(mixed.scale(), Some(MAX_SCALE));
        assert!(mixed.to_string().starts_with("1."));
        assert!(mixed.to_string().ends_with('1'));
        let cancel = Numeric::parse(&format!("-{}", "9".repeat(MAX_DIGITS))).unwrap();
        let got = max.checked_add_aggregate(&cancel).unwrap();
        assert_eq!(got.to_string(), "0");
    }
}
#[test]
fn aggregate_admission_bound_covers_extreme_layouts() {
    let one = Numeric::aggregate_from_i64(1);
    let max = Numeric::parse(&"9".repeat(MAX_DIGITS)).unwrap();
    let tiny = Numeric::parse(&format!("0.{}1", "0".repeat(MAX_SCALE as usize - 1))).unwrap();
    for (a, b) in [(&one, &one), (&max, &one), (&max, &tiny), (&tiny, &tiny)] {
        let cost = a.aggregate_scratch_bound(b);
        assert!(cost < 64 * 1024, "{cost}");
        if let (
            Repr::Finite {
                coefficient: a,
                scale: x,
            },
            Repr::Finite {
                coefficient: b,
                scale: y,
            },
        ) = (&a.0, &b.0)
        {
            let (groups, width) = layout(a, *x, b, *y);
            assert!(groups * 4 < 64 * 1024);
            let output = width.min(MAX_DIGITS);
            assert!(cost >= 2 * groups * 4 + output + output.div_ceil(8) * 4);
        }
    }
}
#[cfg(test)]
mod allocation_probe {
    use super::*;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };
    thread_local! {
        static TRACK: Cell<bool> = const { Cell::new(false) };
        static TOTAL: Cell<usize> = const { Cell::new(0) };
        static LARGEST: Cell<usize> = const { Cell::new(0) };
    }
    struct Allocator;
    fn requested(bytes: usize) {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = TOTAL.try_with(|v| v.set(v.get() + bytes));
            let _ = LARGEST.try_with(|v| v.set(v.get().max(bytes)));
        }
    }
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            requested(layout.size());
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            requested(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
            unsafe { System.dealloc(p, layout) }
        }
        unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            requested(size);
            unsafe { System.realloc(p, layout, size) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Allocator = Allocator;
    fn observe<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
        struct Stop;
        impl Drop for Stop {
            fn drop(&mut self) {
                TRACK.set(false);
            }
        }
        TOTAL.set(0);
        LARGEST.set(0);
        TRACK.set(true);
        let stop = Stop;
        let result = f();
        drop(stop);
        (result, TOTAL.get(), LARGEST.get())
    }
    #[test]
    fn measured_requested_allocations_fit_pre_admission_bound() {
        let max = Numeric::parse(&"9".repeat(MAX_DIGITS)).unwrap();
        let tiny = Numeric::parse(&format!("0.{}1", "0".repeat(MAX_SCALE as usize - 1))).unwrap();
        let negative = Numeric::parse(&format!("-{}", "9".repeat(MAX_DIGITS))).unwrap();
        let one = Numeric::aggregate_from_i64(1);
        let zero = Numeric::aggregate_from_i64(0);
        for (a, b) in [
            (&one, &one),
            (&zero, &zero),
            (&tiny, &tiny),
            (&one, &tiny),
            (&max, &one),
            (&max, &negative),
        ] {
            let bound = a.aggregate_scratch_bound(b);
            let (_result, total, largest) = observe(|| a.checked_add_aggregate(b));
            assert!(total <= bound, "requested {total}, bound {bound}");
            assert!(largest < 64 * 1024);
        }
        for a in ["NaN", "Infinity", "-Infinity"] {
            for b in ["NaN", "Infinity", "-Infinity", "0.000"] {
                let a = Numeric::parse(a).unwrap();
                let b = Numeric::parse(b).unwrap();
                let (_, total, _) = observe(|| a.checked_add_aggregate(&b));
                assert_eq!(total, 0);
                assert_eq!(a.aggregate_scratch_bound(&b), 0);
            }
        }
        for n in [i64::MIN, i64::MAX, 0, -1, 1] {
            let (_, total, _) = observe(|| Numeric::aggregate_from_i64(n));
            assert!(total <= 8);
            #[cfg(target_pointer_width = "64")]
            assert_eq!(total, 0);
        }
    }
}

#[test]
fn aggregate_actual_pg18_text_scale_and_binary_oracle() {
    // Raw Parse0/Describe/Bind/Execute provenance: C dd68-aggregate-scalars numeric-pg-oracle.json.
    for (a, b, text, scale, binary) in [
        ("0", "0", "0", Some(0), "0000000000000000"),
        ("-0.00", "0.000", "0.000", Some(3), "0000000000000003"),
        ("1.2300", "-1.23", "0.0000", Some(4), "0000000000000004"),
        (
            "999999999999999999999999.99999",
            "0.00001",
            "1000000000000000000000000.00000",
            Some(5),
            "00010006000000050001",
        ),
        (
            "-999999999999999999999999.99999",
            "-0.00001",
            "-1000000000000000000000000.00000",
            Some(5),
            "00010006400000050001",
        ),
        (
            "1",
            "0.00000000000000000001",
            "1.00000000000000000001",
            Some(20),
            "0006000000000014000100000000000000000001",
        ),
        ("NaN", "1", "NaN", None, "00000000c0000000"),
        ("Infinity", "-Infinity", "NaN", None, "00000000c0000000"),
        ("-Infinity", "1", "-Infinity", None, "00000000f0000020"),
        (
            "9223372036854775807",
            "-9223372036854775808",
            "-1",
            Some(0),
            "00010000400000000001",
        ),
    ] {
        let a = Numeric::parse(a).unwrap();
        let b = Numeric::parse(b).unwrap();
        let got = a.checked_add_aggregate(&b).unwrap();
        assert_eq!(got.to_string(), text);
        assert_eq!(got.scale(), scale);
        let hex = got
            .to_binary()
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        assert_eq!(hex, binary);
    }
}
