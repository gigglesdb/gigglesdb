//! BigInt nonlinear arithmetic with PostgreSQL REL_18_0 scale selection.
//! See docs/target-dd68-numeric-nonlinear for provenance and qualification.
//! ln_var, log_var and exp_var adapted from PostgreSQL REL_18_0 numeric.c.
//! Copyright (c) 1998-2025, PostgreSQL Global Development Group.
//! PostgreSQL license: tests/fixtures/POSTGRESQL-COPYRIGHT.
// Preserve REL18 constants exactly: changing scale estimates can change dscale.
#![allow(clippy::approx_constant)]
use super::{BigInt, Error, MAX_DIGITS, Numeric, Repr};
use num_traits::{Signed, Zero};

impl Numeric {
    /// Square root, rounded half away from zero at PostgreSQL's selected scale.
    /// Negative finite values and negative infinity return `Error::Range`.
    /// Scratch integers are bounded by twice MAX_DIGITS plus 2004 digits.
    pub fn checked_sqrt(&self) -> Result<Self, Error> {
        let (coefficient, scale) = match &self.0 {
            Repr::NaN | Repr::Infinity => return Ok(self.clone()),
            Repr::NegInfinity => return Err(Error::Range),
            Repr::Finite { coefficient, scale } => (coefficient, *scale),
        };
        if coefficient.is_negative() {
            return Err(Error::Range);
        }
        // PostgreSQL weight is the exponent of the first nonzero base-10000
        // group; normalized zero has weight zero.
        let digits = coefficient.to_str_radix(10).len();
        let weight = if coefficient.is_zero() {
            0
        } else {
            (digits as i32 - scale as i32 - 1).div_euclid(4)
        };
        let rscale = (15 - 2 * weight).max(scale as i32).clamp(0, 1000) as u32;
        // x * 10^(2*rscale) = numerator/denominator. Keeping a rational
        // avoids losing a half-way boundary when the input scale is large.
        let shift = 2 * rscale as i32 - scale as i32;
        let (numerator, denominator) = if shift >= 0 {
            (
                coefficient * BigInt::from(10u8).pow(shift as u32),
                BigInt::from(1u8),
            )
        } else {
            (coefficient.clone(), BigInt::from(10u8).pow((-shift) as u32))
        };
        // Floor(sqrt(floor(n/d))) = floor(sqrt(n/d)). BigInt's integer
        // square root terminates exactly; there is no approximate iteration cap.
        let mut root = (&numerator / &denominator).sqrt();
        // Compare x with (root + 1/2)^2 using integers, including exact ties.
        let midpoint = &root * 2u8 + 1u8;
        if numerator * 4u8 >= &denominator * &midpoint * &midpoint {
            root += 1u8;
        }
        if root.to_str_radix(10).len() > MAX_DIGITS {
            return Err(Error::Limit);
        }
        Self::finite(root, rscale)
    }
}

// Decimal scratch arithmetic for REL18 ln_var/log_var. Signed scales allow
// range reduction to round before the decimal point. Scratch exponents are
// bounded by validated inputs (<= 16384 digits) and output scale (<= 1000).
#[derive(Clone)]
struct Decimal {
    c: BigInt,
    s: i32,
}
impl Decimal {
    fn integer(n: i32) -> Self {
        Self { c: n.into(), s: 0 }
    }
    fn weight(&self) -> i32 {
        if self.c.is_zero() {
            0
        } else {
            (self.c.abs().to_str_radix(10).len() as i32 - self.s - 1).div_euclid(4)
        }
    }
    fn ten(n: i32) -> BigInt {
        assert!((0..=60000).contains(&n));
        BigInt::from(10u8).pow(n as u32)
    }
    fn at(&self, s: i32) -> BigInt {
        if s >= self.s {
            &self.c * Self::ten(s - self.s)
        } else {
            super::round_quotient(self.c.clone(), Self::ten(self.s - s))
        }
    }
    fn add(&self, b: &Self) -> Self {
        let s = self.s.max(b.s);
        Self {
            c: self.at(s) + b.at(s),
            s,
        }
    }
    fn neg(&self) -> Self {
        Self {
            c: -&self.c,
            s: self.s,
        }
    }
    fn mul(&self, b: &Self, s: i32) -> Self {
        let d = Self {
            c: &self.c * &b.c,
            s: self.s + b.s,
        };
        Self { c: d.at(s), s }
    }
    fn div(&self, b: &Self, s: i32) -> Self {
        let shift = s + b.s - self.s;
        let (n, d) = if shift >= 0 {
            (&self.c * Self::ten(shift), b.c.clone())
        } else {
            (self.c.clone(), &b.c * Self::ten(-shift))
        };
        Self {
            c: super::round_quotient(n, d),
            s,
        }
    }
    fn sqrt(&self, s: i32) -> Self {
        let shift = 2 * s - self.s;
        let (n, d) = if shift >= 0 {
            (&self.c * Self::ten(shift), BigInt::from(1))
        } else {
            (self.c.clone(), Self::ten(-shift))
        };
        let mut c = (&n / &d).sqrt();
        let mid = &c * 2 + 1;
        if n * 4 >= d * &mid * &mid {
            c += 1;
        }
        Self { c, s }
    }
    fn cmp(&self, b: &Self) -> std::cmp::Ordering {
        let s = self.s.max(b.s);
        self.at(s).cmp(&b.at(s))
    }
    fn ln_weight(&self) -> i32 {
        let one = Self::integer(1);
        if self.cmp(&Self { c: 9.into(), s: 1 }).is_ge()
            && self.cmp(&Self { c: 11.into(), s: 1 }).is_le()
        {
            let x = self.add(&one.neg());
            if x.c.is_zero() {
                0
            } else {
                x.c.abs().to_str_radix(10).len() as i32 - x.s - 1
            }
        } else {
            // REL18 uses only the first two base-10000 groups for this
            // scale estimate. Floating point never computes result digits.
            let digits = self.c.abs().to_str_radix(10);
            let dw = digits.len() as i32 - self.s - 1;
            let first = (dw.rem_euclid(4) + 1) as usize;
            let take = (first + 4).min(digits.len());
            let leading: f64 = digits[..take].parse().unwrap();
            let ln = leading.ln()
                + (digits.len() as i32 - self.s - take as i32) as f64 * 2.302_585_092_994_046;
            ln.abs().log10() as i32
        }
    }
    fn ln(&self, s: i32) -> Self {
        let mut x = self.clone();
        let mut fact = Self::integer(2);
        let mut roots = 0;
        let low = Self { c: 9.into(), s: 1 };
        let high = Self { c: 11.into(), s: 1 };
        while x.cmp(&low).is_le() || x.cmp(&high).is_ge() {
            x = x.sqrt(s - x.weight() * 4 / 2 + 8);
            fact = fact.mul(&Self::integer(2), 0);
            roots += 1;
        }
        let local = s + ((roots + 1) as f64 * 0.301_029_995_663_981) as i32 + 8;
        let mut result = x
            .add(&Self::integer(-1))
            .div(&x.add(&Self::integer(1)), local);
        let mut xx = result.clone();
        x = result.mul(&result, local);
        let mut n = 1;
        loop {
            n += 2;
            xx = xx.mul(&x, local);
            let elem = xx.div(&Self::integer(n), local);
            if elem.c.is_zero() {
                break;
            }
            result = result.add(&elem);
            if elem.weight() < result.weight() - local * 2 / 4 {
                break;
            }
        }
        result.mul(&fact, s)
    }
    fn finish(&self) -> Result<Numeric, Error> {
        Numeric::finite(self.at(self.s.max(0)), self.s.max(0) as u32)
    }
}
impl Numeric {
    /// Natural logarithm with PostgreSQL REL18 scale selection and guard digits.
    /// Zero and negative inputs return `Error::Range`.
    pub fn checked_ln(&self) -> Result<Self, Error> {
        let x = match &self.0 {
            Repr::NaN | Repr::Infinity => return Ok(self.clone()),
            Repr::NegInfinity => return Err(Error::Range),
            Repr::Finite { coefficient, scale } => Decimal {
                c: coefficient.clone(),
                s: *scale as i32,
            },
        };
        if x.c <= BigInt::zero() {
            return Err(Error::Range);
        }
        let s = (16 - x.ln_weight()).max(x.s).clamp(0, 1000);
        x.ln(s).finish()
    }
    /// Logarithm of `value` in `base`, in PostgreSQL argument order.
    pub fn checked_log(base: &Self, value: &Self) -> Result<Self, Error> {
        if matches!(base.0, Repr::NaN) || matches!(value.0, Repr::NaN) {
            return Ok(Self(Repr::NaN));
        }
        let positive = |x: &Self| {
            matches!(&x.0, Repr::Infinity)
                || matches!(&x.0,Repr::Finite{coefficient,..} if coefficient>&BigInt::zero())
        };
        if !positive(base) || !positive(value) {
            return Err(Error::Range);
        }
        match (&base.0, &value.0) {
            (Repr::Infinity, Repr::Infinity) => Ok(Self(Repr::NaN)),
            (Repr::Infinity, _) => Self::finite(BigInt::zero(), 0),
            (_, Repr::Infinity) => Ok(Self(Repr::Infinity)),
            (
                Repr::Finite {
                    coefficient: a,
                    scale: sa,
                },
                Repr::Finite {
                    coefficient: b,
                    scale: sb,
                },
            ) => {
                let a = Decimal {
                    c: a.clone(),
                    s: *sa as i32,
                };
                let b = Decimal {
                    c: b.clone(),
                    s: *sb as i32,
                };
                let aw = a.ln_weight();
                let bw = b.ln_weight();
                let dw = bw - aw;
                let s = (16 - dw).max(a.s).max(b.s).clamp(0, 1000);
                let la = a.ln((s + dw - aw + 8).max(0));
                let lb = b.ln((s + dw - bw + 8).max(0));
                if la.c.is_zero() {
                    return Err(Error::DivisionByZero);
                }
                lb.div(&la, s).finish()
            }
            _ => unreachable!(),
        }
    }
}
impl Numeric {
    /// Exponential with REL18 scale selection and a BigInt Taylor series.
    pub fn checked_exp(&self) -> Result<Self, Error> {
        let x = match &self.0 {
            Repr::NaN | Repr::Infinity => return Ok(self.clone()),
            Repr::NegInfinity => return Self::finite(BigInt::zero(), 0),
            Repr::Finite { coefficient, scale } => Decimal {
                c: coefficient.clone(),
                s: *scale as i32,
            },
        };
        // Floating point is used only for PostgreSQL's scale/range estimates.
        let val = self.to_string().parse::<f64>().unwrap();
        let estimate = (val * 0.434_294_481_903_252).clamp(-2000.0, 2000.0) as i32;
        let s = (16 - estimate).max(x.s).clamp(0, 1000);
        if val.abs() >= 6000.0 {
            return if val > 0.0 {
                Err(Error::Range)
            } else {
                Self::finite(BigInt::zero(), s as u32)
            };
        }
        x.exp(s)?.finish()
    }
}

impl Decimal {
    fn as_f64(&self) -> f64 {
        // Match numericvar_to_double_no_overflow: correctly parse the full
        // decimal value for estimates, never use floating point for digits.
        format!("{}e{}", self.c, -self.s).parse().unwrap()
    }

    fn exp(&self, s: i32) -> Result<Self, Error> {
        let mut x = self.clone();
        let mut val = x.as_f64();
        if val.abs() >= 6000.0 {
            return if val > 0.0 {
                Err(Error::Range)
            } else {
                Ok(Self { c: 0.into(), s })
            };
        }
        let dw = (val * 0.434_294_481_903_252) as i32;
        let mut halves = 0;
        while val.abs() > 0.01 {
            halves += 1;
            val /= 2.0;
        }
        if halves > 0 {
            x = x.div(&Decimal::integer(1 << halves), x.s + halves);
        }
        let sig = (1 + dw + s + (halves as f64 * 0.301_029_995_663_981) as i32).max(0) + 8;
        let mut local = sig - 1;
        let mut result = Decimal::integer(1).add(&x);
        let mut elem = x.mul(&x, local).div(&Decimal::integer(2), local);
        let mut n = 2;
        while !elem.c.is_zero() {
            result = result.add(&elem);
            n += 1;
            elem = elem.mul(&x, local).div(&Decimal::integer(n), local);
        }
        for _ in 0..halves {
            local = (sig - result.weight() * 8).max(0);
            result = result.mul(&result, local);
        }
        Ok(Self { c: result.at(s), s })
    }
}

// numeric_power, power_var and power_var_int: PostgreSQL REL_18_0 numeric.c.
// Source SHA256 C0E106FC5C0F19D6B1F31050754D691C8E7A253953DC2041392BB6E915B9853A.
impl Numeric {
    /// Power with REL18 integer/fractional paths, scales and guard digits.
    /// Domain/overflow errors are Range; representation ceilings return Limit.
    pub fn checked_pow(&self, exponent: &Self) -> Result<Self, Error> {
        let one = Self::finite(1.into(), 0)?;
        let zero = Self::finite(0.into(), 0)?;
        if matches!(self.0, Repr::NaN) {
            return Ok(if exponent == &zero { one } else { self.clone() });
        }
        if matches!(exponent.0, Repr::NaN) {
            return Ok(if self == &one { one } else { exponent.clone() });
        }
        let a = match &self.0 {
            Repr::Finite { coefficient, scale } => Some(Decimal {
                c: coefficient.clone(),
                s: *scale as i32,
            }),
            _ => None,
        };
        let b = match &exponent.0 {
            Repr::Finite { coefficient, scale } => Some(Decimal {
                c: coefficient.clone(),
                s: *scale as i32,
            }),
            _ => None,
        };
        let integral = b
            .as_ref()
            .is_none_or(|b| (&b.c % Decimal::ten(b.s)).is_zero());
        let odd = b
            .as_ref()
            .is_some_and(|b| integral && !((&b.c / Decimal::ten(b.s)) % 2u8).is_zero());
        let negative = self < &zero;
        if (self == &zero && exponent < &zero) || (negative && !integral) {
            return Err(Error::Range);
        }
        if a.is_none() || b.is_none() {
            if self == &one || exponent == &zero {
                return Ok(one);
            }
            if self == &zero {
                return Ok(zero);
            }
            if b.is_none() {
                let abs_cmp = a.as_ref().map_or(std::cmp::Ordering::Greater, |a| {
                    Decimal {
                        c: a.c.abs(),
                        s: a.s,
                    }
                    .cmp(&Decimal::integer(1))
                });
                if abs_cmp.is_eq() {
                    return Ok(one);
                }
                return Ok(if abs_cmp.is_gt() == (exponent > &zero) {
                    Self(Repr::Infinity)
                } else {
                    zero
                });
            }
            return Ok(if exponent < &zero {
                zero
            } else if negative && odd {
                Self(Repr::NegInfinity)
            } else {
                Self(Repr::Infinity)
            });
        }
        let a = a.unwrap();
        let b = b.unwrap();
        if integral && let Ok(n) = (&b.c / Decimal::ten(b.s)).to_string().parse::<i32>() {
            return a.pow_int(n, b.s)?.finish();
        }
        if a.c.is_zero() {
            return Self::finite(0.into(), 16);
        }
        let a = Decimal {
            c: a.c.abs(),
            s: a.s,
        };
        let lw = a.ln_weight();
        let local = (8 - lw).max(0);
        let estimate = a.ln(local).mul(&b, local).as_f64();
        if estimate.abs() > 6020.0 {
            return if estimate > 0.0 {
                Err(Error::Range)
            } else {
                Self::finite(0.into(), 1000)
            };
        }
        let weight = (estimate * 0.434_294_481_903_252) as i32;
        let s = (16 - weight).max(a.s).max(b.s).clamp(0, 1000);
        let local = ((s + weight).max(0) - lw + 8).max(0);
        let mut result = a.ln(local).mul(&b, local).exp(s)?;
        if negative && odd {
            result.c = -result.c;
        }
        result.finish()
    }
}
impl Decimal {
    fn pow_int(&self, n: i32, exp_scale: i32) -> Result<Self, Error> {
        // REL18 estimate consumes at most the first four base-10000 groups.
        let digits = self.c.abs().to_string();
        let dw = digits.len() as i32 - self.s - 1;
        let first = dw.rem_euclid(4) + 1;
        let take = (first + 12).min(digits.len() as i32) as usize;
        let f = if self.c.is_zero() {
            0.0
        } else {
            let lead: f64 = digits[..take].parse().unwrap();
            n as f64 * (lead.log10() + (digits.len() as i32 - self.s - take as i32) as f64)
        };
        if f > 131072.0 {
            return Err(Error::Range);
        }
        if f + 1.0 < -1000.0 {
            return Ok(Self {
                c: 0.into(),
                s: 1000,
            });
        }
        let s = (16 - f as i32).max(self.s).max(exp_scale).clamp(0, 1000);
        let one = Self::integer(1);
        match n {
            0 => return Ok(Self { c: Self::ten(s), s }),
            1 => return Ok(Self { c: self.at(s), s }),
            -1 => return Ok(one.div(self, s)),
            2 => return Ok(self.mul(self, s)),
            _ => (),
        }
        if self.c.is_zero() {
            return Ok(Self { c: 0.into(), s });
        }
        // Prevent oversized intermediates before multiplying. This is a
        // representation limit, not a claim of PostgreSQL range equivalence.
        let sig = 1 + s + f as i32 + (n.unsigned_abs() as f64).ln() as i32 + 8;
        if sig > 2 * MAX_DIGITS as i32 {
            return Err(Error::Limit);
        }
        let mut mask = n.unsigned_abs();
        let mut base = self.clone();
        let mut result = if mask & 1 != 0 {
            self.clone()
        } else {
            one.clone()
        };
        mask >>= 1;
        while mask > 0 {
            let local = (sig - 8 * base.weight()).min(2 * base.s).max(0);
            if base.c.bits() > 220000 {
                return Err(Error::Limit);
            }
            base = base.mul(&base, local);
            if mask & 1 != 0 {
                let local = (sig - 4 * (base.weight() + result.weight()))
                    .min(base.s + result.s)
                    .max(0);
                if base.c.bits() + result.c.bits() > 440000 {
                    return Err(Error::Limit);
                }
                result = base.mul(&result, local);
            }
            if base.weight() > 32767 || result.weight() > 32767 {
                return if n < 0 {
                    Ok(Self { c: 0.into(), s })
                } else {
                    Err(Error::Range)
                };
            }
            mask >>= 1;
        }
        Ok(if n < 0 {
            one.div(&result, s)
        } else {
            Self { c: result.at(s), s }
        })
    }
}
