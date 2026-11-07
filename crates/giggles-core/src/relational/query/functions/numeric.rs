//! Exact arithmetic composed exclusively from the frozen Numeric API.
use super::*;

// Truncate the decimal representation directly: rounding first may carry into
// an extra coefficient digit and reject a representable truncated result.
pub(super) fn trunc(n: &Numeric, scale: i32) -> R<Numeric> {
    let text = n.to_string();
    let negative = text.starts_with('-');
    let (whole, fraction) = text
        .trim_start_matches('-')
        .split_once('.')
        .unwrap_or((text.trim_start_matches('-'), ""));
    let sign = if negative { "-" } else { "" };
    let out = if scale >= 0 {
        format!(
            "{sign}{whole}.{}",
            &fraction[..fraction.len().min(scale as usize)]
        )
    } else {
        let keep = whole.len().saturating_sub(scale.unsigned_abs() as usize);
        format!("{sign}{}{}", &whole[..keep], "0".repeat(whole.len() - keep))
    };
    Numeric::parse(&out)
        .map_err(pg_error)?
        .round(scale.max(0))
        .map_err(pg_error)
}

pub(super) fn from_float(n: f64, precision: usize) -> R<Numeric> {
    if !n.is_finite() {
        return Numeric::parse(if n.is_nan() {
            "NaN"
        } else if n.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        })
        .map_err(pg_error);
    }
    let text = format!("{n:.precision$e}");
    let (mantissa, exponent) = text.split_once('e').unwrap();
    let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
    Numeric::parse(&format!("{mantissa}e{exponent}")).map_err(pg_error)
}
fn abs(n: &Numeric) -> R<Numeric> {
    Numeric::parse(n.to_string().trim_start_matches('-')).map_err(pg_error)
}
fn quotient(a: &Numeric, b: &Numeric) -> R<Numeric> {
    let q = a.div_at_scale(b, 0).map_err(pg_error)?;
    if a.scale().is_none() || b.scale().is_none() {
        return Ok(q);
    }
    let product = q.checked_mul(b).map_err(pg_error)?;
    if abs(&product)? > abs(a)? {
        q.checked_sub(
            &Numeric::parse(
                if a.to_string().starts_with('-') == b.to_string().starts_with('-') {
                    "1"
                } else {
                    "-1"
                },
            )
            .map_err(pg_error)?,
        )
        .map_err(pg_error)
    } else {
        Ok(q)
    }
}
fn remainder(a: &Numeric, b: &Numeric) -> R<Numeric> {
    if a.to_string() == "NaN" || b.to_string() == "NaN" {
        return Numeric::parse("NaN").map_err(pg_error);
    }
    if *b == Numeric::parse("0").map_err(pg_error)? {
        return Err(SqlError::DivisionByZero);
    }
    if a.scale().is_none() {
        return Numeric::parse("NaN").map_err(pg_error);
    }
    if b.scale().is_none() {
        return Ok(a.clone());
    }
    a.checked_sub(&quotient(a, b)?.checked_mul(b).map_err(pg_error)?)
        .map_err(pg_error)
}
pub(super) fn eval(name: &str, values: &[Value]) -> Option<R<Value>> {
    if let [Value::Numeric(n)] = values {
        let result = match name {
            "sqrt" => Some(n.checked_sqrt()),
            "exp" => Some(n.checked_exp()),
            "ln" => Some(n.checked_ln()),
            "log" | "log10" => Some(Numeric::checked_log(&Numeric::parse("10").unwrap(), n)),
            _ => None,
        };
        if let Some(result) = result {
            return Some(result.map(Value::Numeric).map_err(pg_error));
        }
    }
    if name == "power"
        && let [Value::Numeric(base), Value::Numeric(exponent)] = values
    {
        return Some(
            base.checked_pow(exponent)
                .map(Value::Numeric)
                .map_err(pg_error),
        );
    }
    if name == "log"
        && let [Value::Numeric(base), Value::Numeric(value)] = values
    {
        return Some(
            Numeric::checked_log(base, value)
                .map(Value::Numeric)
                .map_err(pg_error),
        );
    }
    if let Some(result) = float_eval(name, values) {
        return Some(result);
    }
    if matches!(name, "scale" | "min_scale" | "trim_scale" | "factorial") {
        return Some((|| {
            if name == "factorial" {
                let [Value::Int(n)] = values else {
                    return Err(SqlError::Type);
                };
                if *n < 0 {
                    return Err(SqlError::Type);
                }
                // PostgreSQL's result can exceed the frozen Numeric capacity.
                if *n > 32177 {
                    return Err(SqlError::Overflow);
                }
                let mut out = Numeric::parse("1").map_err(pg_error)?;
                for i in 2..=*n {
                    out = out
                        .checked_mul(&Numeric::parse(&i.to_string()).map_err(pg_error)?)
                        .map_err(pg_error)?;
                }
                return Ok(Value::Numeric(out));
            }
            let [Value::Numeric(n)] = values else {
                return Err(SqlError::Type);
            };
            let Some(scale) = n.scale() else {
                return Ok(if name == "trim_scale" {
                    Value::Numeric(n.clone())
                } else {
                    Value::Null
                });
            };
            if name == "scale" {
                return Ok(Value::Int(i64::from(scale)));
            }
            let text = n.to_string();
            let min = text
                .split_once('.')
                .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len());
            if name == "min_scale" {
                return Ok(Value::Int(min as i64));
            }
            Ok(Value::Numeric(n.round(min as i32).map_err(pg_error)?))
        })());
    }
    if !matches!(name, "div" | "mod" | "gcd" | "lcm") {
        return None;
    }
    Some((|| {
        if let [Value::Int(a), Value::Int(b)] = values {
            let out = if name == "mod" {
                if *b == 0 {
                    return Err(SqlError::DivisionByZero);
                }
                a.checked_rem(*b).unwrap_or(0)
            } else {
                let (mut x, mut y) = (a.unsigned_abs(), b.unsigned_abs());
                while y != 0 {
                    (x, y) = (y, x % y);
                }
                let v = if name == "lcm" && x != 0 {
                    u128::from(a.unsigned_abs() / x) * u128::from(b.unsigned_abs())
                } else {
                    u128::from(x)
                };
                i64::try_from(v).map_err(|_| SqlError::Overflow)?
            };
            return Ok(Value::Int(out));
        }
        let [Value::Numeric(a), Value::Numeric(b)] = values else {
            return Err(SqlError::Type);
        };
        let out = match name {
            "div" => quotient(a, b)?,
            "mod" => remainder(a, b)?,
            _ => {
                if a.scale().is_none() || b.scale().is_none() {
                    return Ok(Value::Numeric(Numeric::parse("NaN").map_err(pg_error)?));
                }
                let zero = Numeric::parse("0").map_err(pg_error)?;
                let (mut x, mut y) = (abs(a)?, abs(b)?);
                while y != zero {
                    let r = remainder(&x, &y)?;
                    x = y;
                    y = r;
                }
                let out = if name == "lcm" {
                    if x == zero {
                        zero
                    } else {
                        abs(&quotient(a, &x)?.checked_mul(b).map_err(pg_error)?)?
                    }
                } else {
                    x
                };
                out.round(a.scale().unwrap().max(b.scale().unwrap()) as i32)
                    .map_err(pg_error)?
            }
        };
        Ok(Value::Numeric(out))
    })())
}

// PostgreSQL REL_18_0 float.c domain/range rules; platform libm bytes
// remain subject to exact differential qualification.
pub(super) fn float_arity(name: &str) -> Option<usize> {
    Some(match name {
        "pi" => 0,
        "power" | "atan2" | "atan2d" => 2,
        "sqrt" | "cbrt" | "exp" | "ln" | "log" | "log10" | "radians" | "degrees" | "sin"
        | "cos" | "tan" | "cot" | "asin" | "acos" | "atan" | "sinh" | "cosh" | "tanh" | "asinh"
        | "acosh" | "atanh" | "sind" | "cosd" | "tand" | "cotd" | "asind" | "acosd" | "atand" => 1,
        _ => return None,
    })
}
fn float_eval(name: &str, values: &[Value]) -> Option<R<Value>> {
    if let Some(result) = degree_math::eval(name, values) {
        return Some(result);
    }
    let arity = float_arity(name)?;
    Some((|| {
        if arity != values.len() {
            return Err(SqlError::Type);
        }
        if name == "pi" {
            return Ok(Value::Float8(Float8::from_bits(
                std::f64::consts::PI.to_bits(),
            )));
        }
        let Value::Float8(x) = values[0] else {
            return Err(SqlError::Type);
        };
        let x = x.value();
        let y = if arity == 2 {
            let Value::Float8(y) = values[1] else {
                return Err(SqlError::Type);
            };
            y.value()
        } else {
            0.0
        };
        if (name == "sqrt" && x < 0.0)
            || (matches!(name, "ln" | "log" | "log10") && x <= 0.0)
            || (matches!(name, "asin" | "acos" | "atanh") && x.abs() > 1.0)
            || (name == "acosh" && x < 1.0)
            || (name == "power"
                && ((x == 0.0 && y < 0.0) || (x < 0.0 && y.is_finite() && y != y.trunc())))
            || (matches!(name, "sin" | "cos" | "tan" | "cot") && x.is_infinite())
        {
            return Err(SqlError::Type);
        }
        let out = match name {
            "sqrt" => x.sqrt(),
            "cbrt" => x.cbrt(),
            "exp" => x.exp(),
            "ln" => x.ln(),
            "log" | "log10" => x.log10(),
            "power" => x.powf(y),
            "radians" => x * (std::f64::consts::PI / 180.0),
            "degrees" => x / (std::f64::consts::PI / 180.0),
            "sin" => x.sin(),
            "cos" => x.cos(),
            "tan" => x.tan(),
            "cot" => 1.0 / x.tan(),
            "asin" => x.asin(),
            "acos" => x.acos(),
            "atan" => x.atan(),
            "atan2" => x.atan2(y),
            "sinh" => x.sinh(),
            "cosh" => x.cosh(),
            "tanh" => x.tanh(),
            "asinh" => x.asinh(),
            "acosh" => x.acosh(),
            "atanh" => x.atanh(),
            _ => return Err(SqlError::Unsupported),
        };
        if out.is_infinite()
            && x.is_finite()
            && y.is_finite()
            && !(name == "cot" && x == 0.0 || name == "atanh" && x.abs() == 1.0)
            || out == 0.0
                && (matches!(name, "sqrt" | "cbrt" | "radians") && x != 0.0
                    || name == "exp" && x.is_finite()
                    || name == "power" && x != 0.0 && x.is_finite() && y.is_finite())
        {
            return Err(SqlError::Overflow);
        }
        Ok(Value::Float8(Float8::from_bits(out.to_bits())))
    })())
}
