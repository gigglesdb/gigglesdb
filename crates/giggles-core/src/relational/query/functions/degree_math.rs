//! Degree trigonometry adapted from PostgreSQL REL_18_0 float.c:
//! https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/float.c
//! Source SHA256: 58914ae93c06c281e581a4bb2dcf2280e4ae1619f3ceb8f801f50beca87eecd8.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Permission to use, copy, modify, and distribute this software and its
//! documentation for any purpose, without fee, and without a written agreement
//! is hereby granted, provided that the above copyright notice and this
//! paragraph and the following two paragraphs appear in all copies.
//!
//! IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
//! DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
//! LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
//! EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
//! SUCH DAMAGE.
//!
//! THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
//! BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
//! A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
//! AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
//! SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
use super::*;

// Keep the operations and their order aligned with PostgreSQL. In particular,
// converting to radians followed by a single libm call loses exact special angles.
const RAD: f64 = std::f64::consts::PI / 180.0;
fn sin_q1(m: &giggles_glibm_loader::Glibm, x: f64) -> f64 {
    if x <= 30.0 {
        (m.sin(x * RAD) / m.sin(30.0 * RAD)) / 2.0
    } else {
        cos_q1(m, 90.0 - x)
    }
}
fn cos_q1(m: &giggles_glibm_loader::Glibm, x: f64) -> f64 {
    if x <= 60.0 {
        1.0 - ((1.0 - m.cos(x * RAD)) / (1.0 - m.cos(60.0 * RAD))) / 2.0
    } else {
        sin_q1(m, 90.0 - x)
    }
}
fn asin_q1(m: &giggles_glibm_loader::Glibm, x: f64) -> f64 {
    if x <= 0.5 {
        (m.asin(x) / m.asin(0.5)) * 30.0
    } else {
        90.0 - (m.acos(x) / m.acos(0.5)) * 60.0
    }
}
fn acos_q1(m: &giggles_glibm_loader::Glibm, x: f64) -> f64 {
    if x <= 0.5 {
        90.0 - (m.asin(x) / m.asin(0.5)) * 30.0
    } else {
        (m.acos(x) / m.acos(0.5)) * 60.0
    }
}

pub(super) fn arity(name: &str) -> Option<usize> {
    Some(match name {
        "atan2d" => 2,
        "sind" | "cosd" | "tand" | "cotd" | "asind" | "acosd" | "atand" => 1,
        _ => return None,
    })
}

pub(super) fn eval(name: &str, values: &[Value]) -> Option<R<Value>> {
    let arity = arity(name)?;
    Some((|| {
        if values.len() != arity {
            return Err(SqlError::Type);
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
        let m = numeric::glibm()?;
        let out = if x.is_nan() || y.is_nan() {
            f64::NAN
        } else {
            match name {
                "asind" | "acosd" => {
                    if !(-1.0..=1.0).contains(&x) {
                        return Err(SqlError::Overflow);
                    }
                    match (name, x >= 0.0) {
                        ("asind", true) => asin_q1(m, x),
                        ("asind", false) => -asin_q1(m, -x),
                        (_, true) => acos_q1(m, x),
                        (_, false) => 90.0 + asin_q1(m, -x),
                    }
                }
                "atand" => (m.atan(x) / m.atan(1.0)) * 45.0,
                "atan2d" => (m.atan2(x, y) / m.atan(1.0)) * 45.0,
                _ => {
                    if x.is_infinite() {
                        return Err(SqlError::Overflow);
                    }
                    let mut a = x % 360.0;
                    let mut sign = 1.0;
                    if a < 0.0 {
                        a = -a;
                        if name != "cosd" {
                            sign = -sign;
                        }
                    }
                    if a > 180.0 {
                        a = 360.0 - a;
                        if name != "cosd" {
                            sign = -sign;
                        }
                    }
                    if a > 90.0 {
                        a = 180.0 - a;
                        if name != "sind" {
                            sign = -sign;
                        }
                    }
                    match name {
                        "sind" => sign * sin_q1(m, a),
                        "cosd" => sign * cos_q1(m, a),
                        "tand" | "cotd" => {
                            let ratio = if name == "tand" {
                                (sin_q1(m, a) / cos_q1(m, a)) / (sin_q1(m, 45.0) / cos_q1(m, 45.0))
                            } else {
                                (cos_q1(m, a) / sin_q1(m, a)) / (cos_q1(m, 45.0) / sin_q1(m, 45.0))
                            };
                            let out = sign * ratio;
                            if out == 0.0 { 0.0 } else { out }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        };
        Ok(Value::Float8(Float8::from_bits(out.to_bits())))
    })())
}
