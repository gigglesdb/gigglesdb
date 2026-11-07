use super::*;
/// Equal SQL floats share keys, including every NaN payload and both zeros.
pub(super) fn float_key(v: f64) -> u64 {
    let bits = if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0
    } else {
        v.to_bits()
    };
    if bits >> 63 != 0 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}
pub(super) fn key(n: &Numeric, out: &mut Vec<u8>) {
    let text = n.to_string();
    match text.as_str() {
        "-Infinity" => {
            out.push(0);
            return;
        }
        "Infinity" => {
            out.push(4);
            return;
        }
        "NaN" => {
            out.push(5);
            return;
        }
        _ => {}
    }
    let negative = text.starts_with('-');
    let raw = text.trim_start_matches('-');
    let point = raw.find('.').unwrap_or(raw.len()) as i32;
    let digits: String = raw.chars().filter(|c| *c != '.').collect();
    let leading = digits.bytes().take_while(|c| *c == b'0').count();
    let significant = digits[leading..].trim_end_matches('0');
    if significant.is_empty() {
        out.push(2);
        return;
    }
    out.push(if negative { 1 } else { 3 });
    let mut body = Vec::new();
    body.extend(((point - leading as i32) as u32 ^ (1 << 31)).to_be_bytes());
    body.extend(significant.bytes());
    body.push(0);
    if negative {
        body.iter_mut().for_each(|b| *b = !*b);
    }
    out.extend(body);
}
