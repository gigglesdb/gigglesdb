//! Redaction of upstream error and notice fields.
use crate::error::{NyCode, Refusal};
fn valid(code: &str) -> bool {
    code.len() == 5
        && code
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
}
fn message(code: &str) -> &'static str {
    match code {
        "23505" => "duplicate key value violates unique constraint",
        "23503" => "foreign key violation",
        _ => match &code[..2] {
            "08" => "connection exception",
            "0A" => "feature not supported",
            "22" => "data exception",
            "23" => "integrity constraint violation",
            "25" => "invalid transaction state",
            "28" => "invalid authorization specification",
            "2B" => "dependent privilege descriptors still exist",
            "3B" => "savepoint exception",
            "40" => "transaction rollback",
            "42" => "syntax error or access rule violation",
            "53" => "insufficient resources",
            "54" => "program limit exceeded",
            "55" => "object not in prerequisite state",
            "57" => "operator intervention",
            "58" => "system error",
            _ => "database error",
        },
    }
}
/// Strip all upstream error data except severity and validated SQLSTATE.
pub fn redact_error(fields: &[(u8, String)]) -> Vec<(u8, String)> {
    let code = fields
        .iter()
        .find(|(k, _)| *k == b'C')
        .map(|(_, v)| v.as_str())
        .filter(|v| valid(v))
        .unwrap_or("XX000");
    let mut out = Vec::new();
    for (k, v) in fields {
        match *k {
            b'S' | b'V' => out.push((*k, v.clone())),
            _ => {}
        }
    }
    out.push((b'C', code.into()));
    out.push((b'M', message(code).into()));
    out
}
/// Build a data-free gateway ErrorResponse field list.
pub fn nunya_error(code: NyCode) -> Vec<(u8, String)> {
    gateway_error(code, code.message())
}

pub(super) fn nunya_refusal(error: &Refusal) -> Vec<(u8, String)> {
    gateway_error(error.code, error.message())
}

fn gateway_error(code: NyCode, message: &'static str) -> Vec<(u8, String)> {
    vec![
        (b'S', "ERROR".into()),
        (b'V', "ERROR".into()),
        (b'C', code.sqlstate().into()),
        (b'M', message.into()),
    ]
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strips_details() {
        let out = redact_error(&[
            (b'C', "23505".into()),
            (b'M', "ana".into()),
            (b'D', "Key (email)=(ana@example.com)".into()),
            (b'?', "ana".into()),
        ]);
        assert!(!format!("{out:?}").contains("ana"));
        assert!(out.contains(&(b'C', "23505".into())));
        assert_eq!(out.len(), 2);
    }
    #[test]
    fn bad_state() {
        for c in ["bad", "éabcd", "23a05", ""] {
            assert!(redact_error(&[(b'C', c.into())]).contains(&(b'C', "XX000".into())));
        }
    }
    #[test]
    fn gateway() {
        for c in NyCode::ALL {
            assert!(nunya_error(c).contains(&(b'M', c.message().into())));
        }
    }
}
