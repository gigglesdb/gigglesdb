//! Deterministic bucket-padded authenticated text tokens.
use super::{ScopeKeys, keys::subkey};
use crate::{
    error::{NyCode, Refusal},
    policy::DomainName,
    value::SqlValue,
};
use aes_siv::{KeyInit, siv::Aes256Siv};
use data_encoding::BASE32_NOPAD;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;
fn bad() -> Refusal {
    Refusal::new(NyCode::TokenNotValid, "invalid text token")
}
pub(crate) fn token_parts(text: &str) -> Result<(&str, &str), Refusal> {
    text.strip_prefix("tok_")
        .ok_or_else(|| Refusal::new(NyCode::BlindOperation, "plain text in blind position"))?
        .rsplit_once('_')
        .ok_or_else(bad)
}

/// Decode text only when its expected and embedded domains are not retired.
pub fn decode_checked(
    keys: &ScopeKeys,
    domain: &DomainName,
    value: &SqlValue,
    retired: &super::RetiredDomains,
) -> Result<SqlValue, Refusal> {
    super::check_expected_not_retired(retired, domain)?;
    if let SqlValue::Text(token) = value {
        super::check_not_retired(retired, token)?;
    }
    convert(keys, domain, value, true)
}

fn cipher(keys: &ScopeKeys, domain: &DomainName) -> Aes256Siv {
    let key = subkey(keys, domain, "text");
    let mut expanded = Zeroizing::new([0; 64]);
    Hkdf::<Sha256>::new(None, &*key)
        .expand(b"nunya/v1/siv", &mut *expanded)
        .expect("fixed HKDF output length");
    Aes256Siv::new((&*expanded).into())
}
pub(crate) fn convert(
    keys: &ScopeKeys,
    domain: &DomainName,
    value: &SqlValue,
    decode: bool,
) -> Result<SqlValue, Refusal> {
    let text = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Text(t) => t,
        _ => return Err(Refusal::new(NyCode::NotAnalysable, "text type mismatch")),
    };
    let prefix = format!("tok_{}_", domain.0);
    let ad = format!("nunya/v1|{}", domain.0);
    if !decode {
        if text.len() > 254 {
            return Err(Refusal::new(
                NyCode::BlindOperation,
                "text exceeds token capacity",
            ));
        }
        let size = [16, 32, 64, 128, 256]
            .into_iter()
            .find(|s| *s >= text.len() + 2)
            .ok_or_else(bad)?;
        let mut plain = Zeroizing::new(vec![0; size]);
        plain[..2].copy_from_slice(&(text.len() as u16).to_be_bytes());
        plain[2..text.len() + 2].copy_from_slice(text.as_bytes());
        let encrypted = cipher(keys, domain)
            .encrypt([ad.as_bytes()], &plain)
            .map_err(|_| bad())?;
        return Ok(SqlValue::Text(
            prefix + &BASE32_NOPAD.encode(&encrypted).to_ascii_lowercase(),
        ));
    }
    let (token_domain, body) = token_parts(text)?;
    if token_domain != domain.0 {
        return Err(Refusal::new(
            NyCode::TokenOutsideDomain,
            "text token domain rejected",
        ));
    }
    if ![52, 77, 128, 231, 436].contains(&body.len()) {
        return Err(bad());
    }
    let encrypted = BASE32_NOPAD
        .decode(body.to_ascii_uppercase().as_bytes())
        .map_err(|_| bad())?;
    if ![32, 48, 80, 144, 272].contains(&encrypted.len()) {
        return Err(bad());
    }
    let plain = Zeroizing::new(
        cipher(keys, domain)
            .decrypt([ad.as_bytes()], &encrypted)
            .map_err(|_| bad())?,
    );
    let len = u16::from_be_bytes([plain[0], plain[1]]) as usize;
    if len > 254
        || len + 2 > plain.len()
        || plain[len + 2..].iter().any(|b| *b != 0)
        || (plain.len() > 16 && len + 2 <= plain.len() / 2)
    {
        return Err(bad());
    }
    Ok(SqlValue::Text(
        std::str::from_utf8(&plain[2..len + 2])
            .map_err(|_| bad())?
            .to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(k: &ScopeKeys, d: &str, s: &SqlValue, dec: bool) -> Result<SqlValue, Refusal> {
        convert(k, &DomainName(d.into()), s, dec)
    }
    #[test]
    fn text_properties() {
        let keys = ScopeKeys::new([1; 32]);
        let other = ScopeKeys::new([2; 32]);
        let plain = SqlValue::Text("hello".into());
        let token = run(&keys, "d", &plain, false).unwrap();
        assert_eq!(run(&keys, "d", &token, true).unwrap(), plain);
        assert_eq!(run(&keys, "d", &plain, false).unwrap(), token);
        assert_ne!(run(&other, "d", &plain, false).unwrap(), token);
        for (source, target) in [("d_x", "d"), ("d", "d_x")] {
            let encoded = run(&keys, source, &plain, false).unwrap();
            assert_eq!(
                run(&keys, target, &encoded, true).unwrap_err().code,
                NyCode::TokenOutsideDomain
            );
        }
        for (k, d, v, code) in [
            (&other, "d", &token, NyCode::TokenNotValid),
            (&keys, "x", &token, NyCode::TokenOutsideDomain),
            (&keys, "d", &plain, NyCode::BlindOperation),
        ] {
            assert_eq!(run(k, d, v, true).unwrap_err().code, code);
        }
        let lengths: Vec<_> = [0, 1, 13, 14]
            .map(
                |n| match run(&keys, "d", &SqlValue::Text("x".repeat(n)), false).unwrap() {
                    SqlValue::Text(t) => t.len(),
                    _ => 0,
                },
            )
            .into();
        assert!(lengths.iter().all(|n| *n == lengths[0]));
        for len in [0, 14, 15, 30, 31, 62, 63, 126, 127, 254] {
            let value = SqlValue::Text("x".repeat(len));
            let encoded = run(&keys, "d", &value, false).unwrap();
            assert_eq!(run(&keys, "d", &encoded, true).unwrap(), value);
        }
        assert_eq!(
            run(&keys, "d", &SqlValue::Text("x".repeat(255)), false)
                .unwrap_err()
                .code,
            NyCode::BlindOperation
        );
        if let SqlValue::Text(mut t) = token {
            let end = t.pop().unwrap();
            t.push(if end == 'a' { 'b' } else { 'a' });
            assert_eq!(
                run(&keys, "d", &SqlValue::Text(t), true).unwrap_err().code,
                NyCode::TokenNotValid
            );
        }
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn text_roundtrip(s in ".{0,60}") {
            let k=ScopeKeys::new([4;32]); let v=SqlValue::Text(s);
            let t=run(&k,"p",&v,false).unwrap();
            proptest::prop_assert_eq!(run(&k,"p",&t,true).unwrap(),v);
        }
    }
}
