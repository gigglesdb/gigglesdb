use super::{credential::*, definition::WallClockMs};
use crate::CoreError;

fn record() -> CredentialRecord {
    CredentialRecord {
        issued_at: WallClockMs(1),
        expires_at: Some(WallClockMs(2)),
        issued_by: "a".into(),
        verifier: [0x55; 32],
    }
}
fn rejects(value: &[u8]) {
    assert!(matches!(
        decode_credential(value),
        Err(CoreError::Corrupt(_))
    ));
}
#[test]
fn credential_record_golden() {
    let mut golden = vec![
        1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, b'a',
    ];
    golden.extend([0x55; 32]);
    assert_eq!(encode_credential(&record()).unwrap(), golden);
    assert_eq!(decode_credential(&golden).unwrap(), record());
    assert_eq!(
        credential_key(1, 2),
        vec![24, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]
    );
    assert_eq!(
        decode_credential_key(&credential_key(1, 2)).unwrap(),
        (1, 2)
    );
    let mut no_expiry = record();
    no_expiry.expires_at = None;
    let encoded = encode_credential(&no_expiry).unwrap();
    assert_eq!(&encoded[9..17], &i64::MAX.to_be_bytes());
    assert_eq!(decode_credential(&encoded).unwrap(), no_expiry);
    no_expiry.issued_by = "?".repeat(31) + "x";
    assert_eq!(
        decode_credential(&encode_credential(&no_expiry).unwrap()).unwrap(),
        no_expiry
    );
}
#[test]
fn decode_rejects_bad_version() {
    let mut value = encode_credential(&record()).unwrap();
    value[0] = 2;
    rejects(&value);
}
#[test]
fn decode_rejects_short() {
    let value = encode_credential(&record()).unwrap();
    for len in 0..value.len() {
        rejects(&value[..len]);
    }
}
#[test]
fn decode_rejects_long() {
    let mut value = encode_credential(&record()).unwrap();
    value.push(0);
    rejects(&value);
}
#[test]
fn decode_rejects_zero_ids() {
    for key in [
        credential_key(0, 1),
        credential_key(1, 0),
        vec![],
        vec![24; 16],
        vec![23; 17],
        // Kind 22 is a user collation, even at the credential key length.
        vec![22; 17],
    ] {
        assert!(matches!(
            decode_credential_key(&key),
            Err(CoreError::Corrupt(_))
        ));
    }
}
#[test]
fn decode_rejects_expiry_before_issue() {
    for expires in [0i64, 1] {
        let mut value = encode_credential(&record()).unwrap();
        value[9..17].copy_from_slice(&expires.to_be_bytes());
        rejects(&value);
    }
    let mut invalid = record();
    invalid.expires_at = Some(invalid.issued_at);
    assert!(encode_credential(&invalid).is_err());
}
#[test]
fn decode_rejects_bad_str() {
    let mut value = encode_credential(&record()).unwrap();
    value[19] = 0;
    rejects(&value);
    value[19] = 0xff;
    rejects(&value);
    for len in [0u16, 64, u16::MAX] {
        value[17..19].copy_from_slice(&len.to_be_bytes());
        rejects(&value);
    }
    for issuer in [String::new(), "x".repeat(64)] {
        let mut invalid = record();
        invalid.issued_by = issuer;
        assert!(encode_credential(&invalid).is_err());
    }
}
#[test]
fn secret_format_and_entropy() {
    let (a, hash) = new_secret(0x1234).unwrap();
    let (b, _) = new_secret(0x1234).unwrap();
    assert_eq!(a.expose().len(), 87);
    assert!(a.expose().starts_with("ggtc1_0000000000001234_"));
    assert!(a.expose() != b.expose());
    let (id, random) = parse_secret(a.expose()).unwrap();
    assert_eq!(id, 0x1234);
    assert!(ct_eq(&hash, &verifier(id, &random)));
    assert!(new_secret(0).is_err());
}
#[test]
fn parse_rejects_uppercase_and_garbage() {
    let valid = format!("ggtc1_000000000000000a_{}", "ab".repeat(32));
    assert!(parse_secret(&valid).is_some());
    for bad in [
        valid.to_uppercase(),
        valid.replace('a', "A"),
        valid.replace('b', "g"),
        valid[..86].into(),
        format!("{valid}0"),
        valid.replace("000000000000000a", "0000000000000000"),
        valid.replace('_', "-"),
        "?".repeat(44),
    ] {
        assert!(parse_secret(&bad).is_none());
    }
}
#[test]
fn verifier_matches_only_its_secret() {
    let random = [0x33; 32];
    let hash = verifier(5, &random);
    // Independent SHA-256 golden for domain separator, big-endian id and random bytes.
    assert_eq!(
        hash,
        [
            6, 58, 110, 92, 160, 176, 68, 34, 67, 149, 212, 239, 50, 222, 82, 116, 126, 69, 188,
            81, 216, 247, 5, 238, 241, 140, 99, 223, 188, 159, 237, 150
        ]
    );
    assert!(ct_eq(&hash, &verifier(5, &random)));
    assert!(!ct_eq(&hash, &verifier(6, &random)));
    for i in 0..32 {
        let mut changed = random;
        changed[i] ^= 1;
        assert!(!ct_eq(&hash, &verifier(5, &changed)));
        let mut different = hash;
        different[i] ^= 1;
        assert!(!ct_eq(&hash, &different));
    }
}
#[test]
fn debug_redacts_secret() {
    let (secret, _) = new_secret(1).unwrap();
    assert_eq!(format!("{secret:?}"), "SecretText([redacted])");
}
