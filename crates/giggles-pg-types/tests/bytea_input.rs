use giggles_pg_types::bytea::{ByteaInputError, prepare_bytea_text};
#[test]
fn pg18_empty() {
    let input = std::str::from_utf8(&[]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![]
    );
}
#[test]
fn pg18_ascii() {
    let input = std::str::from_utf8(&[97, 98, 99]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![97, 98, 99]
    );
}
#[test]
fn pg18_hexempty() {
    let input = std::str::from_utf8(&[92, 120]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![]
    );
}
#[test]
fn pg18_hex() {
    let input = std::str::from_utf8(&[92, 120, 48, 48, 97, 66, 102, 102]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![0, 171, 255]
    );
}
#[test]
fn pg18_upperprefix() {
    let input = std::str::from_utf8(&[92, 88, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::EscapeSyntax
    );
}
#[test]
fn pg18_odd() {
    let input = std::str::from_utf8(&[92, 120, 48]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexOddDigits
    );
}
#[test]
fn pg18_badhex() {
    let input = std::str::from_utf8(&[92, 120, 103, 103]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_slash() {
    let input = std::str::from_utf8(&[92]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::EscapeSyntax
    );
}
#[test]
fn pg18_doubled() {
    let input = std::str::from_utf8(&[92, 92]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![92]
    );
}
#[test]
fn pg18_octzero() {
    let input = std::str::from_utf8(&[92, 48, 48, 48]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![0]
    );
}
#[test]
fn pg18_octmax() {
    let input = std::str::from_utf8(&[92, 51, 55, 55]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![255]
    );
}
#[test]
fn pg18_octextra() {
    let input = std::str::from_utf8(&[92, 49, 50, 51, 52]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![83, 52]
    );
}
#[test]
fn pg18_octwide() {
    let input = std::str::from_utf8(&[92, 52, 48, 48]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::EscapeSyntax
    );
}
#[test]
fn pg18_octshort() {
    let input = std::str::from_utf8(&[92, 49, 50]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::EscapeSyntax
    );
}
#[test]
fn pg18_octbad() {
    let input = std::str::from_utf8(&[92, 48, 56, 97]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::EscapeSyntax
    );
}
#[test]
fn pg18_unicode() {
    let input = std::str::from_utf8(&[195, 169, 240, 159, 152, 128]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![195, 169, 240, 159, 152, 128]
    );
}
#[test]
fn pg18_nul() {
    let input = std::str::from_utf8(&[97, 0, 98]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::NulInput
    );
}
#[test]
fn pg18_ws20before() {
    let input = std::str::from_utf8(&[92, 120, 32, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws20within() {
    let input = std::str::from_utf8(&[92, 120, 52, 32, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws20after() {
    let input = std::str::from_utf8(&[92, 120, 52, 49, 32]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws09before() {
    let input = std::str::from_utf8(&[92, 120, 9, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws09within() {
    let input = std::str::from_utf8(&[92, 120, 52, 9, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws09after() {
    let input = std::str::from_utf8(&[92, 120, 52, 49, 9]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws0abefore() {
    let input = std::str::from_utf8(&[92, 120, 10, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws0awithin() {
    let input = std::str::from_utf8(&[92, 120, 52, 10, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0aafter() {
    let input = std::str::from_utf8(&[92, 120, 52, 49, 10]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws0dbefore() {
    let input = std::str::from_utf8(&[92, 120, 13, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws0dwithin() {
    let input = std::str::from_utf8(&[92, 120, 52, 13, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0dafter() {
    let input = std::str::from_utf8(&[92, 120, 52, 49, 13]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX)
            .unwrap()
            .decode()
            .unwrap(),
        vec![65]
    );
}
#[test]
fn pg18_ws0bbefore() {
    let input = std::str::from_utf8(&[92, 120, 11, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0bwithin() {
    let input = std::str::from_utf8(&[92, 120, 52, 11, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0bafter() {
    let input = std::str::from_utf8(&[92, 120, 52, 49, 11]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0cbefore() {
    let input = std::str::from_utf8(&[92, 120, 12, 52, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0cwithin() {
    let input = std::str::from_utf8(&[92, 120, 52, 12, 49]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn pg18_ws0cafter() {
    let input = std::str::from_utf8(&[92, 120, 52, 49, 12]).unwrap();
    assert_eq!(
        prepare_bytea_text(input, usize::MAX).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
}
#[test]
fn exhaustive_octal_triplets_and_escape_widths() {
    for a in b'0'..=b'7' {
        for b in b'0'..=b'7' {
            for c in b'0'..=b'7' {
                let bytes = [b'\\', a, b, c];
                let input = std::str::from_utf8(&bytes).unwrap();
                if a <= b'3' {
                    let value = (a - b'0') * 64 + (b - b'0') * 8 + (c - b'0');
                    assert_eq!(
                        prepare_bytea_text(input, 1).unwrap().decode().unwrap(),
                        [value]
                    );
                    assert_eq!(
                        prepare_bytea_text(input, 0).unwrap_err(),
                        ByteaInputError::OutputLimit
                    );
                    let extra = format!("{input}7");
                    assert_eq!(
                        prepare_bytea_text(&extra, 2).unwrap().decode().unwrap(),
                        [value, b'7']
                    );
                } else {
                    assert_eq!(
                        prepare_bytea_text(input, usize::MAX).unwrap_err(),
                        ByteaInputError::EscapeSyntax
                    );
                }
            }
        }
    }
    for input in ["\\", "\\0", "\\00", "\\8", "\\n", "\\xg", "\\X00"] {
        assert!(prepare_bytea_text(input, usize::MAX).is_err(), "{input:?}");
    }
}
#[test]
fn exhaustive_ascii_hex_pair_boundaries() {
    fn h(v: u8) -> Option<u8> {
        match v {
            b'0'..=b'9' => Some(v - b'0'),
            b'a'..=b'f' => Some(v - b'a' + 10),
            b'A'..=b'F' => Some(v - b'A' + 10),
            _ => None,
        }
    }
    for a in 1u8..=127 {
        for b in 1u8..=127 {
            let bytes = [b'\\', b'x', a, b];
            let input = std::str::from_utf8(&bytes).unwrap();
            let expected = if matches!(a, b' ' | b'\t' | b'\r' | b'\n') {
                if matches!(b, b' ' | b'\t' | b'\r' | b'\n') {
                    Ok(vec![])
                } else if h(b).is_some() {
                    Err(ByteaInputError::HexOddDigits)
                } else {
                    Err(ByteaInputError::HexInvalidDigit)
                }
            } else if let (Some(a), Some(b)) = (h(a), h(b)) {
                Ok(vec![(a << 4) | b])
            } else {
                Err(ByteaInputError::HexInvalidDigit)
            };
            assert_eq!(
                prepare_bytea_text(input, usize::MAX).and_then(|p| p.decode()),
                expected,
                "{bytes:?}"
            );
        }
    }
}
#[test]
fn deterministic_roundtrip_and_exact_limits() {
    let mut seed = 0x67a5_8912u32;
    for len in 0..256 {
        let bytes: Vec<_> = (0..len)
            .map(|_| {
                seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                (seed >> 24) as u8
            })
            .collect();
        let mut hex = String::from("\\x");
        let mut oct = String::new();
        for b in &bytes {
            use std::fmt::Write;
            write!(hex, "{b:02x}").unwrap();
            write!(oct, "\\{b:03o}").unwrap();
        }
        for input in [&hex, &oct] {
            let plan = prepare_bytea_text(input, len).unwrap();
            assert_eq!(plan.decoded_len(), len);
            assert_eq!(plan.decode().unwrap(), bytes);
            if len > 0 {
                assert_eq!(
                    prepare_bytea_text(input, len - 1).unwrap_err(),
                    ByteaInputError::OutputLimit
                );
            }
        }
    }
    assert_eq!(
        prepare_bytea_text("\\x00gg", 0).unwrap_err(),
        ByteaInputError::HexInvalidDigit
    );
    assert_eq!(
        prepare_bytea_text("a\0b", usize::MAX).unwrap_err(),
        ByteaInputError::NulInput
    );
}
