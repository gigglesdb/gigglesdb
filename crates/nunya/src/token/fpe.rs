//! FF1 cycle walking and UUID block permutations.
use super::{ScopeKeys, keys::subkey};
use crate::{
    error::{NyCode, Refusal},
    policy::{DomainName, TypeRange},
    value::{ColumnType, SqlValue},
};
use aes::{
    Aes256,
    cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit},
};

// Inclusive Gregorian 0001-01-01..9999-12-31, relative to the PostgreSQL epoch.
const DATE_MIN: i64 = -730_119;
const DATE_MAX: i64 = 2_921_939;
const DAY_MICROS: i64 = 86_400_000_000;
const TIMESTAMP_MIN: i64 = DATE_MIN * DAY_MICROS;
const TIMESTAMP_MAX: i64 = (DATE_MAX + 1) * DAY_MICROS - 1;

fn refusal(code: NyCode) -> Refusal {
    Refusal::new(code, "format preserving token refused")
}
fn num(digits: &[u16], radix: u32) -> u128 {
    digits
        .iter()
        .fold(0, |a, b| a * u128::from(radix) + u128::from(*b))
}
fn digits(mut value: u128, radix: u32, len: usize) -> Vec<u16> {
    let mut out = vec![0; len];
    for d in out.iter_mut().rev() {
        *d = (value % u128::from(radix)) as u16;
        value /= u128::from(radix);
    }
    out
}
// SP 800-38G Algorithm 7/8, bounded to halves fitting u128 and d <= 16.
// Production input is 2..=64 binary digits; test vectors also use radix 10/36.
fn ff1(
    cipher: &impl BlockCipherEncrypt<BlockSize = aes::cipher::consts::U16>,
    radix: u32,
    tweak: &[u8],
    input: &[u16],
    decrypt: bool,
) -> Vec<u16> {
    let n = input.len();
    let u = n / 2;
    let v = n - u;
    let modulus_v = u128::from(radix).pow(v as u32);
    let b = ((128 - (modulus_v - 1).leading_zeros()) as usize).div_ceil(8);
    let d = 4 * b.div_ceil(4) + 4;
    let mut p = [0u8; 16];
    p[..3].copy_from_slice(&[1, 2, 1]);
    p[3..6].copy_from_slice(&radix.to_be_bytes()[1..]);
    p[6] = 10;
    p[7] = u as u8;
    p[8..12].copy_from_slice(&(n as u32).to_be_bytes());
    p[12..].copy_from_slice(&(tweak.len() as u32).to_be_bytes());
    let mut a = input[..u].to_vec();
    let mut c = input[u..].to_vec();
    for round in 0..10 {
        let i = if decrypt { 9 - round } else { round };
        let m = if i % 2 == 0 { u } else { v };
        let mut q = tweak.to_vec();
        q.resize(tweak.len() + (16 - (tweak.len() + b + 1) % 16) % 16, 0);
        q.push(i as u8);
        let bytes = num(if decrypt { &a } else { &c }, radix).to_be_bytes();
        q.extend_from_slice(&bytes[16 - b..]);
        let mut r = Array::from([0u8; 16]);
        for block in std::iter::once(p.as_slice())
            .chain(q.as_chunks::<16>().0.iter().map(|block| block.as_slice()))
        {
            for (dst, src) in r.iter_mut().zip(block) {
                *dst ^= *src;
            }
            cipher.encrypt_block(&mut r);
        }
        let y = r[..d].iter().fold(0u128, |n, b| (n << 8) | u128::from(*b));
        let modulus = u128::from(radix).pow(m as u32);
        if decrypt {
            let next = digits((num(&c, radix) + modulus - y % modulus) % modulus, radix, m);
            c = a;
            a = next;
        } else {
            let next = digits((num(&a, radix) + y % modulus) % modulus, radix, m);
            a = c;
            c = next;
        }
    }
    a.extend(c);
    a
}
pub(crate) fn convert(
    keys: &ScopeKeys,
    domain: &DomainName,
    ty: &ColumnType,
    range: Option<&TypeRange>,
    value: &SqlValue,
    decode: bool,
) -> Result<SqlValue, Refusal> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if let (ColumnType::Uuid, SqlValue::Uuid(bytes)) = (ty, value) {
        let key = subkey(keys, domain, "uuid");
        let cipher = Aes256::new((&*key).into());
        let mut block = Array::from(*bytes);
        if decode {
            cipher.decrypt_block(&mut block);
        } else {
            cipher.encrypt_block(&mut block);
        }
        return Ok(SqlValue::Uuid(block.into()));
    }
    let (value, lo, hi, tag) = match (ty, value) {
        (ColumnType::Int4, SqlValue::Int4(v)) => (
            i64::from(*v),
            i64::from(i32::MIN),
            i64::from(i32::MAX),
            b"int4".as_slice(),
        ),
        (ColumnType::Int8, SqlValue::Int8(v)) => (*v, i64::MIN, i64::MAX, b"int8".as_slice()),
        (ColumnType::Date, SqlValue::Date(v)) => {
            (i64::from(*v), DATE_MIN, DATE_MAX, b"date".as_slice())
        }
        (ColumnType::Timestamp, SqlValue::Timestamp(v)) => {
            (*v, TIMESTAMP_MIN, TIMESTAMP_MAX, b"ts".as_slice())
        }
        (ColumnType::Timestamptz, SqlValue::Timestamptz(v)) => {
            (*v, TIMESTAMP_MIN, TIMESTAMP_MAX, b"tstz".as_slice())
        }
        _ => return Err(refusal(NyCode::BlindOperation)),
    };
    let (min, max) = range.map_or((lo, hi), |r| (r.min, r.max));
    if min > max
        || (matches!(ty, ColumnType::Int4 | ColumnType::Date)
            && (min < i64::from(i32::MIN) || max > i64::from(i32::MAX)))
    {
        return Err(refusal(NyCode::NotAnalysable));
    }
    let size = (i128::from(max) - i128::from(min) + 1) as u128;
    if size < (1 << 20) {
        return Err(refusal(NyCode::BlindOperation));
    }
    if value < min || value > max {
        return Err(refusal(if decode {
            NyCode::TokenNotValid
        } else {
            NyCode::BlindOperation
        }));
    }
    let bits = (128 - (size - 1).leading_zeros()) as usize;
    let key = subkey(keys, domain, "ff1");
    let cipher = Aes256::new((&*key).into());
    let mut tweak = domain.0.as_bytes().to_vec();
    tweak.push(0);
    tweak.extend_from_slice(tag);
    let mut x = (i128::from(value) - i128::from(min)) as u128;
    for _ in 0..10_000 {
        x = num(&ff1(&cipher, 2, &tweak, &digits(x, 2, bits), decode), 2);
        if x < size {
            let output = (i128::from(min) + x as i128) as i64;
            return Ok(match ty {
                ColumnType::Int4 => SqlValue::Int4(output as i32),
                ColumnType::Int8 => SqlValue::Int8(output),
                ColumnType::Date => SqlValue::Date(output as i32),
                ColumnType::Timestamp => SqlValue::Timestamp(output),
                ColumnType::Timestamptz => SqlValue::Timestamptz(output),
                _ => return Err(refusal(NyCode::NotAnalysable)),
            });
        }
    }
    Err(refusal(NyCode::NotAnalysable))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{StandardCodec, TokenCodec};
    fn roundtrip(ty: ColumnType, range: Option<TypeRange>, v: SqlValue) -> SqlValue {
        let k = ScopeKeys::new([7; 32]);
        let d = DomainName("test".into());
        let t = StandardCodec
            .encode(&k, &d, &ty, range.as_ref(), &v)
            .unwrap();
        assert_eq!(
            StandardCodec
                .decode(&k, &d, &ty, range.as_ref(), &t)
                .unwrap(),
            v
        );
        t
    }
    #[test]
    fn numeric_edges_and_ranges() {
        for v in [i32::MIN, i32::MAX, 0, -1, 571943] {
            roundtrip(ColumnType::Int4, None, SqlValue::Int4(v));
        }
        for v in [i64::MIN, i64::MAX, 0, -1, 78712541890742] {
            roundtrip(ColumnType::Int8, None, SqlValue::Int8(v));
        }
        for v in [DATE_MIN as i32, DATE_MAX as i32] {
            roundtrip(ColumnType::Date, None, SqlValue::Date(v));
        }
        for v in [TIMESTAMP_MIN, TIMESTAMP_MAX] {
            roundtrip(ColumnType::Timestamp, None, SqlValue::Timestamp(v));
            roundtrip(ColumnType::Timestamptz, None, SqlValue::Timestamptz(v));
        }
        let range = TypeRange {
            min: 0,
            max: 1 << 20,
        };
        for v in [0, 1, 100, 1 << 20] {
            let SqlValue::Int8(t) =
                roundtrip(ColumnType::Int8, Some(range.clone()), SqlValue::Int8(v))
            else {
                panic!("unexpected variant")
            };
            assert!((0..=1 << 20).contains(&t));
        }
        let k = ScopeKeys::new([7; 32]);
        let d = DomainName("test".into());
        assert_eq!(
            StandardCodec
                .encode(
                    &k,
                    &d,
                    &ColumnType::Int8,
                    Some(&TypeRange {
                        min: 0,
                        max: (1 << 20) - 2
                    }),
                    &SqlValue::Int8(0)
                )
                .unwrap_err()
                .code,
            NyCode::BlindOperation
        );
        assert_eq!(
            StandardCodec
                .decode(&k, &d, &ColumnType::Int8, Some(&range), &SqlValue::Int8(-1))
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
        assert_ne!(
            StandardCodec
                .decode(&k, &d, &ColumnType::Int8, None, &SqlValue::Int8(123))
                .unwrap(),
            SqlValue::Int8(123)
        );
        assert_ne!(
            roundtrip(ColumnType::Uuid, None, SqlValue::Uuid([0; 16])),
            SqlValue::Uuid([0; 16])
        );
    }
    #[test]
    fn default_temporal_bounds_match_calendar_and_roundtrip() {
        use crate::pgwire::values::decode_text;
        for (ty, min_text, max_text, min_value, max_value) in [
            (
                ColumnType::Date,
                "0001-01-01",
                "9999-12-31",
                SqlValue::Date(DATE_MIN as i32),
                SqlValue::Date(DATE_MAX as i32),
            ),
            (
                ColumnType::Timestamp,
                "0001-01-01 00:00:00",
                "9999-12-31 23:59:59.999999",
                SqlValue::Timestamp(TIMESTAMP_MIN),
                SqlValue::Timestamp(TIMESTAMP_MAX),
            ),
            (
                ColumnType::Timestamptz,
                "0001-01-01 00:00:00+00",
                "9999-12-31 23:59:59.999999+00",
                SqlValue::Timestamptz(TIMESTAMP_MIN),
                SqlValue::Timestamptz(TIMESTAMP_MAX),
            ),
        ] {
            assert_eq!(decode_text(&ty, min_text.as_bytes()).unwrap(), min_value);
            assert_eq!(decode_text(&ty, max_text.as_bytes()).unwrap(), max_value);
            for value in [min_value, max_value] {
                let token = roundtrip(ty.clone(), None, value);
                let (min, max, output) = match token {
                    SqlValue::Date(v) => (DATE_MIN, DATE_MAX, i64::from(v)),
                    SqlValue::Timestamp(v) | SqlValue::Timestamptz(v) => {
                        (TIMESTAMP_MIN, TIMESTAMP_MAX, v)
                    }
                    _ => panic!("unexpected token type"),
                };
                assert!((min..=max).contains(&output));
            }
        }
    }
    #[test]
    fn every_fpe_family_enforces_exact_minimum_domain() {
        let keys = ScopeKeys::new([7; 32]);
        let domain = DomainName("test".into());
        for (ty, value) in [
            (ColumnType::Int4, SqlValue::Int4(0)),
            (ColumnType::Int8, SqlValue::Int8(0)),
            (ColumnType::Date, SqlValue::Date(0)),
            (ColumnType::Timestamp, SqlValue::Timestamp(0)),
            (ColumnType::Timestamptz, SqlValue::Timestamptz(0)),
        ] {
            let short = TypeRange {
                min: 0,
                max: (1 << 20) - 2,
            };
            for decode in [false, true] {
                assert_eq!(
                    convert(&keys, &domain, &ty, Some(&short), &value, decode)
                        .unwrap_err()
                        .code,
                    NyCode::BlindOperation
                );
            }
            let minimum = TypeRange {
                min: 0,
                max: (1 << 20) - 1,
            };
            roundtrip(ty, Some(minimum), value);
        }
    }
    #[test]
    fn nist_samples_1_2_3_7_8_9() {
        // NIST FF1samples.pdf; samples 3 and 9 have radix 36.
        let key = [
            0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf,
            0x4f, 0x3c, 0xef, 0x43, 0x59, 0xd8, 0xd5, 0x80, 0xaa, 0x4f, 0x7f, 0x03, 0x6d, 0x6f,
            0x04, 0xfc, 0x6a, 0x94,
        ];
        let c128 = aes::Aes128::new((&key[..16]).try_into().unwrap());
        let c256 = Aes256::new((&key).into());
        for (radix, tweak, input, out128, out256) in [
            (10, b"".as_slice(), "0123456789", "2433477484", "6657667009"),
            (
                10,
                b"9876543210".as_slice(),
                "0123456789",
                "6124200773",
                "1001623463",
            ),
            (
                36,
                b"7777pqrs777".as_slice(),
                "0123456789abcdefghi",
                "a9tv40mll9kdu509eum",
                "xs8a0azh2avyalyzuwd",
            ),
        ] {
            let parse = |s: &str| {
                s.chars()
                    .map(|c| c.to_digit(radix).unwrap() as u16)
                    .collect::<Vec<_>>()
            };
            let plain = parse(input);
            let e128 = ff1(&c128, radix, tweak, &plain, false);
            let e256 = ff1(&c256, radix, tweak, &plain, false);
            assert_eq!(e128, parse(out128));
            assert_eq!(e256, parse(out256));
            assert_eq!(ff1(&c128, radix, tweak, &e128, true), plain);
            assert_eq!(ff1(&c256, radix, tweak, &e256, true), plain);
        }
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn int8_roundtrip(v in proptest::num::i64::ANY) { roundtrip(ColumnType::Int8,None,SqlValue::Int8(v)); }
        #[test]
        fn int4_roundtrip(v in proptest::num::i32::ANY) { roundtrip(ColumnType::Int4,None,SqlValue::Int4(v)); }
    }
}
