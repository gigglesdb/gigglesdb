//! Validated persistence adapters for the shared PostgreSQL scalar types.
//! Numeric storage is canonical text (including display scale), never a float.
//! Float storage is the raw integer bit pattern, including NaN payloads.
use serde::{Deserialize, Deserializer, Serializer, de};
use std::fmt;

pub mod numeric {
    use super::*;
    use giggles_pg_types::{MAX_TEXT, Numeric};

    pub fn serialize<S: Serializer>(value: &Numeric, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Numeric, D::Error> {
        struct Canonical;
        impl de::Visitor<'_> for Canonical {
            type Value = Numeric;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded canonical numeric text")
            }
            fn visit_str<E: de::Error>(self, text: &str) -> Result<Numeric, E> {
                if text.len() > MAX_TEXT {
                    return Err(E::custom("numeric storage limit"));
                }
                let value = Numeric::parse(text).map_err(E::custom)?;
                if value.to_string() != text {
                    return Err(E::custom("noncanonical numeric storage"));
                }
                Ok(value)
            }
        }
        // Borrow through the format visitor: do not deserialize an unbounded String first.
        deserializer.deserialize_str(Canonical)
    }
}

macro_rules! float_adapter {
    ($module:ident, $ty:ident, $bits:ty) => {
        pub mod $module {
            use super::*;
            use giggles_pg_types::$ty;
            pub fn serialize<S: Serializer>(value: &$ty, serializer: S) -> Result<S::Ok, S::Error> {
                serde::Serialize::serialize(&value.to_bits(), serializer)
            }
            pub fn deserialize<'de, D: Deserializer<'de>>(
                deserializer: D,
            ) -> Result<$ty, D::Error> {
                Ok($ty::from_bits(<$bits>::deserialize(deserializer)?))
            }
        }
    };
}
float_adapter!(real, Float4, u32);
float_adapter!(double, Float8, u64);

/// Check persisted decimal syntax and resource bounds without allocating a value.
pub(crate) fn canonical_numeric_text(text: &str) -> bool {
    if text.len() > giggles_pg_types::MAX_TEXT {
        return false;
    }
    if matches!(text, "NaN" | "Infinity" | "-Infinity") {
        return true;
    }
    let negative = text.starts_with('-');
    let raw = text.strip_prefix('-').unwrap_or(text);
    let mut parts = raw.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || whole.len() > 1 && whole.starts_with('0')
    {
        return false;
    }
    if let Some(f) = frac
        && (f.is_empty()
            || f.len() > giggles_pg_types::MAX_SCALE as usize
            || !f.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    let mut significant = 0usize;
    let mut nonzero = false;
    for b in whole.bytes().chain(frac.unwrap_or("").bytes()) {
        nonzero |= b != b'0';
        if nonzero {
            significant += 1;
        }
    }
    (!negative || nonzero) && significant <= giggles_pg_types::MAX_DIGITS
}

#[cfg(test)]
mod tests {
    #[test]
    fn allocation_free_numeric_admission_matches_canonical_values() {
        for text in [
            "0",
            "0.000",
            "-1.20",
            "12345678901234567890.00100",
            "NaN",
            "Infinity",
            "-Infinity",
        ] {
            assert!(super::canonical_numeric_text(text));
            assert_eq!(
                giggles_pg_types::Numeric::parse(text).unwrap().to_string(),
                text
            );
        }
        for text in [
            "", "+1", "01", "-0", "-0.00", ".1", "1.", "1e2", "nan", " 1", "1_0", "1..0",
        ] {
            assert!(!super::canonical_numeric_text(text), "{text}");
        }
        assert!(!super::canonical_numeric_text(
            &"9".repeat(giggles_pg_types::MAX_DIGITS + 1)
        ));
        assert!(!super::canonical_numeric_text(&format!(
            "0.{}",
            "0".repeat(giggles_pg_types::MAX_SCALE as usize + 1)
        )));
    }

    use super::*;
    use bincode::Options;
    use giggles_pg_types::{Float4, Float8, Numeric};
    use serde::Serialize;
    #[test]
    fn legacy_enum_indexes_are_frozen() {
        use crate::relational::{DataType, Value};
        for (index, value) in [
            Value::Null,
            Value::Bool(false),
            Value::Int(0),
            Value::Text(String::new()),
            Value::Bytes(Vec::new()),
        ]
        .iter()
        .enumerate()
        {
            let bytes = bincode::serialize(value).unwrap();
            assert_eq!(&bytes[..4], &(index as u32).to_le_bytes());
        }
        for (index, ty) in [
            DataType::Boolean,
            DataType::SmallInt,
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Bytes,
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(
                bincode::serialize(ty).unwrap(),
                (index as u32).to_le_bytes()
            );
        }
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Scalars {
        #[serde(with = "numeric")]
        n: Numeric,
        #[serde(with = "real")]
        r: Float4,
        #[serde(with = "double")]
        d: Float8,
    }
    #[test]
    fn storage_representation_is_canonical_text_then_integer_bits() {
        let value = Scalars {
            n: Numeric::parse("9007199254740993.00").unwrap(),
            r: Float4::from_bits(0x80000000),
            d: Float8::from_bits(0x7ff8000000001234),
        };
        let codec = || {
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .with_limit(65536)
                .reject_trailing_bytes()
        };
        let text = b"9007199254740993.00";
        let mut expected = (text.len() as u64).to_be_bytes().to_vec();
        expected.extend(text);
        expected.extend(0x80000000u32.to_be_bytes());
        expected.extend(0x7ff8000000001234u64.to_be_bytes());
        assert_eq!(codec().serialize(&value).unwrap(), expected);
        let back: Scalars = codec().deserialize(&expected).unwrap();
        assert_eq!(back.n.to_string(), "9007199254740993.00");
        assert_eq!(back.r.to_bits(), 0x80000000);
        assert_eq!(back.d.to_bits(), 0x7ff8000000001234);
        expected.push(0);
        assert!(codec().deserialize::<Scalars>(&expected).is_err());
        assert!(
            codec()
                .deserialize::<Scalars>(&u64::MAX.to_be_bytes())
                .is_err()
        );
    }
    #[test]
    fn exact_roundtrips() {
        for text in [
            "0.000",
            "9007199254740993.1200",
            "NaN",
            "Infinity",
            "-Infinity",
        ] {
            for (r, d) in [
                (0x80000000, 0x8000000000000000),
                (0x7fc12345, 0x7ff8000000001234),
            ] {
                let value = Scalars {
                    n: Numeric::parse(text).unwrap(),
                    r: Float4::from_bits(r),
                    d: Float8::from_bits(d),
                };
                let bytes = bincode::serialize(&value).unwrap();
                let back: Scalars = bincode::deserialize(&bytes).unwrap();
                assert_eq!(back.n.to_string(), text);
                assert_eq!(back.r.to_bits(), r);
                assert_eq!(back.d.to_bits(), d);
                assert_eq!(bincode::serialize(&back).unwrap(), bytes);
                let json = serde_json::to_string(&value).unwrap();
                let back: Scalars = serde_json::from_str(&json).unwrap();
                assert_eq!(back.r.to_bits(), r);
                assert_eq!(back.d.to_bits(), d);
            }
        }
    }
    #[test]
    fn rejects_noncanonical_and_unbounded() {
        #[derive(Debug, Deserialize)]
        struct One(#[serde(with = "numeric")] Numeric);
        for text in ["+1", "01", "-0", "1e2", "nan", " 1", "1_000"] {
            let bytes = bincode::serialize(text).unwrap();
            assert!(bincode::deserialize::<One>(&bytes).is_err(), "{text}");
        }
        let bytes = bincode::serialize(&"1".repeat(giggles_pg_types::MAX_TEXT + 1)).unwrap();
        assert!(bincode::deserialize::<One>(&bytes).is_err());
        assert!(
            bincode::DefaultOptions::new()
                .with_limit(32)
                .deserialize::<One>(&bytes)
                .is_err()
        );
        let valid: One = bincode::deserialize(&bincode::serialize("1.00").unwrap()).unwrap();
        assert_eq!(valid.0.to_string(), "1.00");
    }
}

// Fixed-size PostgreSQL binary components preserve epoch values, infinities and
// interval component identity independently of SQL comparison equivalence.
macro_rules! temporal_adapter {
    ($module:ident, $ty:ident, $size:expr) => {
        pub mod $module {
            use super::*;
            use giggles_pg_types::temporal::$ty;
            pub fn serialize<S: Serializer>(value: &$ty, serializer: S) -> Result<S::Ok, S::Error> {
                serde::Serialize::serialize(&value.to_binary(), serializer)
            }
            pub fn deserialize<'de, D: Deserializer<'de>>(
                deserializer: D,
            ) -> Result<$ty, D::Error> {
                let bytes = <[u8; $size]>::deserialize(deserializer)?;
                $ty::from_binary(&bytes).map_err(de::Error::custom)
            }
        }
    };
}
temporal_adapter!(date, Date, 4);
temporal_adapter!(time, Time, 8);
temporal_adapter!(timetz, TimeTz, 12);
temporal_adapter!(timestamp, Timestamp, 8);
temporal_adapter!(timestamptz, TimestampTz, 8);
temporal_adapter!(interval, Interval, 16);

#[cfg(test)]
mod temporal_tests {
    use crate::relational::*;
    use bincode::Options;
    #[test]
    fn append_only_temporal_tags_and_component_roundtrip() {
        let values = [
            Value::Date(Date::from_days(0).unwrap()),
            Value::Time(Time::from_microseconds(86400000000).unwrap()),
            Value::TimeTz(TimeTz::new(Time::from_microseconds(0).unwrap(), 3600).unwrap()),
            Value::Timestamp(Timestamp::NEG_INFINITY),
            Value::TimestampTz(TimestampTz::INFINITY),
            Value::Interval(Interval::new(1, -30, 1)),
        ];
        for (i, v) in values.iter().enumerate() {
            let bytes = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .serialize(v)
                .unwrap();
            assert_eq!(&bytes[..4], &((i + 8) as u32).to_le_bytes());
            let decoded: Value = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .deserialize(&bytes)
                .unwrap();
            assert_eq!(&decoded, v);
            assert_eq!(
                bytes,
                bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .serialize(&decoded)
                    .unwrap()
            );
        }
        let month = Value::Interval(Interval::new(1, 0, 0));
        let days = Value::Interval(Interval::new(0, 30, 0));
        assert_eq!(month, days);
        assert_ne!(
            bincode::serialize(&month).unwrap(),
            bincode::serialize(&days).unwrap()
        );
        for (i, t) in [
            DataType::Date,
            DataType::Time,
            DataType::TimeTz,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Interval,
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(
                bincode::serialize(t).unwrap(),
                ((i + 9) as u32).to_le_bytes()
            );
        }
    }
}
