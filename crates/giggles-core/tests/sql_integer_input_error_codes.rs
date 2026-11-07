use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError as E};
#[test]
fn integer_input_27_literal_sql_error_tags_names_and_local_codes() {
    let errors = [
        E::Unsupported,
        E::Syntax,
        E::Reserved,
        E::UnknownTable,
        E::UnknownColumn,
        E::Duplicate,
        E::Type,
        E::Null,
        E::Parameters,
        E::Limit,
        E::Unbounded,
        E::Version,
        E::Conflict,
        E::StaleSchema,
        E::Transaction,
        E::Aborted,
        E::Overflow,
        E::UndefinedFunction,
        E::InvalidParameterValue,
        E::InvalidDatetimeFormat,
        E::DatetimeFieldOverflow,
        E::IntervalFieldOverflow,
        E::AmbiguousFunction,
        E::GroupingError,
        E::IndeterminateDatatype,
        E::AmbiguousParameter,
        E::InvalidTextRepresentation,
    ];
    let names = [
        "Unsupported",
        "Syntax",
        "Reserved",
        "UnknownTable",
        "UnknownColumn",
        "Duplicate",
        "Type",
        "Null",
        "Parameters",
        "Limit",
        "Unbounded",
        "Version",
        "Conflict",
        "StaleSchema",
        "Transaction",
        "Aborted",
        "Overflow",
        "UndefinedFunction",
        "InvalidParameterValue",
        "InvalidDatetimeFormat",
        "DatetimeFieldOverflow",
        "IntervalFieldOverflow",
        "AmbiguousFunction",
        "GroupingError",
        "IndeterminateDatatype",
        "AmbiguousParameter",
        "InvalidTextRepresentation",
    ];
    for (i, (e, name)) in errors.into_iter().zip(names).enumerate() {
        assert_eq!(
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_little_endian()
                .serialize(&e)
                .unwrap(),
            (i as u32).to_le_bytes()
        );
        assert_eq!(
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&e)
                .unwrap(),
            (i as u32).to_be_bytes()
        );
        assert_eq!(serde_json::to_value(&e).unwrap(), serde_json::json!(name));
        let reply = local::failure(CoreError::Sql(e));
        assert_eq!(reply.code, 200 + i as u32);
        assert_eq!(reply.version, 1);
    }
}

#[test]
fn integer_input_27_closed_errors_reject_unknown_tags_and_payloads() {
    // Literal protocol identities: do not derive the expected tags from enum order.
    let identities = [
        (0u32, "Unsupported", E::Unsupported),
        (1, "Syntax", E::Syntax),
        (2, "Reserved", E::Reserved),
        (3, "UnknownTable", E::UnknownTable),
        (4, "UnknownColumn", E::UnknownColumn),
        (5, "Duplicate", E::Duplicate),
        (6, "Type", E::Type),
        (7, "Null", E::Null),
        (8, "Parameters", E::Parameters),
        (9, "Limit", E::Limit),
        (10, "Unbounded", E::Unbounded),
        (11, "Version", E::Version),
        (12, "Conflict", E::Conflict),
        (13, "StaleSchema", E::StaleSchema),
        (14, "Transaction", E::Transaction),
        (15, "Aborted", E::Aborted),
        (16, "Overflow", E::Overflow),
        (17, "UndefinedFunction", E::UndefinedFunction),
        (18, "InvalidParameterValue", E::InvalidParameterValue),
        (19, "InvalidDatetimeFormat", E::InvalidDatetimeFormat),
        (20, "DatetimeFieldOverflow", E::DatetimeFieldOverflow),
        (21, "IntervalFieldOverflow", E::IntervalFieldOverflow),
        (22, "AmbiguousFunction", E::AmbiguousFunction),
        (23, "GroupingError", E::GroupingError),
        (24, "IndeterminateDatatype", E::IndeterminateDatatype),
        (25, "AmbiguousParameter", E::AmbiguousParameter),
        (
            26,
            "InvalidTextRepresentation",
            E::InvalidTextRepresentation,
        ),
    ];
    for (tag, name, expected) in identities {
        let le = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_little_endian()
            .reject_trailing_bytes();
        let be = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .reject_trailing_bytes();
        for (bytes, big_endian) in [(tag.to_le_bytes(), false), (tag.to_be_bytes(), true)] {
            let decoded: E = if big_endian {
                be.deserialize(&bytes).unwrap()
            } else {
                le.deserialize(&bytes).unwrap()
            };
            assert_eq!(decoded, expected, "tag {tag}, big_endian={big_endian}");
            let encoded = if big_endian {
                be.serialize(&decoded)
            } else {
                le.serialize(&decoded)
            }
            .unwrap();
            assert_eq!(encoded, bytes);
            for length in 0..4 {
                let result = if big_endian {
                    be.deserialize::<E>(&bytes[..length])
                } else {
                    le.deserialize::<E>(&bytes[..length])
                };
                assert!(result.is_err(), "truncated tag {tag} length {length}");
            }
            let mut payload = bytes.to_vec();
            payload.push(0);
            let result = if big_endian {
                be.deserialize::<E>(&payload)
            } else {
                le.deserialize::<E>(&payload)
            };
            assert!(result.is_err(), "payload-bearing tag {tag}");
        }
        let json = serde_json::to_string(name).unwrap();
        assert_eq!(serde_json::from_str::<E>(&json).unwrap(), expected);
        assert_eq!(serde_json::to_string(&expected).unwrap(), json);
        for payload in ["0", "true", "[]", "{}", "\"payload\""] {
            let malformed = format!("{{{json}:{payload}}}");
            assert!(
                serde_json::from_str::<E>(&malformed).is_err(),
                "{malformed}"
            );
        }
        assert!(serde_json::from_str::<E>(&format!("{json} null")).is_err());
        assert!(serde_json::from_str::<E>(&json[..json.len() - 1]).is_err());
    }
    for tag in [43u32, u32::MAX] {
        assert!(
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_little_endian()
                .reject_trailing_bytes()
                .deserialize::<E>(&tag.to_le_bytes())
                .is_err()
        );
        assert!(
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .reject_trailing_bytes()
                .deserialize::<E>(&tag.to_be_bytes())
                .is_err()
        );
    }
    for json in [
        "\"UnknownSqlError\"",
        "\"\"",
        "25",
        "null",
        "[]",
        "{}",
        "{\"UnknownSqlError\":null}",
    ] {
        assert!(serde_json::from_str::<E>(json).is_err(), "{json}");
    }
}
