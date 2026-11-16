//! Evidence-bounded PostgreSQL identity mappings for a GigglesDB destination.

use super::{
    Classification, DomainConstraints, IdentityMapping, Mapping, MappingError, PgType, Value,
};

const EVIDENCE: &str = "GD-0077 E1, giggles-server 61f77012";

/// Matching descriptors are required, including enum and domain definitions.
/// Column typmods/collations require separate preflight, as for IdentityMapping.
pub struct GigglesdbMapping<'a> {
    identity: IdentityMapping<'a>,
}

impl<'a> GigglesdbMapping<'a> {
    pub fn between(source: PgType, destination: &PgType) -> Result<Self, MappingError> {
        Ok(Self {
            identity: IdentityMapping::between(source, destination)?,
        })
    }

    pub fn with_constraints(mut self, constraints: &'a dyn DomainConstraints) -> Self {
        self.identity = self.identity.with_constraints(constraints);
        self
    }

    pub fn pg_type(&self) -> &PgType {
        self.identity.pg_type()
    }
}

// Qualification table: GD-0077 E1, giggles-server 61f77012. Keep all type
// decisions here. UTF8, enum definitions and int4-backed checked domains were
// measured; other encodings/domain bases need their own qualification. Array
// rank and lower bounds live on Value and are checked separately below.
fn qualification(ty: &PgType) -> (Classification, &'static str) {
    use Classification::{Supported, Unsupported, Unverified};
    match ty {
        PgType::Bool
        | PgType::Int2
        | PgType::Int4
        | PgType::Int8
        | PgType::Numeric
        | PgType::Float4
        | PgType::Float8
        | PgType::Bytea
        | PgType::Uuid
        | PgType::Date
        | PgType::Time
        | PgType::Timetz
        | PgType::Timestamp
        | PgType::Timestamptz
        | PgType::Interval
        | PgType::Jsonb => (Supported, "exact scalar round trip"),
        PgType::Text { encoding }
        | PgType::Varchar { encoding }
        | PgType::Char { encoding }
        | PgType::Enum { encoding, .. }
            if encoding == "UTF8" =>
        {
            (Supported, "exact UTF8 storage round trip")
        }
        PgType::Text { .. }
        | PgType::Varchar { .. }
        | PgType::Char { .. }
        | PgType::Enum { .. } => (Unverified, "encoding not measured"),
        PgType::Domain { base, .. } if **base == PgType::Int4 => (
            Supported,
            "checked int4 domain round trip; evaluator required",
        ),
        PgType::Domain { .. } => (Unverified, "domain base not measured"),
        PgType::Json => (
            Unsupported,
            "json DDL rejected (42704); jsonb loses the text contract",
        ),
        PgType::Array(element) => match element.as_ref() {
            PgType::Int4 | PgType::Int8 => (Supported, "one dimension, lower bound 1 only"),
            PgType::Text { encoding } if encoding == "UTF8" => {
                (Supported, "one dimension, lower bound 1 only")
            }
            PgType::Enum { .. } | PgType::Domain { .. } => {
                (Unsupported, "enum/domain array DDL rejected (42601)")
            }
            PgType::Array(_) => (Unsupported, "multidimensional arrays not qualified"),
            _ => (
                Unsupported,
                "array type not qualified; other array DDL rejected (0A000)",
            ),
        },
        PgType::Other { .. } => (Unverified, "type not measured"),
    }
}

impl Mapping for GigglesdbMapping<'_> {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        Ok(value.clone())
    }

    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.forward(value)
    }

    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        let (classification, reason) = qualification(self.pg_type());
        let diagnostic = || format!("{:?}: {reason}; {EVIDENCE}", self.pg_type());
        match classification {
            Classification::Unsupported => return Err(MappingError::Unsupported(diagnostic())),
            Classification::Unverified => return Err(MappingError::Unverified(diagnostic())),
            Classification::Supported => {}
        }
        if let (PgType::Array(element), Value::Array(array)) = (self.pg_type(), value) {
            if array.dimensions.len() > 1 {
                return Err(MappingError::Unsupported(format!(
                    "{:?}: {} dimensions; multidimensional arrays not qualified; {EVIDENCE}",
                    self.pg_type(),
                    array.dimensions.len()
                )));
            }
            for (index, dimension) in array.dimensions.iter().enumerate() {
                if dimension.lower_bound != 1 {
                    return Err(MappingError::OutOfDomain(format!(
                        "{:?}: dimension {index} lower bound {} != 1; {EVIDENCE} Bind rejects non-default bounds (22P02/22P03)",
                        self.pg_type(),
                        dimension.lower_bound
                    )));
                }
            }
            if element != &array.element_type {
                return Err(MappingError::OutOfDomain(
                    "array element type definition differs".into(),
                ));
            }
            for (index, element_value) in array.elements.iter().enumerate() {
                element.check(element_value, None, 0).map_err(|error| {
                    MappingError::OutOfDomain(format!(
                        "array element {index} ({element:?}): {error}"
                    ))
                })?;
            }
        }
        self.identity.domain_check(value)
    }

    fn is_reversible(&self) -> bool {
        self.classification() == Classification::Supported
    }

    fn classification(&self) -> Classification {
        let (classification, _) = qualification(self.pg_type());
        if classification == Classification::Supported {
            self.identity.classification()
        } else {
            classification
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{Array, ArrayDimension, TypeIdentity};

    fn utf8() -> String {
        "UTF8".into()
    }
    fn id(name: &str) -> TypeIdentity {
        TypeIdentity {
            schema: "public".into(),
            name: name.into(),
            definition_digest: [7; 32],
        }
    }
    fn enumeration() -> PgType {
        PgType::Enum {
            type_id: id("n4_enum"),
            labels: vec![b"red".to_vec(), b"blue".to_vec()],
            encoding: utf8(),
        }
    }
    fn domain() -> PgType {
        PgType::Domain {
            type_id: id("n4_domain"),
            base: Box::new(PgType::Int4),
            constraint_ref: "VALUE > 0".into(),
        }
    }
    struct Positive;
    impl DomainConstraints for Positive {
        fn check(
            &self,
            type_id: &TypeIdentity,
            constraint_ref: &str,
            base: &Value,
        ) -> Result<(), MappingError> {
            if type_id == &id("n4_domain")
                && constraint_ref == "VALUE > 0"
                && matches!(base, Value::Null | Value::Int4(1..))
            {
                Ok(())
            } else {
                Err(MappingError::OutOfDomain(
                    "n4_domain CHECK VALUE > 0".into(),
                ))
            }
        }
    }
    fn scalar_cases() -> Vec<(PgType, Vec<&'static str>)> {
        vec![
            (PgType::Bool, vec!["true", "false"]),
            (PgType::Int2, vec!["-32768", "32767"]),
            (PgType::Int4, vec!["-2147483648", "2147483647"]),
            (
                PgType::Int8,
                vec!["-9223372036854775808", "9223372036854775807"],
            ),
            (
                PgType::Numeric,
                vec![
                    "12345678901234567890.123400",
                    "NaN",
                    "Infinity",
                    "-Infinity",
                ],
            ),
            (
                PgType::Float4,
                vec!["1.25", "-0", "NaN", "Infinity", "-Infinity"],
            ),
            (
                PgType::Float8,
                vec!["1.25", "-0", "NaN", "Infinity", "-Infinity"],
            ),
            (
                PgType::Text { encoding: utf8() },
                vec!["", "h\u{e9}llo \u{1f680}"],
            ),
            (PgType::Varchar { encoding: utf8() }, vec!["", "h\u{e9}llo"]),
            (PgType::Char { encoding: utf8() }, vec!["ab   ", "     "]),
            (PgType::Bytea, vec!["\\x", "\\x0001ff"]),
            (PgType::Uuid, vec!["00112233-4455-6677-8899-aabbccddeeff"]),
            (PgType::Date, vec!["2024-02-29", "infinity", "-infinity"]),
            (
                PgType::Time,
                vec!["00:00:00", "24:00:00", "12:34:56.123456"],
            ),
            (PgType::Timetz, vec!["12:34:56.123456+02", "00:00:00-05:30"]),
            (
                PgType::Timestamp,
                vec!["2024-02-29 12:34:56.123456", "infinity", "-infinity"],
            ),
            (
                PgType::Timestamptz,
                vec!["2024-02-29 12:34:56.123456+00", "infinity", "-infinity"],
            ),
            (
                PgType::Interval,
                vec![
                    "1 year 2 mons 3 days 04:05:06.123456",
                    "-2 mons -3 days -04:05:06",
                ],
            ),
            (PgType::Jsonb, vec!["{\"b\":2,\"a\":1}", "null", "[]"]),
            (enumeration(), vec!["red", "blue"]),
            (domain(), vec!["1", "42", "2147483647"]),
        ]
    }

    #[test]
    fn every_qualified_scalar_is_checked_identity() {
        let cases = scalar_cases();
        assert_eq!(cases.len(), 21);
        for (ty, inputs) in cases {
            let mapping = GigglesdbMapping::between(ty.clone(), &ty)
                .unwrap()
                .with_constraints(&Positive);
            assert_eq!(
                mapping.classification(),
                Classification::Supported,
                "{ty:?}"
            );
            assert!(mapping.is_reversible());
            for input in inputs.into_iter().map(Some).chain([None]) {
                let value = Value::parse_pg_text(&ty, input.map(str::as_bytes)).unwrap();
                assert_eq!(mapping.forward(&value).unwrap(), value, "{ty:?}: {input:?}");
                assert_eq!(mapping.inverse(&value).unwrap(), value);
            }
            assert!(
                mapping
                    .forward(&Value::Array(Array {
                        element_type: Box::new(PgType::Int4),
                        dimensions: vec![],
                        elements: vec![]
                    }))
                    .is_err()
            );
        }
    }

    #[test]
    fn every_unqualified_type_blocks_even_null_in_both_directions() {
        let mut unsupported = vec![PgType::Json];
        unsupported.extend(scalar_cases().into_iter().map(|(ty, _)| PgType::Array(Box::new(ty))).filter(|ty| {
            !matches!(ty, PgType::Array(t) if matches!(t.as_ref(), PgType::Int4 | PgType::Int8 | PgType::Text { .. }))
        }));
        unsupported.push(PgType::Array(Box::new(PgType::Json)));
        unsupported.push(PgType::Array(Box::new(PgType::Array(Box::new(
            PgType::Int4,
        )))));
        unsupported.push(PgType::Array(Box::new(PgType::Other {
            name: "inet".into(),
            known_lossy: false,
        })));
        let unverified = vec![
            PgType::Other {
                name: "inet".into(),
                known_lossy: false,
            },
            PgType::Other {
                name: "lossy_extension".into(),
                known_lossy: true,
            },
            PgType::Text {
                encoding: "LATIN1".into(),
            },
            PgType::Varchar {
                encoding: "LATIN1".into(),
            },
            PgType::Char {
                encoding: "LATIN1".into(),
            },
            PgType::Enum {
                type_id: id("latin_enum"),
                labels: vec![b"red".to_vec()],
                encoding: "LATIN1".into(),
            },
            PgType::Domain {
                type_id: id("text_domain"),
                base: Box::new(PgType::Text { encoding: utf8() }),
                constraint_ref: "length(VALUE) > 0".into(),
            },
        ];
        for (types, expected) in [
            (unsupported, Classification::Unsupported),
            (unverified, Classification::Unverified),
        ] {
            for ty in types {
                let mapping = GigglesdbMapping::between(ty.clone(), &ty)
                    .unwrap()
                    .with_constraints(&Positive);
                assert_eq!(mapping.classification(), expected, "{ty:?}");
                assert!(!mapping.is_reversible());
                for result in [mapping.forward(&Value::Null), mapping.inverse(&Value::Null)] {
                    let error = result.unwrap_err();
                    assert!(matches!(
                        (&error, expected),
                        (MappingError::Unsupported(_), Classification::Unsupported)
                            | (MappingError::Unverified(_), Classification::Unverified)
                    ));
                    assert!(error.to_string().contains(EVIDENCE));
                    assert!(error.to_string().contains(&format!("{ty:?}")));
                }
            }
        }
    }

    #[test]
    fn qualified_arrays_preserve_values_and_null_elements() {
        for (element, input) in [
            (PgType::Int4, "{-2147483648,NULL,2147483647}"),
            (
                PgType::Int8,
                "{-9223372036854775808,NULL,9223372036854775807}",
            ),
            (
                PgType::Text { encoding: utf8() },
                "{\"\",NULL,\"h\u{e9}llo\"}",
            ),
        ] {
            let ty = PgType::Array(Box::new(element));
            let mapping = GigglesdbMapping::between(ty.clone(), &ty).unwrap();
            assert_eq!(mapping.classification(), Classification::Supported);
            assert!(mapping.is_reversible());
            for input in [Some(input), Some("{}"), None] {
                let value = Value::parse_pg_text(&ty, input.map(str::as_bytes)).unwrap();
                assert_eq!(mapping.forward(&value).unwrap(), value);
                assert_eq!(mapping.inverse(&value).unwrap(), value);
            }
        }
    }

    #[test]
    fn array_bounds_rank_shape_and_element_domain_block() {
        for element in [
            PgType::Int4,
            PgType::Int8,
            PgType::Text { encoding: utf8() },
        ] {
            let ty = PgType::Array(Box::new(element.clone()));
            let mapping = GigglesdbMapping::between(ty.clone(), &ty).unwrap();
            let valid = Value::parse_pg_text(&ty, Some(b"{NULL}")).unwrap();
            let Value::Array(array) = valid else {
                unreachable!()
            };
            for bound in [i32::MIN, -1, 0, 2, i32::MAX] {
                let mut bad = array.clone();
                bad.dimensions[0].lower_bound = bound;
                let value = Value::Array(bad);
                for error in [
                    mapping.forward(&value).unwrap_err(),
                    mapping.inverse(&value).unwrap_err(),
                ] {
                    assert!(matches!(error, MappingError::OutOfDomain(_)));
                    assert!(error.to_string().contains(&format!("lower bound {bound}")));
                }
            }
            let mut bad = array.clone();
            bad.dimensions.push(ArrayDimension {
                length: 1,
                lower_bound: 1,
            });
            assert!(
                matches!(mapping.forward(&Value::Array(bad)), Err(MappingError::Unsupported(message)) if message.contains("2 dimensions"))
            );
            let mut bad = array.clone();
            bad.elements[0] = Value::Bool(true);
            assert!(
                matches!(mapping.forward(&Value::Array(bad)), Err(MappingError::OutOfDomain(message)) if message.contains("array element 0"))
            );
            let mut bad = array.clone();
            bad.element_type = Box::new(PgType::Bool);
            assert!(
                matches!(mapping.forward(&Value::Array(bad)), Err(MappingError::OutOfDomain(message)) if message.contains("element type definition"))
            );
            let mut bad = array.clone();
            bad.elements.clear();
            assert!(
                matches!(mapping.forward(&Value::Array(bad)), Err(MappingError::Invalid(message)) if message.contains("array shape"))
            );
            if matches!(element, PgType::Text { .. }) {
                let mut bad = array;
                bad.elements[0] = Value::Text(super::super::EncodedBytes {
                    bytes: vec![0],
                    encoding: utf8(),
                });
                assert!(
                    matches!(mapping.forward(&Value::Array(bad)), Err(MappingError::OutOfDomain(message)) if message.contains("array element 0") && message.contains("NUL"))
                );
            }
        }
    }

    #[test]
    fn checked_domains_require_evaluation_and_reject_bad_values() {
        let ty = domain();
        let mapping = GigglesdbMapping::between(ty.clone(), &ty).unwrap();
        assert_eq!(mapping.classification(), Classification::Unverified);
        assert!(!mapping.is_reversible());
        assert!(matches!(
            mapping.forward(&Value::Null),
            Err(MappingError::Unverified(_))
        ));
        let mapping = mapping.with_constraints(&Positive);
        let value = Value::parse_pg_text(&ty, Some(b"-1")).unwrap();
        assert!(
            matches!(mapping.forward(&value), Err(MappingError::OutOfDomain(message)) if message.contains("CHECK"))
        );
        let invalid_enum = Value::parse_pg_text(&enumeration(), Some(b"missing"));
        assert!(matches!(invalid_enum, Err(MappingError::OutOfDomain(_))));
    }

    #[test]
    fn mismatched_descriptors_are_refused() {
        assert!(GigglesdbMapping::between(PgType::Int4, &PgType::Int8).is_err());
        let source = enumeration();
        let mut destination = source.clone();
        if let PgType::Enum { labels, .. } = &mut destination {
            labels.push(b"green".to_vec());
        }
        assert!(GigglesdbMapping::between(source, &destination).is_err());
        let source = domain();
        let mut destination = source.clone();
        if let PgType::Domain { type_id, .. } = &mut destination {
            type_id.definition_digest[0] ^= 1;
        }
        assert!(GigglesdbMapping::between(source, &destination).is_err());
    }
}
