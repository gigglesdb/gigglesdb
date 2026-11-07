use std::{borrow::Cow, cmp::Ordering};

use giggles_pg_types::character::{
    CharKind, CharTypmod, MAX_CHAR_LENGTH, bpchar_char_length, bpchar_cmp, bpchar_eq, bpchar_trim,
    coerce_assignment, coerce_explicit,
};
use proptest::prelude::*;

#[test]
fn declaration_bounds_and_display() {
    for (kind, name, display) in [
        (CharKind::Varchar, "varchar", "character varying"),
        (CharKind::Bpchar, "char", "character"),
    ] {
        for length in [i32::MIN, -1, 0] {
            let error = CharTypmod::new(kind, length).unwrap_err();
            assert_eq!(error.sqlstate(), "22023");
            assert_eq!(
                error.to_string(),
                format!("length for type {name} must be at least 1")
            );
        }
        for length in [MAX_CHAR_LENGTH + 1, i32::MAX] {
            let error = CharTypmod::new(kind, length).unwrap_err();
            assert_eq!(error.sqlstate(), "22023");
            assert_eq!(
                error.to_string(),
                format!("length for type {name} cannot exceed 10485760")
            );
        }
        let typmod = CharTypmod::new(kind, 3).unwrap();
        assert_eq!(typmod.kind(), kind);
        assert_eq!(typmod.length(), 3);
        assert_eq!(typmod.to_string(), "(3)");
        assert_eq!(kind.to_string(), display);
        assert_eq!(typmod.to_typmod(), 7);
        assert!(CharTypmod::from_typmod(kind, 3).is_none());
        assert_eq!(CharTypmod::from_typmod(kind, 4).unwrap().length(), 0);
        assert_eq!(
            CharTypmod::from_typmod(kind, i32::MAX).unwrap().to_typmod(),
            i32::MAX
        );
    }
}

#[test]
fn unicode_space_and_borrowing_semantics() {
    let varchar = CharTypmod::new(CharKind::Varchar, 2).unwrap();
    let bpchar = CharTypmod::new(CharKind::Bpchar, 2).unwrap();
    assert!(matches!(
        coerce_assignment("é🦀", varchar).unwrap(),
        Cow::Borrowed(_)
    ));
    assert_eq!(coerce_assignment("é🦀  ", varchar).unwrap(), "é🦀");
    assert_eq!(coerce_explicit("é🦀x", varchar), "é🦀");
    assert_eq!(coerce_assignment("é", bpchar).unwrap(), "é ");
    assert_eq!(coerce_explicit("", bpchar), "  ");
    for suffix in ["\t", "\n", "\u{a0}", "\u{3000}", "x "] {
        let error = coerce_assignment(&format!("é🦀{suffix}"), varchar).unwrap_err();
        assert_eq!(error.sqlstate(), "22001");
        assert_eq!(
            error.message(),
            "value too long for type character varying(2)"
        );
        assert_eq!(error.to_string(), error.message());
    }
    assert_eq!(
        coerce_assignment("abc", bpchar).unwrap_err().message(),
        "value too long for type character(2)"
    );
    let zero = CharTypmod::from_typmod(CharKind::Bpchar, 4).unwrap();
    assert_eq!(coerce_assignment("  ", zero).unwrap(), "");
    assert_eq!(coerce_explicit("é", zero), "");
}

#[test]
fn trimmed_comparison_and_length() {
    assert_eq!(bpchar_trim("é\t\u{a0}  "), "é\t\u{a0}");
    assert!(bpchar_eq("é", "é  "));
    assert!(!bpchar_eq("é", "é\t"));
    assert_eq!(bpchar_cmp("a  ", "b"), Ordering::Less);
    assert_eq!(bpchar_cmp("é  ", "z"), Ordering::Greater);
    assert_eq!(bpchar_cmp("a", "a  "), Ordering::Equal);
    assert_eq!(bpchar_char_length("é🦀  "), 2);
    assert_eq!(bpchar_char_length("   "), 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn full_valid_range_typmod_roundtrip(length in 1..=MAX_CHAR_LENGTH, bpchar in any::<bool>()) {
        let kind = if bpchar { CharKind::Bpchar } else { CharKind::Varchar };
        let modifier = CharTypmod::new(kind, length).unwrap();
        prop_assert_eq!(CharTypmod::from_typmod(kind, modifier.to_typmod()), Some(modifier));
    }
}

#[test]
fn postgres_character_oracle() {
    let mut checked = std::collections::BTreeMap::<&str, usize>::new();
    for line in include_str!("fixtures/typmod_pg18.tsv").lines() {
        let fields: Vec<_> = line.split('\t').collect();
        let operation = fields[0];
        if operation == "bpchar_eq" {
            assert_eq!(fields.len(), 6);
            assert_eq!(
                bpchar_eq(fields[1], fields[2]),
                match fields[3] {
                    "t" | "true" => true,
                    "f" | "false" => false,
                    other => panic!("invalid oracle boolean: {other}"),
                },
                "{line:?}"
            );
            *checked.entry(operation).or_default() += 1;
            continue;
        }
        if operation == "bpchar_order" {
            assert_eq!(fields.len(), 6);
            let actual = match bpchar_cmp(fields[1], fields[2]) {
                Ordering::Less => "<",
                Ordering::Equal => "=",
                Ordering::Greater => ">",
            };
            assert_eq!(actual, fields[3], "{line:?}");
            *checked.entry(operation).or_default() += 1;
            continue;
        }
        if operation == "typmod" && matches!(fields[1], "varchar(20)" | "char(5)" | "char") {
            let (kind, length) = match fields[1] {
                "varchar(20)" => (CharKind::Varchar, 20),
                "char(5)" => (CharKind::Bpchar, 5),
                _ => (CharKind::Bpchar, 1),
            };
            let modifier = CharTypmod::new(kind, length).unwrap();
            assert_eq!(
                format!("{},{}{}", modifier.to_typmod(), kind, modifier),
                fields[3],
                "{line:?}"
            );
            *checked.entry(operation).or_default() += 1;
            continue;
        }
        if operation == "bpchar_lengths" {
            assert_eq!(fields.len(), 6);
            let typmod = CharTypmod::new(CharKind::Bpchar, fields[2].parse().unwrap()).unwrap();
            let value = coerce_explicit(fields[1], typmod);
            assert_eq!(
                format!("{},{}", bpchar_char_length(&value), value.len()),
                fields[3],
                "{line:?}"
            );
            *checked.entry(operation).or_default() += 1;
            continue;
        }
        if !matches!(
            operation,
            "char_assignment" | "varchar_assignment" | "char_cast" | "varchar_cast"
        ) {
            continue;
        }
        assert_eq!(fields.len(), 6, "malformed oracle row: {line:?}");
        let kind = if operation.starts_with("varchar") {
            CharKind::Varchar
        } else {
            CharKind::Bpchar
        };
        let typmod = CharTypmod::new(kind, fields[2].parse().unwrap()).unwrap();
        let result = if operation.ends_with("assignment") {
            coerce_assignment(fields[1], typmod)
        } else {
            Ok(coerce_explicit(fields[1], typmod))
        };
        if fields[3] == "ERROR" {
            let error = result.unwrap_err();
            assert_eq!(error.sqlstate(), fields[4], "{line:?}");
            if !fields[5].is_empty() {
                assert_eq!(error.message(), fields[5], "{line:?}");
            }
        } else {
            assert_eq!(result.unwrap(), fields[3], "{line:?}");
        }
        *checked.entry(operation).or_default() += 1;
    }
    for (kind, minimum) in [
        ("char_assignment", 8),
        ("varchar_assignment", 8),
        ("char_cast", 8),
        ("varchar_cast", 8),
        ("bpchar_eq", 6),
        ("bpchar_order", 6),
        ("bpchar_lengths", 8),
        ("typmod", 3),
    ] {
        assert!(
            checked.get(kind).copied().unwrap_or(0) >= minimum,
            "missing {kind} oracle coverage"
        );
    }
}
