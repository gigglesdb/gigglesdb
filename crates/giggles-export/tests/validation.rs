mod common;
use common::*;
use giggles_core::relational::{DataType, Value};
use giggles_export::*;
use sha2::{Digest, Sha256};
fn rehash(lines: &mut [serde_json::Value]) {
    let prefix = lines[..lines.len() - 1]
        .iter()
        .map(|l| format!("{}\n", serde_json::to_string(l).unwrap()))
        .collect::<String>();
    let digest = Sha256::digest(prefix.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    lines.last_mut().unwrap()["sha256"] = digest.into();
}
// Value maps sort fields; use textual edits for canonical wire mutation tests.
fn bytes() -> Vec<u8> {
    encode(&sample(), Bounds::default()).unwrap()
}
fn resign_prefix(prefix: &str, counts: &[usize]) -> Vec<u8> {
    let hash = Sha256::digest(prefix.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    format!(
        "{prefix}{{\"record\":\"manifest\",\"sha256\":\"{hash}\",\"table_rows\":{},\"rows\":{}}}\n",
        serde_json::to_string(counts).unwrap(),
        counts.iter().sum::<usize>()
    )
    .into_bytes()
}
#[test]
fn primary_column_rejected_before_destination_creation() {
    let text = String::from_utf8(bytes()).unwrap();
    let prefix = text.split("{\"record\":\"manifest\"").next().unwrap();
    let invalid = resign_prefix(
        &prefix.replacen("\"name\":\"id\"", "\"name\":\"primary\"", 1),
        &[2],
    );
    let p = temp("primary-column-preflight");
    let input = p.join("input.jsonl");
    let target = p.join("target");
    std::fs::write(&input, &invalid).unwrap();
    assert!(import(&input, &target, Bounds::default()).is_err());
    assert!(
        !target.exists(),
        "invalid schema must not create destination"
    );
    assert!(decode(&invalid, Bounds::default()).is_err());
    let mut archive = sample();
    archive.tables[0].columns[0].name = "primary".into();
    assert!(encode(&archive, Bounds::default()).is_err());
}

#[test]
fn legal_keyword_identifiers_remain_importable() {
    for name in ["count", "sum", "min", "max", "key"] {
        let mut archive = sample();
        archive.tables[0].name = "primary".into();
        archive.tables[0].columns[0].name = name.into();
        let p = temp("keyword-import");
        let input = p.join("input.jsonl");
        std::fs::write(&input, encode(&archive, Bounds::default()).unwrap()).unwrap();
        import(&input, p.join("target"), Bounds::default()).unwrap();
    }
}
#[test]
fn rehashed_semantic_wire_negative_controls() {
    let text = String::from_utf8(bytes()).unwrap();
    let lines = text.lines().collect::<Vec<_>>();
    let valid = format!("{}\n{}\n{}\n", lines[0], lines[1], lines[2]);
    assert!(decode(&resign_prefix(&valid, &[2]), Bounds::default()).is_ok());
    for (prefix, counts, reason) in [
        (
            format!("{}\n{}\n{}\n", lines[0], lines[1], lines[1]),
            vec![2],
            "key",
        ),
        (
            format!("{}\n{}\n{}\n", lines[0], lines[2], lines[1]),
            vec![2],
            "key",
        ),
        (
            valid.replacen("\"value\":\"-1\"", "\"value\":\"01\"", 1),
            vec![2],
            "integer",
        ),
        (
            valid.replacen("\"table\":0", "\"table\":1", 1),
            vec![2],
            "table",
        ),
        (
            valid.replacen("\"type\":\"int\",\"value\":\"-1\"", "\"type\":\"null\"", 1),
            vec![2],
            "type/null",
        ),
        (
            valid.replacen("\"name\":\"items\"", "\"name\":\"ledger_state\"", 1),
            vec![2],
            "reserved",
        ),
    ] {
        match decode(&resign_prefix(&prefix, &counts), Bounds::default()) {
            Err(Error::Invalid(s)) => assert!(s.contains(reason), "{s}: expected {reason}"),
            other => panic!("expected semantic {reason}, got {other:?}"),
        }
    }
}
#[test]
fn schema_maxima_and_exact_row_cap() {
    let mut a = sample();
    a.rows[0] = (0..1024)
        .map(|i| vec![Cell::Int(i.to_string()), Cell::Null])
        .collect();
    assert!(encode(&a, Bounds::default()).is_ok());
    a.rows[0].push(vec![Cell::Int("1024".into()), Cell::Null]);
    assert!(encode(&a, Bounds::default()).is_err());
    let mut a = Archive {
        tables: vec![],
        rows: vec![],
    };
    for i in 0..128 {
        let mut t = sample().tables[0].clone();
        t.name = format!("t{i:03}");
        a.tables.push(t);
        a.rows.push(vec![]);
    }
    assert!(encode(&a, Bounds::default()).is_ok());
    let mut t = sample().tables[0].clone();
    t.name = "t999".into();
    a.tables.push(t);
    a.rows.push(vec![]);
    assert!(encode(&a, Bounds::default()).is_err());
    let mut a = sample();
    a.rows[0].clear();
    a.tables[0].columns = (0..64)
        .map(|i| Column {
            name: format!("c{i}"),
            data_type: DataType::BigInt,
            not_null: true,
        })
        .collect();
    assert!(encode(&a, Bounds::default()).is_ok());
    a.tables[0].columns.push(Column {
        name: "extra".into(),
        data_type: DataType::Text,
        not_null: false,
    });
    assert!(encode(&a, Bounds::default()).is_err());
}
#[test]
fn typed_extremes_and_utf8() {
    for v in [
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::Text("\0雪😀e\u{301}\r\n\"\\".into()),
        Value::Bytes((0..=255).collect()),
    ] {
        let c = Cell::from_value(v.clone());
        assert_eq!(c.to_value().unwrap(), v);
        assert_eq!(
            serde_json::from_slice::<Cell>(&serde_json::to_vec(&c).unwrap()).unwrap(),
            c
        );
    }
    let a = sample();
    assert_eq!(
        decode(&encode(&a, Bounds::default()).unwrap(), Bounds::default()).unwrap(),
        a
    );
}
#[test]
fn all_truncations_reject() {
    let b = bytes();
    for n in 0..b.len() {
        assert!(decode(&b[..n], Bounds::default()).is_err(), "prefix {n}")
    }
    assert!(decode(&b, Bounds::default()).is_ok());
}
#[test]
fn every_single_byte_corruption_rejects() {
    let b = bytes();
    for i in 0..b.len() {
        let mut corrupt = b.clone();
        corrupt[i] ^= 1;
        assert!(decode(&corrupt, Bounds::default()).is_err(), "byte {i}");
    }
}
#[test]
fn duplicate_key_reject() {
    let mut a = sample();
    let duplicate = a.rows[0][1].clone();
    a.rows[0].push(duplicate);
    assert!(encode(&a, Bounds::default()).is_err());
}
#[test]
fn out_of_order_reject() {
    let mut a = sample();
    a.rows[0].reverse();
    assert!(encode(&a, Bounds::default()).is_err());
}
#[test]
fn noncanonical_and_duplicate_fields_reject() {
    let s = String::from_utf8(bytes()).unwrap();
    for altered in [
        s.replacen("\"format\":1", "\"format\":1,\"format\":1", 1),
        s.replacen("\"format\":1", "\"format\": 1", 1),
        s.replacen("\"format\":1", "\"format\":1,\"unknown\":0", 1),
        s.replace("\n", "\r\n"),
        s.replacen("雪", "\\u96ea", 1),
        format!("{s}\n"),
        format!("{s}{s}"),
    ] {
        assert!(decode(altered.as_bytes(), Bounds::default()).is_err())
    }
}
#[test]
fn malformed_values_reject() {
    for text in [
        "",
        "+1",
        "01",
        "-0",
        " 1",
        "9223372036854775808",
        "-9223372036854775809",
        "1.0",
    ] {
        assert!(Cell::Int(text.into()).to_value().is_err())
    }
    for text in ["0", "FF", "gg", "a ", "é"] {
        assert!(Cell::Bytes(text.into()).to_value().is_err())
    }
    assert_eq!(
        Cell::Bytes("".into()).to_value().unwrap(),
        Value::Bytes(vec![])
    );
}
#[test]
fn bounds_reject() {
    let a = sample();
    let b = bytes();
    for bounds in [
        Bounds {
            file_bytes: b.len() - 1,
            ..Bounds::default()
        },
        Bounds {
            line_bytes: 1,
            ..Bounds::default()
        },
        Bounds {
            rows_per_table: 1,
            ..Bounds::default()
        },
        Bounds {
            total_rows: 1,
            ..Bounds::default()
        },
        Bounds {
            tables: 0,
            ..Bounds::default()
        },
        Bounds {
            file_bytes: usize::MAX,
            ..Bounds::default()
        },
    ] {
        assert!(encode(&a, bounds).is_err());
        assert!(decode(&b, bounds).is_err())
    }
    assert!(
        encode(
            &a,
            Bounds {
                file_bytes: b.len(),
                ..Bounds::default()
            }
        )
        .is_ok()
    );
}
#[test]
fn reserved_catalog_reject() {
    for name in [
        "ledger_state",
        "outbox_internal_state",
        "tasks",
        "approvals",
        "receipts",
        "operations",
        "giggles_x",
        "pg_catalog",
        "deliveries",
        "system_x",
        "protected_x",
        "consensus_x",
        "membership_x",
    ] {
        let mut a = sample();
        a.tables[0].name = name.into();
        assert!(encode(&a, Bounds::default()).is_err(), "{name}")
    }
}
#[test]
fn invalid_schema_rejects() {
    let base = sample();
    let mut cases = Vec::new();
    let mut a = base.clone();
    a.tables[0].primary_key = vec![];
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].primary_key = vec![2];
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].primary_key = vec![0, 0];
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].columns[0].not_null = false;
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].columns[1].name = "id".into();
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].name = "bad;drop".into();
    cases.push(a);
    let mut a = base.clone();
    a.tables.push(a.tables[0].clone());
    a.rows.push(vec![]);
    cases.push(a);
    let mut a = base.clone();
    a.rows.clear();
    cases.push(a);
    let mut a = base.clone();
    a.rows[0][0].pop();
    cases.push(a);
    let mut a = base.clone();
    a.rows[0][0][0] = Cell::Null;
    cases.push(a);
    let mut a = base.clone();
    a.rows[0][0][0] = Cell::Bool(true);
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].columns[0].data_type = DataType::SmallInt;
    a.rows[0][1][0] = Cell::Int("32768".into());
    cases.push(a);
    let mut a = base.clone();
    a.tables[0].columns[0].data_type = DataType::Integer;
    a.rows[0][1][0] = Cell::Int("2147483648".into());
    cases.push(a);
    for a in cases {
        assert!(encode(&a, Bounds::default()).is_err());
    }
}
#[test]
fn row_payload_boundary() {
    let mut a = sample();
    a.rows[0].truncate(1);
    a.rows[0][0][1] = Cell::Text("x".repeat(65536 - 9));
    assert!(encode(&a, Bounds::default()).is_ok());
    a.rows[0][0][1] = Cell::Text("x".repeat(65536 - 8));
    assert!(encode(&a, Bounds::default()).is_err());
}
#[test]
fn wrong_manifest_counts_and_version_reject() {
    let s = String::from_utf8(bytes()).unwrap();
    for s in [
        s.replacen("\"format\":1", "\"format\":2", 1),
        s.replace("\"table_rows\":[2]", "\"table_rows\":[1]"),
        s.replace("\"rows\":2", "\"rows\":3"),
        s.replace("giggles-logical-v1", "other"),
    ] {
        assert!(decode(s.as_bytes(), Bounds::default()).is_err())
    }
}
#[test]
fn integrity_valid_but_invalid_records_reject() {
    // A rehashed noncanonical record still fails strict canonical validation.
    let mut records = String::from_utf8(bytes())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect::<Vec<serde_json::Value>>();
    records[1]["values"][0]["value"] = "01".into();
    rehash(&mut records);
    let b = records.iter().map(|r| format!("{r}\n")).collect::<String>();
    assert!(decode(b.as_bytes(), Bounds::default()).is_err());
}
#[test]
fn generated_ordered_roundtrips() {
    let mut seed = 0x4d595df4d0f33173u64;
    for _ in 0..128 {
        let mut a = sample();
        a.rows[0].clear();
        for n in 0..32 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let text = format!("雪-{seed:016x}\0");
            a.rows[0].push(vec![
                Cell::Int((n - 16).to_string()),
                if seed.is_multiple_of(3) {
                    Cell::Null
                } else {
                    Cell::Text(text)
                },
            ]);
        }
        let encoded = encode(&a, Bounds::default()).unwrap();
        assert_eq!(decode(&encoded, Bounds::default()).unwrap(), a);
    }
}
