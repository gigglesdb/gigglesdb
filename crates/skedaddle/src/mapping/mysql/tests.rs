use super::*;
use crate::mapping::EncodedBytes;
use crate::mysql::{
    binlog::{BinlogDecoder, MysqlEvent, RowKind},
    schema::Collation,
};
use std::{collections::BTreeMap, io::Cursor, path::PathBuf};
fn col(ty: &str) -> ColumnDescriptor {
    let t = ty.split(['(', ' ']).next().unwrap();
    let character = matches!(
        t,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
    );
    ColumnDescriptor {
        name: "v".into(),
        ordinal: 1,
        data_type: t.into(),
        column_type: ty.into(),
        character_set: character.then(|| "utf8mb4".into()),
        collation: character.then(|| Collation {
            name: "utf8mb4_0900_ai_ci".into(),
            id: 255,
        }),
        nullable: true,
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    }
}
fn round(p: &MysqlPgColumn, v: V) {
    let pg = forward(p, &v).unwrap();
    let back = inverse(p, &pg).unwrap();
    match (&v, &back) {
        (V::Float(a), V::Float(b)) => assert_eq!(a.to_bits(), b.to_bits()),
        (V::Double(a), V::Double(b)) => assert_eq!(a.to_bits(), b.to_bits()),
        _ => assert_eq!(v, back),
    }
    let formatted = pg.format_pg_text(&p.pg_type);
    if matches!(formatted, Err(MappingError::LossyText(_))) {
        assert!(matches!(pg, Value::Float4(_) | Value::Float8(_)));
    } else {
        formatted.unwrap();
    }
    if let Ok(opaque) = wire::to_opaque(&p.source, &v) {
        let mapped = to_pg_opaque(p, &opaque).unwrap();
        assert_eq!(mapped.type_id, p.type_id);
        assert_eq!(
            mapped.bytes,
            if pg == Value::Null {
                None
            } else {
                Some(pg.canonical_bytes().unwrap())
            }
        );
        let adapter = opaque.bytes.map_or(Value::Null, Value::Bytea);
        assert_eq!(Mapping::forward(p, &adapter).unwrap(), pg);
        assert_eq!(Mapping::inverse(p, &pg).unwrap(), adapter);
    }
}
#[test]
fn integer_widths_year_and_null() {
    for (t, bits) in [
        ("tinyint", 8),
        ("smallint", 16),
        ("mediumint", 24),
        ("int", 32),
        ("bigint", 64),
    ] {
        for u in [false, true] {
            let p = plan_column(&col(&format!("{t}{}", if u { " unsigned" } else { "" }))).unwrap();
            let (lo, hi) = integer_bounds(&p.source);
            for n in [lo, 0, hi] {
                round(
                    &p,
                    if u {
                        V::Unsigned(n as u64)
                    } else {
                        V::Signed(n as i64)
                    },
                );
            }
            if bits < 64 {
                assert!(
                    forward(
                        &p,
                        &if u {
                            V::Unsigned((hi + 1) as u64)
                        } else {
                            V::Signed((hi + 1) as i64)
                        }
                    )
                    .is_err()
                );
            }
            round(&p, V::Null);
        }
    }
    let p = plan_column(&col("tinyint(1)")).unwrap();
    assert_eq!(p.pg_type, PgType::Int2);
    let p = plan_column(&col("year")).unwrap();
    for y in [0, 1901, 2155] {
        round(&p, V::Year(format!("{y:04}")));
    }
    for y in [1, 1900, 2156] {
        assert!(forward(&p, &V::Year(format!("{y:04}"))).is_err());
    }
    let p = plan_column(&col("bigint unsigned")).unwrap();
    for n in ["-1", "18446744073709551616", "1.0", "NaN"] {
        assert!(inverse(&p, &Value::Numeric(Numeric::parse(n).unwrap())).is_err());
    }
    let mut c = col("int");
    c.nullable = false;
    assert!(forward(&plan_column(&c).unwrap(), &V::Null).is_err());
}
#[test]
fn numerics_floats_and_bits() {
    let p = plan_column(&col("decimal(65,30)")).unwrap();
    for s in [
        format!("{}.{}", "9".repeat(35), "9".repeat(30)),
        format!("-{}.{}", "9".repeat(35), "9".repeat(30)),
        format!("0.{}", "0".repeat(30)),
    ] {
        round(&p, V::Decimal(s));
    }
    for s in ["1.2", "NaN", "-0.000000000000000000000000000000"] {
        assert!(forward(&p, &V::Decimal(s.into())).is_err());
    }
    for bits in [
        0,
        0x80000000,
        1,
        0x7fc00000,
        0x7fc00123,
        0x7f800000,
        f32::MAX.to_bits(),
    ] {
        round(
            &plan_column(&col("float")).unwrap(),
            V::Float(f32::from_bits(bits)),
        );
    }
    for bits in [
        0,
        0x8000000000000000,
        1,
        0x7ff8000000000000,
        0x7ff8000000000123,
        0x7ff0000000000000,
        f64::MAX.to_bits(),
    ] {
        round(
            &plan_column(&col("double")).unwrap(),
            V::Double(f64::from_bits(bits)),
        );
    }
    for n in [1, 17, 64] {
        let p = plan_column(&col(&format!("bit({n})"))).unwrap();
        let mut b = vec![255; (n as usize).div_ceil(8)];
        if n % 8 != 0 {
            b[0] = (1 << (n % 8)) - 1;
        }
        round(&p, V::Bit(b));
        assert!(p.supported_with_transformation);
        assert!(inverse(&p, &Value::Bytea(vec![])).is_err());
        if n % 8 != 0 {
            assert!(forward(&p, &V::Bit(vec![255; (n as usize).div_ceil(8)])).is_err());
        }
    }
}
#[test]
fn temporal_precision_bounds_and_zero_dates() {
    for f in 0..=6 {
        let us = if f == 0 {
            0
        } else {
            1_000_000 - 10u32.pow(6 - f)
        };
        for t in ["datetime", "timestamp", "time"] {
            let p = plan_column(&col(&format!("{t}({f})"))).unwrap();
            if t == "datetime" {
                round(&p, V::DateTime(2024, 2, 29, 23, 59, 59, us));
            }
            if t == "timestamp" {
                round(
                    &p,
                    V::Timestamp(format!(
                        "1709251199{}",
                        if f == 0 {
                            String::new()
                        } else {
                            format!(".{:0width$}", us / 10u32.pow(6 - f), width = f as usize)
                        }
                    )),
                );
                assert!(
                    forward(
                        &p,
                        &V::Timestamp(if f == 0 {
                            "0".into()
                        } else {
                            format!("0.{}", "0".repeat(f as usize))
                        })
                    )
                    .is_err()
                );
            }
            if t == "time" {
                for negative in [false, true] {
                    round(
                        &p,
                        V::Time {
                            negative,
                            days: 34,
                            hours: 22,
                            minutes: 59,
                            seconds: 59,
                            micros: 0,
                        },
                    );
                }
            }
            if f < 6 {
                let v = if t == "datetime" {
                    V::DateTime(2024, 1, 1, 0, 0, 0, 1)
                } else if t == "time" {
                    V::Time {
                        negative: false,
                        days: 0,
                        hours: 0,
                        minutes: 0,
                        seconds: 0,
                        micros: 1,
                    }
                } else {
                    V::Timestamp("1.000001".into())
                };
                assert!(forward(&p, &v).is_err());
            }
        }
    }
    for t in ["date", "datetime"] {
        let p = plan_column(&col(t)).unwrap();
        for (y, m, d) in [(0, 0, 0), (2024, 0, 10), (2024, 1, 0), (2023, 2, 29)] {
            assert!(forward(&p, &V::DateTime(y, m, d, 0, 0, 0, 0)).is_err());
        }
    }
    round(
        &plan_column(&col("date")).unwrap(),
        V::DateTime(2024, 2, 29, 0, 0, 0, 0),
    );
    let p = plan_column(&col("time(6)")).unwrap();
    for (months, days, micros) in [
        (1, 0, 0),
        (0, 1, 0),
        (0, 0, 3_020_399_000_001),
        (0, 0, i64::MIN),
    ] {
        assert!(
            inverse(
                &p,
                &Value::Interval {
                    months,
                    days,
                    micros
                }
            )
            .is_err()
        );
    }
    assert!(
        forward(
            &p,
            &V::Time {
                negative: true,
                days: 0,
                hours: 0,
                minutes: 0,
                seconds: 0,
                micros: 0
            }
        )
        .is_err()
    );
}
#[test]
fn text_binary_collations_generated_and_unsupported() {
    for t in [
        "char(8)",
        "varchar(8)",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
    ] {
        let p = plan_column(&col(t)).unwrap();
        for b in [vec![], b"a   ".to_vec(), "😀".as_bytes().to_vec()] {
            round(
                &p,
                V::Text {
                    bytes: b,
                    collation: 255,
                },
            );
        }
    }
    for (co, locale, det) in [
        ("utf8mb4_bin", "C", true),
        ("utf8mb4_0900_as_cs", "und-u-ks-level3", true),
        ("utf8mb4_general_ci", "und-u-ks-level2", false),
        ("utf8mb4_0900_ai_ci", "und-u-ks-level1", false),
    ] {
        let mut c = col("varchar(8)");
        c.collation.as_mut().unwrap().name = co.into();
        c.generated = true;
        let p = plan_column(&c).unwrap();
        let requirement = p.collation.unwrap();
        assert_eq!(requirement.locale, locale);
        assert_eq!(requirement.deterministic, det);
        assert_eq!(requirement.pattern_operations_restricted, !det);
        assert!(p.generated);
    }
    for cs in ["ascii", "utf8mb3"] {
        let mut c = col("text");
        c.character_set = Some(cs.into());
        let p = plan_column(&c).unwrap();
        round(
            &p,
            V::Text {
                bytes: b"ASCII".to_vec(),
                collation: 255,
            },
        );
        assert!(
            forward(
                &p,
                &V::Text {
                    bytes: "😀".as_bytes().to_vec(),
                    collation: 255
                }
            )
            .is_err()
        );
    }
    for cs in ["latin1", "sjis"] {
        let mut c = col("text");
        c.character_set = Some(cs.into());
        assert!(matches!(plan_column(&c), Err(MappingError::Unsupported(_))));
    }
    for t in [
        "binary(3)",
        "varbinary(3)",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
    ] {
        let p = plan_column(&col(t)).unwrap();
        round(&p, V::Binary(vec![1, 0, 0]));
        if t != "binary(3)" {
            round(&p, V::Binary(vec![]));
        }
    }
    let mut c = col("varchar(8)");
    c.character_set = Some("binary".into());
    round(
        &plan_column(&c).unwrap(),
        V::Text {
            bytes: vec![0, 128, 255],
            collation: 255,
        },
    );
    for t in [
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "unknown",
    ] {
        assert!(matches!(plan_column(&col(t)),Err(MappingError::Unsupported(s)) if s.contains(t)));
    }
}
#[test]
fn enum_set_json_and_identity() {
    let p = plan_column(&col("enum('zero','one','two')")).unwrap();
    assert!(p.check.as_ref().unwrap().contains("'zero', 'one', 'two'"));
    round(
        &p,
        V::Enum {
            ordinal: 2,
            label: Some(b"one".to_vec()),
        },
    );
    assert!(
        forward(
            &p,
            &V::Enum {
                ordinal: 1,
                label: Some(b"invalid".to_vec())
            }
        )
        .is_err()
    );
    assert!(
        inverse(
            &p,
            &Value::Text(EncodedBytes {
                encoding: "UTF8".into(),
                bytes: b"invalid".to_vec()
            })
        )
        .is_err()
    );
    let p = plan_column(&col("set('a','b','c')")).unwrap();
    for bitmap in 0..8 {
        round(
            &p,
            V::Set {
                bitmap,
                labels: Some(
                    [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _)| bitmap & (1 << i) != 0)
                        .map(|(_, v)| v)
                        .collect(),
                ),
            },
        );
    }
    for s in ["b,a", "a,x", "a,a"] {
        assert!(
            inverse(
                &p,
                &Value::Text(EncodedBytes {
                    encoding: "UTF8".into(),
                    bytes: s.as_bytes().to_vec()
                })
            )
            .is_err()
        );
    }
    let p = plan_column(&col("json")).unwrap();
    assert!(forward(&p, &V::Json(serde_json::json!(1.0))).is_err());
    round(
        &p,
        V::Json(serde_json::json!({"unicode":"😀","big":18446744073709551615u64,"fraction":1.25})),
    );
    assert!(p.supported_with_transformation);
    let p = plan_column(&col("varchar(64)")).unwrap();
    let hash = Sha256::digest(serde_json::to_vec(&(&p.pg_type, 68i32)).unwrap());
    assert_eq!(p.type_id, format!("skva.v1:{hash:x}"));
    let mut opaque = wire::to_opaque(&p.source, &V::Null).unwrap();
    opaque.type_id = "mysql.v2:varchar(64)".into();
    assert!(to_pg_opaque(&p, &opaque).is_err());
}
fn zoo_event(event: MysqlEvent, maps: &mut BTreeMap<u64, Vec<MysqlPgColumn>>, count: &mut usize) {
    match event {
        MysqlEvent::TableMap(t) if t.schema == b"type_zoo" && t.table == b"zoo" => {
            let types = [
                "bigint",
                "tinyint",
                "tinyint unsigned",
                "smallint",
                "smallint unsigned",
                "mediumint",
                "mediumint unsigned",
                "int",
                "int unsigned",
                "bigint",
                "bigint unsigned",
                "decimal(65,30)",
                "float",
                "double",
                "bit(17)",
                "date",
                "datetime(6)",
                "timestamp(6)",
                "time(6)",
                "year",
                "char(8)",
                "varchar(128)",
                "text",
                "binary(8)",
                "blob",
                "enum('zero','one','two')",
                "set('a','b','c')",
                "json",
                "varchar(8)",
                "mediumtext",
            ];
            assert_eq!(t.columns.len(), types.len());
            maps.insert(
                t.id,
                types
                    .iter()
                    .map(|s| plan_column(&col(s)).unwrap())
                    .collect(),
            );
        }
        MysqlEvent::Rows {
            table_id,
            kind: RowKind::Write,
            images,
            ..
        } => {
            if let Some(plans) = maps.get(&table_id) {
                for (_, after) in images {
                    let row = after.unwrap();
                    assert_eq!(row.len(), plans.len());
                    for (p, v) in plans.iter().zip(row) {
                        round(p, v);
                        *count += 1;
                    }
                }
            }
        }
        MysqlEvent::TransactionPayload(events) => {
            for e in events {
                zoo_event(e, maps, count);
            }
        }
        _ => {}
    }
}
#[test]
fn real_zoo_both_versions_both_gtid_modes() {
    for variant in [
        "mysql80-gtid-on",
        "mysql80-gtid-off",
        "mysql84-gtid-on",
        "mysql84-gtid-off",
    ] {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/mysql")
            .join(variant);
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
        let file = manifest[0]["file"].as_str().unwrap();
        let bytes = std::fs::read(root.join(format!("{file}.binlog"))).unwrap();
        let mut input = Cursor::new(&bytes[4..]);
        let mut decoder = BinlogDecoder::new(16 * 1024 * 1024);
        let mut maps = BTreeMap::new();
        let mut count = 0;
        while let Some(e) = decoder.read(&mut input).unwrap() {
            zoo_event(e, &mut maps, &mut count);
        }
        assert_eq!(count, 60, "{variant}");
    }
}
