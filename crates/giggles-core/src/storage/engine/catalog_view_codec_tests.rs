use super::*;
fn view() -> SqlViewDef {
    SqlViewDef {
        schema: 1,
        name: "v".into(),
        columns: vec![SqlViewColumn {
            id: 3,
            name: "c".into(),
            storage_type: DataType::Integer,
            pg_type_oid: 23,
            typmod: -1,
            source: Some((2, 4)),
        }],
        select: "SELECT c FROM t".into(),
        dependencies: vec![SqlViewDependency {
            relation: 2,
            columns: vec![4],
        }],
    }
}
#[test]
fn exact_wire_and_all_storage_tags() {
    assert_eq!(view_key(9), [17, 0, 0, 0, 0, 0, 0, 0, 9]);
    let golden = concat!(
        "020000000000000001000000017600000001",
        "000000000000000300000001630200000017ffffffff",
        "0000000f53454c45435420632046524f4d2074",
        "000000010000000000000002000000010000000000000004",
        "00000000000000020000000000000004"
    );
    let bytes: Vec<_> = (0..golden.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&golden[i..i + 2], 16).unwrap())
        .collect();
    assert_eq!(encode_view(&view()).unwrap(), bytes);
    assert_eq!(decode_view(&bytes).unwrap(), view());
    let mut legacy = bytes[..bytes.len()-16].to_vec(); legacy[0] = 1;
    let mut expected = view(); expected.columns[0].source = None;
    assert_eq!(decode_view(&legacy).unwrap(), expected);
    assert_eq!(encode_view(&expected).unwrap(), legacy);
    for end in 0..legacy.len() { assert!(decode_view(&legacy[..end]).is_err()); }
    legacy.push(0); assert!(decode_view(&legacy).is_err());
    for (tag, ty) in STORAGE_TYPES.iter().enumerate() {
        let mut v = view();
        v.columns[0].storage_type = ty.clone();
        let bytes = encode_view(&v).unwrap();
        assert_eq!(bytes[31], tag as u8);
        assert_eq!(decode_view(&bytes).unwrap(), v);
    }
}
#[test]
fn malformed_wire_and_every_truncation_fail_closed() {
    let bytes = encode_view(&view()).unwrap();
    for end in 0..bytes.len() {
        assert!(decode_view(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing[59..63].copy_from_slice(&2u32.to_be_bytes());
    trailing.extend_from_slice(&bytes[63..83]); assert!(decode_view(&trailing).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_view(&trailing).is_err());
    for (offset, value) in [
        (0, 3),
        (9, 255),
        (13, 255),
        (14, 255),
        (31, 22),
        (40, 255),
        (59, 255),
        (71, 255),
        (90, 3),
        (98, 5),
    ] {
        let mut bad = bytes.clone();
        bad[offset] = value;
        assert!(decode_view(&bad).is_err(), "offset {offset}");
    }
}
#[test]
fn bounded_utf8_names_ids_and_dependency_order() {
    let mut v = view();
    v.name = "é".repeat(31);
    v.columns[0].name = "é".into();
    v.select = "x".repeat(MAX_SELECT_BYTES);
    v.schema = 0;
    v.dependencies[0].columns = vec![4, 5];
    assert_eq!(decode_view(&encode_view(&v).unwrap()).unwrap(), v);
    v.select.push('x');
    assert!(encode_view(&v).is_err());
    for change in 0..12 {
        let mut bad = view();
        match change {
            0 => bad.dependencies.push(bad.dependencies[0].clone()),
            1 => bad.name = "x".repeat(64),
            2 => bad.columns.clear(),
            3 => bad.columns[0].id = 0,
            4 => bad.columns[0].name = "\0".into(),
            5 => bad.columns[0].pg_type_oid = 0,
            6 => bad.columns.push(bad.columns[0].clone()),
            7 => bad.select.clear(),
            8 => bad.dependencies[0].relation = 0,
            9 => bad.dependencies[0].columns.push(0),
            10 => bad.columns[0].source = Some((3, 4)),
            _ => bad.columns[0].source = Some((2, 5)),
        }
        assert!(encode_view(&bad).is_err(), "case {change}");
    }
    v.select = "SELECT 1".into();
    for columns in [vec![4, 4], vec![0]] {
        v.dependencies[0].columns = columns;
        assert!(encode_view(&v).is_err());
    }
    let mut v = view();
    v.dependencies.insert(0, SqlViewDependency { relation: 9, columns: vec![] });
    assert!(encode_view(&v).is_err());
}

#[test]
fn mixed_view_source_bindings_are_rejected() {
    let mut v = view();
    let mut column = v.columns[0].clone();
    column.id += 1; column.name = "other".into(); column.source = None;
    v.columns.push(column);
    assert!(encode_view(&v).is_err());
    v.columns.reverse();
    assert!(encode_view(&v).is_err());
}
