use super::*;

// Independent base-row oracle for variable-length scalar successors. Exercise
// actual range endpoints, not just equality prefixes, with complete PK ties.
#[test]
fn secondary_index_qualification_text_bytes_nul_ranges_match_base_oracle() {
    for text in [false, true] {
        let mut s = Relational::default();
        let ddl = if text {
            "CREATE TABLE q (id BIGINT PRIMARY KEY, k TEXT)"
        } else {
            "CREATE TABLE q (id BIGINT PRIMARY KEY, k BYTEA)"
        };
        let p = Prepared::parse(ddl, 1, s.epoch).unwrap();
        s.execute(&p, &[], None).unwrap();
        let t = s.tables["q"].clone();
        s.secondary_configure_test(t.id, "q_by_k", &[t.columns[1].id], || false)
            .unwrap();
        let mut values = vec![Value::Null];
        for bytes in [b"".as_slice(), b"a", b"a\0", b"a\0\0", b"a\0b", b"b"] {
            values.push(if text {
                Value::Text(String::from_utf8(bytes.to_vec()).unwrap())
            } else {
                Value::Bytes(bytes.to_vec())
            });
        }
        for (i, value) in values.iter().enumerate() {
            for tie in [0, 1] {
                let p = Prepared::parse("INSERT INTO q VALUES ($1,$2)", 1, s.epoch).unwrap();
                s.execute(&p, &[Value::Int((i * 2 + tie) as i64), value.clone()], None)
                    .unwrap();
            }
        }
        let projection = [t.columns[0].id, t.columns[1].id];
        for lo in &values {
            for hi in &values {
                for inclusive_lower in [false, true] {
                    for inclusive_upper in [false, true] {
                        let req = Request {
                            equal: &[],
                            range: Some(Range {
                                column: t.columns[1].id,
                                lower: Some(Endpoint {
                                    value: lo,
                                    inclusive: inclusive_lower,
                                }),
                                upper: Some(Endpoint {
                                    value: hi,
                                    inclusive: inclusive_upper,
                                }),
                            }),
                            projection: &projection,
                            descending: false,
                            residuals: &[],
                            max_rows: MAX_ROWS,
                            max_bytes: MAX_RESULT_BYTES,
                        };
                        let low = encode_key(std::slice::from_ref(lo)).unwrap();
                        let high = encode_key(std::slice::from_ref(hi)).unwrap();
                        let mut expected = Vec::new();
                        for (pk, row) in &s.rows[&t.id] {
                            let key = encode_key(&row.values[1..2]).unwrap();
                            if (key > low || (inclusive_lower && key == low))
                                && (key < high || (inclusive_upper && key == high))
                            {
                                let mut order = key;
                                order.extend(pk);
                                expected.push((order, row.clone()));
                            }
                        }
                        expected.sort_by(|a, b| a.0.cmp(&b.0));
                        let actual = s
                            .secondary_read_engine(&t, s.epoch, &req, || false)
                            .unwrap();
                        assert_eq!(
                            actual.rows,
                            expected.into_iter().map(|(_, row)| row).collect::<Vec<_>>()
                        );
                    }
                }
            }
        }
    }
}
