use giggles_core::relational::{DataType, Value};
use giggles_server::pgwire::{decode, oid};
#[test]
fn logical_oids_and_binary_inputs() {
    assert_eq!(oid(&DataType::Uuid), 2950);
    assert_eq!(oid(&DataType::Jsonb), 3802);
    assert_eq!(oid(&DataType::IntArray), 1007);
    assert_eq!(
        decode(&[0xab; 16], &DataType::Uuid, 1),
        Ok(Value::Text("abababab-abab-abab-abab-abababababab".into()))
    );
    assert_eq!(
        decode(b"\x01{\"a\":1}", &DataType::Jsonb, 1),
        Ok(Value::Text("{\"a\": 1}".into()))
    );
    assert_eq!(decode(b"\x02{}", &DataType::Jsonb, 1), Err("22P03"));
    assert_eq!(decode(&[0; 15], &DataType::Uuid, 1), Err("22P03"));
    let mut b = Vec::new();
    for n in [1i32, 1, 23, 3, 1, 4, 1, -1, 4, 3] {
        b.extend_from_slice(&n.to_be_bytes());
    }
    assert_eq!(
        decode(&b, &DataType::IntArray, 1),
        Ok(Value::Text("{1,NULL,3}".into()))
    );
    b.push(0);
    assert_eq!(decode(&b, &DataType::IntArray, 1), Err("22P03"));
    for (ty, bad) in [
        (DataType::Uuid, "no"),
        (DataType::Jsonb, "{x}"),
        (DataType::IntArray, "{x}"),
    ] {
        assert_eq!(decode(bad.as_bytes(), &ty, 0), Err("22P02"));
    }
}
