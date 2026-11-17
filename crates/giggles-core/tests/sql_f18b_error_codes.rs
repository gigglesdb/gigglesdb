use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError as E};

const TAG: u8 = 52;
const FIRST_UNKNOWN_TAG: u8 = 59; // next free SqlError tag; bump when appending

#[test]
fn f18b_error_contract_goldens() {
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    let error = E::InvalidTimeZoneDisplacementValue;
    let name = "InvalidTimeZoneDisplacementValue";
    assert_eq!(local::failure(CoreError::Sql(error.clone())).code, 252);
    let json = format!("\"{name}\"");
    assert_eq!(serde_json::to_string(&error).unwrap(), json);
    assert_eq!(serde_json::from_str::<E>(&json).unwrap(), error);
    assert_eq!(
        serde_json::from_str::<E>(&format!("{{\"{name}\":null}}")).unwrap(),
        error
    );
    assert_eq!(le.serialize(&error).unwrap(), [TAG, 0, 0, 0]);
    assert_eq!(be.serialize(&error).unwrap(), [0, 0, 0, TAG]);
    assert_eq!(le.deserialize::<E>(&[TAG, 0, 0, 0]).unwrap(), error);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, TAG]).unwrap(), error);
    assert!(le.deserialize::<E>(&[TAG, 0, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, TAG, 0]).is_err());
    assert_eq!(le.deserialize::<E>(&[58, 0, 0, 0]).unwrap(), E::InvalidRecursion);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 58]).unwrap(), E::InvalidRecursion);
    assert_eq!(le.serialize(&E::InvalidRecursion).unwrap(), [58, 0, 0, 0]);
    assert_eq!(be.serialize(&E::InvalidRecursion).unwrap(), [0, 0, 0, 58]);
    assert_eq!(local::failure(CoreError::Sql(E::InvalidRecursion)).code, 258);
    assert_eq!(serde_json::to_string(&E::InvalidRecursion).unwrap(), "\"InvalidRecursion\"");
    assert!(le.deserialize::<E>(&[FIRST_UNKNOWN_TAG, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, FIRST_UNKNOWN_TAG]).is_err());
}
