use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError};

#[test]
fn ambiguous_column_append_contract() {
    let error = SqlError::AmbiguousColumn;
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    assert_eq!(le.serialize(&error).unwrap(), [53, 0, 0, 0]);
    assert_eq!(be.serialize(&error).unwrap(), [0, 0, 0, 53]);
    assert_eq!(le.deserialize::<SqlError>(&[53, 0, 0, 0]).unwrap(), error);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 53]).unwrap(), error);
    assert!(le.deserialize::<SqlError>(&[53, 0, 0, 0, 0]).is_err());
    assert!(be.deserialize::<SqlError>(&[0, 0, 0, 53, 0]).is_err());
    assert_eq!(le.deserialize::<SqlError>(&[58, 0, 0, 0]).unwrap(), SqlError::InvalidRecursion);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 58]).unwrap(), SqlError::InvalidRecursion);
    assert_eq!(le.serialize(&SqlError::InvalidRecursion).unwrap(), [58, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidRecursion).unwrap(), [0, 0, 0, 58]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidRecursion)).code, 258);
    assert_eq!(serde_json::to_string(&SqlError::InvalidRecursion).unwrap(), "\"InvalidRecursion\"");
    assert!(le.deserialize::<SqlError>(&[59, 0, 0, 0]).is_err());
    assert!(be.deserialize::<SqlError>(&[0, 0, 0, 59]).is_err());
    assert_eq!(serde_json::to_string(&error).unwrap(), "\"AmbiguousColumn\"");
    assert_eq!(
        serde_json::from_str::<SqlError>("\"AmbiguousColumn\"").unwrap(),
        error
    );
    assert_eq!(local::failure(CoreError::Sql(error)).code, 253);
}
