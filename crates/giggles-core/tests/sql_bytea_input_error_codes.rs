use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError as E};
#[test]
fn bytea_closed_29_goldens() {
    let error = E::CharacterNotInRepertoire;
    assert_eq!(local::failure(CoreError::Sql(error.clone())).code, 228);
    assert_eq!(
        serde_json::to_string(&error).unwrap(),
        "\"CharacterNotInRepertoire\""
    );
    assert_eq!(
        serde_json::from_str::<E>("{\"CharacterNotInRepertoire\":null}").unwrap(),
        error
    );
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    assert_eq!(le.serialize(&error).unwrap(), [28, 0, 0, 0]);
    assert_eq!(be.serialize(&error).unwrap(), [0, 0, 0, 28]);
    assert_eq!(le.deserialize::<E>(&[28, 0, 0, 0]).unwrap(), error);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 28]).unwrap(), error);
    let division = E::DivisionByZero;
    assert_eq!(local::failure(CoreError::Sql(division.clone())).code, 243);
    assert_eq!(
        serde_json::to_string(&division).unwrap(),
        "\"DivisionByZero\""
    );
    assert_eq!(
        serde_json::from_str::<E>("{\"DivisionByZero\":null}").unwrap(),
        division
    );
    assert_eq!(le.serialize(&division).unwrap(), [43, 0, 0, 0]);
    assert_eq!(be.serialize(&division).unwrap(), [0, 0, 0, 43]);
    assert_eq!(le.deserialize::<E>(&[43, 0, 0, 0]).unwrap(), division);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 43]).unwrap(), division);
    assert!(le.deserialize::<E>(&[58, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, 58]).is_err());
    assert!(le.deserialize::<E>(&[28, 0, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, 28, 0]).is_err());
    for value in [
        "{\"CharacterNotInRepertoire\":\"22021\"}",
        "{\"CharacterNotInRepertoire\":[0]}",
        "\"UnknownSqlError\"",
    ] {
        assert!(serde_json::from_str::<E>(value).is_err());
    }
}
