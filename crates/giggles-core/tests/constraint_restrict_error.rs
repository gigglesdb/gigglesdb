use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError};

#[test]
fn restrict_violation_appends_wire_tag_46() {
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    assert_eq!(
        le.serialize(&SqlError::RestrictViolation).unwrap(),
        [46, 0, 0, 0]
    );
    assert_eq!(
        be.serialize(&SqlError::RestrictViolation).unwrap(),
        [0, 0, 0, 46]
    );
    assert_eq!(
        le.deserialize::<SqlError>(&[46, 0, 0, 0]).unwrap(),
        SqlError::RestrictViolation
    );
    assert_eq!(
        be.deserialize::<SqlError>(&[0, 0, 0, 46]).unwrap(),
        SqlError::RestrictViolation
    );
    assert_eq!(
        local::failure(CoreError::Sql(SqlError::RestrictViolation)).code,
        246
    );
    assert_eq!(
        serde_json::to_string(&SqlError::RestrictViolation).unwrap(),
        "\"RestrictViolation\""
    );
    assert_eq!(le.serialize(&SqlError::WrongObjectType).unwrap(), [47, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::WrongObjectType).unwrap(), [0, 0, 0, 47]);
    assert_eq!(le.deserialize::<SqlError>(&[47, 0, 0, 0]).unwrap(), SqlError::WrongObjectType);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 47]).unwrap(), SqlError::WrongObjectType);
    assert_eq!(local::failure(CoreError::Sql(SqlError::WrongObjectType)).code, 247);
    assert_eq!(serde_json::to_string(&SqlError::WrongObjectType).unwrap(), "\"WrongObjectType\"");
    assert!(le.deserialize::<SqlError>(&[52, 0, 0, 0]).is_err());
}
