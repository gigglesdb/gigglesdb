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
    assert_eq!(le.deserialize::<SqlError>(&[58, 0, 0, 0]).unwrap(), SqlError::InvalidRecursion);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 58]).unwrap(), SqlError::InvalidRecursion);
    assert_eq!(le.serialize(&SqlError::InvalidRecursion).unwrap(), [58, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidRecursion).unwrap(), [0, 0, 0, 58]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidRecursion)).code, 258);
    assert_eq!(serde_json::to_string(&SqlError::InvalidRecursion).unwrap(), "\"InvalidRecursion\"");
    assert_eq!(le.deserialize::<SqlError>(&[59, 0, 0, 0]).unwrap(), SqlError::DuplicateAlias);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 59]).unwrap(), SqlError::DuplicateAlias);
    assert_eq!(le.serialize(&SqlError::DuplicateAlias).unwrap(), [59, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::DuplicateAlias).unwrap(), [0, 0, 0, 59]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::DuplicateAlias)).code, 259);
    assert_eq!(serde_json::to_string(&SqlError::DuplicateAlias).unwrap(), "\"DuplicateAlias\"");
    assert_eq!(le.deserialize::<SqlError>(&[60, 0, 0, 0]).unwrap(), SqlError::WindowingError);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 60]).unwrap(), SqlError::WindowingError);
    assert_eq!(le.serialize(&SqlError::WindowingError).unwrap(), [60, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::WindowingError).unwrap(), [0, 0, 0, 60]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::WindowingError)).code, 260);
    assert_eq!(serde_json::to_string(&SqlError::WindowingError).unwrap(), "\"WindowingError\"");
    assert_eq!(le.deserialize::<SqlError>(&[61, 0, 0, 0]).unwrap(), SqlError::InvalidFrameOffset);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 61]).unwrap(), SqlError::InvalidFrameOffset);
    assert_eq!(le.serialize(&SqlError::InvalidFrameOffset).unwrap(), [61, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidFrameOffset).unwrap(), [0, 0, 0, 61]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidFrameOffset)).code, 261);
    assert_eq!(serde_json::to_string(&SqlError::InvalidFrameOffset).unwrap(), "\"InvalidFrameOffset\"");
    assert_eq!(le.deserialize::<SqlError>(&[62, 0, 0, 0]).unwrap(), SqlError::InvalidNtileArgument);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 62]).unwrap(), SqlError::InvalidNtileArgument);
    assert_eq!(le.serialize(&SqlError::InvalidNtileArgument).unwrap(), [62, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidNtileArgument).unwrap(), [0, 0, 0, 62]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidNtileArgument)).code, 262);
    assert_eq!(serde_json::to_string(&SqlError::InvalidNtileArgument).unwrap(), "\"InvalidNtileArgument\"");
    assert_eq!(le.deserialize::<SqlError>(&[63, 0, 0, 0]).unwrap(), SqlError::InvalidNthValueArgument);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 63]).unwrap(), SqlError::InvalidNthValueArgument);
    assert_eq!(le.serialize(&SqlError::InvalidNthValueArgument).unwrap(), [63, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidNthValueArgument).unwrap(), [0, 0, 0, 63]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidNthValueArgument)).code, 263);
    assert_eq!(serde_json::to_string(&SqlError::InvalidNthValueArgument).unwrap(), "\"InvalidNthValueArgument\"");
    assert_eq!(le.deserialize::<SqlError>(&[64, 0, 0, 0]).unwrap(), SqlError::InvalidTablesampleArgument);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 64]).unwrap(), SqlError::InvalidTablesampleArgument);
    assert_eq!(le.serialize(&SqlError::InvalidTablesampleArgument).unwrap(), [64, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidTablesampleArgument).unwrap(), [0, 0, 0, 64]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidTablesampleArgument)).code, 266);
    assert_eq!(serde_json::to_string(&SqlError::InvalidTablesampleArgument).unwrap(), "\"InvalidTablesampleArgument\"");
    assert!(le.deserialize::<SqlError>(&[65, 0, 0, 0]).is_err());
}
