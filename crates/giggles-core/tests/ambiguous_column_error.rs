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
    assert!(le.deserialize::<SqlError>(&[64, 0, 0, 0]).is_err());
    assert!(be.deserialize::<SqlError>(&[0, 0, 0, 64]).is_err());
    assert_eq!(serde_json::to_string(&error).unwrap(), "\"AmbiguousColumn\"");
    assert_eq!(
        serde_json::from_str::<SqlError>("\"AmbiguousColumn\"").unwrap(),
        error
    );
    assert_eq!(local::failure(CoreError::Sql(error)).code, 253);
}
