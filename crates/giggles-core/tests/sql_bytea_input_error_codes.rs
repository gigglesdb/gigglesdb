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
    assert_eq!(le.deserialize::<E>(&[58, 0, 0, 0]).unwrap(), E::InvalidRecursion);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 58]).unwrap(), E::InvalidRecursion);
    assert_eq!(le.serialize(&E::InvalidRecursion).unwrap(), [58, 0, 0, 0]);
    assert_eq!(be.serialize(&E::InvalidRecursion).unwrap(), [0, 0, 0, 58]);
    assert_eq!(local::failure(CoreError::Sql(E::InvalidRecursion)).code, 258);
    assert_eq!(serde_json::to_string(&E::InvalidRecursion).unwrap(), "\"InvalidRecursion\"");
    assert_eq!(le.deserialize::<E>(&[59, 0, 0, 0]).unwrap(), E::DuplicateAlias);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 59]).unwrap(), E::DuplicateAlias);
    assert_eq!(le.serialize(&E::DuplicateAlias).unwrap(), [59, 0, 0, 0]);
    assert_eq!(be.serialize(&E::DuplicateAlias).unwrap(), [0, 0, 0, 59]);
    assert_eq!(local::failure(CoreError::Sql(E::DuplicateAlias)).code, 259);
    assert_eq!(serde_json::to_string(&E::DuplicateAlias).unwrap(), "\"DuplicateAlias\"");
    assert_eq!(le.deserialize::<E>(&[60, 0, 0, 0]).unwrap(), E::WindowingError);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 60]).unwrap(), E::WindowingError);
    assert_eq!(le.serialize(&E::WindowingError).unwrap(), [60, 0, 0, 0]);
    assert_eq!(be.serialize(&E::WindowingError).unwrap(), [0, 0, 0, 60]);
    assert_eq!(local::failure(CoreError::Sql(E::WindowingError)).code, 260);
    assert_eq!(serde_json::to_string(&E::WindowingError).unwrap(), "\"WindowingError\"");
    assert_eq!(le.deserialize::<E>(&[61, 0, 0, 0]).unwrap(), E::InvalidFrameOffset);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 61]).unwrap(), E::InvalidFrameOffset);
    assert_eq!(le.serialize(&E::InvalidFrameOffset).unwrap(), [61, 0, 0, 0]);
    assert_eq!(be.serialize(&E::InvalidFrameOffset).unwrap(), [0, 0, 0, 61]);
    assert_eq!(local::failure(CoreError::Sql(E::InvalidFrameOffset)).code, 261);
    assert_eq!(serde_json::to_string(&E::InvalidFrameOffset).unwrap(), "\"InvalidFrameOffset\"");
    assert_eq!(le.deserialize::<E>(&[62, 0, 0, 0]).unwrap(), E::InvalidNtileArgument);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 62]).unwrap(), E::InvalidNtileArgument);
    assert_eq!(le.serialize(&E::InvalidNtileArgument).unwrap(), [62, 0, 0, 0]);
    assert_eq!(be.serialize(&E::InvalidNtileArgument).unwrap(), [0, 0, 0, 62]);
    assert_eq!(local::failure(CoreError::Sql(E::InvalidNtileArgument)).code, 262);
    assert_eq!(serde_json::to_string(&E::InvalidNtileArgument).unwrap(), "\"InvalidNtileArgument\"");
    assert_eq!(le.deserialize::<E>(&[63, 0, 0, 0]).unwrap(), E::InvalidNthValueArgument);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 63]).unwrap(), E::InvalidNthValueArgument);
    assert_eq!(le.serialize(&E::InvalidNthValueArgument).unwrap(), [63, 0, 0, 0]);
    assert_eq!(be.serialize(&E::InvalidNthValueArgument).unwrap(), [0, 0, 0, 63]);
    assert_eq!(local::failure(CoreError::Sql(E::InvalidNthValueArgument)).code, 263);
    assert_eq!(serde_json::to_string(&E::InvalidNthValueArgument).unwrap(), "\"InvalidNthValueArgument\"");
    assert_eq!(le.deserialize::<E>(&[64, 0, 0, 0]).unwrap(), E::InvalidTablesampleArgument);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 64]).unwrap(), E::InvalidTablesampleArgument);
    assert_eq!(le.serialize(&E::InvalidTablesampleArgument).unwrap(), [64, 0, 0, 0]);
    assert_eq!(be.serialize(&E::InvalidTablesampleArgument).unwrap(), [0, 0, 0, 64]);
    assert_eq!(local::failure(CoreError::Sql(E::InvalidTablesampleArgument)).code, 266);
    assert_eq!(serde_json::to_string(&E::InvalidTablesampleArgument).unwrap(), "\"InvalidTablesampleArgument\"");
    assert!(le.deserialize::<E>(&[65, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, 65]).is_err());
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
