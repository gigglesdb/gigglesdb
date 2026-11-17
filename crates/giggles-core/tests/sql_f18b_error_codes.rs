use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError as E};

const TAG: u8 = 52;
const FIRST_UNKNOWN_TAG: u8 = 64; // next free SqlError tag; bump when appending

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
    assert!(le.deserialize::<E>(&[FIRST_UNKNOWN_TAG, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, FIRST_UNKNOWN_TAG]).is_err());
}
