use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError as E};

const FIRST_UNKNOWN_TAG: u8 = 53; // next free SqlError tag; bump when appending

#[test]
fn f17_error_contract_goldens() {
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    for (error, tag, code, name) in [
        (E::NullValueNotAllowed, 48, 248, "NullValueNotAllowed"),
        (E::CannotCoerce, 49, 249, "CannotCoerce"),
    ] {
        assert_eq!(local::failure(CoreError::Sql(error.clone())).code, code);
        let json = format!("\"{name}\"");
        assert_eq!(serde_json::to_string(&error).unwrap(), json);
        assert_eq!(serde_json::from_str::<E>(&json).unwrap(), error);
        assert_eq!(
            serde_json::from_str::<E>(&format!("{{\"{name}\":null}}")).unwrap(),
            error
        );
        assert_eq!(le.serialize(&error).unwrap(), [tag, 0, 0, 0]);
        assert_eq!(be.serialize(&error).unwrap(), [0, 0, 0, tag]);
        assert_eq!(le.deserialize::<E>(&[tag, 0, 0, 0]).unwrap(), error);
        assert_eq!(be.deserialize::<E>(&[0, 0, 0, tag]).unwrap(), error);
        assert!(le.deserialize::<E>(&[tag, 0, 0, 0, 0]).is_err());
        assert!(be.deserialize::<E>(&[0, 0, 0, tag, 0]).is_err());
    }
    assert!(le.deserialize::<E>(&[FIRST_UNKNOWN_TAG, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, FIRST_UNKNOWN_TAG]).is_err());
}
