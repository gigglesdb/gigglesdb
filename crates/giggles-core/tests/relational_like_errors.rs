use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError};

#[test]
fn like_errors_append_tags_and_local_codes() {
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    for (error, tag, code, name) in [
        (SqlError::DivisionByZero, 43u32, 243, "DivisionByZero"),
        (
            SqlError::InvalidEscapeSequence,
            44,
            244,
            "InvalidEscapeSequence",
        ),
    ] {
        assert_eq!(le.serialize(&error).unwrap(), tag.to_le_bytes());
        assert_eq!(be.serialize(&error).unwrap(), tag.to_be_bytes());
        assert_eq!(
            le.deserialize::<SqlError>(&tag.to_le_bytes()).unwrap(),
            error
        );
        assert_eq!(
            be.deserialize::<SqlError>(&tag.to_be_bytes()).unwrap(),
            error
        );
        let json = serde_json::to_string(&error).unwrap();
        assert_eq!(json, format!("\"{name}\""));
        assert_eq!(serde_json::from_str::<SqlError>(&json).unwrap(), error);
        assert_eq!(local::failure(CoreError::Sql(error)).code, code);
    }
}
