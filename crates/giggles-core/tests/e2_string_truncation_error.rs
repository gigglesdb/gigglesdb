mod common;
use bincode::Options;
use giggles_core::{CoreError, local, relational::SqlError};

const STRING_DATA_RIGHT_TRUNCATION_TAG: u32 = 51;

#[test]
fn string_data_right_truncation_has_append_only_protocol_identity() {
    let error = SqlError::StringDataRightTruncation;
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    assert_eq!(
        le.serialize(&error).unwrap(),
        STRING_DATA_RIGHT_TRUNCATION_TAG.to_le_bytes()
    );
    assert_eq!(
        be.serialize(&error).unwrap(),
        STRING_DATA_RIGHT_TRUNCATION_TAG.to_be_bytes()
    );
    assert_eq!(
        le.deserialize::<SqlError>(&STRING_DATA_RIGHT_TRUNCATION_TAG.to_le_bytes())
            .unwrap(),
        error
    );
    assert_eq!(
        be.deserialize::<SqlError>(&STRING_DATA_RIGHT_TRUNCATION_TAG.to_be_bytes())
            .unwrap(),
        error
    );
    assert_eq!(
        serde_json::to_string(&error).unwrap(),
        "\"StringDataRightTruncation\""
    );
    assert_eq!(
        serde_json::from_str::<SqlError>("\"StringDataRightTruncation\"").unwrap(),
        error
    );
    assert_eq!(local::failure(CoreError::Sql(error)).code, 251);
    assert_eq!(le.deserialize::<SqlError>(&[58, 0, 0, 0]).unwrap(), SqlError::InvalidRecursion);
    assert_eq!(be.deserialize::<SqlError>(&[0, 0, 0, 58]).unwrap(), SqlError::InvalidRecursion);
    assert_eq!(le.serialize(&SqlError::InvalidRecursion).unwrap(), [58, 0, 0, 0]);
    assert_eq!(be.serialize(&SqlError::InvalidRecursion).unwrap(), [0, 0, 0, 58]);
    assert_eq!(local::failure(CoreError::Sql(SqlError::InvalidRecursion)).code, 258);
    assert_eq!(serde_json::to_string(&SqlError::InvalidRecursion).unwrap(), "\"InvalidRecursion\"");
    assert!(le.deserialize::<SqlError>(&[59, 0, 0, 0]).is_err());
    assert!(be.deserialize::<SqlError>(&[0, 0, 0, 59]).is_err());
}

#[test]
fn oversized_varchar_typmod_rejects_create_and_alter_with_invalid_parameter_value() {
    use giggles_core::{Admin, Limits};
    let admin = Admin::create(common::fresh("e2-oversized-varchar"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t (id int)", &[], None)
        .unwrap();
    for sql in [
        "CREATE TABLE oversized (v varchar(10485761))",
        "ALTER TABLE t ADD COLUMN v varchar(10485761)",
    ] {
        assert_eq!(
            session.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::InvalidParameterValue)),
            "{sql}"
        );
    }
}
