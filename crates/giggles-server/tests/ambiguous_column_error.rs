use giggles_core::{CoreError, local, relational::SqlError};

#[test]
fn ambiguous_column_sqlstate_contract() {
    assert_eq!(
        giggles_server::sqlstate(&CoreError::Sql(SqlError::AmbiguousColumn)),
        "42702"
    );
}

#[test]
fn ambiguous_column_local_code_contract() {
    let error = SqlError::AmbiguousColumn;
    assert_eq!(SqlError::AmbiguousColumn as u32, 53);
    assert_eq!(local::failure(CoreError::Sql(error)).code, 253);
}
