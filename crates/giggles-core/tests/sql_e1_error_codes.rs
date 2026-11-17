use bincode::Options;
use giggles_core::{Admin, CoreError, Limits, local, relational::SqlError as E};
use std::time::{SystemTime, UNIX_EPOCH};

// Parallel tests can read the same nanosecond on Windows; a counter keeps paths unique.
static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[test]
fn e1_error_names_and_local_codes() {
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    for (le_bytes, be_bytes, code, name, error) in [
        (
            [29, 0, 0, 0],
            [0, 0, 0, 29],
            229,
            "DuplicateTable",
            E::DuplicateTable,
        ),
        (
            [30, 0, 0, 0],
            [0, 0, 0, 30],
            230,
            "DuplicateColumn",
            E::DuplicateColumn,
        ),
        (
            [31, 0, 0, 0],
            [0, 0, 0, 31],
            231,
            "DuplicateObject",
            E::DuplicateObject,
        ),
        (
            [32, 0, 0, 0],
            [0, 0, 0, 32],
            232,
            "UndefinedObject",
            E::UndefinedObject,
        ),
        (
            [33, 0, 0, 0],
            [0, 0, 0, 33],
            233,
            "CheckViolation",
            E::CheckViolation,
        ),
        (
            [34, 0, 0, 0],
            [0, 0, 0, 34],
            234,
            "ForeignKeyViolation",
            E::ForeignKeyViolation,
        ),
        (
            [35, 0, 0, 0],
            [0, 0, 0, 35],
            235,
            "DependentObjects",
            E::DependentObjects,
        ),
        (
            [36, 0, 0, 0],
            [0, 0, 0, 36],
            236,
            "GeneratedAlways",
            E::GeneratedAlways,
        ),
        (
            [37, 0, 0, 0],
            [0, 0, 0, 37],
            237,
            "ObjectNotInPrerequisiteState",
            E::ObjectNotInPrerequisiteState,
        ),
        (
            [38, 0, 0, 0],
            [0, 0, 0, 38],
            238,
            "SequenceLimit",
            E::SequenceLimit,
        ),
        (
            [39, 0, 0, 0],
            [0, 0, 0, 39],
            239,
            "NoActiveTransaction",
            E::NoActiveTransaction,
        ),
        (
            [40, 0, 0, 0],
            [0, 0, 0, 40],
            240,
            "InvalidSavepoint",
            E::InvalidSavepoint,
        ),
        (
            [41, 0, 0, 0],
            [0, 0, 0, 41],
            241,
            "InvalidForeignKey",
            E::InvalidForeignKey,
        ),
        (
            [42, 0, 0, 0],
            [0, 0, 0, 42],
            242,
            "InvalidSchema",
            E::InvalidSchema,
        ),
        (
            [43, 0, 0, 0],
            [0, 0, 0, 43],
            243,
            "DivisionByZero",
            E::DivisionByZero,
        ),
    ] {
        let json = serde_json::to_string(&error).unwrap();
        assert_eq!(json, format!("\"{name}\""));
        assert_eq!(serde_json::from_str::<E>(&json).unwrap(), error);
        assert_eq!(
            serde_json::from_str::<E>(&format!("{{\"{name}\":null}}")).unwrap(),
            error
        );
        assert_eq!(le.serialize(&error).unwrap(), le_bytes);
        assert_eq!(be.serialize(&error).unwrap(), be_bytes);
        assert_eq!(le.deserialize::<E>(&le_bytes).unwrap(), error);
        assert_eq!(be.deserialize::<E>(&be_bytes).unwrap(), error);
        assert_eq!(local::failure(CoreError::Sql(error)).code, code);
    }
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
    assert!(le.deserialize::<E>(&[64, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, 64]).is_err());
}

#[test]
fn sql_create_reports_duplicate_table_and_column() {
    let path = std::env::temp_dir().join(format!(
        "giggles-sql-e1-errors-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let create = "CREATE TABLE existing (a int primary key)";
    session.execute(create, &[], None).unwrap();
    assert!(matches!(
        session.execute(create, &[], None),
        Err(CoreError::Sql(E::DuplicateTable))
    ));
    assert!(matches!(
        session.execute("CREATE TABLE t (a int primary key, a int)", &[], None),
        Err(CoreError::Sql(E::DuplicateColumn))
    ));
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(&path).unwrap();
}
