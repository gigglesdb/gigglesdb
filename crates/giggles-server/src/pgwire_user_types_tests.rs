use super::*;
#[test]
fn user_type_oid_and_domain_diagnostic_reach_wire() {
    let d = Description {
        parameters: vec![],
        typmods: vec![-1],
        type_oids: vec![65540],
        columns: vec![giggles_core::relational::ColumnDef {
            id: 0,
            name: "row".into(),
            data_type: DataType::Text,
            not_null: false,
        }],
    };
    let mut bytes = Vec::new();
    row_description(&d, &[], &mut bytes).unwrap();
    assert_eq!(bytes[0], b'T');
    let mut reader = Reader {
        b: &bytes[5..],
        at: 0,
    };
    assert_eq!(reader.u16().unwrap(), 1);
    assert_eq!(reader.text().unwrap(), "row");
    assert_eq!(reader.u32().unwrap(), 0);
    assert_eq!(reader.u16().unwrap(), 0);
    assert_eq!(reader.u32().unwrap(), 65540);
    let mut diagnostic = Diagnostic::default();
    let path = std::env::temp_dir().join(format!("user-type-wire-{}", std::process::id()));
    let admin = giggles_core::Admin::create(&path, giggles_core::Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE DOMAIN positive AS int CHECK (VALUE > 0)", &[], None)
        .unwrap();
    session
        .execute("CREATE TABLE t (v positive)", &[], None)
        .unwrap();
    let result = session.execute("INSERT INTO t VALUES (0)", &[], None);
    assert_eq!(diagnostic.core(result), Err("23514"));
    let expected = "value for domain positive violates check constraint \"positive_check\"";
    assert_eq!(diagnostic.wire_message.as_deref(), Some(expected));
    diagnostic_error("23514", "fallback", Some(&diagnostic), &mut bytes);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
    assert!(
        bytes
            .windows(expected.len())
            .any(|b| b == expected.as_bytes())
    );
}
