use super::*;
use crate::relational::{
    Prepared,
    catalog_ext::CatalogExt,
    column_collations::{Binding, Reference},
};
fn table(state: &mut State, collation: &str) {
    let sql = format!("CREATE TABLE t(id int PRIMARY KEY,v text{collation})");
    let p = Prepared::parse(&sql, 17, state.sql.epoch).unwrap();
    state.sql.execute(&p, &[], None).unwrap();
}
#[test]
fn legacy_old_bytes_default_and_new_feature_witness() {
    let mut old = empty(17);
    table(&mut old, "");
    let old_bytes = records(&old).unwrap();
    let mut default = empty(17);
    table(&mut default, " COLLATE \"default\"");
    assert_eq!(records(&default).unwrap(), old_bytes);
    let mut state = empty(17);
    table(&mut state, " COLLATE \"C\"");
    let r = records(&state).unwrap();
    let bits = feature_envelope::feature_bits_present(&state).unwrap();
    assert_ne!(bits & (1 << 23), 0);
    let restored = from_records_mode(r.clone(), &mut || false, false).unwrap();
    assert_eq!(records(&restored).unwrap(), r);
    let mut malformed = r.clone();
    malformed.insert(vec![10], (bits & !(1 << 23)).to_be_bytes().to_vec());
    assert!(from_records_mode(malformed, &mut || false, false).is_err());
    let mut future = r;
    future.insert(vec![10], (bits | (1 << 24)).to_be_bytes().to_vec());
    future.insert(vec![5], vec![255]);
    assert!(matches!(
        from_records_mode(future, &mut || false, false),
        Err(CoreError::Unsupported)
    ));
}
#[test]
fn catalog_extension_and_v2_record_roundtrips_reject_malformed() {
    let mut state = empty(17);
    table(&mut state, " COLLATE \"C\"");
    let ext = state.sql.catalog_ext();
    assert_eq!(ext.ext_version, 10);
    let bytes = bincode::serialize(ext).unwrap();
    assert_eq!(&bincode::deserialize::<CatalogExt>(&bytes).unwrap(), ext);
    let mut unsupported = bytes.clone();
    unsupported[..4].copy_from_slice(&11u32.to_le_bytes());
    assert!(bincode::deserialize::<CatalogExt>(&unsupported).is_err());
    let b = Binding::Column(Reference::Builtin(950));
    let bytes = engine::catalog_codec::encode_column_collation(&b).unwrap();
    assert_eq!(bytes[0], 1);
    assert_eq!(
        engine::catalog_codec::decode_column_collation(&bytes).unwrap(),
        b
    );
    let mut bad = bytes.clone();
    bad[0] = 2;
    assert!(engine::catalog_codec::decode_column_collation(&bad).is_err());
    for r in [
        Reference::Builtin(0),
        Reference::Builtin(100),
        Reference::User(0),
    ] {
        assert!(engine::catalog_codec::encode_column_collation(&Binding::Column(r)).is_err());
    }
    assert!(
        engine::catalog_codec::encode_column_collation(&Binding::Index(vec![None; 65])).is_err()
    );
    for id in [0, 999] {
        let mut ext = state.sql.catalog_ext().clone();
        ext.column_collations.clear();
        ext.column_collations.insert(id, b.clone());
        assert!(
            crate::relational::Relational::catalog_image(
                state.sql.tables.clone(),
                ext,
                u64::MAX,
                1
            )
            .is_err()
        );
    }
    let id = state.sql.tables["t"].columns[1].id;
    let mut ext = state.sql.catalog_ext().clone();
    ext.column_collations
        .insert(id, Binding::Column(Reference::User(999)));
    assert!(
        crate::relational::Relational::catalog_image(state.sql.tables.clone(), ext, u64::MAX, 1)
            .is_err()
    );
}
