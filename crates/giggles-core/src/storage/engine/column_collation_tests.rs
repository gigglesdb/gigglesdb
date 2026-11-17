use super::*;
use crate::relational::{
    ColumnDef, DataType, TableDef,
    column_collations::{Binding, Reference},
};
use kv::{Durability, Keyspace, KvBackend, KvRead, WriteBatch};
use kv_mem::MemKv;
fn put(kv: &MemKv, key: Vec<u8>, value: Vec<u8>) {
    let mut b = WriteBatch::default();
    b.put(
        Keyspace::Catalog,
        codec::user_key_seek_at(&key, hlc::Ts(1)),
        mvcc::encode_live(&value),
    );
    kv.write(b, Durability::Sync).unwrap();
}
fn table() -> catalog::TableEntry {
    catalog::TableEntry {
        primary_index_birth: None,
        def: TableDef {
            id: 1,
            name: "t".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "v".into(),
                data_type: DataType::Text,
                not_null: true,
            }],
            primary_key: vec![0],
        },
        heap: false,
        columns_ext: Default::default(),
    }
}
#[test]
fn v2_column_collation_bits_malformed_records_and_dependencies() {
    let kv = MemKv::new();
    let t = table();
    let table_bytes = catalog_codec::encode_table(&t, &Default::default()).unwrap();
    put(&kv, catalog_codec::table_key(1), table_bytes.clone());
    let binding = Binding::Column(Reference::Builtin(950));
    let k = catalog_codec::column_collation_key(2);
    let v = catalog_codec::encode_column_collation(&binding).unwrap();
    put(&kv, k.clone(), v.clone());
    assert_eq!(
        content_check::required_bits_for_catalog_key(&k).unwrap(),
        meta::COLUMN_COLLATIONS
    );
    assert_eq!(
        content_check::required_bits_for_catalog_value(&k, &v).unwrap(),
        meta::COLUMN_COLLATIONS
    );
    assert!(content_check::check(&kv, 0).is_err());
    assert!(content_check::check(&kv, meta::COLUMN_COLLATIONS).is_ok());
    assert!(catalog_codec::classify_catalog_key(&catalog_codec::column_collation_key(0)).is_err());
    let mut bad = v.clone();
    bad[0] = 2;
    put(&kv, k.clone(), bad);
    assert!(content_check::check(&kv, meta::COLUMN_COLLATIONS).is_err());
    put(
        &kv,
        k.clone(),
        catalog_codec::encode_column_collation(&Binding::Column(Reference::User(999))).unwrap(),
    );
    assert!(content_check::check(&kv, meta::COLUMN_COLLATIONS).is_err());
    put(&kv, k, v);
    assert_eq!(
        catalog_codec::encode_table(&t, &Default::default()).unwrap(),
        table_bytes
    );
    let other = MemKv::new();
    put(
        &other,
        catalog_codec::column_collation_key(2),
        catalog_codec::encode_column_collation(&binding).unwrap(),
    );
    assert!(content_check::check(&other, meta::COLUMN_COLLATIONS).is_err());
    let _: &dyn KvRead = &kv;
    let mut parts = catalog::CatalogParts::default();
    parts.tables.insert("t".into(), t.def.clone());
    let old = catalog::parts_records(&parts).unwrap();
    parts.ext.column_collations.insert(2, binding);
    parts.ext.ext_version = 10;
    let new = catalog::parts_records(&parts).unwrap();
    assert_eq!(new.len(), old.len() + 1);
    for (k, v) in old {
        assert_eq!(new.get(&k), Some(&v));
    }
}
