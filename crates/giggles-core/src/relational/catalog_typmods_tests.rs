use super::*;
use bincode::Options;
use sha2::{Digest, Sha256};

#[test]
fn admin_catalog_typmods_returns_exact_present_entries_and_current_epoch() {
    let mut state = Relational::default();
    let prepared = Prepared::parse(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a NUMERIC, b NUMERIC, c NUMERIC)",
        17,
        state.epoch,
    )
    .unwrap();
    state.execute(&prepared, &[], None).unwrap();
    let columns = state.tables["t"].columns.clone();
    let first = giggles_pg_types::typmod::NumericTypmod::new(10, 2)
        .unwrap()
        .to_typmod();
    let second = giggles_pg_types::typmod::NumericTypmod::new(8, -3)
        .unwrap()
        .to_typmod();
    state.set_column_typmod(columns[1].id, Some(first)).unwrap();
    state.set_column_typmod(columns[2].id, Some(second)).unwrap();
    state
        .set_column_default(columns[3].id, Some("1".into()))
        .unwrap();
    let expected = (
        state.epoch,
        BTreeMap::from([(columns[1].id, first), (columns[2].id, second)]),
    );

    // Exercise the public restore boundary with a canonical format-53 archive.
    let codec = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .with_limit(16 * 1024 * 1024)
        .reject_trailing_bytes();
    let metadata = (53u32, 17u128, 0u64, 0u64, 0u64, None::<u128>);
    let bits = u64::from(state.contains_typed()) | 64 | 128;
    let records = BTreeMap::from([
        (vec![0u8], codec.serialize(&metadata).unwrap()),
        (vec![5u8], codec.serialize(&state).unwrap()),
        (vec![10u8], bits.to_be_bytes().to_vec()),
    ]);
    let payload = codec.serialize(&records).unwrap();
    let mut archive = b"GDEXP001".to_vec();
    archive.extend(53u32.to_be_bytes());
    archive.extend((payload.len() as u64).to_be_bytes());
    archive.extend(Sha256::digest(&payload));
    archive.extend(payload);

    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::path::PathBuf::from("J:/gigglesdb-work/e1-sql/tmp")
        .join(format!("catalog-typmods-{:032x}", u128::from_be_bytes(nonce)));
    std::fs::create_dir_all(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let export = path.join("fixture.export");
    std::fs::write(&export, archive).unwrap();
    let admin = crate::Admin::restore(&export, path.join("db"), crate::Limits::default()).unwrap();
    assert_eq!(admin.catalog_typmods().unwrap(), expected);
    assert_eq!(admin.catalog().unwrap().0, expected.0);
}
