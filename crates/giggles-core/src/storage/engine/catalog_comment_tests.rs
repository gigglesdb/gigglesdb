use super::super::super::{
    codec,
    hlc::Ts,
    kv::{KvBackend, WriteBatch},
    meta, mvcc,
};
use super::*;

fn plain(ids: &Range<u64>) -> CatalogParts {
    let mut parts = serial_parts(ids);
    parts.ext = CatalogExt::default();
    parts
}

#[test]
fn comment_wire_is_frozen_bounded_and_strict() {
    assert_eq!(
        catalog_codec::comment_key(1, 2),
        [18, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]
    );
    let value = catalog_codec::encode_comment("é").unwrap();
    assert_eq!(value, [1, 0, 0, 0, 2, 0xc3, 0xa9]);
    for text in ["", "hello", "é"] {
        assert_eq!(
            catalog_codec::decode_comment(&catalog_codec::encode_comment(text).unwrap()).unwrap(),
            text
        );
    }
    for bad in [
        vec![],
        vec![2, 0, 0, 0, 0],
        vec![1, 0, 0, 0, 0, 0],
        vec![1, 0, 0, 0, 1, 255],
        vec![1, 255, 255, 255, 255],
    ] {
        assert!(catalog_codec::decode_comment(&bad).is_err());
    }
    assert!(catalog_codec::classify_catalog_key(&catalog_codec::comment_key(0, 2)).is_err());
    assert!(catalog_codec::classify_catalog_key(&[18; 16]).is_err());
    let max = "x".repeat(65536);
    assert_eq!(
        catalog_codec::decode_comment(&catalog_codec::encode_comment(&max).unwrap()).unwrap(),
        max
    );
    assert!(catalog_codec::encode_comment(&(max + "x")).is_err());
}

#[test]
fn comments_overlay_rollback_reopen_and_monotone_guard() {
    let (mem, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(2).unwrap();
    let old = plain(&ids);
    let mut tx = txn(&seq, &locks, 1);
    catalog
        .apply_diff(&mut tx, &empty(), &old, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    assert_eq!(features(seq.kv()), 0);
    let mut wanted = old.clone();
    wanted.comments.insert((ids.start, 0), "relation".into());
    wanted
        .comments
        .insert((ids.start, ids.start + 1), "column é".into());
    let mut tx = txn(&seq, &locks, 2);
    catalog
        .apply_diff(&mut tx, &old, &wanted, &ids, None, &|| false)
        .unwrap();
    assert_eq!(catalog.parts(&mut tx).unwrap().comments, wanted.comments);
    tx.rollback();
    assert!(parts(&catalog, &seq).comments.is_empty());
    assert_eq!(features(seq.kv()), 0);
    let mut tx = txn(&seq, &locks, 3);
    catalog
        .apply_diff(&mut tx, &old, &wanted, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    assert_eq!(features(seq.kv()), meta::RELATION_COMMENTS);
    let mut reader = txn(&seq, &locks, 4);
    let snapshot = catalog.snapshot(&mut reader).unwrap();
    assert_eq!(snapshot.parts.comments, wanted.comments);
    assert!(Arc::ptr_eq(
        &snapshot,
        &catalog.snapshot(&mut reader).unwrap()
    ));
    reader.rollback();
    let kv = Arc::new(Kv::new(Arc::new(mem.crash())));
    content_check::check(kv.as_ref(), meta::RELATION_COMMENTS).unwrap();
    let reopened_seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(1)),
        SequencerConfig::default(),
    )
    .unwrap();
    let reopened = Catalog::open(
        reopened_seq.clone(),
        locks.clone(),
        Arc::new(SequenceAllocator::new(kv, 64)),
    )
    .unwrap();
    assert_eq!(parts(&reopened, &reopened_seq).comments, wanted.comments);
    let mut tx = txn(&seq, &locks, 5);
    catalog
        .apply_diff(&mut tx, &wanted, &old, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    assert_eq!(features(seq.kv()), meta::RELATION_COMMENTS);
    assert!(parts(&catalog, &seq).comments.is_empty());
    assert!(content_check::check(seq.kv(), 0).is_err());
}

#[test]
fn comment_content_requires_bit_and_existing_owner() {
    for column in [0, 2, 9] {
        for live in [false, true] {
            let (mem, seq, locks, catalog) = setup();
            let ids = catalog.reserve_ids(2).unwrap();
            let old = plain(&ids);
            let mut tx = txn(&seq, &locks, 1);
            catalog
                .apply_diff(&mut tx, &empty(), &old, &ids, None, &|| false)
                .unwrap();
            catalog.commit_ddl(tx).unwrap();
            let mut batch = WriteBatch::default();
            batch.put(
                Keyspace::Catalog,
                codec::user_key_seek_at(&catalog_codec::comment_key(1, column), Ts(u64::MAX)),
                if live {
                    mvcc::encode_live(&catalog_codec::encode_comment("x").unwrap())
                } else {
                    mvcc::encode_tombstone()
                },
            );
            mem.write(batch, super::super::super::kv::Durability::Sync)
                .unwrap();
            assert!(content_check::check(mem.as_ref(), 0).is_err());
            assert_eq!(
                content_check::check(mem.as_ref(), meta::RELATION_COMMENTS).is_ok(),
                !live || column != 9
            );
        }
    }
    let mut parts = plain(&(1..3));
    for owner in [(99, 0), (1, 99)] {
        parts.comments.clear();
        parts.comments.insert(owner, "bad".into());
        assert!(parts_records(&parts).is_err());
    }
}
