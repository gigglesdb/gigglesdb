use super::*;
use std::{thread, time::{Duration, Instant}};

#[test]
fn drop_waiting_behind_comment_serializes_and_retry_removes_comments() {
    let (_, seq, locks, catalog) = setup();
    let catalog = Arc::new(catalog);
    let ids = catalog.reserve_ids(2).unwrap();
    let mut old = serial_parts(&ids);
    old.ext = CatalogExt::default();
    let mut create = txn(&seq, &locks, 1);
    catalog.apply_diff(&mut create, &empty(), &old, &ids, None, &|| false).unwrap();
    catalog.commit_ddl(create).unwrap();
    let mut comment = txn(&seq, &locks, 2);
    let mut wanted = old.clone();
    wanted.comments.insert((ids.start, 0), "concurrent".into());
    catalog.apply_diff(&mut comment, &old, &wanted, &ids, None, &|| false).unwrap();
    let (c, s, l) = (catalog.clone(), seq.clone(), locks.clone());
    let dropper = thread::spawn(move || {
        let mut drop = txn(&s, &l, 3);
        c.drop_table(&mut drop, "serial_table", None, &|| false).unwrap();
        c.commit_ddl(drop)
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while locks.waiting_count() == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    catalog.commit_ddl(comment).unwrap();
    assert!(matches!(dropper.join().unwrap(), Err(TxnError::Serialization(_))));
    let mut retry = txn(&seq, &locks, 4);
    catalog.drop_table(&mut retry, "serial_table", None, &|| false).unwrap();
    catalog.commit_ddl(retry).unwrap();
    assert!(parts(&catalog, &seq).comments.is_empty());
    assert!(parts(&catalog, &seq).tables.is_empty());
}
