use super::*;

fn comments(s: &mut SqlSession) -> BTreeMap<(u64, u64), String> {
    let Backend::V2(db) = &s.admin.inner.backend else { unreachable!() };
    let mut implicit = db.begin(Isolation::ReadCommitted);
    db.catalog().parts(s.v2_transaction.as_mut().unwrap_or(&mut implicit)).unwrap().comments
}
#[test]
fn comments_are_transactional_and_follow_stable_column_identity() {
    let path = std::env::temp_dir().join(format!("comment-values-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(id int, v text)", "COMMENT ON TABLE t IS 'é''quoted'",
        "COMMENT ON COLUMN t.v IS 'column'"] { s.execute(sql, &[], None).unwrap(); }
    let original = comments(&mut s);
    assert_eq!(original.values().cloned().collect::<Vec<_>>(), ["é'quoted", "column"]);
    for sql in ["BEGIN", "COMMENT ON TABLE t IS NULL", "SAVEPOINT x", "COMMENT ON COLUMN t.v IS ''"] {
        s.execute(sql, &[], None).unwrap();
    }
    assert!(comments(&mut s).is_empty());
    s.execute("ROLLBACK TO x", &[], None).unwrap();
    assert_eq!(comments(&mut s).len(), 1);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(comments(&mut s), original);
    s.execute("ALTER TABLE t RENAME COLUMN v TO renamed", &[], None).unwrap();
    assert_eq!(comments(&mut s), original);
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(comments(&mut s), original);
    s.execute("ALTER TABLE t DROP COLUMN renamed", &[], None).unwrap();
    assert_eq!(comments(&mut s).len(), 1);
    s.execute("COMMENT ON TABLE t IS ''", &[], None).unwrap();
    assert!(comments(&mut s).is_empty());
    s.execute("COMMENT ON TABLE t IS 'drop'", &[], None).unwrap();
    s.execute("DROP TABLE t", &[], None).unwrap();
    assert!(comments(&mut s).is_empty());
    drop(s); drop(admin); fs::remove_dir_all(path).unwrap();
}
