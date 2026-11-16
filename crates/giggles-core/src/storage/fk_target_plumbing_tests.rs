use super::*;
use engine::{catalog::{self, CatalogParts}, kv::Keyspace, txn::Isolation};
use relational::catalog_ext::{ConstraintKind, ReferencedKey};

const IDENTITY: u64 = 1 << 17;
fn fresh() -> std::path::PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("fk-plumbing-{:032x}", u128::from_be_bytes(nonce)));
    fs::create_dir_all(&path).unwrap();
    path
}
fn fixture(root: &Path, known: bool) -> (Admin, CatalogParts) {
    let admin = Admin::create_engine(root.join("legacy"), Limits::default(), Engine::Legacy).unwrap();
    let mut session = admin.sql_session();
    for sql in ["CREATE TABLE p(id bigint PRIMARY KEY)",
        "CREATE UNIQUE INDEX alternate ON p(id)",
        "CREATE TABLE c(id bigint PRIMARY KEY,a bigint REFERENCES p(id),b bigint REFERENCES p(id),d bigint REFERENCES p(id))"] {
        session.execute(sql, &[], None).unwrap();
    }
    drop(session);
    let parts = admin.inner.change(|s| {
        let (tables, mut ext, next_id, next_version) = s.sql.catalog_parts();
        let indexes = s.sql.secondary_descriptors_engine().to_vec();
        let p = tables["p"].id;
        let c = tables["c"].id;
        if known {
            ext.primary_index_birth.insert(p, next_id);
            for (constraint, target) in ext.constraints.get_mut(&c).unwrap().iter_mut()
                .filter(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }))
                .zip([ReferencedKey::PrimaryKey, ReferencedKey::Secondary(indexes[0].id), ReferencedKey::Unknown]) {
                let ConstraintKind::ForeignKey { target: slot, .. } = &mut constraint.kind else { unreachable!() };
                *slot = target;
            }
        }
        if !known {
            // New DDL binds targets now; explicitly construct the pre-feature fixture.
            for constraint in ext.constraints.values_mut().flatten() {
                if let ConstraintKind::ForeignKey { target, .. } = &mut constraint.kind {
                    *target = ReferencedKey::Unknown;
                }
            }
        }
        s.sql = relational::Relational::catalog_image(tables.clone(), ext, next_id + 1, next_version).unwrap();
        s.sql.catalog_indexes(&indexes).unwrap();
        let (_, ext, _, _) = s.sql.catalog_parts();
        Ok(CatalogParts { tables, ext, indexes, ..CatalogParts::default() })
    }).unwrap();
    (admin, parts)
}
fn parts(admin: &Admin) -> CatalogParts {
    match &admin.inner.backend {
        Backend::V2(db) => {
            let mut tx = db.begin(Isolation::ReadCommitted);
            db.catalog().parts(&mut tx).unwrap()
        }
        _ => admin.inner.inspect(|s| {
            let (tables, ext, _, _) = s.sql.catalog_parts();
            Ok(CatalogParts { tables, ext, indexes: s.sql.secondary_descriptors_engine().to_vec(),
                ..CatalogParts::default() })
        }).unwrap(),
    }
}
fn assert_ext(admin: &Admin, expected: &CatalogParts) {
    let actual = parts(admin);
    assert_eq!(encode(&actual.ext).unwrap(), encode(&expected.ext).unwrap());
    assert_eq!(actual.indexes, expected.indexes);
    let required = catalog::parts_records(&actual).unwrap().into_iter().fold(0, |bits, (k, v)|
        bits | engine::content_check::required_bits_for_catalog_value(&k, &v).unwrap());
    assert_eq!(required & IDENTITY != 0, actual.ext.has_key_dependency_identity());
    if let Backend::V2(db) = &admin.inner.backend {
        let bits = db.kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
        let bits = u64::from_be_bytes(bits.try_into().unwrap());
        assert_eq!(bits & IDENTITY != 0, !expected.ext.primary_index_birth.is_empty());
    }
}
#[test]
fn legacy_reopen_upgrade_and_records_only_roundtrips() {
    for known in [false, true] {
        let root = fresh();
        let (admin, expected) = fixture(&root, known);
        let state = admin.inner.inspect(|s| from_records(records(s)?)).unwrap();
        assert_eq!(encode(&state.sql.catalog_parts().1).unwrap(), encode(&expected.ext).unwrap());
        let export = root.join("legacy.gdexp");
        admin.export(&export).unwrap();
        let restored = Admin::restore(&export, root.join("restored-legacy"), Limits::default()).unwrap();
        assert_ext(&restored, &expected);
        drop(restored);
        drop(admin);
        let admin = Admin::open(root.join("legacy"), Limits::default()).unwrap();
        assert_ext(&admin, &expected);
        drop(admin);
        Admin::upgrade_to_v2(root.join("legacy"), Limits::default()).unwrap();
        let admin = Admin::open(root.join("legacy"), Limits::default()).unwrap();
        assert_ext(&admin, &expected);
        admin.export(root.join("v2.gdexp")).unwrap();
        let restored = Admin::restore(root.join("v2.gdexp"), root.join("restored-v2"), Limits::default()).unwrap();
        assert_ext(&restored, &expected);
        drop(restored);
        drop(admin);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn catalog_diffs_rebuild_and_remove_identity_atomically() {
    let root = fresh();
    let (legacy, expected) = fixture(&root, true);
    let admin = Admin::create_engine(root.join("v2"), Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else { unreachable!() };
    let ids = db.catalog().reserve_ids(1000).unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog().apply_diff(&mut tx, &CatalogParts::default(), &expected, &ids, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert_ext(&admin, &expected);
    let mut changed = expected.clone();
    *changed.ext.primary_index_birth.values_mut().next().unwrap() += 1;
    let cs = changed.ext.constraints.get_mut(&changed.tables["c"].id).unwrap();
    let fk = cs.iter_mut().find(|c| matches!(c.kind, ConstraintKind::ForeignKey { target: ReferencedKey::Unknown, .. })).unwrap();
    let ConstraintKind::ForeignKey { target, .. } = &mut fk.kind else { unreachable!() };
    *target = ReferencedKey::PrimaryKey;
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog().apply_diff(&mut tx, &expected, &changed, &ids, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert_ext(&admin, &changed);
    drop(admin);
    let admin = Admin::open(root.join("v2"), Limits::default()).unwrap();
    assert_ext(&admin, &changed);
    for admin in [&legacy, &admin] {
        admin.sql_session().execute("DROP TABLE p CASCADE", &[], None).unwrap();
        let remaining = parts(admin);
        assert!(remaining.ext.primary_index_birth.is_empty());
        assert!(!remaining.ext.constraints.values().flatten().any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. })));
        if let Backend::V2(db) = &admin.inner.backend {
            let bits = db.kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
            assert_ne!(u64::from_be_bytes(bits.try_into().unwrap()) & IDENTITY, 0);
        }
    }
    drop(admin);
    drop(legacy);
    for name in ["legacy", "v2"] {
        let reopened = Admin::open(root.join(name), Limits::default()).unwrap();
        assert!(parts(&reopened).ext.primary_index_birth.is_empty());
    }
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn unknown_only_uses_old_catalog_versions() {
    let root = fresh();
    let (admin, expected) = fixture(&root, false);
    assert!(expected.ext.wire_version() <= 7);
    for (key, value) in catalog::parts_records(&expected).unwrap() {
        use engine::catalog_codec::{classify_catalog_key, CatalogKind};
        match classify_catalog_key(&key).unwrap() {
            CatalogKind::Table => assert_eq!(value[0], 2),
            CatalogKind::Constraint => assert_eq!(value[0], 1),
            _ => {}
        }
    }
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn engine_catalog_drop_removes_birth_record() {
    let root = fresh();
    let (legacy, mut expected) = fixture(&root, true);
    expected.tables.remove("c");
    expected.ext.constraints.remove(&parts(&legacy).tables["c"].id);
    let admin = Admin::create_engine(root.join("v2"), Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &admin.inner.backend else { unreachable!() };
    let ids = db.catalog().reserve_ids(1000).unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog().apply_diff(&mut tx, &CatalogParts::default(), &expected, &ids, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog().drop_table(&mut tx, "p", None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    assert!(parts(&admin).ext.primary_index_birth.is_empty());
    drop(admin);
    drop(legacy);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn unknown_binding_blocks_every_index_drop_atomically() {
    let root = fresh();
    let (legacy, expected) = fixture(&root, false);
    let v2 = Admin::create_engine(root.join("v2"), Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &v2.inner.backend else { unreachable!() };
    let ids = db.catalog().reserve_ids(1000).unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog().apply_diff(&mut tx, &CatalogParts::default(), &expected, &ids, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    for admin in [&legacy, &v2] {
        let mut s = admin.sql_session();
        s.execute("CREATE INDEX ordinary ON p(id)", &[], None).unwrap();
        let before = catalog::parts_records(&parts(admin)).unwrap();
        for name in ["alternate", "ordinary"] {
        for suffix in ["", " RESTRICT", " CASCADE"] {
            for optional in ["", " IF EXISTS"] {
                let sql = format!("DROP INDEX{optional} {name}{suffix}");
                assert_eq!(crate::sqlstate(&s.execute(&sql, &[], None).unwrap_err()), "0A000");
                assert_eq!(catalog::parts_records(&parts(admin)).unwrap(), before);
            }
        }
        }
        s.execute("CREATE TABLE unrelated(id int)", &[], None).unwrap();
        s.execute("CREATE INDEX free ON unrelated(id)", &[], None).unwrap();
        s.execute("DROP INDEX free CASCADE", &[], None).unwrap();
    }
    drop(v2); drop(legacy);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn unknown_binding_blocks_constraint_drop_atomically() {
    let root = fresh();
    let (legacy, _) = fixture(&root, false);
    legacy.sql_session().execute("ALTER TABLE p ADD CONSTRAINT u UNIQUE(id)", &[], None).unwrap();
    let expected = parts(&legacy);
    let v2 = Admin::create_engine(root.join("v2"), Limits::default(), Engine::V2).unwrap();
    let Backend::V2(db) = &v2.inner.backend else { unreachable!() };
    let ids = db.catalog().reserve_ids(1000).unwrap();
    let mut tx = db.begin(Isolation::ReadCommitted);
    db.catalog().apply_diff(&mut tx, &CatalogParts::default(), &expected, &ids, None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    for admin in [&legacy, &v2] {
        let mut s = admin.sql_session();
        let before = catalog::parts_records(&parts(admin)).unwrap();
        let targets = s.foreign_key_targets().unwrap();
        for (suffix, code) in [("", "2BP01"), (" RESTRICT", "2BP01"), (" CASCADE", "0A000")] {
            let sql = format!("ALTER TABLE p DROP CONSTRAINT u{suffix}");
            assert_eq!(crate::sqlstate(&s.execute(&sql, &[], None).unwrap_err()), code);
            assert_eq!(catalog::parts_records(&parts(admin)).unwrap(), before);
            assert_eq!(s.foreign_key_targets().unwrap(), targets);
        }
    }
    drop(v2); drop(legacy);
    fs::remove_dir_all(root).unwrap();
}
