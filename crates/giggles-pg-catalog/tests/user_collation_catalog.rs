use giggles_pg_catalog::*;
fn rows(snapshot: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    execute(&plan(sql).unwrap().unwrap(), snapshot, &[])
        .unwrap()
        .rows
}
#[test]
fn user_metadata_comments_and_actual_versions() {
    use Value::{Bool as B, Int as I, Null as N, Text as S};
    let snapshot = CatalogSnapshot {
        collations: vec![
            vec![
                I(100000),
                S("mine".into()),
                I(2200),
                I(10),
                S("i".into()),
                B(false),
                I(-1),
                N,
                N,
                S("de-u-co-phonebk".into()),
                N,
                S("1.0".into()),
                S("hello".into()),
            ],
            vec![
                I(100004),
                S("libc_mine".into()),
                I(2200),
                I(10),
                S("c".into()),
                B(true),
                I(6),
                S("en_US.utf8".into()),
                S("en_US.utf8".into()),
                N,
                N,
                S("1.0".into()),
                N,
            ],
            vec![
                I(100008),
                S("builtin_mine".into()),
                I(2200),
                I(10),
                S("b".into()),
                B(true),
                I(6),
                N,
                N,
                S("C.UTF-8".into()),
                N,
                S("1.0".into()),
                N,
            ],
        ],
        ..Default::default()
    };
    assert_eq!(
        rows(
            &snapshot,
            "SELECT collname,collprovider,collisdeterministic,collencoding FROM pg_collation WHERE oid=100000::oid"
        ),
        vec![vec![S("mine".into()), S("i".into()), B(false), I(-1)]]
    );
    assert_eq!(
        rows(
            &snapshot,
            "SELECT obj_description(100000::oid,'pg_collation')"
        ),
        vec![vec![S("hello".into())]]
    );
    for (oid, version) in [(100000, "153.128.46"), (100004, "2.41"), (100008, "1")] {
        assert_eq!(
            rows(
                &snapshot,
                &format!("SELECT pg_collation_actual_version({oid}::oid)")
            ),
            vec![vec![S(version.into())]]
        );
    }
    assert_eq!(
        rows(&snapshot, "SELECT pg_collation_actual_version(0::oid)"),
        vec![vec![N]]
    );
}
