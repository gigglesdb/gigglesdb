use super::*;

#[test]
fn logical_archive_catalog_refuses_descriptors_and_preserves_state() {
    for unique in [false, true] {
        for populated in [false, true] {
            for nulls in [false, true] {
                let mut state = empty(7781);
                sql(
                    &mut state.sql,
                    "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
                );
                sql(&mut state.sql, "CREATE TABLE other (id BIGINT PRIMARY KEY)");
                let t = state.sql.tables["items"].clone();
                state
                    .sql
                    .create_secondary_index_internal_engine(
                        &t,
                        state.sql.epoch,
                        "by_tag",
                        &[t.columns[1].id],
                        unique,
                        || false,
                    )
                    .unwrap();
                if populated {
                    for id in [1, 2] {
                        let tag = if nulls {
                            "NULL".to_owned()
                        } else {
                            format!("'v{id}'")
                        };
                        sql(
                            &mut state.sql,
                            &format!("INSERT INTO items VALUES ({id},{tag})"),
                        );
                    }
                }
                let location = path("logical-archive-guard");
                let admin =
                    Admin::create_state(&location, Limits::default(), state, RecoveryPoint::None)
                        .unwrap();
                let before = admin.inner.inspect(|s| Ok(records(s).unwrap())).unwrap();
                assert_eq!(admin.logical_export_catalog(), Err(CoreError::Unsupported));
                let after = admin.inner.inspect(|s| Ok(records(s).unwrap())).unwrap();
                assert_eq!(before, after);
                drop(admin);
                let reopened = Admin::open(&location, Limits::default()).unwrap();
                assert_eq!(
                    reopened.logical_export_catalog(),
                    Err(CoreError::Unsupported)
                );
                assert_eq!(
                    before,
                    reopened.inner.inspect(|s| Ok(records(s).unwrap())).unwrap()
                );
            }
        }
    }
}

#[test]
fn logical_archive_catalog_accepts_no_descriptors_and_rejects_empty_namespace8() {
    let mut state = empty(7782);
    sql(
        &mut state.sql,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT NOT NULL)",
    );
    let location = path("logical-archive-no-index");
    let admin = Admin::create_state(
        location,
        Limits::default(),
        state.clone(),
        RecoveryPoint::None,
    )
    .unwrap();
    assert_eq!(
        admin.logical_export_catalog().unwrap(),
        admin.catalog().unwrap()
    );
    let mut malformed = records(&state).unwrap();
    metadata(&mut malformed, 7);
    malformed.insert(
        vec![8],
        index_codec::encode(
            &crate::relational::secondary_index::Sidecar::default(),
            || false,
        )
        .unwrap(),
    );
    assert!(matches!(
        from_records(malformed),
        Err(CoreError::Corrupt(_))
    ));
}
