use super::*;
use crate::Limits;
use std::path::PathBuf;
fn path(label: &str) -> PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!(
        "gd42-public-{label}-{:032x}",
        u128::from_be_bytes(nonce)
    ))
}
fn request() -> UniqueIndexRequest {
    UniqueIndexRequest {
        table: "items".into(),
        name: "unique_tag".into(),
        columns: vec!["tag".into()],
    }
}
fn fixture() -> (Admin, Engine) {
    let admin = Admin::create(path("validation"), Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
            &[],
            None,
        )
        .unwrap();
    (admin.clone(), Engine::new(admin))
}
#[test]
fn strict_validation_prepare_and_stale_domain_epoch() {
    for field in [
        "unique",
        "id",
        "predicate",
        "nulls",
        "direction",
        "scope",
        "include",
    ] {
        let value =
            serde_json::json!({"table":"items","name":"unique_tag","columns":["tag"],field:true});
        assert!(serde_json::from_value::<UniqueIndexRequest>(value).is_err());
    }
    let (admin, engine) = fixture();
    let before = admin.catalog().unwrap();
    let bound = engine.prepare_unique_index_bound(&request()).unwrap();
    assert_eq!(bound.description().request, request());
    assert_eq!(admin.catalog().unwrap(), before);
    let (_, other) = fixture();
    assert_eq!(
        other.execute_unique_index_bound(&bound),
        Err(core(CoreError::Sql(SqlError::StaleSchema)))
    );
    for columns in [
        vec![],
        vec!["tag".into(); 65],
        vec!["tag".into(), "tag".into()],
    ] {
        let mut invalid = request();
        invalid.columns = columns;
        assert!(engine.execute_unique_index(&invalid).is_err());
        assert_eq!(admin.catalog().unwrap(), before);
    }
    engine
        .execute_catalog(&CatalogRequest::CreateIndex {
            table: "items".into(),
            name: "ordinary".into(),
            columns: vec!["id".into()],
        })
        .unwrap();
    assert_eq!(
        engine.execute_unique_index_bound(&bound),
        Err(core(CoreError::Sql(SqlError::StaleSchema)))
    );
}
#[test]
fn execution_rechecks_population_and_enforces_null_distinct() {
    let (admin, engine) = fixture();
    let bound = engine.prepare_unique_index_bound(&request()).unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES (1,'a')", &[], None)
        .unwrap();
    let before = admin.catalog().unwrap();
    assert_eq!(
        engine.execute_unique_index_bound(&bound),
        Err(Error::PlanRequired)
    );
    assert_eq!(admin.catalog().unwrap(), before);
    admin
        .sql_session()
        .execute("DELETE FROM items WHERE id=1", &[], None)
        .unwrap();
    engine.execute_unique_index_bound(&bound).unwrap();
    let mut sql = admin.sql_session();
    for text in [
        "INSERT INTO items VALUES (1,'a')",
        "INSERT INTO items VALUES (2,NULL)",
        "INSERT INTO items VALUES (3,NULL)",
    ] {
        sql.execute(text, &[], None).unwrap();
    }
    assert_eq!(
        sql.execute("INSERT INTO items VALUES (4,'a')", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
}

#[test]
fn public_unique_generation_race_and_cancel_preserve_allocator() {
    for prepared in [false, true] {
        for cancel in [false, true] {
            let (admin, engine) = fixture();
            let before = admin.catalog().unwrap();
            let bound = engine.prepare_unique_index_bound(&request()).unwrap();
            let outside = admin.clone();
            let cancellation = crate::cancellation::Cancellation::default();
            let signal = cancellation.clone();
            let result = cancellation.scope(|| {
                super::super::tests::with_publish_hook(
                    move || {
                        assert!(
                            outside
                                .inspect_secondary_catalog_engine(|_, _, _, d| d.is_empty())
                                .unwrap()
                        );
                        if cancel {
                            signal.cancel();
                        } else {
                            outside
                                .sql_session()
                                .execute("INSERT INTO items VALUES (1,'a')", &[], None)
                                .unwrap();
                        }
                    },
                    || {
                        if prepared {
                            engine.execute_unique_index_bound(&bound)
                        } else {
                            engine.execute_unique_index(&request())
                        }
                    },
                )
            });
            assert_eq!(
                result,
                Err(if cancel {
                    core(CoreError::Cancelled)
                } else {
                    Error::Conflict
                })
            );
            assert_eq!(admin.catalog().unwrap(), before);
            assert!(
                admin
                    .inspect_secondary_catalog_engine(|_, _, _, d| d.is_empty())
                    .unwrap()
            );
            if !cancel {
                admin
                    .sql_session()
                    .execute("DELETE FROM items WHERE id=1", &[], None)
                    .unwrap();
            }
            let outcome = engine.execute_unique_index(&request()).unwrap();
            assert_eq!(
                outcome.index_id,
                Some(before.1[0].columns.last().unwrap().id + 1)
            );
        }
    }
}

#[test]
fn public_unique_strict_binding_and_legacy_enum_consumers() {
    let old = CatalogRequest::CreateIndex {
        table: "items".into(),
        name: "old".into(),
        columns: vec!["tag".into()],
    };
    let encoded = serde_json::to_value(&old).unwrap();
    assert_eq!(
        serde_json::from_value::<CatalogRequest>(encoded).unwrap(),
        old
    );
    let (admin, engine) = fixture();
    let before = admin.catalog().unwrap();
    for name in ["", "bad.name", "bad name"] {
        let mut bad = request();
        bad.name = name.into();
        assert!(engine.prepare_unique_index_bound(&bad).is_err());
    }
    for column in ["missing", "tag.bad"] {
        let mut bad = request();
        bad.columns = vec![column.into()];
        assert!(engine.prepare_unique_index_bound(&bad).is_err());
    }
    for _ in 0..3 {
        engine.prepare_unique_index_bound(&request()).unwrap();
    }
    assert_eq!(admin.catalog().unwrap(), before);
    assert_eq!(
        engine.execute_unique_index(&request()).unwrap().index_id,
        Some(before.1[0].columns.last().unwrap().id + 1)
    );
}
