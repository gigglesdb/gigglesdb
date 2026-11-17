use super::*;
use crate::operations::{CatalogRequest, Engine, UniqueIndexRequest};
use std::path::PathBuf;
fn request() -> UniqueIndexRequest {
    UniqueIndexRequest {
        table: "items".into(),
        name: "unique_tag".into(),
        columns: vec!["tag".into()],
    }
}
fn path(label: &str) -> PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!(
        "gd42-public-{label}-{:032x}",
        u128::from_be_bytes(nonce)
    ))
}
#[test]
fn public_unique_actual_writer_all_families_export_restore_reopen_fixtures() {
    let root = std::env::var_os("GD42_PUBLIC_FIXTURES")
        .map(PathBuf::from)
        .unwrap_or_else(|| path("actual-writer"));
    fs::create_dir_all(&root).unwrap();
    println!("actual writer fixture root {}", root.display());
    let mut cases = 0;
    for typed in [false, true] {
        for native in [false, true] {
            for temporal in [false, true] {
                for populated in [false, true] {
                    for mixed in [false, true] {
                        let name = format!(
                            "t{}n{}d{}r{}m{}",
                            u8::from(typed),
                            u8::from(native),
                            u8::from(temporal),
                            u8::from(populated),
                            u8::from(mixed)
                        );
                        let original = root.join(format!("{name}-public-create"));
                        let public = Admin::create(&original, Limits::default()).unwrap();
                        let mut ddl =
                            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT".to_owned();
                        if typed {
                            ddl.push_str(", extra NUMERIC");
                        }
                        if temporal {
                            ddl.push_str(", stamp DATE");
                        }
                        ddl.push(')');
                        public.sql_session().execute(&ddl, &[], None).unwrap();
                        let engine = Engine::new(public.clone());
                        engine.execute_unique_index(&request()).unwrap();
                        if mixed {
                            engine
                                .execute_catalog(&CatalogRequest::CreateIndex {
                                    table: "items".into(),
                                    name: "ordinary_tag".into(),
                                    columns: vec!["tag".into()],
                                })
                                .unwrap();
                        }
                        if populated {
                            for text in [
                                "INSERT INTO items (id,tag) VALUES (1,'a')",
                                "INSERT INTO items (id,tag) VALUES (2,'b')",
                            ] {
                                public.sql_session().execute(text, &[], None).unwrap();
                            }
                        }
                        // Retained empty native receipt state is added only AFTER
                        // the public empty-database create gate has succeeded.
                        let mut s = public.inner.read().unwrap();
                        if native {
                            s.native_outcomes = Some(Default::default());
                        }
                        drop(engine);
                        drop(public);
                        let r = records(&s).unwrap();
                        let metadata: Metadata = decode(&r[&vec![0]]).unwrap();
                        assert_eq!(
                            metadata.format,
                            7 + u32::from(native) + 2 * u32::from(typed) + 12 * u32::from(temporal)
                        );
                        assert_eq!(&r[&vec![8]][8..12], &2u32.to_le_bytes());
                        assert_eq!(records(&from_records(r.clone()).unwrap()).unwrap(), r);
                        fs::write(root.join(format!("{name}.records")), encode(&r).unwrap())
                            .unwrap();
                        let source = root.join(format!("{name}-source"));
                        let export = root.join(format!("{name}.gdexp"));
                        let dest = root.join(format!("{name}-restored"));
                        let admin = Admin::create_state(
                            &source,
                            Limits::default(),
                            s.clone(),
                            RecoveryPoint::None,
                        )
                        .unwrap();
                        admin.export(&export).unwrap();
                        Admin::verify_export(&export).unwrap();
                        drop(admin);
                        let reopened = Admin::open(&source, Limits::default()).unwrap();
                        assert_eq!(
                            [vec![5], vec![8]].map(|key| records(&reopened.inner.read().unwrap())
                                .unwrap()[&key]
                                .clone()),
                            [vec![5], vec![8]].map(|key| r[&key].clone())
                        );
                        drop(reopened);
                        let restored = Admin::restore(&export, &dest, Limits::default()).unwrap();
                        assert_eq!(
                            [vec![5], vec![8]].map(|key| records(&restored.inner.read().unwrap())
                                .unwrap()[&key]
                                .clone()),
                            [vec![5], vec![8]].map(|key| r[&key].clone())
                        );
                        if populated {
                            assert_eq!(
                                restored.sql_session().execute(
                                    "UPDATE items SET tag='a' WHERE id=2",
                                    &[],
                                    None
                                ),
                                Err(CoreError::Sql(relational::SqlError::Duplicate))
                            );
                        }
                        let mut combined = r.clone();
                        combined.insert(vec![9], vec![]);
                        assert!(matches!(
                            from_records(combined),
                            Err(CoreError::Unsupported)
                        ));
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 32);
}

#[test]
fn public_unique_prepare_and_refusals_preserve_exact_records() {
    use crate::operations::Error as OpError;
    for kind in ["native", "sql", "rows"] {
        let a = Admin::create(path(kind), Limits::default()).unwrap();
        a.sql_session()
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
                &[],
                None,
            )
            .unwrap();
        a.inner
            .change_decision(|s| {
                match kind {
                    "native" => s.native_outcomes = Some(Default::default()),
                    "sql" => {
                        s.sql
                            .execute(
                                &relational::Prepared::parse(
                                    "INSERT INTO items VALUES (1,'a')",
                                    s.view.domain,
                                    s.sql.epoch,
                                )
                                .unwrap(),
                                &[],
                                None,
                            )
                            .unwrap();
                    }
                    "rows" => {
                        s.view.rows.insert(
                            Key {
                                tenant: TenantId(1),
                                row: RowId(1),
                            },
                            Row {
                                status: Status::Failed,
                                failed_at: 0,
                                attempt_count: 0,
                                version: 1,
                            },
                        );
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })
            .unwrap();
        let engine = Engine::new(a.clone());
        let before = records(&a.inner.read().unwrap()).unwrap();
        let b = engine.prepare_unique_index_bound(&request()).unwrap();
        assert_eq!(records(&a.inner.read().unwrap()).unwrap(), before);
        for result in [
            engine.execute_unique_index(&request()),
            engine.execute_unique_index_bound(&b),
        ] {
            assert_eq!(
                result,
                Err(if kind == "application" {
                    OpError::Core("Unsupported".into())
                } else {
                    OpError::PlanRequired
                })
            );
            assert_eq!(records(&a.inner.read().unwrap()).unwrap(), before);
        }
    }
}

#[test]
fn public_unique_tiny_create_sql_native_composite_null_enforcement() {
    use crate::relational::Value::{Int, Null, Text};
    let a = Admin::create(path("tiny"), Limits::default()).unwrap();
    a.sql_session()
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT, other TEXT)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(a.clone());
    let mut r = request();
    r.columns.push("other".into());
    let before = records(&a.inner.read().unwrap()).unwrap();
    let bound = engine.prepare_unique_index_bound(&r).unwrap();
    assert_eq!(records(&a.inner.read().unwrap()).unwrap(), before);
    engine.execute_unique_index_bound(&bound).unwrap();
    let t = a.inner.read().unwrap().sql.tables["items"].clone();
    a.sql_session()
        .insert_engine(&t, vec![Int(1), Text("a".into()), Text("x".into())])
        .unwrap();
    assert_eq!(
        a.sql_session()
            .execute("INSERT INTO items VALUES (2,'a','x')", &[], None),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    );
    a.sql_session()
        .insert_engine(&t, vec![Int(2), Text("a".into()), Null])
        .unwrap();
    a.sql_session()
        .insert_engine(&t, vec![Int(3), Text("a".into()), Null])
        .unwrap();
    let before = a.inner.read().unwrap().sql;
    let mut tx = a.sql_session();
    tx.begin_engine().unwrap();
    tx.insert_engine(&t, vec![Int(4), Text("b".into()), Text("y".into())])
        .unwrap();
    assert_eq!(
        tx.insert_engine(&t, vec![Int(5), Text("a".into()), Text("x".into())]),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    );
    assert!(tx.commit_engine().is_err());
    tx.rollback();
    assert_eq!(a.inner.read().unwrap().sql, before);
}
