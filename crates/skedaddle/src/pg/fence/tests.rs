use super::*;
use crate::event::{SourceIdentity, SourceOrigin, TxnId};

fn role(name: &str) -> Role {
    Role {
        name: name.into(),
        login: true,
        superuser: false,
        bypass_rls: false,
    }
}

fn fixture() -> (Fence, Catalog) {
    let table = Table {
        schema: "public".into(),
        name: "items".into(),
        owner: "owner".into(),
    };
    let catalog = Catalog {
        roles: vec![
            role("app"),
            role("bridge"),
            role("writers"),
            role("owner"),
            role("outside"),
        ],
        memberships: vec![
            Membership {
                member: "app".into(),
                role: "bridge".into(),
            },
            Membership {
                member: "bridge".into(),
                role: "writers".into(),
            },
            Membership {
                member: "writers".into(),
                role: "bridge".into(),
            },
        ],
        tables: vec![table.clone()],
        grants: vec![Grant {
            table: table.clone(),
            column: Some("value".into()),
            grantee: Grantee::Role("writers".into()),
            grantor: "owner".into(),
            privilege: Privilege::Update,
            grant_option: true,
        }],
    };
    let mut fence = Fence::new(FenceConfig {
        epoch: 10,
        applications: BTreeSet::from(["app".into()]),
        tables: vec![table],
        acknowledged: BTreeSet::new(),
        deadline_ms: 100,
    })
    .unwrap();
    fence.bind_migration("fence-test").unwrap();
    (fence, catalog)
}

#[test]
fn effective_writers_nested_column_owner_and_public() {
    let (_, mut catalog) = fixture();
    assert_eq!(
        effective_writers(&catalog),
        BTreeSet::from([
            "app".into(),
            "bridge".into(),
            "writers".into(),
            "owner".into()
        ])
    );
    catalog.grants[0].grantee = Grantee::Public;
    assert_eq!(effective_writers(&catalog).len(), catalog.roles.len());
    catalog.grants.clear();
    catalog.roles[4].superuser = true;
    assert_eq!(
        effective_writers(&catalog),
        BTreeSet::from(["owner".into(), "outside".into()])
    );
}

#[test]
fn predefined_write_role_and_public_named_role_are_distinct() {
    let (_, mut catalog) = fixture();
    catalog.roles.push(role("pg_write_all_data"));
    catalog.roles.push(role("PUBLIC"));
    catalog.memberships.push(Membership {
        member: "outside".into(),
        role: "pg_write_all_data".into(),
    });
    catalog.grants[0].grantee = Grantee::Role("PUBLIC".into());
    let writers = effective_writers(&catalog);
    assert!(writers.contains("outside"));
    assert!(writers.contains("pg_write_all_data"));
    assert!(writers.contains("PUBLIC"));
    assert!(!writers.contains("app"));
    assert_eq!(Grantee::Role("PUBLIC".into()).sql(), "\"PUBLIC\"");
    assert_eq!(Grantee::Public.sql(), "PUBLIC");
}

#[test]
fn save_then_unfence_round_trip_preserves_every_grant_attribute() {
    let (mut fence, mut catalog) = fixture();
    let mut downstream = catalog.grants[0].clone();
    downstream.grantor = "writers".into();
    downstream.grantee = Grantee::Role("outside".into());
    downstream.grant_option = false;
    catalog.grants.insert(0, downstream);
    catalog.roles[0].login = false;
    let saved = SavedState {
        catalog,
        authority: Some(Authority { epoch: 9 }),
    };
    fence.saved = Some(saved.clone());
    let restored: Fence = serde_json::from_slice(&serde_json::to_vec(&fence).unwrap()).unwrap();
    assert_eq!(restored.saved.as_ref(), Some(&saved));
    let sql =
        restoration_sql(restored.saved.as_ref().unwrap(), &fence.config.applications).unwrap();
    assert!(
        sql.find("SET LOCAL ROLE \"owner\"").unwrap()
            < sql.find("SET LOCAL ROLE \"writers\"").unwrap()
    );
    assert!(sql.contains("UPDATE (\"value\")"));
    assert!(sql.contains("WITH GRANT OPTION"));
    assert!(sql.contains("ALTER ROLE \"app\" NOLOGIN"));
    assert_eq!(
        sql,
        restoration_sql(&saved, &fence.config.applications).unwrap()
    );
}

#[test]
fn residual_classification_requires_each_explicit_acknowledgement() {
    let (fence, mut catalog) = fixture();
    catalog.roles[4].superuser = true;
    catalog.roles[4].bypass_rls = true;
    let unacked = classify_residuals(&catalog, &fence.config.applications, &BTreeSet::new());
    for kind in [
        ResidualKind::Superuser,
        ResidualKind::TableOwner,
        ResidualKind::BypassRls,
        ResidualKind::OutOfScopeWriter,
        ResidualKind::InScopeWriter,
    ] {
        assert!(
            unacked
                .iter()
                .any(|r| r.principal.kind == kind && !r.acknowledged)
        );
    }
    let acknowledgements = unacked.iter().map(|r| r.principal.clone()).collect();
    assert!(
        classify_residuals(&catalog, &fence.config.applications, &acknowledgements)
            .iter()
            .all(|r| r.acknowledged)
    );
}

struct NoDatabase;
impl Connection for NoDatabase {
    fn query(&mut self, _: &str, _: Duration) -> Result<Rows, String> {
        panic!("failed journal must prevent database execution")
    }
}

#[test]
fn resume_from_each_step_retains_state_and_journals_before_execution() {
    let (mut fence, catalog) = fixture();
    fence.saved = Some(SavedState {
        catalog,
        authority: None,
    });
    fence.drain_lsn = Some(10);
    for step in [
        Step::PreFence,
        Step::Save,
        Step::NoLogin,
        Step::Revoke,
        Step::Terminate,
        Step::Drain,
        Step::Recheck,
        Step::Authority,
    ] {
        fence.next = step;
        let mut resumed: Fence =
            serde_json::from_str(&serde_json::to_string(&fence).unwrap()).unwrap();
        let mut calls = 0;
        let error = resumed.execute_next(
            &mut NoDatabase,
            &mut |intent| {
                calls += 1;
                assert_eq!(intent, &Intent::Step(Box::new(fence.clone())));
                Err("injected journal durability failure".into())
            },
            &mut || panic!("journal must precede hook"),
        );
        assert!(matches!(error, Err(FenceError::Journal(_))));
        assert_eq!(calls, 1);
        assert_eq!(resumed, fence);
    }
    fence.next = Step::Complete;
    fence
        .execute_next(
            &mut NoDatabase,
            &mut |_| panic!("complete is idempotent"),
            &mut || panic!(),
        )
        .unwrap();
}

#[test]
fn pre_fence_failure_changes_nothing_and_retry_completes_hook_step() {
    let (mut fence, _) = fixture();
    assert!(matches!(
        fence.execute_next(&mut NoDatabase, &mut |_| Ok(()), &mut || Err(
            "hook failed".into()
        )),
        Err(FenceError::Hook(_))
    ));
    assert_eq!(fence.next, Step::PreFence);
    assert!(fence.saved.is_none());
    fence
        .execute_next(&mut NoDatabase, &mut |_| Ok(()), &mut || Ok(()))
        .unwrap();
    assert_eq!(fence.next, Step::Save);
}

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "pg:test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}

#[test]
fn breach_detection_checks_boundary_exact_origin_and_window() {
    let mut txn = AssembledTxn::new(
        SourceIdentity {
            engine: "postgresql".into(),
            system_id: "1".into(),
            lineage: "1".into(),
            db_name: "test".into(),
            slot: "test".into(),
        },
        TxnId(vec![1]),
        Position::new("pg:test", 11_u64.to_be_bytes()),
        None,
        vec![],
    )
    .unwrap();
    let drain = Position::new("pg:test", 10_u64.to_be_bytes());
    let check =
        |t: &AssembledTxn, w| detect_breach(t, &drain, &Order, "skedaddle/test", w).unwrap();
    assert_eq!(
        check(&txn, FenceWindow::BeforeActivation),
        Breach::AbortBeforeActivation
    );
    assert_eq!(
        check(&txn, FenceWindow::RollbackWindow),
        Breach::CriticalAlert {
            rollback_eligible: false
        }
    );
    for name in ["other-origin", "skedaddle/test-extra"] {
        txn.source_origin = Some(SourceOrigin {
            name: name.into(),
            lsn: 11,
        });
        assert_eq!(
            check(&txn, FenceWindow::BeforeActivation),
            Breach::AbortBeforeActivation
        );
    }
    txn.source_origin = Some(SourceOrigin {
        name: "skedaddle/test".into(),
        lsn: 11,
    });
    assert_eq!(check(&txn, FenceWindow::BeforeActivation), Breach::None);
    txn.source_origin = None;
    for commit in [9_u64, 10] {
        txn.commit_position = Position::new("pg:test", commit.to_be_bytes());
        assert_eq!(check(&txn, FenceWindow::BeforeActivation), Breach::None);
    }
    txn.commit_position = Position::new("other", vec![11]);
    assert!(
        detect_breach(
            &txn,
            &drain,
            &Order,
            "skedaddle/test",
            FenceWindow::BeforeActivation
        )
        .is_err()
    );
}

#[test]
fn sql_quoting_and_atomic_authority_prerequisites() {
    assert!(!FENCE_METADATA_SQL.contains("CREATE"));
    assert!(FENCE_METADATA_SQL.contains("to_regclass('_skedaddle.fence_state')"));
    assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    assert_eq!(literal("a'\\b"), "E'a''\\\\b'");
    assert_eq!(parse_lsn("1/FFFFFFFF").unwrap(), 0x1ffffffff);
    assert!(parse_lsn("1/100000000").is_err());
    let (mut fence, _) = fixture();
    fence.next = Step::Authority;
    assert!(matches!(
        fence.validate_resume(),
        Err(FenceError::Invalid(_))
    ));
}

#[test]
fn migration_binding_survives_resume_and_rejects_namespace_changes() {
    let (mut fence, _) = fixture();
    fence.bind_migration("fence-test").unwrap();
    assert!(fence.bind_migration("other").is_err());
    assert!(fence.bind_migration("").is_err());
    let resumed: Fence = serde_json::from_slice(&serde_json::to_vec(&fence).unwrap()).unwrap();
    assert_eq!(resumed.migration, "fence-test");
    fence.migration.clear();
    assert!(matches!(
        fence.execute_next(
            &mut NoDatabase,
            &mut |_| panic!("unbound migration must not journal"),
            &mut || panic!()
        ),
        Err(FenceError::Invalid(_))
    ));
}

#[test]
fn saved_fence_sql_keys_both_migration_and_epoch() {
    struct Keyed {
        rows: std::collections::BTreeMap<String, String>,
        migration: String,
        epoch: i64,
        encoded: String,
    }
    impl Connection for Keyed {
        fn query(&mut self, sql: &str, _: Duration) -> Result<Rows, String> {
            if sql == FENCE_METADATA_SQL {
                return Ok(vec![vec!["t".into()]]);
            }
            let key = literal(&self.migration);
            if sql
                == format!(
                    "SELECT state FROM _skedaddle.fence_state WHERE migration={key} AND epoch={}",
                    self.epoch
                )
            {
                return Ok(self
                    .rows
                    .get(&self.migration)
                    .map(|s| vec![vec![s.clone()]])
                    .unwrap_or_default());
            }
            assert_eq!(
                sql,
                format!(
                    "INSERT INTO _skedaddle.fence_state(migration,epoch,state) VALUES({key},{},{}) ON CONFLICT(migration,epoch) DO NOTHING",
                    self.epoch,
                    literal(&self.encoded)
                )
            );
            self.rows
                .entry(self.migration.clone())
                .or_insert(self.encoded.clone());
            Ok(vec![])
        }
    }
    let (mut first, catalog) = fixture();
    first.saved = Some(SavedState {
        catalog,
        authority: None,
    });
    first.migration = "first'quoted".into();
    let mut second = first.clone();
    second.migration = "second".into();
    let mut db = Keyed {
        rows: Default::default(),
        migration: String::new(),
        epoch: first.config.epoch,
        encoded: String::new(),
    };
    for fence in [&mut first, &mut second] {
        db.migration = fence.migration.clone();
        db.encoded = serde_json::to_string(fence).unwrap();
        fence
            .save(&mut db, &mut |_| Ok(()), Duration::from_secs(1))
            .unwrap();
    }
    assert_eq!(db.rows.len(), 2);
    db.migration = first.migration.clone();
    db.encoded = serde_json::to_string(&first).unwrap();
    first.saved = None;
    first
        .save(&mut db, &mut |_| Ok(()), Duration::from_secs(1))
        .unwrap();
    assert!(first.saved.is_some());
    // Even a corrupt row under the right key cannot import another migration.
    db.rows.insert(
        first.migration.clone(),
        serde_json::to_string(&second).unwrap(),
    );
    assert!(matches!(
        first.save(&mut db, &mut |_| Ok(()), Duration::from_secs(1)),
        Err(FenceError::Invalid(_))
    ));
    let ddl = crate::pg::ddl::TriggerSql::new("fence_key", 180000).unwrap();
    assert!(ddl.install.contains("fence_state(migration text NOT NULL,epoch bigint NOT NULL,state text NOT NULL,PRIMARY KEY(migration,epoch))"));
}
