use super::*;
use crate::{
    capability::ItemStatus,
    pg::fence::{Grant, Membership, Role},
};

fn table() -> Table {
    Table {
        schema: "data".into(),
        name: "items".into(),
        owner: "admin".into(),
    }
}

fn saved() -> SavedState {
    SavedState {
        authority: None,
        catalog: Catalog {
            roles: vec![
                Role {
                    name: "app".into(),
                    login: true,
                    superuser: false,
                    bypass_rls: false,
                },
                Role {
                    name: "writers".into(),
                    login: false,
                    superuser: false,
                    bypass_rls: false,
                },
                Role {
                    name: "admin".into(),
                    login: true,
                    superuser: true,
                    bypass_rls: true,
                },
            ],
            memberships: vec![Membership {
                member: "app".into(),
                role: "writers".into(),
            }],
            tables: vec![table()],
            grants: vec![Grant {
                table: table(),
                column: Some("value".into()),
                grantee: Grantee::Role("writers".into()),
                grantor: "admin".into(),
                privilege: Privilege::Update,
                grant_option: true,
            }],
        },
    }
}

fn state(step: Step) -> DestinationFence {
    let mut fence = DestinationFence::new(Config {
        epoch: 49,
        applications: BTreeSet::from(["app".into()]),
        tables: vec![table()],
        timeout_ms: 1000,
        post_activate: None,
    })
    .unwrap();
    fence.saved = Some(saved());
    fence.next = step;
    if matches!(
        step,
        Step::Fenced
            | Step::CheckActivation
            | Step::Restore
            | Step::VerifyActivation
            | Step::PostActivate
            | Step::Complete
    ) {
        let mut catalog = saved().catalog;
        catalog.grants.clear();
        catalog.roles[0].login = false;
        fence.evidence.push(FenceEvidence {
            ordinal: 0,
            observed_at: "now".into(),
            catalog,
        });
    }
    if matches!(
        step,
        Step::CheckActivation
            | Step::Restore
            | Step::VerifyActivation
            | Step::PostActivate
            | Step::Complete
    ) {
        fence.final_boundary = Some("0/1234".into());
    }
    fence
}

#[test]
fn fence_and_activation_plans_preserve_acl_semantics() {
    let sql = state(Step::Fence).planned_sql().unwrap().unwrap();
    assert!(sql.contains("ALTER ROLE \"app\" NOLOGIN"));
    assert!(sql.contains("SET LOCAL ROLE \"admin\"; REVOKE UPDATE (\"value\") ON TABLE \"data\".\"items\" FROM \"writers\" CASCADE"));
    let sql = state(Step::Restore).planned_sql().unwrap().unwrap();
    assert!(sql.contains(
        "GRANT UPDATE (\"value\") ON TABLE \"data\".\"items\" TO \"writers\" WITH GRANT OPTION"
    ));
    assert!(sql.contains("ALTER ROLE \"app\" LOGIN"));
    assert!(sql.starts_with("BEGIN;"));
    assert!(sql.ends_with("COMMIT;"));
}

#[test]
fn resume_from_every_step_has_the_same_plan_and_transition() {
    for step in [
        Step::Save,
        Step::Fence,
        Step::Drain,
        Step::VerifyFence,
        Step::Fenced,
        Step::CheckActivation,
        Step::Restore,
        Step::VerifyActivation,
        Step::PostActivate,
        Step::Complete,
    ] {
        let original = state(step);
        let resumed: DestinationFence =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(original, resumed);
        assert_eq!(
            original.planned_sql().unwrap(),
            resumed.planned_sql().unwrap()
        );
        assert_eq!(original.next.next(), resumed.next.next());
    }
    assert_eq!(Step::Fenced.next(), Step::Fenced);
    assert_eq!(Step::Complete.next(), Step::Complete);
}

#[test]
fn activation_request_is_idempotent_at_every_activation_step() {
    for step in [
        Step::CheckActivation,
        Step::Restore,
        Step::VerifyActivation,
        Step::PostActivate,
        Step::Complete,
    ] {
        let mut fence = state(step);
        let before = fence.clone();
        fence
            .begin_activation("0/1234".into(), BTreeSet::new(), &mut |_| {
                panic!("duplicate intent")
            })
            .unwrap();
        assert_eq!(before, fence);
        assert!(
            fence
                .begin_activation("0/9999".into(), BTreeSet::new(), &mut |_| Ok(()))
                .is_err()
        );
    }
}

#[test]
fn activation_journal_failure_keeps_fence_and_evidence() {
    let mut fence = state(Step::Fenced);
    let before = fence.clone();
    assert!(matches!(
        fence.begin_activation("0/1234".into(), BTreeSet::new(), &mut |_| Err(
            "disk full".into()
        )),
        Err(Error::Journal(_))
    ));
    assert_eq!(fence, before);
    let mut intent = None;
    fence
        .begin_activation("0/1234".into(), BTreeSet::new(), &mut |i| {
            intent = Some(i.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(intent.unwrap().state, fence);
    assert_eq!(fence.evidence, before.evidence);
}

#[test]
fn incomplete_resume_or_boundary_cannot_activate() {
    let mut fence = state(Step::Fenced);
    fence.saved = None;
    assert!(fence.planned_sql().is_err());
    let mut fence = state(Step::Fenced);
    assert!(
        fence
            .begin_activation(String::new(), BTreeSet::new(), &mut |_| Ok(()))
            .is_err()
    );
    fence.evidence.clear();
    assert!(
        fence
            .begin_activation("0/1234".into(), BTreeSet::new(), &mut |_| Ok(()))
            .is_err()
    );
}

#[test]
fn effective_write_paths_and_login_are_fence_breaches() {
    let applications = BTreeSet::from(["app".into()]);
    let mut catalog = saved().catalog;
    assert!(validate_fenced(&catalog, &applications).is_err());
    catalog.roles[0].login = false;
    assert!(validate_fenced(&catalog, &applications).is_err());
    catalog.grants.clear();
    assert!(validate_fenced(&catalog, &applications).is_ok());
    catalog.tables[0].owner = "writers".into();
    assert!(validate_fenced(&catalog, &applications).is_err());
    catalog.tables[0].owner = "admin".into();
    catalog.memberships.push(Membership {
        member: "writers".into(),
        role: "pg_write_all_data".into(),
    });
    assert!(validate_fenced(&catalog, &applications).is_err());
    catalog.memberships[1].role = "admin".into();
    assert!(validate_fenced(&catalog, &applications).is_err());
}

#[test]
fn public_grants_are_revoked_and_original_nologin_is_restored() {
    let mut fence = state(Step::Fence);
    let saved = fence.saved.as_mut().unwrap();
    saved.catalog.roles[0].login = false;
    saved.catalog.grants[0].grantee = Grantee::Public;
    assert!(
        fence
            .planned_sql()
            .unwrap()
            .unwrap()
            .contains("FROM PUBLIC CASCADE")
    );
    fence.next = Step::Restore;
    fence.evidence = state(Step::Fenced).evidence;
    fence.final_boundary = Some("0/1234".into());
    assert!(
        fence
            .planned_sql()
            .unwrap()
            .unwrap()
            .contains("ALTER ROLE \"app\" NOLOGIN")
    );
}

#[test]
fn provisioned_nologin_roles_receive_only_manifest_access_at_activation() {
    let desired = saved();
    let mut initial = desired.clone();
    initial.catalog.roles[0].login = false;
    initial.catalog.grants.clear();
    let config = state(Step::Save).config;
    let mut fence = DestinationFence::with_activation_state(config, desired.clone()).unwrap();
    fence.saved = Some(initial.clone());
    fence.next = Step::Fence;
    let sql = fence.planned_sql().unwrap().unwrap();
    assert!(sql.contains("NOLOGIN"));
    assert!(!sql.contains("REVOKE"));
    fence.next = Step::Restore;
    fence.evidence = state(Step::Fenced).evidence;
    fence.final_boundary = Some("0/1234".into());
    let sql = fence.planned_sql().unwrap().unwrap();
    assert!(sql.contains("GRANT UPDATE"));
    assert!(sql.contains("ALTER ROLE \"app\" LOGIN"));
    assert_eq!(fence.saved.unwrap(), initial);
    let mut invalid = desired.catalog.clone();
    invalid.roles[0].superuser = true;
    assert!(
        validate_activation_state(&initial.catalog, &invalid, &BTreeSet::from(["app".into()]))
            .is_err()
    );
}

fn sequence(increment: i64, cycle: bool, is_called: bool) -> SequenceState {
    SequenceState {
        sequence: Sequence {
            name: SequenceName {
                schema: "odd's".into(),
                name: "seq\"name".into(),
            },
            identity_column: Some(("data".into(), "items".into(), "id".into())),
            increment,
            minimum: -100,
            maximum: 100,
            cycle,
        },
        last_value: -17,
        is_called,
    }
}

#[test]
fn sequence_plan_preserves_called_descending_cycle_and_identity() {
    for increment in [1, -3] {
        for cycle in [false, true] {
            for is_called in [false, true] {
                let state = sequence(increment, cycle, is_called);
                let sql = sequence_sync_sql(std::slice::from_ref(&state)).unwrap();
                assert_eq!(
                    sql,
                    vec![format!(
                        "SELECT setval(E'\"odd''s\".\"seq\"\"name\"'::regclass,-17,{is_called});"
                    )]
                );
                assert!(state.sequence.identity_column.is_some());
                assert_eq!(sql, sequence_sync_sql(&[state]).unwrap());
            }
        }
    }
}

#[test]
fn sequence_plan_rejects_invalid_ranges_and_duplicate_names() {
    let mut state = sequence(-1, true, false);
    assert!(sequence_sync_sql(&[state.clone(), state.clone()]).is_err());
    state.last_value = 101;
    assert!(sequence_sync_sql(&[state.clone()]).is_err());
    state.last_value = 0;
    state.sequence.increment = 0;
    assert!(sequence_sync_sql(&[state]).is_err());
    assert!(sequence_sync_sql(&[]).unwrap().is_empty());
}

#[test]
fn only_external_side_effects_can_be_acknowledged() {
    for kind in [
        BlockerKind::InvalidIndex,
        BlockerKind::NotValidConstraint,
        BlockerKind::ExternalSideEffect,
    ] {
        let mut blocker = Blocker {
            kind: kind.clone(),
            schema: "data".into(),
            table: "items".into(),
            name: "object".into(),
            acknowledged: false,
        };
        assert!(blocker.blocks());
        assert_eq!(
            blocker.capability_item().status,
            ItemStatus::RequiresExternalChange
        );
        blocker.acknowledged = true;
        assert_eq!(blocker.blocks(), kind != BlockerKind::ExternalSideEffect);
        assert!(!blocker.capability_item().sampled);
    }
}

#[test]
fn execution_tags_both_sql_steps_without_changing_pure_plans() {
    struct Recording {
        rows: Rows,
        sql: Vec<String>,
    }
    impl Connection for Recording {
        fn query(&mut self, sql: &str, _: Duration) -> Result<Rows, String> {
            self.sql.push(sql.into());
            Ok(if sql == crate::pg::ddl::ADMIN_INSTALLATION_SQL {
                self.rows.clone()
            } else {
                vec![]
            })
        }
    }
    for step in [Step::Fence, Step::Restore] {
        for installed in [false, true] {
            let mut fence = state(step);
            let plan = fence.planned_sql().unwrap().unwrap();
            assert!(!plan.contains("admin_origin"));
            let mut connection = Recording {
                rows: if installed {
                    vec![vec!["d9".into()]]
                } else {
                    vec![]
                },
                sql: vec![],
            };
            fence
                .execute_next(&mut connection, &mut |_| Ok(()))
                .unwrap();
            assert_eq!(connection.sql.len(), 2);
            assert_eq!(connection.sql[0], crate::pg::ddl::ADMIN_INSTALLATION_SQL);
            let actual = &connection.sql[1];
            assert!(actual.ends_with("COMMIT;"));
            if installed {
                assert_eq!(
                    actual,
                    &plan.replacen(
                        "BEGIN; ",
                        "BEGIN; SET LOCAL skedaddle.admin_origin = 'd9'; ",
                        1
                    )
                );
            } else {
                assert_eq!(actual, &plan);
            }
        }
        let mut connection = Recording {
            rows: vec![vec!["one".into()], vec!["two".into()]],
            sql: vec![],
        };
        let mut fence = state(step);
        assert!(
            fence
                .execute_next(&mut connection, &mut |_| Ok(()))
                .is_err()
        );
        assert_eq!(fence.next, step);
        assert_eq!(connection.sql.len(), 1);
    }
}
