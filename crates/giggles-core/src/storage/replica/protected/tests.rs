use super::*;
use std::{cell::RefCell, collections::BTreeSet};

fn key(row: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(row),
    }
}
fn image(s: &State) -> Image {
    Image::from_bytes(encode(&records(s).unwrap()).unwrap()).unwrap()
}
fn edit(i: &Image, f: impl FnOnce(&mut State)) -> Image {
    let mut s = image_state(i).unwrap();
    f(&mut s);
    image(&s)
}
struct Proof {
    locked: Binding,
    fresh: (u64, u64, u64),
    decision: RefCell<Option<(DecisionId, [u8; 32], Outcome)>>,
}
impl TrustedRuntime for Proof {
    fn verify_locked(&self, b: &Binding) -> Result<(), CoreError> {
        let mut expected = self.locked.clone();
        expected.intent.lower = b.intent.lower;
        expected.intent.upper = b.intent.upper;
        expected.intent.fence = b.intent.fence;
        if expected != *b {
            return Err(CoreError::Fenced);
        }
        Ok(())
    }
    fn verify_decision_fence(&self, b: &Binding) -> Result<(), CoreError> {
        if (b.intent.lower, b.intent.upper, b.intent.fence) != self.fresh {
            return Err(CoreError::Fenced);
        }
        Ok(())
    }
    fn decision(&self, id: &DecisionId, digest: [u8; 32]) -> Result<Outcome, CoreError> {
        match &*self.decision.borrow() {
            Some((d, h, o)) if id == d && *h == digest => Ok(*o),
            _ => Err(CoreError::Fenced),
        }
    }
}
struct Fixture {
    authority: Image,
    identity: Authority,
    rows: Vec<Partition>,
    intent: Intent,
}
impl Fixture {
    fn new() -> Self {
        let mut a = empty(73);
        a.view.tasks.insert(
            TaskId(1),
            TaskState {
                grant: Grant {
                    task: TaskId(1),
                    issuer: Principal(1),
                    executor: Principal(2),
                    approver: Principal(3),
                    policy: Policy {
                        tenant: TenantId(42),
                        columns: BTreeSet::from([Column::Status]),
                        transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                        incident: (100, 110),
                        distinct_limit: 2,
                        transition_limit: 2,
                        approval_above: 2,
                        valid_from: 1,
                        expires: 1000,
                    },
                },
                revoked: false,
                touched: BTreeSet::new(),
                transitions: 0,
            },
        );
        a.accounting.insert(
            TaskId(1),
            TaskAccounting {
                limits: TaskResources {
                    read_rows: 1000,
                    disclosed_rows: 100,
                    encoded_bytes: 1_000_000,
                },
                used: TaskResources::default(),
            },
        );
        let mut data = empty(73);
        for n in 1..=2 {
            data.view.rows.insert(
                key(n),
                Row {
                    status: Status::Failed,
                    failed_at: 103,
                    attempt_count: 0,
                    version: 0,
                },
            );
        }
        Self {
            authority: image(&a),
            identity: Authority {
                location: 11,
                generation: 7,
                applied: 4,
                domain: 73,
                task: TaskId(1),
            },
            rows: vec![Partition {
                owner: 22,
                epoch: 8,
                applied: 19,
                start: Key {
                    tenant: TenantId(0),
                    row: RowId(0),
                },
                end: None,
                image: image(&data),
            }],
            intent: Intent {
                principal: Principal(2),
                request: Request {
                    operation: OperationId(10),
                    selection: Selection::Keys(vec![key(1)]),
                    column: Column::Status,
                    after: Status::Queued,
                },
                approval: None,
                lower: 10,
                upper: 12,
                fence: 9,
            },
        }
    }
    fn proof(&self) -> Proof {
        let (_, locked) = assemble(
            &self.authority,
            self.identity.clone(),
            &self.rows,
            self.intent.clone(),
        )
        .unwrap();
        Proof {
            locked,
            fresh: (20, 22, 9),
            decision: RefCell::new(None),
        }
    }
    fn stage(&self, p: &Proof) -> Staged {
        stage(
            &self.authority,
            self.identity.clone(),
            &self.rows,
            self.intent.clone(),
            p,
        )
        .unwrap()
    }
    fn validated(&self, s: &Staged, p: &Proof) -> Validated {
        validate(
            s,
            &self.authority,
            &self.rows,
            DecisionTime {
                lower: 20,
                upper: 22,
                fence: 9,
            },
            decision(),
            p,
        )
        .unwrap()
    }
    fn apply_rows(&mut self, effects: &[Effect]) {
        for p in &mut self.rows {
            p.image = edit(&p.image, |s| {
                for e in effects {
                    if let Some(row) = s.view.rows.get_mut(&e.key) {
                        assert_eq!(*row, e.before);
                        *row = e.after.clone();
                    }
                }
            });
            p.applied += 1;
        }
    }
    fn approval(&mut self) {
        self.intent.approval = Some(ApprovalId(0));
        let (w, _) = assemble(
            &self.authority,
            self.identity.clone(),
            &self.rows,
            self.intent.clone(),
        )
        .unwrap();
        let (approved, id) = w
            .with_admin(|a| {
                let preview = a.executor(TaskId(1), Principal(2))?.preview_current(
                    &self.intent.request,
                    10,
                    10,
                )?;
                a.approve(Principal(3), preview, Time::Certified(10))
            })
            .unwrap();
        assert_eq!(id, ApprovalId(0));
        self.authority = edit(&approved, |s| s.view.rows.clear());
    }
}
fn decision() -> DecisionId {
    DecisionId {
        home: 55,
        epoch: 2,
        transaction: 999,
    }
}
fn decide(p: &Proof, v: &Validated, o: Outcome) {
    *p.decision.borrow_mut() = Some((decision(), v.digest().unwrap(), o));
}
fn error<T>(r: Result<T, CoreError>, expected: CoreError) {
    match r {
        Err(e) => assert_eq!(e, expected),
        Ok(_) => panic!("unexpected success"),
    }
}

#[test]
fn stage_has_no_input_effects_and_matches_real_executor_exactly() {
    for predicate in [false, true] {
        let mut f = Fixture::new();
        if predicate {
            f.intent.request.selection = Selection::Predicate {
                tenant: TenantId(42),
                status: Status::Failed,
                from: 100,
                until: 110,
            };
        }
        let before = f.authority.bytes().to_vec();
        let rows = f.rows[0].image.bytes().to_vec();
        let p = f.proof();
        let s = f.stage(&p);
        assert_eq!(f.authority.bytes(), before);
        assert_eq!(f.rows[0].image.bytes(), rows);
        let (workspace, _) =
            assemble(&f.authority, f.identity.clone(), &f.rows, f.intent.clone()).unwrap();
        let (real, receipt) = workspace
            .with_admin(|a| {
                a.executor(TaskId(1), Principal(2))?
                    .execute_current(f.intent.request.clone(), 10, 12, None)?
                    .map_err(CoreError::Policy)
            })
            .unwrap();
        assert_eq!(s.0.receipt, receipt);
        let mut expected = image_state(&real).unwrap();
        expected.view.rows.clear();
        assert_eq!(s.0.after, image(&expected));
        assert_eq!(
            s.0.delta.task_after.transitions,
            receipt.charged_transitions
        );
        assert_eq!(s.0.delta.accounting_after.unwrap().used.disclosed_rows, 0);
        assert_eq!(s.0.delta.accounting_after.unwrap().used.encoded_bytes, 0);
        assert!(s.0.delta.accounting_after.unwrap().used.read_rows > 0);
    }
}

#[test]
fn staged_result_matches_existing_replica_task_policy_receipt() {
    let f = Fixture::new();
    let p = f.proof();
    let s = f.stage(&p);
    let (workspace, _) =
        assemble(&f.authority, f.identity.clone(), &f.rows, f.intent.clone()).unwrap();
    let (real, reply) = workspace
        .evaluate(&Command::Task {
            task: TaskId(1),
            principal: Principal(2),
            lower: 10,
            upper: 12,
            action: TaskAction::Execute(f.intent.request.clone(), None),
        })
        .unwrap();
    assert_eq!(reply.code, 0);
    let receipt: Receipt = serde_json::from_value(reply.data).unwrap();
    assert_eq!(s.0.receipt, receipt);
    assert_eq!(
        real.accounting(TaskId(1))
            .unwrap()
            .unwrap()
            .used
            .disclosed_rows,
        1
    );
    assert_eq!(s.0.delta.accounting_after.unwrap().used.disclosed_rows, 0);
}

#[test]
fn unique_authority_location_generation_domain_task_principal_are_bound() {
    let f = Fixture::new();
    let p = f.proof();
    for axis in 0..5 {
        let mut a = f.identity.clone();
        let mut i = f.intent.clone();
        match axis {
            0 => a.location += 1,
            1 => a.generation += 1,
            2 => a.domain += 1,
            3 => a.task = TaskId(2),
            _ => i.principal = Principal(9),
        }
        assert!(stage(&f.authority, a, &f.rows, i, &p).is_err());
    }
}

#[test]
fn complete_coverage_rejects_gaps_overlap_missing_empty_tail_and_wrong_rows() {
    let f = Fixture::new();
    let p = f.proof();
    for axis in 0..6 {
        let mut rows = f.rows.clone();
        match axis {
            0 => rows.clear(),
            1 => rows[0].start = key(0),
            2 => rows[0].end = Some(key(9)),
            3 => rows.push(rows[0].clone()),
            4 => rows[0].start = key(2),
            _ => rows[0].image = Image::new(999).unwrap(),
        }
        assert!(
            stage(
                &f.authority,
                f.identity.clone(),
                &rows,
                f.intent.clone(),
                &p
            )
            .is_err()
        );
    }
    let mut two = f.rows.clone();
    two[0].end = Some(key(2));
    two[0].image = f.rows[0]
        .image
        .row_partition(two[0].start, two[0].end)
        .unwrap();
    two.push(Partition {
        owner: 23,
        epoch: 8,
        applied: 20,
        start: key(2),
        end: None,
        image: f.rows[0].image.row_partition(key(2), None).unwrap(),
    });
    let (_, binding) = assemble(&f.authority, f.identity.clone(), &two, f.intent.clone()).unwrap();
    let p = Proof {
        locked: binding,
        fresh: (20, 22, 9),
        decision: RefCell::new(None),
    };
    assert!(stage(&f.authority, f.identity, &two, f.intent, &p).is_ok());
}

#[test]
fn row_images_cannot_carry_copied_authority_catalog_or_metadata() {
    let f = Fixture::new();
    let p = f.proof();
    for axis in 0..4 {
        let mut rows = f.rows.clone();
        rows[0].image = edit(&rows[0].image, |s| match axis {
            0 => s.view.tasks = image_state(&f.authority).unwrap().view.tasks,
            1 => s.view.schema = 1,
            2 => s.last_time = 1,
            _ => s.origin = Some(99),
        });
        assert!(
            stage(
                &f.authority,
                f.identity.clone(),
                &rows,
                f.intent.clone(),
                &p
            )
            .is_err()
        );
    }
}

#[test]
fn stale_before_rows_phantoms_applied_positions_and_policy_schema_reject() {
    let mut f = Fixture::new();
    f.intent.request.selection = Selection::Predicate {
        tenant: TenantId(42),
        status: Status::Failed,
        from: 100,
        until: 110,
    };
    let p = f.proof();
    let s = f.stage(&p);
    for axis in 0..6 {
        let mut rows = f.rows.clone();
        let mut authority = f.authority.clone();
        match axis {
            0 => {
                rows[0].image = edit(&rows[0].image, |s| {
                    s.view.rows.get_mut(&key(1)).unwrap().version += 1
                })
            }
            1 => {
                rows[0].image = edit(&rows[0].image, |s| {
                    let row = s.view.rows[&key(1)].clone();
                    s.view.rows.insert(key(3), row);
                })
            }
            2 => rows[0].applied += 1,
            3 => authority = edit(&authority, |s| s.view.schema += 1),
            4 => {
                authority = edit(&authority, |s| {
                    s.view
                        .tasks
                        .get_mut(&TaskId(1))
                        .unwrap()
                        .grant
                        .policy
                        .expires += 1
                })
            }
            _ => rows[0].epoch += 1,
        }
        error(
            validate(
                &s,
                &authority,
                &rows,
                DecisionTime {
                    lower: 20,
                    upper: 22,
                    fence: 9,
                },
                decision(),
                &p,
            ),
            CoreError::Fenced,
        );
    }
}

#[test]
fn expired_uncertain_regressing_and_stale_fence_fail_without_mutation() {
    let f = Fixture::new();
    let mut p = f.proof();
    let s = f.stage(&p);
    for (lo, hi, fe, expected) in [
        (1000, 1001, 9, CoreError::Policy(Error::Expired)),
        (23, 22, 9, CoreError::Policy(Error::UncertainTime)),
        (9, 12, 9, CoreError::Policy(Error::TimeRegression)),
        (20, 22, 10, CoreError::Fenced),
    ] {
        if fe == 9 {
            p.fresh = (lo, hi, fe);
        }
        error(
            validate(
                &s,
                &f.authority,
                &f.rows,
                DecisionTime {
                    lower: lo,
                    upper: hi,
                    fence: fe,
                },
                decision(),
                &p,
            ),
            expected,
        );
    }
    assert!(f.authority.snapshot().unwrap().operations.is_empty());
}

#[test]
fn revoke_or_approval_revoke_before_decision_invalidates_stage() {
    let mut f = Fixture::new();
    f.approval();
    let p = f.proof();
    let s = f.stage(&p);
    for approval in [false, true] {
        let revoked = edit(&f.authority, |s| {
            if approval {
                s.view.approvals.get_mut(&ApprovalId(0)).unwrap().revoked = true
            } else {
                s.view.tasks.get_mut(&TaskId(1)).unwrap().revoked = true
            }
        });
        error(
            validate(
                &s,
                &revoked,
                &f.rows,
                DecisionTime {
                    lower: 20,
                    upper: 22,
                    fence: 9,
                },
                decision(),
                &p,
            ),
            CoreError::Fenced,
        );
    }
}

#[test]
fn committed_finalize_does_not_recheck_later_wall_time_or_revoke_fence() {
    let f = Fixture::new();
    let mut p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    // Locks prevent a revoke changing this image until decided intents finalize.
    // Current eligibility may expire/change after decision; finalization uses redo.
    p.fresh = (2000, 2002, 999);
    let result = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert_eq!(result.receipt.decided_at, 22);
    assert_eq!(
        result.authority.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        1
    );
}

#[test]
fn abort_and_pending_cannot_publish_and_dropping_stage_is_inert() {
    let f = Fixture::new();
    let before = f.authority.clone();
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Pending);
    error(publish(&v, &f.authority, &p), CoreError::Fenced);
    decide(&p, &v, Outcome::Aborted);
    assert!(publish(&v, &f.authority, &p).unwrap().is_none());
    drop(v);
    drop(s);
    assert_eq!(f.authority, before);
}

#[test]
fn same_operation_retry_returns_same_receipt_without_any_new_mutation() {
    let mut f = Fixture::new();
    f.approval();
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    let first = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert_eq!(publish(&v, &first.authority, &p).unwrap().unwrap(), first);
    f.authority = first.authority.clone();
    f.apply_rows(&first.row_effects);
    let p = f.proof();
    let retry = f.stage(&p);
    let v = f.validated(&retry, &p);
    decide(&p, &v, Outcome::Committed);
    let replay = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert_eq!(replay.authority, first.authority);
    assert_eq!(replay.receipt, first.receipt);
    assert!(replay.row_effects.is_empty());
    assert_eq!(replay.delta.task_before, replay.delta.task_after);
    assert_eq!(
        replay.delta.accounting_before,
        replay.delta.accounting_after
    );
    assert_eq!(replay.delta.clock_before, replay.delta.clock_after);
    assert_eq!(
        replay.delta.generation_before,
        replay.delta.generation_after
    );
    f.intent.request.after = Status::Failed;
    let p = f.proof();
    error(
        stage(&f.authority, f.identity, &f.rows, f.intent, &p),
        CoreError::Policy(Error::IdConflict),
    );
}

#[test]
fn approval_consumed_exactly_once_and_other_operation_cannot_reuse_it() {
    let mut f = Fixture::new();
    f.approval();
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    let result = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert!(!result.delta.approval_before.unwrap().consumed);
    assert!(result.delta.approval_after.unwrap().consumed);
    f.authority = result.authority;
    f.apply_rows(&result.row_effects);
    f.intent.request.operation = OperationId(11);
    f.intent.lower = 20;
    f.intent.upper = 22;
    f.intent.request.selection = Selection::Keys(vec![key(2)]);
    let p = f.proof();
    error(
        stage(&f.authority, f.identity, &f.rows, f.intent, &p),
        CoreError::Policy(Error::InvalidApproval),
    );
}

#[test]
fn exact_approval_complete_set_and_before_image_are_real_policy_checks() {
    for phantom in [false, true] {
        let mut f = Fixture::new();
        f.intent.request.selection = Selection::Predicate {
            tenant: TenantId(42),
            status: Status::Failed,
            from: 100,
            until: 110,
        };
        f.authority = edit(&f.authority, |s| {
            let t = s.view.tasks.get_mut(&TaskId(1)).unwrap();
            t.grant.policy.distinct_limit = 3;
            t.grant.policy.transition_limit = 3;
        });
        f.approval();
        f.rows[0].image = edit(&f.rows[0].image, |s| {
            if phantom {
                let row = s.view.rows[&key(1)].clone();
                s.view.rows.insert(key(3), row);
            } else {
                s.view.rows.get_mut(&key(1)).unwrap().version += 1
            }
        });
        let p = f.proof();
        error(
            stage(&f.authority, f.identity, &f.rows, f.intent, &p),
            CoreError::Policy(Error::StalePreview),
        );
    }
}

#[test]
fn deterministic_last_slot_schedules_allow_one_authoritative_consumption() {
    // Two workers stage against the same authority. Enumerate both first winners,
    // restart before finalize, duplicate finalize and stale loser revalidation.
    for winner in [1, 2] {
        let mut f = Fixture::new();
        f.authority = edit(&f.authority, |s| {
            s.view
                .tasks
                .get_mut(&TaskId(1))
                .unwrap()
                .grant
                .policy
                .transition_limit = 1
        });
        let mut stages = Vec::new();
        for n in 1..=2 {
            f.intent.request.operation = OperationId(n);
            f.intent.request.selection = Selection::Keys(vec![key(n)]);
            let p = f.proof();
            stages.push((f.stage(&p), p));
        }
        let (s, p) = &stages[(winner - 1) as usize];
        let v = f.validated(s, p);
        let bytes = v.to_bytes().unwrap();
        let recovered = Validated::verify_bytes(&bytes, &f.authority, &f.rows).unwrap();
        decide(p, &recovered, Outcome::Committed);
        let r = publish(&recovered, &f.authority, p).unwrap().unwrap();
        assert_eq!(publish(&recovered, &r.authority, p).unwrap().unwrap(), r);
        let (loser, lp) = &stages[(2 - winner) as usize];
        error(
            validate(
                loser,
                &r.authority,
                &f.rows,
                DecisionTime {
                    lower: 20,
                    upper: 22,
                    fence: 9,
                },
                decision(),
                lp,
            ),
            CoreError::Fenced,
        );
        f.authority = r.authority;
        f.apply_rows(&r.row_effects);
        f.intent.lower = 20;
        f.intent.upper = 22;
        let other = 3 - winner;
        f.intent.request.operation = OperationId(other);
        f.intent.request.selection = Selection::Keys(vec![key(other)]);
        let p = f.proof();
        error(
            stage(
                &f.authority,
                f.identity.clone(),
                &f.rows,
                f.intent.clone(),
                &p,
            ),
            CoreError::Policy(Error::Budget),
        );
        let snapshot = f.authority.snapshot().unwrap();
        assert_eq!(snapshot.tasks[&TaskId(1)].transitions, 1);
        assert_eq!(snapshot.operations.len(), 1);
        assert_eq!(snapshot.tasks[&TaskId(1)].touched.len(), 1);
    }
}

#[test]
fn serialization_is_canonical_recomputed_and_tamper_rejected() {
    let f = Fixture::new();
    let p = f.proof();
    let s = f.stage(&p);
    let bytes = s.to_bytes().unwrap();
    assert_eq!(
        Staged::verify_bytes(&bytes, &f.authority, &f.rows, &p)
            .unwrap()
            .digest()
            .unwrap(),
        s.digest().unwrap()
    );
    for axis in 0..7 {
        let mut body = s.0.clone();
        match axis {
            0 => body.receipt.charged_transitions += 1,
            1 => body.row_effects.clear(),
            2 => body.delta.task_after.transitions = 0,
            3 => body.after = f.authority.clone(),
            4 => body.binding.schema += 1,
            5 => body.binding.ranges[0].image[0] ^= 1,
            _ => body.binding.intent.principal = Principal(9),
        }
        assert!(
            Staged::verify_bytes(&canonical(&body).unwrap(), &f.authority, &f.rows, &p).is_err()
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(Staged::verify_bytes(&trailing, &f.authority, &f.rows, &p).is_err());
    assert!(Staged::verify_bytes(&bytes[..bytes.len() - 1], &f.authority, &f.rows, &p).is_err());
    let v = f.validated(&s, &p);
    let bytes = v.to_bytes().unwrap();
    let restored = Validated::verify_bytes(&bytes, &f.authority, &f.rows).unwrap();
    assert_eq!(restored.digest().unwrap(), v.digest().unwrap());
    decide(&p, &v, Outcome::Committed);
    assert!(publish(&restored, &f.authority, &p).unwrap().is_some());
    let mut tampered = v.0.clone();
    tampered.decision.transaction += 1;
    let changed =
        Validated::verify_bytes(&canonical(&tampered).unwrap(), &f.authority, &f.rows).unwrap();
    error(publish(&changed, &f.authority, &p), CoreError::Fenced);
    let mut tampered = v.0.clone();
    tampered.body.receipt.lifetime_transitions += 1;
    assert!(
        Validated::verify_bytes(&canonical(&tampered).unwrap(), &f.authority, &f.rows).is_err()
    );
}

#[test]
fn unrelated_authorities_preserved_and_intervening_updates_never_overwritten() {
    let mut f = Fixture::new();
    f.authority = edit(&f.authority, |s| {
        let mut t = s.view.tasks[&TaskId(1)].clone();
        t.grant.task = TaskId(2);
        t.transitions = 1;
        t.touched.insert(key(2));
        s.view.tasks.insert(TaskId(2), t);
    });
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    let r = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert_eq!(
        f.authority.snapshot().unwrap().tasks[&TaskId(2)],
        r.authority.snapshot().unwrap().tasks[&TaskId(2)]
    );
    let changed = edit(&f.authority, |s| {
        s.view.tasks.get_mut(&TaskId(2)).unwrap().revoked = true
    });
    error(publish(&v, &changed, &p), CoreError::Fenced);
    assert!(changed.snapshot().unwrap().tasks[&TaskId(2)].revoked);
}

#[test]
fn generation_overflow_reservation_is_atomic_and_sufficient_capacity_finalizes() {
    for remaining in [0, 1, 2] {
        let mut f = Fixture::new();
        f.authority = edit(&f.authority, |s| s.sql.generation = u64::MAX - remaining);
        let before = f.authority.clone();
        let p = f.proof();
        let result = stage(
            &f.authority,
            f.identity.clone(),
            &f.rows,
            f.intent.clone(),
            &p,
        );
        if remaining < 2 {
            error(result, CoreError::Sql(relational::SqlError::Overflow));
        } else {
            let s = result.unwrap();
            let v = f.validated(&s, &p);
            decide(&p, &v, Outcome::Committed);
            let r = publish(&v, &f.authority, &p).unwrap().unwrap();
            assert_eq!(r.delta.generation_after, u64::MAX);
        }
        assert_eq!(f.authority, before);
    }
}

#[test]
fn row_version_and_resource_counter_overflow_fail_without_input_effects() {
    for resource in [false, true] {
        let mut f = Fixture::new();
        if resource {
            f.authority = edit(&f.authority, |s| {
                let a = s.accounting.get_mut(&TaskId(1)).unwrap();
                a.limits.read_rows = u64::MAX;
                a.used.read_rows = u64::MAX;
            });
        } else {
            f.rows[0].image = edit(&f.rows[0].image, |s| {
                s.view.rows.get_mut(&key(1)).unwrap().version = u64::MAX
            });
        }
        let before = f.authority.clone();
        let p = f.proof();
        error(
            stage(&f.authority, f.identity, &f.rows, f.intent, &p),
            CoreError::Policy(if resource {
                Error::Budget
            } else {
                Error::Overflow
            }),
        );
        assert_eq!(f.authority, before);
    }
}

#[test]
fn bounded_rows_participants_and_wire_reject_before_publication() {
    let mut f = Fixture::new();
    f.rows[0].image = edit(&f.rows[0].image, |s| {
        let row = s.view.rows[&key(1)].clone();
        for n in 3..=MAX_ROWS as u64 + 1 {
            s.view.rows.insert(key(n), row.clone());
        }
    });
    assert!(assemble(&f.authority, f.identity.clone(), &f.rows, f.intent.clone()).is_err());
    f.rows = vec![f.rows[0].clone(); MAX_PARTITIONS + 1];
    error(
        assemble(&f.authority, f.identity.clone(), &f.rows, f.intent.clone()),
        CoreError::Limit,
    );
    let bytes = vec![0u8; MAX_BYTES as usize + 1];
    assert!(Validated::verify_bytes(&bytes, &f.authority, &f.rows).is_err());
}

#[test]
fn unsupported_column_duplicate_keys_scope_and_unprovisioned_resources_use_real_errors() {
    for axis in 0..4 {
        let mut f = Fixture::new();
        match axis {
            0 => f.intent.request.column = Column::AttemptCount,
            1 => f.intent.request.selection = Selection::Keys(vec![key(1), key(1)]),
            2 => {
                f.intent.request.selection = Selection::Keys(vec![Key {
                    tenant: TenantId(43),
                    row: RowId(1),
                }])
            }
            _ => f.authority = edit(&f.authority, |s| s.accounting.clear()),
        }
        let p = f.proof();
        error(
            stage(&f.authority, f.identity, &f.rows, f.intent, &p),
            CoreError::Policy(match axis {
                0 | 2 => Error::Scope,
                1 => Error::DuplicateKey,
                _ => Error::Budget,
            }),
        );
    }
}

#[test]
fn canonical_key_order_has_same_stage_digest_but_duplicate_keys_never_do() {
    let mut f = Fixture::new();
    f.intent.request.selection = Selection::Keys(vec![key(2), key(1)]);
    let p = f.proof();
    let first = f.stage(&p);
    f.intent.request.selection = Selection::Keys(vec![key(1), key(2)]);
    let second = f.stage(&p);
    assert_eq!(first.digest().unwrap(), second.digest().unwrap());
    f.intent.request.selection = Selection::Keys(vec![key(1), key(1)]);
    let p = f.proof();
    assert!(stage(&f.authority, f.identity, &f.rows, f.intent, &p).is_err());
}

#[test]
fn already_consumed_or_wrong_approver_approval_cannot_stage() {
    for axis in 0..3 {
        let mut f = Fixture::new();
        f.approval();
        f.authority = edit(&f.authority, |s| {
            let approval = s.view.approvals.get_mut(&ApprovalId(0)).unwrap();
            match axis {
                0 => approval.consumed = true,
                1 => approval.revoked = true,
                _ => approval.approver = Principal(88),
            }
        });
        let p = f.proof();
        error(
            stage(&f.authority, f.identity, &f.rows, f.intent, &p),
            CoreError::Policy(Error::InvalidApproval),
        );
    }
}

#[test]
fn changed_validated_digest_and_immutable_decision_identity_cannot_publish() {
    let f = Fixture::new();
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    for axis in 0..4 {
        let mut value = v.0.clone();
        match axis {
            0 => value.decision.home += 1,
            1 => value.decision.epoch += 1,
            2 => value.decision.transaction += 1,
            _ => value.body.binding.intent.fence += 1,
        }
        // Even semantically re-evaluable bytes cannot claim another decision.
        let altered =
            Validated::verify_bytes(&canonical(&value).unwrap(), &f.authority, &f.rows).unwrap();
        error(publish(&altered, &f.authority, &p), CoreError::Fenced);
    }
}

#[test]
fn unvalidated_deserialized_image_and_relational_catalog_are_rejected() {
    let f = Fixture::new();
    let p = f.proof();
    let invalid: Image = serde_json::from_str("[1,2,3]").unwrap();
    assert!(stage(&invalid, f.identity.clone(), &f.rows, f.intent.clone(), &p).is_err());
    let (catalog,reply)=f.authority.evaluate(&Command::Admin(serde_json::to_vec(&serde_json::json!({
        "command":"sql","sql":"CREATE TABLE app (id BIGINT PRIMARY KEY)","params":[],"expected_version":null
    })).unwrap())).unwrap();
    assert_eq!(reply.code, 0);
    error(
        stage(&catalog, f.identity, &f.rows, f.intent, &p),
        CoreError::Unsupported,
    );
}

#[test]
fn encoded_redo_capacity_is_reserved_before_decision_without_partial_authority() {
    let mut f = Fixture::new();
    // A canonical near-capacity retained operation is valid input. Its payload
    // exercises storage capacity, not any claim about remotely admitted SQL.
    f.authority = edit(&f.authority, |s| {
        s.view.operations.insert(
            (TaskId(1), OperationId(99)),
            BoundDecision {
                payload: vec![7; MAX_BYTES as usize - 1024],
                decision: Err(Error::Scope),
            },
        );
    });
    let before = f.authority.clone();
    let p = f.proof();
    assert!(stage(&f.authority, f.identity, &f.rows, f.intent, &p).is_err());
    assert_eq!(f.authority, before);
    assert_eq!(f.authority.snapshot().unwrap().operations.len(), 1);
    assert_eq!(
        f.authority.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        0
    );
}

#[test]
fn transition_counter_overflow_is_real_policy_error_and_atomic() {
    let mut f = Fixture::new();
    f.authority = edit(&f.authority, |s| {
        let t = s.view.tasks.get_mut(&TaskId(1)).unwrap();
        t.grant.policy.transition_limit = u64::MAX;
        t.transitions = u64::MAX;
    });
    let before = f.authority.clone();
    let p = f.proof();
    error(
        stage(&f.authority, f.identity, &f.rows, f.intent, &p),
        CoreError::Policy(Error::Overflow),
    );
    assert_eq!(f.authority, before);
}

#[test]
fn maximum_declared_rows_and_partitions_stage_exactly_without_capacity_cliff() {
    let mut f = Fixture::new();
    f.authority = edit(&f.authority, |s| {
        let t = s.view.tasks.get_mut(&TaskId(1)).unwrap();
        t.grant.policy.distinct_limit = MAX_ROWS as u64;
        t.grant.policy.transition_limit = MAX_ROWS as u64;
        t.grant.policy.approval_above = MAX_ROWS as u64;
        s.accounting.get_mut(&TaskId(1)).unwrap().limits.read_rows = 10_000;
    });
    let data = edit(&f.rows[0].image, |s| {
        let row = s.view.rows[&key(1)].clone();
        for n in 3..=MAX_ROWS as u64 {
            s.view.rows.insert(key(n), row.clone());
        }
    });
    f.rows = (0..MAX_PARTITIONS)
        .map(|n| {
            let start = if n == 0 {
                Key {
                    tenant: TenantId(0),
                    row: RowId(0),
                }
            } else {
                key(n as u64 * 16 + 1)
            };
            let end = (n + 1 < MAX_PARTITIONS).then(|| key((n as u64 + 1) * 16 + 1));
            Partition {
                owner: n as u64 + 100,
                epoch: 1,
                applied: 1,
                start,
                end,
                image: data.row_partition(start, end).unwrap(),
            }
        })
        .collect();
    f.intent.request.selection = Selection::Predicate {
        tenant: TenantId(42),
        status: Status::Failed,
        from: 100,
        until: 110,
    };
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    let r = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert_eq!(r.row_effects.len(), MAX_ROWS);
    assert_eq!(r.delta.task_after.transitions, MAX_ROWS as u64);
    assert_eq!(r.delta.task_after.touched.len(), MAX_ROWS);
    assert_eq!(
        r.delta.accounting_after.unwrap().used.read_rows,
        MAX_ROWS as u64 * 3 + 1
    );
}

#[test]
fn no_change_request_consumes_read_work_and_receipt_but_no_mutation_budget() {
    let mut f = Fixture::new();
    f.rows[0].image = edit(&f.rows[0].image, |s| {
        s.view.rows.get_mut(&key(1)).unwrap().status = Status::Queued
    });
    let p = f.proof();
    let s = f.stage(&p);
    let v = f.validated(&s, &p);
    decide(&p, &v, Outcome::Committed);
    let r = publish(&v, &f.authority, &p).unwrap().unwrap();
    assert!(r.row_effects.is_empty());
    assert!(r.receipt.effects.is_empty());
    assert_eq!(r.delta.task_before, r.delta.task_after);
    assert_eq!(r.delta.accounting_after.unwrap().used.read_rows, 1);
    assert!(r.delta.operation_before.is_none());
    assert_eq!(r.delta.clock_after, 22);
    assert_eq!(r.delta.accounting_after.unwrap().used.disclosed_rows, 0);
}
