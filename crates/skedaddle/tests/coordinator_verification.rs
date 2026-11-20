mod support;
use skedaddle::coordinator::{
    Transition,
    epoch::Coordinator,
    journal::{Entry, Journal},
    local::Local,
    verification::*,
};
use std::time::Duration;
use support::{fs_store, scratch};

fn definition() -> skedaddle::MigrationDefinition {
    let text =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
            .unwrap();
    skedaddle::MigrationDefinition::from_json(
        text.split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap()
}
fn section() -> Section {
    Section {
        completed: true,
        checked: 1,
        findings: vec![],
        evidence: "inline complete collector transcript".into(),
    }
}
fn report(epoch: u64, generation: u64, run: &str) -> Report {
    let scope = TableScope {
        schema: "public".into(),
        table: "accounts".into(),
        catalog_digest: "a".repeat(64),
        plan_digest: "b".repeat(64),
        key: vec![KeyColumn {
            name: "id".into(),
            kind: KeyType::Int8,
            descending: false,
        }],
    };
    let counts = Counts {
        total: 2,
        verified: 2,
        ..Counts::default()
    };
    let lineage = Lineage {
        system: "system-1".into(),
        database: "db-1".into(),
        timeline: 1,
    };
    let interval = Interval {
        armed: 1,
        source_read: 2,
        destination_read: 3,
        complete_through_lsn: 100,
        evidence: "inline armed complete change transcript".into(),
    };
    let lower = Boundary::NegativeInfinity;
    let upper = Boundary::PositiveInfinity;
    let range = RangeResult {
        id: range_identity(&scope, &lower, &upper).unwrap(),
        lower,
        upper,
        counts: counts.clone(),
        exclusions: vec![],
        interval: interval.clone(),
        applied_lsn: 100,
        applied_lineage: lineage.clone(),
        applied_evidence: "inline committed source LSN proof".into(),
        source_digest: "c".repeat(64),
        destination_digest: "c".repeat(64),
        retry_attempts: 0,
        exhausted_busy: 0,
        outstanding_keys: vec![],
    };
    let mut report = Report {
        version: VERSION,
        coverage_version: VERSION,
        migration: "migration".into(),
        definition_digest: definition().canonical_digest().unwrap(),
        run: run.into(),
        epoch,
        generation,
        lineage,
        completed: true,
        interval,
        tables: vec![TableResult {
            scope,
            counts,
            ranges: vec![range],
        }],
        retry: RetryPolicy {
            version: VERSION,
            policy: "bounded-complete-change-v1".into(),
            budget: 2,
            outstanding_ranges: vec![],
        },
        dead_letters: DeadLetters {
            collection: "dead-letters-1".into(),
            completed: true,
            count: 0,
            identities: vec![],
            evidence: "inline full dead-letter collection scan".into(),
        },
        sections: Sections {
            schema: section(),
            sequences: section(),
            constraints: section(),
            indexes: section(),
            destination_foreign_keys: section(),
        },
        coverage_digest: String::new(),
    };
    seal(&mut report);
    report
}
fn seal(report: &mut Report) {
    report.coverage_digest = report.computed_coverage_digest().unwrap();
}
fn facts(report: &Report) -> CurrentFacts {
    CurrentFacts {
        migration: report.migration.clone(),
        definition_digest: report.definition_digest.clone(),
        epoch: report.epoch,
        generation: report.generation,
        lineage: report.lineage.clone(),
        scope: report.tables.iter().map(|t| t.scope.clone()).collect(),
        capture_lsn: 100,
    }
}
fn journal(root: &std::path::Path) -> Journal {
    Journal::new(fs_store(root), "p2", "migration", Duration::from_secs(3)).unwrap()
}
async fn owner(journal: Journal) -> Coordinator {
    let mut c = Coordinator::observer(
        journal,
        Local::create(scratch("verification-local").join("db")).unwrap(),
        "owner",
    )
    .unwrap();
    c.acquire().await.unwrap();
    c.commit(Transition::Define {
        definition: Box::new(definition()),
    })
    .await
    .unwrap();
    c
}
#[tokio::test]
async fn completion_is_durable_checkpointed_rebuildable_and_idempotent() {
    let root = scratch("verification-objects");
    let j = journal(&root);
    let mut c = owner(j.clone()).await;
    let report = report(1, 0, "run-a");
    c.complete_verification(&report).await.unwrap();
    let seq = c.state().applied_seq;
    c.complete_verification(&report).await.unwrap();
    assert_eq!(c.state().applied_seq, seq);
    assert!(current_verification(c.state(), &facts(&report)).verification);
    c.checkpoint().await.unwrap();
    drop(c);
    for cp in [false, true] {
        let state = j.load(cp).await.unwrap();
        assert!(current_verification(&state, &facts(&report)).verification);
        let local = Local::create(scratch("verification-restart").join("db")).unwrap();
        let mut observer = Coordinator::observer(j.clone(), local, "restart").unwrap();
        observer.refresh().await.unwrap();
        assert_eq!(observer.state(), &state);
    }
    let dest = journal(&scratch("verification-copy"));
    let copied = j.copy_entries_for_rebuild(&dest).await.unwrap();
    assert!(current_verification(&copied, &facts(&report)).verification);
    assert_eq!(dest.load(false).await.unwrap(), copied);
}
#[tokio::test]
async fn stale_or_absent_facts_are_unknown_including_empty_scope() {
    let j = journal(&scratch("verification-stale"));
    let mut c = owner(j).await;
    let r = report(1, 0, "run-a");
    c.complete_verification(&r).await.unwrap();
    let mut variants = vec![];
    let f = facts(&r);
    let mut x = f.clone();
    x.migration.push('x');
    variants.push(x);
    let mut x = f.clone();
    x.definition_digest = "d".repeat(64);
    variants.push(x);
    let mut x = f.clone();
    x.epoch += 1;
    variants.push(x);
    let mut x = f.clone();
    x.generation += 1;
    variants.push(x);
    let mut x = f.clone();
    x.lineage.system.push('x');
    variants.push(x);
    let mut x = f.clone();
    x.lineage.database.push('x');
    variants.push(x);
    let mut x = f.clone();
    x.lineage.timeline += 1;
    variants.push(x);
    let mut x = f.clone();
    x.scope.clear();
    variants.push(x);
    let mut x = f.clone();
    x.scope[0].catalog_digest = "d".repeat(64);
    variants.push(x);
    let mut x = f.clone();
    x.scope[0].plan_digest = "d".repeat(64);
    variants.push(x);
    let mut x = f.clone();
    x.scope[0].key[0].descending = true;
    variants.push(x);
    let mut x = f.clone();
    x.capture_lsn += 1;
    variants.push(x);
    let mut x = f.clone();
    x.capture_lsn = 0;
    variants.push(x);
    for x in variants {
        assert_eq!(
            current_verification(c.state(), &x),
            VerificationFacts::default(),
            "{x:?}"
        );
    }
    assert_eq!(
        current_verification(&Default::default(), &f),
        VerificationFacts::default()
    );
    c.acquire().await.unwrap();
    assert_eq!(
        current_verification(c.state(), &f),
        VerificationFacts::default()
    );
}
#[tokio::test]
async fn invalidation_prevents_old_run_resurrection_and_orphan_acceptance() {
    let j = journal(&scratch("verification-invalidation"));
    let mut c = owner(j.clone()).await;
    let a = report(1, 0, "run-a");
    c.complete_verification(&a).await.unwrap();
    for reason in [
        InvalidationReason::Repair,
        InvalidationReason::DefinitionKeyPlanChange,
        InvalidationReason::LineageDiscontinuity,
        InvalidationReason::EvidenceLoss,
    ] {
        let orphan = report(
            1,
            c.state().verification.generation,
            &format!("orphan-{}", c.state().verification.generation),
        );
        let reference = j.publish_verification(&orphan).await.unwrap();
        c.commit(Transition::VerificationInvalidated {
            invalidation: Invalidation {
                version: VERSION,
                reason,
                scope: InvalidationScope::All,
            },
        })
        .await
        .unwrap();
        assert_eq!(
            current_verification(c.state(), &facts(&a)),
            VerificationFacts::default()
        );
        assert!(
            c.commit(Transition::VerificationCompleted {
                report: Box::new(reference)
            })
            .await
            .is_err()
        );
    }
    let b = report(1, c.state().verification.generation, "run-b");
    c.complete_verification(&b).await.unwrap();
    assert!(current_verification(c.state(), &facts(&b)).verification);
    assert!(c.complete_verification(&a).await.is_err());
    assert!(current_verification(c.state(), &facts(&b)).verification);
    let d = report(1, c.state().verification.generation, "run-d");
    c.complete_verification(&d).await.unwrap();
    assert!(c.complete_verification(&b).await.is_err());
    let before = c.state().applied_seq;
    let invalid = Invalidation {
        version: 999,
        reason: InvalidationReason::Repair,
        scope: InvalidationScope::All,
    };
    let entry = Entry::new(
        c.state(),
        Transition::VerificationInvalidated {
            invalidation: invalid,
        },
        "owner",
    )
    .unwrap();
    assert!(j.put(&entry).await.is_err());
    assert_eq!(j.load(false).await.unwrap().applied_seq, before);
}
#[tokio::test]
async fn missing_or_corrupt_report_rejects_publication_replay_and_checkpoint_recovery() {
    use object_store::ObjectStoreExt;
    for corrupt in [false, true] {
        let root = scratch("verification-loss");
        let store = fs_store(&root);
        let j = journal(&root);
        let mut c = owner(j.clone()).await;
        let r = report(1, 0, "run-a");
        c.complete_verification(&r).await.unwrap();
        c.checkpoint().await.unwrap();
        let key = j.verification_key(&r.run);
        if corrupt {
            store
                .put(&key, object_store::PutPayload::from_static(b"{}"))
                .await
                .unwrap();
        } else {
            store.delete(&key).await.unwrap();
        }
        assert!(j.load(false).await.is_err());
        assert!(j.load(true).await.is_err());
        assert!(c.refresh().await.is_err());
    }
    let root = scratch("verification-missing-before");
    let store = fs_store(&root);
    let j = journal(&root);
    let c = owner(j.clone()).await;
    let r = report(1, 0, "run-a");
    let reference = j.publish_verification(&r).await.unwrap();
    store.delete(&j.verification_key(&r.run)).await.unwrap();
    let entry = Entry::new(
        c.state(),
        Transition::VerificationCompleted {
            report: Box::new(reference),
        },
        "owner",
    )
    .unwrap();
    assert!(j.put(&entry).await.is_err());
    assert!(j.load(false).await.unwrap().verification.report.is_none());
}
#[tokio::test]
async fn report_identity_epoch_definition_and_exact_full_scope_are_enforced() {
    let j = journal(&scratch("verification-binding"));
    let mut c = owner(j.clone()).await;
    for mutation in 0..4 {
        let mut r = report(1, 0, &format!("bad-{mutation}"));
        match mutation {
            0 => r.epoch = 2,
            1 => r.definition_digest = "d".repeat(64),
            2 => r.tables[0].scope.table = "other".into(),
            _ => r.migration = "other".into(),
        };
        let scope = r.tables[0].scope.clone();
        let range = &mut r.tables[0].ranges[0];
        range.id = range_identity(&scope, &range.lower, &range.upper).unwrap();
        seal(&mut r);
        assert!(c.complete_verification(&r).await.is_err());
        assert!(c.state().verification.report.is_none());
    }
    let r = report(1, 0, "run-a");
    let reference = j.publish_verification(&r).await.unwrap();
    assert_eq!(j.publish_verification(&r).await.unwrap(), reference);
    let mut changed = r.clone();
    changed.sections.schema.checked += 1;
    assert!(j.publish_verification(&changed).await.is_err());
}
#[tokio::test]
async fn completed_busy_mismatch_exclusion_and_section_findings_never_mean_success() {
    for category in 0..5 {
        let j = journal(&scratch("verification-negative"));
        let mut c = owner(j).await;
        let mut r = report(1, 0, "negative");
        match category {
            0 => {
                let range = &mut r.tables[0].ranges[0];
                range.counts.verified = 1;
                range.counts.busy = 1;
                range.exhausted_busy = 1;
                range.retry_attempts = 2;
                range.outstanding_keys = vec![vec![Scalar::Integer(5)]];
                r.retry.outstanding_ranges = vec![range.id.clone()];
            }
            1 => {
                let range = &mut r.tables[0].ranges[0];
                range.counts.verified = 1;
                range.counts.mismatched = 1;
                range.destination_digest = "d".repeat(64);
            }
            2 => {
                let range = &mut r.tables[0].ranges[0];
                range.counts.verified = 1;
                range.counts.excluded = 1;
                range.exclusions = vec![Exclusion {
                    key: vec![Scalar::Integer(5)],
                    reason: "unsupported row".into(),
                }];
            }
            3 => r.sections.destination_foreign_keys.findings.push(Finding {
                object: "fk".into(),
                detail: "violating row".into(),
            }),
            _ => {
                r.dead_letters.count = 1;
                r.dead_letters.identities = vec!["dead-1".into()];
            }
        }
        r.tables[0].counts = r.tables[0].ranges[0].counts.clone();
        seal(&mut r);
        c.complete_verification(&r).await.unwrap();
        let f = current_verification(c.state(), &facts(&r));
        assert!(!f.verification);
        assert_eq!(
            f.busy_rows_past_budget,
            Some(if category == 0 { 1 } else { 0 })
        );
        assert_eq!(f.dead_letters, Some(if category == 4 { 1 } else { 0 }));
    }
}
#[tokio::test]
async fn report_validation_rejects_incomplete_ambiguous_and_unordered_evidence() {
    let j = journal(&scratch("verification-rejections"));
    let mut c = owner(j.clone()).await;
    let good = report(1, 0, "good");
    c.complete_verification(&good).await.unwrap();
    let before = c.state().clone();
    for case in 0..18 {
        let mut r = report(1, 0, "bad");
        match case {
            0 => r.version = 2,
            1 => r.coverage_version = 2,
            2 => r.completed = false,
            3 => r.tables.clear(),
            4 => r.sections.schema.completed = false,
            5 => r.dead_letters.completed = false,
            6 => r.tables[0].counts.total = 3,
            7 => r.tables[0].ranges[0].counts.verified = 0,
            8 => r.tables[0].ranges[0].applied_lsn = 99,
            9 => r.tables[0].ranges[0].applied_lineage.timeline = 2,
            10 => r.tables[0].ranges[0].interval.armed = 4,
            11 => r.retry.version = 2,
            12 => r.retry.outstanding_ranges = vec!["unknown".into()],
            13 => r.dead_letters.count = 1,
            14 => r.tables[0].ranges[0].source_digest = "d".repeat(64),
            15 => r.tables[0].ranges[0].lower = Boundary::Key(vec![Scalar::Integer(0)]),
            16 => r.interval.complete_through_lsn = 0,
            _ => r.sections.schema.evidence = "x".repeat(REPORT_BYTES),
        }
        seal(&mut r);
        assert!(j.publish_verification(&r).await.is_err(), "case {case}");
        assert_eq!(j.load(false).await.unwrap(), before);
    }
    let mut r = report(1, 0, "digest");
    r.coverage_digest = "d".repeat(64);
    assert!(r.validate().is_err());
    let value = serde_json::json!({"name":"k","kind":"text","descending":false});
    assert!(serde_json::from_value::<KeyColumn>(value).is_err());
    let value = serde_json::json!({"name":"k","kind":"int8","descending":false,"collation":"C"});
    assert!(serde_json::from_value::<KeyColumn>(value).is_err());
}
#[test]
fn typed_composite_direction_width_and_contiguous_manifest_are_checked() {
    let mut r = report(1, 0, "ranges");
    let table = &mut r.tables[0];
    let base = table.ranges[0].clone();
    let cuts = [
        Boundary::NegativeInfinity,
        Boundary::Key(vec![Scalar::Integer(10)]),
        Boundary::Key(vec![Scalar::Integer(20)]),
        Boundary::PositiveInfinity,
    ];
    table.ranges = (0..3)
        .map(|i| {
            let mut x = base.clone();
            x.lower = cuts[i].clone();
            x.upper = cuts[i + 1].clone();
            x.id = range_identity(&table.scope, &x.lower, &x.upper).unwrap();
            x.counts = Counts::default();
            x
        })
        .collect();
    table.counts = Counts::default();
    seal(&mut r);
    r.validate().unwrap();
    for case in 0..5 {
        let mut bad = r.clone();
        match case {
            0 => bad.tables[0].ranges[1].upper = Boundary::Key(vec![Scalar::Integer(5)]),
            1 => bad.tables[0].ranges[1].lower = Boundary::Key(vec![Scalar::Integer(11)]),
            2 => bad.tables[0].ranges[1].lower = Boundary::Key(vec![Scalar::Bool(true)]),
            3 => bad.tables[0].ranges[1].id = bad.tables[0].ranges[0].id.clone(),
            _ => bad.tables[0].scope.key[0].kind = KeyType::Int2,
        };
        if case == 4 {
            bad.tables[0].ranges[1].upper = Boundary::Key(vec![Scalar::Integer(100000)]);
        }
        for table in &mut bad.tables {
            if case != 3 {
                for range in &mut table.ranges {
                    range.id = range_identity(&table.scope, &range.lower, &range.upper).unwrap();
                }
            }
        }
        seal(&mut bad);
        assert!(bad.validate().is_err(), "case {case}");
    }
    let mut descending = r.clone();
    descending.tables[0].scope.key[0].descending = true;
    let table = &mut descending.tables[0];
    table.ranges[0].upper = Boundary::Key(vec![Scalar::Integer(20)]);
    table.ranges[1].lower = Boundary::Key(vec![Scalar::Integer(20)]);
    table.ranges[1].upper = Boundary::Key(vec![Scalar::Integer(10)]);
    table.ranges[2].lower = Boundary::Key(vec![Scalar::Integer(10)]);
    for range in &mut table.ranges {
        range.id = range_identity(&table.scope, &range.lower, &range.upper).unwrap();
    }
    seal(&mut descending);
    descending.validate().unwrap();
}

#[cfg(feature = "test-crash")]
#[test]
fn crash_after_durable_report_before_transition_preserves_atomicity_and_retry_binding() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    if let Some(root) = std::env::var_os("SKEDADDLE_VERIFICATION_CRASH_ROOT") {
        let root = std::path::PathBuf::from(root);
        assert!(root.starts_with("J:/gigglesdb-work"));
        runtime.block_on(async {
            let j = journal(&root);
            let mut c = owner(j).await;
            c.set_crash_seam(skedaddle::coordinator::CrashSeam::AfterVerificationReport);
            c.complete_verification(&report(1, 0, "orphan"))
                .await
                .unwrap();
        });
        panic!("crash seam did not exit");
    }
    let root = scratch("verification-crash");
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_after_durable_report_before_transition_preserves_atomicity_and_retry_binding",
            "--nocapture",
        ])
        .env("SKEDADDLE_VERIFICATION_CRASH_ROOT", &root)
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(86), "{result:?}");
    runtime.block_on(async {
        let j = journal(&root);
        let r = report(1, 0, "orphan");
        let state = j.load(false).await.unwrap();
        assert!(state.verification.report.is_none());
        let a = j.publish_verification(&r).await.unwrap();
        let b = j.publish_verification(&r).await.unwrap();
        assert_eq!(a, b);
        let mut c = Coordinator::observer(
            j.clone(),
            Local::create(scratch("verification-after-crash").join("db")).unwrap(),
            "new-owner",
        )
        .unwrap();
        c.acquire().await.unwrap();
        assert_eq!(c.state().epoch, 2);
        assert!(c.complete_verification(&r).await.is_err());
        assert!(c.state().verification.report.is_none());
        let fresh = report(2, 0, "fresh");
        c.complete_verification(&fresh).await.unwrap();
        assert!(current_verification(c.state(), &facts(&fresh)).verification);
    });
}

#[test]
fn restricted_order_checks_composite_ties_and_every_supported_scalar() {
    let domains = [
        (KeyType::Bool, Scalar::Bool(false), Scalar::Bool(true)),
        (KeyType::Int2, Scalar::Integer(-1), Scalar::Integer(1)),
        (KeyType::Int4, Scalar::Integer(-1), Scalar::Integer(1)),
        (
            KeyType::Int8,
            Scalar::Integer(i64::MIN),
            Scalar::Integer(i64::MAX),
        ),
        (
            KeyType::Bytea,
            Scalar::Bytea(vec![0, 255]),
            Scalar::Bytea(vec![1]),
        ),
        (KeyType::Uuid, Scalar::Uuid([0; 16]), Scalar::Uuid([1; 16])),
    ];
    for (kind, lo, hi) in domains {
        let mut r = report(1, 0, "typed");
        let table = &mut r.tables[0];
        table.scope.key = vec![
            KeyColumn {
                name: "first".into(),
                kind: KeyType::Bool,
                descending: false,
            },
            KeyColumn {
                name: "tie".into(),
                kind,
                descending: true,
            },
        ];
        let bounds = [
            Boundary::NegativeInfinity,
            Boundary::Key(vec![Scalar::Bool(false), hi]),
            Boundary::Key(vec![Scalar::Bool(false), lo]),
            Boundary::PositiveInfinity,
        ];
        let base = table.ranges[0].clone();
        table.ranges = (0..3)
            .map(|i| {
                let mut x = base.clone();
                x.lower = bounds[i].clone();
                x.upper = bounds[i + 1].clone();
                x.id = range_identity(&table.scope, &x.lower, &x.upper).unwrap();
                x.counts = Counts::default();
                x
            })
            .collect();
        table.counts = Counts::default();
        seal(&mut r);
        r.validate().unwrap();
        let table = &mut r.tables[0];
        table.ranges[1].lower = Boundary::Key(vec![Scalar::Integer(0), Scalar::Bool(false)]);
        for range in &mut table.ranges {
            range.id = range_identity(&table.scope, &range.lower, &range.upper).unwrap();
        }
        seal(&mut r);
        assert!(r.validate().is_err());
    }
}

#[tokio::test]
async fn partial_sections_extra_scope_and_unrelated_invalidation_preserve_last_completion() {
    let j = journal(&scratch("verification-preserve"));
    let mut c = owner(j.clone()).await;
    let good = report(1, 0, "good");
    c.complete_verification(&good).await.unwrap();
    let before = c.state().clone();
    let out = Invalidation {
        version: VERSION,
        reason: InvalidationReason::Repair,
        scope: InvalidationScope::Tables(vec![("other".into(), "table".into())]),
    };
    assert!(
        c.commit(Transition::VerificationInvalidated { invalidation: out })
            .await
            .is_err()
    );
    assert_eq!(c.state(), &before);
    for slot in 0..5 {
        let mut bad = report(1, 0, &format!("section-{slot}"));
        match slot {
            0 => bad.sections.schema.completed = false,
            1 => bad.sections.sequences.completed = false,
            2 => bad.sections.constraints.completed = false,
            3 => bad.sections.indexes.completed = false,
            _ => bad.sections.destination_foreign_keys.completed = false,
        };
        assert!(c.complete_verification(&bad).await.is_err());
        assert_eq!(c.state(), &before);
    }
    let mut partial = report(1, 0, "partial");
    partial.completed = false;
    assert!(c.complete_verification(&partial).await.is_err());
    assert_eq!(c.state(), &before);
    let mut extra = report(1, 0, "extra");
    let mut table = extra.tables[0].clone();
    table.scope.table = "extra".into();
    for range in &mut table.ranges {
        range.id = range_identity(&table.scope, &range.lower, &range.upper).unwrap();
    }
    extra.tables.push(table);
    seal(&mut extra);
    assert!(c.complete_verification(&extra).await.is_err());
    assert_eq!(c.state(), &before);
    let applicable = Invalidation {
        version: VERSION,
        reason: InvalidationReason::Repair,
        scope: InvalidationScope::Tables(vec![("public".into(), "accounts".into())]),
    };
    c.commit(Transition::VerificationInvalidated {
        invalidation: applicable,
    })
    .await
    .unwrap();
    assert_eq!(c.state().verification.generation, 1);
    assert_eq!(
        current_verification(c.state(), &facts(&good)),
        VerificationFacts::default()
    );
}

#[tokio::test]
async fn legacy_verifier_ids_never_collide_with_relocatable_metadata() {
    use skedaddle::coordinator::VerifierRange;
    let j = journal(&scratch("verification-legacy-ids"));
    let path = scratch("verification-legacy-local").join("db");
    let mut c = Coordinator::observer(j.clone(), Local::create(&path).unwrap(), "owner").unwrap();
    c.acquire().await.unwrap();
    c.commit(Transition::Define {
        definition: Box::new(definition()),
    })
    .await
    .unwrap();
    let legacy = VerifierRange {
        table: "public.accounts".into(),
        range_lo: "".into(),
        range_hi: "".into(),
        source_lsn: "100".into(),
        source_digest: "legacy".into(),
        destination_digest: "legacy".into(),
        state: "verified".into(),
        rows: 2,
        retries: 0,
    };
    c.commit(Transition::VerifierRange {
        id: "completed-verification-v1".into(),
        range: Some(legacy.clone()),
    })
    .await
    .unwrap();
    let r = report(1, 0, "run-a");
    c.complete_verification(&r).await.unwrap();
    let reference = c.state().verification.clone();
    c.commit(Transition::VerifierRange {
        id: "completed-verification-v1:".into(),
        range: Some(legacy.clone()),
    })
    .await
    .unwrap();
    assert_eq!(c.state().verifier_ranges.len(), 2);
    assert_eq!(c.state().verification, reference);
    assert!(current_verification(c.state(), &facts(&r)).verification);
    assert_eq!(
        c.local()
            .read_collection("verifier_ranges", 10)
            .unwrap()
            .len(),
        3
    );
    c.checkpoint().await.unwrap();
    let expected = c.state().clone();
    drop(c);
    for checkpoint in [false, true] {
        assert_eq!(j.load(checkpoint).await.unwrap(), expected);
    }
    let local = Local::open(&path, &expected).unwrap();
    assert_eq!(local.state(), &expected);
    assert_eq!(
        local.read_collection("verifier_ranges", 10).unwrap().len(),
        3
    );
    assert_eq!(
        expected.verifier_ranges["completed-verification-v1"],
        legacy
    );
    assert_eq!(
        expected.verifier_ranges["completed-verification-v1:"],
        legacy
    );
}

// Layout witnesses reproduce the pre-V1 public field/variant types. They do
// not instantiate state or duplicate behavior; they bound inline stack cost.
#[allow(dead_code)]
mod pre_v1_layout {
    use skedaddle::coordinator::{self as c, chunk, journal::Journal};
    use std::{collections::BTreeMap, path::PathBuf};
    pub enum Transition {
        Define {
            definition: Box<skedaddle::MigrationDefinition>,
        },
        AcquireOwnership {
            epoch: u64,
        },
        Lifecycle {
            event: c::Event,
        },
        Progress {
            stream: String,
            marker: String,
            position: String,
        },
        Chunk {
            chunk: c::Chunk,
        },
        ChunkPlanned {
            plan: chunk::ChunkPlanRef,
        },
        ChunkStarted {
            chunk: skedaddle::pg::chunk::ChunkRange,
            attempt: u64,
        },
        ChunkDone {
            chunk: skedaddle::pg::chunk::ChunkRange,
            attempt: u64,
            high_lsn: u64,
            chunk_rows: chunk::ChunkRowsRef,
        },
        VerifierRange {
            id: String,
            range: Option<c::VerifierRange>,
        },
        VerifierException {
            id: String,
            exception: Option<c::VerifierException>,
        },
        FenceState {
            db: String,
            role: String,
            saved_privileges_json: String,
        },
        Intent {
            effect: c::EffectKind,
            payload: String,
        },
        ResumeEffect {
            intent_seq: u64,
        },
        EffectCompleted {
            intent_seq: u64,
        },
    }
    pub struct Materialized {
        definition: Option<skedaddle::MigrationDefinition>,
        lifecycle: c::Lifecycle,
        epoch: u64,
        owner: String,
        applied_seq: u64,
        digest: String,
        progress: BTreeMap<String, (String, String)>,
        chunks: BTreeMap<String, c::Chunk>,
        chunk_plans: BTreeMap<String, chunk::ChunkPlanRef>,
        chunk_attempts: BTreeMap<String, chunk::ChunkAttempt>,
        fence_state: BTreeMap<String, (String, String, String)>,
        audit: BTreeMap<u64, c::journal::Entry>,
        intent_roots: BTreeMap<u64, u64>,
        verifier_ranges: BTreeMap<String, c::VerifierRange>,
        verifier_exceptions: BTreeMap<String, c::VerifierException>,
    }
    pub struct Local {
        admin: giggles_core::Admin,
        state: Materialized,
        path: PathBuf,
    }
    pub struct Coordinator {
        journal: Journal,
        local: Local,
        instance: String,
        owns: bool,
        checkpoint_every: u64,
        #[cfg(feature = "test-crash")]
        crash: Option<c::CrashSeam>,
    }
}
#[test]
fn verification_metadata_has_explicit_pre_v1_layout_budget_and_reduces_coordinator_stack() {
    use skedaddle::coordinator::Materialized;
    use std::mem::size_of;
    #[cfg(all(target_os = "windows", target_pointer_width = "64"))]
    use std::mem::size_of_val;
    let old_transition = size_of::<pre_v1_layout::Transition>();
    let new_transition = size_of::<Transition>();
    let old_state = size_of::<pre_v1_layout::Materialized>();
    let new_state = size_of::<Materialized>();
    let old_coordinator = size_of::<pre_v1_layout::Coordinator>();
    let new_coordinator = size_of::<Coordinator>();
    println!(
        "layout bytes pre-V1 -> V1: Transition {old_transition} -> {new_transition}; Materialized {old_state} -> {new_state}; Coordinator {old_coordinator} -> {new_coordinator}"
    );
    assert_eq!(
        new_transition, old_transition,
        "new variants must not enlarge the hot transition enum"
    );
    assert_eq!(
        new_state,
        old_state + size_of::<usize>(),
        "all verification metadata has one pointer inline"
    );
    assert_eq!(
        new_coordinator,
        old_coordinator - old_state + size_of::<usize>(),
        "private Local.state boxing must offset verification growth and shrink the coordinator"
    );
    assert!(new_coordinator < old_coordinator);
    // Windows x64 pre-V1 measurements from the same frozen unowned source:
    // Journal::load = 5624 B, Coordinator::commit(Progress) = 7568 B.
    // These exact old budgets catch nested async growth that struct sizes miss.
    #[cfg(all(target_os = "windows", target_pointer_width = "64"))]
    {
        let j = journal(&scratch("verification-future-budget"));
        let load = j.load(true);
        let load_bytes = size_of_val(&load);
        assert!(
            load_bytes <= 5624,
            "load future grew beyond pre-V1: {load_bytes}"
        );
        drop(load);
        let mut c = support::coordinator(j, "verification-future-budget");
        let commit = c.commit(support::progress(1));
        let commit_bytes = size_of_val(&commit);
        assert!(
            commit_bytes <= 7568,
            "commit future grew beyond pre-V1: {commit_bytes}"
        );
        println!(
            "future bytes pre-V1 -> V1: load 5624 -> {load_bytes}; commit 7568 -> {commit_bytes}"
        );
    }
}

#[tokio::test]
async fn report_objects_never_alias_another_migrations_journal_namespace() {
    let root = scratch("verification-namespace");
    let store = fs_store(&root);
    let first = Journal::new(store.clone(), "p2", "migration", Duration::from_secs(3)).unwrap();
    let second = Journal::new(
        store,
        "p2",
        "migration-verification",
        Duration::from_secs(3),
    )
    .unwrap();
    let mut c = owner(first.clone()).await;
    let r = report(1, 0, "run-a");
    c.complete_verification(&r).await.unwrap();
    assert_eq!(second.load(false).await.unwrap(), Default::default());
    assert_eq!(second.load(true).await.unwrap(), Default::default());
    assert_eq!(second.scrub().await.unwrap(), Default::default());
    assert!(current_verification(&first.load(false).await.unwrap(), &facts(&r)).verification);
}

#[test]
fn native_boundaries_cannot_be_reinterpreted_as_pg_v1_reports() {
    use skedaddle::{
        event::Position,
        verify::{VerifyError, sql::PgSourcePosition},
    };
    let pg = PgSourcePosition::new("pg-report");
    assert_eq!(
        pg_report_lsn("postgres", &pg.position(42), &pg).unwrap(),
        42
    );
    // A native connector's eight-byte collision is still not a PG report.
    for (engine, position) in [
        ("mysql", Position::new("mysql-native", 42_u64.to_be_bytes())),
        ("spanner", Position::new("spanner-native", b"revision-42")),
    ] {
        assert!(matches!(
            pg_report_lsn(engine, &position, &pg),
            Err(VerifyError::NonPgVerificationReportUnsupported)
        ));
    }
    assert!(matches!(
        pg_report_lsn(
            "postgres",
            &Position::new("other-pg", 42_u64.to_be_bytes()),
            &pg
        ),
        Err(VerifyError::Position(_))
    ));
}

fn native_binding() -> skedaddle::coordinator::SourceBinding {
    skedaddle::coordinator::SourceBinding {
        version: 1,
        connector: skedaddle::coordinator::SourceConnector::Mysql,
        order_domain: "mysql:source/gtid".into(),
        stream: "source-stream".into(),
    }
}
async fn native_owner(j: Journal) -> Coordinator {
    let mut c = Coordinator::observer(
        j,
        Local::create(scratch("native-local").join("db")).unwrap(),
        "owner",
    )
    .unwrap();
    c.acquire().await.unwrap();
    let mut d = definition();
    d.source.engine = skedaddle::definition::Engine::Mysql;
    d.source.connection_ref = None;
    d.source.version = "8.4".into();
    d.source.mysql = Some(
        serde_json::from_value(serde_json::json!({
            "host": "127.0.0.1", "port": 3306,
            "user": "env:SKEDADDLE_TEST_MYSQL_USER", "password": "env:SKEDADDLE_TEST_MYSQL_PASSWORD"
        }))
        .unwrap(),
    );
    assert!(d.validate().is_empty());
    c.commit(Transition::Define {
        definition: Box::new(d),
    })
    .await
    .unwrap();
    c.commit(Transition::SourceBindingV1 {
        binding: native_binding(),
    })
    .await
    .unwrap();
    c
}
fn native_report(c: &Coordinator) -> native::Report {
    use skedaddle::{
        event::{Position, PositionWireV1},
        mysql::position::MysqlPosition,
    };
    let pg = report(
        c.state().epoch,
        c.state().verification.generation,
        "native-run",
    );
    let pos = |last: u64| -> PositionWireV1 {
        let p = MysqlPosition::Gtid {
            set: format!("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-{last}")
                .parse()
                .unwrap(),
            server_uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
            file: "mysql-bin.000001".into(),
            offset: 100,
        };
        (&Position::new("mysql:source/gtid", p.encode().unwrap())).into()
    };
    let interval = native::Interval {
        armed: 1,
        source_read: 2,
        destination_read: 3,
        complete_through: pos(9),
        evidence: "complete transcript".into(),
    };
    let scope = native::TableScope {
        schema: "public".into(),
        table: "accounts".into(),
        catalog_digest: "a".repeat(64),
        plan_digest: "b".repeat(64),
        key: vec![native::KeyColumn {
            name: "id".into(),
            mapping_identity: "bigint".into(),
            kind: native::KeyType::Signed { bits: 64 },
        }],
    };
    let lower = native::Boundary::NegativeInfinity;
    let upper = native::Boundary::PositiveInfinity;
    let counts = pg.tables[0].counts.clone();
    let range = native::RangeResult {
        id: native::range_identity(&scope, &lower, &upper).unwrap(),
        lower,
        upper,
        counts: counts.clone(),
        exclusions: vec![],
        interval: native::Interval {
            complete_through: pos(10),
            ..interval.clone()
        },
        applied: pos(100),
        applied_evidence: "applied checkpoint".into(),
        source_digest: "c".repeat(64),
        destination_digest: "c".repeat(64),
        retry_attempts: 0,
        exhausted_busy: 0,
        outstanding_keys: vec![],
    };
    let mut r = native::Report {
        version: 2,
        coverage_version: 2,
        binding: native_binding(),
        migration: "migration".into(),
        definition_digest: c
            .state()
            .definition
            .as_ref()
            .unwrap()
            .canonical_digest()
            .unwrap(),
        run: "native-run".into(),
        epoch: c.state().epoch,
        generation: c.state().verification.generation,
        completed: true,
        interval,
        tables: vec![native::TableResult {
            scope,
            counts,
            ranges: vec![range],
        }],
        retry: RetryPolicy {
            version: 2,
            ..pg.retry
        },
        dead_letters: pg.dead_letters,
        sections: pg.sections,
        coverage_digest: String::new(),
    };
    r.coverage_digest = r.computed_coverage_digest().unwrap();
    r
}
#[tokio::test]
async fn native_completion_checkpoint_recovery_and_copy_preserve_variable_positions() {
    let j = journal(&scratch("native-roundtrip"));
    let mut c = native_owner(j.clone()).await;
    let r = native_report(&c);
    assert_ne!(
        skedaddle::event::Position::try_from(r.interval.complete_through.clone())
            .unwrap()
            .bytes()
            .len(),
        skedaddle::event::Position::try_from(r.tables[0].ranges[0].applied.clone())
            .unwrap()
            .bytes()
            .len()
    );
    let reference = j.publish_native_verification(c.state(), &r).await.unwrap();
    c.commit(Transition::VerificationCompletedNative {
        report: Box::new(reference.clone()),
    })
    .await
    .unwrap();
    let expected = c.state().clone();
    let capture =
        skedaddle::event::Position::try_from(r.interval.complete_through.clone()).unwrap();
    assert!(
        j.native_verification_facts(&expected, &capture)
            .await
            .unwrap()
            .verification
    );
    c.checkpoint().await.unwrap();
    for checkpoint in [false, true] {
        assert_eq!(j.load(checkpoint).await.unwrap(), expected);
    }
    let local = Local::create(scratch("native-recover").join("db")).unwrap();
    let mut observer = Coordinator::observer(j.clone(), local, "restart").unwrap();
    observer.refresh().await.unwrap();
    assert_eq!(observer.state(), &expected);
    assert_eq!(
        *j.native_verification_report(&expected, &reference)
            .await
            .unwrap(),
        r
    );
    let dest = journal(&scratch("native-copy"));
    assert_eq!(j.copy_entries_for_rebuild(&dest).await.unwrap(), expected);
    assert_eq!(dest.load(false).await.unwrap(), expected);
}
#[tokio::test]
async fn native_admission_rejects_versions_binding_and_scope() {
    use skedaddle::coordinator::{Error, SourceBindingError, SourceConnector};
    let j = journal(&scratch("native-admission"));
    let mut c = native_owner(j.clone()).await;
    let r = native_report(&c);
    let good = j.publish_native_verification(c.state(), &r).await.unwrap();
    for mutation in 0..8 {
        let mut bad = good.clone();
        match mutation {
            0 => bad.version = 99,
            1 => bad.binding.connector = SourceConnector::Spanner,
            2 => bad.binding.order_domain = "unknown".into(),
            3 => bad.epoch += 1,
            4 => bad.generation += 1,
            5 => bad.definition_digest = "e".repeat(64),
            6 => bad.scope[0].table = "other".into(),
            _ => bad.binding.stream = "other".into(),
        }
        let e = c
            .commit(Transition::VerificationCompletedNative {
                report: Box::new(bad),
            })
            .await
            .unwrap_err();
        if mutation == 0 {
            assert!(matches!(
                e,
                Error::NativeVerification(native::Error::Version(Some(99)))
            ));
        }
    }
    let mut missing = c.state().clone();
    missing.source_binding = None;
    assert!(matches!(
        j.native_verification_report(&missing, &good).await,
        Err(Error::SourceBinding(SourceBindingError::Missing))
    ));
    let mut unknown = c.state().clone();
    unknown.source_binding.as_mut().unwrap().order_domain = "unknown".into();
    assert!(matches!(
        j.native_verification_report(&unknown, &good).await,
        Err(Error::SourceBinding(SourceBindingError::OrderDomain))
    ));
    assert!(c.state().verification.native_report.is_none());
}
#[tokio::test]
async fn native_missing_or_corrupt_report_refuses_replay_checkpoint_and_refresh() {
    use object_store::ObjectStoreExt;
    for corrupt in [false, true] {
        let root = scratch("native-loss");
        let store = fs_store(&root);
        let j = journal(&root);
        let mut c = native_owner(j.clone()).await;
        let r = native_report(&c);
        let reference = j.publish_native_verification(c.state(), &r).await.unwrap();
        c.commit(Transition::VerificationCompletedNative {
            report: Box::new(reference),
        })
        .await
        .unwrap();
        c.checkpoint().await.unwrap();
        let capture =
            skedaddle::event::Position::try_from(r.interval.complete_through.clone()).unwrap();
        assert!(
            j.native_verification_facts(c.state(), &capture)
                .await
                .unwrap()
                .verification
        );
        let key = j.verification_key(&r.run);
        if corrupt {
            store
                .put(&key, object_store::PutPayload::from_static(b"{}"))
                .await
                .unwrap();
        } else {
            store.delete(&key).await.unwrap();
        }
        assert!(
            j.native_verification_facts(c.state(), &capture)
                .await
                .is_err()
        );
        assert!(j.load(false).await.is_err());
        assert!(j.load(true).await.is_err());
        assert!(c.refresh().await.is_err());
        assert!(c.checkpoint().await.is_err());
    }
}

#[tokio::test]
async fn v1_reference_and_transition_bytes_still_roundtrip_without_native_fields() {
    let j = journal(&scratch("v1-wire"));
    let reference = j.publish_verification(&report(1, 0, "v1")).await.unwrap();
    let bytes = serde_json::to_vec(&reference).unwrap();
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<ReportRef>(&bytes).unwrap()).unwrap(),
        bytes
    );
    let transition = Transition::VerificationCompleted {
        report: Box::new(reference),
    };
    let bytes = serde_json::to_vec(&transition).unwrap();
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<Transition>(&bytes).unwrap()).unwrap(),
        bytes
    );
    let mut state = serde_json::to_value(skedaddle::coordinator::Materialized::default()).unwrap();
    assert!(state.get("verification").is_none());
    state["verification"] =
        serde_json::json!({"report": null,"invalidated": false,"generation": 1,"runs": {}});
    let old = serde_json::to_vec(&state).unwrap();
    let decoded: skedaddle::coordinator::Materialized = serde_json::from_slice(&old).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), state);
}

fn runner_native_key() -> skedaddle::mysql::chunk_key::MysqlTableKey {
    use skedaddle::mysql::chunk_key::*;
    MysqlTableKey {
        event_table: "public.accounts".into(),
        schema: "public".into(),
        table: "accounts".into(),
        columns: vec![MysqlKeyColumn {
            name: "id".into(),
            data_type: "bigint".into(),
            column_type: "bigint".into(),
            character_set: None,
            collation: None,
            type_id: "bigint".into(),
            direction: MysqlKeyDirection::Asc,
        }],
    }
}

#[tokio::test]
async fn runner_native_publication_projects_exact_current_facts_and_refuses_lost_objects() {
    use object_store::ObjectStoreExt;
    use skedaddle::{
        engine::Engine,
        event::Position,
        runner::verification::{native_current, publish_native},
    };
    for corrupt in [false, true] {
        let root = scratch("runner-native-publication");
        let store = fs_store(&root);
        let j = journal(&root);
        let c = native_owner(j.clone()).await;
        let r = native_report(&c);
        let engine = Engine::new(c, j.clone());
        let runtime = engine.runtime();
        publish_native(&runtime, &j, &r, &[runner_native_key()])
            .await
            .unwrap();
        let state = runtime.state().await;
        assert!(state.verification.report.is_none());
        assert!(state.verification.native_report.is_some());
        assert_eq!(j.load(true).await.unwrap(), state);
        let capture = Position::try_from(r.interval.complete_through.clone()).unwrap();
        let scope = r.tables.iter().map(|t| t.scope.clone()).collect::<Vec<_>>();
        let projected = native_current(&j, &state, &capture, &scope).await.unwrap();
        assert!(projected.verification.is_some());
        assert_eq!(projected.verification.unwrap().tables[0].rows.verified, 2);
        assert!(
            native_current(&j, &state, &capture, &[])
                .await
                .unwrap()
                .verification
                .is_none()
        );
        let advanced = Position::try_from(r.tables[0].ranges[0].applied.clone()).unwrap();
        assert!(
            native_current(&j, &state, &advanced, &scope)
                .await
                .unwrap()
                .verification
                .is_none()
        );
        let mut unrecorded = state.clone();
        unrecorded.verification.runs.clear();
        assert!(
            native_current(&j, &unrecorded, &capture, &scope)
                .await
                .is_err()
        );
        let mut stale = state.clone();
        stale.verification.generation += 1;
        assert!(
            native_current(&j, &stale, &capture, &scope)
                .await
                .unwrap()
                .verification
                .is_none()
        );
        let mut wrong = state.clone();
        wrong.source_binding.as_mut().unwrap().stream = "other".into();
        assert!(native_current(&j, &wrong, &capture, &scope).await.is_err());
        if corrupt {
            // Replace the object explicitly: Windows can deny rename-overwrite
            // while an object-store streaming reader is finishing its close.
            store.delete(&j.verification_key(&r.run)).await.unwrap();
            store
                .put(
                    &j.verification_key(&r.run),
                    object_store::PutPayload::from_static(b"{}"),
                )
                .await
                .unwrap();
        } else {
            store.delete(&j.verification_key(&r.run)).await.unwrap();
        }
        for _ in 0..2 {
            assert!(native_current(&j, &state, &capture, &scope).await.is_err());
        }
    }
}

#[tokio::test]
async fn runner_native_creation_refuses_unsupported_keys_before_publication() {
    use skedaddle::{engine::Engine, runner::verification::publish_native};
    let root = scratch("runner-native-key-scope");
    let j = journal(&root);
    let c = native_owner(j.clone()).await;
    let r = native_report(&c);
    let engine = Engine::new(c, j.clone());
    let runtime = engine.runtime();
    for kind in ["date", "datetime", "time", "binary", "varbinary", "varchar"] {
        let mut key = runner_native_key();
        key.columns[0].data_type = kind.into();
        key.columns[0].column_type = kind.into();
        if kind == "varchar" {
            key.columns[0].character_set = Some("utf8mb4".into());
            key.columns[0].collation = Some(skedaddle::mysql::schema::Collation {
                id: 255,
                name: "utf8mb4_0900_ai_ci".into(),
            });
        }
        key.validate().unwrap();
        assert!(matches!(publish_native(&runtime, &j, &r, &[key]).await,
            Err(skedaddle::coordinator::Error::NativeVerification(native::Error::NativeReportKeyUnsupported {table, column, kind: _}))
                if table == "public.accounts" && column == "id"));
        assert!(runtime.state().await.verification.native_report.is_none());
        assert!(
            j.native_verification_report(
                &runtime.state().await,
                &r.reference(
                    &r.binding,
                    &skedaddle::mysql::position::MysqlOrder::gtid("source").unwrap()
                )
                .unwrap()
            )
            .await
            .is_err()
        );
    }
}
