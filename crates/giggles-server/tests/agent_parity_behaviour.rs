//! R3-1003: worked-example behaviour and durable accounting on every agent wire.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{
    APPROVER, APPROVER_PW, Case, LANGUAGE, Native, Observed, ParityFixture, Surface, TASK, agree,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use giggles_core::task::context::TaskDefinitions;
use giggles_core::{
    relational::Value,
    task::{OperationId, Outcome, Receipt, definition::TaskName, inspect_outcome},
};
use serde_json::json;

fn write(op: &str, predicate: &str) -> Case {
    Case::write(
        op,
        &format!("UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND {predicate}"),
        format!(
            "update many shipments where status = \"failed\" and {predicate} set status = \"queued\" at most 25"
        ),
    )
}

fn task(f: &ParityFixture) -> giggles_core::task::definition::TaskObjectId {
    f.admin
        .sql_session()
        .task_definition(&TaskName::new(TASK).unwrap())
        .unwrap()
        .unwrap()
        .id
}

fn receipt(f: &ParityFixture, op: &str) -> Option<Receipt> {
    match inspect_outcome(
        &f.admin.sql_session(),
        task(f),
        &OperationId::new(op).unwrap(),
    )
    .unwrap()
    {
        None | Some(Outcome::Denied(_)) | Some(Outcome::BudgetExceeded(_)) => None,
        Some(Outcome::Committed(r)) => Some(r),
    }
}

// Compare semantic accounting only: statement text, timestamps, credential
// fingerprints and wire entry identifiers intentionally differ between fixtures.
// A receipt's effects_digest is salted per commit (task/commit.rs), so it is
// compared within one fixture (replay, outcome lookup), never across fixtures.
#[derive(Debug, PartialEq)]
struct State {
    rows: Vec<Vec<Value>>,
    counters: (u64, u64),
    receipts: Vec<(String, u64, u32)>,
    disclosure: (u64, u64, u64, u64),
    log: Vec<(u64, u64, u64, u64, u64, [u8; 5])>,
}

fn state(f: &ParityFixture, ops: &[&str]) -> State {
    let receipts: Vec<_> = ops.iter().filter_map(|op| receipt(f, op)).collect();
    let counters = receipts
        .last()
        .map(|r| r.counters_after().unwrap())
        .unwrap_or((0, 0));
    let d = f.admin.task_disclosure(task(f)).unwrap();
    assert!(f.admin.task_read_verify(task(f)).unwrap().ok());
    State {
        rows: f.sql("SELECT merchant_id, id, status, attempt_count, customer_email, card_token FROM shipments ORDER BY merchant_id, id"),
        counters,
        receipts: receipts.iter().map(|r| (String::from_utf8(r.operation_id.clone()).unwrap(), r.seq, r.rows_affected)).collect(),
        disclosure: (d.rows_read, d.rows_returned, d.bytes_returned, d.read_logs),
        log: f.admin.task_read_log(task(f)).unwrap().iter().map(|r|
            (r.charged_rows_read, r.charged_rows_returned, r.charged_bytes, r.released_rows, r.masked_values, r.sqlstate)).collect(),
    }
}

fn success(o: &Observed, affected: u64) {
    assert_eq!(o.sqlstate, None, "{o:?}");
    assert_eq!(o.transport, None, "{o:?}");
    assert_eq!(o.affected, Some(affected), "{o:?}");
}
fn error(o: &Observed, code: &str) {
    assert_eq!(o.sqlstate.as_deref(), Some(code), "{o:?}");
    assert_eq!(o.transport, None, "{o:?}");
    assert!(o.rows.is_empty(), "{o:?}");
}
fn equal_states(states: &[State]) {
    for s in &states[1..] {
        assert_eq!(s.rows, states[0].rows, "durable rows");
        assert_eq!(s.counters, states[0].counters, "mutation counters");
        assert_eq!(
            s.receipts, states[0].receipts,
            "receipts (operation, seq, rows)"
        );
        assert_eq!(s.disclosure, states[0].disclosure, "disclosure charges");
        assert_eq!(s.log, states[0].log, "read log charges");
    }
}

/// An outcome lookup without its salted effects_digest column, for comparing
/// lookups made in different fixtures.
fn unsalted(seen: &[(Surface, Observed)]) -> Vec<(Surface, Observed)> {
    seen.iter()
        .map(|(surface, o)| {
            let mut o = o.clone();
            assert_eq!(
                o.columns.get(4).map(String::as_str),
                Some("effects_digest"),
                "{o:?}"
            );
            for row in &mut o.rows {
                assert_eq!(row[4].as_ref().map(String::len), Some(64), "{row:?}");
                row[4] = None;
            }
            (*surface, o)
        })
        .collect()
}

fn select() -> Case {
    Case::new(
        "SELECT * FROM shipments WHERE status = 'failed'",
        "shipments where status = \"failed\" select *",
    )
}

async fn select_matrix() {
    let mut seen = Vec::new();
    let mut states = Vec::new();
    for surface in Surface::ALL {
        let f = ParityFixture::start_with_credential().await;
        let o = f.run(surface, &select()).await;
        assert_eq!(o.sqlstate, None, "{surface:?}: {o:?}");
        assert_eq!(
            o.columns,
            [
                "id",
                "status",
                "failed_at",
                "attempt_count",
                "customer_email"
            ]
        );
        assert_eq!(o.rows.len(), 31);
        for row in &o.rows {
            assert_eq!(row[1].as_deref(), Some("failed"));
            assert_eq!(row[4], None);
        }
        let s = state(&f, &[]);
        assert_eq!((s.disclosure.0, s.disclosure.3), (31, 1));
        assert!(s.disclosure.1 >= 31);
        assert_eq!(s.disclosure.1, s.log.iter().map(|r| r.1).sum::<u64>());
        assert!(s.disclosure.2 > 0);
        assert_eq!(s.log[0].0, 31);
        assert_eq!(s.log[0].3, 31);
        seen.push((surface, o));
        states.push(s);
        f.stop().await;
    }
    agree(&seen);
    equal_states(&states);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_star_masks_and_hides_identically() {
    select_matrix().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disclosure_charge_equal_on_every_wire_format() {
    select_matrix().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hidden_and_masked_column_denials_identical() {
    use giggles_core::task::DenyRule;
    for (case, rule) in [
        (
            Case::new("SELECT card_token FROM shipments", "shipments select card_token"),
            DenyRule::ColumnNotAvailable,
        ),
        (
            Case::new(
                "SELECT id FROM shipments WHERE customer_email = 'customer1@example.com'",
                "shipments where customer_email = \"customer1@example.com\" select id",
            ),
            DenyRule::MaskedColumnPosition,
        ),
    ] {
        let mut seen = Vec::new();
        let mut states = Vec::new();
        for surface in Surface::ALL {
            let f = ParityFixture::start_with_credential().await;
            let before = state(&f, &[]);
            let o = f.run(surface, &case).await;
            error(&o, "42501");
            assert_eq!(o.task_error.as_deref(), Some("denied"), "{surface:?}: {o:?}");
            assert_eq!(o.message.as_deref(), Some(rule.message()), "{surface:?}: {o:?}");
            let after = state(&f, &[]);
            assert_eq!(after.rows, before.rows);
            assert_eq!(after.disclosure.0, 0);
            seen.push((surface, o));
            states.push(after);
            f.stop().await;
        }
        agree(&seen);
        equal_states(&states);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_update_and_replay_same_receipt() {
    let mut seen = Vec::new();
    let mut states = Vec::new();
    let mut outcomes = Vec::new();
    for surface in Surface::ALL {
        let f = ParityFixture::start_with_credential().await;
        let c = write("a1", "id <= 4");
        let o = f.run(surface, &c).await;
        success(&o, 4);
        let r = receipt(&f, "a1").unwrap();
        assert_eq!(r.counters_after(), Some((4, 4)));
        let before = state(&f, &["a1"]);
        let replay = f.run(surface, &c).await;
        assert_eq!(replay, o);
        assert_eq!(receipt(&f, "a1"), Some(r.clone()));
        let after = state(&f, &["a1"]);
        assert_eq!(after.rows, before.rows);
        assert_eq!(after.counters, before.counters);
        let shown = f.run(surface, &Case::outcome("a1")).await;
        assert_eq!(shown.rows[0][3], Some(r.seq.to_string()));
        assert_eq!(shown.rows[0][4], Some(hex::encode(r.effects_digest)));
        outcomes.push((surface, shown));
        seen.push((surface, o));
        states.push(after);
        f.stop().await;
    }
    agree(&seen);
    agree(&unsalted(&outcomes));
    equal_states(&states);
}

async fn preview(f: &ParityFixture, surface: Surface, c: &Case, count: u64) -> (String, String) {
    let mut p = Case::new(format!("PREVIEW {}", c.sql.split_once("' ").unwrap().1), "");
    let Native::Text(text) = &c.native else {
        unreachable!()
    };
    p.native = Native::Request(
        json!({"taskPreview": {"text": {"text": text, "languageVersion": LANGUAGE}}}),
    );
    let o = f.run(surface, &p).await;
    assert_eq!(o.sqlstate, None, "{surface:?}: {o:?}");
    assert_eq!(o.rows.len(), 1);
    assert_eq!(o.rows[0][2], Some(count.to_string()));
    (o.rows[0][0].clone().unwrap(), o.rows[0][1].clone().unwrap())
}
async fn approve_apply(
    f: &ParityFixture,
    surface: Surface,
    change: &str,
    digest: &str,
    op: &str,
) -> Observed {
    let approver = f.password(APPROVER, APPROVER_PW).await.unwrap();
    approver
        .simple_query(&format!(
            "APPROVE CHANGE {change} OF TASK \"{TASK}\" DIGEST '{digest}'"
        ))
        .await
        .unwrap();
    let mut c = Case::new(
        format!("OPERATION '{op}' APPLY CHANGE {change} DIGEST '{digest}'"),
        "",
    );
    c.operation_id = Some(op.into());
    c.native = Native::Request(
        json!({"taskApply": {"changeId": change, "digest": STANDARD.encode(hex::decode(digest).unwrap())}}),
    );
    f.run(surface, &c).await
}

async fn approval_matrix(conflict: bool) {
    let mut results = Vec::new();
    let mut stale = Vec::new();
    let mut required = Vec::new();
    let mut states = Vec::new();
    let mut outcomes = Vec::new();
    for surface in Surface::ALL {
        let f = ParityFixture::start_with_credential().await;
        success(&f.run(surface, &write("a1", "id <= 4")).await, 4);
        let direct = write("a2", "id >= 5 and id <= 16");
        let before = state(&f, &["a1"]);
        let o = f.run(surface, &direct).await;
        error(&o, "42T01");
        assert!(receipt(&f, "a2").is_none());
        assert_eq!(state(&f, &["a1"]).rows, before.rows);
        required.push((surface, o));
        let (p, d) = preview(&f, surface, &direct, 12).await;
        f.sql("UPDATE shipments SET status = 'delivered', attempt_count = attempt_count + 1 WHERE merchant_id = 42 AND id = 7");
        let changed = state(&f, &["a1"]);
        let o = approve_apply(&f, surface, &p, &d, "a2").await;
        error(&o, "55T01");
        assert_eq!(state(&f, &["a1"]).rows, changed.rows);
        assert!(receipt(&f, "a2").is_none());
        stale.push((surface, o));
        let (p2, d2) = preview(&f, surface, &direct, 11).await;
        assert_ne!(p2, p);
        assert_ne!(d2, d);
        let o = approve_apply(&f, surface, &p2, &d2, "a2").await;
        success(&o, 11);
        let r = receipt(&f, "a2").unwrap();
        assert_eq!(r.counters_after(), Some((15, 15)));
        assert_eq!(r.approver(), Some(APPROVER));
        if conflict {
            let before = state(&f, &["a1", "a2"]);
            let denied = f.run(surface, &direct).await;
            error(&denied, "42T03");
            assert_eq!(receipt(&f, "a2"), Some(r));
            assert_eq!(state(&f, &["a1", "a2"]).rows, before.rows);
            results.push((surface, denied));
        } else {
            results.push((surface, o));
        }
        outcomes.push((surface, f.run(surface, &Case::outcome("a2")).await));
        states.push(state(&f, &["a1", "a2"]));
        f.stop().await;
    }
    agree(&required);
    agree(&stale);
    agree(&results);
    agree(&unsalted(&outcomes));
    equal_states(&states);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approval_required_then_preview_apply() {
    approval_matrix(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operation_conflict() {
    approval_matrix(true).await;
}

async fn fill(f: &ParityFixture, surface: Surface, n: usize) -> Vec<String> {
    let mut ops = Vec::new();
    for start in (1..=n).step_by(4) {
        let end = n.min(start + 3);
        let op = format!("fill.{start}");
        success(
            &f.run(
                surface,
                &write(&op, &format!("id >= {start} and id <= {end}")),
            )
            .await,
            (end - start + 1) as u64,
        );
        ops.push(op);
    }
    ops
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_exceeded() {
    let mut seen = Vec::new();
    let mut states = Vec::new();
    for surface in Surface::ALL {
        let f = ParityFixture::start_with_credential().await;
        let ops = fill(&f, surface, 25).await;
        let ops: Vec<_> = ops.iter().map(String::as_str).collect();
        let before = state(&f, &ops);
        assert_eq!(before.counters, (25, 25));
        let o = f.run(surface, &write("overflow", "id = 26")).await;
        error(&o, "54T01");
        assert!(receipt(&f, "overflow").is_none());
        let after = state(&f, &ops);
        assert_eq!(after.rows, before.rows);
        assert_eq!(after.counters, before.counters);
        seen.push((surface, o));
        states.push(after);
        f.stop().await;
    }
    agree(&seen);
    equal_states(&states);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_scope_row_is_zero_rows() {
    let mut seen = Vec::new();
    let mut states = Vec::new();
    for surface in Surface::ALL {
        let f = ParityFixture::start_with_credential().await;
        let before = state(&f, &[]);
        let o = f.run(surface, &write("outside", "id = 32")).await;
        success(&o, 0);
        let after = state(&f, &["outside"]);
        assert_eq!(after.rows, before.rows);
        assert_eq!(after.counters, (0, 0));
        seen.push((surface, o));
        states.push(after);
        f.stop().await;
    }
    agree(&seen);
    equal_states(&states);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_surface_replay() {
    let f = ParityFixture::start_with_credential().await;
    let c = write("lost", "id <= 4");
    success(&f.run(Surface::PgSimple, &c).await, 4);
    let r = receipt(&f, "lost").unwrap();
    let before = state(&f, &["lost"]);
    let o = agree(&f.run_all(&Case::outcome("lost")).await);
    assert_eq!(o.rows[0][3], Some(r.seq.to_string()));
    assert_eq!(o.rows[0][4], Some(hex::encode(r.effects_digest)));
    let after = state(&f, &["lost"]);
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.counters, before.counters);
    f.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_and_sql_race_same_rows_equal_effects_counters_receipts() {
    let mut states = Vec::new();
    let mut winners = Vec::new();
    for reverse in [false, true, false, true] {
        let f = ParityFixture::start_with_credential().await;
        let ops = fill(&f, Surface::PgSimple, 24).await;
        // Both requests propose the same last row but use different operation
        // IDs. The loser must not consume the remaining row or transition slot.
        let sql = write("race.sql", "id = 25");
        let native = write("race.native", "id = 25");
        let (a, b) = if reverse {
            let (b, a) = tokio::join!(
                f.run(Surface::GrpcText, &native),
                f.run(Surface::PgSimple, &sql)
            );
            (a, b)
        } else {
            tokio::join!(
                f.run(Surface::PgSimple, &sql),
                f.run(Surface::GrpcText, &native)
            )
        };
        let pairs = [
            (Surface::PgSimple, "race.sql", a),
            (Surface::GrpcText, "race.native", b),
        ];
        let won: Vec<_> = pairs
            .iter()
            .filter(|(_, op, _)| receipt(&f, op).is_some_and(|r| r.rows_affected == 1))
            .collect();
        assert_eq!(won.len(), 1, "{pairs:?}");
        let (surface, op, o) = won[0];
        success(o, 1);
        let r = receipt(&f, op).unwrap();
        assert_eq!(r.counters_after(), Some((25, 25)));
        winners.push((*surface, (r.seq, r.rows_affected)));
        let (_, loser_op, loser) = pairs.iter().find(|(_, id, _)| id != op).unwrap();
        // If admission follows the winner, the row no longer matches and a
        // committed zero-row receipt is the defined serial loser result.
        if loser.sqlstate.is_some() {
            error(loser, "54T01");
            assert!(receipt(&f, loser_op).is_none());
        } else {
            success(loser, 0);
        }
        let refs: Vec<_> = ops
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(*op))
            .collect();
        let mut s = state(&f, &refs);
        s.receipts.last_mut().unwrap().0 = "winner".into();
        states.push(s);
        f.stop().await;
    }
    for (_, r) in &winners[1..] {
        assert_eq!(r, &winners[0].1);
    }
    // Read-log scheduling varies in a race; compare its reconciled totals.
    for s in &states {
        assert_eq!(s.counters, (25, 25));
    }
    for s in &mut states {
        s.log.sort();
    }
    equal_states(&states);
}
