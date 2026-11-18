#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{
    APP, APP_PW, APPROVER, APPROVER_PW, CONTROLLER, CONTROLLER_PW, Case, LANGUAGE, Native,
    Observed, ParityFixture, Surface, TASK, http_call, observe_native, observe_pg,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use giggles_core::task::{
    Budget, DenyRule, OperationId, Outcome, ProtectedPoint, TaskError, definition::TaskObjectId,
    inspect_outcome,
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const SURFACES: [Surface; 2] = [Surface::PgSimple, Surface::HttpText];

fn write(op: &str, lo: usize, hi: usize, status: &str) -> Case {
    Case::write(
        op,
        &format!("UPDATE shipments SET status = '{status}' WHERE id BETWEEN {lo} AND {hi}"),
        format!(
            "update many shipments where id >= {lo} and id <= {hi} set status = \"{status}\" at most 25"
        ),
    )
}

#[track_caller]
fn success(o: &Observed, count: u64) {
    assert_eq!(o.sqlstate, None, "{o:?}");
    assert_eq!(o.transport, None, "{o:?}");
    assert_eq!(o.affected, Some(count), "{o:?}");
}

#[track_caller]
fn refusal(o: &Observed, e: TaskError) {
    assert_eq!(o.sqlstate.as_deref(), Some(e.sqlstate()), "{o:?}");
    assert_eq!(o.task_error.as_deref(), e.name(), "{o:?}");
    assert_eq!(o.message.as_deref(), Some(e.message()), "{o:?}");
    assert!(
        o.rows.is_empty() && o.affected.is_none() && o.transport.is_none(),
        "{o:?}"
    );
}

fn task(f: &ParityFixture) -> TaskObjectId {
    f.admin.tasks().get(TASK).unwrap().unwrap().id
}

fn inspect(f: &ParityFixture, op: &str) -> Option<Outcome> {
    inspect_outcome(
        &f.admin.sql_session(),
        task(f),
        &OperationId::new(op).unwrap(),
    )
    .unwrap()
}

fn statuses(f: &ParityFixture) -> String {
    format!(
        "{:?}",
        f.sql("SELECT merchant_id, id, status FROM shipments ORDER BY merchant_id, id")
    )
}

async fn exposure(f: &ParityFixture) -> String {
    let c = f.password(CONTROLLER, CONTROLLER_PW).await.unwrap();
    let rows = c
        .simple_query("SHOW TASK EXPOSURE \"inc-436\"")
        .await
        .unwrap();
    format!(
        "{:?}",
        rows.into_iter()
            .filter_map(|r| match r {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                    (0..r.len())
                        .map(|i| r.get(i).map(str::to_owned))
                        .collect::<Vec<_>>()
                ),
                _ => None,
            })
            .collect::<Vec<_>>()
    )
}

async fn lookup(f: &ParityFixture, s: Surface, op: &str, decision: Option<&str>) -> Observed {
    let o = f.run(s, &Case::outcome(op)).await;
    assert_eq!(o.sqlstate, None, "{o:?}");
    assert_eq!(o.transport, None, "{o:?}");
    if let Some(d) = decision {
        assert_eq!(o.rows.len(), 1, "{o:?}");
        assert_eq!(o.rows[0][0].as_deref(), Some(op));
        assert_eq!(o.rows[0][1].as_deref(), Some(d));
    } else {
        assert!(o.rows.is_empty(), "{o:?}");
    }
    o
}

async fn approved(f: &ParityFixture, s: Surface, op: &str, lo: usize, hi: usize) -> Case {
    let w = write(op, lo, hi, "queued");
    let Native::Text(text) = w.native else {
        unreachable!()
    };
    let p = Case {
        sql: format!("PREVIEW {}", w.sql.split_once("' ").unwrap().1),
        native: Native::Request(
            json!({"taskPreview":{"text":{"text":text,"languageVersion":LANGUAGE}}}),
        ),
        operation_id: None,
    };
    let o = if s == Surface::HttpText {
        // The parity observer cannot render timestamptzBinary. Project only
        // the three preview fields this test uses before normalizing them.
        let token = f.open_http().await.unwrap();
        let peer = (&f.ca, &f.agent);
        let (status, mut reply) = http_call(
            f.agent_http.address,
            peer,
            "POST",
            "/v1/session/execute",
            Some(&token),
            Some(&p.native_body()),
        )
        .await;
        http_call(
            f.agent_http.address,
            peer,
            "DELETE",
            "/v1/session",
            Some(&token),
            None,
        )
        .await;
        assert_eq!(status, 200, "{reply}");
        assert_eq!(reply["code"], "ok", "{reply}");
        reply["sql"]["columns"].as_array_mut().unwrap().truncate(3);
        for row in reply["sql"]["rows"].as_array_mut().unwrap() {
            row["values"].as_array_mut().unwrap().truncate(3);
        }
        observe_native(&reply)
    } else {
        f.run(s, &p).await
    };
    assert_eq!(o.sqlstate, None, "{o:?}");
    assert_eq!(o.rows.len(), 1, "{o:?}");
    assert_eq!(
        o.rows[0][2].as_deref(),
        Some((hi - lo + 1).to_string().as_str())
    );
    let id = o.rows[0][0].as_ref().unwrap();
    let digest = o.rows[0][1].as_ref().unwrap();
    f.password(APPROVER, APPROVER_PW)
        .await
        .unwrap()
        .simple_query(&format!(
            "APPROVE CHANGE {id} OF TASK \"{TASK}\" DIGEST '{digest}'"
        ))
        .await
        .unwrap();
    Case {
        sql: format!("OPERATION '{op}' APPLY CHANGE {id} DIGEST '{digest}'"),
        native: Native::Request(
            json!({"taskApply":{"changeId":id,"digest":STANDARD.encode(hex::decode(digest).unwrap())}}),
        ),
        operation_id: Some(op.into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_same_request_returns_receipt_changed_request_conflicts() {
    let f = ParityFixture::start().await;
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("committed.{i}");
        let w = write(&op, i + 1, i + 1, "queued");
        let first = f.run(s, &w).await;
        success(&first, 1);
        let receipt = lookup(&f, s, &op, Some("committed")).await;
        let before = exposure(&f).await;
        let rows = statuses(&f);
        assert_eq!(f.run(s, &w).await, first);
        assert_eq!(lookup(&f, s, &op, Some("committed")).await, receipt);
        assert_eq!(exposure(&f).await, before);
        refusal(
            &f.run(s, &write(&op, i + 10, i + 10, "queued")).await,
            TaskError::OperationConflict,
        );
        assert_eq!(statuses(&f), rows);
    }
    f.stop().await;
}

async fn recorded(budget: bool) {
    let f = ParityFixture::start().await;
    if budget {
        for i in 0..5 {
            success(
                &f.run(
                    Surface::PgSimple,
                    &write(&format!("burn.{i}"), i * 5 + 1, i * 5 + 5, "queued"),
                )
                .await,
                5,
            );
        }
    }
    let before = statuses(&f);
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("recorded.{i}");
        let id = if budget { 26 + i } else { 1 + i };
        let w = write(&op, id, id, if budget { "queued" } else { "delivered" });
        let e = if budget {
            TaskError::BudgetExceeded(Budget::DistinctRows)
        } else {
            TaskError::Denied(DenyRule::OutsideAllowedWrite)
        };
        refusal(&f.run(s, &w).await, e.clone());
        let stored = lookup(
            &f,
            s,
            &op,
            Some(if budget { "budget_exceeded" } else { "denied" }),
        )
        .await;
        refusal(&f.run(s, &w).await, e);
        assert_eq!(
            lookup(
                &f,
                s,
                &op,
                Some(if budget { "budget_exceeded" } else { "denied" })
            )
            .await,
            stored
        );
        refusal(
            &f.run(s, &write(&op, 29 + i, 29 + i, "queued")).await,
            TaskError::OperationConflict,
        );
    }
    assert_eq!(statuses(&f), before);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn denied_is_recorded() {
    recorded(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_exceeded_is_recorded() {
    recorded(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approval_required_leaves_id_free_then_preview_apply_same_id() {
    let f = ParityFixture::start().await;
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("approval.{i}");
        let lo = 1 + i * 6;
        refusal(
            &f.run(s, &write(&op, lo, lo + 5, "queued")).await,
            TaskError::ApprovalRequired,
        );
        lookup(&f, s, &op, None).await;
        let apply = approved(&f, s, &op, lo, lo + 5).await;
        success(&f.run(s, &apply).await, 6);
        lookup(&f, s, &op, Some("committed")).await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approval_stale_leaves_id_free() {
    let f = ParityFixture::start().await;
    let app = f.password(APP, APP_PW).await.unwrap();
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("stale.{i}");
        let lo = 1 + i * 6;
        let apply = approved(&f, s, &op, lo, lo + 5).await;
        app.simple_query(&format!("UPDATE shipments SET attempt_count = attempt_count + 1 WHERE merchant_id = 42 AND id = {lo}")).await.unwrap();
        let before = statuses(&f);
        refusal(&f.run(s, &apply).await, TaskError::ApprovalStale);
        assert_eq!(statuses(&f), before);
        lookup(&f, s, &op, None).await;
        let fresh = approved(&f, s, &op, lo, lo + 5).await;
        success(&f.run(s, &fresh).await, 6);
        lookup(&f, s, &op, Some("committed")).await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retryable_abort_leaves_id_free() {
    let f = ParityFixture::start().await;
    let app = f.password(APP, APP_PW).await.unwrap();
    for (i, s) in SURFACES.into_iter().enumerate() {
        let lo = 1 + i * 4;
        let read = lo + 3;
        app.simple_query(&format!(
            "UPDATE shipments SET status = 'pending' WHERE merchant_id = 42 AND id = {read}"
        ))
        .await
        .unwrap();
        let op = format!("retry.{i}");
        let w = Case::write(
            &op,
            &format!(
                "UPDATE shipments SET status = 'queued' WHERE id BETWEEN {lo} AND {read} AND status = 'failed'"
            ),
            format!(
                "update many shipments where id >= {lo} and id <= {read} and status = \"failed\" set status = \"queued\" at most 25"
            ),
        );
        let before = statuses(&f);
        let charged_before = exposure(&f).await;
        let (paused_tx, mut paused) = tokio::sync::mpsc::unbounded_channel();
        let (go, go_rx) = std::sync::mpsc::channel();
        let go_rx = Mutex::new(go_rx);
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        f.admin
            .set_protected_hook(Some(Arc::new(move |point| {
                if point == ProtectedPoint::AfterStatement && h.fetch_add(1, Ordering::SeqCst) < 3 {
                    paused_tx.send(()).unwrap();
                    go_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }
            })))
            .unwrap();
        let race = async {
            for _ in 0..3 {
                tokio::time::timeout(Duration::from_secs(20), paused.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let updated = tokio::time::timeout(Duration::from_secs(3), app.simple_query(&format!("UPDATE shipments SET attempt_count = attempt_count + 1 WHERE merchant_id = 42 AND id = {read}"))).await;
                go.send(()).unwrap();
                updated.unwrap().unwrap();
            }
        };
        let (o, ()) = tokio::join!(f.run(s, &w), race);
        f.admin.set_protected_hook(None).unwrap();
        refusal(&o, TaskError::Retry);
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        assert_eq!(statuses(&f), before);
        assert_eq!(exposure(&f).await, charged_before);
        lookup(&f, s, &op, None).await;
        assert!(inspect(&f, &op).is_none());
        success(&f.run(s, &w).await, 3);
    }
    f.stop().await;
}

fn revoke(f: &ParityFixture) {
    let actor = f.admin.roles().get(CONTROLLER).unwrap().unwrap();
    f.admin.tasks().revoke(&actor, TASK).unwrap();
}

fn http_open_refused(o: &Observed) {
    assert_eq!(
        o,
        &Observed {
            transport: Some("http 401".into()),
            ..Observed::default()
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_or_revoked_records_nothing() {
    let f = ParityFixture::start().await;
    let mut pg = f.agent_wire().await;
    let before = statuses(&f);
    f.admin
        .set_task_wall_clock(Some(f.expires_ms as u64 + 1000))
        .unwrap();
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("expired.{i}");
        let w = write(&op, 1 + i, 1 + i, "queued");
        // HTTP expiry is checked at statement execution, not session opening.
        let o = if s == Surface::PgSimple {
            observe_pg(&pg.simple(&w.sql).await)
        } else {
            f.run(s, &w).await
        };
        refusal(&o, TaskError::Expired);
        assert!(inspect(&f, &op).is_none());
    }
    f.admin.set_task_wall_clock(None).unwrap();
    revoke(&f);
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("revoked.{i}");
        let w = write(&op, 3 + i, 3 + i, "queued");
        if s == Surface::PgSimple {
            refusal(&observe_pg(&pg.simple(&w.sql).await), TaskError::Revoked);
        } else {
            http_open_refused(&f.run(s, &w).await);
        }
        assert!(inspect(&f, &op).is_none());
    }
    assert_eq!(statuses(&f), before);
    drop(pg);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcome_lookup_after_expiry_returns_receipt() {
    let f = ParityFixture::start().await;
    let mut receipts = Vec::new();
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("expiry.receipt.{i}");
        success(&f.run(s, &write(&op, i + 1, i + 1, "queued")).await, 1);
        receipts.push(lookup(&f, s, &op, Some("committed")).await);
    }
    f.admin
        .set_task_wall_clock(Some(f.expires_ms as u64 + 1000))
        .unwrap();
    for (i, s) in SURFACES.into_iter().enumerate() {
        assert_eq!(
            lookup(&f, s, &format!("expiry.receipt.{i}"), Some("committed")).await,
            receipts[i]
        );
        refusal(
            &f.run(
                s,
                &write(&format!("expired.new.{i}"), 10 + i, 10 + i, "queued"),
            )
            .await,
            TaskError::Expired,
        );
    }
    f.admin.set_task_wall_clock(None).unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcome_lookup_after_revocation_refused_for_executor_allowed_for_inspector() {
    let f = ParityFixture::start().await;
    let mut pg = f.agent_wire().await;
    let mut receipts = Vec::new();
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("revoked.receipt.{i}");
        success(&f.run(s, &write(&op, i + 1, i + 1, "queued")).await, 1);
        receipts.push(inspect(&f, &op).unwrap());
    }
    f.password(CONTROLLER, CONTROLLER_PW)
        .await
        .unwrap()
        .simple_query("GRANT INSPECT ON TASK \"inc-436\" TO oncall_lead")
        .await
        .unwrap();
    revoke(&f);
    for (i, s) in SURFACES.into_iter().enumerate() {
        let op = format!("revoked.receipt.{i}");
        let c = Case::outcome(&op);
        if s == Surface::PgSimple {
            refusal(&observe_pg(&pg.simple(&c.sql).await), TaskError::Revoked);
        } else {
            http_open_refused(&f.run(s, &c).await);
        }
        // inspect_outcome requires its trusted caller to check INSPECT; it has
        // no role-scoped session parameter. Use the administrator session.
        assert_eq!(inspect(&f, &op), Some(receipts[i].clone()));
        assert!(
            matches!(&receipts[i], Outcome::Committed(r) if r.rows_affected == 1 && !r.is_minimal())
        );
    }
    drop(pg);
    f.stop().await;
}

async fn no_write_state(f: &ParityFixture) -> String {
    format!(
        "{}|{:?}|{:?}|{:?}|{}",
        exposure(f).await,
        f.admin.task_disclosure(task(f)).unwrap(),
        f.admin.task_read_log(task(f)).unwrap(),
        [
            "read.committed.0",
            "read.committed.1",
            "read.denied.0",
            "read.denied.1",
            "unknown.0",
            "unknown.1"
        ]
        .map(|op| inspect(f, op)),
        statuses(f)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcome_lookup_never_writes() {
    let f = ParityFixture::start().await;
    for (i, s) in SURFACES.into_iter().enumerate() {
        success(
            &f.run(
                s,
                &write(&format!("read.committed.{i}"), i + 1, i + 1, "queued"),
            )
            .await,
            1,
        );
        refusal(
            &f.run(
                s,
                &write(&format!("read.denied.{i}"), i + 3, i + 3, "delivered"),
            )
            .await,
            TaskError::Denied(DenyRule::OutsideAllowedWrite),
        );
    }
    let before = no_write_state(&f).await;
    for _ in 0..8 {
        for (i, s) in SURFACES.into_iter().enumerate() {
            lookup(&f, s, &format!("read.committed.{i}"), Some("committed")).await;
            lookup(&f, s, &format!("read.denied.{i}"), Some("denied")).await;
            lookup(&f, s, &format!("unknown.{i}"), None).await;
        }
    }
    assert_eq!(no_write_state(&f).await, before);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_without_operation_id_refused() {
    let f = ParityFixture::start().await;
    for (i, s) in SURFACES.into_iter().enumerate() {
        let w = write(&format!("named.{i}"), i + 1, i + 1, "queued");
        let unnamed = Case {
            sql: w.sql.split_once("' ").unwrap().1.into(),
            native: w.native.clone(),
            operation_id: None,
        };
        let before = statuses(&f);
        refusal(&f.run(s, &unnamed).await, TaskError::OperationIdRequired);
        assert_eq!(statuses(&f), before);
        success(&f.run(s, &w).await, 1);
    }
    f.stop().await;
}
