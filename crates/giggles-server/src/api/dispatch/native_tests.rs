use super::*;
use serde_json::{Value, json};

fn native(value: Value) -> proto::ExecuteRequest {
    crate::api::json::decode_execute(&serde_json::to_vec(&value).unwrap()).unwrap()
}
fn insert(id: i64) -> proto::ExecuteRequest {
    native(
        json!({"operation":{"request":{"insert":{"table":"app","values":[
        {"column":"id","value":{"parameter":"key"}}]}},
        "bindings":[{"name":"key","value":{"intValue":id.to_string()}}]}}),
    )
}
fn read_native() -> proto::ExecuteRequest {
    native(
        json!({"operation":{"request":{"read":{"table":"app","projection":["id"],
        "limit":{"literal":{"intValue":"10"}}}}}}),
    )
}
fn rows(reply: &proto::ExecuteReply) -> usize {
    match reply.native_result.as_ref().unwrap() {
        proto::execute_reply::NativeResult::OperationOutcome(outcome) => outcome.rows.len(),
        _ => panic!("expected operation outcome"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_and_sql_share_snapshot_and_controls_without_worker_sql_parser() {
    let f = Fixture::new();
    drop(f.execute(native(json!({"begin":{}}))).await.unwrap());
    drop(
        f.execute(sql("INSERT INTO app (id) VALUES (1)"))
            .await
            .unwrap(),
    );
    drop(f.execute(insert(2)).await.unwrap());
    let result = f.execute(read_native()).await.unwrap();
    assert_eq!(rows(&result.reply), 2);
    assert_eq!(
        result.reply.transaction_state,
        proto::TransactionState::InTransaction as i32
    );
    assert_eq!(
        f.execute(read_native()).await.err(),
        Some(ApiError::SessionBusy)
    );
    drop(result);
    let block = native(json!({"transaction":{"transaction":{"readOnly":true,
        "steps":[{"letBinding":{"name":"items","request":{"read":{"table":"app","projection":["id"]}}}}],
        "emit":{"values":[{"rows":"items"}]}}}}));
    let result = f.execute(block).await.unwrap();
    assert_eq!(result.reply.code, "ok");
    assert_eq!(
        result.reply.transaction_state,
        proto::TransactionState::InTransaction as i32
    );
    assert!(
        matches!(&result.reply.native_result, Some(proto::execute_reply::NativeResult::TransactionOutcome(outcome)) if outcome.emitted.len()==1)
    );
    drop(result);
    assert_eq!(f.rows(), 0);
    drop(f.execute(native(json!({"commit":{}}))).await.unwrap());
    assert_eq!(f.rows(), 2);
    drop(f.execute(native(json!({"begin":{}}))).await.unwrap());
    drop(f.execute(insert(3)).await.unwrap());
    drop(f.execute(sql("ROLLBACK")).await.unwrap());
    drop(f.execute(sql("BEGIN")).await.unwrap());
    drop(
        f.execute(sql("INSERT INTO app (id) VALUES (4)"))
            .await
            .unwrap(),
    );
    drop(f.execute(native(json!({"rollback":{}}))).await.unwrap());
    assert_eq!(f.rows(), 2);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn all_block_targets_are_authorized_before_binding_or_staging() {
    let f = Fixture::new();
    drop(f.execute(native(json!({"begin":{}}))).await.unwrap());
    let block = native(
        json!({"transaction":{"transaction":{"readOnly":false,"steps":[
            {"execute":{"insert":{"table":"app","values":[{"column":"id","value":{"literal":{"intValue":"1"}}}]}}},
            {"execute":{"read":{"table":"forbidden"}}}
        ]}}}),
    );
    let reply = f.execute(block).await.unwrap();
    assert_eq!(reply.reply.code, "42501");
    assert_eq!(
        reply.reply.transaction_state,
        proto::TransactionState::Aborted as i32
    );
    assert!(reply.reply.native_result.is_none());
    assert_eq!(f.rows(), 0);
    drop(reply);
    let commit = f.execute(native(json!({"commit":{}}))).await.unwrap();
    assert_eq!(commit.reply.code, "25P02");
    drop(commit);
    drop(f.execute(native(json!({"rollback":{}}))).await.unwrap());
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_bind_error_is_typed_redacted_and_aborts_outer_transaction() {
    let f = Fixture::new();
    drop(f.execute(native(json!({"begin":{}}))).await.unwrap());
    let request = native(json!({"operation":{"request":{"read":{"table":"app"}},
        "bindings":[{"name":"private_unused_name","value":{"textValue":"private payload"}}]}}));
    let reply = f.execute(request).await.unwrap();
    assert_eq!(reply.reply.code, "native_error");
    assert_eq!(
        reply.reply.transaction_state,
        proto::TransactionState::Aborted as i32
    );
    assert!(
        matches!(&reply.reply.native_result, Some(proto::execute_reply::NativeResult::NativeError(error))
        if error.kind == proto::NativeErrorClass::Bind as i32)
    );
    let body = std::str::from_utf8(reply.json.as_ref().unwrap()).unwrap();
    assert!(!body.contains("private"));
    drop(reply);
    drop(f.execute(native(json!({"rollback":{}}))).await.unwrap());
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_exact_fences_preserve_durable_winner_and_withhold_closed_response() {
    use giggles_core::DecisionPoint;
    for explicit in [false, true] {
        for point in [
            DecisionPoint::BeforeDecision,
            DecisionPoint::Decided,
            DecisionPoint::Committed,
        ] {
            let f = Fixture::new();
            if explicit {
                drop(f.execute(native(json!({"begin":{}}))).await.unwrap());
                drop(f.execute(insert(1)).await.unwrap());
            }
            let (entered, release) = decision_gate(&f.runtime.service.admin, point);
            let work = launch(
                &f,
                if explicit {
                    native(json!({"commit":{}}))
                } else {
                    insert(1)
                },
            );
            let context = tokio::time::timeout(Duration::from_secs(5), entered)
                .await
                .unwrap()
                .unwrap();
            f.runtime.close(&f.owner, &f.token).unwrap();
            assert_eq!(
                context.check().is_err(),
                point == DecisionPoint::BeforeDecision
            );
            release.send(()).unwrap();
            let reply = work.await.unwrap().unwrap();
            f.runtime.service.admin.set_decision_hook(None);
            assert_eq!(
                reply.reply.code,
                if point == DecisionPoint::BeforeDecision {
                    "57014"
                } else {
                    "ok"
                }
            );
            assert_eq!(reply.lease.check_release(), Err(ApiError::SessionNotFound));
            assert_eq!(
                f.rows(),
                usize::from(point != DecisionPoint::BeforeDecision)
            );
            drop(reply);
            f.runtime.shutdown().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_native_receiver_restores_same_staged_session() {
    let f = Fixture::new();
    drop(f.execute(native(json!({"begin":{}}))).await.unwrap());
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    let work = launch(&f, insert(1));
    signal(entered).await;
    work.abort();
    assert!(work.await.err().unwrap().is_cancelled());
    assert_eq!(
        f.execute(read_native()).await.err(),
        Some(ApiError::SessionBusy)
    );
    release.send(()).unwrap();
    signal(finished).await;
    let result = f.execute(read_native()).await.unwrap();
    assert_eq!(rows(&result.reply), 1);
    drop(result);
    assert_eq!(f.rows(), 0);
    drop(f.execute(native(json!({"commit":{}}))).await.unwrap());
    assert_eq!(f.rows(), 1);
    f.runtime.shutdown().await;
}



#[tokio::test(flavor = "current_thread")]
async fn wrapped_requests_cannot_hide_unauthorized_tables() {
    let f = Fixture::new();
    for table in ["forbidden", "giggles_join"] {
        for depth in 0..=2 {
            let mut request = json!({"insert":{"table":table,"values":[
                {"column":"id","value":{"literal":{"intValue":"1"}}}]}});
            if depth >= 1 {
                request = json!({"onConflict":{"request":request,"target":["id"],"action":{"nothing":{}}}});
            }
            if depth == 2 {
                request = json!({"returning":{"request":request}});
            }
            let result = f
                .execute(native(json!({"operation":{"request":request}})))
                .await
                .unwrap();
            assert_eq!(result.reply.code, "42501");
            assert!(result.reply.native_result.is_none());
            assert_eq!(
                result.reply.transaction_state,
                proto::TransactionState::Idle as i32
            );
        }
    }
    assert_eq!(f.rows(), 0);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_controls_report_transaction_state_and_storage_failure_stays_unknown() {
    let f = Fixture::new();
    for control in [json!({"commit":{}}), json!({"rollback":{}})] {
        let reply = f.execute(native(control)).await.unwrap();
        assert_eq!(reply.reply.code, "25000");
        assert_eq!(
            reply.reply.transaction_state,
            proto::TransactionState::Idle as i32
        );
        assert!(reply.reply.native_result.is_none());
    }
    f.runtime
        .service
        .admin
        .set_crash_point(giggles_core::CrashPoint::ErrorBeforeCommit)
        .unwrap();
    let reply = f.execute(insert(1)).await.unwrap();
    assert_eq!(reply.reply.code, "08007");
    assert_eq!(
        reply.reply.transaction_state,
        proto::TransactionState::Idle as i32
    );
    assert!(matches!(&reply.reply.native_result,
        Some(proto::execute_reply::NativeResult::NativeError(error))
        if error.kind == proto::NativeErrorClass::Unknown as i32));
    drop(reply);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn task_identity_cannot_execute_native_data_blocks_or_controls() {
    let mut f = Fixture::new();
    let fingerprint = f.owner.fingerprint.clone();
    f.runtime
        .service
        .reload_bindings(vec![Binding {
            fingerprint: fingerprint.clone(),
            role: Role::NoSql {
                domain: f
                    .runtime
                    .service
                    .admin
                    .authority_metadata()
                    .unwrap()
                    .0
                    .to_string(),
            },
        }])
        .unwrap();
    f.owner = f.runtime.authenticate(&fingerprint).unwrap();
    f.token = f.runtime.open(&f.owner).unwrap().session_id;
    for request in [
        insert(1),
        read_native(),
        native(json!({"transaction":{"transaction":{"readOnly":true}}})),
        native(json!({"begin":{}})),
        native(json!({"commit":{}})),
        native(json!({"rollback":{}})),
    ] {
        let result = f.execute(request).await.unwrap();
        assert_eq!(result.reply.code, "42501");
        assert_eq!(
            result.reply.transaction_state,
            proto::TransactionState::Idle as i32
        );
        assert!(result.reply.native_result.is_none());
        assert!(result.reply.sql.is_none());
    }
    assert_eq!(f.rows(), 0);
    f.runtime.shutdown().await;
}
