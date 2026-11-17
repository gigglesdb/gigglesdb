//! R3-1001 (GD-0061 phase 10): the parity harness. One worked-example server
//! with every listener up (PostgreSQL password, legacy certificate pgwire,
//! framed native, HTTP, gRPC, and the agent pgwire, HTTP and gRPC
//! listeners), and one case run on the four agent surfaces: pgwire simple,
//! pgwire extended, HTTP JSON native and gRPC native. The fixture and the
//! runner live in `agent_support::parity`; R3-1002 to R3-1005 build their
//! matrices on them.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{
    APP, APP_PW, Case, EXECUTOR, EXECUTOR_PW, Observed, ParityFixture, Surface, TASK, agree,
    grpc_client, http_call,
};
use giggles_core::{
    relational::Value as SqlValue,
    task::{StatementClass, TaskError},
};
use giggles_server::api::proto;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Every listener of the fixture answers its own client.
async fn listeners_answer(f: &ParityFixture) {
    // PostgreSQL password listener: the application logs in; the executor's
    // password does not open it.
    let app = f.password(APP, APP_PW).await.expect("delivery_app login");
    let n: i64 = app
        .query_one("SELECT count(*) FROM shipments WHERE merchant_id = 43", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 40);
    assert!(f.password(EXECUTOR, EXECUTOR_PW).await.is_err());
    // Legacy certificate pgwire listener.
    let mut legacy = f.legacy_wire().await;
    let answer = legacy.simple("SELECT 1").await;
    assert_eq!(agent_support::error_of(&answer), None, "{answer:?}");
    // Framed native listener: the mTLS handshake completes.
    let config = giggles_server::tls::client(
        vec![f.app.cert.der().clone()],
        PrivatePkcs8KeyDer::from(f.app.key.serialize_der()).into(),
        vec![f.ca.cert.der().clone()],
    )
    .unwrap();
    TlsConnector::from(Arc::new((*config).clone()))
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(f.native.address).await.unwrap(),
        )
        .await
        .expect("native listener handshake");
    // Ordinary HTTP and gRPC listeners open sessions for `app`.
    let (status, reply) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 201, "{reply}");
    grpc_client(f.grpc.address, &f.ca, &f.app)
        .await
        .open_session(proto::OpenSessionRequest::default())
        .await
        .expect("ordinary gRPC session");
    // The three agent listeners open task sessions for `inc-436`.
    drop(f.agent_wire().await);
    f.open_http().await.expect("agent HTTP session");
    f.open_grpc().await.expect("agent gRPC session");
}

fn status_of(f: &ParityFixture, id: i64) -> String {
    match &f.sql(&format!(
        "SELECT status FROM shipments WHERE merchant_id = 42 AND id = {id}"
    ))[0][0]
    {
        SqlValue::Text(text) => text.clone(),
        other => panic!("{other:?}"),
    }
}

#[track_caller]
fn refusal(observed: &Observed, error: &TaskError) {
    assert_eq!(
        observed.sqlstate.as_deref(),
        Some(error.sqlstate()),
        "{observed:?}"
    );
    assert_eq!(observed.task_error.as_deref(), error.name(), "{observed:?}");
    assert_eq!(
        observed.message.as_deref(),
        Some(error.message()),
        "{observed:?}"
    );
    assert!(
        observed.columns.is_empty() && observed.rows.is_empty(),
        "{observed:?}"
    );
    assert_eq!(
        (observed.affected, &observed.transport),
        (None, &None),
        "{observed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_smoke_show_and_refusal_agree_on_four_surfaces() {
    let f = ParityFixture::start().await;
    assert_eq!(TASK, "inc-436");
    listeners_answer(&f).await;

    // One protected write over pgwire, so the outcome lookup has a row.
    let write = Case::write(
        "smoke.1",
        "UPDATE shipments SET status = 'queued' WHERE id = 1",
        "update shipments where id = 1 set status = \"queued\"",
    );
    let written = f.run(Surface::PgSimple, &write).await;
    assert_eq!(written.sqlstate, None, "{written:?}");
    assert_eq!(written.affected, Some(1), "{written:?}");
    assert_eq!(status_of(&f, 1), "queued");

    // Show: SHOW OPERATION and taskOutcome agree on all four surfaces.
    let shown = agree(&f.run_all(&Case::outcome("smoke.1")).await);
    assert_eq!(shown.sqlstate, None, "{shown:?}");
    assert_eq!(
        shown.columns,
        [
            "operation_id",
            "decision",
            "row_count",
            "receipt_seq",
            "effects_digest"
        ]
    );
    assert_eq!(shown.rows.len(), 1, "{shown:?}");
    assert_eq!(
        shown.rows[0][..3],
        [
            Some("smoke.1".to_owned()),
            Some("committed".to_owned()),
            Some("1".to_owned())
        ]
    );
    assert_eq!(shown.affected, None);
    let unknown = agree(&f.run_all(&Case::outcome("never")).await);
    assert_eq!(
        (unknown.sqlstate.as_deref(), unknown.rows.len()),
        (None, 0),
        "{unknown:?}"
    );

    // Refusals: the same SQLSTATE, task error name and message everywhere.
    let unnamed = Case::new(
        "UPDATE shipments SET status = 'queued' WHERE id = 2",
        "update shipments where id = 2 set status = \"queued\"",
    );
    refusal(
        &agree(&f.run_all(&unnamed).await),
        &TaskError::OperationIdRequired,
    );
    let begin = Case::new("BEGIN", "transaction { shipments where id = 2 select id }");
    refusal(
        &agree(&f.run_all(&begin).await),
        &TaskError::NotAllowedInTaskSession(StatementClass::TransactionControl),
    );
    assert_eq!(status_of(&f, 2), "failed");
    f.stop().await;
}
