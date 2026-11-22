#![cfg(feature = "engine-rocksdb")]

use super::wire_execution_contract::{
    LOCK_BUDGET_MS, ParityFixture, UPDATE_ROW, WAIT_LIMIT, assert_cancelled, cancel_waiter,
    execute, ok, release_holder, reply, sql, start_holder, wait_for,
};
use giggles_server::{
    MAX_FRAME,
    api::proto::{self, execute_request::Request, value::Value},
    wire::client::Client,
};
use prost::Message;
use std::time::{Duration, Instant};
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_reaches_inline_request() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    for _ in 0..20 {
        cancel_waiter(&f, &mut waiter, Duration::from_secs(1)).await;
    }
    release_holder(&mut holder).await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_cancels_running_inline_request() {
    const UPDATE_SECOND_ROW: &str =
        "UPDATE shipments SET attempt_count=attempt_count+1 WHERE merchant_id=42 AND id=2";

    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    ok(&mut waiter, execute(Request::Begin(proto::BeginRequest {}))).await;
    let updated = ok(&mut waiter, sql(UPDATE_SECOND_ROW, LOCK_BUDGET_MS)).await;
    assert_eq!(updated.sql.unwrap().affected, 1);
    waiter
        .send_execute(sql(UPDATE_ROW, LOCK_BUDGET_MS))
        .await
        .unwrap();
    wait_for(WAIT_LIMIT, "the row-lock wait", || {
        f.admin.waiting_lock_count() == 1 && f.ordinary_api.test_blocking_permits_in_use() == 1
    })
    .await;
    assert_eq!(f.ordinary_api.test_registry_entry_count(), 2);
    let disconnected = Instant::now();
    drop(waiter);
    let limit = Duration::from_millis(500);
    wait_for(limit, "disconnect cleanup", || {
        f.ordinary_api.test_blocking_permits_in_use() == 0
            && f.admin.waiting_lock_count() == 0
            && f.ordinary_api.test_registry_entry_count() == 1
    })
    .await;

    assert!(disconnected.elapsed() < limit);
    let mut replacement = f.wire_client_for(&f.sql_app).await;
    let updated = timeout(
        Duration::from_secs(1),
        ok(&mut replacement, sql(UPDATE_SECOND_ROW, LOCK_BUDGET_MS)),
    )
    .await
    .expect("the disconnected transaction released its lock on row 2");
    assert_eq!(updated.sql.unwrap().affected, 1);
    release_holder(&mut holder).await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goodbye_does_not_cancel_running_inline_request() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    let stats = f
        .ordinary_api
        .test_wire_connection(waiter.local_addr())
        .unwrap();

    waiter
        .send_execute(sql(
            "UPDATE shipments SET status='goodbye completed' WHERE merchant_id=42 AND id=1",
            LOCK_BUDGET_MS,
        ))
        .await
        .unwrap();
    wait_for(WAIT_LIMIT, "the row-lock wait", || {
        f.admin.waiting_lock_count() == 1
    })
    .await;

    waiter.goodbye().await.unwrap();
    wait_for(WAIT_LIMIT, "the reader to receive GOODBYE", || {
        stats.received_frames() == 2
    })
    .await;
    assert_eq!(f.admin.waiting_lock_count(), 1);
    release_holder(&mut holder).await;
    wait_for(WAIT_LIMIT, "the GOODBYE session to close", || {
        f.ordinary_api.test_registry_entry_count() == 1
    })
    .await;

    let result = ok(
        &mut holder,
        sql(
            "SELECT status FROM shipments WHERE merchant_id=42 AND id=1",
            LOCK_BUDGET_MS,
        ),
    )
    .await
    .sql
    .unwrap();
    assert_eq!(
        result.rows[0].values[0].value,
        Some(Value::TextValue("goodbye completed".into()))
    );
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_followed_by_cancel_in_one_write() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    let limit = Duration::from_millis(500);
    for iteration in 0..200 {
        let started = Instant::now();
        let result = timeout(limit, async {
            let id = waiter
                .send_execute_and_cancel(sql(UPDATE_ROW, LOCK_BUDGET_MS))
                .await
                .unwrap();
            reply(&mut waiter, id).await
        })
        .await
        .unwrap_or_else(|_| panic!("iteration {iteration} did not cancel within {limit:?}"));
        assert!(started.elapsed() < limit, "iteration {iteration}");
        assert_cancelled(result);
        assert_eq!(f.admin.waiting_lock_count(), 0);
        assert_eq!(f.ordinary_api.test_blocking_permits_in_use(), 0);
    }
    release_holder(&mut holder).await;
    f.stop().await;
}

async fn start_large_pipeline(f: &ParityFixture, waiter: &mut Client) -> (u32, Vec<u32>) {
    let blocked = waiter
        .send_execute(sql(UPDATE_ROW, LOCK_BUDGET_MS))
        .await
        .unwrap();
    wait_for(WAIT_LIMIT, "the row-lock wait", || {
        f.admin.waiting_lock_count() == 1 && f.ordinary_api.test_blocking_permits_in_use() == 1
    })
    .await;

    let padding = "x".repeat(MAX_FRAME - 256);
    let mut tail = Vec::new();
    for id in 1000..1006 {
        let request = sql(
            &format!("INSERT INTO shipments (merchant_id, id) VALUES (42, {id}) /*{padding}*/"),
            LOCK_BUDGET_MS,
        );
        assert!((MAX_FRAME - 256..=MAX_FRAME).contains(&request.encoded_len()));
        tail.push(waiter.send_execute(request).await.unwrap());
    }
    (blocked, tail)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_behind_large_inline_pipeline() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    let (blocked, tail) = start_large_pipeline(&f, &mut waiter).await;

    let sent = Instant::now();
    let result = timeout(Duration::from_millis(900), async {
        waiter.send_cancel(blocked).await.unwrap();
        reply(&mut waiter, blocked).await
    })
    .await
    .expect("CANCEL reached the lock waiter behind six large requests");
    assert!(sent.elapsed() < Duration::from_secs(1));
    assert_cancelled(result);
    for id in tail {
        let result = reply(&mut waiter, id).await.unwrap();
        assert_eq!(result.code, "ok", "{result:?}");
        assert_eq!(result.sql.unwrap().affected, 1);
    }
    release_holder(&mut holder).await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_behind_large_inline_pipeline() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    start_large_pipeline(&f, &mut waiter).await;

    assert_eq!(f.ordinary_api.test_registry_entry_count(), 2);
    let disconnected = Instant::now();
    drop(waiter);
    wait_for(
        Duration::from_millis(900),
        "disconnect behind six large requests",
        || {
            f.ordinary_api.test_blocking_permits_in_use() == 0
                && f.admin.waiting_lock_count() == 0
                && f.ordinary_api.test_registry_entry_count() == 1
        },
    )
    .await;
    assert!(disconnected.elapsed() < Duration::from_secs(1));
    release_holder(&mut holder).await;
    f.stop().await;
}
