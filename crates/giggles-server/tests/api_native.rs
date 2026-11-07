//! Native requests cross real authenticated transports; trusted SQL inspects
//! durable state independently of the adapters under test.
mod api_support;
use api_support::{Fixture, TestCert, rpc};
use giggles_core::{DecisionPoint, relational::Value as CoreValue};
use giggles_server::api::{json as wire_json, proto};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy, Debug)]
enum Wire {
    Http1,
    Http2,
    Grpc,
}
const WIRES: [Wire; 3] = [Wire::Http1, Wire::Http2, Wire::Grpc];

async fn execute_as(
    f: &Fixture,
    cert: &TestCert,
    token: &str,
    request: Value,
    wire: Wire,
) -> Value {
    match wire {
        Wire::Http1 | Wire::Http2 => {
            let (status, bytes) = f
                .http(
                    cert,
                    "POST",
                    "/v1/session/execute",
                    Some(token),
                    serde_json::to_vec(&request).unwrap(),
                    matches!(wire, Wire::Http2),
                )
                .await
                .unwrap();
            assert_eq!(status, 200, "{wire:?}: {}", String::from_utf8_lossy(&bytes));
            serde_json::from_slice(&bytes).unwrap()
        }
        Wire::Grpc => {
            let request =
                wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
            let mut client = f.grpc_client(cert).await;
            let response = client
                .execute(rpc(token, request))
                .await
                .unwrap()
                .into_inner();
            serde_json::from_slice(&wire_json::encode_reply(&response).unwrap()).unwrap()
        }
    }
}
async fn execute(f: &Fixture, token: &str, request: Value, wire: Wire) -> Value {
    execute_as(f, &f.app, token, request, wire).await
}
async fn ok(f: &Fixture, token: &str, request: Value, wire: Wire) -> Value {
    let reply = execute(f, token, request, wire).await;
    assert_eq!(reply["code"], "ok", "{wire:?}: {reply}");
    reply
}
fn operation(request: Value) -> Value {
    json!({"operation":{"request":request,"bindings":[]}})
}
fn block(read_only: bool, steps: Vec<Value>, emit: Option<Vec<Value>>) -> Value {
    let mut tx = json!({"readOnly":read_only,"steps":steps});
    if let Some(values) = emit {
        tx["emit"] = json!({"values":values});
    }
    json!({"transaction":{"transaction":tx,"bindings":[]}})
}
fn iv(value: i64) -> Value {
    json!({"intValue":value.to_string()})
}
fn tv(value: &str) -> Value {
    json!({"textValue":value})
}
fn literal(value: Value) -> Value {
    json!({"literal":value})
}
fn expr(value: Value) -> Value {
    json!({"value":literal(value)})
}
fn field(name: &str) -> Value {
    json!({"field":name})
}
fn compare(left: Value, kind: &str, right: Value) -> Value {
    json!({"compare":{"left":left,"comparison":format!("NATIVE_COMPARISON_{kind}"),"right":right}})
}
fn eq_id(id: i64) -> Value {
    compare(field("id"), "EQ", expr(iv(id)))
}
fn arithmetic(left: Value, kind: &str, right: Value) -> Value {
    json!({"arithmetic":{"left":left,"operator":format!("NATIVE_ARITHMETIC_OPERATOR_{kind}"),"right":right}})
}
fn call(function: &str, args: Vec<Value>) -> Value {
    json!({"call":{"function":format!("NATIVE_FUNCTION_{function}"),"arguments":args}})
}
fn projection(expression: Value, alias: &str) -> Value {
    json!({"expression":expression,"alias":alias})
}
fn read(limit: i64) -> Value {
    json!({"read":{"table":"app","order":[{"field":"id","descending":false}],"limit":literal(iv(limit)),"projection":["id","body","flag","data"]}})
}
fn insert(id: i64, body: &str) -> Value {
    json!({"insert":{"table":"app","values":[{"column":"id","value":literal(iv(id))},{"column":"body","value":literal(tv(body))}]}})
}
fn returning(request: Value, projections: Vec<Value>) -> Value {
    json!({"returning":{"request":request,"projection":projections}})
}
fn error(reply: &Value, class: &str) {
    assert_ne!(reply["code"], "ok", "{reply}");
    assert_eq!(
        reply["nativeError"]["kind"],
        format!("NATIVE_ERROR_CLASS_{class}"),
        "{reply}"
    );
    assert!(
        reply.get("operationOutcome").is_none() && reply.get("transactionOutcome").is_none(),
        "{reply}"
    );
}
fn count(rows: &Value) -> usize {
    rows.as_array().map_or(0, Vec::len)
}
fn durable_rows(f: &Fixture) -> Vec<giggles_core::relational::SqlRow> {
    f.admin
        .sql_session()
        .execute(
            "SELECT id,body,flag,data FROM app ORDER BY id LIMIT 1024",
            &[],
            None,
        )
        .unwrap()
        .rows
}
fn seed(f: &Fixture, values: &[(i64, &str)]) {
    let mut sql = f.admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for (id, body) in values {
        sql.execute(
            "INSERT INTO app (id,body) VALUES ($1,$2)",
            &[CoreValue::Int(*id), CoreValue::Text((*body).into())],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
}

#[tokio::test]
async fn native_begin_http_write_grpc_block_sql_read_and_native_commit_share_one_session() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    let outsider = f.open_http(&f.app, true).await;
    assert_eq!(
        ok(&f, &token, json!({"begin":{}}), Wire::Grpc).await["transactionState"],
        "TRANSACTION_STATE_IN_TRANSACTION"
    );
    let mut request = operation(json!({"insert":{"table":"app","values":[
        {"column":"id","value":{"parameter":"id"}}, {"column":"body","value":{"parameter":"body"}},
        {"column":"flag","value":literal(json!({"boolValue":false}))},
        {"column":"data","value":literal(json!({"bytesValue":"AP+A"}))}
    ]}}));
    request["operation"]["bindings"] =
        json!([{"name":"id","value":iv(i64::MAX)},{"name":"body","value":tv("snow 雪🙂\0")}]);
    assert_eq!(
        ok(&f, &token, request, Wire::Http1).await["operationOutcome"]["changed"],
        "1"
    );
    let tx = block(
        false,
        vec![json!({"letBinding":{"name":"saved","request":read(10)}})],
        Some(vec![
            json!({"rows":"saved"}),
            json!({"scalar":{"local":{"count":"saved"}}}),
        ]),
    );
    let reply = ok(&f, &token, tx, Wire::Grpc).await;
    assert_eq!(
        reply["transactionState"],
        "TRANSACTION_STATE_IN_TRANSACTION"
    );
    assert_eq!(
        reply["transactionOutcome"]["emitted"][1]["scalar"]["intValue"],
        "1"
    );
    assert_eq!(
        reply["transactionOutcome"]["emitted"][0]["rows"]["columns"][0]["dataType"],
        "NATIVE_DATA_TYPE_BIG_INT"
    );
    let inside = ok(
        &f,
        &token,
        json!({"sql":{"sql":"SELECT id,body,flag,data FROM app LIMIT 1"}}),
        Wire::Http2,
    )
    .await;
    assert_eq!(
        inside["sql"]["rows"][0]["values"],
        json!([iv(i64::MAX),tv("snow 雪🙂\0"),{"boolValue":false},{"bytesValue":"AP+A"}])
    );
    // An ordinary native read commits its owned snapshot. Use a read-only block
    // here so this isolation observation does not invalidate the writer snapshot.
    let observed = ok(
        &f,
        &outsider,
        block(
            true,
            vec![json!({"letBinding":{"name":"r","request":read(10)}})],
            Some(vec![json!({"rows":"r"})]),
        ),
        Wire::Http1,
    )
    .await;
    assert_eq!(
        count(&observed["transactionOutcome"]["emitted"][0]["rows"]["rows"]),
        0
    );
    assert_eq!(
        ok(&f, &token, json!({"commit":{}}), Wire::Http2).await["transactionState"],
        "TRANSACTION_STATE_IDLE"
    );
    assert_eq!(durable_rows(&f).len(), 1);
    assert_eq!(
        count(
            &ok(&f, &outsider, operation(read(10)), Wire::Grpc).await["operationOutcome"]["rows"]
        ),
        1
    );
    f.stop().await;
}

#[tokio::test]
async fn native_and_sql_rollback_discard_each_others_staging_on_every_transport() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    for (i, wire) in WIRES.into_iter().enumerate() {
        ok(&f, &token, json!({"begin":{}}), wire).await;
        ok(
            &f,
            &token,
            json!({"sql":{"sql":"INSERT INTO app (id) VALUES ($1)","params":[iv(i as i64)]}}),
            Wire::Grpc,
        )
        .await;
        ok(&f, &token, json!({"rollback":{}}), wire).await;
        ok(&f, &token, json!({"sql":{"sql":"BEGIN"}}), Wire::Http2).await;
        ok(&f, &token, operation(insert(i as i64, "staged")), wire).await;
        ok(&f, &token, json!({"sql":{"sql":"ROLLBACK"}}), Wire::Http1).await;
    }
    assert!(durable_rows(&f).is_empty());
    f.stop().await;
}

#[tokio::test]
async fn native_atomic_blocks_join_outer_transaction_and_failure_aborts_it() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    ok(&f, &token, json!({"begin":{}}), Wire::Http1).await;
    ok(&f, &token, operation(insert(1, "outer")), Wire::Grpc).await;
    let read_only = block(
        true,
        vec![json!({"letBinding":{"name":"r","request":read(10)}})],
        Some(vec![json!({"rows":"r"})]),
    );
    let reply = ok(&f, &token, read_only, Wire::Http2).await;
    assert_eq!(
        reply["transactionState"],
        "TRANSACTION_STATE_IN_TRANSACTION"
    );
    assert_eq!(
        count(&reply["transactionOutcome"]["emitted"][0]["rows"]["rows"]),
        1
    );
    let failed = block(
        false,
        vec![
            json!({"execute":insert(2,"block")}),
            json!({"require":expr(json!({"boolValue":false}))}),
        ],
        None,
    );
    let reply = execute(&f, &token, failed.clone(), Wire::Grpc).await;
    error(&reply, "REQUIREMENT_FAILED");
    assert_eq!(reply["transactionState"], "TRANSACTION_STATE_ABORTED");
    assert_eq!(
        execute(&f, &token, operation(read(10)), Wire::Http1).await["code"],
        "25P02"
    );
    assert_ne!(
        execute(&f, &token, json!({"commit":{}}), Wire::Http2).await["code"],
        "ok"
    );
    ok(&f, &token, json!({"rollback":{}}), Wire::Grpc).await;
    assert!(durable_rows(&f).is_empty());
    let reply = execute(&f, &token, failed, Wire::Http1).await;
    error(&reply, "REQUIREMENT_FAILED");
    assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IDLE");
    assert!(durable_rows(&f).is_empty());
    f.stop().await;
}

#[tokio::test]
async fn native_block_publishes_once_and_immutable_locals_emit_real_versions() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    let committed = Arc::new(AtomicUsize::new(0));
    let observed = committed.clone();
    f.admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == DecisionPoint::Committed {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    })));
    let request = block(
        false,
        vec![
            json!({"letBinding":{"name":"created","request":returning(insert(7,"before"),vec![])}}),
            json!({"execute":{"updateExpressions":{"table":"app","filter":eq_id(7),"values":[{"column":"body","expression":expr(tv("after"))}],"intent":{"one":{}}}}}),
            json!({"require":compare(json!({"local":{"status":"created"}}),"EQ",expr(tv("success")))}),
        ],
        Some(vec![
            json!({"row":"created"}),
            json!({"scalar":{"local":{"field":{"local":"created","field":"body"}}}}),
            json!({"scalar":{"local":{"changed":"created"}}}),
        ]),
    );
    let reply = ok(&f, &token, request, Wire::Grpc).await;
    f.admin.set_decision_hook(None);
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    let output = &reply["transactionOutcome"];
    assert_eq!(count(&output["summaries"]), 2);
    assert_eq!(output["summaries"][1]["itemIndex"], "1");
    assert_eq!(
        output["emitted"][0]["row"]["row"]["values"][1],
        tv("before")
    );
    assert_eq!(output["emitted"][1]["scalar"], tv("before"));
    assert_eq!(output["emitted"][2]["scalar"], iv(1));
    let version = output["emitted"][0]["row"]["row"]["version"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let durable = durable_rows(&f);
    assert_eq!(durable[0].values[1], CoreValue::Text("after".into()));
    assert!(durable[0].version > version);
    f.stop().await;
}

#[tokio::test]
async fn native_cardinality_precedes_guard_caps_are_atomic_and_noops_preserve_versions() {
    let f = Fixture::new().await;
    seed(&f, &[(1, "same"), (2, "same"), (3, "same")]);
    let token = f.open_http(&f.app, false).await;
    let update = |filter: Value, guard: Option<Value>, intent: Value, text: &str| {
        let mut request = json!({"table":"app","filter":filter,"values":[{"column":"body","value":literal(tv(text))}],"intent":intent});
        if let Some(guard) = guard {
            request["guard"] = guard;
        }
        operation(json!({"update":request}))
    };
    let all = expr(json!({"boolValue":true}));
    let never = expr(json!({"boolValue":false}));
    let before = durable_rows(&f);
    error(
        &execute(
            &f,
            &token,
            update(all.clone(), Some(never.clone()), json!({"one":{}}), "bad"),
            Wire::Http1,
        )
        .await,
        "AMBIGUOUS_MATCH",
    );
    error(
        &execute(
            &f,
            &token,
            update(eq_id(99), None, json!({"one":{}}), "bad"),
            Wire::Http2,
        )
        .await,
        "NOT_FOUND",
    );
    error(
        &execute(
            &f,
            &token,
            update(eq_id(1), Some(never), json!({"one":{}}), "bad"),
            Wire::Grpc,
        )
        .await,
        "CONDITION_FAILED",
    );
    error(
        &execute(
            &f,
            &token,
            update(
                all.clone(),
                None,
                json!({"many":{"cap":literal(iv(2))}}),
                "bad",
            ),
            Wire::Grpc,
        )
        .await,
        "TOO_MANY_MATCHES",
    );
    assert_eq!(durable_rows(&f), before);
    let reply = ok(
        &f,
        &token,
        update(
            all.clone(),
            None,
            json!({"many":{"cap":literal(iv(3))}}),
            "same",
        ),
        Wire::Http2,
    )
    .await;
    assert_eq!(
        (
            reply["operationOutcome"]["matched"].as_str(),
            reply["operationOutcome"]["affected"].as_str(),
            reply["operationOutcome"]["changed"].as_str()
        ),
        (Some("3"), Some("3"), Some("0"))
    );
    assert_eq!(durable_rows(&f), before);
    let delete = returning(
        json!({"delete":{"table":"app","filter":all,"cap":literal(iv(3))}}),
        vec![],
    );
    let reply = ok(&f, &token, operation(delete), Wire::Grpc).await;
    assert_eq!(count(&reply["operationOutcome"]["rows"]), 3);
    assert_eq!(
        reply["operationOutcome"]["rows"][0]["version"],
        before[0].version.to_string()
    );
    assert!(durable_rows(&f).is_empty());
    f.stop().await;
}

#[tokio::test]
async fn native_claim_requires_consumption_and_returns_the_actual_postimage() {
    let f = Fixture::new().await;
    seed(&f, &[(1, "ready"), (2, "ready")]);
    let token = f.open_http(&f.app, true).await;
    let claim = |text: &str| json!({"updateExpressions":{"table":"app","filter":compare(field("body"),"EQ",expr(tv("ready"))),"values":[{"column":"body","expression":expr(tv(text))}],"intent":{"claim":{"order":[{"field":"id","descending":false}]}}}});
    let before = durable_rows(&f);
    error(
        &execute(&f, &token, operation(claim("ready")), Wire::Grpc).await,
        "CLAIM_NOT_CONSUMED",
    );
    assert_eq!(durable_rows(&f), before);
    let reply = ok(
        &f,
        &token,
        operation(returning(
            claim("queued"),
            vec![
                projection(field("id"), "id"),
                projection(field("body"), "body"),
            ],
        )),
        Wire::Http2,
    )
    .await;
    assert_eq!(
        reply["operationOutcome"]["rows"][0]["values"],
        json!([iv(1), tv("queued")])
    );
    let after = durable_rows(&f);
    assert_eq!(
        reply["operationOutcome"]["rows"][0]["version"],
        after[0].version.to_string()
    );
    assert!(after[0].version > before[0].version);
    assert_eq!(after[1], before[1]);
    error(
        &execute(
            &f,
            &token,
            block(false, vec![json!({"execute":claim("queued")})], None),
            Wire::Http1,
        )
        .await,
        "UNSUPPORTED",
    );
    f.stop().await;
}

#[tokio::test]
async fn native_claim_is_provisional_until_the_shared_outer_transaction_commits() {
    let f = Fixture::new().await;
    seed(&f, &[(1, "ready"), (2, "ready")]);
    let token = f.open_http(&f.app, false).await;
    let before = durable_rows(&f);
    let claim = operation(returning(
        json!({"updateExpressions":{
            "table":"app", "filter":compare(field("body"),"EQ",expr(tv("ready"))),
            "values":[{"column":"body","expression":expr(tv("claimed"))}],
            "intent":{"claim":{"order":[{"field":"id","descending":false}]}}
        }}),
        vec![
            projection(field("id"), "id"),
            projection(field("body"), "body"),
        ],
    ));
    for wire in WIRES {
        ok(&f, &token, json!({"begin":{}}), Wire::Http2).await;
        let reply = ok(&f, &token, claim.clone(), wire).await;
        assert_eq!(
            reply["transactionState"],
            "TRANSACTION_STATE_IN_TRANSACTION"
        );
        assert_eq!(
            reply["operationOutcome"]["rows"][0]["values"],
            json!([iv(1), tv("claimed")])
        );
        assert!(
            reply["operationOutcome"]["rows"][0]["version"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > before[0].version
        );
        assert_eq!(durable_rows(&f), before);
        let staged = ok(
            &f,
            &token,
            json!({"sql":{"sql":"SELECT body FROM app WHERE id=1"}}),
            Wire::Grpc,
        )
        .await;
        assert_eq!(staged["sql"]["rows"][0]["values"], json!([tv("claimed")]));
        ok(&f, &token, json!({"sql":{"sql":"ROLLBACK"}}), Wire::Http1).await;
        assert_eq!(durable_rows(&f), before);
    }
    ok(&f, &token, json!({"begin":{}}), Wire::Http1).await;
    let reply = ok(&f, &token, claim, Wire::Grpc).await;
    ok(&f, &token, json!({"commit":{}}), Wire::Http2).await;
    assert_eq!(
        durable_rows(&f)[0].version.to_string(),
        reply["operationOutcome"]["rows"][0]["version"]
    );
    assert_eq!(
        durable_rows(&f)[0].values[1],
        CoreValue::Text("claimed".into())
    );
    f.stop().await;
}

#[tokio::test]
async fn native_checked_expressions_lazy_branches_and_simultaneous_assignments() {
    let f = Fixture::new().await;
    seed(&f, &[(3, "雪🙂a")]);
    let token = f.open_http(&f.app, false).await;
    let zero = arithmetic(expr(iv(1)), "DIVIDE", expr(iv(0)));
    let projections = vec![
        projection(json!({"negate":field("id")}), "negative"),
        projection(arithmetic(field("id"), "ADD", expr(iv(2))), "sum"),
        projection(call("LENGTH", vec![field("body")]), "unicode_length"),
        projection(
            call("LENGTH", vec![expr(json!({"bytesValue":"AP8="}))]),
            "byte_length",
        ),
        projection(
            call("COALESCE", vec![expr(iv(7)), zero.clone()]),
            "coalesced",
        ),
        projection(
            json!({"caseExpr":{"arms":[{"when":expr(json!({"boolValue":true})),"then":expr(iv(9))}],"elseExpr":zero.clone()}}),
            "case_value",
        ),
        projection(
            json!({"isNull":{"expression":field("flag"),"negated":false}}),
            "is_null",
        ),
        projection(
            json!({"notExpr":expr(json!({"boolValue":false}))}),
            "not_false",
        ),
        projection(
            json!({"orExpr":{"left":expr(json!({"boolValue":false})),"right":expr(json!({"boolValue":true}))}}),
            "either",
        ),
        projection(
            arithmetic(expr(iv(i64::MIN)), "REMAINDER", expr(iv(-1))),
            "remainder",
        ),
    ];
    let read_expr = |projection: Vec<Value>| {
        operation(json!({"readExpressions":{"table":"app","projection":projection}}))
    };
    let reply = ok(&f, &token, read_expr(projections), Wire::Grpc).await;
    assert_eq!(
        reply["operationOutcome"]["rows"][0]["values"],
        json!([iv(-3),iv(5),iv(3),iv(2),iv(7),iv(9),{"boolValue":true},{"boolValue":true},{"boolValue":true},iv(0)])
    );
    error(
        &execute(
            &f,
            &token,
            read_expr(vec![projection(
                call("ABS", vec![expr(iv(i64::MIN))]),
                "bad",
            )]),
            Wire::Http1,
        )
        .await,
        "OVERFLOW",
    );
    error(&execute(&f,&token,read_expr(vec![projection(json!({"andExpr":{"left":expr(json!({"boolValue":false})),"right":compare(zero,"EQ",expr(iv(0)))}}),"bad")]),Wire::Http2).await,"DIVISION_BY_ZERO");
    let insert_expr = json!({"insertExpressions":{"table":"app","values":[{"column":"id","expression":expr(iv(i64::MIN))},{"column":"body","expression":expr(tv("\0雪"))},{"column":"flag","expression":expr(json!({"boolValue":false}))},{"column":"data","expression":expr(json!({"bytesValue":"AP+A"}))}]}});
    ok(&f, &token, operation(insert_expr), Wire::Http1).await;
    let rows = durable_rows(&f);
    assert_eq!(
        rows[0].values,
        vec![
            CoreValue::Int(i64::MIN),
            CoreValue::Text("\0雪".into()),
            CoreValue::Bool(false),
            CoreValue::Bytes(vec![0, 255, 128])
        ]
    );
    // Both RHS values must read the same preimage, including the original NULL.
    let update = json!({"updateExpressions":{"table":"app","filter":eq_id(3),"values":[{"column":"body","expression":json!({"caseExpr":{"arms":[{"when":json!({"isNull":{"expression":field("flag"),"negated":false}}),"then":expr(tv("preimage"))}],"elseExpr":expr(tv("wrong"))}})},{"column":"flag","expression":expr(json!({"boolValue":true}))}],"intent":{"one":{}}}});
    ok(&f, &token, operation(update), Wire::Grpc).await;
    assert_eq!(
        durable_rows(&f)[1].values[1],
        CoreValue::Text("preimage".into())
    );
    f.stop().await;
}

#[tokio::test]
async fn native_upsert_preserves_input_mapping_skips_guards_and_duplicate_targets() {
    let f = Fixture::new().await;
    seed(&f, &[(1, "one"), (3, "three")]);
    let token = f.open_http(&f.app, false).await;
    let batch = |ids: &[i64]| json!({"insertMany":{"table":"app","columns":["id","body"],"rows":ids.iter().map(|id|json!({"values":[expr(iv(*id)),expr(tv("incoming"))]})).collect::<Vec<_>>(),"cap":literal(iv(ids.len() as i64))}});
    let skip =
        json!({"onConflict":{"request":batch(&[3,2,1]),"target":["id"],"action":{"nothing":{}}}});
    let reply = ok(&f, &token, operation(returning(skip, vec![])), Wire::Http2).await;
    assert_eq!(
        (
            reply["operationOutcome"]["matched"].as_str(),
            reply["operationOutcome"]["affected"].as_str(),
            reply["operationOutcome"]["changed"].as_str()
        ),
        (Some("3"), Some("1"), Some("1"))
    );
    let mappings = &reply["operationOutcome"]["conflictResults"]["items"];
    assert_eq!(mappings[0]["status"], "NATIVE_CONFLICT_STATUS_SKIPPED");
    assert!(mappings[0].get("returnedRow").is_none());
    assert_eq!(mappings[1]["returnedRow"], "0");
    assert_eq!(reply["operationOutcome"]["rows"][0]["values"][0], iv(2));
    let incoming =
        json!({"conflictField":{"source":"NATIVE_CONFLICT_SOURCE_INCOMING","field":"body"}});
    let existing =
        json!({"conflictField":{"source":"NATIVE_CONFLICT_SOURCE_EXISTING","field":"body"}});
    let upsert = |guard: Value| json!({"onConflict":{"request":batch(&[3,1]),"target":["id"],"action":{"update":{"guard":guard,"values":[{"column":"body","expression":incoming.clone()}]}}}});
    let before = durable_rows(&f);
    error(
        &execute(
            &f,
            &token,
            operation(upsert(compare(existing.clone(), "NE", expr(tv("three"))))),
            Wire::Grpc,
        )
        .await,
        "CONDITION_FAILED",
    );
    assert_eq!(durable_rows(&f), before);
    let reply = ok(
        &f,
        &token,
        operation(returning(
            upsert(compare(existing, "NE", expr(tv("absent")))),
            vec![],
        )),
        Wire::Grpc,
    )
    .await;
    assert_eq!(
        reply["operationOutcome"]["conflictResults"]["items"][0]["returnedRow"],
        "1"
    );
    assert_eq!(
        reply["operationOutcome"]["conflictResults"]["items"][1]["returnedRow"],
        "0"
    );
    let after = durable_rows(&f);
    let duplicate =
        json!({"onConflict":{"request":batch(&[1,1]),"target":["id"],"action":{"nothing":{}}}});
    error(
        &execute(&f, &token, operation(duplicate), Wire::Http1).await,
        "DUPLICATE_TARGET",
    );
    assert_eq!(durable_rows(&f), after);
    f.stop().await;
}

#[tokio::test]
async fn native_complete_input_limit_is_not_a_result_page_limit() {
    let f = Fixture::new().await;
    let mut sql = f.admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 0..1024 {
        sql.execute(
            "INSERT INTO app (id) VALUES ($1)",
            &[CoreValue::Int(id)],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let token = f.open_http(&f.app, false).await;
    let reply = ok(&f, &token, operation(read(1)), Wire::Http1).await;
    assert_eq!(reply["operationOutcome"]["matched"], "1024");
    assert_eq!(count(&reply["operationOutcome"]["rows"]), 1);
    assert_eq!(reply["operationOutcome"]["complete"], false);
    assert_eq!(
        count(&ok(&f, &token, operation(read(1024)), Wire::Grpc).await["operationOutcome"]["rows"]),
        1024
    );
    error(
        &execute(&f, &token, operation(insert(1024, "too_many")), Wire::Http2).await,
        "LIMIT",
    );
    sql.execute("INSERT INTO app (id) VALUES (1024)", &[], None)
        .unwrap();
    for wire in WIRES {
        error(
            &execute(&f, &token, operation(read(1)), wire).await,
            "LIMIT",
        );
    }
    error(
        &execute(
            &f,
            &token,
            operation(json!({"delete":{"table":"app","filter":eq_id(1024)}})),
            Wire::Grpc,
        )
        .await,
        "LIMIT",
    );
    assert_eq!(
        sql.execute("SELECT id FROM app WHERE id=1024", &[], None)
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(sql);
    f.stop().await;
}

#[tokio::test]
async fn native_authorization_rejects_tasks_and_late_block_tables_without_earlier_effects() {
    let f = Fixture::new().await;
    let task = f.open_http(&f.task, false).await;
    for wire in WIRES {
        let reply = execute_as(&f, &f.task, &task, operation(insert(1, "denied")), wire).await;
        assert_eq!(reply["code"], "42501");
        assert!(reply.get("operationOutcome").is_none());
    }
    let token = f.open_http(&f.app, false).await;
    let bad = block(
        false,
        vec![
            json!({"execute":insert(2,"must_not_happen")}),
            json!({"execute":{"read":{"table":"other","limit":literal(iv(1))}}}),
        ],
        None,
    );
    for wire in WIRES {
        let reply = execute(&f, &token, bad.clone(), wire).await;
        assert_eq!(reply["code"], "42501");
        assert!(reply.get("transactionOutcome").is_none());
    }
    assert!(durable_rows(&f).is_empty());
    let reply = execute(
        &f,
        &token,
        operation(json!({"read":{"table":"giggles_join","limit":literal(iv(1))}})),
        Wire::Http2,
    )
    .await;
    assert_eq!(reply["code"], "42501");
    let (status, _) = f
        .http(
            &f.other,
            "POST",
            "/v1/session/execute",
            Some(&token),
            serde_json::to_vec(&operation(read(1))).unwrap(),
            true,
        )
        .await
        .unwrap();
    assert_eq!(status, 404);
    let mut client = f.grpc_client(&f.app).await;
    client
        .execute(rpc(
            &token,
            proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
                budget_ms: None,
            },
        ))
        .await
        .unwrap();
    f.service.reload_bindings(vec![]).unwrap();
    f.runtime.invalidate_bindings();
    f.service.reload_bindings(f.bindings.clone()).unwrap();
    let request =
        wire_json::decode_execute(&serde_json::to_vec(&json!({"begin":{}})).unwrap()).unwrap();
    assert_eq!(
        client
            .execute(rpc(&token, request))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let (status, _) = f
        .http(
            &f.app,
            "POST",
            "/v1/session/execute",
            Some(&token),
            serde_json::to_vec(&operation(read(1))).unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(status, 404);
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn native_wire_rejections_preserve_transaction_and_duplicate_bindings_reject_in_grpc() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    ok(&f, &token, json!({"begin":{}}), Wire::Http1).await;
    ok(&f, &token, operation(insert(1, "staged")), Wire::Grpc).await;
    let mut deep = expr(json!({"boolValue":true}));
    for _ in 0..25 {
        deep = json!({"notExpr":deep});
    }
    let wide_projection = (0..64)
        .map(|i| projection(call("COALESCE", vec![expr(iv(1)); 64]), &format!("c{i}")))
        .collect::<Vec<_>>();
    let invalid = vec![
        json!({"operation":{"request":{}}}),
        json!({"operation":{"request":{"read":{"table":"app","order":[{"field":"id"}]}}}}),
        json!({"transaction":{"transaction":{"steps":[]}}}),
        operation(json!({"read":{"table":"app","filter":deep}})),
        operation(json!({"readExpressions":{"table":"app","projection":wide_projection}})),
        json!({"operation":{"request":read(1),"bindings":[{"name":"x","value":iv(1)},{"name":"x","value":iv(2)}]}}),
        operation(
            json!({"readExpressions":{"table":"app","projection":[projection(json!({"call":{"function":"NATIVE_FUNCTION_ABS","arguments":vec![expr(iv(1));65]}}),"too_many")]}}),
        ),
    ];
    for h2 in [false, true] {
        for request in &invalid {
            let (status, _) = f
                .http(
                    &f.app,
                    "POST",
                    "/v1/session/execute",
                    Some(&token),
                    serde_json::to_vec(request).unwrap(),
                    h2,
                )
                .await
                .unwrap();
            assert_eq!(status, 400, "{request}");
        }
    }
    let valid = json!({"operation":{"request":read(1),"bindings":[{"name":"x","value":iv(1)}]}});
    let mut request = wire_json::decode_execute(&serde_json::to_vec(&valid).unwrap()).unwrap();
    let proto::execute_request::Request::Operation(op) = request.request.as_mut().unwrap() else {
        panic!("operation");
    };
    op.bindings.push(op.bindings[0].clone());
    let mut client = f.grpc_client(&f.app).await;
    assert_eq!(
        client
            .execute(rpc(&token, request))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    // Build the oversized protobuf tree without invoking the JSON validator on it.
    let one_projection = operation(json!({"readExpressions":{"table":"app",
        "projection":[projection(call("COALESCE", vec![expr(iv(1));64]), "c")]}}));
    let mut request =
        wire_json::decode_execute(&serde_json::to_vec(&one_projection).unwrap()).unwrap();
    let proto::execute_request::Request::Operation(op) = request.request.as_mut().unwrap() else {
        panic!("operation");
    };
    let proto::native_request::Request::ReadExpressions(read_request) =
        op.request.as_mut().unwrap().request.as_mut().unwrap()
    else {
        panic!("read expressions");
    };
    read_request.projection = (0..64)
        .map(|i| {
            let mut p = read_request.projection[0].clone();
            p.alias = Some(format!("c{i}"));
            p
        })
        .collect();
    assert_eq!(
        client
            .execute(rpc(&token, request))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut accepted_depth = expr(json!({"boolValue":true}));
    for _ in 0..24 {
        accepted_depth = json!({"notExpr":accepted_depth});
    }
    ok(
        &f,
        &token,
        operation(json!({"read":{"table":"app","filter":accepted_depth,"limit":literal(iv(1))}})),
        Wire::Grpc,
    )
    .await;
    let reply = ok(&f, &token, operation(read(1)), Wire::Http2).await;
    assert_eq!(
        reply["transactionState"],
        "TRANSACTION_STATE_IN_TRANSACTION"
    );
    assert_eq!(count(&reply["operationOutcome"]["rows"]), 1);
    ok(&f, &token, json!({"rollback":{}}), Wire::Grpc).await;
    assert!(durable_rows(&f).is_empty());
    drop(client);
    f.stop().await;
}

#[tokio::test]
async fn native_large_zero_payload_emissions_obey_encoded_reply_limit_without_partial_success() {
    let f = Fixture::new().await;
    let mut sql = f.admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 0..1024 {
        sql.execute(
            "INSERT INTO app (id,body) VALUES ($1,'')",
            &[CoreValue::Int(id)],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    drop(sql);
    let token = f.open_http(&f.app, false).await;
    let projection = (0..64)
        .map(|i| projection(field("body"), &format!("empty_{i}")))
        .collect::<Vec<_>>();
    let wide_read = json!({"readExpressions":{"table":"app","limit":literal(iv(1024)),"projection":projection}});
    for (wire, emissions) in [(Wire::Http1, 16), (Wire::Grpc, 63)] {
        let tx = block(
            true,
            vec![json!({"letBinding":{"name":"rows","request":wide_read}})],
            Some(vec![json!({"rows":"rows"}); emissions]),
        );
        let reply = execute(&f, &token, tx, wire).await;
        assert_eq!(reply["code"], "08007", "{reply}");
        assert!(
            reply.get("operationOutcome").is_none() && reply.get("transactionOutcome").is_none()
        );
        assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IDLE");
    }
    assert_eq!(durable_rows(&f).len(), 1024);
    ok(&f, &token, operation(read(1)), Wire::Grpc).await;
    f.stop().await;
}

#[tokio::test]
async fn dd68_f9_native_surface_keeps_authority_for_deferred_routines() {
    let f = Fixture::new().await;
    let token = f.open_http(&f.app, false).await;
    for wire in WIRES {
        for query in ["SELECT nosuchfn()", "SELECT nosuchschema.lower('a')"] {
            let reply = execute(&f, &token, json!({"sql":{"sql":query}}), wire).await;
            assert_eq!(reply["code"], "42501", "{wire:?}: {query}: {reply}");
        }
    }
    f.stop().await;
}
