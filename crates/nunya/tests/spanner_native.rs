//! Opt-in native Spanner emulator worked example; never run by the offline suite.
use google_cloud_googleapis::spanner::v1::{spanner_client::SpannerClient, *};
use prost_types::{Value, value::Kind};
use std::{collections::BTreeMap, net::TcpListener, path::PathBuf, sync::Arc, time::Duration};
use tonic::{Request, Status, transport::Endpoint};
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Abort<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Abort<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn text(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.into())),
    }
}
fn string(v: &Value) -> &str {
    match &v.kind {
        Some(Kind::StringValue(s)) => s,
        _ => panic!("expected string cell"),
    }
}
fn sql(session: &str, statement: &str) -> ExecuteSqlRequest {
    ExecuteSqlRequest {
        session: session.into(),
        sql: statement.into(),
        ..Default::default()
    }
}
fn parameter(request: &mut ExecuteSqlRequest, token: &str) {
    request.params = Some(prost_types::Struct {
        fields: BTreeMap::from([("email".into(), text(token))]),
    });
    request.param_types.insert(
        "email".into(),
        Type {
            code: TypeCode::String as i32,
            ..Default::default()
        },
    );
}
#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR and NUNYA_TEST_SPANNER_DB"]
async fn session_worked_example() {
    let endpoint = std::env::var("NUNYA_TEST_SPANNER_EMULATOR").unwrap_or_else(|_| {
        panic!("missing required environment variable NUNYA_TEST_SPANNER_EMULATOR")
    });
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")
        .unwrap_or_else(|_| panic!("missing required environment variable NUNYA_TEST_SPANNER_DB"));

    assert_eq!(
        database,
        "projects/test-project/instances/test-instance/databases/gsql"
    );
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let dir = std::env::temp_dir().join(format!(
        "spanner-native-{}",
        data_encoding::HEXLOWER.encode(&nonce)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let _cleanup = Cleanup(dir.clone());
    let (_, mut policy) = super::common::harness_fixture();
    policy.columns = policy
        .columns
        .into_iter()
        .map(|(mut column, treatment)| {
            column.schema.clear();
            column.table = match column.table.as_str() {
                "customers" => "Customers",
                "outbox" => "Outbox",
                _ => panic!("unexpected policy relation"),
            }
            .into();
            column.column = match column.column.as_str() {
                "email" => "Email",
                "to_address" => "ToAddress",
                "phone" => "Phone",
                _ => panic!("unexpected policy column"),
            }
            .into();
            (column, treatment)
        })
        .collect();
    policy.readlog = nunya::policy::ReadLogSink::File {
        path: dir.join("read.log"),
        max_bytes: 1_000_000,
    };
    let log = nunya::readlog::open_sink(&policy.readlog).unwrap();
    let listen = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let section = nunya::spanner_serve::SpannerNativeSection {
        listen: listen.to_string(),
        database: database.clone(),
        emulator_endpoint: Some(format!("http://{endpoint}")),
        endpoint: None,
        snapshot_refresh_ms: None,
    };
    let root = nunya::token::keys::RootKey::from_hex(&"ab".repeat(32)).unwrap();
    let runtime = nunya::state::runtime::StateRuntime::open(
        &dir.join("state"),
        &root,
        &nunya::config::SessionSettings::default(),
        &[],
    )
    .unwrap();
    let state = nunya::spanner_serve::RouteState {
        authority: runtime.authority.clone(),
        registry: runtime.registry.clone(),
        route: nunya::state::model::RouteId::new("spanner-native").unwrap(),
    };
    use nunya::pgwire::auth::ScramVerifier;
    use nunya::state::{
        authority::AcquireRequest,
        model::{Mode, PrincipalKind, Verifier},
    };
    use sha2::{Digest, Sha256};
    let principal = runtime
        .authority
        .create_principal("agent", PrincipalKind::Agent, false, "native-test")
        .unwrap();
    runtime
        .authority
        .set_grants(principal.id, vec![(state.route.clone(), Mode::ReadWrite)])
        .unwrap();
    runtime
        .authority
        .add_principal_credential(
            principal.id,
            Verifier(ScramVerifier::from_password(
                "session-test",
                b"spanner-harness",
                4096,
            )),
            None,
        )
        .unwrap();
    let access = runtime
        .authority
        .acquire_session(
            principal.id,
            AcquireRequest {
                route: state.route.clone(),
                mode: None,
                lifetime_ms: None,
                isolated: false,
                label: None,
                idempotency_key: "native-test".into(),
                verifier: Verifier(ScramVerifier::from_password(
                    "session-test",
                    b"spanner-harness",
                    4096,
                )),
                secret_sha256: Sha256::digest(b"session-test").into(),
            },
        )
        .unwrap();
    let bearer = format!("Bearer nunya.session:{}.session-test", access.id.to_hex());
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let mut server = Abort(tokio::spawn(async move {
        nunya::spanner_serve::run_spanner(
            &section,
            Arc::new(policy),
            root,
            state,
            log,
            PathBuf::new(),
            None,
            rx,
        )
        .await
    }));
    let mut channel = None;
    for _ in 0..200 {
        assert!(!server.0.is_finished(), "gateway stopped during startup");
        if let Ok(c) = Endpoint::from_shared(format!("http://{listen}"))
            .unwrap()
            .connect()
            .await
        {
            channel = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // tonic::Status is large by design for the interceptor contract.
    #[allow(clippy::result_large_err)]
    let mut agent = SpannerClient::with_interceptor(
        channel.expect("gateway ready"),
        move |mut req: Request<()>| -> Result<Request<()>, Status> {
            req.metadata_mut()
                .insert("authorization", bearer.parse().unwrap());
            Ok(req)
        },
    );
    let session = agent
        .create_session(CreateSessionRequest {
            database: database.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .name;
    let result = agent
        .execute_sql(sql(
            &session,
            "SELECT Email, Phone FROM Customers WHERE Id = 1",
        ))
        .await
        .unwrap()
        .into_inner();
    let token = string(&result.rows[0].values[0]).to_owned();
    assert!(token.starts_with("tok_email_"), "email is tokenized");
    assert!(
        string(&result.rows[0].values[1]) == "[hidden]",
        "phone is masked"
    );
    let transaction = agent
        .begin_transaction(BeginTransactionRequest {
            session: session.clone(),
            options: Some(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(Default::default())),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .id;
    let id = i64::from_be_bytes(nonce[..8].try_into().unwrap()) & i64::MAX;
    let mut insert = sql(
        &session,
        &format!(
            "INSERT INTO Outbox (Id, ToAddress, Template) VALUES ({id}, @email, 'native-example')"
        ),
    );
    parameter(&mut insert, &token);
    insert.seqno = 1;
    insert.transaction = Some(TransactionSelector {
        selector: Some(transaction_selector::Selector::Id(transaction.clone())),
    });
    agent.execute_sql(insert).await.unwrap();
    agent
        .commit(CommitRequest {
            session: session.clone(),
            transaction: Some(commit_request::Transaction::TransactionId(transaction)),
            ..Default::default()
        })
        .await
        .unwrap();
    let result = agent
        .execute_sql(sql(
            &session,
            &format!("SELECT ToAddress FROM Outbox WHERE Id = {id}"),
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        string(&result.rows[0].values[0]) == token,
        "outbox read returns token"
    );
    let mut update = sql(&session, "UPDATE Notes SET Body = @email WHERE Id = 1");
    parameter(&mut update, &token);
    let Err(error) = agent.execute_sql(update).await else {
        panic!("token into clear column refused");
    };
    assert!(
        error.message().starts_with("NY001"),
        "wrong token-domain refusal"
    );
    let Err(error) = agent
        .execute_sql(sql(
            &session,
            "SELECT Email FROM Customers WHERE Email LIKE '%example%'",
        ))
        .await
    else {
        panic!("blind LIKE refused");
    };
    assert!(
        error.message().starts_with("NY002"),
        "wrong blind LIKE refusal"
    );
    let Err(error) = agent
        .partition_query(PartitionQueryRequest {
            session: session.clone(),
            sql: "SELECT Email FROM Customers".into(),
            ..Default::default()
        })
        .await
    else {
        panic!("partition query refused");
    };
    assert!(
        error.message().starts_with("NY006"),
        "wrong partition query refusal"
    );
    let direct_channel = Endpoint::from_shared(format!("http://{endpoint}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut direct = SpannerClient::new(direct_channel);
    let direct_session = direct
        .create_session(CreateSessionRequest {
            database,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .name;
    let result = direct
        .execute_sql(sql(
            &direct_session,
            &format!("SELECT ToAddress FROM Outbox WHERE Id = {id}"),
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        string(&result.rows[0].values[0]) == "alice@example.test",
        "upstream stores original value"
    );
    agent
        .delete_session(DeleteSessionRequest { name: session })
        .await
        .unwrap();
    direct
        .delete_session(DeleteSessionRequest {
            name: direct_session,
        })
        .await
        .unwrap();
    shutdown.send(true).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), &mut server.0)
            .await
            .is_ok(),
        "gateway shuts down"
    );
}
