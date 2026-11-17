//! cargo run --example grpc_native -- https://localhost:7435 CA.pem CERT.pem KEY.pem [ID TEXT]
use giggles_server::api::proto::{self, giggles_client::GigglesClient};
use tonic::{
    Request,
    transport::{Certificate, Channel, ClientTlsConfig, Identity},
};

type Error = Box<dyn std::error::Error>;

fn request<T>(session: &str, body: T) -> Result<Request<T>, Error> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("x-giggles-session", session.parse()?);
    Ok(request)
}

async fn execute(
    client: &mut GigglesClient<Channel>,
    session: &str,
    request_variant: proto::execute_request::Request,
) -> Result<proto::ExecuteReply, Error> {
    let reply = client
        .execute(request(
            session,
            proto::ExecuteRequest {
                request: Some(request_variant),
                budget_ms: Some(10_000), operation_id: None,
            },
        )?)
        .await?
        .into_inner();
    if reply.code != "ok" {
        return Err(format!(
            "native request returned {}: {:?}",
            reply.code, reply.native_result
        )
        .into());
    }
    Ok(reply)
}

fn parameter(name: &str) -> proto::NativeValueExpr {
    proto::NativeValueExpr {
        value: Some(proto::native_value_expr::Value::Parameter(name.into())),
    }
}

fn read() -> proto::NativeRequest {
    proto::NativeRequest {
        request: Some(proto::native_request::Request::Read(proto::NativeRead {
            table: Some("app".into()),
            order: vec![proto::NativeSort {
                field: Some("id".into()),
                descending: Some(false),
            }],
            limit: Some(proto::NativeValueExpr {
                value: Some(proto::native_value_expr::Value::Literal(proto::Value {
                    value: Some(proto::value::Value::IntValue(10)),
                })),
            }),
            projection: vec!["id".into(), "body".into()],
            ..Default::default()
        })),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 4 && args.len() != 6 {
        return Err(
            "usage: grpc_native https://localhost:PORT CA.pem CERT.pem KEY.pem [ID TEXT]".into(),
        );
    }
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(std::fs::read(&args[1])?))
        .identity(Identity::from_pem(
            std::fs::read(&args[2])?,
            std::fs::read(&args[3])?,
        ));
    let channel = Channel::from_shared(args[0].clone())?
        .tls_config(tls)?
        .connect()
        .await?;
    let mut client =
        GigglesClient::new(channel).max_decoding_message_size(giggles_server::MAX_RESPONSE);
    let session = client
        .open_session(proto::OpenSessionRequest::default())
        .await?
        .into_inner()
        .session_id;
    let result: Result<(), Error> = async {
        use proto::execute_request::Request as Command;
        execute(
            &mut client,
            &session,
            Command::Begin(proto::BeginRequest {}),
        )
        .await?;
        if args.len() == 6 {
            execute(
                &mut client,
                &session,
                Command::Operation(proto::NativeOperationRequest {
                    request: Some(proto::NativeRequest {
                        request: Some(proto::native_request::Request::Insert(
                            proto::NativeInsert {
                                table: Some("app".into()),
                                values: vec![
                                    proto::NativeValueAssignment {
                                        column: Some("id".into()),
                                        value: Some(parameter("id")),
                                    },
                                    proto::NativeValueAssignment {
                                        column: Some("body".into()),
                                        value: Some(parameter("body")),
                                    },
                                ],
                            },
                        )),
                    }),
                    bindings: vec![
                        proto::NativeBinding {
                            name: Some("id".into()),
                            value: Some(proto::Value {
                                value: Some(proto::value::Value::IntValue(args[4].parse()?)),
                            }),
                        },
                        proto::NativeBinding {
                            name: Some("body".into()),
                            value: Some(proto::Value {
                                value: Some(proto::value::Value::TextValue(args[5].clone())),
                            }),
                        },
                    ],
                    page: None,
                }),
            )
            .await?;
        }
        let operation = execute(
            &mut client,
            &session,
            Command::Operation(proto::NativeOperationRequest {
                request: Some(read()),
                bindings: vec![],
                page: None,
            }),
        )
        .await?;
        // This block joins the explicit transaction and reads its staged writes.
        let block = execute(
            &mut client,
            &session,
            Command::Transaction(proto::NativeTransactionRequest {
                transaction: Some(proto::NativeTransaction {
                    read_only: Some(true),
                    steps: vec![proto::NativeTransactionStep {
                        step: Some(proto::native_transaction_step::Step::LetBinding(
                            proto::NativeLet {
                                name: Some("rows".into()),
                                request: Some(read()),
                            },
                        )),
                    }],
                    emit: Some(proto::NativeEmit {
                        values: vec![proto::NativeEmitExpr {
                            value: Some(proto::native_emit_expr::Value::Rows("rows".into())),
                        }],
                    }),
                }),
                bindings: vec![],
            }),
        )
        .await?;
        execute(
            &mut client,
            &session,
            Command::Commit(proto::CommitRequest {}),
        )
        .await?;
        println!(
            "operation: {:?}\nblock: {:?}",
            operation.native_result, block.native_result
        );
        Ok(())
    }
    .await;
    if result.is_err() {
        // Explicit recovery only: never retry a mutation or a lost COMMIT.
        let _ = execute(
            &mut client,
            &session,
            proto::execute_request::Request::Rollback(proto::RollbackRequest {}),
        )
        .await;
    }
    let closed = client
        .close_session(request(&session, proto::CloseSessionRequest {})?)
        .await;
    result?;
    closed?;
    Ok(())
}
