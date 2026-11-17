//! cargo run --example grpc_transaction -- https://localhost:7435 CA.pem CERT.pem KEY.pem [ID TEXT]
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

async fn sql(
    client: &mut GigglesClient<Channel>,
    session: &str,
    sql: &str,
    params: Vec<proto::Value>,
) -> Result<proto::ExecuteReply, Error> {
    let reply = client
        .execute(request(
            session,
            proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
                    sql: sql.into(),
                    params,
                })),
                budget_ms: Some(10_000), operation_id: None,
            },
        )?)
        .await?
        .into_inner();
    if reply.code != "ok" {
        return Err(format!("SQL returned {}", reply.code).into());
    }
    Ok(reply)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 4 && args.len() != 6 {
        return Err(
            "usage: grpc_transaction https://localhost:PORT CA.pem CERT.pem KEY.pem [ID TEXT]"
                .into(),
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
        sql(&mut client, &session, "BEGIN", vec![]).await?;
        if args.len() == 6 {
            sql(
                &mut client,
                &session,
                "INSERT INTO app (id,body) VALUES ($1,$2)",
                vec![
                    proto::Value {
                        value: Some(proto::value::Value::IntValue(args[4].parse()?)),
                    },
                    proto::Value {
                        value: Some(proto::value::Value::TextValue(args[5].clone())),
                    },
                ],
            )
            .await?;
        }
        let rows = sql(
            &mut client,
            &session,
            "SELECT id,body FROM app ORDER BY id LIMIT 10",
            vec![],
        )
        .await?;
        println!("{:?}", rows.sql);
        sql(&mut client, &session, "COMMIT", vec![]).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        // Recovery is explicit. Never retry the failed statement or COMMIT.
        let _ = sql(&mut client, &session, "ROLLBACK", vec![]).await;
    }
    let closed = client
        .close_session(request(&session, proto::CloseSessionRequest {})?)
        .await;
    result?;
    closed?;
    Ok(())
}
