use giggles_core::{
    OperationId,
    relational::{SqlResult, SqlRow},
};
use giggles_server::{
    MAX_FRAME, Request, Response,
    api::{
        codec::{decode_value, encode_value},
        error::ApiError,
        json,
        proto::{
            ExecuteReply, ExecuteRequest, NativeTextRequest, OpenSessionRequest, PingRequest,
            SqlRequest, execute_request::Request as WireRequest,
        },
    },
    wire::client::{Client, ClientTls, Credential},
};
use prost::Message;
use std::{error::Error, fs, io::Read};

const USAGE: &str = "usage: giggles-client ADDRESS SERVER_NAME CERT KEY CA (REQUEST.json | --sql TEXT | --native-text TEXT | --ping)";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [address, name, cert, key, ca, command @ ..] = args.as_slice() else {
        return Err(USAGE.into());
    };
    let (request, legacy_json) = match command {
        [flag] if flag == "--ping" => (execute(WireRequest::Ping(PingRequest {})), false),
        [flag, sql] if flag == "--sql" => (
            execute(WireRequest::Sql(SqlRequest {
                sql: sql.clone(),
                params: vec![],
            })),
            false,
        ),
        [flag, text] if flag == "--native-text" => (
            execute(WireRequest::Text(NativeTextRequest {
                text: text.clone(),
                ..Default::default()
            })),
            false,
        ),
        [path] if !path.starts_with("--") => (read_request(path)?, true),
        _ => return Err(USAGE.into()),
    };
    if request.encoded_len() > MAX_FRAME {
        return Err("frame limit".into());
    }
    let tls = ClientTls {
        ca: fs::read(ca)?,
        cert: Some(fs::read(cert)?),
        key: Some(fs::read(key)?),
        server_name: name.clone(),
    };
    let (mut client, _) = Client::connect(
        address.parse()?,
        Some(tls),
        OpenSessionRequest::default(),
        Credential::None,
    )
    .await?;
    let reply = client.execute(request).await?;
    if legacy_json {
        println!("{}", serde_json::to_string(&legacy_response(reply)?)?);
    } else {
        println!("{}", String::from_utf8(json::encode_reply(&reply)?)?);
        // The flag modes exit nonzero on a refused or failed statement, so
        // scripts can tell. A request file keeps the old exit status 0.
        if reply.code != "ok" {
            std::process::exit(1);
        }
    }
    Ok(())
}

fn execute(request: WireRequest) -> ExecuteRequest {
    ExecuteRequest {
        request: Some(request),
        ..Default::default()
    }
}

fn read_request(path: &str) -> Result<ExecuteRequest, Box<dyn Error>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take((MAX_FRAME + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_FRAME {
        return Err("frame limit".into());
    }
    let (request, budget_ms) = match serde_json::from_slice(&bytes)? {
        Request::Deadline { budget_ms, request } => {
            if budget_ms > 10_000 {
                return Err("deadline budget_ms must be at most 10000".into());
            }
            (*request, Some(u32::try_from(budget_ms)?))
        }
        request => (request, None),
    };
    let request = match request {
        Request::Sql { sql, params } => WireRequest::Sql(SqlRequest {
            sql,
            params: params.into_iter().map(encode_value).collect(),
        }),
        Request::Ping => WireRequest::Ping(PingRequest {}),
        Request::Deadline { .. } => {
            return Err("a nested Deadline request has no wire equivalent".into());
        }
        #[cfg(feature = "legacy-task")]
        Request::Preview { request } => {
            WireRequest::Preview(giggles_server::api::proto::PreviewRequest {
                request: Some(encode_task(request)),
            })
        }
        #[cfg(feature = "legacy-task")]
        Request::Execute { request, approval } => {
            WireRequest::Execute(giggles_server::api::proto::TaskExecuteRequest {
                request: Some(encode_task(request)),
                approval: approval.map(|id| id.0),
            })
        }
        #[cfg(feature = "legacy-task")]
        Request::RepairSql {
            operation,
            sql,
            approval,
        } => WireRequest::RepairSql(giggles_server::api::proto::RepairSqlRequest {
            operation: Some(operation.0),
            sql,
            approval: approval.map(|id| id.0),
        }),
        #[cfg(feature = "legacy-task")]
        Request::Status { operation } => {
            WireRequest::Status(giggles_server::api::proto::StatusRequest {
                operation: Some(operation.0),
            })
        }
    };
    Ok(ExecuteRequest {
        budget_ms,
        ..execute(request)
    })
}

#[cfg(feature = "legacy-task")]
fn encode_task(request: giggles_core::Request) -> giggles_server::api::proto::TaskRequest {
    use giggles_core::{Column, Selection, Status};
    use giggles_server::api::proto::{self, DeliveryStatus, selection};

    fn status(value: Status) -> i32 {
        (match value {
            Status::Failed => DeliveryStatus::Failed,
            Status::Queued => DeliveryStatus::Queued,
            Status::Succeeded => DeliveryStatus::Succeeded,
        }) as i32
    }

    let column = match request.column {
        Column::Status => proto::Column::Status,
        Column::FailedAt => proto::Column::FailedAt,
        Column::AttemptCount => proto::Column::AttemptCount,
    };
    let selection = match request.selection {
        Selection::Keys(keys) => selection::Selection::Keys(proto::KeyList {
            items: keys
                .into_iter()
                .map(|key| proto::Key {
                    tenant: Some(key.tenant.0),
                    row: Some(key.row.0),
                })
                .collect(),
        }),
        Selection::Predicate {
            tenant,
            status: required,
            from,
            until,
        } => selection::Selection::Predicate(proto::Predicate {
            tenant: Some(tenant.0),
            status: status(required),
            from: Some(from),
            until: Some(until),
        }),
    };
    proto::TaskRequest {
        operation: Some(request.operation.0),
        selection: Some(proto::Selection {
            selection: Some(selection),
        }),
        column: column as i32,
        after: status(request.after),
    }
}

fn legacy_response(reply: ExecuteReply) -> Result<Response, Box<dyn Error>> {
    let sql_types = reply.sql.as_ref().map(|sql| sql.column_types.clone());
    let sql = if let Some(sql) = reply.sql {
        let rows = sql
            .rows
            .into_iter()
            .map(|row| {
                Ok(SqlRow {
                    values: row
                        .values
                        .into_iter()
                        .map(decode_value)
                        .collect::<Result<_, _>>()?,
                    version: row.version,
                })
            })
            .collect::<Result<_, ApiError>>()?;
        Some(SqlResult {
            columns: sql.columns,
            rows,
            affected: sql.affected,
        })
    } else {
        None
    };
    Ok(Response {
        version: reply.version,
        code: reply.code,
        operation: reply.operation.map(OperationId),
        count: reply.count.map(usize::try_from).transpose()?,
        digest: reply.digest,
        sql,
        sql_types,
    })
}
