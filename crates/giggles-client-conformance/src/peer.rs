use crate::{HarnessError, Scenario};
use giggles_consensus::{TaskAction, native};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
use tokio_rustls::TlsAcceptor;

/// Counts only peer observations, never inferred client sends.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Counters {
    pub connections: u64,
    pub authenticated: u64,
    pub request_starts: u64,
    pub complete_execute: BTreeMap<u64, u64>,
    pub complete_status: BTreeMap<u64, u64>,
    pub events: Vec<String>,
}
pub(crate) type Shared = Arc<Mutex<Counters>>;
pub(crate) fn event(state: &Shared, name: &str) {
    state.lock().expect("counter lock").events.push(name.into());
}

async fn bounded_body<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    size: usize,
) -> Result<Vec<u8>, HarnessError> {
    if size == 0 || size > native::MAX_FRAME {
        return Err(HarnessError::Frame);
    }
    let mut bytes = vec![0; size];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

pub(crate) async fn serve(
    listener: TcpListener,
    server: Arc<rustls::ServerConfig>,
    scenario: Scenario,
    seed: u64,
    state: Shared,
    reached: Arc<Notify>,
) -> Result<(), HarnessError> {
    loop {
        let (tcp, _) = listener.accept().await?;
        state.lock().expect("counter lock").connections += 1;
        if scenario == Scenario::TlsDeadline {
            event(&state, "tcp_accepted_tls_not_started");
            reached.notify_one();
            let mut tcp = tcp;
            let mut bytes = [0; 4096];
            while tcp.read(&mut bytes).await? != 0 {}
            continue;
        }
        let Ok(mut tls) = TlsAcceptor::from(server.clone()).accept(tcp).await else {
            event(&state, "tls_rejected");
            continue;
        };
        state.lock().expect("counter lock").authenticated += 1;
        event(&state, "mtls_authenticated");
        if scenario == Scenario::WritePressure {
            event(&state, "no_application_read");
            reached.notify_one();
            // A deliberate no-read interval. This cannot prove client write-stage timing.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }
        if matches!(
            scenario,
            Scenario::Prefix1 | Scenario::Prefix2 | Scenario::Prefix3
        ) {
            let n = match scenario {
                Scenario::Prefix1 => 1,
                Scenario::Prefix2 => 2,
                _ => 3,
            };
            let mut b = vec![0; n];
            if tls.read_exact(&mut b).await.is_ok() {
                state.lock().expect("counter lock").request_starts += 1;
                event(&state, &format!("request_prefix_consumed_{n}"));
            }
            continue;
        }
        let Ok(size) = tls.read_u32().await else {
            continue;
        };
        state.lock().expect("counter lock").request_starts += 1;
        if size == 0 || size as usize > native::MAX_FRAME {
            return Err(HarnessError::Frame);
        }
        if scenario == Scenario::BodyClose {
            let n = (1 + seed as usize % 11).min(size as usize - 1);
            let mut b = vec![0; n];
            tls.read_exact(&mut b).await?;
            event(&state, &format!("request_body_consumed_{n}_of_{size}"));
            continue;
        }
        let Ok(bytes) = bounded_body(&mut tls, size as usize).await else {
            continue;
        };
        let request = native::parse(&bytes).map_err(|_| HarnessError::Frame)?;
        let (id, is_status) = match request.action {
            TaskAction::Execute(r, _) => (r.operation.0, false),
            TaskAction::Status(id) => (id.0, true),
            _ => return Err(HarnessError::Frame),
        };
        let attempt = {
            let mut c = state.lock().expect("counter lock");
            let map = if is_status {
                &mut c.complete_status
            } else {
                &mut c.complete_execute
            };
            let count = map.entry(id).or_default();
            *count += 1;
            *count
        };
        event(
            &state,
            if is_status {
                "complete_status"
            } else {
                "complete_execute"
            },
        );
        reached.notify_one();
        if scenario == Scenario::CompleteClose
            || (scenario == Scenario::LostReplyStatus && !is_status)
        {
            continue;
        }
        if matches!(
            scenario,
            Scenario::ReadDeadline
                | Scenario::AdmissionDeadline
                | Scenario::CancelActive
                | Scenario::CancelAdmission
        ) && attempt == 1
            && !is_status
        {
            event(&state, "waiting_after_complete_execute");
            let mut b = [0; 1];
            let _ = tls.read(&mut b).await;
            continue;
        }
        let code = if scenario == Scenario::AbsentThenCommitted && attempt == 1 {
            "absent"
        } else {
            "committed"
        };
        let mut response = serde_json::json!({"version":1,"id":request.id,"error":null,"reply":{"version":1,"code":0,"data":{"version":1,"code":code,"operation":id}}});
        match scenario {
            Scenario::OuterId => response["id"] = serde_json::json!(request.id.wrapping_add(1)),
            Scenario::OuterVersion => response["version"] = serde_json::json!(2),
            Scenario::ReplyVersion => response["reply"]["version"] = serde_json::json!(2),
            Scenario::InnerId | Scenario::StatusInnerId => {
                response["reply"]["data"]["operation"] = serde_json::json!(id.wrapping_add(1))
            }
            Scenario::InnerVersion | Scenario::StatusInnerVersion => {
                response["reply"]["data"]["version"] = serde_json::json!(2)
            }
            Scenario::Both => response["error"] = serde_json::json!("indeterminate"),
            Scenario::Neither => response["reply"] = serde_json::Value::Null,
            Scenario::Unavailable | Scenario::Indeterminate => {
                response["reply"] = serde_json::Value::Null;
                response["error"] = serde_json::json!(if scenario == Scenario::Unavailable {
                    "unavailable"
                } else {
                    "indeterminate"
                });
            }
            _ => {}
        }
        let reply = serde_json::to_vec(&response).map_err(|_| HarnessError::Frame)?;
        let send: Result<(), std::io::Error> = async {
            match scenario {
                Scenario::ZeroLength => tls.write_u32(0).await?,
                Scenario::OversizeLength => tls.write_u32(u32::MAX).await?,
                Scenario::InvalidJson => {
                    tls.write_u32(1).await?;
                    tls.write_all(b"{").await?;
                }
                Scenario::ReplyPrefix1 | Scenario::ReplyPrefix2 | Scenario::ReplyPrefix3 => {
                    let n = match scenario {
                        Scenario::ReplyPrefix1 => 1,
                        Scenario::ReplyPrefix2 => 2,
                        _ => 3,
                    };
                    tls.write_all(&(reply.len() as u32).to_be_bytes()[..n])
                        .await?;
                }
                Scenario::ReplyBody => {
                    tls.write_u32(reply.len() as u32).await?;
                    tls.write_all(&reply[..reply.len() / 2]).await?;
                }
                _ => native::write_frame(&mut tls, &reply)
                    .await
                    .map_err(|_| std::io::Error::other("scripted write"))?,
            }
            tls.flush().await
        }
        .await;
        event(
            &state,
            if send.is_ok() {
                "scripted_response_written"
            } else {
                "peer_write_failed"
            },
        );
        if send.is_ok() {
            event(
                &state,
                match scenario {
                    Scenario::ReplyPrefix1 => "reply_prefix_written_1",
                    Scenario::ReplyPrefix2 => "reply_prefix_written_2",
                    Scenario::ReplyPrefix3 => "reply_prefix_written_3",
                    Scenario::ReplyBody => "reply_partial_body_written",
                    _ => "reply_script_boundary_reached",
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn framing_rejects_zero_oversize_and_truncation() {
        for bytes in [
            0u32.to_be_bytes().to_vec(),
            u32::MAX.to_be_bytes().to_vec(),
            vec![0, 0, 0, 2, b'{'],
        ] {
            let mut reader = bytes.as_slice();
            let size = reader.read_u32().await.unwrap() as usize;
            assert!(bounded_body(&mut reader, size).await.is_err());
        }
        let bytes = [0, 0, 0, 2, b'{', b'}'];
        let mut reader = bytes.as_slice();
        let size = reader.read_u32().await.unwrap() as usize;
        assert_eq!(bounded_body(&mut reader, size).await.unwrap(), b"{}");
    }
}
