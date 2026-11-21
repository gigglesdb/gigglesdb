//! Bounded delivery of rows retained in a v2 result spill.
use super::{
    codec,
    error::ApiError,
    json::{self, JsonRow},
    proto,
};
use bytes::Bytes;
use giggles_core::{
    CoreError,
    relational::{SqlRow, spill::SpilledRows},
};
use http_body::Frame;
use http_body_util::StreamBody;
use prost::Message;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use tonic::{
    Status,
    codegen::tokio_stream::{Stream, StreamExt},
};

const CHUNK: usize = 64 * 1024;

pub(crate) struct Receiver<T>(mpsc::Receiver<T>);
impl<T> Stream for Receiver<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().0.poll_recv(cx)
    }
}

fn row(row: SqlRow) -> proto::SqlRow {
    proto::SqlRow {
        values: row.values.into_iter().map(codec::encode_value).collect(),
        version: row.version,
    }
}

fn rows(reply: &mut proto::ExecuteReply) -> Option<&mut Vec<proto::SqlRow>> {
    if let Some(sql) = &mut reply.sql {
        return Some(&mut sql.rows);
    }
    match &mut reply.native_result {
        Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) => {
            Some(&mut outcome.rows)
        }
        _ => None,
    }
}

fn status(error: &CoreError) -> Status {
    let code = crate::sqlstate(error);
    let message = format!("{code}: {error}");
    if code == "53100" {
        Status::resource_exhausted(message)
    } else {
        Status::internal(message)
    }
}

/// Successful bodies preserve the unary JSON document. On a spill read error,
/// close the rows array, insert `streamError: {code, message}` in its object,
/// then fail the body (no final HTTP chunk), so buffered clients also fail.
pub(super) fn json_body(
    mut head: proto::ExecuteReply,
    spill: SpilledRows,
) -> Result<tonic::body::Body, ApiError> {
    let mut nonce = [0; 32];
    getrandom::fill(&mut nonce).map_err(|_| ApiError::Internal)?;
    let placeholder = proto::SqlRow {
        values: vec![codec::encode_value(giggles_core::relational::Value::Text(
            hex::encode(nonce),
        ))],
        version: 0,
    };
    rows(&mut head)
        .ok_or(ApiError::Internal)?
        .push(placeholder.clone());
    let encoded = json::encode_reply(&head)?;
    let marker = serde_json::to_vec(&JsonRow(&placeholder)).map_err(|_| ApiError::Internal)?;
    let mut matches = encoded
        .windows(marker.len())
        .enumerate()
        .filter(|(_, bytes)| *bytes == marker);
    let at = matches.next().ok_or(ApiError::Internal)?.0;
    if matches.next().is_some() {
        return Err(ApiError::Internal);
    }
    let prefix = encoded[..at].to_vec();
    let suffix = encoded[at + marker.len()..].to_vec();
    let (sender, receiver) = mpsc::channel(4);
    tokio::task::spawn_blocking(move || {
        let send = |bytes: Vec<u8>| {
            sender
                .blocking_send(Ok(Frame::data(Bytes::from(bytes))))
                .is_ok()
        };
        if !send(prefix) {
            return;
        }
        let mut chunk = Vec::with_capacity(CHUNK);
        let mut first = true;
        for item in spill {
            let value = match item {
                Ok(value) => value,
                Err(error) => {
                    let diagnostic = serde_json::json!({"code":crate::sqlstate(&error),"message":error.to_string()});
                    chunk.extend_from_slice(b"],\"streamError\":");
                    chunk.extend_from_slice(
                        &serde_json::to_vec(&diagnostic).expect("JSON diagnostic"),
                    );
                    chunk.extend_from_slice(&suffix[1..]);
                    if send(chunk) {
                        let _ = sender.blocking_send(Err(status(&error)));
                    }
                    return;
                }
            };
            let encoded = serde_json::to_vec(&JsonRow(&row(value))).expect("validated SQL row");
            if chunk.len() + encoded.len() + 1 > CHUNK
                && !chunk.is_empty()
                && !send(std::mem::take(&mut chunk))
            {
                return;
            }
            if !first {
                chunk.push(b',');
            }
            first = false;
            chunk.extend_from_slice(&encoded);
        }
        chunk.extend_from_slice(&suffix);
        send(chunk);
    });
    let stream = Receiver(receiver).then(|item: Result<Frame<Bytes>, Status>| async move {
        if item.is_err() {
            // Allow the preceding diagnostic DATA frame to flush before aborting.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        item
    });
    Ok(tonic::body::Body::new(StreamBody::new(stream)))
}

pub(crate) fn grpc(
    head: proto::ExecuteReply,
    spill: Option<SpilledRows>,
) -> Receiver<Result<proto::ExecuteReply, Status>> {
    let (sender, receiver) = mpsc::channel(4);
    tokio::task::spawn_blocking(move || {
        let native = head.native_result.is_some();
        if sender.blocking_send(Ok(head)).is_err() {
            return;
        }
        let Some(spill) = spill else {
            return;
        };
        let empty = || {
            let mut reply = proto::ExecuteReply::default();
            if native {
                reply.native_result = Some(proto::execute_reply::NativeResult::OperationOutcome(
                    proto::NativeOutcome::default(),
                ));
            } else {
                reply.sql = Some(proto::SqlResult::default());
            }
            reply
        };
        let mut reply = empty();
        let mut size = 0;
        for item in spill {
            let value = match item {
                Ok(value) => row(value),
                Err(error) => {
                    if size > 0 && sender.blocking_send(Ok(reply)).is_err() {
                        return;
                    }
                    let _ = sender.blocking_send(Err(status(&error)));
                    return;
                }
            };
            let len = value.encoded_len() + 8;
            if size > 0 && size + len > CHUNK - 16 {
                if sender.blocking_send(Ok(reply)).is_err() {
                    return;
                }
                reply = empty();
                size = 0;
            }
            rows(&mut reply).expect("continuation rows").push(value);
            size += len;
        }
        if size > 0 {
            let _ = sender.blocking_send(Ok(reply));
        }
    });
    Receiver(receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn temp_file_status_keeps_sqlstate_and_resource_class() {
        for (disk_full, code, expected) in [
            (false, "58030", tonic::Code::Internal),
            (true, "53100", tonic::Code::ResourceExhausted),
        ] {
            let error = status(&CoreError::TempFile {
                disk_full,
                detail: "read failed".into(),
            });
            assert_eq!(error.code(), expected);
            assert!(error.message().contains(code));
        }
    }
}
