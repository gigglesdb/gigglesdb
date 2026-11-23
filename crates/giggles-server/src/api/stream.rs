//! Bounded delivery of rows retained in a v2 result spill.
use super::{codec, error::ApiError, json::JsonRow, proto};
use crate::transport::MAX_RESPONSE;
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

fn replay_error(error: CoreError, after_commit: bool) -> CoreError {
    if after_commit {
        CoreError::Storage(error.to_string())
    } else {
        error
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

pub(super) fn sql_reply_error(error: ApiError) -> CoreError {
    CoreError::SqlReplyEncoding(match error {
        ApiError::PayloadTooLarge => "54000",
        _ => "XX000",
    })
}

fn json_rows_offset(encoded: &[u8]) -> Result<usize, ApiError> {
    // The head serializes exactly one empty rows array. Quoted strings cannot
    // contain this token unescaped.
    let key = b"\"rows\":[";
    let marker = b"\"rows\":[]";
    let mut matches = encoded
        .windows(marker.len())
        .enumerate()
        .filter(|(_, bytes)| *bytes == marker);
    let (at, _) = matches.next().ok_or(ApiError::Internal)?;
    if matches.next().is_some() {
        return Err(ApiError::Internal);
    }
    Ok(at + key.len())
}

fn json_row(value: SqlRow) -> Result<Vec<u8>, ApiError> {
    serde_json::to_vec(&JsonRow(&row(value))).map_err(|_| ApiError::Internal)
}

pub(super) fn check_json_spill(head: &[u8], spill: &mut SpilledRows) -> Result<(), CoreError> {
    json_rows_offset(head).map_err(sql_reply_error)?;
    // The chunked body has no per-row cap. Use its exact JSON row encoder.
    for item in spill {
        giggles_core::cancellation::check()?;
        json_row(item?).map_err(sql_reply_error)?;
    }
    Ok(())
}

/// Insert spilled rows into the serialized empty array. On a spill read error,
/// close the array, insert a streamError diagnostic and fail the body.
pub(super) fn json_body(
    mut encoded: Vec<u8>,
    spill: SpilledRows,
) -> Result<tonic::body::Body, ApiError> {
    let at = json_rows_offset(&encoded)?;
    let after_commit = spill.after_commit();
    let suffix = encoded.split_off(at);
    let prefix = encoded;
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
            let encoded = match item.and_then(|value| json_row(value).map_err(sql_reply_error)) {
                Ok(encoded) => encoded,
                Err(error) => {
                    let error = replay_error(error, after_commit);
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

struct GrpcRows {
    reply: proto::ExecuteReply,
    size: usize,
    native: bool,
}

impl GrpcRows {
    fn new(native: bool) -> Self {
        let mut reply = proto::ExecuteReply::default();
        if native {
            reply.native_result = Some(proto::execute_reply::NativeResult::OperationOutcome(
                proto::NativeOutcome::default(),
            ));
        } else {
            // Columns and their type OIDs belong to the head, never continuations.
            reply.sql = Some(proto::SqlResult::default());
        }
        Self {
            reply,
            size: 0,
            native,
        }
    }

    fn push(&mut self, value: SqlRow) -> Option<proto::ExecuteReply> {
        let value = row(value);
        let len = value.encoded_len() + 8;
        let full = (self.size > 0 && self.size + len > CHUNK - 16)
            .then(|| std::mem::replace(self, Self::new(self.native)).reply);
        rows(&mut self.reply)
            .expect("continuation rows")
            .push(value);
        self.size += len;
        full
    }

    fn finish(self) -> Option<proto::ExecuteReply> {
        (self.size > 0).then_some(self.reply)
    }
}

fn check_grpc_envelope(reply: &proto::ExecuteReply) -> Result<(), CoreError> {
    if reply.encoded_len() > MAX_RESPONSE {
        return Err(CoreError::SqlReplyEncoding("54000"));
    }
    Ok(())
}

pub(super) fn check_grpc_spill(spill: &mut SpilledRows) -> Result<(), CoreError> {
    let mut chunk = GrpcRows::new(false);
    for item in spill {
        giggles_core::cancellation::check()?;
        if let Some(reply) = chunk.push(item?) {
            check_grpc_envelope(&reply)?;
        }
    }
    if let Some(reply) = chunk.finish() {
        check_grpc_envelope(&reply)?;
    }
    Ok(())
}

pub(super) fn grpc(
    head: proto::ExecuteReply,
    spill: Option<SpilledRows>,
    report_committed: bool,
) -> Receiver<Result<proto::ExecuteReply, Status>> {
    protobuf_chunks(head, spill, report_committed, true)
}

pub(crate) fn spill_chunks(
    head: proto::ExecuteReply,
    spill: Option<SpilledRows>,
    report_committed: bool,
) -> Receiver<Result<proto::ExecuteReply, Status>> {
    // Native wire splits a reply across wire frames.
    protobuf_chunks(head, spill, report_committed, false)
}

fn protobuf_chunks(
    head: proto::ExecuteReply,
    spill: Option<SpilledRows>,
    report_committed: bool,
    grpc_cap: bool,
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
        let after_commit = report_committed && spill.after_commit();
        let send = |reply| {
            if grpc_cap && after_commit && let Err(error) = check_grpc_envelope(&reply) {
                let _ = sender.blocking_send(Err(status(&replay_error(error, true))));
                return false;
            }
            sender.blocking_send(Ok(reply)).is_ok()
        };
        let mut chunk = GrpcRows::new(native);
        for item in spill {
            match item {
                Ok(value) => {
                    if let Some(reply) = chunk.push(value)
                        && !send(reply)
                    {
                        return;
                    }
                }
                Err(error) => {
                    if let Some(reply) = chunk.finish()
                        && !send(reply)
                    {
                        return;
                    }
                    let _ = sender.blocking_send(Err(status(&replay_error(error, after_commit))));
                    return;
                }
            }
        }
        if let Some(reply) = chunk.finish() {
            send(reply);
        }
    });
    Receiver(receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_continuations_do_not_repeat_type_metadata() {
        let mut chunks = GrpcRows::new(false);
        let row = SqlRow {
            values: vec![giggles_core::relational::Value::Text("x".repeat(CHUNK))],
            version: 0,
        };
        assert!(chunks.push(row.clone()).is_none());
        let first = chunks.push(row).unwrap();
        for chunk in [first, chunks.finish().unwrap()] {
            let sql = chunk.sql.unwrap();
            assert_eq!(sql.rows.len(), 1);
            assert!(sql.columns.is_empty());
            assert!(sql.column_types.is_empty());
        }
    }

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
