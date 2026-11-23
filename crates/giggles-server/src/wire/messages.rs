//! Protobuf message bodies for native wire frames.

pub use crate::api::proto::{
    WireBatch, WireBatchEntry, WireBatchMode, WireCancel, WireCloseHandle, WireConnError,
    WirePrepare, WirePrepared, WireReady, WireReply, WireRun, WireShapeRef, wire_batch_entry,
};

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::api::proto::{
        ExecuteReply, ExecuteRequest, NativeBinding, OpenSessionReply, PingRequest, SqlRequest,
        SqlResult, SqlRow, TransactionState, Value, execute_request, value,
    };
    use prost::Message;

    use super::{
        WireBatch, WireBatchEntry, WireBatchMode, WireCancel, WireCloseHandle, WireConnError,
        WirePrepare, WirePrepared, WireReady, WireReply, WireRun, WireShapeRef, wire_batch_entry,
    };

    fn assert_round_trip<M: Message + Default + PartialEq + Debug>(message: &M) {
        let encoded = message.encode_to_vec();
        assert_eq!(M::decode(encoded.as_slice()).unwrap(), *message);
    }

    fn integer(value: i64) -> Value {
        Value {
            value: Some(value::Value::IntValue(value)),
        }
    }

    #[test]
    fn wire_messages_round_trip() {
        assert_round_trip(&WireReady {
            session: Some(OpenSessionReply {
                session_id: "session-1".into(),
                idle_timeout_seconds: 60,
                max_age_seconds: 3600,
                transaction_timeout_seconds: 30,
                scram_server_final: Some("v=server-signature".into()),
                ..Default::default()
            }),
            max_in_flight: 16,
            max_frame: 65_536,
            max_handles: 64,
            max_shapes: 128,
        });
        assert_round_trip(&WirePrepare {
            text: "select id from widgets".into(),
            language_version: Some("1".into()),
        });
        assert_round_trip(&WirePrepared { handle: 7 });
        let run = WireRun {
            handle: 7,
            bindings: vec![NativeBinding {
                name: Some("id".into()),
                value: Some(integer(42)),
            }],
            budget_ms: Some(200),
            operation_id: Some("run-1".into()),
        };
        assert_round_trip(&run);
        assert_round_trip(&WireCloseHandle { handle: 7 });

        let execute_entry = WireBatchEntry {
            request_id: 11,
            entry: Some(wire_batch_entry::Entry::Execute(ExecuteRequest {
                request: Some(execute_request::Request::Sql(SqlRequest {
                    sql: "SELECT $1::bigint AS id".into(),
                    params: vec![integer(42)],
                })),
                budget_ms: Some(100),
                ..Default::default()
            })),
        };
        let run_entry = WireBatchEntry {
            request_id: 12,
            entry: Some(wire_batch_entry::Entry::Run(run)),
        };
        assert_round_trip(&execute_entry);
        assert_round_trip(&run_entry);
        assert_round_trip(&WireBatch {
            mode: WireBatchMode::StopOnError as i32,
            entries: vec![execute_entry, run_entry],
        });

        let registered_shape = WireShapeRef {
            position: 0,
            shape_id: Some(9),
            columns: vec!["id".into()],
        };
        let inline_shape = WireShapeRef {
            position: 1,
            shape_id: None,
            columns: vec!["name".into()],
        };
        assert_round_trip(&registered_shape);
        assert_round_trip(&inline_shape);
        assert_round_trip(&WireReply {
            reply: Some(ExecuteReply {
                version: 1,
                code: "00000".into(),
                count: Some(1),
                sql: Some(SqlResult {
                    columns: vec!["id".into()],
                    rows: vec![SqlRow {
                        values: vec![integer(42)],
                        version: 1,
                    }],
                    affected: 0,
                    column_types: vec![23],
                }),
                transaction_state: TransactionState::Idle as i32,
                ..Default::default()
            }),
            shapes: vec![registered_shape, inline_shape],
        });
        assert_round_trip(&WireCancel {
            target_request_id: 12,
        });
        assert_round_trip(&WireConnError {
            code: "UNSUPPORTED_VERSION".into(),
            message: "Unsupported wire version".into(),
            supported_major: vec![1, 2],
        });
    }

    #[test]
    fn wire_messages_survive_legacy_strip() {
        assert_round_trip(&WireReady {
            session: Some(OpenSessionReply {
                session_id: "wire-session".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_round_trip(&WireBatchEntry {
            request_id: 1,
            entry: Some(wire_batch_entry::Entry::Execute(ExecuteRequest {
                request: Some(execute_request::Request::Ping(PingRequest {})),
                ..Default::default()
            })),
        });
        assert_round_trip(&WireReply {
            reply: Some(ExecuteReply {
                version: 1,
                code: "00000".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_round_trip(&WireRun {
            handle: 1,
            bindings: vec![NativeBinding {
                name: Some("limit".into()),
                value: Some(integer(3)),
            }],
            ..Default::default()
        });
    }
}
