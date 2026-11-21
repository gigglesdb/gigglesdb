//! Typed transport conversion. This module grants no identity or database authority.
use super::{error::ApiError, proto};
use giggles_core::relational::Value;
#[cfg(feature = "legacy-task")]
use giggles_core::{ApprovalId, Column, Key, OperationId, RowId, Selection, Status, TenantId};
use prost::Message;

pub(crate) mod wire_seam {
    pub(crate) use super::super::dispatch::{SpillChunks, WireCall, WireSession, spill_chunks};
}

#[derive(Debug)]
pub enum Invocation {
    Legacy(crate::Request),
    Operation {
        request: giggles_core::operations::Request,
        bindings: giggles_core::operations::Bindings,
    },
    Page {
        request: giggles_core::operations::Request,
        bindings: giggles_core::operations::Bindings,
        after: Vec<giggles_core::relational::Value>,
    },
    Transaction {
        transaction: giggles_core::operations::Transaction,
        bindings: giggles_core::operations::Bindings,
    },
    Text {
        text: String,
        bindings: giggles_core::operations::Bindings,
        language_version: giggles_language::LanguageVersion,
    },
    Catalog(Box<giggles_core::operations::CatalogRequest>),
    Sequence(giggles_core::operations::SequenceCall),
    SearchPath(Option<Vec<String>>),
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackTo(String),
    Release(String),
    Read(Box<giggles_core::relational::SingleSourceRead>),
    Join(Box<giggles_core::relational::TwoSourceRead>),
    /// GD-0061 R3-209: preview one native update (text is lowered first).
    TaskPreview {
        request: giggles_core::operations::Request,
        bindings: giggles_core::operations::Bindings,
    },
    /// Apply a stored preview under the request's `operation_id`.
    TaskApply { change: u64, digest: [u8; 32] },
    /// The stored outcome of the request's `operation_id`.
    TaskOutcome,
    /// GD-0061 R3-113: a profile 0.22 native task statement (CREATE, REVOKE,
    /// SHOW and GRANT / REVOKE forms), lowered to core's statement.
    TaskDdl(giggles_core::task::statements::TaskStatement),
}

impl Invocation {
    fn lower(self) -> Result<Self, ApiError> {
        let Self::Text {
            text,
            bindings,
            language_version,
        } = self
        else {
            return Ok(self);
        };
        // GD-0061 R3-113: task statements come first in the profiles that
        // have them; their head words are never data operations.
        if language_version.supports_tasks()
            && let Ok(statement) = giggles_language::parse_task_with_version(&text, language_version)
        {
            return Ok(Self::TaskDdl(statement));
        }
        match giggles_language::lower_text(&text, &bindings, language_version)
            .map_err(|_| ApiError::InvalidArgument)?
        {
            giggles_language::Lowered::Operation { request, bindings } => {
                Ok(Self::Operation { request, bindings })
            }
            giggles_language::Lowered::Transaction {
                transaction,
                bindings,
            } => Ok(Self::Transaction {
                transaction,
                bindings,
            }),
        }
    }
}

#[derive(Debug)]
pub struct DecodedExecute {
    pub request: Invocation,
    pub budget_ms: u32,
    /// GD-0061 R3-209: the client-chosen operation ID of a task request,
    /// charset-checked as the SQL `OPERATION '<id>'` prefix is (R3-207).
    pub operation_id: Option<crate::agent::statement::OperationId>,
}

pub fn decode_execute(mut input: proto::ExecuteRequest) -> Result<DecodedExecute, ApiError> {
    super::native_codec::validate(&input)?;
    if input.encoded_len() > crate::MAX_FRAME {
        return Err(ApiError::PayloadTooLarge);
    }
    let budget_ms = input.budget_ms.unwrap_or(10_000);
    if budget_ms > 10_000 {
        return Err(ApiError::InvalidArgument);
    }
    let operation_id = input
        .operation_id
        .take()
        .map(|id| {
            crate::agent::statement::OperationId::new(&id).ok_or(ApiError::InvalidArgument)
        })
        .transpose()?;
    let request = decode_request(input.request.ok_or(ApiError::InvalidArgument)?)?;
    // design-2 section 5: the ID is required with an update and task_apply,
    // read by task_outcome, and invalid with anything else.
    let takes_id = match &request {
        Invocation::Operation { request, .. } => {
            super::native::request_class(request) == giggles_core::task::StatementClass::Update
        }
        Invocation::TaskApply { .. } | Invocation::TaskOutcome => {
            if operation_id.is_none() {
                return Err(ApiError::InvalidArgument);
            }
            true
        }
        _ => false,
    };
    if operation_id.is_some() && !takes_id {
        return Err(ApiError::InvalidArgument);
    }
    Ok(DecodedExecute {
        request,
        budget_ms,
        operation_id,
    })
}

fn text_invocation(v: proto::NativeTextRequest) -> Result<Invocation, ApiError> {
    let language_version = v
        .language_version
        .map(|v| {
            serde_json::from_value(serde_json::Value::String(v))
                .map_err(|_| ApiError::InvalidArgument)
        })
        .transpose()?
        .unwrap_or(giggles_language::LanguageVersion::Embedded01);
    Invocation::Text {
        text: v.text,
        bindings: super::native_codec::bindings(v.bindings)?,
        language_version,
    }
    .lower()
}

fn operation_invocation(v: proto::NativeOperationRequest) -> Result<Invocation, ApiError> {
    let request = super::native_codec::request(v.request.ok_or(ApiError::InvalidArgument)?)?;
    let bindings = super::native_codec::bindings(v.bindings)?;
    Ok(match v.page {
        Some(page) => Invocation::Page {
            request,
            bindings,
            after: page.after.into_iter().map(decode_value).collect::<Result<_, _>>()?,
        },
        None => Invocation::Operation { request, bindings },
    })
}

fn decode_request(input: proto::execute_request::Request) -> Result<Invocation, ApiError> {
    use proto::execute_request::Request as P;
    let request = match input {
        P::Catalog(v) => {
            return Ok(Invocation::Catalog(Box::new(super::native_codec::catalog_request(v)?)));
        }
        P::Sequence(v) => return Ok(Invocation::Sequence(super::native_codec::sequence_call(v)?)),
        P::SearchPath(v) => {
            return Ok(Invocation::SearchPath(v.set.filter(|v| *v).map(|_| v.schemas)));
        }
        P::TypedRead(v) => return super::native_codec::typed_read(v),
        P::Text(v) => return text_invocation(v),
        P::Operation(v) => return operation_invocation(v),
        P::Transaction(v) => {
            return Ok(Invocation::Transaction {
                transaction: super::native_codec::transaction(
                    v.transaction.ok_or(ApiError::InvalidArgument)?,
                )?,
                bindings: super::native_codec::bindings(v.bindings)?,
            });
        }
        P::Savepoint(v) => return Ok(Invocation::Savepoint(v.name.ok_or(ApiError::InvalidArgument)?)),
        P::RollbackTo(v) => {
            return Ok(Invocation::RollbackTo(v.name.ok_or(ApiError::InvalidArgument)?));
        }
        P::Release(v) => return Ok(Invocation::Release(v.name.ok_or(ApiError::InvalidArgument)?)),
        P::Begin(_) => return Ok(Invocation::Begin),
        P::Commit(_) => return Ok(Invocation::Commit),
        P::Rollback(_) => return Ok(Invocation::Rollback),
        // GD-0061 R3-209: the change to preview is exactly one native
        // operation (no paging, no transaction).
        P::TaskPreview(v) => {
            use proto::task_preview_request::Change;
            let change = match v.change.ok_or(ApiError::InvalidArgument)? {
                Change::Text(v) => text_invocation(v)?,
                Change::Operation(v) => operation_invocation(v)?,
            };
            let Invocation::Operation { request, bindings } = change else {
                return Err(ApiError::InvalidArgument);
            };
            return Ok(Invocation::TaskPreview { request, bindings });
        }
        P::TaskApply(v) => {
            let digest = v.digest.ok_or(ApiError::InvalidArgument)?;
            return Ok(Invocation::TaskApply {
                change: v.change_id.ok_or(ApiError::InvalidArgument)?,
                digest: digest.try_into().map_err(|_| ApiError::InvalidArgument)?,
            });
        }
        P::TaskOutcome(proto::TaskOutcomeRequest {}) => return Ok(Invocation::TaskOutcome),
        P::Ping(_) => crate::Request::Ping,
        P::Sql(sql) => crate::Request::Sql {
            sql: sql.sql,
            params: sql
                .params
                .into_iter()
                .map(decode_value)
                .collect::<Result<_, _>>()?,
        },
        #[cfg(feature = "legacy-task")]
        P::Preview(p) => crate::Request::Preview {
            request: decode_task(p.request.ok_or(ApiError::InvalidArgument)?)?,
        },
        #[cfg(feature = "legacy-task")]
        P::Execute(p) => crate::Request::Execute {
            request: decode_task(p.request.ok_or(ApiError::InvalidArgument)?)?,
            approval: p.approval.map(ApprovalId),
        },
        #[cfg(feature = "legacy-task")]
        P::RepairSql(p) => crate::Request::RepairSql {
            operation: OperationId(p.operation.ok_or(ApiError::InvalidArgument)?),
            sql: p.sql,
            approval: p.approval.map(ApprovalId),
        },
        #[cfg(feature = "legacy-task")]
        P::Status(p) => crate::Request::Status {
            operation: OperationId(p.operation.ok_or(ApiError::InvalidArgument)?),
        },
    };
    Ok(Invocation::Legacy(request))
}

pub fn decode_value(value: proto::Value) -> Result<Value, ApiError> {
    use proto::value::Value as P;
    Ok(match value.value.ok_or(ApiError::InvalidArgument)? {
        P::NullValue(0) => Value::Null,
        P::NullValue(_) => return Err(ApiError::InvalidArgument),
        P::BoolValue(v) => Value::Bool(v),
        P::IntValue(v) => Value::Int(v),
        P::TextValue(v) => Value::Text(v),
        P::BytesValue(v) => Value::Bytes(v),
        P::NumericValue(v) => Value::Numeric(
            giggles_pg_types::Numeric::parse(&v).map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::Float4Bits(v) => Value::Float4(giggles_pg_types::Float4::from_bits(v)),
        P::DateBinary(v) => Value::Date(
            giggles_core::relational::Date::from_binary(&v)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::TimeBinary(v) => Value::Time(
            giggles_core::relational::PgTime::from_binary(&v)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::TimetzBinary(v) => Value::TimeTz(
            giggles_core::relational::TimeTz::from_binary(&v)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::TimestampBinary(v) => Value::Timestamp(
            giggles_core::relational::Timestamp::from_binary(&v)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::TimestamptzBinary(v) => Value::TimestampTz(
            giggles_core::relational::TimestampTz::from_binary(&v)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::IntervalBinary(v) => Value::Interval(
            giggles_core::relational::Interval::from_binary(&v)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        P::Float8Bits(v) => Value::Float8(giggles_pg_types::Float8::from_bits(v)),
    })
}

#[cfg(feature = "legacy-task")]
fn decode_status(value: i32) -> Result<Status, ApiError> {
    match proto::DeliveryStatus::try_from(value) {
        Ok(proto::DeliveryStatus::Failed) => Ok(Status::Failed),
        Ok(proto::DeliveryStatus::Queued) => Ok(Status::Queued),
        Ok(proto::DeliveryStatus::Succeeded) => Ok(Status::Succeeded),
        _ => Err(ApiError::InvalidArgument),
    }
}

#[cfg(feature = "legacy-task")]
fn decode_task(input: proto::TaskRequest) -> Result<giggles_core::Request, ApiError> {
    let column = match proto::Column::try_from(input.column) {
        Ok(proto::Column::Status) => Column::Status,
        Ok(proto::Column::FailedAt) => Column::FailedAt,
        Ok(proto::Column::AttemptCount) => Column::AttemptCount,
        _ => return Err(ApiError::InvalidArgument),
    };
    let selection = input
        .selection
        .and_then(|s| s.selection)
        .ok_or(ApiError::InvalidArgument)?;
    let selection = match selection {
        proto::selection::Selection::Keys(keys) => Selection::Keys(
            keys.items
                .into_iter()
                .map(|key| {
                    Ok(Key {
                        tenant: TenantId(key.tenant.ok_or(ApiError::InvalidArgument)?),
                        row: RowId(key.row.ok_or(ApiError::InvalidArgument)?),
                    })
                })
                .collect::<Result<_, ApiError>>()?,
        ),
        proto::selection::Selection::Predicate(p) => Selection::Predicate {
            tenant: TenantId(p.tenant.ok_or(ApiError::InvalidArgument)?),
            status: decode_status(p.status)?,
            from: p.from.ok_or(ApiError::InvalidArgument)?,
            until: p.until.ok_or(ApiError::InvalidArgument)?,
        },
    };
    Ok(giggles_core::Request {
        operation: OperationId(input.operation.ok_or(ApiError::InvalidArgument)?),
        selection,
        column,
        after: decode_status(input.after)?,
    })
}

pub fn encode_value(value: Value) -> proto::Value {
    use proto::value::Value as P;
    proto::Value {
        value: Some(match value {
            Value::Null => P::NullValue(0),
            Value::Bool(v) => P::BoolValue(v),
            Value::Int(v) => P::IntValue(v),
            Value::Text(v) => P::TextValue(v),
            Value::Bytes(v) => P::BytesValue(v),
            Value::Numeric(v) => P::NumericValue(v.to_string()),
            Value::Float4(v) => P::Float4Bits(v.to_bits()),
            Value::Date(v) => P::DateBinary(v.to_binary().to_vec()),
            Value::Time(v) => P::TimeBinary(v.to_binary().to_vec()),
            Value::TimeTz(v) => P::TimetzBinary(v.to_binary().to_vec()),
            Value::Timestamp(v) => P::TimestampBinary(v.to_binary().to_vec()),
            Value::TimestampTz(v) => P::TimestamptzBinary(v.to_binary().to_vec()),
            Value::Interval(v) => P::IntervalBinary(v.to_binary().to_vec()),
            Value::Float8(v) => P::Float8Bits(v.to_bits()),
        }),
    }
}

pub fn encode_reply(
    response: crate::Response,
    transaction_state: proto::TransactionState,
) -> Result<proto::ExecuteReply, ApiError> {
    if transaction_state == proto::TransactionState::Unspecified {
        return Err(ApiError::Internal);
    }
    Ok(proto::ExecuteReply {
        native_result: None,
        version: response.version,
        code: response.code,
        operation: response.operation.map(|id| id.0),
        count: response
            .count
            .map(u64::try_from)
            .transpose()
            .map_err(|_| ApiError::Internal)?,
        digest: response.digest,
        sql: response.sql.map(|sql| proto::SqlResult {
            columns: sql.columns,
            rows: sql
                .rows
                .into_iter()
                .map(|row| proto::SqlRow {
                    values: row.values.into_iter().map(encode_value).collect(),
                    version: row.version,
                })
                .collect(),
            affected: sql.affected,
        }),
        transaction_state: transaction_state as i32,
        task_error: None,
        message: None,
    })
}

/// The reply to an error (GD-0061 R3-209, design-2 section 6). `code` is the
/// SQLSTATE; a task refusal also names itself in `task_error` and carries its
/// fixed `message` (`TaskError::name`, `TaskError::message`: no values, no
/// keys, identical to the SQL surface's text). A pass-through error has neither.
pub fn encode_error(
    error: &giggles_core::CoreError,
    transaction_state: proto::TransactionState,
) -> Result<proto::ExecuteReply, ApiError> {
    let mut reply = encode_reply(crate::Response::error(error), transaction_state)?;
    if let giggles_core::CoreError::Task(error) = error
        && let Some(name) = error.name()
    {
        reply.task_error = Some(name.into());
        reply.message = Some(error.message().into());
    }
    Ok(reply)
}

/// The encoded-length check precedes allocation. No gRPC frame prefix is added.
pub fn encode_protobuf(message: &impl Message, max: usize) -> Result<Vec<u8>, ApiError> {
    let len = message.encoded_len();
    if len > max {
        return Err(ApiError::PayloadTooLarge);
    }
    let mut bytes = Vec::with_capacity(len);
    message.encode(&mut bytes).map_err(|_| ApiError::Internal)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "legacy-task"))]
    #[test]
    fn retired_task_wire_fields_decode_as_missing_request() {
        for field in 3..=6 {
            let wire = [(field << 3) | 2, 0];
            let request = proto::ExecuteRequest::decode(wire.as_slice()).unwrap();
            assert!(request.request.is_none());
            assert!(matches!(decode_execute(request), Err(ApiError::InvalidArgument)));
        }
    }

    #[test]
    fn value_extremes_round_trip() {
        for value in [
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Int(0),
            Value::Text("\0\n雪".into()),
            Value::Text(String::new()),
            Value::Bytes(vec![0, 255, 128]),
            Value::Bytes(vec![]),
        ] {
            let encoded = encode_value(value.clone());
            let wire = encode_protobuf(&encoded, 1024).unwrap();
            assert_eq!(
                decode_value(proto::Value::decode(wire.as_slice()).unwrap()).unwrap(),
                value
            );
        }
    }





    #[test]
    fn denied_reply_fields_are_not_reconstructed() {
        let reply = encode_reply(
            crate::Response::code("42501"),
            proto::TransactionState::Idle,
        )
        .unwrap();
        assert_eq!(reply.code, "42501");
        assert_eq!(
            (reply.operation, reply.count, reply.digest, reply.sql),
            (None, None, None, None)
        );
    }

    #[test]
    fn protobuf_encoded_length_cap() {
        let message = proto::SqlRequest {
            sql: "SELECT".into(),
            params: vec![],
        };
        let n = message.encoded_len();
        assert_eq!(encode_protobuf(&message, n).unwrap().len(), n);
        assert_eq!(
            encode_protobuf(&message, n - 1),
            Err(ApiError::PayloadTooLarge)
        );
        assert!(
            encode_protobuf(&proto::OpenSessionRequest::default(), 0)
                .unwrap()
                .is_empty()
        );
        let message = proto::SqlRequest {
            // A string tag and a four-byte length varint add five bytes.
            sql: "x".repeat(crate::MAX_RESPONSE - 5),
            params: vec![],
        };
        assert_eq!(message.encoded_len(), crate::MAX_RESPONSE);
        assert_eq!(
            encode_protobuf(&message, crate::MAX_RESPONSE)
                .unwrap()
                .len(),
            crate::MAX_RESPONSE
        );
        let over = proto::SqlRequest {
            sql: format!("{}x", message.sql),
            params: vec![],
        };
        assert_eq!(
            encode_protobuf(&over, crate::MAX_RESPONSE),
            Err(ApiError::PayloadTooLarge)
        );
    }

    #[test]
    fn protobuf_unknown_and_malformed_fields_follow_contract() {
        // Unknown length-delimited field 127 does not grant a recognized request.
        // Field 7 is now the native operation arm.
        let unknown = proto::ExecuteRequest::decode(&[0xfa, 0x07, 0][..]).unwrap();
        assert!(decode_execute(unknown).is_err());
        // Recognized Ping plus unknown field remains Ping under protobuf rules.
        let known = proto::ExecuteRequest::decode(&[0x0a, 0, 0xfa, 0x07, 0][..]).unwrap();
        assert!(matches!(
            decode_execute(known).unwrap().request,
            Invocation::Legacy(crate::Request::Ping)
        ));
        for invalid in [&[0x0a, 1][..], &[0][..], &[0x12, 3, 0x0a, 1, 0xff][..]] {
            assert!(proto::ExecuteRequest::decode(invalid).is_err());
        }
    }

    #[test]
    fn protobuf_request_cap_is_exact() {
        // Two message/string tags and two three-byte length varints.
        let mut request = proto::ExecuteRequest {
            request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
                sql: "x".repeat(crate::MAX_FRAME - 8),
                params: vec![],
            })),
            budget_ms: None, operation_id: None,
        };
        assert_eq!(request.encoded_len(), crate::MAX_FRAME);
        assert!(decode_execute(request.clone()).is_ok());
        let Some(proto::execute_request::Request::Sql(sql)) = request.request.as_mut() else {
            unreachable!()
        };
        sql.sql.push('x');
        assert_eq!(request.encoded_len(), crate::MAX_FRAME + 1);
        assert!(matches!(
            decode_execute(request),
            Err(ApiError::PayloadTooLarge)
        ));
    }




    #[cfg(feature = "legacy-task")]
    #[test]
    fn duplicate_task_keys_are_preserved() {
        let key = proto::Key {
            tenant: Some(0),
            row: Some(u64::MAX),
        };
        let req = decode_task(proto::TaskRequest {
            operation: Some(0),
            selection: Some(proto::Selection {
                selection: Some(proto::selection::Selection::Keys(proto::KeyList {
                    items: vec![key, key],
                })),
            }),
            column: proto::Column::Status as i32,
            after: proto::DeliveryStatus::Queued as i32,
        })
        .unwrap();
        assert_eq!(req.operation, OperationId(0));
        assert_eq!(
            req.selection,
            Selection::Keys(vec![
                Key {
                    tenant: TenantId(0),
                    row: RowId(u64::MAX)
                };
                2
            ])
        );
    }

    #[cfg(feature = "legacy-task")]
    #[test]
    fn optional_zero_approval_and_budget_are_distinct_from_absence() {
        for approval in [None, Some(0), Some(u64::MAX)] {
            for budget_ms in [None, Some(0), Some(10_000)] {
                let request = proto::ExecuteRequest {
                    request: Some(proto::execute_request::Request::RepairSql(
                        proto::RepairSqlRequest {
                            operation: Some(0),
                            sql: "unchanged".into(),
                            approval,
                        },
                    )),
                    budget_ms, operation_id: None,
                };
                let decoded = decode_execute(request).unwrap();
                assert_eq!(decoded.budget_ms, budget_ms.unwrap_or(10_000));
                assert!(
                    matches!(decoded.request, Invocation::Legacy(crate::Request::RepairSql { approval: got, .. }) if got.map(|a| a.0) == approval)
                );
            }
        }
        assert!(
            decode_execute(proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
                budget_ms: Some(10_001), operation_id: None,
            })
            .is_err()
        );
    }

    #[test]
    fn required_presence_and_unknown_enums_reject() {
        assert_eq!(
            decode_value(proto::Value::default()),
            Err(ApiError::InvalidArgument)
        );
        assert_eq!(
            decode_value(proto::Value {
                value: Some(proto::value::Value::NullValue(7))
            }),
            Err(ApiError::InvalidArgument)
        );
        assert!(decode_execute(proto::ExecuteRequest::default()).is_err());
        #[cfg(feature = "legacy-task")]
        for operation in [None, Some(0), Some(u64::MAX)] {
            let req = proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::Status(
                    proto::StatusRequest { operation },
                )),
                budget_ms: Some(0), operation_id: None,
            };
            let got = decode_execute(req);
            if let Some(id) = operation {
                let got = got.unwrap();
                assert_eq!(got.budget_ms, 0);
                assert!(
                    matches!(got.request, Invocation::Legacy(crate::Request::Status { operation }) if operation.0 == id)
                );
            } else {
                assert!(got.is_err());
            }
        }
        #[cfg(feature = "legacy-task")]
        for value in [0, -1, i32::MAX] {
            assert!(decode_status(value).is_err());
        }
    }

    #[cfg(feature = "legacy-task")]
    #[test]
    fn task_required_presence_and_every_business_enum_are_checked() {
        let valid = proto::TaskRequest {
            operation: Some(0),
            selection: Some(proto::Selection {
                selection: Some(proto::selection::Selection::Predicate(proto::Predicate {
                    tenant: Some(0),
                    status: proto::DeliveryStatus::Failed as i32,
                    from: Some(0),
                    until: Some(u64::MAX),
                })),
            }),
            column: proto::Column::Status as i32,
            after: proto::DeliveryStatus::Queued as i32,
        };
        for (wire_column, native_column) in [
            (proto::Column::Status, Column::Status),
            (proto::Column::FailedAt, Column::FailedAt),
            (proto::Column::AttemptCount, Column::AttemptCount),
        ] {
            for (wire_status, native_status) in [
                (proto::DeliveryStatus::Failed, Status::Failed),
                (proto::DeliveryStatus::Queued, Status::Queued),
                (proto::DeliveryStatus::Succeeded, Status::Succeeded),
            ] {
                let mut req = valid.clone();
                req.column = wire_column as i32;
                req.after = wire_status as i32;
                let got = decode_task(req).unwrap();
                assert_eq!(got.column, native_column);
                assert_eq!(got.after, native_status);
            }
        }
        for missing in 0..7 {
            let mut req = valid.clone();
            match missing {
                0 => req.operation = None,
                1 => req.selection = None,
                2 => req.selection.as_mut().unwrap().selection = None,
                _ => {
                    let Some(proto::selection::Selection::Predicate(p)) =
                        &mut req.selection.as_mut().unwrap().selection
                    else {
                        unreachable!()
                    };
                    match missing {
                        3 => p.tenant = None,
                        4 => p.from = None,
                        5 => p.until = None,
                        _ => p.status = 0,
                    }
                }
            }
            assert!(decode_task(req).is_err(), "missing field {missing}");
        }
        for invalid in [0, -1, i32::MAX] {
            let mut req = valid.clone();
            req.column = invalid;
            assert!(decode_task(req).is_err());
            let mut req = valid.clone();
            req.after = invalid;
            assert!(decode_task(req).is_err());
        }
    }

}
