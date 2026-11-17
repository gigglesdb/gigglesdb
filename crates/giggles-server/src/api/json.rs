//! The deliberately strict, lossless JSON profile of the v1 protobuf contract.
use super::{error::ApiError, proto};
use base64::{Engine, display::Base64Display, engine::general_purpose::STANDARD};
use serde::{
    Serialize, Serializer,
    de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor},
    ser::{Error as _, SerializeMap, SerializeSeq},
};
use std::{collections::BTreeMap, fmt, io};

const MAX_DEPTH: usize = 128;

// This private AST is constructed only by the strict visitor below. It rejects
// duplicate names before insertion, and never creates permissive serde_json::Value.
pub(super) enum Json {
    Object(BTreeMap<String, Json>),
    Array(Vec<Json>),
    String(String),
    Unsigned(u64),
    Bool(bool),
}

struct Seed(usize);
impl<'de> DeserializeSeed<'de> for Seed {
    type Value = Json;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Json, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed {
    type Value = Json;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a strict v1 JSON value")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        if self.0 >= MAX_DEPTH {
            return Err(A::Error::custom("nesting limit"));
        }
        let mut fields = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(A::Error::custom("duplicate field"));
            }
            fields.insert(key, map.next_value_seed(Seed(self.0 + 1))?);
        }
        Ok(Json::Object(fields))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
        if self.0 >= MAX_DEPTH {
            return Err(A::Error::custom("nesting limit"));
        }
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(Seed(self.0 + 1))? {
            values.push(value);
        }
        Ok(Json::Array(values))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Json, E> {
        Ok(Json::String(value.to_owned()))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Json, E> {
        Ok(Json::String(value))
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Json, E> {
        Ok(Json::Unsigned(value))
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Json, E> {
        Err(E::custom("unsigned integer required"))
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Json, E> {
        Err(E::custom("integer token required"))
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Json, E> {
        Ok(Json::Bool(value))
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Json, E> {
        Err(E::custom("null is not a present v1 value"))
    }
}

pub(super) struct Object(BTreeMap<String, Json>);
impl Object {
    pub(super) fn new(value: Json) -> Result<Self, ApiError> {
        match value {
            Json::Object(fields) => Ok(Self(fields)),
            _ => Err(ApiError::InvalidArgument),
        }
    }
    pub(super) fn optional(&mut self, name: &str) -> Option<Json> {
        self.0.remove(name)
    }
    pub(super) fn required(&mut self, name: &str) -> Result<Json, ApiError> {
        self.optional(name).ok_or(ApiError::InvalidArgument)
    }
    pub(super) fn finish(self) -> Result<(), ApiError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ApiError::InvalidArgument)
        }
    }
    pub(super) fn one(mut self) -> Result<(String, Json), ApiError> {
        if self.0.len() != 1 {
            return Err(ApiError::InvalidArgument);
        }
        self.0.pop_first().ok_or(ApiError::InvalidArgument)
    }
}
fn parse(bytes: &[u8]) -> Result<Json, ApiError> {
    if bytes.len() > crate::MAX_FRAME {
        return Err(ApiError::PayloadTooLarge);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    // The visitor below enforces our exact 128-container bound before descent.
    deserializer.disable_recursion_limit();
    let value = Seed(0)
        .deserialize(&mut deserializer)
        .map_err(|_| ApiError::InvalidArgument)?;
    deserializer.end().map_err(|_| ApiError::InvalidArgument)?;
    Ok(value)
}
pub(super) fn text(value: Json) -> Result<String, ApiError> {
    if let Json::String(value) = value {
        Ok(value)
    } else {
        Err(ApiError::InvalidArgument)
    }
}
pub(super) fn array(value: Json) -> Result<Vec<Json>, ApiError> {
    if let Json::Array(value) = value {
        Ok(value)
    } else {
        Err(ApiError::InvalidArgument)
    }
}
fn uint(value: Json) -> Result<u64, ApiError> {
    let value = text(value)?;
    let parsed: u64 = value.parse().map_err(|_| ApiError::InvalidArgument)?;
    if parsed.to_string() == value {
        Ok(parsed)
    } else {
        Err(ApiError::InvalidArgument)
    }
}
pub(super) fn int(value: Json) -> Result<i64, ApiError> {
    let value = text(value)?;
    let parsed: i64 = value.parse().map_err(|_| ApiError::InvalidArgument)?;
    if parsed.to_string() == value {
        Ok(parsed)
    } else {
        Err(ApiError::InvalidArgument)
    }
}
fn uint32(value: Json) -> Result<u32, ApiError> {
    if let Json::Unsigned(value) = value {
        u32::try_from(value).map_err(|_| ApiError::InvalidArgument)
    } else {
        Err(ApiError::InvalidArgument)
    }
}
pub(super) fn empty(value: Json) -> Result<(), ApiError> {
    Object::new(value)?.finish()
}

pub fn decode_empty(bytes: &[u8]) -> Result<(), ApiError> {
    empty(parse(bytes)?)
}
/// Decode and validate a binding or SQL-role sign-in request.
pub fn decode_open(bytes: &[u8]) -> Result<proto::OpenSessionRequest, ApiError> {
    let mut root = Object::new(parse(bytes)?)?;
    let task = root.optional("task").map(text).transpose()?;
    let role = root.optional("role").map(text).transpose()?;
    let database = root.optional("database").map(text).transpose()?;
    let password = root.optional("password").map(text).transpose()?;
    let scram = root.optional("scram");
    if password.is_some() && scram.is_some() {
        return Err(ApiError::InvalidArgument);
    }
    use proto::open_session_request::Credential as C;
    let credential = if let Some(password) = password {
        Some(C::Password(password))
    } else if let Some(scram) = scram {
        let mut scram = Object::new(scram)?;
        let credential = if let Some(first) = scram.optional("clientFirst") {
            C::ScramStart(proto::ScramStart {
                client_first: text(first)?,
            })
        } else {
            C::ScramContinue(proto::ScramContinue {
                exchange: text(scram.required("exchange")?)?,
                client_final: text(scram.required("clientFinal")?)?,
            })
        };
        scram.finish()?;
        Some(credential)
    } else {
        None
    };
    root.finish()?;
    let request = proto::OpenSessionRequest {
        task,
        role,
        database,
        credential,
    };
    super::open::classify_open(&request)?;
    Ok(request)
}

pub fn decode_execute(bytes: &[u8]) -> Result<proto::ExecuteRequest, ApiError> {
    let mut root = Object::new(parse(bytes)?)?;
    let budget_ms = root.optional("budgetMs").map(uint32).transpose()?;
    let (kind, value) = root.one()?;
    use proto::execute_request::Request as P;
    let request = match kind.as_str() {
        "catalog" => P::Catalog(super::native_json::Decoder::default().catalog(value)?),
        "sequence" => P::Sequence(super::native_json::Decoder::default().sequence(value)?),
        "searchPath" => P::SearchPath(super::native_json::Decoder::default().search_path(value)?),
        "typedRead" => P::TypedRead(super::native_json::Decoder::default().typed_read(value)?),
        "text" => P::Text(super::native_json::Decoder::default().native_text_request(value)?),
        "operation" => {
            P::Operation(super::native_json::Decoder::default().native_operation_request(value)?)
        }
        "transaction" => P::Transaction(
            super::native_json::Decoder::default().native_transaction_request(value)?,
        ),
        "savepoint" => P::Savepoint(super::native_json::Decoder::default().savepoint(value)?),
        "rollbackTo" => P::RollbackTo(super::native_json::Decoder::default().savepoint(value)?),
        "release" => P::Release(super::native_json::Decoder::default().savepoint(value)?),
        "begin" => P::Begin(super::native_json::Decoder::default().begin_request(value)?),
        "commit" => P::Commit(super::native_json::Decoder::default().commit_request(value)?),
        "rollback" => P::Rollback(super::native_json::Decoder::default().rollback_request(value)?),
        "ping" => {
            empty(value)?;
            P::Ping(proto::PingRequest {})
        }
        "sql" => {
            let mut fields = Object::new(value)?;
            let sql = fields
                .optional("sql")
                .map(text)
                .transpose()?
                .unwrap_or_default();
            let params = fields
                .optional("params")
                .map(array)
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .map(decode_value)
                .collect::<Result<_, _>>()?;
            fields.finish()?;
            P::Sql(proto::SqlRequest { sql, params })
        }
        "preview" => {
            let mut fields = Object::new(value)?;
            let request = Some(decode_task(fields.required("request")?)?);
            fields.finish()?;
            P::Preview(proto::PreviewRequest { request })
        }
        "execute" => {
            let mut fields = Object::new(value)?;
            let request = Some(decode_task(fields.required("request")?)?);
            let approval = fields.optional("approval").map(uint).transpose()?;
            fields.finish()?;
            P::Execute(proto::TaskExecuteRequest { request, approval })
        }
        "repairSql" => {
            let mut fields = Object::new(value)?;
            let operation = Some(uint(fields.required("operation")?)?);
            let sql = fields
                .optional("sql")
                .map(text)
                .transpose()?
                .unwrap_or_default();
            let approval = fields.optional("approval").map(uint).transpose()?;
            fields.finish()?;
            P::RepairSql(proto::RepairSqlRequest {
                operation,
                sql,
                approval,
            })
        }
        "status" => {
            let mut fields = Object::new(value)?;
            let operation = Some(uint(fields.required("operation")?)?);
            fields.finish()?;
            P::Status(proto::StatusRequest { operation })
        }
        _ => return Err(ApiError::InvalidArgument),
    };
    Ok(proto::ExecuteRequest {
        request: Some(request),
        budget_ms,
    })
}

pub(super) fn decode_value(value: Json) -> Result<proto::Value, ApiError> {
    let (kind, value) = Object::new(value)?.one()?;
    use proto::value::Value as P;
    let value = match kind.as_str() {
        "nullValue" => {
            if text(value)? != "SQL_NULL" {
                return Err(ApiError::InvalidArgument);
            }
            P::NullValue(0)
        }
        "boolValue" => match value {
            Json::Bool(v) => P::BoolValue(v),
            _ => return Err(ApiError::InvalidArgument),
        },
        "dateBinary" => P::DateBinary(
            STANDARD
                .decode(text(value)?)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        "timeBinary" => P::TimeBinary(
            STANDARD
                .decode(text(value)?)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        "timetzBinary" => P::TimetzBinary(
            STANDARD
                .decode(text(value)?)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        "timestampBinary" => P::TimestampBinary(
            STANDARD
                .decode(text(value)?)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        "timestamptzBinary" => P::TimestamptzBinary(
            STANDARD
                .decode(text(value)?)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        "intervalBinary" => P::IntervalBinary(
            STANDARD
                .decode(text(value)?)
                .map_err(|_| ApiError::InvalidArgument)?,
        ),
        "numericValue" => P::NumericValue(text(value)?),
        "float4Bits" => P::Float4Bits(uint32(value)?),
        "float8Bits" => P::Float8Bits(uint(value)?),
        "intValue" => P::IntValue(int(value)?),
        "textValue" => P::TextValue(text(value)?),
        "bytesValue" => {
            let value = text(value)?;
            let bytes = STANDARD
                .decode(&value)
                .map_err(|_| ApiError::InvalidArgument)?;
            if STANDARD.encode(&bytes) != value {
                return Err(ApiError::InvalidArgument);
            }
            P::BytesValue(bytes)
        }
        _ => return Err(ApiError::InvalidArgument),
    };
    Ok(proto::Value { value: Some(value) })
}
fn status(value: Json) -> Result<i32, ApiError> {
    match text(value)?.as_str() {
        "DELIVERY_STATUS_FAILED" => Ok(proto::DeliveryStatus::Failed as i32),
        "DELIVERY_STATUS_QUEUED" => Ok(proto::DeliveryStatus::Queued as i32),
        "DELIVERY_STATUS_SUCCEEDED" => Ok(proto::DeliveryStatus::Succeeded as i32),
        _ => Err(ApiError::InvalidArgument),
    }
}
fn decode_task(value: Json) -> Result<proto::TaskRequest, ApiError> {
    let mut fields = Object::new(value)?;
    let operation = Some(uint(fields.required("operation")?)?);
    let selection = Some(decode_selection(fields.required("selection")?)?);
    let column = match text(fields.required("column")?)?.as_str() {
        "COLUMN_STATUS" => proto::Column::Status,
        "COLUMN_FAILED_AT" => proto::Column::FailedAt,
        "COLUMN_ATTEMPT_COUNT" => proto::Column::AttemptCount,
        _ => return Err(ApiError::InvalidArgument),
    } as i32;
    let after = status(fields.required("after")?)?;
    fields.finish()?;
    Ok(proto::TaskRequest {
        operation,
        selection,
        column,
        after,
    })
}
fn decode_selection(value: Json) -> Result<proto::Selection, ApiError> {
    let (kind, value) = Object::new(value)?.one()?;
    let mut fields = Object::new(value)?;
    let selection = match kind.as_str() {
        "keys" => {
            let items = fields
                .optional("items")
                .map(array)
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .map(|value| {
                    let mut key = Object::new(value)?;
                    let tenant = Some(uint(key.required("tenant")?)?);
                    let row = Some(uint(key.required("row")?)?);
                    key.finish()?;
                    Ok(proto::Key { tenant, row })
                })
                .collect::<Result<_, ApiError>>()?;
            proto::selection::Selection::Keys(proto::KeyList { items })
        }
        "predicate" => {
            let tenant = Some(uint(fields.required("tenant")?)?);
            let status = status(fields.required("status")?)?;
            let from = Some(uint(fields.required("from")?)?);
            let until = Some(uint(fields.required("until")?)?);
            proto::selection::Selection::Predicate(proto::Predicate {
                tenant,
                status,
                from,
                until,
            })
        }
        _ => return Err(ApiError::InvalidArgument),
    };
    fields.finish()?;
    Ok(proto::Selection {
        selection: Some(selection),
    })
}

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}
impl io::Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("response limit"));
        }
        let required = self.bytes.len() + bytes.len();
        if required > self.bytes.capacity() {
            // Vec's default geometric growth could overshoot the output bound
            // after a large string append. Choose bounded growth explicitly.
            let capacity = required
                .max(self.bytes.capacity().saturating_mul(2).max(1024))
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| io::Error::other("response allocation"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, ApiError> {
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exceeded {
            ApiError::PayloadTooLarge
        } else {
            ApiError::Internal
        });
    }
    Ok(writer.bytes)
}
pub(super) struct Decimal<T>(pub(super) T);
impl<T: fmt::Display> Serialize for Decimal<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}
struct Base64<'a>(&'a [u8]);
impl Serialize for Base64<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&Base64Display::new(self.0, &STANDARD))
    }
}
pub(super) struct JsonValue<'a>(pub(super) &'a proto::Value);
impl Serialize for JsonValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        use proto::value::Value as P;
        match self.0.value.as_ref() {
            Some(P::NullValue(0)) => map.serialize_entry("nullValue", "SQL_NULL")?,
            Some(P::BoolValue(v)) => map.serialize_entry("boolValue", v)?,
            Some(P::IntValue(v)) => map.serialize_entry("intValue", &Decimal(v))?,
            Some(P::TextValue(v)) => map.serialize_entry("textValue", v)?,
            Some(P::BytesValue(v)) => map.serialize_entry("bytesValue", &Base64(v))?,
            Some(P::DateBinary(v)) => map.serialize_entry("dateBinary", &STANDARD.encode(v))?,
            Some(P::TimeBinary(v)) => map.serialize_entry("timeBinary", &STANDARD.encode(v))?,
            Some(P::TimetzBinary(v)) => map.serialize_entry("timetzBinary", &STANDARD.encode(v))?,
            Some(P::TimestampBinary(v)) => {
                map.serialize_entry("timestampBinary", &STANDARD.encode(v))?
            }
            Some(P::TimestamptzBinary(v)) => {
                map.serialize_entry("timestamptzBinary", &STANDARD.encode(v))?
            }
            Some(P::IntervalBinary(v)) => {
                map.serialize_entry("intervalBinary", &STANDARD.encode(v))?
            }
            Some(P::NumericValue(v)) => map.serialize_entry("numericValue", v)?,
            Some(P::Float4Bits(v)) => map.serialize_entry("float4Bits", v)?,
            Some(P::Float8Bits(v)) => map.serialize_entry("float8Bits", &Decimal(v))?,
            _ => return Err(S::Error::custom("invalid output value")),
        }
        map.end()
    }
}
pub(super) struct JsonValues<'a>(pub(super) &'a [proto::Value]);
impl Serialize for JsonValues<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            seq.serialize_element(&JsonValue(value))?;
        }
        seq.end()
    }
}
pub(super) struct JsonRow<'a>(pub(super) &'a proto::SqlRow);
impl Serialize for JsonRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("values", &JsonValues(&self.0.values))?;
        map.serialize_entry("version", &Decimal(self.0.version))?;
        map.end()
    }
}
pub(super) struct JsonRows<'a>(pub(super) &'a [proto::SqlRow]);
impl Serialize for JsonRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&JsonRow(row))?;
        }
        seq.end()
    }
}
struct JsonSql<'a>(&'a proto::SqlResult);
impl Serialize for JsonSql<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("columns", &self.0.columns)?;
        map.serialize_entry("rows", &JsonRows(&self.0.rows))?;
        map.serialize_entry("affected", &Decimal(self.0.affected))?;
        map.end()
    }
}
struct JsonReply<'a>(&'a proto::ExecuteReply);
impl Serialize for JsonReply<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let p = self.0;
        let state = proto::TransactionState::try_from(p.transaction_state)
            .map_err(|_| S::Error::custom("invalid output transaction state"))?;
        if state == proto::TransactionState::Unspecified {
            return Err(S::Error::custom("missing output transaction state"));
        }
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("version", &p.version)?;
        map.serialize_entry("code", &p.code)?;
        if let Some(v) = p.operation {
            map.serialize_entry("operation", &Decimal(v))?;
        }
        if let Some(v) = p.count {
            map.serialize_entry("count", &Decimal(v))?;
        }
        if let Some(v) = &p.digest {
            map.serialize_entry("digest", v)?;
        }
        if let Some(v) = &p.sql {
            map.serialize_entry("sql", &JsonSql(v))?;
        }
        if let Some(v) = &p.native_result {
            match v {
                proto::execute_reply::NativeResult::CatalogOutcome(v) => {
                    map.serialize_entry("catalogOutcome", v)?
                }
                proto::execute_reply::NativeResult::OperationOutcome(v) => {
                    map.serialize_entry("operationOutcome", v)?
                }
                proto::execute_reply::NativeResult::TransactionOutcome(v) => {
                    map.serialize_entry("transactionOutcome", v)?
                }
                proto::execute_reply::NativeResult::NativeError(v) => {
                    map.serialize_entry("nativeError", v)?
                }
            }
        }
        map.serialize_entry("transactionState", state.as_str_name())?;
        map.end()
    }
}

pub fn encode_reply(reply: &proto::ExecuteReply) -> Result<Vec<u8>, ApiError> {
    encode(&JsonReply(reply), crate::MAX_RESPONSE)
}
pub fn encode_open(reply: &proto::OpenSessionReply) -> Result<Vec<u8>, ApiError> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Open<'a> {
        session_id: &'a str,
        idle_timeout_seconds: u32,
        max_age_seconds: u32,
        transaction_timeout_seconds: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        scram: Option<Scram<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        scram_server_final: Option<&'a str>,
    }
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Scram<'a> {
        exchange: &'a str,
        server_first: &'a str,
    }
    encode(
        &Open {
            session_id: &reply.session_id,
            idle_timeout_seconds: reply.idle_timeout_seconds,
            max_age_seconds: reply.max_age_seconds,
            transaction_timeout_seconds: reply.transaction_timeout_seconds,
            scram: reply.scram_challenge.as_ref().map(|s| Scram {
                exchange: &s.exchange,
                server_first: &s.server_first,
            }),
            scram_server_final: reply.scram_server_final.as_deref(),
        },
        crate::MAX_RESPONSE,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::codec;
    use giggles_core::{OperationId, relational::Value};
    use prost::Message;

    #[test]
    fn open_request_json_and_protobuf_roundtrips() {
        for input in [
            serde_json::json!({}),
            serde_json::json!({"task":"t"}),
            serde_json::json!({"role":"r","database":"d"}),
            serde_json::json!({"role":"r","database":"d","password":"p"}),
            serde_json::json!({"role":"r","database":"d","scram":{"clientFirst":"n,,n=r,r=x"}}),
            serde_json::json!({"scram":{"exchange":"a".repeat(64),"clientFinal":"c=biws,r=x,p=y"}}),
        ] {
            let request = decode_open(&serde_json::to_vec(&input).unwrap()).unwrap();
            let decoded =
                proto::OpenSessionRequest::decode(request.encode_to_vec().as_slice()).unwrap();
            assert_eq!(request, decoded);
            let mut output = serde_json::Map::new();
            for (key, value) in [
                ("task", decoded.task),
                ("role", decoded.role),
                ("database", decoded.database),
            ] {
                if let Some(value) = value {
                    output.insert(key.into(), value.into());
                }
            }
            use proto::open_session_request::Credential as C;
            match decoded.credential {
                Some(C::Password(p)) => {
                    output.insert("password".into(), p.into());
                }
                Some(C::ScramStart(s)) => {
                    output.insert(
                        "scram".into(),
                        serde_json::json!({"clientFirst":s.client_first}),
                    );
                }
                Some(C::ScramContinue(s)) => {
                    output.insert(
                        "scram".into(),
                        serde_json::json!({"exchange":s.exchange,"clientFinal":s.client_final}),
                    );
                }
                None => {}
            }
            assert_eq!(serde_json::Value::Object(output), input);
        }
    }

    #[test]
    fn open_json_rejects_unknown_duplicate_mixed_and_invalid_fields() {
        for input in [
            serde_json::json!({"scram":{"foo":"x"}}),
            serde_json::json!({"role":"r","database":"d","scram":{"clientFirst":"x","foo":"x"}}),
            serde_json::json!({"role":"r","database":"d","password":"p","scram":{"clientFirst":"x"}}),
            serde_json::json!({"scram":{"clientFirst":"x","exchange":"a".repeat(64),"clientFinal":"x"}}),
            serde_json::json!({"scram":{"exchange":"a".repeat(64)}}),
            serde_json::json!({"task":"t","role":"r","database":"d"}),
            serde_json::json!({"role":"r"}),
            serde_json::json!({"database":"d"}),
            serde_json::json!({"role":"r","database":"d","password":""}),
            serde_json::json!({"role":null}),
            serde_json::json!({"scram":{}}),
            serde_json::json!({"scram_start":{}}),
        ] {
            assert_eq!(
                decode_open(&serde_json::to_vec(&input).unwrap()),
                Err(ApiError::InvalidArgument),
                "{input}"
            );
        }
        assert!(decode_open(br#"{"role":"r","role":"s"}"#).is_err());
    }

    #[test]
    fn open_reply_json_and_protobuf_roundtrips() {
        for (challenge, final_message) in [(false, false), (true, false), (false, true)] {
            let reply = proto::OpenSessionReply {
                session_id: if challenge { "" } else { "session" }.into(),
                idle_timeout_seconds: 1,
                max_age_seconds: 2,
                transaction_timeout_seconds: 3,
                scram_challenge: challenge.then(|| proto::ScramChallenge {
                    exchange: "a".repeat(64),
                    server_first: "r=x,s=y,i=4096".into(),
                }),
                scram_server_final: final_message.then(|| "v=proof".into()),
            };
            let decoded =
                proto::OpenSessionReply::decode(reply.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, reply);
            let json: serde_json::Value =
                serde_json::from_slice(&encode_open(&decoded).unwrap()).unwrap();
            let mut expected = serde_json::json!({"sessionId":reply.session_id,"idleTimeoutSeconds":1,"maxAgeSeconds":2,"transactionTimeoutSeconds":3});
            if challenge {
                expected["scram"] =
                    serde_json::json!({"exchange":"a".repeat(64),"serverFirst":"r=x,s=y,i=4096"});
            }
            if final_message {
                expected["scramServerFinal"] = "v=proof".into();
            }
            assert_eq!(json, expected);
        }
    }

    #[test]
    fn json_rejects_duplicate_unknown_and_deep_fields() {
        for input in [
            r#"{"ping":{},"ping":{}}"#,
            r#"{"ping":{},"sql":{"sql":"BEGIN"}}"#,
            r#"{"ping":{"admin":true}}"#,
            r#"{"ping":{},"budget_ms":1}"#,
            r#"{"sql":{"sql":"BEGIN","sql":"COMMIT"}}"#,
            r#"{"sql":{"params":[{"intValue":"1","textValue":"x"}]}}"#,
            r#"{"ping":{}} {"ping":{}}"#,
            r#"{"sql":{"params":[null]}}"#,
            r#"{"ping":{},"budgetMs":null}"#,
            r#"{"ping":{},"budgetMs":1.0}"#,
            r#"{"ping":{},"budgetMs":1e1}"#,
            r#"{"ping":{},"budgetMs":-0}"#,
            r#"{"ping":{},"budgetMs":"1"}"#,
        ] {
            assert_eq!(
                decode_execute(input.as_bytes()),
                Err(ApiError::InvalidArgument),
                "{input}"
            );
        }
        assert!(decode_empty(b"{}").is_ok());
        assert!(decode_empty(b"{\"unknown\":0}").is_err());
        let at_limit = format!("{}true{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(parse(at_limit.as_bytes()).is_ok());
        let over = format!("[{at_limit}]");
        assert!(parse(over.as_bytes()).is_err());
    }

    #[test]
    fn json_integer_and_base64_canonical_forms() {
        for input in ["0", "-9223372036854775808", "9223372036854775807"] {
            let bytes = format!(r#"{{"sql":{{"params":[{{"intValue":"{input}"}}]}}}}"#);
            let got = codec::decode_execute(decode_execute(bytes.as_bytes()).unwrap()).unwrap();
            let codec::Invocation::Legacy(crate::Request::Sql { params, .. }) = got.request else {
                panic!()
            };
            assert_eq!(params, vec![Value::Int(input.parse().unwrap())]);
        }
        for input in [
            "+1",
            "01",
            "-0",
            "1e1",
            "1.0",
            "9223372036854775808",
            "-9223372036854775809",
            "",
        ] {
            let bytes = format!(r#"{{"sql":{{"params":[{{"intValue":"{input}"}}]}}}}"#);
            assert!(decode_execute(bytes.as_bytes()).is_err(), "{input}");
        }
        for input in ["0", "18446744073709551615"] {
            let bytes = format!(r#"{{"status":{{"operation":"{input}"}}}}"#);
            assert!(decode_execute(bytes.as_bytes()).is_ok());
        }
        for input in ["+1", "01", "-1", "18446744073709551616"] {
            let bytes = format!(r#"{{"status":{{"operation":"{input}"}}}}"#);
            assert!(decode_execute(bytes.as_bytes()).is_err());
        }
        for input in ["", "AA==", "AP+A"] {
            let bytes = format!(r#"{{"sql":{{"params":[{{"bytesValue":"{input}"}}]}}}}"#);
            assert!(decode_execute(bytes.as_bytes()).is_ok());
        }
        for input in ["AA", "AA=", "AB==", "_w==", "AA== "] {
            let bytes = format!(r#"{{"sql":{{"params":[{{"bytesValue":"{input}"}}]}}}}"#);
            assert!(decode_execute(bytes.as_bytes()).is_err(), "{input}");
        }
        assert!(decode_execute(br#"{"status":{"operation":1}}"#).is_err());
    }

    #[test]
    fn all_request_variants_match_native() {
        let cases = [
            (r#"{"ping":{}}"#, serde_json::json!({"kind":"Ping"})),
            (
                r#"{"sql":{"sql":"SELECT $1","params":[{"intValue":"-3"},{"boolValue":false},{"nullValue":"SQL_NULL"},{"textValue":"\u0000雪"},{"bytesValue":"AP8="}]}}"#,
                serde_json::json!({"kind":"Sql","sql":"SELECT $1","params":[{"Int":-3},{"Bool":false},"Null",{"Text":"\0雪"},{"Bytes":[0,255]}]}),
            ),
            (
                r#"{"repairSql":{"operation":"0","sql":"repair","approval":"0"}}"#,
                serde_json::json!({"kind":"RepairSql","operation":0,"sql":"repair","approval":0}),
            ),
            (
                r#"{"status":{"operation":"18446744073709551615"}}"#,
                serde_json::json!({"kind":"Status","operation":u64::MAX}),
            ),
            (
                r#"{"preview":{"request":{"operation":"7","selection":{"keys":{"items":[{"tenant":"42","row":"3"}]}},"column":"COLUMN_STATUS","after":"DELIVERY_STATUS_QUEUED"}}}"#,
                serde_json::json!({"kind":"Preview","request":{"operation":7,"selection":{"Keys":[{"tenant":42,"row":3}]},"column":"Status","after":"Queued"}}),
            ),
            (
                r#"{"execute":{"request":{"operation":"8","selection":{"predicate":{"tenant":"42","status":"DELIVERY_STATUS_FAILED","from":"1","until":"99"}},"column":"COLUMN_ATTEMPT_COUNT","after":"DELIVERY_STATUS_SUCCEEDED"},"approval":"0"},"budgetMs":0}"#,
                serde_json::json!({"kind":"Execute","request":{"operation":8,"selection":{"Predicate":{"tenant":42,"status":"Failed","from":1,"until":99}},"column":"AttemptCount","after":"Succeeded"},"approval":0}),
            ),
        ];
        for (json, expected) in cases {
            let proto = decode_execute(json.as_bytes()).unwrap();
            let wire = proto.encode_to_vec();
            let decoded =
                codec::decode_execute(proto::ExecuteRequest::decode(wire.as_slice()).unwrap())
                    .unwrap();
            let codec::Invocation::Legacy(request) = decoded.request else {
                panic!("legacy request expected")
            };
            assert_eq!(serde_json::to_value(request).unwrap(), expected);
        }
    }

    #[test]
    fn json_reply_preserves_strings_null_and_binary() {
        let response = crate::Response {
            sql: Some(giggles_core::relational::SqlResult {
                columns: vec!["n".into(), "b".into()],
                rows: vec![giggles_core::relational::SqlRow {
                    values: vec![
                        Value::Int(i64::MIN),
                        Value::Bytes(vec![0, 255]),
                        Value::Null,
                        Value::Bool(false),
                    ],
                    version: u64::MAX,
                }],
                affected: u64::MAX,
            }),
            ..crate::Response::code("ok")
        };
        let reply = codec::encode_reply(response, proto::TransactionState::InTransaction).unwrap();
        let encoded = encode_reply(&reply).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["sql"]["affected"], u64::MAX.to_string());
        assert_eq!(value["sql"]["rows"][0]["version"], u64::MAX.to_string());
        assert_eq!(
            value["sql"]["rows"][0]["values"],
            serde_json::json!([
                {"intValue":i64::MIN.to_string()},{"bytesValue":"AP8="},{"nullValue":"SQL_NULL"},{"boolValue":false}
            ])
        );
        assert!(value.get("operation").is_none());
        assert_eq!(
            value["transactionState"],
            "TRANSACTION_STATE_IN_TRANSACTION"
        );
    }

    #[test]
    fn encode_cap_checked_before_append() {
        use std::io::Write;
        let mut writer = LimitedWriter {
            bytes: vec![],
            limit: 3,
            exceeded: false,
        };
        writer.write_all(b"abc").unwrap();
        assert!(writer.write_all(b"d").is_err());
        assert_eq!(writer.bytes, b"abc");
        assert!(writer.bytes.capacity() <= writer.limit);
        let mut growing = LimitedWriter {
            bytes: vec![],
            limit: 4096,
            exceeded: false,
        };
        growing.write_all(&vec![b'x'; 3000]).unwrap();
        growing.write_all(b"x").unwrap();
        assert!(growing.bytes.capacity() <= growing.limit);
        assert_eq!(encode(&"abc", 5).unwrap(), br#""abc""#);
        assert_eq!(encode(&"abc", 4), Err(ApiError::PayloadTooLarge));
        let at_limit = "x".repeat(crate::MAX_RESPONSE - 2);
        assert_eq!(
            encode(&at_limit, crate::MAX_RESPONSE).unwrap().len(),
            crate::MAX_RESPONSE
        );
        let over = "x".repeat(crate::MAX_RESPONSE - 1);
        assert_eq!(
            encode(&over, crate::MAX_RESPONSE),
            Err(ApiError::PayloadTooLarge)
        );
    }

    #[test]
    fn body_limit_is_checked_even_with_whitespace() {
        let mut input = b"{\"ping\":{}}".to_vec();
        input.resize(crate::MAX_FRAME, b' ');
        assert!(decode_execute(&input).is_ok());
        input.push(b' ');
        assert_eq!(decode_execute(&input), Err(ApiError::PayloadTooLarge));
    }

    #[test]
    fn task_envelopes_fit_reserved_allowance() {
        // Fixed HTTP/gRPC response metadata, HTTP/2 framing and gRPC prefix must
        // remain within this deliberately conservative 512-byte allowance.
        // No arbitrary request metadata may be reflected by either transport.
        const TRANSPORT_ALLOWANCE: usize = 512;
        let codes = [
            "ok",
            "preview",
            "committed",
            "absent",
            "28000",
            "42501",
            "55000",
            "54000",
            "GD001",
            "GD002",
            "22023",
            "0A000",
            "42601",
            "42P01",
            "42703",
            "23505",
            "23502",
            "42804",
            "40001",
            "25000",
            "25P02",
            "22003",
            "57014",
            "53300",
            "08007",
            "XX000",
        ];
        for code in codes {
            for mask in 0..8 {
                let response = crate::Response {
                    operation: (mask & 1 != 0).then_some(OperationId(u64::MAX)),
                    count: (mask & 2 != 0).then_some(usize::MAX),
                    digest: (mask & 4 != 0).then(|| "f".repeat(64)),
                    ..crate::Response::code(code)
                };
                let reserved = serde_json::to_vec(&response).unwrap().len() + 1024;
                for state in [
                    proto::TransactionState::Idle,
                    proto::TransactionState::InTransaction,
                    proto::TransactionState::Aborted,
                ] {
                    let reply = codec::encode_reply(response.clone(), state).unwrap();
                    assert!(encode_reply(&reply).unwrap().len() + TRANSPORT_ALLOWANCE <= reserved);
                    assert!(reply.encoded_len() + 5 + TRANSPORT_ALLOWANCE <= reserved);
                }
            }
        }
    }
}
