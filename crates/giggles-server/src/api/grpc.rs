//! Generated tonic service behind the shared authenticated Hyper driver.
use super::{
    body::{self, ResponseBody},
    dispatch::{ApiRuntime, CallOwnership, Encoding, ResponseLease},
    error::ApiError,
    identity::ConnectionIdentity,
    net::{self, ActivityTicket},
    proto::{
        self,
        giggles_server::{Giggles, GigglesServer},
    },
};
use crate::transport::{IO_TIMEOUT, MAX_FRAME, MAX_RESPONSE};
use ::http::{HeaderMap, Method, Request, Response, Version, header};
use http_body_util::Full;
use hyper::body::Incoming;
use prost::Message;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tonic::{Request as RpcRequest, Response as RpcResponse, Status};
use tower::ServiceExt;

#[derive(Clone)]
struct Context {
    identity: ConnectionIdentity,
    token: Option<String>,
    received: Instant,
    deadline: Option<Instant>,
    ownership: Arc<CallOwnership>,
    lease: Arc<Mutex<Option<ResponseLease>>>,
    delivery_owner: Arc<Mutex<Option<ConnectionIdentity>>>,
}

struct Rpc {
    runtime: Arc<ApiRuntime>,
}

#[tonic::async_trait]
impl Giggles for Rpc {
    async fn open_session(
        &self,
        request: RpcRequest<proto::OpenSessionRequest>,
    ) -> Result<RpcResponse<proto::OpenSessionReply>, Status> {
        let context = request
            .extensions()
            .get::<Context>()
            .ok_or_else(|| ApiError::Internal.grpc_status())?;
        check_deadline(context)?;
        let reply = self
            .runtime
            .open_request(&context.identity, request.get_ref())
            .map_err(ApiError::grpc_status)?;
        *context.delivery_owner.lock().expect("delivery owner lock") = self
            .runtime
            .open_delivery_owner(&context.identity, &reply)
            .map_err(ApiError::grpc_status)?;
        Ok(RpcResponse::new(reply))
    }

    async fn execute(
        &self,
        request: RpcRequest<proto::ExecuteRequest>,
    ) -> Result<RpcResponse<proto::ExecuteReply>, Status> {
        let context = request
            .extensions()
            .get::<Context>()
            .cloned()
            .ok_or_else(|| ApiError::Internal.grpc_status())?;
        check_deadline(&context)?;
        let completion = self
            .runtime
            .execute(
                &context.identity,
                context
                    .token
                    .as_deref()
                    .ok_or_else(|| ApiError::InvalidArgument.grpc_status())?,
                request.into_inner(),
                context.received,
                context.deadline,
                context.ownership,
                Encoding::Protobuf,
            )
            .await
            .map_err(ApiError::grpc_status)?;
        completion
            .lease
            .check_release()
            .map_err(ApiError::grpc_status)?;
        *context
            .lease
            .lock()
            .map_err(|_| ApiError::Internal.grpc_status())? = Some(completion.lease);
        Ok(RpcResponse::new(completion.reply))
    }

    async fn close_session(
        &self,
        request: RpcRequest<proto::CloseSessionRequest>,
    ) -> Result<RpcResponse<proto::CloseSessionReply>, Status> {
        let context = request
            .extensions()
            .get::<Context>()
            .ok_or_else(|| ApiError::Internal.grpc_status())?;
        check_deadline(context)?;
        self.runtime
            .close(
                &context.identity,
                context
                    .token
                    .as_deref()
                    .ok_or_else(|| ApiError::InvalidArgument.grpc_status())?,
            )
            .map_err(ApiError::grpc_status)?;
        Ok(RpcResponse::new(proto::CloseSessionReply {}))
    }
}

fn check_deadline(context: &Context) -> Result<(), Status> {
    if context
        .deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        Err(ApiError::DeadlineExceeded.grpc_status())
    } else {
        Ok(())
    }
}

pub(crate) async fn handle(
    request: Request<Incoming>,
    runtime: Arc<ApiRuntime>,
    identity: ConnectionIdentity,
    ownership: Arc<CallOwnership>,
    ticket: ActivityTicket,
    headers_received: Instant,
) -> Response<ResponseBody> {
    let path = request.uri().path().to_owned();
    let open = path == "/giggles.v1.Giggles/OpenSession";
    let execute = path == "/giggles.v1.Giggles/Execute";
    let close = path == "/giggles.v1.Giggles/CloseSession";
    if !open && !execute && !close {
        return status_response(
            Status::unimplemented("method not found"),
            Some(ownership),
            ticket,
        );
    }
    let checked = (|| {
        if request.version() != Version::HTTP_2
            || request.method() != Method::POST
            || request.uri().query().is_some()
        {
            return Err(ApiError::InvalidArgument);
        }
        validate_media(request.headers())?;
        let token = net::session_header(request.headers(), !open)?;
        let deadline = parse_timeout(request.headers(), headers_received)?;
        net::declared_length(request.headers(), MAX_FRAME + 5)?;
        Ok((token, deadline))
    })();
    let (token, deadline) = match checked {
        Ok(checked) => checked,
        Err(error) => return error_response(error, Some(ownership), ticket),
    };
    let body_deadline = deadline.map_or(headers_received + IO_TIMEOUT, |deadline| {
        deadline.min(headers_received + IO_TIMEOUT)
    });
    ticket.stage(body_deadline);
    let (parts, body) = request.into_parts();
    let bytes = match body::collect(body, MAX_FRAME + 5, body_deadline).await {
        Ok(bytes) => bytes,
        Err(error) => return error_response(error, Some(ownership), ticket),
    };
    let received = Instant::now();
    // Tonic drains the rest of a unary request for trailers. Validate the
    // bounded envelope first so extra messages cannot be silently discarded.
    // Generated tonic/prost code remains responsible for decoding and replies.
    let validated = validate_unary(&bytes).and_then(|message| {
        if execute {
            proto::ExecuteRequest::decode(message)
                .map(|request| request.budget_ms.unwrap_or(10_000))
        } else if open {
            proto::OpenSessionRequest::decode(message).map(|_| 10_000)
        } else {
            proto::CloseSessionRequest::decode(message).map(|_| 10_000)
        }
        .map_err(|_| ApiError::InvalidArgument)
    });
    let budget = match validated {
        Ok(budget) => budget,
        Err(error) => return error_response(error, Some(ownership), ticket),
    };
    let decision_deadline = received + Duration::from_millis(u64::from(budget.min(10_000)));
    // The runtime fences decisions at their deadline. Allow bounded time for
    // its cancellation result to become a response instead of racing that fence.
    ticket.stage(
        deadline.map_or(decision_deadline, |deadline| {
            deadline.min(decision_deadline)
        }) + IO_TIMEOUT,
    );
    let lease = Arc::new(Mutex::new(None));
    let delivery_owner = Arc::new(Mutex::new(None));
    let delivery_service = runtime.service().clone();
    let context = Context {
        identity,
        token,
        received,
        deadline,
        ownership: ownership.clone(),
        lease: lease.clone(),
        delivery_owner: delivery_owner.clone(),
    };
    let mut request = Request::from_parts(parts, Full::new(bytes));
    request.extensions_mut().insert(context);
    let service = GigglesServer::new(Rpc { runtime })
        .max_decoding_message_size(MAX_FRAME)
        .max_encoding_message_size(MAX_RESPONSE);
    let response = match service.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let lease = lease.lock().expect("response lease lock").take();
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    let mut body = ResponseBody::new(body, Some(ownership), lease, ticket);
    if let Some(owner) = delivery_owner.lock().expect("delivery owner lock").take() {
        body.guard_binding(owner, delivery_service);
    }
    Response::from_parts(parts, body)
}

fn validate_unary(bytes: &[u8]) -> Result<&[u8], ApiError> {
    if bytes.len() < 5 {
        return Err(ApiError::InvalidArgument);
    }
    match bytes[0] {
        0 => {}
        1 => return Err(ApiError::UnsupportedMediaType),
        _ => return Err(ApiError::InvalidArgument),
    }
    let length = u32::from_be_bytes(bytes[1..5].try_into().expect("five-byte envelope")) as usize;
    if length > MAX_FRAME {
        return Err(ApiError::PayloadTooLarge);
    }
    if bytes.len() - 5 != length {
        return Err(ApiError::InvalidArgument);
    }
    Ok(&bytes[5..])
}

fn validate_media(headers: &HeaderMap) -> Result<(), ApiError> {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let value = values.next().ok_or(ApiError::UnsupportedMediaType)?;
    if values.next().is_some() {
        return Err(ApiError::UnsupportedMediaType);
    }
    let value = value.to_str().map_err(|_| ApiError::UnsupportedMediaType)?;
    if !value.eq_ignore_ascii_case("application/grpc")
        && !value.eq_ignore_ascii_case("application/grpc+proto")
    {
        return Err(ApiError::UnsupportedMediaType);
    }
    if headers.contains_key(header::CONTENT_ENCODING) {
        return Err(ApiError::UnsupportedMediaType);
    }
    let mut values = headers.get_all("grpc-encoding").iter();
    if let Some(value) = values.next()
        && (value != "identity" || values.next().is_some())
    {
        return Err(ApiError::UnsupportedMediaType);
    }
    Ok(())
}

fn parse_timeout(headers: &HeaderMap, received: Instant) -> Result<Option<Instant>, ApiError> {
    let mut values = headers.get_all("grpc-timeout").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::InvalidArgument);
    }
    let value = value.as_bytes();
    if !(2..=9).contains(&value.len()) {
        return Err(ApiError::InvalidArgument);
    }
    let digits = &value[..value.len() - 1];
    if !digits.iter().all(u8::is_ascii_digit) {
        return Err(ApiError::InvalidArgument);
    }
    let number = digits
        .iter()
        .try_fold(0u64, |n, digit| {
            n.checked_mul(10)?.checked_add(u64::from(digit - b'0'))
        })
        .ok_or(ApiError::InvalidArgument)?;
    let duration = match value[value.len() - 1] {
        b'H' => Duration::from_secs(number.checked_mul(3600).ok_or(ApiError::InvalidArgument)?),
        b'M' => Duration::from_secs(number.checked_mul(60).ok_or(ApiError::InvalidArgument)?),
        b'S' => Duration::from_secs(number),
        b'm' => Duration::from_millis(number),
        b'u' => Duration::from_micros(number),
        b'n' => Duration::from_nanos(number),
        _ => return Err(ApiError::InvalidArgument),
    };
    received
        .checked_add(duration)
        .map(Some)
        .ok_or(ApiError::InvalidArgument)
}

pub(crate) fn error_response(
    error: ApiError,
    ownership: Option<Arc<CallOwnership>>,
    ticket: ActivityTicket,
) -> Response<ResponseBody> {
    status_response(error.grpc_status(), ownership, ticket)
}

fn status_response(
    status: Status,
    ownership: Option<Arc<CallOwnership>>,
    ticket: ActivityTicket,
) -> Response<ResponseBody> {
    let mut response = Response::new(ResponseBody::new(
        tonic::body::Body::empty(),
        ownership,
        None,
        ticket,
    ));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/grpc"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    status
        .add_header(response.headers_mut())
        .expect("fixed diagnostic headers");
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timeout_units_zero_duplicates_and_grammar() {
        let now = Instant::now();
        for (value, expected) in [
            ("0n", Duration::ZERO),
            ("1H", Duration::from_secs(3600)),
            ("2M", Duration::from_secs(120)),
            ("3S", Duration::from_secs(3)),
            ("4m", Duration::from_millis(4)),
            ("5u", Duration::from_micros(5)),
            ("6n", Duration::from_nanos(6)),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("grpc-timeout", value.parse().unwrap());
            assert_eq!(
                parse_timeout(&headers, now).unwrap(),
                now.checked_add(expected)
            );
        }
        for value in [
            "",
            "S",
            "1",
            "123456789S",
            "-1S",
            "+1S",
            "1s",
            "1.0S",
            " 1S",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("grpc-timeout", value.parse().unwrap());
            assert_eq!(parse_timeout(&headers, now), Err(ApiError::InvalidArgument));
        }
        let mut headers = HeaderMap::new();
        headers.append("grpc-timeout", "1S".parse().unwrap());
        headers.append("grpc-timeout", "2S".parse().unwrap());
        assert_eq!(parse_timeout(&headers, now), Err(ApiError::InvalidArgument));
    }

    #[test]
    fn unary_empty_extra_truncated_oversize_and_compression() {
        assert!(validate_unary(&[0, 0, 0, 0, 0]).unwrap().is_empty());
        assert_eq!(validate_unary(&[]), Err(ApiError::InvalidArgument));
        assert_eq!(
            validate_unary(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(ApiError::InvalidArgument)
        );
        assert_eq!(
            validate_unary(&[0, 0, 0, 0, 1]),
            Err(ApiError::InvalidArgument)
        );
        assert_eq!(
            validate_unary(&[0, 255, 255, 255, 255]),
            Err(ApiError::PayloadTooLarge)
        );
        assert_eq!(
            validate_unary(&[1, 0, 0, 0, 0]),
            Err(ApiError::UnsupportedMediaType)
        );
    }
}
