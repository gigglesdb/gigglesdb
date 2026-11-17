//! Strict JSON routes; execution and session ownership live in the shared runtime.
use super::{
    body::{self, ResponseBody},
    dispatch::{ApiRuntime, CallOwnership, Encoding, ResponseLease},
    error::ApiError,
    identity::ConnectionIdentity,
    json,
    net::{self, ActivityTicket},
};
use crate::transport::{IO_TIMEOUT, MAX_FRAME};
use ::http::{HeaderMap, Method, Request, Response, StatusCode, header};
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
enum Route {
    Open,
    Execute,
    Close,
}

pub(crate) async fn handle(
    request: Request<Incoming>,
    runtime: Arc<ApiRuntime>,
    identity: ConnectionIdentity,
    ownership: Arc<CallOwnership>,
    ticket: ActivityTicket,
    headers_received: Instant,
) -> Response<ResponseBody> {
    let route = match request.uri().path() {
        "/v1/sessions" => Route::Open,
        "/v1/session/execute" => Route::Execute,
        "/v1/session" => Route::Close,
        _ => {
            return with_status(
                ApiError::InvalidArgument,
                StatusCode::NOT_FOUND,
                Some(ownership),
                ticket,
            );
        }
    };
    let method = match route {
        Route::Open | Route::Execute => Method::POST,
        Route::Close => Method::DELETE,
    };
    if request.method() != method {
        let mut response = with_status(
            ApiError::InvalidArgument,
            StatusCode::METHOD_NOT_ALLOWED,
            Some(ownership),
            ticket,
        );
        response.headers_mut().insert(
            header::ALLOW,
            method.as_str().parse().expect("fixed method"),
        );
        return response;
    }
    let checked = (|| {
        if request.uri().query().is_some() {
            return Err(ApiError::InvalidArgument);
        }
        let token = net::session_header(request.headers(), !matches!(route, Route::Open))?;
        validate_media(request.headers(), !matches!(route, Route::Close))?;
        net::declared_length(request.headers(), MAX_FRAME)?;
        Ok(token)
    })();
    let token = match checked {
        Ok(token) => token,
        Err(error) => return error_response(error, Some(ownership), ticket),
    };
    let deadline = headers_received + IO_TIMEOUT;
    ticket.stage(deadline);
    let bytes = match body::collect(request.into_body(), MAX_FRAME, deadline).await {
        Ok(bytes) => bytes,
        Err(error) => return error_response(error, Some(ownership), ticket),
    };
    let received = Instant::now();
    let mut delivery_owner = None;
    let result = match route {
        Route::Open => json::decode_open(&bytes)
            .and_then(|open| runtime.open_request(&identity, &open))
            .and_then(|reply| {
                delivery_owner = runtime.open_delivery_owner(&identity, &reply)?;
                let status = if reply.scram_challenge.is_some() {
                    StatusCode::OK
                } else {
                    StatusCode::CREATED
                };
                json::encode_open(&reply).map(|bytes| (status, bytes, None))
            }),
        Route::Close => {
            if bytes.is_empty() {
                runtime
                    .close(&identity, token.as_deref().expect("validated token"))
                    .map(|()| (StatusCode::NO_CONTENT, Vec::new(), None))
            } else {
                Err(ApiError::InvalidArgument)
            }
        }
        Route::Execute => match json::decode_execute(&bytes) {
            Err(error) => Err(error),
            Ok(request) => {
                ticket.stage(
                    received
                        + Duration::from_millis(u64::from(
                            request.budget_ms.unwrap_or(10_000).min(10_000),
                        ))
                        + IO_TIMEOUT,
                );
                runtime
                    .execute(
                        &identity,
                        token.as_deref().expect("validated token"),
                        request,
                        received,
                        None,
                        ownership.clone(),
                        Encoding::Json,
                    )
                    .await
                    .and_then(|completion| {
                        completion.lease.check_release()?;
                        Ok((
                            StatusCode::OK,
                            completion.json.ok_or(ApiError::Internal)?,
                            Some(completion.lease),
                        ))
                    })
            }
        },
    };
    match result {
        Ok((status, bytes, lease)) => {
            let mut response = response(status, bytes, Some(ownership), lease, ticket);
            if let Some(owner) = delivery_owner {
                response
                    .body_mut()
                    .guard_binding(owner, runtime.service().clone());
            }
            response
        }
        Err(error) => error_response(error, Some(ownership), ticket),
    }
}

fn validate_media(headers: &HeaderMap, json: bool) -> Result<(), ApiError> {
    if headers.contains_key(header::CONTENT_ENCODING) {
        return Err(ApiError::UnsupportedMediaType);
    }
    if !json {
        return Ok(());
    }
    let mut types = headers.get_all(header::CONTENT_TYPE).iter();
    let content_type = types.next().ok_or(ApiError::UnsupportedMediaType)?;
    if types.next().is_some() {
        return Err(ApiError::UnsupportedMediaType);
    }
    let mut pieces = content_type
        .to_str()
        .map_err(|_| ApiError::UnsupportedMediaType)?
        .split(';');
    if !pieces
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case("application/json")
    {
        return Err(ApiError::UnsupportedMediaType);
    }
    if let Some(parameter) = pieces.next() {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            return Err(ApiError::UnsupportedMediaType);
        };
        if !name.trim().eq_ignore_ascii_case("charset")
            || !value.trim().eq_ignore_ascii_case("utf-8")
        {
            return Err(ApiError::UnsupportedMediaType);
        }
    }
    if pieces.next().is_some() {
        return Err(ApiError::UnsupportedMediaType);
    }
    Ok(())
}

pub(crate) fn error_response(
    error: ApiError,
    ownership: Option<Arc<CallOwnership>>,
    ticket: ActivityTicket,
) -> Response<ResponseBody> {
    with_status(
        error,
        StatusCode::from_u16(error.http_status()).expect("fixed status"),
        ownership,
        ticket,
    )
}

fn with_status(
    error: ApiError,
    status: StatusCode,
    ownership: Option<Arc<CallOwnership>>,
    ticket: ActivityTicket,
) -> Response<ResponseBody> {
    let mut response = response(status, error.json_body(), ownership, None, ticket);
    // Hyper drops an unread HTTP/2 body by resetting its stream. On HTTP/1,
    // closing avoids interpreting rejected body bytes as another request.
    response.headers_mut().insert(
        header::CONNECTION,
        header::HeaderValue::from_static("close"),
    );
    response
}

fn response(
    status: StatusCode,
    bytes: Vec<u8>,
    ownership: Option<Arc<CallOwnership>>,
    lease: Option<ResponseLease>,
    ticket: ActivityTicket,
) -> Response<ResponseBody> {
    let mut response = Response::new(ResponseBody::new(
        tonic::body::Body::new(Full::new(Bytes::from(bytes))),
        ownership,
        lease,
        ticket,
    ));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    if status != StatusCode::NO_CONTENT {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_response_metadata_fits_task_envelope_allowance() {
        for error in [
            ApiError::InvalidArgument,
            ApiError::Unauthenticated,
            ApiError::SessionNotFound,
            ApiError::SessionBusy,
            ApiError::ResourceExhausted,
            ApiError::PayloadTooLarge,
            ApiError::UnsupportedMediaType,
            ApiError::DeadlineExceeded,
            ApiError::Unavailable,
            ApiError::Internal,
        ] {
            let activity = Arc::new(net::ConnectionActivity::new());
            let response = error_response(error, None, activity.begin());
            let bytes: usize = response
                .headers()
                .iter()
                .map(|(name, value)| name.as_str().len() + value.len() + 4)
                .sum();
            // Reserve status line, Date, Content-Length, Allow and framing that
            // the protocol driver may append to the fixed adapter headers.
            assert!(bytes + 160 <= 512);
        }
    }
    #[test]
    fn media_profile_is_narrow_and_case_insensitive() {
        for value in ["application/json", "Application/JSON; Charset=UTF-8"] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, value.parse().unwrap());
            assert!(validate_media(&headers, true).is_ok());
        }
        for value in [
            "text/json",
            "application/json;charset=latin1",
            "application/json;x=y",
            "application/json;charset=utf-8;charset=utf-8",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, value.parse().unwrap());
            assert_eq!(
                validate_media(&headers, true),
                Err(ApiError::UnsupportedMediaType)
            );
        }
    }
}
