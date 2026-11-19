//! Fixed transport diagnostics. Domain decision codes remain in ExecuteReply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiError {
    InvalidArgument,
    LegacyTaskRetired,
    Unauthenticated,
    SessionNotFound,
    SessionBusy,
    ResourceExhausted,
    PayloadTooLarge,
    UnsupportedMediaType,
    DeadlineExceeded,
    Unavailable,
    Internal,
}

impl ApiError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidArgument | Self::LegacyTaskRetired => "INVALID_ARGUMENT",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::SessionNotFound => "SESSION_NOT_FOUND",
            Self::SessionBusy => "SESSION_BUSY",
            Self::ResourceExhausted | Self::PayloadTooLarge => "RESOURCE_EXHAUSTED",
            Self::UnsupportedMediaType => "UNSUPPORTED_MEDIA_TYPE",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::Unavailable => "UNAVAILABLE",
            Self::Internal => "INTERNAL",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid request",
            Self::LegacyTaskRetired => "retired legacy task prototype; enable the legacy-task feature",
            Self::Unauthenticated => "authentication required",
            Self::SessionNotFound => "session not found",
            Self::SessionBusy => "session busy",
            Self::ResourceExhausted | Self::PayloadTooLarge => "resource limit",
            Self::UnsupportedMediaType => "unsupported encoding",
            Self::DeadlineExceeded => "deadline exceeded",
            Self::Unavailable => "service unavailable",
            Self::Internal => "internal error; outcome may be ambiguous",
        }
    }

    pub fn http_status(self) -> u16 {
        match self {
            Self::InvalidArgument | Self::LegacyTaskRetired => 400,
            Self::Unauthenticated => 401,
            Self::SessionNotFound => 404,
            Self::SessionBusy => 409,
            Self::ResourceExhausted => 429,
            Self::PayloadTooLarge => 413,
            Self::UnsupportedMediaType => 415,
            Self::DeadlineExceeded => 408,
            Self::Unavailable => 503,
            Self::Internal => 500,
        }
    }

    pub fn grpc_status(self) -> tonic::Status {
        let code = match self {
            Self::InvalidArgument | Self::LegacyTaskRetired => tonic::Code::InvalidArgument,
            Self::Unauthenticated => tonic::Code::Unauthenticated,
            Self::SessionNotFound => tonic::Code::NotFound,
            Self::SessionBusy => tonic::Code::Aborted,
            Self::ResourceExhausted | Self::PayloadTooLarge => tonic::Code::ResourceExhausted,
            Self::UnsupportedMediaType => tonic::Code::Unimplemented,
            Self::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            Self::Unavailable => tonic::Code::Unavailable,
            Self::Internal => tonic::Code::Internal,
        };
        tonic::Status::new(code, self.message())
    }

    pub fn json_body(self) -> Vec<u8> {
        // Both strings are fixed ASCII literals; no submitted data is echoed.
        format!(
            "{{\"error\":{{\"code\":\"{}\",\"message\":\"{}\"}}}}",
            self.code(),
            self.message()
        )
        .into_bytes()
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for ApiError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_transport_errors_match_contract() {
        use tonic::Code;
        for (error, http, grpc) in [
            (ApiError::InvalidArgument, 400, Code::InvalidArgument),
            (ApiError::Unauthenticated, 401, Code::Unauthenticated),
            (ApiError::SessionNotFound, 404, Code::NotFound),
            (ApiError::SessionBusy, 409, Code::Aborted),
            (ApiError::ResourceExhausted, 429, Code::ResourceExhausted),
            (ApiError::PayloadTooLarge, 413, Code::ResourceExhausted),
            (ApiError::UnsupportedMediaType, 415, Code::Unimplemented),
            (ApiError::DeadlineExceeded, 408, Code::DeadlineExceeded),
            (ApiError::Unavailable, 503, Code::Unavailable),
            (ApiError::Internal, 500, Code::Internal),
        ] {
            assert_eq!(error.http_status(), http);
            assert_eq!(error.grpc_status().code(), grpc);
            assert_eq!(error.grpc_status().message(), error.message());
            let value: serde_json::Value = serde_json::from_slice(&error.json_body()).unwrap();
            assert_eq!(
                value,
                serde_json::json!({"error":{"code":error.code(),"message":error.message()}})
            );
            assert!(error.json_body().len() < 256);
        }
    }
}
