//! Fixed gRPC statuses that never copy upstream data.
use crate::error::NyCode;
use tonic::{Code, Status};
/// Replace an upstream code with a fixed message and empty details and metadata.
pub fn redact_status(code: Code) -> Status {
    Status::new(
        code,
        match code {
            Code::Ok => "success",
            Code::Cancelled => "request cancelled",
            Code::Unknown => "upstream error",
            Code::InvalidArgument => "invalid argument",
            Code::DeadlineExceeded => "request deadline exceeded",
            Code::NotFound => "row not found",
            Code::AlreadyExists => "row already exists",
            Code::PermissionDenied => "permission denied",
            Code::ResourceExhausted => "resources exhausted",
            Code::FailedPrecondition => "precondition failed",
            Code::Aborted => "transaction aborted",
            Code::OutOfRange => "value out of range",
            Code::Unimplemented => "operation not implemented",
            Code::Internal => "internal upstream error",
            Code::Unavailable => "upstream unavailable",
            Code::DataLoss => "upstream data loss",
            Code::Unauthenticated => "authentication required",
        },
    )
}
/// Map a Nunya refusal to its fixed public gRPC status.
pub fn nunya_status(code: NyCode) -> Status {
    let grpc = match code {
        NyCode::SessionEnded => Code::PermissionDenied,
        NyCode::WriteOutcomeUnknown => Code::Unknown,
        NyCode::NotAnalysable => Code::InvalidArgument,
        NyCode::SchemaStale | NyCode::ReadLogUnavailable => Code::FailedPrecondition,
        NyCode::TokenOutsideDomain
        | NyCode::BlindOperation
        | NyCode::TokenNotValid
        | NyCode::MaskedExpression
        | NyCode::StatementNotAllowed
        | NyCode::TokenizeLimit
        | NyCode::ProtocolNotAllowed => Code::PermissionDenied,
    };
    Status::new(grpc, format!("{}: {}", code.sqlstate(), code.message()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redacts() {
        for n in 0..=16 {
            let original = Status::new(Code::from_i32(n), "private upstream text");
            let status = redact_status(original.code());
            assert!(!status.message().contains("private"));
            assert!(status.details().is_empty());
            assert!(status.metadata().is_empty());
        }
    }
    #[test]
    fn refusals() {
        for code in NyCode::ALL {
            let s = nunya_status(code);
            assert!(!matches!(
                s.code(),
                Code::Aborted
                    | Code::Unavailable
                    | Code::ResourceExhausted
                    | Code::Internal
                    | Code::NotFound
                    | Code::DeadlineExceeded
            ));
            if matches!(code, NyCode::SchemaStale | NyCode::ReadLogUnavailable) {
                assert_eq!(s.code(), Code::FailedPrecondition);
            }
            assert_eq!(
                s.message(),
                format!("{}: {}", code.sqlstate(), code.message())
            );
        }
    }
}
