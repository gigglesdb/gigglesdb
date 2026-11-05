use crate::{Operation, OperationId};
use giggles_core::local::Reply;

/// Fixed local error classes never include peer data, keys, or request payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientError {
    InvalidConfig,
    Credentials,
    InvalidOperation,
    OversizeRequest,
}
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ClientError {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denial {
    Authentication,
    Policy,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnknownReason {
    Transport,
    Timeout,
    Protocol,
    ServerIndeterminate,
    Withheld,
}
/// Server-released result. Debug hides the payload; inspect explicitly.
pub struct Recorded(pub(crate) Reply);
impl Recorded {
    pub fn reply(&self) -> &Reply {
        &self.0
    }
    pub fn into_reply(self) -> Reply {
        self.0
    }
    pub fn is_committed(&self) -> bool {
        self.0.code == 0 && self.0.data["code"] == "committed"
    }
    /// Decode the published redacted task result, including its operation binding.
    pub fn task_result(&self) -> giggles_core::replica::NativeResponse {
        // Client validates this exact type before constructing Recorded.
        serde_json::from_value(self.0.data.clone()).expect("validated native result")
    }
}
impl std::fmt::Debug for Recorded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Recorded([redacted])")
    }
}
#[derive(Debug)]
pub enum Outcome {
    /// A released result, including preview, absent status, or recorded rejection.
    Recorded(Recorded),
    Denied(Denial),
    Unavailable,
    /// No application bytes were sent; includes admission/backoff timeout.
    TimeoutBeforeSend,
    /// TLS verification/handshake failed before application data was sent.
    TlsRejected,
    Protocol,
    Unknown {
        operation_id: OperationId,
        reason: UnknownReason,
    },
}
impl Outcome {
    /// Build a status query for an ambiguous execution, retaining its business ID.
    pub fn resolution(
        &self,
        correlation_id: u64,
        deadline_ms: u64,
    ) -> Result<Option<Operation>, crate::ClientError> {
        match self {
            Self::Unknown { operation_id, .. } => {
                Operation::status(correlation_id, *operation_id, deadline_ms).map(Some)
            }
            _ => Ok(None),
        }
    }
}
