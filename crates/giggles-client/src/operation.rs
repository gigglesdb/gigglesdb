use crate::{ApprovalId, ClientError, OperationId, Request};
use giggles_consensus::{TaskAction, native};
use std::{io::Write, sync::Arc};

/// Immutable request bytes reused unchanged across safe retries. No generated IDs.
#[derive(Clone)]
pub struct Operation {
    pub(crate) bytes: Arc<[u8]>,
    pub(crate) id: u64,
    pub(crate) operation_id: OperationId,
    pub(crate) execute: bool,
    pub(crate) deadline_ms: u64,
}
impl std::fmt::Debug for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Operation([redacted])")
    }
}
struct Bounded(Vec<u8>);
impl Write for Bounded {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if b.len() > native::MAX_FRAME.saturating_sub(self.0.len()) {
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Operation {
    pub fn preview(id: u64, request: Request, deadline_ms: u64) -> Result<Self, ClientError> {
        Self::from_request(native::Request {
            version: 1,
            id,
            deadline_ms,
            action: TaskAction::Preview(request),
        })
    }
    pub fn execute(
        id: u64,
        request: Request,
        approval: Option<ApprovalId>,
        deadline_ms: u64,
    ) -> Result<Self, ClientError> {
        Self::from_request(native::Request {
            version: 1,
            id,
            deadline_ms,
            action: TaskAction::Execute(request, approval),
        })
    }
    pub fn status(id: u64, operation: OperationId, deadline_ms: u64) -> Result<Self, ClientError> {
        Self::from_request(native::Request {
            version: 1,
            id,
            deadline_ms,
            action: TaskAction::Status(operation),
        })
    }
    pub fn from_request(request: native::Request) -> Result<Self, ClientError> {
        if request.version != 1 || !(1..=native::MAX_DEADLINE_MS).contains(&request.deadline_ms) {
            return Err(ClientError::InvalidOperation);
        }
        let operation_id = match &request.action {
            TaskAction::Preview(r) | TaskAction::Execute(r, _) => r.operation,
            TaskAction::Status(id) => *id,
        };
        let execute = matches!(&request.action, TaskAction::Execute(..));
        let mut writer = Bounded(Vec::new());
        serde_json::to_writer(&mut writer, &request).map_err(|_| ClientError::OversizeRequest)?;
        native::parse(&writer.0).map_err(|_| ClientError::InvalidOperation)?;
        Ok(Self {
            bytes: writer.0.into(),
            id: request.id,
            operation_id,
            execute,
            deadline_ms: request.deadline_ms,
        })
    }
    pub fn operation_id(&self) -> OperationId {
        self.operation_id
    }
    /// Canonical protocol request; explicit access may reveal application payload.
    pub fn wire_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
