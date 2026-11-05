use crate::{
    ClientError, Config, Denial, Operation, Outcome, Recorded, UnknownReason,
    transport::{Failure, Tls, Transport},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep_until, timeout_at},
};

/// Clone shares one bounded admission pool. Calls spawn no background work.
#[derive(Clone)]
pub struct Client {
    config: Config,
    transport: Arc<dyn Transport>,
    permits: Arc<Semaphore>,
}
impl Client {
    pub fn new(config: Config) -> Result<Self, ClientError> {
        config.validate()?;
        let transport = Arc::new(Tls::new(&config)?);
        Ok(Self::with_transport(config, transport))
    }
    fn with_transport(config: Config, transport: Arc<dyn Transport>) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(config.limits.max_in_flight)),
            config,
            transport,
        }
    }
    pub async fn preview(
        &self,
        id: u64,
        request: crate::Request,
        deadline_ms: u64,
    ) -> Result<Outcome, ClientError> {
        Ok(self
            .send(&Operation::preview(id, request, deadline_ms)?)
            .await)
    }
    pub async fn execute(
        &self,
        id: u64,
        request: crate::Request,
        approval: Option<crate::ApprovalId>,
        deadline_ms: u64,
    ) -> Result<Outcome, ClientError> {
        Ok(self
            .send(&Operation::execute(id, request, approval, deadline_ms)?)
            .await)
    }
    pub async fn status(
        &self,
        id: u64,
        operation: crate::OperationId,
        deadline_ms: u64,
    ) -> Result<Outcome, ClientError> {
        Ok(self
            .send(&Operation::status(id, operation, deadline_ms)?)
            .await)
    }
    /// Execute retries only connection failures before send. String server errors
    /// cannot establish a structured predecision outcome for an execute.
    /// A call cancelled after sending may have committed. Retain the input ID.
    pub async fn send(&self, operation: &Operation) -> Outcome {
        let limits = &self.config.limits;
        let deadline = Instant::now() + limits.operation;
        let Ok(Ok(_permit)) = timeout_at(deadline, self.permits.acquire()).await else {
            return Outcome::TimeoutBeforeSend;
        };
        let unknown = |reason| Outcome::Unknown {
            operation_id: operation.operation_id,
            reason,
        };
        let mut last_timeout = false;
        let mut ever_sent = false;
        for attempt in 0..limits.max_attempts {
            last_timeout = false;
            if attempt > 0 {
                let wake = Instant::now() + limits.backoff;
                if wake >= deadline {
                    return if ever_sent {
                        unknown(UnknownReason::Timeout)
                    } else {
                        Outcome::TimeoutBeforeSend
                    };
                }
                sleep_until(wake).await;
            }
            if Instant::now() >= deadline {
                return if ever_sent {
                    unknown(UnknownReason::Timeout)
                } else {
                    Outcome::TimeoutBeforeSend
                };
            }
            let sent = AtomicBool::new(false);
            let request_deadline = deadline.min(
                Instant::now()
                    + limits
                        .request
                        .min(std::time::Duration::from_millis(operation.deadline_ms)),
            );
            let result = timeout_at(
                request_deadline,
                self.transport.exchange(
                    self.config.endpoints[attempt % self.config.endpoints.len()],
                    operation,
                    request_deadline,
                    limits.connect,
                    &sent,
                ),
            )
            .await
            .unwrap_or(Err(Failure::Timeout));
            ever_sent |= sent.load(Ordering::Acquire);
            match result {
                Ok(response) => {
                    if response.version != 1
                        || response.id != operation.id
                        || response.error.is_some() == response.reply.is_some()
                    {
                        return if operation.execute {
                            unknown(UnknownReason::Protocol)
                        } else {
                            Outcome::Protocol
                        };
                    }
                    if let Some(reply) = response.reply {
                        let parsed = serde_json::from_value::<giggles_core::replica::NativeResponse>(
                            reply.data.clone(),
                        );
                        let Ok(result) = parsed else {
                            return if operation.execute {
                                unknown(UnknownReason::Protocol)
                            } else {
                                Outcome::Protocol
                            };
                        };
                        if reply.version != 1 || reply.code != 0 || result.version != 1 {
                            return if operation.execute {
                                unknown(UnknownReason::Protocol)
                            } else {
                                Outcome::Protocol
                            };
                        }
                        // Release failures deliberately omit operation binding. They may
                        // follow a durable decision regardless of their string code.
                        if result.operation.is_none() {
                            return if operation.execute {
                                unknown(UnknownReason::Withheld)
                            } else {
                                Outcome::Denied(if result.code == "28000" {
                                    Denial::Authentication
                                } else {
                                    Denial::Policy
                                })
                            };
                        }
                        if result.operation != Some(operation.operation_id)
                            || !matches!(
                                result.code.as_str(),
                                "preview"
                                    | "committed"
                                    | "absent"
                                    | "28000"
                                    | "42501"
                                    | "55000"
                                    | "54000"
                                    | "GD001"
                                    | "GD002"
                                    | "22023"
                                    | "57014"
                                    | "53300"
                                    | "08007"
                                    | "XX000"
                            )
                        {
                            return if operation.execute {
                                unknown(UnknownReason::Protocol)
                            } else {
                                Outcome::Protocol
                            };
                        }
                        return Outcome::Recorded(Recorded(reply));
                    }
                    match response.error.as_deref() {
                        Some("unavailable" | "not_leader" | "capacity") => {
                            if operation.execute {
                                return unknown(UnknownReason::ServerIndeterminate);
                            }
                        }
                        Some("authentication") if !operation.execute => {
                            return Outcome::Denied(Denial::Authentication);
                        }
                        Some("indeterminate") => {
                            return unknown(UnknownReason::ServerIndeterminate);
                        }
                        // Unknown server errors can include authority/release failures.
                        _ => {
                            return if operation.execute {
                                unknown(UnknownReason::Withheld)
                            } else {
                                Outcome::Denied(Denial::Policy)
                            };
                        }
                    }
                }
                Err(error) => {
                    last_timeout = matches!(error, Failure::Timeout);
                    if sent.load(Ordering::Acquire) {
                        return unknown(match error {
                            Failure::Timeout => UnknownReason::Timeout,
                            Failure::Protocol => UnknownReason::Protocol,
                            _ => UnknownReason::Transport,
                        });
                    }
                    match error {
                        Failure::Tls => return Outcome::TlsRejected,
                        Failure::Protocol => return Outcome::Protocol,
                        Failure::Timeout if Instant::now() >= deadline => {
                            return if ever_sent {
                                unknown(UnknownReason::Timeout)
                            } else {
                                Outcome::TimeoutBeforeSend
                            };
                        }
                        _ => {}
                    }
                }
            }
        }
        if last_timeout {
            if ever_sent {
                unknown(UnknownReason::Timeout)
            } else {
                Outcome::TimeoutBeforeSend
            }
        } else {
            Outcome::Unavailable
        }
    }
}

#[cfg(test)]
mod tests;
