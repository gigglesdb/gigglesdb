use super::error::ApiError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub(crate) const MAX_CALLS: usize = 128;
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const MAX_AGE: Duration = Duration::from_secs(3600);
pub(crate) const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiLimits {
    pub sessions: usize,
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self { sessions: 32 }
    }
}

impl ApiLimits {
    pub fn validate(self) -> Result<Self, ApiError> {
        if (1..=128).contains(&self.sessions) {
            Ok(self)
        } else {
            Err(ApiError::InvalidArgument)
        }
    }
}
