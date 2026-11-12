//! Incremental PostgreSQL tag/length envelopes, separate from pgoutput parsing.
//!
//! Own this buffer across cancelled network reads. Push only bytes that a read
//! has completed, retain any unconsumed input, and drain a complete frame before
//! reading further. One frame is buffered, so a coalesced network read cannot
//! defeat the bound. Startup/SSL negotiation packets are outside this API.

use super::pgoutput::{DecodeError, Limits, Result, XLOG_DATA_HEADER_BYTES, limit};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendFrame {
    pub tag: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct FrameBuffer {
    bytes: Vec<u8>,
    maximum_payload: usize,
    poisoned: bool,
}

impl Default for FrameBuffer {
    fn default() -> Self {
        Self {
            bytes: Vec::new(),
            maximum_payload: Limits::default().message_bytes + XLOG_DATA_HEADER_BYTES,
            poisoned: false,
        }
    }
}

impl FrameBuffer {
    pub fn new(limits: Limits) -> Result<Self> {
        let limits = limits.validate()?;
        Ok(Self {
            maximum_payload: limits.message_bytes + XLOG_DATA_HEADER_BYTES,
            ..Self::default()
        })
    }

    pub fn buffered_len(&self) -> usize {
        self.bytes.len()
    }

    fn frame_len(&self) -> Result<Option<usize>> {
        if self.poisoned {
            return Err(DecodeError::Invalid {
                offset: 0,
                field: "poisoned frame buffer; reconnect required",
            });
        }
        if self.bytes.len() < 5 {
            return Ok(None);
        }
        let mut encoded = [0; 4];
        encoded.copy_from_slice(&self.bytes[1..5]);
        let length = u32::from_be_bytes(encoded) as usize;
        if length < 4 {
            return Err(DecodeError::Invalid {
                offset: 1,
                field: "backend frame length below four",
            });
        }
        limit(
            "backend frame payload bytes",
            length - 4,
            self.maximum_payload,
        )?;
        Ok(Some(length + 1))
    }

    /// Consumes up to one frame, returning the number of input bytes accepted.
    /// A complete buffered frame returns zero. Header errors poison this buffer;
    /// discard it with the connection rather than guessing a framing boundary.
    pub fn push(&mut self, input: &[u8]) -> Result<usize> {
        let result = self.push_inner(input);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn push_inner(&mut self, input: &[u8]) -> Result<usize> {
        self.frame_len()?;
        let header_bytes = (5 - self.bytes.len().min(5)).min(input.len());
        self.bytes.extend_from_slice(&input[..header_bytes]);
        let Some(total) = self.frame_len()? else {
            return Ok(header_bytes);
        };
        let body_bytes = (total - self.bytes.len()).min(input.len() - header_bytes);
        self.bytes
            .extend_from_slice(&input[header_bytes..header_bytes + body_bytes]);
        Ok(header_bytes + body_bytes)
    }

    pub fn next_frame(&mut self) -> Result<Option<BackendFrame>> {
        let Some(total) = self.frame_len()? else {
            return Ok(None);
        };
        if self.bytes.len() < total {
            return Ok(None);
        }
        let tag = self.bytes[0];
        // Move the allocation to the frame; no second payload allocation/copy.
        let mut payload = std::mem::take(&mut self.bytes);
        payload.drain(..5);
        Ok(Some(BackendFrame { tag, payload }))
    }

    /// At network EOF, a partial header or body is an error, not a clean close.
    /// Drain complete frames first.
    pub fn finish(&self) -> Result<()> {
        let total = self.frame_len()?.unwrap_or(5);
        if self.bytes.is_empty() {
            return Ok(());
        }
        if total == self.bytes.len() {
            return Err(DecodeError::Invalid {
                offset: 0,
                field: "drain complete frame before checking EOF",
            });
        }
        Err(DecodeError::Truncated {
            offset: self.bytes.len(),
            needed: total - self.bytes.len(),
            remaining: 0,
        })
    }
}
