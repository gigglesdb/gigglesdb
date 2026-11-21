//! Native wire framing for protocol version 1.
//!
//! The 8-byte preface is `GGLN` followed by big-endian u16 major and minor
//! versions. A frame starts with a 12-byte header: big-endian u32 length of
//! everything after that field, u8 kind, u8 flags (MORE and SKIPPED), zero
//! u16 reserved, and big-endian u32 request ID. The remaining bytes are the
//! body, limited to [`crate::MAX_FRAME`].
//!
//! The decoder validates a complete header before reserving any space based
//! on its length. Each such reservation is bounded by one maximum-sized
//! frame with its header. Callers control bytes appended through
//! [`FrameDecoder::read_buf`] and must bound their socket reads independently.

use std::ops::Range;

use bytes::BytesMut;

pub const PREFACE_MAGIC: [u8; 4] = *b"GGLN";
pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 0;
pub const PREFACE_LEN: usize = 8;
pub const HEADER_LEN: usize = 12;

const HEADER_AFTER_LENGTH: usize = HEADER_LEN - 4;
const RETAINED_CAPACITY: usize = 4096;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    Hello = 1,
    Auth = 2,
    Ready = 3,
    Execute = 4,
    Prepare = 5,
    Prepared = 6,
    Run = 7,
    CloseHandle = 8,
    Batch = 9,
    Reply = 10,
    Cancel = 11,
    Goodbye = 12,
    ConnError = 13,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Flags(pub u8);

impl Flags {
    pub const MORE: u8 = 0x01;
    pub const SKIPPED: u8 = 0x02;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    ClientToServer,
    ServerToClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: FrameKind,
    pub flags: Flags,
    pub request_id: u32,
    pub body_len: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    BadLength,
    TooLarge { body_len: usize },
    Reserved,
    UnknownFlags(u8),
    UnknownKind(u8),
    WrongDirection(FrameKind),
}

#[derive(Debug, PartialEq, Eq)]
pub enum PrefaceError {
    BadMagic,
    UnsupportedVersion { major: u16, minor: u16 },
}

/// Accepts any minor version within the supported major version.
pub fn parse_preface(bytes: &[u8; PREFACE_LEN]) -> Result<(u16, u16), PrefaceError> {
    if bytes[..4] != PREFACE_MAGIC {
        return Err(PrefaceError::BadMagic);
    }
    let major = u16::from_be_bytes([bytes[4], bytes[5]]);
    let minor = u16::from_be_bytes([bytes[6], bytes[7]]);
    if major != PROTOCOL_MAJOR {
        return Err(PrefaceError::UnsupportedVersion { major, minor });
    }
    Ok((major, minor))
}

/// Appends this implementation's protocol preface.
pub fn write_preface(out: &mut Vec<u8>) {
    out.extend_from_slice(&PREFACE_MAGIC);
    out.extend_from_slice(&PROTOCOL_MAJOR.to_be_bytes());
    out.extend_from_slice(&PROTOCOL_MINOR.to_be_bytes());
}

fn encode_header(
    out: &mut Vec<u8>,
    kind: FrameKind,
    flags: Flags,
    request_id: u32,
    body_len: usize,
) {
    assert!(body_len <= crate::MAX_FRAME, "frame body exceeds MAX_FRAME");
    out.reserve(HEADER_LEN + body_len);
    let length = (HEADER_AFTER_LENGTH + body_len) as u32;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&[kind as u8, flags.0, 0, 0]);
    out.extend_from_slice(&request_id.to_be_bytes());
}

/// Appends a frame. Panics if the body exceeds [`crate::MAX_FRAME`].
pub fn encode_frame(
    out: &mut Vec<u8>,
    kind: FrameKind,
    flags: Flags,
    request_id: u32,
    body: &[u8],
) {
    encode_header(out, kind, flags, request_id, body.len());
    out.extend_from_slice(body);
}

/// Appends a protobuf message as a frame. Panics if the encoded body exceeds
/// [`crate::MAX_FRAME`].
pub fn encode_message<M: prost::Message>(
    out: &mut Vec<u8>,
    kind: FrameKind,
    flags: Flags,
    request_id: u32,
    message: &M,
) {
    encode_header(out, kind, flags, request_id, message.encoded_len());
    message
        .encode(out)
        .expect("frame body capacity was reserved");
}

/// Incremental decoder for one direction of a connection.
///
/// A framing error ends decoding for this instance. The caller must close
/// the connection when [`Self::next_frame`] returns an error.
pub struct FrameDecoder {
    buffer: BytesMut,
    read_offset: usize,
    max_body: usize,
    direction: Direction,
    failed: bool,
}

impl FrameDecoder {
    /// Creates a decoder with a body limit no greater than [`crate::MAX_FRAME`].
    pub fn new(direction: Direction, max_body: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(HEADER_LEN),
            read_offset: 0,
            max_body: max_body.min(crate::MAX_FRAME),
            direction,
            failed: false,
        }
    }

    /// Returns the buffer for appending bytes. Call `next_frame` after each
    /// read to validate headers before making any length-based reads.
    pub fn read_buf(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Returns the next frame and its body's indices from the start of the
    /// buffer. Advancing to the next frame leaves previous ranges valid until
    /// `consume_decoded` discards their bytes. Returns `Ok(None)` when more
    /// bytes are needed or decoding has ended after an earlier error.
    pub fn next_frame(&mut self) -> Result<Option<(FrameHeader, Range<usize>)>, FrameError> {
        if self.failed {
            return Ok(None);
        }
        let unread = &self.buffer[self.read_offset..];
        let Some(bytes) = unread.first_chunk::<HEADER_LEN>() else {
            return Ok(None);
        };
        let header = match self.decode_header(bytes) {
            Ok(header) => header,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let frame_len = HEADER_LEN + header.body_len;
        if unread.len() < frame_len {
            // At offset zero, an exact reservation avoids geometric growth.
            // Otherwise the caller can reclaim the returned frames first.
            if self.read_offset == 0 && self.buffer.capacity() < frame_len {
                let mut buffer = BytesMut::with_capacity(frame_len);
                buffer.extend_from_slice(&self.buffer);
                self.buffer = buffer;
            }
            return Ok(None);
        }

        let body_start = self.read_offset + HEADER_LEN;
        self.read_offset += frame_len;
        Ok(Some((header, body_start..self.read_offset)))
    }

    /// Borrows the body identified by a range from `next_frame`.
    pub fn body(&self, range: Range<usize>) -> &[u8] {
        &self.buffer[range]
    }

    /// Drops the bytes of all returned frames and invalidates their ranges.
    pub fn consume_decoded(&mut self) {
        if self.read_offset == 0 {
            return;
        }
        let remaining = self.buffer.len() - self.read_offset;
        if self.buffer.capacity() > RETAINED_CAPACITY && remaining <= RETAINED_CAPACITY {
            let mut buffer = BytesMut::with_capacity(remaining.max(HEADER_LEN));
            buffer.extend_from_slice(&self.buffer[self.read_offset..]);
            self.buffer = buffer;
        } else {
            self.buffer.copy_within(self.read_offset.., 0);
            self.buffer.truncate(remaining);
        }
        self.read_offset = 0;
    }

    fn decode_header(&self, bytes: &[u8; HEADER_LEN]) -> Result<FrameHeader, FrameError> {
        let length = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        let body_len = length
            .checked_sub(HEADER_AFTER_LENGTH)
            .ok_or(FrameError::BadLength)?;
        if body_len > self.max_body {
            return Err(FrameError::TooLarge { body_len });
        }
        if bytes[6..8] != [0, 0] {
            return Err(FrameError::Reserved);
        }
        let flags = Flags(bytes[5]);
        if flags.0 & !(Flags::MORE | Flags::SKIPPED) != 0 {
            return Err(FrameError::UnknownFlags(flags.0));
        }
        let kind = match bytes[4] {
            1 => FrameKind::Hello,
            2 => FrameKind::Auth,
            3 => FrameKind::Ready,
            4 => FrameKind::Execute,
            5 => FrameKind::Prepare,
            6 => FrameKind::Prepared,
            7 => FrameKind::Run,
            8 => FrameKind::CloseHandle,
            9 => FrameKind::Batch,
            10 => FrameKind::Reply,
            11 => FrameKind::Cancel,
            12 => FrameKind::Goodbye,
            13 => FrameKind::ConnError,
            unknown => return Err(FrameError::UnknownKind(unknown)),
        };
        if !matches!(
            (self.direction, kind),
            (_, FrameKind::Auth)
                | (
                    Direction::ClientToServer,
                    FrameKind::Hello
                        | FrameKind::Execute
                        | FrameKind::Prepare
                        | FrameKind::Run
                        | FrameKind::CloseHandle
                        | FrameKind::Batch
                        | FrameKind::Cancel
                        | FrameKind::Goodbye
                )
                | (
                    Direction::ServerToClient,
                    FrameKind::Ready
                        | FrameKind::Prepared
                        | FrameKind::Reply
                        | FrameKind::ConnError
                )
        ) {
            return Err(FrameError::WrongDirection(kind));
        }
        let request_id = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        Ok(FrameHeader {
            kind,
            flags,
            request_id,
            body_len,
        })
    }
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;
