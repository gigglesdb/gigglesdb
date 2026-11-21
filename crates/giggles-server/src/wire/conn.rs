//! Connection-bound sessions for the native wire.
//!
//! Transport errors use CONN_ERROR with the request ID. Lost authentication
//! or a missing session closes the connection after that reply. Framing and
//! sign-in or idle session failures use request ID zero and terminate the connection.

use super::{
    exec::run_call,
    frame::{
        Direction, Flags, FrameDecoder, FrameHeader, FrameKind, HEADER_LEN, PREFACE_LEN,
        PROTOCOL_MAJOR, PrefaceError, encode_frame, encode_message, parse_preface,
    },
    messages::{WireCancel, WireConnError, WireReady, WireReply},
};
use crate::{
    MAX_FRAME, MAX_RESPONSE,
    api::{
        ApiRuntime, CallOwnership, Completion, ConnectionIdentity, ResponseLease,
        codec::{
            self,
            wire_seam::{SpillChunks, WireCall, WireSession, spill_chunks},
        },
        error::ApiError,
        proto,
    },
    transport::IO_TIMEOUT,
};
use bytes::BufMut;
use giggles_core::Cancellation;
use prost::Message;
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{self as tokio_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::OwnedSemaphorePermit,
    time::{MissedTickBehavior, interval_at, sleep_until, timeout},
};
use tonic::{Code, Status, codegen::tokio_stream::StreamExt};

#[cfg(feature = "test-hooks")]
use std::sync::atomic::AtomicBool;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

const READ_CHUNK: usize = 8 * 1024;
const LOOKAHEAD_BYTES: usize = 1024 * 1024 + MAX_FRAME;
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const RETAIN_CAPACITY: usize = 64 * 1024;
const REPLY_BATCH_GAP: Duration = Duration::from_millis(1);
const MAX_AUTH_ROUNDS: usize = 4;

pub(crate) struct WireLimits {
    pub max_in_flight: usize,
    pub max_pending_reply_bytes: usize,
    pub max_handles: usize,
    pub max_shapes: usize,
}

impl Default for WireLimits {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            max_pending_reply_bytes: 2 * MAX_RESPONSE,
            max_handles: 256,
            max_shapes: 256,
        }
    }
}

pub(crate) async fn serve_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    stream: S,
    runtime: Arc<ApiRuntime>,
    identity: Result<ConnectionIdentity, ApiError>,
    limits: WireLimits,
    socket: Arc<OwnedSemaphorePermit>,
    #[cfg(feature = "test-hooks")] stats: Arc<ConnectionStats>,
) -> io::Result<()> {
    serve(
        stream,
        runtime,
        identity,
        limits,
        socket,
        #[cfg(all(test, not(feature = "test-hooks")))]
        Arc::new(ConnectionStats::default()),
        #[cfg(feature = "test-hooks")]
        stats,
        #[cfg(test)]
        None,
    )
    .await
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
pub struct ConnectionStats {
    #[cfg(test)]
    pub(super) decoder_len: AtomicUsize,
    #[cfg(test)]
    pub(super) peak_decoder_len: AtomicUsize,
    #[cfg(test)]
    pub(super) read_bytes: AtomicUsize,
    #[cfg(test)]
    pub(super) in_flight: AtomicUsize,
    #[cfg(test)]
    pub(super) pending_bytes: AtomicUsize,
    #[cfg(test)]
    pub(super) write_capacity: AtomicUsize,
    #[cfg(feature = "test-hooks")]
    peak_pending_bytes: AtomicUsize,
    #[cfg(feature = "test-hooks")]
    peak_in_flight: AtomicUsize,
    #[cfg(feature = "test-hooks")]
    queued: AtomicUsize,
    #[cfg(feature = "test-hooks")]
    hold_queue: AtomicBool,
    #[cfg(feature = "test-hooks")]
    hold_idle_checks: AtomicBool,
    #[cfg(feature = "test-hooks")]
    hold_after_more: AtomicBool,
}

#[cfg(feature = "test-hooks")]
impl ConnectionStats {
    pub fn peak_pending_reply_bytes(&self) -> usize {
        self.peak_pending_bytes.load(Relaxed)
    }

    pub fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Relaxed)
    }

    pub fn queued_requests(&self) -> usize {
        self.queued.load(Relaxed)
    }

    /// Leaves requests queued until the test drops the connection.
    pub fn hold_queued_requests(&self) {
        self.hold_queue.store(true, Relaxed);
    }

    /// Lets a test exercise per-request revocation after the binding changes.
    pub fn hold_idle_checks(&self) {
        self.hold_idle_checks.store(true, Relaxed);
    }

    /// Stops the next multi-frame reply after its first MORE frame. The
    /// connection still writes that frame and reads the client's CANCEL.
    pub fn hold_reply_after_first_more(&self) {
        self.hold_after_more.store(true, Relaxed);
    }
}

pub(super) async fn serve<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    runtime: Arc<ApiRuntime>,
    identity: Result<ConnectionIdentity, ApiError>,
    limits: WireLimits,
    socket: Arc<OwnedSemaphorePermit>,
    #[cfg(any(test, feature = "test-hooks"))] stats: Arc<ConnectionStats>,
    #[cfg(test)] hook: Option<Arc<super::exec::CallHook>>,
) -> io::Result<()> {
    if limits.max_in_flight == 0 || limits.max_pending_reply_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "zero wire connection limit",
        ));
    }
    let (session, ready) = match handshake(&mut stream, &runtime, identity, &limits).await {
        Ok(opened) => opened,
        Err(HandshakeError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(());
        }
        Err(HandshakeError::Io(error)) => return Err(error),
        Err(error) => return write_output(&mut stream, &error.into_frame()).await,
    };
    let session = Session {
        runtime: &runtime,
        value: Some(session),
    };
    write_output(&mut stream, &ready).await?;
    connection_loop(
        stream,
        session,
        &limits,
        &socket,
        #[cfg(any(test, feature = "test-hooks"))]
        &stats,
        #[cfg(test)]
        hook.as_deref(),
    )
    .await
}

pub(super) enum HandshakeError {
    Io(io::Error),
    Protocol(&'static str, bool),
    SignIn(ApiError),
}

impl From<io::Error> for HandshakeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl HandshakeError {
    #[cfg(test)]
    pub(super) fn is_protocol(&self) -> bool {
        matches!(self, Self::Protocol(..))
    }

    fn into_frame(self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Protocol(message, version) => {
                append_error(&mut out, 0, "protocol", message, version);
            }
            Self::SignIn(error) => append_api_error(&mut out, 0, error),
            Self::Io(_) => unreachable!("I/O errors cannot be sent as handshake frames"),
        }
        out
    }
}

pub(super) async fn read_handshake<R: AsyncRead + Unpin, M: Message + Default>(
    reader: &mut R,
    expected: FrameKind,
) -> Result<(u32, M), HandshakeError> {
    timeout(IO_TIMEOUT, async {
        // Read exactly one frame so a pipelined request stays on the socket.
        let mut header = [0; HEADER_LEN];
        reader.read_exact(&mut header).await?;
        let mut decoder = FrameDecoder::new(Direction::ClientToServer, MAX_FRAME);
        decoder.read_buf().extend_from_slice(&header);
        let invalid = || HandshakeError::Protocol("invalid handshake frame", false);
        let frame = decoder.next_frame().map_err(|_| invalid())?;
        if header[4] != expected as u8 || header[5] != 0 || header[8..] == [0; 4] {
            return Err(HandshakeError::Protocol(
                "unexpected handshake frame",
                false,
            ));
        }
        let (header, body) = match frame {
            Some(frame) => frame,
            None => {
                // next_frame validated the header and reserved for its body.
                let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize - 8;
                let mut body = reader.take(len as u64);
                while body.limit() != 0 {
                    if body.read_buf(decoder.read_buf()).await? == 0 {
                        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
                    }
                }
                decoder
                    .next_frame()
                    .map_err(|_| invalid())?
                    .ok_or_else(invalid)?
            }
        };
        let message = M::decode(decoder.body(body)).map_err(|_| invalid())?;
        Ok((header.request_id, message))
    })
    .await
    .map_err(io::Error::from)?
}

async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    runtime: &ApiRuntime,
    identity: Result<ConnectionIdentity, ApiError>,
    limits: &WireLimits,
) -> Result<(WireSession, Vec<u8>), HandshakeError> {
    let mut preface = [0; PREFACE_LEN];
    timeout(IO_TIMEOUT, stream.read_exact(&mut preface))
        .await
        .map_err(io::Error::from)??;
    parse_preface(&preface).map_err(|error| match error {
        PrefaceError::BadMagic => HandshakeError::Protocol("invalid wire preface", false),
        PrefaceError::UnsupportedVersion { .. } => {
            HandshakeError::Protocol("unsupported wire version", true)
        }
    })?;
    let (mut id, mut request) = read_handshake(stream, FrameKind::Hello).await?;
    let identity = identity.map_err(HandshakeError::SignIn)?;
    for round in 0..=MAX_AUTH_ROUNDS {
        let (reply, session) = runtime
            .open_wire(&identity, &request)
            .map_err(HandshakeError::SignIn)?;
        let mut out = Vec::new();
        if let Some(session) = session {
            let ready = WireReady {
                session: Some(reply),
                max_in_flight: limits.max_in_flight as u32,
                max_frame: MAX_FRAME as u32,
                max_handles: limits.max_handles as u32,
                max_shapes: limits.max_shapes as u32,
            };
            encode_message(&mut out, FrameKind::Ready, Flags(0), id, &ready);
            return Ok((session, out));
        }
        if round == MAX_AUTH_ROUNDS {
            return Err(HandshakeError::Protocol(
                "too many authentication rounds",
                false,
            ));
        }
        let challenge = reply
            .scram_challenge
            .ok_or(HandshakeError::SignIn(ApiError::Internal))?;
        encode_message(&mut out, FrameKind::Auth, Flags(0), id, &challenge);
        write_output(stream, &out).await?;
        let (auth_id, response) = read_handshake(stream, FrameKind::Auth).await?;
        id = auth_id;
        request = proto::OpenSessionRequest {
            credential: Some(proto::open_session_request::Credential::ScramContinue(
                response,
            )),
            ..Default::default()
        };
    }
    unreachable!("last authentication round returns")
}

struct Session<'a> {
    runtime: &'a ApiRuntime,
    value: Option<WireSession>,
}

impl Session<'_> {
    fn check_idle(&mut self) -> Result<(), ApiError> {
        let session = self.value.as_mut().expect("open session");
        self.runtime.wire_recheck(session)?;
        if !self.runtime.wire_session_alive(session) {
            return Err(ApiError::SessionNotFound);
        }
        Ok(())
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if let Some(session) = self.value.take() {
            self.runtime.close_wire(session);
        }
    }
}

struct Queued {
    id: u32,
    call: Result<WireCall, ApiError>,
}

struct Deferred {
    header: FrameHeader,
    body: Vec<u8>,
    received: Instant,
    cancelled: bool,
}

impl Queued {
    fn decode(runtime: &ApiRuntime, header: FrameHeader, body: &[u8], received: Instant) -> Self {
        let call = if header.kind == FrameKind::Execute {
            decode_call(runtime, body, received)
        } else {
            // Operation handles and batches are implemented in Stack 4.
            Err(ApiError::InvalidArgument)
        };
        Self {
            id: header.request_id,
            call,
        }
    }

    fn cancel(&mut self) {
        if let Ok(call) = &self.call {
            call.cancel.cancel();
        }
        self.call = Err(ApiError::DeadlineExceeded);
    }
}

fn decode_call(runtime: &ApiRuntime, body: &[u8], received: Instant) -> Result<WireCall, ApiError> {
    let request = proto::ExecuteRequest::decode(body).map_err(|_| ApiError::InvalidArgument)?;
    let budget_ms = request.budget_ms;
    let decoded = codec::decode_execute(request)?;
    Ok(WireCall {
        request: decoded.request,
        operation_id: decoded.operation_id,
        budget_ms,
        received,
        cancel: runtime.wire_cancellation(received, budget_ms)?,
    })
}

struct ActiveCall {
    id: u32,
    cancel: Cancellation,
    _ownership: Arc<CallOwnership>,
}

struct Delivery {
    call: ActiveCall,
    lease: ResponseLease,
    chunks: Option<SpillChunks<Result<proto::ExecuteReply, Status>>>,
    part: Vec<u8>,
    offset: usize,
    next: Option<Vec<u8>>,
}

impl Delivery {
    fn new(call: ActiveCall, completion: Completion) -> Self {
        let Completion {
            reply,
            spill,
            lease,
            ..
        } = completion;
        let (part, chunks) = match spill {
            Some(spill) => (Vec::new(), Some(spill_chunks(reply, Some(spill)))),
            None => (encode_reply(reply), None),
        };
        Self {
            call,
            lease,
            chunks,
            part,
            offset: 0,
            next: None,
        }
    }

    fn can_queue_frame(&self) -> bool {
        self.part.len() - self.offset > MAX_FRAME || self.next.is_some() || self.chunks.is_none()
    }

    fn queue_frame(&mut self, out: &mut Vec<u8>) -> Result<bool, ApiError> {
        self.lease.check_release()?;
        let end = self.part.len().min(self.offset + MAX_FRAME);
        let more = end < self.part.len() || self.next.is_some();
        encode_frame(
            out,
            FrameKind::Reply,
            Flags(if more { Flags::MORE } else { 0 }),
            self.call.id,
            &self.part[self.offset..end],
        );
        self.offset = end;
        if self.offset == self.part.len()
            && let Some(next) = self.next.take()
        {
            self.part = next;
            self.offset = 0;
        }
        Ok(!more)
    }

    async fn read_chunk(&mut self) -> Option<Result<proto::ExecuteReply, Status>> {
        self.chunks.as_mut().expect("active spill").next().await
    }

    fn accept_chunk(&mut self, chunk: Option<proto::ExecuteReply>) {
        if let Some(reply) = chunk {
            let bytes = encode_reply(reply);
            if self.part.is_empty() {
                self.part = bytes;
            } else {
                // One part of lookahead allows the last nonempty frame to end MORE.
                self.next = Some(bytes);
            }
        } else {
            self.chunks = None;
        }
    }
}

fn encode_reply(reply: proto::ExecuteReply) -> Vec<u8> {
    WireReply {
        reply: Some(reply),
        shapes: Vec::new(),
    }
    .encode_to_vec()
}

fn append_api_error(out: &mut Vec<u8>, id: u32, error: ApiError) {
    append_error(out, id, error.code(), error.message(), false);
}

fn append_error(out: &mut Vec<u8>, id: u32, code: &str, message: &str, show_version: bool) {
    encode_message(
        out,
        FrameKind::ConnError,
        Flags(0),
        id,
        &WireConnError {
            code: code.into(),
            message: message.into(),
            supported_major: if show_version {
                vec![u32::from(PROTOCOL_MAJOR)]
            } else {
                vec![]
            },
        },
    );
}

fn request_error(
    session: &mut Option<Session<'_>>,
    out: &mut Vec<u8>,
    ids: &mut HashSet<u32>,
    id: u32,
    error: ApiError,
) -> bool {
    let fatal = matches!(error, ApiError::Unauthenticated | ApiError::SessionNotFound);
    let recheck_error = if fatal {
        None
    } else {
        let session = session.as_mut().expect("open session");
        session
            .runtime
            .wire_recheck(session.value.as_mut().expect("open session"))
            .err()
    };
    append_api_error(out, id, recheck_error.unwrap_or(error));
    ids.remove(&id);
    fatal || recheck_error.is_some()
}

async fn write_output(writer: &mut (impl AsyncWrite + Unpin), out: &[u8]) -> io::Result<()> {
    timeout(IO_TIMEOUT, async {
        writer.write_all(out).await?;
        writer.flush().await
    })
    .await?
}

async fn write_batch<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut out: Vec<u8>,
) -> (W, Vec<u8>, io::Result<()>) {
    let result = write_output(&mut writer, &out).await;
    if out.capacity() > RETAIN_CAPACITY {
        out = Vec::new();
    } else {
        out.clear();
    }
    (writer, out, result)
}

async fn poll_active<F: Future>(future: &mut Option<Pin<Box<F>>>) -> F::Output {
    future.as_mut().expect("active future").await
}

async fn execute<'a>(
    mut session: Session<'a>,
    call: WireCall,
    ownership: Arc<CallOwnership>,
    #[cfg(test)] hook: Option<&super::exec::CallHook>,
) -> (Session<'a>, Result<Completion, ApiError>) {
    #[cfg(test)]
    if let Some(hook) = hook
        && let Err(error) = hook.before(&call.cancel).await
    {
        return (session, Err(error));
    }
    let result = run_call(
        session.runtime,
        session.value.as_mut().expect("open session"),
        call,
        ownership,
    )
    .await;
    (session, result)
}

enum Exit {
    Io(io::Result<()>),
    Goodbye,
    Protocol(&'static str),
    Session(ApiError),
}

async fn connection_loop<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    session: Session<'_>,
    limits: &WireLimits,
    socket: &Arc<OwnedSemaphorePermit>,
    #[cfg(any(test, feature = "test-hooks"))] stats: &ConnectionStats,
    #[cfg(test)] hook: Option<&super::exec::CallHook>,
) -> io::Result<()> {
    let runtime = session.runtime;
    // Keep a separate receiver for waiting. changed() marks its update seen,
    // leaving the session's receiver to drive the actual identity re-check.
    let mut binding_changes = session
        .value
        .as_ref()
        .expect("open session")
        .binding_changes();
    let mut idle_check = interval_at(
        tokio::time::Instant::now() + IDLE_CHECK_INTERVAL,
        IDLE_CHECK_INTERVAL,
    );
    idle_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut session = Some(session);
    let (mut reader, writer) = tokio_io::split(stream);
    let mut writer = Some(writer);
    let mut decoder = FrameDecoder::new(Direction::ClientToServer, MAX_FRAME);
    let mut queued: VecDeque<Queued> = VecDeque::new();
    let mut deferred: VecDeque<Deferred> = VecDeque::new();
    let mut deferred_bytes = 0;
    let mut lookahead_frames = 0;
    // Both admitted and deferred request IDs stay reserved until their reply.
    let mut ids = HashSet::new();
    let mut running = None;
    let mut active: Option<ActiveCall> = None;
    let mut delivery: Option<Delivery> = None;
    let mut pending = Vec::new();
    let mut writing = None;
    let mut writing_bytes = 0;
    let mut last_reply = tokio::time::Instant::now();
    let mut close_after_reply = false;
    #[cfg(feature = "test-hooks")]
    let mut paused_stream = false;

    let exit = 'connection: loop {
        let before = pending.len();
        let below_cap = writing_bytes + pending.len() < limits.max_pending_reply_bytes;
        if !close_after_reply && below_cap {
            loop {
                let slots = limits.max_in_flight - (ids.len() - deferred.len());
                if slots > 0 {
                    for _ in 0..slots {
                        let Some(frame) = deferred.pop_front() else {
                            break;
                        };
                        deferred_bytes -= HEADER_LEN + frame.body.len();
                        let entry = if frame.cancelled {
                            Queued {
                                id: frame.header.request_id,
                                call: Err(ApiError::DeadlineExceeded),
                            }
                        } else {
                            Queued::decode(runtime, frame.header, &frame.body, frame.received)
                        };
                        queued.push_back(entry);
                    }
                    // A freed slot starts a new lookahead window. Held frames
                    // continue to count against it.
                    lookahead_frames = deferred.len();
                }
                if lookahead_frames == limits.max_in_flight {
                    break;
                }
                let (header, body) = match decoder.next_frame() {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(_) => break 'connection Exit::Protocol("invalid frame header"),
                };
                let full = ids.len() - deferred.len() == limits.max_in_flight;
                lookahead_frames += usize::from(full);
                let id = header.request_id;
                if id == 0 || ids.contains(&id) || header.flags.0 != 0 {
                    break 'connection Exit::Protocol("invalid client frame ID or flags");
                }
                match header.kind {
                    FrameKind::Execute
                    | FrameKind::Prepare
                    | FrameKind::Run
                    | FrameKind::CloseHandle
                    | FrameKind::Batch => {
                        ids.insert(id);
                        let received = Instant::now();
                        let body = decoder.body(body);
                        if full {
                            deferred_bytes += HEADER_LEN + body.len();
                            deferred.push_back(Deferred {
                                header,
                                body: body.to_vec(),
                                received,
                                cancelled: false,
                            });
                        } else {
                            queued.push_back(Queued::decode(runtime, header, body, received));
                        }
                    }
                    FrameKind::Cancel => {
                        let Ok(cancel) = WireCancel::decode(decoder.body(body)) else {
                            break 'connection Exit::Protocol("invalid CANCEL frame");
                        };
                        let target = cancel.target_request_id;
                        if let Some(call) = active.as_ref().filter(|call| call.id == target) {
                            // Keep the dispatcher result, including SQLSTATE 57014
                            // when a running SQL statement is cancelled.
                            call.cancel.cancel();
                        } else if delivery
                            .as_ref()
                            .is_some_and(|reply| reply.call.id == target)
                        {
                            let reply = delivery.take().expect("matching delivery");
                            reply.call.cancel.cancel();
                            close_after_reply = request_error(
                                &mut session,
                                &mut pending,
                                &mut ids,
                                target,
                                ApiError::DeadlineExceeded,
                            );
                            // The error frees a slot and may use the last reply space.
                            if close_after_reply
                                || writing_bytes + pending.len() >= limits.max_pending_reply_bytes
                            {
                                break;
                            }
                        } else if let Some(entry) =
                            queued.iter_mut().find(|entry| entry.id == target)
                        {
                            entry.cancel();
                        } else if let Some(frame) =
                            deferred.iter_mut().find(|frame| frame.header.request_id == target)
                        {
                            frame.cancelled = true;
                        }
                    }
                    FrameKind::Goodbye if body.is_empty() => break 'connection Exit::Goodbye,
                    _ => break 'connection Exit::Protocol("unexpected client frame"),
                }
            }
            let before = decoder.read_buf().len();
            decoder.consume_decoded();
            let buf = decoder.read_buf();
            if buf.len() < before
                && buf.len() <= RETAIN_CAPACITY
                && buf.capacity() > RETAIN_CAPACITY
            {
                *buf = bytes::BytesMut::from(buf.as_ref());
            }
        }
        let filled_queue = ids.len() - deferred.len() == limits.max_in_flight;
        #[cfg(feature = "test-hooks")]
        stats.peak_in_flight.fetch_max(ids.len() - deferred.len(), Relaxed);
        while !close_after_reply && writing_bytes + pending.len() < limits.max_pending_reply_bytes {
            if let Some(reply) = delivery.as_mut() {
                #[cfg(feature = "test-hooks")]
                if paused_stream {
                    break;
                }
                if !reply.can_queue_frame() {
                    break;
                }
                match reply.queue_frame(&mut pending) {
                    Ok(false) => {
                        #[cfg(feature = "test-hooks")]
                        if stats.hold_after_more.swap(false, Relaxed) {
                            paused_stream = true;
                        }
                        continue;
                    }
                    Ok(true) => {
                        ids.remove(&reply.call.id);
                    }
                    Err(error) => {
                        close_after_reply =
                            request_error(&mut session, &mut pending, &mut ids, reply.call.id, error);
                    }
                }
                delivery = None;
            } else if running.is_none() {
                #[cfg(feature = "test-hooks")]
                if stats.hold_queue.load(Relaxed) {
                    break;
                }
                let Some(entry) = queued.pop_front() else {
                    break;
                };
                let admission = entry.call.and_then(|call| {
                    call.cancel
                        .check()
                        .map_err(|_| ApiError::DeadlineExceeded)?;
                    Ok((call, runtime.admit_call(socket.clone())?))
                });
                match admission {
                    Ok((call, ownership)) => {
                        active = Some(ActiveCall {
                            id: entry.id,
                            cancel: call.cancel.clone(),
                            _ownership: ownership.clone(),
                        });
                        running = Some(Box::pin(execute(
                            session.take().expect("open session"),
                            call,
                            ownership,
                            #[cfg(test)]
                            hook,
                        )));
                    }
                    Err(error) => {
                        close_after_reply = request_error(&mut session, &mut pending, &mut ids, entry.id, error);
                    }
                }
            } else {
                break;
            }
        }
        if pending.len() != before {
            last_reply = tokio::time::Instant::now();
        }
        let pending_bytes = writing_bytes + pending.len();
        if close_after_reply && pending_bytes == 0 {
            break Exit::Io(Ok(()));
        }

        #[cfg(test)]
        {
            let decoder_len = decoder.read_buf().len();
            stats.decoder_len.store(decoder_len, Relaxed);
            stats.peak_decoder_len.fetch_max(decoder_len, Relaxed);
            stats.in_flight.store(ids.len() - deferred.len(), Relaxed);
            stats.write_capacity.store(pending.capacity(), Relaxed);
            stats.pending_bytes.store(pending_bytes, Relaxed);
        }
        #[cfg(feature = "test-hooks")]
        {
            stats.queued.store(queued.len(), Relaxed);
            stats.peak_pending_bytes.fetch_max(pending_bytes, Relaxed);
        }

        let below_cap = pending_bytes < limits.max_pending_reply_bytes;
        let decoder_len = decoder.read_buf().len();
        let idle = ids.is_empty() && pending_bytes == 0;
        let can_check_idle = idle && !close_after_reply;
        let can_decode = !close_after_reply && below_cap;
        let has_slot = ids.len() - deferred.len() < limits.max_in_flight;
        // A reply may free a slot after the decoding pass. Promote deferred
        // frames first, then consume any bytes already in the decoder.
        if can_decode
            && has_slot
            && (!deferred.is_empty()
                || (filled_queue && (decoder_len != 0 || lookahead_frames != 0)))
        {
            continue;
        }
        // Read controls and detect EOF even with every request slot occupied.
        // The shared byte cap includes both deferred frames and unread bytes.
        let read_limit = READ_CHUNK.min(LOOKAHEAD_BYTES - deferred_bytes - decoder_len);
        let can_read = can_decode
            && read_limit > 0
            && (has_slot || lookahead_frames < limits.max_in_flight);
        let can_pull_chunk = below_cap
            && delivery
                .as_ref()
                .is_some_and(|reply| reply.chunks.is_some());
        #[cfg(feature = "test-hooks")]
        let can_pull_chunk = can_pull_chunk && !paused_stream;
        // Collect adjacent small replies while their executions are completing.
        // A full buffer, idle pipeline or spill can write immediately.
        let write_at = if below_cap && delivery.is_none() && running.is_some() {
            last_reply + REPLY_BATCH_GAP
        } else {
            last_reply
        };

        tokio::select! {
            read = async { reader.read_buf(&mut decoder.read_buf().limit(read_limit)).await }, if can_read => {
                match read {
                    Ok(0) => break Exit::Io(Ok(())),
                    Ok(_bytes) => {
                        #[cfg(test)]
                        {
                            stats.read_bytes.fetch_add(_bytes, Relaxed);
                            stats.peak_decoder_len.fetch_max(decoder.read_buf().len(), Relaxed);
                        }
                    }
                    Err(error) => break Exit::Io(Err(error)),
                }
            }
            _ = async {
                tokio::select! {
                    _ = binding_changes.changed() => {}
                    _ = idle_check.tick() => {}
                }
            }, if can_check_idle => {
                #[cfg(feature = "test-hooks")]
                if stats.hold_idle_checks.load(Relaxed) {
                    continue;
                }
                if let Err(error) = session.as_mut().expect("open session").check_idle() {
                    break Exit::Session(error);
                }
            }
            (returned, result) = poll_active(&mut running), if running.is_some() => {
                session = Some(returned);
                running = None;
                let call = active.take().expect("running call");
                let result = result.and_then(|completion| {
                    completion.lease.check_release()?;
                    Ok(completion)
                });
                match result {
                    Ok(completion) => {
                        #[cfg(feature = "test-hooks")]
                        { paused_stream = false; }
                        delivery = Some(Delivery::new(call, completion));
                    }
                    Err(error) => close_after_reply = request_error(&mut session, &mut pending, &mut ids, call.id, error),
                }
                last_reply = tokio::time::Instant::now();
            }
            chunk = async { delivery.as_mut().expect("active spill").read_chunk().await }, if can_pull_chunk => {
                match chunk {
                    Some(Err(status)) => {
                        let reply = delivery.take().expect("active spill");
                        let error = match status.code() {
                            Code::ResourceExhausted => ApiError::ResourceExhausted,
                            _ => ApiError::Internal,
                        };
                        close_after_reply = request_error(&mut session, &mut pending, &mut ids, reply.call.id, error);
                    }
                    chunk => delivery.as_mut().expect("active spill").accept_chunk(chunk.transpose().expect("successful chunk")),
                }
            }
            _ = sleep_until(write_at), if writing.is_none() && !pending.is_empty() => {
                writing_bytes = pending.len();
                writing = Some(Box::pin(write_batch(writer.take().expect("idle writer"), std::mem::take(&mut pending))));
            }
            (returned, reusable, result) = poll_active(&mut writing), if writing.is_some() => {
                writing = None;
                writing_bytes = 0;
                writer = Some(returned);
                if let Err(error) = result {
                    break Exit::Io(Err(error));
                }
                if pending.is_empty() && reusable.capacity() > pending.capacity() {
                    pending = reusable;
                }
            }
        }
    };

    if let Some(running) = running {
        if !matches!(exit, Exit::Goodbye) {
            active.as_ref().expect("running call").cancel.cancel();
        }
        let (returned, completion) = running.await;
        drop(completion);
        session = Some(returned);
    }
    let stream_call = delivery.take().map(|reply| {
        if !matches!(exit, Exit::Goodbye) {
            reply.call.cancel.cancel();
        }
        reply.call
    });
    drop(session);
    drop(active);
    drop(stream_call);

    let (code, message) = match exit {
        Exit::Io(result) => return result,
        Exit::Goodbye => return Ok(()),
        Exit::Protocol(message) => ("protocol", message),
        Exit::Session(error) => (error.code(), error.message()),
    };
    // Finish any write already started so the peer can decode the fatal frame.
    let mut writer = if let Some(writing) = writing {
        let (writer, _, result) = writing.await;
        result?;
        writer
    } else {
        writer.expect("idle writer")
    };
    pending.clear();
    append_error(&mut pending, 0, code, message, false);
    write_output(&mut writer, &pending).await
}
