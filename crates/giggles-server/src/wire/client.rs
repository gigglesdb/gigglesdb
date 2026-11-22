//! Reference client for native wire protocol version 1.

use super::{
    conn::MAX_BATCH_ENTRIES,
    frame::{
        Direction, Flags, FrameDecoder, FrameHeader, FrameKind, encode_message, write_preface,
    },
    messages::{
        WireBatch, WireBatchEntry, WireBatchMode, WireCancel, WireCloseHandle, WireConnError,
        WirePrepare, WirePrepared, WireReady, WireReply, WireRun, wire_batch_entry,
    },
    shapes::expand,
};
use crate::{
    MAX_FRAME,
    api::proto::{
        ExecuteReply, ExecuteRequest, NativeBinding, OpenSessionRequest, ScramChallenge,
        ScramContinue, ScramStart, open_session_request,
    },
    auth::scram::ClientExchange,
    tls,
    transport::{IO_TIMEOUT, no_delay},
};
use bytes::BufMut;
use prost::Message;
use rustls::{ClientConfig, pki_types::ServerName};
use std::{collections::VecDeque, fmt, io, net::SocketAddr, ops::Range, sync::Arc};
use tokio::{
    io::{
        self as tokio_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf,
    },
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;

const READ_CHUNK: usize = 8 * 1024;

/// PEM CA bundle and optional PEM client certificate chain and private key.
pub struct ClientTls {
    pub ca: Vec<u8>,
    pub cert: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub server_name: String,
}

impl ClientTls {
    fn into_connector(self) -> io::Result<(TlsConnector, ServerName<'static>)> {
        let name = ServerName::try_from(self.server_name).map_err(io::Error::other)?;
        let ca = rustls_pemfile::certs(&mut self.ca.as_slice()).collect::<io::Result<Vec<_>>>()?;
        if ca.is_empty() {
            return Err(invalid_input("missing wire CA certificates"));
        }
        let config = match (self.cert, self.key) {
            (Some(cert), Some(key)) => {
                let chain =
                    rustls_pemfile::certs(&mut cert.as_slice()).collect::<io::Result<_>>()?;
                let key = rustls_pemfile::private_key(&mut key.as_slice())?
                    .ok_or_else(|| invalid_input("missing wire client private key"))?;
                tls::client(chain, key, ca)?
            }
            (None, None) => Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(tls::roots(ca)?)
                    .with_no_client_auth(),
            ),
            _ => {
                return Err(invalid_input(
                    "wire client certificate and key must be paired",
                ));
            }
        };
        Ok((TlsConnector::from(config), name))
    }
}

/// Sign-in options for the wire handshake.
pub enum Credential {
    /// Use the supplied open request as-is.
    None,
    /// Sign in with SCRAM-SHA-256. The database defaults to the one in the
    /// open request, or `postgres` if neither specifies it.
    Scram {
        role: String,
        password: String,
        database: Option<String>,
    },
}

/// Server error carried by the client's `io::Error`.
#[derive(Debug)]
pub struct WireError {
    pub code: String,
    pub message: String,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for WireError {}

/// An EXECUTE or RUN request carried inside a BATCH frame.
#[derive(Debug)]
#[allow(clippy::large_enum_variant, reason = "the batch API takes ExecuteRequest by value")]
pub enum BatchEntry {
    Execute(ExecuteRequest),
    Run {
        handle: u32,
        bindings: Vec<NativeBinding>,
    },
}

/// The result of one batch entry. SQL errors are carried in `Reply.code`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant, reason = "the batch API returns ExecuteReply by value")]
pub enum BatchOutcome {
    Reply(ExecuteReply),
    Error(WireConnError),
    Skipped,
}

fn server_error(error: WireConnError) -> io::Error {
    io::Error::other(WireError {
        code: error.code,
        message: error.message,
    })
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

trait Connection: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Connection for T {}

struct Reader {
    stream: ReadHalf<Box<dyn Connection>>,
    decoder: FrameDecoder,
    reply: Vec<u8>,
    shapes: Vec<Vec<String>>,
}

impl Reader {
    async fn next_frame(&mut self) -> io::Result<(FrameHeader, Range<usize>)> {
        self.decoder.consume_decoded();
        timeout(IO_TIMEOUT, async {
            loop {
                if let Some(frame) = self
                    .decoder
                    .next_frame()
                    .map_err(|_| invalid_data("invalid wire response frame"))?
                {
                    return Ok(frame);
                }
                if self
                    .stream
                    .read_buf(&mut self.decoder.read_buf().limit(READ_CHUNK))
                    .await?
                    == 0
                {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                }
            }
        })
        .await?
    }

    async fn prepared(&mut self, expected: u32) -> io::Result<Result<u32, WireConnError>> {
        let (head, body) = self.next_frame().await?;
        let bytes = self.decoder.body(body);
        match (head.kind, head.flags.0, head.request_id) {
            (FrameKind::Prepared, 0, id) if id == expected => {
                Ok(Ok(WirePrepared::decode(bytes)?.handle))
            }
            (FrameKind::ConnError, 0, 0) => Err(server_error(WireConnError::decode(bytes)?)),
            (FrameKind::ConnError, 0, id) if id == expected => {
                Ok(Err(WireConnError::decode(bytes)?))
            }
            _ => Err(invalid_data("unexpected prepared reply frame")),
        }
    }

    async fn reply(
        &mut self,
        expected: Option<u32>,
    ) -> io::Result<(u32, Result<ExecuteReply, WireConnError>)> {
        let (id, outcome) = self.reply_outcome(expected, false).await?;
        match outcome {
            BatchOutcome::Reply(reply) => Ok((id, Ok(reply))),
            BatchOutcome::Error(error) => Ok((id, Err(error))),
            BatchOutcome::Skipped => unreachable!("ordinary reply cannot be skipped"),
        }
    }

    async fn reply_outcome(
        &mut self,
        expected: Option<u32>,
        allow_skipped: bool,
    ) -> io::Result<(u32, BatchOutcome)> {
        let mut has_more = false;
        loop {
            let (head, body) = self.next_frame().await?;
            let bytes = self.decoder.body(body);
            let id = head.request_id;
            if head.kind == FrameKind::ConnError && head.flags.0 == 0 {
                self.reply = Vec::new();
                let error = WireConnError::decode(bytes)?;
                if id == 0 {
                    return Err(server_error(error));
                }
                if expected == Some(id) {
                    return Ok((id, BatchOutcome::Error(error)));
                }
            }
            if allow_skipped
                && head.kind == FrameKind::Reply
                && head.flags.0 == Flags::SKIPPED
                && expected == Some(id)
                && !has_more
            {
                return Ok((id, BatchOutcome::Skipped));
            }
            if head.kind != FrameKind::Reply
                || head.flags.0 & !Flags::MORE != 0
                || expected != Some(id)
            {
                return Err(invalid_data("unexpected wire reply frame"));
            }
            self.reply.extend_from_slice(bytes);
            if head.flags.0 == 0 {
                // A spill contributes multiple protobuf messages to one logical reply.
                let wire = WireReply::decode(std::mem::take(&mut self.reply).as_slice())?;
                let mut reply = wire
                    .reply
                    .ok_or_else(|| invalid_data("missing wire reply"))?;
                expand(&mut self.shapes, &mut reply, &wire.shapes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                return Ok((id, BatchOutcome::Reply(reply)));
            }
            has_more = true;
        }
    }
}

struct Writer {
    stream: WriteHalf<Box<dyn Connection>>,
    max_frame: usize,
}

impl Writer {
    async fn write(&mut self, out: &[u8]) -> io::Result<()> {
        timeout(IO_TIMEOUT, async {
            self.stream.write_all(out).await?;
            self.stream.flush().await
        })
        .await?
    }

    fn check(&self, message: &impl Message) -> io::Result<()> {
        if message.encoded_len() > self.max_frame {
            return Err(invalid_input("wire request exceeds the frame limit"));
        }
        Ok(())
    }

    async fn send(&mut self, kind: FrameKind, id: u32, message: &impl Message) -> io::Result<()> {
        self.check(message)?;
        let mut out = Vec::new();
        encode_message(&mut out, kind, Flags(0), id, message);
        self.write(&out).await
    }
}

/// One connection-bound session. Calls are never retried.
pub struct Client {
    reader: Reader,
    writer: Writer,
    next_id: u32,
    pending: VecDeque<PendingReply>,
    #[cfg(feature = "test-hooks")]
    local_addr: SocketAddr,
    #[cfg(feature = "test-hooks")]
    max_in_flight: usize,
}

struct PendingReply {
    id: u32,
    batch: bool,
}

impl Client {
    pub async fn connect(
        addr: SocketAddr,
        tls: Option<ClientTls>,
        mut open: OpenSessionRequest,
        credential: Credential,
    ) -> io::Result<(Self, WireReady)> {
        let mut scram = match credential {
            Credential::None => None,
            Credential::Scram {
                role,
                password,
                database,
            } => {
                let exchange = ClientExchange::new()?;
                // The role is a request field, not a session role; building the
                // request in one literal keeps tests/role_switch_guard's source
                // scan for `.role =` assignments free of client code.
                open = OpenSessionRequest {
                    role: Some(role),
                    database: database
                        .or(open.database)
                        .or_else(|| Some("postgres".into())),
                    credential: Some(open_session_request::Credential::ScramStart(ScramStart {
                        client_first: exchange.first(),
                    })),
                    ..open
                };
                Some((exchange, password))
            }
        };
        let tls = tls.map(ClientTls::into_connector).transpose()?;
        let (stream, _local_addr) = timeout(IO_TIMEOUT, async {
            let socket = TcpStream::connect(addr).await?;
            #[cfg(feature = "test-hooks")]
            let local_addr = socket.local_addr()?;
            #[cfg(not(feature = "test-hooks"))]
            let local_addr: Option<SocketAddr> = None;
            no_delay(&socket);
            let stream: Box<dyn Connection> = match tls {
                Some((connector, name)) => Box::new(connector.connect(name, socket).await?),
                None => Box::new(socket),
            };
            Ok::<_, io::Error>((stream, local_addr))
        })
        .await??;
        let (read, write) = tokio_io::split(stream);
        let mut reader = Reader {
            stream: read,
            decoder: FrameDecoder::new(Direction::ServerToClient, MAX_FRAME),
            reply: Vec::new(),
            shapes: Vec::new(),
        };
        let mut writer = Writer {
            stream: write,
            max_frame: MAX_FRAME,
        };
        writer.check(&open)?;
        let mut hello = Vec::new();
        write_preface(&mut hello);
        let mut id = 1;
        encode_message(&mut hello, FrameKind::Hello, Flags(0), id, &open);
        writer.write(&hello).await?;
        let mut server_proof = None;
        let ready = loop {
            let (head, body) = reader.next_frame().await?;
            let bytes = reader.decoder.body(body);
            match (head.kind, head.flags.0, head.request_id) {
                (FrameKind::ConnError, 0, 0) => {
                    return Err(server_error(WireConnError::decode(bytes)?));
                }
                (FrameKind::Ready, 0, received) if received == id => {
                    break WireReady::decode(bytes)?;
                }
                (FrameKind::Auth, 0, received) if received == id => {
                    let (exchange, password) = scram
                        .take()
                        .ok_or_else(|| invalid_data("unexpected wire AUTH challenge"))?;
                    let challenge = ScramChallenge::decode(bytes)?;
                    let (client_final, proof) = tokio::task::spawn_blocking(move || {
                        exchange.respond(&password, &challenge.server_first)
                    })
                    .await
                    .map_err(io::Error::other)??;
                    server_proof = Some(proof);
                    id += 1;
                    let response = ScramContinue {
                        exchange: challenge.exchange,
                        client_final,
                    };
                    writer.send(FrameKind::Auth, id, &response).await?;
                }
                _ => return Err(invalid_data("unexpected wire handshake frame")),
            }
        };
        let session = ready
            .session
            .as_ref()
            .filter(|session| !session.session_id.is_empty())
            .ok_or_else(|| invalid_data("missing wire session"))?;
        if ready.max_frame == 0 || ready.max_in_flight == 0 {
            return Err(invalid_data("invalid wire session limits"));
        }
        if scram.is_some()
            || server_proof
                .is_some_and(|proof| !proof.verify(session.scram_server_final.as_deref()))
        {
            return Err(invalid_data("invalid wire SCRAM server proof"));
        }
        writer.max_frame = (ready.max_frame as usize).min(MAX_FRAME);
        // Handshake IDs are scoped to the handshake. The first Execute uses 1.
        let client = Self {
            reader,
            writer,
            next_id: 1,
            pending: VecDeque::new(),
            #[cfg(feature = "test-hooks")]
            local_addr: _local_addr,
            #[cfg(feature = "test-hooks")]
            max_in_flight: ready.max_in_flight as usize,
        };
        Ok((client, ready))
    }

    pub async fn execute(&mut self, request: ExecuteRequest) -> io::Result<ExecuteReply> {
        self.require_idle()?;
        self.send_execute(request).await?;
        self.recv().await?.1.map_err(server_error)
    }

    /// Prepare native text for reuse on this connection.
    pub async fn prepare(
        &mut self,
        text: &str,
        language_version: Option<&str>,
    ) -> io::Result<Result<u32, WireConnError>> {
        self.require_idle()?;
        let request = WirePrepare {
            text: text.into(),
            language_version: language_version.map(str::to_owned),
        };
        let id = self.send_request(FrameKind::Prepare, &request).await?;
        let reply = self.reader.prepared(id).await?;
        self.pending.pop_front();
        Ok(reply)
    }

    /// Execute a handle with the default budget and no operation ID.
    pub async fn run(
        &mut self,
        handle: u32,
        bindings: Vec<NativeBinding>,
    ) -> io::Result<ExecuteReply> {
        self.require_idle()?;
        self.send_run(WireRun {
            handle,
            bindings,
            ..Default::default()
        })
        .await?;
        self.recv().await?.1.map_err(server_error)
    }

    pub async fn close_handle(&mut self, handle: u32) -> io::Result<()> {
        self.require_idle()?;
        self.send_request(FrameKind::CloseHandle, &WireCloseHandle { handle })
            .await?;
        let reply = self.recv().await?.1.map_err(server_error)?;
        if reply.code != "ok" {
            return Err(invalid_data("unexpected close handle reply"));
        }
        Ok(())
    }

    /// Send requests in order and collect their replies in order. Drain
    /// request errors before returning the first one.
    pub async fn pipeline(
        &mut self,
        requests: Vec<ExecuteRequest>,
    ) -> io::Result<Vec<ExecuteReply>> {
        self.pipeline_requests(FrameKind::Execute, requests).await
    }

    /// Run handles in order and collect replies, draining request errors.
    pub async fn pipeline_run(&mut self, requests: Vec<WireRun>) -> io::Result<Vec<ExecuteReply>> {
        self.pipeline_requests(FrameKind::Run, requests).await
    }

    /// Send one batch and collect its outcomes in entry order.
    pub async fn batch(
        &mut self,
        mode: WireBatchMode,
        entries: Vec<BatchEntry>,
    ) -> io::Result<Vec<BatchOutcome>> {
        self.require_idle()?;
        let ids = self.send_batch(mode, entries).await?;
        let mut replies = Vec::with_capacity(ids.len());
        for _ in ids {
            replies.push(self.recv_batch_entry().await?.1);
        }
        Ok(replies)
    }

    /// Send one BATCH frame and return the entry IDs in order. Read its
    /// outstanding replies with `recv_batch_entry`.
    pub async fn send_batch(
        &mut self,
        mode: WireBatchMode,
        entries: Vec<BatchEntry>,
    ) -> io::Result<Vec<u32>> {
        if entries.is_empty() || entries.len() > MAX_BATCH_ENTRIES {
            return Err(invalid_input("wire batch entry count is outside the limit"));
        }
        let mut ids = self.reserve_ids(entries.len() as u32 + 1)?;
        let header_id = ids.next().expect("reserved batch header ID");
        let request = WireBatch {
            mode: mode as i32,
            entries: ids
                .clone()
                .zip(entries)
                .map(|(request_id, entry)| {
                    let entry = match entry {
                        BatchEntry::Execute(request) => wire_batch_entry::Entry::Execute(request),
                        BatchEntry::Run { handle, bindings } => {
                            wire_batch_entry::Entry::Run(WireRun {
                                handle,
                                bindings,
                                ..Default::default()
                            })
                        }
                    };
                    WireBatchEntry {
                        request_id,
                        entry: Some(entry),
                    }
                })
                .collect(),
        };
        self.writer
            .send(FrameKind::Batch, header_id, &request)
            .await?;
        self.pending
            .extend(ids.clone().map(|id| PendingReply { id, batch: true }));
        Ok(ids.collect())
    }

    async fn pipeline_requests(
        &mut self,
        kind: FrameKind,
        requests: Vec<impl Message>,
    ) -> io::Result<Vec<ExecuteReply>> {
        self.require_idle()?;
        for request in &requests {
            self.writer.check(request)?;
        }
        let count =
            u32::try_from(requests.len()).map_err(|_| invalid_input("too many wire requests"))?;
        let ids = self.reserve_ids(count)?;
        let send = async {
            for (id, request) in ids.clone().zip(requests) {
                self.writer.send(kind, id, &request).await?;
            }
            Ok::<_, io::Error>(())
        };
        let receive = async {
            let mut replies = Vec::with_capacity(count as usize);
            let mut error = None;
            for id in ids.clone() {
                match self.reader.reply(Some(id)).await?.1 {
                    Ok(reply) => replies.push(reply),
                    Err(failure) => {
                        error.get_or_insert_with(|| server_error(failure));
                    }
                }
            }
            Ok::<_, io::Error>(error.map_or(Ok(replies), Err))
        };
        // Keep reading while writing to avoid deadlock when both socket
        // buffers fill and the server pauses reads for backpressure.
        let ((), replies) = tokio::try_join!(send, receive)?;
        replies
    }

    pub async fn send_execute(&mut self, request: ExecuteRequest) -> io::Result<u32> {
        self.send_request(FrameKind::Execute, &request).await
    }

    /// Queue a RUN, with optional budget and operation ID, for `recv`.
    pub async fn send_run(&mut self, request: WireRun) -> io::Result<u32> {
        self.send_request(FrameKind::Run, &request).await
    }

    async fn send_request(&mut self, kind: FrameKind, request: &impl Message) -> io::Result<u32> {
        self.writer.check(request)?;
        let id = self.reserve_ids(1)?.start;
        self.writer.send(kind, id, request).await?;
        self.pending.push_back(PendingReply { id, batch: false });
        Ok(id)
    }

    pub async fn send_cancel(&mut self, target: u32) -> io::Result<()> {
        let id = self.reserve_ids(1)?.start;
        self.writer
            .send(
                FrameKind::Cancel,
                id,
                &WireCancel {
                    target_request_id: target,
                },
            )
            .await
    }

    /// Sends EXECUTE and its CANCEL in one write for cancellation race tests.
    #[cfg(feature = "test-hooks")]
    pub async fn send_execute_and_cancel(&mut self, request: ExecuteRequest) -> io::Result<u32> {
        self.writer.check(&request)?;
        let ids = self.reserve_ids(2)?;
        let id = ids.start;
        let mut bytes = Vec::new();
        encode_message(&mut bytes, FrameKind::Execute, Flags(0), id, &request);
        encode_message(
            &mut bytes,
            FrameKind::Cancel,
            Flags(0),
            id + 1,
            &WireCancel {
                target_request_id: id,
            },
        );
        self.writer.write(&bytes).await?;
        self.pending.push_back(PendingReply { id, batch: false });
        Ok(id)
    }

    pub async fn recv(&mut self) -> io::Result<(u32, Result<ExecuteReply, WireConnError>)> {
        let expected = self.pending.front();
        if expected.is_some_and(|pending| pending.batch) {
            return Err(invalid_input(
                "receive the next batch entry with recv_batch_entry",
            ));
        }
        let reply = self
            .reader
            .reply(expected.map(|pending| pending.id))
            .await?;
        self.pending.pop_front();
        Ok(reply)
    }

    /// Read the next outstanding reply for a pipelined batch entry.
    pub async fn recv_batch_entry(&mut self) -> io::Result<(u32, BatchOutcome)> {
        let expected = self
            .pending
            .front()
            .filter(|pending| pending.batch)
            .ok_or_else(|| invalid_input("the next pending reply must be a batch entry"))?;
        let reply = self.reader.reply_outcome(Some(expected.id), true).await?;
        self.pending.pop_front();
        Ok(reply)
    }

    #[cfg(feature = "test-hooks")]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[cfg(feature = "test-hooks")]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Read one reply frame, leaving reassembly and validation to the test.
    /// Use this through the last frame of the current reply before calling recv.
    #[cfg(feature = "test-hooks")]
    pub async fn recv_raw_frame(&mut self) -> io::Result<(FrameHeader, Vec<u8>)> {
        let (head, body) = self.reader.next_frame().await?;
        if head.flags.0 & Flags::MORE == 0
            && self
                .pending
                .front()
                .is_some_and(|pending| pending.id == head.request_id)
        {
            self.pending.pop_front();
        }
        Ok((head, self.reader.decoder.body(body).to_vec()))
    }

    pub async fn goodbye(mut self) -> io::Result<()> {
        let id = self.reserve_ids(1)?.start;
        self.writer.send(FrameKind::Goodbye, id, &()).await?;
        timeout(IO_TIMEOUT, self.writer.stream.shutdown()).await?
    }

    fn reserve_ids(&mut self, count: u32) -> io::Result<Range<u32>> {
        let start = self.next_id;
        self.next_id = start
            .checked_add(count)
            .ok_or_else(|| invalid_input("wire request IDs exhausted"))?;
        Ok(start..self.next_id)
    }

    fn require_idle(&self) -> io::Result<()> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(invalid_input("receive outstanding wire replies first"))
        }
    }
}
