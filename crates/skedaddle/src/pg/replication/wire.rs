use std::time::{SystemTime, UNIX_EPOCH};

use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{PostgresVersion, ReplicationError as E, Result};
use crate::pg::{
    frame::{BackendFrame, FrameBuffer},
    pgoutput::Limits,
};

const CONTROL_LIMIT: usize = 64 * 1024;

/// Read and write cursors live outside any cancellable future. Always drain
/// coalesced bytes before reading again; never buffer more than one frame + 8KiB.
pub(super) struct Wire<S> {
    io: S,
    frames: FrameBuffer,
    read: [u8; 8192],
    start: usize,
    end: usize,
    outgoing: Vec<u8>,
    written: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Wire<S> {
    pub(super) fn new(io: S, limits: Limits) -> Result<Self> {
        Ok(Self {
            io,
            frames: FrameBuffer::new(limits)?,
            read: [0; 8192],
            start: 0,
            end: 0,
            outgoing: Vec::new(),
            written: 0,
        })
    }

    pub(super) async fn frame(&mut self) -> Result<BackendFrame> {
        loop {
            if let Some(frame) = self.frames.next_frame()? {
                return Ok(frame);
            }
            if self.start < self.end {
                self.start += self.frames.push(&self.read[self.start..self.end])?;
            } else {
                // read(), unlike read_exact(), is cancellation safe.
                let n = self.io.read(&mut self.read).await?;
                if n == 0 {
                    self.frames.finish()?;
                    return Err(E::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "replication connection closed",
                    )));
                }
                self.start = 0;
                self.end = n;
            }
        }
    }

    async fn control_frame(&mut self) -> Result<BackendFrame> {
        let frame = self.frame().await?;
        if frame.payload.len() > CONTROL_LIMIT {
            return Err(E::Invalid("control frame exceeds 64KiB"));
        }
        Ok(frame)
    }

    pub(super) fn queue(&mut self, packet: Vec<u8>) -> Result<()> {
        if !self.outgoing.is_empty() {
            return Err(E::Invalid("pending frontend packet"));
        }
        self.outgoing = packet;
        self.written = 0;
        Ok(())
    }

    pub(super) async fn flush_pending(&mut self) -> Result<()> {
        if self.outgoing.is_empty() {
            return Ok(());
        }
        while self.written < self.outgoing.len() {
            let n = self.io.write(&self.outgoing[self.written..]).await?;
            if n == 0 {
                return Err(E::Io(std::io::ErrorKind::WriteZero.into()));
            }
            self.written += n;
        }
        self.io.flush().await?;
        self.outgoing.clear();
        self.written = 0;
        Ok(())
    }

    pub(super) async fn send(&mut self, packet: Vec<u8>) -> Result<()> {
        self.queue(packet)?;
        self.flush_pending().await
    }

    pub(super) async fn send_query(&mut self, query: &str) -> Result<()> {
        self.send(packet(b'Q', &cstring(query)?)?).await
    }

    pub(super) async fn authenticate(&mut self, password: &[u8]) -> Result<PostgresVersion> {
        let mut auth = Authentication::new();
        let mut version = None;
        let mut encoding = false;
        let mut standard_strings = false;
        // Bound a peer which sends endless notices/parameters before ReadyForQuery.
        for _ in 0..256 {
            let frame = self.control_frame().await?;
            match frame.tag {
                b'R' => {
                    if let Some(response) = auth.step(&frame.payload, password)? {
                        self.send(response).await?;
                    }
                }
                b'S' => {
                    let (name, value) = parameter(&frame.payload)?;
                    match name {
                        "server_version" => version = Some(PostgresVersion::parse(value)?),
                        "client_encoding" => encoding = value == "UTF8",
                        "standard_conforming_strings" => standard_strings = value == "on",
                        _ => {}
                    }
                }
                b'K' if frame.payload.len() == 8 => {}
                b'N' => {}
                b'E' => return Err(server_error(&frame.payload)?),
                b'Z' if frame.payload == *b"I"
                    && auth.complete()
                    && encoding
                    && standard_strings =>
                {
                    return version.ok_or(E::Invalid("missing server_version"));
                }
                _ => {
                    return Err(E::Authentication(
                        "unexpected startup frame or incomplete SCRAM",
                    ));
                }
            }
        }
        Err(E::Authentication("startup frame count limit"))
    }

    pub(super) async fn query_one(&mut self, query: &str) -> Result<Vec<Option<String>>> {
        let mut rows = self.query(query).await?;
        if rows.len() != 1 {
            return Err(E::Invalid("expected exactly one query result row"));
        }
        Ok(rows.remove(0))
    }

    pub(super) async fn query(&mut self, query: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.send_query(query).await?;
        let mut rows = Vec::new();
        for _ in 0..256 {
            let frame = self.control_frame().await?;
            match frame.tag {
                b'D' => {
                    if rows.len() == 2 {
                        return Err(E::Invalid("replication command row limit"));
                    }
                    rows.push(parse_row(&frame.payload)?);
                }
                b'T' | b'C' | b'N' => {}
                b'S' => {
                    parameter(&frame.payload)?;
                }
                b'E' => return Err(server_error(&frame.payload)?),
                b'Z' if frame.payload == *b"I" => return Ok(rows),
                _ => return Err(E::Invalid("unexpected replication command response")),
            }
        }
        Err(E::Invalid("command frame count limit"))
    }
}

pub(super) fn cstring(value: &str) -> Result<Vec<u8>> {
    if value.contains('\0') || value.len() > CONTROL_LIMIT {
        return Err(E::Invalid("invalid frontend string"));
    }
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    Ok(bytes)
}

pub(super) fn startup(user: &str, database: &str) -> Result<Vec<u8>> {
    if user.is_empty() || database.is_empty() {
        return Err(E::Invalid("empty user or database"));
    }
    let mut body = 196608_u32.to_be_bytes().to_vec();
    for (name, value) in [
        ("user", user),
        ("database", database),
        ("application_name", "skedaddle"),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
    ] {
        body.extend(cstring(name)?);
        body.extend(cstring(value)?);
    }
    body.push(0);
    if body.len() > CONTROL_LIMIT {
        return Err(E::Invalid("startup packet limit"));
    }
    let mut packet = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    packet.extend(body);
    Ok(packet)
}

fn packet(tag: u8, body: &[u8]) -> Result<Vec<u8>> {
    if body.len() > CONTROL_LIMIT {
        return Err(E::Invalid("frontend packet limit"));
    }
    let mut bytes = vec![tag];
    bytes.extend(((body.len() + 4) as u32).to_be_bytes());
    bytes.extend(body);
    Ok(bytes)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AuthState {
    Initial,
    Continue,
    Final,
    Ok,
    Complete,
    Failed,
}

struct Authentication {
    state: AuthState,
    scram: Option<ScramSha256>,
}

impl Authentication {
    fn new() -> Self {
        Self {
            state: AuthState::Initial,
            scram: None,
        }
    }
    fn complete(&self) -> bool {
        self.state == AuthState::Complete
    }

    fn step(&mut self, payload: &[u8], password: &[u8]) -> Result<Option<Vec<u8>>> {
        let state = std::mem::replace(&mut self.state, AuthState::Failed);
        let (code, data) = payload
            .split_at_checked(4)
            .ok_or(E::Authentication("short authentication message"))?;
        let code = u32::from_be_bytes(
            code.try_into()
                .map_err(|_| E::Authentication("auth code"))?,
        );
        match (state, code) {
            (AuthState::Initial, 10) => {
                if !data.ends_with(&[0, 0])
                    || !data[..data.len() - 1]
                        .split(|b| *b == 0)
                        .any(|s| s == b"SCRAM-SHA-256")
                {
                    return Err(E::Authentication("SCRAM-SHA-256 not offered"));
                }
                let scram = ScramSha256::new(password, ChannelBinding::unsupported());
                let mut response = cstring("SCRAM-SHA-256")?;
                response.extend((scram.message().len() as u32).to_be_bytes());
                response.extend(scram.message());
                self.scram = Some(scram);
                self.state = AuthState::Continue;
                Ok(Some(packet(b'p', &response)?))
            }
            (AuthState::Continue, 11) => {
                // Bound CPU work; reject zero rounds, non-extending nonce, and huge
                // salts before handing the transcript to postgres-protocol.
                let message = std::str::from_utf8(data)
                    .map_err(|_| E::Authentication("invalid SCRAM continuation"))?;
                let scram = self
                    .scram
                    .as_mut()
                    .ok_or(E::Authentication("missing SCRAM state"))?;
                let nonce = std::str::from_utf8(scram.message())
                    .map_err(|_| E::Authentication("SCRAM nonce"))?
                    .strip_prefix("n,,n=,r=")
                    .ok_or(E::Authentication("SCRAM nonce"))?;
                let fields: Vec<_> = message.split(',').collect();
                if fields.len() != 3
                    || !fields[0]
                        .strip_prefix("r=")
                        .is_some_and(|n| n.starts_with(nonce) && n.len() > nonce.len())
                    || !fields[1].starts_with("s=")
                    || fields[1].len() > 1024
                    || !fields[2]
                        .strip_prefix("i=")
                        .and_then(|i| i.parse::<u32>().ok())
                        .is_some_and(|i| (1..=100_000).contains(&i))
                {
                    return Err(E::Authentication("invalid or excessive SCRAM parameters"));
                }
                scram
                    .update(data)
                    .map_err(|_| E::Authentication("invalid SCRAM continuation"))?;
                let response = packet(b'p', scram.message())?;
                self.state = AuthState::Final;
                Ok(Some(response))
            }
            (AuthState::Final, 12) => {
                self.scram
                    .as_mut()
                    .ok_or(E::Authentication("missing SCRAM state"))?
                    .finish(data)
                    .map_err(|_| E::Authentication("SCRAM server signature verification failed"))?;
                self.state = AuthState::Ok;
                Ok(None)
            }
            (AuthState::Ok, 0) if data.is_empty() => {
                self.state = AuthState::Complete;
                Ok(None)
            }
            _ => Err(E::Authentication(
                "unsupported or out-of-order authentication",
            )),
        }
    }
}

fn parameter(payload: &[u8]) -> Result<(&str, &str)> {
    let mut parts = payload.split(|b| *b == 0);
    let name = parts.next().ok_or(E::Invalid("parameter name"))?;
    let value = parts.next().ok_or(E::Invalid("parameter value"))?;
    if parts.next() != Some(&[][..]) || parts.next().is_some() {
        return Err(E::Invalid("parameter terminator"));
    }
    Ok((
        std::str::from_utf8(name).map_err(|_| E::Invalid("parameter UTF8"))?,
        std::str::from_utf8(value).map_err(|_| E::Invalid("parameter UTF8"))?,
    ))
}

pub(super) fn parse_row(payload: &[u8]) -> Result<Vec<Option<String>>> {
    let mut p = payload;
    let count = u16::from_be_bytes(take::<2>(&mut p)?) as usize;
    if count > 16 {
        return Err(E::Invalid("replication command column limit"));
    }
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(take::<4>(&mut p)?);
        if length == -1 {
            row.push(None);
            continue;
        }
        if length < 0 {
            return Err(E::Invalid("negative DataRow length"));
        }
        let (value, rest) = p
            .split_at_checked(length as usize)
            .ok_or(E::Invalid("short DataRow"))?;
        row.push(Some(
            std::str::from_utf8(value)
                .map_err(|_| E::Invalid("DataRow UTF8"))?
                .into(),
        ));
        p = rest;
    }
    if !p.is_empty() {
        return Err(E::Invalid("trailing DataRow bytes"));
    }
    Ok(row)
}

fn take<const N: usize>(p: &mut &[u8]) -> Result<[u8; N]> {
    let (head, rest) = p.split_at_checked(N).ok_or(E::Invalid("short DataRow"))?;
    let value = head.try_into().map_err(|_| E::Invalid("DataRow field"))?;
    *p = rest;
    Ok(value)
}

pub(super) fn text(row: &[Option<String>], index: usize) -> Result<&str> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or(E::Invalid("missing/null replication result field"))
}

pub(super) fn server_error(payload: &[u8]) -> Result<E> {
    let mut p = payload;
    let mut code = None;
    let mut message = None;
    while let Some((&tag, rest)) = p.split_first() {
        if tag == 0 {
            if !rest.is_empty() {
                return Err(E::Invalid("trailing ErrorResponse bytes"));
            }
            return Ok(E::Server {
                code: code.ok_or(E::Invalid("missing SQLSTATE"))?,
                message: message.ok_or(E::Invalid("missing server error message"))?,
            });
        }
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(E::Invalid("unterminated ErrorResponse"))?;
        let value =
            std::str::from_utf8(&rest[..end]).map_err(|_| E::Invalid("ErrorResponse UTF8"))?;
        match tag {
            b'C' => code = Some(value.into()),
            b'M' => message = Some(value.into()),
            _ => {}
        }
        p = &rest[end + 1..];
    }
    Err(E::Invalid("missing ErrorResponse terminator"))
}

pub(super) fn pg_time() -> Result<i64> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| E::Invalid("clock before Unix epoch"))?
        .as_micros();
    i64::try_from(micros)
        .ok()
        .and_then(|n| n.checked_sub(946_684_800_000_000))
        .ok_or(E::Invalid("PostgreSQL timestamp overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha2::{Digest, Sha256};
    use std::time::Duration;

    fn auth_frame(code: u32, data: &[u8]) -> Vec<u8> {
        let mut p = code.to_be_bytes().to_vec();
        p.extend(data);
        p
    }

    // Independent test oracle: RFC 5802 HMAC/PBKDF2 expressions using SHA-256.
    // Production crypto is exclusively postgres-protocol. This oracle is first
    // checked against the literal RFC 7677 proof and server-signature vectors.
    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut block = [0; 64];
        if key.len() > 64 {
            block[..32].copy_from_slice(&Sha256::digest(key));
        } else {
            block[..key.len()].copy_from_slice(key);
        }
        let mut inner = Sha256::new();
        inner.update(block.map(|b| b ^ 0x36));
        inner.update(data);
        let mut outer = Sha256::new();
        outer.update(block.map(|b| b ^ 0x5c));
        outer.update(inner.finalize());
        outer.finalize().to_vec()
    }

    fn proof(first: &str, server: &str, final_without_proof: &str) -> (String, String) {
        let mut salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        salt.extend([0, 0, 0, 1]);
        let mut u = hmac(b"pencil", &salt);
        let mut salted = u.clone();
        for _ in 1..4096 {
            u = hmac(b"pencil", &u);
            for (a, b) in salted.iter_mut().zip(&u) {
                *a ^= b;
            }
        }
        let transcript = format!("{first},{server},{final_without_proof}");
        let mut key = hmac(&salted, b"Client Key");
        let signature = hmac(&Sha256::digest(&key), transcript.as_bytes());
        for (a, b) in key.iter_mut().zip(signature) {
            *a ^= b;
        }
        (
            STANDARD.encode(key),
            STANDARD.encode(hmac(&hmac(&salted, b"Server Key"), transcript.as_bytes())),
        )
    }

    #[test]
    fn rfc7677_oracle_and_real_scram_exchange() {
        let nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let server = format!("r={nonce},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
        let final_message = format!("c=biws,r={nonce}");
        assert_eq!(
            proof("n=user,r=rOprNGfwEbeRWgbNEkqO", &server, &final_message),
            (
                "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=".into(),
                "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".into()
            )
        );

        let mut auth = Authentication::new();
        let initial = auth
            .step(&auth_frame(10, b"SCRAM-SHA-256\0\0"), b"pencil")
            .unwrap()
            .unwrap();
        assert_eq!(&initial[..5], &[b'p', 0, 0, 0, (initial.len() - 1) as u8]);
        let first = std::str::from_utf8(&initial[23..]).unwrap();
        assert!(first.starts_with("n,,n=,r="));
        let server = format!("r={}SERVER,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", &first[8..]);
        let response = auth
            .step(&auth_frame(11, server.as_bytes()), b"unused")
            .unwrap()
            .unwrap();
        let final_message = std::str::from_utf8(&response[5..]).unwrap();
        let (without, actual_proof) = final_message.rsplit_once(",p=").unwrap();
        let (expected_proof, signature) = proof(&first[3..], &server, without);
        assert_eq!(actual_proof, expected_proof);
        assert!(!auth.complete());
        auth.step(&auth_frame(12, format!("v={signature}").as_bytes()), b"")
            .unwrap();
        assert!(!auth.complete());
        auth.step(&auth_frame(0, b""), b"").unwrap();
        assert!(auth.complete());
        assert!(auth.step(&auth_frame(0, b""), b"").is_err());
    }

    #[test]
    fn authentication_rejects_downgrades_and_bad_order() {
        for code in [0, 3, 5, 11, 12] {
            assert!(
                Authentication::new()
                    .step(&auth_frame(code, b""), b"secret")
                    .is_err()
            );
        }
        for offer in [
            &b"SCRAM-SHA-256\0"[..],
            b"SCRAM-SHA-256-PLUS\0\0",
            b"MD5\0\0",
            b"",
        ] {
            assert!(
                Authentication::new()
                    .step(&auth_frame(10, offer), b"secret")
                    .is_err()
            );
        }
        let mut auth = Authentication::new();
        auth.step(&auth_frame(10, b"SCRAM-SHA-256\0\0"), b"secret")
            .unwrap();
        assert!(auth.step(&auth_frame(0, b""), b"").is_err());
    }

    #[test]
    fn scram_rejects_invalid_nonce_rounds_and_signature() {
        for rounds in ["0", "100001", "-1", "4294967296", "1x"] {
            let mut auth = Authentication::new();
            auth.step(&auth_frame(10, b"SCRAM-SHA-256\0\0"), b"pencil")
                .unwrap();
            let nonce = std::str::from_utf8(auth.scram.as_ref().unwrap().message()).unwrap()[8..]
                .to_string();
            let server = format!("r={nonce}SERVER,s=W22ZaJ0SNY7soEsUEjb6gQ==,i={rounds}");
            assert!(auth.step(&auth_frame(11, server.as_bytes()), b"").is_err());
        }
        for extension in [false, true] {
            let mut auth = Authentication::new();
            auth.step(&auth_frame(10, b"SCRAM-SHA-256\0\0"), b"pencil")
                .unwrap();
            let nonce = std::str::from_utf8(auth.scram.as_ref().unwrap().message()).unwrap()[8..]
                .to_string();
            let suffix = if extension { "SERVER" } else { "" };
            let server = format!("r={nonce}{suffix},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
            let response = auth.step(&auth_frame(11, server.as_bytes()), b"");
            if extension {
                response.unwrap();
                assert!(
                    auth.step(
                        &auth_frame(12, b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
                        b""
                    )
                    .is_err()
                );
            } else {
                assert!(response.is_err());
            }
        }
    }

    #[test]
    fn startup_bytes_and_nul_rejection() {
        let bytes = startup("replicator", "source").unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len()
        );
        let mut expected = vec![0, 3, 0, 0];
        expected.extend(b"user\0replicator\0database\0source\0application_name\0skedaddle\0replication\0database\0client_encoding\0UTF8\0standard_conforming_strings\0on\0\0");
        assert_eq!(&bytes[4..], &expected);
        assert!(startup("evil\0user", "db").is_err());
        assert!(startup("user", "").is_err());
        assert!(startup(&"x".repeat(CONTROL_LIMIT), "db").is_err());
    }

    #[test]
    fn strict_control_parsers() {
        let row = [0, 2, 0, 0, 0, 1, b'x', 255, 255, 255, 255];
        assert_eq!(parse_row(&row).unwrap(), vec![Some("x".into()), None]);
        for n in 0..row.len() {
            assert!(parse_row(&row[..n]).is_err());
        }
        let mut extra = row.to_vec();
        extra.push(0);
        assert!(parse_row(&extra).is_err());
        assert!(parse_row(&[0, 17]).is_err());
        assert!(parse_row(&[0, 1, 255, 255, 255, 254]).is_err());
        assert_eq!(
            parameter(b"client_encoding\0UTF8\0").unwrap(),
            ("client_encoding", "UTF8")
        );
        for p in [&b"x\0y"[..], b"x\0y\0\0", b"x\0"] {
            assert!(parameter(p).is_err());
        }
        assert!(
            matches!(server_error(b"SERROR\0C42704\0Mmissing slot\0\0").unwrap(), E::Server { code, .. } if code == "42704")
        );
        assert!(server_error(b"C42704\0Munterminated").is_err());
    }

    #[tokio::test]
    async fn cancelled_fragmented_read_preserves_partial_frame_and_coalescing() {
        let (client, mut peer) = tokio::io::duplex(128);
        let mut wire = Wire::new(client, Limits::default()).unwrap();
        let frame = packet(b'd', b"payload").unwrap();
        for byte in &frame[..frame.len() - 1] {
            peer.write_all(&[*byte]).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(1), wire.frame())
                    .await
                    .is_err()
            );
        }
        let mut remaining = vec![*frame.last().unwrap()];
        remaining.extend(packet(b'N', b"second").unwrap());
        peer.write_all(&remaining).await.unwrap();
        assert_eq!(wire.frame().await.unwrap().payload, b"payload");
        assert_eq!(wire.frame().await.unwrap().payload, b"second");
    }

    #[tokio::test]
    async fn cancelled_partial_write_resumes_without_duplicating_bytes() {
        let (client, mut peer) = tokio::io::duplex(7);
        let mut wire = Wire::new(client, Limits::default()).unwrap();
        let packet = super::super::status(42, 1).unwrap();
        wire.queue(packet.to_vec()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(5), wire.flush_pending())
                .await
                .is_err()
        );
        assert!(wire.written > 0 && wire.written < packet.len());
        let (sent, received) = tokio::join!(wire.flush_pending(), async {
            let mut received = [0; 39];
            peer.read_exact(&mut received).await.unwrap();
            received
        });
        sent.unwrap();
        assert_eq!(received, packet);
        assert!(wire.outgoing.is_empty());
    }

    #[tokio::test]
    async fn network_eof_and_frame_limit_fail_closed() {
        let (client, mut peer) = tokio::io::duplex(32);
        let mut wire = Wire::new(client, Limits::default()).unwrap();
        peer.write_all(&[b'd', 0, 0, 0, 12, 1]).await.unwrap();
        drop(peer);
        assert!(matches!(wire.frame().await, Err(E::Decode(_))));
        let (client, mut peer) = tokio::io::duplex(32);
        let mut wire = Wire::new(client, Limits::default()).unwrap();
        peer.write_all(&[b'd', 0x7f, 255, 255, 255]).await.unwrap();
        assert!(matches!(wire.frame().await, Err(E::Decode(_))));
        assert!(matches!(wire.frame().await, Err(E::Decode(_))));
    }
}
