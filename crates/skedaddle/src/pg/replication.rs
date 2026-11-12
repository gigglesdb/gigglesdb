//! TLS-only, SCRAM-authenticated logical replication for PostgreSQL 16–18.
//!
//! Persist the slot's consistent point before starting a new capture. On resume,
//! use [`ReplicationConnection::reconnect`] with the manifest-durable position.
//! WAL positions in received frames are informational and never acknowledge data.
//! Cancellation of `next` preserves partial reads and status writes. Cancellation
//! of a command requires dropping the connection (its command state stays busy).

mod session;
mod wire;
pub use session::{REQUIRED_SETTINGS_SQL, SessionSettings};

use std::{error::Error, fmt, sync::Arc, time::Duration};

use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::Instant,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::{
    frame::BackendFrame,
    pgoutput::{CopyData, DecodeError, Decoder, Limits, StandbyStatusUpdate},
};
use wire::{Wire, text};

pub type Result<T> = std::result::Result<T, ReplicationError>;

#[derive(Debug)]
pub enum ReplicationError {
    Io(std::io::Error),
    Tls(rustls::Error),
    Decode(DecodeError),
    Invalid(&'static str),
    Authentication(&'static str),
    Server { code: String, message: String },
    RecoveryRequired(RecoveryRequired),
    Timeout,
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "replication I/O: {e}"),
            Self::Tls(e) => write!(f, "replication TLS: {e}"),
            Self::Decode(e) => e.fmt(f),
            Self::Server { code, message } => write!(f, "PostgreSQL {code}: {message}"),
            // Authentication errors deliberately exclude password/SCRAM payloads.
            other => write!(f, "replication: {other:?}"),
        }
    }
}

impl Error for ReplicationError {}
impl From<std::io::Error> for ReplicationError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<rustls::Error> for ReplicationError {
    fn from(e: rustls::Error) -> Self {
        Self::Tls(e)
    }
}
impl From<DecodeError> for ReplicationError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryRequired {
    SystemIdChanged {
        expected: u64,
        actual: u64,
    },
    TimelineChanged {
        expected: u32,
        actual: u32,
    },
    MissingSlot(String),
    SlotIdentityChanged(String),
    SlotInvalidated(String),
    /// A recreated/advanced slot or a server behind the durable checkpoint.
    PositionUnavailable {
        durable_lsn: u64,
    },
}

/// xlogpos is recorded, but normal WAL growth is not a lineage change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lineage {
    pub system_id: u64,
    pub timeline: u32,
    pub xlogpos: u64,
    pub database: String,
}

impl Lineage {
    pub fn check(&self, actual: &Self) -> Result<()> {
        let reason = if self.system_id != actual.system_id {
            Some(RecoveryRequired::SystemIdChanged {
                expected: self.system_id,
                actual: actual.system_id,
            })
        } else if self.timeline != actual.timeline {
            Some(RecoveryRequired::TimelineChanged {
                expected: self.timeline,
                actual: actual.timeline,
            })
        } else if self.database != actual.database {
            Some(RecoveryRequired::SlotIdentityChanged(
                "database changed".into(),
            ))
        } else {
            None
        };
        match reason {
            Some(r) => Err(ReplicationError::RecoveryRequired(r)),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostgresVersion {
    V16,
    V17,
    V18,
}

impl PostgresVersion {
    fn parse(version: &str) -> Result<Self> {
        match version.split('.').next() {
            Some("16") => Ok(Self::V16),
            Some("17") => Ok(Self::V17),
            Some("18") => Ok(Self::V18),
            _ => Err(ReplicationError::Invalid(
                "supported PostgreSQL versions are 16–18",
            )),
        }
    }
}

/// No custom TLS verifier or plaintext option is exposed. Roots are explicit.
/// Intentionally does not implement Debug, because it contains a password.
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub server_name: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub roots: RootCertStore,
    pub connect_timeout: Duration,
    pub status_interval: Duration,
    pub limits: Limits,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedSlot {
    pub name: String,
    pub consistent_point: u64,
}

/// Store together with a durable manifest. `durable_lsn` is never inferred from WAL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub lineage: Lineage,
    pub slot: String,
    pub durable_lsn: u64,
}

/// One bounded CopyData payload. Decode while borrowing it; no logical data copy.
#[derive(Debug)]
pub struct ReplicationMessage {
    payload: Vec<u8>,
    decoder: Decoder,
}

impl ReplicationMessage {
    pub fn decode(&self) -> std::result::Result<CopyData<'_>, DecodeError> {
        self.decoder.decode_copy_data(&self.payload)
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Ready,
    Busy,
    Streaming,
    Failed,
}

pub struct ReplicationConnection<S = TlsStream<TcpStream>> {
    wire: Wire<S>,
    lineage: Lineage,
    version: PostgresVersion,
    state: State,
    decoder: Decoder,
    interval: Duration,
    next_status: Instant,
    durable_lsn: u64,
    settings: SessionSettings,
    command_timeout: Duration,
}

impl ReplicationConnection {
    /// Initial discovery. Existing captures must use `reconnect`, which also
    /// checks lineage and slot retention before starting at the exact checkpoint.
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        if config.status_interval.is_zero()
            || config.status_interval > Duration::from_secs(60)
            || config.connect_timeout.is_zero()
            || config.connect_timeout > Duration::from_secs(300)
            || config.roots.is_empty()
        {
            return Err(ReplicationError::Invalid(
                "roots and bounded nonzero connection/status durations required",
            ));
        }
        Decoder::new(config.limits)?;
        tokio::time::timeout(config.connect_timeout, async {
            let startup = wire::startup(&config.user, &config.database)?;
            let mut tcp = TcpStream::connect((config.host.as_str(), config.port)).await?;
            tcp.set_nodelay(true)?;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await?;
            if tcp.read_u8().await? != b'S' {
                return Err(ReplicationError::Authentication("server refused TLS"));
            }
            let mut tls = ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(config.roots.clone())
            .with_no_client_auth();
            tls.resumption = rustls::client::Resumption::disabled();
            let name = ServerName::try_from(config.server_name.clone())
                .map_err(|_| ReplicationError::Invalid("invalid TLS server name"))?;
            let io = TlsConnector::from(Arc::new(tls)).connect(name, tcp).await?;
            let mut wire = Wire::new(io, config.limits)?;
            wire.send(startup).await?;
            let version = wire.authenticate(config.password.as_bytes()).await?;
            Self::initialize(wire, version, config).await
        })
        .await
        .map_err(|_| ReplicationError::Timeout)?
    }

    pub async fn reconnect(
        config: &ConnectionConfig,
        checkpoint: &Checkpoint,
        publications: &[String],
    ) -> Result<Self> {
        let mut connection = Self::connect(config).await?;
        checkpoint.lineage.check(&connection.lineage)?;
        connection
            .start(&checkpoint.slot, checkpoint.durable_lsn, publications)
            .await?;
        Ok(connection)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> ReplicationConnection<S> {
    // Shared by the authenticated TLS connect path and duplex protocol tests.
    async fn initialize(
        mut wire: Wire<S>,
        version: PostgresVersion,
        config: &ConnectionConfig,
    ) -> Result<Self> {
        let row = wire.query_one("IDENTIFY_SYSTEM").await?;
        if row.len() != 4 {
            return Err(ReplicationError::Invalid("IDENTIFY_SYSTEM column count"));
        }
        let lineage = Lineage {
            system_id: text(&row, 0)?
                .parse()
                .map_err(|_| ReplicationError::Invalid("system id"))?,
            timeline: text(&row, 1)?
                .parse()
                .map_err(|_| ReplicationError::Invalid("timeline"))?,
            xlogpos: parse_lsn(text(&row, 2)?)?,
            database: text(&row, 3)?.into(),
        };
        if lineage.database != config.database || lineage.timeline == 0 || lineage.system_id == 0 {
            return Err(ReplicationError::Invalid("IDENTIFY_SYSTEM identity"));
        }
        let settings = session::setup(&mut wire).await?;
        Ok(Self {
            wire,
            lineage,
            version,
            state: State::Ready,
            decoder: Decoder::new(config.limits)?,
            interval: config.status_interval,
            next_status: Instant::now(),
            durable_lsn: 0,
            settings,
            command_timeout: config.connect_timeout,
        })
    }

    pub fn lineage(&self) -> &Lineage {
        &self.lineage
    }
    pub fn version(&self) -> PostgresVersion {
        self.version
    }

    /// Actual SHOW values from this replication session, after setup. Both
    /// `connect` and `reconnect` apply setup before returning or starting COPY.
    pub fn session_settings(&self) -> &SessionSettings {
        &self.settings
    }

    /// Reapply the fixed mapping settings and read them back on this session.
    /// Only simple-query SET/SHOW commands are exposed, before COPY. Work is
    /// bounded by the connect timeout and the existing frame/row/byte limits.
    /// Cancellation or error leaves this connection busy and unusable.
    pub async fn setup_mapping_session(&mut self) -> Result<&SessionSettings> {
        self.ready()?;
        self.state = State::Busy;
        let settings = tokio::time::timeout(self.command_timeout, session::setup(&mut self.wire))
            .await
            .map_err(|_| ReplicationError::Timeout)??;
        self.settings = settings;
        self.state = State::Ready;
        Ok(&self.settings)
    }

    pub async fn create_slot(&mut self, slot: &str) -> Result<CreatedSlot> {
        let command = create_slot_command(self.version, slot)?;
        self.ready()?;
        self.state = State::Busy;
        let row = self.wire.query_one(&command).await?;
        if row.len() != 4 || text(&row, 0)? != slot || text(&row, 3)? != "pgoutput" {
            return Err(ReplicationError::Invalid("CREATE_REPLICATION_SLOT result"));
        }
        let consistent_point = parse_lsn(text(&row, 1)?)?;
        self.state = State::Ready;
        Ok(CreatedSlot {
            name: slot.into(),
            consistent_point,
        })
    }

    fn ready(&self) -> Result<()> {
        if self.state != State::Ready {
            Err(ReplicationError::Invalid(
                "connection busy or failed; reconnect required",
            ))
        } else {
            Ok(())
        }
    }

    /// Validates the named pgoutput slot, database and retained cursor before COPY.
    /// Never creates a missing slot or substitutes a server-provided position.
    pub async fn start(
        &mut self,
        slot: &str,
        durable_lsn: u64,
        publications: &[String],
    ) -> Result<()> {
        let command = start_command(slot, durable_lsn, publications)?;
        self.ready()?;
        self.state = State::Busy;
        // Slot creation can advance WAL after the initial discovery. Refresh the
        // server position and recheck identity before validating the cursor.
        let current = self.wire.query_one("IDENTIFY_SYSTEM").await?;
        let actual = Lineage {
            system_id: text(&current, 0)?
                .parse()
                .map_err(|_| ReplicationError::Invalid("system id"))?,
            timeline: text(&current, 1)?
                .parse()
                .map_err(|_| ReplicationError::Invalid("timeline"))?,
            xlogpos: parse_lsn(text(&current, 2)?)?,
            database: text(&current, 3)?.into(),
        };
        self.lineage.check(&actual)?;
        self.lineage = actual;
        let query = format!(
            "SELECT slot_name, plugin, slot_type, database, restart_lsn::text, confirmed_flush_lsn::text, wal_status FROM pg_replication_slots WHERE slot_name = '{slot}'"
        );
        let rows = self.wire.query(&query).await?;
        validate_slot(&rows, slot, &self.lineage, durable_lsn)?;
        self.wire.send_query(&command).await?;
        loop {
            let frame = self.wire.frame().await?;
            match frame.tag {
                b'W' if frame.payload == [0, 0, 0] => break,
                b'N' => {}
                b'E' => {
                    let error = wire::server_error(&frame.payload)?;
                    return Err(match &error {
                        ReplicationError::Server { code, .. } if code == "42704" => {
                            ReplicationError::RecoveryRequired(RecoveryRequired::MissingSlot(
                                slot.into(),
                            ))
                        }
                        ReplicationError::Server { code, .. } if code == "55000" => {
                            ReplicationError::RecoveryRequired(RecoveryRequired::SlotInvalidated(
                                slot.into(),
                            ))
                        }
                        _ => error,
                    });
                }
                _ => return Err(ReplicationError::Invalid("expected CopyBothResponse")),
            }
        }
        self.state = State::Streaming;
        self.durable_lsn = durable_lsn;
        self.next_status = Instant::now();
        Ok(())
    }

    /// Cancellation-safe polling, including cancellation during feedback writes.
    /// Call regularly, or select this future against cancellation/ack changes.
    /// While the caller processes a message, it owns the scheduling responsibility.
    /// Durable positions must be monotonic even across cancelled calls.
    pub async fn next(&mut self, durable_ack_lsn: u64) -> Result<ReplicationMessage> {
        if self.state != State::Streaming {
            return Err(ReplicationError::Invalid(
                "not streaming; reconnect required",
            ));
        }
        if durable_ack_lsn < self.durable_lsn {
            return Err(ReplicationError::Invalid(
                "durable acknowledgement regressed",
            ));
        }
        self.durable_lsn = durable_ack_lsn;
        let result = self.next_inner().await;
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }

    async fn next_inner(&mut self) -> Result<ReplicationMessage> {
        // A prior cancelled call may have queued/partially written a status frame.
        self.wire.flush_pending().await?;
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(self.next_status) => {
                    self.queue_status()?;
                    self.wire.flush_pending().await?;
                }
                frame = self.wire.frame() => {
                    let BackendFrame { tag, payload } = frame?;
                    match tag {
                        b'd' => {
                            if matches!(self.decoder.decode_copy_data(&payload)?, CopyData::PrimaryKeepalive { reply_requested: true, .. }) {
                                self.queue_status()?;
                                self.wire.flush_pending().await?;
                            }
                            return Ok(ReplicationMessage { payload, decoder: self.decoder });
                        }
                        b'N' => {},
                        b'E' => return Err(wire::server_error(&payload)?),
                        _ => return Err(ReplicationError::Invalid("unexpected replication frame; reconnect required")),
                    }
                }
            }
        }
    }

    fn queue_status(&mut self) -> Result<()> {
        let bytes = status(self.durable_lsn, wire::pg_time()?)?;
        self.wire.queue(bytes.to_vec())?;
        self.next_status = Instant::now() + self.interval;
        Ok(())
    }
}

fn status(durable_lsn: u64, client_time: i64) -> Result<[u8; 39]> {
    Ok(StandbyStatusUpdate {
        write_lsn: durable_lsn,
        flush_lsn: durable_lsn,
        apply_lsn: durable_lsn,
        client_time,
        reply_requested: false,
    }
    .encode_copy_data(durable_lsn)?)
}

fn validate_slot(
    rows: &[Vec<Option<String>>],
    slot: &str,
    lineage: &Lineage,
    durable: u64,
) -> Result<()> {
    let reason = if rows.is_empty() {
        Some(RecoveryRequired::MissingSlot(slot.into()))
    } else if rows.len() != 1
        || rows[0].len() != 7
        || rows[0][0].as_deref() != Some(slot)
        || rows[0][1].as_deref() != Some("pgoutput")
        || rows[0][2].as_deref() != Some("logical")
        || rows[0][3].as_deref() != Some(lineage.database.as_str())
    {
        Some(RecoveryRequired::SlotIdentityChanged(slot.into()))
    } else if rows[0][4].is_none()
        || rows[0][5].is_none()
        || !matches!(
            rows[0][6].as_deref(),
            Some("reserved" | "extended" | "unreserved")
        )
    {
        Some(RecoveryRequired::SlotInvalidated(slot.into()))
    } else if parse_lsn(text(&rows[0], 4)?)? > durable
        || parse_lsn(text(&rows[0], 5)?)? > durable
        || lineage.xlogpos < durable
    {
        Some(RecoveryRequired::PositionUnavailable {
            durable_lsn: durable,
        })
    } else {
        None
    };
    match reason {
        Some(r) => Err(ReplicationError::RecoveryRequired(r)),
        None => Ok(()),
    }
}

fn slot_name(slot: &str) -> Result<()> {
    if slot.is_empty()
        || slot.len() > 63
        || !slot
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(ReplicationError::Invalid(
            "slot must contain 1–63 lowercase ASCII letters, digits or underscores",
        ));
    }
    Ok(())
}

pub fn create_slot_command(version: PostgresVersion, slot: &str) -> Result<String> {
    slot_name(slot)?;
    // S1 explicitly verified the parenthesized form on each of 16, 17 and 18.
    let suffix = match version {
        PostgresVersion::V16 | PostgresVersion::V17 | PostgresVersion::V18 => {
            "(SNAPSHOT 'nothing')"
        }
    };
    Ok(format!(
        "CREATE_REPLICATION_SLOT \"{slot}\" LOGICAL pgoutput {suffix}"
    ))
}

pub fn start_command(slot: &str, lsn: u64, publications: &[String]) -> Result<String> {
    slot_name(slot)?;
    if publications.is_empty() || publications.len() > 256 {
        return Err(ReplicationError::Invalid(
            "1–256 publication names required",
        ));
    }
    let mut names = Vec::with_capacity(publications.len());
    for name in publications {
        wire::cstring(name)?;
        if name.is_empty() || name.len() > 63 {
            return Err(ReplicationError::Invalid("publication name length"));
        }
        names.push(format!("\"{}\"", name.replace('"', "\"\"")));
    }
    let names = names.join(",").replace('\'', "''");
    // Standard-conforming strings are forced in startup. Backslashes are data.
    Ok(format!(
        "START_REPLICATION SLOT \"{slot}\" LOGICAL {:X}/{:X} (proto_version '1', publication_names '{names}', messages 'true', origin 'none', streaming 'off')",
        lsn >> 32,
        lsn & 0xffff_ffff
    ))
}

pub fn parse_lsn(value: &str) -> Result<u64> {
    let (hi, lo) = value
        .split_once('/')
        .ok_or(ReplicationError::Invalid("LSN"))?;
    if hi.is_empty()
        || lo.is_empty()
        || hi.len() > 8
        || lo.len() > 8
        || !hi.bytes().chain(lo.bytes()).all(|b| b.is_ascii_hexdigit())
    {
        return Err(ReplicationError::Invalid("LSN"));
    }
    Ok(
        (u64::from_str_radix(hi, 16).map_err(|_| ReplicationError::Invalid("LSN"))? << 32)
            | u64::from_str_radix(lo, 16).map_err(|_| ReplicationError::Invalid("LSN"))?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lineage() -> Lineage {
        Lineage {
            system_id: 123,
            timeline: 1,
            xlogpos: 0x100,
            database: "db".into(),
        }
    }

    #[test]
    fn commands_match_s1_on_all_supported_versions() {
        for version in [
            PostgresVersion::V16,
            PostgresVersion::V17,
            PostgresVersion::V18,
        ] {
            assert_eq!(
                create_slot_command(version, "capture").unwrap(),
                "CREATE_REPLICATION_SLOT \"capture\" LOGICAL pgoutput (SNAPSHOT 'nothing')"
            );
        }
        assert_eq!(
            start_command("capture", 0x123456789, &["pub".into()]).unwrap(),
            "START_REPLICATION SLOT \"capture\" LOGICAL 1/23456789 (proto_version '1', publication_names '\"pub\"', messages 'true', origin 'none', streaming 'off')"
        );
        for version in ["15.9", "19.0", "devel", ""] {
            assert!(PostgresVersion::parse(version).is_err());
        }
        assert_eq!(
            PostgresVersion::parse("16.10 (vendor build)").unwrap(),
            PostgresVersion::V16
        );
    }

    #[test]
    fn command_input_is_quoted_and_bounded() {
        for slot in ["", "CAPTURE", "a;DROP", "a'b", "a\\b", "a\0b", "é"] {
            assert!(create_slot_command(PostgresVersion::V16, slot).is_err());
            assert!(start_command(slot, 1, &["pub".into()]).is_err());
        }
        assert!(create_slot_command(PostgresVersion::V16, &"a".repeat(64)).is_err());
        assert!(start_command("slot", 1, &[]).is_err());
        assert!(start_command("slot", 1, &["a\0b".into()]).is_err());
        assert!(start_command("slot", 1, &["".into()]).is_err());
        assert!(start_command("slot", 1, &vec!["pub".into(); 257]).is_err());
        let command = start_command("slot", 1, &["a\"b'c\\d, e".into(), "二".into()]).unwrap();
        assert!(command.contains("publication_names '\"a\"\"b''c\\d, e\",\"二\"'"));
    }

    #[test]
    fn lineage_requires_system_timeline_and_database_but_allows_wal_growth() {
        let expected = lineage();
        let mut actual = expected.clone();
        actual.xlogpos += 100;
        expected.check(&actual).unwrap();
        actual.system_id += 1;
        assert!(matches!(
            expected.check(&actual),
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::SystemIdChanged { .. }
            ))
        ));
        actual = expected.clone();
        actual.timeline += 1;
        assert!(matches!(
            expected.check(&actual),
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::TimelineChanged { .. }
            ))
        ));
        actual = expected.clone();
        actual.database = "other".into();
        assert!(matches!(
            expected.check(&actual),
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::SlotIdentityChanged(_)
            ))
        ));
    }

    #[test]
    fn slot_missing_changed_invalidated_or_ahead_requires_recovery() {
        let row = [
            "slot", "pgoutput", "logical", "db", "0/10", "0/20", "reserved",
        ]
        .map(|v| Some(v.into()))
        .to_vec();
        validate_slot(std::slice::from_ref(&row), "slot", &lineage(), 0x20).unwrap();
        assert!(matches!(
            validate_slot(&[], "slot", &lineage(), 0x20),
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::MissingSlot(_)
            ))
        ));
        for (index, value) in [
            (0, Some("other")),
            (1, Some("other")),
            (1, None),
            (2, Some("physical")),
            (3, Some("other")),
        ] {
            let mut bad = row.clone();
            bad[index] = value.map(String::from);
            assert!(matches!(
                validate_slot(&[bad], "slot", &lineage(), 0x20),
                Err(ReplicationError::RecoveryRequired(
                    RecoveryRequired::SlotIdentityChanged(_)
                ))
            ));
        }
        for index in [4, 5, 6] {
            let mut bad = row.clone();
            bad[index] = None;
            assert!(matches!(
                validate_slot(&[bad], "slot", &lineage(), 0x20),
                Err(ReplicationError::RecoveryRequired(
                    RecoveryRequired::SlotInvalidated(_)
                ))
            ));
        }
        let mut bad = row.clone();
        bad[6] = Some("lost".into());
        assert!(matches!(
            validate_slot(&[bad], "slot", &lineage(), 0x20),
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::SlotInvalidated(_)
            ))
        ));
        for (index, value) in [(4, "0/30"), (5, "0/30")] {
            let mut bad = row.clone();
            bad[index] = Some(value.into());
            assert!(matches!(
                validate_slot(&[bad], "slot", &lineage(), 0x20),
                Err(ReplicationError::RecoveryRequired(
                    RecoveryRequired::PositionUnavailable { .. }
                ))
            ));
        }
        assert!(matches!(
            validate_slot(&[row], "slot", &lineage(), 0x200),
            Err(ReplicationError::RecoveryRequired(
                RecoveryRequired::PositionUnavailable { .. }
            ))
        ));
    }

    #[test]
    fn feedback_uses_only_the_durable_position() {
        for durable in [0, 1, 0x123456789, u64::MAX] {
            let frame = status(durable, -5).unwrap();
            for offset in [6, 14, 22] {
                assert_eq!(&frame[offset..offset + 8], &durable.to_be_bytes());
            }
            assert_eq!(&frame[30..38], &(-5_i64).to_be_bytes());
            assert_eq!(frame[38], 0);
        }
    }

    #[test]
    fn strict_lsn_boundaries() {
        assert_eq!(parse_lsn("FFFFFFFF/FFFFFFFF").unwrap(), u64::MAX);
        assert_eq!(parse_lsn("0/0").unwrap(), 0);
        for bad in [
            "/1",
            "1/",
            "1/2/3",
            "0x1/2",
            "100000000/0",
            "1/100000000",
            "+1/2",
            "1/ 2",
        ] {
            assert!(parse_lsn(bad).is_err());
        }
    }

    #[tokio::test]
    async fn tls_refusal_never_sends_startup_or_credentials() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        // Fault injection at SSL negotiation, paired with the harness real-TLS
        // test. This peer implements no authentication or database behavior.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut roots = RootCertStore::empty();
        roots.roots.push(rustls::pki_types::TrustAnchor {
            subject: Vec::new().into(),
            subject_public_key_info: Vec::new().into(),
            name_constraints: None,
        });
        let config = ConnectionConfig {
            host: "127.0.0.1".into(),
            port: listener.local_addr().unwrap().port(),
            server_name: "localhost".into(),
            user: "secret-user".into(),
            password: "secret-password".into(),
            database: "source".into(),
            roots,
            connect_timeout: Duration::from_secs(2),
            status_interval: Duration::from_secs(1),
            limits: Limits::default(),
        };
        let (connection, ()) = tokio::join!(ReplicationConnection::connect(&config), async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 8];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
            socket.write_all(b"N").await.unwrap();
            let mut unexpected = [0; 1];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut unexpected))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        assert!(matches!(
            connection,
            Err(ReplicationError::Authentication("server refused TLS"))
        ));
    }
}
