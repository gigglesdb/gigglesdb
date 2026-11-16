//! CA- and hostname-verified MySQL connections and versioned replication requests.
use super::{
    GtidSet, MysqlPosition,
    binlog::{BinlogDecoder, MysqlEvent, PositionedEvent},
};
use futures::Stream;
use mysql_async::{
    BinlogStream, BinlogStreamRequest, Conn, OptsBuilder, SslOpts, prelude::Queryable,
};
use mysql_common::packets::{GnoInterval, Sid};
use serde::Deserialize;
use std::{
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Debug)]
pub enum MysqlError {
    Io(std::io::Error),
    Driver(mysql_async::Error),
    UnsupportedVersion(String),
    Invalid(&'static str),
    UnsupportedEvent(u8),
    PayloadTooLarge { limit: usize },
    MissingPrimaryKey { schema: String, table: String },
    Value(&'static str),
    Assembly(&'static str),
    TransactionTooLarge { limit: usize },
    Event(crate::event::EventError),
}
impl std::fmt::Display for MysqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for MysqlError {}
impl From<std::io::Error> for MysqlError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<mysql_async::Error> for MysqlError {
    fn from(e: mysql_async::Error) -> Self {
        Self::Driver(e)
    }
}
impl From<crate::event::EventError> for MysqlError {
    fn from(e: crate::event::EventError) -> Self {
        Self::Event(e)
    }
}
pub type Result<T> = std::result::Result<T, MysqlError>;

// Deliberately no Debug: configurations contain credentials.
#[derive(Clone, Deserialize)]
pub struct MysqlConfig {
    pub host: String,
    pub port: u16,
    pub ca: PathBuf,
    pub user: String,
    pub password: String,
    pub profile: String,
    pub gtid: String,
    pub container_id: String,
    pub max_decompressed_bytes: usize,
}
impl MysqlConfig {
    pub async fn connect(&self) -> Result<MysqlClient> {
        // Other workspace dependencies enable a second rustls provider. The pinned
        // driver uses the process default; retain one already selected by the host.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let opts = OptsBuilder::default()
            .ip_or_hostname(&self.host)
            .tcp_port(self.port)
            .user(Some(&self.user))
            .pass(Some(&self.password))
            .ssl_opts(SslOpts::default().with_root_certs(vec![self.ca.clone().into()]));
        let mut conn = Conn::new(opts).await?;
        let version: String = conn
            .query_first("SELECT @@version")
            .await?
            .ok_or(MysqlError::Invalid("missing version"))?;
        let version = MysqlVersion::parse(&version)?;
        let cipher: (String, String) = conn
            .query_first("SHOW STATUS LIKE 'Ssl_cipher'")
            .await?
            .ok_or(MysqlError::Invalid("missing TLS status"))?;
        if cipher.1.is_empty() {
            return Err(MysqlError::Invalid("TLS required"));
        }
        Ok(MysqlClient {
            conn,
            version,
            limit: self.max_decompressed_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MysqlVersion {
    V80,
    V84,
}
impl MysqlVersion {
    pub fn parse(version: &str) -> Result<Self> {
        let prefix = version.split('-').next().unwrap_or("");
        let parts: Vec<_> = prefix.split('.').collect();
        if parts.len() == 3 && parts[2].parse::<u32>().is_ok() {
            match (parts[0], parts[1]) {
                ("8", "0") => return Ok(Self::V80),
                ("8", "4") => return Ok(Self::V84),
                _ => {}
            }
        }
        Err(MysqlError::UnsupportedVersion(version.into()))
    }
    pub fn status_sql(self) -> &'static str {
        match self {
            Self::V80 => "SHOW MASTER STATUS",
            Self::V84 => "SHOW BINARY LOG STATUS",
        }
    }
}

#[derive(Debug)]
pub struct MysqlStatus {
    pub file: String,
    pub offset: u64,
    pub server_uuid: String,
    pub executed: GtidSet,
    pub purged: GtidSet,
    pub gtid_on: bool,
    pub binary_logs: Vec<(String, u64)>,
}
pub struct MysqlClient {
    pub conn: Conn,
    pub version: MysqlVersion,
    limit: usize,
}
/// Decoded live events, including the inner events emitted by mysql_async exactly once.
pub struct MysqlBinlogStream {
    transport: BinlogStream,
    decoder: BinlogDecoder,
    ended: bool,
    file: String,
    payload_end: Option<u64>,
}
impl MysqlBinlogStream {
    pub async fn close(self) -> Result<()> {
        self.transport.close().await?;
        Ok(())
    }
}
impl Stream for MysqlBinlogStream {
    type Item = Result<PositionedEvent>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while !this.ended {
            let event = match std::task::ready!(Pin::new(&mut this.transport).poll_next(cx)) {
                Some(Ok(event)) => event,
                Some(Err(error)) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(error.into())));
                }
                None => {
                    this.ended = true;
                    break;
                }
            };
            let end = u64::from(event.header().log_pos());
            match this.decoder.decode_stream_event(&event) {
                Ok(Some(decoded)) => {
                    if let MysqlEvent::Rotate { file, .. } = &decoded {
                        this.file = match String::from_utf8(file.clone()) {
                            Ok(file) => file,
                            Err(_) => {
                                this.ended = true;
                                return Poll::Ready(Some(Err(MysqlError::Invalid(
                                    "binlog filename",
                                ))));
                            }
                        };
                    }
                    let log_pos = this.payload_end.unwrap_or(end);
                    if matches!(
                        decoded,
                        MysqlEvent::Xid(_)
                            | MysqlEvent::Query {
                                kind: super::binlog::QueryKind::Commit,
                                ..
                            }
                    ) {
                        this.payload_end = None;
                    }
                    return Poll::Ready(Some(Ok(PositionedEvent {
                        event: decoded,
                        file: this.file.clone(),
                        log_pos,
                    })));
                }
                Ok(None) => {
                    this.payload_end = Some(end);
                    continue;
                }
                Err(error) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
        Poll::Ready(None)
    }
}
pub struct MysqlBinlogRequest {
    server_id: u32,
    file: Vec<u8>,
    offset: u64,
    sids: Option<Vec<Sid<'static>>>,
}
impl MysqlBinlogRequest {
    pub fn request(&self) -> BinlogStreamRequest<'_> {
        let request = BinlogStreamRequest::new(self.server_id)
            .with_filename(&self.file)
            .with_pos(self.offset);
        match &self.sids {
            Some(sids) => request.with_gtid().with_gtid_set(sids.clone()),
            None => request,
        }
    }
}
impl MysqlClient {
    /// Consumes the verified TLS connection. An empty GTID request uses the current
    /// status file/offset only for the dump; callers still track GTIDs from events.
    pub async fn open_stream(
        mut self,
        position: &MysqlPosition,
        server_id: u32,
        heartbeat: Duration,
    ) -> Result<MysqlBinlogStream> {
        let request = self.binlog_request(position, server_id, heartbeat).await?;
        Ok(MysqlBinlogStream {
            transport: self.conn.get_binlog_stream(request.request()).await?,
            decoder: BinlogDecoder::new(self.limit),
            ended: false,
            file: String::from_utf8(request.file.clone())
                .map_err(|_| MysqlError::Invalid("binlog filename"))?,
            payload_end: None,
        })
    }
    pub async fn status(&mut self) -> Result<MysqlStatus> {
        let row: mysql_async::Row = self
            .conn
            .query_first(self.version.status_sql())
            .await?
            .ok_or(MysqlError::Invalid("binary logging disabled"))?;
        let (executed, purged, server_uuid, mode): (String, String, String, String) = self
            .conn
            .query_first("SELECT @@gtid_executed,@@gtid_purged,@@server_uuid,@@gtid_mode")
            .await?
            .ok_or(MysqlError::Invalid("missing server variables"))?;
        let logs: Vec<mysql_async::Row> = self.conn.query("SHOW BINARY LOGS").await?;
        let binary_logs = logs
            .iter()
            .map(|r| {
                Ok((
                    r.get(0).ok_or(MysqlError::Invalid("log filename"))?,
                    r.get(1).ok_or(MysqlError::Invalid("log size"))?,
                ))
            })
            .collect::<Result<_>>()?;
        Ok(MysqlStatus {
            file: row.get(0).ok_or(MysqlError::Invalid("status filename"))?,
            offset: row.get(1).ok_or(MysqlError::Invalid("status offset"))?,
            server_uuid,
            executed: executed
                .parse()
                .map_err(|_| MysqlError::Invalid("executed GTIDs"))?,
            purged: purged
                .parse()
                .map_err(|_| MysqlError::Invalid("purged GTIDs"))?,
            gtid_on: mode == "ON",
            binary_logs,
        })
    }
    pub async fn binlog_request(
        &mut self,
        position: &MysqlPosition,
        server_id: u32,
        heartbeat: Duration,
    ) -> Result<MysqlBinlogRequest> {
        if server_id == 0 || heartbeat.is_zero() || heartbeat.as_nanos() > u64::MAX.into() {
            return Err(MysqlError::Invalid("server id or heartbeat"));
        }
        let status = self.status().await?;
        let mut request = MysqlBinlogRequest {
            server_id,
            file: Vec::new(),
            offset: 4,
            sids: None,
        };
        let request = match position {
            MysqlPosition::Gtid { set, .. } => {
                if !status.gtid_on {
                    return Err(MysqlError::Invalid(
                        "GTID auto-position requires gtid_mode=ON",
                    ));
                }
                if set.to_string().is_empty() {
                    // mysql_common drops BINLOG_THROUGH_GTID for empty SID blocks.
                    if status.offset < 4 || status.offset > u32::MAX.into() {
                        return Err(MysqlError::Invalid(
                            "file offset outside COM_BINLOG_DUMP range",
                        ));
                    }
                    request.file = status.file.into_bytes();
                    request.offset = status.offset;
                } else {
                    request.sids = Some(sids(set)?);
                }
                request
            }
            MysqlPosition::File {
                server_uuid,
                file_seq,
                offset,
            } => {
                if *offset < 4 || *offset > u32::MAX.into() {
                    return Err(MysqlError::Invalid(
                        "file offset outside COM_BINLOG_DUMP range",
                    ));
                }
                if *server_uuid != status.server_uuid {
                    return Err(MysqlError::Invalid("file position server mismatch"));
                }
                let file = status
                    .binary_logs
                    .iter()
                    .find(|(name, _)| {
                        name.rsplit('.').next().and_then(|s| s.parse::<u64>().ok())
                            == Some(*file_seq)
                    })
                    .ok_or(MysqlError::Invalid("binlog file unavailable"))?;
                request.file = file.0.as_bytes().to_vec();
                request.offset = *offset;
                request
            }
        };
        self.conn
            .query_drop(format!(
                "SET @master_heartbeat_period={}",
                heartbeat.as_nanos()
            ))
            .await?;
        Ok(request)
    }
}
fn sids(set: &GtidSet) -> Result<Vec<Sid<'static>>> {
    let mut result = Vec::new();
    for group in set.to_string().split(',').filter(|s| !s.is_empty()) {
        let mut parts = group.split(':');
        let source = uuid::Uuid::parse_str(parts.next().unwrap())
            .map_err(|_| MysqlError::Invalid("GTID UUID"))?;
        let mut intervals = Vec::new();
        for interval in parts {
            let (start, end) = interval.split_once('-').unwrap_or((interval, interval));
            let start = start
                .parse::<u64>()
                .map_err(|_| MysqlError::Invalid("tagged GTID requests unsupported"))?;
            let end = end
                .parse::<u64>()
                .map_err(|_| MysqlError::Invalid("GTID interval"))?;
            intervals.push(GnoInterval::check_and_new(
                start,
                end.checked_add(1)
                    .ok_or(MysqlError::Invalid("GTID overflow"))?,
            )?);
        }
        result.push(Sid::new(*source.as_bytes()).with_intervals(intervals));
    }
    Ok(result)
}
