//! Authenticated MySQL tip, source-clock age and binlog retention observations.
//!
//! Register this probe before emitting its transactional watermark. The engine
//! admits the tip only when that exact marker is manifest-durably captured.
//! Schema is deliberately None until the neutral schema-boundary seam exists:
//! M8 DDL stops still stop capture, preventing the run from reaching readiness.
use super::{
    GtidSet, MysqlOrder, MysqlPosition,
    client::{MysqlConfig, MysqlError, MysqlStatus, Result},
    resume,
};
use crate::{
    engine::{LagMeasure, ReadinessProbe, ReadinessProbeSample, RetentionMargin},
    event::{EventError, Position, PositionOrder, SourceIdentity},
};
use futures::StreamExt;
use mysql_async::{BinlogStreamRequest, prelude::Queryable};
use mysql_common::binlog::events::EventData;
use std::{
    cmp::Ordering,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct LogEvidence {
    file: String,
    created_us: Option<i64>,
    previous: GtidSet,
}
#[derive(Debug)]
pub struct MysqlReadinessProbe {
    source: SourceIdentity,
    tip: Position,
    order: MysqlOrder,
    status: MysqlStatus,
    clock_us: i64,
    clock_at: Instant,
    started: Instant,
    expiry_seconds: u64,
    logs: Option<Vec<LogEvidence>>,
    retention_issue: Option<String>,
}
impl MysqlReadinessProbe {
    /// Uses separate TLS connections for bounded binlog header reads. Missing
    /// header evidence yields unknown retention, never a guessed file age.
    pub async fn observe(
        config: &MysqlConfig,
        source: SourceIdentity,
        durable: &MysqlPosition,
        server_id: u32,
    ) -> Result<Self> {
        if source.engine != "mysql" || source.system_id.is_empty() || server_id == 0 {
            return Err(MysqlError::Invalid("MySQL readiness identity or server id"));
        }
        let mut client = config.connect().await?;
        let mut status = client.status().await?;
        let gtid = matches!(durable, MysqlPosition::Gtid { .. });
        let first = file_hint(durable, &status);
        let mut retention_issue = None;
        let mut logs = if let Some(first) = first {
            let from = status.binary_logs.iter().position(|(f, _)| *f == first);
            let to = status
                .binary_logs
                .iter()
                .position(|(f, _)| *f == status.file);
            if let (Some(from), Some(to)) = (from, to) {
                if from <= to && to - from < 64 {
                    let headers = async {
                        let mut logs = Vec::new();
                        for (file, _) in &status.binary_logs[from..=to] {
                            logs.push(
                                log_header(config, file, &status.server_uuid, server_id, gtid)
                                    .await?,
                            );
                        }
                        Ok::<_, MysqlError>(logs)
                    };
                    tokio::time::timeout(Duration::from_secs(10), headers)
                        .await
                        .inspect_err(|_| {
                            retention_issue = Some("binlog header read timed out".into())
                        })
                        .ok()
                        .and_then(|r| {
                            r.inspect_err(|error| retention_issue = Some(error.to_string()))
                                .ok()
                        })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        // File headers are immutable preparation evidence. Start freshness at
        // the live status/clock sample, after TLS/header acquisition, then
        // revalidate the inventory so a purge or rotation cannot reuse it.
        let started = Instant::now();
        status = client.status().await?;
        // Bind executed GTIDs and file coordinates to the same status row.
        let row: mysql_async::Row = client
            .conn
            .query_first(client.version.status_sql())
            .await?
            .ok_or(MysqlError::Invalid("readiness status"))?;
        status.file = row
            .get(0)
            .ok_or(MysqlError::Invalid("readiness filename"))?;
        status.offset = row.get(1).ok_or(MysqlError::Invalid("readiness offset"))?;
        let executed: String = row.get(4).ok_or(MysqlError::Invalid("readiness GTIDs"))?;
        status.executed = executed.parse()?;
        let order = if gtid {
            MysqlOrder::gtid(&source.system_id)?
        } else {
            MysqlOrder::file(&status.server_uuid)?
        };
        let native = if gtid {
            MysqlPosition::Gtid {
                set: status.executed.clone(),
                server_uuid: status.server_uuid.clone(),
                file: status.file.clone(),
                offset: status.offset,
            }
        } else {
            MysqlPosition::File {
                server_uuid: status.server_uuid.clone(),
                file_seq: sequence(&status.file).ok_or(MysqlError::Invalid("binlog sequence"))?,
                offset: status.offset,
            }
        };
        let tip = Position::new(order.domain(), native.encode()?);
        let durable_position = Position::new(order.domain(), durable.encode()?);
        if durable_position.compare(&tip, &order)? == Ordering::Greater {
            return Err(EventError::InvalidEncoding("MySQL readiness applied beyond tip").into());
        }
        // Clock sample start gives a conservative upper age bound despite RTT.
        let clock_at = Instant::now();
        let (clock_us, expiry_seconds): (i64, u64) = client.conn.query_first(
            "SELECT CAST(UNIX_TIMESTAMP(NOW(6))*1000000 AS SIGNED), @@binlog_expire_logs_seconds"
        ).await?.ok_or(MysqlError::Invalid("readiness source clock"))?;
        if logs.as_ref().is_some_and(|logs| {
            logs.last().is_none_or(|log| log.file != status.file)
                || logs
                    .iter()
                    .any(|log| !status.binary_logs.iter().any(|(file, _)| *file == log.file))
        }) {
            logs = None;
            retention_issue = Some("binlog inventory changed during header acquisition".into());
        }
        Ok(Self {
            source,
            tip,
            order,
            status,
            clock_us,
            clock_at,
            started,
            expiry_seconds,
            logs,
            retention_issue,
        })
    }

    /// Header acquisition failures explain unknown retention without granting it.
    pub fn retention_diagnostic(&self) -> Option<&str> {
        self.retention_issue.as_deref()
    }

    fn retention(
        &self,
        applied: &Position,
        native: &MysqlPosition,
        now: i64,
    ) -> Option<RetentionMargin> {
        let available = resume::check(native, &self.status).is_ok();
        let remaining = if available {
            let logs = self.logs.as_ref()?;
            let needed = match native {
                MysqlPosition::File { .. } => {
                    let file = file_hint(native, &self.status)?;
                    logs.iter().find(|l| l.file == file)?
                }
                MysqlPosition::Gtid { set, .. } => {
                    // Previous-GTIDs in the next file delimit this file exactly;
                    // no row scan or transaction-count inference from bytes.
                    if !self
                        .status
                        .executed
                        .difference(set)
                        .is_subset(&self.status.executed.difference(&logs.first()?.previous))
                    {
                        return None;
                    }
                    let mut needed = logs.last()?;
                    for (i, log) in logs.iter().enumerate() {
                        let end = logs
                            .get(i + 1)
                            .map_or(&self.status.executed, |next| &next.previous);
                        if !log.previous.is_subset(end) {
                            return None;
                        }
                        if end.difference(&log.previous).difference(set) != GtidSet::default() {
                            needed = log;
                            break;
                        }
                    }
                    needed
                }
            };
            let age = u64::try_from(now.checked_sub(needed.created_us?)?)
                .ok()?
                .div_ceil(1_000_000);
            // Zero disables automatic expiration and supplies no finite bound.
            if self.expiry_seconds == 0 {
                return None;
            }
            self.expiry_seconds.saturating_sub(age)
        } else {
            0
        };
        Some(RetentionMargin {
            position: applied.clone(),
            remaining: LagMeasure {
                value: remaining,
                unit: "seconds",
            },
            history_available: available,
            time_to_exhaustion: Some(Duration::from_secs(remaining)),
        })
    }
}
impl ReadinessProbe for MysqlReadinessProbe {
    fn source(&self) -> &SourceIdentity {
        &self.source
    }
    fn tip(&self) -> &Position {
        &self.tip
    }
    fn observed_at(&self) -> Instant {
        self.started
    }
    fn sample(
        &self,
        applied: &Position,
        oldest_commit: Option<Option<i64>>,
    ) -> std::result::Result<ReadinessProbeSample, EventError> {
        if applied.compare(&self.tip, &self.order)? == Ordering::Greater {
            return Err(EventError::InvalidEncoding(
                "MySQL readiness applied beyond tip",
            ));
        }
        let native = MysqlPosition::decode(applied.bytes())?;
        let lag = match &native {
            MysqlPosition::Gtid { set, .. } if self.status.gtid_on => {
                count(&self.status.executed.difference(set)).map(|value| LagMeasure {
                    value,
                    unit: "transactions",
                })
            }
            MysqlPosition::File { .. } => {
                bytes_behind(&native, &self.status).map(|value| LagMeasure {
                    value,
                    unit: "bytes",
                })
            }
            _ => None,
        };
        let now = i64::try_from(self.clock_at.elapsed().as_micros())
            .ok()
            .and_then(|elapsed| self.clock_us.checked_add(elapsed));
        Ok(ReadinessProbeSample {
            lag,
            oldest_unapplied_age_ms: now.and_then(|now| commit_age(now, oldest_commit)),
            retention: now.and_then(|now| self.retention(applied, &native, now)),
            schema: None,
            observed_at: self.started,
        })
    }
}
fn commit_age(now: i64, oldest: Option<Option<i64>>) -> Option<u64> {
    match oldest {
        None => Some(0),
        Some(Some(commit)) if commit > 0 => u64::try_from(now.checked_sub(commit)?)
            .ok()
            .map(|age| age.div_ceil(1000)),
        _ => None,
    }
}
fn sequence(file: &str) -> Option<u64> {
    file.rsplit_once('.')?.1.parse().ok()
}
fn file_hint(position: &MysqlPosition, status: &MysqlStatus) -> Option<String> {
    match position {
        MysqlPosition::Gtid {
            server_uuid, file, ..
        } if server_uuid.eq_ignore_ascii_case(&status.server_uuid) => Some(file.clone()),
        MysqlPosition::File {
            server_uuid,
            file_seq,
            ..
        } if server_uuid.eq_ignore_ascii_case(&status.server_uuid) => status
            .binary_logs
            .iter()
            .find(|(f, _)| sequence(f) == Some(*file_seq))
            .map(|(f, _)| f.clone()),
        _ => None,
    }
}
fn bytes_behind(position: &MysqlPosition, status: &MysqlStatus) -> Option<u64> {
    let MysqlPosition::File { offset, .. } = position else {
        return None;
    };
    let file = file_hint(position, status)?;
    let from = status.binary_logs.iter().position(|(f, _)| *f == file)?;
    let to = status
        .binary_logs
        .iter()
        .position(|(f, _)| *f == status.file)?;
    if from > to || *offset < 4 {
        return None;
    }
    if from == to {
        return status.offset.checked_sub(*offset);
    }
    let mut bytes = status.binary_logs[from].1.checked_sub(*offset)?;
    for i in from + 1..=to {
        if sequence(&status.binary_logs[i].0)?
            != sequence(&status.binary_logs[i - 1].0)?.checked_add(1)?
        {
            return None;
        }
        // Sum physical binlog lengths across rotations, in native bytes.
        let end = if i == to {
            status.offset
        } else {
            status.binary_logs[i].1
        };
        bytes = bytes.checked_add(end)?;
    }
    Some(bytes)
}
fn count(set: &GtidSet) -> Option<u64> {
    // Work on canonical interval text, without expanding individual GTIDs.
    set.to_string()
        .split(',')
        .filter(|s| !s.is_empty())
        .try_fold(0u64, |sum, group| {
            group.split(':').skip(1).try_fold(sum, |sum, interval| {
                let (lo, hi) = interval.split_once('-').unwrap_or((interval, interval));
                sum.checked_add(
                    hi.parse::<u64>()
                        .ok()?
                        .checked_sub(lo.parse::<u64>().ok()?)?
                        .checked_add(1)?,
                )
            })
        })
}
async fn log_header(
    config: &MysqlConfig,
    file: &str,
    expected_server: &str,
    server_id: u32,
    gtid: bool,
) -> Result<LogEvidence> {
    let mut client = config.connect().await?;
    let server: Option<String> = client.conn.query_first("SELECT @@server_uuid").await?;
    if !server.is_some_and(|server| server.eq_ignore_ascii_case(expected_server)) {
        return Err(MysqlError::Invalid("retention header server changed"));
    }
    let mut stream = client
        .conn
        .get_binlog_stream(
            BinlogStreamRequest::new(server_id)
                .with_filename(file.as_bytes())
                .with_pos(4)
                .with_non_blocking(),
        )
        .await?;
    let result = async {
        let mut created_us = None;
        let mut read = 0u64;
        while let Some(event) = stream.next().await {
            let event = event?;
            read += u64::from(event.header().event_size());
            if read > 1_048_576 {
                break;
            }
            match event.read_data()? {
                Some(EventData::FormatDescriptionEvent(f)) => {
                    // Artificial startup events have no source creation time.
                    let seconds = if f.create_timestamp() > 0 {
                        f.create_timestamp()
                    } else {
                        event.header().timestamp()
                    };
                    if seconds > 0 {
                        created_us = Some(i64::from(seconds) * 1_000_000);
                    }
                    if !gtid {
                        return Ok(LogEvidence {
                            file: file.into(),
                            created_us,
                            previous: GtidSet::default(),
                        });
                    }
                }
                Some(EventData::PreviousGtidsEvent(p)) => {
                    let mut groups = Vec::new();
                    for sid in p.sids() {
                        if sid.tag().is_some() {
                            return Err(MysqlError::Invalid("tagged GTID retention"));
                        }
                        let mut group = uuid::Uuid::from_bytes(sid.uuid()).to_string();
                        for interval in sid.intervals() {
                            group.push_str(&format!(
                                ":{}-{}",
                                interval.start(),
                                interval.end() - 1
                            ));
                        }
                        groups.push(group);
                    }
                    return Ok(LogEvidence {
                        file: file.into(),
                        created_us,
                        previous: groups.join(",").parse()?,
                    });
                }
                _ => {}
            }
        }
        Err(MysqlError::Invalid("binlog retention header unavailable"))
    }
    .await;
    stream.close().await?;
    result
}

#[cfg(test)]
#[path = "readiness_tests.rs"]
mod tests;
