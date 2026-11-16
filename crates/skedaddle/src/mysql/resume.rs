//! Pure retention and source-lineage checks, run before every dump request.
use super::{GtidSet, MysqlPosition, client::MysqlStatus};
use std::fmt;

pub type ServerObservation = MysqlStatus;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResumePlan {
    Gtid {
        set: GtidSet,
        /// Diagnostic/reconnect hint only; never constrains a GTID dump.
        reconnect_hint: (String, u64),
    },
    File {
        file: String,
        offset: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResumeBlocker {
    HistoryLost {
        missing: GtidSet,
    },
    FilePurged {
        file: String,
    },
    OffsetOutsideFile {
        file: String,
        offset: u64,
        size: u64,
    },
    SourceMismatch {
        durable: String,
        observed: String,
    },
    Divergent {
        server: String,
        missing: GtidSet,
    },
    GtidDisabled,
    TaggedGtid,
}
impl fmt::Display for ResumeBlocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HistoryLost { missing } => write!(
                f,
                "history lost: uncaptured transactions {missing} were purged; restart the migration with a new copy"
            ),
            Self::FilePurged { file } => write!(
                f,
                "history lost: binary log {file} was purged; restart the migration with a new copy"
            ),
            Self::OffsetOutsideFile { file, offset, size } => write!(
                f,
                "history lost: offset {offset} is outside binary log {file} (size {size}) or the dump range 4..=u32::MAX; restore the source history or restart with a new copy"
            ),
            Self::SourceMismatch { durable, observed } => write!(
                f,
                "source identity mismatch: durable server {durable}, connected server {observed}; reconnect to the original source or restart with a new copy"
            ),
            Self::Divergent { server, missing } => write!(
                f,
                "server {server} is missing captured transactions {missing}; reconnect to a server with the complete captured history"
            ),
            Self::GtidDisabled => f.write_str(
                "GTID resume requires gtid_mode=ON; restore GTID mode or restart with a new copy",
            ),
            Self::TaggedGtid => f.write_str(
                "tagged GTIDs are unsupported; use an untagged source and restart with a new copy",
            ),
        }
    }
}
impl std::error::Error for ResumeBlocker {}

fn tagged(set: &GtidSet) -> bool {
    set.to_string().split(',').any(|group| {
        group
            .split(':')
            .skip(1)
            .any(|part| part.bytes().any(|c| !c.is_ascii_digit() && c != b'-'))
    })
}
fn file_plan(
    server: &str,
    file: String,
    offset: u64,
    observed: &ServerObservation,
) -> Result<ResumePlan, ResumeBlocker> {
    if !server.eq_ignore_ascii_case(&observed.server_uuid) {
        return Err(ResumeBlocker::SourceMismatch {
            durable: server.into(),
            observed: observed.server_uuid.clone(),
        });
    }
    let size = observed
        .binary_logs
        .iter()
        .find(|(name, _)| *name == file)
        .map(|(_, size)| *size)
        .ok_or_else(|| ResumeBlocker::FilePurged { file: file.clone() })?;
    if offset < 4 || offset > u64::from(u32::MAX) || offset > size {
        return Err(ResumeBlocker::OffsetOutsideFile { file, offset, size });
    }
    Ok(ResumePlan::File { file, offset })
}

pub fn check(
    durable: &MysqlPosition,
    observed: &ServerObservation,
) -> Result<ResumePlan, ResumeBlocker> {
    match durable {
        MysqlPosition::Gtid {
            set,
            server_uuid,
            file,
            offset,
        } => {
            if !observed.gtid_on {
                return Err(ResumeBlocker::GtidDisabled);
            }
            if [set, &observed.executed, &observed.purged]
                .into_iter()
                .any(tagged)
            {
                return Err(ResumeBlocker::TaggedGtid);
            }
            let missing = set.difference(&observed.executed);
            if missing != GtidSet::default() {
                return Err(ResumeBlocker::Divergent {
                    server: observed.server_uuid.clone(),
                    missing,
                });
            }
            let missing = observed.purged.difference(set);
            if missing != GtidSet::default() {
                return Err(ResumeBlocker::HistoryLost { missing });
            }
            // Empty SID blocks cannot use auto-position. Preserve the DURABLE
            // file/offset, never the server's newer current status coordinates.
            if *set == GtidSet::default() {
                return file_plan(server_uuid, file.clone(), *offset, observed);
            }
            Ok(ResumePlan::Gtid {
                set: set.clone(),
                reconnect_hint: (file.clone(), *offset),
            })
        }
        MysqlPosition::File {
            server_uuid,
            file_seq,
            offset,
        } => {
            let file = observed
                .binary_logs
                .iter()
                .find(|(name, _)| {
                    name.rsplit('.').next().and_then(|s| s.parse::<u64>().ok()) == Some(*file_seq)
                })
                .map(|(name, _)| name.clone())
                .unwrap_or_else(|| {
                    let prefix = observed
                        .file
                        .rsplit_once('.')
                        .map_or("mysql-bin", |(prefix, _)| prefix);
                    format!("{prefix}.{file_seq:06}")
                });
            file_plan(server_uuid, file, *offset, observed)
        }
    }
}

#[cfg(test)]
mod tests;
