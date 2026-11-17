//! MySQL installation effects, dispatched only after the engine journals intent.
use super::{
    MysqlPosition,
    client::{MysqlClient, MysqlConfig, MysqlError, MysqlVersion},
    control,
    dest_schema::{self, MappedSchema},
    schema::TableDescriptor,
    startup::{GtidMode, MysqlStartupRecord, SelectedTable, StartupError},
};
use crate::{
    engine::{Installation, Installer, StartupMarker},
    event::SourceIdentity,
    pg,
    security::TlsPolicy,
};

pub const MAPPING_VERSION: &str = "mysql.v1";
pub const VALUE_VERSION: &str = "skva.v1";
const PLAN: &[Installation] = &[
    Installation::ControlSchema,
    Installation::SourceStartup,
    Installation::DestinationSchema,
    Installation::ProgressTables,
];

#[derive(Debug)]
pub enum InstallerError {
    UnsupportedStep(Installation),
    MissingStartup,
    InvalidSelection,
    Source(MysqlError),
    Startup(StartupError),
    Mapping(dest_schema::Blocker),
    Destination(dest_schema::Error),
    Connect(pg::tls::Error),
    Progress(pg::apply::executor::Error),
}
impl std::fmt::Display for InstallerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MySQL installer: {self:?}")
    }
}
impl std::error::Error for InstallerError {}
fn engine_error(e: InstallerError) -> crate::coordinator::Error {
    MysqlError::Installer(Box::new(e)).into()
}

/// Dedicated admin configuration; connection credentials never enter identity().
/// A recovered record must be supplied before retrying SourceStartup. The caller
/// restores it from the authoritative source-startup journal stream.
pub struct MysqlInstaller {
    source_config: MysqlConfig,
    destination_config: pg::tls::Config,
    source: Option<MysqlClient>,
    destination: Option<tokio_postgres::Client>,
    identity: SourceIdentity,
    tables: Vec<TableDescriptor>,
    selected: Vec<SelectedTable>,
    mapped: MappedSchema,
    pub startup_record: Option<MysqlStartupRecord>,
}
impl MysqlInstaller {
    pub fn new(
        source: MysqlConfig,
        destination: pg::tls::Config,
        identity: SourceIdentity,
        tables: Vec<TableDescriptor>,
        mut selected: Vec<SelectedTable>,
    ) -> Result<Self, InstallerError> {
        selected.sort_by(|a, b| (&a.schema, &a.table).cmp(&(&b.schema, &b.table)));
        let projected = selected
            .iter()
            .map(|s| {
                let matches: Vec<_> = tables
                    .iter()
                    .filter(|t| t.schema == s.schema && t.table == s.table)
                    .collect();
                if matches.len() != 1 {
                    return Err(InstallerError::InvalidSelection);
                }
                let mut t = matches[0].clone();
                if s.primary_key != t.primary_key {
                    return Err(InstallerError::InvalidSelection);
                }
                t.columns = s
                    .columns
                    .iter()
                    .map(|n| {
                        t.columns
                            .iter()
                            .find(|c| &c.name == n)
                            .cloned()
                            .ok_or(InstallerError::InvalidSelection)
                    })
                    .collect::<Result<_, _>>()?;
                Ok(t)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if projected.is_empty() {
            return Err(InstallerError::InvalidSelection);
        }
        let mapped = dest_schema::build(&projected).map_err(InstallerError::Mapping)?;
        // Validate complete descriptors and projection using the startup codec,
        // without treating these placeholder coordinates as installation truth.
        MysqlStartupRecord::build(
            &identity,
            GtidMode::OFF,
            MysqlVersion::V84,
            &MysqlPosition::File {
                server_uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
                file_seq: 1,
                offset: 4,
            },
            &selected,
            &tables,
            vec![MAPPING_VERSION.into(), VALUE_VERSION.into()],
        )
        .map_err(InstallerError::Startup)?;
        Ok(Self {
            source_config: source,
            destination_config: destination,
            source: None,
            destination: None,
            identity,
            tables,
            selected,
            mapped,
            startup_record: None,
        })
    }
    async fn source(&mut self) -> Result<&mut MysqlClient, InstallerError> {
        if self.source.is_none() {
            self.source = Some(
                self.source_config
                    .connect()
                    .await
                    .map_err(InstallerError::Source)?,
            );
        }
        Ok(self.source.as_mut().unwrap())
    }
    async fn destination(&mut self) -> Result<&mut tokio_postgres::Client, InstallerError> {
        if self.destination.is_none() {
            let (client, driver, _) =
                pg::tls::connect(&self.destination_config, &TlsPolicy::Required)
                    .await
                    .map_err(InstallerError::Connect)?;
            tokio::spawn(driver);
            self.destination = Some(client);
        }
        Ok(self.destination.as_mut().unwrap())
    }
    /// Typed entry point, also used by the engine adapter below.
    pub async fn install_step(&mut self, step: Installation) -> Result<(), InstallerError> {
        match step {
            Installation::Publication | Installation::DdlTrigger | Installation::Slot => {
                return Err(InstallerError::UnsupportedStep(step));
            }
            Installation::ControlSchema => {
                control::install_watermark_table(&mut self.source().await?.conn)
                    .await
                    .map_err(InstallerError::Source)?;
            }
            Installation::SourceStartup => {
                if self.startup_record.is_some() {
                    return Ok(());
                }
                let source = self.source().await?;
                let family = source.version;
                let status = source.status().await.map_err(InstallerError::Source)?;
                let mode = if status.gtid_on {
                    GtidMode::ON
                } else {
                    GtidMode::OFF
                };
                let position = if status.gtid_on {
                    MysqlPosition::Gtid {
                        set: status.executed,
                        server_uuid: status.server_uuid,
                        file: status.file,
                        offset: status.offset,
                    }
                } else {
                    let file_seq = status
                        .file
                        .rsplit('.')
                        .next()
                        .and_then(|s| s.parse().ok())
                        .ok_or(InstallerError::Source(MysqlError::Invalid(
                            "binlog file sequence",
                        )))?;
                    MysqlPosition::File {
                        server_uuid: status.server_uuid,
                        file_seq,
                        offset: status.offset,
                    }
                };
                self.startup_record = Some(
                    MysqlStartupRecord::build(
                        &self.identity,
                        mode,
                        family,
                        &position,
                        &self.selected,
                        &self.tables,
                        vec![MAPPING_VERSION.into(), VALUE_VERSION.into()],
                    )
                    .map_err(InstallerError::Startup)?,
                );
            }
            Installation::DestinationSchema => {
                let mapped = self.mapped.clone();
                let client = self.destination().await?;
                dest_schema::install(client, &mapped)
                    .await
                    .map_err(InstallerError::Destination)?;
                let mismatches = dest_schema::verify_destination(client, &mapped)
                    .await
                    .map_err(InstallerError::Destination)?;
                if !mismatches.is_empty() {
                    return Err(InstallerError::Destination(dest_schema::Error::Mismatches(
                        mismatches,
                    )));
                }
            }
            Installation::ProgressTables => {
                pg::apply::executor::install_opaque_progress(
                    self.destination().await?,
                    |_| async { Ok(()) },
                )
                .await
                .map_err(InstallerError::Progress)?;
            }
        }
        Ok(())
    }
}
impl Installer for MysqlInstaller {
    fn plan(&self) -> &'static [Installation] {
        PLAN
    }
    fn identity(&self) -> String {
        let digest: String = self
            .mapped
            .digest()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        serde_json::json!({"connector":"mysql", "tables":self.selected.iter()
            .map(|s| [&s.schema,&s.table]).collect::<Vec<_>>(),
            "projection":self.selected, "destination_schema_digest":digest})
        .to_string()
    }
    fn markers_after(&self, step: Installation) -> crate::engine::Result<Vec<StartupMarker>> {
        if step != Installation::SourceStartup {
            return Ok(vec![]);
        }
        let record = self
            .startup_record
            .as_ref()
            .ok_or_else(|| engine_error(InstallerError::MissingStartup))?;
        Ok(vec![StartupMarker {
            stream: "source-startup".into(),
            marker: record
                .encode()
                .map_err(|e| engine_error(InstallerError::Startup(e)))?,
            label: "installed".into(),
        }])
    }
    async fn install(&mut self, step: Installation) -> crate::engine::Result<()> {
        self.install_step(step).await.map_err(engine_error)
    }
}
#[cfg(test)]
#[path = "installer_tests.rs"]
mod tests;
