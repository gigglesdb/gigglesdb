//! Runner adapters retain PostgreSQL source installation and select destination effects.
use super::apply::{GigglesdbExecutor, install_progress};
use crate::{
    buffer::reader::ReaderCheckpoint,
    engine::{Installation, Installer, PgInstaller},
    event::AssembledTxn,
    mapping::{Mapping, Value, gigglesdb::GigglesdbMapping},
    pg::apply::{
        self,
        executor::{self, Connector, Destination, Executor},
    },
};
use async_trait::async_trait;

pub(crate) struct DestinationInstaller {
    pub pg: PgInstaller,
    pub gigglesdb: bool,
}
impl Installer for DestinationInstaller {
    fn identity(&self) -> String {
        self.pg.identity()
    }
    fn schema_baseline(&self) -> Option<String> {
        self.pg.schema_baseline()
    }
    fn slot_marker(&self) -> crate::engine::Result<Option<String>> {
        self.pg.slot_marker()
    }
    async fn install(&mut self, step: Installation) -> crate::engine::Result<()> {
        if self.gigglesdb && step == Installation::ProgressTables {
            // Engine::start has durably journaled this installation intent.
            install_progress(&mut self.pg.destination, |_| async { Ok(()) })
                .await
                .map_err(|_| {
                    crate::coordinator::Error::RecoveryRequired(
                        "GigglesDB progress installation failed".into(),
                    )
                })
        } else {
            self.pg.install(step).await
        }
    }
}

pub(crate) enum RunnerDestination<C> {
    Postgresql(Executor<C>),
    Gigglesdb(GigglesdbExecutor<C>),
}
impl<C: Connector> RunnerDestination<C> {
    pub async fn close(self) -> executor::Result<()> {
        match self {
            Self::Postgresql(executor) => executor.close().await,
            Self::Gigglesdb(executor) => executor.close().await,
        }
    }
}
#[async_trait]
impl<C: Connector> Destination for RunnerDestination<C> {
    async fn apply(
        &mut self,
        plan: &apply::TransactionPlan,
    ) -> executor::Result<apply::CommitDecision> {
        match self {
            Self::Postgresql(executor) => executor.apply(plan).await,
            Self::Gigglesdb(executor) => executor.apply(plan).await,
        }
    }
    async fn retain_after_checkpoint(
        &mut self,
        checkpoint: &ReaderCheckpoint,
    ) -> executor::Result<()> {
        match self {
            Self::Postgresql(executor) => executor.retain_after_checkpoint(checkpoint).await,
            Self::Gigglesdb(executor) => executor.retain_after_checkpoint(checkpoint).await,
        }
    }
}

pub(crate) fn check_types(table: &apply::Table) -> Result<(), apply::ApplyError> {
    for column in &table.columns {
        GigglesdbMapping::between(column.pg_type.clone(), &column.pg_type)?
            .domain_check(&Value::Null)?;
    }
    Ok(())
}

/// N4 mappings preserve the canonical value, while imposing GigglesDB's narrower
/// qualified domain. Check every image before the PG planner encodes parameters.
pub(crate) fn check_values(
    txn: &AssembledTxn,
    tables: &[apply::Table],
) -> Result<(), apply::ApplyError> {
    for event in &txn.events {
        let Some(name) = &event.table else { continue };
        let table = tables
            .iter()
            .find(|t| &t.event_table == name)
            .ok_or(apply::ApplyError::Invalid("unqualified GigglesDB table"))?;
        for row in std::iter::once(&event.key)
            .chain(event.before.iter())
            .chain(event.after.iter())
        {
            for (name, opaque) in row {
                let column = table
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or(apply::ApplyError::Invalid("unqualified GigglesDB column"))?;
                let value = opaque
                    .bytes
                    .as_deref()
                    .map(Value::from_canonical_bytes)
                    .transpose()?
                    .unwrap_or(Value::Null);
                let mapped = GigglesdbMapping::between(column.pg_type.clone(), &column.pg_type)?
                    .forward(&value)?;
                if mapped != value {
                    return Err(apply::ApplyError::Invalid(
                        "GigglesDB identity mapping changed value",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::PgType;
    use tokio_postgres::types::Type;

    #[test]
    fn runner_rejects_unqualified_types_even_without_rows() {
        let mut table = apply::Table {
            event_table: "public.t".into(),
            schema: "public".into(),
            name: "t".into(),
            schema_version: 0,
            key: vec!["id".into()],
            columns: vec![apply::Column {
                name: "id".into(),
                type_id: "int4".into(),
                pg_type: PgType::Int4,
                wire_type: Type::INT4,
            }],
        };
        assert!(check_types(&table).is_ok());
        for ty in [PgType::Json, PgType::Array(Box::new(PgType::Bool))] {
            table.columns[0].pg_type = ty;
            assert!(check_types(&table).is_err());
        }
    }
}
