//! Index publication. The storage caller owns business eligibility.
use super::secondary_index::{self as si, Catalog, Descriptor, Kind, Sidecar};
use super::*;

fn kind(t: &DataType) -> Option<Kind> {
    Some(match t {
        DataType::Boolean => Kind::Boolean,
        DataType::SmallInt => Kind::SmallInt,
        DataType::Integer => Kind::Integer,
        DataType::BigInt => Kind::BigInt,
        DataType::Text => Kind::Text,
        DataType::Bytes => Kind::Bytes,
        _ => return None,
    })
}
struct View<'a>(&'a Relational);
impl Catalog for View<'_> {
    fn table_exists(&self, id: u64) -> bool {
        self.0.tables.values().any(|t| t.id == id)
    }
    fn column_kind(&self, table: u64, column: u64) -> Option<Kind> {
        self.0
            .tables
            .values()
            .find(|t| t.id == table)?
            .columns
            .iter()
            .find(|c| c.id == column)
            .and_then(|c| kind(&c.data_type))
    }
}
impl Relational {
    pub(crate) fn secondary_descriptors_engine(&self) -> &[Descriptor] {
        self.secondary.descriptors()
    }
    pub(crate) fn create_secondary_index_engine(
        &mut self,
        table: &TableDef,
        epoch: u64,
        name: &str,
        columns: &[u64],
        cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        self.create_secondary_index_internal_engine(table, epoch, name, columns, false, cancel)
    }
    pub(crate) fn create_secondary_index_internal_engine(
        &mut self,
        table: &TableDef,
        epoch: u64,
        name: &str,
        columns: &[u64],
        unique: bool,
        cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        self.create_secondary_index_body(table, epoch, name, columns, unique, true, cancel)
    }
    pub(crate) fn create_secondary_index_populated_engine(
        &mut self,
        table: &TableDef,
        epoch: u64,
        name: &str,
        columns: &[u64],
        unique: bool,
        cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        self.create_secondary_index_body(table, epoch, name, columns, unique, false, cancel)
    }
    #[allow(clippy::too_many_arguments)]
    fn create_secondary_index_body(
        &mut self,
        table: &TableDef,
        epoch: u64,
        name: &str,
        columns: &[u64],
        unique: bool,
        require_empty: bool,
        mut cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        if epoch != self.epoch || self.tables.get(&table.name) != Some(table) {
            return Err(SqlError::StaleSchema);
        }
        ident(name)?;
        if columns.is_empty()
            || columns.len() > si::MAX_ARITY
            || self.secondary.descriptors().len() >= si::MAX_INDEXES
        {
            return Err(SqlError::Limit);
        }
        super::unique(columns.iter())?;
        if self.secondary.descriptors().iter().any(|d| d.name == name) {
            return Err(SqlError::Duplicate);
        }
        if self
            .secondary
            .descriptors()
            .iter()
            .filter(|d| d.table == table.id)
            .count()
            >= si::MAX_INDEXES_PER_TABLE
        {
            return Err(SqlError::Limit);
        }
        for id in columns
            .iter()
            .copied()
            .chain(table.primary_key.iter().map(|p| table.columns[*p].id))
        {
            let c = table
                .columns
                .iter()
                .find(|c| c.id == id)
                .ok_or(SqlError::UnknownColumn)?;
            if kind(&c.data_type).is_none() {
                return Err(SqlError::Unsupported);
            }
        }
        // Typed creation retains its empty-database requirement. Relational holds
        // complete in-memory rows, so populated SQL creation builds the whole table.
        if require_empty && !self.rows_empty_engine() {
            return Err(SqlError::Unsupported);
        }
        self.secondary_validate_engine(&mut cancel)?;
        let mut next = self.next_id;
        let id = allocate(&mut next)?;
        let epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        let mut descriptors = self.secondary.descriptors().to_vec();
        let descriptor = Descriptor {
            id,
            table: table.id,
            name: name.to_owned(),
            columns: columns.to_vec(),
            key_encoding: 1,
            unique,
        };
        if unique {
            let proof = self.unique_readiness_engine(
                &descriptor,
                self.epoch,
                self.generation,
                super::secondary_index_unique_readiness::HARD,
                &mut cancel,
            )?;
            proof.consume_for(self, &descriptor, self.epoch, self.generation)?;
        }
        descriptors.push(descriptor);
        let staged = if require_empty {
            let mut staged = Sidecar::default();
            staged
                .build(&descriptors, &[], &View(self), &mut cancel)
                .map_err(|e| match e {
                    si::Error::Cancelled => SqlError::Aborted,
                    si::Error::Limit => SqlError::Limit,
                    _ => SqlError::Type,
                })?;
            staged
        } else {
            self.secondary_build_complete_engine(&descriptors, &mut cancel)?
        };
        if cancel() {
            return Err(SqlError::Aborted);
        }
        self.secondary = staged;
        self.next_id = next;
        self.epoch = epoch;
        Ok(id)
    }
}
