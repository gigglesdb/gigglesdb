//! Index publication. The storage caller owns business eligibility.
use super::secondary_index::{self as si, Catalog, Descriptor, Kind, Sidecar};
use super::*;

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
            .and_then(|c| Kind::for_type(&c.data_type))
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
        self.create_secondary_index_body(table, epoch, name, columns, unique, true, None, None, cancel)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_secondary_index_populated_engine(
        &mut self,
        table: &TableDef,
        epoch: u64,
        name: &str,
        columns: &[u64],
        unique: bool,
        predicate: Option<partial_index::Predicate>,
        collations: Option<&[Option<column_collations::Reference>]>,
        cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        self.create_secondary_index_body(table, epoch, name, columns, unique, false, predicate, collations, cancel)
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
        predicate: Option<partial_index::Predicate>,
        collations: Option<&[Option<column_collations::Reference>]>,
        mut cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        if epoch != self.epoch || self.tables.get(&self.table_key(table)) != Some(table) {
            return Err(SqlError::StaleSchema);
        }
        ident(name)?;
        let key = relation_key(self.table_schema(table.id), name);
        let name = key.as_str();
        if columns.is_empty() || columns.len() > si::MAX_ARITY {
            return Err(SqlError::Limit);
        }
        super::unique(columns.iter())?;
        if self.secondary.descriptors().iter().any(|d| d.name == name) {
            return Err(SqlError::Duplicate);
        }
        if self.relation_name_taken(name) {
            return Err(SqlError::DuplicateTable);
        }
        if table.primary_key.iter().any(|p| table.columns[*p].data_type.is_temporal()) {
            return Err(SqlError::Unsupported);
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
            if Kind::for_type(&c.data_type).is_none() {
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
            predicate,
        };
        let mut equality_state;
        let equality = if let Some(collations) = collations {
            equality_state = self.clone();
            std::sync::Arc::make_mut(&mut equality_state.ext).column_collations.insert(id, column_collations::Binding::Index(collations.to_vec()));
            &equality_state
        } else { &*self };
        if unique {
            let duplicate = |error, cancel: &mut _| {
                if error == SqlError::Duplicate {
                    let _ = equality.validate_index_details(
                        table, &self.rows[&table.id], std::slice::from_ref(&descriptor),
                        true, |_| false, cancel,
                    );
                }
                error
            };
            equality.validate_collated_unique(
                table, &self.rows[&table.id], std::slice::from_ref(&descriptor), &mut cancel,
            ).map_err(|e| {
                if e == SqlError::Duplicate {
                    user_types::view_replace_error(format!(
                        "could not create unique index \"{}\"", split_key(name).1,
                    ));
                }
                duplicate(e, &mut cancel)
            })?;
            let proof = self.unique_readiness_engine(
                &descriptor,
                self.epoch,
                self.generation,
                super::secondary_index_unique_readiness::HARD,
                &mut cancel,
            ).map_err(|e| duplicate(e, &mut cancel))?;
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
            equality.secondary_build_complete_engine(&descriptors, &mut cancel)?
        };
        if cancel() {
            return Err(SqlError::Aborted);
        }
        self.secondary = staged;
        if let Some(collations) = collations {
            std::sync::Arc::make_mut(&mut self.ext).column_collations.insert(id, column_collations::Binding::Index(collations.to_vec()));
            std::sync::Arc::make_mut(&mut self.ext).ext_version = self.ext.ext_version.max(10);
        }
        self.next_id = next;
        self.epoch = epoch;
        Ok(id)
    }
}
