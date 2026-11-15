//! Whole-table removal deliberately bypasses row-level referential actions.
use super::*;
impl Prepared {
    pub(crate) fn truncate_tables(&self, state: &Relational) -> R<Vec<String>> {
        self.truncate_targets(state, true)
    }
    // Describe and authority resolve all targets, but utility execution owns
    // RESTRICT validation. In particular, read-only rejection must win before
    // checking whether an outside table references a requested target.
    pub(crate) fn truncate_authority_tables(&self, state: &Relational) -> R<Vec<String>> {
        self.truncate_targets(state, false)
    }
    fn truncate_targets(&self, state: &Relational, enforce_restrict: bool) -> R<Vec<String>> {
        let Statement::Truncate { names, cascade, .. } = &self.statement else {
            return Ok(vec![]);
        };
        self.validate_schemas(state)?;
        let mut ids = BTreeSet::new();
        for name in names {
            let table = state.tables.get(name).ok_or_else(|| {
                if state.relation_name_taken(name) {
                    SqlError::WrongObjectType
                } else {
                    SqlError::UnknownTable
                }
            })?;
            ids.insert(table.id);
        }
        loop {
            let before = ids.len();
            for (&child, constraints) in &state.ext.constraints {
                for constraint in constraints {
                    if let ConstraintKind::ForeignKey { ref_table, .. } = constraint.kind
                        && ids.contains(&ref_table)
                        && !ids.contains(&child)
                    {
                        if !cascade {
                            if enforce_restrict {
                                user_types::truncate_reference_error();
                                return Err(SqlError::Unsupported);
                            }
                            continue;
                        }
                        ids.insert(child);
                    }
                }
            }
            if ids.len() == before {
                break;
            }
        }
        Ok(state
            .tables
            .iter()
            .filter(|(_, table)| ids.contains(&table.id))
            .map(|(name, _)| name.clone())
            .collect())
    }
    pub(crate) fn truncate_restart(&self, state: &mut Relational, names: &[String]) -> R<()> {
        let ids: BTreeSet<_> = names.iter().map(|name| state.tables[name].id).collect();
        // Incarnation replacement is the existing transactional ALTER SEQUENCE
        // RESTART mechanism; old live reservations cannot overwrite the restart.
        for sequence in state.ext.sequences.values_mut() {
            if sequence
                .owned_by
                .is_some_and(|(table, _)| ids.contains(&table))
            {
                let mut bytes = [0; 16];
                getrandom::fill(&mut bytes).map_err(|_| SqlError::Unsupported)?;
                sequence.incarnation = u128::from_le_bytes(bytes);
            }
        }
        Ok(())
    }
    pub(crate) fn truncate_restarts_identity(&self) -> bool {
        matches!(self.statement, Statement::Truncate { restart: true, .. })
    }
}
