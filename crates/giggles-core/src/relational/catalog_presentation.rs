//! Read-only presentation of existing catalog extensions; no wire format changes.
use super::*;
/// Persisted referenced key; index IDs identify secondary index descriptors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForeignKeyTarget { PrimaryKey, Index(u64) }
#[derive(Clone, Debug, Default)]
pub struct CatalogConstraint {
    pub id: u64, pub table: u64, pub name: String, pub kind: char,
    pub columns: Vec<u64>, pub foreign_table: u64, pub foreign_columns: Vec<u64>,
    /// None denotes an unknown legacy binding.
    pub foreign_key_target: Option<ForeignKeyTarget>,
    pub update: char, pub delete: char, pub deferrable: bool, pub deferred: bool,
    pub validated: bool, pub expression: Option<String>, pub index: u64,
}
#[derive(Clone, Debug)]
pub struct CatalogView {
    pub table: TableDef,
    pub definition: String,
    pub type_oids: Vec<u32>,
    pub typmods: Vec<i32>,
}
#[derive(Clone, Debug, Default)]
pub struct CatalogPresentation {
    pub epoch: u64,
    pub routines: BTreeMap<u64, (String, Routine)>,
    pub triggers: Vec<Trigger>,
    pub views: BTreeMap<u64, CatalogView>,
    /// Stable (table ID, column ID), with zero identifying the table itself.
    pub comments: BTreeMap<(u64, u64), String>,
    pub defaults: BTreeMap<u64, String>,
    pub identities: BTreeMap<u64, char>,
    pub gin_indexes: BTreeMap<u64, gin_indexes::GinIndex>,
    pub table_storage: BTreeMap<u64, TableStorage>,
    pub generated_columns: BTreeMap<u64, generated_columns::GeneratedColumn>,
    pub primary_names: BTreeMap<u64, String>,
    pub constraints: Vec<CatalogConstraint>,
}
impl Relational {
    #[cfg(test)]
    pub(crate) fn catalog_presentation(&self) -> R<CatalogPresentation> {
        self.catalog_presentation_in_path(&["public".into()])
    }
    pub(crate) fn catalog_presentation_in_path(&self, path: &[String]) -> R<CatalogPresentation> {
        let mut out = CatalogPresentation {
            epoch: self.epoch,
            routines: self.ext.routines.iter().map(|(&id, routine)| {
                let schema = self.routine_schema(routine.schema).ok_or(SqlError::InvalidSchema)?;
                Ok((id, (schema.to_owned(), routine.clone())))
            }).collect::<R<_>>()?,
            triggers: self.ext.triggers.values().flat_map(BTreeMap::values).cloned().collect(),
            ..Default::default()
        };
        out.gin_indexes.clone_from(&self.ext.gin_indexes);
        out.table_storage.clone_from(&self.ext.table_storage);
        out.generated_columns.clone_from(&self.ext.generated_columns);
        let action = |a: &Action| match a {
            Action::NoAction => 'a', Action::Restrict => 'r', Action::Cascade => 'c',
            Action::SetNull => 'n', Action::SetDefault => 'd',
        };
        for table in self.tables.values() {
            if let Some(name) = self.primary_key_constraint_name(table) {
                out.primary_names.insert(table.id, name);
            }
            for column in table.logical_columns() {
                if let Some(identity) = self.column_identity(column.id) {
                    out.identities.insert(column.id, if identity.always { 'a' } else { 'd' });
                } else if let Some(sql) = self.column_default(column.id) {
                    let display = enum_alter::present_default(self.column_type(column.id), sql)?;
                    out.defaults.insert(column.id, display.unwrap_or_else(|| defaults::present_default(self, sql, path)));
                }
            }
            for c in self.table_constraints(table.id) {
                let mut info = CatalogConstraint { id: c.id, table: table.id, name: c.name.clone(),
                    validated: true, ..Default::default() };
                match &c.kind {
                    ConstraintKind::PrimaryKey => continue,
                    ConstraintKind::Unique { columns, index } => {
                        info.kind = 'u'; info.columns = columns.clone(); info.index = *index;
                    }
                    ConstraintKind::Check { expr, not_valid } => {
                        info.kind = 'c'; info.validated = !not_valid;
                        let expr = expr.strip_suffix(" NOT VALID").unwrap_or(expr);
                        let expr = expr.strip_prefix("CHECK (").and_then(|s| s.strip_suffix(')'))
                            .ok_or(SqlError::Unsupported)?;
                        let tokens = lex(expr)?;
                        info.columns = table.logical_columns().filter(|col| tokens.iter().enumerate().any(|(i,t)|
                            matches!(t, Token::Word(w) | Token::QuotedIdent(w) if *w == col.name)
                            && tokens.get(i + 1) != Some(&Token::Symbol('('))
                            && tokens.get(i.wrapping_sub(1)) != Some(&Token::Symbol(':'))
                            && !matches!(t, Token::Word(w) if matches!(w.as_str(), "true"|"false"|"null"|"case"|"when"|"then"|"else"|"end"|"and"|"or"|"not"|"is"|"in"|"like"|"escape"))))
                            .map(|col| col.id).collect();
                        info.expression = Some(expr.into());
                    }
                    ConstraintKind::ForeignKey { columns, ref_table, ref_columns, on_delete,
                        on_update, not_valid, deferral, target } => {
                        info.foreign_key_target = match target {
                            ReferencedKey::Unknown => None,
                            ReferencedKey::PrimaryKey => Some(ForeignKeyTarget::PrimaryKey),
                            ReferencedKey::Secondary(id) => Some(ForeignKeyTarget::Index(*id)),
                        };
                        info.kind = 'f'; info.columns = columns.clone();
                        info.foreign_table = *ref_table; info.foreign_columns = ref_columns.clone();
                        info.update = action(on_update); info.delete = action(on_delete);
                        info.validated = !not_valid;
                        info.deferrable = *deferral != Deferral::NotDeferrable;
                        info.deferred = *deferral == Deferral::DeferrableDeferred;
                    }
                }
                out.constraints.push(info);
            }
        }
        Ok(out)
    }
}
