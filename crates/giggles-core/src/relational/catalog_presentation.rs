//! Read-only presentation of existing catalog extensions; no wire format changes.
use super::*;
#[derive(Clone, Debug, Default)]
pub struct CatalogConstraint {
    pub id: u64, pub table: u64, pub name: String, pub kind: char,
    pub columns: Vec<u64>, pub foreign_table: u64, pub foreign_columns: Vec<u64>,
    pub update: char, pub delete: char, pub deferrable: bool, pub deferred: bool,
    pub validated: bool, pub expression: Option<String>,
}
#[derive(Clone, Debug, Default)]
pub struct CatalogPresentation {
    pub epoch: u64,
    pub defaults: BTreeMap<u64, String>,
    pub identities: BTreeMap<u64, char>,
    pub primary_names: BTreeMap<u64, String>,
    pub constraints: Vec<CatalogConstraint>,
}
impl Relational {
    pub(crate) fn catalog_presentation(&self) -> R<CatalogPresentation> {
        let mut out = CatalogPresentation { epoch: self.epoch, ..Default::default() };
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
                    out.defaults.insert(column.id, sql.to_owned());
                }
            }
            for c in self.table_constraints(table.id) {
                let mut info = CatalogConstraint { id: c.id, table: table.id, name: c.name.clone(),
                    validated: true, ..Default::default() };
                match &c.kind {
                    ConstraintKind::PrimaryKey => continue,
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
                        on_update, not_valid, deferral } => {
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
