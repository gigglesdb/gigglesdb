//! Read access to untrusted-text column labels (GD-0061 R3-307).
use super::{Admin, Backend, CoreError, engine};

impl Admin {
    /// Columns that carry the `untrusted_text` security label, as
    /// `(table name, column name)` pairs from one committed catalog snapshot,
    /// sorted. Table names are schema-qualified with `.` outside `public`.
    /// Legacy databases have no labels.
    pub fn untrusted_text_columns(&self) -> Result<Vec<(String, String)>, CoreError> {
        let Backend::V2(db) = &self.inner.backend else {
            return Ok(Vec::new());
        };
        let _ticket = self.inner.enter()?;
        let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
        let catalog = db.catalog().snapshot(&mut txn)?;
        let parts = &catalog.parts;
        let mut out = Vec::new();
        for (key, table) in &parts.tables {
            let (schema, bare) = crate::relational::split_key(key);
            let name = if schema == "public" { bare.to_owned() } else { format!("{schema}.{bare}") };
            let marks = crate::task::columns::ColumnMarks::from_labels(table, &parts.column_labels);
            for column in &table.columns {
                if marks.is_untrusted(&column.name) {
                    out.push((name.clone(), column.name.clone()));
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

impl super::SqlSession {
    /// Column labels of this session's catalog snapshot (its open transaction,
    /// else the committed catalog), keyed by (table id, column id). Legacy
    /// databases have none. GD-0061 R3-304.
    pub(crate) fn task_column_labels(
        &self,
    ) -> Result<std::collections::BTreeMap<(u64, u64), u8>, CoreError> {
        match &self.admin.inner.backend {
            Backend::Legacy(_) => Ok(std::collections::BTreeMap::new()),
            Backend::V2(db) => {
                let _ticket = self.admin.inner.enter()?;
                db.column_labels(self.v2_transaction.as_ref())
            }
        }
    }
}
