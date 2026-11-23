//! Bind a planned SELECT source to INSERT assignment types.
use super::*;

impl Plan {
    pub(in crate::relational) fn bind_insert_outputs(
        &self,
        state: &Relational,
        targets: &[DataType],
        parameters: &mut [Option<DataType>],
    ) -> R<Vec<Option<assignment::SourceType>>> {
        let mut meta = self.clone().resolve(state, parameters)?;
        if let Body::Select(select) = &self.body
            && select.query.items.len() == targets.len()
        {
            let mut changed = false;
            for ((item, _), target) in select.query.items.iter().zip(targets) {
                if let Item::Scalar(Scalar::Param(n)) = item
                    && let Some(slot @ None) = parameters.get_mut(n - 1)
                {
                    *slot = Some(target.clone());
                    changed = true;
                }
            }
            if changed {
                meta = self.clone().resolve(state, parameters)?;
            }
        }
        if meta.columns.len() != targets.len() {
            return Err(SqlError::Syntax);
        }
        meta.columns
            .into_iter()
            .zip(meta.unknown)
            .zip(targets)
            .enumerate()
            .map(|(i, ((column, unknown), target))| {
                if unknown {
                    return Ok(None);
                }
                let source = assignment::SourceType::new(&column.data_type, meta.type_oids.get(i));
                if source.compatible(target) {
                    Ok(Some(source))
                } else {
                    Err(SqlError::Type)
                }
            })
            .collect()
    }
}
