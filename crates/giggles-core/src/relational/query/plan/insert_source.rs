//! Bind a planned SELECT source to INSERT assignment types.
use super::*;

impl Plan {
    pub(in crate::relational) fn bind_insert_outputs(
        &self,
        state: &Relational,
        targets: &[&ColumnDef],
        parameters: &mut [Option<DataType>],
    ) -> R<Vec<Option<assignment::SourceType>>> {
        let mut meta = self.clone().resolve(state, parameters)?;
        if let Body::Select(select) = &self.body
            && select.query.items.len() == targets.len()
        {
            let mut changed = false;
            for ((item, _), target) in select.query.items.iter().zip(targets) {
                if let Item::Scalar(scalar @ Scalar::Param(n)) = item
                    && parameters.get(n - 1).is_some_and(Option::is_none)
                {
                    let definition = state.column_type(target.id)
                        .filter(|ty| matches!(ty, TypeDef::Enum { .. }));
                    scalar.infer_enum_parameter(&target.data_type, definition, parameters)?;
                    parameters[n - 1].get_or_insert_with(|| target.data_type.clone());
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
                if unknown.is_some() {
                    return Ok(None);
                }
                let source = assignment::SourceType::new(&column.data_type, meta.type_oids.get(i));
                let source_enum = meta.collations.get(i).and_then(Option::as_ref)
                    .and_then(BoundScalar::attached_enum);
                source.check_column(state, target, source_enum)?;
                Ok(Some(source))
            })
            .collect()
    }
}
