use super::*;
impl Query {
    pub(super) fn allow_duplicate_outputs(&mut self) {
        if !matches!(self.filter, Some(Filter::Clauses(_))) {
            self.filter = Some(Filter::Clauses(Box::new(QueryClauses {
                filter: self.filter.take(),
                ..Default::default()
            })));
        }
        if let Some(Filter::Clauses(c)) = &mut self.filter {
            c.allow_duplicate_outputs = true;
        }
    }
    pub(in crate::relational) fn mutation_input(table: &str, keys: BTreeSet<Vec<u8>>) -> Self {
        let mut query = Self::materialized_returning(table, &[]);
        query.filter = Some(Filter::Clauses(Box::new(QueryClauses {
            mutation_keys: Some(std::sync::Arc::new(keys)),
            ..Default::default()
        })));
        query
    }
    pub(super) fn mutation_input_contains(&self, key: &[u8]) -> bool {
        if let Some(Filter::Clauses(c)) = &self.filter {
            c.mutation_keys
                .as_ref()
                .is_none_or(|keys| keys.contains(key))
        } else {
            true
        }
    }
    pub(in crate::relational) fn mutation_input_keys(&self) -> Option<BTreeSet<Vec<u8>>> {
        if let Some(Filter::Clauses(c)) = &self.filter {
            c.mutation_keys.as_deref().cloned()
        } else {
            None
        }
    }
    pub(in crate::relational) fn materialized_returning(
        table: &str,
        columns: &[ColumnDef],
    ) -> Self {
        let mut query = Self {
            explain: false,
            sources: vec![Source {
                name: table.into(),
                alias: split_key(table).1.into(),
            }],
            joins: vec![],
            items: columns
                .iter()
                .map(|c| {
                    (
                        Item::Scalar(Scalar::Cast(
                            Box::new(Scalar::Literal(Value::Null)),
                            c.data_type.clone(),
                        )),
                        Some(c.name.clone()),
                    )
                })
                .collect(),
            predicates: vec![],
            filter: None,
            order: vec![],
            limit: None,
        };
        query.allow_duplicate_outputs();
        query
    }
    pub(in crate::relational) fn rewrite_returning_images(
        &mut self,
        aliases: &[(String, &str); 2],
    ) -> R<bool> {
        let mut images = false;
        for (item, alias) in &mut self.items {
            if let Item::Scalar(Scalar::Column(c)) = item
                && alias.is_none()
                    && aliases.iter().any(|(a, _)| {
                        c.qualifier.as_ref() == Some(a) || c.qualifier.is_none() && &c.name == a
                    })
                {
                    *alias = Some(c.name.clone());
            }
        }
        plan::subquery::map_query(self, &mut |s| {
            if let Scalar::Column(c) = s {
                if let Some(q) = &c.qualifier {
                    if let Some((_, image)) = aliases.iter().find(|(alias, _)| alias == q) {
                        c.name = format!("\0returning_{image}_{}", c.name);
                        c.qualifier = None;
                        images = true;
                    }
                } else if let Some((_, image)) = aliases.iter().find(|(alias, _)| alias == &c.name)
                {
                    c.name = format!("\0returning_{image}");
                    images = true;
                }
            }
            Ok(())
        })?;
        Ok(images)
    }

    pub(in crate::relational) fn image_whole_columns(&self, state: &Relational) -> R<Vec<bool>> {
        Ok(self.image_projection(true,state)?.items.iter().map(|(item,_)| matches!(item, Item::Scalar(Scalar::Column(c)) if c.name=="\0returning_old" || c.name=="\0returning_new")).collect())
    }

    pub(in crate::relational) fn image_projection(
        &self,
        images: bool,
        state: &Relational,
    ) -> R<query::Query> {
        let mut query = self.clone();
        if images {
            let table = state
                .tables
                .get(query.target())
                .ok_or(SqlError::UnknownTable)?;
            let mut items = vec![];
            for (item, alias) in &query.items {
                if matches!(item, Item::Star | Item::QualifiedStar { .. }) {
                    for c in table.logical_columns() {
                        items.push((
                            Item::Scalar(Scalar::Column(ColumnRef {
                                qualifier: None,
                                name: c.name.clone(),
                            })),
                            None,
                        ));
                    }
                } else {
                    let alias = alias.clone().or_else(|| match item {
                        Item::Scalar(Scalar::Column(c)) => c
                            .name
                            .strip_prefix("\0returning_old_")
                            .or_else(|| c.name.strip_prefix("\0returning_new_"))
                            .map(str::to_owned)
                            .or_else(|| c.name.strip_prefix("\0returning_").map(str::to_owned)),
                        _ => None,
                    });
                    items.push((item.clone(), alias));
                }
            }
            query.items = items;
            query.allow_duplicate_outputs();
        }
        Ok(query)
    }
}

pub(in crate::relational) fn record(values: &[Value]) -> R<String> {
    values
        .iter()
        .map(|v| {
            if *v == Value::Null {
                return Ok(String::new());
            }
            if let Value::Bool(v) = v {
                return Ok(if *v { "t" } else { "f" }.into());
            }
            let text = match functions::coerce_clocked(v.clone(), &DataType::Text, None)? {
                Value::Text(s) => s,
                _ => return Err(SqlError::Type),
            };
            Ok(
                if text.is_empty()
                    || text
                        .chars()
                        .any(|c| matches!(c, '(' | ')' | ',' | '"' | '\\') || c.is_whitespace())
                {
                    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\"\""))
                } else {
                    text
                },
            )
        })
        .collect::<R<Vec<_>>>()
        .map(|parts| format!("({})", parts.join(",")))
}
