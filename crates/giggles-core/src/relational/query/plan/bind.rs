use super::*;

pub(super) struct Meta {
    pub columns: Vec<ColumnDef>,
    pub typmods: Vec<i32>,
    pub type_oids: Vec<u32>,
    pub unknown: Vec<bool>,
    // Binding-only scalar metadata for set-operation collation diagnostics.
    pub collations: Vec<Option<BoundScalar>>,
}
pub(super) struct BoundPlan {
    pub plan: Plan,
    pub description: Description,
}
fn no_column(_: &ColumnRef, _: usize) -> R<BoundColumn> {
    Err(SqlError::UnknownColumn)
}
pub(super) fn scalar_type(s: &Scalar, parameters: &mut [Option<DataType>]) -> R<DataType> {
    s.infer(None, parameters, &no_column, &[])?;
    Ok(s.bind(&no_column, &[], parameters)?.ty)
}
/// PostgreSQL's left-to-right common-type selection. Unknowns have already
/// been removed; actual conversion and scalar evaluation stay in Scalar.
pub(super) fn common(types: impl Iterator<Item = DataType>) -> R<DataType> {
    let types: Vec<_> = types.collect();
    let Some(mut candidate) = types.first().cloned() else {
        return Ok(DataType::Text);
    };
    for ty in &types[1..] {
        if implicit(&candidate, ty) && !implicit(ty, &candidate) {
            candidate = ty.clone();
        }
    }
    if types.iter().all(|ty| implicit(ty, &candidate)) {
        Ok(candidate)
    } else {
        Err(SqlError::Type)
    }
}
// The built-in implicit pg_cast edges (including array element widening).
// Assignment/explicit casts must never admit unrelated UNION/VALUES types.
fn implicit(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    let numeric = |ty: &DataType| match ty {
        SmallInt => Some(0),
        Integer => Some(1),
        BigInt => Some(2),
        Numeric => Some(3),
        Real => Some(4),
        DoublePrecision => Some(5),
        _ => None,
    };
    from == to
        || matches!((numeric(from),numeric(to)),(Some(a),Some(b)) if a <= b)
        || matches!(
            (from, to),
            (Text | Varchar | Bpchar, Text | Varchar | Bpchar)
                | (Date, Timestamp | TimestampTz)
                | (Timestamp, TimestampTz)
                | (Time, TimeTz)
                | (IntArray, BigIntArray)
        )
}
fn values_cell(
    scalar: &mut Scalar, ty: &DataType, parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let cast = Scalar::Cast(Box::new(scalar.clone()), ty.clone());
    let bound = cast.bind(&no_column, &[], parameters);
    if matches!(bound, Err(SqlError::InvalidTextRepresentation))
        && let Scalar::Literal(Value::Text(text)) = scalar
        && let Some(type_name) = match ty {
            DataType::SmallInt => Some("smallint"),
            DataType::Integer => Some("integer"),
            DataType::BigInt => Some("bigint"),
            _ => None,
        }
    {
        user_types::view_replace_error(format!(r#"invalid input syntax for type {type_name}: "{text}""#));
    }
    *scalar = cast;
    bound
}

fn unknown(s: &Scalar, parameters: &[Option<DataType>]) -> bool {
    matches!(s, Scalar::Literal(Value::Null | Value::Text(_)))
        || matches!(s, Scalar::Param(n) if parameters.get(n - 1).is_some_and(Option::is_none))
}
impl Plan {
    pub(in crate::relational) fn describe(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<Description> {
        Ok(self.bind(state, count, declared)?.description)
    }
    pub(in crate::relational) fn validate_immutable_constants(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<()> {
        let bound = self.bind(state, count, declared)?;
        let parameters: Vec<_> = bound.description.parameters.into_iter().map(Some).collect();
        bound.plan.validate_constants(state, &parameters)
    }
    pub(super) fn validate_constants(
        &self,
        state: &Relational,
        parameters: &[Option<DataType>],
    ) -> R<()> {
        let _metadata = functions::collation::ColumnScope::enter(&[], &[])?;
        if self.mutation.is_some() || self.merge.is_some() || self.insert.is_some() { return Ok(()); }
        if !self.ctes.is_empty() {
            let mut plan = self.clone();
            let mut parameters = parameters.to_vec();
            let catalog = cte::resolve(&mut plan.ctes, state, &mut parameters)?;
            for cte in &plan.ctes {
                cte::validate_constants(cte, &catalog, &parameters)?;
            }
            plan.ctes.clear();
            return plan.validate_constants(&catalog, &parameters);
        }
        match &self.body {
            Body::Select(select) if !select.applies.is_empty() => {
                subquery::resolve(
                    select, self.decorations.grouping_sets.as_ref(), state, &mut parameters.to_vec(),
                )?;
            }
            Body::Select(select) => {
                if let Some(from) = &select.from {
                    from.validate_constants(state, parameters)?;
                    let shape = from.clone().resolve(state, &mut parameters.to_vec())?;
                    let catalog = join::catalog(state, &shape)?;
                    join::prepare(&select.query, &shape)?
                        .bind_projection_seeded(&catalog, parameters.to_vec())?
                        .validate_immutable_constants()?;
                    return Ok(());
                }
                let mut input_collations = None;
                let synthetic = if let Some(input) = &select.input {
                    input.query.validate_constants(state, parameters)?;
                    let mut meta = input
                        .query
                        .clone()
                        .resolve(state, &mut parameters.to_vec())?;
                    window::columns(&select.windows, &mut meta, &mut parameters.to_vec())?;
                    input_collations = Some(meta.collations);
                    Some(input_state(state, input, &meta.columns, &meta.typmods)?)
                } else {
                    None
                };
                let state = synthetic.as_ref().unwrap_or(state);
                let _collations = input_collations
                    .as_ref()
                    .map(|metadata| {
                        functions::collation::ColumnScope::enter(
                            &state.tables[&select.input.as_ref().unwrap().name].columns,
                            metadata,
                        )
                    })
                    .transpose()?;
                let query = window::prepare(select, state)?;
                cte::prepare_query(&query, state)?
                    .bind_projection_seeded(state, parameters.to_vec())?
                    .validate_immutable_constants()?;
            }
            Body::Values(rows) => {
                let mut budget = 0;
                for scalar in rows.iter().flatten() {
                    scalar
                        .bind(&no_column, &[], parameters)?
                        .plan_constant(&mut budget)?;
                }
            }
            Body::Set { left, right, .. } => {
                left.validate_constants(state, parameters)?;
                right.validate_constants(state, parameters)?;
            }
        }
        Ok(())
    }
    pub(super) fn bind(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<BoundPlan> {
        let _metadata = functions::collation::ColumnScope::enter(&[], &[])?;
        if count > MAX_PARAMS || declared.len() > count {
            return Err(SqlError::Parameters);
        }
        let mut parameters = vec![None; count];
        parameters[..declared.len()].clone_from_slice(declared);
        let mut plan = self.clone();
        let meta = plan.resolve(state, &mut parameters)?;
        Ok(BoundPlan {
            plan,
            description: Description {
                columns: meta.columns,
                typmods: meta.typmods,
                type_oids: meta.type_oids,
                parameters: parameters
                    .into_iter()
                    .map(|t| t.unwrap_or(DataType::Text))
                    .collect(),
            },
        })
    }
    pub(super) fn resolve(
        &mut self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<Meta> {
        let catalog = if self.ctes.is_empty() {
            None
        } else {
            Some(cte::resolve(&mut self.ctes, state, parameters)?)
        };
        let state = catalog.as_ref().unwrap_or(state);
        if let Some(m) = &mut self.mutation { return m.resolve(state, parameters); }
        if let Some(m) = &mut self.merge { return m.resolve(state, parameters); }
        if let Some(insert) = &mut self.insert {
            return dml::resolve(&mut insert.prepared, state, parameters, &insert.parameters);
        }
        // Pagination can determine an otherwise unknown projection parameter.
        for s in self.limit.iter().chain(&self.offset) {
            s.infer(Some(&DataType::BigInt), parameters,
                &|_, _| Err(SqlError::InvalidColumnReference), &[])?;
        }
        if let Some(sample) = &self.decorations.sample {
            for s in std::iter::once(&sample.percent).chain(sample.seed.iter()) {
                s.infer(Some(&DataType::DoublePrecision), parameters, &no_column, &[])?;
                Scalar::Cast(Box::new(s.clone()), DataType::DoublePrecision).bind(&no_column, &[], parameters)?;
            }
        }
        let mut meta = match &mut self.body {
            Body::Select(select) if !select.applies.is_empty() => {
                subquery::resolve(select, self.decorations.grouping_sets.as_ref(), state, parameters)?
            }
            Body::Select(select) => {
                let shape = select
                    .from
                    .as_mut()
                    .map(|from| from.resolve(state, parameters))
                    .transpose()?;
                let mut input_collations = None;
                let synthetic = if let Some(shape) = &shape {
                    Some(join::catalog(state, shape)?)
                } else if let Some(input) = &mut select.input {
                    let mut meta = input.query.resolve(state, parameters)?;
                    window::columns(&select.windows, &mut meta, parameters)?;
                    input_collations = Some(meta.collations);
                    Some(input_state(state, input, &meta.columns, &meta.typmods)?)
                } else {
                    None
                };
                let state = synthetic.as_ref().unwrap_or(state);
                let _collations = input_collations
                    .as_ref()
                    .map(|metadata| {
                        functions::collation::ColumnScope::enter(
                            &state.tables[&select.input.as_ref().unwrap().name].columns,
                            metadata,
                        )
                    })
                    .transpose()?;
                let query = if let Some(shape) = &shape {
                    join::prepare(&select.query, shape)?
                } else {
                    let q = window::prepare(select, state)?;
                    cte::prepare_query(&q, state)?
                };
                paging::validate_distinct(&query, self.decorations.distinct_on, &self.order, state)?;
                let mut unknown_parameters: Vec<_> = select
                    .query
                    .items
                    .iter()
                    .filter_map(|(item, _)| {
                        if let Item::Scalar(Scalar::Param(n)) = item {
                            if parameters.get(n - 1).is_some_and(Option::is_none) {
                                Some(*n)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut seeded = parameters.to_vec();
                for (i, slot) in seeded.iter_mut().enumerate() {
                    if !select.parameters.contains(&(i + 1)) && slot.is_none() {
                        *slot = Some(DataType::Text);
                    }
                }
                let bound = match query.bind_projection_seeded(state, seeded.clone()) {
                    Ok(bound) => {
                        unknown_parameters.clear();
                        bound
                    }
                    Err(SqlError::Parameters) if !unknown_parameters.is_empty() => {
                        for n in &unknown_parameters {
                            seeded[*n - 1] = Some(DataType::Text);
                        }
                        query.bind_projection_seeded(state, seeded)?
                    }
                    Err(error) => return Err(error),
                };
                let mut description = bound.description(state);
                group_sets::validate(&query, self.decorations.grouping_sets.as_ref(), state, &description.parameters)?;
                let oids = cte::projection_oids(&bound);
                if oids.iter().any(|oid| *oid != 0) {
                    description.type_oids = oids;
                }
                let unknown = query
                    .insert_outputs(state)?
                    .iter()
                    .map(Option::is_some)
                    .collect();
                for (i, ty) in description.parameters.into_iter().enumerate() {
                    if select.parameters.contains(&(i + 1))
                        && !unknown_parameters.contains(&(i + 1))
                    {
                        parameters[i] = Some(ty);
                    }
                }
                let collations = if let Some(grouping) = bound.grouping {
                    grouping.into_collation_outputs()
                } else {
                    bound
                        .items
                        .into_iter()
                        .zip(&description.columns)
                        .map(|(item, column)| match item {
                            BoundItem::Scalar(value) => Some(value),
                            BoundItem::Aggregate(kind, Some(value)) => kind.result_metadata(value, &column.data_type),
                            _ => None,
                        })
                        .collect()
                };
                let mut meta = Meta {
                    columns: description.columns,
                    typmods: description.typmods,
                    type_oids: description.type_oids,
                    unknown,
                    collations,
                };
                if select.query.items.len() == meta.columns.len() {
                    for (i, (item, _)) in select.query.items.iter().enumerate() {
                        if matches!(item, Item::Scalar(Scalar::Param(n)) if unknown_parameters.contains(n))
                        {
                            meta.unknown[i] = true;
                        }
                    }
                }
                // Synthetic IDs exist only to carry typmods inside the temporary
                // binder catalog. Never expose them to the real catalog's OID lookup.
                if select.input.is_some()
                    || select
                        .query
                        .sources
                        .iter()
                        .any(|s| s.name.starts_with("\0plan_"))
                {
                    for column in &mut meta.columns {
                        column.id = 0;
                    }
                }
                meta
            }
            Body::Values(rows) => {
                let width = rows.first().ok_or(SqlError::Syntax)?.len();
                let mut columns = vec![];
                let mut collations = vec![];
                for i in 0..width {
                    let mut typed = vec![];
                    for row in rows.iter() {
                        if row.len() != width {
                            return Err(SqlError::Syntax);
                        }
                        if !unknown(&row[i], parameters) {
                            row[i].infer(None, parameters, &no_column, &[])?;
                            typed.push(row[i].bind(&no_column, &[], parameters)?);
                        }
                    }
                    let enum_type = functions::enum_order::common_enum(
                        "VALUES", typed.iter().map(BoundScalar::type_identity),
                    )?;
                    let ty = common(typed.iter().map(|value| value.ty.clone()))?;
                    if let Some(enum_type) = enum_type {
                        for row in rows.iter_mut() {
                            if unknown(&row[i], parameters) {
                                retarget_enum(&mut row[i], &ty, enum_type)?;
                                row[i].infer(Some(&ty), parameters, &no_column, &[])?;
                            }
                        }
                        collations.push(Some(BoundScalar::enum_metadata(ty.clone(), enum_type)));
                    } else {
                        let mut metadata = None;
                        for row in rows.iter_mut() {
                            row[i].infer(Some(&ty), parameters, &no_column, &[])?;
                            let value = values_cell(&mut row[i], &ty, parameters)?;
                            metadata = Some(functions::collation::combined(
                                metadata.as_ref(),
                                Some(&value),
                                ty.clone(),
                            )?);
                        }
                        collations.push(metadata);
                    }
                    columns.push(ColumnDef {
                        id: 0,
                        name: format!("column{}", i + 1),
                        data_type: ty,
                        not_null: false,
                    });
                }
                let type_oids: Vec<_> = collations.iter().map(|value| {
                    value.as_ref().map_or(0, |value| value.cast_enum_oid(state))
                }).collect();
                Meta {
                    columns,
                    typmods: vec![-1; width],
                    type_oids: if type_oids.iter().any(|oid| *oid != 0) { type_oids } else { vec![] },
                    unknown: vec![false; width],
                    collations,
                }
            }
            Body::Set {
                op,
                all,
                left,
                right,
            } => {
                let mut a = left.resolve(state, parameters)?;
                let b = right.resolve(state, parameters)?;
                // bpchar consumers remain closed until DD-0068 comparison semantics land.
                if a.columns
                    .iter()
                    .chain(&b.columns)
                    .any(|c| c.data_type == DataType::Bpchar)
                {
                    return Err(SqlError::Unsupported);
                }
                if a.columns.len() != b.columns.len() {
                    return Err(SqlError::Syntax);
                }
                let construct = match op {
                    SetOp::Union => "UNION",
                    SetOp::Intersect => "INTERSECT",
                    SetOp::Except => "EXCEPT",
                };
                let a_oids = std::mem::replace(&mut a.type_oids, vec![0; a.columns.len()]);
                a.collations.resize_with(a.columns.len(), || None);
                for i in 0..a.columns.len() {
                    let a_json = a_oids.get(i) == Some(&json_aggregate::OID);
                    let b_json = b.type_oids.get(i) == Some(&json_aggregate::OID);
                    if a_json || b_json {
                        if a_json != b_json && !(if a_json { b.unknown[i] } else { a.unknown[i] }) {
                            let types = [(&a, a_json), (&b, b_json)].map(|(m, json)| {
                                if json {
                                    "json"
                                } else {
                                    defaults::type_name(&m.columns[i].data_type)
                                }
                            });
                            user_types::view_replace_error(format!("{construct} types {} and {} cannot be matched", types[0], types[1]));
                            return Err(SqlError::Type);
                        }
                        if !matches!(op, SetOp::Union) || !*all {
                            user_types::view_replace_error("could not identify an equality operator for type json".into());
                            return Err(SqlError::UndefinedFunction);
                        }
                    }
                    let typed = [&a, &b].into_iter().filter(|m| !m.unknown[i]);
                    let enum_type = functions::enum_order::common_enum(
                        construct,
                        typed.clone().map(|m| {
                            (&m.columns[i].data_type, m.collations.get(i).and_then(Option::as_ref)
                                .and_then(BoundScalar::attached_enum))
                        }),
                    )?;
                    let ty = common(typed.map(|m| m.columns[i].data_type.clone()))?;
                    left.retarget(i, &ty, enum_type, parameters)?;
                    right.retarget(i, &ty, enum_type, parameters)?;
                    let value = functions::collation::combined(
                        a.collations[i].as_ref().filter(|_| !a.unknown[i] || enum_type.is_none()),
                        b.collations.get(i).and_then(Option::as_ref)
                            .filter(|_| !b.unknown[i] || enum_type.is_none()),
                        ty.clone(),
                    )?;
                    if !matches!(op, SetOp::Union) || !*all {
                        value.require_collation()?;
                    }
                    a.type_oids[i] = if a_json || b_json { json_aggregate::OID } else { value.cast_enum_oid(state) };
                    a.collations[i] = Some(value);
                    let same = a.columns[i].data_type == b.columns[i].data_type
                        && a.typmods[i] == b.typmods[i];
                    a.columns[i].data_type = ty;
                    a.columns[i].id = 0;
                    a.columns[i].not_null = false;
                    if !same {
                        a.typmods[i] = -1;
                    }
                    a.unknown[i] = false;
                }
                if a.type_oids.iter().all(|oid| *oid == 0) {
                    a.type_oids.clear();
                }
                a
            }
        };
        let table = result_table(&meta.columns);
        let _collations =
            functions::collation::ColumnScope::enter(&table.columns, &meta.collations)?;
        for (s, _, _) in &self.order {
            if let Scalar::Literal(Value::Int(n)) = s
                && (*n < 1 || *n as u64 > meta.columns.len().saturating_sub(self.decorations.hidden) as u64)
            { return Err(SqlError::InvalidColumnReference); }
            if self.set_result() && !matches!(s, Scalar::Column(_) | Scalar::Literal(_)) {
                return Err(SqlError::Unsupported);
            }
            let scalar = order_scalar(s, &table)?;
            scalar.infer(None, parameters, &|c, _| order_column(c, &table), &[&table])?;
            if let Scalar::Param(n) = &scalar {
                parameters[*n - 1].get_or_insert(DataType::Text);
            }
            scalar
                .bind(&|c, _| order_column(c, &table), &[&table], parameters)?
                .require_collation()?;
        }
        for s in self.limit.iter().chain(&self.offset) {
            let no_page_column = |_: &ColumnRef, _: usize| Err(SqlError::InvalidColumnReference);
            s.infer(Some(&DataType::BigInt), parameters, &no_page_column, &[])?;
            let ty = s.bind(&no_page_column, &[], parameters)?.ty;
            if !integer_type(&ty)
                && !matches!(ty, DataType::Numeric | DataType::Real | DataType::DoublePrecision)
                && !matches!(s, Scalar::Literal(Value::Null | Value::Text(_)))
            {
                return Err(SqlError::Type);
            }
        }
        paging::trim_meta(&mut meta, self.decorations.hidden);
        Ok(meta)
    }
    pub(super) fn set_result(&self) -> bool {
        match &self.body {
            Body::Set { .. } => true,
            Body::Select(s) => s.input.as_ref().is_some_and(|i| i.query.set_result()),
            _ => false,
        }
    }
    fn retarget(
        &mut self,
        i: usize,
        ty: &DataType,
        enum_type: Option<&TypeDef>,
        parameters: &mut [Option<DataType>],
    ) -> R<()> {
        if let Body::Select(select) = &mut self.body
            && select.query.items.len() > i
            && let (Item::Scalar(s), alias) = &mut select.query.items[i]
            && unknown(s, parameters)
        {
            s.infer(Some(ty), parameters, &no_column, &[])?;
            let name = s.bind(&no_column, &[], parameters)?.name().to_string();
            if alias.is_none() {
                *alias = Some(name);
            }
            if let Some(enum_type) = enum_type {
                retarget_enum(s, ty, enum_type)?;
            } else {
                *s = Scalar::Cast(Box::new(s.clone()), ty.clone());
                s.bind(&no_column, &[], parameters)?;
            }
        }
        Ok(())
    }
}
fn retarget_enum(scalar: &mut Scalar, storage: &DataType, enum_type: &TypeDef) -> R<()> {
    if let Scalar::Literal(value) = scalar {
        BoundScalar::enum_literal(value, storage, enum_type)?;
    }
    let name = user_types::TypeReference {
        name: enum_type.name().to_owned(),
        array: *storage == DataType::TextArray,
    };
    *scalar = Scalar::Domain(
        Box::new(scalar.clone()), name, Box::new(Ok(enum_type.clone())),
    );
    Ok(())
}

pub(super) fn output_column(c: &ColumnRef, table: &TableDef) -> R<BoundColumn> {
    if c.qualifier.is_some() {
        return Err(SqlError::UnknownTable);
    }
    let matches = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.name == c.name)
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [i] => Ok(BoundColumn {
            source: 0,
            column: *i,
        }),
        [] => Err(SqlError::UnknownColumn),
        _ => Err(SqlError::AmbiguousColumn),
    }
}
pub(super) fn order_scalar(s: &Scalar, table: &TableDef) -> R<Scalar> {
    if let Scalar::Literal(Value::Int(n)) = s {
        let i = usize::try_from(*n)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .filter(|i| *i < table.columns.len())
            .ok_or(SqlError::InvalidColumnReference)?;
        return Ok(Scalar::Column(ColumnRef {
            qualifier: None,
            name: format!("\0ordinal_{i}"),
        }));
    }
    if matches!(s, Scalar::Literal(_)) {
        return Err(SqlError::Syntax);
    }
    Ok(s.clone())
}
pub(super) fn order_column(c: &ColumnRef, table: &TableDef) -> R<BoundColumn> {
    if let Some(i) = c
        .name
        .strip_prefix("\0ordinal_")
        .and_then(|s| s.parse::<usize>().ok())
    {
        return Ok(BoundColumn {
            source: 0,
            column: i,
        });
    }
    output_column(c, table)
}
pub(super) fn input_state(
    base: &Relational,
    input: &Input,
    columns: &[ColumnDef],
    typmods: &[i32],
) -> R<Relational> {
    if input.aliases.len() > columns.len() {
        return Err(SqlError::InvalidColumnReference);
    }
    let mut columns = columns.to_vec();
    for (i, column) in columns.iter_mut().enumerate() {
        column.id = i as u64 + 1;
        if let Some(alias) = input.aliases.get(i) {
            column.name = alias.clone();
        }
    }
    let mut state = cte::catalog(base);
    cte::install(&mut state, &input.name, &columns, typmods)?;
    Ok(state)
}

pub(super) fn result_table(columns: &[ColumnDef]) -> TableDef {
    let mut columns = columns.to_vec();
    for (i, c) in columns.iter_mut().enumerate() {
        c.id = i as u64 + 1;
    }
    TableDef {
        id: 0,
        name: "output".into(),
        columns,
        primary_key: vec![],
    }
}
