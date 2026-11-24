use super::*;
fn key(lookup: &Lookup<'_>, table: &TableDef, columns: &[u64]) -> R<Vec<Value>> {
    if columns.len() > 64 || columns.iter().collect::<BTreeSet<_>>().len() != columns.len() { return Err(Error::InvalidSnapshot); }
    columns.iter().map(|id| lookup.columns.get(&(table.id, *id))
        .map(|i| I(*i as i64 + 1)).ok_or(Error::InvalidSnapshot)).collect()
}
fn depend(budget: &mut Budget, all: &mut BTreeMap<String, Relation>, class: i64, oid: i64, target: i64, column: Value, kind: &str) -> R<()> {
    if let Some(relation) = all.get_mut("pg_depend") {
        budget.add(&mut relation.rows, vec![I(class), I(oid), I(0), I(1259), I(target), column, s(kind)])?;
    }
    Ok(())
}
pub(super) fn populate(snapshot: &CatalogSnapshot, lookup: &Lookup<'_>, budget: &mut Budget, all: &mut BTreeMap<String, Relation>, used: &mut BTreeSet<i64>, next: &mut i64) -> R<()> {
    if snapshot.constraints.len() > 8192 { return Err(Error::Limit); }
    let bytes = snapshot.column_defaults.values().map(String::len)
        .chain(snapshot.generated_columns.values().map(|column| column.expression.len()))
        .chain(snapshot.constraints.iter().map(|c| c.name.len() + c.expression.as_ref().map_or(0, String::len)))
        .try_fold(0usize, |n, v| n.checked_add(v).ok_or(Error::Limit))?;
    if bytes > MAX_CATALOG_BYTES { return Err(Error::Limit); }
    for c in &snapshot.constraints {
        let table = lookup.tables.get(&c.table).ok_or(Error::InvalidSnapshot)?;
        let oid = i64::from(table_oid(c.id)?);
        let rel = i64::from(table_oid(c.table)?);
        if c.name.is_empty() || c.name.len() > 63 || !matches!(c.kind, 'c' | 'f' | 'u')
            || c.expression.as_ref().is_some_and(|e| e.len() > 65536)
            || c.deferred && !c.deferrable || c.columns.len() > 64 { return Err(Error::InvalidSnapshot); }
        let keys = key(lookup, table, &c.columns)?;
        let (foreign, foreign_keys, index) = if c.kind == 'f' {
            let parent = lookup.tables.get(&c.foreign_table).ok_or(Error::InvalidSnapshot)?;
            let same = |ids: &[u64]| ids.len() == c.foreign_columns.len() && ids.iter().all(|i| c.foreign_columns.contains(i));
            let index = match c.foreign_key_target {
                None => 0,
                Some(ForeignKeyTarget::PrimaryKey) => {
                    let pk = parent.primary_key.iter().map(|i| parent.columns.get(*i)
                        .map(|c| c.id).ok_or(Error::InvalidSnapshot)).collect::<R<Vec<_>>>()?;
                    if pk.is_empty() || !same(&pk) { return Err(Error::InvalidSnapshot); }
                    table_oid(parent.id)? + 1
                }
                Some(ForeignKeyTarget::Index(id)) => {
                    let index = lookup.indexes.get(&id).filter(|i| i.table == parent.id
                        && i.unique && same(&i.columns)).ok_or(Error::InvalidSnapshot)?;
                    table_oid(index.id)?
                }
            };
            if keys.is_empty() || keys.len() != c.foreign_columns.len()
                || !matches!(c.update, 'a'|'r'|'c'|'n'|'d') || !matches!(c.delete, 'a'|'r'|'c'|'n'|'d') {
                return Err(Error::InvalidSnapshot);
            }
            (i64::from(table_oid(parent.id)?), Value::Array(key(lookup, parent, &c.foreign_columns)?), i64::from(index))
        } else if c.kind == 'u' {
            let index = lookup.indexes.get(&c.index).filter(|i| i.table == c.table
                && i.unique && i.columns == c.columns).ok_or(Error::InvalidSnapshot)?;
            if keys.is_empty() || c.deferrable || c.deferred || !c.validated { return Err(Error::InvalidSnapshot); }
            (0, N, i64::from(table_oid(index.id)?))
        } else {
            if c.expression.is_none() { return Err(Error::InvalidSnapshot); }
            (0, N, 0)
        };
        if let Some(relation) = all.get_mut("pg_constraint") {
            budget.add(&mut relation.rows, vec![I(oid), s(&c.name), I(i64::from(snapshot.table_schema(c.table).1)),
            s(&c.kind.to_string()), I(rel), I(index), if keys.is_empty() { N } else { Value::Array(keys.clone()) }, I(foreign), foreign_keys.clone(),
            B(c.deferrable), B(c.deferred), B(c.validated), N, I(0),
            s(if c.kind == 'f' { match c.update {'a'=>"a",'r'=>"r",'c'=>"c",'n'=>"n",_=>"d"} } else { " " }),
            s(if c.kind == 'f' { match c.delete {'a'=>"a",'r'=>"r",'c'=>"c",'n'=>"n",_=>"d"} } else { " " }),
            s(if c.kind == 'f' { "s" } else { " " }), B(matches!(c.kind, 'f' | 'u')), c.expression.as_ref().map_or(N, |e| s(e)), N])?;
        }
        if c.kind == 'u' && let Some(relation) = all.get_mut("pg_depend") {
            budget.add(&mut relation.rows, vec![I(1259), I(index), I(0), I(2606), I(oid), I(0), s("i")])?;
        }
        if keys.is_empty() { depend(budget, all, 2606, oid, rel, I(0), "a")?; }
        for k in keys { depend(budget, all, 2606, oid, rel, k.clone(), "a")?;
            if c.kind == 'c' { depend(budget, all, 2606, oid, rel, k, "n")?; }
        }
        if let Value::Array(keys) = foreign_keys {
            for k in keys { depend(budget, all, 2606, oid, foreign, k, "n")?; }
            if index != 0 { depend(budget, all, 2606, oid, index, I(0), "n")?; }
        }
    }
    for table in &snapshot.tables {
        let rel = i64::from(table_oid(table.id)?);
        for (i, col) in table.columns.iter().enumerate() {
            let identity = snapshot.column_identities.get(&col.id);
            let generated = snapshot.generated_columns.get(&col.id);
            let default = generated.map(|column| &column.expression)
                .or_else(|| snapshot.column_defaults.get(&col.id).filter(|_| identity.is_none()));
            if default.is_some_and(|e| e.len() > 65536) || identity.is_some_and(|c| !matches!(c, 'a' | 'd')) { return Err(Error::InvalidSnapshot); }
            if let Some(expr) = default && (all.contains_key("pg_attrdef") || all.contains_key("pg_depend")) {
                while !used.insert(*next) {
                    budget.visit(1)?;
                    *next -= 1;
                }
                if let Some(relation) = all.get_mut("pg_attrdef") {
                    budget.add(&mut relation.rows, vec![I(*next), I(rel), I(i as i64 + 1), s(expr)])?;
                }
                depend(budget, all, 2604, *next, rel, I(i as i64 + 1), if generated.is_some() { "i" } else { "a" })?;
                if let Some(generated) = generated {
                    for reference in key(lookup, table, &generated.references)? {
                        depend(budget, all, 2604, *next, rel, reference, "n")?;
                    }
                }
                *next -= 1;
            }
        }
    }
    if all.contains_key("pg_depend") {
        for index in snapshot.indexes.iter().filter(|i| lookup.tables.contains_key(&i.table)
            && !lookup.constraint_indexes.contains(&i.id)) {
            for k in key(lookup, lookup.tables[&index.table], &index.columns)? {
                depend(budget, all, 1259, i64::from(table_oid(index.id)?), i64::from(table_oid(index.table)?), k, "a")?;
            }
        }
    }
    for row in rows_mut(all, "pg_class") {
        budget.rewrite(row, |row| {
            row[16] = I(lookup.table_checks.get(&row_oid(&row[0])).copied().unwrap_or(0));
        })?;
    }
    if !all.contains_key("pg_depend") {
        return Ok(());
    }
    let mut dependencies = Vec::new();
    for row in &all["pg_constraint"].rows {
        budget.visit(1)?;
        if matches!(&row[3], Value::Text(kind) if kind == "p" || kind == "n")
            && let (I(oid), I(rel), Value::Array(keys)) = (&row[0], &row[4], &row[6])
        {
            for key in keys {
                budget.add(&mut dependencies, vec![I(2606), I(*oid), I(0), I(1259), I(*rel), key.clone(), s("a")])?;
            }
        }
    }
    all.get_mut("pg_depend").unwrap().rows.extend(dependencies);
    Ok(())
}
pub(crate) struct Definitions<'a> {
    constraints: HashMap<i64, &'a ConstraintDef>,
    tables: HashMap<u64, &'a TableDef>,
    names: HashMap<(&'a str, &'a str), u64>,
}

impl<'a> Definitions<'a> {
    pub(crate) fn new(snapshot: &'a CatalogSnapshot) -> Self {
        let mut definitions = Self {
            constraints: HashMap::new(),
            tables: HashMap::new(),
            names: HashMap::new(),
        };
        for c in &snapshot.constraints {
            if let Ok(oid) = table_oid(c.id) {
                definitions.constraints.entry(i64::from(oid)).or_insert(c);
            }
        }
        for table in &snapshot.tables {
            definitions.tables.entry(table.id).or_insert(table);
            definitions.names.entry((snapshot.table_schema(table.id).0, &table.name))
                .or_insert(table.id);
        }
        definitions
    }

    pub(crate) fn definition(&self, snapshot: &CatalogSnapshot, oid: i64, pretty: bool) -> Option<String> {
        let c = self.constraints.get(&oid)?;
        let names = |table: u64, ids: &[u64]| -> Option<String> {
            let table = self.tables.get(&table)?;
            ids.iter().map(|id| table.columns.iter().find(|c| c.id == *id).map(|c| quote_identifier(&c.name)))
                .collect::<Option<Vec<_>>>().map(|v| v.join(", "))
        };
        let mut result = if c.kind == 'c' {
            let expr = c.expression.as_ref()?;
            let expr = if pretty { expr.strip_prefix('(').and_then(|e| e.strip_suffix(')')).unwrap_or(expr) } else { expr };
            format!("CHECK ({expr})")
        } else if c.kind == 'u' {
            format!("UNIQUE ({})", names(c.table, &c.columns)?)
        } else {
            let parent = self.tables.get(&c.foreign_table)?;
            let schema = snapshot.table_schema(parent.id).0;
            let visible = snapshot.search_path.iter()
                .find_map(|schema| self.names.get(&(schema.as_str(), parent.name.as_str())))
                .is_some_and(|&id| id == parent.id);
            let parent_name = if visible { quote_identifier(&parent.name) }
                else { format!("{}.{}", quote_identifier(schema), quote_identifier(&parent.name)) };
            let mut result = format!("FOREIGN KEY ({}) REFERENCES {}({})", names(c.table, &c.columns)?, parent_name, names(parent.id, &c.foreign_columns)?);
            for (clause, code) in [("UPDATE", c.update), ("DELETE", c.delete)] {
                let action = match code { 'r'=>"RESTRICT", 'c'=>"CASCADE", 'n'=>"SET NULL", 'd'=>"SET DEFAULT", _=>continue };
                result.push_str(&format!(" ON {clause} {action}"));
            }
            if c.deferrable { result.push_str(" DEFERRABLE"); }
            if c.deferred { result.push_str(" INITIALLY DEFERRED"); }
            result
        };
        if !c.validated { result.push_str(" NOT VALID"); }
        Some(result)
    }
}
