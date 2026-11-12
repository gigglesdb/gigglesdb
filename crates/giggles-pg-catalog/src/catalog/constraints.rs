use super::*;
fn key(table: &TableDef, columns: &[u64]) -> R<Vec<Value>> {
    if columns.len() > 64 || columns.iter().collect::<BTreeSet<_>>().len() != columns.len() { return Err(Error::InvalidSnapshot); }
    columns.iter().map(|id| table.columns.iter().position(|c| c.id == *id)
        .map(|i| I(i as i64 + 1)).ok_or(Error::InvalidSnapshot)).collect()
}
fn depend(all: &mut BTreeMap<String, Relation>, class: i64, oid: i64, target: i64, column: Value, kind: &str) {
    all.get_mut("pg_depend").unwrap().rows.push(vec![I(class), I(oid), I(0), I(1259), I(target), column, s(kind)]);
}
pub(super) fn populate(snapshot: &CatalogSnapshot, all: &mut BTreeMap<String, Relation>, used: &mut BTreeSet<i64>, next: &mut i64) -> R<()> {
    if snapshot.constraints.len() > 8192 { return Err(Error::Limit); }
    let bytes = snapshot.column_defaults.values().map(String::len).chain(snapshot.constraints.iter().map(|c| c.name.len() + c.expression.as_ref().map_or(0, String::len)))
        .try_fold(0usize, |n, v| n.checked_add(v).ok_or(Error::Limit))?;
    if bytes > 65536 { return Err(Error::Limit); }
    for c in &snapshot.constraints {
        let table = snapshot.tables.iter().find(|t| t.id == c.table).ok_or(Error::InvalidSnapshot)?;
        let oid = i64::from(table_oid(c.id)?);
        let rel = i64::from(table_oid(c.table)?);
        if c.name.is_empty() || c.name.len() > 63 || !matches!(c.kind, 'c' | 'f')
            || c.expression.as_ref().is_some_and(|e| e.len() > 65536)
            || c.deferred && !c.deferrable || c.columns.len() > 64 { return Err(Error::InvalidSnapshot); }
        let keys = key(table, &c.columns)?;
        let (foreign, foreign_keys, index) = if c.kind == 'f' {
            let parent = snapshot.tables.iter().find(|t| t.id == c.foreign_table).ok_or(Error::InvalidSnapshot)?;
            let pk: Vec<_> = parent.primary_key.iter().map(|i| parent.columns[*i].id).collect();
            let same = |ids: &[u64]| ids.len() == c.foreign_columns.len() && ids.iter().all(|i| c.foreign_columns.contains(i));
            let index = if !pk.is_empty() && same(&pk) { table_oid(parent.id)? + 1 } else {
                table_oid(snapshot.indexes.iter().find(|i| i.table == parent.id && i.unique && same(&i.columns))
                    .ok_or(Error::InvalidSnapshot)?.id)?
            };
            if keys.is_empty() || keys.len() != c.foreign_columns.len()
                || !matches!(c.update, 'a'|'r'|'c'|'n'|'d') || !matches!(c.delete, 'a'|'r'|'c'|'n'|'d') {
                return Err(Error::InvalidSnapshot);
            }
            (i64::from(table_oid(parent.id)?), Value::Array(key(parent, &c.foreign_columns)?), i64::from(index))
        } else {
            if c.expression.is_none() { return Err(Error::InvalidSnapshot); }
            (0, N, 0)
        };
        all.get_mut("pg_constraint").unwrap().rows.push(vec![I(oid), s(&c.name), I(i64::from(snapshot.table_schema(c.table).1)),
            s(&c.kind.to_string()), I(rel), I(index), if keys.is_empty() { N } else { Value::Array(keys.clone()) }, I(foreign), foreign_keys.clone(),
            B(c.deferrable), B(c.deferred), B(c.validated), N, I(0),
            s(if c.kind == 'f' { match c.update {'a'=>"a",'r'=>"r",'c'=>"c",'n'=>"n",_=>"d"} } else { " " }),
            s(if c.kind == 'f' { match c.delete {'a'=>"a",'r'=>"r",'c'=>"c",'n'=>"n",_=>"d"} } else { " " }),
            s(if c.kind == 'f' { "s" } else { " " }), B(c.kind == 'f'), c.expression.as_ref().map_or(N, |e| s(e)), N]);
        if keys.is_empty() { depend(all, 2606, oid, rel, I(0), "a"); }
        for k in keys { depend(all, 2606, oid, rel, k.clone(), "a");
            if c.kind == 'c' { depend(all, 2606, oid, rel, k, "n"); }
        }
        if let Value::Array(keys) = foreign_keys {
            for k in keys { depend(all, 2606, oid, foreign, k, "n"); }
            depend(all, 2606, oid, index, I(0), "n");
        }
    }
    for table in &snapshot.tables {
        let rel = i64::from(table_oid(table.id)?);
        for (i, col) in table.columns.iter().enumerate() {
            let identity = snapshot.column_identities.get(&col.id);
            let default = snapshot.column_defaults.get(&col.id).filter(|_| identity.is_none());
            if default.is_some_and(|e| e.len() > 65536) || identity.is_some_and(|c| !matches!(c, 'a' | 'd')) { return Err(Error::InvalidSnapshot); }
            let row = all.get_mut("pg_attribute").unwrap().rows.iter_mut().find(|row| row[0] == I(rel) && row[4] == I(i as i64 + 1)).unwrap();
            row[12] = B(default.is_some()); row[14] = s(&identity.map_or(String::new(), char::to_string));
            if let Some(expr) = default {
                while !used.insert(*next) { *next -= 1; }
                all.get_mut("pg_attrdef").unwrap().rows.push(vec![I(*next), I(rel), I(i as i64 + 1), s(expr)]);
                depend(all, 2604, *next, rel, I(i as i64 + 1), "a"); *next -= 1;
            }
        }
    }
    for index in snapshot.indexes.iter().filter(|i| snapshot.tables.iter().any(|t| t.id == i.table)) {
        for k in key(snapshot.tables.iter().find(|t| t.id == index.table).ok_or(Error::InvalidSnapshot)?, &index.columns)? {
            depend(all, 1259, i64::from(table_oid(index.id)?), i64::from(table_oid(index.table)?), k, "a");
        }
    }
    for row in &mut all.get_mut("pg_class").unwrap().rows {
        row[16] = I(snapshot.constraints.iter().filter(|c| c.kind == 'c' && table_oid(c.table).is_ok_and(|id| row[0] == I(i64::from(id)))).count() as i64);
    }
    let rows = all["pg_constraint"].rows.clone();
    for row in rows.into_iter().filter(|r| r[3] == s("p") || r[3] == s("n")) {
        if let (I(oid), I(rel), Value::Array(keys)) = (&row[0], &row[4], &row[6]) {
            for k in keys { depend(all, 2606, *oid, *rel, k.clone(), "a"); }
        }
    }
    Ok(())
}
pub(crate) fn definition(snapshot: &CatalogSnapshot, oid: i64, pretty: bool) -> Option<String> {
    let c = snapshot.constraints.iter().find(|c| table_oid(c.id).is_ok_and(|id| i64::from(id) == oid))?;
    let names = |table: u64, ids: &[u64]| -> Option<String> {
        let table = snapshot.tables.iter().find(|t| t.id == table)?;
        ids.iter().map(|id| table.columns.iter().find(|c| c.id == *id).map(|c| quote_identifier(&c.name)))
            .collect::<Option<Vec<_>>>().map(|v| v.join(", "))
    };
    let mut result = if c.kind == 'c' {
        let expr = c.expression.as_ref()?;
        let expr = if pretty { expr.strip_prefix('(').and_then(|e| e.strip_suffix(')')).unwrap_or(expr) } else { expr };
        format!("CHECK ({expr})")
    } else {
        let parent = snapshot.tables.iter().find(|t| t.id == c.foreign_table)?;
        let schema = snapshot.table_schema(parent.id).0;
        let visible = snapshot.search_path.iter().find_map(|schema| snapshot.tables.iter()
            .find(|t| t.name == parent.name && snapshot.table_schema(t.id).0 == schema))
            .is_some_and(|t| t.id == parent.id);
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
