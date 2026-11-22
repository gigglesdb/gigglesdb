use super::*;
use std::collections::HashMap;

/// Charge owned catalog rows separately from result formatting and scalar values.
pub(crate) const MAX_CATALOG_BYTES: usize = 128 * 1024 * 1024;

/// Meters catalog construction separately from SQL operator execution.
pub(super) struct Budget {
    pub(super) work: usize,
    pub(super) bytes: usize,
    pub(super) byte_limit: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            work: 0,
            bytes: 0,
            byte_limit: MAX_CATALOG_BYTES,
        }
    }
}

impl Budget {
    pub(super) fn visit(&mut self, units: usize) -> R<()> {
        self.work = self.work.saturating_add(units);
        if self.work > MAX_WORK {
            return Err(Error::Limit);
        }
        Ok(())
    }

    pub(super) fn charge(&mut self, bytes: usize) -> R<()> {
        self.bytes = self.bytes.checked_add(bytes).ok_or(Error::Limit)?;
        if self.bytes > self.byte_limit {
            return Err(Error::Limit);
        }
        Ok(())
    }

    pub(super) fn add(&mut self, rows: &mut Vec<Vec<Value>>, row: Vec<Value>) -> R<()> {
        self.visit(1)?;
        self.charge(row_bytes(&row))?;
        rows.push(row);
        Ok(())
    }

    pub(super) fn rewrite(
        &mut self,
        row: &mut Vec<Value>,
        rewrite: impl FnOnce(&mut Vec<Value>),
    ) -> R<()> {
        self.visit(1)?;
        self.bytes -= row_bytes(row);
        rewrite(row);
        self.charge(row_bytes(row))
    }
}

pub(crate) fn row_bytes(row: &[Value]) -> usize {
    // Include row storage and the owned payload of variable-width values.
    fn payload(value: &Value) -> usize {
        match value {
            Value::Text(s) => s.capacity(),
            Value::Bytes(b) => b.capacity(),
            Value::Array(v) | Value::Record(v) => {
                v.capacity() * size_of::<Value>() + v.iter().map(payload).sum::<usize>()
            }
            // Numeric's serialized digits provide a conservative payload estimate.
            Value::Numeric(_) => value.bytes(),
            _ => 0,
        }
    }
    size_of::<Vec<Value>>() + size_of_val(row) + row.iter().map(payload).sum::<usize>()
}

/// The synthetic constraint and default OIDs descend from u32::MAX. Reserve
/// the snapshot-supplied OIDs published by the full catalog, even when their
/// relations aren't requested. Fixed bootstrap OIDs are all below 65536;
/// explicit constraints have OIDs at or above 65536 and the work allowance
/// prevents synthetic allocation from reaching the bootstrap range.
pub(super) fn occupied_oids(snapshot: &CatalogSnapshot, lookup: &Lookup<'_>, budget: &mut Budget) -> R<BTreeSet<i64>> {
    let mut used = BTreeSet::new();
    for table in &snapshot.tables {
        budget.visit(1)?;
        let oid = i64::from(table_oid(table.id)?);
        used.extend([oid, oid + 3]);
        if !table.primary_key.is_empty() {
            used.extend([oid + 1, oid + 2]);
        }
    }
    for index in snapshot.indexes.iter().filter(|index| lookup.tables.contains_key(&index.table)) {
        budget.visit(1)?;
        used.insert(i64::from(table_oid(index.id)?));
    }
    for ty in &snapshot.user_types {
        budget.visit(1 + ty.labels.len())?;
        used.extend((0..=ty.labels.len()).map(|n| i64::from(ty.oid) + 4 * n as i64));
    }
    budget.visit(snapshot.schemas.len() + snapshot.roles.len()
        + snapshot.extensions.len() + snapshot.collations.len())?;
    used.extend(snapshot.schemas.values().map(|&(oid, _)| i64::from(oid)));
    used.extend(snapshot.roles.iter().map(|role| i64::from(role.oid)));
    used.extend(snapshot.extensions.iter().filter_map(|ext| {
        ext.installed.as_ref().map(|&(oid, _, _, _)| i64::from(oid))
    }));
    used.extend(snapshot.collations.iter().filter_map(|row| match row[0] {
        I(oid) => Some(oid),
        _ => None,
    }));
    Ok(used)
}

pub(super) fn oid(value: &Value) -> i64 {
    let I(oid) = value else {
        unreachable!("catalog OID is an integer")
    };
    *oid
}

pub(super) fn by_oid(rows: &[Vec<Value>]) -> HashMap<i64, &[Value]> {
    let mut index = HashMap::new();
    for row in rows {
        if let I(oid) = row[0] {
            index.entry(oid).or_insert(row.as_slice());
        }
    }
    index
}

#[derive(Default)]
pub(super) struct Lookup<'a> {
    pub(super) tables: HashMap<u64, &'a TableDef>,
    pub(super) relations: HashMap<i64, &'a TableDef>,
    pub(super) primary_indexes: HashMap<i64, &'a TableDef>,
    pub(super) indexes: HashMap<u64, &'a IndexDef>,
    pub(super) index_relations: HashMap<i64, &'a IndexDef>,
    pub(super) indexes_by_table: HashMap<u64, Vec<&'a IndexDef>>,
    pub(super) columns: HashMap<(u64, u64), usize>,
    pub(super) roles: HashMap<i64, &'a str>,
    pub(super) namespaces: HashMap<i64, &'a str>,
    pub(super) user_types: HashMap<i64, &'a UserType>,
    pub(super) collations: HashMap<u32, &'a str>,
    pub(super) user_collations: HashMap<i64, &'a [Value]>,
    pub(super) table_checks: HashMap<i64, i64>,
    pub(super) constraint_indexes: BTreeSet<u64>,
    pub(super) constrained_tables: BTreeSet<u64>,
}

impl<'a> Lookup<'a> {
    pub(super) fn new(snapshot: &'a CatalogSnapshot, budget: &mut Budget) -> R<Self> {
        let mut lookup = Self::default();
        for table in &snapshot.tables {
            budget.visit(1 + table.columns.len())?;
            let oid = i64::from(table_oid(table.id)?);
            if lookup.tables.insert(table.id, table).is_some() {
                return Err(Error::InvalidSnapshot);
            }
            lookup.relations.insert(oid, table);
            lookup.primary_indexes.insert(oid + 1, table);
            for (offset, column) in table.columns.iter().enumerate() {
                lookup.columns.insert((table.id, column.id), offset);
            }
        }
        for index in &snapshot.indexes {
            budget.visit(1 + index.columns.len())?;
            if let Ok(oid) = table_oid(index.id) {
                lookup
                    .index_relations
                    .entry(i64::from(oid))
                    .or_insert(index);
            }
            if lookup.tables.contains_key(&index.table) {
                lookup.indexes.insert(index.id, index);
                lookup
                    .indexes_by_table
                    .entry(index.table)
                    .or_default()
                    .push(index);
            }
        }
        for indexes in lookup.indexes_by_table.values_mut() {
            indexes.sort_by_key(|index| index.id);
        }
        for constraint in &snapshot.constraints {
            budget.visit(1 + constraint.columns.len() + constraint.foreign_columns.len())?;
            lookup.constrained_tables.insert(constraint.table);
            match constraint.kind {
                'c' => {
                    *lookup
                        .table_checks
                        .entry(i64::from(table_oid(constraint.table)?))
                        .or_default() += 1
                }
                'u' => {
                    lookup.constraint_indexes.insert(constraint.index);
                }
                _ => {}
            }
        }
        budget.visit(snapshot.roles.len() + snapshot.schemas.len() + snapshot.user_types.len())?;
        let role_names: BTreeSet<_> = snapshot
            .roles
            .iter()
            .map(|role| role.name.as_str())
            .collect();
        lookup.roles.extend(
            PREDEFINED_ROLES
                .iter()
                .filter(|(_, name)| !role_names.contains(name))
                .map(|&(oid, name)| (i64::from(oid), name)),
        );
        lookup.roles.extend(
            snapshot
                .roles
                .iter()
                .map(|role| (i64::from(role.oid), role.name.as_str())),
        );
        lookup.namespaces.extend([
            (11, "pg_catalog"),
            (2200, "public"),
            (13293, "information_schema"),
        ]);
        lookup.namespaces.extend(
            snapshot
                .schemas
                .iter()
                .map(|(name, &(oid, _))| (i64::from(oid), name.as_str())),
        );
        lookup
            .user_types
            .extend(snapshot.user_types.iter().map(|ty| (i64::from(ty.oid), ty)));
        for line in include_str!("../../../giggles-icu/src/pg_collation.txt").lines() {
            budget.visit(1)?;
            let mut fields = line.split('|');
            let oid = fields.next().unwrap().parse().unwrap();
            lookup.collations.insert(oid, fields.next().unwrap());
        }
        for row in &snapshot.collations {
            budget.visit(1)?;
            if row.len() != 13 {
                return Err(Error::InvalidSnapshot);
            }
            if let I(oid) = row[0] {
                lookup.user_collations.entry(oid).or_insert(row);
            }
        }
        Ok(lookup)
    }

    pub(super) fn column(&self, table: &TableDef, id: u64) -> Option<&ColumnDef> {
        let table = self.tables[&table.id];
        self.columns
            .get(&(table.id, id))
            .map(|&offset| &table.columns[offset])
    }
}
