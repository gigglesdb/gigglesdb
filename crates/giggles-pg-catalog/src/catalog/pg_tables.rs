//! pg_tables over the same authorized inventory as pg_class.
use super::*;
pub(super) fn schema() -> Vec<Field> {
    fields(&[("schemaname", T::Name), ("tablename", T::Name), ("tableowner", T::Name), ("tablespace", T::Name), ("hasindexes", T::Bool), ("hasrules", T::Bool), ("hastriggers", T::Bool), ("rowsecurity", T::Bool)])
}
pub(super) fn populate(lookup: &Lookup<'_>, budget: &mut Budget, all: &mut BTreeMap<String, Relation>) -> R<()> {
    let mut rows = Vec::new();
    for row in &all["pg_class"].rows {
        budget.visit(1)?;
        if !matches!(&row[3], Value::Text(kind) if kind == "r" || kind == "p") {
            continue;
        }
        let namespace = lookup.namespaces[&row_oid(&row[2])];
        let owner = lookup.roles.get(&row_oid(&row[4])).map_or(N, |name| s(name));
        // Every PG18 physical system catalog has an index; virtual pg_class
        // deliberately omits those index descriptors from its finite inventory.
        let has_indexes = if row[2] == I(11) { B(true) } else { row[6].clone() };
        budget.add(&mut rows, vec![s(namespace), row[1].clone(), owner, N, has_indexes, row[17].clone(), row[18].clone(), row[19].clone()])?;
    }
    all.get_mut("pg_tables").unwrap().rows = rows;
    Ok(())
}
