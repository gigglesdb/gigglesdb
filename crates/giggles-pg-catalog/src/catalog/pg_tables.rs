//! pg_tables over the same authorized inventory as pg_class.
use super::*;
pub(super) fn schema() -> Vec<Field> {
    fields(&[("schemaname", T::Name), ("tablename", T::Name), ("tableowner", T::Name), ("tablespace", T::Name), ("hasindexes", T::Bool), ("hasrules", T::Bool), ("hastriggers", T::Bool), ("rowsecurity", T::Bool)])
}
pub(super) fn populate(snapshot: &CatalogSnapshot, all: &mut BTreeMap<String, Relation>) {
    let rows = all["pg_class"].rows.iter().filter(|r| r[3] == s("r") || r[3] == s("p")).map(|r| {
        let namespace = all["pg_namespace"].rows.iter().find(|n| n[0] == r[2]).expect("class namespace");
        let owner = if let I(id) = r[4] { role_name(snapshot, id).map(s).unwrap_or(N) } else { N };
        // Every PG18 physical system catalog has an index; virtual pg_class
        // deliberately omits those index descriptors from its finite inventory.
        vec![namespace[1].clone(), r[1].clone(), owner, N, if r[2] == I(11) { B(true) } else { r[6].clone() }, r[17].clone(), r[18].clone(), r[19].clone()]
    }).collect();
    all.get_mut("pg_tables").unwrap().rows = rows;
}
