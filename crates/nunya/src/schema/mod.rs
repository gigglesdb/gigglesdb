//! Database schema snapshots and exact-name resolution.
use crate::value::ColumnType;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
/// Pg.
pub mod pg;
/// Spanner gsql.
pub mod spanner_gsql;
/// Spanner pg.
pub mod spanner_pg;
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
/// Fully qualified column name using database-stored case, preserving quoted identifiers.
pub struct ColumnRef {
    /// Schema.
    pub schema: String,
    /// Table.
    pub table: String,
    /// Column.
    pub column: String,
}
/// Database identity of a captured column.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum ColumnId {
    /// PostgreSQL identity, stable across column renames.
    Pg {
        /// Owning relation OID.
        relation_oid: u32,
        /// Attribute number within the relation.
        attnum: i16,
    },
    /// Spanner identity from exact names and the raw information-schema type.
    Spanner {
        /// Schema name.
        schema: String,
        /// Table name.
        table: String,
        /// Column name.
        column: String,
        /// Exact information-schema type string.
        spanner_type: String,
    },
}
/// Column.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    /// Name.
    pub name: String,
    /// Database column type.
    pub ty: ColumnType,
    /// Whether the database allows NULL.
    pub nullable: bool,
    /// Generated expression text, when present.
    pub generated: Option<String>,
    /// Default expression text, when present.
    pub default: Option<String>,
}
/// Relation kind.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RelationKind {
    /// Table.
    Table,
    /// View.
    View,
    /// MaterializedView.
    MaterializedView,
    /// Foreign.
    Foreign,
    /// Other.
    Other,
}
/// Relation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Relation {
    /// Schema.
    pub schema: String,
    /// Name.
    pub name: String,
    /// Kind.
    pub kind: RelationKind,
    /// Columns.
    pub columns: Vec<Column>,
    /// View definition.
    pub view_definition: Option<String>,
    /// Primary-key column names in key order.
    pub primary_key: Vec<String>,
}
/// Dependent kind.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DependentKind {
    /// Trigger.
    Trigger,
    /// Rule.
    Rule,
    /// Policy.
    Policy,
    /// GeneratedColumn.
    GeneratedColumn,
    /// Default.
    Default,
    /// View.
    View,
}
/// Dependent object.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DependentObject {
    /// Kind.
    pub kind: DependentKind,
    /// Name.
    pub name: String,
    /// Columns read by this dependent object.
    pub reads: Vec<ColumnRef>,
    /// Qualified table written by this dependent object, if any.
    pub writes_table: Option<(String, String)>,
}
/// Snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    /// Relations.
    pub relations: Vec<Relation>,
    /// Captured column identities; an empty map means identity is unknown.
    #[serde(default)]
    pub column_ids: BTreeMap<ColumnRef, ColumnId>,
    /// Dependents.
    pub dependents: Vec<DependentObject>,
    /// Sorted, deduplicated lower-case names of user-defined routines and operators.
    #[serde(default)]
    pub user_routines: Vec<String>,
    /// Search path.
    pub search_path: Vec<String>,
    /// Exact schema names captured by the loader, when provenance is known.
    #[serde(default)]
    pub captured_schemas: Option<Vec<String>>,
    /// SHA-256 of canonical relations, dependents, routine names, and known column identities.
    pub fingerprint: [u8; 32],
    /// Snapshot capture time in milliseconds since the Unix epoch.
    pub taken_at_unix_ms: u64,
}
impl Snapshot {
    /// Build a snapshot with a canonical, order-independent schema fingerprint.
    pub fn new(
        relations: Vec<Relation>,
        dependents: Vec<DependentObject>,
        search_path: Vec<String>,
        taken_at_unix_ms: u64,
    ) -> Self {
        Self {
            relations,
            column_ids: BTreeMap::new(),
            dependents,
            search_path,
            captured_schemas: None,
            user_routines: vec![],
            fingerprint: [0; 32],
            taken_at_unix_ms,
        }
        .with_user_routines(vec![])
    }
    /// Record captured schema names without changing the schema fingerprint.
    pub fn with_captured_schemas(mut self, mut schemas: Vec<String>) -> Self {
        schemas.sort();
        schemas.dedup();
        self.captured_schemas = Some(schemas);
        self
    }
    /// Canonicalize user routine names and include them in the schema fingerprint.
    pub fn with_user_routines(mut self, names: Vec<String>) -> Snapshot {
        self.user_routines = names.into_iter().map(|name| name.to_lowercase()).collect();
        self.user_routines.sort();
        self.user_routines.dedup();
        self.recompute_fingerprint();
        self
    }
    /// Attach column identities and include them in the schema fingerprint.
    pub fn with_column_ids(mut self, ids: BTreeMap<ColumnRef, ColumnId>) -> Snapshot {
        self.column_ids = ids;
        self.recompute_fingerprint();
        self
    }
    /// Look up the captured identity of a fully qualified column.
    pub fn column_id(&self, c: &ColumnRef) -> Option<&ColumnId> {
        self.column_ids.get(c)
    }
    fn recompute_fingerprint(&mut self) {
        let mut canonical_relations: Vec<_> = self.relations.iter().collect();
        canonical_relations.sort_by_cached_key(|r| {
            serde_json::to_vec(r).expect("schema serialization is infallible")
        });
        let mut canonical_dependents = self.dependents.clone();
        for dependent in &mut canonical_dependents {
            dependent.reads.sort();
        }
        canonical_dependents.sort_by_cached_key(|d| {
            serde_json::to_vec(d).expect("schema serialization is infallible")
        });
        let canonical = if self.column_ids.is_empty() {
            serde_json::to_vec(&(
                canonical_relations,
                canonical_dependents,
                &self.user_routines,
            ))
        } else {
            let ids: Vec<_> = self.column_ids.iter().collect();
            serde_json::to_vec(&(
                canonical_relations,
                canonical_dependents,
                &self.user_routines,
                ids,
            ))
        }
        .expect("schema serialization is infallible");
        self.fingerprint = Sha256::digest(canonical).into();
    }
    /// Resolve a relation by exact stored name, walking the search path when unqualified.
    pub fn resolve_relation(&self, schema: Option<&str>, name: &str) -> Option<&Relation> {
        if let Some(schema) = schema {
            self.relations
                .iter()
                .find(|r| r.schema == schema && r.name == name)
        } else {
            self.search_path.iter().find_map(|schema| {
                self.relations
                    .iter()
                    .find(|r| r.schema == *schema && r.name == name)
            })
        }
    }
    /// Find a column by its fully qualified stored name.
    pub fn column(&self, r: &ColumnRef) -> Option<&Column> {
        self.resolve_relation(Some(&r.schema), &r.table)?
            .columns
            .iter()
            .find(|c| c.name == r.column)
    }
}
/// SHA-256 of canonical table/column names, types and captured identities.
pub fn catalog_fingerprint(snapshot: &Snapshot) -> String {
    let mut rows: Vec<Vec<u8>> = snapshot.relations.iter().map(|r| {
        let mut columns: Vec<_> = r.columns.iter().map(|c| {
            let name = ColumnRef { schema: r.schema.clone(), table: r.name.clone(), column: c.name.clone() };
            serde_json::to_vec(&(&c.name, &c.ty, snapshot.column_id(&name)))
                .expect("schema serialization is infallible")
        }).collect();
        columns.sort();
        serde_json::to_vec(&(&r.schema, &r.name, columns)).expect("schema serialization is infallible")
    }).collect();
    rows.sort();
    data_encoding::HEXLOWER.encode(&Sha256::digest(serde_json::to_vec(&rows).expect("schema serialization is infallible")))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn relation(schema: &str) -> Relation {
        Relation {
            schema: schema.into(),
            name: "items".into(),
            kind: RelationKind::Table,
            columns: vec![],
            view_definition: None,
            primary_key: vec![],
        }
    }
    #[test]
    fn captured_schemas_preserve_fingerprint_and_default_for_legacy_snapshots() {
        let base = Snapshot::new(vec![], vec![], vec!["public".into()], 0);
        assert_eq!(base.captured_schemas, None);
        let captured = base.clone().with_captured_schemas(vec![
            "public".into(), "Other".into(), "public".into(),
        ]);
        assert_eq!(captured.captured_schemas, Some(vec!["Other".into(), "public".into()]));
        assert_eq!(captured.fingerprint, base.fingerprint);
        assert_eq!(captured.clone().with_user_routines(vec![]).fingerprint, base.fingerprint);
        assert_eq!(serde_json::from_slice::<Snapshot>(&serde_json::to_vec(&captured).unwrap()).unwrap(), captured);
        let mut legacy = serde_json::to_value(&base).unwrap();
        legacy.as_object_mut().unwrap().remove("captured_schemas");
        let restored: Snapshot = serde_json::from_value(legacy).unwrap();
        assert_eq!(restored.captured_schemas, None);
        assert_eq!(restored, base);
    }
    #[test]
    fn column_identity_fingerprints_and_legacy_json() {
        let base = Snapshot::new(vec![relation("public")], vec![], vec![], 0);
        let names = vec!["Routine".into()];
        let legacy = base.clone().with_user_routines(names.clone());
        assert_eq!(
            base.clone().with_column_ids(BTreeMap::new()).fingerprint,
            base.fingerprint
        );
        assert_eq!(
            legacy.clone().with_column_ids(BTreeMap::new()).fingerprint,
            legacy.fingerprint
        );
        let canonical =
            serde_json::to_vec(&(&legacy.relations, &legacy.dependents, &legacy.user_routines))
                .unwrap();
        assert_eq!(
            legacy.fingerprint,
            <[u8; 32]>::from(Sha256::digest(canonical))
        );
        let c = ColumnRef {
            schema: "public".into(),
            table: "items".into(),
            column: "id".into(),
        };
        let id = ColumnId::Pg {
            relation_oid: 42,
            attnum: 1,
        };
        let ids = BTreeMap::from([(c.clone(), id.clone())]);
        let first = base
            .clone()
            .with_column_ids(ids.clone())
            .with_user_routines(names.clone());
        let second = base.clone().with_user_routines(names).with_column_ids(ids);
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(first.column_id(&c), Some(&id));
        assert_eq!(base.column_id(&c), None);
        assert_ne!(first.fingerprint, legacy.fingerprint);
        for changed in [
            ColumnId::Pg {
                relation_oid: 43,
                attnum: 1,
            },
            ColumnId::Pg {
                relation_oid: 42,
                attnum: 2,
            },
        ] {
            assert_ne!(
                first.fingerprint,
                legacy
                    .clone()
                    .with_column_ids(BTreeMap::from([(c.clone(), changed)]))
                    .fingerprint
            );
        }
        let mut json = serde_json::to_value(&legacy).unwrap();
        json.as_object_mut().unwrap().remove("column_ids");
        let restored: Snapshot = serde_json::from_value(json).unwrap();
        assert!(restored.column_ids.is_empty());
        assert_eq!(restored, legacy);
    }
    #[test]
    fn user_routines_are_canonical() {
        let base = Snapshot::new(vec![], vec![], vec![], 0);
        let a = base
            .clone()
            .with_user_routines(vec!["Z".into(), "a".into(), "z".into()]);
        let b = base
            .clone()
            .with_user_routines(vec!["a".into(), "z".into()]);
        assert_eq!(a.user_routines, vec!["a", "z"]);
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_eq!(
            a.clone()
                .with_user_routines(a.user_routines.clone())
                .fingerprint,
            a.fingerprint
        );
        assert_ne!(base.fingerprint, a.fingerprint);
        assert_eq!(
            base.clone().with_user_routines(vec![]).fingerprint,
            base.fingerprint
        );
    }
    #[test]
    fn search_path_and_exact_names() {
        let s = Snapshot::new(
            vec![relation("public"), relation("private")],
            vec![],
            vec!["private".into(), "public".into()],
            0,
        );
        assert_eq!(s.resolve_relation(None, "items").unwrap().schema, "private");
        assert_eq!(
            s.resolve_relation(Some("public"), "items").unwrap().schema,
            "public"
        );
        assert!(s.resolve_relation(None, "Items").is_none());
    }
    #[test]
    fn fingerprint_is_canonical_and_sensitive() {
        let a = relation("a");
        let b = relation("b");
        let d = DependentObject {
            kind: DependentKind::View,
            name: "v".into(),
            reads: vec![],
            writes_table: None,
        };
        let e = DependentObject {
            name: "w".into(),
            ..d.clone()
        };
        let s = Snapshot::new(
            vec![a.clone(), b.clone()],
            vec![d.clone(), e.clone()],
            vec![],
            0,
        );
        let t = Snapshot::new(vec![b.clone(), a.clone()], vec![e, d], vec![], 1);
        assert_eq!(s.fingerprint, t.fingerprint);
        let mut c = a;
        c.columns.push(Column {
            name: "id".into(),
            ty: ColumnType::Int8,
            nullable: false,
            generated: None,
            default: None,
        });
        assert_ne!(
            s.fingerprint,
            Snapshot::new(vec![c, b], s.dependents.clone(), vec![], 0).fingerprint
        );
    }
}

