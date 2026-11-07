//! Bounded, versioned catalog metadata for forthcoming DDL.
use super::*;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use std::fmt;
pub(crate) const UNSUPPORTED_CATALOG_EXT: &str = "unsupported relational catalog extension";
pub(crate) const MAX_EXT_COLUMNS: usize = MAX_TABLES * MAX_COLUMNS;
pub(crate) const MAX_EXT_CONSTRAINTS: usize = MAX_EXT_COLUMNS;
pub(crate) const MAX_EXT_SEQUENCES: usize = MAX_EXT_COLUMNS;
const MAX_EXPR: usize = 65536;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogExt {
    #[serde(deserialize_with = "version")]
    pub(crate) ext_version: u32,
    #[serde(deserialize_with = "column_map")]
    pub(crate) columns: BTreeMap<u64, ColumnExt>,
    #[serde(deserialize_with = "constraint_map")]
    pub(crate) constraints: BTreeMap<u64, Vec<Constraint>>,
    #[serde(deserialize_with = "sequence_map")]
    pub(crate) sequences: BTreeMap<u64, SequenceDef>,
    #[serde(deserialize_with = "type_map")]
    pub(crate) types: BTreeMap<u64, TypeDef>,
}
impl Default for CatalogExt {
    fn default() -> Self {
        Self {
            ext_version: 1,
            columns: BTreeMap::new(),
            constraints: BTreeMap::new(),
            sequences: BTreeMap::new(),
            types: BTreeMap::new(),
        }
    }
}
impl CatalogExt {
    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
            && self.constraints.is_empty()
            && self.sequences.is_empty()
            && self.types.is_empty()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct ColumnExt {
    pub(crate) typmod: Option<i32>,
    #[serde(deserialize_with = "optional_expr")]
    pub(crate) default: Option<String>,
    pub(crate) identity: Option<Identity>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub(crate) always: bool,
    pub(crate) sequence_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Constraint {
    pub(crate) id: u64,
    #[serde(deserialize_with = "name")]
    pub(crate) name: String,
    pub(crate) kind: ConstraintKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ConstraintKind {
    Check {
        #[serde(deserialize_with = "expression")]
        expr: String,
        not_valid: bool,
    },
    ForeignKey {
        #[serde(deserialize_with = "ids")]
        columns: Vec<u64>,
        ref_table: u64,
        #[serde(deserialize_with = "ids")]
        ref_columns: Vec<u64>,
        on_delete: Action,
        on_update: Action,
        not_valid: bool,
    },
    PrimaryKey,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)] // PostgreSQL's referential action names
pub(crate) enum Action {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceDef {
    #[serde(deserialize_with = "name")]
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) start: i64,
    pub(crate) increment: i64,
    pub(crate) min: i64,
    pub(crate) max: i64,
    pub(crate) cache: i64,
    pub(crate) cycle: bool,
    pub(crate) owned_by: Option<(u64, u64)>,
    pub(crate) identity: bool,
    pub(crate) incarnation: u128,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TypeDef {}
fn version<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
    let v = u32::deserialize(d)?;
    if v != 1 {
        return Err(de::Error::custom(UNSUPPORTED_CATALOG_EXT));
    }
    Ok(v)
}
fn bounded_vec<'de, D, T, const N: usize>(d: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct V<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for V<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded sequence")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            if a.size_hint().is_some_and(|n| n > N) {
                return Err(de::Error::custom("catalog sequence limit"));
            }
            let mut v = Vec::new();
            while let Some(x) = a.next_element()? {
                if v.len() == N {
                    return Err(de::Error::custom("catalog sequence limit"));
                }
                v.push(x);
            }
            Ok(v)
        }
    }
    d.deserialize_seq(V::<T, N>(std::marker::PhantomData))
}
fn bounded_map<'de, D, T, const N: usize>(d: D) -> Result<BTreeMap<u64, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct V<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for V<T, N> {
        type Value = BTreeMap<u64, T>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded map")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            if a.size_hint().is_some_and(|n| n > N) {
                return Err(de::Error::custom("catalog map limit"));
            }
            let mut v = BTreeMap::new();
            while let Some(k) = a.next_key()? {
                if v.len() == N {
                    return Err(de::Error::custom("catalog map limit"));
                }
                if v.insert(k, a.next_value()?).is_some() {
                    return Err(de::Error::custom("duplicate catalog key"));
                }
            }
            Ok(v)
        }
    }
    d.deserialize_map(V::<T, N>(std::marker::PhantomData))
}
fn string<'de, D: serde::Deserializer<'de>, const N: usize>(d: D) -> Result<String, D::Error> {
    String::from_utf8(bounded_vec::<D, u8, N>(d)?).map_err(de::Error::custom)
}
fn name<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    string::<D, 63>(d)
}
fn expression<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    string::<D, MAX_EXPR>(d)
}
fn optional_expr<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<String>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("optional expression")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            expression(d).map(Some)
        }
    }
    d.deserialize_option(V)
}
fn ids<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    bounded_vec::<D, u64, MAX_COLUMNS>(d)
}
fn column_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<u64, ColumnExt>, D::Error> {
    bounded_map::<D, ColumnExt, MAX_EXT_COLUMNS>(d)
}
#[derive(Deserialize)]
struct Constraints(#[serde(deserialize_with = "constraints")] Vec<Constraint>);
fn constraints<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Constraint>, D::Error> {
    bounded_vec::<D, Constraint, MAX_EXT_CONSTRAINTS>(d)
}
fn constraint_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<u64, Vec<Constraint>>, D::Error> {
    Ok(bounded_map::<D, Constraints, MAX_TABLES>(d)?
        .into_iter()
        .map(|(k, v)| (k, v.0))
        .collect())
}
fn sequence_map<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<u64, SequenceDef>, D::Error> {
    bounded_map::<D, SequenceDef, MAX_EXT_SEQUENCES>(d)
}
fn type_map<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<u64, TypeDef>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = BTreeMap<u64, TypeDef>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("empty type map")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            if a.size_hint().is_some_and(|n| n > 0) || a.next_key::<u64>()?.is_some() {
                return Err(de::Error::custom(UNSUPPORTED_CATALOG_EXT));
            }
            Ok(BTreeMap::new())
        }
    }
    d.deserialize_map(V)
}
pub(crate) fn validate_typmod(ty: &DataType, t: i32) -> R<()> {
    let valid = match ty {
        DataType::Numeric => {
            giggles_pg_types::typmod::NumericTypmod::from_typmod(t).is_some_and(|m| {
                giggles_pg_types::typmod::NumericTypmod::new(m.precision(), m.scale()).is_ok()
                    && m.to_typmod() == t
            })
        }
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz => {
            (0..=6).contains(&t)
        }
        DataType::Interval => {
            let range = (t >> 16) & 0x7fff;
            let prec = t & 0xffff;
            let masks = [
                4, 2, 8, 1024, 2048, 4096, 6, 1032, 3080, 7176, 3072, 7168, 6144,
            ];
            t >= 0
                && (range == 32767 || masks.contains(&range))
                && (prec == 65535 || (0..=6).contains(&prec))
                && (prec == 65535 || range == 32767 || range & 4096 != 0)
                && !(range == 32767 && prec == 65535)
        }
        _ => false,
    };
    if valid { Ok(()) } else { Err(SqlError::Type) }
}
impl Relational {
    pub(crate) fn catalog_ext(&self) -> &CatalogExt {
        &self.ext
    }
    pub(super) fn normalize_ext(ext: &mut CatalogExt) {
        ext.columns.retain(|_, v| v != &ColumnExt::default());
        ext.constraints.retain(|_, v| !v.is_empty());
    }
    pub(crate) fn validate_ext(&self) -> R<()> {
        self.validate_ext_candidate(&self.ext, self.next_id, None)
    }
    pub(super) fn validate_ext_candidate(
        &self,
        ext: &CatalogExt,
        next_id: u64,
        excluded_table: Option<u64>,
    ) -> R<()> {
        if ext.ext_version != 1 || !ext.types.is_empty() {
            return Err(SqlError::Unsupported);
        }
        if self.format != 1 {
            return Err(SqlError::Type);
        }
        if ext.columns.len() > MAX_EXT_COLUMNS
            || ext.constraints.len() > MAX_TABLES
            || ext.sequences.len() > MAX_EXT_SEQUENCES
        {
            return Err(SqlError::Type);
        }
        if let Some(excluded) = excluded_table
            && ext.constraints.values().flatten().any(|constraint| {
                matches!(&constraint.kind, ConstraintKind::ForeignKey { ref_table, .. } if *ref_table == excluded)
            }) {
                return Err(SqlError::Unsupported);
            }
        let tables: BTreeMap<_, _> = self
            .tables
            .values()
            .filter(|t| Some(t.id) != excluded_table)
            .map(|t| (t.id, t))
            .collect();
        let columns: BTreeMap<_, _> = self
            .tables
            .values()
            .filter(|t| Some(t.id) != excluded_table)
            .flat_map(|t| t.columns.iter().map(move |c| (c.id, (t.id, c))))
            .collect();
        let mut used: BTreeSet<_> = tables.keys().chain(columns.keys()).copied().collect();
        for (id, e) in &ext.columns {
            let (_, c) = columns.get(id).ok_or(SqlError::Type)?;
            if let Some(t) = e.typmod {
                validate_typmod(&c.data_type, t)?;
            }
            if e.default.as_ref().is_some_and(|s| s.len() > MAX_EXPR) {
                return Err(SqlError::Type);
            }
            if let Some(i) = &e.identity
                && !ext
                    .sequences
                    .get(&i.sequence_id)
                    .is_some_and(|s| s.identity)
                {
                    return Err(SqlError::Type);
                }
        }
        let mut total = 0usize;
        for (table, cs) in &ext.constraints {
            let local = tables.get(table).ok_or(SqlError::Type)?;
            let mut names = BTreeSet::new();
            for c in cs {
                total += 1;
                if total > MAX_EXT_CONSTRAINTS
                    || c.id == 0
                    || c.id >= next_id
                    || !used.insert(c.id)
                    || c.name.len() > 63
                    || c.name.is_empty()
                    || !names.insert(&c.name)
                {
                    return Err(SqlError::Type);
                }
                match &c.kind {
                    ConstraintKind::Check { expr, .. } => {
                        if expr.len() > MAX_EXPR {
                            return Err(SqlError::Type);
                        }
                    }
                    ConstraintKind::ForeignKey {
                        columns: cols,
                        ref_table,
                        ref_columns,
                        ..
                    } => {
                        let remote = tables.get(ref_table).ok_or(SqlError::Type)?;
                        if cols.is_empty()
                            || cols.len() > MAX_COLUMNS
                            || cols.len() != ref_columns.len()
                        {
                            return Err(SqlError::Type);
                        }
                        let mut seen = BTreeSet::new();
                        let mut refs = BTreeSet::new();
                        for (a, b) in cols.iter().zip(ref_columns) {
                            if !local.columns.iter().any(|c| c.id == *a)
                                || !remote.columns.iter().any(|c| c.id == *b)
                                || !seen.insert(a)
                                || !refs.insert(b)
                            {
                                return Err(SqlError::Type);
                            }
                        }
                    }
                    ConstraintKind::PrimaryKey => {}
                }
            }
        }
        let mut names = BTreeSet::new();
        for (id, s) in &ext.sequences {
            if *id == 0 || *id >= next_id || !used.insert(*id) || !names.insert(&s.name) {
                return Err(SqlError::Type);
            }
            ident(&s.name)?;
            if s.name.len() > 63
                || !matches!(
                    s.data_type,
                    DataType::SmallInt | DataType::Integer | DataType::BigInt
                )
                || s.increment == 0
                || s.min > s.max
                || s.start < s.min
                || s.start > s.max
                || s.cache <= 0
            {
                return Err(SqlError::Type);
            }
            let (lo, hi) = match s.data_type {
                DataType::SmallInt => (i16::MIN as i64, i16::MAX as i64),
                DataType::Integer => (i32::MIN as i64, i32::MAX as i64),
                _ => (i64::MIN, i64::MAX),
            };
            if s.min < lo || s.max > hi {
                return Err(SqlError::Type);
            }
            if let Some((t, c)) = s.owned_by
                && !columns.get(&c).is_some_and(|(owner, _)| *owner == t) {
                    return Err(SqlError::Type);
                }
        }
        Ok(())
    }
    fn require_column(&self, id: u64) -> R<()> {
        if self
            .tables
            .values()
            .any(|t| t.columns.iter().any(|c| c.id == id))
        {
            Ok(())
        } else {
            Err(SqlError::Type)
        }
    }
    fn mutate_ext(&mut self, f: impl FnOnce(&mut CatalogExt, &mut u64) -> R<()>) -> R<()> {
        let mut ext = self.ext.clone();
        let mut next_id = self.next_id;
        f(&mut ext, &mut next_id)?;
        Self::normalize_ext(&mut ext);
        self.validate_ext_candidate(&ext, next_id, None)?;
        self.ext = ext;
        self.next_id = next_id;
        Ok(())
    }
    pub(crate) fn set_column_typmod(&mut self, id: u64, t: Option<i32>) -> R<()> {
        let col = self
            .tables
            .values()
            .flat_map(|t| &t.columns)
            .find(|c| c.id == id)
            .ok_or(SqlError::Type)?;
        if let Some(t) = t {
            validate_typmod(&col.data_type, t).map_err(|_| SqlError::InvalidParameterValue)?;
        }
        self.mutate_ext(|ext, _| {
            ext.columns.entry(id).or_default().typmod = t;
            Ok(())
        })
    }
    pub(crate) fn set_column_default(&mut self, id: u64, v: Option<String>) -> R<()> {
        self.require_column(id)?;
        self.mutate_ext(|ext, _| {
            ext.columns.entry(id).or_default().default = v;
            Ok(())
        })
    }
    pub(crate) fn set_column_identity(&mut self, id: u64, v: Option<Identity>) -> R<()> {
        self.require_column(id)?;
        self.mutate_ext(|ext, _| {
            ext.columns.entry(id).or_default().identity = v;
            Ok(())
        })
    }
    pub(crate) fn add_constraint(&mut self, table: u64, c: Constraint) -> R<()> {
        self.mutate_ext(|ext, next_id| {
            if c.id != *next_id {
                return Err(SqlError::Type);
            }
            allocate(next_id)?;
            ext.constraints.entry(table).or_default().push(c);
            Ok(())
        })
    }
    pub(crate) fn remove_constraint(&mut self, table: u64, name: &str) -> R<()> {
        if !self.tables.values().any(|t| t.id == table) {
            return Err(SqlError::Type);
        }
        self.mutate_ext(|ext, _| {
            if let Some(cs) = ext.constraints.get_mut(&table) {
                cs.retain(|c| c.name != name)
            }
            Ok(())
        })
    }
    pub(crate) fn add_sequence(&mut self, v: SequenceDef) -> R<u64> {
        let mut id = 0;
        self.mutate_ext(|ext, next_id| {
            id = allocate(next_id)?;
            ext.sequences.insert(id, v);
            Ok(())
        })?;
        Ok(id)
    }
    pub(crate) fn remove_sequence(&mut self, id: u64) -> R<()> {
        self.mutate_ext(|ext, _| {
            ext.sequences.remove(&id);
            Ok(())
        })
    }
    pub(crate) fn drop_column_ext(&mut self, id: u64) -> R<()> {
        self.mutate_ext(|ext, _| {
            ext.columns.remove(&id);
            ext.sequences
                .retain(|_, v| !matches!(v.owned_by,Some((_,c)) if c==id));
            Ok(())
        })
    }
    pub(crate) fn column_typmod(&self, id: u64) -> i32 {
        self.ext
            .columns
            .get(&id)
            .and_then(|c| c.typmod)
            .unwrap_or(-1)
    }
    pub(crate) fn column_default(&self, id: u64) -> Option<&str> {
        self.ext.columns.get(&id).and_then(|c| c.default.as_deref())
    }
    pub(crate) fn column_identity(&self, id: u64) -> Option<&Identity> {
        self.ext.columns.get(&id).and_then(|c| c.identity.as_ref())
    }
    pub(crate) fn table_constraints(&self, id: u64) -> &[Constraint] {
        self.ext
            .constraints
            .get(&id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
    pub(crate) fn sequence_by_name(&self, name: &str) -> Option<(u64, &SequenceDef)> {
        self.ext
            .sequences
            .iter()
            .find(|(_, s)| s.name == name)
            .map(|(id, s)| (*id, s))
    }
}
