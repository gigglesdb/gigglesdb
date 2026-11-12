use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use tokio_postgres::IsolationLevel;

use super::super::{
    ConstraintFinding, ForeignKeyFinding, IndexFinding, KeyOrder, RangePlan, Result, SchemaFinding,
    SequenceFinding, VerifyError,
};
use super::{
    PgRowSource, Side, chunk_error, decode_key, ident, key_order, parameters, parse_count, pg_error,
};
use crate::pg::chunk::QualifiedName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaColumn {
    pub name: String,
    pub position: i16,
    pub sql_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub generated: String,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceState {
    pub name: String,
    pub last_value: i64,
    pub is_called: bool,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cycle: bool,
}
impl SequenceState {
    fn next(&self) -> Option<i128> {
        let next = i128::from(self.last_value)
            + if self.is_called {
                i128::from(self.increment)
            } else {
                0
            };
        (next >= i128::from(self.min) && next <= i128::from(self.max)).then_some(next)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintState {
    pub name: String,
    pub kind: String,
    pub definition: String,
    pub validated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexState {
    pub name: String,
    pub definition: String,
    pub valid: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogSections {
    pub columns: Vec<SchemaColumn>,
    pub sequences: Vec<SequenceState>,
    pub constraints: Vec<ConstraintState>,
    pub indexes: Vec<IndexState>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SectionDifferences {
    pub schema: Vec<SchemaFinding>,
    pub sequences: Vec<SequenceFinding>,
    pub constraints: Vec<ConstraintFinding>,
    pub indexes: Vec<IndexFinding>,
}

/// Absence and extra objects are differences too. Invalid destination objects
/// are findings even when the source has the same invalid state.
pub fn diff_sections(
    source: &CatalogSections,
    destination: &CatalogSections,
) -> SectionDifferences {
    let schema = diff_named(&source.columns, &destination.columns, |v| &v.name)
        .into_iter()
        .map(|object| SchemaFinding {
            object,
            detail: "column definition differs".into(),
        })
        .collect();
    let mut sequences = Vec::new();
    let src: BTreeMap<_, _> = source.sequences.iter().map(|s| (&s.name, s)).collect();
    let dst: BTreeMap<_, _> = destination.sequences.iter().map(|s| (&s.name, s)).collect();
    for name in src
        .keys()
        .chain(dst.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let detail = match (src.get(name), dst.get(name)) {
            (Some(s), Some(d))
                if (s.increment, s.min, s.max, s.cycle) != (d.increment, d.min, d.max, d.cycle) =>
            {
                Some("sequence configuration differs")
            }
            (Some(s), Some(_)) if s.cycle => Some("cycling sequence position is not orderable"),
            (Some(s), Some(d)) => match (s.next(), d.next()) {
                (None, Some(_)) => Some("destination sequence is behind exhausted source"),
                (Some(a), Some(b)) if (s.increment > 0 && b < a) || (s.increment < 0 && b > a) => {
                    Some("destination sequence is behind source")
                }
                _ => None,
            },
            _ => Some("sequence missing or extra"),
        };
        if let Some(detail) = detail {
            sequences.push(SequenceFinding {
                sequence: name.clone(),
                detail: detail.into(),
            });
        }
    }
    let mut constraint_names: BTreeSet<_> =
        diff_named(&source.constraints, &destination.constraints, |v| &v.name)
            .into_iter()
            .collect();
    constraint_names.extend(
        destination
            .constraints
            .iter()
            .filter(|c| !c.validated)
            .map(|c| c.name.clone()),
    );
    let constraints = constraint_names
        .into_iter()
        .map(|constraint| ConstraintFinding {
            constraint,
            detail: "constraint differs or destination is NOT VALID".into(),
        })
        .collect();
    let mut index_names: BTreeSet<_> =
        diff_named(&source.indexes, &destination.indexes, |v| &v.name)
            .into_iter()
            .collect();
    index_names.extend(
        destination
            .indexes
            .iter()
            .filter(|i| !i.valid)
            .map(|i| i.name.clone()),
    );
    let indexes = index_names
        .into_iter()
        .map(|index| IndexFinding {
            index,
            detail: "index differs or destination indisvalid is false".into(),
        })
        .collect();
    SectionDifferences {
        schema,
        sequences,
        constraints,
        indexes,
    }
}

fn diff_named<T: PartialEq>(
    source: &[T],
    destination: &[T],
    name: impl Fn(&T) -> &String,
) -> Vec<String> {
    let source: BTreeMap<_, _> = source.iter().map(|v| (name(v), v)).collect();
    let destination: BTreeMap<_, _> = destination.iter().map(|v| (name(v), v)).collect();
    source
        .keys()
        .chain(destination.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| source.get(key) != destination.get(key))
        .cloned()
        .collect()
}

impl<O: KeyOrder + Send> PgRowSource<O> {
    /// Catalog collection is table-scoped and uses one read-only RR snapshot.
    /// Sequence positions are non-MVCC PostgreSQL state, sampled without nextval.
    pub async fn collect_sections(&mut self) -> Result<CatalogSections> {
        let relation = self.key.relation.sql().map_err(chunk_error)?;
        let tx = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(pg_error)?;
        let columns = tx.query("SELECT a.attname::text, a.attnum, format_type(a.atttypid,a.atttypmod), NOT a.attnotnull, pg_get_expr(d.adbin,d.adrelid), a.attgenerated::text, a.attidentity::text FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=$1::text::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", &[&relation]).await.map_err(pg_error)?
            .iter().map(|r| SchemaColumn { name:r.get(0), position:r.get(1), sql_type:r.get(2), nullable:r.get(3), default:r.get(4), generated:r.get(5), identity:r.get(6) }).collect();
        let constraints = tx.query("SELECT conname::text, contype::text, pg_get_constraintdef(oid), convalidated FROM pg_constraint WHERE conrelid=$1::text::regclass ORDER BY conname", &[&relation]).await.map_err(pg_error)?
            .iter().map(|r| ConstraintState { name:r.get(0), kind:r.get(1), definition:r.get(2), validated:r.get(3) }).collect();
        let indexes = tx.query("SELECT c.relname::text, pg_get_indexdef(i.indexrelid), i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=$1::text::regclass ORDER BY c.relname", &[&relation]).await.map_err(pg_error)?
            .iter().map(|r| IndexState { name:r.get(0), definition:r.get(1), valid:r.get(2) }).collect();
        let seqs = tx.query("WITH scoped AS (SELECT d.objid AS oid FROM pg_depend d WHERE d.refobjid=$1::text::regclass AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') UNION SELECT d.refobjid FROM pg_attrdef a JOIN pg_depend d ON d.classid='pg_attrdef'::regclass AND d.objid=a.oid AND d.refclassid='pg_class'::regclass WHERE a.adrelid=$1::text::regclass) SELECT n.nspname::text, c.relname::text, s.seqincrement, s.seqmin, s.seqmax, s.seqcycle FROM scoped JOIN pg_class c ON c.oid=scoped.oid AND c.relkind='S' JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_sequence s ON s.seqrelid=c.oid ORDER BY 1,2", &[&relation]).await.map_err(pg_error)?;
        let mut sequences = Vec::new();
        for seq in seqs {
            let name = QualifiedName {
                schema: seq.get(0),
                name: seq.get(1),
            }
            .sql()
            .map_err(chunk_error)?;
            let state = tx
                .query_one(&format!("SELECT last_value, is_called FROM {name}"), &[])
                .await
                .map_err(pg_error)?;
            sequences.push(SequenceState {
                name,
                last_value: state.get(0),
                is_called: state.get(1),
                increment: seq.get(2),
                min: seq.get(3),
                max: seq.get(4),
                cycle: seq.get(5),
            });
        }
        tx.commit().await.map_err(pg_error)?;
        Ok(CatalogSections {
            columns,
            sequences,
            constraints,
            indexes,
        })
    }

    /// Scans child keys in fixed-size pages and evaluates a NOT EXISTS anti-join
    /// on each page. No parent rows or child payloads cross the connection.
    /// NOT VALID constraints are included; MATCH SIMPLE/FULL NULL rules differ.
    pub async fn check_foreign_keys(&mut self, page_rows: u32) -> Result<Vec<ForeignKeyFinding>> {
        if self.side != Side::Destination {
            return Err(VerifyError::Invalid("FK integrity is destination-only"));
        }
        if page_rows == 0 || u64::from(page_rows) > self.budget.max_rows {
            return Err(VerifyError::Invalid("FK page row cap"));
        }
        let relation = self.key.relation.sql().map_err(chunk_error)?;
        let tx = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(pg_error)?;
        let fks = tx.query("SELECT c.conname::text, n.nspname::text, p.relname::text, c.confmatchtype::text, array_agg(a.attname::text ORDER BY k.ord), array_agg(b.attname::text ORDER BY k.ord), array_agg(o.oprname::text ORDER BY k.ord), array_agg(onsp.nspname::text ORDER BY k.ord) FROM pg_constraint c JOIN pg_class p ON p.oid=c.confrelid JOIN pg_namespace n ON n.oid=p.relnamespace CROSS JOIN LATERAL unnest(c.conkey,c.confkey,c.conpfeqop) WITH ORDINALITY k(child,parent,eqop,ord) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.child JOIN pg_attribute b ON b.attrelid=c.confrelid AND b.attnum=k.parent JOIN pg_operator o ON o.oid=k.eqop JOIN pg_namespace onsp ON onsp.oid=o.oprnamespace WHERE c.conrelid=$1::text::regclass AND c.contype='f' GROUP BY c.oid,c.conname,c.confmatchtype,n.nspname,p.relname ORDER BY c.conname", &[&relation]).await.map_err(pg_error)?;
        let mut findings = Vec::new();
        for fk in fks {
            let name: String = fk.get(0);
            let parent = QualifiedName {
                schema: fk.get(1),
                name: fk.get(2),
            }
            .sql()
            .map_err(chunk_error)?;
            let match_type: String = fk.get(3);
            if match_type != "s" && match_type != "f" {
                return Err(VerifyError::Invalid("unsupported FK match type"));
            }
            let child: Vec<String> = fk.get(4);
            let parent_columns: Vec<String> = fk.get(5);
            let operators: Vec<String> = fk.get(6);
            let namespaces: Vec<String> = fk.get(7);
            let mut equalities = Vec::new();
            for (((c, p), op), ns) in child
                .iter()
                .zip(&parent_columns)
                .zip(&operators)
                .zip(&namespaces)
            {
                // Operator names are catalog data, but cannot be identifier-quoted.
                if op.is_empty() || !op.bytes().all(|b| b"+-*/<>=~!@#%^&|`?".contains(&b)) {
                    return Err(VerifyError::Invalid("FK operator"));
                }
                equalities.push(format!(
                    "p.{} OPERATOR({}.{}) c.{}",
                    ident(p)?,
                    ident(ns)?,
                    op,
                    ident(c)?
                ));
            }
            let nonnull = child
                .iter()
                .map(|c| Ok(format!("c.{} IS NOT NULL", ident(c)?)))
                .collect::<Result<Vec<_>>>()?;
            let eligible = nonnull.join(if match_type == "s" { " AND " } else { " OR " });
            let all_nonnull = nonnull.join(" AND ");
            let violation = format!(
                "({eligible}) AND (NOT ({all_nonnull}) OR NOT EXISTS (SELECT 1 FROM {parent} p WHERE {}))",
                equalities.join(" AND ")
            );
            let selected = self
                .key
                .columns
                .iter()
                .map(|k| {
                    Ok(super::text_expression(
                        &format!("c.{}", ident(&k.name)?),
                        &k.pg_type,
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            let mut lo = None;
            let mut violations = 0u64;
            loop {
                let page = RangePlan {
                    id: crate::pg::chunk::ChunkId {
                        table: self.key.table.clone(),
                        plan: "0".repeat(64),
                        ordinal: 0,
                    },
                    lo: lo.clone(),
                    hi: None,
                };
                let query = self.key.read_query(&page).map_err(chunk_error)?;
                // Read only key/FK fields, even when excluded payloads are huge.
                let fields = self
                    .key
                    .columns
                    .iter()
                    .map(|k| k.name.clone())
                    .chain(child.iter().cloned())
                    .collect::<BTreeSet<_>>();
                let fields = fields
                    .iter()
                    .map(|c| ident(c))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let base = query
                    .sql
                    .replacen("SELECT *", &format!("SELECT {fields}"), 1);
                let offset = usize::from(lo.is_some());
                let cte =
                    format!("WITH c AS MATERIALIZED ({base} LIMIT {page_rows} OFFSET {offset})");
                let size_terms = self
                    .key
                    .columns
                    .iter()
                    .map(|k| {
                        Ok(format!(
                            "8::numeric + COALESCE(octet_length({}),0)",
                            super::text_expression(&format!("c.{}", ident(&k.name)?), &k.pg_type)
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(" + ");
                let size_sql =
                    format!("{cte} SELECT COALESCE(sum(1 + {size_terms}),0)::text FROM c");
                let size = tx
                    .query_one(&size_sql, &parameters(&query))
                    .await
                    .map_err(pg_error)?;
                if parse_count(size.get(0))? > self.budget.max_bytes {
                    return Err(VerifyError::Invalid("FK key page exceeds byte cap"));
                }
                let ordering = key_order(&self.key, Some("c"))?;
                let page_sql =
                    format!("{cte} SELECT {selected}, ({violation}) FROM c ORDER BY {ordering}");
                let rows = tx
                    .query(&page_sql, &parameters(&query))
                    .await
                    .map_err(pg_error)?;
                if rows.is_empty() {
                    break;
                }
                let mut wire_bytes = 0u64;
                for row in &rows {
                    for i in 0..self.key.columns.len() {
                        let text: Option<&str> = row.get(i);
                        wire_bytes = wire_bytes
                            .checked_add(8 + text.map_or(0, |s| s.len() as u64))
                            .ok_or(VerifyError::Invalid("FK page size overflow"))?;
                    }
                    if row.get::<_, bool>(self.key.columns.len()) {
                        violations += 1;
                    }
                }
                if wire_bytes > self.budget.max_bytes {
                    return Err(VerifyError::Invalid("FK key page exceeds byte cap"));
                }
                let last = decode_key(rows.last().expect("nonempty page"), &self.key)?;
                if lo
                    .as_ref()
                    .map(|previous| self.order.compare_keys(previous, &last))
                    .transpose()?
                    .is_some_and(|v| v != Ordering::Less)
                {
                    return Err(VerifyError::Invalid("FK page did not advance"));
                }
                lo = Some(last);
            }
            if violations > 0 {
                findings.push(ForeignKeyFinding {
                    constraint: name,
                    violating_rows: violations,
                });
            }
        }
        tx.commit().await.map_err(pg_error)?;
        Ok(findings)
    }
}
