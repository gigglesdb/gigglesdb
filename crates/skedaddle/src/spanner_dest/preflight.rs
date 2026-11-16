//! Read-only destination inspection. Catalog compatibility never qualifies production.
use super::{
    apply::{self, Destination},
    limits::{self, Kind},
};
use crate::{
    capability::{CapabilityItem, Criticality, Evidence, ItemStatus},
    definition::{ObjectKind, ObjectSelector, SpannerDialect},
    mapping::{
        PgType,
        pg_spanner::{self, Dialect, Source},
    },
    pg::preflight::{Check, Snapshot},
};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::types::{Kind as PgKind, Type};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    SourceEvidence,
    MissingTable,
    PrimaryKey,
    ExtraForeignKey,
    ExtraUnique,
    ExtraCheck,
    Cascade,
    GeneratedWrite,
    ColumnType,
    FutureTimestamp,
    MutationLimit,
    ReplayShape,
}
#[derive(Clone, Debug)]
pub struct Issue {
    pub rule: Rule,
    pub table: String,
    pub detail: String,
    pub blocking: bool,
    pub remediation: &'static str,
}
#[derive(Clone, Debug, Default)]
pub struct Report {
    pub issues: Vec<Issue>,
    pub generated_for_verification: Vec<(String, String)>,
    pub commit_timestamp_columns: Vec<(String, String)>,
    pub estimated_mutations: Option<u64>,
}
impl Report {
    pub fn blockers(&self) -> impl Iterator<Item = &Issue> {
        self.issues.iter().filter(|i| i.blocking)
    }
    fn issue(
        &mut self,
        rule: Rule,
        table: &str,
        detail: impl Into<String>,
        blocking: bool,
        remediation: &'static str,
    ) {
        self.issues.push(Issue {
            rule,
            table: table.into(),
            detail: detail.into(),
            blocking,
            remediation,
        });
    }
    /// Coordinator integration seam; successful inspection remains Unverified.
    pub fn capability_items(&self) -> Vec<CapabilityItem> {
        let item =
            |id: String, status, criticality, remediation: String, description| CapabilityItem {
                id,
                object: None,
                status,
                sampled: false,
                criticality,
                remediation,
                evidence: vec![Evidence {
                    source: "spanner-destination-preflight".into(),
                    description,
                }],
            };
        let mut items = vec![item(
            "destination.spanner.preflight".into(),
            ItemStatus::Unverified,
            Criticality::Critical,
            "Qualify production apply, replay, cutover and verification before readiness.".into(),
            format!(
                "{} blockers; estimated mutations {:?}; stored generated columns {:?}; commit-timestamp columns {:?}. Source timestamps are preserved.",
                self.blockers().count(),
                self.estimated_mutations,
                self.generated_for_verification,
                self.commit_timestamp_columns
            ),
        )];
        items.extend(self.issues.iter().enumerate().map(|(n, i)| {
            item(
                format!("destination.spanner.{:?}.{n}", i.rule),
                if i.blocking {
                    ItemStatus::RequiresExternalChange
                } else {
                    ItemStatus::Unverified
                },
                if i.blocking {
                    Criticality::Critical
                } else {
                    Criticality::Advisory
                },
                i.remediation.into(),
                format!("{}: {}", i.table, i.detail),
            )
        }));
        items
    }
}
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// Same source server/client UTF8 readback contract as pg::catalog::build.
    pub source_utf8_verified: bool,
    /// PG preflight exposes no observed maximum transaction cardinality. Supply
    /// an upper bound across ALL selected tables, not a per-table average.
    pub worst_case_rows: Option<u64>,
    pub possible_future_timestamps: BTreeSet<(String, String)>,
    /// External invariant evidence (a sample is insufficient).
    pub no_future_timestamp_evidence: BTreeMap<(String, String), String>,
}
#[derive(Clone, Debug, Default)]
pub struct Column {
    pub sql_type: String,
    pub generated: bool,
    pub stored: bool,
    pub commit_timestamp: bool,
    pub nullable: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Constraint {
    Unique(Vec<String>),
    Check(String),
    ForeignKey {
        columns: Vec<String>,
        parent: String,
        referenced: Vec<String>,
        cascade: bool,
    },
}
#[derive(Clone, Debug, Default)]
pub struct Table {
    pub columns: BTreeMap<String, Column>,
    pub constraints: Vec<Constraint>,
    pub parent: Option<String>,
    pub cascade: bool,
    pub mutations: limits::Table,
}
pub type Catalog = BTreeMap<String, Table>;

fn rows<'a>(snapshot: &'a Snapshot, object: &ObjectSelector, check: Check) -> Option<&'a [Json]> {
    let mut found = snapshot.observations.iter().filter(|o| {
        o.check == check
            && o.object.as_ref().is_some_and(|s| {
                s.schema == object.schema && s.name == object.name && s.kind == object.kind
            })
    });
    let o = found.next()?;
    (o.error.is_none() && found.next().is_none()).then_some(o.rows.as_slice())
}
fn strings(value: &Json) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_owned))
        .collect()
}
fn pg_source(row: &Json) -> Option<Source> {
    fn builtin(t: &Type) -> Option<PgType> {
        Some(match *t {
            Type::INT2 => PgType::Int2,
            Type::INT4 => PgType::Int4,
            Type::INT8 => PgType::Int8,
            Type::FLOAT4 => PgType::Float4,
            Type::FLOAT8 => PgType::Float8,
            Type::BOOL => PgType::Bool,
            Type::BYTEA => PgType::Bytea,
            Type::NUMERIC => PgType::Numeric,
            Type::DATE => PgType::Date,
            Type::TIMESTAMPTZ => PgType::Timestamptz,
            Type::TIMESTAMP => PgType::Timestamp,
            Type::TEXT => PgType::Text {
                encoding: "UTF8".into(),
            },
            Type::VARCHAR => PgType::Varchar {
                encoding: "UTF8".into(),
            },
            Type::BPCHAR => PgType::Char {
                encoding: "UTF8".into(),
            },
            Type::UUID => PgType::Uuid,
            Type::JSON => PgType::Json,
            Type::JSONB => PgType::Jsonb,
            _ => {
                if let PgKind::Array(t) = t.kind() {
                    PgType::Array(Box::new(builtin(t)?))
                } else {
                    return None;
                }
            }
        })
    }
    let wire = Type::from_oid(row["oid"].as_u64()?.try_into().ok()?)?;
    if row["schema"] != "pg_catalog"
        || row["name"] != wire.name()
        || row["kind"] != "b"
        || row["base_oid"] != 0
        || row["element_oid"].as_u64()?
            != match wire.kind() {
                PgKind::Array(t) => u64::from(t.oid()),
                _ => 0,
            }
    {
        return None;
    }
    let pg_type = builtin(&wire)?;
    let scalar = if let PgType::Array(t) = &pg_type {
        t.as_ref()
    } else {
        &pg_type
    };
    let typmod = i32::try_from(row["typmod"].as_i64()?).ok()?;
    let length = if matches!(scalar, PgType::Varchar { .. } | PgType::Char { .. }) && typmod >= 4 {
        Some((typmod - 4) as usize)
    } else {
        None
    };
    let numeric = if *scalar == PgType::Numeric && typmod >= 4 {
        let t = typmod - 4;
        Some(((t >> 16) as u16, (((t & 0x7ff) ^ 1024) - 1024) as i16))
    } else {
        None
    };
    Some(Source {
        pg_type,
        length,
        numeric,
    })
}
// Remove insignificant whitespace only, preserving quoted identifiers/literals.
fn compact(sql: &str) -> String {
    let mut quote = None;
    sql.chars()
        .filter(|c| {
            if matches!(*c, '\'' | '"' | '`') {
                if quote == Some(*c) {
                    quote = None;
                } else if quote.is_none() {
                    quote = Some(*c);
                }
            }
            quote.is_some() || !c.is_whitespace()
        })
        .collect()
}
fn expression(sql: &str) -> String {
    let mut s = compact(sql);
    loop {
        if !s.starts_with('(') || !s.ends_with(')') {
            return s;
        }
        let (mut depth, mut quote, mut encloses) = (0i32, None, true);
        for (i, c) in s.char_indices() {
            if matches!(c, '\'' | '"' | '`') {
                if quote == Some(c) {
                    quote = None;
                } else if quote.is_none() {
                    quote = Some(c);
                }
            } else if quote.is_none() {
                if c == '(' {
                    depth += 1;
                } else if c == ')' {
                    depth -= 1;
                }
                if depth == 0 && i + 1 != s.len() {
                    encloses = false;
                    break;
                }
            }
        }
        if !encloses || depth != 0 {
            return s;
        }
        s = s[1..s.len() - 1].into();
    }
}
fn identifier(name: &str) -> String {
    if !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        name.into()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
fn fk_definition(columns: &[String], parent: &str, referenced: &[String], cascade: bool) -> String {
    let list = |v: &[String]| {
        v.iter()
            .map(|s| identifier(s))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "FOREIGN KEY ({}) REFERENCES {}({}){}",
        list(columns),
        parent,
        list(referenced),
        if cascade { " ON DELETE CASCADE" } else { "" }
    )
}
fn source_has_fk(
    definitions: &[String],
    schema: &str,
    columns: &[String],
    parent: &str,
    referenced: &[String],
    cascade: bool,
) -> bool {
    [
        identifier(parent),
        format!("{}.{}", identifier(schema), identifier(parent)),
    ]
    .iter()
    .any(|p| {
        let wanted = compact(&fk_definition(columns, p, referenced, cascade));
        definitions.iter().any(|d| compact(d) == wanted)
    })
}
fn replay_shape(t: &Table, dialect: SpannerDialect) -> bool {
    let string = if dialect == SpannerDialect::GoogleSql {
        "STRING(MAX)"
    } else {
        "character varying"
    };
    let short = if dialect == SpannerDialect::GoogleSql {
        "STRING(64)"
    } else {
        "character varying(64)"
    };
    let timestamp = if dialect == SpannerDialect::GoogleSql {
        "TIMESTAMP"
    } else {
        "timestamp with time zone"
    };
    let expected = [
        ("source_txn_id", short),
        ("source_txn_digest", short),
        ("source_identity", string),
        ("txn_identity", string),
        ("source_position", string),
        ("applied_at", timestamp),
    ];
    t.columns.len() == 6
        && t.mutations.primary_key == ["source_txn_id"]
        && t.parent.is_none()
        && t.constraints.is_empty()
        && t.mutations.indexes.is_empty()
        && expected.iter().all(|(n, ty)| {
            t.columns.get(*n).is_some_and(|c| {
                c.sql_type == *ty && !c.generated && !c.nullable && !c.commit_timestamp
            })
        })
        && t.mutations.default_columns.is_empty()
}
/// Selected objects map by exact table/column name to the destination's default
/// schema. Missing/failed source evidence is a blocker, never an empty schema.
pub fn assess(
    source: &Snapshot,
    selected: &[ObjectSelector],
    catalog: &Catalog,
    dialect: SpannerDialect,
    config: &limits::Config,
    options: &Options,
) -> Report {
    let mut out = Report::default();
    let mut max_row = 0;
    let mapping_dialect = if dialect == SpannerDialect::GoogleSql {
        Dialect::GoogleSql
    } else {
        Dialect::PostgreSql
    };
    let mut names = BTreeSet::new();
    for object in selected {
        let name = &object.name;
        let evidence = rows(source, object, Check::ColumnTypes)
            .zip(rows(source, object, Check::Key))
            .zip(rows(source, object, Check::SchemaDescriptor));
        let Some(((columns, keys), descriptors)) = evidence.filter(|((c, _), d)| {
            !c.is_empty()
                && d.len() == 1
                && d[0]
                    .get("constraints")
                    .is_some_and(|v| v.is_null() || v.is_array())
        }) else {
            out.issue(
                Rule::SourceEvidence,
                name,
                "Missing complete PG column/key/schema descriptor evidence",
                true,
                "Recollect PG preflight from one consistent snapshot.",
            );
            continue;
        };
        if object.kind != ObjectKind::Table
            || !names.insert(name)
            || name.eq_ignore_ascii_case(&config.replay_table)
        {
            out.issue(
                Rule::SourceEvidence,
                name,
                "Non-table, duplicate destination name or reserved replay table",
                true,
                "Select uniquely mapped tables and reserve the configured replay table.",
            );
            continue;
        }
        let Some(table) = catalog.get(name) else {
            out.issue(
                Rule::MissingTable,
                name,
                "Destination table is absent",
                true,
                "Create the mapped destination table before copy.",
            );
            continue;
        };
        let primary = keys
            .iter()
            .find(|k| k["primary"] == true && k["valid"] == true && k["ready"] == true)
            .and_then(|k| strings(&k["columns"]));
        if primary
            .as_ref()
            .is_none_or(|key| key.is_empty() || *key != table.mutations.primary_key)
            || table
                .mutations
                .primary_key
                .iter()
                .any(|k| object.exclude_columns.contains(k))
        {
            out.issue(
                Rule::PrimaryKey,
                name,
                "Primary-key columns/order differ or source key is absent/excluded",
                true,
                "Create a destination primary key with the source columns, order and mapped types.",
            );
        }
        let definitions: Vec<_> = descriptors[0]["constraints"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| c[3].as_str().map(str::to_owned))
            .collect();
        for constraint in &table.constraints {
            let (rule, matched) = match constraint {
                Constraint::Unique(cols) => (
                    Rule::ExtraUnique,
                    keys.iter().any(|k| {
                        k["unique"] == true
                            && k["valid"] == true
                            && k["ready"] == true
                            && k["partial"] == false
                            && k["expression"] == false
                            && strings(&k["columns"]).as_ref() == Some(cols)
                    }),
                ),
                Constraint::Check(expr) => (
                    Rule::ExtraCheck,
                    definitions.iter().any(|d| {
                        d.strip_prefix("CHECK ")
                            .is_some_and(|e| expression(e) == expression(expr))
                    }),
                ),
                Constraint::ForeignKey {
                    columns,
                    parent,
                    referenced,
                    cascade,
                } => (
                    Rule::ExtraForeignKey,
                    source_has_fk(
                        &definitions,
                        &object.schema,
                        columns,
                        parent,
                        referenced,
                        *cascade,
                    ),
                ),
            };
            if !matched {
                out.issue(rule, name, format!("Destination-only constraint {constraint:?}"), true,
                "Remove the extra destination constraint or qualify an equivalent source constraint; names alone are not evidence.");
            }
        }
        for (child_name, _) in catalog
            .iter()
            .filter(|(_, t)| t.cascade && t.parent.as_ref() == Some(name))
        {
            let child_object = selected.iter().find(|o| &o.name == child_name);
            let safe = child_object
                .and_then(|o| rows(source, o, Check::SchemaDescriptor))
                .filter(|d| d.len() == 1)
                .is_some_and(|d| {
                    let defs: Vec<_> = d[0]["constraints"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|c| c[3].as_str().map(str::to_owned))
                        .collect();
                    source_has_fk(
                        &defs,
                        &object.schema,
                        &table.mutations.primary_key,
                        name,
                        &table.mutations.primary_key,
                        true,
                    )
                });
            if !safe {
                out.issue(Rule::Cascade, name, format!("Interleaved child {child_name} could be deleted without source deletes"), true,
                "Remove ON DELETE CASCADE or select the child and prove the source has the same cascading relationship.");
            }
        }
        let mut written = BTreeSet::new();
        for column in columns.iter().filter(|c| {
            c["column"]
                .as_str()
                .is_none_or(|n| !object.exclude_columns.iter().any(|e| e == n))
        }) {
            let Some(n) = column["column"].as_str() else {
                out.issue(
                    Rule::SourceEvidence,
                    name,
                    "Missing column name",
                    true,
                    "Recollect PG column metadata.",
                );
                continue;
            };
            written.insert(n.into());
            let actual = table.columns.get(n);
            if actual.is_some_and(|c| c.generated) {
                out.issue(Rule::GeneratedWrite, name, n, true, "Omit the generated column from the source projection and verify its recomputed values.");
            }
            let expected =
                pg_source(column).and_then(|s| pg_spanner::destination(&s, mapping_dialect).ok());
            if !options.source_utf8_verified
                && [25, 1042, 1043, 1009, 1014, 1015].contains(&column["oid"].as_u64().unwrap_or(0))
            {
                out.issue(Rule::SourceEvidence, name, format!("{n}: source encoding is unverified"), true,
                    "Read back both source server_encoding and client_encoding as UTF8 before qualifying text mappings.");
            }
            let normalize = |s: &str| {
                s.to_ascii_lowercase()
                    .replace("character varying", "varchar")
                    .replace("timestamp with time zone", "timestamptz")
            };
            if expected.as_ref().zip(actual).is_none_or(|(e, a)| {
                normalize(&e.sql) != normalize(&a.sql_type)
                    && !(a.commit_timestamp
                        && e.sql == "timestamptz"
                        && a.sql_type == "spanner.commit_timestamp")
            }) {
                out.issue(Rule::ColumnType, name, format!("{n}: mapped {:?}, actual {:?}", expected.map(|e| e.sql), actual.map(|c| &c.sql_type)), true,
                    "Use the pg_spanner mapped type, or transform/exclude the unsupported source column explicitly.");
            }
            if actual.is_some_and(|c| c.commit_timestamp) {
                let key = (name.clone(), n.into());
                if options.possible_future_timestamps.contains(&key) {
                    out.issue(Rule::FutureTimestamp, name, n, true, "Remove allow_commit_timestamp or enforce a source invariant excluding future timestamps; source values are preserved.");
                } else if options
                    .no_future_timestamp_evidence
                    .get(&key)
                    .is_none_or(|e| e.trim().is_empty())
                {
                    out.issue(Rule::FutureTimestamp, name, format!("{n}: future timestamp domain is unverified"), false,
                        "Prove a source invariant excluding future timestamps or remove allow_commit_timestamp; samples do not prove future safety.");
                }
            }
        }
        for (n, c) in &table.columns {
            if c.generated && c.stored {
                out.generated_for_verification
                    .push((name.clone(), n.clone()));
            }
            if c.commit_timestamp {
                out.commit_timestamp_columns.push((name.clone(), n.clone()));
            }
        }
        max_row = max_row.max(
            [Kind::Insert, Kind::Update, Kind::Delete]
                .into_iter()
                .map(|k| limits::cells(&table.mutations, k, &written))
                .max()
                .unwrap_or(0),
        );
        if table
            .mutations
            .indexes
            .values()
            .any(|i| !i.is_disjoint(&table.mutations.array_columns))
        {
            out.issue(Rule::MutationLimit, name, "Array-index cardinality is not bounded by row count", true,
                "Remove the array index or qualify a conservative per-value cardinality bound before apply.");
        }
    }
    if apply::replay_ddl(config, dialect).is_err()
        || catalog
            .get(&config.replay_table)
            .is_some_and(|t| !replay_shape(t, dialect))
    {
        out.issue(Rule::ReplayShape, &config.replay_table, "Invalid replay name or existing table is not Skedaddle-shaped", true,
            "Choose a free valid replay-table name or restore the exact Skedaddle replay schema; never alter a user table automatically.");
    }
    if let Some(rows) = options.worst_case_rows {
        let receipt = catalog
            .get(&config.replay_table)
            .map_or(limits::REPLAY_CELLS, |t| {
                limits::cells(&t.mutations, Kind::Insert, &t.mutations.columns)
            });
        let estimate = max_row.saturating_mul(rows).saturating_add(receipt);
        out.estimated_mutations = Some(estimate);
        if limits::admit(config, [estimate]).is_err() {
            out.issue(Rule::MutationLimit, "<transaction>",
            format!("{estimate} estimated mutations exceeds {}", config.mutation_limit), true,
            "Reduce the largest source transaction or index fanout; configure a proven row bound and documented mutation limit. Do not split atomic source transactions.");
        }
    } else {
        out.issue(Rule::MutationLimit, "<transaction>", "No maximum source transaction row count is available", false,
        "Configure a conservative worst-case transaction row count; runtime admission remains mandatory.");
    }
    out
}

/// Live catalog queries require complete INFORMATION_SCHEMA visibility.
/// Query/decode errors are returned, never converted to a passing empty catalog.
pub async fn collect(destination: &Destination) -> apply::Result<Catalog> {
    let schema = if destination.dialect == SpannerDialect::GoogleSql {
        ""
    } else {
        "public"
    };
    let mutations = destination.catalog().await?;
    let mut catalog: Catalog = mutations
        .into_iter()
        .map(|(n, mutations)| {
            (
                n,
                Table {
                    mutations,
                    ..Default::default()
                },
            )
        })
        .collect();
    let mut rows = destination.database.single_use().build().execute_query(format!(
        "SELECT TABLE_NAME,COLUMN_NAME,SPANNER_TYPE,IS_GENERATED,IS_STORED,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}'")).await?;
    while let Some(row) = rows.next().await {
        let row = row?;
        let get = |i| row.get::<String>(i).map_err(|_| apply::Error::Database);
        let stored: Option<String> = row.get(4).map_err(|_| apply::Error::Database)?;
        let ty = get(2)?;
        catalog.entry(get(0)?).or_default().columns.insert(
            get(1)?,
            Column {
                commit_timestamp: ty == "spanner.commit_timestamp",
                sql_type: ty,
                generated: get(3)? == "ALWAYS",
                stored: stored.as_deref() == Some("YES"),
                nullable: get(5)? == "YES",
            },
        );
    }
    let mut rows = destination.database.single_use().build().execute_query(format!(
        "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE TABLE_SCHEMA='{schema}' AND OPTION_NAME='allow_commit_timestamp' AND OPTION_VALUE='TRUE'")).await?;
    while let Some(row) = rows.next().await {
        let row = row?;
        let t: String = row.get(0).map_err(|_| apply::Error::Database)?;
        let c: String = row.get(1).map_err(|_| apply::Error::Database)?;
        if let Some(c) = catalog.get_mut(&t).and_then(|t| t.columns.get_mut(&c)) {
            c.commit_timestamp = true;
        }
    }
    let mut rows = destination.database.single_use().build().execute_query(format!(
        "SELECT TABLE_NAME,PARENT_TABLE_NAME,ON_DELETE_ACTION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}'")).await?;
    while let Some(row) = rows.next().await {
        let row = row?;
        let name: String = row.get(0).map_err(|_| apply::Error::Database)?;
        let parent: Option<String> = row.get(1).map_err(|_| apply::Error::Database)?;
        let action: Option<String> = row.get(2).map_err(|_| apply::Error::Database)?;
        if let Some(t) = catalog.get_mut(&name) {
            t.parent = parent;
            t.cascade = action.as_deref() == Some("CASCADE");
        }
    }
    // Explicit UNIQUE indexes are constraints too; hidden FK indexes are not.
    let unique_filter = if destination.dialect == SpannerDialect::GoogleSql {
        "i.IS_UNIQUE=TRUE AND i.SPANNER_IS_MANAGED=FALSE"
    } else {
        "i.IS_UNIQUE='YES' AND i.SPANNER_IS_MANAGED='NO'"
    };
    let mut rows = destination.database.single_use().build().execute_query(format!(
        "SELECT i.TABLE_NAME,i.INDEX_NAME,c.COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES i JOIN INFORMATION_SCHEMA.INDEX_COLUMNS c ON i.TABLE_SCHEMA=c.TABLE_SCHEMA AND i.TABLE_NAME=c.TABLE_NAME AND i.INDEX_NAME=c.INDEX_NAME WHERE i.TABLE_SCHEMA='{schema}' AND {unique_filter} AND i.INDEX_TYPE<>'PRIMARY_KEY' AND c.ORDINAL_POSITION IS NOT NULL ORDER BY i.TABLE_NAME,i.INDEX_NAME,c.ORDINAL_POSITION")).await?;
    let mut unique: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    while let Some(row) = rows.next().await {
        let r = row?;
        unique
            .entry((
                r.get(0).map_err(|_| apply::Error::Database)?,
                r.get(1).map_err(|_| apply::Error::Database)?,
            ))
            .or_default()
            .push(r.get(2).map_err(|_| apply::Error::Database)?);
    }
    for ((t, _), columns) in unique {
        catalog
            .entry(t)
            .or_default()
            .constraints
            .push(Constraint::Unique(columns));
    }
    let mut rows = destination.database.single_use().build().execute_query(format!(
        "SELECT t.TABLE_NAME,c.CHECK_CLAUSE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS c ON t.CONSTRAINT_SCHEMA=c.CONSTRAINT_SCHEMA AND t.CONSTRAINT_NAME=c.CONSTRAINT_NAME WHERE t.TABLE_SCHEMA='{schema}' AND t.CONSTRAINT_TYPE='CHECK'")).await?;
    while let Some(row) = rows.next().await {
        let r = row?;
        let t: String = r.get(0).map_err(|_| apply::Error::Database)?;
        let expr: String = r.get(1).map_err(|_| apply::Error::Database)?;
        // NOT NULL pseudo-checks are accounted for by column nullability.
        let is_not_null = catalog.get(&t).is_some_and(|table| {
            table.columns.iter().any(|(n, c)| {
                !c.nullable
                    && [
                        format!("{} IS NOT NULL", identifier(n)),
                        format!("`{n}` IS NOT NULL"),
                    ]
                    .iter()
                    .any(|e| expression(&expr) == expression(e))
            })
        });
        if !is_not_null {
            catalog
                .entry(t)
                .or_default()
                .constraints
                .push(Constraint::Check(expr));
        }
    }
    let mut rows = destination.database.single_use().build().execute_query(format!(
        "SELECT k.TABLE_NAME,k.CONSTRAINT_NAME,k.COLUMN_NAME,p.TABLE_NAME,p.COLUMN_NAME,r.DELETE_RULE FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE p ON r.UNIQUE_CONSTRAINT_SCHEMA=p.CONSTRAINT_SCHEMA AND r.UNIQUE_CONSTRAINT_NAME=p.CONSTRAINT_NAME AND k.POSITION_IN_UNIQUE_CONSTRAINT=p.ORDINAL_POSITION WHERE k.TABLE_SCHEMA='{schema}' ORDER BY k.TABLE_NAME,k.CONSTRAINT_NAME,k.ORDINAL_POSITION")).await?;
    type ForeignKeyIdentity = (String, String, String, bool);
    type ForeignKeyColumns = (Vec<String>, Vec<String>);
    let mut fks: BTreeMap<ForeignKeyIdentity, ForeignKeyColumns> = BTreeMap::new();
    while let Some(row) = rows.next().await {
        let r = row?;
        let get = |i| r.get::<String>(i).map_err(|_| apply::Error::Database);
        let entry = fks
            .entry((get(0)?, get(1)?, get(3)?, get(5)? == "CASCADE"))
            .or_default();
        entry.0.push(get(2)?);
        entry.1.push(get(4)?);
    }
    for ((t, _, parent, cascade), (columns, referenced)) in fks {
        catalog
            .entry(t)
            .or_default()
            .constraints
            .push(Constraint::ForeignKey {
                columns,
                parent,
                referenced,
                cascade,
            });
    }
    Ok(catalog)
}
pub async fn inspect(
    source: &Snapshot,
    selected: &[ObjectSelector],
    destination: &Destination,
    options: &Options,
) -> apply::Result<Report> {
    Ok(assess(
        source,
        selected,
        &collect(destination).await?,
        destination.dialect,
        &destination.config,
        options,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::preflight::Observation;
    use serde_json::json;
    fn fixture() -> (Snapshot, Vec<ObjectSelector>, Catalog, Options) {
        let object = ObjectSelector {
            schema: "public".into(),
            name: "items".into(),
            kind: ObjectKind::Table,
            exclude_columns: vec![],
        };
        let column = json!({"column":"id","oid":20,"schema":"pg_catalog","name":"int8","kind":"b","base_oid":0,"element_oid":0,"typmod":-1});
        let obs = |check, rows| Observation {
            check,
            object: Some(object.clone()),
            sql: "constructed descriptor".into(),
            rows,
            error: None,
        };
        let snapshot = Snapshot {
            observations: vec![
                obs(Check::ColumnTypes, vec![column]),
                obs(
                    Check::Key,
                    vec![
                        json!({"primary":true,"unique":true,"valid":true,"ready":true,"partial":false,"expression":false,"columns":["id"]}),
                    ],
                ),
                obs(Check::SchemaDescriptor, vec![json!({"constraints":null})]),
            ],
        };
        let table = Table {
            columns: [(
                "id".into(),
                Column {
                    sql_type: "INT64".into(),
                    ..Default::default()
                },
            )]
            .into(),
            mutations: limits::Table {
                primary_key: vec!["id".into()],
                columns: ["id".into()].into(),
                ..Default::default()
            },
            ..Default::default()
        };
        (
            snapshot,
            vec![object],
            [("items".into(), table)].into(),
            Options {
                worst_case_rows: Some(1),
                ..Default::default()
            },
        )
    }
    fn run(s: &Snapshot, o: &[ObjectSelector], c: &Catalog, options: &Options) -> Report {
        assess(
            s,
            o,
            c,
            SpannerDialect::GoogleSql,
            &limits::Config::default(),
            options,
        )
    }
    #[test]
    fn compliant_missing_key_and_type_rules() {
        let (s, o, mut c, opts) = fixture();
        let report = run(&s, &o, &c, &opts);
        assert_eq!(report.blockers().count(), 0);
        assert_eq!(report.estimated_mutations, Some(7));
        assert_eq!(report.capability_items()[0].status, ItemStatus::Unverified);
        c.get_mut("items").unwrap().mutations.primary_key = vec!["other".into()];
        assert_eq!(
            run(&s, &o, &c, &opts).blockers().next().unwrap().rule,
            Rule::PrimaryKey
        );
        c.get_mut("items")
            .unwrap()
            .columns
            .get_mut("id")
            .unwrap()
            .sql_type = "STRING(MAX)".into();
        assert!(
            run(&s, &o, &c, &opts)
                .blockers()
                .any(|i| i.rule == Rule::ColumnType)
        );
        c.clear();
        assert_eq!(
            run(&s, &o, &c, &opts).blockers().next().unwrap().rule,
            Rule::MissingTable
        );
        assert_eq!(
            run(&Snapshot::default(), &o, &c, &opts)
                .blockers()
                .next()
                .unwrap()
                .rule,
            Rule::SourceEvidence
        );
    }
    #[test]
    fn constraints_are_semantic_and_cascade_is_not_transaction_order() {
        let (mut s, o, mut c, opts) = fixture();
        let fk = Constraint::ForeignKey {
            columns: vec!["id".into()],
            parent: "parent".into(),
            referenced: vec!["id".into()],
            cascade: false,
        };
        for (constraint, rule) in [
            (fk.clone(), Rule::ExtraForeignKey),
            (Constraint::Unique(vec!["other".into()]), Rule::ExtraUnique),
            (Constraint::Check("id>0".into()), Rule::ExtraCheck),
        ] {
            c.get_mut("items").unwrap().constraints = vec![constraint];
            let report = run(&s, &o, &c, &opts);
            assert_eq!(
                report.blockers().map(|i| i.rule).collect::<Vec<_>>(),
                [rule]
            );
            assert_eq!(
                report.capability_items()[1].criticality,
                Criticality::Critical
            );
        }
        s.observations[2].rows[0]["constraints"] = json!([
            ["fk", "f", true, "FOREIGN KEY (id) REFERENCES parent(id)"],
            ["ck", "c", true, "CHECK (id>0)"]
        ]);
        c.get_mut("items").unwrap().constraints = vec![
            fk,
            Constraint::Unique(vec!["id".into()]),
            Constraint::Check("id>0".into()),
        ];
        assert_eq!(run(&s, &o, &c, &opts).blockers().count(), 0);
        c.insert(
            "child".into(),
            Table {
                parent: Some("items".into()),
                ..Default::default()
            },
        );
        assert_eq!(run(&s, &o, &c, &opts).blockers().count(), 0); // parent may arrive later in the SAME transaction
        c.get_mut("child").unwrap().cascade = true;
        assert_eq!(
            run(&s, &o, &c, &opts).blockers().next().unwrap().rule,
            Rule::Cascade
        );
    }
    #[test]
    fn generated_and_commit_timestamp_domains() {
        let (mut s, mut o, mut c, mut opts) = fixture();
        let col = c.get_mut("items").unwrap().columns.get_mut("id").unwrap();
        col.generated = true;
        col.stored = true;
        let report = run(&s, &o, &c, &opts);
        assert_eq!(report.blockers().next().unwrap().rule, Rule::GeneratedWrite);
        assert_eq!(
            report.generated_for_verification,
            [("items".into(), "id".into())]
        );
        o[0].exclude_columns = vec!["id".into()];
        assert!(
            !run(&s, &o, &c, &opts)
                .blockers()
                .any(|i| i.rule == Rule::GeneratedWrite)
        );
        o[0].exclude_columns.clear();
        s.observations[0].rows[0]["oid"] = json!(1184);
        s.observations[0].rows[0]["name"] = json!("timestamptz");
        let col = c.get_mut("items").unwrap().columns.get_mut("id").unwrap();
        col.generated = false;
        col.sql_type = "TIMESTAMP".into();
        col.commit_timestamp = true;
        let report = run(&s, &o, &c, &opts);
        assert_eq!(report.blockers().count(), 0);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.rule == Rule::FutureTimestamp && !i.blocking)
        );
        assert_eq!(report.commit_timestamp_columns.len(), 1);
        opts.possible_future_timestamps
            .insert(("items".into(), "id".into()));
        assert_eq!(
            run(&s, &o, &c, &opts).blockers().next().unwrap().rule,
            Rule::FutureTimestamp
        );
    }
    #[test]
    fn unsupported_types_typmods_and_mutation_bound() {
        let (mut s, o, c, mut opts) = fixture();
        opts.worst_case_rows = Some(u64::MAX);
        assert_eq!(
            run(&s, &o, &c, &opts).blockers().next().unwrap().rule,
            Rule::MutationLimit
        );
        s.observations[0].rows[0]["oid"] = json!(1114);
        s.observations[0].rows[0]["name"] = json!("timestamp");
        assert!(
            run(&s, &o, &c, &opts)
                .blockers()
                .any(|i| i.rule == Rule::ColumnType)
        );
        let numeric = json!({"oid":1231,"schema":"pg_catalog","name":"_numeric","kind":"b","base_oid":0,"element_oid":1700,"typmod":((5<<16)|2)+4});
        assert_eq!(pg_source(&numeric).unwrap().numeric, Some((5, 2)));
        assert!(pg_spanner::destination(&pg_source(&numeric).unwrap(), Dialect::GoogleSql).is_ok());
    }
    #[test]
    fn replay_shape_and_configured_name() {
        let (s, o, mut c, opts) = fixture();
        let mut config = limits::Config {
            replay_table: "CustomReplay".into(),
            ..Default::default()
        };
        let mut table = Table {
            mutations: limits::Table {
                primary_key: vec!["source_txn_id".into()],
                ..Default::default()
            },
            ..Default::default()
        };
        for (n, t) in [
            ("source_txn_id", "STRING(64)"),
            ("source_txn_digest", "STRING(64)"),
            ("source_identity", "STRING(MAX)"),
            ("txn_identity", "STRING(MAX)"),
            ("source_position", "STRING(MAX)"),
            ("applied_at", "TIMESTAMP"),
        ] {
            table.columns.insert(
                n.into(),
                Column {
                    sql_type: t.into(),
                    ..Default::default()
                },
            );
        }
        c.insert(config.replay_table.clone(), table);
        assert_eq!(
            assess(&s, &o, &c, SpannerDialect::GoogleSql, &config, &opts)
                .blockers()
                .count(),
            0
        );
        c.get_mut(&config.replay_table)
            .unwrap()
            .columns
            .get_mut("applied_at")
            .unwrap()
            .nullable = true;
        assert_eq!(
            assess(&s, &o, &c, SpannerDialect::GoogleSql, &config, &opts)
                .blockers()
                .next()
                .unwrap()
                .rule,
            Rule::ReplayShape
        );
        config.replay_table = "invalid.name".into();
        assert!(
            assess(&s, &o, &c, SpannerDialect::GoogleSql, &config, &opts)
                .blockers()
                .any(|i| i.rule == Rule::ReplayShape)
        );
        assert_ne!(compact("CHECK (v='a b')"), compact("CHECK (v='ab')"));
        assert_eq!(expression("((id > 0))"), expression("id > 0"));
        assert_ne!(expression("(id > 0) OR (id < 0)"), expression("id > 0"));
    }
}
