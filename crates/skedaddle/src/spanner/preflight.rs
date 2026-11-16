//! Read-only source requirements; successful emulator evidence stays Unverified.
use super::{
    client::{Client, Error},
    schema::{self, Metadata, SchemaSnapshot, text, yes},
};
use crate::{
    capability::{CapabilityItem, Criticality, Evidence, ItemStatus},
    definition::ObjectSelector,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    Stream,
    Coverage,
    CaptureType,
    TtlDeletes,
    TransactionExclusion,
    ModTypes,
    VirtualGenerated,
    VersionRetention,
    StreamRetention,
    Estimate,
}
#[derive(Clone, Debug)]
pub struct Issue {
    pub rule: Rule,
    pub blocking: bool,
    pub detail: String,
    pub remediation: &'static str,
}
#[derive(Clone, Debug)]
pub struct Report {
    pub schema: SchemaSnapshot,
    pub issues: Vec<Issue>,
    pub generated_for_verification: Vec<(String, String)>,
    pub copy_seconds: f64,
    pub version_margin_seconds: f64,
    pub stream_margin_seconds: f64,
}
#[derive(Clone, Debug)]
pub struct Size {
    pub rows: u64,
    pub bytes: u64,
}
#[derive(Clone, Debug)]
pub struct Options {
    /// Exact table-key -> operator size estimates. Live collection counts rows
    /// when absent, assuming estimated_row_bytes (default 1 KiB, not a bound).
    pub sizes: BTreeMap<String, Size>,
    pub estimated_row_bytes: u64,
    pub rows_per_second: f64,
    pub bytes_per_second: f64,
    pub catchup_seconds: f64,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            sizes: BTreeMap::new(),
            estimated_row_bytes: 1024,
            rows_per_second: 10_000.0,
            bytes_per_second: 10_000_000.0,
            catchup_seconds: 3600.0,
        }
    }
}
fn seconds(s: &str) -> Option<f64> {
    let s = s.trim_matches(['\'', '"']).trim();
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let value: f64 = s[..split].parse().ok()?;
    let unit = match s[split..].trim() {
        "s" => 1.,
        "m" => 60.,
        "h" => 3600.,
        "d" => 86400.,
        _ => return None,
    };
    let value = value * unit;
    (value.is_finite() && value > 0.).then_some(value)
}
impl Report {
    pub fn blockers(&self) -> impl Iterator<Item = &Issue> {
        self.issues.iter().filter(|i| i.blocking)
    }
    fn issue(
        &mut self,
        rule: Rule,
        blocking: bool,
        detail: impl Into<String>,
        remediation: &'static str,
    ) {
        self.issues.push(Issue {
            rule,
            blocking,
            detail: detail.into(),
            remediation,
        });
    }
    pub fn capability_items(&self) -> Vec<CapabilityItem> {
        let item = |id, status, criticality, remediation: String, description| CapabilityItem {
            id,
            object: None,
            status,
            criticality,
            sampled: false,
            remediation,
            evidence: vec![Evidence {
                source: "spanner-source-preflight".into(),
                description,
            }],
        };
        let mut items = vec![item("source.spanner.preflight".into(), ItemStatus::Unverified, Criticality::Critical,
            "Qualify production snapshot, CDC, schema mapping and generated-column recompute/verification.".into(),
            format!("{} blockers; copy {}s; version margin {}s; stream margin {}s; stored generated {:?}; digest {:?}",
                self.blockers().count(), self.copy_seconds, self.version_margin_seconds, self.stream_margin_seconds,
                self.generated_for_verification, self.schema.digest()))];
        items.extend(self.issues.iter().enumerate().map(|(n, i)| {
            item(
                format!("source.spanner.{:?}.{n}", i.rule),
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
                i.detail.clone(),
            )
        }));
        items
    }
}
/// Pure catalog validation; absence of default-valued option rows uses Spanner's
/// documented defaults (OLD_AND_NEW_VALUES, 1d stream, 1h version, false filters).
/// Model: max(total rows/rows_per_second, total bytes/bytes_per_second).
pub fn assess(
    metadata: &Metadata,
    selected: &[ObjectSelector],
    stream: &str,
    options: &Options,
) -> Result<Report, Error> {
    let snapshot = SchemaSnapshot::from_metadata(metadata, selected)?;
    let mut out = Report {
        schema: snapshot.clone(),
        issues: vec![],
        generated_for_verification: vec![],
        copy_seconds: 0.,
        version_margin_seconds: 0.,
        stream_margin_seconds: 0.,
    };
    let streams: Vec<_> = metadata
        .get("change_streams")
        .into_iter()
        .flatten()
        .filter(|r| text(r, "change_stream_name") == stream)
        .collect();
    let exists = streams.len() == 1;
    let stream_schema = streams
        .first()
        .map(|r| text(r, "change_stream_schema"))
        .unwrap_or("");
    let matching = |r: &&schema::Row| {
        text(r, "change_stream_name") == stream && text(r, "change_stream_schema") == stream_schema
    };
    let opts: BTreeMap<_, _> = metadata
        .get("change_stream_options")
        .into_iter()
        .flatten()
        .filter(matching)
        .map(|r| {
            (
                text(r, "option_name"),
                text(r, "option_value").trim_matches(['\'', '"']),
            )
        })
        .collect();
    let option = |name, default| opts.get(name).copied().unwrap_or(default);
    if !exists {
        out.issue(
            Rule::Stream,
            true,
            "Change stream missing or ambiguous",
            "Create the named stream with full metadata visibility.",
        );
    }
    if exists {
        for o in selected {
            let table = &snapshot.tables[&schema::key(&o.schema, &o.name)];
            let watched = metadata
                .get("change_stream_tables")
                .into_iter()
                .flatten()
                .filter(matching)
                .find(|r| text(r, "table_schema") == o.schema && text(r, "table_name") == o.name);
            let all = yes(text(streams[0], "all"))
                || watched.is_some_and(|r| yes(text(r, "all_columns")));
            let columns: std::collections::BTreeSet<_> = metadata
                .get("change_stream_columns")
                .into_iter()
                .flatten()
                .filter(matching)
                .filter(|r| text(r, "table_schema") == o.schema && text(r, "table_name") == o.name)
                .map(|r| text(r, "column_name"))
                .collect();
            // Primary keys are always tracked, even with an explicit column list.
            let keys: std::collections::BTreeSet<_> = table
                .index_columns
                .values()
                .filter(|r| text(r, "index_name") == "PRIMARY_KEY")
                .map(|r| text(r, "column_name"))
                .collect();
            if !all
                && (watched.is_none()
                    || table
                        .columns
                        .values()
                        .filter(|r| {
                            text(r, "is_generated") != "ALWAYS"
                                && !keys.contains(text(r, "column_name"))
                        })
                        .any(|r| !columns.contains(text(r, "column_name"))))
            {
                out.issue(
                    Rule::Coverage,
                    true,
                    format!("Incomplete stream coverage: {}.{}", o.schema, o.name),
                    "Watch every selected table and every non-generated column.",
                );
            }
        }
        match option("value_capture_type", "OLD_AND_NEW_VALUES") {
            "NEW_ROW_AND_OLD_VALUES" => {}
            "OLD_AND_NEW_VALUES" => out.issue(
                Rule::CaptureType,
                false,
                "OLD_AND_NEW_VALUES: every update must write full keys",
                "Enforce full-key updates or use NEW_ROW_AND_OLD_VALUES.",
            ),
            _ => out.issue(
                Rule::CaptureType,
                true,
                "Insufficient row images",
                "Use NEW_ROW_AND_OLD_VALUES.",
            ),
        }
        for (name, rule, remediation) in [
            (
                "exclude_ttl_deletes",
                Rule::TtlDeletes,
                "Include TTL deletes; the definition has no explicit TTL-gap acceptance flag.",
            ),
            (
                "allow_txn_exclusion",
                Rule::TransactionExclusion,
                "Disable transaction exclusion on the stream.",
            ),
            (
                "exclude_insert",
                Rule::ModTypes,
                "Include all insert/update/delete records.",
            ),
            (
                "exclude_update",
                Rule::ModTypes,
                "Include all insert/update/delete records.",
            ),
            (
                "exclude_delete",
                Rule::ModTypes,
                "Include all insert/update/delete records.",
            ),
        ] {
            let value = option(name, "false");
            if !value.eq_ignore_ascii_case("false") && value != "NO" {
                out.issue(rule, true, format!("{name}={value}"), remediation);
            }
        }
    }
    for (id, t) in &snapshot.tables {
        for r in t
            .columns
            .values()
            .filter(|r| text(r, "is_generated") == "ALWAYS")
        {
            if yes(text(r, "is_stored")) {
                out.generated_for_verification
                    .push((id.clone(), text(r, "column_name").into()));
            } else {
                out.issue(
                    Rule::VirtualGenerated,
                    true,
                    format!("Virtual generated column {id}.{}", text(r, "column_name")),
                    "Use a stored generated column and recompute/verify on destination.",
                );
            }
        }
    }
    let valid = !selected.is_empty()
        && options.rows_per_second.is_finite()
        && options.rows_per_second > 0.
        && options.bytes_per_second.is_finite()
        && options.bytes_per_second > 0.
        && options.catchup_seconds.is_finite()
        && options.catchup_seconds >= 0.;
    let mut rows = 0f64;
    let mut bytes = 0f64;
    let mut missing = false;
    for id in snapshot.tables.keys() {
        if let Some(size) = options.sizes.get(id) {
            rows += size.rows as f64;
            bytes += size.bytes as f64;
        } else {
            missing = true;
        }
    }
    if !valid || missing {
        out.issue(
            Rule::Estimate,
            true,
            "Missing sizes or invalid throughput/catch-up estimates",
            "Supply sizes for every selected table and finite positive throughput.",
        );
    } else {
        out.copy_seconds = (rows / options.rows_per_second).max(bytes / options.bytes_per_second);
    }
    let version = metadata
        .get("database_options")
        .into_iter()
        .flatten()
        .find(|r| text(r, "option_name") == "version_retention_period")
        .map(|r| text(r, "option_value"))
        .unwrap_or("1h");
    for (rule, retention, required) in [
        (Rule::VersionRetention, version, out.copy_seconds),
        (
            Rule::StreamRetention,
            option("retention_period", "1d"),
            out.copy_seconds + options.catchup_seconds,
        ),
    ] {
        let margin = seconds(retention).map(|s| s - required);
        if rule == Rule::VersionRetention {
            out.version_margin_seconds = margin.unwrap_or(f64::NEG_INFINITY);
        } else {
            out.stream_margin_seconds = margin.unwrap_or(f64::NEG_INFINITY);
        }
        if margin.is_none_or(|v| v < 0.) {
            out.issue(
                rule,
                true,
                format!("Retention {retention}; required {required}s"),
                "Increase retention or reduce the estimated copy/catch-up duration.",
            );
        }
    }
    Ok(out)
}
pub async fn inspect(
    client: &Client,
    selected: &[ObjectSelector],
    options: &Options,
) -> Result<Report, Error> {
    let metadata = schema::collect(client, true).await?;
    let (db, endpoint, _) = client.snapshot_connection();
    let quote = if endpoint.dialect == crate::definition::SpannerDialect::GoogleSql {
        '`'
    } else {
        '"'
    };
    let identifier = |s: &str| {
        format!(
            "{quote}{}{quote}",
            s.replace(quote, &format!("{quote}{quote}"))
        )
    };
    let mut options = options.clone();
    for o in selected {
        let id = schema::key(&o.schema, &o.name);
        if !options.sizes.contains_key(&id) {
            let table = if o.schema.is_empty() {
                identifier(&o.name)
            } else {
                format!("{}.{}", identifier(&o.schema), identifier(&o.name))
            };
            let mut result = db
                .single_use()
                .build()
                .execute_query(format!("SELECT COUNT(*) FROM {table}"))
                .await?;
            let count: i64 = result.next().await.ok_or("missing row count")??.get(0)?;
            let rows = u64::try_from(count)?;
            options.sizes.insert(
                id,
                Size {
                    rows,
                    bytes: rows
                        .checked_mul(options.estimated_row_bytes)
                        .ok_or("size estimate overflow")?,
                },
            );
        }
    }
    assess(&metadata, selected, &endpoint.change_stream, &options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::ObjectKind;
    fn row(fields: &[(&str, &str)]) -> schema::Row {
        fields
            .iter()
            .map(|(k, v)| ((*k).into(), Some((*v).into())))
            .collect()
    }
    fn fixture() -> (Metadata, Vec<ObjectSelector>, Options) {
        let selected = vec![ObjectSelector {
            schema: "".into(),
            name: "t".into(),
            kind: ObjectKind::Table,
            exclude_columns: vec![],
        }];
        let r = |fields: &[(&str, &str)]| {
            let mut r = row(&[("table_schema", ""), ("table_name", "t")]);
            r.extend(row(fields));
            r
        };
        let metadata = [
            ("tables".into(), vec![r(&[])]),
            (
                "columns".into(),
                vec![
                    r(&[
                        ("column_name", "id"),
                        ("spanner_type", "INT64"),
                        ("is_generated", "NEVER"),
                    ]),
                    r(&[
                        ("column_name", "v"),
                        ("spanner_type", "INT64"),
                        ("is_generated", "NEVER"),
                    ]),
                ],
            ),
            (
                "indexes".into(),
                vec![r(&[
                    ("index_name", "PRIMARY_KEY"),
                    ("index_type", "PRIMARY_KEY"),
                ])],
            ),
            (
                "index_columns".into(),
                vec![r(&[
                    ("index_name", "PRIMARY_KEY"),
                    ("column_name", "id"),
                    ("ordinal_position", "1"),
                ])],
            ),
            (
                "change_streams".into(),
                vec![row(&[
                    ("change_stream_name", "s"),
                    ("change_stream_schema", ""),
                    ("all", "TRUE"),
                ])],
            ),
            (
                "change_stream_options".into(),
                vec![row(&[
                    ("change_stream_name", "s"),
                    ("change_stream_schema", ""),
                    ("option_name", "value_capture_type"),
                    ("option_value", "NEW_ROW_AND_OLD_VALUES"),
                ])],
            ),
        ]
        .into();
        let options = Options {
            sizes: [(
                schema::key("", "t"),
                Size {
                    rows: 100,
                    bytes: 102400,
                },
            )]
            .into(),
            ..Default::default()
        };
        (metadata, selected, options)
    }
    #[test]
    fn catalog_checks_and_unverified_capabilities() {
        let (m, s, o) = fixture();
        let r = assess(&m, &s, "s", &o).unwrap();
        assert_eq!(r.blockers().count(), 0);
        assert!(r.version_margin_seconds > 0. && r.stream_margin_seconds > 0.);
        assert!(
            r.capability_items()
                .iter()
                .all(|i| i.status == ItemStatus::Unverified)
        );
        for (name, value, expected, blocking) in [
            ("exclude_ttl_deletes", "true", Rule::TtlDeletes, true),
            (
                "allow_txn_exclusion",
                "true",
                Rule::TransactionExclusion,
                true,
            ),
            ("exclude_insert", "true", Rule::ModTypes, true),
            ("exclude_update", "true", Rule::ModTypes, true),
            ("exclude_delete", "true", Rule::ModTypes, true),
            (
                "value_capture_type",
                "OLD_AND_NEW_VALUES",
                Rule::CaptureType,
                false,
            ),
            ("value_capture_type", "NEW_VALUES", Rule::CaptureType, true),
        ] {
            let mut m = m.clone();
            let opts = m.get_mut("change_stream_options").unwrap();
            opts.retain(|r| text(r, "option_name") != name);
            opts.push(row(&[
                ("change_stream_name", "s"),
                ("change_stream_schema", ""),
                ("option_name", name),
                ("option_value", value),
            ]));
            let r = assess(&m, &s, "s", &o).unwrap();
            assert_eq!(
                r.issues
                    .iter()
                    .map(|i| (i.rule, i.blocking))
                    .collect::<Vec<_>>(),
                [(expected, blocking)]
            );
        }
    }
    #[test]
    fn generated_coverage_and_retention() {
        let (mut m, s, mut o) = fixture();
        m.get_mut("change_streams").unwrap()[0].insert("all".into(), Some("FALSE".into()));
        assert_eq!(
            assess(&m, &s, "s", &o)
                .unwrap()
                .blockers()
                .next()
                .unwrap()
                .rule,
            Rule::Coverage
        );
        m.get_mut("change_streams").unwrap()[0].insert("all".into(), Some("YES".into()));
        let c = &mut m.get_mut("columns").unwrap()[0];
        c.insert("is_generated".into(), Some("ALWAYS".into()));
        c.insert("is_stored".into(), Some("NO".into()));
        assert_eq!(
            assess(&m, &s, "s", &o)
                .unwrap()
                .blockers()
                .next()
                .unwrap()
                .rule,
            Rule::VirtualGenerated
        );
        m.get_mut("columns").unwrap()[0].insert("is_stored".into(), Some("YES".into()));
        assert_eq!(
            assess(&m, &s, "s", &o)
                .unwrap()
                .generated_for_verification
                .len(),
            1
        );
        o.rows_per_second = 0.001;
        assert_eq!(
            assess(&m, &s, "s", &o)
                .unwrap()
                .blockers()
                .map(|i| i.rule)
                .collect::<Vec<_>>(),
            [Rule::VersionRetention, Rule::StreamRetention]
        );
        o.sizes.clear();
        assert!(
            assess(&m, &s, "s", &o)
                .unwrap()
                .blockers()
                .any(|i| i.rule == Rule::Estimate)
        );
    }
    #[test]
    fn canonical_schema_and_typed_drift() {
        let (mut m, s, _) = fixture();
        let a = SchemaSnapshot::from_metadata(&m, &s).unwrap();
        m.get_mut("tables")
            .unwrap()
            .push(row(&[("table_schema", ""), ("table_name", "unselected")]));
        assert_eq!(
            a.digest(),
            SchemaSnapshot::from_metadata(&m, &s).unwrap().digest()
        );
        m.get_mut("columns").unwrap()[0].insert("spanner_type".into(), Some("STRING(MAX)".into()));
        let b = SchemaSnapshot::from_metadata(&m, &s).unwrap();
        assert_eq!(schema::diff(&a, &b)[0].kind, schema::ChangeKind::Columns);
        assert_ne!(a.digest(), b.digest());
        assert_eq!(
            schema::diff(&a, &SchemaSnapshot::default())[0].kind,
            schema::ChangeKind::Removed
        );
    }
    #[test]
    fn missing_stream_and_explicit_column_coverage() {
        let (mut m, s, o) = fixture();
        assert_eq!(
            assess(&m, &s, "missing", &o)
                .unwrap()
                .blockers()
                .map(|i| i.rule)
                .collect::<Vec<_>>(),
            [Rule::Stream]
        );
        m.get_mut("change_streams").unwrap()[0].insert("all".into(), Some("NO".into()));
        let tracked = row(&[
            ("change_stream_schema", ""),
            ("change_stream_name", "s"),
            ("table_schema", ""),
            ("table_name", "t"),
            ("all_columns", "NO"),
        ]);
        m.insert("change_stream_tables".into(), vec![tracked.clone()]);
        let mut column = tracked;
        column.insert("column_name".into(), Some("v".into()));
        m.insert("change_stream_columns".into(), vec![column]);
        assert_eq!(assess(&m, &s, "s", &o).unwrap().blockers().count(), 0);
        m.get_mut("change_stream_columns").unwrap().clear();
        assert_eq!(
            assess(&m, &s, "s", &o)
                .unwrap()
                .blockers()
                .map(|i| i.rule)
                .collect::<Vec<_>>(),
            [Rule::Coverage]
        );
    }
}
