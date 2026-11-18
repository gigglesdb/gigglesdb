//! Deterministic, value-free policy projections and changes.
use super::{
    PolicyDocument, RouteDefault, Treatment,
    bindings::{RouteBindings, TypeFamily},
    effective::{self, Effective, Origin},
};
use crate::schema::{ColumnRef, Snapshot};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Public value-free treatment metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct View {
    /// Treatment name.
    pub treatment: &'static str,
    /// Selection origin.
    pub origin: &'static str,
    /// Token domain name, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}
fn origin(o: Origin) -> &'static str {
    match o {
        Origin::Linked => "linked",
        Origin::NeedsRepair => "needs_repair",
        Origin::Unbound => "unbound",
        Origin::Explicit => "explicit",
        Origin::RouteDefault => "route_default",
        Origin::NoCodec => "no_codec",
        Origin::JsonNoPaths => "json_no_paths",
        Origin::JsonPaths => "json_paths",
        Origin::JsonPathsUnsupportedType => "json_paths_unsupported_type",
    }
}
/// Project every captured column without placeholders or values.
pub fn views(
    doc: &PolicyDocument,
    snapshot: &Snapshot,
    bindings: &RouteBindings,
    draft: bool,
) -> BTreeMap<ColumnRef, View> {
    effective::effective_all(doc, snapshot, bindings)
        .into_iter()
        .map(|(c, (e, o))| {
            let (t, d) = match e {
                Effective::Show => ("show", None),
                Effective::Masked { .. } => (
                    if matches!(o, Origin::Explicit) {
                        "mask"
                    } else {
                        "withheld"
                    },
                    None,
                ),
                Effective::Blind { domain }
                | Effective::BlindLinked { domain }
                | Effective::BlindPrivate { domain } => ("tokenize", Some(domain.0)),
            };
            let mut v = View {
                treatment: t,
                origin: origin(o),
                domain: d,
            };
            if draft && o != Origin::NeedsRepair {
                if let Some(link) = doc.links.iter().find(|l| l.fields.contains(&c)) {
                    let retained = link
                        .fields
                        .iter()
                        .map(|field| snapshot.column_id(field).and_then(|id| bindings.get(id)))
                        .collect::<Option<Vec<_>>>()
                        .filter(|records| {
                            records.iter().all(|r| {
                                r.linked && r.status == super::bindings::BindingStatus::Active
                            })
                        })
                        .and_then(|records| {
                            records
                                .first()
                                .and_then(|first| first.domain.as_ref())
                                .filter(|domain| {
                                    records.iter().all(|r| r.domain.as_ref() == Some(*domain))
                                })
                                .cloned()
                        });
                    v = View {
                        treatment: "tokenize",
                        origin: origin(o),
                        domain: Some(
                            link.domain
                                .as_ref()
                                .or(retained.as_ref())
                                .map(|d| d.0.clone())
                                .unwrap_or_else(|| "new".into()),
                        ),
                    };
                } else if o == Origin::Unbound
                    && doc.route_default == RouteDefault::Tokenize
                    && !doc.json_paths.iter().any(|r| r.column == c)
                {
                    match doc.policy.columns.get(&c) {
                        Some(Treatment::Show) => {
                            v = View {
                                treatment: "show",
                                origin: origin(o),
                                domain: None,
                            }
                        }
                        None if snapshot
                            .column(&c)
                            .is_some_and(|col| TypeFamily::of(&col.ty).is_some()) =>
                        {
                            v = View {
                                treatment: "tokenize",
                                origin: origin(o),
                                domain: Some("new".into()),
                            }
                        }
                        Some(Treatment::Masked { .. }) | Some(Treatment::Blind { .. }) | None => {}
                    }
                }
            }
            (c, v)
        })
        .collect()
}
/// Counts suitable for audit records.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Summary {
    /// Newly visible columns.
    pub newly_visible: usize,
    /// Newly masked columns.
    pub newly_masked: usize,
    /// Newly tokenized columns.
    pub newly_tokenized: usize,
    /// Changed token domains.
    pub domain_changes: usize,
    /// Added links.
    pub links_added: usize,
    /// Removed links.
    pub links_removed: usize,
    /// Unrepaired columns.
    pub repairs_needed: usize,
}
/// Value-free deterministic policy change.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PolicyDiff {
    /// Columns becoming visible.
    pub newly_visible: Vec<Value>,
    /// Columns becoming masked or withheld.
    pub newly_masked: Vec<Value>,
    /// Columns becoming tokenized.
    pub newly_tokenized: Vec<Value>,
    /// Columns changing token domain.
    pub domain_changes: Vec<Value>,
    /// Canonical added link sets.
    pub links_added: Vec<Value>,
    /// Canonical removed link sets.
    pub links_removed: Vec<Value>,
    /// Columns requiring repair.
    pub repairs_needed: Vec<Value>,
    /// Route mode change.
    pub mode_change: Option<Value>,
    /// Change counts.
    pub summary: Summary,
}
fn links(doc: &PolicyDocument) -> BTreeMap<String, Value> {
    doc.links
        .iter()
        .map(|l| {
            let mut fields = l.fields.clone();
            fields.sort();
            fields.dedup();
            let v = json!({"fields":fields,"domain":l.domain.as_ref().map(|d|&d.0)});
            (v.to_string(), v)
        })
        .collect()
}
fn mode(m: RouteDefault) -> &'static str {
    match m {
        RouteDefault::Show => "show",
        RouteDefault::Tokenize => "tokenize",
    }
}
/// Compare active treatments with a draft, without assigning persisted bindings.
pub fn diff(
    before: &PolicyDocument,
    after: &PolicyDocument,
    snapshot: &Snapshot,
    bindings: &RouteBindings,
) -> PolicyDiff {
    let a = views(before, snapshot, bindings, false);
    let mut b = views(after, snapshot, bindings, true);
    // The compiler intentionally honors old link bindings until publication.
    // Project only changed memberships here; this never changes persisted records.
    for (column, view) in &mut b {
        let unchanged = before.policy.columns.get(column) == after.policy.columns.get(column)
            && (before.route_default == after.route_default
                || before.policy.columns.contains_key(column))
            && before
                .json_paths
                .iter()
                .filter(|r| &r.column == column)
                .eq(after.json_paths.iter().filter(|r| &r.column == column))
            && before
                .links
                .iter()
                .filter(|l| l.fields.contains(column))
                .eq(after.links.iter().filter(|l| l.fields.contains(column)));
        if unchanged {
            if let Some(old) = a.get(column) {
                *view = old.clone();
            }
            continue;
        }
        let old_link = before.links.iter().find(|l| l.fields.contains(column));
        let new_link = after.links.iter().find(|l| l.fields.contains(column));
        if old_link != new_link && view.origin != "needs_repair" {
            match new_link {
                Some(link) if link.domain.is_none() => view.domain = Some("new".into()),
                Some(_) => {}
                None if old_link.is_some() => {
                    if let Some(projected) =
                        views(after, snapshot, &RouteBindings::default(), true).get(column)
                    {
                        *view = projected.clone();
                    }
                }
                None => {}
            }
        }
    }
    let mut d = PolicyDiff::default();
    for (c, v) in b {
        let Some(old) = a.get(&c) else { continue };
        let entry =
            json!({"schema":c.schema,"table":c.table,"column":c.column,"before":old,"after":v});
        if v.treatment == "show" && old.treatment != "show" {
            d.newly_visible.push(entry.clone());
        }
        if matches!(v.treatment, "mask" | "withheld")
            && matches!(old.treatment, "show" | "tokenize")
        {
            d.newly_masked.push(entry.clone());
        }
        if v.treatment == "tokenize" && matches!(old.treatment, "show" | "mask") {
            d.newly_tokenized.push(entry.clone());
        }
        if v.treatment == "tokenize" && old.treatment == "tokenize" && v.domain != old.domain {
            d.domain_changes.push(entry.clone());
        }
        if matches!(v.origin, "needs_repair" | "unbound") {
            d.repairs_needed.push(entry);
        }
    }
    let old = links(before);
    let new = links(after);
    d.links_added = new
        .iter()
        .filter(|(k, _)| !old.contains_key(*k))
        .map(|(_, v)| v.clone())
        .collect();
    d.links_removed = old
        .iter()
        .filter(|(k, _)| !new.contains_key(*k))
        .map(|(_, v)| v.clone())
        .collect();
    if before.route_default != after.route_default {
        d.mode_change =
            Some(json!({"before":mode(before.route_default),"after":mode(after.route_default)}));
    }
    d.summary = Summary {
        newly_visible: d.newly_visible.len(),
        newly_masked: d.newly_masked.len(),
        newly_tokenized: d.newly_tokenized.len(),
        domain_changes: d.domain_changes.len(),
        links_added: d.links_added.len(),
        links_removed: d.links_removed.len(),
        repairs_needed: d.repairs_needed.len(),
    };
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{DomainName, LinkRule, file::parse_policy_document},
        schema::{Column, Relation, RelationKind},
        value::ColumnType,
    };
    pub(crate) fn snapshot() -> Snapshot {
        Snapshot::new(
            vec![Relation {
                schema: "public".into(),
                name: "accounts".into(),
                kind: RelationKind::Table,
                columns: ["z_sensitive_name", "a_sensitive_name"]
                    .into_iter()
                    .map(|name| Column {
                        name: name.into(),
                        ty: ColumnType::Text,
                        nullable: true,
                        generated: None,
                        default: None,
                    })
                    .collect(),
                view_definition: None,
                primary_key: vec![],
            }],
            vec![],
            vec![],
            0,
        )
    }
    fn doc(mode: &str) -> PolicyDocument {
        parse_policy_document(&format!("version=2\ndefault='{mode}'\n")).unwrap()
    }
    #[test]
    fn mode_switches_cover_every_supported_unlisted_column_and_are_sorted() {
        let show = doc("show");
        let token = doc("tokenize");
        let s = snapshot();
        let b = RouteBindings::default();
        let d = diff(&show, &token, &s, &b);
        assert_eq!(d.summary.newly_tokenized, 2);
        assert_eq!(d.newly_tokenized[0]["column"], "a_sensitive_name");
        assert_eq!(d.newly_tokenized[0]["after"]["domain"], "new");
        assert_eq!(
            serde_json::to_string(&d).unwrap(),
            serde_json::to_string(&diff(&show, &token, &s, &b)).unwrap()
        );
        assert_eq!(diff(&token, &show, &s, &b).summary.newly_visible, 2);
    }
    #[test]
    fn unchanged_unbound_policy_only_reports_repairs() {
        let doc = doc("tokenize");
        let d = diff(&doc, &doc, &snapshot(), &RouteBindings::default());
        assert_eq!(d.summary.newly_tokenized, 0);
        assert_eq!(d.summary.repairs_needed, 2);
        assert!(d.domain_changes.is_empty());
    }
    #[test]
    fn adding_mask_is_value_free() {
        let before = doc("show");
        let mut after = before.clone();
        let c = ColumnRef {
            schema: "public".into(),
            table: "accounts".into(),
            column: "a_sensitive_name".into(),
        };
        after.policy.columns.insert(
            c,
            Treatment::Masked {
                placeholder: Some("DO_NOT_RENDER_PLACEHOLDER".into()),
            },
        );
        let d = diff(&before, &after, &snapshot(), &RouteBindings::default());
        assert_eq!(d.summary.newly_masked, 1);
        assert!(
            !serde_json::to_string(&d)
                .unwrap()
                .contains("DO_NOT_RENDER_PLACEHOLDER")
        );
    }
    #[test]
    fn link_changes_report_new_domain_and_link() {
        let mut before = doc("show");
        let fields: Vec<_> = views(&before, &snapshot(), &RouteBindings::default(), false)
            .into_keys()
            .collect();
        for c in &fields {
            before.policy.columns.insert(
                c.clone(),
                Treatment::Blind {
                    domain: DomainName("old".into()),
                },
            );
        }
        let mut after = before.clone();
        after.links.push(LinkRule {
            fields,
            domain: Some(DomainName("linked".into())),
        });
        let d = diff(&before, &after, &snapshot(), &RouteBindings::default());
        assert_eq!(d.summary.links_added, 1);
        assert_eq!(d.summary.domain_changes, 2);
        assert_eq!(d.domain_changes[0]["after"]["domain"], "linked");
    }
}
