//! Policy validation against a captured database schema.
use super::{
    DomainName, Policy, PolicyDocument, Treatment,
    bindings::TypeFamily,
    file::{PolicyError, error},
};
use crate::{
    schema::{ColumnRef, DependentKind, RelationKind, Snapshot},
    value::ColumnType,
};
use std::collections::{BTreeMap, HashSet};

fn column_name(column: &ColumnRef) -> String {
    format!("{}.{}.{}", column.schema, column.table, column.column)
}
fn family(ty: &ColumnType) -> ColumnType {
    if ty.is_textual() {
        ColumnType::Text
    } else {
        ty.clone()
    }
}
fn bounds(ty: &ColumnType) -> Option<(i64, i64)> {
    match ty {
        ColumnType::Int2 => Some((i16::MIN.into(), i16::MAX.into())),
        ColumnType::Int4 => Some((i32::MIN.into(), i32::MAX.into())),
        ColumnType::Int8 => Some((i64::MIN, i64::MAX)),
        // Blind temporal domains are limited to Gregorian years 0001..=9999.
        ColumnType::Date => Some((-730_119, 2_921_939)),
        ColumnType::Timestamp | ColumnType::Timestamptz => {
            Some((-730_119 * 86_400_000_000, 2_921_940 * 86_400_000_000 - 1))
        }
        ColumnType::Text
        | ColumnType::Varchar(_)
        | ColumnType::Bpchar(_)
        | ColumnType::Uuid
        | ColumnType::Bool
        | ColumnType::Numeric
        | ColumnType::Bytea
        | ColumnType::Json
        | ColumnType::Jsonb
        | ColumnType::Array(_)
        | ColumnType::Enum(_)
        | ColumnType::Other(_) => None,
    }
}
fn dependent_kind(kind: &DependentKind) -> &'static str {
    match kind {
        DependentKind::Trigger => "trigger",
        DependentKind::Rule => "rule",
        DependentKind::Policy => "policy",
        DependentKind::GeneratedColumn => "generated column",
        DependentKind::Default => "default",
        DependentKind::View => "view",
    }
}

/// Collect policy/schema mismatches and unsafe dependents without exposing values.
pub fn validate(policy: &Policy, snapshot: &Snapshot) -> Result<(), Vec<PolicyError>> {
    let mut errors = vec![];
    let mut families: BTreeMap<&DomainName, Vec<(ColumnType, &ColumnRef)>> = BTreeMap::new();
    for (column, treatment) in &policy.columns {
        if matches!(treatment, Treatment::Show) {
            continue;
        }
        let context = format!("column {}", column_name(column));
        let relation = snapshot.resolve_relation(Some(&column.schema), &column.table);
        match relation {
            None => error(&mut errors, &context, "column does not exist in snapshot"),
            Some(relation) if relation.kind != RelationKind::Table => error(
                &mut errors,
                &context,
                "policy columns must belong to a Table relation",
            ),
            Some(_) => (),
        }
        let Some(schema_column) = snapshot.column(column) else {
            if relation.is_some() {
                error(&mut errors, &context, "column does not exist in snapshot");
            }
            continue;
        };
        match treatment {
            Treatment::Show => (),
            Treatment::Masked { placeholder } => {
                if placeholder.is_some() && !schema_column.ty.is_textual() {
                    error(
                        &mut errors,
                        &context,
                        "non-textual masked column requires a NULL placeholder",
                    );
                }
            }
            Treatment::Blind { domain } => {
                if !policy.domains.contains_key(domain) {
                    error(
                        &mut errors,
                        &context,
                        &format!("domain {} is not declared", domain.0),
                    );
                }
                match &schema_column.ty {
                    ColumnType::Text
                    | ColumnType::Varchar(_)
                    | ColumnType::Bpchar(_)
                    | ColumnType::Int4
                    | ColumnType::Int8
                    | ColumnType::Uuid
                    | ColumnType::Date
                    | ColumnType::Timestamp
                    | ColumnType::Timestamptz => (),
                    ColumnType::Int2 => error(
                        &mut errors,
                        &context,
                        "too few values to encrypt safely; mask it instead",
                    ),
                    ColumnType::Bool | ColumnType::Enum(_) => {
                        error(&mut errors, &context, "mask it instead")
                    }
                    ColumnType::Numeric
                    | ColumnType::Bytea
                    | ColumnType::Json
                    | ColumnType::Jsonb
                    | ColumnType::Array(_)
                    | ColumnType::Other(_) => error(
                        &mut errors,
                        &context,
                        "type not supported for blind columns yet",
                    ),
                }
                families
                    .entry(domain)
                    .or_default()
                    .push((family(&schema_column.ty), column));
            }
        }
    }
    for (domain, columns) in families {
        let context = format!("domain {}", domain.0);
        let types: HashSet<_> = columns.iter().map(|(ty, _)| ty).collect();
        if types.len() > 1 {
            error(
                &mut errors,
                &context,
                "columns must belong to one type family",
            );
        }
        if let Some(range) = policy
            .domains
            .get(domain)
            .and_then(|settings| settings.range.as_ref())
        {
            let size = i128::from(range.max) - i128::from(range.min) + 1;
            if size < (1 << 20) {
                error(
                    &mut errors,
                    &context,
                    "range must contain at least 2^20 values",
                );
            }
            for ty in types {
                match bounds(ty) {
                    Some((min, max)) => {
                        if range.min < min || range.max > max {
                            error(
                                &mut errors,
                                &context,
                                "range exceeds the column type bounds",
                            );
                        }
                    }
                    None => error(
                        &mut errors,
                        &context,
                        "range requires an integer, date or timestamp family",
                    ),
                }
            }
        }
    }
    if let Err(dependent_errors) = validate_dependents(policy, snapshot) {
        errors.extend(dependent_errors);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Refuse dependents that can copy protected columns outside the gateway.
pub fn validate_dependents(policy: &Policy, snapshot: &Snapshot) -> Result<(), Vec<PolicyError>> {
    let mut errors = Vec::new();
    for dependent in &snapshot.dependents {
        if dependent.kind == DependentKind::View {
            continue;
        }
        for column in &dependent.reads {
            let treatment = match policy.treatment(column) {
                Some(Treatment::Blind { .. }) => "blind",
                Some(Treatment::Masked { .. }) => "masked",
                Some(Treatment::Show) | None => continue,
            };
            errors.push(PolicyError {
                message: format!(
                    "{} {} reads {treatment} column {}; Nunya cannot see what it copies",
                    dependent_kind(&dependent.kind),
                    dependent.name,
                    column_name(column)
                ),
            });
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// A names-only advisory about a valid identity-anchored declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyWarning {
    /// Names-only description of the declaration and its captured name.
    pub message: String,
}

/// Validate declarations, allowing missing Show names only with live identity anchors.
pub fn validate_document(
    doc: &PolicyDocument,
    snapshot: &Snapshot,
    bindings: &super::bindings::RouteBindings,
) -> Result<Vec<PolicyWarning>, Vec<PolicyError>> {
    let mut warnings = vec![];
    let mut errors = vec![];
    let path_columns: HashSet<_> = doc.json_paths.iter().map(|rule| &rule.column).collect();
    for column in &path_columns {
        if snapshot.column(column).is_none() {
            error(&mut errors, &format!("column {}", column_name(column)), "column does not exist in snapshot");
        }
    }
    for (column, treatment) in &doc.policy.columns {
        if *treatment != Treatment::Show || snapshot.column(column).is_some() {
            continue;
        }
        if path_columns.contains(column) {
            continue;
        }
        let renamed = bindings.iter().find_map(|(identity, record)| {
            if record.status != super::bindings::BindingStatus::Active
                || record.show_rule.as_ref() != Some(column)
            {
                return None;
            }
            snapshot.column_ids.iter().find_map(|(name, id)| {
                (id == identity && snapshot.column(name).is_some()).then_some(name)
            })
        });
        if let Some(renamed) = renamed {
            warnings.push(PolicyWarning { message: format!(
                "show rule names {}, renamed to {}", column_name(column), column_name(renamed)
            ) });
        } else {
            error(&mut errors, &format!("column {}", column_name(column)), "column does not exist in snapshot");
        }
    }
    if let Err(other) = validate(&doc.policy, snapshot) { errors.extend(other); }
    if let Err(other) = validate_links(doc, snapshot) { errors.extend(other); }
    if errors.is_empty() { Ok(warnings) } else { Err(errors) }
}

/// Validate linked fields and declared domain compatibility against a snapshot.
pub fn validate_links(doc: &PolicyDocument, snapshot: &Snapshot) -> Result<(), Vec<PolicyError>> {
    let mut errors = vec![];
    for link in &doc.links {
        let mut families = HashSet::new();
        for column in &link.fields {
            let context = format!("link column {}", column_name(column));
            if let Some(captured) = snapshot.column(column) {
                if TypeFamily::of(&captured.ty).is_none() {
                    error(&mut errors, &context, "type has no supported codec");
                }
                if !captured.ty.is_textual() {
                    error(&mut errors, &context, "only text fields can be linked");
                }
                families.insert(family(&captured.ty));
            } else {
                error(&mut errors, &context, "column does not exist in snapshot");
            }
            match doc.policy.treatment(column) {
                None => (),
                Some(Treatment::Blind { domain }) if link.domain.as_ref() == Some(domain) => (),
                Some(Treatment::Show | Treatment::Masked { .. } | Treatment::Blind { .. }) => {
                    error(
                        &mut errors,
                        &context,
                        "explicit column treatment conflicts with link",
                    );
                }
            }
            if doc.json_paths.iter().any(|rule| &rule.column == column) {
                error(&mut errors, &context, "json_path conflicts with link");
            }
        }
        if let Some(domain) = &link.domain {
            for (column, treatment) in &doc.policy.columns {
                if matches!(treatment, Treatment::Blind { domain: other } if other == domain)
                    && let Some(captured) = snapshot.column(column)
                {
                    families.insert(family(&captured.ty));
                }
            }
        }
        if families.len() > 1 {
            error(
                &mut errors,
                "link",
                "columns must belong to one type family",
            );
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{DomainSettings, ReadLogSink, TokenizeSetting, TypeRange},
        schema::{Column, DependentObject, Relation},
    };
    fn fixture(types: &[ColumnType]) -> (Policy, Snapshot) {
        let mut policy = Policy {
            retired_domains: Default::default(),
            json_filters: Default::default(),
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: ReadLogSink::Stdout,
        };
        let domain = DomainName("email".into());
        policy.domains.insert(
            domain.clone(),
            DomainSettings {
                tokenize: TokenizeSetting::Off,
                range: None,
            },
        );
        let columns = types
            .iter()
            .enumerate()
            .map(|(i, ty)| {
                let name = format!("c{i}");
                policy.columns.insert(
                    ColumnRef {
                        schema: "public".into(),
                        table: "customers".into(),
                        column: name.clone(),
                    },
                    Treatment::Blind {
                        domain: domain.clone(),
                    },
                );
                Column {
                    name,
                    ty: ty.clone(),
                    nullable: true,
                    generated: None,
                    default: None,
                }
            })
            .collect();
        let snapshot = Snapshot::new(
            vec![Relation {
                schema: "public".into(),
                name: "customers".into(),
                kind: RelationKind::Table,
                columns,
                view_definition: None,
                primary_key: vec![],
            }],
            vec![],
            vec!["public".into()],
            0,
        );
        (policy, snapshot)
    }
    fn has(policy: &Policy, snapshot: &Snapshot, text: &str) -> bool {
        validate(policy, snapshot)
            .unwrap_err()
            .iter()
            .any(|error| error.message.contains(text))
    }
    #[test]
    fn valid_and_textual_family() {
        for types in [
            vec![ColumnType::Text],
            vec![
                ColumnType::Text,
                ColumnType::Varchar(Some(20)),
                ColumnType::Bpchar(None),
            ],
        ] {
            let (policy, snapshot) = fixture(&types);
            assert!(validate(&policy, &snapshot).is_ok());
        }
    }
    #[test]
    fn missing_column_and_non_table_relation() {
        let (policy, mut snapshot) = fixture(&[ColumnType::Text]);
        snapshot.relations[0].columns.clear();
        assert!(has(&policy, &snapshot, "does not exist"));
        let (policy, mut snapshot) = fixture(&[ColumnType::Text]);
        snapshot.relations[0].kind = RelationKind::View;
        assert!(has(&policy, &snapshot, "Table relation"));
    }
    #[test]
    fn rejects_unsafe_types_and_mixed_families() {
        for (ty, message) in [
            (ColumnType::Int2, "too few values"),
            (ColumnType::Bool, "mask it instead"),
            (ColumnType::Enum("private_type".into()), "mask it instead"),
            (ColumnType::Numeric, "type not supported"),
        ] {
            let (policy, snapshot) = fixture(&[ty]);
            assert!(has(&policy, &snapshot, message));
        }
        let (policy, snapshot) = fixture(&[ColumnType::Text, ColumnType::Int8]);
        assert!(has(&policy, &snapshot, "one type family"));
    }
    #[test]
    fn range_size_family_and_bounds() {
        let (mut policy, snapshot) = fixture(&[ColumnType::Int4]);
        let settings = policy.domains.get_mut(&DomainName("email".into())).unwrap();
        settings.range = Some(TypeRange {
            min: 0,
            max: (1 << 20) - 1,
        });
        assert!(validate(&policy, &snapshot).is_ok());
        policy
            .domains
            .get_mut(&DomainName("email".into()))
            .unwrap()
            .range = Some(TypeRange { min: 0, max: 3 });
        assert!(has(&policy, &snapshot, "at least 2^20"));
        policy
            .domains
            .get_mut(&DomainName("email".into()))
            .unwrap()
            .range = Some(TypeRange {
            min: i32::MIN.into(),
            max: i64::from(i32::MAX) + 1,
        });
        assert!(has(&policy, &snapshot, "type bounds"));
        let (mut policy, snapshot) = fixture(&[ColumnType::Text]);
        policy
            .domains
            .get_mut(&DomainName("email".into()))
            .unwrap()
            .range = Some(TypeRange {
            min: 0,
            max: i64::MAX,
        });
        assert!(has(&policy, &snapshot, "range requires"));
        let (mut policy, snapshot) = fixture(&[ColumnType::Int8]);
        policy
            .domains
            .get_mut(&DomainName("email".into()))
            .unwrap()
            .range = Some(TypeRange {
            min: i64::MIN,
            max: i64::MAX,
        });
        assert!(validate(&policy, &snapshot).is_ok());
    }
    #[test]
    fn non_text_placeholder() {
        let (mut policy, snapshot) = fixture(&[ColumnType::Int8]);
        *policy.columns.values_mut().next().unwrap() = Treatment::Masked {
            placeholder: Some("redacted".into()),
        };
        assert!(has(&policy, &snapshot, "NULL placeholder"));
    }
    #[test]
    fn temporal_ranges_accept_finite_bounds_and_reject_outside() {
        for (ty, min, max) in [
            (ColumnType::Date, -730_119, 2_921_939),
            (
                ColumnType::Timestamp,
                -730_119 * 86_400_000_000,
                2_921_940 * 86_400_000_000 - 1,
            ),
            (
                ColumnType::Timestamptz,
                -730_119 * 86_400_000_000,
                2_921_940 * 86_400_000_000 - 1,
            ),
        ] {
            let (mut policy, snapshot) = fixture(&[ty]);
            let domain = DomainName("email".into());
            policy.domains.get_mut(&domain).unwrap().range = Some(TypeRange { min, max });
            assert!(validate(&policy, &snapshot).is_ok());
            policy.domains.get_mut(&domain).unwrap().range = Some(TypeRange { min: min - 1, max });
            assert!(has(&policy, &snapshot, "type bounds"));
            policy.domains.get_mut(&domain).unwrap().range = Some(TypeRange { min, max: max + 1 });
            assert!(has(&policy, &snapshot, "type bounds"));
        }
    }
    #[test]
    fn all_fpe_families_accept_defaults_and_enforce_minimum_range() {
        for ty in [
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Date,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
        ] {
            let (mut policy, snapshot) = fixture(&[ty]);
            assert!(validate(&policy, &snapshot).is_ok());
            let domain = DomainName("email".into());
            policy.domains.get_mut(&domain).unwrap().range = Some(TypeRange {
                min: 0,
                max: (1 << 20) - 2,
            });
            assert!(has(&policy, &snapshot, "at least 2^20"));
            policy.domains.get_mut(&domain).unwrap().range = Some(TypeRange {
                min: 0,
                max: (1 << 20) - 1,
            });
            assert!(validate(&policy, &snapshot).is_ok());
        }
    }
    #[test]
    fn dependent_trigger_is_named_and_view_is_allowed() {
        let (policy, mut snapshot) = fixture(&[ColumnType::Text]);
        snapshot.dependents.push(DependentObject {
            kind: DependentKind::Trigger,
            name: "audit_email".into(),
            reads: policy.columns.keys().cloned().collect(),
            writes_table: None,
        });
        assert!(has(
            &policy,
            &snapshot,
            "trigger audit_email reads blind column public.customers.c0"
        ));
        snapshot.dependents[0].kind = DependentKind::View;
        assert!(validate(&policy, &snapshot).is_ok());
    }
    #[test]
    fn unused_domains_are_ignored_and_errors_aggregate() {
        let (mut policy, mut snapshot) = fixture(&[ColumnType::Bool, ColumnType::Int2]);
        policy.domains.insert(
            DomainName("unused".into()),
            DomainSettings {
                tokenize: TokenizeSetting::Off,
                range: Some(TypeRange { min: 2, max: 1 }),
            },
        );
        let errors = validate(&policy, &snapshot).unwrap_err();
        assert_eq!(errors.len(), 3);
        snapshot.relations[0]
            .columns
            .iter_mut()
            .for_each(|c| c.ty = ColumnType::Text);
        assert!(validate(&policy, &snapshot).is_ok());
    }

    #[test]
    fn show_is_validated_like_an_absent_column() {
        let policy = Policy {
            retired_domains: Default::default(),
            json_filters: Default::default(),
            columns: [(
                ColumnRef {
                    schema: "missing".into(),
                    table: "missing".into(),
                    column: "missing".into(),
                },
                Treatment::Show,
            )]
            .into(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: super::super::ReadLogSink::Stdout,
        };
        assert!(validate(&policy, &Snapshot::new(vec![], vec![], vec![], 0)).is_ok());
    }
    fn linked(types: &[ColumnType], declared: bool) -> (PolicyDocument, Snapshot) {
        let (mut policy, snapshot) = fixture(types);
        let fields = policy.columns.keys().take(2).cloned().collect();
        policy.columns.clear();
        (
            PolicyDocument {
                file_readlog: None,
                file_snapshot_max_age_ms: None,
                policy,
                version: super::super::PolicyVersion::V2,
                route_default: super::super::RouteDefault::Tokenize,
                json_paths: vec![],
                links: vec![super::super::LinkRule {
                    fields,
                    domain: declared.then(|| DomainName("email".into())),
                }],
            },
            snapshot,
        )
    }
    fn link_has(doc: &PolicyDocument, snapshot: &Snapshot, text: &str) -> bool {
        validate_links(doc, snapshot)
            .unwrap_err()
            .iter()
            .any(|error| error.message.starts_with("link") && error.message.contains(text))
    }
    #[test]
    fn missing_json_path_columns_are_errors_once_per_column() {
        let (mut doc, snapshot) = linked(&[ColumnType::Text], false);
        doc.links.clear();
        let column = ColumnRef { schema: "public".into(), table: "customers".into(), column: "doc".into() };
        doc.policy.columns.insert(column.clone(), Treatment::Show);
        for key in ["a", "b"] {
            doc.json_paths.push(super::super::JsonPathRule {
                column: column.clone(),
                path: vec![super::super::PathSegment::Key(key.into())],
                treatment: super::super::JsonPathTreatment::Masked,
            });
        }
        let errors = validate_document(&doc, &snapshot, &super::super::bindings::RouteBindings::default()).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].message, "column public.customers.doc: column does not exist in snapshot");
    }

    #[test]
    fn links_require_existing_columns() {
        let (doc, mut snapshot) = linked(&[ColumnType::Text, ColumnType::Text], false);
        snapshot.relations[0].columns.pop();
        assert!(link_has(&doc, &snapshot, "does not exist"));
    }
    #[test]
    fn links_require_supported_codecs() {
        for ty in [ColumnType::Bool, ColumnType::Jsonb, ColumnType::Int2] {
            let (doc, snapshot) = linked(&[ty.clone(), ty], false);
            assert!(link_has(&doc, &snapshot, "no supported codec"));
        }
    }
    #[test]
    fn links_refuse_every_fpe_family() {
        for ty in [ColumnType::Int4, ColumnType::Int8, ColumnType::Uuid, ColumnType::Date,
            ColumnType::Timestamp, ColumnType::Timestamptz] {
            let (doc, snapshot) = linked(&[ty.clone(), ty], false);
            assert!(link_has(&doc, &snapshot, "only text fields can be linked"));
        }
    }
    #[test]
    fn links_require_one_family() {
        let (doc, snapshot) = linked(&[ColumnType::Text, ColumnType::Int8], false);
        assert!(link_has(&doc, &snapshot, "one type family"));
    }
    #[test]
    fn links_accept_textual_family_and_unlisted_fields() {
        for declared in [false, true] {
            let (doc, snapshot) =
                linked(&[ColumnType::Varchar(Some(12)), ColumnType::Text], declared);
            assert!(validate_links(&doc, &snapshot).is_ok());
        }
    }
    #[test]
    fn links_refuse_conflicting_explicit_treatments() {
        for treatment in [
            Treatment::Show,
            Treatment::Masked { placeholder: None },
            Treatment::Blind {
                domain: DomainName("other".into()),
            },
        ] {
            let (mut doc, snapshot) = linked(&[ColumnType::Text, ColumnType::Text], true);
            doc.policy
                .columns
                .insert(doc.links[0].fields[0].clone(), treatment);
            assert!(link_has(
                &doc,
                &snapshot,
                "explicit column treatment conflicts"
            ));
        }
    }
    #[test]
    fn links_accept_only_matching_explicit_blind_domain() {
        for declared in [false, true] {
            let (mut doc, snapshot) = linked(&[ColumnType::Text, ColumnType::Text], declared);
            doc.policy.columns.insert(
                doc.links[0].fields[0].clone(),
                Treatment::Blind {
                    domain: DomainName("email".into()),
                },
            );
            assert_eq!(validate_links(&doc, &snapshot).is_ok(), declared);
        }
    }
    #[test]
    fn links_refuse_json_path_on_linked_column() {
        let (mut doc, snapshot) = linked(&[ColumnType::Text, ColumnType::Text], false);
        doc.json_paths.push(super::super::JsonPathRule {
            column: doc.links[0].fields[0].clone(),
            path: vec![],
            treatment: super::super::JsonPathTreatment::Masked,
        });
        assert!(link_has(&doc, &snapshot, "json_path conflicts"));
    }
    #[test]
    fn links_include_other_explicit_domain_columns_in_family_check() {
        for ty in [ColumnType::Int8, ColumnType::Varchar(None)] {
            let (mut doc, snapshot) =
                linked(&[ColumnType::Text, ColumnType::Text, ty.clone()], true);
            doc.policy.columns.insert(
                ColumnRef {
                    schema: "public".into(),
                    table: "customers".into(),
                    column: "c2".into(),
                },
                Treatment::Blind {
                    domain: DomainName("email".into()),
                },
            );
            assert_eq!(validate_links(&doc, &snapshot).is_ok(), ty.is_textual());
        }
    }
}
