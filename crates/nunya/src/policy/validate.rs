//! Policy validation against a captured database schema.
use super::{
    DomainName, Policy, Treatment,
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
    for dependent in &snapshot.dependents {
        if dependent.kind == DependentKind::View {
            continue;
        }
        for column in &dependent.reads {
            let treatment = match policy.treatment(column) {
                Some(Treatment::Blind { .. }) => "blind",
                Some(Treatment::Masked { .. }) => "masked",
                None => continue,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{DomainSettings, ReadLogSink, TokenizeSetting, TypeRange},
        schema::{Column, DependentObject, Relation},
    };
    fn fixture(types: &[ColumnType]) -> (Policy, Snapshot) {
        let mut policy = Policy {
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
}
