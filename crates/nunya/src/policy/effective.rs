//! Compile declared policy into a treatment for every captured column.
use super::{DomainName, PolicyDocument, RouteDefault, Treatment};
use crate::{
    schema::{ColumnRef, Snapshot},
    value::ColumnType,
};
use std::collections::BTreeMap;

/// Placeholder for a private token domain assigned and persisted by GD-0088 stack 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingPrivateDomain;

/// Treatment compiled for a captured column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effective {
    /// Permit plaintext values.
    Show,
    /// Substitute a placeholder or SQL NULL.
    Masked {
        /// Replacement text, with None meaning SQL NULL.
        placeholder: Option<String>,
    },
    /// Tokenize using a declared domain.
    Blind {
        /// Declared token domain.
        domain: DomainName,
    },
    /// Tokenize using a private domain that has not yet been assigned.
    BlindPrivate(PendingPrivateDomain),
}

/// Reason the compiler selected a treatment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// An explicit column rule selected the treatment.
    Explicit,
    /// The route default selected the treatment.
    RouteDefault,
    /// No codec supports default tokenization of this type.
    NoCodec,
    /// A JSON column has no path rules.
    JsonNoPaths,
    /// JSON path enforcement is pending, so the entire column is withheld.
    JsonPathsPending,
}

/// Failure to compile a column treatment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectiveError {
    /// The requested column is absent from the snapshot.
    UnknownColumn,
}
impl std::fmt::Display for EffectiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownColumn => f.write_str("column does not exist in snapshot"),
        }
    }
}
impl std::error::Error for EffectiveError {}

/// Compile a column treatment, refusing columns absent from the snapshot.
pub fn effective(
    doc: &PolicyDocument,
    snapshot: &Snapshot,
    column: &ColumnRef,
) -> Result<(Effective, Origin), EffectiveError> {
    let captured = snapshot
        .column(column)
        .ok_or(EffectiveError::UnknownColumn)?;
    Ok(compile(doc, column, &captured.ty))
}

/// Compile exactly one treatment for every fully qualified snapshot column.
pub fn effective_all(
    doc: &PolicyDocument,
    snapshot: &Snapshot,
) -> BTreeMap<ColumnRef, (Effective, Origin)> {
    snapshot
        .relations
        .iter()
        .flat_map(|relation| {
            relation.columns.iter().map(|column| {
                let reference = ColumnRef {
                    schema: relation.schema.clone(),
                    table: relation.name.clone(),
                    column: column.name.clone(),
                };
                let treatment = compile(doc, &reference, &column.ty);
                (reference, treatment)
            })
        })
        .collect()
}

fn compile(doc: &PolicyDocument, column: &ColumnRef, ty: &ColumnType) -> (Effective, Origin) {
    if doc.json_paths.iter().any(|rule| rule.column == *column) {
        return (
            Effective::Masked { placeholder: None },
            Origin::JsonPathsPending,
        );
    }
    if let Some(treatment) = doc.policy.columns.get(column) {
        let treatment = match treatment {
            Treatment::Show => Effective::Show,
            Treatment::Masked { placeholder } => Effective::Masked {
                placeholder: placeholder.clone(),
            },
            Treatment::Blind { domain } => Effective::Blind {
                domain: domain.clone(),
            },
        };
        return (treatment, Origin::Explicit);
    }
    match doc.route_default {
        RouteDefault::Show => (Effective::Show, Origin::RouteDefault),
        RouteDefault::Tokenize => match ty {
            ColumnType::Text
            | ColumnType::Varchar(_)
            | ColumnType::Bpchar(_)
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Uuid
            | ColumnType::Date
            | ColumnType::Timestamp
            | ColumnType::Timestamptz => (
                Effective::BlindPrivate(PendingPrivateDomain),
                Origin::RouteDefault,
            ),
            ColumnType::Int2
            | ColumnType::Bool
            | ColumnType::Numeric
            | ColumnType::Bytea
            | ColumnType::Enum(_)
            | ColumnType::Array(_)
            | ColumnType::Other(_) => (Effective::Masked { placeholder: None }, Origin::NoCodec),
            ColumnType::Json | ColumnType::Jsonb => {
                (Effective::Masked { placeholder: None }, Origin::JsonNoPaths)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{
            JsonPathRule, JsonPathTreatment, PathSegment, Policy, PolicyVersion, ReadLogSink,
            file::parse_policy_document,
        },
        schema::{Column, Relation, RelationKind},
    };

    fn document(mode: RouteDefault) -> PolicyDocument {
        PolicyDocument {
            file_readlog: None,
            file_snapshot_max_age_ms: None,
            policy: Policy {
                columns: BTreeMap::new(),
                domains: BTreeMap::new(),
                snapshot_max_age_ms: 60000,
                readlog: ReadLogSink::Stdout,
            },
            version: PolicyVersion::V2,
            route_default: mode,
            json_paths: vec![],
        }
    }

    fn all_types() -> Vec<ColumnType> {
        vec![
            ColumnType::Text,
            ColumnType::Varchar(None),
            ColumnType::Varchar(Some(20)),
            ColumnType::Bpchar(None),
            ColumnType::Bpchar(Some(10)),
            ColumnType::Int2,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Uuid,
            ColumnType::Date,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::Bool,
            ColumnType::Numeric,
            ColumnType::Bytea,
            ColumnType::Json,
            ColumnType::Jsonb,
            ColumnType::Array(Box::new(ColumnType::Text)),
            ColumnType::Enum("enum_type".into()),
            ColumnType::Other("other_type".into()),
        ]
    }

    // Exhaustive test-side guard: every new ColumnType requires an explicit expectation.
    fn tokenize_expected(ty: &ColumnType) -> (Effective, Origin) {
        match ty {
            ColumnType::Text
            | ColumnType::Varchar(_)
            | ColumnType::Bpchar(_)
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Uuid
            | ColumnType::Date
            | ColumnType::Timestamp
            | ColumnType::Timestamptz => (
                Effective::BlindPrivate(PendingPrivateDomain),
                Origin::RouteDefault,
            ),
            ColumnType::Int2
            | ColumnType::Bool
            | ColumnType::Numeric
            | ColumnType::Bytea
            | ColumnType::Enum(_)
            | ColumnType::Array(_)
            | ColumnType::Other(_) => (Effective::Masked { placeholder: None }, Origin::NoCodec),
            ColumnType::Json | ColumnType::Jsonb => {
                (Effective::Masked { placeholder: None }, Origin::JsonNoPaths)
            }
        }
    }

    fn column(name: String, ty: ColumnType) -> Column {
        Column {
            name,
            ty,
            nullable: true,
            generated: None,
            default: None,
        }
    }

    fn snapshot(entries: Vec<(ColumnRef, ColumnType)>) -> Snapshot {
        let mut relations = BTreeMap::new();
        for (reference, ty) in entries {
            let relation = relations
                .entry((reference.schema.clone(), reference.table.clone()))
                .or_insert_with(|| Relation {
                    schema: reference.schema,
                    name: reference.table,
                    kind: RelationKind::Table,
                    columns: vec![],
                    view_definition: None,
                    primary_key: vec![],
                });
            relation.columns.push(column(reference.column, ty));
        }
        Snapshot::new(relations.into_values().collect(), vec![], vec![], 0)
    }

    fn type_entries() -> Vec<(ColumnRef, ColumnType)> {
        all_types()
            .into_iter()
            .enumerate()
            .map(|(i, ty)| {
                (
                    ColumnRef {
                        schema: "MixedCase".into(),
                        table: "Types".into(),
                        column: format!("c{i}"),
                    },
                    ty,
                )
            })
            .collect()
    }

    #[test]
    fn defaults_cover_every_type() {
        let entries = type_entries();
        let snapshot = snapshot(entries.clone());
        for mode in [RouteDefault::Tokenize, RouteDefault::Show] {
            let doc = document(mode);
            for (reference, ty) in &entries {
                let expected = match mode {
                    RouteDefault::Tokenize => tokenize_expected(ty),
                    RouteDefault::Show => (Effective::Show, Origin::RouteDefault),
                };
                assert_eq!(
                    effective(&doc, &snapshot, reference),
                    Ok(expected),
                    "{ty:?}"
                );
            }
        }
    }

    #[test]
    fn explicit_rules_win_in_both_modes() {
        let entries = type_entries();
        let snapshot = snapshot(entries.clone());
        for mode in [RouteDefault::Tokenize, RouteDefault::Show] {
            for (treatment, expected) in [
                (Treatment::Show, Effective::Show),
                (
                    Treatment::Masked { placeholder: None },
                    Effective::Masked { placeholder: None },
                ),
                (
                    Treatment::Masked {
                        placeholder: Some("hidden".into()),
                    },
                    Effective::Masked {
                        placeholder: Some("hidden".into()),
                    },
                ),
                (
                    Treatment::Blind {
                        domain: DomainName("declared".into()),
                    },
                    Effective::Blind {
                        domain: DomainName("declared".into()),
                    },
                ),
            ] {
                let mut doc = document(mode);
                for (reference, _) in &entries {
                    doc.policy
                        .columns
                        .insert(reference.clone(), treatment.clone());
                    assert_eq!(
                        effective(&doc, &snapshot, reference),
                        Ok((expected.clone(), Origin::Explicit))
                    );
                }
            }
        }
    }

    #[test]
    fn path_rules_withhold_the_whole_column_in_both_modes() {
        let entries = type_entries();
        let snapshot = snapshot(entries.clone());
        for mode in [RouteDefault::Tokenize, RouteDefault::Show] {
            // Include non-JSON types: any path declaration must fail closed.
            for (reference, _) in &entries {
                for treatment in [
                    None,
                    Some(Treatment::Show),
                    Some(Treatment::Masked {
                        placeholder: Some("hidden".into()),
                    }),
                    Some(Treatment::Blind {
                        domain: DomainName("declared".into()),
                    }),
                ] {
                    let mut doc = document(mode);
                    doc.json_paths.push(JsonPathRule {
                        column: reference.clone(),
                        path: vec![PathSegment::Key("key".into())],
                        treatment: JsonPathTreatment::Masked,
                    });
                    if let Some(treatment) = treatment {
                        doc.policy.columns.insert(reference.clone(), treatment);
                    }
                    assert_eq!(
                        effective(&doc, &snapshot, reference),
                        Ok((
                            Effective::Masked { placeholder: None },
                            Origin::JsonPathsPending
                        ))
                    );
                }
            }
        }
    }

    #[test]
    fn unknown_column_is_rejected_before_rules() {
        let entries = type_entries();
        let reference = entries[0].0.clone();
        let empty = snapshot(vec![]);
        let mut doc = document(RouteDefault::Show);
        doc.policy
            .columns
            .insert(reference.clone(), Treatment::Show);
        doc.json_paths.push(JsonPathRule {
            column: reference.clone(),
            path: vec![],
            treatment: JsonPathTreatment::Masked,
        });
        assert_eq!(
            effective(&doc, &empty, &reference),
            Err(EffectiveError::UnknownColumn)
        );
        assert_eq!(
            EffectiveError::UnknownColumn.to_string(),
            "column does not exist in snapshot"
        );
    }

    #[test]
    fn legacy_round_trip_preserves_listed_and_unlisted_treatments() {
        for source in [
            include_str!("../../examples/policy.toml"),
            include_str!("../../harness/policy.toml"),
            include_str!("../../harness/spanner/policy_gsql.toml"),
        ] {
            let doc = parse_policy_document(source).unwrap();
            assert_eq!(doc.version, PolicyVersion::Legacy);
            let mut entries = type_entries();
            entries.extend(
                doc.policy
                    .columns
                    .keys()
                    .cloned()
                    .map(|reference| (reference, ColumnType::Text)),
            );
            let snapshot = snapshot(entries.clone());
            for (reference, _) in entries {
                let expected = match doc.policy.columns.get(&reference) {
                    Some(Treatment::Show) => (Effective::Show, Origin::Explicit),
                    Some(Treatment::Masked { placeholder }) => (
                        Effective::Masked {
                            placeholder: placeholder.clone(),
                        },
                        Origin::Explicit,
                    ),
                    Some(Treatment::Blind { domain }) => (
                        Effective::Blind {
                            domain: domain.clone(),
                        },
                        Origin::Explicit,
                    ),
                    None => (Effective::Show, Origin::RouteDefault),
                };
                assert_eq!(effective(&doc, &snapshot, &reference), Ok(expected));
            }
        }
    }

    #[test]
    fn all_covers_every_column_using_exact_relation_names() {
        let mut entries = type_entries();
        let mut other = entries[0].clone();
        other.0.schema = String::new();
        entries.push(other.clone());
        other.0.table = "types".into();
        entries.push(other);
        let snapshot = snapshot(entries.clone());
        let mut doc = document(RouteDefault::Tokenize);
        doc.policy
            .columns
            .insert(entries[0].0.clone(), Treatment::Show);
        doc.json_paths.push(JsonPathRule {
            column: entries[1].0.clone(),
            path: vec![],
            treatment: JsonPathTreatment::Masked,
        });
        let all = effective_all(&doc, &snapshot);
        assert_eq!(all.len(), entries.len());
        for (reference, _) in entries {
            assert_eq!(
                all.get(&reference).cloned(),
                Some(effective(&doc, &snapshot, &reference).unwrap())
            );
        }
    }
}
