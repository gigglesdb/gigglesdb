//! Dialect-neutral resolved statements and rewrite plans.
use crate::{error::NyCode, policy::DomainName, schema::ColumnRef, value::ColumnType};
/// Statement policy checker.
pub mod check;
/// Statement rewrite planning.
pub mod rewrite;
/// Information-flow label of a value or expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Label {
    /// Plain.
    Plain,
    /// Blind.
    Blind(DomainName),
    /// Masked.
    Masked,
    /// Computed from blind or masked data beyond a bare-column passthrough.
    Tainted,
}

/// Half-open byte offsets into the original SQL query.
#[derive(Debug, Clone, PartialEq)]
pub struct Span {
    /// Inclusive starting byte offset.
    pub start: usize,
    /// Exclusive ending byte offset.
    pub end: usize,
}

/// Value source.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueSource {
    /// One-based client parameter index.
    Param(u32), // $n, 1-based
    /// Agent-supplied literal, with its source span and unquoted text.
    Literal {
        /// Span.
        span: Span,
        /// Text.
        text: String,
    }, // literal as written; text is the unquoted value
    /// A text literal on a route that never rewrites literals (GoogleSQL); tokens must be parameters.
    NonTokenText,
    /// Resolved column and its information-flow label.
    Column {
        /// Column.
        column: ColumnRef,
        /// Label.
        label: Label,
    },
    /// A guarded Text JSON leaf in its declared domain.
    JsonLeaf(DomainName),
    /// Null.
    Null,
    /// Computed.
    Computed(Label), // any other expression, with its joined label
}

/// Context.
#[derive(Debug, Clone, PartialEq)]
pub enum Context {
    /// SelectList.
    SelectList,
    /// Where.
    Where,
    /// JoinOn.
    JoinOn,
    /// Having.
    Having,
    /// GroupBy.
    GroupBy,
    /// OrderBy.
    OrderBy,
    /// Returning.
    Returning,
    /// Values.
    Values,
    /// SetClause.
    SetClause,
    /// Other.
    Other,
}

/// Use kind.
#[derive(Debug, Clone, PartialEq)]
pub enum UseKind {
    /// An operation on a guarded Text JSON leaf, independent of the base type.
    JsonLeaf { /// Original operation.
        kind: Box<UseKind> },
    /// Output.
    Output, // appears (bare) as an output column
    /// Equality with its direct counterpart on either side.
    Equality {
        /// Other.
        other: ValueSource,
    }, // blind = other   (either side)
    /// GroupBy.
    GroupBy,
    /// Count.
    Count, // COUNT(col)
    /// CountDistinct.
    CountDistinct,
    /// IsNull.
    IsNull,
    /// Comparison.
    Comparison, // <, >, <=, >=, <>, BETWEEN, IN-list with non-param, ANY/ALL
    /// Pattern.
    Pattern, // LIKE, ILIKE, SIMILAR, ~
    /// Order.
    Order, // ORDER BY, DISTINCT ON, window ORDER BY, MIN/MAX
    /// Function or operator call consuming this column.
    Function {
        /// Name.
        name: String,
    }, // any function/operator call taking it as input
    /// Cast.
    Cast,
    /// Aggregate call other than COUNT.
    Aggregate {
        /// Name.
        name: String,
    }, // any aggregate other than COUNT
    /// An operation not represented by another use kind.
    Other {
        /// What.
        what: &'static str,
    },
}

/// Column use.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnUse {
    /// Column.
    pub column: ColumnRef,
    /// Label.
    pub label: Label,
    /// Kind.
    pub kind: UseKind,
    /// Context.
    pub context: Context,
}

/// Output column.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputColumn {
    /// Name.
    pub name: String,
    /// Label.
    pub label: Label,
    /// Column supplying the output's policy and type. A blind output may use
    /// a representative column when all possible source columns share its domain and type.
    pub source: Option<ColumnRef>,
    /// Known database column type, if resolved.
    pub ty: Option<ColumnType>,
}

/// Write target.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteTarget {
    /// Column.
    pub column: ColumnRef,
    /// Label.
    pub label: Label,
    /// Value.
    pub value: ValueSource,
}

/// Statement kind.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementKind {
    /// Select.
    Select,
    /// A SELECT with explicit direct JSON extraction projections.
    JsonSelect {
        /// Output ordinals and their extraction treatment.
        outputs: Vec<(usize, OutputRewrite)>,
        /// SQL substitutions required to preserve JSON types.
        inputs: Vec<InputRewrite>,
    },
    /// Insert.
    Insert,
    /// Update.
    Update,
    /// Delete.
    Delete,
    /// Begin.
    Begin,
    /// Commit.
    Commit,
    /// Rollback.
    Rollback,
    /// Savepoint.
    Savepoint,
    /// ReleaseSavepoint.
    ReleaseSavepoint,
    /// RollbackToSavepoint.
    RollbackToSavepoint,
    /// A harmless session setting answered locally.
    SessionNoop {
        /// Command completion tag.
        tag: &'static str,
    },
    /// Explicit tokenization in the named domain.
    Tokenize {
        /// Domain.
        domain: DomainName,
        /// Arg.
        arg: ValueSource,
    },
    /// Refused.
    Refused(NyCode), // the lowering already knows it must refuse (e.g. DDL, COPY)
}

/// Resolved statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedStatement {
    /// Fixed upstream SQL for an allowlisted metadata statement.
    pub canonical_sql: Option<&'static str>,
    /// Kind.
    pub kind: StatementKind,
    /// Outputs.
    pub outputs: Vec<OutputColumn>,
    /// Every use of blind, masked, or tainted data anywhere in the statement.
    pub uses: Vec<ColumnUse>, // every use of a Blind/Masked/Tainted value, anywhere in the statement
    /// Writes.
    pub writes: Vec<WriteTarget>,
    /// Param count.
    pub param_count: u32,
    /// Parameters used outside direct blind equality/write counterparts; casts to the column type stay direct.
    pub plain_params: Vec<u32>, // every $n that appears anywhere OTHER than as the direct counterpart of a blind equality or the value of a blind write (casts of the param to the column type count as direct)
    /// This statement's byte span within a multi-statement simple query.
    pub source_span: Span, // this statement's span in a multi-statement simple query
}

/// Output rewrite.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputRewrite {
    /// Release an extracted JSON root through rebased path rules.
    JsonExtract {
        /// Rebased rules and default mode.
        filter: crate::policy::JsonColumnFilter,
        /// Original column JSON type.
        base: ColumnType,
        /// Render the filtered root with PostgreSQL text extraction semantics.
        text: bool,
        /// Preserve an empty extracted object only for an array element.
        keep_empty: bool,
    },
    /// Release a direct JSON read only through its path filter.
    JsonFilter {
        /// Compiled path rules and default mode.
        filter: crate::policy::JsonColumnFilter,
        /// Captured JSON database type.
        ty: ColumnType,
    },
    /// Pass.
    Pass,
    /// Tokenize this output in the named domain.
    Tokenize {
        /// Domain.
        domain: DomainName,
        /// Database type used for token conversion.
        ty: ColumnType,
    },
    /// Replace the output with a placeholder, or NULL when absent.
    Mask {
        /// Placeholder.
        placeholder: Option<String>,
    },
}
/// Input rewrite.
#[derive(Debug, Clone, PartialEq)]
pub enum InputRewrite {
    /// Replace a validated SQL expression span.
    ReplaceSpan {
        /// Original SQL expression extent.
        span: Span,
        /// Validated replacement SQL.
        text: String,
    },
    /// Decrypt a one-based parameter in a domain and column type.
    DecryptParam {
        /// Index.
        index: u32,
        /// Domain.
        domain: DomainName,
        /// Database type used for token conversion.
        ty: ColumnType,
    },
    /// Replace a literal at the given source span with its decoded value.
    DecryptLiteral {
        /// Span.
        span: Span,
        /// Domain.
        domain: DomainName,
        /// Database type used for token conversion.
        ty: ColumnType,
    },
}
/// Rewrite plan.
#[derive(Debug, Clone, PartialEq)]
pub struct RewritePlan {
    /// Outputs.
    pub outputs: Vec<OutputRewrite>,
    /// Inputs.
    pub inputs: Vec<InputRewrite>,
    /// Tokenize.
    pub tokenize: Option<(DomainName, ValueSource)>,
}

impl Label {
    /// Join expression labels conservatively.
    pub fn join(a: Self, b: Self) -> Self {
        if a == Self::Plain {
            b
        } else if b == Self::Plain || a == b {
            a
        } else {
            Self::Tainted
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_join_conservatively() {
        let a = Label::Blind(DomainName("a".into()));
        let b = Label::Blind(DomainName("b".into()));
        for x in [
            Label::Plain,
            a.clone(),
            b.clone(),
            Label::Masked,
            Label::Tainted,
        ] {
            assert_eq!(Label::join(Label::Plain, x.clone()), x);
            assert_eq!(Label::join(x.clone(), Label::Plain), x);
            assert_eq!(Label::join(x.clone(), x.clone()), x);
        }
        assert_eq!(Label::join(a.clone(), b), Label::Tainted);
        assert_eq!(Label::join(a, Label::Masked), Label::Tainted);
    }
}
