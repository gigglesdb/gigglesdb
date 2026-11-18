//! SQLSTATE identities shared by SQL and native frontends.
use crate::{CoreError, Error};

pub fn sqlstate(e: &CoreError) -> &'static str {
    use crate::relational::SqlError as S;
    match e {
        CoreError::Policy(Error::Authentication | Error::UnknownTask) => "28000",
        CoreError::Policy(Error::Revoked | Error::Expired | Error::Scope) => "42501",
        CoreError::Policy(Error::UncertainTime | Error::TimeRegression) => "55000",
        CoreError::Policy(Error::Budget) => "54000",
        CoreError::Policy(
            Error::ApprovalRequired | Error::InvalidApproval | Error::StalePreview,
        ) => "GD001",
        CoreError::Policy(Error::IdConflict) => "GD002",
        CoreError::Policy(
            Error::InvalidGrant
            | Error::TaskExists
            | Error::MissingRow
            | Error::DuplicateKey
            | Error::InvalidPredicate
            | Error::Overflow,
        ) => "22023",
        CoreError::Sql(S::Unsupported | S::Reserved)
        | CoreError::Unsupported
        | CoreError::EngineUnsupported { .. } => "0A000",
        CoreError::Sql(S::Syntax) => "42601",
        CoreError::Sql(S::UnknownTable) => "42P01",
        CoreError::Sql(S::UnknownColumn) => "42703",
        CoreError::Sql(S::Duplicate) => "23505",
        CoreError::Sql(S::Null) => "23502",
        CoreError::Sql(S::Type | S::Parameters) => "42804",
        CoreError::Sql(S::Limit | S::Unbounded) | CoreError::Limit => "54000",
        CoreError::Sql(S::Version | S::Conflict) => "40001",
        CoreError::Sql(S::StaleSchema) => "0A000",
        CoreError::Sql(S::Transaction) => "25000",
        CoreError::Sql(S::Aborted) => "25P02",
        CoreError::Sql(S::Overflow) => "22003",
        CoreError::Sql(S::StringDataRightTruncation) => "22001",
        CoreError::Sql(S::UndefinedFunction) => "42883",
        CoreError::Sql(S::InvalidParameterValue) => "22023",
        CoreError::Sql(S::InvalidDatetimeFormat) => "22007",
        CoreError::Sql(S::DatetimeFieldOverflow) => "22008",
        CoreError::Sql(S::IntervalFieldOverflow) => "22015",
        CoreError::Sql(S::AmbiguousFunction) => "42725",
        CoreError::Sql(S::GroupingError) => "42803",
        CoreError::Sql(S::IndeterminateDatatype) => "42P18",
        CoreError::Sql(S::AmbiguousParameter) => "42P08",
        CoreError::Sql(S::InvalidTextRepresentation) => "22P02",
        CoreError::Sql(S::SubstringError) => "22011",
        CoreError::Sql(S::CharacterNotInRepertoire) => "22021",
        CoreError::Sql(S::DuplicateTable) => "42P07",
        CoreError::Sql(S::DuplicateColumn) => "42701",
        CoreError::Sql(S::DuplicateObject) => "42710",
        CoreError::Sql(S::UndefinedObject) => "42704",
        CoreError::Sql(S::CheckViolation) => "23514",
        CoreError::Sql(S::ForeignKeyViolation) => "23503",
        CoreError::Sql(S::DependentObjects) => "2BP01",
        CoreError::Sql(S::GeneratedAlways) => "428C9",
        CoreError::Sql(S::ObjectNotInPrerequisiteState) => "55000",
        CoreError::Sql(S::SequenceLimit) => "2200H",
        CoreError::Sql(S::NoActiveTransaction) => "25P01",
        CoreError::Sql(S::InvalidSavepoint) => "3B001",
        CoreError::Sql(S::InvalidForeignKey) => "42830",
        CoreError::Sql(S::InvalidSchema) => "3F000",
        CoreError::DuplicateSchema => "42P06",
        CoreError::Sql(S::DivisionByZero) => "22012",
        CoreError::Sql(S::InvalidEscapeSequence) => "22025",
        CoreError::Sql(S::InvalidTableDefinition) => "42P16",
        CoreError::Sql(S::RestrictViolation) => "23001",
        CoreError::Sql(S::WrongObjectType) => "42809",
        CoreError::Sql(S::NullValueNotAllowed) => "22004",
        CoreError::Sql(S::CannotCoerce) => "42846",
        CoreError::Sql(S::BadCopyFileFormat) => "22P04",
        CoreError::Sql(S::InvalidTimeZoneDisplacementValue) => "22009",
        CoreError::Sql(S::AmbiguousColumn) => "42702",
        CoreError::Sql(S::InvalidRowCountInLimitClause) => "2201W",
        CoreError::Sql(S::InvalidRowCountInResultOffsetClause) => "2201X",
        CoreError::Sql(S::CardinalityViolation) => "21000",
        CoreError::Sql(S::InvalidColumnReference) => "42P10",
        CoreError::Sql(S::CollationMismatch) => "42P21",
        CoreError::Sql(S::IndeterminateCollation) => "42P22",
        CoreError::Task(e) => e.sqlstate(),
        CoreError::Sql(S::InvalidRecursion) => "42P19",
        CoreError::Sql(S::DuplicateAlias) => "42712",
        CoreError::Sql(S::WindowingError) => "42P20",
        CoreError::Sql(S::InvalidFrameOffset) => "22013",
        CoreError::Sql(S::InvalidNtileArgument) => "22014",
        CoreError::Sql(S::InvalidNthValueArgument) => "22016",
        CoreError::Sql(S::InvalidTablesampleArgument) => "2202H",
        CoreError::Cancelled => "57014",
        CoreError::Busy => "53300",
        CoreError::LockNotAvailable => "55P03",
        CoreError::Deadlock => "40P01",
        CoreError::WriteFenced { .. } => "25006",
        CoreError::Storage(_) | CoreError::Fenced => "08007",
        CoreError::Corrupt(_) | CoreError::Exists => "XX000",
        CoreError::TempFile { disk_full: true, .. } => "53100",
        CoreError::TempFile { disk_full: false, .. } => "58030",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational;

    #[test]
    fn sqlstate_time_zone_displacement() {
        assert_eq!(
            sqlstate(&CoreError::Sql(
                relational::SqlError::InvalidTimeZoneDisplacementValue
            )),
            "22009"
        );
    }
}
