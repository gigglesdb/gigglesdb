//! Fixed, data-free gateway refusals.
/// Stable gateway refusal code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NyCode {
    /// token used outside its domain.
    TokenOutsideDomain,
    /// operation not allowed on a blind column.
    BlindOperation,
    /// token not valid in this session.
    TokenNotValid,
    /// statement could not be fully analysed.
    NotAnalysable,
    /// masked column used in an expression.
    MaskedExpression,
    /// statement kind not allowed through the gateway.
    StatementNotAllowed,
    /// tokenize limit reached.
    TokenizeLimit,
    /// schema snapshot is stale.
    SchemaStale,
    /// read log unavailable.
    ReadLogUnavailable,
    /// protocol feature not allowed through the gateway.
    ProtocolNotAllowed,
    /// access session ended.
    SessionEnded,
    /// write outcome unknown.
    WriteOutcomeUnknown,
    /// Per-pod serving lease lapsed.
    ServingUnavailable,
}
impl NyCode {
    /// Every gateway refusal code in SQLSTATE order.
    pub const ALL: [NyCode; 13] = [
        Self::TokenOutsideDomain,
        Self::BlindOperation,
        Self::TokenNotValid,
        Self::NotAnalysable,
        Self::MaskedExpression,
        Self::StatementNotAllowed,
        Self::TokenizeLimit,
        Self::SchemaStale,
        Self::ReadLogUnavailable,
        Self::ProtocolNotAllowed,
        Self::SessionEnded,
        Self::WriteOutcomeUnknown,
        Self::ServingUnavailable,
    ];
    /// Return the five-character gateway SQLSTATE.
    pub fn sqlstate(self) -> &'static str {
        match self {
            Self::TokenOutsideDomain => "NY001",
            Self::BlindOperation => "NY002",
            Self::TokenNotValid => "NY003",
            Self::NotAnalysable => "NY004",
            Self::MaskedExpression => "NY005",
            Self::StatementNotAllowed => "NY006",
            Self::TokenizeLimit => "NY007",
            Self::SchemaStale => "NY008",
            Self::ReadLogUnavailable => "NY009",
            Self::ProtocolNotAllowed => "NY010",
            Self::SessionEnded => "NY011",
            Self::WriteOutcomeUnknown => "NY012",
            Self::ServingUnavailable => "NY013",
        }
    }
    /// Return a fixed message safe to show agents.
    pub fn message(self) -> &'static str {
        match self {
            Self::TokenOutsideDomain => "token used outside its domain",
            Self::BlindOperation => "operation not allowed on a blind column",
            Self::TokenNotValid => "token not valid in this session",
            Self::NotAnalysable => "statement could not be fully analysed",
            Self::MaskedExpression => "masked column used in an expression",
            Self::StatementNotAllowed => "statement kind not allowed through the gateway",
            Self::TokenizeLimit => "tokenize limit reached",
            Self::SchemaStale => "schema snapshot is stale",
            Self::ReadLogUnavailable => "read log unavailable",
            Self::ProtocolNotAllowed => "protocol feature not allowed through the gateway",
            Self::SessionEnded => "access session ended",
            Self::WriteOutcomeUnknown => "write outcome unknown",
            Self::ServingUnavailable => "serving lease lapsed",
        }
    }
}
pub(crate) const JSON_GROUP_BLIND: &str =
    "GROUP BY, ORDER BY and DISTINCT require a JSON show policy for the leaf. The path is blind";
pub(crate) const JSON_GROUP_MASKED: &str =
    "GROUP BY, ORDER BY and DISTINCT require a JSON show policy for the leaf. The path is masked";
pub(crate) const JSON_GROUP_UNLISTED: &str =
    "GROUP BY, ORDER BY and DISTINCT require a JSON show policy for the leaf. The path is unlisted";
pub(crate) const JSON_GROUP_NOT_LEAF: &str =
    "GROUP BY, ORDER BY and DISTINCT require a single JSON show rule covering the leaf";

/// A refusal carrying a fixed code and a static, data-free reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Agent-visible refusal code.
    pub code: NyCode,
    /// Static, data-free reason. Only explicitly public messages reach agents.
    pub reason: &'static str,
}
impl Refusal {
    /// Construct a refusal using a data-free static hint.
    pub fn new(code: NyCode, reason: &'static str) -> Self {
        Self { code, reason }
    }

    /// Return the fixed public message, including allowlisted policy details.
    pub(crate) fn message(&self) -> &'static str {
        match (self.code, self.reason) {
            (
                NyCode::MaskedExpression,
                JSON_GROUP_BLIND | JSON_GROUP_MASKED | JSON_GROUP_UNLISTED | JSON_GROUP_NOT_LEAF,
            ) => self.reason,
            _ => self.code.message(),
        }
    }
}
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for Refusal {}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn codes_are_unique_and_well_formed() {
        let mut seen = std::collections::HashSet::new();
        for code in NyCode::ALL {
            assert_eq!(code.sqlstate().len(), 5);
            assert!(code.sqlstate().starts_with("NY"));
            assert!(seen.insert(code.sqlstate()));
            assert!(!code.message().is_empty());
        }
    }
    #[test]
    fn session_and_write_outcome_codes() {
        assert!(NyCode::ALL.contains(&NyCode::SessionEnded));
        assert!(NyCode::ALL.contains(&NyCode::WriteOutcomeUnknown));
        assert_eq!(NyCode::SessionEnded.sqlstate(), "NY011");
        assert_eq!(NyCode::SessionEnded.message(), "access session ended");
        assert_eq!(NyCode::WriteOutcomeUnknown.sqlstate(), "NY012");
        assert_eq!(
            NyCode::WriteOutcomeUnknown.message(),
            "write outcome unknown"
        );
    }
    #[test]
    fn display_omits_internal_hint() {
        assert_eq!(
            Refusal::new(NyCode::BlindOperation, "internal hint").to_string(),
            NyCode::BlindOperation.message()
        );
    }
}
