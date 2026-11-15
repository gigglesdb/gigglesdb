//! Fixed, data-free gateway refusals.
/// Stable gateway refusal code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NyCode {
    /// token used outside its domain.
    TokenOutsideDomain,
    /// operation not allowed on a blind column.
    BlindOperation,
    /// token not valid in this task.
    TokenNotValid,
    /// statement could not be fully analysed.
    NotAnalysable,
    /// masked column used in an expression.
    MaskedExpression,
    /// statement kind not allowed through the gateway.
    StatementNotAllowed,
    /// tokenize limit reached for this task.
    TokenizeLimit,
    /// schema snapshot is stale.
    SchemaStale,
    /// read log unavailable.
    ReadLogUnavailable,
    /// protocol feature not allowed through the gateway.
    ProtocolNotAllowed,
}
impl NyCode {
    /// Every gateway refusal code in SQLSTATE order.
    pub const ALL: [NyCode; 10] = [
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
        }
    }
    /// Return a fixed message safe to show agents.
    pub fn message(self) -> &'static str {
        match self {
            Self::TokenOutsideDomain => "token used outside its domain",
            Self::BlindOperation => "operation not allowed on a blind column",
            Self::TokenNotValid => "token not valid in this task",
            Self::NotAnalysable => "statement could not be fully analysed",
            Self::MaskedExpression => "masked column used in an expression",
            Self::StatementNotAllowed => "statement kind not allowed through the gateway",
            Self::TokenizeLimit => "tokenize limit reached for this task",
            Self::SchemaStale => "schema snapshot is stale",
            Self::ReadLogUnavailable => "read log unavailable",
            Self::ProtocolNotAllowed => "protocol feature not allowed through the gateway",
        }
    }
}
/// A refusal carrying only a fixed code and static internal hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Agent-visible refusal code.
    pub code: NyCode,
    /// Static internal hint, never shown to agents and never containing data.
    pub reason: &'static str,
}
impl Refusal {
    /// Construct a refusal using a data-free static hint.
    pub fn new(code: NyCode, reason: &'static str) -> Self {
        Self { code, reason }
    }
}
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code.message())
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
    fn display_omits_internal_hint() {
        assert_eq!(
            Refusal::new(NyCode::BlindOperation, "internal hint").to_string(),
            NyCode::BlindOperation.message()
        );
    }
}
