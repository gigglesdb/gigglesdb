//! The protected-statement surface of a task session (GD-0061 R3-207, design-2
//! section 5 with coordinator ruling R2): operation IDs, preview, apply and
//! outcome lookup. The words are parsed only when the session is bound to a
//! task, so ordinary sessions see no grammar change.
//!
//! | Form                                                       | Class         |
//! |------------------------------------------------------------|---------------|
//! | `OPERATION '<id>' UPDATE ...`                              | Update        |
//! | `PREVIEW UPDATE ...`                                       | Preview       |
//! | `OPERATION '<id>' APPLY CHANGE <change_id> DIGEST '<hex>'` | Apply         |
//! | `SHOW OPERATION '<id>'`                                    | OutcomeLookup |
//!
//! [`protected_seam`] is the one place the Phase 6 and 7 items fill in. The
//! direct write runs `task::protected_commit` (R3-615), the outcome lookup
//! `task::outcome` (R3-617), and preview and apply (R3-707) `task::preview`
//! and the protected commit of a stored preview (`agent::preview`). R3-626
//! added `Update` and `OutcomeLookup` to `task::admit::ENABLED` (design-2
//! section 4) and R3-707 `Preview` and `Apply`, so every form runs over the
//! wire. A digest that is not 64 hex digits is 22023 (D7.1's form takes a
//! `'<64 hex>'` value); every other malformed form is 42601.

use giggles_core::{
    CoreError,
    operations as ops,
    relational::{ColumnDef, DataType, Description, Prepared, SqlError, SqlResult, SqlRow, Value},
    task::{Actor, Outcome, ProtectedRequest, StatementClass, TaskError},
};
use std::sync::{Arc, Mutex};

/// The row bound of a native update's intent: `one` is exactly one row,
/// `many .. at most n` at most `n` (R3-708's `intent_bound`, with the same
/// native errors).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowLimit {
    One,
    AtMost(u64),
}

impl RowLimit {
    /// `NotFound` or `AmbiguousMatch` for `One`, `TooManyMatches` past `n`.
    pub(crate) fn check(self, rows: u64) -> Result<(), ops::Error> {
        match self {
            RowLimit::One if rows == 0 => Err(ops::Error::NotFound),
            RowLimit::One if rows > 1 => Err(ops::Error::AmbiguousMatch),
            RowLimit::AtMost(cap) if rows > cap => Err(ops::Error::TooManyMatches),
            _ => Ok(()),
        }
    }
}

/// A native update's [`RowLimit`], checked by the protected commit on the
/// real affected count of the attempt that would commit
/// (`task::protected_commit_checked`). A refusal records nothing; the native
/// error is kept here for the caller, which answers it as a native error.
#[derive(Clone, Debug)]
pub(crate) struct RowBound {
    limit: RowLimit,
    refused: Arc<Mutex<Option<ops::Error>>>,
}

impl RowBound {
    pub(crate) fn new(limit: RowLimit) -> Self {
        RowBound { limit, refused: Arc::default() }
    }
    /// The commit's check: a refusal leaves its native error in `self` and
    /// returns 21000 (cardinality_violation) to the commit.
    fn check(&self, rows: usize) -> Result<(), TaskError> {
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        self.limit.check(rows).map_err(|error| {
            *self.refused.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(error);
            TaskError::Sql(SqlError::CardinalityViolation)
        })
    }
    /// The native error of the commit's refusal, if it refused.
    pub(crate) fn refused(&self) -> Option<ops::Error> {
        self.refused.lock().unwrap_or_else(|poison| poison.into_inner()).take()
    }
}

/// The longest operation ID, in bytes (`task::ledger::MAX_OPERATION_ID`).
pub const MAX_OPERATION_ID: usize = 128;

/// A client-chosen operation ID: 1..=128 bytes of `[A-Za-z0-9._:-]`, so the same
/// token fits the HTTP and gRPC fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(id: &str) -> Option<Self> {
        ((1..=MAX_OPERATION_ID).contains(&id.len())
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-')))
        .then(|| Self(id.to_owned()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// The same ID as core's ledger key. Every server ID is a valid core ID
    /// (core accepts any 1..=128 bytes); a mismatch still fails closed.
    pub(crate) fn to_core(&self) -> Result<giggles_core::task::OperationId, CoreError> {
        giggles_core::task::OperationId::new(self.0.as_bytes())
            .ok_or(CoreError::Task(TaskError::OperationIdRequired))
    }
}

/// The leading words of a protected form, as the lexer found them. For
/// `Direct` and `Preview` the wrapped `UPDATE` is the text returned beside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtectedHead {
    Direct { op: OperationId },
    Preview,
    Apply { op: OperationId, change: u64, digest: [u8; 32] },
    Outcome { op: OperationId },
}

impl ProtectedHead {
    pub fn class(&self) -> StatementClass {
        match self {
            ProtectedHead::Direct { .. } => StatementClass::Update,
            ProtectedHead::Preview => StatementClass::Preview,
            ProtectedHead::Apply { .. } => StatementClass::Apply,
            ProtectedHead::Outcome { .. } => StatementClass::OutcomeLookup,
        }
    }
}

/// A protected statement after admission, ready for [`Session::execute_protected`].
///
/// [`Session::execute_protected`]: crate::Session::execute_protected
/// `Direct` keeps the wrapped `UPDATE`'s text exactly as received (the suffix
/// of the statement after the `OPERATION '<id>'` prefix): it is fingerprinted
/// with the parameters, so a resend under the same ID replays.
/// `Preview` keeps the wrapped `UPDATE`'s text as well: `task::preview`
/// prepares and stores it (D7.2), and `prepared` is what was admitted and
/// described.
#[derive(Clone, Debug)]
pub(crate) enum ProtectedStatement {
    Direct {
        op: OperationId,
        prepared: Box<Prepared>,
        text: Arc<str>,
        /// The native intent's bound; `None` for SQL (`OPERATION '<id>' UPDATE`).
        bound: Option<RowBound>,
    },
    Preview {
        #[allow(dead_code, reason = "kept as admitted and described; task::preview re-prepares the text")]
        prepared: Box<Prepared>,
        text: Arc<str>,
    },
    Apply { op: OperationId, preview: u64, digest: [u8; 32] },
    Outcome { op: OperationId },
}

impl ProtectedStatement {
    pub(crate) fn class(&self) -> StatementClass {
        match self {
            ProtectedStatement::Direct { .. } => StatementClass::Update,
            ProtectedStatement::Preview { .. } => StatementClass::Preview,
            ProtectedStatement::Apply { .. } => StatementClass::Apply,
            ProtectedStatement::Outcome { .. } => StatementClass::OutcomeLookup,
        }
    }
    /// The CommandComplete word: the two write forms answer `UPDATE n`.
    pub(crate) fn command(&self) -> &'static str {
        match self {
            ProtectedStatement::Direct { .. } | ProtectedStatement::Apply { .. } => "UPDATE",
            ProtectedStatement::Preview { .. } | ProtectedStatement::Outcome { .. } => "SELECT",
        }
    }
}

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        id: 0,
        name: name.into(),
        data_type,
        not_null: false,
    }
}

/// The result shape of each form (section 5; ruling R2 names the preview's ID
/// `change_id`). A direct write has the wrapped `UPDATE`'s own shape (its
/// RETURNING list), so `Direct` takes that description; `parameters` of a
/// `Preview` are those of its wrapped `UPDATE` as well.
pub fn description(head: &ProtectedHead, wrapped: Option<&Description>) -> Description {
    let parameters = wrapped.map(|d| d.parameters.clone()).unwrap_or_default();
    let columns = match head {
        ProtectedHead::Direct { .. } => {
            return wrapped.cloned().unwrap_or(Description {
                type_oids: vec![],
                typmods: vec![],
                parameters: vec![],
                columns: vec![],
            });
        }
        ProtectedHead::Apply { .. } => vec![],
        ProtectedHead::Preview => vec![
            column("change_id", DataType::BigInt),
            column("digest", DataType::Text),
            column("row_count", DataType::BigInt),
            column("expires_at", DataType::TimestampTz),
        ],
        ProtectedHead::Outcome { .. } => vec![
            column("operation_id", DataType::Text),
            column("decision", DataType::Text),
            column("row_count", DataType::BigInt),
            column("receipt_seq", DataType::BigInt),
            column("effects_digest", DataType::Text),
        ],
    };
    Description {
        type_oids: vec![],
        typmods: vec![-1; columns.len()],
        parameters,
        columns,
    }
}

/// The seam the Phase 6 and 7 bodies fill in. `Direct` runs
/// `task::protected_commit` as the session's executor (R3-615), `Outcome`
/// runs `task::outcome` (R3-617), and `Preview` and `Apply` (R3-707) run
/// `task::preview` and the protected commit of the stored preview
/// (`agent::preview`). It runs inside `Session::execute_protected`, after
/// admission and with the session's `in_protected` flag set.
pub(crate) fn protected_seam(
    session: &mut crate::Session,
    statement: &ProtectedStatement,
    params: &[Value],
) -> Result<SqlResult, CoreError> {
    match statement {
        ProtectedStatement::Direct { op, prepared, text, bound } => {
            direct(session, op, prepared, text, params, bound.as_ref())
        }
        ProtectedStatement::Outcome { op } => show_outcome(session, op),
        ProtectedStatement::Preview { text, .. } => super::preview::preview(session, text, params),
        ProtectedStatement::Apply {
            op,
            preview,
            digest,
        } => super::preview::apply(session, op, *preview, *digest),
    }
}

/// `OPERATION '<id>' UPDATE ...`: one protected commit. The reply is
/// `UPDATE n` with the receipt's row count; a replayed operation answers the
/// stored receipt's count. A `TaskError` keeps its own SQLSTATE.
fn direct(
    session: &mut crate::Session,
    op: &OperationId,
    prepared: &Prepared,
    text: &Arc<str>,
    params: &[Value],
    bound: Option<&RowBound>,
) -> Result<SqlResult, CoreError> {
    let ctx = session
        .task()
        .cloned()
        .ok_or(CoreError::Policy(giggles_core::Error::Scope))?;
    // protected_commit returns a receipt, not rows: a RETURNING list would be
    // silently dropped, so it is refused before anything runs.
    if !session.describe(prepared)?.columns.is_empty() {
        return Err(CoreError::Unsupported);
    }
    let op = op.to_core()?;
    let request = ProtectedRequest::Direct {
        statement: prepared.clone(),
        params: params.to_vec(),
        text: text.clone(),
    };
    let actor = Actor::Executor(&ctx);
    let receipt = match bound {
        None => giggles_core::task::protected_commit(&mut session.sql, actor, &op, request),
        Some(bound) => giggles_core::task::protected_commit_checked(
            &mut session.sql,
            actor,
            &op,
            request,
            &|rows| bound.check(rows),
        ),
    }
    .map_err(CoreError::from)?;
    Ok(SqlResult {
        columns: Vec::new(),
        rows: Vec::new(),
        affected: u64::from(receipt.rows_affected),
    })
}

/// `SHOW OPERATION '<id>'`: the stored outcome of the ID under the session's
/// task, as one row of [`description`]'s columns (`operation_id`, `decision`,
/// `row_count`, `receipt_seq`, `effects_digest`), or no row when the ID is
/// unknown. `decision` is `committed`, `denied` or `budget_exceeded`; the last
/// three columns are NULL for a denial. A revoked task is 28T01; an expired
/// one can still look up its outcomes.
fn show_outcome(session: &mut crate::Session, op: &OperationId) -> Result<SqlResult, CoreError> {
    let ctx = session
        .task()
        .cloned()
        .ok_or(CoreError::Policy(giggles_core::Error::Scope))?;
    let core_op = op.to_core()?;
    let found = giggles_core::task::outcome(&session.sql, &ctx, &core_op).map_err(CoreError::from)?;
    let rows = found
        .map(|outcome| {
            let (decision, row_count, seq, digest) = match outcome {
                Outcome::Committed(receipt) => (
                    "committed",
                    Value::Int(i64::from(receipt.rows_affected)),
                    Value::Int(i64::try_from(receipt.seq).unwrap_or(i64::MAX)),
                    Value::Text(receipt.effects_digest.iter().map(|b| format!("{b:02x}")).collect()),
                ),
                Outcome::Denied(_) => ("denied", Value::Null, Value::Null, Value::Null),
                Outcome::BudgetExceeded(_) => {
                    ("budget_exceeded", Value::Null, Value::Null, Value::Null)
                }
            };
            SqlRow {
                values: vec![
                    Value::Text(op.as_str().to_owned()),
                    Value::Text(decision.to_owned()),
                    row_count,
                    seq,
                    digest,
                ],
                version: 0,
            }
        })
        .into_iter()
        .collect::<Vec<_>>();
    Ok(SqlResult {
        columns: ["operation_id", "decision", "row_count", "receipt_seq", "effects_digest"]
            .map(String::from)
            .to_vec(),
        affected: rows.len() as u64,
        rows,
    })
}

/// Test-only route to the seam for classes admission keeps out of
/// `task::admit::ENABLED` (since R3-707 and R3-315 only the classes a task
/// never runs, such as `Insert`, used to reach core's own refusals):
/// classes listed here are admitted on this thread by `Session::admit`.
/// Compiled into unit tests only.
#[cfg(test)]
pub(crate) mod test_route {
    use giggles_core::task::StatementClass;
    use std::cell::RefCell;

    thread_local! {
        static ENABLED: RefCell<Vec<StatementClass>> = const { RefCell::new(Vec::new()) };
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn enable(classes: &[StatementClass]) {
        ENABLED.with(|enabled| *enabled.borrow_mut() = classes.to_vec());
    }

    pub(crate) fn enabled(class: StatementClass) -> bool {
        ENABLED.with(|enabled| enabled.borrow().contains(&class))
    }
}

const SYNTAX: &str = "42601";
/// A digest literal that is not exactly 64 hex digits (invalid_parameter_value).
const BAD_DIGEST: &str = "22023";

/// A lexer over the leading words; it never parses the wrapped statement.
struct Lexer<'a> {
    sql: &'a str,
    at: usize,
}

impl<'a> Lexer<'a> {
    fn skip_space(&mut self) {
        let b = self.sql.as_bytes();
        while self.at < b.len() && b[self.at].is_ascii_whitespace() {
            self.at += 1;
        }
    }
    /// The next word, without consuming it, and where it ends.
    fn peek_word(&mut self) -> Option<(&'a str, usize)> {
        self.skip_space();
        let b = self.sql.as_bytes();
        let start = self.at;
        let mut end = start;
        while end < b.len() && (b[end].is_ascii_alphanumeric() || b[end] == b'_' || b[end] == b'$')
        {
            end += 1;
        }
        (end > start && b[start].is_ascii_alphabetic()).then(|| (&self.sql[start..end], end))
    }
    fn word(&mut self, expected: &str) -> bool {
        match self.peek_word() {
            Some((word, end)) if word.eq_ignore_ascii_case(expected) => {
                self.at = end;
                true
            }
            _ => false,
        }
    }
    /// A standard string literal (`'...'`, a doubled quote is one quote), as
    /// the boundary lexer at `pgwire_simple.rs::split` reads one. No `E''`,
    /// dollar quoting or adjacent-literal continuation.
    fn literal(&mut self) -> Result<String, &'static str> {
        self.skip_space();
        let b = self.sql.as_bytes();
        if b.get(self.at) != Some(&b'\'') {
            return Err(SYNTAX);
        }
        let mut at = self.at + 1;
        let mut value = Vec::new();
        loop {
            match b.get(at) {
                None => return Err(SYNTAX),
                Some(b'\'') if b.get(at + 1) == Some(&b'\'') => {
                    value.push(b'\'');
                    at += 2;
                }
                Some(b'\'') => {
                    at += 1;
                    break;
                }
                Some(&c) => {
                    value.push(c);
                    at += 1;
                }
            }
        }
        self.at = at;
        String::from_utf8(value).map_err(|_| SYNTAX)
    }
    fn operation_id(&mut self) -> Result<OperationId, &'static str> {
        OperationId::new(&self.literal()?).ok_or(SYNTAX)
    }
    fn unsigned(&mut self) -> Result<u64, &'static str> {
        self.skip_space();
        let b = self.sql.as_bytes();
        let start = self.at;
        while self.at < b.len() && b[self.at].is_ascii_digit() {
            self.at += 1;
        }
        if self.at == start
            || b.get(self.at)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'.')
        {
            return Err(SYNTAX);
        }
        self.sql[start..self.at].parse().map_err(|_| SYNTAX)
    }
    /// Only whitespace and one optional `;` may follow.
    fn end(&mut self) -> Result<(), &'static str> {
        self.skip_space();
        if self.sql.as_bytes().get(self.at) == Some(&b';') {
            self.at += 1;
            self.skip_space();
        }
        if self.at == self.sql.len() {
            Ok(())
        } else {
            Err(SYNTAX)
        }
    }
    /// The wrapped statement must be an `UPDATE`; it starts here.
    fn wrapped_update(&mut self) -> Result<&'a str, &'static str> {
        let Some((word, _)) = self.peek_word() else {
            return Err(SYNTAX);
        };
        if word.eq_ignore_ascii_case("update") {
            Ok(&self.sql[self.at..])
        } else {
            Err(SYNTAX)
        }
    }
}

/// Recognizes the protected forms. `Ok(None)` is any other statement, which
/// takes the ordinary task path; a malformed form is `42601`. For `Direct` and
/// `Preview` the second value is the wrapped `UPDATE`, a suffix of `sql`, so
/// its `$n` numbering is unchanged; it is empty for the other forms.
pub fn parse(sql: &str) -> Result<Option<(ProtectedHead, &str)>, &'static str> {
    let mut lexer = Lexer { sql, at: 0 };
    let Some((first, _)) = lexer.peek_word() else {
        return Ok(None);
    };
    if first.eq_ignore_ascii_case("operation") {
        lexer.word("operation");
        let op = lexer.operation_id()?;
        if lexer.word("apply") {
            if !lexer.word("change") {
                return Err(SYNTAX);
            }
            let change = lexer.unsigned()?;
            if !lexer.word("digest") {
                return Err(SYNTAX);
            }
            let text = lexer.literal()?;
            lexer.end()?;
            // The value is checked once the whole form has parsed.
            let mut digest = [0u8; 32];
            if text.len() != 64 || hex::decode_to_slice(&text, &mut digest).is_err() {
                return Err(BAD_DIGEST);
            }
            return Ok(Some((ProtectedHead::Apply { op, change, digest }, "")));
        }
        let wrapped = lexer.wrapped_update()?;
        return Ok(Some((ProtectedHead::Direct { op }, wrapped)));
    }
    if first.eq_ignore_ascii_case("preview") {
        lexer.word("preview");
        // R3-804: `PREVIEW UNDO ..` is an undo form, not a protected one; the
        // task path refuses it as `Other`.
        if lexer
            .peek_word()
            .is_some_and(|(word, _)| word.eq_ignore_ascii_case("undo"))
        {
            return Ok(None);
        }
        let wrapped = lexer.wrapped_update()?;
        return Ok(Some((ProtectedHead::Preview, wrapped)));
    }
    if first.eq_ignore_ascii_case("show") {
        lexer.word("show");
        if !lexer.word("operation") {
            return Ok(None);
        }
        let op = lexer.operation_id()?;
        lexer.end()?;
        return Ok(Some((ProtectedHead::Outcome { op }, "")));
    }
    Ok(None)
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "statement_tests.rs"]
mod seam_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn op(id: &str) -> OperationId {
        OperationId::new(id).unwrap()
    }

    #[test]
    fn forms() {
        assert_eq!(
            parse("OPERATION 'a1' UPDATE shipments SET status = $1 WHERE id = $2").unwrap(),
            Some((
                ProtectedHead::Direct { op: op("a1") },
                "UPDATE shipments SET status = $1 WHERE id = $2"
            ))
        );
        assert_eq!(
            parse("  preview\n\tupdate shipments SET status = 'x'").unwrap(),
            Some((ProtectedHead::Preview, "update shipments SET status = 'x'"))
        );
        let hex = "ab".repeat(32);
        assert_eq!(
            parse(&format!("OPERATION 'a2' APPLY CHANGE 17 DIGEST '{hex}' ;")).unwrap(),
            Some((
                ProtectedHead::Apply {
                    op: op("a2"),
                    change: 17,
                    digest: [0xab; 32]
                },
                ""
            ))
        );
        assert_eq!(
            parse("SHOW OPERATION 'a2';").unwrap(),
            Some((ProtectedHead::Outcome { op: op("a2") }, ""))
        );
        for other in ["SHOW search_path", "SELECT 1", "UPDATE shipments SET status = 'x'", "", ";"] {
            assert_eq!(parse(other), Ok(None), "{other}");
        }
    }

    #[test]
    fn malformed_forms_are_syntax_errors() {
        let hex = "ab".repeat(32);
        for bad in [
            "OPERATION 'a1' SELECT 1".to_owned(),
            "OPERATION 'a1'".to_owned(),
            "OPERATION a1 UPDATE shipments SET status = 'x'".to_owned(),
            "OPERATION '' UPDATE shipments SET status = 'x'".to_owned(),
            "OPERATION 'a b' UPDATE shipments SET status = 'x'".to_owned(),
            "OPERATION 'it''s' UPDATE shipments SET status = 'x'".to_owned(),
            format!("OPERATION '{}' UPDATE shipments SET status = 'x'", "a".repeat(129)),
            "OPERATION 'a1 UPDATE shipments".to_owned(),
            "OPERATION 'a1' OPERATION 'a2' UPDATE shipments SET status = 'x'".to_owned(),
            "PREVIEW SELECT 1".to_owned(),
            "PREVIEW".to_owned(),
            "PREVIEW updates SET x = 1".to_owned(),
            format!("OPERATION 'a2' APPLY CHANGE -1 DIGEST '{hex}'"),
            format!("OPERATION 'a2' APPLY CHANGE 1x DIGEST '{hex}'"),
            format!("OPERATION 'a2' APPLY CHANGE 99999999999999999999 DIGEST '{hex}'"),
            format!("OPERATION 'a2' APPLY PREVIEW 1 DIGEST '{hex}'"),
            format!("OPERATION 'a2' APPLY CHANGE 1 DIGEST '{hex}' trailing"),
            "OPERATION 'a2' APPLY CHANGE 1 DIGEST 'abc' trailing".to_owned(),
            "SHOW OPERATION a2".to_owned(),
            "SHOW OPERATION 'a2' extra".to_owned(),
        ] {
            assert_eq!(parse(&bad), Err("42601"), "{bad}");
        }
        // A well-formed APPLY whose digest is not 64 hex digits is 22023.
        for bad in ["abc".to_owned(), "zz".repeat(32), String::new(), format!("{hex}0")] {
            let sql = format!("OPERATION 'a2' APPLY CHANGE 1 DIGEST '{bad}'");
            assert_eq!(parse(&sql), Err("22023"), "{sql}");
        }
        assert!(parse(&format!("OPERATION '{}' UPDATE t SET a = 1", "a".repeat(128))).is_ok());
        assert!(parse("OPERATION 'A-z.0_9:x' UPDATE t SET a = 1").is_ok());
    }

    #[test]
    fn descriptions() {
        let preview = description(&ProtectedHead::Preview, None);
        let names: Vec<_> = preview.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["change_id", "digest", "row_count", "expires_at"]);
        let outcome = description(&ProtectedHead::Outcome { op: op("a") }, None);
        let names: Vec<_> = outcome.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["operation_id", "decision", "row_count", "receipt_seq", "effects_digest"]
        );
        let apply = ProtectedHead::Apply {
            op: op("a"),
            change: 1,
            digest: [0; 32],
        };
        assert!(description(&apply, None).columns.is_empty());
    }
}
