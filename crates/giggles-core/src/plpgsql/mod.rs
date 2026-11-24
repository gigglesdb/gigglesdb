//! PL/pgSQL compilation and trigger execution. SQL runs through the caller's SPI.
use crate::CoreError;
use crate::relational::{DataType, SqlDiagnosticScope, Value};
use std::sync::Arc;

mod compiler;
mod lexer;
mod runtime;
mod sql;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct CompiledFunction {
    signature: Arc<FunctionSignature>,
    block: Arc<Block>,
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionSignature {
    /// Display name used in PostgreSQL-style diagnostics.
    pub name: String,
    pub kind: FunctionKind,
    pub arguments: Vec<FunctionArgument>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FunctionKind {
    Trigger,
    #[allow(dead_code)] // Reserved for the interpreter's ordinary routine entry point.
    Routine,
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionArgument {
    /// The caller supplies the normalized name, when one was declared.
    pub name: Option<String>,
    #[allow(dead_code, reason = "Ordinary routine execution will use the parameter type")]
    pub data_type: DataType,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PlCompileError {
    pub sqlstate: &'static str,
    pub message: String,
    pub position: Option<usize>,
    pub hint: Option<String>,
}

pub(crate) fn compile(
    body: &str,
    sig: &FunctionSignature,
) -> Result<CompiledFunction, PlCompileError> {
    // Declaration types and embedded SQL use parsers that publish diagnostics
    // for their caller. Keep those probes local to the body compilation.
    let _diagnostics = SqlDiagnosticScope::enter();
    compiler::compile(body, sig)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpiParam {
    pub data_type: DataType,
    pub value: Value,
    /// Exact PostgreSQL type when the engine uses a wider carrier (name/oid).
    pub type_oid: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpiColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Default)]
pub(crate) struct SpiResult {
    pub columns: Vec<SpiColumn>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: u64,
}

#[derive(Debug)]
pub(crate) struct SpiError {
    pub error: CoreError,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

pub(crate) trait Spi {
    fn execute(&mut self, sql: &str, params: &[SpiParam]) -> Result<SpiResult, SpiError>;

    /// Receives the notices from this invocation, in order, even if it failed.
    /// The executor can retain them for delivery alongside the trigger result.
    fn notices(&mut self, _notices: Vec<PlNotice>) {}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code, reason = "The trigger-firing stack supplies the timing")]
pub(crate) enum TriggerWhen {
    Before,
    After,
    InsteadOf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TriggerLevel {
    #[allow(dead_code, reason = "The trigger-firing stack supplies the level")]
    Row,
    Statement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code, reason = "The trigger-firing stack supplies the event")]
pub(crate) enum TriggerOp {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug)]
pub(crate) struct TriggerData {
    pub name: String,
    pub when: TriggerWhen,
    pub level: TriggerLevel,
    pub op: TriggerOp,
    pub relation_name: String,
    pub schema_name: String,
    pub relation_oid: u32,
    pub arguments: Vec<String>,
    pub columns: Vec<SpiColumn>,
    pub old: Option<Vec<Value>>,
    pub new: Option<Vec<Value>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TriggerReturn {
    Row(Vec<Value>),
    Null,
}

#[derive(Debug)]
pub(crate) struct PlRuntimeError {
    pub sqlstate: Box<str>,
    #[allow(dead_code, reason = "The trigger-firing stack reports the message")]
    pub message: Box<str>,
    pub detail: Option<Box<str>>,
    pub hint: Option<Box<str>>,
    pub query: Option<Box<str>>,
    pub context: Vec<String>,
    pub spi_error: Option<Box<SpiError>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RaiseLevel {
    Debug,
    Log,
    Info,
    Notice,
    Warning,
    Exception,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PlNotice {
    pub level: RaiseLevel,
    pub sqlstate: Box<str>,
    pub message: Box<str>,
    pub detail: Option<Box<str>>,
    pub hint: Option<Box<str>>,
}

// The trigger-firing stack will call this interpreter entry point.
#[allow(dead_code)]
pub(crate) fn exec_trigger(
    f: &CompiledFunction,
    ctx: &TriggerData,
    spi: &mut dyn Spi,
) -> Result<TriggerReturn, PlRuntimeError> {
    runtime::exec_trigger(f, ctx, spi)
}

#[derive(Debug)]
struct Block {
    label: Option<String>,
    declarations: Vec<Declaration>,
    statements: Vec<Statement>,
}

#[derive(Debug)]
struct Declaration {
    line: usize,
    name: String,
    ty: Option<ScalarType>,
    constant: bool,
    not_null: bool,
    default: Option<ValueExpr>,
}

#[derive(Clone, Debug)]
struct ScalarType {
    data_type: DataType,
    /// Validated spelling, including any type modifier, to use in SPI casts.
    spelling: Box<str>,
    has_modifier: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Target {
    name: String,
    field: Option<String>,
}

#[derive(Debug)]
struct Statement {
    line: usize,
    kind: Stmt,
}

#[derive(Debug)]
enum Stmt {
    Block(Block),
    Assign(Target, ValueExpr, Box<str>),
    If(Vec<(Sql, Vec<Statement>)>, Vec<Statement>),
    Case {
        value: Option<Sql>,
        arms: Vec<(Vec<Sql>, Vec<Statement>)>,
        otherwise: Option<Vec<Statement>>,
    },
    Loop {
        label: Option<String>,
        kind: LoopKind,
        statements: Vec<Statement>,
    },
    LoopControl {
        exit: bool,
        label: Option<String>,
        condition: Option<Sql>,
    },
    Return(ReturnValue),
    Null,
    Perform(Sql),
    Raise(Box<Raise>),
    Sql {
        sql: Sql,
        into: Option<SelectInto>,
        modifies: bool,
    },
    Diagnostics(Target),
}

#[derive(Debug)]
enum LoopKind {
    Plain,
    While(Sql),
    Integer(Box<IntegerFor>),
}

#[derive(Debug)]
struct IntegerFor {
    name: String,
    reverse: bool,
    lower: Sql,
    upper: Sql,
    step: Option<Sql>,
}

#[derive(Debug)]
enum ReturnValue {
    Null,
    Value(ValueExpr),
    Row(Sql),
}

#[derive(Debug)]
enum ValueExpr {
    Record(String),
    Scalar(Sql),
}

#[derive(Debug)]
struct SelectInto {
    strict: bool,
    targets: Vec<Target>,
}

#[derive(Debug)]
struct Raise {
    level: RaiseLevel,
    format: String,
    args: Vec<Sql>,
    options: Vec<(RaiseOption, Sql)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RaiseOption {
    Code,
    Message,
    Detail,
    Hint,
}

#[derive(Debug, PartialEq, Eq)]
struct Sql {
    text: Box<str>,
    original: Box<str>,
    params: Box<[Reference]>,
}

#[derive(Debug, PartialEq, Eq)]
enum Reference {
    Scalar(String),
    Field(String, String),
    Arg(ArgIndex),
    RecordNull { name: String, negated: bool },
}

#[derive(Debug, PartialEq, Eq)]
enum ArgIndex {
    Literal(i64),
    Expression(Box<Sql>),
}
