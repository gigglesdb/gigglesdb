//! PL/pgSQL (stub; replaced by the real module at merge).
pub(crate) struct FunctionSignature { pub returns_trigger: bool, pub arg_names: Vec<Option<String>>, pub function_name: String }
#[derive(Clone, Debug)] pub(crate) struct CompiledFunction;
pub(crate) struct PlCompileError { pub sqlstate: &'static str, pub message: String, pub position: Option<usize>, pub hint: Option<String> }
pub(crate) fn compile(_body: &str, _sig: &FunctionSignature) -> Result<CompiledFunction, PlCompileError> { Ok(CompiledFunction) }
