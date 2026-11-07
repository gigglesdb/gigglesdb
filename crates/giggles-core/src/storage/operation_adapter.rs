//! Unpublished immutable typed-operation reducer seam. Application dispatch is disabled.
use super::State;
use crate::operations::{self, Bindings, BoundDescription, Outcome, Request};

#[allow(dead_code)]
pub(crate) fn execute(
    state: &State,
    request: &Request,
    bindings: &Bindings,
) -> operations::Result<(State, Outcome)> {
    operations::state_adapter::execute(state, request, bindings)
}

/// Exact canonical catalog/domain/table fence and explicit optimistic row version.
/// No caller authority or retry ledger is inferred from the supplied description.
#[allow(dead_code)]
pub(crate) fn execute_fenced(
    state: &State,
    request: &Request,
    bindings: &Bindings,
    description: &BoundDescription,
    expected_version: Option<u64>,
) -> operations::Result<(State, Outcome)> {
    operations::state_adapter::execute_fenced(
        state,
        request,
        bindings,
        description,
        expected_version,
    )
}

#[cfg(test)]
mod tests;
