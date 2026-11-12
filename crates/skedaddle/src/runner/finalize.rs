//! Resumable owner-only cleanup. Progress payloads stay behind coordinator metadata.
use super::*;
use crate::{
    coordinator::{self, Materialized, State},
    cutover,
    engine::Runtime,
    rollback::finalize::{
        Finalize, FinalizeRecord, FinalizeStep, Inventory, Object, ObjectKind, Side,
    },
};
use std::sync::Arc;

pub(super) const REQUEST: &str = "runner-finalize-request-v1";
const CHECKPOINT: &str = "runner-finalize-checkpoint-v1";
const INTENT: &str = "runner-finalize-intent-v1";

pub(crate) fn allowed(state: &Materialized) -> bool {
    state.lifecycle.state == State::RollbackWindow
        || (state.lifecycle.state == State::DestinationActive
            && state.rollback_eligibility_ended.is_some())
}
fn unavailable() -> coordinator::Error {
    coordinator::Error::RecoveryRequired(
        "finalize evidence or endpoint unavailable (details redacted)".into(),
    )
}
fn manifest(state: &Materialized, id: &str) -> coordinator::Result<Inventory> {
    let checkpoint = state.progress.get("source-slot").ok_or_else(unavailable)?;
    let checkpoint =
        crate::engine::decode_source_checkpoint(&checkpoint.0).map_err(|_| unavailable())?;
    if checkpoint.slot != format!("skedaddle_{id}") {
        return Err(unavailable());
    }
    let mut installed = Inventory::new();
    for (side, kind, name) in [
        (Side::Source, ObjectKind::Slot, checkpoint.slot),
        (
            Side::Source,
            ObjectKind::Publication,
            format!("skedaddle_{id}"),
        ),
        (
            Side::Source,
            ObjectKind::EventTrigger,
            format!("skedaddle_ddl_{id}"),
        ),
        (Side::Source, ObjectKind::Schema, "_skedaddle".into()),
        (Side::Destination, ObjectKind::Schema, "_skedaddle".into()),
        (
            Side::Destination,
            ObjectKind::ReplicationOrigin,
            format!("skedaddle_{id}"),
        ),
    ] {
        installed.insert(Object { side, kind, name });
    }
    if state.verification.reverse.is_some() {
        for (side, kind, name) in [
            (
                Side::Destination,
                ObjectKind::Slot,
                format!("skedaddle_{id}_rev"),
            ),
            (
                Side::Destination,
                ObjectKind::Publication,
                format!("skedaddle_{id}_rev"),
            ),
            (
                Side::Destination,
                ObjectKind::EventTrigger,
                format!("skedaddle_ddl_{id}"),
            ),
            (
                Side::Source,
                ObjectKind::ReplicationOrigin,
                format!("skedaddle_{id}_rev"),
            ),
        ] {
            installed.insert(Object { side, kind, name });
        }
    }
    Ok(installed)
}
/// Caller has already journaled admission and dropped the directly-owned capture sockets.
/// The driver independently verifies that owned slots are inactive before destructive DDL.
pub(super) async fn run(
    runtime: &Runtime,
    id: &str,
    drop_objects: bool,
    definition: Option<&MigrationDefinition>,
) -> coordinator::Result<()> {
    if !drop_objects {
        return Ok(());
    }
    let definition = definition.ok_or_else(unavailable)?.clone();
    let state = runtime.state().await;
    let mut saved = match state.progress.get(CHECKPOINT) {
        Some((bytes, _)) => serde_json::from_str::<Finalize>(bytes)?,
        None => Finalize::new(
            cutover::Key {
                migration: id.into(),
                cutover: format!("finalize-{id}"),
                epoch: state.epoch.try_into().map_err(|_| unavailable())?,
                coordinator: state.owner.clone(),
            },
            true,
            manifest(&state, id)?,
        )
        .map_err(|_| unavailable())?,
    };
    // Adopt a checkpoint only under this acquired journal epoch.
    saved.key.epoch = state.epoch.try_into().map_err(|_| unavailable())?;
    saved.key.coordinator = state.owner;
    let runtime = runtime.clone();
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| unavailable())?,
        );
        let mut redactor = Redactor::default();
        let sc = config(&definition.source, &mut redactor).map_err(|_| unavailable())?;
        let dc = config(&definition.destination, &mut redactor).map_err(|_| unavailable())?;
        let mut source = super::cutover::Pg::open(rt.clone(), &sc, &definition.source)
            .map_err(|_| unavailable())?;
        let mut destination = super::cutover::Pg::open(rt, &dc, &definition.destination)
            .map_err(|_| unavailable())?;
        let mut authority = runtime.cutover_authority();
        while saved.next != FinalizeStep::Complete {
            let result = saved.execute_next(
                &mut authority,
                &mut source,
                &mut destination,
                &mut |_| Ok(()),
                &mut |record| {
                    let stream = if matches!(record, FinalizeRecord::Checkpoint(_)) {
                        CHECKPOINT
                    } else {
                        INTENT
                    };
                    let value = match record {
                        FinalizeRecord::Checkpoint(value) => serde_json::to_string(value),
                        _ => serde_json::to_string(record),
                    }
                    .map_err(|_| "serialize finalize".to_string())?;
                    handle
                        .block_on(async {
                            runtime.refresh_owner().await?;
                            runtime.marker(stream, value, "finalize".into()).await
                        })
                        .map_err(|_| "journal finalize".to_string())
                },
            );
            if matches!(result, Err(cutover::Error::LostOwnership)) {
                return Err(coordinator::Error::LostOwnership);
            }
            if result.is_err() {
                // Preserve ownership errors rather than reporting them as transport failures.
                handle.block_on(runtime.refresh_owner())?;
                return Err(unavailable());
            }
        }
        if saved.leftovers().is_none_or(|objects| !objects.is_empty()) {
            return Err(unavailable());
        }
        Ok(())
    })
    .await
    .map_err(|_| unavailable())?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_requires_window_or_ended_eligibility() {
        let mut state = Materialized::default();
        for lifecycle in State::ALL {
            state.lifecycle.state = lifecycle;
            assert_eq!(allowed(&state), lifecycle == State::RollbackWindow);
            state.rollback_eligibility_ended =
                Some(coordinator::reverse::EligibilityEnd::OperatorFinalized);
            assert_eq!(
                allowed(&state),
                matches!(lifecycle, State::RollbackWindow | State::DestinationActive)
            );
            state.rollback_eligibility_ended = None;
        }
    }
}
