//! Spanner destination mutations, atomic replay evidence, and admission limits.
pub mod apply;
pub mod limits;
pub mod preflight;

use crate::{
    buffer::reader::BufferReader,
    engine::{self, Applier, Runtime},
    event::AssembledTxn,
};
use apply::{Destination, Write};
use tokio::sync::watch;

/// Plans qualified single-row mutations from one authenticated buffer record.
/// Errors/cancellation poison this instance; restart from the saved checkpoint.
/// Replay receipts are retained indefinitely, so collection never outruns a
/// durable Runtime checkpoint (bounded receipt GC is a separate operation).
pub struct SpannerApplier<P> {
    reader: BufferReader,
    destination: Destination,
    plan: P,
    blocked: bool,
}
impl<P> SpannerApplier<P> {
    pub fn new(reader: BufferReader, destination: Destination, plan: P) -> Self {
        Self {
            reader,
            destination,
            plan,
            blocked: false,
        }
    }
}
impl<P: FnMut(&AssembledTxn) -> apply::Result<Vec<Write>>> Applier for SpannerApplier<P> {
    async fn step(&mut self, runtime: &Runtime) -> engine::Result<bool> {
        self.run_step(runtime, None).await
    }
    async fn controlled_step(
        &mut self,
        runtime: &Runtime,
        stop: watch::Receiver<bool>,
    ) -> engine::Result<bool> {
        self.run_step(runtime, Some(stop)).await
    }
}
impl<P: FnMut(&AssembledTxn) -> apply::Result<Vec<Write>>> SpannerApplier<P> {
    async fn run_step(
        &mut self,
        runtime: &Runtime,
        stop: Option<watch::Receiver<bool>>,
    ) -> engine::Result<bool> {
        let recovery =
            |e: &dyn std::fmt::Display| crate::coordinator::Error::RecoveryRequired(e.to_string());
        if self.blocked {
            return Err(recovery(&"Spanner applier requires reconstruction"));
        }
        if stop
            .as_ref()
            .is_some_and(|s| *s.borrow() || s.has_changed().is_err())
        {
            return Ok(false);
        }
        runtime
            .state()
            .await
            .operational_schema
            .require_clear()
            .map_err(|e| recovery(&e))?;
        self.blocked = true;
        let Some(transaction) = self.reader.next_txn().await.map_err(|e| recovery(&e))? else {
            self.blocked = false;
            return Ok(false);
        };
        let writes = (self.plan)(&transaction).map_err(|e| recovery(&e))?;
        if stop
            .as_ref()
            .is_some_and(|s| *s.borrow() || s.has_changed().is_err())
        {
            return Ok(false);
        }
        runtime
            .state()
            .await
            .operational_schema
            .require_clear()
            .map_err(|e| recovery(&e))?;
        // No select/cancellation from here through durable checkpoint save.
        self.destination
            .apply(&transaction, &writes)
            .await
            .map_err(|e| recovery(&e))?;
        let checkpoint = self.reader.checkpoint();
        let marker = engine::encode_reader_checkpoint(checkpoint)?;
        runtime
            .marker("applied", marker, checkpoint.next_mseq.to_string())
            .await?;
        self.blocked = false;
        Ok(true)
    }
}
