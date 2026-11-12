//! One durable rollback step under exclusive cutover authority.
//!
//! Run on a blocking thread outside an entered Tokio runtime. Rebuild endpoint
//! adapters from native receipts before retrying. The caller owns backoff and
//! must never retry Invalid as if it were a transient transport failure.
use super::{apply, destination, source};
use crate::{
    coordinator::{Materialized, State, rollback::Step},
    cutover::{self, Authority, Key},
};

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Progress,
    Complete,
    DeadlineAborted,
}
impl Outcome {
    pub fn exit_code(&self) -> Option<u8> {
        match self {
            Self::Progress => None,
            Self::Complete => Some(0),
            Self::DeadlineAborted => Some(2),
        }
    }
}

pub struct Components<'a, D, C, P, V, S, H, J> {
    pub destination: &'a mut D,
    pub capture: &'a mut C,
    pub pre_hook: &'a mut P,
    pub replay: &'a apply::Replay,
    pub target: &'a mut dyn crate::pg::apply::executor::Destination,
    pub verification: &'a mut V,
    pub source: &'a mut S,
    pub post_hook: &'a mut H,
    pub journal: &'a mut J,
}
fn apply_error(error: apply::Error) -> cutover::Error {
    match error {
        apply::Error::Retry => cutover::Error::Component("rollback retry required".into()),
        apply::Error::OperatorAction(reason) => cutover::Error::Invalid(reason),
    }
}
fn owned(state: &Materialized, key: &Key) -> cutover::Result<()> {
    if key.epoch <= 0 || state.epoch != key.epoch as u64 || state.owner != key.coordinator {
        return Err(cutover::Error::LostOwnership);
    }
    Ok(())
}
/// The snapshot selects a slice only. Each slice revalidates under authority;
/// a stale generation can never authorize a source effect or destination release.
pub fn execute_next<A, D, C, P, V, S, H, J>(
    key: &Key,
    now_ms: u64,
    authority: &mut A,
    executor: &tokio::runtime::Runtime,
    c: &mut Components<'_, D, C, P, V, S, H, J>,
) -> cutover::Result<Outcome>
where
    A: Authority,
    D: cutover::SourceFence,
    C: destination::Capture,
    P: destination::PreHook,
    V: apply::Verification,
    S: source::Effects,
    H: source::PostHook,
    J: destination::Journal + apply::Journal + source::Journal,
{
    let state = destination::Journal::state(c.journal);
    owned(&state, key)?;
    let cp = state
        .rollback
        .as_ref()
        .ok_or(cutover::Error::Invalid("rollback checkpoint missing"))?;
    if cp.next == Step::ReverseApply && !cp.point_of_no_return && now_ms >= cp.deadline_at_ms {
        return destination::execute_next(
            key,
            now_ms,
            authority,
            c.destination,
            c.capture,
            c.pre_hook,
            c.journal,
        )
        .map(|outcome| {
            if outcome == destination::Outcome::Restored {
                Outcome::DeadlineAborted
            } else {
                Outcome::Progress
            }
        });
    }
    match cp.next {
        Step::PreHook
        | Step::FenceDestination
        | Step::Drain
        | Step::FinalMarker
        | Step::RestoreDestination => {
            match destination::execute_next(
                key,
                now_ms,
                authority,
                c.destination,
                c.capture,
                c.pre_hook,
                c.journal,
            )? {
                destination::Outcome::Restored => Ok(Outcome::DeadlineAborted),
                _ => Ok(Outcome::Progress),
            }
        }
        _ => authority.run(key, || {
            let state = destination::Journal::state(c.journal);
            owned(&state, key)?;
            let cp = state
                .rollback
                .as_ref()
                .ok_or(cutover::Error::Invalid("rollback checkpoint missing"))?;
            match cp.next {
                Step::Complete if state.lifecycle.state == State::SourceActiveAfterRollback => {
                    Ok(Outcome::Complete)
                }
                Step::Aborted if state.lifecycle.state == State::RollbackWindow => {
                    Ok(Outcome::DeadlineAborted)
                }
                Step::ReverseApply | Step::Verify => {
                    executor
                        .block_on(apply::execute_next(
                            key,
                            c.replay,
                            c.target,
                            c.verification,
                            c.journal,
                        ))
                        .map_err(apply_error)?;
                    Ok(Outcome::Progress)
                }
                Step::Sequences | Step::RestoreSource | Step::PostHook => {
                    match source::execute_next(key, c.source, c.post_hook, c.journal)
                        .map_err(apply_error)?
                    {
                        source::Outcome::Complete => Ok(Outcome::Complete),
                        source::Outcome::Progress => Ok(Outcome::Progress),
                    }
                }
                _ => Err(cutover::Error::Invalid("rollback step differs")),
            }
        }),
    }
}

/// Durable bridge for synchronous callbacks, including callbacks made while
/// reverse apply is polling. A joined thread drives each journal operation;
/// this avoids nested Tokio block_on and never detaches a publication.
pub struct RuntimeJournal<'a> {
    runtime: &'a crate::engine::Runtime,
    state: Materialized,
    request: String,
    initial: crate::buffer::reader::ReaderCheckpoint,
}
fn bridge<T: Send>(
    future: impl std::future::Future<Output = crate::coordinator::Result<T>> + Send,
) -> cutover::Result<T> {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| {
                        cutover::Error::Component("rollback journal runtime unavailable".into())
                    })?
                    .block_on(future)
                    .map_err(coordinator_error)
            })
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}
impl<'a> RuntimeJournal<'a> {
    pub fn new(
        runtime: &'a crate::engine::Runtime,
        initial: crate::buffer::reader::ReaderCheckpoint,
    ) -> cutover::Result<Self> {
        let state = bridge(async {
            runtime.refresh_owner().await?;
            Ok(runtime.state().await)
        })?;
        let request = state
            .rollback
            .as_ref()
            .ok_or(cutover::Error::Invalid("rollback admission missing"))?
            .request
            .clone();
        Ok(Self {
            runtime,
            state,
            request,
            initial,
        })
    }
    fn stream(&self, kind: &str) -> String {
        serde_json::json!(["rollback-driver-v1", self.request, kind]).to_string()
    }
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn commit(&mut self, transition: crate::coordinator::Transition) -> cutover::Result<()> {
        let runtime = self.runtime;
        let request = &self.request;
        let (result, state) = bridge(async {
            let result = async {
                runtime.refresh_owner().await?;
                if runtime
                    .state()
                    .await
                    .rollback
                    .as_ref()
                    .map(|cp| &cp.request)
                    != Some(request)
                {
                    return Err(crate::coordinator::Error::LostOwnership);
                }
                runtime.commit(transition).await
            }
            .await;
            // Retain authenticated materialization even when an acknowledgement
            // was lost; source compensation must recognize a terminal journal.
            Ok((result, runtime.state().await))
        })?;
        self.state = state;
        result.map_err(coordinator_error)
    }
    fn publish(&mut self, record: crate::coordinator::rollback::Record) -> cutover::Result<()> {
        self.commit(crate::coordinator::Transition::Rollback {
            record: Box::new(record),
        })
    }
    fn marker(&mut self, kind: &str, marker: String, position: String) -> cutover::Result<()> {
        self.commit(crate::coordinator::Transition::Progress {
            stream: self.stream(kind),
            marker,
            position,
        })
    }
    fn append<T: serde::Serialize + serde::de::DeserializeOwned + Clone>(
        &mut self,
        kind: &str,
        record: &T,
    ) -> cutover::Result<()> {
        let mut records: Vec<T> = match self.state.progress.get(&self.stream(kind)) {
            Some((value, _)) => serde_json::from_str(value)
                .map_err(|_| cutover::Error::Invalid("rollback native log invalid"))?,
            None => vec![],
        };
        records.push(record.clone());
        self.marker(
            kind,
            serde_json::to_string(&records)
                .map_err(|_| cutover::Error::Invalid("rollback log invalid"))?,
            "native".into(),
        )
    }
    pub fn destination_records(&self) -> cutover::Result<Vec<cutover::Record>> {
        match self.state.progress.get(&self.stream("destination")) {
            Some((value, _)) => serde_json::from_str(value)
                .map_err(|_| cutover::Error::Invalid("rollback destination log invalid")),
            None => Ok(vec![]),
        }
    }
}
fn coordinator_error(error: crate::coordinator::Error) -> cutover::Error {
    match error {
        crate::coordinator::Error::LostOwnership | crate::coordinator::Error::Observer => {
            cutover::Error::LostOwnership
        }
        crate::coordinator::Error::Rejected(_) | crate::coordinator::Error::RecoveryRequired(_) => {
            cutover::Error::Invalid("rollback journal evidence differs")
        }
        _ => cutover::Error::Component("rollback journal unavailable".into()),
    }
}
fn journal_error(error: cutover::Error) -> apply::Error {
    match error {
        cutover::Error::Invalid(reason) => apply::Error::OperatorAction(reason),
        _ => apply::Error::Retry,
    }
}
impl destination::Journal for RuntimeJournal<'_> {
    fn state(&self) -> Materialized {
        self.state()
    }
    fn publish(&mut self, record: crate::coordinator::rollback::Record) -> cutover::Result<()> {
        self.publish(record)
    }
    fn native(&mut self, record: &cutover::Record) -> Result<(), String> {
        self.append("destination", record)
            .map_err(|e| e.to_string())
    }
}
impl source::Journal for RuntimeJournal<'_> {
    fn state(&self) -> Materialized {
        self.state()
    }
    fn publish(&mut self, record: crate::coordinator::rollback::Record) -> apply::Result<()> {
        self.publish(record).map_err(journal_error)
    }
    fn native(&mut self, record: &crate::pg::fence::Intent) -> Result<(), String> {
        self.append("source", record).map_err(|e| e.to_string())
    }
}
impl apply::Journal for RuntimeJournal<'_> {
    fn state(&self) -> Materialized {
        self.state()
    }
    fn publish(&mut self, record: crate::coordinator::rollback::Record) -> apply::Result<()> {
        self.publish(record).map_err(journal_error)
    }
    fn progress(
        &self,
        request: &str,
    ) -> apply::Result<(crate::buffer::reader::ReaderCheckpoint, bool)> {
        if request != self.request {
            return Err(apply::Error::OperatorAction("rollback request differs"));
        }
        match self.state.progress.get(&self.stream("apply")) {
            Some((checkpoint, reached)) => Ok((
                crate::engine::decode_reader_checkpoint(checkpoint).map_err(|_| {
                    apply::Error::OperatorAction("reverse reader checkpoint invalid")
                })?,
                serde_json::from_str(reached)
                    .map_err(|_| apply::Error::OperatorAction("reverse boundary invalid"))?,
            )),
            None => Ok((self.initial.clone(), false)),
        }
    }
    fn save(
        &mut self,
        request: &str,
        checkpoint: crate::buffer::reader::ReaderCheckpoint,
        reached: bool,
    ) -> apply::Result<()> {
        if request != self.request {
            return Err(apply::Error::OperatorAction("rollback request differs"));
        }
        self.marker(
            "apply",
            crate::engine::encode_reader_checkpoint(&checkpoint)
                .map_err(|_| apply::Error::OperatorAction("reverse reader checkpoint invalid"))?,
            reached.to_string(),
        )
        .map_err(journal_error)
    }
}

#[cfg(test)]
pub(crate) mod tests;
