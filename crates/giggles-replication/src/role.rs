//! Publish an engine only after confirming leadership and applied quorum state.
use crate::sink::ReplicaHandle;
use raft::StateRole;
use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

pub trait Engine: Send + 'static {
    fn fence(&self);
    fn is_fenced(&self) -> bool {
        false
    }
}
pub enum Serving<E> {
    Follower { leader: u64, term: u64 },
    Leader { term: u64, engine: Arc<E> },
}
impl<E> Clone for Serving<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Follower { leader, term } => Self::Follower {
                leader: *leader,
                term: *term,
            },
            Self::Leader { term, engine } => Self::Leader {
                term: *term,
                engine: engine.clone(),
            },
        }
    }
}
struct Shared<E> {
    serving: Serving<E>,
    stop: bool,
}
pub struct RoleManager<E: Engine> {
    shared: Arc<(Mutex<Shared<E>>, Condvar)>,
    join: thread::JoinHandle<()>,
}
// Arc<E> crosses the driver/client thread boundary, so E must also be Sync.
impl<E: Engine + Sync> RoleManager<E> {
    pub fn start(
        handle: ReplicaHandle,
        mut open: Box<dyn FnMut(u64) -> Result<E, String> + Send>,
        poll: Duration,
    ) -> Self {
        assert!(!poll.is_zero(), "poll interval must be positive");
        let role = handle.role();
        let shared = Arc::new((
            Mutex::new(Shared::<E> {
                serving: Serving::Follower {
                    leader: role.leader,
                    term: role.term,
                },
                stop: false,
            }),
            Condvar::new(),
        ));
        let worker = shared.clone();
        let join = thread::spawn(move || {
            let (lock, changed) = &*worker;
            loop {
                let role = handle.role();
                let mut state = lock.lock().expect("role manager mutex poisoned");
                let stopping = state.stop;
                if let Serving::Leader { term, engine } = &state.serving
                    && (stopping
                        || handle.is_stopped()
                        || role.state != StateRole::Leader
                        || role.term != *term
                        || engine.is_fenced())
                {
                    engine.fence();
                    state.serving = Serving::Follower {
                        leader: role.leader,
                        term: role.term,
                    };
                    changed.notify_all();
                }
                if stopping {
                    break;
                }
                if !matches!(state.serving, Serving::Leader { .. }) {
                    state.serving = Serving::Follower {
                        leader: role.leader,
                        term: role.term,
                    };
                    changed.notify_all();
                    drop(state);
                    if !handle.is_stopped()
                        && role.state == StateRole::Leader
                        && handle.read_barrier().is_ok()
                    {
                        // The barrier or open callback may have overlapped a term change.
                        let confirmed = handle.role();
                        if !handle.is_stopped()
                            && confirmed.state == StateRole::Leader
                            && confirmed.term == role.term
                            && let Ok(engine) = open(role.term)
                        {
                            let mut state = lock.lock().expect("role manager mutex poisoned");
                            let latest = handle.role();
                            if !state.stop
                                && !handle.is_stopped()
                                && latest.state == StateRole::Leader
                                && latest.term == role.term
                            {
                                state.serving = Serving::Leader {
                                    term: role.term,
                                    engine: Arc::new(engine),
                                };
                                changed.notify_all();
                            } else {
                                engine.fence();
                            }
                        }
                    }
                    state = lock.lock().expect("role manager mutex poisoned");
                }
                let (state, _) = changed
                    .wait_timeout_while(state, poll, |s| !s.stop)
                    .expect("role manager mutex poisoned");
                drop(state);
            }
        });
        Self { shared, join }
    }
    pub fn current(&self) -> Serving<E> {
        self.shared
            .0
            .lock()
            .expect("role manager mutex poisoned")
            .serving
            .clone()
    }
    pub fn wait_for(
        &self,
        pred: impl Fn(&Serving<E>) -> bool,
        timeout: Duration,
    ) -> Option<Serving<E>> {
        let (lock, changed) = &*self.shared;
        let (state, _) = changed
            .wait_timeout_while(
                lock.lock().expect("role manager mutex poisoned"),
                timeout,
                |s| !pred(&s.serving),
            )
            .expect("role manager mutex poisoned");
        pred(&state.serving).then(|| state.serving.clone())
    }
    pub fn stop(self) {
        self.shared
            .0
            .lock()
            .expect("role manager mutex poisoned")
            .stop = true;
        self.shared.1.notify_all();
        self.join.join().expect("role manager thread panicked");
    }
}
