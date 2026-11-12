//! Dedicated, private advisory-lock sessions. Never reconnect a held capability.
use crate::{
    coordinator::exclusion::{Error, Result},
    pg::tls,
    security::TlsPolicy,
};

pub(crate) struct Session {
    client: tokio_postgres::Client,
    driver: Driver,
    pid: i32,
}

// The driver must outlive/close independently of the caller's runtime. Merely
// aborting a spawned task leaves its socket open until that runtime polls again.
struct Driver {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Driver {
    async fn connect(
        config: &tls::Config,
        policy: &TlsPolicy,
    ) -> Result<(Self, tokio_postgres::Client)> {
        let config = config.clone();
        let policy = *policy;
        let (ready, client) = tokio::sync::oneshot::channel();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("migration-exclusion-pg".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    let _ = ready.send(Err(Error::Connection));
                    return;
                };
                runtime.block_on(async move {
                    let connection = tokio::select! {
                        biased;
                        _ = &mut stopped => return,
                        result = tls::connect(&config, &policy) => result,
                    };
                    let Ok((client, driver, _)) = connection else {
                        let _ = ready.send(Err(Error::Connection));
                        return;
                    };
                    if ready.send(Ok(client)).is_err() {
                        return;
                    }
                    tokio::select! {
                        biased;
                        _ = stopped => {},
                        _ = driver => {},
                    }
                    // Dropping the losing driver future closes its socket here.
                });
            })
            .map_err(|_| Error::Connection)?;
        // RAII exists before waiting for startup, so cancelled acquisition also
        // interrupts a pending TLS handshake and joins the driver thread.
        let driver = Self {
            stop: Some(stop),
            thread: Some(thread),
        };
        let client = client.await.map_err(|_| Error::Connection)??;
        Ok((driver, client))
    }
    fn is_finished(&self) -> bool {
        self.thread
            .as_ref()
            .is_none_or(|thread| thread.is_finished())
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Session {
    pub(crate) async fn acquire(
        config: &tls::Config,
        policy: &TlsPolicy,
        migration: &str,
        wait: bool,
    ) -> Result<Self> {
        let (driver, client) = Driver::connect(config, policy).await?;
        // Construct RAII before any further await, including the blocking lock
        // query. Cancelling acquisition must close that pending session too.
        let mut session = Self {
            client,
            driver,
            pid: 0,
        };
        session.pid = session
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(|_| Error::Connection)?
            .get(0);
        if wait {
            session
                .client
                .query_one(
                    "SELECT pg_advisory_lock(hashtextextended($1, 0))",
                    &[&migration],
                )
                .await
                .map_err(|_| Error::Connection)?;
        } else if !session
            .client
            .query_one(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                &[&migration],
            )
            .await
            .map_err(|_| Error::Connection)?
            .get::<_, bool>(0)
        {
            return Err(Error::Busy);
        }
        session.check_alive(migration).await?;
        Ok(session)
    }
    pub(crate) fn pid(&self) -> i32 {
        self.pid
    }
    pub(crate) async fn check_alive(&self, migration: &str) -> Result<()> {
        if self.client.is_closed() || self.driver.is_finished() {
            return Err(Error::Lost);
        }
        let held: bool = self
            .client
            .query_one(
                "SELECT pg_backend_pid() = $2 AND EXISTS (
                SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
                AND pid = pg_backend_pid() AND granted AND mode = 'ExclusiveLock'
                AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                AND classid = ((hashtextextended($1, 0) >> 32) & 4294967295)::oid
                AND objid = (hashtextextended($1, 0) & 4294967295)::oid AND objsubid = 1
            )",
                &[&migration, &self.pid],
            )
            .await
            .map_err(|_| Error::Lost)?
            .get(0);
        if held { Ok(()) } else { Err(Error::Lost) }
    }
}
