//! Atomic, durable tokenization accounting.
use super::*;
use crate::policy::DomainName;

/// Limits applied to the next explicit tokenization.
#[derive(Clone, Copy, Debug)]
pub struct TokenizeLimits {
    /// Maximum tokenizations in one session and domain.
    pub per_session: u64,
    /// Maximum tokenizations in one principal window and domain.
    pub per_principal: u64,
    /// Fixed principal window duration in milliseconds.
    pub principal_window_ms: u64,
}
/// Data-free tokenization refusal.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TokenizeError {
    /// Session cannot authorize tokenization.
    #[error("session ended")]
    Ended,
    /// One of the tokenization limits was reached.
    #[error("tokenization limit reached")]
    LimitReached,
    /// Durable state is unavailable or malformed.
    #[error("state store unavailable")]
    Store,
}
impl From<StoreError> for TokenizeError {
    fn from(_: StoreError) -> Self {
        Self::Store
    }
}
impl From<ModelError> for TokenizeError {
    fn from(_: ModelError) -> Self {
        Self::Store
    }
}
// Count followed by durable window end: readers need expiry without policy parameters.
pub(super) fn decode_principal_count(bytes: &[u8]) -> Result<(u64, u64), ModelError> {
    if bytes.len() != 16 {
        return Err(ModelError);
    }
    Ok((decode_count(&bytes[..8])?, decode_count(&bytes[8..])?))
}
impl Authority {
    /// Increment both durable counters atomically after checking session validity.
    pub fn record_tokenization(
        &self,
        session: SessionId,
        domain: &DomainName,
        limits: &TokenizeLimits,
    ) -> Result<(), TokenizeError> {
        self.admit(session, Operation::Read)
            .map_err(|_| TokenizeError::Ended)?;
        let _writer = self.writer.lock().unwrap();
        // Recheck after waiting for revocation or other writers.
        self.admit(session, Operation::Read)
            .map_err(|_| TokenizeError::Ended)?;
        let mut tx = self.store.write()?;
        let now = self.store.now_ms();
        let row: AccessSession = decode(
            &tx.get(Table::Sessions, &keys::session(session))?
                .ok_or(TokenizeError::Ended)?,
        )?;
        if row.state != SessionState::Active || now >= row.expires_ms {
            return Err(TokenizeError::Ended);
        }
        let session_key = keys::session_counter(session, domain)?;
        let count = tx
            .get(Table::SessionCounters, &session_key)?
            .map(|b| decode_count(&b))
            .transpose()?
            .unwrap_or(0);
        if count >= limits.per_session {
            return Err(TokenizeError::LimitReached);
        }
        let prefix = keys::principal_counter_prefix(row.principal, domain)?;
        let rows = tx.scan_prefix(Table::PrincipalCounters, &prefix)?;
        let mut current = None;
        for (key, bytes) in &rows {
            let start = keys::principal_counter_window(key, &prefix).ok_or(TokenizeError::Store)?;
            let (count, end) = decode_principal_count(bytes)?;
            if end < start {
                return Err(TokenizeError::Store);
            }
            if end > now && current.replace((start, count, end)).is_some() {
                return Err(TokenizeError::Store);
            }
        }
        let (start, principal_count, end) =
            current.unwrap_or((now, 0, now.saturating_add(limits.principal_window_ms)));
        if principal_count >= limits.per_principal {
            return Err(TokenizeError::LimitReached);
        }
        let principal_key = keys::principal_counter(row.principal, domain, start)?;
        for (key, _) in rows {
            if key != principal_key {
                tx.delete(Table::PrincipalCounters, &key)?;
            }
        }
        let value = [encode_count(principal_count + 1), encode_count(end)].concat();
        tx.put(
            Table::SessionCounters,
            &session_key,
            &encode_count(count + 1),
        )?;
        tx.put(Table::PrincipalCounters, &principal_key, &value)?;
        tx.commit()?;
        Ok(())
    }
    /// Read the durable count for a session and domain, including ended sessions.
    pub fn session_tokenizations(
        &self,
        session: SessionId,
        domain: &DomainName,
    ) -> Result<u64, TokenizeError> {
        Ok(self
            .store
            .read()?
            .get(
                Table::SessionCounters,
                &keys::session_counter(session, domain)?,
            )?
            .map(|b| decode_count(&b))
            .transpose()?
            .unwrap_or(0))
    }
    /// Read the current durable principal window and count, if it has not ended.
    pub fn principal_tokenizations(
        &self,
        principal: PrincipalId,
        domain: &DomainName,
    ) -> Result<Option<(u64, u64)>, TokenizeError> {
        let read = self.store.read()?;
        let now = self.store.now_ms();
        let prefix = keys::principal_counter_prefix(principal, domain)?;
        let mut current = None;
        for (key, bytes) in read.scan_prefix(Table::PrincipalCounters, &prefix)? {
            let start =
                keys::principal_counter_window(&key, &prefix).ok_or(TokenizeError::Store)?;
            let (count, end) = decode_principal_count(&bytes)?;
            if end < start {
                return Err(TokenizeError::Store);
            }
            if end > now && current.replace((start, count)).is_some() {
                return Err(TokenizeError::Store);
            }
        }
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::*;
    use super::*;
    use crate::state::store::RedbStore;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn domain() -> DomainName {
        DomainName("email".into())
    }
    fn limits() -> TokenizeLimits {
        TokenizeLimits {
            per_session: 100,
            per_principal: 1000,
            principal_window_ms: 100,
        }
    }
    fn reopen(f: Fixture) -> Fixture {
        let Fixture {
            a,
            store,
            clock,
            hook,
            dir,
        } = f;
        drop(a);
        drop(store);
        let c = clock.clone();
        let store = Arc::new(
            RedbStore::with_clock(&dir.0, &root(), Arc::new(move || c.load(Ordering::SeqCst)))
                .unwrap(),
        );
        let a = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        a.set_hook(hook.clone());
        Fixture {
            a,
            store,
            clock,
            hook,
            dir,
        }
    }
    #[test]
    fn parallel_session_limit() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        let limits = TokenizeLimits {
            per_session: 50,
            ..limits()
        };
        let ok = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..40 {
                        match f.a.record_tokenization(s.id, &domain(), &limits) {
                            Ok(()) => {
                                ok.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => assert_eq!(e, TokenizeError::LimitReached),
                        }
                    }
                });
            }
        });
        assert_eq!(ok.load(Ordering::SeqCst), 50);
        assert_eq!(f.a.session_tokenizations(s.id, &domain()).unwrap(), 50);
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1000, 50))
        );
    }
    #[test]
    fn parallel_principal_limit() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let sessions = [f.acquire(p.id, "one"), f.acquire(p.id, "two")];
        let limits = TokenizeLimits {
            per_principal: 30,
            ..limits()
        };
        let ok = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for i in 0..8 {
                let sid = sessions[i % 2].id;
                let f = &f;
                let limits = &limits;
                let ok = &ok;
                scope.spawn(move || {
                    for _ in 0..40 {
                        match f.a.record_tokenization(sid, &domain(), limits) {
                            Ok(()) => {
                                ok.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => assert_eq!(e, TokenizeError::LimitReached),
                        }
                    }
                });
            }
        });
        assert_eq!(ok.load(Ordering::SeqCst), 30);
        assert_eq!(
            sessions
                .iter()
                .map(|s| f.a.session_tokenizations(s.id, &domain()).unwrap())
                .sum::<u64>(),
            30
        );
    }
    #[test]
    fn revocation_races_eight_incrementers_and_survives_reopen() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        let limits = TokenizeLimits {
            per_session: 20,
            per_principal: 20,
            ..limits()
        };
        let ok = AtomicUsize::new(0);
        let stopped = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    while !stopped.load(Ordering::SeqCst) {
                        match f.a.record_tokenization(s.id, &domain(), &limits) {
                            Ok(()) => {
                                ok.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(TokenizeError::LimitReached | TokenizeError::Ended) => {}
                            Err(e) => panic!("unexpected counter error: {e}"),
                        }
                    }
                    for _ in 0..8 {
                        assert_eq!(
                            f.a.record_tokenization(s.id, &domain(), &limits),
                            Err(TokenizeError::Ended)
                        );
                    }
                });
            }
            while ok.load(Ordering::SeqCst) == 0 {
                std::thread::yield_now();
            }
            f.a.revoke(s.id, EndReason::Requested).unwrap();
            stopped.store(true, Ordering::SeqCst);
            assert_eq!(
                f.a.admit(s.id, Operation::Read).unwrap_err(),
                AdmitError::Ended
            );
        });
        let count = ok.load(Ordering::SeqCst) as u64;
        assert!(count <= limits.per_session);
        assert_eq!(f.a.session_tokenizations(s.id, &domain()).unwrap(), count);
        let f = reopen(f);
        assert_eq!(f.a.session(s.id).unwrap().state, SessionState::Revoked);
        assert_eq!(f.a.session_tokenizations(s.id, &domain()).unwrap(), count);
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1000, count))
        );
        assert_eq!(
            f.a.record_tokenization(s.id, &domain(), &limits),
            Err(TokenizeError::Ended)
        );
    }
    #[test]
    fn clock_step_back_keeps_the_current_window() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        let limits = TokenizeLimits {
            principal_window_ms: 1000,
            ..limits()
        };
        f.clock.store(5000, Ordering::SeqCst);
        f.a.record_tokenization(s.id, &domain(), &limits).unwrap();
        f.clock.store(4000, Ordering::SeqCst);
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((5000, 1))
        );
        f.a.record_tokenization(s.id, &domain(), &limits).unwrap();
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((5000, 2))
        );
        assert_principal_row(&f, p.id, 5000, 2, 6000);
    }

    fn assert_principal_row(f: &Fixture, principal: PrincipalId, start: u64, count: u64, end: u64) {
        let prefix = keys::principal_counter_prefix(principal, &domain()).unwrap();
        let rows = f
            .store
            .read()
            .unwrap()
            .scan_prefix(Table::PrincipalCounters, &prefix)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            keys::principal_counter_window(&rows[0].0, &prefix),
            Some(start)
        );
        assert_eq!(decode_principal_count(&rows[0].1).unwrap(), (count, end));
    }

    #[test]
    fn window_length_change_applies_from_the_next_window() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        let original = TokenizeLimits {
            principal_window_ms: 1000,
            ..limits()
        };
        let changed = TokenizeLimits {
            principal_window_ms: 100,
            ..original
        };
        f.a.record_tokenization(s.id, &domain(), &original).unwrap();
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1000, 1))
        );
        assert_principal_row(&f, p.id, 1000, 1, 2000);
        for (now, count) in [(1500, 2), (1999, 3)] {
            f.clock.store(now, Ordering::SeqCst);
            assert_eq!(
                f.a.principal_tokenizations(p.id, &domain()).unwrap(),
                Some((1000, count - 1))
            );
            f.a.record_tokenization(s.id, &domain(), &changed).unwrap();
            assert_eq!(
                f.a.principal_tokenizations(p.id, &domain()).unwrap(),
                Some((1000, count))
            );
            assert_principal_row(&f, p.id, 1000, count, 2000);
        }
        f.clock.store(2000, Ordering::SeqCst);
        assert_eq!(f.a.principal_tokenizations(p.id, &domain()).unwrap(), None);
        f.a.record_tokenization(s.id, &domain(), &changed).unwrap();
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((2000, 1))
        );
        assert_principal_row(&f, p.id, 2000, 1, 2100);
        f.clock.store(2099, Ordering::SeqCst);
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((2000, 1))
        );
        f.clock.store(2100, Ordering::SeqCst);
        assert_eq!(f.a.principal_tokenizations(p.id, &domain()).unwrap(), None);
    }

    #[test]
    fn durable_window_survives_sessions_credentials_and_policy_limits() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        let limits = TokenizeLimits {
            per_principal: 2,
            ..limits()
        };
        f.a.record_tokenization(s.id, &domain(), &limits).unwrap();
        let second = f.acquire(p.id, "two");
        f.a.renew(p.id, second.id, None, Some((verifier(), [1; 32])))
            .unwrap();
        let cid =
            f.a.add_principal_credential(p.id, verifier(), None)
                .unwrap();
        f.a.revoke_principal_credential(p.id, cid).unwrap();
        f.a.add_principal_credential(p.id, verifier(), None)
            .unwrap();
        f.a.record_tokenization(second.id, &domain(), &limits)
            .unwrap();
        let f = reopen(f);
        assert_eq!(f.a.session_tokenizations(s.id, &domain()).unwrap(), 1);
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1000, 2))
        );
        f.clock.store(1099, Ordering::SeqCst);
        assert_eq!(
            f.a.record_tokenization(second.id, &domain(), &limits),
            Err(TokenizeError::LimitReached)
        );
        let lower = TokenizeLimits {
            per_session: 1,
            per_principal: 100,
            ..limits
        };
        assert_eq!(
            f.a.record_tokenization(second.id, &domain(), &lower),
            Err(TokenizeError::LimitReached)
        );
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1000, 2))
        );
        f.clock.store(1100, Ordering::SeqCst);
        assert_eq!(f.a.principal_tokenizations(p.id, &domain()).unwrap(), None);
        f.a.record_tokenization(second.id, &domain(), &limits)
            .unwrap();
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1100, 1))
        );
        let read = f.store.read().unwrap();
        let rows = read
            .scan_prefix(
                Table::PrincipalCounters,
                &keys::principal_counter_prefix(p.id, &domain()).unwrap(),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            keys::principal_counter_window(
                &rows[0].0,
                &keys::principal_counter_prefix(p.id, &domain()).unwrap()
            ),
            Some(1100)
        );
    }
    #[test]
    fn ended_and_unknown_sessions_never_write_counters() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let revoked = f.acquire(p.id, "revoked");
        f.a.revoke(revoked.id, EndReason::Requested).unwrap();
        let mut req = request("expired");
        req.lifetime_ms = Some(1);
        let expired = f.a.acquire_session(p.id, req).unwrap();
        f.clock.store(1001, Ordering::SeqCst);
        for sid in [revoked.id, expired.id, SessionId::random()] {
            assert_eq!(
                f.a.record_tokenization(sid, &domain(), &limits()),
                Err(TokenizeError::Ended)
            );
        }
        let read = f.store.read().unwrap();
        for table in [Table::SessionCounters, Table::PrincipalCounters] {
            assert!(read.scan_prefix(table, b"").unwrap().is_empty());
        }
    }
    #[test]
    fn refused_or_failed_transaction_changes_neither_counter() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        f.a.record_tokenization(s.id, &domain(), &limits()).unwrap();
        f.store.fail_commits(true);
        assert_eq!(
            f.a.record_tokenization(s.id, &domain(), &limits()),
            Err(TokenizeError::Store)
        );
        f.store.fail_commits(false);
        assert_eq!(f.a.session_tokenizations(s.id, &domain()).unwrap(), 1);
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1000, 1))
        );
        let zero = TokenizeLimits {
            per_principal: 0,
            ..limits()
        };
        let other = DomainName("other".into());
        assert_eq!(
            f.a.record_tokenization(s.id, &other, &zero),
            Err(TokenizeError::LimitReached)
        );
        assert_eq!(f.a.session_tokenizations(s.id, &other).unwrap(), 0);
        assert_eq!(f.a.principal_tokenizations(p.id, &other).unwrap(), None);
    }
    #[test]
    fn ended_sessions_never_write_counters() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let revoked = f.acquire(p.id, "revoked");
        f.a.revoke(revoked.id, EndReason::Requested).unwrap();
        let mut req = request("expired");
        req.lifetime_ms = Some(1);
        let expired = f.a.acquire_session(p.id, req).unwrap();
        f.clock.store(1001, Ordering::SeqCst);
        let snapshot = || {
            let tx = f.store.read().unwrap();
            Table::ALL
                .iter()
                .map(|t| tx.scan_prefix(*t, b"").unwrap())
                .collect::<Vec<_>>()
        };
        let before = snapshot();
        for id in [revoked.id, expired.id, SessionId::random()] {
            assert_eq!(
                f.a.record_tokenization(id, &domain(), &limits()),
                Err(TokenizeError::Ended)
            );
            assert_eq!(snapshot(), before);
        }
    }
    #[test]
    fn window_rollover_deletes_old_row_and_limits_do_not_clear_counts() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let s = f.acquire(p.id, "one");
        let low = TokenizeLimits {
            per_principal: 1,
            ..limits()
        };
        f.a.record_tokenization(s.id, &domain(), &low).unwrap();
        f.a.record_tokenization(s.id, &domain(), &limits()).unwrap();
        assert_eq!(
            f.a.record_tokenization(s.id, &domain(), &low),
            Err(TokenizeError::LimitReached)
        );
        assert_eq!(f.a.session_tokenizations(s.id, &domain()).unwrap(), 2);
        f.clock.store(1099, Ordering::SeqCst);
        assert_eq!(
            f.a.record_tokenization(s.id, &domain(), &low),
            Err(TokenizeError::LimitReached)
        );
        f.clock.store(1100, Ordering::SeqCst);
        assert_eq!(f.a.principal_tokenizations(p.id, &domain()).unwrap(), None);
        f.a.record_tokenization(s.id, &domain(), &low).unwrap();
        assert_eq!(
            f.a.principal_tokenizations(p.id, &domain()).unwrap(),
            Some((1100, 1))
        );
        let rows = f
            .store
            .read()
            .unwrap()
            .scan_prefix(
                Table::PrincipalCounters,
                &keys::principal_counter_prefix(p.id, &domain()).unwrap(),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            keys::principal_counter_window(
                &rows[0].0,
                &keys::principal_counter_prefix(p.id, &domain()).unwrap()
            ),
            Some(1100)
        );
    }
    #[test]
    fn renew_and_flush_drain_pending_idle_endings() {
        for renew in [false, true] {
            let f = Fixture::limits(SessionLimits {
                max_lifetime_ms: 100_000,
                idle_timeout_ms: Some(10),
            });
            let p = f.principal("owner", Mode::ReadOnly);
            let ended = f.acquire(p.id, "ended");
            f.clock.store(1011, Ordering::SeqCst);
            assert_eq!(
                f.a.admit(ended.id, Operation::Read).unwrap_err(),
                AdmitError::Ended
            );
            let active = f.acquire(p.id, "active");
            if renew {
                f.a.renew(p.id, active.id, None, None).unwrap();
            } else {
                f.a.flush_last_used().unwrap();
            }
            assert_ne!(f.stored(ended.id).state, SessionState::Active);
            assert_eq!(f.hook.0.lock().unwrap().len(), 1);
            let f = reopen(f);
            assert_eq!(
                f.a.session(ended.id).unwrap().revoke_reason,
                Some(EndReason::IdleTimeout)
            );
        }
    }
}
