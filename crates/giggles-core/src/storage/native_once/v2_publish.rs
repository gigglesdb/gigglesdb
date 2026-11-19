//! Atomic native outcome publication through the v2 sequencer.
use super::*;
use crate::storage::{engine::{kernel::Kernel, once_guard::{CommitGuard, GuardRejection}, txn::TxnError}, once_session::OnceSetup};

impl SqlSession {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_native_once_v2(
        mut self, setup: OnceSetup, owner: NativeOnceOwner, accepted_fingerprint: [u8; 32],
        result: Vec<u8>, retention_seconds: u64, mode: NativePublishMode,
        clock: &mut dyn FnMut() -> NativeResult<NativeClockLease>,
    ) -> NativeResult<(NativeOnceRecord, bool)> {
        if !Arc::ptr_eq(&self.admin.inner, &owner.inner) { return Err(NativeOnceError::DomainMismatch); }
        if !(1..=MAX_NATIVE_RETENTION_SECONDS).contains(&retention_seconds) || result.len() > MAX_NATIVE_RESULT_BYTES {
            return Err(NativeOnceError::RetentionUnavailable);
        }
        if self.v2_transaction.is_none() || self.transaction_id.is_none() {
            return Err(CoreError::Sql(relational::SqlError::Transaction).into());
        }
        if mode == NativePublishMode::Success && self.aborted {
            return Err(CoreError::Sql(relational::SqlError::Aborted).into());
        }
        if mode == NativePublishMode::Terminal {
            self.v2_transaction.as_mut().expect("active transaction")
                .rollback_to(&setup.initial).map_err(CoreError::from)?;
            self.deferral = Default::default();
            self.pending_sequences.lock().unwrap_or_else(|e| e.into_inner()).clear();
        }
        let inner = self.admin.inner.clone();
        let Backend::V2(db) = &inner.backend else { unreachable!() };
        // Match SQL COMMIT's sequence validation, serialization and publication.
        let mut live = db.sql_counters.lock_cancellable()?;
        let txn = self.v2_transaction.as_mut().expect("active transaction");
        if mode == NativePublishMode::Success {
            v2_sql::validate_pending(db, txn, &mut self.deferral)?;
            v2_sql::sql_sequences::stage_pending(db, txn, &self.pending_sequences)?;
        }
        let _ticket = inner.enter()?;
        let _gate = inner.gate.lock_notified(|| {
            inner.decision_point(DecisionPoint::WaitingLock)
        })?;
        if inner.fenced.load(Ordering::Acquire) { return Err(CoreError::Fenced.into()); }
        let (ledger, header) = v2::read_v2_view(db).inspect_err(|_| { inner.fenced.store(true, Ordering::Release); })?;
        let view = v2::v2_view(db, ledger.as_ref(), header);
        check_key(&view, &owner.key)?;
        if let Some(record) = retained(&view, &owner.key)? {
            return replay(&record, owner.submitted).map(|record| (record, true));
        }
        if !inner.native_once_owners.lock_cancellable()?.get(&owner.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.token, &owner.token)) {
            return Err(CoreError::Fenced.into());
        }
        let (lease, response) = {
            // OnceSetup holds this covering lock already, so reacquisition cannot wait under the gate.
            let mut guard = Kernel::lock(self.v2_transaction.as_mut().expect("active transaction"), &db.kv,
                None, &|| crate::cancellation::check().is_err())?;
            if db.task_kernel_unsupported() {
                // A replicated store keeps the v1 `[07]` ledger (no protected records).
                let lease = certify_lease(setup.image.0.native_outcomes.as_ref(), clock()?)?;
                let mut after = setup.image.clone();
                after.0.native_outcomes.get_or_insert_with(NativeOutcomes::default).insert_outcome(
                    owner.key.clone(), owner.submitted, accepted_fingerprint, result, retention_seconds, lease,
                )?;
                let response = retained(&LedgerView::borrow(&after.0), &owner.key)?.expect("accepted record");
                Kernel::stage_from(&mut guard, setup.before.as_ref(), &after)?;
                (lease, response)
            } else {
                // Shared ledger (R3-625): migrate `[07]` in this transaction if
                // needed, keep `[07]` as the empty v1 ledger beside the metadata,
                // and write this outcome as a `22` record counted in `26 00`.
                let (header, _) = v2_migrate::shared_header(&mut guard)?;
                let header = header.unwrap_or_default();
                let lease = certify_watermark(header.once_upper_watermark_s, clock()?)?;
                let mut after = setup.image.clone();
                after.0.native_outcomes = Some(NativeOutcomes::default());
                Kernel::stage_from(&mut guard, setup.before.as_ref(), &after)?;
                let response = v2_migrate::stage_outcome(&mut guard, &header, &owner.key, owner.submitted,
                    accepted_fingerprint, result, retention_seconds, lease)?;
                (lease, response)
            }
        };
        let point = *inner.crash.lock().map_err(|_| CoreError::Fenced)?;
        if point == CrashPoint::BeforeCommit { crash_now(); }
        if point == CrashPoint::ErrorBeforeCommit { return Err(CoreError::Storage("injected before commit".into()).into()); }
        let hook_inner = inner.clone();
        let guard = CommitGuard::capture(setup.fence, lease.valid_until)
            .with_hook(Arc::new(move |point| hook_inner.decision_point(point)));
        if let Some(guard) = self.xact.lock().map_err(|_| CoreError::Fenced)?.take() {
            self.v2_transaction.as_mut().expect("active transaction").attach_xact(guard).map_err(CoreError::from)?;
        }
        // Taking the transaction removes Drop's slot evidence; retire that slot first.
        inner.sql_transactions.fetch_sub(1, Ordering::AcqRel);
        let txn = self.v2_transaction.take().expect("active transaction");
        let reclaim = txn.reclaim_after_commit;
        db.catalog().commit_ddl_guarded(txn, guard).map_err(|error| match error {
            TxnError::Guard(GuardRejection::LeaseExpired) => NativeOnceError::ClockUncertain,
            TxnError::CommitAmbiguous => {
                inner.fenced.store(true, Ordering::Release);
                CoreError::Storage("engine commit outcome unknown".into()).into()
            }
            other => CoreError::from(other).into(),
        })?;
        for (id, counter) in self.pending_sequences.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            let slot = live.entry(*id).or_default();
            let mut current = slot.counter.lock_cancellable()?;
            if db.catalog().current_local_view()?.sequences.get(id)
                    .is_some_and(|def| def.incarnation == counter.incarnation)
                    && current.counter.as_ref().is_none_or(|current| current.incarnation != counter.incarnation) {
                v2_sql::sql_sequences::install_and_wake(slot, &mut current, counter.clone())?;
            }
        }
        drop(live);
        self.rollback();
        if reclaim { let _ = db.reclaim_background(); }
        if point == CrashPoint::AfterCommit { crash_now(); }
        inner.decision_point(DecisionPoint::Committed);
        if point == CrashPoint::ErrorAfterCommit {
            inner.fenced.store(true, Ordering::Release);
            return Err(CoreError::Storage("injected lost commit response".into()).into());
        }
        if point == CrashPoint::BeforeAck { crash_now(); }
        Ok((response, false))
    }
}
