//! Borrowed committed catalog access for trusted engine inspection.
use super::{Admin, CoreError, Holder};
use crate::relational::TableDef;
use std::collections::BTreeMap;

impl Admin {
    pub(crate) fn explain_limits_engine(&self) -> super::Limits {
        self.inner.limits
    }
    /// The callback borrows one committed snapshot. No transaction, publication
    /// or extra State clone is involved. The underlying bounded state read still
    /// performs the storage kernel's ordinary decoding and validation.
    pub(crate) fn inspect_catalog_engine<T>(
        &self,
        inspect: impl FnOnce(u128, u64, &BTreeMap<String, TableDef>) -> T,
    ) -> Result<T, CoreError> {
        if let Some(db) = self.lease(Holder::Admin)? {
            let _ticket = self.inner.enter()?;
            return db.inspect_binding(None, inspect);
        }
        self.inner.inspect(|state| {
            crate::cancellation::check()?;
            let result = inspect(state.view.domain, state.sql.epoch, &state.sql.tables);
            crate::cancellation::check()?;
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Cancellation, DecisionPoint, Limits,
        operations::{Engine, Error, InspectionRequest},
    };
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };

    fn database(admitted: usize) -> Admin {
        let root = crate::test_artifact_root();
        std::fs::create_dir_all(&root).unwrap();
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).unwrap();
        Admin::create(
            root.join(format!(
                "catalog-inspection-{:032x}",
                u128::from_le_bytes(id)
            )),
            Limits {
                admitted,
                ..Limits::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn borrowed_catalog_inspection_holds_one_committed_snapshot() {
        let admin = database(8);
        admin
            .sql_session()
            .execute("CREATE TABLE first (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let (waiting, notified) = mpsc::channel();
        admin.set_decision_hook(Some(Arc::new(move |point| {
            if point == DecisionPoint::WaitingLock {
                waiting.send(()).unwrap();
            }
        })));
        let ((domain, epoch, names), writer) = admin
            .inspect_catalog_engine(|domain, epoch, tables| {
                let writer_admin = admin.clone();
                let writer = std::thread::spawn(move || {
                    writer_admin.sql_session().execute(
                        "CREATE TABLE second (id BIGINT PRIMARY KEY)",
                        &[],
                        None,
                    )
                });
                notified
                    .recv_timeout(Duration::from_secs(5))
                    .expect("competing SQL must wait for the inspection snapshot");
                (
                    (domain, epoch, tables.keys().cloned().collect::<Vec<_>>()),
                    writer,
                )
            })
            .unwrap();
        writer.join().unwrap().unwrap();
        admin.set_decision_hook(None);
        assert_eq!(names, ["first"]);
        assert_eq!(epoch, 1);
        assert_eq!(domain, admin.snapshot().unwrap().domain);
        let fresh = admin
            .inspect_catalog_engine(|current_domain, current_epoch, tables| {
                (
                    current_domain,
                    current_epoch,
                    tables.keys().cloned().collect::<Vec<_>>(),
                )
            })
            .unwrap();
        assert_eq!(fresh, (domain, 2, vec!["first".into(), "second".into()]));
    }

    #[test]
    fn borrowed_catalog_inspection_checks_admission_and_cancellation_before_disclosure() {
        let admin = database(1);
        let engine = Engine::new(admin.clone());
        let refused = admin
            .inspect_catalog_engine(|_, _, _| engine.inspect(&InspectionRequest::Tables))
            .unwrap();
        assert_eq!(refused, Err(Error::Core(CoreError::Busy.to_string())));
        let cancellation = Cancellation::default();
        assert_eq!(
            cancellation.scope(|| admin.inspect_catalog_engine(|_, _, _| {
                assert!(cancellation.cancel());
                "provisional metadata"
            })),
            Err(CoreError::Cancelled)
        );
        assert_eq!(admin.catalog().unwrap(), (0, vec![]));
        admin
            .inner
            .fenced
            .store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(
            engine.inspect(&InspectionRequest::Tables),
            Err(Error::Core(CoreError::Fenced.to_string()))
        );
        admin
            .inner
            .fenced
            .store(false, std::sync::atomic::Ordering::Release);
        assert_eq!(admin.catalog().unwrap(), (0, vec![]));
    }
}
