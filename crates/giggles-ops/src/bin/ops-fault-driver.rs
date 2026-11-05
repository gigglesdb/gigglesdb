//! Test-only subprocess driver. Faults require explicit command arguments.
use giggles_core::{Admin, Limits};
use giggles_ops::*;
fn main() {
    let a: Vec<_> = std::env::args().collect();
    assert_eq!(a.len(), 6);
    let point = match a[2].as_str() {
        "pre-export" => FaultPoint::BeforeExport,
        "post-snapshot" => FaultPoint::AfterSnapshot,
        "pre-publish" => FaultPoint::BeforePublish,
        "post-publish" => FaultPoint::AfterPublish,
        "pre-restore" => FaultPoint::BeforeRestore,
        "created" => FaultPoint::InstallCreated,
        "staged" => FaultPoint::InstallStaged,
        "committed" => FaultPoint::InstallCommitted,
        "post-install" => FaultPoint::AfterInstall,
        _ => panic!("unknown test seam"),
    };
    let c = Catalog::open(&a[3], OpsLimits::default()).unwrap();
    let id = BackupId::new(&a[4]).unwrap();
    let fault = Fault {
        point,
        action: FaultAction::Abort,
    };
    if a[1] == "backup" {
        let admin = Admin::open(&a[5], Limits::default()).unwrap();
        c.backup_with_fault(&admin, &id, fault).unwrap();
    } else {
        c.restore_with_fault(&id, &a[5], fault).unwrap();
    }
    panic!("fault did not terminate child");
}
