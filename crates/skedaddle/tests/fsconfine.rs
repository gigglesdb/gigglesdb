use skedaddle::fsconfine::Dir;
#[cfg(windows)]
use std::path::Path;
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
};

fn fixture(name: &str) -> PathBuf {
    let base =
        PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("explicit task test root"));
    assert!(base.starts_with("J:/gigglesdb-work") || base.starts_with("/work/"));
    let path = base.join(format!("fsconfine-{name}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).unwrap();
    path
}
fn write(dir: &Dir, name: &str, bytes: &[u8]) {
    let mut file = dir.create_file(name).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}
fn read(dir: &Dir, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    dir.open_file(name)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}

#[test]
fn traversal_and_absolute_components_are_rejected() {
    let root = fixture("traversal");
    let dir = Dir::open_root(&root).unwrap();
    for path in [
        "../sentinel",
        "/sentinel",
        "a/../../sentinel",
        "a/./sentinel",
        "a//sentinel",
        "C:/sentinel",
        "a\\sentinel",
        "sentinel:stream",
        ".",
        "..",
    ] {
        assert!(dir.create_file(path).is_err(), "create {path}");
        assert!(dir.open_file(path).is_err(), "open {path}");
        assert!(dir.remove_file(path).is_err(), "delete {path}");
        assert!(dir.dir(path, true).is_err(), "dir {path}");
        assert!(dir.rename_new("source", path).is_err(), "rename {path}");
    }
}

#[test]
fn create_publish_sync_and_delete_are_relative_and_no_clobber() {
    let root = fixture("operations");
    let dir = Dir::open_root(&root).unwrap();
    let nested = dir.dir("one/two", true).unwrap();
    write(&nested, "stage", b"original");
    nested.rename_new("stage", "published").unwrap();
    assert_eq!(read(&nested, "published"), b"original");
    write(&nested, "other", b"other");
    assert!(nested.rename_new("other", "published").is_err());
    assert_eq!(read(&nested, "published"), b"original");
    assert_eq!(read(&nested, "other"), b"other");
    nested.remove_file("published").unwrap();
    nested.remove_file("other").unwrap();
    nested.sync().unwrap();
    assert!(nested.names().unwrap().is_empty());
    drop(nested);
    dir.remove_dir("one/two").unwrap();
    dir.remove_dir("one").unwrap();
    dir.sync().unwrap();
}

#[test]
fn single_character_leaves_publish_beside_their_staging_names() {
    let root = fixture("single-character-publication");
    let dir = Dir::open_root(&root).unwrap();
    for leaf in ["x", "é", "🦀"] {
        let stage = format!("{leaf}.stage");
        write(&dir, &stage, b"original");
        dir.rename_new(&stage, leaf).unwrap();
        assert_eq!(read(&dir, leaf), b"original");
        assert!(!root.join(&stage).exists());

        write(&dir, &stage, b"other");
        assert!(dir.rename_new(&stage, leaf).is_err());
        assert_eq!(read(&dir, leaf), b"original");
        assert_eq!(read(&dir, &stage), b"other");
    }
    dir.sync().unwrap();
}

#[cfg(windows)]
fn junction(link: &Path, target: &Path) {
    let status = std::process::Command::new("cmd")
        .args(["/c", "mklink", "/J"])
        .arg(link.to_string_lossy().replace('/', "\\"))
        .arg(target.to_string_lossy().replace('/', "\\"))
        .status()
        .unwrap();
    assert!(status.success());
}

#[cfg(windows)]
#[test]
fn junctions_are_rejected_at_root_ancestor_and_leaf() {
    let root = fixture("junctions");
    let outside = fixture("outside");
    fs::write(outside.join("sentinel"), b"safe").unwrap();
    junction(&root.join("link"), &outside);
    let dir = Dir::open_root(&root).unwrap();
    assert!(Dir::open_root(&root.join("link")).is_err());
    assert!(dir.dir("link", false).is_err());
    assert!(dir.open_file("link/sentinel").is_err());
    assert!(dir.create_file("link/created").is_err());
    assert!(dir.remove_file("link/sentinel").is_err());
    assert!(dir.remove_dir("link").is_err());
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
    assert!(!outside.join("created").exists());
}

#[cfg(windows)]
#[test]
fn windows_pins_root_and_ancestors_against_mid_operation_swap() {
    let parent = fixture("pinned");
    let root = parent.join("root");
    fs::create_dir_all(root.join("ancestor")).unwrap();
    let outside = fixture("pin-outside");
    fs::write(outside.join("sentinel"), b"safe").unwrap();
    let dir = Dir::open_root(&root).unwrap();
    let child = dir.dir("ancestor", false).unwrap();
    write(&child, "sentinel", b"owned");
    let thread_root = root.clone();
    let attempted = std::thread::spawn(move || {
        assert!(fs::rename(&thread_root, thread_root.with_file_name("moved")).is_err());
        assert!(
            fs::rename(
                thread_root.join("ancestor"),
                thread_root.join("moved-ancestor")
            )
            .is_err()
        );
    });
    attempted.join().unwrap();
    child.remove_file("sentinel").unwrap();
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::sync::{Arc, Barrier};

    #[test]
    fn symlinks_at_every_level_are_refused_and_outside_survives() {
        let root = fixture("symlinks");
        let outside = fixture("outside");
        fs::write(outside.join("sentinel"), b"safe").unwrap();
        symlink(&outside, root.join("ancestor")).unwrap();
        symlink(outside.join("sentinel"), root.join("leaf")).unwrap();
        let dir = Dir::open_root(&root).unwrap();
        assert!(Dir::open_root(&root.join("ancestor")).is_err());
        for path in ["ancestor/sentinel", "leaf"] {
            assert!(dir.open_file(path).is_err());
            assert!(dir.remove_file(path).is_err());
            assert!(dir.create_file(path).is_err());
        }
        assert!(dir.dir("ancestor/created", true).is_err());
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
        assert!(!outside.join("created").exists());
    }

    #[test]
    fn root_swapped_for_symlink_after_pin_cannot_redirect_delete() {
        let parent = fixture("root-swap");
        let root = parent.join("root");
        fs::create_dir(&root).unwrap();
        let outside = fixture("root-outside");
        fs::write(outside.join("sentinel"), b"safe").unwrap();
        let dir = Dir::open_root(&root).unwrap();
        write(&dir, "sentinel", b"owned");
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();
        let worker_root = root.clone();
        let worker_outside = outside.clone();
        let attacker = std::thread::spawn(move || {
            worker_barrier.wait();
            fs::rename(&worker_root, worker_root.with_file_name("detached")).unwrap();
            symlink(&worker_outside, &worker_root).unwrap();
            worker_barrier.wait();
        });
        barrier.wait();
        barrier.wait();
        dir.remove_file("sentinel").unwrap();
        write(&dir, "stage", b"owned");
        dir.rename_new("stage", "published").unwrap();
        assert_eq!(read(&dir, "published"), b"owned");
        dir.remove_file("published").unwrap();
        dir.sync().unwrap();
        attacker.join().unwrap();
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
        assert!(!outside.join("published").exists());
    }

    #[test]
    fn ancestor_swapped_mid_operation_never_deletes_outside() {
        let root = fixture("ancestor-swap");
        fs::create_dir(root.join("ancestor")).unwrap();
        let outside = fixture("ancestor-outside");
        fs::write(outside.join("sentinel"), b"safe").unwrap();
        let dir = Dir::open_root(&root).unwrap();
        let pinned = dir.dir("ancestor", false).unwrap();
        write(&pinned, "sentinel", b"owned");
        let thread_root = root.clone();
        let thread_outside = outside.clone();
        std::thread::spawn(move || {
            fs::rename(thread_root.join("ancestor"), thread_root.join("detached")).unwrap();
            symlink(thread_outside, thread_root.join("ancestor")).unwrap();
        })
        .join()
        .unwrap();
        assert!(dir.remove_file("ancestor/sentinel").is_err());
        pinned.remove_file("sentinel").unwrap();
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn core_export_bridge_is_bound_to_the_open_directory() {
        use giggles_core::{Admin, Limits};
        let parent = fixture("core-bridge");
        let root = parent.join("root");
        fs::create_dir(&root).unwrap();
        let outside = fixture("core-outside");
        fs::write(outside.join("archive.export"), b"sentinel").unwrap();
        let admin = Admin::create(parent.join("database"), Limits::default()).unwrap();
        let dir = Dir::open_root(&root).unwrap();
        let archive = dir.core_path(&root, "archive.export").unwrap();
        fs::rename(&root, parent.join("detached")).unwrap();
        symlink(&outside, &root).unwrap();
        admin.export(&archive).unwrap();
        Admin::verify_export(&archive).unwrap();
        for name in dir.names().unwrap() {
            dir.remove_file(&name).unwrap();
        }
        assert_eq!(
            fs::read(outside.join("archive.export")).unwrap(),
            b"sentinel"
        );
    }

    #[tokio::test]
    async fn local_backend_keeps_its_original_root_after_swap() {
        use skedaddle::buffer::backends::{BufferStore, ObjectBufferStore};
        let parent = fixture("backend-swap");
        let root = parent.join("root");
        fs::create_dir(&root).unwrap();
        let outside = fixture("backend-outside");
        fs::write(outside.join("sentinel"), b"safe").unwrap();
        let store = ObjectBufferStore::local(&root, std::time::Duration::from_secs(10)).unwrap();
        store.put_create_only("sentinel", b"owned").await.unwrap();
        fs::rename(&root, parent.join("detached")).unwrap();
        symlink(&outside, &root).unwrap();
        assert_eq!(store.get("sentinel").await.unwrap().unwrap(), b"owned");
        store
            .put_create_only("nested/created", b"new")
            .await
            .unwrap();
        assert_eq!(
            store.list_prefix("", None, 10).await.unwrap().objects.len(),
            2
        );
        store.delete("sentinel").await.unwrap();
        store.delete("nested/created").await.unwrap();
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
        assert!(!outside.join("nested").exists());
    }
}

#[test]
fn repeated_and_concurrent_enumeration_has_independent_cursors() {
    let root = fixture("enumeration");
    let dir = Dir::open_root(&root).unwrap();
    // More than one 64 KiB native query buffer, to exercise cursor isolation.
    let expected: Vec<_> = (0..300)
        .map(|n| std::ffi::OsString::from(format!("entry-{n:04}-{}", "x".repeat(128))))
        .collect();
    for name in &expected {
        dir.create_file(name).unwrap();
    }
    assert!(dir.dir(".", false).is_err());
    assert!(dir.dir("", false).is_err());
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let dir = &dir;
            let expected = &expected;
            scope.spawn(move || {
                for _ in 0..8 {
                    let mut names = dir.names().unwrap();
                    names.sort();
                    assert_eq!(&names, expected);
                }
            });
        }
    });
}

fn scratch_assembler(root: &std::path::Path) -> skedaddle::buffer::writer::TransactionAssembler {
    use skedaddle::event::{Position, SourceIdentity, TxnId};
    skedaddle::buffer::writer::TransactionAssembler::new(
        SourceIdentity {
            engine: "pg".into(),
            system_id: "test".into(),
            lineage: "test".into(),
            db_name: "db".into(),
            slot: "slot".into(),
        },
        TxnId(vec![1]),
        Position::new("fsconfine", vec![1]),
        None,
        1,
        1024 * 1024,
        root,
    )
    .unwrap()
}

struct ScratchOrder;
impl skedaddle::event::PositionOrder for ScratchOrder {
    fn domain(&self) -> &str {
        "fsconfine"
    }
    fn compare(
        &self,
        a: &[u8],
        b: &[u8],
    ) -> Result<std::cmp::Ordering, skedaddle::event::EventError> {
        Ok(a.cmp(b))
    }
}

async fn scratch_writer(root: &std::path::Path) -> skedaddle::buffer::writer::BufferWriter {
    use skedaddle::buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    };
    let store =
        ObjectBufferStore::local(fixture("scratch-store"), std::time::Duration::from_secs(10))
            .unwrap();
    let (tx, _rx) = std::sync::mpsc::channel();
    BufferWriter::open_with_scratch(
        std::sync::Arc::new(store),
        WriterConfig {
            prefix: "buffer".into(),
            migration_id: "m".into(),
            stream: "s".into(),
            epoch: 1,
            segment_bytes: 1024 * 1024,
            segment_transactions: 1,
            segment_age: std::time::Duration::from_secs(10),
            max_transaction_bytes: 1024 * 1024,
        },
        std::sync::Arc::new(ScratchOrder),
        tx,
        root,
    )
    .await
    .unwrap()
}

#[cfg(unix)]
#[tokio::test]
async fn writer_and_assembler_keep_scratch_capability_after_root_swap() {
    let parent = fixture("scratch-swap");
    let root = parent.join("root");
    fs::create_dir(&root).unwrap();
    let outside = fixture("scratch-outside");
    fs::write(outside.join("sentinel"), b"safe").unwrap();
    let assembler = scratch_assembler(&root);
    let mut writer = scratch_writer(&root).await;
    let detached = parent.join("detached");
    fs::rename(&root, &detached).unwrap();
    std::os::unix::fs::symlink(&outside, &root).unwrap();
    let prepared = assembler.finish().unwrap();
    assert_eq!(fs::read_dir(&detached).unwrap().count(), 1);
    writer
        .append_streaming(prepared, std::time::Instant::now())
        .await
        .unwrap();
    assert_eq!(writer.next_manifest_sequence(), 1);
    assert_eq!(fs::read_dir(&detached).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 1);
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
}

#[cfg(windows)]
#[tokio::test]
async fn writer_and_assembler_pin_scratch_and_refuse_junction_roots() {
    let parent = fixture("scratch-pin");
    let root = parent.join("root");
    fs::create_dir(&root).unwrap();
    let assembler = scratch_assembler(&root);
    assert!(fs::rename(&root, parent.join("moved")).is_err());
    let mut writer = scratch_writer(&root).await;
    let prepared = assembler.finish().unwrap();
    assert!(fs::rename(&root, parent.join("moved")).is_err());
    writer
        .append_streaming(prepared, std::time::Instant::now())
        .await
        .unwrap();
    assert_eq!(writer.next_manifest_sequence(), 1);
    assert!(fs::rename(&root, parent.join("moved")).is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    let outside = fixture("scratch-junction-outside");
    fs::write(outside.join("sentinel"), b"safe").unwrap();
    junction(&parent.join("link"), &outside);
    use skedaddle::buffer::writer::TransactionAssembler;
    use skedaddle::event::{Position, SourceIdentity, TxnId};
    assert!(
        TransactionAssembler::new(
            SourceIdentity {
                engine: "pg".into(),
                system_id: "test".into(),
                lineage: "test".into(),
                db_name: "db".into(),
                slot: "slot".into()
            },
            TxnId(vec![1]),
            Position::new("fsconfine", vec![1]),
            None,
            1,
            1024,
            parent.join("link")
        )
        .is_err()
    );
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"safe");
}
