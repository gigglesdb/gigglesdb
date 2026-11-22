//! Filesystem identity for a local database's recovery source.

use std::{fs::File, io, path::Path};

/// Keep the directory open so its ID cannot be recycled after removal.
/// The handle does not carry the database writer lock.
#[derive(Debug)]
pub(super) struct DirectoryIdentity {
    _directory: File,
    id: (u64, u128),
}

impl DirectoryIdentity {
    pub(super) fn read(path: &Path) -> io::Result<Self> {
        let directory = open_directory(path)?;
        let id = file_id(&directory)?;
        Ok(Self { _directory: directory, id })
    }

    pub(super) fn matches(&self, path: &Path) -> bool {
        Self::read(path).is_ok_and(|other| self.id == other.id)
    }
}

#[cfg(not(windows))]
fn open_directory(path: &Path) -> io::Result<File> {
    File::open(path)
}

#[cfg(unix)]
fn file_id(directory: &File) -> io::Result<(u64, u128)> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::from(io::ErrorKind::NotADirectory));
    }
    Ok((metadata.dev(), metadata.ino().into()))
}

#[cfg(windows)]
fn open_directory(path: &Path) -> io::Result<File> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};

    const FILE_READ_ATTRIBUTES: u32 = 0x80;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    // OpenOptions shares reads, writes and deletes by default.
    OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(windows)]
fn file_id(directory: &File) -> io::Result<(u64, u128)> {
    use std::{ffi::c_void, mem::size_of, os::windows::io::AsRawHandle};

    const FILE_ID_INFO_CLASS: i32 = 18;
    #[repr(C)]
    #[derive(Default)]
    struct FileIdInfo {
        volume_serial_number: u64,
        file_id: [u8; 16],
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandleEx(
            handle: *mut c_void,
            class: i32,
            info: *mut FileIdInfo,
            size: u32,
        ) -> i32;
    }

    if !directory.metadata()?.is_dir() {
        return Err(io::Error::from(io::ErrorKind::NotADirectory));
    }
    let mut info = FileIdInfo::default();
    // SAFETY: the File owns a live handle, and info is writable storage with
    // the layout and size of the Windows FILE_ID_INFO for this information class.
    let ok = unsafe {
        GetFileInformationByHandleEx(
            directory.as_raw_handle(),
            FILE_ID_INFO_CLASS,
            &mut info,
            size_of::<FileIdInfo>() as u32,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((info.volume_serial_number, u128::from_ne_bytes(info.file_id)))
}

#[cfg(not(any(unix, windows)))]
fn file_id(_directory: &File) -> io::Result<(u64, u128)> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}
