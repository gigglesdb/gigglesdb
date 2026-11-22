//! Filesystem identity for a local database's recovery source.

use std::{
    fmt,
    fs::File,
    io,
    path::{Path, PathBuf},
};

type FileId = (u64, u128);

#[derive(Debug)]
pub(super) enum RecoveryDirectoryError {
    IdentityChanged,
    #[cfg(any(target_os = "linux", windows))]
    LinkedEntry(std::ffi::OsString),
    Io(io::Error),
}

impl From<io::Error> for RecoveryDirectoryError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for RecoveryDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityChanged => f.write_str("directory identity changed"),
            #[cfg(any(target_os = "linux", windows))]
            Self::LinkedEntry(name) => write!(f, "linked recovery entry {name:?}"),
            Self::Io(error) => fmt::Display::fmt(error, f),
        }
    }
}

/// A verified path for the duration of the owned directory handle.
#[cfg_attr(
    not(any(target_os = "linux", windows)),
    allow(dead_code, reason = "recovery is refused on other platforms")
)]
pub(super) struct PinnedDirectory {
    directory: File,
    path: PathBuf,
}

impl PinnedDirectory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn check_recovery_links(&self) -> Result<(), RecoveryDirectoryError> {
        #[cfg(any(target_os = "linux", windows))]
        {
            // Use the pinned path and reject pre-existing links before opening
            // any files for writing. A local writer racing this scan can still
            // change entries before RocksDB opens them.
            for entry in self.path.read_dir()? {
                let entry = entry?;
                let name = entry.file_name();
                match is_linked_recovery_entry(&entry.path(), &name) {
                    Ok(true) => return Err(RecoveryDirectoryError::LinkedEntry(name)),
                    Ok(false) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }

    pub(super) fn into_parts(self) -> (File, PathBuf) {
        (self.directory, self.path)
    }
}

#[cfg(any(target_os = "linux", windows))]
fn is_linked_recovery_entry(path: &Path, name: &std::ffi::OsStr) -> io::Result<bool> {
    #[cfg(test)]
    if let Some(hook) = TEST_BEFORE_RECOVERY_ENTRY_METADATA.get() {
        hook(path)?;
    }
    let metadata = path.symlink_metadata()?;
    Ok(is_link_or_reparse_point(&metadata)
        || (metadata.is_file()
            && is_mutable_recovery_file(name)
            && has_multiple_links(path, &metadata)?))
}

#[cfg(all(test, any(target_os = "linux", windows)))]
type RecoveryEntryHook = fn(&Path) -> io::Result<()>;

#[cfg(all(test, any(target_os = "linux", windows)))]
thread_local! {
    pub(super) static TEST_BEFORE_RECOVERY_ENTRY_METADATA: std::cell::Cell<Option<RecoveryEntryHook>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(any(target_os = "linux", windows))]
fn is_mutable_recovery_file(name: &std::ffi::OsStr) -> bool {
    use super::engine::{kv_rocksdb::WRITER_LOCK_FILE, meta::{MARKER, TMP}};

    let name = name.as_encoded_bytes();
    let matches = |actual: &[u8], expected: &[u8]| {
        if cfg!(windows) {
            actual.eq_ignore_ascii_case(expected)
        } else {
            actual == expected
        }
    };
    [WRITER_LOCK_FILE, "LOCK", "CURRENT", "IDENTITY", MARKER, TMP]
        .iter()
        .any(|file| matches(name, file.as_bytes()))
        || [b"LOG".as_slice(), b"OPTIONS-", b"MANIFEST-"]
            .iter()
            .any(|prefix| name.get(..prefix.len()).is_some_and(|part| matches(part, prefix)))
        || [b".log".as_slice(), b".dbtmp"].iter().any(|suffix| {
            name.len().checked_sub(suffix.len()).is_some_and(|start| matches(&name[start..], suffix))
        })
}

#[cfg(target_os = "linux")]
fn is_link_or_reparse_point(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(target_os = "linux")]
fn has_multiple_links(_path: &Path, metadata: &std::fs::Metadata) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    Ok(metadata.nlink() > 1)
}

#[cfg(windows)]
fn is_link_or_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(windows)]
fn has_multiple_links(path: &Path, _metadata: &std::fs::Metadata) -> io::Result<bool> {
    use std::{ffi::c_void, fs::OpenOptions, os::windows::{fs::OpenOptionsExt, io::AsRawHandle}};

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

    #[repr(C)]
    #[derive(Default)]
    struct ByHandleFileInformation {
        attributes: u32,
        creation_time: [u32; 2],
        last_access_time: [u32; 2],
        last_write_time: [u32; 2],
        volume_serial_number: u32,
        file_size: [u32; 2],
        number_of_links: u32,
        file_index: [u32; 2],
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandle(
            handle: *mut c_void,
            info: *mut ByHandleFileInformation,
        ) -> i32;
    }

    let file = OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let mut info = ByHandleFileInformation::default();
    // SAFETY: file owns a live handle and info is writable storage matching
    // the layout of BY_HANDLE_FILE_INFORMATION.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info.number_of_links > 1)
}

/// Keep the directory open so its ID cannot be recycled after removal.
/// The handle does not carry the database writer lock.
#[derive(Debug)]
pub(super) struct DirectoryIdentity {
    _directory: File,
    id: FileId,
}

impl DirectoryIdentity {
    pub(super) fn read(path: &Path) -> io::Result<Self> {
        let directory = open_directory(path)?;
        let id = file_id(&directory)?;
        Ok(Self { _directory: directory, id })
    }

    pub(super) fn matches(&self, path: &Path) -> bool {
        self.verify_path(path).is_ok()
    }

    pub(super) fn verify_path(&self, path: &Path) -> Result<(), RecoveryDirectoryError> {
        verify_id(Self::read(path)?.id, self.id)
    }

    #[cfg(any(target_os = "linux", windows))]
    pub(super) fn pin(&self, path: &Path) -> Result<PinnedDirectory, RecoveryDirectoryError> {
        let directory = open_pinned_directory(path)?;
        let id = file_id(&directory)?;
        verify_id(id, self.id)?;
        let path = pinned_path(&directory, id)?;
        Ok(PinnedDirectory { directory, path })
    }

    #[cfg(not(any(target_os = "linux", windows)))]
    pub(super) fn pin(&self, _path: &Path) -> Result<PinnedDirectory, RecoveryDirectoryError> {
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }
}

fn verify_id(actual: FileId, expected: FileId) -> Result<(), RecoveryDirectoryError> {
    if actual != expected {
        return Err(RecoveryDirectoryError::IdentityChanged);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
use self::open_directory as open_pinned_directory;

#[cfg(target_os = "linux")]
fn pinned_path(directory: &File, id: FileId) -> Result<PathBuf, RecoveryDirectoryError> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};

    let path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    let metadata = path.metadata()?;
    verify_id((metadata.dev(), metadata.ino().into()), id)?;
    Ok(path)
}

#[cfg(not(windows))]
fn open_directory(path: &Path) -> io::Result<File> {
    File::open(path)
}

#[cfg(unix)]
fn file_id(directory: &File) -> io::Result<FileId> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::from(io::ErrorKind::NotADirectory));
    }
    Ok((metadata.dev(), metadata.ino().into()))
}

#[cfg(windows)]
const FILE_READ_ATTRIBUTES: u32 = 0x80;
#[cfg(windows)]
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

#[cfg(windows)]
fn open_directory(path: &Path) -> io::Result<File> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};

    // OpenOptions shares reads, writes and deletes by default.
    OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(windows)]
fn open_pinned_directory(path: &Path) -> io::Result<File> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};

    const FILE_LIST_DIRECTORY: u32 = 0x1;
    const FILE_SHARE_READ: u32 = 0x1;
    const FILE_SHARE_WRITE: u32 = 0x2;
    // Attribute-only handles do not participate in sharing checks. Listing
    // access and omitting FILE_SHARE_DELETE prevent renames while pinned.
    OpenOptions::new()
        .access_mode(FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(windows)]
fn pinned_path(directory: &File, id: FileId) -> Result<PathBuf, RecoveryDirectoryError> {
    use std::{ffi::c_void, os::windows::io::AsRawHandle, ptr};

    const FILE_NAME_NORMALIZED: u32 = 0;
    const VOLUME_NAME_DOS: u32 = 0;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFinalPathNameByHandleW(
            handle: *mut c_void,
            path: *mut u16,
            length: u32,
            flags: u32,
        ) -> u32;
    }

    let flags = FILE_NAME_NORMALIZED | VOLUME_NAME_DOS;
    let handle = directory.as_raw_handle();
    // SAFETY: directory owns the handle. A null buffer of size zero queries
    // the required length including the terminating NUL.
    let len = unsafe { GetFinalPathNameByHandleW(handle, ptr::null_mut(), 0, flags) };
    if len == 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut buffer = vec![0; len as usize];
    // SAFETY: buffer has len writable UTF-16 elements and handle is live.
    let written = unsafe { GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), len, flags) };
    if written == 0 {
        return Err(io::Error::last_os_error().into());
    }
    if written >= len {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "pinned directory path changed").into());
    }

    // RocksDB expects UTF-8 and appends filenames with forward slashes.
    let path = String::from_utf16(&buffer[..written as usize])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let path = if let Some(unc) = path.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{unc}"))
    } else if let Some(drive) = path.strip_prefix(r"\\?\")
        && let [letter, b':', b'\\', ..] = drive.as_bytes()
        && letter.is_ascii_alphabetic()
    {
        PathBuf::from(drive)
    } else {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "unsupported pinned directory path").into());
    };
    if !path.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "unsupported pinned directory path").into());
    }
    verify_id(file_id(&open_directory(&path)?)?, id)?;
    Ok(path)
}

#[cfg(windows)]
fn file_id(directory: &File) -> io::Result<FileId> {
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
fn file_id(_directory: &File) -> io::Result<FileId> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}
