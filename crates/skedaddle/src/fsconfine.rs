//! Filesystem operations rooted in retained directory handles.
//!
//! Every user component is opened separately without following links. A renamed
//! directory remains the same capability; replacing its old name cannot redirect
//! an operation. Callers must use a dedicated product root (hard links and mounts
//! are outside the symlink/reparse-point threat model).
use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "unsafe confined filesystem path",
    )
}

fn components(path: &Path) -> io::Result<Vec<OsString>> {
    // Reject Windows separators, streams and dot components on every platform.
    // Path::components normalizes internal `.` so check the raw spelling too.
    let spelling = path.to_string_lossy();
    if spelling.is_empty()
        || spelling.contains(['\\', ':', '\0'])
        || spelling
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(invalid());
    }
    path.components()
        .map(|c| match c {
            Component::Normal(name) => Ok(name.to_owned()),
            _ => Err(invalid()),
        })
        .collect()
}

/// A directory capability. Parent handles remain alive as well, preventing
/// Windows rename/delete of checked ancestors for path-only core APIs.
#[derive(Clone)]
pub struct Dir {
    file: Arc<File>,
    parent: Option<Arc<Dir>>,
}

impl Dir {
    /// Open an absolute root once, walking from the filesystem/volume root.
    pub fn open_root(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(invalid());
        }
        let mut base = PathBuf::new();
        let mut names = Vec::new();
        for component in path.components() {
            match component {
                Component::Prefix(_) | Component::RootDir if names.is_empty() => {
                    base.push(component)
                }
                Component::Normal(name) => {
                    // Validate without rejecting the platform's absolute prefix.
                    components(Path::new(name))?;
                    names.push(name);
                }
                _ => return Err(invalid()),
            }
        }
        let mut dir = Self {
            file: Arc::new(platform::root(&base)?),
            parent: None,
        };
        for name in names {
            dir = dir.child(name, false, false)?;
        }
        Ok(dir)
    }

    fn child(&self, name: &OsStr, create: bool, exclusive: bool) -> io::Result<Self> {
        let file = platform::directory(&self.file, name, create, exclusive)?;
        Ok(Self {
            file: Arc::new(file),
            parent: Some(Arc::new(self.clone())),
        })
    }

    /// Walk a relative directory, optionally creating missing components.
    pub fn dir(&self, path: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        let mut dir = self.clone();
        for name in components(path.as_ref())? {
            dir = dir.child(&name, create, false)?;
        }
        Ok(dir)
    }

    /// Create one new directory. Never adopt an existing scratch directory.
    pub fn create_dir(&self, name: impl AsRef<Path>) -> io::Result<Self> {
        let parts = components(name.as_ref())?;
        if parts.len() != 1 {
            return Err(invalid());
        }
        self.child(&parts[0], true, true)
    }

    fn parent_leaf(&self, path: &Path) -> io::Result<(Self, OsString)> {
        let mut parts = components(path)?;
        let leaf = parts.pop().ok_or_else(invalid)?;
        let mut parent = self.clone();
        for name in parts {
            parent = parent.child(&name, false, false)?;
        }
        Ok((parent, leaf))
    }

    pub fn open_file(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let (parent, leaf) = self.parent_leaf(path.as_ref())?;
        platform::file(&parent.file, &leaf, false)
    }

    pub fn create_file(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let (parent, leaf) = self.parent_leaf(path.as_ref())?;
        platform::file(&parent.file, &leaf, true)
    }

    /// No-clobber publication; both names are relative to pinned directories.
    pub fn rename_new(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
        let (source, from) = self.parent_leaf(from.as_ref())?;
        let (dest, to) = self.parent_leaf(to.as_ref())?;
        platform::rename_new(&source.file, &from, &dest.file, &to)
    }

    pub fn remove_file(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let (parent, leaf) = self.parent_leaf(path.as_ref())?;
        let file = platform::file(&parent.file, &leaf, false)?;
        self.remove_open_file_in(&parent, &leaf, &file)
    }

    /// Retain an owner's lock through reclamation. Windows disposes the exact
    /// opened object; Unix unlinks a leaf in a pinned parent, never its target.
    pub(crate) fn remove_open_file(&self, name: impl AsRef<Path>, file: &File) -> io::Result<()> {
        let (parent, leaf) = self.parent_leaf(name.as_ref())?;
        self.remove_open_file_in(&parent, &leaf, file)
    }

    fn remove_open_file_in(&self, parent: &Self, name: &OsStr, file: &File) -> io::Result<()> {
        platform::remove_file(&parent.file, name, file)
    }

    pub fn remove_dir(&self, name: impl AsRef<Path>) -> io::Result<()> {
        let (parent, leaf) = self.parent_leaf(name.as_ref())?;
        platform::remove_dir(&parent.file, &leaf)
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Names are enumerated through the directory handle. Their type must be
    /// established by opening them through this capability, never by path.
    pub fn names(&self) -> io::Result<Vec<OsString>> {
        platform::names(&self.file)
    }

    /// Compatibility guards for a path-only upstream API. Product cleanup must
    /// still use this capability. Unix callers must use `core_path` below.
    pub fn guards(&self) -> io::Result<Vec<File>> {
        let mut files = if let Some(parent) = &self.parent {
            parent.guards()?
        } else {
            Vec::new()
        };
        files.push(self.file.try_clone()?);
        Ok(files)
    }

    /// Bridge for giggles-core's path-only export/restore interface. On Linux,
    /// the kernel's fd namespace anchors resolution to this retained directory.
    /// On Windows retained handles deny ancestor rename/delete. This path must
    /// only be used while this Dir is alive, with a validated single leaf.
    pub fn core_path(&self, configured: &Path, name: &str) -> io::Result<PathBuf> {
        if components(Path::new(name))?.len() != 1 {
            return Err(invalid());
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let _ = configured;
            Ok(PathBuf::from(format!("/proc/self/fd/{}", self.file.as_raw_fd())).join(name))
        }
        #[cfg(windows)]
        {
            Ok(configured.join(name))
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = configured;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "core export requires an fd path",
            ))
        }
    }
}

#[cfg(unix)]
mod platform {
    use super::*;
    use rustix::fs::{self, AtFlags, Mode, OFlags};

    fn flags(directory: bool) -> OFlags {
        OFlags::CLOEXEC
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | if directory {
                OFlags::RDONLY | OFlags::DIRECTORY
            } else {
                OFlags::RDWR
            }
    }
    pub fn root(path: &Path) -> io::Result<File> {
        Ok(fs::open(path, flags(true), Mode::empty())?.into())
    }
    pub fn directory(
        parent: &File,
        name: &OsStr,
        create: bool,
        exclusive: bool,
    ) -> io::Result<File> {
        if create {
            match fs::mkdirat(parent, name, Mode::RWXU) {
                Ok(()) => parent.sync_all()?,
                Err(rustix::io::Errno::EXIST) if !exclusive => (),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(fs::openat(parent, name, flags(true), Mode::empty())?.into())
    }
    pub fn file(parent: &File, name: &OsStr, create: bool) -> io::Result<File> {
        let file: File = fs::openat(
            parent,
            name,
            flags(false)
                | if create {
                    OFlags::CREATE | OFlags::EXCL
                } else {
                    OFlags::empty()
                },
            Mode::RUSR | Mode::WUSR,
        )?
        .into();
        if !file.metadata()?.is_file() {
            return Err(invalid());
        }
        Ok(file)
    }
    pub fn rename_new(parent: &File, from: &OsStr, dest: &File, to: &OsStr) -> io::Result<()> {
        let _source = file(parent, from, false)?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        fs::renameat_with(parent, from, dest, to, fs::RenameFlags::NOREPLACE)?;
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            // Portable no-clobber fallback where renameat2 is unavailable.
            fs::linkat(parent, from, dest, to, AtFlags::empty())?;
            fs::unlinkat(parent, from, AtFlags::empty())?;
        }
        Ok(())
    }
    pub fn remove_file(parent: &File, name: &OsStr, _file: &File) -> io::Result<()> {
        Ok(fs::unlinkat(parent, name, AtFlags::empty())?)
    }
    pub fn remove_dir(parent: &File, name: &OsStr) -> io::Result<()> {
        let _dir = directory(parent, name, false, false)?;
        Ok(fs::unlinkat(parent, name, AtFlags::REMOVEDIR)?)
    }
    pub fn names(file: &File) -> io::Result<Vec<OsString>> {
        use std::os::unix::ffi::OsStrExt;
        fs::Dir::read_from(file)?
            .filter_map(|entry| match entry {
                Ok(entry)
                    if entry.file_name().to_bytes() == b"."
                        || entry.file_name().to_bytes() == b".." =>
                {
                    None
                }
                Ok(entry) => Some(Ok(
                    OsStr::from_bytes(entry.file_name().to_bytes()).to_owned()
                )),
                Err(e) => Some(Err(e.into())),
            })
            .collect()
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::{
        ffi::c_void,
        mem,
        os::windows::{
            ffi::{OsStrExt, OsStringExt},
            fs::{MetadataExt, OpenOptionsExt},
            io::{AsRawHandle, FromRawHandle},
        },
        ptr,
    };
    type Handle = *mut c_void;
    #[repr(C)]
    struct Unicode {
        length: u16,
        maximum: u16,
        buffer: *mut u16,
    }
    #[repr(C)]
    struct Attributes {
        length: u32,
        root: Handle,
        name: *mut Unicode,
        flags: u32,
        security: Handle,
        qos: Handle,
    }
    #[repr(C)]
    struct Status {
        status: usize,
        information: usize,
    }
    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn NtCreateFile(
            out: *mut Handle,
            access: u32,
            attributes: *mut Attributes,
            status: *mut Status,
            allocation: *const i64,
            file_attributes: u32,
            share: u32,
            disposition: u32,
            options: u32,
            ea: Handle,
            ea_length: u32,
        ) -> i32;
        fn NtSetInformationFile(
            file: Handle,
            status: *mut Status,
            info: Handle,
            length: u32,
            class: u32,
        ) -> i32;
        fn NtQueryDirectoryFile(
            file: Handle,
            event: Handle,
            apc: Handle,
            context: Handle,
            status: *mut Status,
            info: Handle,
            length: u32,
            class: u32,
            single: u8,
            pattern: *mut Unicode,
            restart: u8,
        ) -> i32;
        fn RtlNtStatusToDosError(status: i32) -> u32;
    }
    fn checked(status: i32) -> io::Result<()> {
        if status < 0 {
            // SAFETY: pure NTSTATUS conversion, no pointers.
            Err(io::Error::from_raw_os_error(
                unsafe { RtlNtStatusToDosError(status) } as i32,
            ))
        } else {
            Ok(())
        }
    }
    fn validate(file: File, directory: bool) -> io::Result<File> {
        let meta = file.metadata()?;
        if meta.file_attributes() & 0x400 != 0
            || meta.is_dir() != directory
            || (!directory && !meta.is_file())
        {
            return Err(invalid());
        }
        Ok(file)
    }
    pub fn root(path: &Path) -> io::Result<File> {
        // Only the volume root is opened by Win32 path. All subsequent names
        // use NtCreateFile.RootDirectory. OPEN_REPARSE_POINT | BACKUP_SEMANTICS.
        validate(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(3)
                .custom_flags(0x00200000 | 0x02000000)
                .open(path)?,
            true,
        )
    }
    fn open(
        parent: &File,
        name: &OsStr,
        directory: bool,
        create: bool,
        exclusive: bool,
        delete: bool,
    ) -> io::Result<File> {
        let mut wide: Vec<u16> = name.encode_wide().collect();
        let length = u16::try_from(wide.len() * 2).map_err(|_| invalid())?;
        let mut unicode = Unicode {
            length,
            maximum: length,
            buffer: wide.as_mut_ptr(),
        };
        let mut attrs = Attributes {
            length: mem::size_of::<Attributes>() as u32,
            root: parent.as_raw_handle(),
            name: &mut unicode,
            flags: 0x40,
            security: ptr::null_mut(),
            qos: ptr::null_mut(),
        };
        let mut status = Status {
            status: 0,
            information: 0,
        };
        let mut handle = ptr::null_mut();
        // FILE_OPEN_REPARSE_POINT (native equivalent of the Win32 flag),
        // synchronous I/O, backup intent, and an explicit directory/file type.
        // Directories deny FILE_SHARE_DELETE; leaves allow handle disposition.
        let options = 0x00200000 | 0x20 | 0x4000 | if directory { 1 } else { 0x40 };
        let access = 0x00100000 | 0x80000000 | 0x40000000 | if delete { 0x10000 } else { 0 };
        // SAFETY: all structures/buffers live through this synchronous call.
        checked(unsafe {
            NtCreateFile(
                &mut handle,
                access,
                &mut attrs,
                &mut status,
                ptr::null(),
                0,
                if directory && !delete { 3 } else { 7 },
                if exclusive {
                    2
                } else if create {
                    3
                } else {
                    1
                },
                options,
                ptr::null_mut(),
                0,
            )
        })?;
        // SAFETY: successful NtCreateFile returned one owned file handle.
        validate(unsafe { File::from_raw_handle(handle) }, directory)
    }
    pub fn directory(
        parent: &File,
        name: &OsStr,
        create: bool,
        exclusive: bool,
    ) -> io::Result<File> {
        let result = open(parent, name, true, create, exclusive, false)?;
        if create {
            parent.sync_all()?;
        }
        Ok(result)
    }
    pub fn file(parent: &File, name: &OsStr, create: bool) -> io::Result<File> {
        open(parent, name, false, create, create, true)
    }
    pub fn rename_new(parent: &File, from: &OsStr, dest: &File, to: &OsStr) -> io::Result<()> {
        let source = file(parent, from, false)?;
        // FILE_RENAME_INFORMATION has a variable UTF-16 tail, naturally aligned.
        #[repr(C)]
        struct Rename {
            replace: u8,
            root: Handle,
            length: u32,
            name: [u16; 1],
        }
        let wide: Vec<u16> = to.encode_wide().collect();
        let offset = mem::offset_of!(Rename, name);
        // NT requires at least the full structure size, including its trailing
        // alignment padding, even when the UTF-16 name fits before that padding.
        let bytes = (offset + wide.len() * 2).max(mem::size_of::<Rename>());
        let mut buffer = vec![0usize; bytes.div_ceil(mem::size_of::<usize>())];
        let info = buffer.as_mut_ptr().cast::<Rename>();
        let mut status = Status {
            status: 0,
            information: 0,
        };
        // SAFETY: aligned buffer contains the fixed header and complete tail.
        unsafe {
            (*info).replace = 0;
            (*info).root = dest.as_raw_handle();
            (*info).length = (wide.len() * 2) as u32;
            ptr::copy_nonoverlapping(
                wide.as_ptr(),
                (info.cast::<u8>()).add(offset).cast::<u16>(),
                wide.len(),
            );
            checked(NtSetInformationFile(
                source.as_raw_handle(),
                &mut status,
                info.cast(),
                bytes as u32,
                10,
            ))
        }
    }
    fn dispose(file: &File) -> io::Result<()> {
        let mut delete: u8 = 1;
        let mut status = Status {
            status: 0,
            information: 0,
        };
        // SAFETY: one-byte FILE_DISPOSITION_INFORMATION, valid owned handle.
        checked(unsafe {
            NtSetInformationFile(
                file.as_raw_handle(),
                &mut status,
                (&mut delete as *mut u8).cast(),
                1,
                13,
            )
        })
    }
    pub fn remove_file(_parent: &File, _name: &OsStr, file: &File) -> io::Result<()> {
        dispose(file)
    }
    pub fn remove_dir(parent: &File, name: &OsStr) -> io::Result<()> {
        let dir = open(parent, name, true, false, false, true)?;
        dispose(&dir)
    }
    pub fn names(file: &File) -> io::Result<Vec<OsString>> {
        // Use a fresh relative directory handle so simultaneous enumerations do
        // not share the kernel enumeration cursor. An empty native name
        // reopens RootDirectory itself; NT does not normalize Win32 `.`.
        // This bypass is internal: public relative paths still reject empties.
        let scan = directory(file, OsStr::new(""), false, false)?;
        let mut result = Vec::new();
        let mut buffer = [0u64; 8192];
        let mut restart = 1;
        loop {
            let mut status = Status {
                status: 0,
                information: 0,
            };
            // SAFETY: writable aligned buffer; synchronous directory handle.
            let code = unsafe {
                NtQueryDirectoryFile(
                    scan.as_raw_handle(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    &mut status,
                    buffer.as_mut_ptr().cast(),
                    mem::size_of_val(&buffer) as u32,
                    12,
                    0,
                    ptr::null_mut(),
                    restart,
                )
            };
            if code as u32 == 0x80000006 {
                break;
            } // STATUS_NO_MORE_FILES
            checked(code)?;
            restart = 0;
            if status.information > mem::size_of_val(&buffer) {
                return Err(invalid());
            }
            let mut offset = 0;
            loop {
                // FILE_NAMES_INFORMATION: next offset, index, byte length, name.
                if offset + 12 > status.information {
                    return Err(invalid());
                }
                // SAFETY: bounds verified before reading the header and tail.
                let bytes = unsafe {
                    std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), status.information)
                };
                let next = u32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("header"))
                    as usize;
                let length =
                    u32::from_ne_bytes(bytes[offset + 8..offset + 12].try_into().expect("header"))
                        as usize;
                if !length.is_multiple_of(2) || length > bytes.len() - offset - 12 {
                    return Err(invalid());
                }
                let wide: Vec<u16> = bytes[offset + 12..offset + 12 + length]
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| u16::from_ne_bytes(*c))
                    .collect();
                let name = OsString::from_wide(&wide);
                if name != "." && name != ".." {
                    result.push(name);
                }
                if next == 0 {
                    break;
                }
                if next < 12 || next > bytes.len() - offset {
                    return Err(invalid());
                }
                offset = offset.checked_add(next).ok_or_else(invalid)?;
            }
        }
        Ok(result)
    }
}
