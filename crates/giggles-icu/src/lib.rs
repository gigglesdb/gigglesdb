//! Pinned ICU4C 76.1 runtime and PostgreSQL-compatible collation operations.
//!
//! UTF-8 comparison and sort keys require valid UTF-8 (invalid input panics).
//! Sort keys include ICU's terminating zero. Keep the runtime and its data with
//! any persisted keys; changing ICU requires rebuilding those keys.
use libloading::Library;
use std::{
    cmp::Ordering,
    collections::HashMap,
    ffi::{CStr, CString, c_char, c_void},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

type CaseMap = unsafe extern "C" fn(*mut u16, i32, *const u16, i32, *const c_char, *mut i32) -> i32;
type TitleMap = unsafe extern "C" fn(*mut u16, i32, *const u16, i32, *mut c_void, *const c_char, *mut i32) -> i32;
type Open = unsafe extern "C" fn(*const c_char, *mut i32) -> *mut c_void;
type Close = unsafe extern "C" fn(*mut c_void);
type Compare = unsafe extern "C" fn(*const c_void, *const u8, i32, *const u8, i32, *mut i32) -> i32;
type Key = unsafe extern "C" fn(*const c_void, *const u16, i32, *mut u8, i32) -> i32;
type Version = unsafe extern "C" fn(*const c_void, *mut u8);
type Rules = unsafe extern "C" fn(*const c_void, *mut i32) -> *const u16;
type OpenRules =
    unsafe extern "C" fn(*const u16, i32, i32, i32, *mut c_void, *mut i32) -> *mut c_void;
type Tag = unsafe extern "C" fn(*const c_char, *mut c_char, i32, i8, *mut i32) -> i32;
type Language = unsafe extern "C" fn(*const c_char, *mut c_char, i32, *mut i32) -> i32;
type Count = unsafe extern "C" fn() -> i32;
type Available = unsafe extern "C" fn(i32) -> *const c_char;
type ErrorName = unsafe extern "C" fn(i32) -> *const c_char;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcuError(pub String);
impl std::fmt::Display for IcuError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for IcuError {}
fn cstring(s: &str) -> Result<CString, IcuError> {
    CString::new(s).map_err(|_| IcuError("ICU locale contains NUL".into()))
}
fn length(n: usize) -> i32 {
    i32::try_from(n).expect("ICU input exceeds i32::MAX")
}

pub struct Icu {
    upper: CaseMap,
    lower: CaseMap,
    title: TitleMap,
    open: Open,
    close: Close,
    compare: Compare,
    key: Key,
    version: Version,
    rules: Rules,
    open_rules: OpenRules,
    tag: Tag,
    language: Language,
    count: Count,
    available: Available,
    locale_count: Count,
    locale_available: Available,
    error_name: ErrorName,
    // Drop dependents before their dependencies.
    _i18n: Library,
    _uc: Library,
    _data: Library,
}
impl std::fmt::Debug for Icu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Icu(76.1)")
    }
}
static DEFAULT: OnceLock<Result<Arc<Icu>, IcuError>> = OnceLock::new();
type Registry = Mutex<HashMap<PathBuf, Arc<Icu>>>;
static REGISTRY: OnceLock<Registry> = OnceLock::new();
// Match scripts/icu/provision.sh, including shell ${VAR:-default} semantics.
fn provision_cache() -> Option<PathBuf> {
    let nonempty = |name: &str| std::env::var_os(name).filter(|value| !value.is_empty());
    #[cfg(windows)]
    let (root, platform) = (
        nonempty("GIGGLES_ICU_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("J:/gigglesdb-work/icu")),
        "windows",
    );
    #[cfg(target_os = "linux")]
    let (root, platform) = (
        nonempty("GIGGLES_ICU_CACHE").map(PathBuf::from).or_else(|| {
            nonempty("XDG_CACHE_HOME")
                .map(PathBuf::from)
                .or_else(|| nonempty("HOME").map(|home| PathBuf::from(home).join(".cache")))
                .map(|cache| cache.join("gigglesdb-icu"))
        })?,
        "linux",
    );
    #[cfg(any(windows, target_os = "linux"))]
    {
        Some(root.join("76.1").join(platform))
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = nonempty;
        None
    }
}
impl Icu {
    pub fn load() -> Result<Arc<Self>, IcuError> {
        DEFAULT
            .get_or_init(|| {
                if let Some(dir) = std::env::var_os("GIGGLES_ICU_DIR") {
                    return Self::load_from(dir);
                }
                let mut errors = Vec::new();
                if let Ok(exe) = std::env::current_exe() {
                    match Self::load_from(exe.parent().unwrap().join("icu")) {
                        Ok(icu) => return Ok(icu),
                        Err(e) => errors.push(e.to_string()),
                    }
                }
                if let Some(cache) = provision_cache() {
                    match Self::load_from(cache) {
                        Ok(icu) => return Ok(icu),
                        Err(e) => errors.push(e.to_string()),
                    }
                }
                #[cfg(target_os = "linux")]
                match Self::load_from(Path::new("")) {
                    Ok(icu) => return Ok(icu),
                    Err(e) => errors.push(e.to_string()),
                }
                Err(IcuError(format!(
                    "ICU 76.1 unavailable: {}",
                    errors.join("; ")
                )))
            })
            .clone()
    }
    pub fn load_from(dir: impl AsRef<Path>) -> Result<Arc<Self>, IcuError> {
        let dir = dir.as_ref();
        let key = if dir.as_os_str().is_empty() {
            PathBuf::new()
        } else {
            dir.canonicalize()
                .map_err(|e| IcuError(format!("ICU {}: {e}", dir.display())))?
        };
        let mut registry = REGISTRY
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap();
        if let Some(icu) = registry.get(&key) {
            return Ok(icu.clone());
        }
        // SAFETY: libraries are pinned to the versioned C ABI; all function
        // pointers remain owned by Icu for at least as long as any collator.
        let icu = unsafe { Self::load_libraries(&key) }
            .map_err(|e| IcuError(format!("ICU {}: {e}", dir.display())))?;
        let icu = Arc::new(icu);
        registry.insert(key, icu.clone());
        Ok(icu)
    }
    unsafe fn load_libraries(dir: &Path) -> Result<Self, IcuError> {
        #[cfg(windows)]
        let names = ["icudt76.dll", "icuuc76.dll", "icuin76.dll"];
        #[cfg(not(windows))]
        let names = ["libicudata.so.76", "libicuuc.so.76", "libicui18n.so.76"];
        // RTLD_GLOBAL allows ICU libraries to resolve one another in a private
        // directory without LD_LIBRARY_PATH (which is fixed at process start).
        unsafe fn library(path: &Path) -> Result<Library, IcuError> {
            #[cfg(unix)]
            let result = unsafe {
                libloading::os::unix::Library::open(
                    Some(path),
                    libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_GLOBAL,
                )
            }
            .map(Library::from);
            #[cfg(not(unix))]
            let result = unsafe { Library::new(path) };
            result.map_err(|e| IcuError(e.to_string()))
        }
        unsafe fn symbol<T: Copy>(lib: &Library, name: &[u8]) -> Result<T, IcuError> {
            unsafe { lib.get::<T>(name) }
                .map(|s| *s)
                .map_err(|e| IcuError(e.to_string()))
        }
        // SAFETY: every declaration below follows ICU4C's public C headers.
        unsafe {
            let data = library(&dir.join(names[0]))?;
            let uc = library(&dir.join(names[1]))?;
            let i18n = library(&dir.join(names[2]))?;
            let get_version: unsafe extern "C" fn(*mut u8) = symbol(&uc, b"u_getVersion_76")?;
            let mut version = [0; 4];
            get_version(version.as_mut_ptr());
            if version != [76, 1, 0, 0] {
                return Err(IcuError(format!(
                    "expected ICU 76.1.0.0, found {}.{}.{}.{}",
                    version[0], version[1], version[2], version[3]
                )));
            }
            Ok(Self {
                upper: symbol(&uc, b"u_strToUpper_76")?,
                lower: symbol(&uc, b"u_strToLower_76")?,
                title: symbol(&uc, b"u_strToTitle_76")?,
                open: symbol(&i18n, b"ucol_open_76")?,
                close: symbol(&i18n, b"ucol_close_76")?,
                compare: symbol(&i18n, b"ucol_strcollUTF8_76")?,
                key: symbol(&i18n, b"ucol_getSortKey_76")?,
                version: symbol(&i18n, b"ucol_getVersion_76")?,
                rules: symbol(&i18n, b"ucol_getRules_76")?,
                open_rules: symbol(&i18n, b"ucol_openRules_76")?,
                tag: symbol(&uc, b"uloc_toLanguageTag_76")?,
                language: symbol(&uc, b"uloc_getLanguage_76")?,
                count: symbol(&i18n, b"ucol_countAvailable_76")?,
                available: symbol(&i18n, b"ucol_getAvailable_76")?,
                locale_count: symbol(&uc, b"uloc_countAvailable_76")?,
                locale_available: symbol(&uc, b"uloc_getAvailable_76")?,
                error_name: symbol(&uc, b"u_errorName_76")?,
                _i18n: i18n,
                _uc: uc,
                _data: data,
            })
        }
    }
    /// ICU locale-sensitive UTF-16 casing, converted back to UTF-8.
    pub fn case_map(&self, locale: &str, operation: &str, text: &str) -> Result<String, IcuError> {
        let locale = cstring(locale)?;
        let input: Vec<u16> = text.encode_utf16().collect();
        let mut output = vec![0; input.len().saturating_mul(2).saturating_add(16)];
        loop {
            let mut status = 0;
            // SAFETY: bounded UTF-16 buffers, terminated locale, no retained pointers.
            let n = unsafe {
                match operation {
                    "upper" => (self.upper)(output.as_mut_ptr(), length(output.len()), input.as_ptr(), length(input.len()), locale.as_ptr(), &mut status),
                    "lower" => (self.lower)(output.as_mut_ptr(), length(output.len()), input.as_ptr(), length(input.len()), locale.as_ptr(), &mut status),
                    "initcap" => (self.title)(output.as_mut_ptr(), length(output.len()), input.as_ptr(), length(input.len()), std::ptr::null_mut(), locale.as_ptr(), &mut status),
                    _ => return Err(IcuError("unknown case operation".into())),
                }
            };
            if status == 15 { output.resize(n as usize + 1, 0); continue; }
            if status > 0 { return Err(IcuError(self.error(status))); }
            return String::from_utf16(&output[..n as usize]).map_err(|e| IcuError(e.to_string()));
        }
    }
    fn error(&self, status: i32) -> String {
        // SAFETY: ICU returns a static NUL-terminated string for any status.
        unsafe {
            CStr::from_ptr((self.error_name)(status))
                .to_string_lossy()
                .into_owned()
        }
    }
    pub fn available_collation_locales(&self) -> Vec<String> {
        // SAFETY: ICU's enumerated locale strings have static storage.
        unsafe {
            (0..(self.count)())
                .map(|i| {
                    CStr::from_ptr((self.available)(i))
                        .to_string_lossy()
                        .into_owned()
                })
                .collect()
        }
    }
    pub fn canonical_language_tag(&self, locale: &str) -> Result<LanguageTag, IcuError> {
        let input = cstring(locale)?;
        let mut warnings = Vec::new();
        let mut buffer = vec![0u8; 32];
        let tag = loop {
            let mut status = 0;
            // SAFETY: input is terminated and the output has the advertised capacity.
            let n = unsafe {
                (self.tag)(
                    input.as_ptr(),
                    buffer.as_mut_ptr().cast(),
                    length(buffer.len()),
                    1,
                    &mut status,
                )
            };
            if status == 15 || status == -124 {
                buffer.resize(
                    buffer
                        .len()
                        .checked_mul(2)
                        .ok_or_else(|| IcuError("language tag too long".into()))?,
                    0,
                );
                continue;
            }
            if status > 0 {
                warnings.push(format!(
                    "could not convert locale name \"{locale}\" to language tag: {}",
                    self.error(status)
                ));
                break locale.to_owned();
            }
            break String::from_utf8(buffer[..n as usize].to_vec())
                .expect("ICU language tag is ASCII");
        };
        // PostgreSQL maps C/POSIX to en_US_POSIX when opening collators.
        let tag = if matches!(locale, "C" | "POSIX") {
            "en-US-u-va-posix".to_owned()
        } else {
            tag
        };
        let get_language = |s: &CStr| -> Result<String, IcuError> {
            let mut out = [0u8; 12];
            let mut status = 0;
            // SAFETY: bounded buffer and terminated locale, no retained pointers.
            let n = unsafe {
                (self.language)(
                    s.as_ptr(),
                    out.as_mut_ptr().cast(),
                    length(out.len()),
                    &mut status,
                )
            };
            if status > 0 || status == -124 {
                return Err(IcuError(self.error(status)));
            }
            Ok(String::from_utf8_lossy(&out[..n as usize]).into_owned())
        };
        let canonical = cstring(&tag)?;
        match get_language(&canonical) {
            Err(e) => warnings.push(format!(
                "could not get language from ICU locale \"{tag}\": {e}"
            )),
            Ok(lang) => {
                let mut found = matches!(lang.as_str(), "" | "root" | "und");
                // SAFETY: ICU locale enumeration returns static terminated strings.
                for i in 0..unsafe { (self.locale_count)() } {
                    if found {
                        break;
                    }
                    let other = unsafe { CStr::from_ptr((self.locale_available)(i)) };
                    found = get_language(other).is_ok_and(|s| s == lang);
                }
                if !found {
                    warnings.push(format!(
                        "ICU locale \"{tag}\" has unknown language \"{lang}\""
                    ));
                }
            }
        }
        let notice = (tag != locale)
            .then(|| format!("using standard form \"{tag}\" for ICU locale \"{locale}\""));
        Ok(LanguageTag {
            tag,
            notice,
            warnings,
        })
    }
}

/// Diagnostic message bodies (SQLSTATE 00000 for notice, 22023 for warnings).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguageTag {
    pub tag: String,
    pub notice: Option<String>,
    pub warnings: Vec<String>,
}
pub fn canonical_language_tag(locale: &str) -> Result<LanguageTag, IcuError> {
    Icu::load()?.canonical_language_tag(locale)
}
pub fn available_collation_locales() -> Result<Vec<String>, IcuError> {
    Ok(Icu::load()?.available_collation_locales())
}

struct Handle(*mut c_void);
// SAFETY: Handle is only accessed under its collator's mutex. ICU permits
// moving collators between threads and use by one thread at a time.
unsafe impl Send for Handle {}
pub struct Collator {
    handle: Mutex<Handle>,
    icu: Arc<Icu>,
}
impl Collator {
    pub fn open(locale: &str, rules: Option<&str>) -> Result<Self, IcuError> {
        Self::open_with(Icu::load()?, locale, rules)
    }
    pub fn open_with(icu: Arc<Icu>, locale: &str, rules: Option<&str>) -> Result<Self, IcuError> {
        let locale = cstring(if matches!(locale, "C" | "POSIX") {
            "en-US-u-va-posix"
        } else {
            locale
        })?;
        let mut status = 0;
        // SAFETY: locale is terminated; ICU retains no pointer to the input.
        let raw = unsafe { (icu.open)(locale.as_ptr(), &mut status) };
        if status > 0 || raw.is_null() {
            if !raw.is_null() {
                unsafe { (icu.close)(raw) };
            }
            return Err(IcuError(format!("ucol_open: {}", icu.error(status))));
        }
        let mut coll = Self {
            handle: Mutex::new(Handle(raw)),
            icu,
        };
        if let Some(rules) = rules {
            let mut n = 0;
            // SAFETY: base rules remain valid while the owned base collator lives.
            let base = unsafe { (coll.icu.rules)(raw, &mut n) };
            let mut all = if n == 0 {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(base, n as usize) }.to_vec()
            };
            all.extend(rules.encode_utf16());
            status = 0;
            // ICU's default normalization and tertiary strength match PG18.
            let tailored = unsafe {
                (coll.icu.open_rules)(
                    all.as_ptr(),
                    length(all.len()),
                    -1,
                    2,
                    std::ptr::null_mut(),
                    &mut status,
                )
            };
            if status > 0 || tailored.is_null() {
                if !tailored.is_null() {
                    unsafe { (coll.icu.close)(tailored) };
                }
                return Err(IcuError(format!(
                    "ucol_openRules: {}",
                    coll.icu.error(status)
                )));
            }
            unsafe { (coll.icu.close)(raw) };
            coll.handle = Mutex::new(Handle(tailored));
        }
        Ok(coll)
    }
    pub fn compare_utf8(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.compare_str(
            std::str::from_utf8(a).expect("ICU requires valid UTF-8"),
            std::str::from_utf8(b).expect("ICU requires valid UTF-8"),
        )
    }
    pub fn compare_str(&self, a: &str, b: &str) -> Ordering {
        let handle = self.handle.lock().unwrap();
        let mut status = 0;
        // SAFETY: mutex excludes concurrent access; inputs have explicit lengths.
        let result = unsafe {
            (self.icu.compare)(
                handle.0,
                a.as_ptr(),
                length(a.len()),
                b.as_ptr(),
                length(b.len()),
                &mut status,
            )
        };
        assert!(
            status <= 0,
            "ICU comparison failed: {}",
            self.icu.error(status)
        );
        result.cmp(&0)
    }
    pub fn sort_key(&self, s: &[u8]) -> Vec<u8> {
        let utf16: Vec<u16> = std::str::from_utf8(s)
            .expect("ICU requires valid UTF-8")
            .encode_utf16()
            .collect();
        let handle = self.handle.lock().unwrap();
        // SAFETY: preflight uses zero capacity; second call has exact capacity.
        unsafe {
            let n = (self.icu.key)(
                handle.0,
                utf16.as_ptr(),
                length(utf16.len()),
                std::ptr::null_mut(),
                0,
            );
            assert!(n > 0, "ICU sort key failed");
            let mut key = vec![0; n as usize];
            let actual = (self.icu.key)(
                handle.0,
                utf16.as_ptr(),
                length(utf16.len()),
                key.as_mut_ptr(),
                n,
            );
            assert_eq!(actual, n);
            key
        }
    }
    pub fn version(&self) -> String {
        let handle = self.handle.lock().unwrap();
        let mut version = [0; 4];
        // SAFETY: version output has the four bytes required by ICU.
        unsafe { (self.icu.version)(handle.0, version.as_mut_ptr()) };
        let mut n = 4;
        while n > 2 && version[n - 1] == 0 {
            n -= 1;
        }
        version[..n]
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(".")
    }
}
impl Drop for Collator {
    fn drop(&mut self) {
        // SAFETY: unique ownership during Drop, and ICU outlives its handle.
        unsafe { (self.icu.close)(self.handle.get_mut().unwrap_or_else(|p| p.into_inner()).0) };
    }
}
pub fn pg_compare(collator: &Collator, deterministic: bool, a: &[u8], b: &[u8]) -> Ordering {
    let result = collator.compare_utf8(a, b);
    if deterministic && result == Ordering::Equal {
        a.cmp(b)
    } else {
        result
    }
}
