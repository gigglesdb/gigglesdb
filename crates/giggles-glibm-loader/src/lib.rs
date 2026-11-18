//! MIT runtime boundary for the independently distributed LGPL math library.
use libloading::Library;
use std::{
    ffi::CStr,
    path::{Path, PathBuf},
    sync::OnceLock,
};
pub const IDENTITY: &str = "giggles-glibm/1 glibc-2.41-12+deb13u4 x86_64-fma";
#[cfg(windows)]
pub const LIBRARY: &str = "giggles_glibm.dll";
#[cfg(target_os = "linux")]
pub const LIBRARY: &str = "libgiggles_glibm.so";
#[cfg(target_os = "macos")]
pub const LIBRARY: &str = "libgiggles_glibm.dylib";
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlibmError(pub String);
impl std::fmt::Display for GlibmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for GlibmError {}
type Unary = unsafe extern "C" fn(f64) -> f64;
type Binary = unsafe extern "C" fn(f64, f64) -> f64;
pub struct Glibm {
    cbrt: Unary,
    exp: Unary,
    log: Unary,
    log10: Unary,
    sin: Unary,
    cos: Unary,
    tan: Unary,
    asin: Unary,
    acos: Unary,
    atan: Unary,
    sinh: Unary,
    cosh: Unary,
    tanh: Unary,
    asinh: Unary,
    acosh: Unary,
    atanh: Unary,
    erf: Unary,
    erfc: Unary,
    tgamma: Unary,
    lgamma: Unary,
    pow: Binary,
    atan2: Binary,
    // Keep the library alive for all copied function pointers.
    _library: Library,
}
static DEFAULT: OnceLock<Result<Glibm, GlibmError>> = OnceLock::new();
pub fn provision_cache() -> Option<PathBuf> {
    let nonempty = |name| std::env::var_os(name).filter(|v| !v.is_empty());
    if let Some(dir) = nonempty("GIGGLES_GLIBM_CACHE") {
        return Some(dir.into());
    }
    if cfg!(windows) {
        return Some("J:/gigglesdb-work/glibm".into());
    }
    nonempty("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| nonempty("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .map(|p| p.join("gigglesdb-glibm"))
}
fn check_identity(actual: &[u8]) -> Result<(), GlibmError> {
    if actual == IDENTITY.as_bytes() {
        Ok(())
    } else {
        Err(GlibmError(format!(
            "identity mismatch: expected {IDENTITY:?}, got {:?}",
            String::from_utf8_lossy(actual)
        )))
    }
}
impl Glibm {
    pub fn get() -> Result<&'static Self, GlibmError> {
        DEFAULT
            .get_or_init(Self::load)
            .as_ref()
            .map_err(Clone::clone)
    }
    fn load() -> Result<Self, GlibmError> {
        let mut dirs = Vec::new();
        if let Some(dir) = std::env::var_os("GIGGLES_GLIBM_DIR").filter(|v| !v.is_empty()) {
            dirs.push(PathBuf::from(dir));
        }
        let mut errors = Vec::new();
        match std::env::current_exe() {
            Ok(exe) => {
                if let Some(dir) = exe.parent() {
                    dirs.push(dir.to_path_buf());
                    if let Some(parent) = dir.parent() {
                        dirs.push(parent.to_path_buf());
                    }
                }
            }
            Err(e) => errors.push(format!("executable directory: {e}")),
        }
        if let Some(cache) = provision_cache() {
            dirs.push(cache);
        }
        for dir in dirs {
            match Self::load_from(dir) {
                Ok(m) => return Ok(m),
                Err(e) => errors.push(e.0),
            }
        }
        Err(GlibmError(format!(
            "giggles-glibm unavailable: {}",
            errors.join("; ")
        )))
    }
    pub fn load_from(dir: impl AsRef<Path>) -> Result<Self, GlibmError> {
        let path = dir.as_ref().join(LIBRARY);
        // SAFETY: the configured artifact is trusted native code. Validate its
        // versioned ABI before resolving functions; retain it for their lifetime.
        unsafe {
            let result = (|| {
                let lib = Library::new(&path).map_err(|e| GlibmError(e.to_string()))?;
                let identity = lib
                    .get::<unsafe extern "C" fn() -> *const std::ffi::c_char>(
                        b"giggles_glibm_identity\0",
                    )
                    .map_err(|e| GlibmError(e.to_string()))?;
                let ptr = identity();
                if ptr.is_null() {
                    return Err(GlibmError("null identity".into()));
                }
                check_identity(CStr::from_ptr(ptr).to_bytes())?;
                Ok(Self {
                    cbrt: *lib
                        .get(b"giggles_glibm_cbrt\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    exp: *lib
                        .get(b"giggles_glibm_exp\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    log: *lib
                        .get(b"giggles_glibm_log\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    log10: *lib
                        .get(b"giggles_glibm_log10\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    sin: *lib
                        .get(b"giggles_glibm_sin\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    cos: *lib
                        .get(b"giggles_glibm_cos\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    tan: *lib
                        .get(b"giggles_glibm_tan\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    asin: *lib
                        .get(b"giggles_glibm_asin\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    acos: *lib
                        .get(b"giggles_glibm_acos\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    atan: *lib
                        .get(b"giggles_glibm_atan\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    sinh: *lib
                        .get(b"giggles_glibm_sinh\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    cosh: *lib
                        .get(b"giggles_glibm_cosh\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    tanh: *lib
                        .get(b"giggles_glibm_tanh\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    asinh: *lib
                        .get(b"giggles_glibm_asinh\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    acosh: *lib
                        .get(b"giggles_glibm_acosh\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    atanh: *lib
                        .get(b"giggles_glibm_atanh\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    erf: *lib
                        .get(b"giggles_glibm_erf\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    erfc: *lib
                        .get(b"giggles_glibm_erfc\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    tgamma: *lib
                        .get(b"giggles_glibm_tgamma\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    lgamma: *lib
                        .get(b"giggles_glibm_lgamma\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    pow: *lib
                        .get(b"giggles_glibm_pow\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    atan2: *lib
                        .get(b"giggles_glibm_atan2\0")
                        .map_err(|e| GlibmError(e.to_string()))?,
                    _library: lib,
                })
            })();
            result.map_err(|e: GlibmError| GlibmError(format!("{}: {e}", path.display())))
        }
    }
    pub fn cbrt(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.cbrt)(x) }
    }
    pub fn exp(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.exp)(x) }
    }
    pub fn log(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.log)(x) }
    }
    pub fn log10(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.log10)(x) }
    }
    pub fn sin(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.sin)(x) }
    }
    pub fn cos(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.cos)(x) }
    }
    pub fn tan(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.tan)(x) }
    }
    pub fn asin(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.asin)(x) }
    }
    pub fn acos(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.acos)(x) }
    }
    pub fn atan(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.atan)(x) }
    }
    pub fn sinh(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.sinh)(x) }
    }
    pub fn cosh(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.cosh)(x) }
    }
    pub fn tanh(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.tanh)(x) }
    }
    pub fn asinh(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.asinh)(x) }
    }
    pub fn acosh(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.acosh)(x) }
    }
    pub fn atanh(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.atanh)(x) }
    }
    pub fn erf(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.erf)(x) }
    }
    pub fn erfc(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.erfc)(x) }
    }
    pub fn tgamma(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.tgamma)(x) }
    }
    pub fn lgamma(&self, x: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.lgamma)(x) }
    }
    pub fn pow(&self, x: f64, y: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.pow)(x, y) }
    }
    pub fn atan2(&self, x: f64, y: f64) -> f64 {
        // SAFETY: validated ABI and retained library.
        unsafe { (self.atan2)(x, y) }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configured_library() {
        if let Some(dir) = std::env::var_os("GIGGLES_GLIBM_DIR") {
            let m = Glibm::load_from(dir).unwrap();
            assert_eq!(m.cbrt(27.0).to_bits(), 0x4008000000000001);
            assert_eq!(m.sin(0.0), 0.0);
            assert_eq!(m.pow(2.0, 3.0), 8.0);
            assert!(std::ptr::eq(Glibm::get().unwrap(), Glibm::get().unwrap()));
        }
    }
    #[test]
    fn missing_library() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("no-such-library-directory");
        let err = Glibm::load_from(&dir).err().unwrap().to_string();
        assert!(err.contains(dir.to_str().unwrap()) && err.contains(LIBRARY));
    }
    #[test]
    fn wrong_identity() {
        assert!(
            check_identity(b"wrong-version")
                .unwrap_err()
                .0
                .contains("identity mismatch")
        );
        assert!(check_identity(IDENTITY.as_bytes()).is_ok());
    }
    #[test]
    fn no_static_lgpl_dependency() {
        let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        for entry in std::fs::read_dir(crates).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() == "giggles-glibm" {
                continue;
            }
            let manifest = entry.path().join("Cargo.toml");
            if let Ok(text) = std::fs::read_to_string(&manifest) {
                assert!(
                    !text.lines().any(|l| l
                        .split_once('=')
                        .is_some_and(|(key, value)| key.trim() == "giggles-glibm"
                            || value.trim().starts_with("\"giggles-glibm\""))),
                    "{}",
                    manifest.display()
                );
            }
        }
    }
}
