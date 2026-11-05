//! Version 1 trusted local C ABI; see include/giggles.h and README.md.
use giggles_core::{local, *};
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, OnceLock},
};
const INVALID: u32 = 18;
const PANIC: u32 = 19;
const BUFFER_SMALL: u32 = 20;
const MAX_HANDLES: usize = 1024;
const MAX_BUFFERS: usize = 128 * 1024 * 1024;
enum Handle {
    Admin(Admin),
    Executor(Executor),
    SqlSession(Box<SqlSession>),
    Buffer(Vec<u8>),
}
struct Registry {
    next: u64,
    handles: BTreeMap<u64, Handle>,
    bytes: usize,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            next: 1,
            handles: BTreeMap::new(),
            bytes: 0,
        }
    }
}
impl Registry {
    fn capacity(&self, bytes: usize) -> bool {
        self.next < u64::MAX
            && self.handles.len() < MAX_HANDLES
            && bytes <= MAX_BUFFERS - self.bytes
    }
    fn add(&mut self, h: Handle) -> Result<u64, u32> {
        let n = if let Handle::Buffer(b) = &h {
            b.len()
        } else {
            0
        };
        if !self.capacity(n) {
            return Err(16);
        }
        let id = self.next;
        self.next = self.next.checked_add(1).ok_or(16u32)?;
        self.bytes += n;
        self.handles.insert(id, h);
        Ok(id)
    }
}
fn registry() -> &'static Mutex<Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(Registry::default()))
}
fn boundary(f: impl FnOnce() -> u32) -> u32 {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(PANIC)
}
/// # Safety
/// Non-null input points to `len` readable bytes for the duration of this call.
unsafe fn input<'a>(p: *const u8, len: usize) -> Result<&'a [u8], u32> {
    if len > local::MAX_INPUT {
        return Err(16);
    }
    if len == 0 {
        return Ok(&[]);
    }
    if p.is_null() {
        return Err(INVALID);
    }
    // SAFETY: C caller guarantees readable input; length was bounded above.
    Ok(unsafe { std::slice::from_raw_parts(p, len) })
}
#[unsafe(no_mangle)]
pub extern "C" fn gd_v1_abi_version() -> u32 {
    1
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Open {
    mode: String,
    path: String,
    export: Option<String>,
}
/// # Safety
/// Input must be readable for len bytes; out must be aligned and writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gd_v1_admin_open(p: *const u8, len: usize, out: *mut u64) -> u32 {
    boundary(|| {
        if out.is_null() {
            return INVALID;
        }
        // SAFETY: required output contract.
        unsafe {
            *out = 0;
        }
        let b = match unsafe { input(p, len) } {
            Ok(b) => b,
            Err(c) => return c,
        };
        let o: Open = match serde_json::from_slice(b) {
            Ok(o) => o,
            Err(_) => return 17,
        };
        let mut r = registry().lock().unwrap_or_else(|p| p.into_inner());
        // Reserve capacity before any filesystem mutation.
        if !r.capacity(0) {
            return 16;
        }
        let a = match o.mode.as_str() {
            "create" if o.export.is_none() => Admin::create(o.path, Limits::default()),
            "open" if o.export.is_none() => Admin::open(o.path, Limits::default()),
            "restore" => match o.export {
                Some(e) => Admin::restore(e, o.path, Limits::default()),
                None => return 17,
            },
            _ => return 17,
        };
        match a {
            Ok(a) => match r.add(Handle::Admin(a)) {
                Ok(id) => {
                    unsafe {
                        *out = id;
                    }
                    0
                }
                Err(c) => c,
            },
            Err(e) => local::failure(e).code,
        }
    })
}
/// # Safety
/// out must be aligned and writable. Admin is trusted to bind the principal.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gd_v1_executor(
    admin: u64,
    task: u64,
    principal: u64,
    out: *mut u64,
) -> u32 {
    boundary(|| {
        if out.is_null() {
            return INVALID;
        }
        unsafe {
            *out = 0;
        }
        let mut r = registry().lock().unwrap_or_else(|p| p.into_inner());
        let a = match r.handles.get(&admin) {
            Some(Handle::Admin(a)) => a.clone(),
            _ => return INVALID,
        };
        match a.executor(TaskId(task), Principal(principal)) {
            Ok(e) => match r.add(Handle::Executor(e)) {
                Ok(id) => {
                    unsafe {
                        *out = id;
                    }
                    0
                }
                Err(c) => c,
            },
            Err(e) => local::failure(e).code,
        }
    })
}
/// # Safety
/// Input must be readable for len bytes; out must be aligned/writable and must
/// not overlap input. Returned buffer handle must be closed even on denial.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gd_v1_call(handle: u64, p: *const u8, len: usize, out: *mut u64) -> u32 {
    boundary(|| {
        if out.is_null() {
            return INVALID;
        }
        unsafe {
            *out = 0;
        }
        let b = match unsafe { input(p, len) } {
            Ok(b) => b,
            Err(c) => return c,
        };
        let mut r = registry().lock().unwrap_or_else(|p| p.into_inner());
        // Reserve a response slot and its maximum bytes before a durable action.
        if !r.capacity(local::MAX_OUTPUT) {
            return 16;
        }
        let reply = match r.handles.get_mut(&handle) {
            Some(Handle::Admin(a)) => local::admin(a, b),
            Some(Handle::Executor(e)) => local::executor(e, b),
            Some(Handle::SqlSession(s)) => local::sql_session(s, b),
            _ => return INVALID,
        };
        let code = reply.code;
        let bytes = match serde_json::to_vec(&reply) {
            Ok(b) => b,
            Err(_) => return 17,
        };
        if bytes.len() > local::MAX_OUTPUT {
            return 16;
        }
        match r.add(Handle::Buffer(bytes)) {
            Ok(id) => {
                unsafe {
                    *out = id;
                }
                code
            }
            Err(c) => c,
        }
    })
}
/// Creates a trusted SQL session. Executor handles cannot create sessions.
/// # Safety
/// out must point to aligned writable u64 storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gd_v1_sql_session(admin: u64, out: *mut u64) -> u32 {
    boundary(|| {
        if out.is_null() {
            return INVALID;
        }
        unsafe {
            *out = 0;
        }
        let mut r = registry().lock().unwrap_or_else(|p| p.into_inner());
        if !r.capacity(0) {
            return 16;
        }
        let session = match r.handles.get(&admin) {
            Some(Handle::Admin(a)) => a.sql_session(),
            _ => return INVALID,
        };
        match r.add(Handle::SqlSession(Box::new(session))) {
            Ok(id) => {
                unsafe {
                    *out = id;
                }
                0
            }
            Err(c) => c,
        }
    })
}
/// # Safety
/// needed is writable/aligned. If capacity > 0, dst points to capacity writable
/// bytes, not overlapping needed. Query with NULL,0 returns required byte count.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gd_v1_buffer_copy(
    handle: u64,
    dst: *mut u8,
    capacity: usize,
    needed: *mut usize,
) -> u32 {
    boundary(|| {
        if needed.is_null() {
            return INVALID;
        }
        unsafe {
            *needed = 0;
        }
        if capacity > local::MAX_OUTPUT {
            return 16;
        }
        let r = registry().lock().unwrap_or_else(|p| p.into_inner());
        let b = match r.handles.get(&handle) {
            Some(Handle::Buffer(b)) => b,
            _ => return INVALID,
        };
        unsafe {
            *needed = b.len();
        }
        if capacity < b.len() {
            return BUFFER_SMALL;
        }
        if dst.is_null() {
            return INVALID;
        }
        // SAFETY: caller supplies writable capacity; Rust allocation is never exposed.
        unsafe {
            std::ptr::copy_nonoverlapping(b.as_ptr(), dst, b.len());
        }
        0
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn gd_v1_close(handle: u64) -> u32 {
    boundary(|| {
        let mut r = registry().lock().unwrap_or_else(|p| p.into_inner());
        match r.handles.remove(&handle) {
            Some(Handle::Buffer(b)) => {
                r.bytes -= b.len();
                0
            }
            Some(_) => 0,
            None => INVALID,
        }
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn panic_is_contained() {
        assert_eq!(boundary(|| panic!("test unwind")), 19);
    }
    #[test]
    fn registry_poison_after_unwind_still_allows_resource_release() {
        let id = registry()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .add(Handle::Buffer(vec![1, 2, 3]))
            .unwrap();
        assert_eq!(
            boundary(|| {
                let _guard = registry().lock().unwrap_or_else(|p| p.into_inner());
                panic!("inside registry");
            }),
            19
        );
        assert_eq!(gd_v1_close(id), 0);
        assert_eq!(gd_v1_close(id), 18);
    }
    #[test]
    fn handle_id_exhaustion_and_byte_capacity_are_preflighted_without_reuse() {
        let mut r = Registry {
            next: u64::MAX - 1,
            ..Registry::default()
        };
        assert!(r.capacity(0));
        assert_eq!(r.add(Handle::Buffer(vec![1])), Ok(u64::MAX - 1));
        assert!(!r.capacity(0));
        assert_eq!(r.add(Handle::Buffer(vec![])), Err(16));
        assert_eq!(r.handles.len(), 1);
        let r = Registry {
            bytes: MAX_BUFFERS,
            ..Registry::default()
        };
        assert!(!r.capacity(1));
        assert!(r.capacity(0));
    }
    #[test]
    fn invalid_and_wrong_kind() {
        unsafe {
            let mut out = 99;
            assert_eq!(gd_v1_admin_open(std::ptr::null(), 1, &mut out), 18);
            assert_eq!(out, 0);
            assert_eq!(gd_v1_admin_open(std::ptr::null(), usize::MAX, &mut out), 16);
            assert_eq!(gd_v1_executor(0, 1, 2, &mut out), 18);
            assert_eq!(gd_v1_call(0, std::ptr::null(), 0, &mut out), 18);
            assert_eq!(gd_v1_close(0), 18);
            assert_eq!(gd_v1_buffer_copy(0, std::ptr::null_mut(), 0, &mut 0), 18);
            assert_eq!(
                gd_v1_admin_open(std::ptr::null(), 0, std::ptr::null_mut()),
                18
            );
        }
    }
}
