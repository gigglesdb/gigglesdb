#ifndef GIGGLES_H
#define GIGGLES_H
#include <stdint.h>
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Version 1, cdecl. Opaque process-local handles, zero invalid, never reused.
   Admin/Executor/Buffer are distinct registry types. No Rust pointers escape.
   Close every handle once, including response handles returned on policy denial.
   Inputs: bounded UTF-8 JSON (<=1MiB), not NUL terminated. Output JSON bytes are
   copied into caller memory (<=64MiB), not NUL terminated. No borrowed buffers.
   At most 1024 handles and 128MiB retained buffers. Concurrent calls serialize.
   Readable/writable aligned C memory and non-overlap are caller obligations.
   A malicious caller in this process is outside the threat model.
   Code 0 success, 1 absent status, 10 storage (outcome may be unknown; reopen and
   status ORIGINAL operation), 11 corrupt, 12 unsupported, 13 exists, 14 busy,
   15 fenced, 16 limit, 17 invalid JSON, 18 invalid handle/pointer,
   19 contained unwind (drop all handles, recover status), 20 buffer too small.
   100..117 policy codes documented in README. Panics unwind-contained; abort,
   invalid memory and allocation failure are outside containment.
   Admin open JSON: {"mode":"create"|"open"|"restore","path":"...", "export":"..."}
   export field only for restore. Call JSON matches core local protocol.
   Trusted host owns Admin, principal rebind, filesystem and Certified time.
*/
typedef uint64_t gd_handle;
enum gd_v1_code {
 GD_OK=0, GD_ABSENT=1, GD_STORAGE=10, GD_CORRUPT=11, GD_UNSUPPORTED=12,
 GD_EXISTS=13, GD_BUSY=14, GD_FENCED=15, GD_LIMIT=16, GD_INVALID_JSON=17,
 GD_INVALID_HANDLE=18, GD_PANIC=19, GD_BUFFER_SMALL=20,
 GD_AUTHENTICATION=100, GD_INVALID_GRANT=101, GD_TASK_EXISTS=102,
 GD_UNKNOWN_TASK=103, GD_REVOKED=104, GD_EXPIRED=105, GD_UNCERTAIN_TIME=106,
 GD_TIME_REGRESSION=107, GD_SCOPE=108, GD_MISSING_ROW=109, GD_DUPLICATE_KEY=110,
 GD_INVALID_PREDICATE=111, GD_BUDGET=112, GD_APPROVAL_REQUIRED=113,
 GD_INVALID_APPROVAL=114, GD_STALE_PREVIEW=115, GD_ID_CONFLICT=116, GD_OVERFLOW=117
};
uint32_t gd_v1_abi_version(void);
uint32_t gd_v1_admin_open(const uint8_t*,size_t,gd_handle*);
uint32_t gd_v1_executor(gd_handle,uint64_t,uint64_t,gd_handle*);
uint32_t gd_v1_call(gd_handle,const uint8_t*,size_t,gd_handle*);
uint32_t gd_v1_buffer_copy(gd_handle,uint8_t*,size_t,size_t*);
uint32_t gd_v1_close(gd_handle);
#ifdef __cplusplus
}
#endif
#endif
