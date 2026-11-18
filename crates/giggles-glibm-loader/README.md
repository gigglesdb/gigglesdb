# giggles-glibm-loader

MIT runtime loader for the separately distributed LGPL giggles-glibm cdylib.
There is no Cargo dependency on that library. `Glibm::get()` caches its first
success or failure for the process lifetime; safe methods retain the library.

Search order: GIGGLES_GLIBM_DIR, executable directory, its parent, build cache.
The cache is GIGGLES_GLIBM_CACHE, defaulting to J:/gigglesdb-work/glibm on Windows
or ${XDG_CACHE_HOME:-$HOME/.cache}/gigglesdb-glibm elsewhere.
Run `bash scripts/glibm/build.sh` to provision it (with cargo environment set).
The loader rejects any identity except
`giggles-glibm/1 glibc-2.41-12+deb13u4 x86_64-fma`.
All failed candidates and their error reasons appear in the final error.
Configured native libraries must be trusted, as with any runtime-loaded code.
