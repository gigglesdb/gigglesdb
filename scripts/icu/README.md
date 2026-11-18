# icu

Runtime provisioning scripts for contributors and distributors who need the pinned ICU collation libraries.

**Status:** Requires exactly ICU 76.1.0.0; supports Windows binaries and Linux x86_64 source builds.

Part of [GigglesDB](../../README.md).

Run `bash scripts/icu/provision.sh`; the loader finds its cache automatically.
Set `GIGGLES_ICU_DIR` to its output to require that installation exclusively.
Cargo only builds the Rust wrapper; ICU is loaded at runtime. Windows uses the
official Win64 MSVC2022 binaries (the MSVC runtime must be installed). Linux
x86_64 builds the pinned official sources with shared libraries and full data;
install a C/C++ toolchain, make, curl, CA certificates and tar first. ICU build
tools remain enabled because disabling tools also disables the full data build.

The SHA-256-pinned downloads install below `$GIGGLES_ICU_CACHE`, defaulting to
the script's fixed Windows cache directory (override with `GIGGLES_ICU_CACHE`) or
`${XDG_CACHE_HOME:-$HOME/.cache}/gigglesdb-icu` on Linux. Its `76.1/windows`
or `76.1/linux` subdirectory contains the installation. Library and
licence hashes are checked on repeat calls. Provisioning writes only inside
that cache; build/download scratch is removed on exit.

Exactly ICU 76.1.0.0 is required: its collation data, tailoring and version
strings reproduce the PostgreSQL 18.6 oracle. Other versions can change order
and invalidate persisted index keys. The loader rejects them. It tries the
explicit `GIGGLES_ICU_DIR` exclusively when set (failure is an error). Otherwise
it tries `icu` beside the executable, then the provision cache described above,
then Linux system 76 SONAMEs. Empty cache variables use their defaults, as in
the provisioning script. Explicit `load_from` selects only its directory.

The Linux gate runner provisions ICU before any tests, exports the returned
directory for every test run, and records the provisioning result and elapsed
seconds in its summary. A provisioning failure stops the gate before tests.

The Rust crate is MIT. ICU and its Unicode data use the ICU/Unicode licence;
the upstream Unicode licence text (`LICENSE`, installed alongside libraries)
must ship with redistributed binaries. Retain the full data library.
