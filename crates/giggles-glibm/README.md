# giggles-glibm

Portable scalar binary64 routines derived from Debian glibc
**2.41-12+deb13u4**, matching the x86_64 libm used by `postgres:18.6`.
This is an **LGPL-2.1-or-later** library, with zero Cargo dependencies.
Keep it a separately loaded dynamic library: no GigglesDB crate may depend on
its rlib. The rlib exists for qualification tests and standalone consumers.
It is not wired into GigglesDB.

## Status and integration

This lane implements cbrt, exp, log, log10, pow, sinh, cosh, tanh, asinh,
acosh, atanh, erf, erfc, tgamma and lgamma (including lgamma_neg).
The seven IBM exports intentionally return NaN in `src/ibm_pending.rs`.
**Negative gamma paths depend on sin/cos and cannot pass standalone
qualification until the parallel IBM module is merged.** Tests do not hide
these failures. To integrate, replace `mod ibm_pending` and its reexports
with `mod ibm` and its reexports, and set `IBM_PENDING` to false. This also
enables checking all seven IBM fixture/sweep files.

All 15 implemented routines passed 2,000 fixtures and 1,000,000 sweep records
each in both debug and release diagnostics with the parallel lane's IBM
module substituted temporarily. That module is not included in this commit.
The standalone crate qualifies 13 routines; negative tgamma/lgamma records
remain expected failures until integration. Do not deploy this stub build.

## Provenance and operation

Source: <https://deb.debian.org/debian/pool/main/g/glibc/>:

* `glibc_2.41.orig.tar.xz`, SHA-256
  `f24aa441021121a79266f0d75242706cab8843a47901fefe74527491807f1998`.
* `glibc_2.41-12+deb13u4.debian.tar.xz`, SHA-256
  `dda4153511bfd543502d18e5bc9323110996fe9c531f14ae3fad6eb17f027c7d`.

Debian patches, including `git-updates.diff`, were applied in series order.
`COPYING.LIB` is copied verbatim from that glibc tree. Source file names and
copyright notices are recorded in the Rust modules. Tables preserve the
source binary64 bits. `extended.rs` is a small integer implementation of
the x87 significand operations needed by the selected gamma_product helper.

The reproduced variant is a mix: FMA implementations for exp/log/pow,
log1p/expm1, baseline SSE2 for the other routines, and the **ldbl-96 x87
gamma_product** override. The latter rounds intermediate products to a
64-bit significand; using the dbl-64 helper instead is not bit-exact.
Runtime resolved internal addresses were exp `0x7cfc0`, log `0x7d660`,
pow `0x7dcf0`; gamma_product's x87 instructions begin at `0x57b00`.
The oracle CPU advertises FMA and AVX2. Explicit `mul_add` follows the
image's disassembly, including the lack of FMA in exp2.

Only binary64 arithmetic, sqrt, mul_add, comparisons and integer/bit
operations are used. No platform transcendental functions or libm are
called. Assumes IEEE binary64, round-to-nearest ties-to-even, gradual
underflow; errno and floating-point exception flags are not reproduced.
NaN payload/sign handling is tested in release as well as debug, since
optimizer folding of operations on known NaNs can discard their payloads.

## Build and test in the assigned lane

From Git Bash, with a lane lock held:

```sh
source J:/gigglesdb-work/pgparity/p1/env.sh J:/gigglesdb-work/pgparity/p1/target-w
export TEMP=J:/gigglesdb-work/pgparity/p1/tmp-1b-1
export TMP="$TEMP"
cd J:/gigglesdb-work/wt/pgp-1w/crates/giggles-glibm
bash J:/gigglesdb-work/r2/buildslot2.sh cargo build --release
export GIGGLES_GLIBM_SWEEP_DIR=J:/gigglesdb-work/pgparity/p1/glibm-sweep
bash J:/gigglesdb-work/r2/buildslot2.sh cargo test --test all -- --nocapture
bash J:/gigglesdb-work/r2/buildslot2.sh cargo clippy --all-targets -- -D warnings
```

The Windows artifact is `target-w/release/giggles_glibm.dll`. On Linux
the cdylib is `libgiggles_glibm.so`. `tools/giggles_glibm.h` describes all
23 exports. Identity is the static NUL-terminated string
`giggles-glibm/1 glibc-2.41-12+deb13u4 x86_64-fma`.

## Oracle capture

```sh
bash tools/run-capture.sh J:/gigglesdb-work/pgparity/p1 1000000
```

The script builds `capture.c` with GCC and `-fno-builtin` in a disposable
Debian container, then runs it against the PostgreSQL image's own libm via
dlsym. It removes both containers on exit. It refuses a different libc6
revision. There are 22 `.bin` files, little-endian `(x,out)` or
`(a,b,out)` u64 records; pow uses `(x,y)` and atan2 `(y,x)`.
Sampling covers raw random bits/log-uniform magnitudes, useful bounded
domains, signed zeros, subnormals, infinities, signed/payload NaNs,
branch boundaries, gamma poles and their adjacent representable values.
Binary functions additionally cover the Cartesian product of special
values. `fixtures.py` selects at most 2,000 records per function.
`verify-oracle.py` maps PostgreSQL ln/log/power/gamma to
libm log/log10/pow/tgamma. It compares all plain-libm rows by round-trip
bits, allowing only the NaN and zero normalization inherent in PG text.
The captured generator matched all 140,395 applicable PostgreSQL rows.
