#!/usr/bin/env bash
# Source on Linux before running Rust tests. Preserve the test storage contract.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
jroot="$RUNNER_TEMP/jroot"
mkdir -p "$jroot/gigglesdb-work/tmp/skedaddle"
export TEMP=j:/gigglesdb-work/tmp TMP=j:/gigglesdb-work/tmp
export SKEDADDLE_TEST_ROOT=J:/gigglesdb-work/tmp/skedaddle
export TMPDIR="$jroot/gigglesdb-work/tmp"
for crate in "$root"/crates/*; do
  [[ -f $crate/Cargo.toml ]] || continue
  ln -sfn "$jroot" "$crate/j:"
  ln -sfn "$jroot" "$crate/J:"
done
export GIGGLES_ICU_CACHE="$RUNNER_TEMP/icu"
export GIGGLES_GLIBM_CACHE="$RUNNER_TEMP/glibm"
GIGGLES_ICU_DIR=$(bash "$root/scripts/icu/provision.sh")
export GIGGLES_ICU_DIR
GIGGLES_GLIBM_DIR=$(bash "$root/scripts/glibm/build.sh")
export GIGGLES_GLIBM_DIR
