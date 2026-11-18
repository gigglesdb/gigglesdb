#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) cache=${GIGGLES_GLIBM_CACHE:-J:/gigglesdb-work/glibm}; library=giggles_glibm.dll ;;
  Darwin*) cache=${GIGGLES_GLIBM_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/gigglesdb-glibm}; library=libgiggles_glibm.dylib ;;
  *) cache=${GIGGLES_GLIBM_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/gigglesdb-glibm}; library=libgiggles_glibm.so ;;
esac
# Resolve relative target paths before changing directory.
target=${CARGO_TARGET_DIR:-$root/crates/giggles-glibm/target}
case "$target" in /*|[A-Za-z]:/*) ;; *) target="$PWD/$target" ;; esac
export CARGO_TARGET_DIR="$target"
cd "$root/crates/giggles-glibm"
if [[ $(uname -s) = MINGW* || $(uname -s) = MSYS* ]]; then
  bash J:/gigglesdb-work/r2/buildslot2.sh cargo build --locked --release >&2
else
  cargo build --locked --release >&2
fi
mkdir -p "$cache"
cp "$target/release/$library" "$cache/$library"
printf '%s\n' "$cache"
