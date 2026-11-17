#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: build-linux.sh <out-dir>' >&2; exit 1; }
mkdir -p "$1"
out=$(cd "$1" && pwd -P)
crate=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
export MSYS_NO_PATHCONV=1
native_path() { if command -v cygpath >/dev/null; then cygpath -am "$1"; else printf '%s\n' "$1"; fi; }
for volume in gd81-d1-cargo-target gd81-d1-cargo-registry; do
  docker volume create --label gd81-d1=1 "$volume" >/dev/null
done
image=rust:1.90.0-slim-bookworm
start=$SECONDS
build() {
  local wrapper=()
  # Split the optional wrapper command into arguments without pathname expansion.
  read -r -a wrapper <<< "${NUNYA_BUILD_WRAPPER:-}"
  "${wrapper[@]}" docker run --rm --name gd81-d1-build --label gd81-d1=1 --pull never \
    --mount "type=bind,source=$(native_path "$crate"),target=/src,readonly" \
    --mount "type=bind,source=$(native_path "$out"),target=/out" \
    --mount type=volume,source=gd81-d1-cargo-target,target=/target \
    --mount type=volume,source=gd81-d1-cargo-registry,target=/usr/local/cargo/registry \
    -e CARGO_TARGET_DIR=/target -e CARGO_BUILD_JOBS=4 -e CARGO_INCREMENTAL=0 \
    -e TEMP=/target/tmp -e TMP=/target/tmp -e TMPDIR=/target/tmp \
    -w /src "$image" bash -euc '
      mkdir -p /target/tmp
      apt-get update && apt-get install -y --no-install-recommends clang libclang-dev
      cargo build --release --locked --bin nunya
      cp /target/release/nunya /out/nunya
      chmod 755 /out/nunya
    '
}
if ! build >"$out/build-linux.log" 2>&1; then
  if grep -Eq 'requires rustc|rustc .* is not supported' "$out/build-linux.log"; then
    image=rust:1.99.1-slim-bookworm
    docker pull "$image" >>"$out/build-linux.log" 2>&1
    build >>"$out/build-linux.log" 2>&1
  else
    echo "Linux build failed; see $out/build-linux.log" >&2
    exit 1
  fi
fi
printf 'Linux build: %ss, image %s\n' "$((SECONDS-start))" "$image" | tee "$out/build-linux-summary.txt"
