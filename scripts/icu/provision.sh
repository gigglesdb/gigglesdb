#!/usr/bin/env bash
# ICU is provisioned separately from cargo. Only the final directory goes to stdout.
set -euo pipefail
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) platform=windows; cache=${GIGGLES_ICU_CACHE:-J:/gigglesdb-work/icu} ;;
  Linux*) [[ $(uname -m) == x86_64 ]] || { echo 'ICU: unsupported architecture' >&2; exit 1; }
    platform=linux; cache=${GIGGLES_ICU_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/gigglesdb-icu} ;;
  *) echo 'ICU: unsupported platform' >&2; exit 1 ;;
esac
mkdir -p "$cache"
cache=$(cd "$cache" && pwd)
install="$cache/76.1/$platform"
if [[ -f "$install/SHA256SUMS" ]] && (cd "$install" && sha256sum --check --status SHA256SUMS); then
  printf '%s\n' "$install"; exit 0
fi
work=$(mktemp -d "$cache/provision.XXXXXXXX")
trap 'rm -rf "$work"' EXIT
base=https://github.com/unicode-org/icu/releases/download/release-76-1
if [[ $platform == windows ]]; then
  artifact=icu4c-76_1-Win64-MSVC2022.zip
  expected=bedba77dd1feca09e9ae9922109a285c0ecf46d09c80b65eae6eae63a4e155dc
else
  artifact=icu4c-76_1-src.tgz
  expected=dfacb46bfe4747410472ce3e1144bf28a102feeaa4e3875bac9b4c6cf30f4f3e
fi
curl --fail --location --retry 3 "$base/$artifact" -o "$work/$artifact" >&2
printf '%s  %s\n' "$expected" "$work/$artifact" | sha256sum --check >&2
mkdir -p "$install"
if [[ $platform == windows ]]; then
  unzip -q "$work/$artifact" -d "$work/unpack"
  for dll in icudt76.dll icuuc76.dll icuin76.dll; do
    file=$(find "$work/unpack" -path "*/bin64/$dll" -print -quit)
    [[ -n $file ]] || { echo "Missing $dll" >&2; exit 1; }
    cp "$file" "$install/"
  done
  license=$(find "$work/unpack" -iname LICENSE -o -iname license.txt | head -n1)
  [[ -n $license ]] || { echo 'Missing ICU license' >&2; exit 1; }
  cp "$license" "$install/LICENSE"
  (cd "$install" && sha256sum icudt76.dll icuuc76.dll icuin76.dll LICENSE > SHA256SUMS)
else
  tar -xzf "$work/$artifact" -C "$work"
  mkdir "$work/build"
  (cd "$work/build"
    "$work/icu/source/runConfigureICU" Linux --prefix="$install" --enable-shared --disable-static --with-data-packaging=library --disable-tests --disable-samples --disable-extras --enable-tools >&2
    # ICU disables the data build with --disable-tools; pkgdata is required.
    make -j4 >&2
    make install >&2)
  cp "$work/icu/LICENSE" "$install/LICENSE"
  # Keep loader dependencies beside the libraries, including their SONAME links.
  cp -a "$install/lib/"libicu*.so* "$install/"
  (cd "$install" && sha256sum libicu*.so* LICENSE > SHA256SUMS)
fi
printf '%s\n' "$install"
