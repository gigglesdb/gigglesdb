#!/usr/bin/env bash
# Fetch exactly the pinned driver jars, validating existing files too.
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: fetch-jdbc.sh DIR' >&2; exit 1; }
dir=$1
manifest=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/jdbc-jars.txt
mkdir -p "$dir"
while read -r name url sha extra; do
  [[ -z $name || $name == \#* ]] && continue
  [[ -z ${extra:-} && $sha =~ ^[0-9a-f]{64}$ && $name != */* ]] || exit 1
  destination=$dir/$name
  if [[ ! -f $destination ]]; then
    partial=$(mktemp "$dir/.jdbc-download.XXXXXX")
    trap 'rm -f "$partial"' EXIT
    curl --fail --silent --show-error --location "$url" -o "$partial"
    actual=$(sha256sum "$partial"); actual=${actual%% *}
    [[ $actual == "$sha" ]] || { echo 'JDBC SHA-256 mismatch' >&2; exit 1; }
    mv "$partial" "$destination"
    trap - EXIT
  fi
  actual=$(sha256sum "$destination"); actual=${actual%% *}
  [[ $actual == "$sha" ]] || { echo 'JDBC SHA-256 mismatch' >&2; exit 1; }
done < "$manifest"
