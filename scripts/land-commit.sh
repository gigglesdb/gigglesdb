#!/usr/bin/env bash
# Land one commit on refs/heads/main of the shared checkout, following the
# AGENTS.md "Committing in the shared checkout" rules:
#   private GIT_INDEX_FILE seeded from the expected main, LF blobs only,
#   numstat review, update-ref compare-and-swap, verify, reset the shared
#   index for the committed paths, then sync K: copies that are not dirty.
#
# Usage:
#   scripts/land-commit.sh --base <expected-old-main> --from <LF worktree> \
#       --message-file <file> [--repo <shared checkout>] [--dry-run] \
#       [--allow-stale-base] <path>...
#
# <path> is repo-relative. Content comes from <LF worktree>/<path> (create the
# worktree with core.autocrlf=false). A path missing from the worktree is
# deleted. To land a gated stack of commits as a batch, call this once per
# commit, passing the previous call's printed commit as the next --base.
# Use --allow-stale-base to bypass the lane path check.
# It never pushes and never runs the gate: run the full gate first.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=${GIGGLES_REPO:-$(dirname "$(git -C "$script_dir" rev-parse --path-format=absolute --git-common-dir)")}
base='' from='' msgfile='' dry=0 allow_stale_base=0
paths=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base) base=$2; shift 2 ;;
    --from) from=$2; shift 2 ;;
    --message-file) msgfile=$2; shift 2 ;;
    --repo) repo=$2; shift 2 ;;
    --dry-run) dry=1; shift ;;
    --allow-stale-base) allow_stale_base=1; shift ;;
    --) shift; paths+=("$@"); break ;;
    -*) echo "unknown option $1" >&2; exit 2 ;;
    *) paths+=("$1"); shift ;;
  esac
done
[ -n "$base" ] && [ -n "$from" ] && [ -n "$msgfile" ] && [ ${#paths[@]} -gt 0 ] || {
  sed -n '2,19p' "$0" >&2; exit 2; }
grep -q '^Co-Authored-By: ' "$msgfile" || { echo "message lacks Co-Authored-By trailer" >&2; exit 2; }

g() { git -C "$repo" "$@"; }
base=$(g rev-parse --verify "$base^{commit}")
cur=$(g rev-parse refs/heads/main)
[ "$cur" = "$base" ] || { echo "main is $cur, expected $base: rebase and re-gate" >&2; exit 1; }

path_entry() {
  g ls-tree --full-tree --format='%(objectmode) %(objectname)' "$1" -- ":(literal)$2"
}

if [ "$allow_stale_base" -eq 0 ]; then
  lane_head=$(git -C "$from" rev-parse HEAD)
  lane_base=$(g merge-base "$base" "$lane_head") || {
    echo "cannot find a common base for lane $lane_head and main $base" >&2; exit 1; }
  if [ "$lane_base" != "$base" ]; then
    lane_history=$(g rev-list --first-parent "$lane_base..$lane_head")
  fi
fi

work=$(mktemp -d "${TEMP:-/tmp}/land-commit.XXXXXX")
trap 'rm -rf "$work"' EXIT
export GIT_INDEX_FILE="$work/index"
g read-tree "$base"
for p in "${paths[@]}"; do
  if [ -e "$from/$p" ]; then
    if [ "$(tr -cd '\r' < "$from/$p" | wc -c)" -ne 0 ] && grep -qI . "$from/$p"; then
      echo "CR bytes in text file $from/$p: committed blobs must be LF" >&2; exit 1
    fi
    h=$(g hash-object -w --no-filters "$from/$p")
    g update-index --add --cacheinfo "100644,$h,$p"
    landing_entry="100644 $h"
  else
    g update-index --force-remove -- "$p"
    landing_entry=''
  fi

  if [ "$allow_stale_base" -eq 0 ] && [ "$lane_base" != "$base" ]; then
    # Keep main's entry if it changed since the fork, unless the lane's
    # committed history already includes that entry. Earlier stack landings
    # create new commit IDs, so compare path entries along the first-parent
    # history and require the proposed entry to match the lane's HEAD.
    base_entry=$(path_entry "$base" "$p")
    fork_entry=$(path_entry "$lane_base" "$p")
    if [ "$base_entry" = "$fork_entry" ] || [ "$base_entry" = "$landing_entry" ]; then
      continue
    fi
    head_entry=$(path_entry "$lane_head" "$p")
    if [ "$landing_entry" = "$head_entry" ]; then
      for lane_commit in $lane_history; do
        history_entry=$(path_entry "$lane_commit" "$p")
        if [ "$base_entry" = "$history_entry" ]; then
          continue 2
        fi
      done
    fi
    echo "main changed $p since your lane's base $lane_base; rebase the lane onto $base" >&2
    exit 1
  fi
done
echo "--- numstat vs $base (a small edit shown as a whole-file rewrite means CRLF):"
g diff --cached --numstat "$base"
tree=$(g write-tree)
[ "$tree" != "$(g rev-parse "$base^{tree}")" ] || { echo "no changes" >&2; exit 1; }
new=$(g commit-tree "$tree" -p "$base" -F "$msgfile")
unset GIT_INDEX_FILE
if [ $dry -eq 1 ]; then echo "dry run: would land $new"; exit 0; fi

g update-ref refs/heads/main "$new" "$base"
[ "$(g rev-parse refs/heads/main)" = "$new" ] || { echo "update-ref did not land $new" >&2; exit 1; }

g reset -q -- "${paths[@]}"
skipped=()
for p in "${paths[@]}"; do
  if [ ! -e "$repo/$p" ] \
     || [ "$(g hash-object -- "$p")" = "$(g rev-parse -q --verify "$base:$p" || echo none)" ]; then
    g -c core.autocrlf=false checkout "$new" -- "$p" 2>/dev/null || rm -f "$repo/$p"
  else
    skipped+=("$p")
  fi
done
[ ${#skipped[@]} -eq 0 ] || printf 'K: copy has foreign edits, left alone: %s\n' "${skipped[@]}" >&2
echo "landed $new"
