#!/usr/bin/env bash
set -euo pipefail
tag=$GITHUB_REF_NAME
number='(0|[1-9][0-9]*)'
identifier='(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)'
if [[ ! $tag =~ ^(giggles|nunya|skedaddle)-v($number\.$number\.$number)(-($identifier)(\.$identifier)*)?$ ]]; then
  echo "Invalid release tag: $tag; expected <product>-vMAJOR.MINOR.PATCH[-prerelease]" >&2
  exit 1
fi
product=${tag%%-v*}
version=${tag#*-v}
# Docker tags have a 128-character limit.
[[ ${#version} -le 128 ]] || { echo 'Version exceeds Docker tag length' >&2; exit 1; }
git fetch origin main
commit=$(git rev-parse "refs/tags/$tag^{commit}")
git merge-base --is-ancestor "$commit" origin/main || {
  echo 'Release commit must be an ancestor of origin/main' >&2; exit 1;
}
image="ghcr.io/${OWNER,,}/$product"
source="$image:sha-${commit:0:7}"
if ! descriptor=$(docker buildx imagetools inspect "$source" --format '{{json .Manifest}}'); then
  echo 'CI on main has not published an image for this commit' >&2; exit 1
fi
digest=$(jq -er '.digest' <<< "$descriptor")
# Pull by digest; inspection reads the image config, not the index annotations.
docker pull "$image@$digest"
revision=$(docker image inspect "$image@$digest" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
[[ $revision == "$commit" ]] || { echo "Revision mismatch: expected $commit, got $revision" >&2; exit 1; }
tags=("$version")
prerelease=()
if [[ $version == *-* ]]; then
  prerelease=(--prerelease)
else
  tags+=("${version%.*}" latest)
fi
args=()
for target in "${tags[@]}"; do args+=(--tag "$image:$target"); done
# No rebuild and no wrapper index: retain the original digest.
docker buildx imagetools create --prefer-index=false "${args[@]}" "$image@$digest"
for target in "${tags[@]}"; do
  actual=$(docker buildx imagetools inspect "$image:$target" --format '{{json .Manifest}}' | jq -er '.digest')
  [[ $actual == "$digest" ]] || { echo "Digest mismatch for $image:$target" >&2; exit 1; }
done
notes="$RUNNER_TEMP/release-notes.md"
printf 'Image: `%s:%s`\n\nDigest: `%s`\n\nImmutable reference: `%s@%s`\n'   "$image" "$version" "$digest" "$image" "$digest" > "$notes"
gh release create "$tag" --verify-tag --generate-notes "${prerelease[@]}" --notes-file "$notes"
cat "$notes" >> "$GITHUB_STEP_SUMMARY"
