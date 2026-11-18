#!/usr/bin/env bash
# Build in Debian, execute ONLY in the pinned PostgreSQL oracle image.
set -euo pipefail
root=$(cd "$(dirname "$0")" && pwd)
task=${1:?usage: run-capture.sh J:/task-dir [count]}
count=${2:-1000000}
mkdir -p "$task/glibm-sweep" "$task/tmp-1b-1"
builder=pgp-1b-capture-build
oracle=pgp-1b-capture-oracle
trap 'docker rm -f "$builder" "$oracle" >/dev/null 2>&1 || true' EXIT
MSYS_NO_PATHCONV=1 docker run -d --name "$builder" -v "$task:/task" debian:trixie sleep infinity
docker cp "$root/capture.c" "$builder:/tmp/capture.c"
MSYS_NO_PATHCONV=1 docker exec "$builder" sh -c 'apt-get update -qq && apt-get install -y -qq gcc > /task/tmp-1b-1/capture-apt.log 2>&1 && gcc -O2 -fno-builtin -Wall -Wextra /tmp/capture.c -ldl -o /task/tmp-1b-1/capture'
MSYS_NO_PATHCONV=1 docker run -d --name "$oracle" -v "$task:/task" --entrypoint sleep postgres:18.6 infinity
MSYS_NO_PATHCONV=1 docker exec "$oracle" sh -c 'dpkg-query -W libc6; grep -m1 flags /proc/cpuinfo' > "$task/tmp-1b-1/oracle-environment.txt"
grep -q '2.41-12+deb13u4' "$task/tmp-1b-1/oracle-environment.txt"
MSYS_NO_PATHCONV=1 docker exec "$oracle" /task/tmp-1b-1/capture /task/glibm-sweep "$count" 2> "$task/tmp-1b-1/capture.log"
python "$root/fixtures.py" "$task/glibm-sweep" "$root/../tests/fixtures"
python "$root/verify-oracle.py" "$task/fixtures/float/oracle.txt" "$oracle" > "$task/tmp-1b-1/oracle-verification.txt"
