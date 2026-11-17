#!/usr/bin/env bash
# Box-side setup for the gigglesdb-bench-base snapshot (GD-0082 W2). Run on a
# boat.dev box started from gigglesdb-gate-base, with /home/user/gigglesdb
# already checked out at the commit to warm. Never run locally. Redo it with:
#   boat new --from gigglesdb-gate-base --type large --no-env --ttl 7200
#   (upload this script, check out main), bash bench-snapshot-setup.sh
#   boat snapshot <id> gigglesdb-bench-base; boat delete <id>
# Installs PostgreSQL 18 (PGDG apt; no default cluster, service disabled),
# Node 22 (official tarball, SHA-256 verified), npm ci for scripts/app-loadtest,
# and builds giggles-server --release so the release target dir is warm.
# Writes /home/user/bench.env and /home/user/bench-base.txt (versions).
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
source "$HOME/gate.env"
repo=/home/user/gigglesdb

# PostgreSQL 18 from the PGDG apt repository.
sudo apt-get update -q
sudo apt-get install -yq curl ca-certificates gnupg lsb-release xz-utils util-linux jq
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc
codename=$(lsb_release -cs)
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $codename-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
sudo install -d /etc/postgresql-common
echo "create_main_cluster = false" | sudo tee /etc/postgresql-common/createcluster.conf >/dev/null
sudo apt-get update -q
sudo apt-get install -yq postgresql-18 postgresql-client-18
sudo systemctl disable --now postgresql >/dev/null 2>&1 || true
pgbin=/usr/lib/postgresql/18/bin
"$pgbin/postgres" --version

# Node 22, official linux-x64 tarball checked against SHASUMS256.txt.
nd=$(mktemp -d)
curl -fsSL -o "$nd/SHASUMS256.txt" https://nodejs.org/dist/latest-v22.x/SHASUMS256.txt
tarball=$(grep -oE 'node-v22\.[0-9]+\.[0-9]+-linux-x64\.tar\.xz' "$nd/SHASUMS256.txt" | head -1)
curl -fsSL -o "$nd/$tarball" "https://nodejs.org/dist/latest-v22.x/$tarball"
( cd "$nd" && grep " $tarball\$" SHASUMS256.txt | sha256sum -c - )
sudo rm -rf /usr/local/lib/nodejs && sudo mkdir -p /usr/local/lib/nodejs
sudo tar -xJf "$nd/$tarball" -C /usr/local/lib/nodejs --strip-components=1
for b in node npm npx; do sudo ln -sfn "/usr/local/lib/nodejs/bin/$b" "/usr/local/bin/$b"; done
rm -rf "$nd"
node --version; npm --version

# app-loadtest dependencies; the stamp lets the runner skip npm ci when the
# lock file is unchanged.
( cd "$repo/scripts/app-loadtest" && npm ci --no-audit --no-fund )
sha256sum "$repo/scripts/app-loadtest/package-lock.json" | cut -d' ' -f1 > /home/user/bench-npm.stamp

# Release build of giggles-server; the snapshot keeps target/release warm.
cat > "$HOME/bench.env" <<'ENV'
source "$HOME/gate.env"
export CARGO_BUILD_JOBS=$(nproc)
export PATH=/usr/lib/postgresql/18/bin:/usr/local/bin:$PATH
ENV
source "$HOME/bench.env"
if command -v sccache >/dev/null 2>&1; then export RUSTC_WRAPPER=$(command -v sccache); fi
s=$(date +%s)
( cd "$repo/crates/giggles-server" && cargo build --release --bin giggles-server )
build_s=$(( $(date +%s) - s ))
"$CARGO_TARGET_DIR/release/giggles-server" 2>&1 | head -1 || true

{
  echo "commit $(git -C "$repo" rev-parse HEAD)"
  echo "os $(lsb_release -ds)"
  echo "kernel $(uname -r)"
  echo "cpu $(lscpu | sed -n 's/^Model name:[[:space:]]*//p') nproc=$(nproc)"
  echo "postgres $("$pgbin/postgres" --version)"
  echo "node $(node --version) npm $(npm --version)"
  echo "rustc $(rustc --version)"
  echo "sccache $(command -v sccache || echo none)"
  echo "release_build_seconds $build_s"
  echo "created $(date -u +%FT%TZ)"
} > /home/user/bench-base.txt
cat /home/user/bench-base.txt
df -h /home/user | tail -1
echo BENCH_SETUP_OK
