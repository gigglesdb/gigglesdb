# Private harness orchestration; sourced by ../run.sh.
# Usage: run.sh private [--work-root DIR] [--nunya-exe PATH] [--linux-bin PATH]
# Env: NUNYA_EXE, NUNYA_BUILD_WRAPPER, NUNYA_PG_PORT, NUNYA_MGMT_PORT.
native_path() {
  if command -v cygpath >/dev/null; then cygpath -am "$1"; else printf '%s\n' "$1"; fi
}
private_cleanup() {
  local status=$? failed=0 kind remaining
  trap - EXIT
  "${compose[@]}" down -v >"$run/down.log" 2>&1 || failed=1
  for kind in container volume network; do
    case "$kind" in
      container) remaining=$(docker ps -aq --filter "name=$project") || failed=1 ;;
      volume) remaining=$(docker volume ls -q --filter "name=$project") || failed=1 ;;
      network) remaining=$(docker network ls -q --filter "name=$project") || failed=1 ;;
    esac
    if [[ -n $remaining ]]; then
      printf 'Cleanup failed: project %s still has %s resources\n' "$project" "$kind" >&2
      failed=1
    fi
  done
  (( failed == 0 )) || status=1
  exit "$status"
}
# Each scenario returns success only after all its assertions pass.
scenario_bootstrap() {
  local first second child_bash
  child_bash=$(native_path "$(command -v bash)")
  local child='set -euo pipefail
    [[ $PGUSER =~ ^session:[0-9a-f]{32}$ ]]
    [[ $PGHOST == 127.0.0.1 && $PGPORT == "$1" ]]
    [[ -n $PGPASSWORD ]]
    exec 3<>"/dev/tcp/$PGHOST/$PGPORT"
    exec 3>&-
    printf "%s\n" "$PGUSER"'
  first=$("$nunya_exe" exec "${helper_opts[@]}" --context d1 --ttl 1h -- \
    "$child_bash" -c "$child" bootstrap "$NUNYA_PG_PORT") || return 1
  second=$("$nunya_exe" exec "${helper_opts[@]}" --context d1 --ttl 1h -- \
    "$child_bash" -c "$child" bootstrap "$NUNYA_PG_PORT") || return 1
  [[ $first =~ ^session:[0-9a-f]{32}$ && $first == "$second" ]]
}
# Run the driver behind exec, keeping session credentials out of argv and logs.
scenario_psycopg_driver() {
  local mode=$1 context=$2 ttl=$3 line pid input output status=0 control=0 summary=0
  local counted_pass=0 counted_fail=0
  scenario_pass=0 scenario_fail=1
  coproc PSYCOPG {
    "$nunya_exe" exec "${helper_opts[@]}" --context "$context" --ttl "$ttl" -- \
      "${NUNYA_PYTHON:-python}" -I "$(native_path "$source_dir/drivers/psycopg_check.py")" --mode "$mode"
  }
  pid=$PSYCOPG_PID
  # Duplicates remain valid after bash unsets the coprocess variables on exit.
  exec {output}<&"${PSYCOPG[0]}" {input}>&"${PSYCOPG[1]}"
  while IFS= read -r -t 120 line <&"$output"; do
    line=${line%$'\r'}
    printf '%s\n' "$line"
    case "$line" in
      'PASS step '*) counted_pass=$((counted_pass + 1)) ;;
      'FAIL step '*) counted_fail=$((counted_fail + 1)) ;;
      'WAIT restart')
        if [[ $mode != main || $control != 0 ]]; then status=1; break; fi
        "${compose[@]}" restart nunya >"$run/restart.log" 2>&1 || { status=1; break; }
        "${compose[@]}" up -d --wait --wait-timeout 180 --no-recreate nunya \
          >>"$run/restart.log" 2>&1 || { status=1; break; }
        control=1
        printf 'GO\n' >&"$input" || { status=1; break; }
        ;;
      'WAIT revoke')
        if [[ $mode != main || $control != 1 ]]; then status=1; break; fi
        "$nunya_exe" session revoke "${helper_opts[@]:0:8}" --context "$context" \
          >"$run/revoke.log" 2>&1 || { status=1; break; }
        control=2
        printf 'GO\n' >&"$input" || { status=1; break; }
        ;;
      'WAIT expiry')
        if [[ $mode != expiry || $control != 0 ]]; then status=1; break; fi
        # TTL starts before the first query; 25s from here includes 5s margin.
        sleep 25
        control=1
        printf 'GO\n' >&"$input" || { status=1; break; }
        ;;
      *)
        if [[ $line =~ ^([0-9]+)\ passed\;\ ([0-9]+)\ failed$ ]]; then
          scenario_pass=${BASH_REMATCH[1]} scenario_fail=${BASH_REMATCH[2]}
          summary=$((summary + 1))
        else status=1; break; fi
        ;;
    esac
  done
  exec {input}>&- {output}<&-
  wait "$pid" || status=1
  [[ $summary == 1 && $scenario_pass == "$counted_pass" && $scenario_fail == "$counted_fail" ]] || status=1
  if [[ $mode == main ]]; then
    [[ $control == 2 && $((counted_pass + counted_fail)) == 12 ]] || status=1
  else
    [[ $control == 1 && $((counted_pass + counted_fail)) == 3 ]] || status=1
  fi
  (( status == 0 && scenario_fail == 0 ))
}
scenario_psycopg() { scenario_psycopg_driver main d1-psycopg 1h; }
scenario_psycopg_expiry() { scenario_psycopg_driver expiry d1-psycopg-exp 20s; }
run_scenario() {
  local name=$1 pass=0 fail=0
  local scenario_pass= scenario_fail=
  if "scenario_$name" >"$run/scenario-$name.log" 2>&1; then pass=1; else fail=1; fi
  if (( pass )); then printf 'SCENARIO %s: PASS\n' "$name"; else printf 'SCENARIO %s: FAIL\n' "$name"; fi
  printf 'SUMMARY %s %s %s\n' "$name" "${scenario_pass:-$pass}" "${scenario_fail:-$fail}"
  (( fail == 0 ))
}
private_main() {
  umask 077
  # Native Windows tools need drive paths when MSYS conversion is disabled.
  run=$(native_path "$run")
  nunya_exe=$(native_path "$nunya_exe")
  export MSYS_NO_PATHCONV=1
  export NUNYA_PG_PORT=${NUNYA_PG_PORT:-26432} NUNYA_MGMT_PORT=${NUNYA_MGMT_PORT:-26481}
  project="gd81-d1-$$-$RANDOM"
  compose=(docker compose -p "$project" -f "$run/compose.yaml" --profile private)
  trap private_cleanup EXIT
  mkdir -p "$run/config" "$run/bin" "$run/data/db" "$run/home"
  chmod 700 "$run/home"
  cp "$source_dir/compose.yaml" "$source_dir/seed.sql" "$run/"
  cp "$source_dir/private/nunya.toml" "$run/config/"
  cp "$source_dir/policy.toml" "$run/config/"
  "$nunya_exe" key generate >"$run/config/root.key"
  printf 'nunya-harness-only\n' >"$run/config/upstream.password"
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=gd81-d1-CA \
    -keyout "$run/ca.key" -out "$run/ca.pem" >"$run/tls.log" 2>&1
  openssl req -newkey rsa:2048 -nodes -subj /CN=nunya \
    -keyout "$run/config/management.key" -out "$run/management.csr" >>"$run/tls.log" 2>&1
  printf 'subjectAltName=DNS:nunya,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >"$run/management.ext"
  openssl x509 -req -days 1 -in "$run/management.csr" -CA "$run/ca.pem" \
    -CAkey "$run/ca.key" -CAcreateserial -extfile "$run/management.ext" \
    -out "$run/config/management.pem" >>"$run/tls.log" 2>&1
  if [[ -n $linux_bin ]]; then cp "$linux_bin" "$run/bin/nunya"
  else bash "$source_dir/build-linux.sh" "$run/bin"; fi
  "${compose[@]}" up -d --wait --wait-timeout 180 >"$run/up.log" 2>&1
  local admin=(--server unix:/var/lib/nunya/nunya.sock) credential
  "${compose[@]}" exec -T nunya /opt/nunya/nunya principal create d1-agent --kind agent \
    "${admin[@]}" >"$run/principal.log" 2>&1
  "${compose[@]}" exec -T nunya /opt/nunya/nunya principal grant d1-agent \
    --route postgres=read-write "${admin[@]}" >"$run/grant.log" 2>&1
  # Keep stdout in memory; persist only the validated secret line.
  credential=$("${compose[@]}" exec -T nunya /opt/nunya/nunya principal credential add \
    d1-agent --generate-secret "${admin[@]}" 2>"$run/credential.log")
  credential=${credential//$'\r'/}
  [[ $credential =~ ^[0-9a-f]{32}$'\n'([0-9a-f]{64})$ ]] || return 1
  printf '%s\n' "${BASH_REMATCH[1]}" >"$run/principal.secret"
  unset credential
  chmod 600 "$run/principal.secret"
  export NUNYA_HOME
  NUNYA_HOME=$(native_path "$run/home")
  helper_opts=(--server "https://127.0.0.1:$NUNYA_MGMT_PORT" --ca-file "$(native_path "$run/ca.pem")"
    --principal d1-agent --principal-secret-file "$(native_path "$run/principal.secret")"
    --pg-host 127.0.0.1 --pg-port "$NUNYA_PG_PORT")
  # Later items add one function and one name here per scenario.
  local failed=0 scenario
  for scenario in bootstrap psycopg psycopg_expiry; do run_scenario "$scenario" || failed=1; done
  return "$failed"
}
