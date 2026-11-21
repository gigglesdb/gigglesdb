# Shared slot code for scripts/gate-remote.sh and scripts/bench-remote.sh.
# Sourced, not run. The caller defines boatdir, max_boxes, who, type, short,
# g() (git in the repo),
# ref, boxname, box, started, slot, log() and bj() (the boat CLI), plus
# adopt_box() (looks up the box named $boxname in `bj list --all --json`).
#
# A slot is the directory $boatdir/slots/slot-<n>, taken with an atomic mkdir.
# Besides the human-readable `owner` file, the scripts keep a `lease` file of
# key=value lines in it: the owner's pid, the pid's start time (from
# /proc/<pid>/stat, so a reused pid is not mistaken for the owner), its
# Windows pid and image, the host, and the run's ref, box name and box id.
# The lease is replaced atomically (write, then rename), so every read sees
# one owner's whole lease.
#
# A gate killed outright (SIGKILL, taskkill /F, a harness killing the process
# group) never runs its EXIT trap, so its box, slot, usage-log END line and
# ref are left behind. slot_reclaim_stale, run before each attempt to take a
# slot, finds slots whose lease owner is provably dead and finishes them:
# deletes the box named in the lease (looking it up by its unique name when
# the owner died inside `boat new`), writes the missing usage-log lines
# marked reclaimed, removes the run's ref and releases the slot.
#
# Safety rules (a wrong reclaim deletes a live gate's box):
# - A slot without a lease is never touched (a slot being taken right now,
#   cluster-remote.sh slots, or slots from older script versions).
# - The owner counts as alive unless the evidence says it is dead: a lease
#   from another host, an unreadable pid or start time, a kill -0 failure
#   other than "No such process", or a tasklist error all count as alive.
# - Only the box id or box name in the dead owner's own lease is ever
#   deleted; both are unique to that run.
# - Reclaims are serialized by the $boatdir/slots/.reclaim.lock directory,
#   and the lease is re-read and re-checked under it.
# - A box that may still exist (delete failed and the listing still shows
#   it, or the listing failed) keeps its slot held, so the box cap stays
#   honest; the log says to delete it by hand.

slot_lease_field() { # <lease text> <key>
  printf '%s\n' "$1" | sed -n "/^$2=/{s/^$2=//p;q;}"
}
slot_pid_start() { # <pid>: start time of a live pid, empty if unknown
  local stat
  stat=$(cat "/proc/$1/stat" 2>/dev/null) || return 0
  # Field 22 (starttime); skip "pid (comm)" first, comm may hold spaces.
  printf '%s\n' "${stat##*) }" | awk '{print $20}'
}
slot_clean() { printf '%s' "$1" | tr -d '\r\n'; }
slot_lease_write() { # <slot dir>: (re)write this process's lease
  local d=$1 winpid='' image=''
  winpid=$(cat "/proc/$$/winpid" 2>/dev/null) || winpid=''
  image=$(cat "/proc/$$/winexename" 2>/dev/null) || image=''
  image=${image##*[\\/]}
  {
    echo "kind=${slot_kind:-gate}"
    echo "pid=$$"
    echo "pid_start=$(slot_pid_start $$)"
    echo "winpid=$winpid"
    echo "winimage=$(slot_clean "$image")"
    echo "host=$(slot_clean "${HOSTNAME:-$(hostname 2>/dev/null)}")"
    echo "who=$(slot_clean "$who")"
    echo "type=$(slot_clean "$type")"
    echo "short=$(slot_clean "$short")"
    echo "ref=$(slot_clean "$ref")"
    echo "boxname=$(slot_clean "$boxname")"
    echo "box=$(slot_clean "$box")"
    echo "started=$started"
  } > "$d/lease.tmp.$$" && mv -f "$d/lease.tmp.$$" "$d/lease"
}
slot_winpid_alive() { # <winpid> <image>: 0 = may be alive, 1 = gone
  local out row image
  [[ "$1" =~ ^[0-9]+$ ]] || return 0
  command -v tasklist.exe >/dev/null 2>&1 || return 0
  out=$(MSYS_NO_PATHCONV=1 tasklist.exe /FI "PID eq $1" /FO CSV /NH 2>/dev/null) || return 0
  row=$(printf '%s\n' "$out" | tr -d '\r' | grep -E "^\"[^\"]*\",\"$1\"," | head -n 1) || true
  if [ -z "$row" ]; then
    # tasklist answers a filter that matches nothing with one non-CSV line.
    printf '%s\n' "$out" | tr -d '\r' | grep -q '^"' && return 0
    [ -n "$(printf '%s' "$out" | tr -d '[:space:]')" ] || return 0
    return 1
  fi
  [ -n "$2" ] || return 0
  image=${row#\"}; image=${image%%\"*}
  # The same Windows pid now running another program is not the owner.
  [ "${image,,}" = "${2,,}" ] && return 0
  return 1
}
slot_owner_alive() { # <lease text>: 0 = alive or unknown, 1 = provably dead
  local lease=$1 pid start host err cur winpid
  pid=$(slot_lease_field "$lease" pid)
  start=$(slot_lease_field "$lease" pid_start)
  host=$(slot_lease_field "$lease" host)
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  [ "$host" = "$(slot_clean "${HOSTNAME:-$(hostname 2>/dev/null)}")" ] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    cur=$(slot_pid_start "$pid")
    # Same pid, same start time (or no way to tell): the owner.
    [ -n "$start" ] && [ -n "$cur" ] && [ "$cur" != "$start" ] || return 0
  else
    err=$(kill -0 "$pid" 2>&1) || true
    case "$err" in *'No such process'*) ;; *) return 0 ;; esac
  fi
  # Also ask Windows: a lease written under another MSYS runtime has a pid
  # this runtime cannot see.
  winpid=$(slot_lease_field "$lease" winpid)
  if [ -n "$winpid" ]; then
    slot_winpid_alive "$winpid" "$(slot_lease_field "$lease" winimage)" && return 0
  fi
  return 1
}
slot_age() { # <path>: seconds since its mtime (0 if unreadable)
  local m; m=$(stat -c %Y "$1" 2>/dev/null) || m=$(date +%s)
  echo $(( $(date +%s) - m ))
}
slot_bj() { # boat call with a bound, so a hung CLI cannot hold the lock
  timeout --kill-after=10 120 "$B" --no-update "$@"
}
slot_lock_take() { # serialize reclaims; 1 = another reclaim is running
  local lock=$boatdir/slots/.reclaim.lock lease moved age
  if mkdir "$lock" 2>/dev/null; then slot_lease_write "$lock"; return 0; fi
  # A reclaimer killed mid-reclaim: take over only when it is dead (or died
  # before writing its lease) and the lock is older than any reclaim takes
  # (two boat calls of at most 130s each).
  lease=$(cat "$lock/lease" 2>/dev/null) || lease=''
  age=$(slot_age "$lock")
  [ "$age" -gt 900 ] || return 1
  if [ -n "$lease" ]; then slot_owner_alive "$lease" && return 1; fi
  # Rename, then check the renamed lock is the one judged dead: a second
  # taker that raced us may have renamed it first and made a fresh lock.
  moved=$lock.stale.$$
  mv "$lock" "$moved" 2>/dev/null || return 1
  if [ "$(cat "$moved/lease" 2>/dev/null || true)" != "$lease" ]; then
    mv -T "$moved" "$lock" 2>/dev/null ||   # not ours: put it back
      log "WARNING: could not put back reclaim lock $moved (harmless: slots are re-checked before release)"
    return 1
  fi
  log "removed stale reclaim lock (pid $(slot_lease_field "$lease" pid), ${age}s old)"
  rm -rf "$moved"
  if mkdir "$lock" 2>/dev/null; then slot_lease_write "$lock"; return 0; fi
  return 1
}
slot_lock_drop() { rm -rf "$boatdir/slots/.reclaim.lock"; }
slot_reclaim_one() { # <slot dir> <lease text>; called under the lock
  local d=$1 lease=$2 name=${1##*/} kind dpid dwho dtype dshort dref dbox dname dstarted
  local listing tag mins found note
  kind=$(slot_lease_field "$lease" kind); dpid=$(slot_lease_field "$lease" pid)
  dwho=$(slot_lease_field "$lease" who); dtype=$(slot_lease_field "$lease" type)
  dshort=$(slot_lease_field "$lease" short); dref=$(slot_lease_field "$lease" ref)
  dbox=$(slot_lease_field "$lease" box); dname=$(slot_lease_field "$lease" boxname)
  dstarted=$(slot_lease_field "$lease" started)
  tag=''; [ "$kind" = bench ] && tag=' bench'
  log "reclaiming $name: owner pid $dpid ($dwho, tip $dshort) is dead"
  if [ -z "$dbox" ] && [ -n "$dname" ]; then
    # The owner died during or right after `boat new`: find the box by its
    # unique name, with the scripts' own listing parser.
    slot_listing=$(slot_bj list --all --json 2>/dev/null) || {
      log "WARNING: could not list boxes to look for $dname; $name stays held"; return 1; }
    # A global name: adopt_box's own `local listing` would shadow a local one.
    dbox=$(bj() { printf '%s\n' "$slot_listing"; }; boxname=$dname; adopt_box)
    # A box still being created may not be listed yet: hold the slot until
    # the lease is older than any `boat new` can take, then look once more.
    if [ -z "$dbox" ] && [ "$(slot_age "$d/lease")" -lt 600 ]; then
      log "no box named $dname listed yet; $name stays held until its lease is 10 min old"; return 1
    fi
  fi
  if [ -n "$dbox" ]; then
    [[ "$dbox" =~ ^bx_[a-z0-9]+$ ]] || { log "WARNING: bad box id '$dbox' in $name lease; $name stays held"; return 1; }
    if slot_bj delete "$dbox" --yes >/dev/null 2>&1; then
      note="deleted box $dbox"
    else
      # Gone already (TTL, or deleted by hand)? Only a listing can say.
      listing=$(slot_bj list --all --json 2>/dev/null) || {
        log "WARNING: delete of $dbox failed and boxes could not be listed; $name stays held, delete it by hand"; return 1; }
      if printf '%s' "$listing" | grep -q "\"$dbox\""; then
        log "WARNING: delete of $dbox failed and it still exists; $name stays held, delete it by hand"; return 1
      fi
      note="box $dbox already gone"
    fi
    found=$(grep -c "| $dbox |.*| START" "$boatdir/usage.log" 2>/dev/null || true)
    if [ "${found:-0}" = 0 ]; then
      echo "$(date -u +%FT%TZ) | $dwho | $dbox | $dtype | $dshort | START$tag (reclaimed: owner died before logging it)" >> "$boatdir/usage.log"
    fi
    mins=0
    [[ "$dstarted" =~ ^[0-9]+$ ]] && mins=$(( ($(date +%s) - dstarted + 59) / 60 ))
    # The owner may have died inside its own cleanup, after its END line.
    found=$(grep -c "| $dbox |.*| END" "$boatdir/usage.log" 2>/dev/null || true)
    if [ "${found:-0}" = 0 ]; then
      echo "$(date -u +%FT%TZ) | $dwho | $dbox | $dtype | $dshort | END$tag rc=reclaimed ${mins}min (owner pid $dpid died; reclaimed by $(slot_clean "$who") pid $$)" >> "$boatdir/usage.log"
    fi
  else
    note="no box${dname:+ named $dname found}"
  fi
  if [[ "$dref" =~ ^refs/(gate|bench)-remote/[0-9]{8}T[0-9]{6}Z-[0-9]+$ ]]; then
    g update-ref -d "$dref" 2>/dev/null && note="$note, removed $dref"
  fi
  # Release only the slot that was reclaimed, never one a new run took since.
  if [ "$(cat "$d/lease" 2>/dev/null || true)" != "$lease" ]; then
    log "WARNING: $name changed hands during the reclaim; leaving it"; return 1
  fi
  rm -rf "$d"
  note="released $name: $note"
  log "reclaimed $name (owner pid $dpid, $dwho, tip $dshort): $note"
  echo "$(date -u +%FT%TZ) | reclaimed by $(slot_clean "$who") pid $$ | owner pid $dpid $dwho $dshort | $note" >> "$boatdir/reclaim.log"
  return 0
}
slot_reclaim_stale() { # finish every slot whose lease owner is dead
  local d lease again
  for d in "$boatdir"/slots/slot-*; do
    [ -d "$d" ] || continue
    if [ ! -f "$d/lease" ]; then
      # Never reclaimed (cluster-remote.sh, older scripts, a slot being
      # taken), but an old one is worth a look by hand.
      [ -f "$d/owner" ] && [ "$(slot_age "$d/owner")" -gt 10800 ] &&
        log "WARNING: ${d##*/} has no lease and is over 3 h old; check it by hand: $(head -n 1 "$d/owner" 2>/dev/null)"
      continue
    fi
    lease=$(cat "$d/lease" 2>/dev/null) || continue
    slot_owner_alive "$lease" && continue
    slot_lock_take || { log "another reclaim is running; skipping ${d##*/} this round"; return 0; }
    again=$(cat "$d/lease" 2>/dev/null) || again=''
    if [ -n "$again" ] && [ "$again" = "$lease" ] && ! slot_owner_alive "$again"; then
      slot_reclaim_one "$d" "$again" || true
    fi
    slot_lock_drop
  done
  return 0
}
slot_acquire() { # <wait seconds>: sets slot; returns 1 if none freed in time
  local deadline n s
  deadline=$(( $(date +%s) + $1 ))
  while :; do
    slot_reclaim_stale
    for n in $(seq 1 "$max_boxes"); do s=slot-$n
      if mkdir "$boatdir/slots/$s" 2>/dev/null; then
        slot=$s
        slot_lease_write "$boatdir/slots/$s"
        return 0
      fi
    done
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep "${slot_poll:-30}"
  done
}
slot_release() { # remove this process's slot, never one another owner holds
  local d lease pid
  [ -n "$slot" ] || return 0
  d=$boatdir/slots/$slot
  [ -d "$d" ] || return 0
  if [ -f "$d/lease" ]; then
    lease=$(cat "$d/lease" 2>/dev/null) || lease=''
    pid=$(slot_lease_field "$lease" pid)
    if [ "$pid" != "$$" ]; then
      log "WARNING: $slot lease belongs to pid ${pid:-?}, not this run; leaving it"
      return 0
    fi
  fi
  rm -rf "$d"
}
