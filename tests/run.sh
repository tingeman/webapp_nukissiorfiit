#!/bin/bash
# Tests of the webapp_nuki image. Run on the workstation with the docker CLI against a built
# image, from the repository root:
#
#   docker build -t webapp_nuki:local .
#   bash tests/run.sh webapp_nuki:local
#
# The release build (dtu_sus_data02 scripts/build_image.sh) runs the same command in the
# checkout of the release ref and writes no bundle unless it exits 0. Works under bash on
# Linux and WSL, and from Git Bash on Windows (Docker Desktop). Three layers:
#
# * unit tests (tests/unit/test_*.py, names "unit: <module>: <description>"): app/db_connector.py
#   (request building, date normalisation, error mapping 404/400/504/500, JSON -> DataFrame,
#   the async wrappers) with the standard library's unittest, against the stub monitoring_db
#   API (tests/fake_api.py, fixtures in tests/fixtures/) on 127.0.0.1. All of them run in ONE
#   container of the image (--network none, as the image's user) through
#   tests/unit/run_unit.py, which prints one PASS/FAIL line per unit test.
# * image checks: the image's user is plotly (uid 30000), its command is gunicorn without
#   --reload, it has a HEALTHCHECK, pip check passes, the app's packages import, the dev
#   tools (IPython, ipdb) are not installed, and the Dockerfile's ARG PYTHON_IMAGE default is
#   NUKI_PYTHON_IMAGE of compose.pins.env. One shared container.
# * smoke tests (marked slow): the image started with its own command, a dummy env file and
#   an unreachable INFLUXDB_URL on a private --internal network, with the stub API (run with
#   the image's python) as the backend in the production URL form
#   (http://<host>/mdb/api/v1). It must become healthy (its own HEALTHCHECK), answer 200 on
#   /app/nuki/, and the main Dash callback must return the figures built from the stub data
#   (tests/smoke_check.py, run in the container). One app container for all smoke tests.
#
# Nothing leaves the workstation: no real API, InfluxDB or secrets are used. All containers
# have names starting with a unique prefix and are removed at the end, together with the
# network and the temporary directory. Nothing else is touched. Each test prints
# "PASS: <name>" or "FAIL: <name>: <reason>". The last line is "ALL <n> TESTS PASSED"
# (exit 0) or "<m> OF <n> TESTS FAILED" (exit 1).
#
# Selecting tests, for quicker runs while working on a change:
#
#   TEST_FILTER='^unit:' bash tests/run.sh webapp_nuki:local    # only the unit tests (~5 s)
#   TEST_FILTER='^image' bash tests/run.sh webapp_nuki:local    # names matching (grep -E)
#   bash tests/run.sh --quick webapp_nuki:local                 # skip the slow (smoke) tests
#
# TEST_FILTER is an extended regular expression matched against the test names (as printed
# after "PASS: "); only matching tests run. --quick (before or after the image) skips the
# tests marked slow. A setup runs only if a selected test needs it. Skipped tests are counted
# in a "SKIPPED: <k> test(s)" line; a selection without any test is an error (exit 2). The
# full suite (no TEST_FILTER, no --quick) is what the release build runs.
#
# Testing the checkout's code without rebuilding: APP_SRC=<dir> (e.g. APP_SRC=app) copies
# that directory over /app in the unit and smoke containers before they start, so a code
# change is tested in seconds. Rebuild when app/requirements.txt or the Dockerfile changes.
#
#   APP_SRC=app TEST_FILTER='^unit:' bash tests/run.sh webapp_nuki:local
# shellcheck disable=SC2329  # test functions are called indirectly through run_test
set -euo pipefail

usage() { echo "Usage: [TEST_FILTER=<regex>] [APP_SRC=<dir>] $0 [--quick] <image>" >&2; exit 2; }
IMAGE="" QUICK=0
for a in "$@"; do
  case "$a" in
    --quick) QUICK=1 ;;
    -*) usage ;;
    *) [ -z "$IMAGE" ] || usage; IMAGE="$a" ;;
  esac
done
[ -n "$IMAGE" ] || usage
TEST_FILTER="${TEST_FILTER:-}"
# grep exits 2 on an invalid expression
rc=0; printf '' | grep -Eq -- "$TEST_FILTER" || rc=$?
[ "$rc" -le 1 ] || { echo "invalid TEST_FILTER: $TEST_FILTER" >&2; exit 2; }
command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "image not found: $IMAGE" >&2; exit 2; }

TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$TESTS_DIR/.." && pwd)"
for f in fake_api.py smoke_check.py unit/run_unit.py fixtures/metadata.json fixtures/measurements.json; do
  [ -f "$TESTS_DIR/$f" ] || { echo "missing $TESTS_DIR/$f" >&2; exit 2; }
done
APP_SRC="${APP_SRC:-}"
if [ -n "$APP_SRC" ]; then
  [ -f "$APP_SRC/app.py" ] || { echo "APP_SRC=$APP_SRC has no app.py" >&2; exit 2; }
  APP_SRC="$(cd "$APP_SRC" && pwd)"
fi

# Git Bash: keep container paths (/app/...) as they are, and give docker Windows paths for
# the host side of bind mounts (see hp below).
export MSYS_NO_PATHCONV=1

PFX="nuki-test-$$"
NET="$PFX-net"
T="$(mktemp -d "${TMPDIR:-/tmp}/nuki-test.XXXXXX")"
chmod 0755 "$T"

# Host path for a bind mount: unchanged on Linux/WSL, C:/... form under Git Bash
hp() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s\n' "$1"; fi; }

cleanup() {
  local rc=$?
  trap - EXIT
  local ids
  ids="$(docker ps -aq --filter "name=${PFX}-" 2>/dev/null || true)"
  # shellcheck disable=SC2086  # word splitting of the id list is intended
  [ -z "$ids" ] || docker rm -f $ids >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  [ ! -d "$T" ] || rm -rf "$T" 2>/dev/null || echo "WARNING: could not remove $T" >&2
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# The dummy env file (config.py reads /app/secrets/.env.$APP_ENV at import). No real values:
# InfluxDB is unreachable (the InfluxDB code is due for removal and gets no stub), the
# backend is the stub API in the production URL form.
ENV_FILE="$T/env.production"
printf '%s\n' 'DEBUG=False' 'INFLUXDB_URL=http://influxdb.invalid:8086' 'INFLUXDB_TOKEN=dummy-token' \
  'INFLUXDB_ORG=dummy-org' 'BACKEND_API_BASE_URL=http://mdb-api:8099/mdb/api/v1' \
  'BACKEND_API_TIMEOUT_SECONDS=10' 'BACKEND_API_MAX_RETRIES=0' > "$ENV_FILE"
chmod 0644 "$ENV_FILE"   # read by uid 30000 in the container
ENV_MOUNT=(-e APP_ENV=production -v "$(hp "$ENV_FILE"):/app/secrets/.env.production:ro")

# With APP_SRC: mount it and copy it over /app (as the image's user, who owns /app) first
SRC_MOUNT=() SRC_WRAP=()
if [ -n "$APP_SRC" ]; then
  SRC_MOUNT=(-v "$(hp "$APP_SRC"):/appsrc:ro")
  SRC_WRAP=(sh -c 'cp -R /appsrc/. /app/ && exec "$@"' sh)
fi

# ---- helpers -------------------------------------------------------------------------

fail_t() { echo "$*"; exit 1; }                       # fail the current test (runs in a subshell)
expect() { [ "$2" = "$3" ] || fail_t "$1: expected [$3], got [$2]"; }
contains() { case "$2" in *"$3"*) ;; *) fail_t "$1: output does not contain [$3]: $2" ;; esac; }
lacks() { case "$2" in *"$3"*) fail_t "$1: output contains [$3]: $(printf '%s\n' "$2" | grep -F -m3 -- "$3")" ;; esac; }

wait_until() {   # wait_until <tries> <cmd...>: run cmd every 0.5 s until it succeeds
  local n=$1; shift
  while [ "$n" -gt 0 ]; do "$@" >/dev/null 2>&1 && return 0; sleep 0.5; n=$((n - 1)); done
  return 1
}

# Several checks share one container: its script runs commands with emit (EMIT), which
# prints "<key>.rc <exit code>" and "<key>.out <line>" for every line of the command's
# output. facts <group> <output> stores them in F: F[<group>.<key>.rc] is the exit code,
# F[<group>.<key>.out] the output lines. F[<group>] holds the whole output, for messages.
EMIT='emit() { k=$1; shift; o="$("$@" 2>&1)"; r=$?; echo "$k.rc $r"; [ -z "$o" ] || printf "%s\n" "$o" | sed "s/^/$k.out /"; }'
declare -A F=()
facts() {   # facts <group> <output>
  local line k v
  F[$1]="$2"
  while IFS= read -r line; do
    k="$1.${line%% *}" v="${line#* }"
    [ "$line" != "${line%% *}" ] || v=""
    case "$k" in
      *.out) F[$k]="${F[$k]:+${F[$k]}$'\n'}$v" ;;
      *) F[$k]="$v" ;;
    esac
  done <<< "$2"
}

PASSED=0 FAILED=0
run_test() {   # run_test <name> <function> [args...]
  local name="$1" fn="$2"; shift 2
  local out rc=0
  out="$($fn "$@" 2>&1)" || rc=$?
  if [ "$rc" -eq 0 ]; then
    PASSED=$((PASSED + 1)); echo "PASS: $name"
  else
    FAILED=$((FAILED + 1)); echo "FAIL: $name: $(printf '%s' "$out" | head -n 12 | sed '2,$s/^/    /')"
  fi
}

# ---- unit tests ----------------------------------------------------------------------

# Names of the unit tests, read from the test files: "unit: <module>: <docstring>", the
# names run_unit.py prints (it reports a test without a one-line docstring as failed)
unit_names() {
  local f m
  for f in "$TESTS_DIR"/unit/test_*.py; do
    m="${f##*/test_}" m="${m%.py}"
    sed -n -e 's/\r$//' -e '/^    def test_/{n;s/\r$//;s/^ *"""\(.*\)"""$/unit: '"$m"': \1/p}' "$f"
  done
}

# The selected unit tests in one container; counts and prints their PASS/FAIL lines. A
# selected test without a result line counts as failed.
run_unit() {   # run_unit <name>...
  local out rc=0 line name nl=$'\n'
  out="$(docker run --rm --network none --name "$PFX-unit" -e PYTHONDONTWRITEBYTECODE=1 "${ENV_MOUNT[@]}" \
    -v "$(hp "$TESTS_DIR"):/tests:ro" "${SRC_MOUNT[@]}" "$IMAGE" "${SRC_WRAP[@]}" \
    python /tests/unit/run_unit.py "$@" 2>&1)" || rc=$?
  while IFS= read -r line; do
    case "$line" in
      "PASS: "*) PASSED=$((PASSED + 1)); echo "$line" ;;
      "FAIL: "*) FAILED=$((FAILED + 1)); echo "$line" ;;
      "    "*) echo "$line" ;;
    esac
  done <<< "$out"
  for name in "$@"; do
    case "$nl$out$nl" in
      *"${nl}PASS: $name$nl"* | *"${nl}FAIL: $name: "*) ;;
      *) FAILED=$((FAILED + 1))
         echo "FAIL: $name: no result from the unit test container (exit code $rc)"
         printf '%s\n' "$out" | tail -n 5 | sed 's/^/    /' ;;
    esac
  done
}

# ---- image checks --------------------------------------------------------------------
# One container (as the image's user, --network none) collects everything; the tests check
# their part of its output (F[img.*]), or the image's configuration (docker image inspect).

img_setup() {
  facts img "$(docker run --rm --network none --name "$PFX-img" "$IMAGE" sh -c "$EMIT"'
    emit uid id -u
    emit user id -un
    emit pipcheck python -m pip check
    emit imports python -c "import dash, dash_bootstrap_components, dash_ag_grid, plotly, pandas, flask, werkzeug, gunicorn, asgiref, pydantic, pydantic_settings, dotenv, requests, influxdb_client, matplotlib, kaleido"
    emit ipython python -c "import IPython"
    emit ipdb python -c "import ipdb"
  ' 2>&1)" || true
}
need_img() { [ -n "${F[img.uid.rc]:-}" ] || fail_t "the image check container did not run: ${F[img]:-}"; }
inspect() { docker image inspect -f "$1" "$IMAGE"; }

t_user() {
  need_img
  expect "image user" "$(inspect '{{.Config.User}}')" "plotly"
  expect "uid of the image's user" "${F[img.uid.out]:-}" "30000"
  expect "name of the image's user" "${F[img.user.out]:-}" "plotly"
}

t_cmd() {
  local cmd
  cmd="$(inspect '{{join .Config.Cmd " "}}')"
  contains "image command" "$cmd" "gunicorn -b 0.0.0.0:8050"
  contains "image command" "$cmd" " app:server"
  lacks "image command" "$cmd" "--reload"
  expect "entrypoint" "$(inspect '{{json .Config.Entrypoint}}')" "null"
}

t_healthcheck() {
  local hc
  hc="$(inspect '{{if .Config.Healthcheck}}{{join .Config.Healthcheck.Test " "}}{{end}}')"
  contains "HEALTHCHECK" "$hc" "CMD python -c"
  contains "HEALTHCHECK" "$hc" "http://127.0.0.1:8050/app/nuki/"
}

t_pin() {
  local df pins
  df="$(sed -n -e 's/\r$//' -e 's/^ARG PYTHON_IMAGE=//p' "$REPO_DIR/Dockerfile")"
  pins="$(sed -n -e 's/\r$//' -e 's/^NUKI_PYTHON_IMAGE=//p' "$REPO_DIR/compose.pins.env")"
  [ -n "$pins" ] || fail_t "no NUKI_PYTHON_IMAGE in compose.pins.env"
  case "$pins" in *@sha256:*) ;; *) fail_t "NUKI_PYTHON_IMAGE is not pinned by digest: $pins" ;; esac
  expect "Dockerfile ARG PYTHON_IMAGE vs compose.pins.env NUKI_PYTHON_IMAGE" "$df" "$pins"
  expect "Dockerfile FROM" "$(sed -n -e 's/\r$//' -e 's/^FROM //p' "$REPO_DIR/Dockerfile")" '${PYTHON_IMAGE}'
}

t_pip_check() { need_img; expect "pip check (${F[img.pipcheck.out]:-})" "${F[img.pipcheck.rc]:-}" "0"; }
t_imports() { need_img; expect "imports (${F[img.imports.out]:-})" "${F[img.imports.rc]:-}" "0"; }
t_no_dev_tools() {
  need_img
  [ "${F[img.ipython.rc]:-}" != 0 ] || fail_t "IPython is installed"
  [ "${F[img.ipdb.rc]:-}" != 0 ] || fail_t "ipdb is installed"
}

# ---- smoke tests ---------------------------------------------------------------------
# The stub API ("mdb-api" on a private --internal network, run with the image's python) and
# one app container with the image's own command and healthcheck. Facts in F[app.*].

APP="$PFX-app"
app_setup() {
  local api="$PFX-api" cmd=()
  facts app "$( (
    docker network create --internal "$NET" >/dev/null || { echo "net.out could not create $NET"; exit 0; }
    docker run -d --network "$NET" --network-alias mdb-api --name "$api" \
      -v "$(hp "$TESTS_DIR"):/tests:ro" "$IMAGE" python -u /tests/fake_api.py /tests/fixtures 8099 >/dev/null
    wait_until 40 sh -c "docker logs '$api' 2>&1 | grep -q 'listening'" || echo "api.out stub API did not start: $(docker logs "$api" 2>&1 | tail -n 3)"
    # the image's own command (also behind the APP_SRC copy)
    if [ -n "$APP_SRC" ]; then mapfile -t cmd < <(inspect '{{range .Config.Cmd}}{{println .}}{{end}}' | sed '/^$/d'); fi
    docker run -d --network "$NET" --name "$APP" "${ENV_MOUNT[@]}" -v "$(hp "$TESTS_DIR"):/tests:ro" \
      "${SRC_MOUNT[@]}" "$IMAGE" "${SRC_WRAP[@]}" "${cmd[@]}" >/dev/null || { echo "start.out could not start"; exit 0; }
    SECONDS=0
    wait_until 240 sh -c "docker inspect -f '{{.State.Health.Status}} {{.State.Running}}' '$APP' | grep -Eq '^healthy|false$'" || true
    echo "health.out $(docker inspect -f '{{.State.Health.Status}}' "$APP")"
    echo "running.out $(docker inspect -f '{{.State.Running}}' "$APP")"
    echo "secs.out $SECONDS"
    docker exec "$APP" sh -c "$EMIT"'
      emit pid1 sh -c "tr \"\\0\" \" \" < /proc/1/cmdline"
      emit pid1uid stat -c %u /proc/1
      emit index python /tests/smoke_check.py index
      emit callback python /tests/smoke_check.py callback' 2>&1 || true
    docker logs "$APP" 2>&1 | sed 's/^/log.out /'
  ) 2>&1)"
}
need_app() { [ -n "${F[app.health.out]:-}" ] || fail_t "the app container did not start: ${F[app]:-}"; }

t_smoke_healthy() {
  need_app
  expect "health status after ${F[app.secs.out]:-?} s (log: $(printf '%s\n' "${F[app.log.out]:-}" | tail -n 5))" "${F[app.health.out]:-}" "healthy"
  expect "running" "${F[app.running.out]:-}" "true"
  contains "log" "${F[app.log.out]:-}" "app.py loaded"
}

t_smoke_process() {
  need_app
  contains "PID 1" "${F[app.pid1.out]:-}" "gunicorn -b 0.0.0.0:8050"
  lacks "PID 1" "${F[app.pid1.out]:-}" "--reload"
  expect "uid of PID 1" "${F[app.pid1uid.out]:-}" "30000"
}

t_smoke_index() {
  need_app
  expect "GET /app/nuki/ (${F[app.index.out]:-})" "${F[app.index.rc]:-}" "0"
}

t_smoke_callback() {
  need_app
  expect "callback (${F[app.callback.out]:-})" "${F[app.callback.rc]:-}" "0"
}

t_smoke_log() {
  need_app
  lacks "app log" "${F[app.log.out]:-}" "Traceback"
  lacks "app log" "${F[app.log.out]:-}" "An unknown error occurred"
  lacks "app log" "${F[app.log.out]:-}" "No data found"
}

# ---- run -----------------------------------------------------------------------------
#   needs <setup>              the tests registered after it need <setup>_setup ("" for none)
#   add_test [--slow] <name> <function> [args...]   (--quick skips --slow tests)

T_NAMES=() T_CMDS=() T_NEEDS=() T_SLOW=() NEED=""
needs() { NEED="$1"; }
add_test() {
  local slow=0
  if [ "$1" = --slow ]; then slow=1; shift; fi
  T_NAMES+=("$1") T_CMDS+=("${*:2}") T_NEEDS+=("$NEED") T_SLOW+=("$slow")
}

SETUPS_DONE=" "
run_setup() {
  case "$SETUPS_DONE" in *" $1 "*) return 0 ;; esac
  "${1}_setup"
  SETUPS_DONE="$SETUPS_DONE$1 "
}

needs ""
while IFS= read -r n; do add_test "$n" UNIT; done < <(unit_names)

needs img
add_test "image runs as plotly, uid 30000" t_user
add_test "image pip check passes" t_pip_check
add_test "image python packages import" t_imports
add_test "image lacks the dev tools IPython and ipdb" t_no_dev_tools
needs ""
add_test "image command is gunicorn on :8050 without --reload" t_cmd
add_test "image has a HEALTHCHECK on /app/nuki/ with python" t_healthcheck
add_test "image Dockerfile ARG PYTHON_IMAGE equals NUKI_PYTHON_IMAGE in compose.pins.env (digest)" t_pin

needs app
add_test --slow "smoke: app with unreachable InfluxDB starts and becomes healthy" t_smoke_healthy
add_test --slow "smoke: gunicorn is PID 1 as uid 30000, without --reload" t_smoke_process
add_test --slow "smoke: GET /app/nuki/ returns 200 with the Dash page" t_smoke_index
add_test --slow "smoke: Dash callback returns the figures and table built from the stub API data" t_smoke_callback
add_test --slow "smoke: app log has no traceback or data error" t_smoke_log

# Selection: one grep over all names (line numbers of the matching names)
SELECTED=() SKIPPED=0
MATCHED=" $(printf '%s\n' "${T_NAMES[@]}" | grep -nE -- "$TEST_FILTER" | cut -d: -f1 | tr '\n' ' ' || true) "
for i in "${!T_NAMES[@]}"; do
  if [[ "$MATCHED" != *" $((i + 1)) "* ]] || { [ "$QUICK" -eq 1 ] && [ "${T_SLOW[i]}" -eq 1 ]; }; then
    SKIPPED=$((SKIPPED + 1))
  else
    SELECTED+=("$i")
  fi
done
[ "${#SELECTED[@]}" -gt 0 ] || { echo "no test selected (TEST_FILTER='$TEST_FILTER', quick=$QUICK)" >&2; exit 2; }

UNITS=()
for i in "${SELECTED[@]}"; do [ "${T_CMDS[i]}" != UNIT ] || UNITS+=("${T_NAMES[i]}"); done
[ "${#UNITS[@]}" -eq 0 ] || run_unit "${UNITS[@]}"

for i in "${SELECTED[@]}"; do
  [ "${T_CMDS[i]}" != UNIT ] || continue
  [ -z "${T_NEEDS[i]}" ] || run_setup "${T_NEEDS[i]}"
  read -ra cmd <<< "${T_CMDS[i]}"   # function and arguments; the arguments have no spaces
  run_test "${T_NAMES[i]}" "${cmd[@]}"
done

[ "$SKIPPED" -eq 0 ] || echo "SKIPPED: $SKIPPED test(s) (filter/quick)"
TOTAL=$((PASSED + FAILED))
if [ "$FAILED" -eq 0 ]; then
  echo "ALL $TOTAL TESTS PASSED"
  exit 0
fi
echo "$FAILED OF $TOTAL TESTS FAILED"
exit 1
