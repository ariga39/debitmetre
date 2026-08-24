#!/usr/bin/env bash
#
# Real-Codex loopback self-test (issue #5).
#
# Opt-in system acceptance: starts the current debitmetre binary on a loopback
# port, points the operator's existing authenticated codex CLI at it through a
# temporary model provider, gives it a tiny deterministic add(a,b) task in a
# disposable git repository, runs an independent Python test, and reports only
# sanitized pass/fail evidence for task success, canonical usage, model
# attribution, per-model summary, and diagnostic logs.
#
# Hard opt-in: set DEBITMETRE_REAL_E2E=1. Without it this script exits 2 and
# does nothing; it is never invoked by cargo tests or CI. The synthetic X-Meter
# key is runtime-only (from DEBITMETRE_E2E_METER_KEY or generated); only its
# SHA-256 digest enters the temporary gateway config, and the raw key is never
# printed or persisted. No VPS, Docker, nginx, systemd, or fixed deployment is
# assumed: everything runs on the loopback interface, is torn down by a trap,
# and generated artifacts are never retained.
#
# Prerequisites: bash, cargo, codex, python3, jq, curl, git, sha256sum,
# timeout, mktemp, head, base64, tr, cut, cat, tail, sleep, mkdir, rm, grep.
#
# Configurable via environment:
#   DEBITMETRE_REAL_E2E       must be "1" to run (opt-in guard)
#   DEBITMETRE_E2E_METER_KEY  synthetic X-Meter key (default: generated)
#   DEBITMETRE_E2E_PORT       loopback port (default: 18787; integer 1..65535,
#                             fails clearly if occupied)
#   DEBITMETRE_E2E_TIMEOUT    seconds allowed for the codex step (default: 600;
#                             positive integer bounded at 86400)
#
# Exit status: 0 only when every sanitized check passes; any failure exits
# non-zero after printing a stage-named error. Without the opt-in flag the
# script exits 2.

set -euo pipefail

# Fail with a stage-named, sanitized error. No credential, body, or raw value
# is ever echoed.
die() {
    local stage="$1"
    shift
    echo "debitmetre e2e: error [$stage]: $*" >&2
    exit 1
}

# --- opt-in guard ----------------------------------------------------------
# Never run in normal cargo tests or CI; without the flag this is a no-op that
# exits 2 (exit 0 is reserved for a fully passing E2E).
if [ "${DEBITMETRE_REAL_E2E:-}" != "1" ]; then
    echo "debitmetre e2e: opt-in required: set DEBITMETRE_REAL_E2E=1 to run the real-codex loopback self-test"
    exit 2
fi

# --- location and prerequisites -------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

for tool in cargo codex python3 jq curl git sha256sum timeout mktemp \
    head base64 tr cut cat tail sleep mkdir rm grep; do
    command -v "$tool" >/dev/null 2>&1 || die prereq "missing prerequisite '$tool'"
done

# --- parameters ------------------------------------------------------------
PORT="${DEBITMETRE_E2E_PORT:-18787}"
TIMEOUT="${DEBITMETRE_E2E_TIMEOUT:-600}"

# --- validate parameters before any traffic --------------------------------
# Reject invalid values (including 0, negatives, fractions, and option-like
# strings) before the gateway or codex is ever started.
if ! [[ "$PORT" =~ ^[0-9]{1,5}$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    die port "invalid DEBITMETRE_E2E_PORT '$PORT' (expected an integer port 1..65535)"
fi
if ! [[ "$TIMEOUT" =~ ^[1-9][0-9]{0,4}$ ]] || [ "$TIMEOUT" -gt 86400 ]; then
    die timeout "invalid DEBITMETRE_E2E_TIMEOUT '$TIMEOUT' (expected a positive integer of seconds, at most 86400)"
fi

# --- temporary workspace and cleanup trap ----------------------------------
WORKDIR="$(mktemp -d)"
GATEWAY_PID=""
cleanup() {
    local rc=$?
    if [ -n "$GATEWAY_PID" ]; then
        kill "$GATEWAY_PID" 2>/dev/null || true
        wait "$GATEWAY_PID" 2>/dev/null || true
        GATEWAY_PID=""
    fi
    rm -rf "$WORKDIR"
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- the loopback port must be free; fail clearly if occupied --------------
if python3 - "$PORT" <<'PY'
import socket
import sys

port = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind(("127.0.0.1", port))
except OSError:
    sys.exit(1)
else:
    s.close()
PY
then
    :
else
    die port "loopback port $PORT is already in use; set DEBITMETRE_E2E_PORT to a free port"
fi

# --- synthetic X-Meter key (runtime-only, never printed or persisted) ------
# The raw key lives only in this process's environment; the temporary gateway
# config stores only its SHA-256 digest, and codex sends it through the
# env_http_headers reference to DEBITMETRE_E2E_METER_KEY.
KEY="${DEBITMETRE_E2E_METER_KEY:-}"
if [ -z "$KEY" ]; then
    KEY="dm-e2e-$(head -c 24 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9')"
fi
export DEBITMETRE_E2E_METER_KEY="$KEY"
DIGEST="$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)"

# --- disposable gateway config (digest only, no raw key) -------------------
# The usage_file path is serialized as a TOML basic string by python3 (an
# existing prerequisite), so a path containing backslashes, double quotes,
# newlines, tabs, carriage returns, or any other control character is always
# escaped correctly and can never break or extend the config.
umask 077
USAGE_FILE="$WORKDIR/usage.jsonl"
USAGE_FILE_ESC="$(python3 - "$USAGE_FILE" <<'PY'
import sys


def toml_basic(value):
    out = []
    for ch in value:
        code = ord(ch)
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif code < 0x20 or code == 0x7F:
            out.append("\\u%04x" % code)
        else:
            out.append(ch)
    return "".join(out)


print(toml_basic(sys.argv[1]))
PY
)"
cat > "$WORKDIR/gateway.toml" <<EOF
listen = "127.0.0.1:$PORT"
usage_file = "$USAGE_FILE_ESC"

[machine_keys]
"$DIGEST" = "machine-e2e"
EOF

# --- build the current binary ----------------------------------------------
if ! cargo build --release > "$WORKDIR/build.log" 2>&1; then
    tail -20 "$WORKDIR/build.log" >&2 || true
    die build "cargo build --release failed"
fi
BIN="$REPO_ROOT/target/release/debitmetre"

# --- start the gateway on the loopback port --------------------------------
"$BIN" --config "$WORKDIR/gateway.toml" > "$WORKDIR/gateway.log" 2>&1 &
GATEWAY_PID=$!

ready=0
for ((attempt = 0; attempt < 30; attempt++)); do
    if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" != "1" ]; then
    die start "gateway did not become ready on 127.0.0.1:$PORT"
fi

# --- disposable task repository with an independent Python test ------------
mkdir -p "$WORKDIR/task-repo/src"
cat > "$WORKDIR/task-repo/README.md" <<'EOF'
# tiny add task

Implement the missing `add` function in `src/add.py`.

Independent success check (do not modify `test_add.py`):

    python3 test_add.py
EOF
cat > "$WORKDIR/task-repo/test_add.py" <<'PY'
import sys
sys.path.insert(0, "src")
from add import add

cases = [(1, 2, 3), (-1, 1, 0), (2, 3, 5), (100, 200, 300)]
for a, b, expected in cases:
    got = add(a, b)
    assert got == expected, f"add({a}, {b}) = {got}, expected {expected}"
print("all add() tests passed")
PY
cat > "$WORKDIR/task-repo/src/add.py" <<'PY'
def add(a, b):
    raise NotImplementedError("implement add(a, b)")
PY
git -C "$WORKDIR/task-repo" init -q
git -C "$WORKDIR/task-repo" add -A
git -C "$WORKDIR/task-repo" -c user.name="debitmetre-e2e" -c user.email="e2e@example.com" commit -qm "task scaffold"

# --- run the authenticated codex CLI against the local gateway -------------
# A fixed marker inside the prompt lets the log check prove that request
# bodies never reach the gateway's diagnostic logs.
MARKER="DEBITMETRE-E2E-TASK-BODY-MARKER"
PROMPT="$MARKER Implement the missing add function in src/add.py so that the independent check 'python3 test_add.py' passes. Do not modify test_add.py."
CODEX_EXIT=0
if timeout "$TIMEOUT" codex exec -C "$WORKDIR/task-repo" \
    -c 'model_providers.debitmetre.name="debitmetre"' \
    -c "model_providers.debitmetre.base_url=\"http://127.0.0.1:$PORT/v1\"" \
    -c 'model_providers.debitmetre.wire_api="responses"' \
    -c 'model_providers.debitmetre.requires_openai_auth=true' \
    -c 'model_providers.debitmetre.env_http_headers={ "X-Meter-Key" = "DEBITMETRE_E2E_METER_KEY" }' \
    -c 'model_provider="debitmetre"' \
    --dangerously-bypass-approvals-and-sandbox --ephemeral \
    "$PROMPT" < /dev/null > "$WORKDIR/codex-run.out" 2>&1
then
    CODEX_EXIT=0
else
    CODEX_EXIT=$?
    die codex "codex exec failed (exit $CODEX_EXIT after up to ${TIMEOUT}s)"
fi

# --- independent Python test (the task's ground truth) ---------------------
if ! ( cd "$WORKDIR/task-repo" && python3 test_add.py ) > "$WORKDIR/test.out" 2>&1; then
    die test "independent add(a,b) test failed: $(tail -1 "$WORKDIR/test.out" 2>/dev/null || true)"
fi

# --- stop the gateway so the audit JSONL is fully flushed ------------------
kill "$GATEWAY_PID" 2>/dev/null || true
wait "$GATEWAY_PID" 2>/dev/null || true
GATEWAY_PID=""
sleep 1

# --- validate audit records; no raw records are printed --------------------
# At least one record must be a canonical schema_version=1 request record with
# the required lifecycle fields, a non-null model, exactly the seven canonical
# usage counters, every present counter a number that is integral and
# nonnegative, and some present counter positive.
if ! jq -e --slurp '
    any(.[];
        (. | keys | sort) == [
            "accounting_quality", "event_id", "kind", "machine_id",
            "metering_error", "model", "operation", "outcome",
            "schema_version", "timestamp", "upstream_status", "usage"
        ]
        and (.schema_version == 1)
        and (.kind == "request")
        and ((.machine_id | type == "string") and (.machine_id | length > 0))
        and ((.model | type == "string") and (.model | length > 0))
        and (.operation == "response" or .operation == "compaction")
        and (.outcome == "completed" or .outcome == "incomplete"
            or .outcome == "upstream_error" or .outcome == "transport_error"
            or .outcome == "upstream_interrupted" or .outcome == "client_cancelled")
        and (.accounting_quality == "complete" or .accounting_quality == "partial"
            or .accounting_quality == "inconsistent" or .accounting_quality == "unavailable")
        and (.metering_error == null or .metering_error == "missing_usage"
            or .metering_error == "malformed_usage" or .metering_error == "event_too_large")
        and (.upstream_status == null
            or ((.upstream_status | type == "number") and (.upstream_status | floor == .)
                and (.upstream_status >= 100) and (.upstream_status <= 599)))
        and (.event_id | type == "string")
        and (.event_id | test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"))
        and (.timestamp | type == "string")
        and (.timestamp | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"))
        and (.usage != null)
        and (.usage | keys | sort) == ["cache_read", "cache_write", "input_total", "output_total", "reasoning", "total", "uncached"]
        and ([.usage[] | select(. != null)] | all(.[]; (type == "number") and (floor == .) and (. >= 0)))
        and ([.usage[] | select(. != null)] | any(.[]; (type == "number") and (. > 0)))
    )' "$WORKDIR/usage.jsonl" >/dev/null 2>&1
then
    die validate "no canonical schema_version=1 request record with the seven canonical usage counters, a non-null model, and integral nonnegative nonzero usage"
fi

# --- debitmetre summary corresponds to the canonical audit records ---------
# The expected (machine_id, model) groups and totals are derived from the
# accepted canonical records; each must appear in the summary with the same
# record count and a matching positive total. Per-model names and token totals
# are never printed.
if ! "$BIN" summary --config "$WORKDIR/gateway.toml" > "$WORKDIR/summary.out" 2> "$WORKDIR/summary.err"; then
    die summary "debitmetre summary command failed"
fi
if ! SUMMARY_ROWS="$(python3 - "$WORKDIR/usage.jsonl" "$WORKDIR/summary.out" <<'PY'
import json
import sys

usage_file, summary_file = sys.argv[1], sys.argv[2]

groups = {}
with open(usage_file, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec.get("schema_version") != 1 or rec.get("kind") != "request":
            continue
        if rec.get("usage") is None or rec.get("model") is None:
            continue
        key = (rec.get("machine_id"), rec.get("model"))
        count, total, present = groups.get(key, (0, 0, False))
        count += 1
        value = rec["usage"].get("total")
        if value is not None:
            total += value
            present = True
        groups[key] = (count, total, present)

if not groups:
    print("no canonical usage group found in the audit file", file=sys.stderr)
    sys.exit(1)

rows = {}
with open(summary_file, encoding="utf-8") as f:
    lines = f.readlines()
for line in lines[1:]:
    fields = line.split()
    if len(fields) < 10:
        continue
    machine, model = fields[0], fields[1]
    if machine == "-" or model == "-" or model == "=":
        continue
    rows[(machine, model)] = fields

ok = True
for key, (count, total, present) in groups.items():
    machine, model = key
    row = rows.get(key)
    if row is None:
        print(f"summary missing group machine={machine}", file=sys.stderr)
        ok = False
        continue
    records = int(row[2])
    total_cell = row[9]
    if records != count:
        print(f"summary records mismatch machine={machine}: expected {count} got {records}", file=sys.stderr)
        ok = False
    if not present:
        if total_cell != "-":
            print(f"summary total should be '-' machine={machine} got {total_cell}", file=sys.stderr)
            ok = False
    else:
        if total_cell == "-" or int(total_cell) != total or total <= 0:
            print(f"summary total mismatch machine={machine}: expected {total} got {total_cell}", file=sys.stderr)
            ok = False

if not any(present and total > 0 for (_count, total, present) in groups.values()):
    print("no canonical usage group has a positive total", file=sys.stderr)
    ok = False

if not ok:
    sys.exit(1)
print(len(groups))
PY
)"
then
    die summary "debitmetre summary does not correspond to the accepted canonical audit records"
fi

# --- diagnostic logs: lifecycle events present, no key or body leak --------
LOG="$WORKDIR/gateway.log"
if ! grep -q 'request accepted' "$LOG"; then
    die logs "gateway log lacks an accepted-request lifecycle event"
fi
if ! grep -q 'upstream response' "$LOG"; then
    die logs "gateway log lacks an upstream-response lifecycle event"
fi
if grep -qF "$KEY" "$LOG"; then
    die logs "gateway log contains the raw meter key"
fi
if grep -qF "$MARKER" "$LOG"; then
    die logs "gateway log contains the task body marker"
fi

# --- the runtime meter key must never appear in any generated artifact ------
# Scanned before cleanup (the trap removes everything): a leak is a hard
# failure, not something to preserve or emit.
if grep -rqF --exclude-dir=.git -- "$KEY" "$WORKDIR" 2>/dev/null; then
    die keyleak "raw meter key found in a generated artifact"
fi

# --- concise sanitized evidence --------------------------------------------
# Only aggregate counts are printed: never raw records, prompts, responses,
# model names, exact token totals, credentials, the Codex transcript, or the
# task body.
RECORDS="$(jq --slurp 'length' "$WORKDIR/usage.jsonl")"
echo "debitmetre e2e: PASS task: independent add(a,b) Python test passed"
echo "debitmetre e2e: PASS audit: canonical schema_version=1 request record with required lifecycle fields, exactly seven usage counters, non-null model, and integral nonnegative nonzero usage"
echo "debitmetre e2e: PASS summary: per-model summary groups match the accepted canonical audit records with nonzero totals"
echo "debitmetre e2e: PASS logs: accepted/upstream lifecycle events; no raw meter key or task body marker"
echo "debitmetre e2e: PASS artifacts: raw meter key absent from all generated artifacts"
echo "debitmetre e2e: evidence: codex_exit=$CODEX_EXIT records=$RECORDS port=$PORT"
echo "debitmetre e2e: evidence: summary_rows=$SUMMARY_ROWS has_nonzero=1"
