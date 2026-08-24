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
# Hard opt-in: set DEBITMETRE_REAL_E2E=1. Without it this script is a safe
# no-op; it is never invoked by cargo tests or CI. The synthetic X-Meter key is
# runtime-only (from DEBITMETRE_E2E_METER_KEY or generated); only its SHA-256
# digest enters the temporary gateway config, and the raw key is never printed
# or persisted. No VPS, Docker, nginx, systemd, or fixed deployment is assumed:
# everything runs on the loopback interface and is torn down by a trap unless
# DEBITMETRE_E2E_KEEP=1.
#
# Prerequisites: bash, cargo, codex, python3, jq, curl, git, sha256sum,
# timeout, awk.
#
# Configurable via environment:
#   DEBITMETRE_REAL_E2E       must be "1" to run (opt-in guard)
#   DEBITMETRE_E2E_METER_KEY  synthetic X-Meter key (default: generated)
#   DEBITMETRE_E2E_PORT       loopback port (default: 18787; fails clearly if occupied)
#   DEBITMETRE_E2E_TIMEOUT    seconds allowed for the codex step (default: 600)
#   DEBITMETRE_E2E_KEEP       "1" retains artifacts in the temp dir
#
# Exit status: 0 only when every sanitized check passes; any failure exits
# non-zero after printing a stage-named error.

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
# Never run in normal cargo tests or CI; without the flag this is a no-op.
if [ "${DEBITMETRE_REAL_E2E:-}" != "1" ]; then
    echo "debitmetre e2e: opt-in required: set DEBITMETRE_REAL_E2E=1 to run the real-codex loopback self-test"
    exit 0
fi

# --- location and prerequisites -------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

for tool in cargo codex python3 jq curl git sha256sum timeout awk; do
    command -v "$tool" >/dev/null 2>&1 || die prereq "missing prerequisite '$tool'"
done

# --- parameters ------------------------------------------------------------
PORT="${DEBITMETRE_E2E_PORT:-18787}"
TIMEOUT="${DEBITMETRE_E2E_TIMEOUT:-600}"
KEEP="${DEBITMETRE_E2E_KEEP:-0}"

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
    if [ "$KEEP" = "1" ]; then
        echo "debitmetre e2e: artifacts kept in $WORKDIR" >&2
    else
        rm -rf "$WORKDIR"
    fi
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
umask 077
cat > "$WORKDIR/gateway.toml" <<EOF
listen = "127.0.0.1:$PORT"
usage_file = "$WORKDIR/usage.jsonl"

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
if ! timeout "$TIMEOUT" codex exec -C "$WORKDIR/task-repo" \
    -c 'model_providers.debitmetre.name="debitmetre"' \
    -c "model_providers.debitmetre.base_url=\"http://127.0.0.1:$PORT/v1\"" \
    -c 'model_providers.debitmetre.wire_api="responses"' \
    -c 'model_providers.debitmetre.requires_openai_auth=true' \
    -c 'model_providers.debitmetre.env_http_headers={ "X-Meter-Key" = "DEBITMETRE_E2E_METER_KEY" }' \
    -c 'model_provider="debitmetre"' \
    --dangerously-bypass-approvals-and-sandbox --ephemeral \
    "$PROMPT" < /dev/null > "$WORKDIR/codex-run.out" 2>&1
then
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
if ! jq -e --slurp '
    any(.[];
        (.kind == "request")
        and (.usage != null)
        and (.model != null)
        and ([.usage | to_entries[] | select(.value != null) | .value] | all(.[]; . >= 0))
        and ([.usage | to_entries[] | select(.value != null) | .value] | any(.[]; . > 0))
    )' "$WORKDIR/usage.jsonl" >/dev/null 2>&1
then
    die validate "no accepted audit record with non-null usage and model, nonnegative counters, and nonzero usage"
fi

# --- debitmetre summary: a non-missing model group with nonzero totals -----
if ! "$BIN" summary --config "$WORKDIR/gateway.toml" > "$WORKDIR/summary.out" 2> "$WORKDIR/summary.err"; then
    die summary "debitmetre summary command failed"
fi
if ! awk 'NR >= 2 && $1 != "machine" && $2 != "-" && $2 != "=" && $NF ~ /^[0-9]+$/ && $NF > 0 { found = 1 } END { exit found ? 0 : 1 }' "$WORKDIR/summary.out"
then
    die summary "debitmetre summary shows no model group with nonzero totals"
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

# --- concise sanitized evidence --------------------------------------------
RECORDS="$(jq --slurp 'length' "$WORKDIR/usage.jsonl")"
echo "debitmetre e2e: PASS task: independent add(a,b) Python test passed"
echo "debitmetre e2e: PASS audit: accepted record has non-null usage and model, nonnegative counters, nonzero usage"
echo "debitmetre e2e: PASS summary: debitmetre summary shows a model group with nonzero totals"
echo "debitmetre e2e: PASS logs: accepted/upstream lifecycle events; no raw meter key or task body marker"
echo "debitmetre e2e: evidence: codex_exit=$CODEX_EXIT records=$RECORDS port=$PORT"
echo "debitmetre e2e: evidence: debitmetre summary (sanitized aggregate):"
cat "$WORKDIR/summary.out"