#!/usr/bin/env bash
#
# Local seam test for the deterministic diagnostic task (issue #31).
#
# Captures honest red evidence without calling any upstream: it generates the
# task repository, proves the independent acceptance command fails on the seeded
# defects, verifies the protected contract files are unchanged, applies a
# minimal reference repair to the source modules, proves the *same* acceptance
# command then passes, and verifies the protected files are still byte-for-byte
# unchanged while the source changed. This is the testable process-level seam:
# it never starts the gateway and never invokes Codex.
#
# Usage: scripts/verify-diagnostic-task.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

WORKDIR="$(mktemp -d)"
cleanup() {
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# --- red: the generated acceptance command fails on the seeded defects -------
"$SCRIPT_DIR/gen-diagnostic-task.sh" "$WORKDIR/task" >/dev/null
PROTECTED=(README.md data/orders.txt data/orders2.txt test_report.py)
BEFORE=()
for f in "${PROTECTED[@]}"; do
    BEFORE+=("$(sha256sum "$WORKDIR/task/$f" | cut -d' ' -f1)")
done

if "$SCRIPT_DIR/check-diagnostic-task.sh" "$WORKDIR/task" >"$WORKDIR/red.out" 2>&1; then
    echo "diagnostic task seam: red FAILED - seeded acceptance unexpectedly passed" >&2
    exit 1
fi
echo "diagnostic task seam: RED acceptance failed on seeded defects (no upstream)"

# --- minimal green: apply the reference repair to source modules only ---------
cat > "$WORKDIR/task/src/loader.py" <<'PY'
def load_orders(path):
    orders = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sku, qty, price = line.split(":")
            orders.append({"sku": sku, "qty": int(qty), "unit_price": int(price)})
    return orders
PY

cat > "$WORKDIR/task/src/aggregate.py" <<'PY'
TAX_RATE = 0.08


def compute_totals(orders):
    subtotal = sum(o["qty"] * o["unit_price"] for o in orders)
    tax = int(subtotal * TAX_RATE)
    return {"subtotal": subtotal, "tax": tax, "total": subtotal + tax}


def format_report(totals):
    return "subtotal={} tax={} total={}".format(
        totals["subtotal"], totals["tax"], totals["total"]
    )
PY

"$SCRIPT_DIR/check-diagnostic-task.sh" "$WORKDIR/task" >"$WORKDIR/green.out" 2>&1
echo "diagnostic task seam: GREEN acceptance passed after source repair"

# --- protected files must be unchanged; source must have changed ---------------
for i in "${!PROTECTED[@]}"; do
    f="${PROTECTED[$i]}"
    now="$(sha256sum "$WORKDIR/task/$f" | cut -d' ' -f1)"
    if [ "$now" != "${BEFORE[$i]}" ]; then
        echo "diagnostic task seam: FAILED protected file changed: $f" >&2
        exit 1
    fi
done
echo "diagnostic task seam: protected files (README, fixture, acceptance) unchanged"

if git -C "$WORKDIR/task" diff --quiet -- src/; then
    echo "diagnostic task seam: FAILED source modules were not modified" >&2
    exit 1
fi
echo "diagnostic task seam: source modules changed to achieve the pass"

echo "diagnostic task seam: PASS (red evidence -> source repair -> green evidence)"
