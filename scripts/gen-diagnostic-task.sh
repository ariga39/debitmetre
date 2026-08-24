#!/usr/bin/env bash
#
# Generate the deterministic multi-file Python diagnostic task (issue #31).
#
# Writes a self-contained, standard-library-only Python task repository into
# <dest-dir> with an interacting loader/aggregate/CLI call chain and two seeded
# behavioral defects that a code-reading model must find and repair. The task
# README, data fixture, and acceptance test are contract files that a correct
# run must leave byte-for-byte unchanged; success must come only from editing
# the source modules under src/.
#
# The repository is deterministic (fixed files, fixed seed data) and is
# committed as its own disposable git repo so the E2E and the local seam test
# can detect any unauthorized modification of protected files.
#
# Usage: scripts/gen-diagnostic-task.sh <dest-dir>
#
# Prints the destination directory on success.

set -euo pipefail

DEST="${1:?usage: gen-diagnostic-task.sh <dest-dir>}"

# --- deterministic multi-file task -----------------------------------------
# Order report task: a CLI loads line-based orders, aggregates them, and prints
# a single report line. Two independent defects live in the source modules:
#   * loader.py   ignores the recorded quantity (treats every line as qty=1)
#   * aggregate.py applies the tax twice
# Their combined effect is visible only through the CLI-level report total, so
# the acceptance command (which asserts the exact report line) fails until both
# defects are repaired in source. README.md, data/orders.txt, and
# test_report.py are protected contract files.

mkdir -p "$DEST/src" "$DEST/data"

cat > "$DEST/README.md" <<'EOF'
# order report task

This tiny program loads line-based orders and prints an aggregated checkout
report. Repair the two seeded defects so that the independent acceptance check
passes.

Independent success check (do not modify `test_report.py`, `data/orders.txt`,
or this README; repair only the source modules under `src/`):

    python3 test_report.py

The expected report line is printed by the acceptance check.
EOF

cat > "$DEST/data/orders.txt" <<'EOF'
A-100:2:1250
B-200:1:800
C-300:4:300
EOF

cat > "$DEST/src/loader.py" <<'PY'
def load_orders(path):
    orders = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sku, qty, price = line.split(":")
            orders.append({"sku": sku, "qty": 1, "unit_price": int(price)})
    return orders
PY

cat > "$DEST/src/aggregate.py" <<'PY'
TAX_RATE = 0.08


def compute_totals(orders):
    subtotal = sum(o["qty"] * o["unit_price"] for o in orders)
    tax = int(subtotal * TAX_RATE) * 2
    return {"subtotal": subtotal, "tax": tax, "total": subtotal + tax}


def format_report(totals):
    return "subtotal={} tax={} total={}".format(
        totals["subtotal"], totals["tax"], totals["total"]
    )
PY

cat > "$DEST/src/main.py" <<'PY'
import sys

from aggregate import compute_totals, format_report
from loader import load_orders


def main(argv):
    if len(argv) != 2:
        print("usage: python3 main.py <orders-file>", file=sys.stderr)
        return 2
    orders = load_orders(argv[1])
    totals = compute_totals(orders)
    print(format_report(totals))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
PY

cat > "$DEST/test_report.py" <<'PY'
import subprocess
import sys

EXPECTED = "subtotal=4500 tax=360 total=4860"

out = subprocess.run(
    [sys.executable, "src/main.py", "data/orders.txt"],
    capture_output=True,
    text=True,
)
if out.returncode != 0:
    print("CLI failed:", out.stderr, file=sys.stderr)
    sys.exit(1)
report = out.stdout.strip().splitlines()[-1]
if report != EXPECTED:
    print("got: %r\nexpected: %r" % (report, EXPECTED), file=sys.stderr)
    sys.exit(1)
print("all order report tests passed")
PY

git -C "$DEST" init -q
git -C "$DEST" add -A
git -C "$DEST" -c user.name="debitmetre-e2e" -c user.email="e2e@example.com" \
    commit -qm "task scaffold"

echo "$DEST"
