#!/usr/bin/env bash
#
# Independent acceptance command for the diagnostic task (issue #31).
#
# Runs the deterministic task's acceptance test against a task repository and
# reports pass/fail. This is the same command the E2E uses both before invoking
# Codex (must fail on the seeded defects) and after Codex finishes (must pass).
#
# Usage: scripts/check-diagnostic-task.sh <task-dir>

set -euo pipefail

TASK_DIR="${1:?usage: check-diagnostic-task.sh <task-dir>}"

( cd "$TASK_DIR" && python3 test_report.py )
