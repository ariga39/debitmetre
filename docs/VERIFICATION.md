# debitmetre — verification guide

How to verify a build: the opt-in mock load smoke, the opt-in real-Codex loopback self-test, their
prerequisites, and the process-level test. These are operator/contributor acceptance tools; they do not
change production behavior.

## Mock load smoke through the real gateway (opt-in)

`debitmetre smoke` is an operator opt-in load smoke (issue #25): it runs the external
[oha](https://github.com/hatoo/oha) load generator through the **real release gateway** into a
loopback-only deterministic mock upstream that streams terminal SSE carrying canonical model/usage, then
reconciles the canonical audit records. It contacts no real upstream and consumes zero model tokens. Because
it needs the test-only upstream seam to point the fixed upstream at the mock, it is available only in a
test-feature build:

```sh
OHA_BIN=/path/to/oha cargo build --release --features test-upstream-override
OHA_BIN=/path/to/oha target/release/debitmetre smoke --count 100 --concurrency 10
```

The harness finds oha via `OHA_BIN` or on `PATH`, or `--oha <PATH>`. Workload and sizing are configurable
with safe defaults (`--count 100`, `--concurrency 10`, `--response-bytes 4096`, `--delay-ms 5`) that still
create concurrent live streams on an ordinary machine; `--port 0` picks a free loopback port. It reports the
oha version/workload, completed/success/errors, reference RPS + p50/p95/p99, gateway baseline/peak/end RSS,
and canonical audit accepted/metered counts. It fails on load errors, non-2xx responses, or missing or
mismatched audit records, cleans up its local processes and temporary artifacts, and prints only sanitized
aggregate evidence. It never applies an arbitrary RSS threshold and never changes production behavior.

## Real-Codex loopback self-test (opt-in)

`scripts/e2e-real-codex.sh` is an operator opt-in system acceptance: it starts the current `debitmetre`
binary on a loopback port, points your existing authenticated `codex` CLI at it through a temporary model
provider, and runs the explicit installed `gpt-5.6-luna` model (issue #31) on a deterministic multi-file
Python diagnostic task in a disposable git repository. The generated task has an interacting
loader/aggregate/CLI call chain with two seeded behavioral defects; the independent acceptance command
must **fail before** Codex runs (honest red evidence) and **pass after** Codex repairs the source, while
the task README, data fixture, and acceptance test stay byte-for-byte unchanged (success must come from
real source changes only). It prints only sanitized pass/fail evidence (task red/green, canonical audit
record shape, per-model summary grouping, lifecycle logs, protected-file provenance). It also explicitly
proves, from sanitized method/route/status lifecycle-log evidence, that the Codex model-discovery
`GET /v1/models` request was accepted and received a 2xx upstream response, so a non-fatal local or
upstream 404 cannot be silently hidden by the later task success (issue #29). Only after the explicit
`DEBITMETRE_REAL_E2E=1` opt-in does it contact the real Codex upstream through your own authenticated
codex login; it never inspects OAuth, never prints or persists the synthetic X-Meter key, and never
retains generated artifacts (all temporary state is removed by a trap). It is strictly opt-in and never
runs under `cargo test` or CI:

```sh
DEBITMETRE_REAL_E2E=1 scripts/e2e-real-codex.sh
```

## Diagnostic-task local seam check (no upstream)

`scripts/verify-diagnostic-task.sh` is the testable local seam behind the E2E task: it generates the same
deterministic task repository, proves the acceptance command fails on the seeded defects (red), verifies
the protected contract files are unchanged, applies a minimal reference repair to the source modules,
proves the same acceptance command then passes (green), and verifies the protected files are still
unchanged while the source changed. It never starts the gateway and never calls Codex or any upstream, so
it runs anywhere with `bash`, `python3`, and `sha256sum`:

```sh
scripts/verify-diagnostic-task.sh
```

## Prerequisites

- Mock load smoke: `cargo`, `oha` (via `OHA_BIN`, `--oha`, or `PATH`), and the tooling needed to build the
  test-feature binary.
- Real-Codex self-test: `cargo`, `codex`, `python3`, `jq`, `curl`, `git`, `sha256sum`, `timeout`, `mktemp`,
  `head`, `base64`, `tr`, `cut`, `cat`, `tail`, `sleep`, `mkdir`, `rm`, `grep`, plus the repository's own
  `scripts/gen-diagnostic-task.sh` and `scripts/check-diagnostic-task.sh` task generators. Without
  `DEBITMETRE_REAL_E2E=1` the script exits non-zero and does nothing. The script header lists the
  configurable loopback port and step timeout; invalid values (including 0, negatives, fractions, and
  option-like strings) fail before any traffic, and the port must be a free integer in `1..65535`.
- Diagnostic-task local seam check: `bash`, `python3`, `git`, `mktemp`, `sha256sum`, plus the repository's
  own `scripts/gen-diagnostic-task.sh` and `scripts/check-diagnostic-task.sh` generators; it never needs
  `codex`, the gateway, or an upstream.

## Process-level test

The process-level test starts the built binary with synthetic configuration, observes readiness, a
representative request lifecycle against a fake upstream, and fail-closed exits on invalid configuration. It
uses the test-only `test-upstream-override` feature (never enabled in production builds), which points the
fixed upstream at a fake one through `DEBITMETRE_TEST_UPSTREAM`:

```sh
cargo test --all-features --test service_process
```

For the full behavior-test seams, see DESIGN.md §10.
