# debitmetre

A central transparent proxy gateway: it sits between multiple Codex clients and the single OpenAI Codex upstream,
and records per-model, per-machine token usage (raw token facts) for offline cost estimation.
The MVP does not compute prices or produce reports; it only emits best-effort canonical JSONL audits.

## Documentation navigation

New contributors can read in this order to obtain the complete current design, without any external material:

1. [CONTEXT.md](CONTEXT.md) — project-specific glossary of ubiquitous-language terms.
2. [docs/DESIGN.md](docs/DESIGN.md) — **the only authoritative current design specification**:
   fixed routing and protocol behavior, X-Meter-Key authentication and machine mapping, token accounting basis,
   canonical audit schema, fail-open/fail-closed semantics, deployment boundary,
   MVP scope and non-goals, the three TDD seams, issues pending manual PoC, and the initial outcome issues.
3. [docs/adr/](docs/adr/) — architecture decision records (hard-to-reverse trade-offs):
   central gateway boundary (0001), independent Rust transparent proxy (0002),
   token facts vs. pricing/reporting separation (0003).
4. [docs/DESIGN-REVIEW.md](docs/DESIGN-REVIEW.md) — design-grill process record and superseded draft statements.
5. [docs/research/community-codex-proxies.md](docs/research/community-codex-proxies.md) —
   community-implementation research; evidence input only, DESIGN.md wins in case of conflict.

## Collaboration rules

[AGENTS.md](AGENTS.md): delivery-first behavior-based TDD (small red→green vertical loops, no
speculative scope), GitHub issue → PR → acceptance → merge, public-repo privacy red lines,
and reuse of compatible mature crates and community source before writing bespoke infrastructure.

## Run the gateway

Build and run the single binary against a local TOML configuration:

```sh
cargo build --release
sudo install -m 0755 target/release/debitmetre /usr/local/bin/debitmetre
sudo install -m 0600 config.example.toml /etc/debitmetre/config.toml
debitmetre --config /etc/debitmetre/config.toml
```

`--config` defaults to `/etc/debitmetre/config.toml`; use `debitmetre --help` for the full usage text.
Operational logs go to stderr (suitable for a terminal, journald, or systemd) and never print
meter keys or request/response bodies.

### View accumulated usage

Run one local command with the same configuration to see the recorded token facts
accumulated in the configured usage file, grouped by machine and model:

```sh
debitmetre summary --config /etc/debitmetre/config.toml
```

The output is a fixed-width table (machine, model, record count, then input,
uncached, cache-read, cache-write, output, reasoning, and total tokens). Totals
sum only the values actually recorded; a counter that was never recorded shows
`-` and is never invented as 0, and no prices are computed. A final coverage
line reports the overall metering coverage of accepted request lifecycles:
every valid canonical record contributes one accepted lifecycle, and one whose
record carries a non-null usage object counts as metered (partial usage still
counts). An unfinished
trailing line left by a process crash is ignored with a warning while earlier
complete records are still summarized. Warnings go to stderr.

### Configuration

The `machine_keys` table maps the **SHA-256 digest** of each machine's meter key to a stable
machine id. Compute the digest for a meter key once, then paste the lowercase hex output:

```sh
printf '%s' 'test-meter-key-machine-a' | sha256sum
```

```toml
# config.example.toml — synthetic example; do not commit real meter keys
listen = "127.0.0.1:8787"

# Append-only JSONL usage file; opened at startup (fail-closed on bad path).
usage_file = "/var/lib/debitmetre/usage.jsonl"

[machine_keys]
"82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051" = "machine-a"
```

The gateway binds the configured `listen` address (typically loopback, with nginx terminating TLS at
the edge, see DESIGN.md §8) and refuses to start on invalid or unreadable configuration:
missing file, malformed TOML, unknown fields, an invalid listen address, a blank `usage_file`,
an empty `machine_keys` table, a non-64-character lowercase-hex digest, or a blank machine id all
exit non-zero with a useful error. The configured `usage_file` is opened at startup the same way:
an invalid or unwritable path prevents startup, while a transient write failure after startup is
fail-open (the caller-visible upstream response stays unchanged and a sanitized `audit_write_failed`
diagnostic is emitted to stderr).

### Check readiness and traffic

```sh
curl -fsS http://127.0.0.1:8787/healthz    # 200 once configured and listening
```

Codex clients reach the gateway through the existing authenticated transparent proxy routes:
`POST /v1/responses` and `POST /v1/responses/compact` with an `X-Meter-Key` header. The upstream is
fixed in code and redirects are never followed.

### Real-Codex loopback self-test (opt-in)

`scripts/e2e-real-codex.sh` is an operator opt-in system acceptance: it starts
the current `debitmetre` binary on a loopback port, points your existing
authenticated `codex` CLI at it through a temporary model provider, gives it a
tiny deterministic `add(a,b)` task in a disposable git repository, runs an
independent Python test, and prints only sanitized pass/fail evidence (task
test, canonical audit record shape, per-model summary grouping, lifecycle
logs). Only after the explicit `DEBITMETRE_REAL_E2E=1` opt-in does it contact
the real Codex upstream through your own authenticated codex login; it never
inspects OAuth, never prints or persists the synthetic X-Meter key, and never
retains generated artifacts (all temporary state is removed by a trap). It is
strictly opt-in and never runs under `cargo test` or CI:

```sh
DEBITMETRE_REAL_E2E=1 scripts/e2e-real-codex.sh
```

Prerequisites: `cargo`, `codex`, `python3`, `jq`, `curl`, `git`, `sha256sum`,
`timeout`, `mktemp`, `head`, `base64`, `tr`, `cut`, `cat`, `tail`, `sleep`,
`mkdir`, `rm`, `grep`. Without `DEBITMETRE_REAL_E2E=1` the script exits
non-zero and does nothing. The script header lists the configurable loopback
port and step timeout; invalid values (including 0, negatives, fractions, and
option-like strings) fail before any traffic, and the port must be a free
integer in `1..65535`.

### Mock load smoke through the real gateway (opt-in)

`debitmetre smoke` is an operator opt-in load smoke (issue #25): it runs the
external [oha](https://github.com/hatoo/oha) load generator through the **real
release gateway** into a loopback-only deterministic mock upstream that streams
terminal SSE carrying canonical model/usage, then reconciles the canonical audit
records. It contacts no real upstream and consumes zero model tokens. Because it
needs the test-only upstream seam to point the fixed upstream at the mock, it is
available only in a test-feature build:

```sh
OHA_BIN=/path/to/oha cargo build --release --features test-upstream-override
OHA_BIN=/path/to/oha target/release/debitmetre smoke --count 100 --concurrency 10
```

The harness finds oha via `OHA_BIN` or on `PATH`, or `--oha <PATH>`. Workload and
sizing are configurable with safe defaults (`--count 100`, `--concurrency 10`,
`--response-bytes 4096`, `--delay-ms 5`) that still create concurrent live
streams on an ordinary machine; `--port 0` picks a free loopback port. It
reports the oha version/workload, completed/success/errors, reference
RPS + p50/p95/p99, gateway baseline/peak/end RSS, and canonical audit
accepted/metered counts. It fails on load errors, non-2xx responses, or missing
or mismatched audit records, cleans up its local processes and temporary
artifacts, and prints only sanitized aggregate evidence. It never applies an
arbitrary RSS threshold and never changes production behavior.

### Process-level test

The process-level test starts the built binary with synthetic configuration, observes readiness,
a representative request lifecycle against a fake upstream, and fail-closed exits on invalid
configuration. It uses the test-only `test-upstream-override` feature (never enabled in production
builds), which points the fixed upstream at a fake one through `DEBITMETRE_TEST_UPSTREAM`:

```sh
cargo test --all-features --test service_process
```
