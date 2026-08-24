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
`-` and is never invented as 0, and no prices are computed. An unfinished
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
test, audit record shape, per-model summary totals, lifecycle logs). It never
contacts a paid upstream beyond your own codex login, never inspects OAuth, and
never prints or persists the synthetic X-Meter key. It is strictly opt-in and
never runs under `cargo test` or CI:

```sh
DEBITMETRE_REAL_E2E=1 scripts/e2e-real-codex.sh
```

Prerequisites: `cargo`, `codex`, `python3`, `jq`, `curl`, `git`, `sha256sum`,
`timeout`, `awk`. Without `DEBITMETRE_REAL_E2E=1` the script is a safe no-op.
The script header lists the configurable loopback port, step timeout, meter
key, and `DEBITMETRE_E2E_KEEP=1` artifact-retention flag.

### Process-level test

The process-level test starts the built binary with synthetic configuration, observes readiness,
a representative request lifecycle against a fake upstream, and fail-closed exits on invalid
configuration. It uses the test-only `test-upstream-override` feature (never enabled in production
builds), which points the fixed upstream at a fake one through `DEBITMETRE_TEST_UPSTREAM`:

```sh
cargo test --all-features --test service_process
```
