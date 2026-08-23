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

### Configuration

The `machine_keys` table maps the **SHA-256 digest** of each machine's meter key to a stable
machine id. Compute the digest for a meter key once, then paste the lowercase hex output:

```sh
printf '%s' 'your-meter-key' | sha256sum
```

```toml
# config.example.toml — synthetic example; do not commit real meter keys
listen = "127.0.0.1:8787"

[machine_keys]
"82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051" = "machine-a"
```

The gateway binds the configured `listen` address (typically loopback, with nginx terminating TLS at
the edge, see DESIGN.md §8) and refuses to start on invalid or unreadable configuration:
missing file, malformed TOML, unknown fields, an invalid listen address, an empty `machine_keys`
table, a non-64-character lowercase-hex digest, or a blank machine id all exit non-zero with a useful error.

### Check readiness and traffic

```sh
curl -fsS http://127.0.0.1:8787/healthz    # 200 once configured and listening
```

Codex clients reach the gateway through the existing authenticated transparent proxy routes:
`POST /v1/responses` and `POST /v1/responses/compact` with an `X-Meter-Key` header. The upstream is
fixed in code and redirects are never followed.

### Process-level test

The process-level test starts the built binary with synthetic configuration, observes readiness,
a representative request lifecycle against a fake upstream, and fail-closed exits on invalid
configuration. It uses the test-only `test-upstream-override` feature (never enabled in production
builds), which points the fixed upstream at a fake one through `DEBITMETRE_TEST_UPSTREAM`:

```sh
cargo test --all-features --test service_process
```
